//! SIGHUP-driven configuration hot reload. Loads a new
//! [`crate::config::Config`] from disk, classifies each changed field
//! as hot-reloadable or restart-required, and atomically swaps the
//! shared [`arc_swap::ArcSwap`] handle on success.

use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::config::Config;

/// Outcome of a single SIGHUP-triggered reload attempt. Reported via the
/// `halephant.config.reloads` counter and the structured log event on the
/// `SIGHUP` branch in `main`.
#[derive(Debug)]
pub enum ReloadOutcome {
    /// New config loaded and swapped in atomically. The caller runs
    /// `topology.refresh()` and `pools.warm_up()` so any added nodes,
    /// pools, or `min_connections` floors take effect immediately.
    Success,
    /// The new config parsed successfully but changes one or more
    /// fields that are bound at startup (listen sockets, OTel
    /// providers, logging subscriber, …). The old config stays in
    /// place; operators must restart the process to apply the change.
    RestartRequired(&'static str),
    /// The file could not be read, could not be parsed, or failed
    /// validation. The old config stays in place.
    ParseFailed(anyhow::Error),
}

/// Attempt to reload the config from disk and swap it into the shared
/// `ArcSwap`. On success the swap is atomic and immediately visible to
/// every component holding a handle to `config`; the caller is
/// responsible for running side effects (topology refresh, warm-up).
/// On failure the swap is not performed.
///
/// This is `async` because it offloads the file read + TOML parse to
/// `tokio::task::spawn_blocking`: `Config::load` uses
/// `std::fs::read_to_string` under the hood, and running that on an
/// async worker would stall the accept loop and every other task on
/// the same worker for the duration of the disk read — potentially
/// noticeable on NFS-mounted or encrypted filesystems.
pub async fn reload_config(config_path: &Path, config: &ArcSwap<Config>) -> ReloadOutcome {
    // Clone the path into the blocking task because the closure
    // requires `'static` data; `PathBuf` is a small, cheap clone.
    let config_path_owned = config_path.to_path_buf();
    let load_result = tokio::task::spawn_blocking(move || Config::load(&config_path_owned)).await;

    let new_config = match load_result {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => return ReloadOutcome::ParseFailed(e.into()),
        Err(join_err) => {
            return ReloadOutcome::ParseFailed(anyhow::anyhow!(
                "config load task failed: {join_err}"
            ));
        }
    };

    // `load_full` snapshots the current config for field comparison.
    // Holding the snapshot only for the duration of `requires_restart`
    // keeps the window between the comparison and the subsequent
    // `store` small; a concurrent reload is impossible because SIGHUP
    // is serialized through the main select loop.
    let current = config.load_full();
    if let Some(field) = requires_restart(&current, &new_config) {
        return ReloadOutcome::RestartRequired(field);
    }

    config.store(Arc::new(new_config));
    ReloadOutcome::Success
}

/// Compare two configs and return the first field whose change cannot
/// be applied without a process restart. Returns `None` when every
/// difference is safe to hot-reload.
///
/// Restart-required fields fall into three categories:
/// - **Bound at startup:** `server.listen`, `admin.listen`. The TCP
///   sockets are created once during `main` and cannot be rebound
///   under an already-serving process.
/// - **Cached into long-lived handles:** `server.pgpass` (loaded into
///   `Arc<Pgpass>` once), `server.workers` (consumed by the tokio
///   runtime).
/// - **Subscriber / exporter configuration:** `otel.endpoint`,
///   `otel.service_name`, `logging.format`, `logging.level`. These
///   configure the tracing subscriber and OTel providers that live
///   for the process lifetime.
pub fn requires_restart(old: &Config, new: &Config) -> Option<&'static str> {
    if old.server.listen != new.server.listen {
        return Some("server.listen");
    }
    if old.server.pgpass != new.server.pgpass {
        return Some("server.pgpass");
    }
    if old.server.workers != new.server.workers {
        return Some("server.workers");
    }
    if old.admin.listen != new.admin.listen {
        return Some("admin.listen");
    }
    if old.otel.endpoint != new.otel.endpoint {
        return Some("otel.endpoint");
    }
    if old.otel.service_name != new.otel.service_name {
        return Some("otel.service_name");
    }
    if old.logging.format != new.logging.format {
        return Some("logging.format");
    }
    if old.logging.level != new.logging.level {
        return Some("logging.level");
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;

    /// Minimal TOML config with a single cluster — used as the baseline
    /// for field-mutation tests. Every `requires_restart` case builds a
    /// pair of configs from this template with one field altered.
    const BASE_TOML: &str = r#"
        [server]
        listen = ["0.0.0.0:6432"]

        [logging]
        level = "info"

        [cluster.main]
        nodes = ["127.0.0.1:5432"]

        [cluster.main.pool.mydb.user.myuser]
    "#;

    fn base_config() -> Config {
        Config::parse(BASE_TOML).unwrap()
    }

    // ---- requires_restart: identity -------------------------------------

    #[test]
    fn requires_restart_identical_configs_returns_none() {
        let a = base_config();
        let b = base_config();
        assert_eq!(requires_restart(&a, &b), None);
    }

    // ---- requires_restart: restart-bound fields -------------------------

    #[test]
    fn requires_restart_flags_listen_change() {
        let old = base_config();
        let new = Config::parse(&BASE_TOML.replace(
            r#"listen = ["0.0.0.0:6432"]"#,
            r#"listen = ["0.0.0.0:7000"]"#,
        ))
        .unwrap();
        assert_eq!(requires_restart(&old, &new), Some("server.listen"));
    }

    #[test]
    fn requires_restart_flags_pgpass_change() {
        let old = base_config();
        let mut new = base_config();
        new.server.pgpass = Some(PathBuf::from("/tmp/pgpass"));
        assert_eq!(requires_restart(&old, &new), Some("server.pgpass"));
    }

    #[test]
    fn requires_restart_flags_workers_change() {
        let old = base_config();
        let mut new = base_config();
        new.server.workers = 4;
        assert_eq!(requires_restart(&old, &new), Some("server.workers"));
    }

    #[test]
    fn requires_restart_flags_admin_listen_change() {
        let old = base_config();
        let mut new = base_config();
        new.admin.listen = Some("0.0.0.0:6433".into());
        assert_eq!(requires_restart(&old, &new), Some("admin.listen"));
    }

    #[test]
    fn requires_restart_flags_otel_endpoint_change() {
        let old = base_config();
        let mut new = base_config();
        new.otel.endpoint = Some("http://collector:4317".into());
        assert_eq!(requires_restart(&old, &new), Some("otel.endpoint"));
    }

    #[test]
    fn requires_restart_flags_otel_service_name_change() {
        let old = base_config();
        let mut new = base_config();
        new.otel.service_name = "halephant-prod".into();
        assert_eq!(requires_restart(&old, &new), Some("otel.service_name"));
    }

    #[test]
    fn requires_restart_flags_logging_format_change() {
        let old = base_config();
        let mut new = base_config();
        new.logging.format = crate::config::logging::LogFormat::Text;
        assert_eq!(requires_restart(&old, &new), Some("logging.format"));
    }

    #[test]
    fn requires_restart_flags_logging_level_change() {
        let old = base_config();
        let mut new = base_config();
        new.logging.level = "debug".into();
        assert_eq!(requires_restart(&old, &new), Some("logging.level"));
    }

    // ---- requires_restart: hot-reloadable fields ------------------------

    #[test]
    fn requires_restart_allows_cluster_node_addition() {
        let old = base_config();
        let new = Config::parse(&BASE_TOML.replace(
            r#"nodes = ["127.0.0.1:5432"]"#,
            r#"nodes = ["127.0.0.1:5432", "127.0.0.1:5433"]"#,
        ))
        .unwrap();
        assert_eq!(requires_restart(&old, &new), None);
    }

    #[test]
    fn requires_restart_allows_max_connections_change() {
        let old = base_config();
        let new = Config::parse(
            r#"
            [server]
            listen = ["0.0.0.0:6432"]

            [logging]
            level = "info"

            [cluster.main]
            nodes = ["127.0.0.1:5432"]

            [cluster.main.pool.mydb]
            max_connections = { primary = 50 }

            [cluster.main.pool.mydb.user.myuser]
            "#,
        )
        .unwrap();
        assert_eq!(requires_restart(&old, &new), None);
    }

    #[test]
    fn requires_restart_allows_checkout_timeout_change() {
        let old = base_config();
        let new = Config::parse(
            r#"
            [server]
            listen = ["0.0.0.0:6432"]
            checkout_timeout = "5s"

            [logging]
            level = "info"

            [cluster.main]
            nodes = ["127.0.0.1:5432"]

            [cluster.main.pool.mydb.user.myuser]
            "#,
        )
        .unwrap();
        assert_eq!(requires_restart(&old, &new), None);
    }

    #[test]
    fn requires_restart_allows_shutdown_timeout_change() {
        let old = base_config();
        let new = Config::parse(
            r#"
            [server]
            listen = ["0.0.0.0:6432"]
            shutdown_timeout = "60s"

            [logging]
            level = "info"

            [cluster.main]
            nodes = ["127.0.0.1:5432"]

            [cluster.main.pool.mydb.user.myuser]
            "#,
        )
        .unwrap();
        assert_eq!(requires_restart(&old, &new), None);
    }

    #[test]
    fn requires_restart_allows_adding_new_cluster() {
        let old = base_config();
        let new = Config::parse(
            r#"
            [server]
            listen = ["0.0.0.0:6432"]

            [logging]
            level = "info"

            [cluster.main]
            nodes = ["127.0.0.1:5432"]

            [cluster.main.pool.mydb.user.myuser]

            [cluster.warehouse]
            nodes = ["127.0.0.1:5433"]

            [cluster.warehouse.pool.analytics.user.analyst]
            "#,
        )
        .unwrap();
        assert_eq!(requires_restart(&old, &new), None);
    }

    // ---- reload_config ---------------------------------------------------

    /// Write a TOML config to a temp file and return the path. The
    /// `tempfile::NamedTempFile` handle must be kept alive by the test
    /// so the file isn't removed out from under `reload_config`.
    fn write_temp_config(toml: &str) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut f = tempfile::Builder::new()
            .prefix("halephant-reload-")
            .suffix(".toml")
            .tempfile()
            .unwrap();
        f.write_all(toml.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[tokio::test]
    async fn reload_config_success_swaps_arc() {
        let initial = base_config();
        let shared = Arc::new(ArcSwap::from_pointee(initial));

        // Write a hot-reloadable new config (raised max_connections).
        let new_toml = r#"
            [server]
            listen = ["0.0.0.0:6432"]

            [logging]
            level = "info"

            [cluster.main]
            nodes = ["127.0.0.1:5432"]

            [cluster.main.pool.mydb]
            max_connections = { primary = 77 }

            [cluster.main.pool.mydb.user.myuser]
        "#;
        let file = write_temp_config(new_toml);

        let outcome = reload_config(file.path(), &shared).await;
        assert!(matches!(outcome, ReloadOutcome::Success), "{outcome:?}");

        // The swap is visible on the next `load`.
        let after = shared.load_full();
        let (_, _, pool) = after.find_pool("mydb").expect("pool still exists");
        assert_eq!(pool.max_connections.primary, 77);
    }

    #[tokio::test]
    async fn reload_config_restart_required_does_not_swap() {
        let initial = base_config();
        let shared = Arc::new(ArcSwap::from_pointee(initial));

        // Change `server.listen` — classified as restart-required.
        let new_toml = r#"
            [server]
            listen = ["0.0.0.0:7000"]

            [logging]
            level = "info"

            [cluster.main]
            nodes = ["127.0.0.1:5432"]

            [cluster.main.pool.mydb.user.myuser]
        "#;
        let file = write_temp_config(new_toml);

        let outcome = reload_config(file.path(), &shared).await;
        assert!(
            matches!(outcome, ReloadOutcome::RestartRequired("server.listen")),
            "{outcome:?}"
        );

        // Old config is still in place — `server.listen` unchanged.
        let after = shared.load_full();
        assert_eq!(after.server.listen, vec!["0.0.0.0:6432".to_owned()]);
    }

    #[tokio::test]
    async fn reload_config_parse_error_does_not_swap() {
        let initial = base_config();
        let shared = Arc::new(ArcSwap::from_pointee(initial));

        // Not valid TOML.
        let file = write_temp_config("this is [not valid toml\n");

        let outcome = reload_config(file.path(), &shared).await;
        assert!(
            matches!(outcome, ReloadOutcome::ParseFailed(_)),
            "{outcome:?}"
        );

        // Old config is still in place.
        let after = shared.load_full();
        assert!(after.find_pool("mydb").is_some());
    }

    #[tokio::test]
    async fn reload_config_validation_error_does_not_swap() {
        let initial = base_config();
        let shared = Arc::new(ArcSwap::from_pointee(initial));

        // Parses as TOML but fails `Config::validate()` — `server.listen`
        // cannot be empty.
        let new_toml = r#"
            [server]
            listen = []

            [cluster.main]
            nodes = ["127.0.0.1:5432"]

            [cluster.main.pool.mydb.user.myuser]
        "#;
        let file = write_temp_config(new_toml);

        let outcome = reload_config(file.path(), &shared).await;
        assert!(
            matches!(outcome, ReloadOutcome::ParseFailed(_)),
            "{outcome:?}"
        );

        // Old config is still in place.
        let after = shared.load_full();
        assert_eq!(after.server.listen, vec!["0.0.0.0:6432".to_owned()]);
    }

    #[tokio::test]
    async fn reload_config_missing_file_returns_parse_failed() {
        let initial = base_config();
        let shared = Arc::new(ArcSwap::from_pointee(initial));

        let missing = Path::new("/nonexistent/halephant-reload.toml");
        let outcome = reload_config(missing, &shared).await;
        assert!(
            matches!(outcome, ReloadOutcome::ParseFailed(_)),
            "{outcome:?}"
        );
    }
}
