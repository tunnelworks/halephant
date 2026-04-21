#![allow(clippy::unwrap_used)]
use halephant_core::config::Config;
use halephant_core::config::cluster;
use halephant_core::config::logging;
use halephant_core::config::otel;
use halephant_core::errors::ConfigError;
use std::time::Duration;

fn minimal_config() -> &'static str {
    r#"
        [cluster.main]
        nodes = ["127.0.0.1:5432"]

        [cluster.main.pool.mydb]

        [cluster.main.pool.mydb.user.alice]

        [cluster.main.pool.mydb.user.bob]
    "#
}

#[test]
fn parse_minimal() {
    let config = Config::parse(minimal_config()).unwrap();
    let cluster = config.cluster.get("main").unwrap();
    assert_eq!(cluster.nodes, vec!["127.0.0.1:5432"]);
    let pool = cluster.pool.get("mydb").unwrap();
    assert_eq!(pool.user.len(), 2);
    assert!(pool.user.contains_key("alice"));
    assert!(pool.user.contains_key("bob"));
}

#[test]
fn defaults_applied() {
    let config = Config::parse(minimal_config()).unwrap();
    assert_eq!(config.server.listen, vec!["0.0.0.0:6432"]);
    assert_eq!(config.server.shutdown_timeout, Duration::from_secs(30));
    assert_eq!(config.server.max_prepared_statements, 0);
    assert_eq!(config.logging.format, logging::LogFormat::Json);
    assert_eq!(config.logging.level, "info");

    let cluster = config.cluster.get("main").unwrap();
    assert_eq!(cluster.admin_user, "halephant");
    assert_eq!(cluster.connect_timeout, Duration::from_secs(5));
    assert_eq!(cluster.auth.cache_ttl, Duration::from_mins(5));
    assert_eq!(cluster.topology.interval, Duration::from_secs(5));
    assert_eq!(cluster.topology.timeout, Duration::from_secs(3));

    let pool = cluster.pool.get("mydb").unwrap();
    assert_eq!(pool.mode, cluster::pool::PoolMode::Transaction);
    assert_eq!(pool.max_connections.primary, 100);
    assert_eq!(pool.idle_timeout, Duration::from_mins(5));

    let alice = pool.user.get("alice").unwrap();
    assert_eq!(
        alice.min_connections,
        cluster::pool::ConnectionLimits {
            primary: 0,
            replica: 0
        }
    );
    assert!(alice.parameters.application_name.is_none());
    assert!(alice.parameters.options.is_empty());
}

#[test]
fn parse_full() {
    let config = Config::parse(
        r#"
        [server]
        listen = ["0.0.0.0:5555", "[::]:5555"]
        workers = 4
        shutdown_timeout = "1m"

        [logging]
        format = "text"
        level = "debug"

        [cluster.orders]
        nodes = ["pg-orders:5432", "pg-orders-r1:5432", "pg-orders-r2:5432"]
        admin_user = "admin"
        connect_timeout = "10s"

        [cluster.orders.auth]
        query = "SELECT u, p FROM users WHERE u = $1"
        cache_ttl = "10m"

        [cluster.orders.topology]
        interval = "10s"
        timeout = "2s"

        [cluster.orders.pool.orders_prod]
        mode = "transaction"
        max_connections = { primary = 200 }
        idle_timeout = "5m"
        max_lifetime = "1h"

        [cluster.orders.pool.orders_prod.user.app]
        min_connections = { primary = 10 }

        [cluster.orders.pool.orders_prod.user.app.parameters]
        application_name = "orders-api"
        options = { search_path = "orders,public" }

        [cluster.orders.pool.orders_prod.user.readonly.parameters]
        application_name = "orders-reports"
        options = { default_transaction_read_only = "on" }

        [cluster.analytics]
        nodes = ["pg-analytics:5432"]

        [cluster.analytics.pool.analytics_prod]
        mode = "session"

        [cluster.analytics.pool.analytics_prod.user.analyst.parameters]
        application_name = "analytics"
    "#,
    )
    .unwrap();

    assert_eq!(config.server.listen, vec!["0.0.0.0:5555", "[::]:5555"]);
    assert_eq!(config.server.workers, 4);
    assert_eq!(config.server.shutdown_timeout, Duration::from_mins(1));
    assert_eq!(config.logging.format, logging::LogFormat::Text);

    let orders = config.cluster.get("orders").unwrap();
    assert_eq!(orders.nodes.len(), 3);
    assert_eq!(orders.admin_user, "admin");
    assert_eq!(orders.connect_timeout, Duration::from_secs(10));
    assert_eq!(orders.auth.cache_ttl, Duration::from_mins(10));
    assert_eq!(orders.topology.interval, Duration::from_secs(10));

    let analytics = config.cluster.get("analytics").unwrap();
    assert_eq!(analytics.admin_user, "halephant"); // default

    let pool = orders.pool.get("orders_prod").unwrap();
    assert_eq!(pool.user.len(), 2);
    let app = pool.user.get("app").unwrap();
    assert_eq!(app.min_connections.primary, 10);
    assert_eq!(
        app.parameters.application_name.as_deref(),
        Some("orders-api")
    );
    assert_eq!(
        app.parameters.options.get("search_path"),
        Some(&"orders,public".to_owned())
    );
    assert!(pool.user.contains_key("readonly"));
    assert_eq!(pool.max_connections.primary, 200);
    assert_eq!(pool.mode, cluster::pool::PoolMode::Transaction);

    let analytics_pool = analytics.pool.get("analytics_prod").unwrap();
    assert_eq!(analytics_pool.mode, cluster::pool::PoolMode::Session);
    assert!(analytics_pool.user.contains_key("analyst"));
}

#[test]
fn listen_multiple_addresses() {
    let config = Config::parse(
        r#"
        [server]
        listen = ["127.0.0.1:6432", "[::1]:6432"]
    "#,
    )
    .unwrap();
    assert_eq!(config.server.listen.len(), 2);
}

#[test]
fn is_user_allowed() {
    let config = Config::parse(minimal_config()).unwrap();
    assert!(config.is_user_allowed("mydb", "alice"));
    assert!(config.is_user_allowed("mydb", "bob"));
    assert!(!config.is_user_allowed("mydb", "eve"));
    assert!(!config.is_user_allowed("other", "alice"));
}

#[test]
fn find_user() {
    let config = Config::parse(minimal_config()).unwrap();
    assert!(config.find_user("mydb", "alice").is_some());
    assert!(config.find_user("mydb", "eve").is_none());
}

#[test]
fn pool_mode_default() {
    let config = Config::parse(minimal_config()).unwrap();
    let cluster = config.cluster.get("main").unwrap();
    let pool = cluster.pool.get("mydb").unwrap();
    assert_eq!(pool.mode, cluster::pool::PoolMode::Transaction);
}

// ---------------------------------------------------------------------------
// Validation errors
// ---------------------------------------------------------------------------

#[test]
fn pool_no_users() {
    let err = Config::parse(
        r#"
        [cluster.main]
        nodes = ["a:5432"]

        [cluster.main.pool.mydb]
    "#,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::Validation(_)));
    assert!(err.to_string().contains("user"));
}

#[test]
fn min_connections_exceeds_max() {
    let err = Config::parse(
        r#"
        [cluster.main]
        nodes = ["a:5432"]

        [cluster.main.pool.mydb]
        max_connections = { primary = 5 }

        [cluster.main.pool.mydb.user.alice]
        min_connections = { primary = 3 }

        [cluster.main.pool.mydb.user.bob]
        min_connections = { primary = 3 }
    "#,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::Validation(_)));
    assert!(err.to_string().contains("min_connections"));
}

#[test]
fn duplicate_database_across_clusters() {
    let err = Config::parse(
        r#"
        [cluster.prod_a]
        nodes = ["a:5432"]

        [cluster.prod_a.pool.app_db.user.alice]

        [cluster.prod_b]
        nodes = ["b:5432"]

        [cluster.prod_b.pool.app_db.user.alice]
    "#,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::Validation(_)));
    assert!(err.to_string().contains("multiple clusters"));
}

#[test]
fn cluster_with_no_nodes() {
    let err = Config::parse(
        r"
        [cluster.main]
        nodes = []
    ",
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::Validation(_)));
    assert!(err.to_string().contains("no nodes"));
}

#[test]
fn empty_listen_rejected() {
    let err = Config::parse(
        r"
        [server]
        listen = []
    ",
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::Validation(_)));
    assert!(err.to_string().contains("listen"));
}

#[test]
fn listen_mode_rejected_in_session_mode() {
    let err = Config::parse(
        r#"
        [cluster.main]
        nodes = ["a:5432"]

        [cluster.main.pool.mydb]
        mode = "session"
        listen_mode = "pin"

        [cluster.main.pool.mydb.user.alice]
    "#,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::Validation(_)));
    assert!(err.to_string().contains("listen_mode"));
}

#[test]
fn listen_mode_rejected_multiplex_in_session_mode() {
    let err = Config::parse(
        r#"
        [cluster.main]
        nodes = ["a:5432"]

        [cluster.main.pool.mydb]
        mode = "session"
        listen_mode = "multiplex"

        [cluster.main.pool.mydb.user.alice]
    "#,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::Validation(_)));
    assert!(err.to_string().contains("listen_mode"));
}

#[test]
fn listen_mode_allowed_in_transaction_mode() {
    // Explicit listen_mode on a transaction-mode pool is valid.
    Config::parse(
        r#"
        [cluster.main]
        nodes = ["a:5432"]

        [cluster.main.pool.mydb]
        mode = "transaction"
        listen_mode = "multiplex"

        [cluster.main.pool.mydb.user.alice]
    "#,
    )
    .unwrap();
}

#[test]
fn session_mode_without_listen_mode_is_valid() {
    Config::parse(
        r#"
        [cluster.main]
        nodes = ["a:5432"]

        [cluster.main.pool.mydb]
        mode = "session"

        [cluster.main.pool.mydb.user.alice]
    "#,
    )
    .unwrap();
}

#[test]
fn transaction_mode_without_listen_mode_is_valid() {
    let config = Config::parse(
        r#"
        [cluster.main]
        nodes = ["a:5432"]

        [cluster.main.pool.mydb]
        mode = "transaction"

        [cluster.main.pool.mydb.user.alice]
    "#,
    )
    .unwrap();
    let pool = config
        .cluster
        .get("main")
        .unwrap()
        .pool
        .get("mydb")
        .unwrap();
    assert_eq!(pool.listen_mode, None);
}

#[test]
fn invalid_toml() {
    let err = Config::parse("this is not valid toml [[[").unwrap_err();
    assert!(matches!(err, ConfigError::Parse(_)));
}

#[test]
fn empty_config_is_valid() {
    let config = Config::parse("").unwrap();
    assert!(config.cluster.is_empty());
}

#[test]
fn database_name_override() {
    let config = Config::parse(
        r#"
        [cluster.main]
        nodes = ["a:5432"]

        [cluster.main.pool.my_alias]
        database = "actual_db"

        [cluster.main.pool.my_alias.user.app]
    "#,
    )
    .unwrap();
    let cluster = config.cluster.get("main").unwrap();
    let pool = cluster.pool.get("my_alias").unwrap();
    assert_eq!(pool.database_name("my_alias"), "actual_db");
}

#[test]
fn alias_and_read_only() {
    let config = Config::parse(
        r#"
        [cluster.main]
        nodes = ["a:5432"]

        [cluster.main.pool.mydb]

        [cluster.main.pool.mydb.user.app_ro]
        alias = "app"
        max_connections = { replica = 10 }
    "#,
    )
    .unwrap();
    let user = config.find_user("mydb", "app_ro").unwrap();
    assert_eq!(user.upstream_name("app_ro"), "app");
    assert!(user.is_read_only());
}

// ---------------------------------------------------------------------------
// OpenTelemetry config
// ---------------------------------------------------------------------------

#[test]
fn otel_defaults() {
    let config = Config::parse(minimal_config()).unwrap();
    assert!(config.otel.endpoint.is_none());
    assert_eq!(config.otel.service_name, "halephant");
    assert_eq!(config.otel.query_text, otel::QueryText::Off);
}

#[test]
fn otel_query_text_modes() {
    for (input, expected) in [
        ("off", otel::QueryText::Off),
        ("sanitized", otel::QueryText::Sanitized),
        ("raw", otel::QueryText::Raw),
    ] {
        let config = Config::parse(&format!(
            r#"
            [otel]
            query_text = "{input}"
        "#
        ))
        .unwrap();
        assert_eq!(config.otel.query_text, expected);
    }
}

#[test]
fn otel_with_endpoint() {
    let config = Config::parse(
        r#"
        [otel]
        endpoint = "http://localhost:4317"

        [cluster.main]
        nodes = ["a:5432"]

        [cluster.main.pool.mydb.user.alice]
    "#,
    )
    .unwrap();
    assert_eq!(
        config.otel.endpoint.as_deref(),
        Some("http://localhost:4317")
    );
    assert_eq!(config.otel.service_name, "halephant");
}

#[test]
fn otel_custom_service_name() {
    let config = Config::parse(
        r#"
        [otel]
        endpoint = "http://collector:4317"
        service_name = "halephant-prod"
    "#,
    )
    .unwrap();
    assert_eq!(
        config.otel.endpoint.as_deref(),
        Some("http://collector:4317")
    );
    assert_eq!(config.otel.service_name, "halephant-prod");
}

// ---------------------------------------------------------------------------
// Admin config
// ---------------------------------------------------------------------------

#[test]
fn admin_defaults() {
    let config = Config::parse(minimal_config()).unwrap();
    assert!(config.admin.listen.is_none());
}

#[test]
fn admin_with_listen() {
    let config = Config::parse(
        r#"
        [admin]
        listen = "0.0.0.0:6433"
    "#,
    )
    .unwrap();
    assert_eq!(config.admin.listen.as_deref(), Some("0.0.0.0:6433"));
}
