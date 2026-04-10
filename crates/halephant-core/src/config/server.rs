use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::errors;

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_listen")]
    pub listen: Vec<String>,

    /// Path to a `.pgpass` file for upstream password authentication. Defaults
    /// to `~/.pgpass` or the `PGPASSFILE` environment variable.
    pub pgpass: Option<PathBuf>,

    #[serde(default)]
    pub workers: usize,

    #[serde(default = "default_shutdown_timeout", with = "humantime_serde")]
    pub shutdown_timeout: Duration,

    /// Maximum time a client waits in the per-role wait queue when every
    /// candidate pool is at its `max_connections`. After this elapses,
    /// the checkout fails with a classified `checkout_timeout` error.
    #[serde(default = "default_checkout_timeout", with = "humantime_serde")]
    pub checkout_timeout: Duration,

    /// Maximum unique prepared statements tracked globally. Set to 0 for
    /// unlimited. Lower this in memory-constrained environments.
    #[serde(default = "default_max_prepared_statements")]
    pub max_prepared_statements: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            pgpass: None,
            workers: 0,
            shutdown_timeout: default_shutdown_timeout(),
            checkout_timeout: default_checkout_timeout(),
            max_prepared_statements: default_max_prepared_statements(),
        }
    }
}

impl ServerConfig {
    pub(crate) fn validate(&self) -> Result<(), errors::ConfigError> {
        if self.listen.is_empty() {
            return Err(errors::ConfigError::Validation(
                "server.listen must have at least one address".into(),
            ));
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

fn default_listen() -> Vec<String> {
    vec!["0.0.0.0:6432".into()]
}

fn default_shutdown_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_checkout_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_max_prepared_statements() -> u32 {
    0
}
