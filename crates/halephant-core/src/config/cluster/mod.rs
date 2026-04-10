pub mod auth;
pub mod pool;
pub mod topology;

use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;

use crate::errors;

#[derive(Debug, Deserialize)]
pub struct ClusterConfig {
    pub nodes: Vec<String>,

    #[serde(default = "default_admin_user")]
    pub admin_user: String,

    /// Database the admin connection selects in its startup message.
    /// Defaults to `"postgres"`, which is cluster-global and carries
    /// `pg_shadow` — enough for the default `auth.query`. Override
    /// when `auth.query` references database-local objects such as a
    /// custom role-mapping table that lives in a specific application
    /// database.
    #[serde(default = "default_admin_database")]
    pub admin_database: String,

    #[serde(default = "default_connect_timeout", with = "humantime_serde")]
    pub connect_timeout: Duration,

    #[serde(default)]
    pub auth: auth::AuthConfig,

    #[serde(default)]
    pub topology: topology::TopologyConfig,

    #[serde(default)]
    pub pool: HashMap<String, pool::PoolConfig>,
}

impl ClusterConfig {
    pub(crate) fn validate(&self, name: &str) -> Result<(), errors::ConfigError> {
        if self.nodes.is_empty() {
            return Err(errors::ConfigError::Validation(format!(
                "cluster {name:?} has no nodes"
            )));
        }

        for (db_name, pool) in &self.pool {
            pool.validate(db_name)?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

fn default_admin_user() -> String {
    "halephant".into()
}

fn default_admin_database() -> String {
    "postgres".into()
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(5)
}
