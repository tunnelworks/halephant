pub mod admin;
pub mod cluster;
pub mod logging;
pub mod otel;
pub mod reload;
pub mod server;

use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::Deserialize;

use crate::errors;

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: server::ServerConfig,

    #[serde(default)]
    pub logging: logging::LoggingConfig,

    #[serde(default)]
    pub admin: admin::AdminConfig,

    #[serde(default)]
    pub otel: otel::OtelConfig,

    #[serde(default)]
    pub cluster: HashMap<String, cluster::ClusterConfig>,
}

impl Config {
    /// Load and validate a config file.
    pub fn load(path: &Path) -> Result<Self, errors::ConfigError> {
        let content = std::fs::read_to_string(path)?;
        Self::parse(&content)
    }

    /// Parse and validate a config from a TOML string.
    pub fn parse(s: &str) -> Result<Self, errors::ConfigError> {
        let config: Config = toml::from_str(s)?;
        config.validate()?;
        Ok(config)
    }

    /// Find the cluster name, cluster config, and pool config for a database.
    pub fn find_pool(
        &self,
        database: &str,
    ) -> Option<(&str, &cluster::ClusterConfig, &cluster::pool::PoolConfig)> {
        for (name, cluster) in &self.cluster {
            if let Some(pool) = cluster.pool.get(database) {
                return Some((name, cluster, pool));
            }
        }
        None
    }

    /// Check whether a user is allowed to connect to the given database.
    pub fn is_user_allowed(&self, database: &str, user: &str) -> bool {
        self.find_user(database, user).is_some()
    }

    /// Find the user config for a (database, user) pair.
    pub fn find_user(
        &self,
        database: &str,
        user: &str,
    ) -> Option<&cluster::pool::user::UserConfig> {
        let (_, _, pool) = self.find_pool(database)?;
        pool.user.get(user)
    }

    fn validate(&self) -> Result<(), errors::ConfigError> {
        self.server.validate()?;

        // Database names must be unique across clusters — the PostgreSQL
        // client protocol routes by database name alone.
        let mut seen_databases = HashSet::new();
        for cluster in self.cluster.values() {
            for db_name in cluster.pool.keys() {
                if !seen_databases.insert(db_name) {
                    return Err(errors::ConfigError::Validation(format!(
                        "database {db_name:?} appears in multiple clusters"
                    )));
                }
            }
        }

        for (cluster_name, cluster) in &self.cluster {
            cluster.validate(cluster_name)?;
        }

        Ok(())
    }
}
