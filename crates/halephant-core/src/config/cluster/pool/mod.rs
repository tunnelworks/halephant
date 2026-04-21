pub mod user;

use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;

use crate::errors;

#[derive(Debug, Deserialize)]
pub struct PoolConfig {
    /// Override the PostgreSQL database name when it differs from the TOML key.
    pub database: Option<String>,

    #[serde(default)]
    pub mode: PoolMode,

    /// LISTEN/NOTIFY handling in transaction mode. Not applicable to session mode.
    pub listen_mode: Option<ListenMode>,

    #[serde(default = "default_pool_max_connections")]
    pub max_connections: ConnectionLimits,

    #[serde(default = "default_idle_timeout", with = "humantime_serde")]
    pub idle_timeout: Duration,

    #[serde(default = "default_max_lifetime", with = "humantime_serde")]
    pub max_lifetime: Duration,

    /// Per-pool override of `[server] checkout_timeout`. When set, the
    /// pool uses this value instead of the server-wide default. Useful
    /// for mixing latency-sensitive OLTP pools (short timeout) with
    /// analytics pools (long or no timeout) in the same halephant
    /// process.
    #[serde(default, with = "humantime_serde")]
    pub checkout_timeout: Option<Duration>,

    #[serde(default)]
    pub user: HashMap<String, user::UserConfig>,
}

impl PoolConfig {
    /// The effective database name — the explicit `database` field if set,
    /// otherwise the TOML key is used by the caller.
    pub fn database_name<'a>(&'a self, key: &'a str) -> &'a str {
        self.database.as_deref().unwrap_or(key)
    }

    pub(crate) fn validate(&self, name: &str) -> Result<(), errors::ConfigError> {
        if self.user.is_empty() {
            return Err(errors::ConfigError::Validation(format!(
                "pool {name:?}: at least one user is required"
            )));
        }

        if self.mode == PoolMode::Session && self.listen_mode.is_some() {
            return Err(errors::ConfigError::Validation(format!(
                "pool {name:?}: listen_mode is only supported in transaction mode"
            )));
        }

        let total_min_primary: u32 = self.user.values().map(|u| u.min_connections.primary).sum();
        if total_min_primary > self.max_connections.primary {
            return Err(errors::ConfigError::Validation(format!(
                "pool {:?}: total min_connections.primary ({total_min_primary}) exceeds max_connections.primary ({})",
                name, self.max_connections.primary
            )));
        }
        let total_min_replica: u32 = self.user.values().map(|u| u.min_connections.replica).sum();
        if total_min_replica > self.max_connections.replica {
            return Err(errors::ConfigError::Validation(format!(
                "pool {:?}: total min_connections.replica ({total_min_replica}) exceeds max_connections.replica ({})",
                name, self.max_connections.replica
            )));
        }

        Ok(())
    }
}

/// Connection limits split by primary (rw) and replica (ro).
#[derive(Debug, Default, PartialEq, Eq, Deserialize)]
pub struct ConnectionLimits {
    #[serde(default)]
    pub primary: u32,
    #[serde(default)]
    pub replica: u32,
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PoolMode {
    #[default]
    Transaction,
    Session,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListenMode {
    /// Pin the connection on LISTEN (switches to session mode for that client).
    #[default]
    Pin,
    /// Multiplex LISTEN across a shared connection with fan-out.
    Multiplex,
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

fn default_pool_max_connections() -> ConnectionLimits {
    ConnectionLimits {
        primary: 100,
        replica: 0,
    }
}

fn default_idle_timeout() -> Duration {
    Duration::from_mins(5)
}

fn default_max_lifetime() -> Duration {
    Duration::from_hours(1)
}
