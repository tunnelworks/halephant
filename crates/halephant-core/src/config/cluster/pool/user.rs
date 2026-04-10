use std::collections::HashMap;

use serde::Deserialize;

use crate::config::cluster::pool::ConnectionLimits;

#[derive(Debug, Deserialize)]
pub struct UserConfig {
    /// This user is an alias for another PostgreSQL role. When set, halephant
    /// authenticates using the aliased role's credentials and connects upstream
    /// as that role.
    pub alias: Option<String>,

    /// Maximum connections for this user, split by primary (rw) and replica (ro).
    pub max_connections: Option<ConnectionLimits>,

    /// Minimum idle connections to maintain for this user at startup.
    #[serde(default)]
    pub min_connections: ConnectionLimits,

    /// PostgreSQL connection parameters sent in the StartupMessage.
    #[serde(default)]
    pub parameters: UserParameters,
}

impl UserConfig {
    /// The effective upstream user — the `alias` target if set, otherwise the
    /// TOML key (the client-facing username) is used by the caller.
    pub fn upstream_name<'a>(&'a self, key: &'a str) -> &'a str {
        self.alias.as_deref().unwrap_or(key)
    }

    /// Whether this user is read-only (no rw capacity).
    pub fn is_read_only(&self) -> bool {
        self.max_connections
            .as_ref()
            .is_some_and(|c| c.primary == 0 && c.replica > 0)
    }
}

/// PostgreSQL parameters sent in the StartupMessage.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct UserParameters {
    /// Application name visible in `pg_stat_activity`.
    pub application_name: Option<String>,

    /// GUC settings passed via the `options` connection parameter as `-c`
    /// key-value pairs (for example, `search_path`, `statement_timeout`).
    #[serde(default)]
    pub options: HashMap<String, String>,
}
