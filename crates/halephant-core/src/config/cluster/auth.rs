use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AuthConfig {
    #[serde(default = "default_auth_query")]
    pub query: String,

    #[serde(default = "default_auth_cache_ttl", with = "humantime_serde")]
    pub cache_ttl: Duration,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            query: default_auth_query(),
            cache_ttl: default_auth_cache_ttl(),
        }
    }
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

fn default_auth_query() -> String {
    "SELECT usename, passwd FROM pg_shadow WHERE usename = $1".into()
}

fn default_auth_cache_ttl() -> Duration {
    Duration::from_mins(5)
}
