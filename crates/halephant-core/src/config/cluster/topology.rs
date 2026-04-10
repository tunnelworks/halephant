use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TopologyConfig {
    #[serde(default = "default_interval", with = "humantime_serde")]
    pub interval: Duration,

    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Duration,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            interval: default_interval(),
            timeout: default_timeout(),
        }
    }
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

fn default_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_timeout() -> Duration {
    Duration::from_secs(3)
}
