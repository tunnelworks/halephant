use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    #[serde(default)]
    pub format: LogFormat,

    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            format: LogFormat::default(),
            level: default_log_level(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Json,
    Text,
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

fn default_log_level() -> String {
    "info".into()
}
