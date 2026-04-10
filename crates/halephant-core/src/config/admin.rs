use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
pub struct AdminConfig {
    /// Admin HTTP API listen address. When absent, the admin API is disabled.
    pub listen: Option<String>,
}
