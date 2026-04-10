use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct OtelConfig {
    /// OTLP gRPC collector endpoint. When absent, tracing export is disabled.
    pub endpoint: Option<String>,

    /// Service name reported in traces.
    #[serde(default = "default_otel_service_name")]
    pub service_name: String,

    /// Controls how SQL query text is recorded on trace spans.
    #[serde(default)]
    pub query_text: QueryText,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            service_name: default_otel_service_name(),
            query_text: QueryText::default(),
        }
    }
}

/// Controls how SQL query text appears in trace spans.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryText {
    /// Do not record `db.query.text`. Only `db.query.summary` is set.
    #[default]
    Off,
    /// Record `db.query.text` with literals replaced by `?`. Extended protocol
    /// queries (already parameterized) are recorded as-is.
    Sanitized,
    /// Record the full SQL verbatim, including literal values.
    Raw,
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

fn default_otel_service_name() -> String {
    "halephant".into()
}
