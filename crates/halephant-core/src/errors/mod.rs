#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),

    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("{0}")]
    Validation(String),
}

/// Error type for wire protocol parsing.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("unknown message type: {0:#04x}")]
    UnknownMessageType(u8),

    #[error("{message} (at byte {position})")]
    InvalidValue { position: usize, message: String },

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("authentication rejected for {user:?} on {database:?}")]
    Rejected { database: String, user: String },

    #[error("invalid credentials")]
    InvalidCredentials,

    #[error("failed to fetch verifier: {0}")]
    VerifierFetch(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("wire protocol error: {0}")]
    Wire(#[from] ProtocolError),
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("database {database:?} is not configured in any cluster")]
    UnknownDatabase { database: String },

    #[error("no primary discovered for cluster {cluster:?}")]
    NoPrimary { cluster: String },

    #[error("no healthy replica available for cluster {cluster:?}")]
    NoReplica { cluster: String },
}
