//! SCRAM verifier — the server-side credential format PostgreSQL
//! stores in `pg_authid.rolpassword`. Halephant consumes these via
//! the auth query (see [`crate::auth::query`]) and uses them to
//! authenticate clients without ever holding a plaintext password.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;

use crate::errors;

/// A parsed SCRAM-SHA-256 verifier containing the precomputed keys
/// needed to authenticate a client without knowing the plaintext
/// password.
///
/// This type is used by both the server state machine ([`super::server::ScramServer`])
/// and the verifier cache ([`crate::auth::query::VerifierCache`]) —
/// re-exported from [`super`] as `scram::ScramVerifier` because it's
/// the shared data type that glues the server-side auth path together.
#[derive(Debug, Clone)]
pub struct ScramVerifier {
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

impl ScramVerifier {
    /// Parse a PostgreSQL-format SCRAM verifier string:
    /// `SCRAM-SHA-256$<iterations>:<salt_b64>$<StoredKey_b64>:<ServerKey_b64>`
    pub fn parse(s: &str) -> Result<Self, errors::AuthError> {
        let s = s.strip_prefix("SCRAM-SHA-256$").ok_or_else(|| {
            errors::AuthError::Protocol("verifier missing SCRAM-SHA-256 prefix".into())
        })?;

        let (iter_salt, keys) = s
            .split_once('$')
            .ok_or_else(|| errors::AuthError::Protocol("malformed verifier".into()))?;

        let (iter_str, salt_b64) = iter_salt.split_once(':').ok_or_else(|| {
            errors::AuthError::Protocol("malformed verifier iterations:salt".into())
        })?;

        let (stored_key_b64, server_key_b64) = keys.split_once(':').ok_or_else(|| {
            errors::AuthError::Protocol("malformed verifier StoredKey:ServerKey".into())
        })?;

        let iterations: u32 = iter_str
            .parse()
            .map_err(|_| errors::AuthError::Protocol("invalid iteration count".into()))?;
        let salt = B64
            .decode(salt_b64)
            .map_err(|_| errors::AuthError::Protocol("invalid base64 in salt".into()))?;
        let stored_key: [u8; 32] = B64
            .decode(stored_key_b64)
            .map_err(|_| errors::AuthError::Protocol("invalid base64 in StoredKey".into()))?
            .try_into()
            .map_err(|_| errors::AuthError::Protocol("StoredKey is not 32 bytes".into()))?;
        let server_key: [u8; 32] = B64
            .decode(server_key_b64)
            .map_err(|_| errors::AuthError::Protocol("invalid base64 in ServerKey".into()))?
            .try_into()
            .map_err(|_| errors::AuthError::Protocol("ServerKey is not 32 bytes".into()))?;

        Ok(Self {
            iterations,
            salt,
            stored_key,
            server_key,
        })
    }
}
