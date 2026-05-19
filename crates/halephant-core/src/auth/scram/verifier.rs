//! SCRAM verifier — the server-side credential format PostgreSQL stores in
//! `pg_authid.rolpassword`. Halephant consumes these via the auth query
//! (see [`crate::auth::query`]) and uses them to authenticate clients without
//! ever holding a plaintext password.
//!
//! The credential payload itself is just `tinyscram::Credential`. The PG-format
//! verifier-string parser lives here because the
//! `SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey>` encoding is specific to
//! PostgreSQL's `pg_authid.rolpassword` column — `tinyscram` doesn't know about
//! it and shouldn't.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;

use crate::errors;

/// Halephant's credential carrier is identical in shape to `tinyscram::Credential`
/// (`iterations`, `salt`, `stored_key`, `server_key`); the type alias avoids
/// a wrapping struct that would only re-export the same four fields.
pub type ScramVerifier = tinyscram::Credential;

/// Parse a PostgreSQL-format SCRAM verifier string:
/// `SCRAM-SHA-256$<iterations>:<salt_b64>$<StoredKey_b64>:<ServerKey_b64>`
pub fn parse_verifier(s: &str) -> Result<ScramVerifier, errors::AuthError> {
    let s = s.strip_prefix("SCRAM-SHA-256$").ok_or_else(|| {
        errors::AuthError::Protocol("verifier missing SCRAM-SHA-256 prefix".into())
    })?;

    let (iter_salt, keys) = s
        .split_once('$')
        .ok_or_else(|| errors::AuthError::Protocol("malformed verifier".into()))?;

    let (iter_str, salt_b64) = iter_salt
        .split_once(':')
        .ok_or_else(|| errors::AuthError::Protocol("malformed verifier iterations:salt".into()))?;

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

    Ok(ScramVerifier {
        iterations,
        salt,
        stored_key,
        server_key,
    })
}
