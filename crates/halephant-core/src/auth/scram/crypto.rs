//! Shared SCRAM primitives used by both the client-side and
//! server-side state machines. These are kept in a dedicated module
//! so that `client.rs` and `server.rs` can focus on their respective
//! wire drivers without pulling each other in through sibling imports.
//!
//! `extract_attr` sits here even though it's a SCRAM message parser
//! rather than a crypto primitive — it's a tiny helper that both
//! sides of the exchange need, and a separate file for three lines
//! would fragment the module more than it helps.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::errors;

type HmacSha256 = Hmac<Sha256>;

/// Compute `HMAC-SHA256(key, data)` and return the 32-byte tag.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// Compute `SHA-256(data)` and return the 32-byte digest.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

/// Extract a named attribute from a comma-separated SCRAM message.
/// For example, `extract_attr("n=user,r=abc", 'r')` returns `Ok("abc")`.
pub fn extract_attr(msg: &str, attr: char) -> Result<&str, errors::AuthError> {
    let prefix = format!("{attr}=");
    for part in msg.split(',') {
        if let Some(value) = part.strip_prefix(&prefix) {
            return Ok(value);
        }
    }
    Err(errors::AuthError::Protocol(format!(
        "missing attribute '{attr}' in SCRAM message"
    )))
}
