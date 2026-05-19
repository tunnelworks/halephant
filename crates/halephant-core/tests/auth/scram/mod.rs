#![allow(clippy::unwrap_used, clippy::panic)]

mod verifier;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;

use halephant_core::auth::scram::server::parse_sasl_initial_response;
use halephant_core::auth::scram::{ScramVerifier, parse_verifier};
use halephant_core::errors::AuthError;

/// Build a verifier from a plaintext password. Used both directly and by the
/// PG-format round-trip tests.
pub(crate) fn make_verifier(password: &str, salt: &[u8], iterations: u32) -> ScramVerifier {
    let keys = tinyscram::crypto::DerivedKeys::from_password(password, salt, iterations);
    ScramVerifier {
        iterations,
        salt: salt.to_vec(),
        stored_key: keys.stored_key,
        server_key: keys.server_key,
    }
}

fn format_pg_verifier(v: &ScramVerifier) -> String {
    format!(
        "SCRAM-SHA-256${}:{}${}:{}",
        v.iterations,
        B64.encode(&v.salt),
        B64.encode(v.stored_key),
        B64.encode(v.server_key),
    )
}

// ---------------------------------------------------------------------------
// PG-format verifier parsing — halephant-specific encoding from
// `pg_authid.rolpassword`. The pure SCRAM exchange is covered by `tinyscram`'s
// own test suite, so this file only keeps tests for code that lives in
// halephant.
// ---------------------------------------------------------------------------

#[test]
fn parse_valid_verifier() {
    let v = make_verifier("hello", b"salt", 4096);
    let parsed = parse_verifier(&format_pg_verifier(&v)).unwrap();
    assert_eq!(parsed.iterations, 4096);
    assert_eq!(parsed.salt, b"salt");
    assert_eq!(parsed.stored_key, v.stored_key);
    assert_eq!(parsed.server_key, v.server_key);
}

#[test]
fn parse_high_iterations() {
    let v = make_verifier("pw", b"s", 100_000);
    let parsed = parse_verifier(&format_pg_verifier(&v)).unwrap();
    assert_eq!(parsed.iterations, 100_000);
}

#[test]
fn parse_missing_prefix() {
    let err = parse_verifier("MD5abc123").unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

#[test]
fn parse_missing_dollar() {
    let err = parse_verifier("SCRAM-SHA-256$4096:c2FsdA==").unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

#[test]
fn parse_bad_iterations() {
    let err = parse_verifier("SCRAM-SHA-256$notanumber:c2FsdA==$aa:bb").unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

#[test]
fn parse_bad_base64_salt() {
    let err = parse_verifier("SCRAM-SHA-256$4096:!!!$aa:bb").unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

#[test]
fn parse_bad_base64_stored_key() {
    let v = make_verifier("pw", b"s", 1);
    let bad = format!(
        "SCRAM-SHA-256$1:{}$!!!:{}",
        B64.encode(&v.salt),
        B64.encode(v.server_key),
    );
    let err = parse_verifier(&bad).unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

#[test]
fn parse_wrong_key_length() {
    let short = B64.encode([0u8; 16]);
    let full = B64.encode([0u8; 32]);
    let bad = format!("SCRAM-SHA-256$4096:c2FsdA==${short}:{full}");
    let err = parse_verifier(&bad).unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

// ---------------------------------------------------------------------------
// SASLInitialResponse framing — PG wraps the SCRAM `client-first-message`
// bytes in `<mechanism>\0<i32 length><client-first>`. The parser is the only
// halephant-side code that touches this layout.
// ---------------------------------------------------------------------------

#[test]
fn sasl_initial_valid() {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"SCRAM-SHA-256\0");
    let data = b"n,,n=alice,r=nonce";
    buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
    buf.extend_from_slice(data);

    let (mech, resp) = parse_sasl_initial_response(&buf).unwrap();
    assert_eq!(mech, "SCRAM-SHA-256");
    assert_eq!(resp, data);
}

#[test]
fn sasl_initial_no_response_data() {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"SCRAM-SHA-256\0");
    buf.extend_from_slice(&(-1i32).to_be_bytes());

    let (mech, resp) = parse_sasl_initial_response(&buf).unwrap();
    assert_eq!(mech, "SCRAM-SHA-256");
    assert!(resp.is_empty());
}

#[test]
fn sasl_initial_missing_null() {
    let err = parse_sasl_initial_response(b"no-null-terminator").unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

#[test]
fn sasl_initial_truncated_length() {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"SCRAM-SHA-256\0");
    buf.push(0); // only 1 byte, need 4
    let err = parse_sasl_initial_response(&buf).unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

#[test]
fn sasl_initial_truncated_data() {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"SCRAM-SHA-256\0");
    buf.extend_from_slice(&100i32.to_be_bytes()); // claims 100 bytes
    buf.extend_from_slice(b"short"); // only 5 bytes
    let err = parse_sasl_initial_response(&buf).unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}
