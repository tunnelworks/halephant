use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;

use halephant_core::auth::scram::parse_verifier;

/// Build a verifier, format it as PostgreSQL would, then parse it back and
/// confirm every field round-trips. Halephant-specific: the SCRAM exchange
/// itself is covered by `tinyscram`'s test suite, but the PG `pg_authid`
/// encoding lives here.
#[test]
fn verifier_round_trip() {
    let v = super::make_verifier("testpass", b"somesalt", 4096);
    let pg_format = format!(
        "SCRAM-SHA-256${}:{}${}:{}",
        v.iterations,
        B64.encode(&v.salt),
        B64.encode(v.stored_key),
        B64.encode(v.server_key),
    );

    let parsed = parse_verifier(&pg_format).unwrap();
    assert_eq!(parsed.iterations, 4096);
    assert_eq!(parsed.salt, b"somesalt");
    assert_eq!(parsed.stored_key, v.stored_key);
    assert_eq!(parsed.server_key, v.server_key);
}
