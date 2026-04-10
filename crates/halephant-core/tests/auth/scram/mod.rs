#![allow(clippy::unwrap_used, clippy::panic)]

mod client;
mod server;
mod verifier;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use sha2::Sha256;

use halephant_core::auth::scram::ScramVerifier;
use halephant_core::auth::scram::client::ScramClient;
use halephant_core::auth::scram::crypto;
use halephant_core::auth::scram::server::{ScramServer, parse_sasl_initial_response};
use halephant_core::errors::AuthError;

// ---------------------------------------------------------------------------
// Helpers — client-side SCRAM for testing
// ---------------------------------------------------------------------------

/// Build a verifier from a plaintext password (requires PBKDF2).
fn make_verifier(password: &str, salt: &[u8], iterations: u32) -> ScramVerifier {
    use pbkdf2::pbkdf2_hmac;

    let mut salted_password = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted_password);

    let client_key = crypto::hmac_sha256(&salted_password, b"Client Key");
    let stored_key = crypto::sha256(&client_key);
    let server_key = crypto::hmac_sha256(&salted_password, b"Server Key");

    ScramVerifier {
        iterations,
        salt: salt.to_vec(),
        stored_key,
        server_key,
    }
}

/// Format a verifier in PostgreSQL's pg_authid format.
fn format_pg_verifier(v: &ScramVerifier) -> String {
    format!(
        "SCRAM-SHA-256${}:{}${}:{}",
        v.iterations,
        B64.encode(&v.salt),
        B64.encode(v.stored_key),
        B64.encode(v.server_key),
    )
}

/// Simulate a client-side SCRAM response (client-final-message) given the
/// password, the client-first-bare, and the server-first-message.
fn client_final(
    password: &str,
    salt: &[u8],
    iterations: u32,
    client_first_bare: &str,
    server_first: &str,
    combined_nonce: &str,
) -> String {
    use pbkdf2::pbkdf2_hmac;

    let mut salted = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted);

    let client_key = crypto::hmac_sha256(&salted, b"Client Key");
    let stored_key = crypto::sha256(&client_key);

    let without_proof = format!("c=biws,r={combined_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");

    let client_signature = crypto::hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_signature[i];
    }

    format!("{without_proof},p={}", B64.encode(proof))
}

fn extract_attr(msg: &str, attr: char) -> &str {
    let prefix = format!("{attr}=");
    msg.split(',')
        .find_map(|p| p.strip_prefix(&prefix))
        .unwrap()
}

// ---------------------------------------------------------------------------
// ScramVerifier::parse
// ---------------------------------------------------------------------------

#[test]
fn parse_valid_verifier() {
    let v = make_verifier("hello", b"salt", 4096);
    let pg = format_pg_verifier(&v);
    let parsed = ScramVerifier::parse(&pg).unwrap();
    assert_eq!(parsed.iterations, 4096);
    assert_eq!(parsed.salt, b"salt");
    assert_eq!(parsed.stored_key, v.stored_key);
    assert_eq!(parsed.server_key, v.server_key);
}

#[test]
fn parse_high_iterations() {
    let v = make_verifier("pw", b"s", 100_000);
    let parsed = ScramVerifier::parse(&format_pg_verifier(&v)).unwrap();
    assert_eq!(parsed.iterations, 100_000);
}

#[test]
fn parse_missing_prefix() {
    let err = ScramVerifier::parse("MD5abc123").unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

#[test]
fn parse_missing_dollar() {
    let err = ScramVerifier::parse("SCRAM-SHA-256$4096:c2FsdA==").unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

#[test]
fn parse_bad_iterations() {
    let err = ScramVerifier::parse("SCRAM-SHA-256$notanumber:c2FsdA==$aa:bb").unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

#[test]
fn parse_bad_base64_salt() {
    let err = ScramVerifier::parse("SCRAM-SHA-256$4096:!!!$aa:bb").unwrap_err();
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
    let err = ScramVerifier::parse(&bad).unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

#[test]
fn parse_wrong_key_length() {
    // StoredKey is only 16 bytes instead of 32
    let short = B64.encode([0u8; 16]);
    let full = B64.encode([0u8; 32]);
    let bad = format!("SCRAM-SHA-256$4096:c2FsdA==${short}:{full}");
    let err = ScramVerifier::parse(&bad).unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

// ---------------------------------------------------------------------------
// Full SCRAM exchange
// ---------------------------------------------------------------------------

#[test]
fn exchange_correct_password() {
    let password = "secret123";
    let salt = b"test_salt";
    let iterations = 4096;
    let verifier = make_verifier(password, salt, iterations);
    let mut server = ScramServer::new(verifier);

    let client_nonce = "client_nonce_abc";
    let client_first = format!("n,,n=alice,r={client_nonce}");

    let server_first_bytes = server.handle_client_first(client_first.as_bytes()).unwrap();
    let server_first = std::str::from_utf8(&server_first_bytes).unwrap();

    let combined_nonce = extract_attr(server_first, 'r');
    assert!(combined_nonce.starts_with(client_nonce));
    assert!(combined_nonce.len() > client_nonce.len());

    let client_first_bare = format!("n=alice,r={client_nonce}");
    let cf = client_final(
        password,
        salt,
        iterations,
        &client_first_bare,
        server_first,
        combined_nonce,
    );

    let server_final_bytes = server.handle_client_final(cf.as_bytes()).unwrap();
    let server_final = std::str::from_utf8(&server_final_bytes).unwrap();
    assert!(server_final.starts_with("v="));
}

#[test]
fn exchange_wrong_password() {
    let verifier = make_verifier("correct", b"salt", 4096);
    let mut server = ScramServer::new(verifier);

    let client_first = b"n,,n=user,r=nonce123";
    let server_first_bytes = server.handle_client_first(client_first).unwrap();
    let server_first = std::str::from_utf8(&server_first_bytes).unwrap();
    let combined_nonce = extract_attr(server_first, 'r');

    let cf = client_final(
        "wrong",
        b"salt",
        4096,
        "n=user,r=nonce123",
        server_first,
        combined_nonce,
    );

    let err = server.handle_client_final(cf.as_bytes()).unwrap_err();
    assert!(matches!(err, AuthError::InvalidCredentials));
}

#[test]
fn exchange_empty_password() {
    let verifier = make_verifier("", b"salt", 4096);
    let mut server = ScramServer::new(verifier);

    let client_first = b"n,,n=user,r=abc";
    let sf_bytes = server.handle_client_first(client_first).unwrap();
    let sf = std::str::from_utf8(&sf_bytes).unwrap();
    let cn = extract_attr(sf, 'r');

    let cf = client_final("", b"salt", 4096, "n=user,r=abc", sf, cn);
    assert!(server.handle_client_final(cf.as_bytes()).is_ok());
}

#[test]
fn exchange_unicode_password() {
    let verifier = make_verifier("p@$$wörd🔒", b"salt", 4096);
    let mut server = ScramServer::new(verifier);

    let client_first = b"n,,n=user,r=xyz";
    let sf_bytes = server.handle_client_first(client_first).unwrap();
    let sf = std::str::from_utf8(&sf_bytes).unwrap();
    let cn = extract_attr(sf, 'r');

    let cf = client_final("p@$$wörd🔒", b"salt", 4096, "n=user,r=xyz", sf, cn);
    assert!(server.handle_client_final(cf.as_bytes()).is_ok());
}

#[test]
fn exchange_bad_client_first_no_gs2_header() {
    let verifier = make_verifier("pw", b"s", 1);
    let mut server = ScramServer::new(verifier);
    let err = server
        .handle_client_first(b"no_gs2_header_here")
        .unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

/// A client that sends `y,,` (channel binding supported but not
/// negotiated) must be accepted: this is the default libpq behaviour
/// over TLS with `channel_binding=prefer`, and rejecting it would
/// break every libpq client connecting to halephant over TLS.
#[test]
fn exchange_accepts_gs2_y_flag_for_libpq_compat() {
    let verifier = make_verifier("pw", b"s", 4096);
    let mut server = ScramServer::new(verifier);
    let result = server.handle_client_first(b"y,,n=user,r=abcdef");
    assert!(
        result.is_ok(),
        "y,, must be accepted for libpq compat: {result:?}"
    );
}

/// A client that sends `p=tls-server-end-point,,` is *requiring*
/// channel binding. Halephant does not implement channel binding,
/// so silently treating this as `n,,` would strip the client's
/// explicit security requirement — the client would believe the
/// authentication is bound to the TLS channel when it is not,
/// leaving a MITM-downgrade window. The exchange must fail loudly
/// with a protocol error instead.
#[test]
fn exchange_rejects_gs2_p_flag_channel_binding_required() {
    let verifier = make_verifier("pw", b"s", 4096);
    let mut server = ScramServer::new(verifier);
    let err = server
        .handle_client_first(b"p=tls-server-end-point,,n=user,r=abcdef")
        .unwrap_err();
    match err {
        AuthError::Protocol(msg) => assert!(
            msg.contains("channel binding"),
            "expected channel-binding rejection, got: {msg}"
        ),
        other => panic!("expected Protocol error, got: {other:?}"),
    }
}

/// `p=<anything>` with any cbind-type must be rejected — not just
/// the specific `tls-server-end-point` form libpq uses.
#[test]
fn exchange_rejects_gs2_p_flag_generic_cbind_type() {
    let verifier = make_verifier("pw", b"s", 4096);
    let mut server = ScramServer::new(verifier);
    let err = server
        .handle_client_first(b"p=tls-unique,,n=user,r=abcdef")
        .unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

/// A non-empty authzid (SASL identity switching) must be rejected
/// rather than silently ignored — otherwise a client asking to
/// authenticate as one identity while acting as another would be
/// authenticated without warning. Halephant does not support
/// authzid, so the only acceptable value is the empty string.
#[test]
fn exchange_rejects_non_empty_authzid() {
    let verifier = make_verifier("pw", b"s", 4096);
    let mut server = ScramServer::new(verifier);
    let err = server
        .handle_client_first(b"n,a=someoneelse,n=user,r=abcdef")
        .unwrap_err();
    match err {
        AuthError::Protocol(msg) => assert!(
            msg.contains("authzid"),
            "expected authzid rejection, got: {msg}"
        ),
        other => panic!("expected Protocol error, got: {other:?}"),
    }
}

#[test]
fn exchange_bad_client_final_no_proof() {
    let verifier = make_verifier("pw", b"s", 4096);
    let mut server = ScramServer::new(verifier);
    server.handle_client_first(b"n,,n=u,r=abc").unwrap();

    let err = server.handle_client_final(b"c=biws,r=nonce").unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

#[test]
fn exchange_bad_client_final_invalid_base64_proof() {
    let verifier = make_verifier("pw", b"s", 4096);
    let mut server = ScramServer::new(verifier);
    server.handle_client_first(b"n,,n=u,r=abc").unwrap();

    let err = server
        .handle_client_final(b"c=biws,r=nonce,p=!!!notbase64")
        .unwrap_err();
    assert!(matches!(err, AuthError::Protocol(_)));
}

// ---------------------------------------------------------------------------
// SASLInitialResponse parsing
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

// ---------------------------------------------------------------------------
// Full round-trip between `ScramClient` and `ScramServer`
// ---------------------------------------------------------------------------

/// Marked `#[tokio::test]` because `ScramClient::handle_server_first`
/// is async: it offloads PBKDF2 to the tokio blocking thread pool
/// so high iteration counts don't starve the async executor.
#[tokio::test]
async fn client_server_round_trip() {
    let password = "clientpass";
    let salt = b"csalt";
    let iterations = 4096;

    let verifier = make_verifier(password, salt, iterations);
    let mut server = ScramServer::new(verifier);
    let mut client = ScramClient::new(password);

    // Client sends initial response.
    let initial = client.initial_response();
    let (mech, client_first) = parse_sasl_initial_response(&initial).unwrap();
    assert_eq!(mech, "SCRAM-SHA-256");

    // Server processes client-first, returns server-first.
    let server_first = server.handle_client_first(client_first).unwrap();

    // Client processes server-first, returns client-final.
    let client_final = client.handle_server_first(&server_first).await.unwrap();

    // Server processes client-final, returns server-final.
    let server_final = server.handle_client_final(&client_final).unwrap();

    // Client verifies server-final.
    client.handle_server_final(&server_final).unwrap();
}
