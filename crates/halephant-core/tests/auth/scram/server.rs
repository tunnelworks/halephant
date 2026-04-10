use halephant_core::auth::scram::crypto;
use halephant_core::auth::scram::server;
use halephant_core::errors::AuthError;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use sha2::Sha256;

/// Simulate a full SCRAM exchange by driving `ScramServer` with a
/// hand-rolled client that knows the password. Exercises the
/// state machine without going through the wire driver.
#[test]
fn full_scram_exchange() {
    use pbkdf2::pbkdf2_hmac;

    let password = "supersecret";
    let salt = b"randomsalt";
    let iterations = 4096;

    let verifier = super::make_verifier(password, salt, iterations);
    let mut server = server::ScramServer::new(verifier);

    // -- Client first --
    let client_nonce = "rOprNGfwEbeRWgbNEkqO";
    let client_first = format!("n,,n=testuser,r={client_nonce}");

    let server_first_bytes = server.handle_client_first(client_first.as_bytes()).unwrap();
    let server_first = std::str::from_utf8(&server_first_bytes).unwrap();

    // Extract combined nonce and verify it starts with client nonce.
    let combined_nonce = crypto::extract_attr(server_first, 'r').unwrap();
    assert!(combined_nonce.starts_with(client_nonce));

    // -- Client final (compute proof like a real client would) --
    let mut salted_password = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted_password);

    let client_key = crypto::hmac_sha256(&salted_password, b"Client Key");
    let stored_key = crypto::sha256(&client_key);

    let client_first_bare = format!("n=testuser,r={client_nonce}");
    let client_final_without_proof = format!("c=biws,r={combined_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");

    let client_signature = crypto::hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut client_proof = [0u8; 32];
    for i in 0..32 {
        client_proof[i] = client_key[i] ^ client_signature[i];
    }

    let client_final = format!(
        "{},p={}",
        client_final_without_proof,
        B64.encode(client_proof)
    );

    let server_final_bytes = server.handle_client_final(client_final.as_bytes()).unwrap();
    let server_final = std::str::from_utf8(&server_final_bytes).unwrap();

    // Verify server proof.
    let server_key = crypto::hmac_sha256(&salted_password, b"Server Key");
    let expected_sig = crypto::hmac_sha256(&server_key, auth_message.as_bytes());
    let expected_final = format!("v={}", B64.encode(expected_sig));
    assert_eq!(server_final, expected_final);
}

#[test]
fn wrong_password_rejected() {
    use pbkdf2::pbkdf2_hmac;

    let verifier = super::make_verifier("correct", b"salt", 4096);
    let mut server = server::ScramServer::new(verifier);

    let client_nonce = "abcdef";
    let client_first = format!("n,,n=user,r={client_nonce}");
    let server_first_bytes = server.handle_client_first(client_first.as_bytes()).unwrap();
    let server_first = std::str::from_utf8(&server_first_bytes).unwrap();
    let combined_nonce = crypto::extract_attr(server_first, 'r').unwrap();

    // Derive proof from the WRONG password.
    let mut salted = [0u8; 32];
    pbkdf2_hmac::<Sha256>(b"wrong", b"salt", 4096, &mut salted);

    let ck = crypto::hmac_sha256(&salted, b"Client Key");
    let sk = crypto::sha256(&ck);

    let bare = format!("n=user,r={client_nonce}");
    let cfwp = format!("c=biws,r={combined_nonce}");
    let auth_msg = format!("{bare},{server_first},{cfwp}");
    let csig = crypto::hmac_sha256(&sk, auth_msg.as_bytes());
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = ck[i] ^ csig[i];
    }

    let client_final = format!("{cfwp},p={}", B64.encode(proof));
    let result = server.handle_client_final(client_final.as_bytes());
    assert!(matches!(result, Err(AuthError::InvalidCredentials)));
}

#[test]
fn parse_sasl_initial() {
    // mechanism\0 + int32(len) + data
    let mut buf = Vec::new();
    buf.extend_from_slice(b"SCRAM-SHA-256\0");
    let data = b"n,,n=user,r=nonce";
    buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
    buf.extend_from_slice(data);

    let (mech, resp) = server::parse_sasl_initial_response(&buf).unwrap();
    assert_eq!(mech, "SCRAM-SHA-256");
    assert_eq!(resp, data);
}
