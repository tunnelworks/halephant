use super::make_verifier;
use halephant_core::auth::scram::client;
use halephant_core::auth::scram::server;

/// A tampered server-final must be rejected — the client
/// verifies the server signature against its own derived key.
///
/// Marked `#[tokio::test]` because `ScramClient::handle_server_first`
/// is async: it offloads PBKDF2 to the tokio blocking thread pool
/// so high iteration counts don't starve the async executor.
#[tokio::test]
async fn client_rejects_bad_server() {
    let password = "pass";
    let verifier = make_verifier(password, b"salt", 4096);
    let mut server = server::ScramServer::new(verifier);
    let mut client = client::ScramClient::new(password);

    let initial = client.initial_response();
    let (_, client_first) = server::parse_sasl_initial_response(&initial).unwrap();
    let server_first = server.handle_client_first(client_first).unwrap();
    let client_final = client.handle_server_first(&server_first).await.unwrap();
    let _ = server.handle_client_final(&client_final).unwrap();

    // Tamper with server-final.
    let result = client.handle_server_final(b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
    assert!(result.is_err());
}
