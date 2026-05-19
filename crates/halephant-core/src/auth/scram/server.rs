//! Server-side SCRAM-SHA-256 wire driver for when halephant authenticates a
//! downstream client.
//!
//! The pure SCRAM state machine lives in [`tinyscram::ServerSession`]; this
//! module owns only the PostgreSQL wire framing (`AuthenticationSASL` /
//! `AuthenticationSASLContinue` / `AuthenticationSASLFinal` / `AuthenticationOk`
//! envelopes and the SASLInitialResponse layout that wraps the SCRAM bytes).
//!
//! Halephant-level concerns — policy checks, verifier fetching, tracing spans —
//! live in [`crate::auth::Authenticator`] and are explicitly NOT part of this
//! module. That separation lets the pure SCRAM exchange be tested with canned
//! verifiers without pulling in `Config` or `VerifierCache`.
//!
//! Symmetric with [`super::client`] but inverted along the wire.

use futures_util::{SinkExt, StreamExt};
use tinyscram::ServerSession;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::errors;
use crate::proto::backend::BackendMessage;
use crate::proto::codec::FrontendCodec;
use crate::proto::frontend::FrontendMessage;

use super::verifier::ScramVerifier;

/// Drive the server side of SCRAM-SHA-256 over an already-framed downstream
/// client connection.
///
/// On success, `AuthenticationOk` has been sent and the client is authenticated.
/// The caller is responsible for any halephant-level concerns before this call
/// (policy checks, verifier fetching, span setup) and after (sending
/// `ParameterStatus`, `BackendKeyData`, success/failure logging). This function
/// is pure protocol plumbing — no tracing events, no knowledge of `Config`, no
/// logging.
pub async fn authenticate(
    client: &mut Framed<TcpStream, FrontendCodec>,
    verifier: ScramVerifier,
) -> Result<(), errors::AuthError> {
    let mut scram = ServerSession::new(verifier);

    // Step 1: Send AuthenticationSASL to request SCRAM-SHA-256.
    client
        .send(BackendMessage::AuthenticationSasl {
            mechanisms: vec!["SCRAM-SHA-256".into()],
        })
        .await?;

    // Step 2: Read client's SASLInitialResponse.
    let initial_data = match client.next().await.transpose()? {
        Some(FrontendMessage::PasswordMessage(data)) => data,
        Some(other) => {
            return Err(errors::AuthError::Protocol(format!(
                "expected SASLInitialResponse, got {other:?}"
            )));
        }
        None => {
            return Err(errors::AuthError::Protocol(
                "client disconnected during SASL".into(),
            ));
        }
    };

    let (mechanism, client_first) = parse_sasl_initial_response(&initial_data)?;
    if mechanism != "SCRAM-SHA-256" {
        return Err(errors::AuthError::Protocol(format!(
            "unsupported SASL mechanism: {mechanism:?}"
        )));
    }

    // Step 3: Process client-first, send server-first via AuthenticationSASLContinue.
    let server_first = scram.handle_client_first(client_first).map_err(map_scram)?;
    client
        .send(BackendMessage::AuthenticationSaslContinue { data: server_first })
        .await?;

    // Step 4: Read client's SASLResponse (client-final-message).
    let final_data = match client.next().await.transpose()? {
        Some(FrontendMessage::PasswordMessage(data)) => data,
        Some(other) => {
            return Err(errors::AuthError::Protocol(format!(
                "expected SASLResponse, got {other:?}"
            )));
        }
        None => {
            return Err(errors::AuthError::Protocol(
                "client disconnected during SASL".into(),
            ));
        }
    };

    // Step 5: Verify client proof.
    let server_final = scram.handle_client_final(&final_data).map_err(map_scram)?;

    // Step 6: Send server-final via AuthenticationSASLFinal, then AuthenticationOk.
    client
        .feed(BackendMessage::AuthenticationSaslFinal { data: server_final })
        .await?;
    client.send(BackendMessage::AuthenticationOk).await?;

    Ok(())
}

/// Parse the payload of a PasswordMessage that carries a SASLInitialResponse.
/// Returns `(mechanism_name, initial_response_data)`.
///
/// Server-side only: the client sends this message, the server parses it. PG
/// wraps the SCRAM `client-first-message` bytes in this framing; the bytes
/// themselves are an opaque payload as far as `tinyscram` is concerned.
pub fn parse_sasl_initial_response(data: &[u8]) -> Result<(&str, &[u8]), errors::AuthError> {
    let nul = data
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| errors::AuthError::Protocol("missing null in SASLInitialResponse".into()))?;
    let mechanism = std::str::from_utf8(&data[..nul])
        .map_err(|_| errors::AuthError::Protocol("invalid UTF-8 in mechanism name".into()))?;
    let rest = &data[nul + 1..];
    if rest.len() < 4 {
        return Err(errors::AuthError::Protocol(
            "truncated SASLInitialResponse length".into(),
        ));
    }
    let len = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
    let response = if len < 0 {
        &[]
    } else {
        let len = len as usize;
        if rest.len() < 4 + len {
            return Err(errors::AuthError::Protocol(
                "truncated SASLInitialResponse data".into(),
            ));
        }
        &rest[4..4 + len]
    };
    Ok((mechanism, response))
}

/// `InvalidProof` is the credential-mismatch path and must surface as
/// `InvalidCredentials` so the upper auth layer can apply the standard
/// bad-password treatment (tarpit, log). Everything else is a protocol-level
/// failure with no special halephant handling.
pub(crate) fn map_scram(e: tinyscram::Error) -> errors::AuthError {
    match e {
        tinyscram::Error::InvalidProof => errors::AuthError::InvalidCredentials,
        other => errors::AuthError::Protocol(format!("scram: {other}")),
    }
}
