//! Client-side SCRAM-SHA-256 wire driver for when halephant authenticates
//! against an upstream PostgreSQL server.
//!
//! The pure SCRAM state machine lives in [`tinyscram::ClientSession`]; this
//! module owns only the PostgreSQL wire framing (SASLInitialResponse layout
//! around the SCRAM bytes, and the `AuthenticationSASLContinue` /
//! `AuthenticationSASLFinal` / `AuthenticationOk` envelopes around the
//! responses).
//!
//! PBKDF2 is offloaded to the tokio blocking thread pool because PostgreSQL 15+
//! defaults to 600,000 iterations — running that synchronously on an async
//! worker would starve every other task on the same worker thread.
//! `VerifierCache::try_get_with` additionally coalesces concurrent auth
//! requests, so a cache miss under blocking PBKDF2 would freeze every pending
//! authenticator waiting on the shared fetch.
//!
//! Symmetric with [`super::server`] but inverted along the wire: halephant is
//! the party *proving* knowledge of the password here, not the party verifying
//! a proof.

use futures_util::{SinkExt, StreamExt};
use tinyscram::ClientSession;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::errors;
use crate::proto::backend::BackendMessage;
use crate::proto::codec::BackendCodec;
use crate::proto::frontend::FrontendMessage;

use super::server::map_scram;

/// Wrap a SCRAM `client-first-message` byte string in the PostgreSQL
/// SASLInitialResponse envelope: `<mechanism>\0<i32 length><client-first>`.
fn sasl_initial_response(client_first: &[u8]) -> Vec<u8> {
    let mechanism = b"SCRAM-SHA-256\0";
    let mut buf = Vec::with_capacity(mechanism.len() + 4 + client_first.len());
    buf.extend_from_slice(mechanism);
    buf.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
    buf.extend_from_slice(client_first);
    buf
}

/// Drive the client side of SCRAM-SHA-256 on an already-framed upstream
/// connection. Call this after receiving `AuthenticationSasl` with
/// SCRAM-SHA-256 in the mechanism list. On success, `AuthenticationOk` has been
/// consumed from the stream.
pub async fn authenticate(
    framed: &mut Framed<TcpStream, BackendCodec>,
    password: &str,
) -> Result<(), errors::AuthError> {
    // PostgreSQL carries the username in StartupMessage; SCRAM's `n=<authcid>`
    // is therefore empty in PG's flavour. tinyscram serialises an empty
    // authcid as `n=,r=<nonce>` — the literal libpq sends.
    let mut session = ClientSession::new("", password);

    // Send SASLInitialResponse wrapping client-first.
    let client_first = session.client_first();
    framed
        .send(FrontendMessage::PasswordMessage(sasl_initial_response(
            &client_first,
        )))
        .await?;

    // Read AuthenticationSASLContinue (server-first).
    let server_first = match framed.next().await.transpose()? {
        Some(BackendMessage::AuthenticationSaslContinue { data }) => data,
        Some(BackendMessage::ErrorResponse(err)) => {
            return Err(errors::AuthError::VerifierFetch(format!(
                "SCRAM auth error: {}",
                err.message().unwrap_or("unknown")
            )));
        }
        other => {
            return Err(errors::AuthError::Protocol(format!(
                "expected AuthenticationSASLContinue, got {other:?}"
            )));
        }
    };

    // Offload `handle_server_first` to the blocking pool: it runs PBKDF2 with
    // a server-supplied iteration count (>=4096, often hundreds of thousands).
    // Move the session in and back out so the trailing `handle_server_final`
    // call still has access to the cached server key.
    let (mut session, client_final) = tokio::task::spawn_blocking(move || {
        let mut s = session;
        let result = s.handle_server_first(&server_first);
        (s, result)
    })
    .await
    .map_err(|e| errors::AuthError::Protocol(format!("pbkdf2 task failed: {e}")))?;
    let client_final = client_final.map_err(map_scram)?;

    framed
        .send(FrontendMessage::PasswordMessage(client_final))
        .await?;

    // Read AuthenticationSASLFinal (server-final).
    match framed.next().await.transpose()? {
        Some(BackendMessage::AuthenticationSaslFinal { data }) => {
            session.handle_server_final(&data).map_err(map_scram)?;
        }
        Some(BackendMessage::ErrorResponse(err)) => {
            return Err(errors::AuthError::VerifierFetch(format!(
                "SCRAM auth failed: {}",
                err.message().unwrap_or("unknown")
            )));
        }
        other => {
            return Err(errors::AuthError::Protocol(format!(
                "expected AuthenticationSASLFinal, got {other:?}"
            )));
        }
    }

    // AuthenticationOk follows.
    match framed.next().await.transpose()? {
        Some(BackendMessage::AuthenticationOk) => Ok(()),
        Some(BackendMessage::ErrorResponse(err)) => Err(errors::AuthError::VerifierFetch(format!(
            "auth failed after SCRAM: {}",
            err.message().unwrap_or("unknown")
        ))),
        other => Err(errors::AuthError::Protocol(format!(
            "expected AuthenticationOk, got {other:?}"
        ))),
    }
}
