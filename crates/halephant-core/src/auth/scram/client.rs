//! Client-side SCRAM-SHA-256: state machine + wire driver for when
//! halephant authenticates against an upstream PostgreSQL server.
//!
//! `ScramClient` is the pure state machine. `authenticate` is the
//! full wire exchange: given an already-framed upstream connection
//! and a plaintext password, it sends the SASL initial response,
//! drives the round-trip, and consumes the upstream's
//! `AuthenticationOk`.
//!
//! Symmetric with [`super::server`] but inverted along the wire:
//! halephant is the party *proving* knowledge of the password here,
//! not the party verifying a proof.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::errors;
use crate::proto::backend::BackendMessage;
use crate::proto::codec::BackendCodec;
use crate::proto::frontend::FrontendMessage;

use super::crypto::{extract_attr, hmac_sha256, sha256};

// ---------------------------------------------------------------------------
// ScramClient — the state machine
// ---------------------------------------------------------------------------

/// Client-side SCRAM-SHA-256 state machine. Holds the plaintext
/// password and the derived keys for one authentication exchange.
/// Construct a fresh instance per upstream connection.
pub struct ScramClient {
    password: String,
    nonce: String,
    client_first_bare: String,
    server_first: String,
    salted_password: [u8; 32],
}

impl ScramClient {
    pub fn new(password: &str) -> Self {
        let mut nonce_bytes = [0u8; 18];
        rand::rng().fill(&mut nonce_bytes);
        let nonce = B64.encode(nonce_bytes);

        let client_first_bare = format!("n=,r={nonce}");

        Self {
            password: password.to_owned(),
            nonce,
            client_first_bare,
            server_first: String::new(),
            salted_password: [0u8; 32],
        }
    }

    /// Build the SASLInitialResponse payload (mechanism name + client-first-message).
    pub fn initial_response(&self) -> Vec<u8> {
        let mechanism = b"SCRAM-SHA-256\0";
        let client_first = format!("n,,{}", self.client_first_bare);
        let data = client_first.as_bytes();
        let mut buf = Vec::with_capacity(mechanism.len() + 4 + data.len());
        buf.extend_from_slice(mechanism);
        buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
        buf.extend_from_slice(data);
        buf
    }

    /// Process the server-first-message (from `AuthenticationSASLContinue`).
    /// Returns the client-final-message bytes to send as `SASLResponse`.
    ///
    /// This method is `async` because it offloads the PBKDF2 key
    /// derivation to the tokio blocking thread pool. PostgreSQL 15+
    /// defaults to 600_000 iterations (up from 4_096 in earlier
    /// versions), which takes hundreds of milliseconds to hash —
    /// running that synchronously on an async worker would starve
    /// every other task on the same worker thread for the full
    /// duration. `VerifierCache::try_get_with` additionally
    /// coalesces concurrent auth requests, so a cache miss under
    /// blocking PBKDF2 would freeze every pending authenticator
    /// waiting on the shared fetch.
    pub async fn handle_server_first(&mut self, data: &[u8]) -> Result<Vec<u8>, errors::AuthError> {
        let msg = std::str::from_utf8(data)
            .map_err(|_| errors::AuthError::Protocol("server-first-message is not UTF-8".into()))?;

        msg.clone_into(&mut self.server_first);

        let combined_nonce = extract_attr(msg, 'r')?;
        if !combined_nonce.starts_with(&self.nonce) {
            return Err(errors::AuthError::Protocol(
                "server nonce does not start with client nonce".into(),
            ));
        }

        let salt_b64 = extract_attr(msg, 's')?;
        let salt = B64
            .decode(salt_b64)
            .map_err(|_| errors::AuthError::Protocol("invalid base64 in salt".into()))?;
        let iterations: u32 = extract_attr(msg, 'i')?
            .parse()
            .map_err(|_| errors::AuthError::Protocol("invalid iteration count".into()))?;

        // Derive SaltedPassword via PBKDF2 on the blocking thread
        // pool — see the method docstring for the motivation. The
        // password and salt must be owned by the closure because
        // `spawn_blocking` requires a `'static` bound; `password`
        // is cloned from `self.password` and `salt` is already the
        // owned `Vec<u8>` from the B64 decode above.
        let password = self.password.clone();
        self.salted_password = tokio::task::spawn_blocking(move || {
            let mut out = [0u8; 32];
            pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, iterations, &mut out);
            out
        })
        .await
        .map_err(|e| errors::AuthError::Protocol(format!("pbkdf2 task failed: {e}")))?;

        let client_key = hmac_sha256(&self.salted_password, b"Client Key");
        let stored_key = sha256(&client_key);

        let client_final_without_proof = format!("c=biws,r={combined_nonce}");
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, client_final_without_proof,
        );

        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let mut client_proof = [0u8; 32];
        for i in 0..32 {
            client_proof[i] = client_key[i] ^ client_signature[i];
        }

        let client_final = format!(
            "{},p={}",
            client_final_without_proof,
            B64.encode(client_proof),
        );

        Ok(client_final.as_bytes().to_vec())
    }

    /// Verify the server-final-message (from `AuthenticationSASLFinal`).
    pub fn handle_server_final(&self, data: &[u8]) -> Result<(), errors::AuthError> {
        let msg = std::str::from_utf8(data)
            .map_err(|_| errors::AuthError::Protocol("server-final-message is not UTF-8".into()))?;

        // Check for server error.
        if msg.starts_with("e=") {
            return Err(errors::AuthError::Protocol(format!(
                "SCRAM server error: {msg}"
            )));
        }

        let verifier_b64 = extract_attr(msg, 'v')?;
        let server_signature = B64.decode(verifier_b64).map_err(|_| {
            errors::AuthError::Protocol("invalid base64 in server signature".into())
        })?;

        // Recompute expected server signature.
        let server_key = hmac_sha256(&self.salted_password, b"Server Key");
        let client_final_without_proof = {
            // Reconstruct from server_first nonce.
            let combined_nonce = extract_attr(&self.server_first, 'r').map_err(|_| {
                errors::AuthError::Protocol("missing nonce in cached server-first".into())
            })?;
            format!("c=biws,r={combined_nonce}")
        };
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, client_final_without_proof,
        );
        let expected = hmac_sha256(&server_key, auth_message.as_bytes());

        // Constant-time comparison against the expected server
        // signature. Same reasoning as the server-side proof check:
        // an attacker impersonating the upstream (MITM or rogue
        // proxy) could otherwise time-attack this comparison to
        // brute-force `expected` byte by byte across repeated
        // authentication attempts. `ct_eq` on `&[u8]` returns
        // `Choice(0)` if lengths differ, so the explicit length
        // guard below is redundant for correctness but makes the
        // 32-byte invariant visible at the call site.
        if server_signature.len() != 32 || !bool::from(server_signature.ct_eq(&expected[..])) {
            return Err(errors::AuthError::Protocol(
                "server signature verification failed".into(),
            ));
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// authenticate — the client-side wire driver
// ---------------------------------------------------------------------------

/// Drive the client side of SCRAM-SHA-256 on an already-framed
/// upstream connection. Call this after receiving `AuthenticationSasl`
/// with SCRAM-SHA-256 in the mechanism list. On success,
/// `AuthenticationOk` has been consumed from the stream.
///
/// Symmetric with [`super::server::authenticate`], which drives the
/// other side of the same exchange with inverted codec direction.
pub async fn authenticate(
    framed: &mut Framed<TcpStream, BackendCodec>,
    password: &str,
) -> Result<(), errors::AuthError> {
    let mut client = ScramClient::new(password);

    // Send SASLInitialResponse.
    framed
        .send(FrontendMessage::PasswordMessage(client.initial_response()))
        .await?;

    // Read AuthenticationSASLContinue.
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

    // Send SASLResponse (client-final). `handle_server_first` is
    // async because it offloads PBKDF2 to the blocking thread pool.
    let client_final = client.handle_server_first(&server_first).await?;
    framed
        .send(FrontendMessage::PasswordMessage(client_final))
        .await?;

    // Read AuthenticationSASLFinal.
    match framed.next().await.transpose()? {
        Some(BackendMessage::AuthenticationSaslFinal { data }) => {
            client.handle_server_final(&data)?;
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
