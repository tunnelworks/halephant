//! Server-side SCRAM-SHA-256: state machine + wire driver for when
//! halephant authenticates a downstream client.
//!
//! `ScramServer` is the pure state machine (RFC 5802). `authenticate`
//! is the full wire exchange: given an already-framed client
//! connection and a pre-fetched [`ScramVerifier`], it sends
//! `AuthenticationSasl`, drives the SCRAM round-trip, and ends with
//! `AuthenticationOk` downstream.
//!
//! The halephant-level concerns — policy checks, verifier fetching,
//! tracing spans — live in [`crate::auth::Authenticator`] and are
//! explicitly NOT part of this module. That separation lets the
//! pure SCRAM exchange be tested with canned verifiers without
//! pulling in `Config` or `VerifierCache`.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use subtle::ConstantTimeEq;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::errors;
use crate::proto::backend::BackendMessage;
use crate::proto::codec::FrontendCodec;
use crate::proto::frontend::FrontendMessage;

use super::crypto::{extract_attr, hmac_sha256, sha256};
use super::verifier::ScramVerifier;

// ---------------------------------------------------------------------------
// ScramServer — the state machine
// ---------------------------------------------------------------------------

/// Server-side SCRAM-SHA-256 state machine (RFC 5802). Drives a
/// single authentication exchange; construct a fresh instance per
/// client.
pub struct ScramServer {
    verifier: ScramVerifier,
    server_nonce: String,
    client_first_bare: String,
    server_first: String,
}

impl ScramServer {
    pub fn new(verifier: ScramVerifier) -> Self {
        let mut nonce_bytes = [0u8; 18];
        rand::rng().fill(&mut nonce_bytes);
        let server_nonce = B64.encode(nonce_bytes);

        Self {
            verifier,
            server_nonce,
            client_first_bare: String::new(),
            server_first: String::new(),
        }
    }

    /// Process the client-first-message. Returns the server-first-message.
    ///
    /// Input: the raw bytes from the SASLInitialResponse (the initial
    /// client response data, NOT including the mechanism name).
    pub fn handle_client_first(&mut self, data: &[u8]) -> Result<Vec<u8>, errors::AuthError> {
        let msg = std::str::from_utf8(data)
            .map_err(|_| errors::AuthError::Protocol("client-first-message is not UTF-8".into()))?;

        // Strip GS2 header ("n,,") to get client-first-message-bare.
        let bare = strip_gs2_header(msg)?;
        bare.clone_into(&mut self.client_first_bare);

        let client_nonce = extract_attr(bare, 'r')?;
        let combined_nonce = format!("{client_nonce}{}", self.server_nonce);

        self.server_first = format!(
            "r={},s={},i={}",
            combined_nonce,
            B64.encode(&self.verifier.salt),
            self.verifier.iterations,
        );

        Ok(self.server_first.as_bytes().to_vec())
    }

    /// Process the client-final-message. Returns the server-final-message on
    /// success, or an error if the client proof is invalid.
    ///
    /// Input: the raw bytes from the SASLResponse.
    pub fn handle_client_final(&self, data: &[u8]) -> Result<Vec<u8>, errors::AuthError> {
        let msg = std::str::from_utf8(data)
            .map_err(|_| errors::AuthError::Protocol("client-final-message is not UTF-8".into()))?;

        // Verify the nonce matches the combined nonce from server-first (RFC 5802 §5.1).
        let expected_nonce = extract_attr(&self.server_first, 'r')?;
        let actual_nonce = extract_attr(msg, 'r')?;
        if actual_nonce != expected_nonce {
            return Err(errors::AuthError::Protocol(
                "nonce mismatch in client-final".into(),
            ));
        }

        let proof_b64 = extract_attr(msg, 'p')?;

        // client-final-message-without-proof: everything before ",p=..."
        let without_proof = msg
            .rsplit_once(",p=")
            .map(|(prefix, _)| prefix)
            .ok_or_else(|| {
                errors::AuthError::Protocol("missing proof in client-final-message".into())
            })?;

        // AuthMessage = client-first-bare + "," + server-first + "," + client-final-without-proof
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof,
        );

        // Verify the client proof.
        let client_proof = B64
            .decode(proof_b64)
            .map_err(|_| errors::AuthError::Protocol("invalid base64 in client proof".into()))?;
        if client_proof.len() != 32 {
            return Err(errors::AuthError::Protocol(
                "client proof is not 32 bytes".into(),
            ));
        }

        let client_signature = hmac_sha256(&self.verifier.stored_key, auth_message.as_bytes());

        // ClientKey = ClientProof XOR ClientSignature
        let mut client_key = [0u8; 32];
        for i in 0..32 {
            client_key[i] = client_proof[i] ^ client_signature[i];
        }

        // Verify SHA-256(ClientKey) == StoredKey using a constant-time
        // comparison. `[u8; 32]::eq` (and `!=`) short-circuit on the
        // first differing byte, which turns credential verification
        // into a timing oracle: an attacker controls the proof bytes
        // via the SCRAM exchange, so byte-by-byte comparison lets
        // them brute-force the computed StoredKey one byte at a time
        // across repeated authentication attempts. `ConstantTimeEq`
        // compares all bytes unconditionally.
        let computed_stored_key = sha256(&client_key);
        if !bool::from(computed_stored_key.ct_eq(&self.verifier.stored_key)) {
            return Err(errors::AuthError::InvalidCredentials);
        }

        // Compute server-final-message.
        let server_signature = hmac_sha256(&self.verifier.server_key, auth_message.as_bytes());
        let server_final = format!("v={}", B64.encode(server_signature));

        Ok(server_final.as_bytes().to_vec())
    }
}

// ---------------------------------------------------------------------------
// authenticate — the server-side wire driver
// ---------------------------------------------------------------------------

/// Drive the server side of SCRAM-SHA-256 over an already-framed
/// downstream client connection.
///
/// On success, `AuthenticationOk` has been sent and the client is
/// authenticated. The caller is responsible for any halephant-level
/// concerns before this call (policy checks, verifier fetching,
/// span setup) and after (sending `ParameterStatus`, `BackendKeyData`,
/// success/failure logging). This function is pure protocol plumbing
/// — no tracing events, no knowledge of `Config`, no logging.
///
/// Symmetric with [`super::client::authenticate`], which drives the
/// other side of the same exchange with inverted codec direction.
pub async fn authenticate(
    client: &mut Framed<TcpStream, FrontendCodec>,
    verifier: ScramVerifier,
) -> Result<(), errors::AuthError> {
    let mut scram = ScramServer::new(verifier);

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
    let server_first = scram.handle_client_first(client_first)?;
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
    let server_final = scram.handle_client_final(&final_data)?;

    // Step 6: Send server-final via AuthenticationSASLFinal, then AuthenticationOk.
    client
        .feed(BackendMessage::AuthenticationSaslFinal { data: server_final })
        .await?;
    client.send(BackendMessage::AuthenticationOk).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// SASLInitialResponse parsing
// ---------------------------------------------------------------------------

/// Parse the payload of a PasswordMessage that carries a
/// SASLInitialResponse. Returns `(mechanism_name, initial_response_data)`.
///
/// Server-side only: the client sends this message, the server
/// parses it. Lives here rather than in `crypto.rs` because nothing
/// on the client side needs to parse its own outbound frames.
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

// ---------------------------------------------------------------------------
// Helpers — server-side only
// ---------------------------------------------------------------------------

/// Parse and validate the GS2 header from a client-first-message,
/// returning the bare client-first-message (everything after the
/// header).
///
/// The header has the form `<gs2-cbind-flag>,<authzid>,`. RFC 5802
/// §5.1 defines three values for the cbind flag:
///
/// - `n` — client does not support channel binding.
/// - `y` — client supports channel binding but believes the server
///   does not. libpq sends this when `channel_binding=prefer` (its
///   default) and the server's offered mechanism list omits
///   `-PLUS`, which is halephant's situation because it only
///   advertises `SCRAM-SHA-256`. Rejecting `y` here would break
///   every libpq client connecting over TLS with the default
///   settings, so we accept it.
/// - `p=<cbind-type>` — client **requires** channel binding of the
///   named type. Silently treating this as if the client had sent
///   `n,,` would strip the client's explicit security requirement
///   — the client believes the authentication is bound to the TLS
///   channel when it isn't, leaving it vulnerable to a MITM that
///   downgrades the exchange. Halephant does not implement channel
///   binding, so `p=...` is rejected with a clear error rather
///   than silently accepted.
///
/// The authzid field (between the two commas) must be empty:
/// halephant does not support SASL authzid identity switching, so
/// a non-empty value is rejected rather than silently ignored —
/// otherwise a client asking to act as a different identity would
/// be authenticated as the authentication identity with no warning.
fn strip_gs2_header(msg: &str) -> Result<&str, errors::AuthError> {
    let after_flag = if let Some(rest) = msg.strip_prefix("n,") {
        rest
    } else if let Some(rest) = msg.strip_prefix("y,") {
        rest
    } else if msg.starts_with("p=") {
        return Err(errors::AuthError::Protocol(
            "channel binding (gs2-cbind-flag 'p=...') is not supported".into(),
        ));
    } else {
        return Err(errors::AuthError::Protocol(
            "unsupported or malformed gs2-cbind-flag in client-first-message".into(),
        ));
    };

    // Authzid must be empty: the next character has to be the
    // separator comma that ends the GS2 header. A non-empty authzid
    // (`a=someuser,`) hits the `None` branch here.
    after_flag.strip_prefix(',').ok_or_else(|| {
        errors::AuthError::Protocol("non-empty authzid in GS2 header is not supported".into())
    })
}
