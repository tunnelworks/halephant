use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::errors;
use crate::proto::backend::BackendMessage;
use crate::proto::frontend::FrontendMessage;

// ---------------------------------------------------------------------------
// FrontendCodec — client-facing side of the proxy
//
// Decodes FrontendMessage (client → proxy)
// Encodes BackendMessage (proxy → client)
// ---------------------------------------------------------------------------

/// Codec for the client-facing half of a proxied connection.
pub struct FrontendCodec {
    /// `true` while we are expecting initial messages (startup / SSL / cancel)
    /// that lack a type byte. Switches to `false` after a `StartupMessage`.
    startup: bool,
}

impl FrontendCodec {
    pub fn new() -> Self {
        Self { startup: true }
    }

    /// Creates a codec that skips startup message parsing. Use this for
    /// connections that have already completed the startup handshake.
    pub fn post_startup() -> Self {
        Self { startup: false }
    }
}

impl Default for FrontendCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for FrontendCodec {
    type Item = FrontendMessage;
    type Error = errors::ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.startup {
            decode_initial(src)
        } else {
            decode_regular(src, FrontendMessage::parse)
        }
        .map(|opt| {
            if let Some(ref msg) = opt {
                match msg {
                    // SSLRequest / GssEncRequest: stay in startup mode — the
                    // real StartupMessage follows after the proxy responds.
                    // CancelRequest is a one-shot message on a throwaway
                    // connection, but keep startup mode just in case.
                    FrontendMessage::SslRequest
                    | FrontendMessage::GssEncRequest
                    | FrontendMessage::CancelRequest { .. } => {}
                    // Any other initial message (StartupMessage) transitions
                    // to regular message framing.
                    _ => self.startup = false,
                }
            }
            opt
        })
    }
}

impl Encoder<BackendMessage> for FrontendCodec {
    type Error = errors::ProtocolError;

    fn encode(&mut self, item: BackendMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BackendCodec — server-facing side of the proxy
//
// Decodes BackendMessage (server → proxy)
// Encodes FrontendMessage (proxy → server)
// ---------------------------------------------------------------------------

/// Codec for the server-facing half of a proxied connection.
pub struct BackendCodec;

impl BackendCodec {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BackendCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for BackendCodec {
    type Item = BackendMessage;
    type Error = errors::ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        decode_regular(src, BackendMessage::parse)
    }
}

impl Encoder<FrontendMessage> for BackendCodec {
    type Error = errors::ProtocolError;

    fn encode(&mut self, item: FrontendMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared framing helpers
// ---------------------------------------------------------------------------

/// Decode an initial message (no type byte): `[length: i32] [payload]`.
fn decode_initial(src: &mut BytesMut) -> Result<Option<FrontendMessage>, errors::ProtocolError> {
    if src.len() < 4 {
        return Ok(None);
    }
    let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
    if length < 4 {
        return Err(errors::ProtocolError::InvalidValue {
            position: 0,
            message: format!("initial message length too small: {length}"),
        });
    }
    if src.len() < length {
        // Not enough data yet — wait for more.
        return Ok(None);
    }
    let frame = src.split_to(length);
    FrontendMessage::parse_initial(&frame).map(Some)
}

/// Decode a regular message: `[type: u8] [length: i32] [payload]`.
///
/// The `parse` callback receives `(type_byte, payload_slice)`.
fn decode_regular<M>(
    src: &mut BytesMut,
    parse: impl FnOnce(u8, &[u8]) -> Result<M, errors::ProtocolError>,
) -> Result<Option<M>, errors::ProtocolError> {
    if src.len() < 5 {
        return Ok(None);
    }
    let msg_type = src[0];
    let length = u32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize;
    if length < 4 {
        return Err(errors::ProtocolError::InvalidValue {
            position: 1,
            message: format!("message length too small: {length}"),
        });
    }
    let total = 1 + length; // type byte + length value (which includes its own 4 bytes)
    if src.len() < total {
        return Ok(None);
    }
    let frame = src.split_to(total);
    let payload = &frame[5..]; // skip type (1) + length (4)
    parse(msg_type, payload).map(Some)
}
