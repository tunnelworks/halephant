use bytes::{BufMut, BytesMut};

use crate::errors;
use crate::proto::coding;
use crate::proto::cursor::{self, Cursor};
use crate::proto::types;

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

/// A message sent by a PostgreSQL frontend (client → server).
#[derive(Debug, Clone, PartialEq)]
pub enum FrontendMessage {
    // -- Initial messages (no type byte on the wire) --
    Startup(Startup),
    SslRequest,
    GssEncRequest,
    CancelRequest { process_id: i32, secret_key: i32 },

    // -- Regular messages --
    Bind(Bind),
    Close(Close),
    CopyData(Vec<u8>),
    CopyDone,
    CopyFail(String),
    Describe(Describe),
    Execute(Execute),
    Flush,
    FunctionCall(Vec<u8>),
    Parse(Parse),
    PasswordMessage(Vec<u8>),
    Query(String),
    Sync,
    Terminate,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Startup {
    pub version: ProtocolVersion,
    pub parameters: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolVersion {
    pub major: i16,
    pub minor: i16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Parse {
    pub name: String,
    pub query: String,
    pub param_types: Vec<types::Oid>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Bind {
    pub portal: String,
    pub statement: String,
    pub param_formats: Vec<types::FormatCode>,
    pub params: Vec<Option<Vec<u8>>>,
    pub result_formats: Vec<types::FormatCode>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Describe {
    pub kind: TargetKind,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Execute {
    pub portal: String,
    pub max_rows: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Close {
    pub kind: TargetKind,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetKind {
    Statement,
    Portal,
}

// ---------------------------------------------------------------------------
// Well-known codes for initial messages
// ---------------------------------------------------------------------------

const PROTOCOL_V3: i32 = 196_608; // 3 << 16
const SSL_REQUEST_CODE: i32 = 80_877_103;
const CANCEL_REQUEST_CODE: i32 = 80_877_102;
const GSS_ENC_REQUEST_CODE: i32 = 80_877_104;

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

fn decode_target_kind(cur: &mut Cursor<'_>) -> Result<TargetKind, errors::ProtocolError> {
    let start = cur.position();
    match cur.read_u8()? {
        b'S' => Ok(TargetKind::Statement),
        b'P' => Ok(TargetKind::Portal),
        b => Err(errors::ProtocolError::InvalidValue {
            position: start,
            message: format!("expected 'S' or 'P' for target kind, got {b:#04x}"),
        }),
    }
}

fn decode_startup_body(cur: &mut Cursor<'_>) -> Result<FrontendMessage, errors::ProtocolError> {
    let mut parameters = Vec::new();
    loop {
        let key = cur.read_cstring()?;
        if key.is_empty() {
            break;
        }
        let value = cur.read_cstring()?;
        parameters.push((key, value));
    }
    Ok(FrontendMessage::Startup(Startup {
        version: ProtocolVersion { major: 3, minor: 0 },
        parameters,
    }))
}

fn decode_query(cur: &mut Cursor<'_>) -> Result<FrontendMessage, errors::ProtocolError> {
    Ok(FrontendMessage::Query(cur.read_cstring()?))
}

fn decode_parse(cur: &mut Cursor<'_>) -> Result<FrontendMessage, errors::ProtocolError> {
    let name = cur.read_cstring()?;
    let query = cur.read_cstring()?;
    let param_types = cur.read_count_prefixed(|cur| Ok(cur.read_i32()? as u32))?;
    Ok(FrontendMessage::Parse(Parse {
        name,
        query,
        param_types,
    }))
}

fn decode_bind(cur: &mut Cursor<'_>) -> Result<FrontendMessage, errors::ProtocolError> {
    let portal = cur.read_cstring()?;
    let statement = cur.read_cstring()?;
    let param_formats = cur.read_count_prefixed(Cursor::read_format_code)?;
    let params = cur.read_count_prefixed(Cursor::read_nullable_bytes)?;
    let result_formats = cur.read_count_prefixed(Cursor::read_format_code)?;
    Ok(FrontendMessage::Bind(Bind {
        portal,
        statement,
        param_formats,
        params,
        result_formats,
    }))
}

fn decode_describe(cur: &mut Cursor<'_>) -> Result<FrontendMessage, errors::ProtocolError> {
    let kind = decode_target_kind(cur)?;
    let name = cur.read_cstring()?;
    Ok(FrontendMessage::Describe(Describe { kind, name }))
}

fn decode_execute(cur: &mut Cursor<'_>) -> Result<FrontendMessage, errors::ProtocolError> {
    let portal = cur.read_cstring()?;
    let max_rows = cur.read_i32()?;
    Ok(FrontendMessage::Execute(Execute { portal, max_rows }))
}

fn decode_close(cur: &mut Cursor<'_>) -> Result<FrontendMessage, errors::ProtocolError> {
    let kind = decode_target_kind(cur)?;
    let name = cur.read_cstring()?;
    Ok(FrontendMessage::Close(Close { kind, name }))
}

fn decode_copy_fail(cur: &mut Cursor<'_>) -> Result<FrontendMessage, errors::ProtocolError> {
    Ok(FrontendMessage::CopyFail(cur.read_cstring()?))
}

// ---------------------------------------------------------------------------
// Public parse API
// ---------------------------------------------------------------------------

impl FrontendMessage {
    /// Parse an initial message (startup / SSL / cancel / GSS-enc).
    ///
    /// The input is the *complete* message including the leading 4-byte length.
    pub fn parse_initial(input: &[u8]) -> Result<Self, errors::ProtocolError> {
        cursor::run(input, |cur| {
            // The length field already framed this message in the
            // codec; its value is not re-validated here.
            let _length = cur.read_i32()?;
            let code = cur.read_i32()?;
            match code {
                PROTOCOL_V3 => decode_startup_body(cur),
                SSL_REQUEST_CODE => Ok(FrontendMessage::SslRequest),
                CANCEL_REQUEST_CODE => {
                    let process_id = cur.read_i32()?;
                    let secret_key = cur.read_i32()?;
                    Ok(FrontendMessage::CancelRequest {
                        process_id,
                        secret_key,
                    })
                }
                GSS_ENC_REQUEST_CODE => Ok(FrontendMessage::GssEncRequest),
                // The code field starts at byte 4, after the length.
                _ => Err(errors::ProtocolError::InvalidValue {
                    position: 4,
                    message: format!("unknown initial message code: {code}"),
                }),
            }
        })
    }

    /// Parse a regular (non-initial) message.
    ///
    /// `msg_type` is the leading type byte; `payload` is everything after the
    /// 4-byte length field (i.e. the length-field's value minus 4).
    pub fn parse(msg_type: u8, payload: &[u8]) -> Result<Self, errors::ProtocolError> {
        match msg_type {
            b'B' => cursor::run(payload, decode_bind),
            b'C' => cursor::run(payload, decode_close),
            b'c' => Ok(FrontendMessage::CopyDone),
            b'd' => Ok(FrontendMessage::CopyData(payload.to_vec())),
            b'D' => cursor::run(payload, decode_describe),
            b'E' => cursor::run(payload, decode_execute),
            b'f' => cursor::run(payload, decode_copy_fail),
            b'F' => Ok(FrontendMessage::FunctionCall(payload.to_vec())),
            b'H' => Ok(FrontendMessage::Flush),
            b'P' => cursor::run(payload, decode_parse),
            b'p' => Ok(FrontendMessage::PasswordMessage(payload.to_vec())),
            b'Q' => cursor::run(payload, decode_query),
            b'S' => Ok(FrontendMessage::Sync),
            b'X' => Ok(FrontendMessage::Terminate),
            _ => Err(errors::ProtocolError::UnknownMessageType(msg_type)),
        }
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

impl FrontendMessage {
    /// Encode this message into `dst` in PostgreSQL wire format.
    pub fn encode(&self, dst: &mut BytesMut) {
        match self {
            FrontendMessage::Startup(startup) => coding::encode_initial(dst, |dst| {
                dst.put_i16(startup.version.major);
                dst.put_i16(startup.version.minor);
                for (key, value) in &startup.parameters {
                    coding::encode_cstring(dst, key);
                    coding::encode_cstring(dst, value);
                }
                dst.put_u8(0); // trailing null terminator
            }),

            FrontendMessage::SslRequest => coding::encode_initial(dst, |dst| {
                dst.put_i32(SSL_REQUEST_CODE);
            }),

            FrontendMessage::GssEncRequest => coding::encode_initial(dst, |dst| {
                dst.put_i32(GSS_ENC_REQUEST_CODE);
            }),

            FrontendMessage::CancelRequest {
                process_id,
                secret_key,
            } => coding::encode_initial(dst, |dst| {
                dst.put_i32(CANCEL_REQUEST_CODE);
                dst.put_i32(*process_id);
                dst.put_i32(*secret_key);
            }),

            FrontendMessage::Query(query) => coding::encode_message(dst, b'Q', |dst| {
                coding::encode_cstring(dst, query);
            }),

            FrontendMessage::Parse(p) => coding::encode_message(dst, b'P', |dst| {
                coding::encode_cstring(dst, &p.name);
                coding::encode_cstring(dst, &p.query);
                coding::encode_count_prefixed(dst, &p.param_types, |dst, oid| {
                    dst.put_i32(*oid as i32);
                });
            }),

            FrontendMessage::Bind(b) => coding::encode_message(dst, b'B', |dst| {
                coding::encode_cstring(dst, &b.portal);
                coding::encode_cstring(dst, &b.statement);
                coding::encode_count_prefixed(dst, &b.param_formats, |dst, fc| {
                    dst.put_i16(*fc as i16);
                });
                coding::encode_count_prefixed(dst, &b.params, |dst, param| match param {
                    None => dst.put_i32(-1),
                    Some(data) => {
                        dst.put_i32(data.len() as i32);
                        dst.extend_from_slice(data);
                    }
                });
                coding::encode_count_prefixed(dst, &b.result_formats, |dst, fc| {
                    dst.put_i16(*fc as i16);
                });
            }),

            FrontendMessage::Describe(d) => coding::encode_message(dst, b'D', |dst| {
                dst.put_u8(encode_target_kind(d.kind));
                coding::encode_cstring(dst, &d.name);
            }),

            FrontendMessage::Execute(e) => coding::encode_message(dst, b'E', |dst| {
                coding::encode_cstring(dst, &e.portal);
                dst.put_i32(e.max_rows);
            }),

            FrontendMessage::Close(c) => coding::encode_message(dst, b'C', |dst| {
                dst.put_u8(encode_target_kind(c.kind));
                coding::encode_cstring(dst, &c.name);
            }),

            FrontendMessage::Sync => coding::encode_message(dst, b'S', |_| {}),
            FrontendMessage::Flush => coding::encode_message(dst, b'H', |_| {}),
            FrontendMessage::Terminate => coding::encode_message(dst, b'X', |_| {}),
            FrontendMessage::CopyDone => coding::encode_message(dst, b'c', |_| {}),

            FrontendMessage::CopyData(data) => coding::encode_message(dst, b'd', |dst| {
                dst.extend_from_slice(data);
            }),

            FrontendMessage::CopyFail(msg) => coding::encode_message(dst, b'f', |dst| {
                coding::encode_cstring(dst, msg);
            }),

            FrontendMessage::FunctionCall(data) => coding::encode_message(dst, b'F', |dst| {
                dst.extend_from_slice(data);
            }),

            FrontendMessage::PasswordMessage(data) => coding::encode_message(dst, b'p', |dst| {
                dst.extend_from_slice(data);
            }),
        }
    }
}

fn encode_target_kind(kind: TargetKind) -> u8 {
    match kind {
        TargetKind::Statement => b'S',
        TargetKind::Portal => b'P',
    }
}
