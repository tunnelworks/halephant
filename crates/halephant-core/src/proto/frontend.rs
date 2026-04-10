use bytes::{BufMut, BytesMut};
use chumsky::prelude::*;
use chumsky::span::SimpleSpan;

use crate::errors;
use crate::proto::coding;
use crate::proto::parser;
use crate::proto::primitives;
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
// Chumsky parsers
// ---------------------------------------------------------------------------

fn target_kind<'src>() -> impl Parser<'src, &'src [u8], TargetKind, parser::PgExtra<'src>> + Clone {
    any().try_map(|b, span| match b {
        b'S' => Ok(TargetKind::Statement),
        b'P' => Ok(TargetKind::Portal),
        _ => Err(Rich::custom(
            span,
            format!("expected 'S' or 'P' for target kind, got {b:#04x}"),
        )),
    })
}

fn startup_body<'src>() -> impl Parser<'src, &'src [u8], FrontendMessage, parser::PgExtra<'src>> {
    custom(|inp| {
        let mut parameters = Vec::new();
        loop {
            let key: String = inp.parse(primitives::pg_cstring())?;
            if key.is_empty() {
                break;
            }
            let value: String = inp.parse(primitives::pg_cstring())?;
            parameters.push((key, value));
        }
        Ok(FrontendMessage::Startup(Startup {
            version: ProtocolVersion { major: 3, minor: 0 },
            parameters,
        }))
    })
}

fn query_parser<'src>() -> impl Parser<'src, &'src [u8], FrontendMessage, parser::PgExtra<'src>> {
    primitives::pg_cstring().map(FrontendMessage::Query)
}

fn parse_parser<'src>() -> impl Parser<'src, &'src [u8], FrontendMessage, parser::PgExtra<'src>> {
    custom(|inp| {
        let name = inp.parse(primitives::pg_cstring())?;
        let query = inp.parse(primitives::pg_cstring())?;
        let param_types = inp.parse(primitives::pg_count_prefixed(
            primitives::pg_int32().map(|v| v as u32),
        ))?;
        Ok(FrontendMessage::Parse(Parse {
            name,
            query,
            param_types,
        }))
    })
}

fn bind_parser<'src>() -> impl Parser<'src, &'src [u8], FrontendMessage, parser::PgExtra<'src>> {
    custom(|inp| {
        let portal = inp.parse(primitives::pg_cstring())?;
        let statement = inp.parse(primitives::pg_cstring())?;
        let param_formats =
            inp.parse(primitives::pg_count_prefixed(primitives::pg_format_code()))?;
        let params = inp.parse(primitives::pg_count_prefixed(
            primitives::pg_nullable_bytes(),
        ))?;
        let result_formats =
            inp.parse(primitives::pg_count_prefixed(primitives::pg_format_code()))?;
        Ok(FrontendMessage::Bind(Bind {
            portal,
            statement,
            param_formats,
            params,
            result_formats,
        }))
    })
}

fn describe_parser<'src>() -> impl Parser<'src, &'src [u8], FrontendMessage, parser::PgExtra<'src>>
{
    target_kind()
        .then(primitives::pg_cstring())
        .map(|(kind, name)| FrontendMessage::Describe(Describe { kind, name }))
}

fn execute_parser<'src>() -> impl Parser<'src, &'src [u8], FrontendMessage, parser::PgExtra<'src>> {
    primitives::pg_cstring()
        .then(primitives::pg_int32())
        .map(|(portal, max_rows)| FrontendMessage::Execute(Execute { portal, max_rows }))
}

fn close_parser<'src>() -> impl Parser<'src, &'src [u8], FrontendMessage, parser::PgExtra<'src>> {
    target_kind()
        .then(primitives::pg_cstring())
        .map(|(kind, name)| FrontendMessage::Close(Close { kind, name }))
}

fn copy_fail_parser<'src>() -> impl Parser<'src, &'src [u8], FrontendMessage, parser::PgExtra<'src>>
{
    primitives::pg_cstring().map(FrontendMessage::CopyFail)
}

// ---------------------------------------------------------------------------
// Public parse API
// ---------------------------------------------------------------------------

impl FrontendMessage {
    /// Parse an initial message (startup / SSL / cancel / GSS-enc).
    ///
    /// The input is the *complete* message including the leading 4-byte length.
    pub fn parse_initial(input: &[u8]) -> Result<Self, errors::ProtocolError> {
        let parser = custom(|inp| {
            let _length: i32 = inp.parse(primitives::pg_int32())?;
            let code: i32 = inp.parse(primitives::pg_int32())?;
            match code {
                PROTOCOL_V3 => inp.parse(startup_body()),
                SSL_REQUEST_CODE => Ok(FrontendMessage::SslRequest),
                CANCEL_REQUEST_CODE => {
                    let process_id = inp.parse(primitives::pg_int32())?;
                    let secret_key = inp.parse(primitives::pg_int32())?;
                    Ok(FrontendMessage::CancelRequest {
                        process_id,
                        secret_key,
                    })
                }
                GSS_ENC_REQUEST_CODE => Ok(FrontendMessage::GssEncRequest),
                _ => Err(Rich::custom(
                    SimpleSpan::new(4, 8),
                    format!("unknown initial message code: {code}"),
                )),
            }
        });
        parser::run(parser, input)
    }

    /// Parse a regular (non-initial) message.
    ///
    /// `msg_type` is the leading type byte; `payload` is everything after the
    /// 4-byte length field (i.e. the length-field's value minus 4).
    pub fn parse(msg_type: u8, payload: &[u8]) -> Result<Self, errors::ProtocolError> {
        match msg_type {
            b'B' => parser::run(bind_parser(), payload),
            b'C' => parser::run(close_parser(), payload),
            b'c' => Ok(FrontendMessage::CopyDone),
            b'd' => Ok(FrontendMessage::CopyData(payload.to_vec())),
            b'D' => parser::run(describe_parser(), payload),
            b'E' => parser::run(execute_parser(), payload),
            b'f' => parser::run(copy_fail_parser(), payload),
            b'F' => Ok(FrontendMessage::FunctionCall(payload.to_vec())),
            b'H' => Ok(FrontendMessage::Flush),
            b'P' => parser::run(parse_parser(), payload),
            b'p' => Ok(FrontendMessage::PasswordMessage(payload.to_vec())),
            b'Q' => parser::run(query_parser(), payload),
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
