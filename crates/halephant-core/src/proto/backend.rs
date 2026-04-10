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

/// A message sent by a PostgreSQL backend (server → client).
#[derive(Debug, Clone, PartialEq)]
pub enum BackendMessage {
    // -- Authentication --
    AuthenticationOk,
    AuthenticationCleartextPassword,
    AuthenticationMd5Password { salt: [u8; 4] },
    AuthenticationSasl { mechanisms: Vec<String> },
    AuthenticationSaslContinue { data: Vec<u8> },
    AuthenticationSaslFinal { data: Vec<u8> },

    // -- Key exchange --
    BackendKeyData { process_id: i32, secret_key: i32 },

    // -- Command lifecycle --
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    PortalSuspended,
    EmptyQueryResponse,
    CommandComplete(String),

    // -- Query results --
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<Vec<u8>>>),
    ParameterDescription(Vec<types::Oid>),

    // -- Session state --
    ParameterStatus { name: String, value: String },
    ReadyForQuery(types::TransactionStatus),

    // -- COPY --
    CopyInResponse(CopyResponse),
    CopyOutResponse(CopyResponse),
    CopyBothResponse(CopyResponse),
    CopyData(Vec<u8>),
    CopyDone,

    // -- Errors and notices --
    ErrorResponse(NoticeFields),
    NoticeResponse(NoticeFields),

    // -- Async notifications --
    NotificationResponse(Notification),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: types::Oid,
    pub column_id: i16,
    pub type_oid: types::Oid,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: types::FormatCode,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CopyResponse {
    pub format: types::FormatCode,
    pub column_formats: Vec<types::FormatCode>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Notification {
    pub process_id: i32,
    pub channel: String,
    pub payload: String,
}

/// Tagged field list used by `ErrorResponse` and `NoticeResponse`.
#[derive(Debug, Clone, PartialEq)]
pub struct NoticeFields {
    pub fields: Vec<(u8, String)>,
}

impl NoticeFields {
    pub fn severity(&self) -> Option<&str> {
        self.get(b'S')
    }
    pub fn code(&self) -> Option<&str> {
        self.get(b'C')
    }
    pub fn message(&self) -> Option<&str> {
        self.get(b'M')
    }
    pub fn detail(&self) -> Option<&str> {
        self.get(b'D')
    }
    pub fn hint(&self) -> Option<&str> {
        self.get(b'H')
    }

    fn get(&self, tag: u8) -> Option<&str> {
        self.fields
            .iter()
            .find(|(t, _)| *t == tag)
            .map(|(_, v)| v.as_str())
    }
}

// ---------------------------------------------------------------------------
// Chumsky parsers
// ---------------------------------------------------------------------------

fn authentication<'src>() -> impl Parser<'src, &'src [u8], BackendMessage, parser::PgExtra<'src>> {
    custom(|inp| {
        let auth_type: i32 = inp.parse(primitives::pg_int32())?;
        match auth_type {
            0 => Ok(BackendMessage::AuthenticationOk),
            3 => Ok(BackendMessage::AuthenticationCleartextPassword),
            5 => {
                let salt = [
                    inp.parse(any())?,
                    inp.parse(any())?,
                    inp.parse(any())?,
                    inp.parse(any())?,
                ];
                Ok(BackendMessage::AuthenticationMd5Password { salt })
            }
            10 => {
                let mut mechanisms = Vec::new();
                loop {
                    let mech: String = inp.parse(primitives::pg_cstring())?;
                    if mech.is_empty() {
                        break;
                    }
                    mechanisms.push(mech);
                }
                Ok(BackendMessage::AuthenticationSasl { mechanisms })
            }
            11 => {
                let data = inp.parse(any().repeated().collect::<Vec<u8>>())?;
                Ok(BackendMessage::AuthenticationSaslContinue { data })
            }
            12 => {
                let data = inp.parse(any().repeated().collect::<Vec<u8>>())?;
                Ok(BackendMessage::AuthenticationSaslFinal { data })
            }
            _ => Err(Rich::custom(
                SimpleSpan::new(0, 4),
                format!("unknown authentication type: {auth_type}"),
            )),
        }
    })
}

fn backend_key_data<'src>() -> impl Parser<'src, &'src [u8], BackendMessage, parser::PgExtra<'src>>
{
    primitives::pg_int32()
        .then(primitives::pg_int32())
        .map(|(process_id, secret_key)| BackendMessage::BackendKeyData {
            process_id,
            secret_key,
        })
}

fn command_complete<'src>() -> impl Parser<'src, &'src [u8], BackendMessage, parser::PgExtra<'src>>
{
    primitives::pg_cstring().map(BackendMessage::CommandComplete)
}

fn data_row<'src>() -> impl Parser<'src, &'src [u8], BackendMessage, parser::PgExtra<'src>> {
    primitives::pg_count_prefixed(primitives::pg_nullable_bytes()).map(BackendMessage::DataRow)
}

fn row_description<'src>() -> impl Parser<'src, &'src [u8], BackendMessage, parser::PgExtra<'src>> {
    primitives::pg_count_prefixed(custom(|inp| {
        let name = inp.parse(primitives::pg_cstring())?;
        let table_oid = inp.parse(primitives::pg_int32())? as u32;
        let column_id = inp.parse(primitives::pg_int16())?;
        let type_oid = inp.parse(primitives::pg_int32())? as u32;
        let type_size = inp.parse(primitives::pg_int16())?;
        let type_modifier = inp.parse(primitives::pg_int32())?;
        let format = inp.parse(primitives::pg_format_code())?;
        Ok(FieldDescription {
            name,
            table_oid,
            column_id,
            type_oid,
            type_size,
            type_modifier,
            format,
        })
    }))
    .map(BackendMessage::RowDescription)
}

fn parameter_status<'src>() -> impl Parser<'src, &'src [u8], BackendMessage, parser::PgExtra<'src>>
{
    primitives::pg_cstring()
        .then(primitives::pg_cstring())
        .map(|(name, value)| BackendMessage::ParameterStatus { name, value })
}

fn parameter_description<'src>()
-> impl Parser<'src, &'src [u8], BackendMessage, parser::PgExtra<'src>> {
    primitives::pg_count_prefixed(primitives::pg_int32().map(|v| v as u32))
        .map(BackendMessage::ParameterDescription)
}

fn ready_for_query<'src>() -> impl Parser<'src, &'src [u8], BackendMessage, parser::PgExtra<'src>> {
    any()
        .try_map(|b, span| match b {
            b'I' => Ok(types::TransactionStatus::Idle),
            b'T' => Ok(types::TransactionStatus::InTransaction),
            b'E' => Ok(types::TransactionStatus::Failed),
            _ => Err(Rich::custom(
                span,
                format!("unknown transaction status: {b:#04x}"),
            )),
        })
        .map(BackendMessage::ReadyForQuery)
}

fn notification<'src>() -> impl Parser<'src, &'src [u8], BackendMessage, parser::PgExtra<'src>> {
    primitives::pg_int32()
        .then(primitives::pg_cstring())
        .then(primitives::pg_cstring())
        .map(|((process_id, channel), payload)| {
            BackendMessage::NotificationResponse(Notification {
                process_id,
                channel,
                payload,
            })
        })
}

fn notice_fields<'src>() -> impl Parser<'src, &'src [u8], NoticeFields, parser::PgExtra<'src>> {
    custom(|inp| {
        let mut fields = Vec::new();
        loop {
            let tag: u8 = inp.parse(any())?;
            if tag == 0 {
                break;
            }
            let value: String = inp.parse(primitives::pg_cstring())?;
            fields.push((tag, value));
        }
        Ok(NoticeFields { fields })
    })
}

fn copy_response<'src>() -> impl Parser<'src, &'src [u8], CopyResponse, parser::PgExtra<'src>> {
    any()
        .try_map(|b, span| match b {
            0 => Ok(types::FormatCode::Text),
            1 => Ok(types::FormatCode::Binary),
            _ => Err(Rich::custom(
                span,
                format!("invalid overall format code: {b}"),
            )),
        })
        .then(primitives::pg_count_prefixed(primitives::pg_format_code()))
        .map(|(format, column_formats)| CopyResponse {
            format,
            column_formats,
        })
}

// ---------------------------------------------------------------------------
// Public parse API
// ---------------------------------------------------------------------------

impl BackendMessage {
    /// Parse a backend message.
    ///
    /// `msg_type` is the leading type byte; `payload` is everything after the
    /// 4-byte length field.
    pub fn parse(msg_type: u8, payload: &[u8]) -> Result<Self, errors::ProtocolError> {
        match msg_type {
            b'1' => Ok(BackendMessage::ParseComplete),
            b'2' => Ok(BackendMessage::BindComplete),
            b'3' => Ok(BackendMessage::CloseComplete),
            b'A' => parser::run(notification(), payload),
            b'C' => parser::run(command_complete(), payload),
            b'c' => Ok(BackendMessage::CopyDone),
            b'd' => Ok(BackendMessage::CopyData(payload.to_vec())),
            b'D' => parser::run(data_row(), payload),
            b'E' => parser::run(notice_fields().map(BackendMessage::ErrorResponse), payload),
            b'G' => parser::run(copy_response().map(BackendMessage::CopyInResponse), payload),
            b'H' => parser::run(
                copy_response().map(BackendMessage::CopyOutResponse),
                payload,
            ),
            b'I' => Ok(BackendMessage::EmptyQueryResponse),
            b'K' => parser::run(backend_key_data(), payload),
            b'n' => Ok(BackendMessage::NoData),
            b'N' => parser::run(notice_fields().map(BackendMessage::NoticeResponse), payload),
            b'R' => parser::run(authentication(), payload),
            b's' => Ok(BackendMessage::PortalSuspended),
            b'S' => parser::run(parameter_status(), payload),
            b't' => parser::run(parameter_description(), payload),
            b'T' => parser::run(row_description(), payload),
            b'W' => parser::run(
                copy_response().map(BackendMessage::CopyBothResponse),
                payload,
            ),
            b'Z' => parser::run(ready_for_query(), payload),
            _ => Err(errors::ProtocolError::UnknownMessageType(msg_type)),
        }
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

impl BackendMessage {
    /// Encode this message into `dst` in PostgreSQL wire format.
    pub fn encode(&self, dst: &mut BytesMut) {
        match self {
            BackendMessage::AuthenticationOk => coding::encode_message(dst, b'R', |dst| {
                dst.put_i32(0);
            }),
            BackendMessage::AuthenticationCleartextPassword => {
                coding::encode_message(dst, b'R', |dst| {
                    dst.put_i32(3);
                });
            }
            BackendMessage::AuthenticationMd5Password { salt } => {
                coding::encode_message(dst, b'R', |dst| {
                    dst.put_i32(5);
                    dst.extend_from_slice(salt);
                });
            }
            BackendMessage::AuthenticationSasl { mechanisms } => {
                coding::encode_message(dst, b'R', |dst| {
                    dst.put_i32(10);
                    for mech in mechanisms {
                        coding::encode_cstring(dst, mech);
                    }
                    dst.put_u8(0); // trailing null
                });
            }
            BackendMessage::AuthenticationSaslContinue { data } => {
                coding::encode_message(dst, b'R', |dst| {
                    dst.put_i32(11);
                    dst.extend_from_slice(data);
                });
            }
            BackendMessage::AuthenticationSaslFinal { data } => {
                coding::encode_message(dst, b'R', |dst| {
                    dst.put_i32(12);
                    dst.extend_from_slice(data);
                });
            }

            BackendMessage::BackendKeyData {
                process_id,
                secret_key,
            } => coding::encode_message(dst, b'K', |dst| {
                dst.put_i32(*process_id);
                dst.put_i32(*secret_key);
            }),

            BackendMessage::ParseComplete => coding::encode_message(dst, b'1', |_| {}),
            BackendMessage::BindComplete => coding::encode_message(dst, b'2', |_| {}),
            BackendMessage::CloseComplete => coding::encode_message(dst, b'3', |_| {}),
            BackendMessage::NoData => coding::encode_message(dst, b'n', |_| {}),
            BackendMessage::PortalSuspended => coding::encode_message(dst, b's', |_| {}),
            BackendMessage::EmptyQueryResponse => coding::encode_message(dst, b'I', |_| {}),
            BackendMessage::CopyDone => coding::encode_message(dst, b'c', |_| {}),

            BackendMessage::CommandComplete(tag) => coding::encode_message(dst, b'C', |dst| {
                coding::encode_cstring(dst, tag);
            }),

            BackendMessage::RowDescription(fields) => coding::encode_message(dst, b'T', |dst| {
                coding::encode_count_prefixed(dst, fields, |dst, f| {
                    coding::encode_cstring(dst, &f.name);
                    dst.put_i32(f.table_oid as i32);
                    dst.put_i16(f.column_id);
                    dst.put_i32(f.type_oid as i32);
                    dst.put_i16(f.type_size);
                    dst.put_i32(f.type_modifier);
                    dst.put_i16(f.format as i16);
                });
            }),

            BackendMessage::DataRow(columns) => coding::encode_message(dst, b'D', |dst| {
                coding::encode_count_prefixed(dst, columns, |dst, col| match col {
                    None => dst.put_i32(-1),
                    Some(data) => {
                        dst.put_i32(data.len() as i32);
                        dst.extend_from_slice(data);
                    }
                });
            }),

            BackendMessage::ParameterDescription(oids) => {
                coding::encode_message(dst, b't', |dst| {
                    coding::encode_count_prefixed(dst, oids, |dst, oid| {
                        dst.put_i32(*oid as i32);
                    });
                });
            }

            BackendMessage::ParameterStatus { name, value } => {
                coding::encode_message(dst, b'S', |dst| {
                    coding::encode_cstring(dst, name);
                    coding::encode_cstring(dst, value);
                });
            }

            BackendMessage::ReadyForQuery(status) => coding::encode_message(dst, b'Z', |dst| {
                dst.put_u8(match status {
                    types::TransactionStatus::Idle => b'I',
                    types::TransactionStatus::InTransaction => b'T',
                    types::TransactionStatus::Failed => b'E',
                });
            }),

            BackendMessage::CopyInResponse(cr) => coding::encode_message(dst, b'G', |dst| {
                encode_copy_response(dst, cr);
            }),
            BackendMessage::CopyOutResponse(cr) => coding::encode_message(dst, b'H', |dst| {
                encode_copy_response(dst, cr);
            }),
            BackendMessage::CopyBothResponse(cr) => coding::encode_message(dst, b'W', |dst| {
                encode_copy_response(dst, cr);
            }),

            BackendMessage::CopyData(data) => coding::encode_message(dst, b'd', |dst| {
                dst.extend_from_slice(data);
            }),

            BackendMessage::ErrorResponse(fields) => coding::encode_message(dst, b'E', |dst| {
                encode_notice_fields(dst, fields);
            }),
            BackendMessage::NoticeResponse(fields) => coding::encode_message(dst, b'N', |dst| {
                encode_notice_fields(dst, fields);
            }),

            BackendMessage::NotificationResponse(n) => coding::encode_message(dst, b'A', |dst| {
                dst.put_i32(n.process_id);
                coding::encode_cstring(dst, &n.channel);
                coding::encode_cstring(dst, &n.payload);
            }),
        }
    }
}

fn encode_notice_fields(dst: &mut BytesMut, fields: &NoticeFields) {
    for (tag, value) in &fields.fields {
        dst.put_u8(*tag);
        coding::encode_cstring(dst, value);
    }
    dst.put_u8(0); // terminator
}

fn encode_copy_response(dst: &mut BytesMut, cr: &CopyResponse) {
    dst.put_u8(cr.format as u8);
    coding::encode_count_prefixed(dst, &cr.column_formats, |dst, fc| {
        dst.put_i16(*fc as i16);
    });
}
