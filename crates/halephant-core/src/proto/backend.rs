use bytes::{BufMut, BytesMut};

use crate::errors;
use crate::proto::coding;
use crate::proto::cursor::{self, Cursor};
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

    // -- Function call --
    FunctionCallResponse(Vec<u8>),

    // -- Protocol negotiation --
    NegotiateProtocolVersion(Vec<u8>),
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
// Decoding
// ---------------------------------------------------------------------------

fn decode_authentication(cur: &mut Cursor<'_>) -> Result<BackendMessage, errors::ProtocolError> {
    // The authentication type field starts at payload offset zero, so
    // the unknown-type error reports position 0.
    let auth_type = cur.read_i32()?;
    match auth_type {
        0 => Ok(BackendMessage::AuthenticationOk),
        3 => Ok(BackendMessage::AuthenticationCleartextPassword),
        5 => {
            let salt = [
                cur.read_u8()?,
                cur.read_u8()?,
                cur.read_u8()?,
                cur.read_u8()?,
            ];
            Ok(BackendMessage::AuthenticationMd5Password { salt })
        }
        10 => {
            let mut mechanisms = Vec::new();
            loop {
                let mech = cur.read_cstring()?;
                if mech.is_empty() {
                    break;
                }
                mechanisms.push(mech);
            }
            Ok(BackendMessage::AuthenticationSasl { mechanisms })
        }
        11 => Ok(BackendMessage::AuthenticationSaslContinue {
            data: cur.take_rest().to_vec(),
        }),
        12 => Ok(BackendMessage::AuthenticationSaslFinal {
            data: cur.take_rest().to_vec(),
        }),
        _ => Err(errors::ProtocolError::InvalidValue {
            position: 0,
            message: format!("unknown authentication type: {auth_type}"),
        }),
    }
}

fn decode_backend_key_data(cur: &mut Cursor<'_>) -> Result<BackendMessage, errors::ProtocolError> {
    let process_id = cur.read_i32()?;
    let secret_key = cur.read_i32()?;
    Ok(BackendMessage::BackendKeyData {
        process_id,
        secret_key,
    })
}

fn decode_command_complete(cur: &mut Cursor<'_>) -> Result<BackendMessage, errors::ProtocolError> {
    Ok(BackendMessage::CommandComplete(cur.read_cstring()?))
}

fn decode_data_row(cur: &mut Cursor<'_>) -> Result<BackendMessage, errors::ProtocolError> {
    let columns = cur.read_count_prefixed(Cursor::read_nullable_bytes)?;
    Ok(BackendMessage::DataRow(columns))
}

fn decode_row_description(cur: &mut Cursor<'_>) -> Result<BackendMessage, errors::ProtocolError> {
    let fields = cur.read_count_prefixed(|cur| {
        let name = cur.read_cstring()?;
        let table_oid = cur.read_i32()? as u32;
        let column_id = cur.read_i16()?;
        let type_oid = cur.read_i32()? as u32;
        let type_size = cur.read_i16()?;
        let type_modifier = cur.read_i32()?;
        let format = cur.read_format_code()?;
        Ok(FieldDescription {
            name,
            table_oid,
            column_id,
            type_oid,
            type_size,
            type_modifier,
            format,
        })
    })?;
    Ok(BackendMessage::RowDescription(fields))
}

fn decode_parameter_status(cur: &mut Cursor<'_>) -> Result<BackendMessage, errors::ProtocolError> {
    let name = cur.read_cstring()?;
    let value = cur.read_cstring()?;
    Ok(BackendMessage::ParameterStatus { name, value })
}

fn decode_parameter_description(
    cur: &mut Cursor<'_>,
) -> Result<BackendMessage, errors::ProtocolError> {
    let oids = cur.read_count_prefixed(|cur| Ok(cur.read_i32()? as u32))?;
    Ok(BackendMessage::ParameterDescription(oids))
}

fn decode_ready_for_query(cur: &mut Cursor<'_>) -> Result<BackendMessage, errors::ProtocolError> {
    let start = cur.position();
    let status = match cur.read_u8()? {
        b'I' => types::TransactionStatus::Idle,
        b'T' => types::TransactionStatus::InTransaction,
        b'E' => types::TransactionStatus::Failed,
        b => {
            return Err(errors::ProtocolError::InvalidValue {
                position: start,
                message: format!("unknown transaction status: {b:#04x}"),
            });
        }
    };
    Ok(BackendMessage::ReadyForQuery(status))
}

fn decode_notification(cur: &mut Cursor<'_>) -> Result<BackendMessage, errors::ProtocolError> {
    let process_id = cur.read_i32()?;
    let channel = cur.read_cstring()?;
    let payload = cur.read_cstring()?;
    Ok(BackendMessage::NotificationResponse(Notification {
        process_id,
        channel,
        payload,
    }))
}

fn decode_notice_fields(cur: &mut Cursor<'_>) -> Result<NoticeFields, errors::ProtocolError> {
    let mut fields = Vec::new();
    loop {
        let tag = cur.read_u8()?;
        if tag == 0 {
            break;
        }
        let value = cur.read_cstring()?;
        fields.push((tag, value));
    }
    Ok(NoticeFields { fields })
}

fn decode_copy_response(cur: &mut Cursor<'_>) -> Result<CopyResponse, errors::ProtocolError> {
    // The overall format is a single raw byte (0 or 1), unlike the
    // per-column format codes, which are 16-bit integers.
    let start = cur.position();
    let format = match cur.read_u8()? {
        0 => types::FormatCode::Text,
        1 => types::FormatCode::Binary,
        b => {
            return Err(errors::ProtocolError::InvalidValue {
                position: start,
                message: format!("invalid overall format code: {b}"),
            });
        }
    };
    let column_formats = cur.read_count_prefixed(Cursor::read_format_code)?;
    Ok(CopyResponse {
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
            b'A' => cursor::run(payload, decode_notification),
            b'C' => cursor::run(payload, decode_command_complete),
            b'c' => Ok(BackendMessage::CopyDone),
            b'd' => Ok(BackendMessage::CopyData(payload.to_vec())),
            b'D' => cursor::run(payload, decode_data_row),
            b'E' => cursor::run(payload, |cur| {
                decode_notice_fields(cur).map(BackendMessage::ErrorResponse)
            }),
            b'G' => cursor::run(payload, |cur| {
                decode_copy_response(cur).map(BackendMessage::CopyInResponse)
            }),
            b'H' => cursor::run(payload, |cur| {
                decode_copy_response(cur).map(BackendMessage::CopyOutResponse)
            }),
            b'I' => Ok(BackendMessage::EmptyQueryResponse),
            b'K' => cursor::run(payload, decode_backend_key_data),
            b'n' => Ok(BackendMessage::NoData),
            b'N' => cursor::run(payload, |cur| {
                decode_notice_fields(cur).map(BackendMessage::NoticeResponse)
            }),
            b'R' => cursor::run(payload, decode_authentication),
            b's' => Ok(BackendMessage::PortalSuspended),
            b'S' => cursor::run(payload, decode_parameter_status),
            b't' => cursor::run(payload, decode_parameter_description),
            b'T' => cursor::run(payload, decode_row_description),
            b'v' => Ok(BackendMessage::NegotiateProtocolVersion(payload.to_vec())),
            b'V' => Ok(BackendMessage::FunctionCallResponse(payload.to_vec())),
            b'W' => cursor::run(payload, |cur| {
                decode_copy_response(cur).map(BackendMessage::CopyBothResponse)
            }),
            b'Z' => cursor::run(payload, decode_ready_for_query),
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

            BackendMessage::FunctionCallResponse(data) => {
                coding::encode_message(dst, b'V', |dst| {
                    dst.extend_from_slice(data);
                });
            }

            BackendMessage::NegotiateProtocolVersion(data) => {
                coding::encode_message(dst, b'v', |dst| {
                    dst.extend_from_slice(data);
                });
            }
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
