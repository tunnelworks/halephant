#![allow(clippy::unwrap_used)]
use bytes::BytesMut;
use halephant_core::errors::ProtocolError;
use halephant_core::proto::backend::*;
use halephant_core::proto::types;

/// Encode a backend message, then re-parse it and assert equality.
fn round_trip(msg: &BackendMessage) {
    let mut buf = BytesMut::new();
    msg.encode(&mut buf);

    let msg_type = buf[0];
    let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let payload = &buf[5..=length];
    let decoded = BackendMessage::parse(msg_type, payload).expect("parse failed");
    assert_eq!(msg, &decoded, "round-trip failed");
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

#[test]
fn authentication_ok() {
    round_trip(&BackendMessage::AuthenticationOk);
}

#[test]
fn authentication_cleartext() {
    round_trip(&BackendMessage::AuthenticationCleartextPassword);
}

#[test]
fn authentication_md5() {
    round_trip(&BackendMessage::AuthenticationMd5Password {
        salt: [0xDE, 0xAD, 0xBE, 0xEF],
    });
}

#[test]
fn authentication_sasl() {
    round_trip(&BackendMessage::AuthenticationSasl {
        mechanisms: vec!["SCRAM-SHA-256".into()],
    });
}

#[test]
fn authentication_sasl_continue() {
    round_trip(&BackendMessage::AuthenticationSaslContinue {
        data: b"server-first-message".to_vec(),
    });
}

#[test]
fn authentication_sasl_final() {
    round_trip(&BackendMessage::AuthenticationSaslFinal {
        data: b"server-final-message".to_vec(),
    });
}

// ---------------------------------------------------------------------------
// Key exchange and session state
// ---------------------------------------------------------------------------

#[test]
fn backend_key_data() {
    round_trip(&BackendMessage::BackendKeyData {
        process_id: 1234,
        secret_key: 5678,
    });
}

#[test]
fn parameter_status() {
    round_trip(&BackendMessage::ParameterStatus {
        name: "server_version".into(),
        value: "16.2".into(),
    });
}

#[test]
fn ready_for_query_idle() {
    round_trip(&BackendMessage::ReadyForQuery(
        types::TransactionStatus::Idle,
    ));
}

#[test]
fn ready_for_query_in_transaction() {
    round_trip(&BackendMessage::ReadyForQuery(
        types::TransactionStatus::InTransaction,
    ));
}

#[test]
fn ready_for_query_failed() {
    round_trip(&BackendMessage::ReadyForQuery(
        types::TransactionStatus::Failed,
    ));
}

// ---------------------------------------------------------------------------
// Command lifecycle
// ---------------------------------------------------------------------------

#[test]
fn parse_complete() {
    round_trip(&BackendMessage::ParseComplete);
}

#[test]
fn bind_complete() {
    round_trip(&BackendMessage::BindComplete);
}

#[test]
fn close_complete() {
    round_trip(&BackendMessage::CloseComplete);
}

#[test]
fn no_data() {
    round_trip(&BackendMessage::NoData);
}

#[test]
fn portal_suspended() {
    round_trip(&BackendMessage::PortalSuspended);
}

#[test]
fn empty_query_response() {
    round_trip(&BackendMessage::EmptyQueryResponse);
}

#[test]
fn command_complete() {
    round_trip(&BackendMessage::CommandComplete("SELECT 1".into()));
}

#[test]
fn command_complete_insert() {
    round_trip(&BackendMessage::CommandComplete("INSERT 0 5".into()));
}

// ---------------------------------------------------------------------------
// Query results
// ---------------------------------------------------------------------------

#[test]
fn row_description() {
    round_trip(&BackendMessage::RowDescription(vec![
        FieldDescription {
            name: "id".into(),
            table_oid: 16384,
            column_id: 1,
            type_oid: 23, // int4
            type_size: 4,
            type_modifier: -1,
            format: types::FormatCode::Text,
        },
        FieldDescription {
            name: "name".into(),
            table_oid: 16384,
            column_id: 2,
            type_oid: 25, // text
            type_size: -1,
            type_modifier: -1,
            format: types::FormatCode::Text,
        },
    ]));
}

#[test]
fn data_row() {
    round_trip(&BackendMessage::DataRow(vec![
        Some(b"42".to_vec()),
        Some(b"hello".to_vec()),
        None,
    ]));
}

#[test]
fn data_row_empty() {
    round_trip(&BackendMessage::DataRow(vec![]));
}

#[test]
fn parameter_description() {
    round_trip(&BackendMessage::ParameterDescription(vec![23, 25]));
}

// ---------------------------------------------------------------------------
// COPY
// ---------------------------------------------------------------------------

#[test]
fn copy_in_response() {
    round_trip(&BackendMessage::CopyInResponse(CopyResponse {
        format: types::FormatCode::Text,
        column_formats: vec![types::FormatCode::Text, types::FormatCode::Text],
    }));
}

#[test]
fn copy_out_response() {
    round_trip(&BackendMessage::CopyOutResponse(CopyResponse {
        format: types::FormatCode::Binary,
        column_formats: vec![types::FormatCode::Binary],
    }));
}

#[test]
fn copy_both_response() {
    round_trip(&BackendMessage::CopyBothResponse(CopyResponse {
        format: types::FormatCode::Text,
        column_formats: vec![],
    }));
}

#[test]
fn copy_data() {
    round_trip(&BackendMessage::CopyData(b"some\tdata\n".to_vec()));
}

#[test]
fn copy_done() {
    round_trip(&BackendMessage::CopyDone);
}

// ---------------------------------------------------------------------------
// Errors and notices
// ---------------------------------------------------------------------------

#[test]
fn error_response() {
    let msg = BackendMessage::ErrorResponse(NoticeFields {
        fields: vec![
            (b'S', "ERROR".into()),
            (b'C', "42P01".into()),
            (b'M', "relation \"foo\" does not exist".into()),
        ],
    });
    round_trip(&msg);

    // Also test the accessor methods
    if let BackendMessage::ErrorResponse(ref fields) = msg {
        assert_eq!(fields.severity(), Some("ERROR"));
        assert_eq!(fields.code(), Some("42P01"));
        assert_eq!(fields.message(), Some("relation \"foo\" does not exist"));
        assert_eq!(fields.detail(), None);
        assert_eq!(fields.hint(), None);
    }
}

#[test]
fn notice_response() {
    round_trip(&BackendMessage::NoticeResponse(NoticeFields {
        fields: vec![
            (b'S', "WARNING".into()),
            (b'C', "01000".into()),
            (b'M', "some warning".into()),
        ],
    }));
}

// ---------------------------------------------------------------------------
// Async notifications
// ---------------------------------------------------------------------------

#[test]
fn notification_response() {
    round_trip(&BackendMessage::NotificationResponse(Notification {
        process_id: 9999,
        channel: "my_channel".into(),
        payload: "event data".into(),
    }));
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[test]
fn unknown_message_type() {
    let err = BackendMessage::parse(0xFF, &[]).unwrap_err();
    assert!(matches!(err, ProtocolError::UnknownMessageType(0xFF)));
}

#[test]
fn unknown_auth_type() {
    // auth_type = 99
    let payload = 99i32.to_be_bytes();
    let err = BackendMessage::parse(b'R', &payload).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidValue { .. }));
}

#[test]
fn truncated_ready_for_query() {
    let err = BackendMessage::parse(b'Z', &[]).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidValue { .. }));
}

#[test]
fn invalid_transaction_status() {
    let err = BackendMessage::parse(b'Z', b"X").unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidValue { .. }));
}
