#![allow(clippy::unwrap_used)]
use bytes::BytesMut;
use halephant_core::errors::ProtocolError;
use halephant_core::proto::frontend::*;
use halephant_core::proto::types;

/// Encode a message, then re-parse it and assert equality.
fn round_trip(msg: &FrontendMessage) {
    let mut buf = BytesMut::new();
    msg.encode(&mut buf);
    let decoded = decode_from_wire(&buf, msg);
    assert_eq!(msg, &decoded, "round-trip failed");
}

/// Decode a frontend message from its full wire encoding.
/// Initial messages (no type byte) need `parse_initial`; regular messages
/// have a type byte.
fn decode_from_wire(buf: &[u8], original: &FrontendMessage) -> FrontendMessage {
    match original {
        FrontendMessage::Startup(_)
        | FrontendMessage::SslRequest
        | FrontendMessage::GssEncRequest
        | FrontendMessage::CancelRequest { .. } => {
            FrontendMessage::parse_initial(buf).expect("parse_initial failed")
        }
        _ => {
            let msg_type = buf[0];
            let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            let payload = &buf[5..=length];
            FrontendMessage::parse(msg_type, payload).expect("parse failed")
        }
    }
}

// ---------------------------------------------------------------------------
// Round-trip tests
// ---------------------------------------------------------------------------

#[test]
fn startup_message() {
    round_trip(&FrontendMessage::Startup(Startup {
        version: ProtocolVersion { major: 3, minor: 0 },
        parameters: vec![
            ("user".into(), "postgres".into()),
            ("database".into(), "mydb".into()),
            ("client_encoding".into(), "UTF8".into()),
        ],
    }));
}

#[test]
fn ssl_request() {
    round_trip(&FrontendMessage::SslRequest);
}

#[test]
fn gss_enc_request() {
    round_trip(&FrontendMessage::GssEncRequest);
}

#[test]
fn cancel_request() {
    round_trip(&FrontendMessage::CancelRequest {
        process_id: 12345,
        secret_key: 67890,
    });
}

#[test]
fn query() {
    round_trip(&FrontendMessage::Query("SELECT 1".into()));
}

#[test]
fn query_empty() {
    round_trip(&FrontendMessage::Query(String::new()));
}

#[test]
fn parse_simple() {
    round_trip(&FrontendMessage::Parse(Parse {
        name: String::new(),
        query: "SELECT $1::int".into(),
        param_types: vec![23], // int4 oid
    }));
}

#[test]
fn parse_named_with_multiple_params() {
    round_trip(&FrontendMessage::Parse(Parse {
        name: "my_stmt".into(),
        query: "INSERT INTO t VALUES ($1, $2)".into(),
        param_types: vec![25, 23], // text, int4
    }));
}

#[test]
fn bind_with_values() {
    round_trip(&FrontendMessage::Bind(Bind {
        portal: String::new(),
        statement: "my_stmt".into(),
        param_formats: vec![types::FormatCode::Text],
        params: vec![Some(b"hello".to_vec()), None, Some(b"42".to_vec())],
        result_formats: vec![types::FormatCode::Text, types::FormatCode::Binary],
    }));
}

#[test]
fn bind_no_params() {
    round_trip(&FrontendMessage::Bind(Bind {
        portal: String::new(),
        statement: String::new(),
        param_formats: vec![],
        params: vec![],
        result_formats: vec![],
    }));
}

#[test]
fn describe_statement() {
    round_trip(&FrontendMessage::Describe(Describe {
        kind: TargetKind::Statement,
        name: "my_stmt".into(),
    }));
}

#[test]
fn describe_portal() {
    round_trip(&FrontendMessage::Describe(Describe {
        kind: TargetKind::Portal,
        name: String::new(),
    }));
}

#[test]
fn execute() {
    round_trip(&FrontendMessage::Execute(Execute {
        portal: String::new(),
        max_rows: 0,
    }));
}

#[test]
fn execute_with_limit() {
    round_trip(&FrontendMessage::Execute(Execute {
        portal: "my_portal".into(),
        max_rows: 100,
    }));
}

#[test]
fn close_statement() {
    round_trip(&FrontendMessage::Close(Close {
        kind: TargetKind::Statement,
        name: "my_stmt".into(),
    }));
}

#[test]
fn sync() {
    round_trip(&FrontendMessage::Sync);
}

#[test]
fn flush() {
    round_trip(&FrontendMessage::Flush);
}

#[test]
fn terminate() {
    round_trip(&FrontendMessage::Terminate);
}

#[test]
fn copy_data() {
    round_trip(&FrontendMessage::CopyData(b"row1\tval1\n".to_vec()));
}

#[test]
fn copy_done() {
    round_trip(&FrontendMessage::CopyDone);
}

#[test]
fn copy_fail() {
    round_trip(&FrontendMessage::CopyFail("something went wrong".into()));
}

#[test]
fn password_message() {
    round_trip(&FrontendMessage::PasswordMessage(b"secret".to_vec()));
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[test]
fn unknown_message_type() {
    let err = FrontendMessage::parse(0xFF, &[]).unwrap_err();
    assert!(
        matches!(err, ProtocolError::UnknownMessageType(0xFF)),
        "expected UnknownMessageType, got: {err}"
    );
}

#[test]
fn unknown_initial_code() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&8i32.to_be_bytes()); // length
    buf.extend_from_slice(&99999i32.to_be_bytes()); // bogus code
    let err = FrontendMessage::parse_initial(&buf).unwrap_err();
    assert!(
        matches!(err, ProtocolError::InvalidValue { .. }),
        "expected InvalidValue, got: {err}"
    );
}

#[test]
fn invalid_target_kind() {
    // Describe with invalid kind byte 'Z'
    let payload = b"Zmy_stmt\0";
    let err = FrontendMessage::parse(b'D', payload).unwrap_err();
    assert!(
        matches!(err, ProtocolError::InvalidValue { .. }),
        "expected InvalidValue, got: {err}"
    );
}
