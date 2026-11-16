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

#[test]
fn function_call() {
    // Function oid 42, no argument formats, no arguments, text result format.
    let mut payload = 42i32.to_be_bytes().to_vec();
    payload.extend_from_slice(&0i16.to_be_bytes());
    payload.extend_from_slice(&0i16.to_be_bytes());
    payload.extend_from_slice(&0i16.to_be_bytes());
    round_trip(&FrontendMessage::FunctionCall(payload));
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

// ---------------------------------------------------------------------------
// Hostile input: counts and lengths from the wire must never reach an
// allocation unchecked (robustness review H1).
// ---------------------------------------------------------------------------

#[test]
fn bind_negative_param_format_count() {
    // Bind with a format-code count of 0xFFFF (-1 as i16). An unguarded
    // `as usize` cast turns it into usize::MAX and panics on
    // `Vec::with_capacity`; the parser must return an error instead.
    let mut payload = Vec::new();
    payload.extend_from_slice(b"\0\0"); // empty portal, empty statement
    payload.extend_from_slice(&[0xFF, 0xFF]); // param format count -1
    let err = FrontendMessage::parse(b'B', &payload).unwrap_err();
    assert!(
        matches!(err, ProtocolError::InvalidValue { .. }),
        "expected InvalidValue, got: {err}"
    );
}

#[test]
fn parse_negative_param_type_count() {
    // Parse with a param-type count of 0xFFFF (-1 as i16).
    let mut payload = Vec::new();
    payload.extend_from_slice(b"\0SELECT 1\0"); // unnamed statement, query
    payload.extend_from_slice(&[0xFF, 0xFF]); // param type count -1
    let err = FrontendMessage::parse(b'P', &payload).unwrap_err();
    assert!(
        matches!(err, ProtocolError::InvalidValue { .. }),
        "expected InvalidValue, got: {err}"
    );
}

#[test]
fn bind_param_length_exceeds_payload() {
    // A ~10-byte Bind declaring a ~2 GiB parameter length. The declared
    // length must be rejected against the remaining payload before any
    // allocation happens, not discovered later by running out of input.
    let mut payload = Vec::new();
    payload.extend_from_slice(b"\0\0"); // empty portal, empty statement
    payload.extend_from_slice(&0i16.to_be_bytes()); // no param formats
    payload.extend_from_slice(&1i16.to_be_bytes()); // one parameter
    payload.extend_from_slice(&i32::MAX.to_be_bytes()); // declared length ~2 GiB
    let err = FrontendMessage::parse(b'B', &payload).unwrap_err();
    assert!(
        matches!(err, ProtocolError::InvalidValue { .. }) && err.to_string().contains("exceeds"),
        "expected a declared-length guard error, got: {err}"
    );
}

#[test]
fn bind_param_count_exceeds_payload() {
    // A parameter count far larger than the bytes behind it. Every item
    // consumes input, so the count must be rejected against the remaining
    // payload before reserving.
    let mut payload = Vec::new();
    payload.extend_from_slice(b"\0\0"); // empty portal, empty statement
    payload.extend_from_slice(&0i16.to_be_bytes()); // no param formats
    payload.extend_from_slice(&i16::MAX.to_be_bytes()); // 32767 parameters
    let err = FrontendMessage::parse(b'B', &payload).unwrap_err();
    assert!(
        matches!(err, ProtocolError::InvalidValue { .. }) && err.to_string().contains("exceeds"),
        "expected a declared-count guard error, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Hostile input: truncated integers and cstrings
// ---------------------------------------------------------------------------

#[test]
fn execute_truncated_max_rows() {
    // Execute with only two of the four max_rows bytes.
    let err = FrontendMessage::parse(b'E', b"\0\x00\x01").unwrap_err();
    assert!(
        matches!(err, ProtocolError::InvalidValue { .. }),
        "expected InvalidValue, got: {err}"
    );
}

#[test]
fn query_unterminated_cstring() {
    // Query whose SQL text is missing the NUL terminator.
    let err = FrontendMessage::parse(b'Q', b"SELECT 1").unwrap_err();
    assert!(
        matches!(err, ProtocolError::InvalidValue { .. }),
        "expected InvalidValue, got: {err}"
    );
}

#[test]
fn bind_truncated_param_bytes() {
    // Bind declaring a five-byte parameter but delivering only three bytes.
    let mut payload = Vec::new();
    payload.extend_from_slice(b"\0\0"); // empty portal, empty statement
    payload.extend_from_slice(&0i16.to_be_bytes()); // no param formats
    payload.extend_from_slice(&1i16.to_be_bytes()); // one parameter
    payload.extend_from_slice(&5i32.to_be_bytes()); // declared length 5
    payload.extend_from_slice(b"abc"); // only 3 bytes present
    let err = FrontendMessage::parse(b'B', &payload).unwrap_err();
    assert!(
        matches!(err, ProtocolError::InvalidValue { .. }),
        "expected InvalidValue, got: {err}"
    );
}
