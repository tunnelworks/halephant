#![allow(clippy::unwrap_used)]
use bytes::BytesMut;
use tokio_util::codec::Decoder;

use halephant_core::proto::backend::BackendMessage;
use halephant_core::proto::codec::{BackendCodec, FrontendCodec};
use halephant_core::proto::frontend::*;
use halephant_core::proto::types;

// ---------------------------------------------------------------------------
// FrontendCodec — initial message framing
// ---------------------------------------------------------------------------

#[test]
fn frontend_codec_ssl_then_startup() {
    let mut codec = FrontendCodec::new();
    let mut buf = BytesMut::new();

    // Encode an SSLRequest
    FrontendMessage::SslRequest.encode(&mut buf);

    // Encode a StartupMessage
    FrontendMessage::Startup(Startup {
        version: ProtocolVersion { major: 3, minor: 0 },
        parameters: vec![("user".into(), "pg".into())],
    })
    .encode(&mut buf);

    // Decode SSLRequest
    let msg1 = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(msg1, FrontendMessage::SslRequest);

    // Decode StartupMessage
    let msg2 = codec.decode(&mut buf).unwrap().unwrap();
    assert!(matches!(msg2, FrontendMessage::Startup(_)));

    // Buffer is empty
    assert!(codec.decode(&mut buf).unwrap().is_none());
}

#[test]
fn frontend_codec_regular_messages() {
    let mut codec = FrontendCodec::new();
    let mut buf = BytesMut::new();

    // First send a startup to transition out of initial mode
    FrontendMessage::Startup(Startup {
        version: ProtocolVersion { major: 3, minor: 0 },
        parameters: vec![("user".into(), "pg".into())],
    })
    .encode(&mut buf);

    // Then send regular messages
    FrontendMessage::Query("SELECT 1".into()).encode(&mut buf);
    FrontendMessage::Sync.encode(&mut buf);
    FrontendMessage::Terminate.encode(&mut buf);

    // Decode all
    let _ = codec.decode(&mut buf).unwrap().unwrap(); // startup
    let msg2 = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(msg2, FrontendMessage::Query("SELECT 1".into()));
    let msg3 = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(msg3, FrontendMessage::Sync);
    let msg4 = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(msg4, FrontendMessage::Terminate);

    assert!(codec.decode(&mut buf).unwrap().is_none());
}

#[test]
fn frontend_codec_incremental_delivery() {
    let mut codec = FrontendCodec::new();

    // Build a full startup message
    let mut full = BytesMut::new();
    FrontendMessage::Startup(Startup {
        version: ProtocolVersion { major: 3, minor: 0 },
        parameters: vec![("user".into(), "pg".into())],
    })
    .encode(&mut full);

    // Feed one byte at a time
    let mut buf = BytesMut::new();
    for i in 0..full.len() {
        buf.extend_from_slice(&full[i..=i]);
        let result = codec.decode(&mut buf).unwrap();
        if i < full.len() - 1 {
            assert!(result.is_none(), "decoded too early at byte {i}");
        } else {
            assert!(result.is_some(), "should have decoded at final byte");
        }
    }
}

// ---------------------------------------------------------------------------
// BackendCodec — all messages have type+length framing
// ---------------------------------------------------------------------------

#[test]
fn backend_codec_ready_for_query() {
    let mut codec = BackendCodec::new();
    let mut buf = BytesMut::new();
    BackendMessage::ReadyForQuery(types::TransactionStatus::Idle).encode(&mut buf);

    let msg = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(
        msg,
        BackendMessage::ReadyForQuery(types::TransactionStatus::Idle)
    );
}

#[test]
fn backend_codec_multiple_messages() {
    let mut codec = BackendCodec::new();
    let mut buf = BytesMut::new();

    BackendMessage::AuthenticationOk.encode(&mut buf);
    BackendMessage::ParameterStatus {
        name: "server_version".into(),
        value: "16.2".into(),
    }
    .encode(&mut buf);
    BackendMessage::BackendKeyData {
        process_id: 100,
        secret_key: 200,
    }
    .encode(&mut buf);
    BackendMessage::ReadyForQuery(types::TransactionStatus::Idle).encode(&mut buf);

    let msg1 = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(msg1, BackendMessage::AuthenticationOk);

    let msg2 = codec.decode(&mut buf).unwrap().unwrap();
    assert!(matches!(msg2, BackendMessage::ParameterStatus { .. }));

    let msg3 = codec.decode(&mut buf).unwrap().unwrap();
    assert!(matches!(msg3, BackendMessage::BackendKeyData { .. }));

    let msg4 = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(
        msg4,
        BackendMessage::ReadyForQuery(types::TransactionStatus::Idle)
    );

    assert!(codec.decode(&mut buf).unwrap().is_none());
}

#[test]
fn backend_codec_incremental_delivery() {
    let mut codec = BackendCodec::new();

    let mut full = BytesMut::new();
    BackendMessage::CommandComplete("SELECT 1".into()).encode(&mut full);

    // Feed one byte at a time
    let mut buf = BytesMut::new();
    for i in 0..full.len() {
        buf.extend_from_slice(&full[i..=i]);
        let result = codec.decode(&mut buf).unwrap();
        if i < full.len() - 1 {
            assert!(result.is_none(), "decoded too early at byte {i}");
        } else {
            assert!(result.is_some(), "should have decoded at final byte");
        }
    }
}

#[test]
fn backend_codec_returns_none_on_empty() {
    let mut codec = BackendCodec::new();
    let mut buf = BytesMut::new();
    assert!(codec.decode(&mut buf).unwrap().is_none());
}

#[test]
fn frontend_codec_cancel_request() {
    let mut codec = FrontendCodec::new();
    let mut buf = BytesMut::new();

    FrontendMessage::CancelRequest {
        process_id: 42,
        secret_key: 99,
    }
    .encode(&mut buf);

    let msg = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(
        msg,
        FrontendMessage::CancelRequest {
            process_id: 42,
            secret_key: 99,
        }
    );
}
