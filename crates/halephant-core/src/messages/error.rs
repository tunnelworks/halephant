//! Error-response frame builders — two flavours:
//!
//! - [`send_fatal`] for `"FATAL"` errors where the connection is
//!   closing anyway (auth rejection, upstream closed mid-transaction).
//!   Errors during send are swallowed and logged — if we couldn't
//!   push the error frame, the socket is already gone.
//!
//! - [`send_error`] for non-fatal `"ERROR"` rejections where the
//!   client's session continues afterwards (read-write override on
//!   a replica-routed transaction, rejected SQL statements). Follows
//!   the error with `ReadyForQuery(status)` so the client knows the
//!   session is still usable, and propagates send errors.

use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::warn;

use crate::proto::backend::{BackendMessage, NoticeFields};
use crate::proto::codec::FrontendCodec;
use crate::proto::types::TransactionStatus;

/// Send a FATAL `ErrorResponse` frame. Used when the connection is
/// closing anyway — the caller typically bails immediately after.
///
/// Errors during send are logged but not propagated: if we couldn't
/// deliver the error frame, the socket is already gone and there's
/// nothing useful the caller could do with a send error.
pub async fn send_fatal(
    client: &mut Framed<TcpStream, FrontendCodec>,
    sqlstate: &str,
    message: &str,
) {
    let frame = BackendMessage::ErrorResponse(NoticeFields {
        fields: vec![
            (b'S', "FATAL".into()),
            (b'C', sqlstate.into()),
            (b'M', message.into()),
        ],
    });
    if let Err(e) = client.send(frame).await {
        warn!(%e, sqlstate, "failed to send FATAL error response to client");
    }
}

/// Send a non-fatal `ErrorResponse` frame followed by
/// `ReadyForQuery(status)` so the client can continue the session
/// after the error. Used for in-session rejections like read-write
/// override attempts on a replica-routed transaction.
///
/// The `status` parameter **must** reflect the actual session state
/// at the call site — same rule as
/// [`super::synthetic::send_synthetic_ok`].
pub async fn send_error(
    client: &mut Framed<TcpStream, FrontendCodec>,
    sqlstate: &str,
    message: &str,
    status: TransactionStatus,
) -> anyhow::Result<()> {
    client
        .feed(BackendMessage::ErrorResponse(NoticeFields {
            fields: vec![
                (b'S', "ERROR".into()),
                (b'C', sqlstate.into()),
                (b'M', message.into()),
            ],
        }))
        .await?;
    client.send(BackendMessage::ReadyForQuery(status)).await?;
    Ok(())
}
