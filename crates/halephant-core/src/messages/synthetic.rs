//! Synthetic response frames — `CommandComplete` + `ReadyForQuery`
//! sequences that halephant fabricates when it intercepts a client
//! statement instead of forwarding it to the upstream.
//!
//! Used by the transaction forwarder for LISTEN/UNLISTEN
//! (multiplex mode), DEALLOCATE, and Close-statement interception.

use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::proto::backend::BackendMessage;
use crate::proto::codec::FrontendCodec;
use crate::proto::types::TransactionStatus;

/// Send a synthetic `CommandComplete(tag)` followed by
/// `ReadyForQuery(status)` to the client as if the server had
/// processed the intercepted statement.
///
/// The `status` parameter **must** match the actual session state
/// at the call site — sending `Idle` from inside an open
/// transaction makes the client believe the transaction is over.
/// The transaction forwarder threads the most recent server
/// `ReadyForQuery` status through to guarantee this.
pub async fn send_synthetic_ok(
    client: &mut Framed<TcpStream, FrontendCodec>,
    tag: &str,
    status: TransactionStatus,
) -> anyhow::Result<()> {
    client
        .feed(BackendMessage::CommandComplete(tag.to_owned()))
        .await?;
    client.send(BackendMessage::ReadyForQuery(status)).await?;
    Ok(())
}
