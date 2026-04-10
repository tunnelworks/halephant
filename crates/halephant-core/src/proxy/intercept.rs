//! Message interceptors that produce synthetic client-facing
//! responses without involving the upstream server. Each handler
//! either consumes the message (returns `Some(true)`) or defers it to
//! the normal forwarding path (returns `None`).

use std::sync::Arc;

use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::debug;

use crate::listener::ClientNotifications;
use crate::pool::PoolManager;
use crate::proto;
use crate::proxy::prepared::ClientPrepared;
use crate::sql;

/// Intercept Close messages for named statements. Returns:
/// - `Some(true)` — intercepted, synthetic CloseComplete sent to client
/// - `None` — not a statement Close, caller should forward normally
pub(super) async fn handle_close_intercept(
    msg: &proto::frontend::FrontendMessage,
    client_prepared: &mut ClientPrepared,
    client: &mut Framed<TcpStream, proto::codec::FrontendCodec>,
    pools: &Arc<PoolManager>,
) -> anyhow::Result<Option<bool>> {
    if let proto::frontend::FrontendMessage::Close(close) = msg
        && close.kind == proto::frontend::TargetKind::Statement
        && !close.name.is_empty()
    {
        {
            let mut store = pools.stmt_store.lock();
            client_prepared.remove(&close.name, &mut store);
        }
        client
            .send(proto::backend::BackendMessage::CloseComplete)
            .await?;
        return Ok(Some(true));
    }
    Ok(None)
}

/// Intercept `DEALLOCATE name` and `DEALLOCATE ALL` queries in the
/// simple query protocol. Returns:
/// - `Some(true)` — intercepted, synthetic `CommandComplete` +
///   `ReadyForQuery` sent to the client
/// - `None` — not a DEALLOCATE, caller should forward normally
pub(super) async fn handle_deallocate_intercept(
    msg: &proto::frontend::FrontendMessage,
    client_prepared: &mut ClientPrepared,
    client: &mut Framed<TcpStream, proto::codec::FrontendCodec>,
    pools: &Arc<PoolManager>,
    status: proto::types::TransactionStatus,
) -> anyhow::Result<Option<bool>> {
    let proto::frontend::FrontendMessage::Query(query) = msg else {
        return Ok(None);
    };

    let sql::Statement::Deallocate { target } = sql::parse(query) else {
        return Ok(None);
    };

    match target {
        sql::DeallocateTarget::Name(name) => {
            debug!(%name, "intercepting DEALLOCATE for named statement");
            let mut store = pools.stmt_store.lock();
            // `remove` is a no-op if the name isn't tracked — the
            // client's DEALLOCATE for an unknown name would error on
            // the server, but since we can't forward (no canonical
            // mapping), we still send the success response. The
            // alternative — synthesising a server error — isn't
            // worth the added complexity since clients that
            // DEALLOCATE unknown statements are almost always bugs.
            client_prepared.remove(&name, &mut store);
        }
        sql::DeallocateTarget::All => {
            debug!("intercepting DEALLOCATE ALL, releasing all client prepared statements");
            let mut store = pools.stmt_store.lock();
            client_prepared.release_all(&mut store);
        }
    }

    crate::messages::synthetic::send_synthetic_ok(client, "DEALLOCATE", status).await?;
    Ok(Some(true))
}

/// If the message is a LISTEN/UNLISTEN and we're in multiplex mode, handle it
/// locally (update subscriptions, send synthetic response). Returns:
/// - `Some(true)` if the message was intercepted (caller should continue/skip)
/// - `Some(false)` or `None` if the message should be forwarded normally
pub(super) async fn handle_listen_intercept(
    msg: &proto::frontend::FrontendMessage,
    notifications: &mut Option<ClientNotifications>,
    client: &mut Framed<TcpStream, proto::codec::FrontendCodec>,
    status: proto::types::TransactionStatus,
) -> anyhow::Result<Option<bool>> {
    let proto::frontend::FrontendMessage::Query(query) = msg else {
        return Ok(None);
    };

    match sql::parse(query) {
        sql::Statement::Listen { channel } => {
            if let Some(notifs) = notifications.as_mut() {
                debug!(%channel, "multiplex: subscribing client to channel");
                notifs.listen(&channel);
                crate::messages::synthetic::send_synthetic_ok(client, "LISTEN", status).await?;
                return Ok(Some(true));
            }
        }
        sql::Statement::Unlisten { target } => {
            if let Some(notifs) = notifications.as_mut() {
                match target {
                    sql::UnlistenTarget::Star => {
                        debug!("multiplex: unsubscribing client from all channels");
                        notifs.unlisten_all();
                    }
                    sql::UnlistenTarget::Channel(channel) => {
                        debug!(%channel, "multiplex: unsubscribing client from channel");
                        notifs.unlisten(&channel);
                    }
                }
                crate::messages::synthetic::send_synthetic_ok(client, "UNLISTEN", status).await?;
                return Ok(Some(true));
            }
        }
        _ => {}
    }

    Ok(None)
}
