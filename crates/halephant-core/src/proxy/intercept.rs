//! Message interceptors that produce synthetic client-facing
//! responses without involving the upstream server. Each handler
//! either consumes the message (returns `true`) or defers it to the
//! normal forwarding path (returns `false`).
//!
//! The SQL-driven handlers take the caller's one-per-message
//! [`sql::Statement`] classification instead of re-parsing the
//! message. Callers only offer statements from simple-protocol
//! `Query` messages for interception: a `Parse`-carried statement is
//! being prepared, not executed, and a synthetic simple-query
//! response would desynchronize the extended-protocol reply stream.

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

/// Intercept Close messages for named statements. Returns `true` when
/// the Close was absorbed and a synthetic CloseComplete was sent to
/// the client; `false` when the caller should forward normally.
pub(super) async fn handle_close_intercept(
    msg: &proto::frontend::FrontendMessage,
    client_prepared: &mut ClientPrepared,
    client: &mut Framed<TcpStream, proto::codec::FrontendCodec>,
    pools: &Arc<PoolManager>,
) -> anyhow::Result<bool> {
    if let proto::frontend::FrontendMessage::Close(close) = msg
        && close.kind == proto::frontend::TargetKind::Statement
    {
        {
            let mut store = pools.stmt_store.lock();
            client_prepared.remove(&close.name, &mut store);
        }
        client
            .send(proto::backend::BackendMessage::CloseComplete)
            .await?;
        return Ok(true);
    }
    Ok(false)
}

/// Intercept `DEALLOCATE name` and `DEALLOCATE ALL` statements in the
/// simple query protocol. Returns `true` when the statement was
/// absorbed and a synthetic `CommandComplete` + `ReadyForQuery` was
/// sent to the client; `false` when the caller should forward
/// normally.
pub(super) async fn handle_deallocate_intercept(
    stmt: &sql::Statement,
    client_prepared: &mut ClientPrepared,
    client: &mut Framed<TcpStream, proto::codec::FrontendCodec>,
    pools: &Arc<PoolManager>,
    status: proto::types::TransactionStatus,
) -> anyhow::Result<bool> {
    let sql::Statement::Deallocate { target } = stmt else {
        return Ok(false);
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
            client_prepared.remove(name, &mut store);
        }
        sql::DeallocateTarget::All => {
            debug!("intercepting DEALLOCATE ALL, releasing all client prepared statements");
            let mut store = pools.stmt_store.lock();
            client_prepared.release_all(&mut store);
        }
    }

    crate::messages::synthetic::send_synthetic_ok(client, "DEALLOCATE", status).await?;
    Ok(true)
}

/// Intercept LISTEN/UNLISTEN statements when multiplexing is active
/// for this client (update subscriptions, send synthetic response).
/// Returns `true` when the statement was absorbed; `false` when the
/// caller should forward normally (including when no subscription
/// exists to multiplex onto).
pub(super) async fn handle_listen_intercept(
    stmt: &sql::Statement,
    notifications: &mut Option<ClientNotifications>,
    client: &mut Framed<TcpStream, proto::codec::FrontendCodec>,
    status: proto::types::TransactionStatus,
) -> anyhow::Result<bool> {
    match stmt {
        sql::Statement::Listen { channel } => {
            if let Some(notifs) = notifications.as_mut() {
                debug!(%channel, "multiplex: subscribing client to channel");
                notifs.listen(channel);
                crate::messages::synthetic::send_synthetic_ok(client, "LISTEN", status).await?;
                return Ok(true);
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
                        notifs.unlisten(channel);
                    }
                }
                crate::messages::synthetic::send_synthetic_ok(client, "UNLISTEN", status).await?;
                return Ok(true);
            }
        }
        _ => {}
    }

    Ok(false)
}
