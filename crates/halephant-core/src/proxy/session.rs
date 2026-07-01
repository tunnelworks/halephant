use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::trace;

use crate::connections::server::{Reply, ServerConn};
use crate::pool::PoolManager;
use crate::proto::backend::BackendMessage;
use crate::proto::codec::FrontendCodec;
use crate::proto::frontend::FrontendMessage;
use crate::proto::types::TransactionStatus;
use crate::proxy::intercept::{handle_close_intercept, handle_deallocate_intercept};
use crate::proxy::prepared::{ClientPrepared, PreparedGuard, rewrite_outbound};

/// Forward messages between client and server for the lifetime of the session.
///
/// `Terminate` from the client is intercepted so the server connection can be
/// returned to the pool rather than closed.
///
/// One client owns the backend for the whole session, but the backend is
/// reused by later sessions, so prepared statements still flow through
/// halephant's canonical-name rewriting. Forwarding client-chosen names (such
/// as psycopg3's `_pg3_0`, which restarts at 0 on every new connection)
/// verbatim would collide with a name a previous session left prepared on that
/// backend and draw `42P05 duplicate_prepared_statement`. Rewriting every
/// `Parse` to `SHA-256(query, param_oids)` makes the backend see only
/// collision-free hashes; the per-backend cache that records them persists
/// across sessions, while the per-client name map is dropped on disconnect.
pub async fn forward(
    client: &mut Framed<TcpStream, FrontendCodec>,
    server: &mut ServerConn,
    pools: &Arc<PoolManager>,
) -> anyhow::Result<()> {
    // Drop guard releases the session's statement-store references even
    // if the client task is cancelled mid-forward.
    let mut client_prepared = PreparedGuard {
        inner: ClientPrepared::new(),
        pools: Arc::clone(pools),
    };
    forward_loop(client, server, pools, &mut client_prepared.inner).await
}

async fn forward_loop(
    client: &mut Framed<TcpStream, FrontendCodec>,
    server: &mut ServerConn,
    pools: &Arc<PoolManager>,
    client_prepared: &mut ClientPrepared,
) -> anyhow::Result<()> {
    // The per-backend prepared cache persists across sessions, but the
    // reply bookkeeping must start clean for this session.
    server.statements.reset_pending();
    // Track the latest backend transaction status so intercepted
    // messages (DEALLOCATE) can synthesize an accurate ReadyForQuery.
    let mut last_status = TransactionStatus::Idle;

    loop {
        tokio::select! {
            msg = client.next() => match msg.transpose()? {
                Some(FrontendMessage::Terminate) => {
                    trace!("client sent Terminate");
                    return Ok(());
                }
                Some(msg) => {
                    // Absorb statement Close: drop the per-client mapping
                    // but never forward to the backend — the canonical may
                    // still be needed by another session.
                    if let Some(true) =
                        handle_close_intercept(&msg, client_prepared, client, pools).await?
                    {
                        continue;
                    }
                    // Absorb DEALLOCATE for the same reason: the backend
                    // only knows canonical names, never the client's.
                    if let Some(true) = handle_deallocate_intercept(
                        &msg,
                        client_prepared,
                        client,
                        pools,
                        last_status,
                    )
                    .await?
                    {
                        continue;
                    }
                    // Rewrite prepared-statement names to canonicals (and
                    // re-Parse on cache miss) before forwarding.
                    if let Some(msg) =
                        rewrite_outbound(msg, client_prepared, server, pools, client).await?
                    {
                        trace!(?msg, "client -> server");
                        server.framed.send(msg).await?;
                    }
                }
                None => return Ok(()),
            },
            msg = server.framed.next() => match msg.transpose()? {
                // Filter out completions that answer Parse/Close messages
                // halephant injected on the client's behalf — same rules as
                // transaction mode, driven by the per-connection FIFO.
                Some(BackendMessage::ParseComplete) => match server.statements.next_parse_reply() {
                    Reply::Suppress => trace!("suppressing injected ParseComplete"),
                    Reply::Forward => client.send(BackendMessage::ParseComplete).await?,
                },
                Some(BackendMessage::CloseComplete) => match server.statements.next_close_reply() {
                    Reply::Suppress => trace!("suppressing injected CloseComplete"),
                    Reply::Forward => client.send(BackendMessage::CloseComplete).await?,
                },
                Some(BackendMessage::ReadyForQuery(status)) => {
                    last_status = status;
                    server.last_tx_status = status;
                    client.send(BackendMessage::ReadyForQuery(status)).await?;
                }
                Some(BackendMessage::ErrorResponse(ref err)) => {
                    // Roll back optimistic prepared-statement inserts the
                    // aborted batch will never confirm — essential here
                    // because session mode reuses one backend for the whole
                    // session with no per-transaction checkout to reset it.
                    server.statements.roll_back_after_error();
                    client
                        .send(BackendMessage::ErrorResponse(err.clone()))
                        .await?;
                }
                Some(msg) => {
                    trace!(?msg, "server -> client");
                    client.send(msg).await?;
                }
                None => anyhow::bail!("upstream server closed unexpectedly"),
            },
        }
    }
}
