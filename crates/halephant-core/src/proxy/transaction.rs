//! Transaction-mode proxy: the outer loop cycles between an idle
//! state (no server connection) and an active state (holding a
//! checked-out [`crate::connections::server::ServerConn`] for the
//! duration of a single transaction). On `ReadyForQuery(Idle)` the
//! connection is returned to the pool; on `Terminate` or disconnect
//! the loop exits.

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{Instrument, debug, field, info_span, trace, warn};

use crate::clients::{ClientGuard, ClientState};
use crate::config::cluster::pool::ListenMode;
use crate::config::otel::QueryText;
use crate::connections::server::{Reply, ServerConn};
use crate::listener::ClientNotifications;
use crate::o11y;
use crate::pool::PoolManager;
use crate::proto;
use crate::proxy::intercept::{
    handle_close_intercept, handle_deallocate_intercept, handle_listen_intercept,
};
use crate::proxy::prepared::{ClientPrepared, PreparedGuard, rewrite_outbound};
use crate::sql;

/// Track session-state-affecting commands on the server connection so
/// halephant's per-connection pool state stays in sync with what
/// PostgreSQL actually holds. Two trackers are kept in step here:
///
/// - `server.dirty_vars` — GUC variables the client has SET and that
///   `reset_connection` must RESET on checkin.
/// - `server.statements` — canonical names of prepared statements
///   registered on this backend via `ensure_prepared`.
///
/// Only tracks the simple query protocol (Query messages) — a SET
/// inside a Parse is not tracked because it hasn't been executed yet.
fn track_set_reset(msg: &proto::frontend::FrontendMessage, server: &mut ServerConn) {
    let proto::frontend::FrontendMessage::Query(query) = msg else {
        return;
    };
    // SET LOCAL falls into the wildcard arm below: its effect is
    // transaction-scoped and unwound automatically on COMMIT/ROLLBACK,
    // so there's nothing to track in dirty_vars.
    match sql::parse(query) {
        sql::Statement::Set {
            scope: sql::SetScope::Session,
            parameter,
            ..
        } => {
            server.dirty_vars.insert(parameter);
        }
        sql::Statement::Reset {
            target: sql::ResetTarget::All,
        } => {
            server.dirty_vars.clear();
        }
        sql::Statement::Reset {
            target: sql::ResetTarget::Parameter(name),
        } => {
            server.dirty_vars.remove(&name);
        }
        sql::Statement::Discard {
            target: sql::DiscardTarget::All,
        } => {
            server.statements.discard_all();
            server.dirty_vars.clear();
        }
        _ => {}
    }
}

/// Why we returned from the active forwarding state.
enum Release {
    /// Server sent ReadyForQuery(Idle) — transaction complete, connection can
    /// be returned to the pool. The client remains connected and may start
    /// another transaction.
    Idle,
    /// Client sent Terminate or disconnected.
    ClientDone,
}

/// Outer loop: alternate between idle (no server connection) and active
/// (forwarding a transaction).
#[allow(clippy::too_many_arguments)]
pub async fn forward(
    client: &mut Framed<TcpStream, proto::codec::FrontendCodec>,
    pools: &Arc<PoolManager>,
    client_guard: &ClientGuard,
    notifications: &mut Option<ClientNotifications>,
    listen_mode: ListenMode,
    database: &str,
    user: &str,
    read_only: bool,
    query_text: QueryText,
) -> anyhow::Result<()> {
    let mut client_prepared = PreparedGuard {
        inner: ClientPrepared::new(),
        pools: Arc::clone(pools),
    };
    forward_loop(
        client,
        pools,
        client_guard,
        notifications,
        listen_mode,
        database,
        user,
        read_only,
        &mut client_prepared.inner,
        query_text,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn forward_loop(
    client: &mut Framed<TcpStream, proto::codec::FrontendCodec>,
    pools: &Arc<PoolManager>,
    client_guard: &ClientGuard,
    notifications: &mut Option<ClientNotifications>,
    listen_mode: ListenMode,
    database: &str,
    user: &str,
    read_only: bool,
    client_prepared: &mut ClientPrepared,
    query_text: QueryText,
) -> anyhow::Result<()> {
    loop {
        // Idle — wait for the client to send something (or a notification).
        let first_msg = loop {
            tokio::select! {
                msg = client.next() => match msg.transpose()? {
                    Some(proto::frontend::FrontendMessage::Terminate) => {
                        trace!("client sent Terminate (idle)");
                        return Ok(());
                    }
                    Some(msg) => {
                        // We're in the outer idle loop with no server
                        // connection checked out — by definition the
                        // session has no active transaction, so any
                        // synthetic intercept response advertises
                        // `Idle` to the client.
                        let idle = proto::types::TransactionStatus::Idle;

                        // In multiplex mode, intercept LISTEN/UNLISTEN in idle state
                        // (no server connection needed).
                        if listen_mode == ListenMode::Multiplex
                            && let Some(true) =
                                handle_listen_intercept(&msg, notifications, client, idle).await?
                        {
                            continue; // stay idle
                        }

                        // Intercept Close for named statements — return synthetic
                        // CloseComplete without needing a server connection.
                        if let Some(true) = handle_close_intercept(&msg, client_prepared, client, pools).await? {
                            continue; // stay idle
                        }

                        // Intercept DEALLOCATE queries for the same
                        // reason — halephant renames client Parse names
                        // to canonical hashes, so a raw DEALLOCATE
                        // would not find the statement on the server.
                        if let Some(true) = handle_deallocate_intercept(&msg, client_prepared, client, pools, idle).await? {
                            continue; // stay idle
                        }

                        break msg;
                    }
                    None => return Ok(()),
                },
                Some(notif) = recv_notification(notifications) => {
                    client.send(notif).await?;
                }
            }
        };

        // Reject read-write overrides on read-only sessions.
        if read_only && is_read_write_override_msg(&first_msg) {
            reject_read_write(client, proto::types::TransactionStatus::Idle).await?;
            continue;
        }

        // Determine routing for this transaction. If the first message is
        // BEGIN READ ONLY (or START TRANSACTION READ ONLY), route to a replica
        // even if the session isn't globally read-only.
        let txn_read_only = read_only || is_read_only_query(&first_msg);

        let txn_span = info_span!(
            "proxy.transaction",
            db.namespace = database,
            user,
            server.address = field::Empty,
            otel.status_code = field::Empty,
            otel.status_description = field::Empty,
        );

        let release = async {
            let result: anyhow::Result<Release> = async {
                let mut guard = pools
                    .checkout(client_guard, database, user, txn_read_only)
                    .await?;
                client_guard.set_state(ClientState::InTransaction);
                tracing::Span::current().record("server.address", guard.node());

                // Reset this connection's injected-response FIFOs only. A
                // client that disconnected mid extended-protocol batch can
                // leave a ParseComplete/CloseComplete disposition whose reply
                // the backend skipped and will never send; clearing here
                // keeps it from misaligning this transaction's filter.
                //
                // This is NOT where stale prepared-cache entries are handled:
                // a rejected `Parse` rolls back its optimistic insert at the
                // `ErrorResponse` arm below (before checkin), so it never
                // reaches a later checkout. `reset_pending` is FIFO alignment
                // only, not a catch-all for error-induced staleness.
                guard.conn().statements.reset_pending();

                // In pin mode, check if the first message is LISTEN — pin immediately.
                let mut pinned = false;
                if matches!(classify(&first_msg), sql::Statement::Listen { .. }) {
                    pinned = listen_mode == ListenMode::Pin;
                }
                if pinned {
                    warn!("LISTEN detected, pinning connection to session mode");
                }

                let mut tracker = o11y::spans::StatementTracker::new(query_text);

                // Track SET/RESET and rewrite/send the first message.
                tracker.on_client_msg(&first_msg, |name| {
                    resolve_stmt_query(name, client_prepared, pools)
                });
                track_set_reset(&first_msg, guard.conn());
                if let Some(first_msg) =
                    rewrite_outbound(first_msg, client_prepared, guard.conn(), pools, client)
                        .await?
                {
                    guard.conn().framed.send(first_msg).await?;
                }
                debug!("transaction started");

                // Forward until the transaction completes (ReadyForQuery Idle).
                let result = forward_until_idle(
                    client,
                    guard.conn(),
                    notifications,
                    listen_mode,
                    &mut pinned,
                    client_prepared,
                    pools,
                    txn_read_only,
                    &mut tracker,
                )
                .await;

                match result {
                    Ok(Release::Idle) => {
                        debug!("transaction complete, connection returned to pool");
                        guard.checkin();
                        Ok(Release::Idle)
                    }
                    Ok(Release::ClientDone) => {
                        guard.checkin();
                        Ok(Release::ClientDone)
                    }
                    Err(e) => Err(e),
                }
            }
            .await;

            // Back to Idle regardless of result — InTransaction is only
            // set while we're actively holding a backend. On error the
            // checkin happened inside the inner result; on success the
            // explicit checkin above already ran.
            client_guard.set_state(ClientState::Idle);

            let span = tracing::Span::current();
            match &result {
                Ok(_) => {
                    span.record("otel.status_code", "OK");
                }
                Err(e) => {
                    span.record("otel.status_code", "ERROR");
                    span.record("otel.status_description", e.to_string().as_str());
                }
            }
            result
        }
        .instrument(txn_span)
        .await;

        match release {
            Ok(Release::Idle) => {}
            Ok(Release::ClientDone) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

/// Inner loop: forward bidirectionally until ReadyForQuery(Idle) or the client
/// goes away.
#[allow(clippy::too_many_arguments)]
async fn forward_until_idle(
    client: &mut Framed<TcpStream, proto::codec::FrontendCodec>,
    server: &mut ServerConn,
    notifications: &mut Option<ClientNotifications>,
    listen_mode: ListenMode,
    pinned: &mut bool,
    client_prepared: &mut ClientPrepared,
    pools: &Arc<PoolManager>,
    txn_read_only: bool,
    tracker: &mut o11y::spans::StatementTracker,
) -> anyhow::Result<Release> {
    // Track the most recent transaction status from the server so
    // synthetic responses for intercepted client messages reflect
    // the actual session state. The default is `InTransaction`
    // because we entered this function after sending the first
    // message of a new transaction — even if no server response has
    // arrived yet (the client may have pipelined a BEGIN + LISTEN),
    // the expected post-first-message state is in-transaction. The
    // server's eventual ReadyForQuery overwrites this with the real
    // status, including `InFailedTransaction` when applicable.
    let mut last_server_status = proto::types::TransactionStatus::InTransaction;

    loop {
        tokio::select! {
            msg = client.next() => match msg.transpose()? {
                Some(proto::frontend::FrontendMessage::Terminate) => {
                    trace!("client sent Terminate (in transaction)");
                    return Ok(Release::ClientDone);
                }
                Some(msg) => {
                    // Multiplex: intercept LISTEN/UNLISTEN even mid-transaction.
                    if listen_mode == ListenMode::Multiplex
                        && let Some(true) = handle_listen_intercept(
                            &msg,
                            notifications,
                            client,
                            last_server_status,
                        )
                        .await?
                    {
                        continue;
                    }

                    // Reject read-write overrides on read-only transactions.
                    if txn_read_only && is_read_write_override_msg(&msg) {
                        reject_read_write(client, proto::types::TransactionStatus::InTransaction).await?;
                        continue;
                    }

                    // Intercept Close for named statements.
                    if let Some(true) = handle_close_intercept(&msg, client_prepared, client, pools).await? {
                        continue;
                    }

                    // Intercept DEALLOCATE queries.
                    if let Some(true) = handle_deallocate_intercept(
                        &msg,
                        client_prepared,
                        client,
                        pools,
                        last_server_status,
                    )
                    .await?
                    {
                        continue;
                    }

                    // Pin mode: detect LISTEN/UNLISTEN for pinning.
                    if listen_mode == ListenMode::Pin {
                        match classify(&msg) {
                            sql::Statement::Listen { .. } if !*pinned => {
                                warn!("LISTEN detected, pinning connection to session mode");
                                *pinned = true;
                            }
                            sql::Statement::Unlisten {
                                target: sql::UnlistenTarget::Star,
                            } if *pinned => {
                                debug!("UNLISTEN * detected, unpinning connection");
                                *pinned = false;
                            }
                            _ => {}
                        }
                    }

                    // Record statement span before rewriting.
                    tracker.on_client_msg(&msg, |name| resolve_stmt_query(name, client_prepared, pools));

                    // Track SET/RESET and rewrite prepared statement names.
                    track_set_reset(&msg, server);
                    if let Some(msg) = rewrite_outbound(msg, client_prepared, server, pools, client).await? {
                        trace!(?msg, "client -> server");
                        server.framed.send(msg).await?;
                    }
                }
                None => return Ok(Release::ClientDone),
            },
            msg = server.framed.next() => match msg.transpose()? {
                Some(proto::backend::BackendMessage::ReadyForQuery(status)) => {
                    last_server_status = status;
                    server.last_tx_status = status;
                    tracker.drain();
                    client.send(proto::backend::BackendMessage::ReadyForQuery(status)).await?;
                    if status == proto::types::TransactionStatus::Idle && !*pinned {
                        return Ok(Release::Idle);
                    }
                }
                Some(proto::backend::BackendMessage::CommandComplete(ref tag)) => {
                    tracker.on_command_complete(tag);
                    client.send(proto::backend::BackendMessage::CommandComplete(tag.clone())).await?;
                }
                Some(proto::backend::BackendMessage::EmptyQueryResponse) => {
                    tracker.on_command_complete("");
                    client.send(proto::backend::BackendMessage::EmptyQueryResponse).await?;
                }
                Some(proto::backend::BackendMessage::ErrorResponse(ref err)) => {
                    tracker.on_error(err.message().unwrap_or("unknown"));
                    // The error aborts the extended-protocol batch: every
                    // message after it is skipped, so any Parse halephant
                    // recorded optimistically will never be confirmed. Roll
                    // those inserts back so the pooled connection doesn't
                    // claim statements the backend never prepared.
                    server.statements.roll_back_after_error();
                    client.send(proto::backend::BackendMessage::ErrorResponse(err.clone())).await?;
                }
                Some(proto::backend::BackendMessage::ParseComplete) => {
                    // A ParseComplete answers a Parse halephant either
                    // forwarded for the client (deliver it) or injected to
                    // re-prepare on the client's behalf (swallow it). The
                    // disposition was recorded in send order when the Parse
                    // went out.
                    match server.statements.next_parse_reply() {
                        Reply::Suppress => trace!("suppressing injected ParseComplete"),
                        Reply::Forward => {
                            client.send(proto::backend::BackendMessage::ParseComplete).await?;
                        }
                    }
                }
                Some(proto::backend::BackendMessage::CloseComplete) => {
                    // Likewise for CloseComplete: forward the client's portal
                    // closes, swallow halephant's LRU-eviction closes.
                    match server.statements.next_close_reply() {
                        Reply::Suppress => trace!("suppressing injected CloseComplete"),
                        Reply::Forward => {
                            client.send(proto::backend::BackendMessage::CloseComplete).await?;
                        }
                    }
                }
                Some(msg) => {
                    trace!(?msg, "server -> client");
                    client.send(msg).await?;
                }
                None => {
                    tracker.drain();
                    // Send a synthesized FATAL ErrorResponse so the
                    // client knows the transaction was lost, rather
                    // than just seeing EOF.
                    crate::messages::error::send_fatal(
                        client,
                        "08006",
                        "upstream server closed unexpectedly",
                    )
                    .await;
                    anyhow::bail!("upstream server closed unexpectedly");
                }
            },
            Some(notif) = recv_notification(notifications) => {
                client.send(notif).await?;
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Receive a notification from the client's subscription, or pend forever if
/// multiplexing is not active for this client.
async fn recv_notification(
    notifs: &mut Option<ClientNotifications>,
) -> Option<proto::backend::BackendMessage> {
    match notifs {
        Some(n) => n.recv().await,
        None => std::future::pending().await,
    }
}

/// Look up the query text for a named prepared statement via the client's
/// name mapping and the global statement store. Used as the fallback for
/// cross-transaction statement reuse.
fn resolve_stmt_query(
    client_name: &str,
    client_prepared: &ClientPrepared,
    pools: &PoolManager,
) -> Option<String> {
    let canon = client_prepared.resolve(client_name)?;
    let store = pools.stmt_store.lock();
    store.get(canon).map(|parse| parse.query.clone())
}

/// Parse the SQL body of a client message, if any.
fn classify(msg: &proto::frontend::FrontendMessage) -> sql::Statement {
    match msg {
        proto::frontend::FrontendMessage::Query(q) => sql::parse(q),
        proto::frontend::FrontendMessage::Parse(p) => sql::parse(&p.query),
        _ => sql::Statement::Other,
    }
}

/// Check if a message is a `BEGIN READ ONLY` (or `START TRANSACTION READ ONLY`).
fn is_read_only_query(msg: &proto::frontend::FrontendMessage) -> bool {
    matches!(
        classify(msg),
        sql::Statement::Begin {
            options: sql::TransactionOptions {
                read_only: Some(true),
            },
        }
    )
}

/// Check if a message attempts to switch to read-write mode.
fn is_read_write_override_msg(msg: &proto::frontend::FrontendMessage) -> bool {
    classify(msg).is_read_write_override()
}

/// Send an error to the client rejecting a read-write override on a
/// replica-routed connection. Thin wrapper around
/// [`crate::messages::error::send_error`] that hardcodes the
/// SQLSTATE and message for this specific rejection.
async fn reject_read_write(
    client: &mut Framed<TcpStream, proto::codec::FrontendCodec>,
    status: proto::types::TransactionStatus,
) -> anyhow::Result<()> {
    crate::messages::error::send_error(
        client,
        "25006",
        "cannot switch to read-write mode: connection is routed to a read replica",
        status,
    )
    .await
}
