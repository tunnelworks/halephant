//! Per-client wire-protocol entry point. Drives the PostgreSQL
//! startup handshake (SSL/GSS negotiation, `StartupMessage` parsing,
//! cancel forwarding), authenticates, checks out an initial server
//! connection, sets up multiplexed LISTEN when configured, and
//! dispatches to [`crate::proxy::session::forward`] or
//! [`crate::proxy::transaction::forward`] based on the pool mode.

use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{Instrument, info_span, trace};

use crate::auth::Authenticator;
use crate::clients::{self, ClientRegistry, ClientState};
use crate::config::Config;
use crate::config::cluster::pool::{ListenMode, PoolMode};
use crate::listener::ListenerManager;
use crate::pool::{PoolManager, Routing};
use crate::proto::codec::{BackendCodec, FrontendCodec};
use crate::proto::frontend::FrontendMessage;
use crate::proxy;

/// Accept a client connection, run the PostgreSQL startup handshake,
/// authenticate, and forward traffic until the client disconnects.
/// Wraps the whole session in the root `proxy.client` span so every
/// nested span (setup, auth, checkout, transaction, statement) appears
/// under a single trace from accept through cleanup.
pub async fn forward(
    client_stream: TcpStream,
    client_addr: SocketAddr,
    config: &Config,
    pools: &Arc<PoolManager>,
    listeners: &ListenerManager,
    auth: &Authenticator,
    clients: &Arc<ClientRegistry>,
) -> anyhow::Result<()> {
    let client_guard = clients.register(client_addr);
    let client_id = client_guard.id();

    // Root span for the entire client session. Every subsequent span
    // (proxy.setup, auth, pool.checkout, pool.connect, proxy.session,
    // proxy.transaction, proxy.statement, ...) nests under this one so a
    // single trace covers accept → cleanup for a client. `db.namespace`
    // and `user` start empty; they're recorded once parsed from the
    // startup message.
    let client_span = info_span!(
        "proxy.client",
        halephant.client.id = client_id.as_u64(),
        client.address = %client_addr,
        db.namespace = tracing::field::Empty,
        user = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    );

    async move {
        let result = forward_inner(
            client_stream,
            client_addr,
            config,
            pools,
            listeners,
            auth,
            &client_guard,
        )
        .await;
        let span = tracing::Span::current();
        match &result {
            Ok(()) => {
                span.record("otel.status_code", "OK");
            }
            Err(e) => {
                span.record("otel.status_code", "ERROR");
                span.record("otel.status_description", e.to_string().as_str());
            }
        }
        // `client_guard` dropped here — entry removed from the registry.
        result
    }
    .instrument(client_span)
    .await
}

async fn forward_inner(
    client_stream: TcpStream,
    client_addr: SocketAddr,
    config: &Config,
    pools: &Arc<PoolManager>,
    listeners: &ListenerManager,
    auth: &Authenticator,
    client_guard: &clients::ClientGuard,
) -> anyhow::Result<()> {
    let mut client = Framed::new(client_stream, FrontendCodec::new());

    // Phase 1: SSL/GSS negotiation -> read StartupMessage.
    let startup = loop {
        match client.next().await.transpose()? {
            Some(FrontendMessage::SslRequest) => {
                trace!("responding N to SSLRequest");
                client.get_mut().write_all(b"N").await?;
            }
            Some(FrontendMessage::GssEncRequest) => {
                trace!("responding N to GssEncRequest");
                client.get_mut().write_all(b"N").await?;
            }
            Some(FrontendMessage::CancelRequest {
                process_id,
                secret_key,
            }) => {
                return forward_cancel(config, process_id, secret_key).await;
            }
            Some(FrontendMessage::Startup(s)) => break s,
            Some(other) => anyhow::bail!("unexpected message during startup: {other:?}"),
            None => return Ok(()),
        }
    };

    let database = startup_param(&startup.parameters, "database");
    let user = startup_param(&startup.parameters, "user");

    // Log client-supplied startup parameters that halephant does not
    // forward. In transaction mode, server connections are shared so
    // per-client startup parameters (options, application_name, etc.)
    // cannot be forwarded — they would leak to the next client. These
    // must be configured in the TOML per-user instead.
    for (key, value) in &startup.parameters {
        if key != "database" && key != "user" {
            trace!(%database, %user, param = %key, %value, "ignoring client startup parameter");
        }
    }

    // Record the client's identity on both the registry entry and the
    // enclosing `proxy.client` span now that it's known.
    client_guard.set_database_and_user(&database, &user);
    client_guard.set_state(ClientState::Authenticating);
    {
        let span = tracing::Span::current();
        span.record("db.namespace", database.as_str());
        span.record("user", user.as_str());
    }

    let mode = config
        .find_pool(&database)
        .map(|(_, _, p)| p.mode)
        .unwrap_or_default();
    let read_only = config
        .find_user(&database, &user)
        .is_some_and(crate::config::cluster::pool::user::UserConfig::is_read_only);
    trace!(%database, %user, ?mode, read_only, "client startup");

    // Phase 2-3: Authenticate, check out a server connection, and send
    // post-auth startup messages. Wrapped in a short-lived span so auth and
    // the initial checkout appear as a single trace without orphaned children.
    let mut guard = async {
        let result: anyhow::Result<_> = async {
            let (_, cluster, _) = config
                .find_pool(&database)
                .ok_or_else(|| anyhow::anyhow!("database {database:?} not configured"))?;
            let admin_addr = pools.resolve(&database, Routing::Primary)?;
            let admin_password = pools
                .pgpass()
                .lookup_addr(&admin_addr, &cluster.admin_database, &cluster.admin_user)
                .map(str::to_owned);
            if admin_password.is_none() {
                trace!(
                    %database,
                    admin_user = %cluster.admin_user,
                    admin_database = %cluster.admin_database,
                    %admin_addr,
                    "no .pgpass entry for admin connection (upstream may use trust auth)"
                );
            }
            auth.authenticate(
                &mut client,
                &database,
                &user,
                config,
                &admin_addr,
                admin_password.as_deref(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("authentication failed: {e}"))?;
            let mut guard = pools
                .checkout(client_guard, &database, &user, read_only)
                .await?;
            crate::messages::send_post_auth_startup(&mut client, guard.conn()).await?;
            Ok(guard)
        }
        .await;
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
    .instrument(info_span!(
        "proxy.setup",
        client.address = %client_addr,
        db.namespace = %database,
        user = %user,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    ))
    .await?;

    // Auth + initial checkout done — the client is now connected and
    // ready. Phase 2 will flip to `InTransaction` / `Waiting` during
    // forward and checkout respectively.
    client_guard.set_state(ClientState::Idle);

    // Phase 4: Set up multiplexed LISTEN if configured.
    let listen_mode = config
        .find_pool(&database)
        .and_then(|(_, _, p)| p.listen_mode)
        .unwrap_or_default();
    let mut notifications = if listen_mode == ListenMode::Multiplex {
        Some(listeners.subscribe(&database, &user))
    } else {
        None
    };

    // Phase 5: Forward based on pool mode.
    match mode {
        PoolMode::Session => {
            let node = guard.node().to_owned();
            // Session mode holds the backend for the entire client
            // lifetime — reflect that in the registry as InTransaction
            // (which stands in for "actively holding a backend").
            client_guard.set_state(ClientState::InTransaction);
            async {
                let result = proxy::session::forward(&mut client, guard.conn()).await;
                let span = tracing::Span::current();
                match &result {
                    Ok(()) => {
                        span.record("otel.status_code", "OK");
                    }
                    Err(e) => {
                        span.record("otel.status_code", "ERROR");
                        span.record("otel.status_description", e.to_string().as_str());
                    }
                }
                if result.is_ok() {
                    guard.checkin();
                }
                result
            }
            .instrument(info_span!(
                "proxy.session",
                client.address = %client_addr,
                db.namespace = %database,
                user = %user,
                server.address = node.as_str(),
                otel.status_code = tracing::field::Empty,
                otel.status_description = tracing::field::Empty,
            ))
            .await
        }
        PoolMode::Transaction => {
            guard.checkin();
            proxy::transaction::forward(
                &mut client,
                pools,
                client_guard,
                &mut notifications,
                listen_mode,
                &database,
                &user,
                read_only,
                config.otel.query_text,
            )
            .await
        }
    }
}

/// Forward a cancel request to every node in every configured cluster.
/// Only the server that owns the `(pid, secret_key)` pair will act on
/// it; everything else discards.
///
/// Fans every delivery out in parallel and caps each individual
/// connect at 500 ms. Cancels are time-sensitive — without the
/// timeout an unreachable node blocks on the OS TCP connect budget
/// (75–127 s on Linux); without the parallelism, N down nodes stack
/// those delays sequentially and push the cancel past the point a
/// user would notice.
async fn forward_cancel(config: &Config, process_id: i32, secret_key: i32) -> anyhow::Result<()> {
    const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

    trace!(process_id, secret_key, "forwarding cancel request");
    let mut tasks = tokio::task::JoinSet::new();
    for cluster in config.cluster.values() {
        for addr in &cluster.nodes {
            let addr = addr.clone();
            tasks.spawn(async move {
                let Ok(Ok(upstream)) =
                    tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr.as_str())).await
                else {
                    return;
                };
                let mut server = Framed::new(upstream, BackendCodec::new());
                let _ = server
                    .send(FrontendMessage::CancelRequest {
                        process_id,
                        secret_key,
                    })
                    .await;
            });
        }
    }
    // Drain the JoinSet so the function doesn't return before the
    // deliveries complete — the caller is the client's own connection
    // handler, which exits immediately after, and the tokio runtime
    // would cancel pending futures on task drop.
    while tasks.join_next().await.is_some() {}
    Ok(())
}

fn startup_param(params: &[(String, String)], key: &str) -> String {
    params
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}
