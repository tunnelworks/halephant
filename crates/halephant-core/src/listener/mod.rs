//! Multiplexed LISTEN/NOTIFY — shared listener connections with fan-out.
//!
//! Instead of pinning a server connection per client, one shared connection
//! per (database, user) pool subscribes to all channels and broadcasts
//! notifications to subscribed clients via `tokio::sync::broadcast`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio_util::codec::Framed;
use tracing::{debug, trace, warn};

use crate::auth::pgpass::Pgpass;
use crate::config::cluster::pool::user::UserParameters;
use crate::proto::backend::BackendMessage;
use crate::proto::codec::BackendCodec;
use crate::proto::frontend::FrontendMessage;

use crate::topology::TopologyManager;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A notification received from the shared listener connection.
#[derive(Debug, Clone)]
pub struct Notification {
    pub process_id: i32,
    pub channel: String,
    pub payload: String,
}

/// Manages shared listener connections for all pools.
pub struct ListenerManager {
    pools: Mutex<HashMap<(String, String), Arc<ListenerPool>>>,
    topology: Arc<TopologyManager>,
    config: Arc<ArcSwap<crate::config::Config>>,
    pgpass: Arc<Pgpass>,
}

/// A shared listener for a single (database, user) pool.
pub struct ListenerPool {
    tx: broadcast::Sender<Notification>,
    refs: Mutex<HashMap<String, usize>>,
    /// Command channel feeding the background listener task. Unbounded
    /// because LISTEN/UNLISTEN commands are inherently bounded by the
    /// number of distinct channels the application subscribes to
    /// (usually small, stable, and known up front) — so delivery
    /// matters more than backpressure. A bounded channel here would
    /// drop commands on `try_send` during a burst and leave the
    /// ref-count map inconsistent with PostgreSQL's actual subscription
    /// state.
    cmd_tx: mpsc::UnboundedSender<ListenCmd>,
}

/// Client-side subscription state. Tracks which channels this client
/// subscribes to and provides a `recv` method for the forwarding loop.
pub struct ClientNotifications {
    pool: Arc<ListenerPool>,
    rx: broadcast::Receiver<Notification>,
    channels: HashSet<String>,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

enum ListenCmd {
    Listen(String),
    Unlisten(String),
}

// ---------------------------------------------------------------------------
// ListenerManager
// ---------------------------------------------------------------------------

impl ListenerManager {
    pub fn new(
        config: Arc<ArcSwap<crate::config::Config>>,
        topology: Arc<TopologyManager>,
        pgpass: Arc<Pgpass>,
    ) -> Self {
        Self {
            pools: Mutex::new(HashMap::new()),
            topology,
            config,
            pgpass,
        }
    }

    /// Get or create a shared listener pool for (database, user). Spawns the
    /// background listener task on first access.
    pub fn subscribe(&self, database: &str, user: &str) -> ClientNotifications {
        let key = (database.to_owned(), user.to_owned());
        let pool = {
            let mut pools = self.pools.lock();

            // Evict a stale entry whose background task has exited
            // (e.g. after MAX_CONSECUTIVE_FAILURES). Without this,
            // the existing entry's broadcast channel stays alive
            // (the Sender is still in the Arc) but no task is
            // driving it, so recv() blocks forever.
            if pools.get(&key).is_some_and(|p| p.cmd_tx.is_closed()) {
                pools.remove(&key);
            }

            pools
                .entry(key)
                .or_insert_with(|| {
                    let (tx, _) = broadcast::channel(4096);
                    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
                    let pool = Arc::new(ListenerPool {
                        tx,
                        refs: Mutex::new(HashMap::new()),
                        cmd_tx,
                    });
                    let pool2 = Arc::clone(&pool);
                    let topo = Arc::clone(&self.topology);
                    let cfg = Arc::clone(&self.config);
                    let pgpass = Arc::clone(&self.pgpass);
                    let db = database.to_owned();
                    let u = user.to_owned();
                    tokio::spawn(async move {
                        listener_task(pool2, topo, cfg, pgpass, db, u, cmd_rx).await;
                    });
                    pool
                })
                .clone()
        };
        let rx = pool.tx.subscribe();
        ClientNotifications {
            pool,
            rx,
            channels: HashSet::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// ListenerPool — ref-counted channel subscriptions
// ---------------------------------------------------------------------------

impl ListenerPool {
    fn add_ref(&self, channel: &str) {
        let mut refs = self.refs.lock();
        let count = refs.entry(channel.to_owned()).or_insert(0);
        *count += 1;
        if *count == 1 {
            // Unbounded send: only fails if the receiver has been
            // dropped, which happens when the background listener
            // task gave up after MAX_CONSECUTIVE_FAILURES. Log at
            // warn so the operator sees subscriptions silently
            // missing; the ListenerPool is effectively dead at this
            // point and new subscribers won't receive notifications
            // regardless.
            if let Err(e) = self.cmd_tx.send(ListenCmd::Listen(channel.to_owned())) {
                warn!(channel, %e, "listener task is gone, LISTEN command dropped");
            }
        }
    }

    fn remove_ref(&self, channel: &str) {
        let mut refs = self.refs.lock();
        if let Some(count) = refs.get_mut(channel) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                refs.remove(channel);
                // `remove_ref` runs in `ClientNotifications::drop`,
                // so we can't await — unbounded `send` is what lets
                // this stay sync while also being lossless. Same
                // dead-task fallback as `add_ref`; a failed UNLISTEN
                // leaks a server-side subscription on an already
                // unusable listener connection.
                if let Err(e) = self.cmd_tx.send(ListenCmd::Unlisten(channel.to_owned())) {
                    warn!(channel, %e, "listener task is gone, UNLISTEN command dropped");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ClientNotifications — per-client subscription state
// ---------------------------------------------------------------------------

impl ClientNotifications {
    /// Subscribe this client to a channel.
    pub fn listen(&mut self, channel: &str) {
        if self.channels.insert(channel.to_owned()) {
            self.pool.add_ref(channel);
        }
    }

    /// Unsubscribe this client from a channel.
    pub fn unlisten(&mut self, channel: &str) {
        if self.channels.remove(channel) {
            self.pool.remove_ref(channel);
        }
    }

    /// Unsubscribe this client from all channels.
    pub fn unlisten_all(&mut self) {
        for ch in self.channels.drain() {
            self.pool.remove_ref(&ch);
        }
    }

    /// Wait for a notification on a channel this client subscribes to.
    /// Returns `None` when the broadcast channel is closed.
    pub async fn recv(&mut self) -> Option<BackendMessage> {
        loop {
            match self.rx.recv().await {
                Ok(n) if self.channels.contains(&n.channel) => {
                    return Some(BackendMessage::NotificationResponse(
                        crate::proto::backend::Notification {
                            process_id: n.process_id,
                            channel: n.channel,
                            payload: n.payload,
                        },
                    ));
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(missed = n, "client lagged behind notification stream");
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

impl Drop for ClientNotifications {
    fn drop(&mut self) {
        self.unlisten_all();
    }
}

// ---------------------------------------------------------------------------
// Background listener task
// ---------------------------------------------------------------------------

async fn listener_task(
    pool: Arc<ListenerPool>,
    topology: Arc<TopologyManager>,
    config: Arc<ArcSwap<crate::config::Config>>,
    pgpass: Arc<Pgpass>,
    database: String,
    user: String,
    mut cmd_rx: mpsc::UnboundedReceiver<ListenCmd>,
) {
    const MAX_CONSECUTIVE_FAILURES: u32 = 10;
    let mut consecutive_failures = 0u32;

    loop {
        consecutive_failures += 1;

        // Reload the config snapshot each iteration so a hot-reload
        // that changes `connect_timeout` or moves the database to a
        // different cluster is picked up on reconnect.
        let cfg = config.load_full();
        let resolved = cfg
            .find_pool(&database)
            .map(|(name, cluster, pool_config)| {
                // Resolve the upstream identity here so the listener
                // honours the same alias / `database`-override semantics
                // as the rest of the pool. The user lookup may miss if
                // the user was removed by a hot-reload between accept and
                // reconnect — fall back to the client-facing name in that
                // case so the connect attempt fails with a clear server
                // error rather than a silent identity mismatch.
                let user_config = pool_config.user.get(&user);
                let upstream_database = pool_config.database_name(&database).to_owned();
                let upstream_user = user_config
                    .map_or(user.as_str(), |u| u.upstream_name(&user))
                    .to_owned();
                let params = user_config
                    .map(|u| u.parameters.clone())
                    .unwrap_or_default();
                (
                    name,
                    cluster.connect_timeout,
                    upstream_database,
                    upstream_user,
                    params,
                )
            });

        let err = match resolved {
            None => "database not configured".to_owned(),
            Some((cluster_name, connect_timeout, upstream_database, upstream_user, params)) => {
                match topology.primary(cluster_name) {
                    None => "no primary discovered".to_owned(),
                    Some(upstream_addr) => {
                        // Resolve the password from `.pgpass` for the
                        // upstream identity. Done per reconnect so a
                        // pgpass refresh (or a topology flip to a node
                        // with a different host) is picked up.
                        let password = pgpass
                            .lookup_addr(&upstream_addr, &upstream_database, &upstream_user)
                            .map(str::to_owned);

                        let started = std::time::Instant::now();
                        match run_listener(
                            &pool,
                            &upstream_addr,
                            &upstream_database,
                            &upstream_user,
                            password.as_deref(),
                            &params,
                            &mut cmd_rx,
                            connect_timeout,
                        )
                        .await
                        {
                            Ok(()) => break,
                            Err(e) => {
                                // If the listener ran for a meaningful
                                // duration, the failure is transient —
                                // start a fresh budget.
                                if started.elapsed() > Duration::from_secs(5) {
                                    consecutive_failures = 0;
                                }
                                e.to_string()
                            }
                        }
                    }
                }
            }
        };
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
            tracing::error!(
                %err, %database, %user,
                "listener: giving up after {MAX_CONSECUTIVE_FAILURES} consecutive failures"
            );
            return;
        }
        warn!(%err, %database, %user, consecutive_failures, "listener: retrying...");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

// Private helper with a single caller — bundling these into a struct
// would just push the same eight values one indirection away.
#[allow(clippy::too_many_arguments)]
async fn run_listener(
    pool: &ListenerPool,
    upstream_addr: &str,
    upstream_database: &str,
    upstream_user: &str,
    password: Option<&str>,
    params: &UserParameters,
    cmd_rx: &mut mpsc::UnboundedReceiver<ListenCmd>,
    connect_timeout: Duration,
) -> anyhow::Result<()> {
    // `max_prepared = 0` because the listener never issues prepared
    // statements — only LISTEN/UNLISTEN/Query.
    let server = crate::connections::server::connect_server(
        upstream_addr,
        upstream_database,
        upstream_user,
        password,
        params,
        0,
        connect_timeout,
    )
    .await?;
    let mut conn = server.framed;

    debug!(
        database = upstream_database,
        user = upstream_user,
        "shared listener connection established"
    );

    // Re-subscribe to all channels that have active references (reconnection).
    let channels_to_restore: Vec<String> = {
        let refs = pool.refs.lock();
        refs.keys().cloned().collect()
    };
    for channel in &channels_to_restore {
        let query = format!("LISTEN \"{}\"", channel.replace('"', "\"\""));
        conn.send(FrontendMessage::Query(query)).await?;
        drain_response(&mut conn, &pool.tx).await?;
    }
    if !channels_to_restore.is_empty() {
        debug!(
            channels = channels_to_restore.len(),
            "re-subscribed after reconnect"
        );
    }

    // Main loop: process commands and forward notifications.
    //
    // `biased` polls `cmd_rx` before `conn.next()` so a pending
    // LISTEN/UNLISTEN is registered with PostgreSQL as soon as
    // possible after a client subscribes — minimizing the window
    // where a client thinks it's listening but the server isn't yet
    // forwarding events on that channel. The trade-off is that a
    // sustained burst of subscription churn could let in-flight
    // notifications accumulate in the broadcast ring buffer (4096
    // entries) until subscribers fall behind with `Lagged`. In
    // practice the subscription rate is bounded by client
    // connect/disconnect events and stays orders of magnitude below
    // the notification rate; `drain_response` also forwards any
    // notifications interleaved with each command's response, so
    // commands cannot fully starve the notification path.
    loop {
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => match cmd {
                Some(ListenCmd::Listen(ch)) => {
                    let query = format!("LISTEN \"{}\"", ch.replace('"', "\"\""));
                    conn.send(FrontendMessage::Query(query)).await?;
                    drain_response(&mut conn, &pool.tx).await?;
                    trace!(channel = %ch, "shared listener: LISTEN");
                }
                Some(ListenCmd::Unlisten(ch)) => {
                    let query = format!("UNLISTEN \"{}\"", ch.replace('"', "\"\""));
                    conn.send(FrontendMessage::Query(query)).await?;
                    drain_response(&mut conn, &pool.tx).await?;
                    trace!(channel = %ch, "shared listener: UNLISTEN");
                }
                None => return Ok(()), // all handles dropped
            },
            msg = conn.next() => match msg.transpose()? {
                Some(BackendMessage::NotificationResponse(n)) => {
                    let _ = pool.tx.send(Notification {
                        process_id: n.process_id,
                        channel: n.channel,
                        payload: n.payload,
                    });
                }
                Some(_) => {} // ignore notices, parameter status, etc.
                None => anyhow::bail!("listener connection closed"),
            },
        }
    }
}

/// Drain the server response after a LISTEN/UNLISTEN command, forwarding any
/// notifications that arrive between CommandComplete and ReadyForQuery.
async fn drain_response(
    conn: &mut Framed<TcpStream, BackendCodec>,
    tx: &broadcast::Sender<Notification>,
) -> anyhow::Result<()> {
    loop {
        match conn.next().await.transpose()? {
            Some(BackendMessage::ReadyForQuery(_)) => return Ok(()),
            Some(BackendMessage::NotificationResponse(n)) => {
                let _ = tx.send(Notification {
                    process_id: n.process_id,
                    channel: n.channel,
                    payload: n.payload,
                });
            }
            Some(BackendMessage::ErrorResponse(e)) => {
                anyhow::bail!(
                    "LISTEN/UNLISTEN error: {}",
                    e.message().unwrap_or("unknown")
                );
            }
            None => anyhow::bail!("connection closed during LISTEN/UNLISTEN"),
            // CommandComplete, notices, etc.
            Some(_) => {}
        }
    }
}
