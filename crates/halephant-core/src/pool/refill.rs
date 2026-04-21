//! Background connection management: warm-up, shortfall refills,
//! scavenger ticks, and the reset-and-return path taken by
//! [`crate::pool::types::ConnGuard::checkin`].

use std::sync::Arc;
use std::time::Instant;

use tracing::{Instrument, debug, warn};

use crate::connections::reset::reset_connection;
use crate::connections::server::{self, ServerConn};
use crate::pool::PoolManager;
use crate::pool::types::{ConnId, IdleConn, PoolKey};

/// Maximum in-flight connection opens per call to
/// [`PoolManager::spawn_connections`]. Because PostgreSQL's postmaster
/// accepts and forks backends serially, slamming a single node with many
/// simultaneous connects queues every SCRAM handshake behind the fork
/// loop and inflates per-connection latency. A small cap keeps each
/// connect close to its minimum wall time while still parallelizing
/// across different nodes (each batch gets its own semaphore).
const PER_NODE_REFILL_CONCURRENCY: usize = 8;

impl PoolManager {
    /// Open idle connections at startup based on `min_connections` config.
    pub async fn warm_up(self: &Arc<Self>) {
        let shortfalls = self.shortfalls();
        if shortfalls.is_empty() {
            debug!("warm-up: nothing to do");
            return;
        }

        let mut tasks = tokio::task::JoinSet::new();
        for (key, n) in shortfalls {
            let this = Arc::clone(self);
            tasks.spawn(async move { this.spawn_connections(&key, n).await });
        }

        let mut opened = 0u32;
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(n) => opened += n,
                Err(e) => warn!("warm-up task panicked: {e}"),
            }
        }
        debug!(opened, "warm-up complete");
    }

    /// Remove idle connections that exceed `max_lifetime` or whose socket is
    /// dead, then apply `idle_timeout` only to connections in excess of
    /// `min_connections` for that pool entry.
    ///
    /// `max_lifetime` and dead-socket detection always apply because those
    /// represent unhealthy connections. `idle_timeout` represents excess
    /// capacity that can be released — so it must respect the floor set by
    /// `min_connections`, otherwise warm-up connections decay away even when
    /// the user explicitly asked for a minimum.
    ///
    /// Returns the number of connections removed across all pool entries.
    /// Normally invoked from [`PoolManager::run_scavenger`]; exposed so tests
    /// can drive it directly without spinning up the full background task.
    pub fn scavenge_idle(&self) -> u32 {
        let cfg = self.config.load_full();
        let mut freed: Vec<(PoolKey, u32)> = Vec::new();
        let mut removed = 0u32;

        {
            let mut inner = self.inner.lock();

            for (key, pool) in &mut inner.pools {
                let pool_config = cfg.find_pool(&key.database).map(|(_, _, p)| p);
                let idle_timeout =
                    pool_config.map_or(std::time::Duration::from_mins(5), |p| p.idle_timeout);
                let max_lifetime =
                    pool_config.map_or(std::time::Duration::from_hours(1), |p| p.max_lifetime);

                let before = pool.idle.len();

                // Remove dead sockets and connections past max_lifetime
                // unconditionally (regardless of min_connections floor).
                pool.idle.retain(|idle| {
                    idle.conn.created_at.elapsed() <= max_lifetime
                        && crate::connections::sock::is_alive(idle.conn.framed.get_ref())
                });

                // Apply idle_timeout only to excess capacity above
                // the min_connections floor.
                let min_idle = self.min_for_key(&cfg, key) as usize;
                let removable = pool.idle.len().saturating_sub(min_idle);
                let mut dropped = 0usize;
                pool.idle.retain(|idle| {
                    if dropped >= removable {
                        return true;
                    }
                    if idle.idle_since.elapsed() > idle_timeout {
                        dropped += 1;
                        return false;
                    }
                    true
                });

                let n = (before - pool.idle.len()) as u32;
                if n > 0 {
                    freed.push((key.clone(), n));
                }
                removed += n;
            }

            inner.waits.retain(|_, q| !q.waiters.is_empty());
        }

        // Removing idle connections frees physical capacity — wake
        // any waiter that was blocked on node-level limits.
        for (key, n) in &freed {
            for _ in 0..*n {
                self.wake_one_for_node(key);
            }
        }

        removed
    }

    /// Enumerate every configured pool key whose connection count is below
    /// its `min_for_key` floor, paired with the deficit. Walks the
    /// configured `(cluster, database, user)` tuples and resolves the
    /// expected primary and replica nodes from current topology — so the
    /// result includes brand-new replicas that have no pool entry yet.
    fn shortfalls(&self) -> Vec<(PoolKey, u32)> {
        let cfg = self.config.load_full();
        let inner = self.inner.lock();
        let mut result = Vec::new();

        for (cluster_name, cluster) in &cfg.cluster {
            let primary = self.topology.primary(cluster_name);
            let replicas = self.topology.replicas(cluster_name);

            for (db_name, pool_config) in &cluster.pool {
                for (user_name, user_config) in &pool_config.user {
                    // Primary shortfall.
                    if user_config.min_connections.primary > 0
                        && let Some(ref primary_addr) = primary
                    {
                        let key = PoolKey {
                            node: primary_addr.clone(),
                            database: db_name.clone(),
                            user: user_name.clone(),
                        };
                        let total = inner.pools.get(&key).map_or(0, |p| p.total() as u32);
                        let want = user_config.min_connections.primary;
                        if total < want {
                            result.push((key, want - total));
                        }
                    }

                    // Replica shortfall — per replica node, not distributed.
                    if user_config.min_connections.replica > 0 {
                        for replica_addr in &replicas {
                            let key = PoolKey {
                                node: replica_addr.clone(),
                                database: db_name.clone(),
                                user: user_name.clone(),
                            };
                            let total = inner.pools.get(&key).map_or(0, |p| p.total() as u32);
                            let want = user_config.min_connections.replica;
                            if total < want {
                                result.push((key, want - total));
                            }
                        }
                    }
                }
            }
        }

        result
    }

    /// Open up to `n` new connections for the given pool key in parallel
    /// and insert each successful one into the idle queue. Returns the
    /// number opened. Per-connection failures are logged and counted as
    /// zero so a single bad node does not abort the rest of the batch.
    ///
    /// Wrapped in a `pool.refill` span so the per-connection `pool.connect`
    /// spans nest under it in OTel traces.
    #[tracing::instrument(name = "pool.refill", skip_all, fields(
        server.address = %key.node,
        db.namespace = %key.database,
        user = %key.user,
        count = n,
        opened = tracing::field::Empty,
        otel.status_code,
    ))]
    async fn spawn_connections(self: &Arc<Self>, key: &PoolKey, n: u32) -> u32 {
        if n == 0 {
            return 0;
        }

        // Resolve the upstream identity once for the batch. Connection
        // failures from a missing config or user are surfaced via warn,
        // not by silently returning 0 — that would mask config drift.
        let cfg = self.config.load_full();
        let (upstream_database, upstream_user, params, max_prepared, connect_timeout) = {
            let Some((_, cluster_config, pool_config)) = cfg.find_pool(&key.database) else {
                warn!(
                    database = %key.database,
                    "refill: database not configured",
                );
                return 0;
            };
            let Some(user_config) = pool_config.user.get(&key.user) else {
                warn!(
                    database = %key.database,
                    user = %key.user,
                    "refill: user not configured",
                );
                return 0;
            };
            (
                pool_config.database_name(&key.database).to_owned(),
                user_config.upstream_name(&key.user).to_owned(),
                user_config.parameters.clone(),
                cfg.server.max_prepared_statements,
                cluster_config.connect_timeout,
            )
        };

        let password = self.lookup_password(&key.node, &upstream_database, &upstream_user);

        // Cap in-flight opens to the same upstream node. PostgreSQL's
        // postmaster accepts and forks backends serially — slamming it
        // with tons of simultaneous connects queues every SCRAM handshake
        // behind the fork loop, so each individual `pool.connect` span
        // stretches to hundreds of milliseconds. Capping the batch keeps
        // the per-connect wall time close to the minimum and gets the
        // earliest connections into the idle queue sooner. This semaphore
        // is local to one `spawn_connections` call, so different nodes
        // still run fully in parallel.
        let permits = Arc::new(tokio::sync::Semaphore::new(PER_NODE_REFILL_CONCURRENCY));

        let parent_span = tracing::Span::current();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..n {
            let this = Arc::clone(self);
            let key = key.clone();
            let upstream_database = upstream_database.clone();
            let upstream_user = upstream_user.clone();
            let password = password.clone();
            let params = params.clone();
            let permits = Arc::clone(&permits);

            tasks.spawn(
                async move {
                    // Acquire before connecting so permits throttle the
                    // actual handshake work, not just scheduling.
                    let _permit = permits
                        .acquire_owned()
                        .await
                        .expect("refill semaphore is never closed");
                    match server::connect_server(
                        &key.node,
                        &upstream_database,
                        &upstream_user,
                        password.as_deref(),
                        &params,
                        max_prepared,
                        connect_timeout,
                    )
                    .await
                    {
                        Ok(conn) => {
                            {
                                let mut inner = this.inner.lock();
                                let pool = inner.pools.entry(key.clone()).or_default();
                                pool.idle.push_back(IdleConn {
                                    id: ConnId::next(),
                                    conn,
                                    idle_since: Instant::now(),
                                });
                            }
                            // New idle connection — wake a waiter if
                            // anyone is queued for this role.
                            this.wake_one_for_node(&key);
                            true
                        }
                        Err(e) => {
                            warn!(
                                database = key.database,
                                user = key.user,
                                node = key.node,
                                %e,
                                "refill connection failed",
                            );
                            false
                        }
                    }
                }
                .instrument(parent_span.clone()),
            );
        }

        let mut opened = 0u32;
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(true) => opened += 1,
                Ok(false) => {}
                Err(e) => warn!(%e, "refill task panicked"),
            }
        }
        let span = tracing::Span::current();
        span.record("opened", opened);
        // Status is OK as long as at least one connection made it into the
        // pool. A batch where every task failed is recorded as error.
        if opened > 0 || n == 0 {
            span.record("otel.status_code", "OK");
        } else {
            span.record("otel.status_code", "ERROR");
        }
        opened
    }

    /// Run the idle scavenger and refill loop at a fixed interval.
    ///
    /// Each tick prunes expired idle connections (preserving the
    /// `min_connections` floor), then refills any pool entry below its
    /// floor by fanning every `spawn_connections` call into a
    /// `JoinSet` and awaiting them concurrently — matching
    /// [`PoolManager::warm_up`]. Running them in series here would
    /// let a single slow node blow most of the 30-second budget and
    /// leave unrelated pools under their `min_connections` floor.
    /// The per-tick state is logged at debug level so the loop is
    /// visible without needing trace-level filters.
    pub async fn run_scavenger(self: &Arc<Self>) {
        let interval = std::time::Duration::from_secs(30);
        loop {
            tokio::time::sleep(interval).await;
            let removed = self.scavenge_idle();
            let shortfalls = self.shortfalls();
            debug!(removed, refills = shortfalls.len(), "scavenger tick");
            let mut tasks = tokio::task::JoinSet::new();
            for (key, n) in shortfalls {
                let this = Arc::clone(self);
                tasks.spawn(async move { this.spawn_connections(&key, n).await });
            }
            while let Some(result) = tasks.join_next().await {
                if let Err(e) = result {
                    warn!(%e, "scavenger refill task panicked");
                }
            }
        }
    }

    pub(in crate::pool) async fn reset_and_return(
        self: &Arc<Self>,
        key: PoolKey,
        id: ConnId,
        mut conn: ServerConn,
    ) {
        match reset_connection(&mut conn).await {
            Ok(()) => {
                let returned = {
                    let mut inner = self.inner.lock();
                    if let Some(pool) = inner.pools.get_mut(&key) {
                        pool.resetting.remove(&id);
                        pool.idle.push_back(IdleConn {
                            id,
                            conn,
                            idle_since: Instant::now(),
                        });
                        true
                    } else {
                        false
                    }
                };
                if returned {
                    self.wake_one_for_node(&key);
                }
            }
            Err(e) => {
                debug!(
                    node = key.node,
                    database = key.database,
                    user = key.user,
                    %e,
                    "reset failed, discarding connection"
                );
                {
                    let mut inner = self.inner.lock();
                    if let Some(pool) = inner.pools.get_mut(&key) {
                        pool.resetting.remove(&id);
                    }
                }
                self.wake_one_for_node(&key);
            }
        }
    }
}
