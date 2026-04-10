//! Connection checkout — the hot path through the pool. Walks
//! candidate nodes, falls back to opening new connections, and
//! enqueues on the shared `(database, user, role)` wait queue when
//! every candidate is at its per-node ceiling.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use tokio::sync::oneshot;
use tracing::{debug, trace};

use crate::clients;
use crate::connections::server;
use crate::errors::ResolveError;
use crate::o11y;
use crate::pool::PoolManager;
use crate::pool::types::{ConnGuard, PoolKey, Routing, TryCheckout, WaitKey};

impl PoolManager {
    /// Check out a server connection.
    ///
    /// When `read_only` is true, routes to a replica. When false, routes
    /// to the primary. Returns an RAII guard that auto-discards on drop;
    /// call `checkin` to return the connection for reuse.
    ///
    /// If every candidate node for the resolved role is at its per-node
    /// `max_connections`, the checkout enqueues on a shared
    /// `(database, user, role)` wait queue and blocks until either:
    ///
    /// 1. a connection is released on any candidate node and this
    ///    waiter is the next in the FIFO queue, or
    /// 2. `server.checkout_timeout` elapses since the first enqueue,
    ///    in which case the checkout fails with a classified
    ///    `checkout_timeout` error.
    ///
    /// The `ClientGuard` is flipped to `ClientState::Waiting` for the
    /// duration of any blocked wait and back to `ClientState::Idle` on
    /// success, timeout, or spurious wakeup.
    #[tracing::instrument(name = "pool.checkout", skip_all, err(Display), fields(
        db.namespace = %database,
        user = %user,
        halephant.client.id = client.id().as_u64(),
        read_only,
        pool.reused,
        pool.waited,
        otel.status_code,
        otel.status_description,
    ))]
    pub async fn checkout(
        self: &Arc<Self>,
        client: &clients::ClientGuard,
        database: &str,
        user: &str,
        read_only: bool,
    ) -> anyhow::Result<ConnGuard> {
        let routing = if read_only {
            Routing::Replica
        } else {
            Routing::Primary
        };
        let wait_key = WaitKey {
            database: database.to_owned(),
            user: user.to_owned(),
            role: routing,
        };

        // Snapshot config once for the whole checkout. An atomic swap
        // during this operation does not affect in-flight checkouts —
        // they finish under the old view and the next checkout picks
        // up the new one. `cfg` owns an `Arc<Config>`, so borrows
        // derived from it are valid for the function lifetime and
        // survive across `.await`.
        let cfg = self.config.load_full();
        let max_prepared_statements = cfg.server.max_prepared_statements;

        // Resolve config-derived parameters once. The per-pool
        // `checkout_timeout` override takes precedence over the
        // server-wide default.
        let (
            cluster_name,
            connect_timeout,
            idle_timeout,
            max_lifetime,
            checkout_timeout,
            upstream_database,
        ) = {
            let Some((name, cluster, pool_config)) = cfg.find_pool(database) else {
                return Err(ResolveError::UnknownDatabase {
                    database: database.to_owned(),
                }
                .into());
            };
            (
                name.to_owned(),
                cluster.connect_timeout,
                pool_config.idle_timeout,
                pool_config.max_lifetime,
                pool_config
                    .checkout_timeout
                    .unwrap_or(cfg.server.checkout_timeout),
                pool_config.database_name(database).to_owned(),
            )
        };

        // Look up user-specific connection parameters once for the batch.
        // These are only used if we actually open a new connection.
        let (upstream_user, user_params) = {
            let user_config = cfg.find_user(database, user);
            let upstream_user = user_config
                .map_or(user, |u| u.upstream_name(user))
                .to_owned();
            let params = user_config
                .map(|u| u.parameters.clone())
                .unwrap_or_default();
            (upstream_user, params)
        };

        let checkout_start = Instant::now();
        let mut queue_start: Option<Instant> = None;

        async {
            loop {
                // Recompute candidates each loop iteration so a
                // topology change (e.g., primary failover) during a
                // wait is picked up on retry.
                let candidates = self.candidate_nodes(&cfg, database, user, routing);
                if candidates.is_empty() {
                    return Err(match routing {
                        Routing::Primary => ResolveError::NoPrimary {
                            cluster: cluster_name.clone(),
                        },
                        Routing::Replica => ResolveError::NoReplica {
                            cluster: cluster_name.clone(),
                        },
                    }
                    .into());
                }

                // 1. Try every candidate atomically before giving up.
                //    `any_full` tracks whether any candidate was at its
                //    ceiling (meaning a slot will eventually free up and
                //    waiting is productive); `last_connect_error` keeps
                //    the most recent connect/auth failure so we can
                //    surface it if no candidate is worth waiting on.
                let mut any_full = false;
                let mut last_connect_error: Option<anyhow::Error> = None;
                for key in &candidates {
                    let max = self.max_for_key(&cfg, key);
                    match self.try_checkout(key, max, idle_timeout, max_lifetime) {
                        TryCheckout::TookIdle(idle) => {
                            if let Some(queued_at) = queue_start {
                                o11y::metrics::record_wait_duration(
                                    queued_at.elapsed(),
                                    database,
                                    user,
                                    routing,
                                );
                            }
                            o11y::metrics::record_checkout(
                                checkout_start,
                                database,
                                user,
                                &key.node,
                            );
                            let span = tracing::Span::current();
                            span.record("pool.reused", true);
                            span.record("pool.waited", queue_start.is_some());
                            debug!(
                                database,
                                user,
                                node = %key.node,
                                "reusing idle connection",
                            );
                            return Ok(ConnGuard {
                                pools: Arc::clone(self),
                                key: key.clone(),
                                conn: Some(idle.conn),
                            });
                        }
                        TryCheckout::MayOpen => {
                            let password =
                                self.lookup_password(&key.node, &upstream_database, &upstream_user);
                            match server::connect_server(
                                &key.node,
                                &upstream_database,
                                &upstream_user,
                                password.as_deref(),
                                &user_params,
                                max_prepared_statements,
                                connect_timeout,
                            )
                            .await
                            {
                                Ok(conn) => {
                                    if let Some(queued_at) = queue_start {
                                        o11y::metrics::record_wait_duration(
                                            queued_at.elapsed(),
                                            database,
                                            user,
                                            routing,
                                        );
                                    }
                                    o11y::metrics::record_checkout(
                                        checkout_start,
                                        database,
                                        user,
                                        &key.node,
                                    );
                                    let span = tracing::Span::current();
                                    span.record("pool.reused", false);
                                    span.record("pool.waited", queue_start.is_some());
                                    debug!(
                                        database,
                                        user,
                                        node = %key.node,
                                        "opened new server connection",
                                    );
                                    return Ok(ConnGuard {
                                        pools: Arc::clone(self),
                                        key: key.clone(),
                                        conn: Some(conn),
                                    });
                                }
                                Err(e) => {
                                    // Connect/auth failed on this
                                    // candidate — release the slot we
                                    // speculatively claimed and fall
                                    // through to the next candidate so
                                    // a transient failure on one replica
                                    // doesn't forfeit a healthy one
                                    // later in the round-robin order.
                                    self.discard_internal(key);
                                    last_connect_error = Some(e);
                                }
                            }
                        }
                        TryCheckout::Full => {
                            any_full = true;
                        }
                    }
                }

                // 2. Every candidate either refused connections or hit
                //    its ceiling. If nothing was Full, there's no slot
                //    to wait for — surface the connect error directly.
                //    Otherwise fall through to the enqueue-and-wait
                //    path so the Full candidate can wake us when a
                //    slot frees up.
                if !any_full && let Some(e) = last_connect_error {
                    return Err(e);
                }

                // 3. Every candidate is at capacity. Enqueue and wait.
                //    Record Waiting state and the target role only while
                //    actually blocked so fast checkouts don't flicker
                //    through Waiting in admin output.
                let rx = {
                    let (tx, rx) = oneshot::channel();
                    let enqueued_at = Instant::now();
                    let mut inner = self.inner.lock();
                    inner
                        .waits
                        .entry(wait_key.clone())
                        .or_default()
                        .waiters
                        .push_back((enqueued_at, tx));
                    rx
                };
                client.set_waiting_for(Some(clients::WaitTarget {
                    database: database.to_owned(),
                    user: user.to_owned(),
                    role: match routing {
                        Routing::Primary => clients::WaitRole::Primary,
                        Routing::Replica => clients::WaitRole::Replica,
                    },
                }));
                client.set_state(clients::ClientState::Waiting);
                // First enqueue stamps the queue clock; subsequent
                // re-enqueues after a spurious wakeup keep the
                // original value so the metric reflects total queue
                // time, not per-wait segments.
                queue_start.get_or_insert_with(Instant::now);

                // Single timeout budget across the full checkout —
                // spurious wakeups don't get a fresh clock. Measured
                // from `checkout_start` so pre-enqueue candidate
                // attempts count toward the overall deadline.
                let elapsed = checkout_start.elapsed();
                let remaining = checkout_timeout.saturating_sub(elapsed);
                if remaining.is_zero() {
                    client.set_waiting_for(None);
                    client.set_state(clients::ClientState::Idle);
                    anyhow::bail!(
                        "checkout timed out after {checkout_timeout:?} for {database}/{user}",
                    );
                }
                let wake = tokio::time::timeout(remaining, rx).await;
                client.set_waiting_for(None);
                client.set_state(clients::ClientState::Idle);

                if wake.is_err() {
                    // Outer `tokio::time::timeout` elapsed.
                    anyhow::bail!(
                        "checkout timed out after {checkout_timeout:?} for {database}/{user}",
                    );
                }
                // Shutdown wakes everyone via `wake_all_waiters`. A
                // waiter that was spuriously woken by an unrelated
                // checkin right as shutdown started would also loop
                // here, so the check happens after the wake.
                if self.shutting_down.load(Ordering::Acquire) {
                    anyhow::bail!("halephant is shutting down");
                }
                // Woken — loop top re-runs the candidate check.
            }
        }
        .await
        .inspect(|_| {
            tracing::Span::current().record("otel.status_code", "OK");
        })
        .inspect_err(|e| {
            // Classify the failure so `db.client.connection.errors` time
            // series break down cleanly in dashboards.
            let msg = e.to_string();
            let error_type = if let Some(resolve_err) = e.downcast_ref::<ResolveError>() {
                match resolve_err {
                    ResolveError::UnknownDatabase { .. } => "unknown_database",
                    ResolveError::NoPrimary { .. } => "no_primary",
                    ResolveError::NoReplica { .. } => "no_replica",
                }
            } else if msg.contains("shutting down") {
                "shutting_down"
            } else if msg.contains("checkout timed out") {
                "checkout_timeout"
            } else {
                "connect_failed"
            };
            o11y::metrics::record_checkout_error(database, user, error_type);
            let span = tracing::Span::current();
            span.record("otel.status_code", "ERROR");
            span.record("otel.status_description", "checkout failed");
        })
    }

    /// Atomic check-and-claim for a single pool key.
    ///
    /// Discards expired idle connections inline (idle_timeout /
    /// max_lifetime). Increments `pool.active` on success so a concurrent
    /// checkout can't double-claim the same slot. Holds the inner lock
    /// for the duration of one HashMap lookup + VecDeque pop — no
    /// `.await` while locked.
    fn try_checkout(
        &self,
        key: &PoolKey,
        max: u32,
        idle_timeout: std::time::Duration,
        max_lifetime: std::time::Duration,
    ) -> TryCheckout {
        let mut inner = self.inner.lock();
        let pool = inner.pools.entry(key.clone()).or_default();

        // Drain expired idle entries until we find a usable one or the
        // queue is empty. Each iteration drops the front entry.
        while let Some(idle) = pool.idle.pop_front() {
            if idle.idle_since.elapsed() > idle_timeout {
                trace!(
                    database = %key.database,
                    user = %key.user,
                    "discarding idle connection (idle timeout)",
                );
                continue;
            }
            if idle.conn.created_at.elapsed() > max_lifetime {
                trace!(
                    database = %key.database,
                    user = %key.user,
                    "discarding idle connection (max lifetime)",
                );
                continue;
            }
            pool.active += 1;
            return TryCheckout::TookIdle(Box::new(idle));
        }

        if max == 0 || pool.total() < max {
            pool.active += 1;
            TryCheckout::MayOpen
        } else {
            TryCheckout::Full
        }
    }

    /// Release a slot that was speculatively claimed by a failed
    /// connect attempt or a dropped [`ConnGuard`]. Wakes a waiter if
    /// there is one — the just-freed slot is a retry opportunity.
    pub(in crate::pool) fn discard_internal(&self, key: &PoolKey) {
        {
            let mut inner = self.inner.lock();
            if let Some(pool) = inner.pools.get_mut(key) {
                pool.active = pool.active.saturating_sub(1);
                trace!(
                    node = key.node,
                    database = key.database,
                    user = key.user,
                    "connection discarded"
                );
            }
        }
        // Freeing an active slot may allow a waiter to grow the pool.
        self.wake_one_for_node(key);
    }
}
