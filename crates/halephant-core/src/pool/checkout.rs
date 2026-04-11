//! Connection checkout — the hot path through the pool.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tracing::debug;

use crate::clients;
use crate::connections::server;
use crate::errors::ResolveError;
use crate::o11y;
use crate::pool::PoolManager;
use crate::pool::types::{CandidateResult, ConnGuard, ConnId, PoolKey, Routing, WaitKey, Waiter};

impl PoolManager {
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
            role: routing,
        };

        let cfg = self.config.load_full();
        let max_prepared_statements = cfg.server.max_prepared_statements;

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
                let candidates = self.resolve_candidates(&cfg, database, user, routing);
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

                match self.try_candidates(&cfg, &candidates, idle_timeout, max_lifetime) {
                    CandidateResult::GotIdle { key, conn } => {
                        record_checkout_success(
                            &key,
                            checkout_start,
                            queue_start,
                            database,
                            user,
                            routing,
                            true,
                        );
                        return Ok(ConnGuard {
                            pools: Arc::clone(self),
                            key,
                            id: conn.id,
                            conn: Some(conn.conn),
                        });
                    }
                    CandidateResult::Claimed { key, id } => {
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
                                record_checkout_success(
                                    &key,
                                    checkout_start,
                                    queue_start,
                                    database,
                                    user,
                                    routing,
                                    false,
                                );
                                return Ok(ConnGuard {
                                    pools: Arc::clone(self),
                                    key,
                                    id,
                                    conn: Some(conn),
                                });
                            }
                            Err(e) => {
                                self.discard_internal(&key, id);
                                if candidates.len() == 1 {
                                    return Err(e);
                                }
                            }
                        }
                    }
                    CandidateResult::AllFull => {}
                }

                let rx = {
                    let (tx, rx) = oneshot::channel();
                    let mut inner = self.inner.lock();
                    inner
                        .waits
                        .entry(wait_key.clone())
                        .or_default()
                        .waiters
                        .push_back(Waiter {
                            enqueued_at: Instant::now(),
                            user: user.to_owned(),
                            tx,
                        });
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
                queue_start.get_or_insert_with(Instant::now);

                let remaining = checkout_timeout.saturating_sub(checkout_start.elapsed());
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
                    anyhow::bail!(
                        "checkout timed out after {checkout_timeout:?} for {database}/{user}",
                    );
                }
                if self.shutting_down.load(Ordering::Acquire) {
                    anyhow::bail!("halephant is shutting down");
                }
            }
        }
        .await
        .inspect(|_| {
            tracing::Span::current().record("otel.status_code", "OK");
        })
        .inspect_err(|e| {
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

    // -----------------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------------

    fn resolve_candidates(
        &self,
        cfg: &crate::config::Config,
        database: &str,
        user: &str,
        routing: Routing,
    ) -> Vec<PoolKey> {
        let Some((cluster_name, _, _)) = cfg.find_pool(database) else {
            return Vec::new();
        };
        match routing {
            Routing::Primary => {
                let Some(primary) = self.topology.primary(cluster_name) else {
                    return Vec::new();
                };
                vec![PoolKey {
                    node: primary,
                    database: database.to_owned(),
                    user: user.to_owned(),
                }]
            }
            Routing::Replica => self
                .topology
                .replicas(cluster_name)
                .iter()
                .map(|addr| PoolKey {
                    node: addr.clone(),
                    database: database.to_owned(),
                    user: user.to_owned(),
                })
                .collect(),
        }
    }

    fn try_candidates(
        &self,
        cfg: &crate::config::Config,
        candidates: &[PoolKey],
        idle_timeout: Duration,
        max_lifetime: Duration,
    ) -> CandidateResult {
        let mut inner = self.inner.lock();

        // Rank by active count (least-connections). Rotate for tie-break.
        let mut ranked: Vec<&PoolKey> = candidates.iter().collect();
        if ranked.len() > 1 {
            let start = self.replica_rr.fetch_add(1, Ordering::Relaxed) % ranked.len();
            ranked.rotate_left(start);
            ranked.sort_by_key(|k| inner.pools.get(*k).map_or(0, |p| p.active.len()));
        }

        for key in ranked {
            let limits = self.resolve_limits(cfg, key);

            // Try to reuse an idle connection. Scoped so the mutable
            // pool borrow drops before the node_total scan.
            let idle = {
                let pool = inner.pools.entry(key.clone()).or_default();
                let idle = pool.pop_idle(idle_timeout, max_lifetime);
                if let Some(ref c) = idle {
                    pool.active.insert(c.id);
                }
                idle
            };
            if let Some(idle) = idle {
                return CandidateResult::GotIdle {
                    key: key.clone(),
                    conn: Box::new(idle),
                };
            }

            let user_total = inner.pools.get(key).map_or(0, super::types::Pool::total);
            if !limits.user_has_capacity(user_total) {
                continue;
            }

            let node_total = inner.node_total(&key.node, &key.database);
            if !limits.pool_has_capacity(node_total) && !Self::evict_idle_for_node(&mut inner, key)
            {
                continue;
            }

            let id = ConnId::next();
            inner
                .pools
                .entry(key.clone())
                .or_default()
                .active
                .insert(id);
            return CandidateResult::Claimed {
                key: key.clone(),
                id,
            };
        }

        CandidateResult::AllFull
    }

    fn evict_idle_for_node(inner: &mut super::types::Inner, key: &PoolKey) -> bool {
        let victim = inner
            .pools
            .iter()
            .filter(|(k, p)| k.node == key.node && k.database == key.database && !p.idle.is_empty())
            .min_by_key(|(_, p)| p.idle.front().map_or(Instant::now(), |i| i.idle_since))
            .map(|(k, _)| k.clone());
        if let Some(ref vk) = victim
            && let Some(pool) = inner.pools.get_mut(vk)
        {
            pool.idle.pop_front();
        }
        victim.is_some()
    }

    pub(in crate::pool) fn discard_internal(&self, key: &PoolKey, id: ConnId) {
        {
            let mut inner = self.inner.lock();
            if let Some(pool) = inner.pools.get_mut(key) {
                pool.active.remove(&id);
            }
        }
        self.wake_one_for_node(key);
    }
}

fn record_checkout_success(
    key: &PoolKey,
    checkout_start: Instant,
    queue_start: Option<Instant>,
    database: &str,
    user: &str,
    routing: Routing,
    reused: bool,
) {
    if let Some(queued_at) = queue_start {
        o11y::metrics::record_wait_duration(queued_at.elapsed(), database, user, routing);
    }
    o11y::metrics::record_checkout(checkout_start, database, user, &key.node);
    let span = tracing::Span::current();
    span.record("pool.reused", reused);
    span.record("pool.waited", queue_start.is_some());
    debug!(database, user, node = %key.node, reused, "checkout complete");
}
