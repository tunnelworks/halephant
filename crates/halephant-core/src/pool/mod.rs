#![cfg_attr(test, allow(clippy::unwrap_used))]
mod candidates;
mod checkout;
pub mod prepared;
mod refill;
pub mod types;
mod wait;

pub use types::{ConnGuard, Routing};

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use tracing::debug;

use crate::auth::pgpass::Pgpass;
use crate::config::Config;
use crate::errors::ResolveError;
use crate::o11y;
use crate::topology;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Manages connection pools keyed by (node, database, user). Each upstream
/// node gets its own pool, enabling per-node failover and separate
/// primary/replica connection limits.
pub struct PoolManager {
    pub(in crate::pool) inner: Mutex<types::Inner>,
    /// Hot-reloadable configuration handle. Operations that span
    /// multiple reads snapshot via `self.config.load_full()` at the
    /// start for intra-operation consistency; transient reads (e.g.,
    /// from background tasks like `reset_and_return`) can load fresh
    /// on each access to pick up the latest config.
    pub(in crate::pool) config: Arc<ArcSwap<Config>>,
    pub(in crate::pool) topology: Arc<topology::TopologyManager>,
    pub(in crate::pool) pgpass: Arc<Pgpass>,
    /// Round-robin counter for replica selection across clusters.
    pub(in crate::pool) replica_rr: AtomicUsize,
    /// Global prepared statement store shared across all clients.
    pub(crate) stmt_store: Mutex<prepared::StatementStore>,
    /// Set by [`PoolManager::shutdown`] to signal queued waiters to
    /// abort. Sits on a dedicated atomic so the wait loop can check it
    /// without reacquiring the inner lock.
    pub(in crate::pool) shutting_down: AtomicBool,
}

// ---------------------------------------------------------------------------
// PoolManager — construction and public API
// ---------------------------------------------------------------------------

impl PoolManager {
    pub fn new(
        config: Arc<ArcSwap<Config>>,
        topology: Arc<topology::TopologyManager>,
        pgpass: Arc<Pgpass>,
    ) -> Self {
        Self {
            inner: Mutex::new(types::Inner {
                pools: HashMap::new(),
                waits: HashMap::new(),
            }),
            config,
            topology,
            pgpass,
            replica_rr: AtomicUsize::new(0),
            stmt_store: Mutex::new(prepared::StatementStore::new()),
            shutting_down: AtomicBool::new(false),
        }
    }

    /// Access the shared config handle. Used by the reload path to
    /// atomically swap in a new `Config` via `store`; callers that just
    /// want to read the current config should use `load_full()` on the
    /// returned handle for intra-operation consistency.
    pub fn config(&self) -> &Arc<ArcSwap<Config>> {
        &self.config
    }

    /// Check whether a canonical statement exists in the global store.
    #[doc(hidden)]
    pub fn has_prepared(&self, canon: &str) -> bool {
        self.stmt_store.lock().get(canon).is_some()
    }

    /// Access the pgpass store.
    pub fn pgpass(&self) -> &Pgpass {
        &self.pgpass
    }

    /// Signal that halephant is shutting down: set the shutdown flag
    /// and wake every queued waiter so they return a classified error
    /// immediately instead of sitting on the queue until `checkout_timeout`
    /// expires or they get force-aborted.
    ///
    /// Called from the graceful shutdown path in `main` right after the
    /// accept loop exits, before draining client tasks.
    pub fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        self.wake_all_waiters();
    }

    /// Resolve a routing target for a database. Returns the upstream
    /// address as an owned string.
    ///
    /// Returns a specific [`ResolveError`] if the database is unknown, the
    /// primary has not been discovered, or no healthy replica is available.
    pub fn resolve(&self, database: &str, routing: Routing) -> Result<String, ResolveError> {
        let cfg = self.config.load_full();
        let (cluster_name, _, _) =
            cfg.find_pool(database)
                .ok_or_else(|| ResolveError::UnknownDatabase {
                    database: database.to_owned(),
                })?;

        match routing {
            Routing::Primary => {
                self.topology
                    .primary(cluster_name)
                    .ok_or_else(|| ResolveError::NoPrimary {
                        cluster: cluster_name.to_owned(),
                    })
            }
            Routing::Replica => {
                let replicas = self.topology.replicas(cluster_name);
                if replicas.is_empty() {
                    return Err(ResolveError::NoReplica {
                        cluster: cluster_name.to_owned(),
                    });
                }
                // `resolve` is a read-only lookup used by admin/metrics
                // callers that want _an_ address, not a reservation.
                // Snapshot the RR counter with `load` instead of
                // `fetch_add` so these calls don't steal a rotation
                // slot from the checkout path and skew replica
                // distribution.
                let idx = self.replica_rr.load(Ordering::Relaxed) % replicas.len();
                Ok(replicas[idx].clone())
            }
        }
    }

    /// Look up the upstream password from the `.pgpass` file for the given
    /// connection parameters. Returns `None` if no matching entry exists.
    pub(in crate::pool) fn lookup_password(
        &self,
        addr: &str,
        database: &str,
        user: &str,
    ) -> Option<String> {
        self.pgpass
            .lookup_addr(addr, database, user)
            .map(str::to_owned)
    }

    /// Drain idle connections for a specific node across all databases/users.
    /// Called when topology detects a node role change or unreachability.
    pub fn drain_node(&self, node: &str) {
        let drained = {
            let mut inner = self.inner.lock();
            let mut drained = 0u32;
            for (key, pool) in &mut inner.pools {
                if key.node == node {
                    drained += pool.idle.len() as u32;
                    pool.idle.clear();
                }
            }
            drained
        };
        if drained > 0 {
            debug!(node, drained, "drained idle connections for node");
        }
        self.wake_all_waiters();
    }

    pub fn drain_all(&self) {
        let mut inner = self.inner.lock();
        let mut drained = 0u32;
        for pool in inner.pools.values_mut() {
            drained += pool.idle.len() as u32;
            pool.idle.clear();
        }
        if drained > 0 {
            debug!(drained, "drained all idle connections for shutdown");
        }
    }

    /// Returns a snapshot of all pool states for metrics reporting.
    pub fn pool_stats(&self) -> Vec<(o11y::metrics::PoolKeyInfo, o11y::metrics::PoolStats)> {
        let inner = self.inner.lock();
        inner
            .pools
            .iter()
            .map(|(key, pool)| {
                (
                    o11y::metrics::PoolKeyInfo {
                        node: key.node.clone(),
                        database: key.database.clone(),
                        user: key.user.clone(),
                    },
                    o11y::metrics::PoolStats {
                        active: pool.active.len() as u32,
                        idle: pool.idle.len() as u32,
                        resetting: pool.resetting.len() as u32,
                    },
                )
            })
            .collect()
    }

    /// Returns a snapshot of every non-empty wait queue, broken down
    /// per user within each `(database, role)` queue.
    #[must_use]
    pub fn queue_stats(&self) -> Vec<o11y::metrics::QueueInfo> {
        let now = Instant::now();
        let inner = self.inner.lock();
        let mut result = Vec::new();
        for (key, queue) in &inner.waits {
            // Aggregate per-user within the shared queue.
            let mut per_user: HashMap<&str, (u32, Instant)> = HashMap::new();
            for waiter in &queue.waiters {
                let entry = per_user
                    .entry(&waiter.user)
                    .or_insert((0, waiter.enqueued_at));
                entry.0 += 1;
                if waiter.enqueued_at < entry.1 {
                    entry.1 = waiter.enqueued_at;
                }
            }
            for (user, (depth, oldest)) in per_user {
                result.push(o11y::metrics::QueueInfo {
                    database: key.database.clone(),
                    user: user.to_owned(),
                    role: key.role,
                    depth,
                    oldest_wait_secs: now.saturating_duration_since(oldest).as_secs_f64(),
                });
            }
        }
        result
    }

    /// Returns the configured connection limits for each pool.
    pub fn pool_limits(&self) -> Vec<o11y::metrics::PoolLimits> {
        let cfg = self.config.load_full();
        cfg.cluster
            .values()
            .flat_map(|cluster| {
                cluster
                    .pool
                    .iter()
                    .map(|(db_name, pool)| o11y::metrics::PoolLimits {
                        database: pool.database_name(db_name).to_owned(),
                        max_primary: pool.max_connections.primary,
                        max_replica: pool.max_connections.replica,
                    })
            })
            .collect()
    }
}
