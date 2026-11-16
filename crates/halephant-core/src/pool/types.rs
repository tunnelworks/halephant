use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::connections::server::ServerConn;
use crate::errors::ResolveError;

// ---------------------------------------------------------------------------
// ConnId — unique identifier for a physical connection
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub struct ConnId(u64);

impl ConnId {
    pub fn next() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

// ---------------------------------------------------------------------------
// Public enums
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Routing {
    Primary,
    Replica,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::pool) enum NodeRole {
    Primary,
    Replica,
    Unknown,
}

// ---------------------------------------------------------------------------
// CheckoutError — typed checkout failure taxonomy
// ---------------------------------------------------------------------------

/// A checkout failure originating inside the pool, as opposed to a
/// resolution failure ([`ResolveError`]) or an upstream connect error
/// (a bare [`anyhow::Error`] from the connect path).
///
/// Metric classification downcasts to this type instead of matching on
/// error prose, so upstream text such as PostgreSQL's `57P03` message
/// ("the database system is shutting down") can never masquerade as
/// halephant's own shutdown. The Display text is operator-facing and
/// stays stable across releases.
#[derive(Debug, thiserror::Error)]
pub enum CheckoutError {
    /// No connection became available within `checkout_timeout`.
    #[error("checkout timed out after {timeout:?} for {database}/{user}")]
    Timeout {
        timeout: Duration,
        database: String,
        user: String,
    },

    /// Halephant is shutting down; queued waiters abort immediately.
    #[error("halephant is shutting down")]
    ShuttingDown,

    /// The user has no `[cluster.*.pool.*.user.*]` entry for the
    /// database. Checkout and refill both refuse — mirroring the
    /// frontend's `is_user_allowed` gate — instead of falling back to
    /// the client-facing name, which could mix connections
    /// authenticated as different PostgreSQL roles in one pool.
    #[error("user {user:?} is not configured for database {database:?}")]
    UserNotConfigured { database: String, user: String },
}

/// Classifies a checkout failure into the low-cardinality `error.type`
/// metric label recorded by the checkout path. Typed errors map to
/// their own labels; anything else — upstream connect failures
/// included — falls back to `connect_failed`.
pub fn classify_checkout_error(e: &anyhow::Error) -> &'static str {
    if let Some(resolve_err) = e.downcast_ref::<ResolveError>() {
        match resolve_err {
            ResolveError::UnknownDatabase { .. } => "unknown_database",
            ResolveError::NoPrimary { .. } => "no_primary",
            ResolveError::NoReplica { .. } => "no_replica",
        }
    } else if let Some(checkout_err) = e.downcast_ref::<CheckoutError>() {
        match checkout_err {
            CheckoutError::Timeout { .. } => "checkout_timeout",
            CheckoutError::ShuttingDown => "shutting_down",
            CheckoutError::UserNotConfigured { .. } => "user_not_configured",
        }
    } else {
        "connect_failed"
    }
}

// ---------------------------------------------------------------------------
// Wait queue
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::pool) struct WaitKey {
    pub(in crate::pool) database: String,
    pub(in crate::pool) role: Routing,
}

#[derive(Default)]
pub(in crate::pool) struct WaitQueue {
    pub(in crate::pool) waiters: VecDeque<Waiter>,
}

pub(in crate::pool) struct Waiter {
    pub(in crate::pool) enqueued_at: Instant,
    pub(in crate::pool) user: String,
    pub(in crate::pool) tx: oneshot::Sender<()>,
}

// ---------------------------------------------------------------------------
// Checkout result
// ---------------------------------------------------------------------------

pub(in crate::pool) enum CandidateResult {
    GotIdle { key: PoolKey, conn: Box<IdleConn> },
    Claimed { key: PoolKey, id: ConnId },
    AllFull,
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

pub(in crate::pool) struct Inner {
    pub(in crate::pool) pools: HashMap<PoolKey, Pool>,
    pub(in crate::pool) waits: HashMap<WaitKey, WaitQueue>,
}

impl Inner {
    /// Total physical connections for `(node, database)` across all users.
    /// Derived from collection lengths — cannot drift.
    pub(in crate::pool) fn node_total(&self, node: &str, database: &str) -> usize {
        self.pools
            .iter()
            .filter(|(k, _)| k.node == node && k.database == database)
            .map(|(_, p)| p.total())
            .sum()
    }
}

#[derive(Hash, Eq, PartialEq, Clone)]
pub(in crate::pool) struct PoolKey {
    pub(in crate::pool) node: String,
    pub(in crate::pool) database: String,
    pub(in crate::pool) user: String,
}

#[derive(Default)]
pub(in crate::pool) struct Pool {
    pub(in crate::pool) idle: VecDeque<IdleConn>,
    pub(in crate::pool) active: HashSet<ConnId>,
    pub(in crate::pool) resetting: HashSet<ConnId>,
}

impl Pool {
    pub(in crate::pool) fn total(&self) -> usize {
        self.active.len() + self.idle.len() + self.resetting.len()
    }

    /// Pop the first usable idle connection, discarding expired ones.
    pub(in crate::pool) fn pop_idle(
        &mut self,
        idle_timeout: Duration,
        max_lifetime: Duration,
    ) -> Option<IdleConn> {
        while let Some(idle) = self.idle.pop_front() {
            if !idle.is_expired(idle_timeout, max_lifetime) {
                return Some(idle);
            }
        }
        None
    }
}

pub(in crate::pool) struct IdleConn {
    pub(in crate::pool) id: ConnId,
    pub(in crate::pool) conn: ServerConn,
    pub(in crate::pool) idle_since: Instant,
}

impl IdleConn {
    pub(in crate::pool) fn is_expired(
        &self,
        idle_timeout: Duration,
        max_lifetime: Duration,
    ) -> bool {
        self.idle_since.elapsed() > idle_timeout || self.conn.created_at.elapsed() > max_lifetime
    }
}

// ---------------------------------------------------------------------------
// Guards
// ---------------------------------------------------------------------------

pub struct ConnGuard {
    pub(in crate::pool) pools: Arc<super::PoolManager>,
    pub(in crate::pool) key: PoolKey,
    pub(in crate::pool) id: ConnId,
    pub(in crate::pool) conn: Option<ServerConn>,
}

impl ConnGuard {
    pub fn conn(&mut self) -> &mut ServerConn {
        self.conn.as_mut().expect("connection already consumed")
    }

    pub fn node(&self) -> &str {
        &self.key.node
    }

    pub fn checkin(mut self) {
        if let Some(conn) = self.conn.take() {
            {
                let mut inner = self.pools.inner.lock();
                if let Some(pool) = inner.pools.get_mut(&self.key) {
                    pool.active.remove(&self.id);
                    pool.resetting.insert(self.id);
                }
            }
            let pools = Arc::clone(&self.pools);
            let key = self.key.clone();
            let id = self.id;
            tokio::spawn(async move {
                let guard = ResetGuard {
                    pools: Arc::clone(&pools),
                    key: key.clone(),
                    id,
                    done: false,
                };
                pools.reset_and_return(key, id, conn).await;
                guard.disarm();
            });
        }
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        if self.conn.take().is_some() {
            self.pools.discard_internal(&self.key, self.id);
        }
    }
}

struct ResetGuard {
    pools: Arc<super::PoolManager>,
    key: PoolKey,
    id: ConnId,
    done: bool,
}

impl ResetGuard {
    fn disarm(mut self) {
        self.done = true;
    }
}

impl Drop for ResetGuard {
    fn drop(&mut self) {
        if !self.done {
            let mut inner = self.pools.inner.lock();
            if let Some(pool) = inner.pools.get_mut(&self.key) {
                pool.resetting.remove(&self.id);
            }
        }
    }
}
