use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::oneshot;

use crate::connections::server::ServerConn;

/// Routing intent for connection resolution. Also used as the role
/// discriminator in [`WaitKey`] — "I'm waiting for a writer" vs "I'm
/// waiting for a reader".
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Routing {
    Primary,
    Replica,
}

/// Identity of a wait queue. The `(database, user)` portion ties the
/// queue to the client's identity; the `role` discriminates primary from
/// replica waiters. Replica queues are **shared** across every replica
/// node serving the same `(database, user)` — any replica with spare
/// capacity can serve any waiter from the corresponding queue, so load
/// distribution happens at wake time rather than enqueue time.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::pool) struct WaitKey {
    pub(in crate::pool) database: String,
    pub(in crate::pool) user: String,
    pub(in crate::pool) role: Routing,
}

/// FIFO queue of clients blocked on pool capacity for a particular
/// `(database, user, role)`. Each entry is a `(enqueue_time, sender)`
/// pair so the admin endpoint can report the oldest wait duration; the
/// receiver is awaited inside the blocked checkout. A dropped receiver
/// (cancelled waiter) is detected lazily at wake time.
#[derive(Default)]
pub(in crate::pool) struct WaitQueue {
    pub(in crate::pool) waiters: VecDeque<(Instant, oneshot::Sender<()>)>,
}

/// Result of a single-node checkout attempt. Used by the candidate loop
/// in [`PoolManager::checkout`] to decide whether to take the connection,
/// open a new one, or move on to the next candidate.
///
/// The `TookIdle` variant boxes `IdleConn` — it holds a framed TCP
/// stream that's much larger than the other unit variants, and boxing
/// keeps the stack footprint of the match arms small.
pub(in crate::pool) enum TryCheckout {
    /// A usable idle connection was popped. `active` has been
    /// incremented; the caller must wrap the conn in a `ConnGuard`.
    TookIdle(Box<IdleConn>),
    /// The pool has spare capacity. `active` has been incremented; the
    /// caller must open a new upstream connection and release the slot
    /// on failure via `discard_internal`.
    MayOpen,
    /// The pool is at its per-node ceiling. No state changes; the
    /// caller should try the next candidate or enqueue.
    Full,
}

/// Classification of an upstream node within its cluster as known to current
/// topology. Used by per-key min/max lookups.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::pool) enum NodeRole {
    Primary,
    Replica,
    Unknown,
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

pub(in crate::pool) struct Inner {
    /// Physical per-node connection pools.
    pub(in crate::pool) pools: HashMap<PoolKey, Pool>,
    /// Per-(database, user, role) wait queues shared across every node
    /// that serves a given role. Entries are created lazily on first
    /// wait and kept around for the process lifetime.
    pub(in crate::pool) waits: HashMap<WaitKey, WaitQueue>,
}

/// Pool key includes the upstream node address so each node has its own pool.
#[derive(Hash, Eq, PartialEq, Clone)]
pub(in crate::pool) struct PoolKey {
    pub(in crate::pool) node: String,
    pub(in crate::pool) database: String,
    pub(in crate::pool) user: String,
}

#[derive(Default)]
pub(in crate::pool) struct Pool {
    pub(in crate::pool) idle: VecDeque<IdleConn>,
    pub(in crate::pool) active: u32,
    pub(in crate::pool) resetting: u32,
}

pub(in crate::pool) struct IdleConn {
    pub(in crate::pool) conn: ServerConn,
    pub(in crate::pool) idle_since: Instant,
}

impl Pool {
    pub(in crate::pool) fn total(&self) -> u32 {
        self.active + self.idle.len() as u32 + self.resetting
    }
}

// ---------------------------------------------------------------------------
// Guards
// ---------------------------------------------------------------------------

/// RAII guard for a checked-out server connection. Automatically discards the
/// connection (decrementing pool counters) when dropped. Call `checkin` to
/// return the connection to the pool for reuse instead.
pub struct ConnGuard {
    pub(in crate::pool) pools: Arc<super::PoolManager>,
    pub(in crate::pool) key: PoolKey,
    pub(in crate::pool) conn: Option<ServerConn>,
}

impl ConnGuard {
    /// Access the underlying server connection.
    pub fn conn(&mut self) -> &mut ServerConn {
        self.conn.as_mut().expect("connection already consumed")
    }

    /// Returns the upstream node address for this connection.
    pub fn node(&self) -> &str {
        &self.key.node
    }

    /// Return the connection to the pool for reuse. The reset runs in a
    /// background task so the caller is not blocked. The pool slot is freed
    /// immediately so concurrent checkouts are not blocked.
    pub fn checkin(mut self) {
        if let Some(conn) = self.conn.take() {
            {
                let mut inner = self.pools.inner.lock();
                if let Some(pool) = inner.pools.get_mut(&self.key) {
                    pool.active = pool.active.saturating_sub(1);
                    pool.resetting += 1;
                }
            }
            let pools = Arc::clone(&self.pools);
            let key = self.key.clone();
            tokio::spawn(async move {
                // Guard ensures resetting is decremented even if the task is
                // cancelled (e.g., during runtime shutdown).
                let guard = ResetGuard {
                    pools: Arc::clone(&pools),
                    key: key.clone(),
                    done: false,
                };
                pools.reset_and_return(key, conn).await;
                // Mark as done so the guard skips its Drop cleanup.
                guard.disarm();
            });
        }
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        if self.conn.take().is_some() {
            self.pools.discard_internal(&self.key);
        }
    }
}

/// Drop guard for the background reset task. If the task is cancelled before
/// `reset_and_return` completes, this decrements `resetting` so the counter
/// doesn't leak.
struct ResetGuard {
    pools: Arc<super::PoolManager>,
    key: PoolKey,
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
                pool.resetting = pool.resetting.saturating_sub(1);
            }
        }
    }
}
