//! Wait-queue bookkeeping for the checkout path. All waiters for a
//! given `(database, user, role)` share a single FIFO queue; this
//! module owns the wake paths that free waiters on checkin, refill,
//! reset failure, and topology changes.

use crate::pool::PoolManager;
use crate::pool::types::{NodeRole, PoolKey, Routing, WaitKey};

impl PoolManager {
    /// Wake every currently queued waiter across every wait queue.
    /// Used by `drain_node` on topology changes so waiters retry with
    /// fresh candidates. Empty entries are pruned on the way out so
    /// the map doesn't retain stale `(database, user, role)` keys.
    pub(in crate::pool) fn wake_all_waiters(&self) {
        let mut inner = self.inner.lock();
        for queue in inner.waits.values_mut() {
            while let Some((_enqueued_at, tx)) = queue.waiters.pop_front() {
                let _ = tx.send(());
            }
        }
        inner.waits.retain(|_, q| !q.waiters.is_empty());
    }

    /// Resolve the wait key for a pool key based on current topology.
    /// Returns `None` if the node's role is unknown (topology hasn't
    /// classified it yet) or the database isn't configured.
    ///
    /// Loads the latest config snapshot inline because the callers
    /// (`wake_one_for_node` from background reset/discard paths) do
    /// not carry a snapshot of their own — we want them to always
    /// dispatch against the most current role assignment.
    fn wait_key_for_node(&self, key: &PoolKey) -> Option<WaitKey> {
        let cfg = self.config.load();
        let (cluster_name, _, _) = cfg.find_pool(&key.database)?;
        let role = match self.role_for_node(cluster_name, &key.node) {
            NodeRole::Primary => Routing::Primary,
            NodeRole::Replica => Routing::Replica,
            NodeRole::Unknown => return None,
        };
        Some(WaitKey {
            database: key.database.clone(),
            user: key.user.clone(),
            role,
        })
    }

    /// Wake one waiter on the wait queue that matches this pool key's
    /// current role. Skips senders whose receivers have been dropped
    /// (cancelled waiters) lazily.
    ///
    /// Called from every code path that frees a slot on a node or adds
    /// an idle connection to it: reset-and-return, discard, refill,
    /// drain.
    pub(in crate::pool) fn wake_one_for_node(&self, key: &PoolKey) {
        let Some(wait_key) = self.wait_key_for_node(key) else {
            return;
        };
        let mut inner = self.inner.lock();
        let Some(queue) = inner.waits.get_mut(&wait_key) else {
            return;
        };
        while let Some((_enqueued_at, tx)) = queue.waiters.pop_front() {
            if tx.send(()).is_ok() {
                return;
            }
            // Sender failed — receiver was dropped (cancelled waiter);
            // skip to the next one.
        }
    }
}
