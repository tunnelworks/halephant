//! Wait-queue management. Queues are keyed by `(database, role)` and
//! shared across all users — any freed slot can wake any waiting user.

use crate::pool::PoolManager;
use crate::pool::types::{NodeRole, PoolKey, Routing, WaitKey};

impl PoolManager {
    /// Wake every queued waiter across all wait queues.
    pub(in crate::pool) fn wake_all_waiters(&self) {
        let mut inner = self.inner.lock();
        for queue in inner.waits.values_mut() {
            while let Some(waiter) = queue.waiters.pop_front() {
                let _ = waiter.tx.send(());
            }
        }
        inner.waits.retain(|_, q| !q.waiters.is_empty());
    }

    /// Wake one waiter that can use capacity freed on this node.
    /// Freed primary capacity benefits both write waiters and
    /// read-only waiters (who fall back to the primary when replicas
    /// are full). Freed replica capacity only benefits replica waiters.
    pub(in crate::pool) fn wake_one_for_node(&self, key: &PoolKey) {
        let cfg = self.config.load();
        let Some((cluster_name, _, _)) = cfg.find_pool(&key.database) else {
            return;
        };
        // Wake the freed node's own role first. Freed primary
        // capacity also benefits read-only waiters (who fall back
        // to the primary), so wake Replica second. Freed replica
        // capacity only helps replica waiters.
        let roles: &[Routing] = match self.role_for_node(cluster_name, &key.node) {
            NodeRole::Primary => &[Routing::Primary, Routing::Replica],
            NodeRole::Replica => &[Routing::Replica],
            NodeRole::Unknown => return,
        };
        let mut inner = self.inner.lock();
        for &role in roles {
            let wait_key = WaitKey {
                database: key.database.clone(),
                role,
            };
            if let Some(queue) = inner.waits.get_mut(&wait_key) {
                while let Some(waiter) = queue.waiters.pop_front() {
                    if waiter.tx.send(()).is_ok() {
                        return;
                    }
                }
            }
        }
    }
}
