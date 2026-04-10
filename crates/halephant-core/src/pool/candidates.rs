//! Candidate selection for pool checkouts. Given a `(database, user,
//! routing)` tuple, enumerates the pool keys that can satisfy the
//! request in least-connections order (with round-robin tie-break),
//! and provides the min/max helpers used by refill, checkout, and
//! scavenger.

use std::sync::atomic::Ordering;

use crate::config::Config;
use crate::pool::PoolManager;
use crate::pool::types::{NodeRole, PoolKey, Routing};

impl PoolManager {
    /// Classify an upstream node as the cluster's primary, one of its
    /// replicas, or unknown to current topology.
    pub(in crate::pool) fn role_for_node(&self, cluster_name: &str, node: &str) -> NodeRole {
        if self.topology.primary(cluster_name).as_deref() == Some(node) {
            NodeRole::Primary
        } else if self
            .topology
            .replicas(cluster_name)
            .iter()
            .any(|r| r == node)
        {
            NodeRole::Replica
        } else {
            NodeRole::Unknown
        }
    }

    /// The floor of connections that must exist for the given pool key,
    /// based on current topology and the user's `min_connections` config.
    /// Returns 0 if the database, user, or node is not currently recognized.
    pub(in crate::pool) fn min_for_key(&self, cfg: &Config, key: &PoolKey) -> u32 {
        let Some((cluster_name, _, pool_config)) = cfg.find_pool(&key.database) else {
            return 0;
        };
        let Some(user_config) = pool_config.user.get(&key.user) else {
            return 0;
        };
        match self.role_for_node(cluster_name, &key.node) {
            NodeRole::Primary => user_config.min_connections.primary,
            NodeRole::Replica => user_config.min_connections.replica,
            NodeRole::Unknown => 0,
        }
    }

    /// The ceiling of connections allowed for the given pool key. Returns
    /// 0 if the node is not currently recognized as primary or replica.
    pub(in crate::pool) fn max_for_key(&self, cfg: &Config, key: &PoolKey) -> u32 {
        let Some((cluster_name, _, pool_config)) = cfg.find_pool(&key.database) else {
            return 0;
        };
        match self.role_for_node(cluster_name, &key.node) {
            NodeRole::Primary => pool_config.max_connections.primary,
            NodeRole::Replica => pool_config.max_connections.replica,
            NodeRole::Unknown => 0,
        }
    }

    /// Ordered list of pool keys that can satisfy a checkout for the
    /// given `(database, user, routing)`. For `Primary`, a one-element
    /// vec with the current primary. For `Replica`, every replica node
    /// ordered **least-connections first** — the replica with the
    /// fewest currently-active checkouts is tried ahead of busier
    /// peers, so traffic naturally drains toward the replica with
    /// spare capacity. Ties (common on a cold cluster where every
    /// replica reads 0 active) are broken by rotating the input list
    /// with a shared round-robin counter, so equally-loaded replicas
    /// still spread across the fleet instead of always hammering
    /// whichever replica topology happens to list first.
    ///
    /// Why not plain round-robin? Once any replica approaches its
    /// `max_connections`, round-robin still routes every Nth checkout
    /// at the hot replica and forces it to block in the wait queue;
    /// least-connections picks the cool peer instead and avoids the
    /// wait entirely.
    ///
    /// Callers iterate until the first successful `try_checkout`.
    /// Returns an empty vec if the database isn't configured or if
    /// topology has no primary / no replicas for this routing intent.
    pub(in crate::pool) fn candidate_nodes(
        &self,
        cfg: &Config,
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
            Routing::Replica => {
                let replicas = self.topology.replicas(cluster_name);
                if replicas.is_empty() {
                    return Vec::new();
                }

                // Snapshot (key, active) under a single lock so the
                // load values form a consistent view. The lock is
                // released before the sort and before the caller
                // iterates, so checkout can race with other pool
                // mutations — that's inherent to any load-balancing
                // policy without reservation.
                let mut keys_with_load: Vec<(PoolKey, u32)> = {
                    let inner = self.inner.lock();
                    replicas
                        .iter()
                        .map(|addr| {
                            let key = PoolKey {
                                node: addr.clone(),
                                database: database.to_owned(),
                                user: user.to_owned(),
                            };
                            let load = inner.pools.get(&key).map_or(0, |p| p.active);
                            (key, load)
                        })
                        .collect()
                };

                // Rotate before the stable sort so equally-loaded
                // replicas (typically every replica on a cold
                // cluster) cycle through the fleet instead of
                // stacking on the first one.
                let start = self.replica_rr.fetch_add(1, Ordering::Relaxed) % keys_with_load.len();
                keys_with_load.rotate_left(start);
                keys_with_load.sort_by_key(|(_, load)| *load);

                keys_with_load.into_iter().map(|(k, _)| k).collect()
            }
        }
    }
}
