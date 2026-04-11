//! Config-derived limit lookups for checkout, refill, and scavenger.

use crate::config::Config;
use crate::pool::PoolManager;
use crate::pool::types::{NodeRole, PoolKey};

/// Resolved connection limits for a single candidate, computed once
/// from config + topology so callers don't repeat the lookup.
pub(in crate::pool) struct Limits {
    pub(in crate::pool) pool_max: usize,
    pub(in crate::pool) user_max: Option<usize>,
}

impl Limits {
    /// Whether the per-user limit allows another connection.
    pub(in crate::pool) fn user_has_capacity(&self, user_total: usize) -> bool {
        match self.user_max {
            Some(max) => user_total < max,
            None => true,
        }
    }

    /// Whether the per-node pool limit allows another connection.
    pub(in crate::pool) fn pool_has_capacity(&self, node_total: usize) -> bool {
        node_total < self.pool_max
    }
}

impl PoolManager {
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

    /// Resolve both pool-level and user-level limits for a candidate.
    pub(in crate::pool) fn resolve_limits(&self, cfg: &Config, key: &PoolKey) -> Limits {
        let Some((cluster_name, _, pool_config)) = cfg.find_pool(&key.database) else {
            return Limits {
                pool_max: 0,
                user_max: Some(0),
            };
        };
        let role = self.role_for_node(cluster_name, &key.node);
        let pool_max = match role {
            NodeRole::Primary => pool_config.max_connections.primary,
            NodeRole::Replica => pool_config.max_connections.replica,
            NodeRole::Unknown => 0,
        } as usize;
        let user_max = pool_config.user.get(&key.user).and_then(|u| {
            let limits = u.max_connections.as_ref()?;
            Some(match role {
                NodeRole::Primary => limits.primary,
                NodeRole::Replica => limits.replica,
                NodeRole::Unknown => 0,
            } as usize)
        });
        Limits { pool_max, user_max }
    }

    /// Min connections floor for a pool key. Used by the scavenger.
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
}
