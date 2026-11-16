//! Config-derived limit and identity lookups for checkout, refill, and
//! scavenger.

use std::time::Duration;

use crate::config::Config;
use crate::config::cluster::pool::user::UserParameters;
use crate::connections::server::{self, ServerConn};
use crate::errors::ResolveError;
use crate::pool::PoolManager;
use crate::pool::types::{CheckoutError, NodeRole, PoolKey};

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

/// The upstream connect identity for a `(database, user)` pair:
/// everything the connect path needs beyond the node address.
///
/// [`ConnectSpec::resolve`] is the single owner of the identity
/// derivation, consumed by both checkout and refill, so a pool can
/// never mix connections authenticated as different PostgreSQL roles.
#[derive(Clone)]
pub(in crate::pool) struct ConnectSpec {
    pub(in crate::pool) upstream_database: String,
    pub(in crate::pool) upstream_user: String,
    pub(in crate::pool) params: UserParameters,
    pub(in crate::pool) max_prepared: u32,
    pub(in crate::pool) connect_timeout: Duration,
}

impl ConnectSpec {
    /// Resolves the upstream connect identity from config. A missing
    /// database maps to [`ResolveError::UnknownDatabase`]; a missing
    /// user is refused with [`CheckoutError::UserNotConfigured`] —
    /// matching the frontend's `is_user_allowed` gate — never mapped
    /// to the client-facing name.
    pub(in crate::pool) fn resolve(
        cfg: &Config,
        database: &str,
        user: &str,
    ) -> anyhow::Result<Self> {
        let Some((_, cluster_config, pool_config)) = cfg.find_pool(database) else {
            return Err(ResolveError::UnknownDatabase {
                database: database.to_owned(),
            }
            .into());
        };
        let Some(user_config) = pool_config.user.get(user) else {
            return Err(CheckoutError::UserNotConfigured {
                database: database.to_owned(),
                user: user.to_owned(),
            }
            .into());
        };
        Ok(Self {
            upstream_database: pool_config.database_name(database).to_owned(),
            upstream_user: user_config.upstream_name(user).to_owned(),
            params: user_config.parameters.clone(),
            max_prepared: cfg.server.max_prepared_statements,
            connect_timeout: cluster_config.connect_timeout,
        })
    }
}

impl PoolManager {
    /// Opens one upstream connection to `node` with the resolved
    /// identity. Wraps [`server::connect_server`], centralizing the
    /// pgpass password lookup so checkout and refill share one connect
    /// path.
    pub(in crate::pool) async fn connect_with_spec(
        &self,
        node: &str,
        spec: &ConnectSpec,
    ) -> anyhow::Result<ServerConn> {
        let password = self.lookup_password(node, &spec.upstream_database, &spec.upstream_user);
        server::connect_server(
            node,
            &spec.upstream_database,
            &spec.upstream_user,
            password.as_deref(),
            &spec.params,
            spec.max_prepared,
            spec.connect_timeout,
        )
        .await
    }

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
