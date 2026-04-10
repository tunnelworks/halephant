use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{Instrument, debug, info, trace, warn};

use crate::auth::pgpass::Pgpass;
use crate::config::Config;
use crate::config::cluster::pool::user::UserParameters;
use crate::connections::server as connections_server;
use crate::proto::backend::BackendMessage;
use crate::proto::codec::BackendCodec;
use crate::proto::frontend::FrontendMessage;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Discovered topology for a single cluster.
#[derive(Debug, Clone, Default)]
pub struct ClusterTopology {
    pub primary: Option<String>,
    pub replicas: Vec<String>,
    pub unreachable: Vec<String>,
}

/// Manages topology discovery for all clusters.
pub struct TopologyManager {
    state: Mutex<HashMap<String, ClusterTopology>>,
    config: Arc<ArcSwap<Config>>,
    pgpass: Arc<Pgpass>,
}

impl TopologyManager {
    pub fn new(config: Arc<ArcSwap<Config>>, pgpass: Arc<Pgpass>) -> Self {
        Self {
            state: Mutex::new(HashMap::new()),
            config,
            pgpass,
        }
    }

    /// Get the current primary address for a cluster.
    pub fn primary(&self, cluster: &str) -> Option<String> {
        let state = self.state.lock();
        state.get(cluster)?.primary.clone()
    }

    /// Get the current replica addresses for a cluster.
    pub fn replicas(&self, cluster: &str) -> Vec<String> {
        let state = self.state.lock();
        state
            .get(cluster)
            .map(|t| t.replicas.clone())
            .unwrap_or_default()
    }

    /// Get the full topology snapshot for a cluster.
    pub fn get(&self, cluster: &str) -> Option<ClusterTopology> {
        let state = self.state.lock();
        state.get(cluster).cloned()
    }

    /// Insert or replace the topology snapshot for a cluster.
    pub fn set(&self, cluster: &str, topology: ClusterTopology) {
        let mut state = self.state.lock();
        state.insert(cluster.to_owned(), topology);
    }

    /// Probe all nodes in all clusters and update the topology state.
    /// Returns a list of node addresses whose role changed (should be drained).
    #[tracing::instrument(name = "topology.refresh", skip_all, fields(
        clusters = tracing::field::Empty,
        otel.status_code,
    ))]
    pub async fn refresh(&self) -> Vec<String> {
        // Snapshot once so the refresh observes a consistent cluster
        // list across every probe, even if a hot-reload swaps the
        // config mid-run.
        let cfg = self.config.load_full();
        tracing::Span::current().record("clusters", cfg.cluster.len());

        let mut changed_nodes = Vec::new();
        for (name, cluster) in &cfg.cluster {
            let topo = discover_cluster(
                name,
                &cluster.nodes,
                &cluster.admin_user,
                &cluster.admin_database,
                cluster.topology.timeout,
                &self.pgpass,
            )
            .await;

            let mut state = self.state.lock();
            let prev = state.get(name.as_str());

            // Detect role changes and collect affected nodes for draining.
            if let Some(prev) = prev {
                // Total probe failure: every node is unreachable and no
                // primary was found. Preserve the previous topology so a
                // transient network blip doesn't erase a known-good
                // primary and break all new connections until the next
                // successful refresh.
                if topo.primary.is_none() && topo.replicas.is_empty() {
                    warn!(
                        cluster = %name,
                        unreachable = topo.unreachable.len(),
                        "all probes failed, preserving previous topology"
                    );
                    continue;
                }

                if prev.primary != topo.primary {
                    // Old primary changed — drain it.
                    if let Some(ref old) = prev.primary {
                        changed_nodes.push(old.clone());
                    }
                    if let Some(ref new_primary) = topo.primary {
                        // New primary (was replica) — drain its replica connections.
                        changed_nodes.push(new_primary.clone());
                        info!(
                            cluster = %name,
                            old = ?prev.primary,
                            new = %new_primary,
                            "primary changed"
                        );
                    } else if prev.primary.is_some() {
                        warn!(cluster = %name, "no primary found");
                    }
                }
                // Drain newly unreachable nodes.
                for node in &topo.unreachable {
                    if !prev.unreachable.contains(node) {
                        changed_nodes.push(node.clone());
                    }
                }
            } else if let Some(ref primary) = topo.primary {
                info!(
                    cluster = %name,
                    primary = %primary,
                    replicas = topo.replicas.len(),
                    "initial topology discovered"
                );
            }

            state.insert(name.clone(), topo);
        }

        // Prune state entries for clusters removed by a hot-reload.
        {
            let mut state = self.state.lock();
            state.retain(|name, topo| {
                if cfg.cluster.contains_key(name) {
                    return true;
                }
                info!(cluster = %name, "cluster removed by reload, pruning topology");
                if let Some(ref primary) = topo.primary {
                    changed_nodes.push(primary.clone());
                }
                changed_nodes.extend(topo.replicas.iter().cloned());
                changed_nodes.extend(topo.unreachable.iter().cloned());
                false
            });
        }

        tracing::Span::current().record("otel.status_code", "OK");
        changed_nodes
    }

    /// Run the topology refresh loop at the configured interval, draining
    /// pool connections for any nodes whose role changes. Intended to be
    /// spawned as a background task.
    pub async fn run_loop(self: &Arc<Self>, pools: &Arc<crate::pool::PoolManager>) {
        loop {
            let interval = {
                let cfg = self.config.load();
                cfg.cluster
                    .values()
                    .map(|c| c.topology.interval)
                    .min()
                    .unwrap_or(Duration::from_secs(5))
            };
            tokio::time::sleep(interval).await;
            let changed = self.refresh().await;
            for node in &changed {
                pools.drain_node(node);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Node probing
// ---------------------------------------------------------------------------

/// Discover the topology of a single cluster by probing every node concurrently.
async fn discover_cluster(
    cluster_name: &str,
    nodes: &[String],
    admin_user: &str,
    admin_database: &str,
    check_timeout: Duration,
    pgpass: &Pgpass,
) -> ClusterTopology {
    let parent_span = tracing::Span::current();
    let mut tasks = tokio::task::JoinSet::new();

    for node in nodes {
        let node = node.clone();
        let admin_user = admin_user.to_owned();
        let admin_database = admin_database.to_owned();
        let password = pgpass
            .lookup_addr(&node, &admin_database, &admin_user)
            .map(str::to_owned);
        tasks.spawn(
            async move {
                let result = timeout(
                    check_timeout,
                    check_node(
                        &node,
                        &admin_user,
                        &admin_database,
                        password.as_deref(),
                        check_timeout,
                    ),
                )
                .await;
                (node, result)
            }
            .instrument(parent_span.clone()),
        );
    }

    let mut topo = ClusterTopology::default();
    while let Some(joined) = tasks.join_next().await {
        let (node, result) = match joined {
            Ok(pair) => pair,
            Err(e) => {
                warn!(cluster = %cluster_name, %e, "topology probe task panicked");
                continue;
            }
        };
        match result {
            Ok(Ok(is_recovery)) => {
                if is_recovery {
                    trace!(cluster = %cluster_name, node = %node, "replica");
                    topo.replicas.push(node);
                } else {
                    trace!(cluster = %cluster_name, node = %node, "primary");
                    if topo.primary.is_some() {
                        warn!(
                            cluster = %cluster_name,
                            node = %node,
                            existing = ?topo.primary,
                            "multiple primaries detected — possible split-brain"
                        );
                    }
                    topo.primary = Some(node);
                }
            }
            Ok(Err(e)) => {
                debug!(cluster = %cluster_name, node = %node, %e, "unreachable");
                topo.unreachable.push(node);
            }
            Err(_) => {
                debug!(cluster = %cluster_name, node = %node, "check timed out");
                topo.unreachable.push(node);
            }
        }
    }

    // Probes complete out of order — sort replicas so round-robin
    // distribution and diagnostic output stay deterministic.
    topo.replicas.sort();
    topo.unreachable.sort();

    topo
}

/// Connect to a node and run `SELECT pg_is_in_recovery()`.
/// Returns `true` if the node is a replica, `false` if it is a primary.
#[tracing::instrument(name = "topology.probe", skip_all, err(Display), fields(
    otel.kind = "client",
    server.address = %addr,
    user = %admin_user,
    db.namespace = %admin_database,
    db.system.name = "postgresql",
    otel.status_code,
    otel.status_description,
))]
async fn check_node(
    addr: &str,
    admin_user: &str,
    admin_database: &str,
    password: Option<&str>,
    connect_timeout: Duration,
) -> anyhow::Result<bool> {
    let params = UserParameters::default();
    let mut conn = connections_server::connect_server(
        addr,
        admin_database,
        admin_user,
        password,
        &params,
        0,
        connect_timeout,
    )
    .await?;

    let is_recovery = run_role_query(&mut conn.framed).await?;

    let _ = conn.framed.send(FrontendMessage::Terminate).await;

    let span = tracing::Span::current();
    span.record("otel.status_code", "OK");
    Ok(is_recovery)
}

/// Send `SELECT pg_is_in_recovery()` and parse the response.
#[tracing::instrument(name = "topology.role_query", skip_all, err(Display), fields(
    otel.status_code,
    otel.status_description,
))]
async fn run_role_query(framed: &mut Framed<TcpStream, BackendCodec>) -> anyhow::Result<bool> {
    framed
        .send(FrontendMessage::Query("SELECT pg_is_in_recovery()".into()))
        .await?;

    let mut is_recovery = false;
    loop {
        match framed.next().await.transpose()? {
            Some(BackendMessage::DataRow(cols)) => {
                if let Some(Some(val)) = cols.first() {
                    is_recovery = val == b"t";
                }
            }
            Some(BackendMessage::ReadyForQuery(_)) => break,
            Some(BackendMessage::ErrorResponse(e)) => {
                anyhow::bail!(
                    "pg_is_in_recovery() error: {}",
                    e.message().unwrap_or("unknown")
                );
            }
            // RowDescription, CommandComplete, etc.
            _ => {}
        }
    }

    let span = tracing::Span::current();
    span.record("otel.status_code", "OK");
    Ok(is_recovery)
}
