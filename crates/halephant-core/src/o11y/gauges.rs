//! Observable gauge registration — callback-driven metrics that the
//! OTel meter polls on each export cycle. Separated from the
//! recorded metrics in `metrics` because observable gauges must be
//! held alive for the process lifetime.

use std::sync::Arc;

use arc_swap::ArcSwap;
use opentelemetry::KeyValue;
use opentelemetry::metrics::ObservableGauge;

use crate::clients::ClientRegistry;
use crate::config::Config;
use crate::pool::{PoolManager, Routing};
use crate::topology::TopologyManager;

/// Owns the observable gauge handles so their callbacks remain
/// registered for the process lifetime. Dropping this struct
/// unregisters every gauge.
///
/// Call sites typically bind this to a `_metrics` variable to silence
/// the unused-field lint while keeping the handles alive until
/// process exit.
#[allow(dead_code)]
pub struct PoolGauges {
    count: ObservableGauge<u64>,
    max: ObservableGauge<u64>,
    healthy: ObservableGauge<u64>,
    clients: ObservableGauge<u64>,
    queue_depth: ObservableGauge<u64>,
}

/// Register the pool/client/topology observable gauges against the
/// OTel global meter. When OTel is disabled the global meter returns
/// no-op instruments (zero overhead).
///
/// Gauges registered:
///
/// - `db.client.connection.count` — connection counts per `(node,
///   database, user)` split by `state` (active/idle/resetting).
/// - `db.client.connection.max` — configured `max_connections` per
///   database split by `role` (primary/replica).
/// - `db.server.healthy` — whether each upstream node is reachable
///   (`1` = healthy, `0` = unreachable), tagged with `server.address`
///   and `cluster`.
/// - `halephant.client.connections` — live client counts grouped by
///   `state`.
/// - `halephant.client.queue_depth` — number of clients currently
///   blocked on a wait queue, grouped by `(database, user, role)`.
///   Only non-empty queues emit a series so the metric's
///   cardinality tracks contention rather than configured pools.
pub fn register_pool_gauges(
    pools: &Arc<PoolManager>,
    config: Arc<ArcSwap<Config>>,
    topology: Arc<TopologyManager>,
    clients: &Arc<ClientRegistry>,
) -> PoolGauges {
    let meter = opentelemetry::global::meter("halephant");

    // Pool connection counts by state.
    let pools_for_count = Arc::clone(pools);
    let count = meter
        .u64_observable_gauge("db.client.connection.count")
        .with_description("Number of connections in the pool")
        .with_callback(move |observer| {
            for (key, stats) in pools_for_count.pool_stats() {
                let base = [
                    KeyValue::new("server.address", key.node),
                    KeyValue::new("db.namespace", key.database),
                    KeyValue::new("user", key.user),
                ];
                let with_state = |state: &'static str, value: u32| {
                    let mut attrs = base.clone().to_vec();
                    attrs.push(KeyValue::new("state", state));
                    observer.observe(u64::from(value), &attrs);
                };
                with_state("active", stats.active);
                with_state("idle", stats.idle);
                with_state("resetting", stats.resetting);
            }
        })
        .build();

    // Configured max pool size.
    let pools_for_max = Arc::clone(pools);
    let max = meter
        .u64_observable_gauge("db.client.connection.max")
        .with_description("Configured maximum number of connections")
        .with_callback(move |observer| {
            for limits in pools_for_max.pool_limits() {
                let db = KeyValue::new("db.namespace", limits.database);
                if limits.max_primary > 0 {
                    observer.observe(
                        u64::from(limits.max_primary),
                        &[db.clone(), KeyValue::new("role", "primary")],
                    );
                }
                if limits.max_replica > 0 {
                    observer.observe(
                        u64::from(limits.max_replica),
                        &[db.clone(), KeyValue::new("role", "replica")],
                    );
                }
            }
        })
        .build();

    // Upstream node health.
    let healthy = meter
        .u64_observable_gauge("db.server.healthy")
        .with_description("Whether an upstream node is reachable (1 = healthy, 0 = unreachable)")
        .with_callback(move |observer| {
            // Snapshot per observation cycle so reload-added nodes
            // appear in the next 15-second export interval.
            let cfg = config.load_full();
            for (name, cluster) in &cfg.cluster {
                let Some(topo) = topology.get(name) else {
                    continue;
                };
                for node in &cluster.nodes {
                    let healthy = !topo.unreachable.contains(node);
                    observer.observe(
                        u64::from(healthy),
                        &[
                            KeyValue::new("server.address", node.clone()),
                            KeyValue::new("cluster", name.clone()),
                        ],
                    );
                }
            }
        })
        .build();

    // Client-side state counts. Observed by state attribute so dashboards
    // can break down "how many clients are idle vs waiting vs
    // authenticating". A single `counts_by_state` call reads every
    // bucket from the same locked pass, so the five series are
    // self-consistent and the registry mutex is touched once per
    // observation cycle.
    let clients_for_gauge = Arc::clone(clients);
    let clients_gauge = meter
        .u64_observable_gauge("halephant.client.connections")
        .with_description("Number of client connections grouped by state")
        .with_callback(move |observer| {
            for (state, count) in clients_for_gauge.counts_by_state() {
                observer.observe(count, &[KeyValue::new("state", state.as_str())]);
            }
        })
        .build();

    // Wait-queue depth per (database, user, role). Only non-empty
    // queues are reported (emits zero series when no contention),
    // which keeps the metric time series count proportional to
    // contention, not configured pools.
    let pools_for_queue = Arc::clone(pools);
    let queue_depth_gauge = meter
        .u64_observable_gauge("halephant.client.queue_depth")
        .with_description(
            "Number of clients currently blocked waiting for pool capacity, \
             grouped by (database, user, role)",
        )
        .with_callback(move |observer| {
            for q in pools_for_queue.queue_stats() {
                let role = match q.role {
                    Routing::Primary => "primary",
                    Routing::Replica => "replica",
                };
                observer.observe(
                    u64::from(q.depth),
                    &[
                        KeyValue::new("db.namespace", q.database),
                        KeyValue::new("user", q.user),
                        KeyValue::new("role", role),
                    ],
                );
            }
        })
        .build();

    PoolGauges {
        count,
        max,
        healthy,
        clients: clients_gauge,
        queue_depth: queue_depth_gauge,
    }
}
