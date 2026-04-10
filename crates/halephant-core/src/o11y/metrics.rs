use std::sync::LazyLock;
use std::time::Instant;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};

/// Pool metrics instruments, lazily initialized from the global meter.
pub(crate) struct Metrics {
    /// Connection errors broken down by `error.type` attribute. Covers both
    /// client-side checkout failures and upstream connect failures.
    pub errors: Counter<u64>,
    /// Time spent waiting for a pool connection (checkout duration).
    pub wait_time: Histogram<f64>,
    /// Time to establish a new upstream TCP connection.
    pub create_time: Histogram<f64>,
    /// Time a client spent blocked in the role wait queue because every
    /// candidate pool was at its `max_connections`. Recorded only when a
    /// checkout actually enqueued — fast checkouts are excluded so the
    /// histogram describes genuine contention.
    pub queue_wait: Histogram<f64>,
}

pub(crate) static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let meter: Meter = opentelemetry::global::meter("halephant");
    Metrics {
        errors: meter
            .u64_counter("db.client.connection.errors")
            .with_description("Number of connection errors")
            .build(),
        wait_time: meter
            .f64_histogram("db.client.connection.wait_time")
            .with_description("Time spent waiting for a pool connection")
            .with_unit("s")
            .build(),
        create_time: meter
            .f64_histogram("db.client.connection.create_time")
            .with_description("Time to establish a new upstream connection")
            .with_unit("s")
            .build(),
        queue_wait: meter
            .f64_histogram("halephant.client.wait_duration")
            .with_description(
                "Time a client spent blocked on the role wait queue \
                 waiting for pool capacity to become available",
            )
            .with_unit("s")
            .build(),
    }
});

/// Record a successful checkout duration.
pub(crate) fn record_checkout(start: Instant, database: &str, user: &str, node: &str) {
    METRICS.wait_time.record(
        start.elapsed().as_secs_f64(),
        &[
            KeyValue::new("db.namespace", database.to_owned()),
            KeyValue::new("user", user.to_owned()),
            KeyValue::new("server.address", node.to_owned()),
        ],
    );
}

/// Record the duration a client was blocked on the wait queue for the
/// given `(database, user, role)`. Only called when the checkout
/// actually enqueued (not on fast-path reuses) so the histogram
/// describes real contention.
pub(crate) fn record_wait_duration(
    elapsed: std::time::Duration,
    database: &str,
    user: &str,
    routing: crate::pool::Routing,
) {
    METRICS.queue_wait.record(
        elapsed.as_secs_f64(),
        &[
            KeyValue::new("db.namespace", database.to_owned()),
            KeyValue::new("user", user.to_owned()),
            KeyValue::new(
                "role",
                match routing {
                    crate::pool::Routing::Primary => "primary",
                    crate::pool::Routing::Replica => "replica",
                },
            ),
        ],
    );
}

/// Record a checkout failure with a classifying `error.type` so dashboards
/// can distinguish pool exhaustion from resolve failures from upstream
/// connect errors. `error_type` should be a low-cardinality label such as
/// `"pool_exhausted"`, `"checkout_timeout"`, `"unknown_database"`,
/// `"no_primary"`, `"no_replica"`, or `"connect_failed"`.
pub(crate) fn record_checkout_error(database: &str, user: &str, error_type: &'static str) {
    METRICS.errors.add(
        1,
        &[
            KeyValue::new("db.namespace", database.to_owned()),
            KeyValue::new("user", user.to_owned()),
            KeyValue::new("error.type", error_type),
        ],
    );
}

/// Record a new upstream connection duration.
pub(crate) fn record_connect(start: Instant, addr: &str, database: &str) {
    METRICS.create_time.record(
        start.elapsed().as_secs_f64(),
        &[
            KeyValue::new("server.address", addr.to_owned()),
            KeyValue::new("db.namespace", database.to_owned()),
        ],
    );
}

/// Record a connection error.
pub(crate) fn record_error(error_type: &str, addr: &str) {
    METRICS.errors.add(
        1,
        &[
            KeyValue::new("error.type", error_type.to_owned()),
            KeyValue::new("server.address", addr.to_owned()),
        ],
    );
}

// ---------------------------------------------------------------------------
// Pool stats
// ---------------------------------------------------------------------------

/// Identifies a pool by upstream node, database, and user.
pub struct PoolKeyInfo {
    pub node: String,
    pub database: String,
    pub user: String,
}

/// Snapshot of a single pool's connection counts.
pub struct PoolStats {
    pub active: u32,
    pub idle: u32,
    pub resetting: u32,
}

/// Configured connection limits for a pool.
pub struct PoolLimits {
    pub database: String,
    pub max_primary: u32,
    pub max_replica: u32,
}

/// Snapshot of a single wait queue: (database, user, role) plus how
/// many clients are currently blocked and how long the oldest has been
/// waiting. Returned by [`PoolManager::queue_stats`].
pub struct QueueInfo {
    pub database: String,
    pub user: String,
    pub role: crate::pool::Routing,
    pub depth: u32,
    pub oldest_wait_secs: f64,
}
