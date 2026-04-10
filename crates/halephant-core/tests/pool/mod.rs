#![allow(clippy::unwrap_used, clippy::panic)]

mod pool_test;

use std::sync::Arc;

use arc_swap::ArcSwap;
use halephant_core::config::Config;
use halephant_core::pool::PoolManager;
use halephant_core::topology::{ClusterTopology, TopologyManager};

use halephant_core::clients::{ClientGuard, ClientRegistry};

/// Create a standalone client guard for tests. The registry is owned
/// by the returned guard via its internal `Arc`, so the guard remains
/// valid for the duration of the test even though we drop the
/// registry variable.
pub(crate) fn test_client_guard() -> ClientGuard {
    let registry = Arc::new(ClientRegistry::new());
    registry.register("127.0.0.1:0".parse().unwrap())
}

/// Build a pool and topology manager with primary state pre-seeded for every
/// cluster in the config. Without this, resolution fails at the topology
/// layer — tests that exercise the connect path need topology to be known.
fn make_pool_with_seeded_primary(toml: &str) -> Arc<PoolManager> {
    let config = Arc::new(ArcSwap::from_pointee(Config::parse(toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    for (name, cluster) in &config.load().cluster {
        topology.set(
            name,
            ClusterTopology {
                primary: cluster.nodes.first().cloned(),
                replicas: Vec::new(),
                unreachable: Vec::new(),
            },
        );
    }
    Arc::new(PoolManager::new(config, topology, pgpass))
}

/// Build a pool without seeding topology. Used by tests that expect resolve
/// errors without ever reaching the connect path.
fn make_pool(toml: &str) -> Arc<PoolManager> {
    let config = Arc::new(ArcSwap::from_pointee(Config::parse(toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    Arc::new(PoolManager::new(config, topology, pgpass))
}

fn pool_with_max(max_connections: u32) -> Arc<PoolManager> {
    make_pool_with_seeded_primary(&format!(
        r#"
        [cluster.test]
        nodes = ["127.0.0.1:59999"]

        [cluster.test.pool.testdb]
        max_connections = {{ primary = {max_connections} }}

        [cluster.test.pool.testdb.user.user]
        [cluster.test.pool.testdb.user.alice]
        [cluster.test.pool.testdb.user.bob]
        "#
    ))
}

/// Extract the error message from a checkout result, panicking if it succeeded.
async fn checkout_err(pool: &Arc<PoolManager>, db: &str, user: &str) -> String {
    let client = test_client_guard();
    match pool.checkout(&client, db, user, false).await {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected checkout to fail for {db}/{user}"),
    }
}

// ---------------------------------------------------------------------------
// Checkout fails gracefully when upstream is unreachable
// ---------------------------------------------------------------------------

#[tokio::test]
async fn checkout_unreachable_upstream() {
    let pool = pool_with_max(10);
    let msg = checkout_err(&pool, "testdb", "user").await;
    assert!(
        msg.contains("connect") || msg.contains("refused"),
        "error should mention connection failure: {msg}"
    );
}

#[tokio::test]
async fn checkout_unknown_database_rejected() {
    let pool = pool_with_max(10);
    let msg = checkout_err(&pool, "unknown_db", "user").await;
    assert!(
        msg.contains("not configured"),
        "unknown database should fail: {msg}"
    );
}

#[tokio::test]
async fn checkout_no_clusters_configured() {
    let pool = make_pool("");
    let msg = checkout_err(&pool, "db", "user").await;
    assert!(
        msg.contains("not configured"),
        "should report database not configured: {msg}"
    );
}

#[tokio::test]
async fn checkout_no_primary_discovered() {
    // Topology not seeded — resolve fails with NoPrimary before any
    // connection attempt is made.
    let pool = make_pool(
        r#"
        [cluster.test]
        nodes = ["127.0.0.1:59999"]

        [cluster.test.pool.testdb.user.user]
    "#,
    );
    let msg = checkout_err(&pool, "testdb", "user").await;
    assert!(
        msg.contains("no primary discovered"),
        "missing topology should fail with NoPrimary: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Pool exhaustion: active count is decremented after failed connect
// ---------------------------------------------------------------------------

#[tokio::test]
async fn failed_connect_does_not_exhaust_pool() {
    let pool = pool_with_max(1);

    let msg1 = checkout_err(&pool, "testdb", "user").await;
    assert!(!msg1.contains("exhausted"), "first attempt: {msg1}");

    let msg2 = checkout_err(&pool, "testdb", "user").await;
    assert!(!msg2.contains("exhausted"), "second attempt: {msg2}");

    let msg3 = checkout_err(&pool, "testdb", "user").await;
    assert!(!msg3.contains("exhausted"), "third attempt: {msg3}");
}

// ---------------------------------------------------------------------------
// Config resolution — correct cluster per database
// ---------------------------------------------------------------------------

#[tokio::test]
async fn resolves_correct_cluster() {
    let pool = make_pool_with_seeded_primary(
        r#"
        [cluster.cluster_a]
        nodes = ["127.0.0.1:59998"]

        [cluster.cluster_a.pool.db_a.user.user]

        [cluster.cluster_b]
        nodes = ["127.0.0.1:59997"]

        [cluster.cluster_b.pool.db_b.user.user]
    "#,
    );

    let err_a = checkout_err(&pool, "db_a", "user").await;
    assert!(
        err_a.contains("59998"),
        "db_a should route to 59998: {err_a}"
    );

    let err_b = checkout_err(&pool, "db_b", "user").await;
    assert!(
        err_b.contains("59997"),
        "db_b should route to 59997: {err_b}"
    );
}

// ---------------------------------------------------------------------------
// Separate pools per user
// ---------------------------------------------------------------------------

#[tokio::test]
async fn separate_pools_per_user() {
    let pool = pool_with_max(10);

    // Different users get independent pool slots (both fail because upstream
    // is unreachable, but they don't interfere with each other).
    let _ = checkout_err(&pool, "testdb", "alice").await;
    let _ = checkout_err(&pool, "testdb", "bob").await;
}

// ---------------------------------------------------------------------------
// Hot reload — pool observes ArcSwap config changes
// ---------------------------------------------------------------------------

/// A live config swap via `ArcSwap.store()` is visible to subsequent pool
/// reads. Exercises the end-to-end hot-reload path without involving
/// SIGHUP or disk I/O: the same primitives that `reload_config` uses
/// in the binary drive this test.
#[tokio::test]
async fn pool_observes_config_swap() {
    let pool = make_pool_with_seeded_primary(
        r#"
        [cluster.test]
        nodes = ["127.0.0.1:59999"]

        [cluster.test.pool.mydb]
        max_connections = { primary = 10 }

        [cluster.test.pool.mydb.user.myuser]
        "#,
    );

    // Sanity check: pool_limits reports the initial value.
    let initial = pool
        .pool_limits()
        .into_iter()
        .find(|l| l.database == "mydb")
        .expect("mydb pool exists");
    assert_eq!(initial.max_primary, 10);

    // Build and store a new config with a raised max_connections. This
    // is exactly what `reload_config` does in the binary once the
    // restart-required check passes.
    let new_cfg = Config::parse(
        r#"
        [cluster.test]
        nodes = ["127.0.0.1:59999"]

        [cluster.test.pool.mydb]
        max_connections = { primary = 42 }

        [cluster.test.pool.mydb.user.myuser]
        "#,
    )
    .unwrap();
    pool.config().store(Arc::new(new_cfg));

    // The next read through `pool_limits` observes the swapped config.
    // This verifies the `self.config.load_full()` snapshot at the top
    // of `pool_limits` picks up the new value, and by extension, every
    // other pool method that snapshots at call time.
    let after = pool
        .pool_limits()
        .into_iter()
        .find(|l| l.database == "mydb")
        .expect("mydb pool still exists after reload");
    assert_eq!(after.max_primary, 42);
}

/// A swap that adds a brand-new `(database, user)` pair makes it
/// resolvable via `PoolManager::resolve` without restarting the
/// process. Exercises `candidate_nodes` / `find_pool` on the swapped
/// config.
#[tokio::test]
async fn pool_observes_added_database_after_swap() {
    let pool = make_pool_with_seeded_primary(
        r#"
        [cluster.test]
        nodes = ["127.0.0.1:59999"]

        [cluster.test.pool.existing.user.alice]
        "#,
    );

    // Before the swap, `newdb` is unknown.
    let err = checkout_err(&pool, "newdb", "bob").await;
    assert!(
        err.contains("not configured"),
        "newdb should be unknown pre-reload: {err}"
    );

    // Swap in a config that adds `newdb` alongside the existing pool.
    // Keep `existing` so the topology we seeded is still consistent
    // with `find_pool` lookups from callers that target it.
    let new_cfg = Config::parse(
        r#"
        [cluster.test]
        nodes = ["127.0.0.1:59999"]

        [cluster.test.pool.existing.user.alice]

        [cluster.test.pool.newdb.user.bob]
        "#,
    )
    .unwrap();
    pool.config().store(Arc::new(new_cfg));

    // After the swap, `newdb` resolves; the checkout still fails
    // because nothing is listening on the mock port, but the
    // classification moves from `unknown_database` to a connect error.
    let err = checkout_err(&pool, "newdb", "bob").await;
    assert!(
        !err.contains("not configured"),
        "newdb should be recognized after reload: {err}"
    );
}
