#![allow(clippy::unwrap_used)]
#[path = "pool_test/mock_pg.rs"]
mod mock_pg;

use std::sync::Arc;

use arc_swap::ArcSwap;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use halephant_core::config::Config;
use halephant_core::config::cluster::pool::ListenMode;
use halephant_core::pool::PoolManager;
use halephant_core::proto::backend::BackendMessage;
use halephant_core::proto::codec::{BackendCodec, FrontendCodec};
use halephant_core::proto::frontend::FrontendMessage;
use halephant_core::proto::types::TransactionStatus;
use halephant_core::topology::{ClusterTopology, TopologyManager};

use super::test_client_guard;
use mock_pg::{MockBehavior, MockPg};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// All test configs use a very short `checkout_timeout` so exhaustion
/// tests fail fast instead of hanging for the 30-second production
/// default. Tests that want to verify genuine wait-queue behavior
/// configure the sleep interval explicitly.
const TEST_CHECKOUT_TIMEOUT: &str = "50ms";

fn test_config(node_addr: &str) -> Config {
    Config::parse(&format!(
        r#"
        [server]
        checkout_timeout = "{TEST_CHECKOUT_TIMEOUT}"

        [cluster.test]
        nodes = ["{node_addr}"]

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 10 }}

        [cluster.test.pool.testdb.user.testuser]
    "#
    ))
    .unwrap()
}

fn test_config_max(node_addr: &str, max: u32) -> Config {
    Config::parse(&format!(
        r#"
        [server]
        checkout_timeout = "{TEST_CHECKOUT_TIMEOUT}"

        [cluster.test]
        nodes = ["{node_addr}"]

        [cluster.test.pool.testdb]
        max_connections = {{ primary = {max} }}

        [cluster.test.pool.testdb.user.testuser]
    "#
    ))
    .unwrap()
}

fn test_config_with_replica(primary_addr: &str, replica_addr: &str) -> Config {
    Config::parse(&format!(
        r#"
        [server]
        checkout_timeout = "{TEST_CHECKOUT_TIMEOUT}"

        [cluster.test]
        nodes = ["{primary_addr}", "{replica_addr}"]

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 10, replica = 10 }}

        [cluster.test.pool.testdb.user.testuser]

        [cluster.test.pool.testdb.user.rouser]
        max_connections = {{ replica = 10 }}
    "#
    ))
    .unwrap()
}

fn test_config_session(node_addr: &str) -> Config {
    Config::parse(&format!(
        r#"
        [server]
        checkout_timeout = "{TEST_CHECKOUT_TIMEOUT}"

        [cluster.test]
        nodes = ["{node_addr}"]

        [cluster.test.pool.testdb]
        mode = "session"
        max_connections = {{ primary = 10 }}

        [cluster.test.pool.testdb.user.testuser]
    "#
    ))
    .unwrap()
}

/// Create a connected TCP pair (client end, server end).
async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, server) = tokio::join!(TcpStream::connect(addr), async {
        listener.accept().await.map(|(s, _)| s)
    });
    (client.unwrap(), server.unwrap())
}

/// Seed the topology of the `test` cluster with a single primary address.
/// Tests that drive mocks with behaviors incompatible with the real topology
/// probe (SCRAM, CloseOnQuery, ...) cannot rely on `refresh` to populate
/// topology state, so they inject it directly.
fn seed_primary(topology: &TopologyManager, primary_addr: String) {
    topology.set(
        "test",
        ClusterTopology {
            primary: Some(primary_addr),
            replicas: Vec::new(),
            unreachable: Vec::new(),
        },
    );
}

/// Set up a pool manager backed by a mock primary.
fn setup_pool(mock: &MockPg) -> (Arc<PoolManager>, Arc<ArcSwap<Config>>) {
    setup_pool_with_config(test_config(&mock.addr()), mock)
}

fn setup_pool_with_config(cfg: Config, mock: &MockPg) -> (Arc<PoolManager>, Arc<ArcSwap<Config>>) {
    let config = Arc::new(ArcSwap::from_pointee(cfg));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary(&topology, mock.addr());
    let pools = Arc::new(PoolManager::new(
        Arc::clone(&config),
        Arc::clone(&topology),
        pgpass,
    ));
    (pools, config)
}

/// Run transaction::forward in a background task and return its handle.
fn spawn_tx_forward(
    proxy_stream: TcpStream,
    pools: Arc<PoolManager>,
    database: &str,
    user: &str,
    read_only: bool,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let db = database.to_owned();
    let usr = user.to_owned();
    tokio::spawn(async move {
        let client = test_client_guard();
        let mut proxy_client = Framed::new(proxy_stream, FrontendCodec::post_startup());
        halephant_core::proxy::transaction::forward(
            &mut proxy_client,
            &pools,
            &client,
            &mut None,
            ListenMode::Pin,
            &db,
            &usr,
            read_only,
            halephant_core::config::otel::QueryText::Off,
        )
        .await
    })
}

/// Send a simple query and collect all response messages until ReadyForQuery.
async fn query_collect(
    client: &mut Framed<TcpStream, BackendCodec>,
    sql: &str,
) -> Vec<BackendMessage> {
    client
        .send(FrontendMessage::Query(sql.into()))
        .await
        .unwrap();
    let mut msgs = Vec::new();
    loop {
        let msg = client.next().await.unwrap().unwrap();
        let is_ready = matches!(msg, BackendMessage::ReadyForQuery(_));
        msgs.push(msg);
        if is_ready {
            break;
        }
    }
    msgs
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basic_round_trip() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let (pools, _config) = setup_pool(&mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    let msgs = query_collect(&mut client, "SELECT 1").await;

    // Expect: RowDescription, CommandComplete, ReadyForQuery(Idle).
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::RowDescription(_))),
        "expected RowDescription in response"
    );
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::CommandComplete(_))),
        "expected CommandComplete in response"
    );
    assert!(
        matches!(
            msgs.last(),
            Some(BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        ),
        "expected ReadyForQuery(Idle) as final message"
    );

    // Clean shutdown.
    client.send(FrontendMessage::Terminate).await.unwrap();
    handle.await.unwrap().unwrap();
    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_reuse() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let (pools, _config) = setup_pool(&mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    // First query — opens a new upstream connection.
    let _ = query_collect(&mut client, "SELECT 1").await;
    assert_eq!(mock.connections(), 1, "first query opens one connection");

    // Second query — should reuse the connection after checkin + reset.
    // Poll until the background reset returns a connection to the idle pool.
    // Each attempt snapshots the mock's connection count before and after; if
    // the count doesn't increase, checkout reused an idle connection.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let before = mock.connections();
        let _ = query_collect(&mut client, "SELECT 2").await;
        if mock.connections() == before {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "connection was not reused within 2 s"
        );
    }

    client.send(FrontendMessage::Terminate).await.unwrap();
    handle.await.unwrap().unwrap();
    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_exhaustion() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let config = Arc::new(ArcSwap::from_pointee(test_config_max(&mock.addr(), 1)));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));
    topology.refresh().await;
    let pools = Arc::new(PoolManager::new(
        Arc::clone(&config),
        Arc::clone(&topology),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));

    let client = test_client_guard();

    // First checkout succeeds.
    let guard = pools.checkout(&client, "testdb", "testuser", false).await;
    assert!(guard.is_ok(), "first checkout should succeed");

    // Second checkout exceeds max=1. With queueing, it enqueues and
    // then times out at the configured `checkout_timeout` because the
    // first guard is still held. The classified error mentions the
    // timeout, not literal "pool exhausted".
    let result = pools.checkout(&client, "testdb", "testuser", false).await;
    assert!(result.is_err(), "second checkout should fail");
    let err = result.err().unwrap();
    assert!(
        err.to_string().contains("timed out"),
        "error should mention checkout timeout, got: {err}"
    );

    // Drop the first guard (discards connection).
    drop(guard);
    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_boundary() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let (pools, _config) = setup_pool(&mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    // BEGIN.
    let msgs = query_collect(&mut client, "BEGIN").await;
    assert!(
        matches!(
            msgs.last(),
            Some(BackendMessage::ReadyForQuery(
                TransactionStatus::InTransaction
            ))
        ),
        "expected InTransaction after BEGIN"
    );

    // Query inside transaction.
    let msgs = query_collect(&mut client, "SELECT 1").await;
    assert!(
        matches!(
            msgs.last(),
            Some(BackendMessage::ReadyForQuery(
                TransactionStatus::InTransaction
            ))
        ),
        "expected InTransaction during transaction"
    );

    // COMMIT.
    let msgs = query_collect(&mut client, "COMMIT").await;
    assert!(
        matches!(
            msgs.last(),
            Some(BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        ),
        "expected Idle after COMMIT"
    );

    client.send(FrontendMessage::Terminate).await.unwrap();
    handle.await.unwrap().unwrap();
    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_close_mid_transaction() {
    let mock = MockPg::start(MockBehavior::CloseOnQuery).await;
    let (pools, _config) = setup_pool(&mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    // Send a query — the mock will accept the pool's startup handshake but
    // close the connection when it receives the forwarded query.
    client
        .send(FrontendMessage::Query("SELECT 1".into()))
        .await
        .unwrap();

    // The proxy should synthesize an ErrorResponse (FATAL 08006) to the client.
    let mut got_error = false;
    loop {
        match client.next().await {
            Some(Ok(BackendMessage::ErrorResponse(fields))) => {
                let code = fields
                    .fields
                    .iter()
                    .find(|(k, _)| *k == b'C')
                    .map(|(_, v)| v.as_str());
                assert_eq!(code, Some("08006"), "expected connection_failure SQLSTATE");
                got_error = true;
            }
            Some(Ok(BackendMessage::ReadyForQuery(_))) | None => break, // ReadyForQuery or connection closed
            _ => {}
        }
    }
    assert!(got_error, "expected synthesized ErrorResponse from proxy");

    // The forward task should return an error.
    let result = handle.await.unwrap();
    assert!(result.is_err());
    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_close_after_startup() {
    // The mock accepts the pool's startup handshake then immediately drops
    // the connection. The proxy may fail sending the query (broken pipe) or
    // detect EOF when reading the response — either way the forward task
    // errors and the client connection closes.
    let mock = MockPg::start(MockBehavior::CloseAfterStartup).await;
    let (pools, _config) = setup_pool(&mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    client
        .send(FrontendMessage::Query("SELECT 1".into()))
        .await
        .unwrap();

    // Drain whatever the proxy sends. Depending on timing we may see a
    // synthesized 08006 ErrorResponse or just EOF.
    while let Some(Ok(_)) = client.next().await {}

    // The forward task must have failed.
    let result = handle.await.unwrap();
    assert!(result.is_err());
    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_only_routing() {
    let primary = MockPg::start(MockBehavior::Ok).await;
    let replica = MockPg::start_replica(MockBehavior::Ok).await;
    let config = Arc::new(ArcSwap::from_pointee(test_config_with_replica(
        &primary.addr(),
        &replica.addr(),
    )));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));
    topology.refresh().await;
    let pools = Arc::new(PoolManager::new(
        Arc::clone(&config),
        Arc::clone(&topology),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));

    let client = test_client_guard();

    // Read-only checkout should go to the replica.
    let guard = pools.checkout(&client, "testdb", "testuser", true).await;
    assert!(guard.is_ok(), "read-only checkout should succeed");
    // topology probe uses 1 conn each; checkout adds 1 to replica.
    assert_eq!(
        replica.connections(),
        2, // 1 topology probe + 1 checkout
        "read-only checkout should go to replica"
    );

    drop(guard);

    // Read-write checkout should go to the primary.
    let guard = pools.checkout(&client, "testdb", "testuser", false).await;
    assert!(guard.is_ok(), "read-write checkout should succeed");
    assert_eq!(
        primary.connections(),
        2, // 1 topology probe + 1 checkout
        "read-write checkout should go to primary"
    );

    drop(guard);
    primary.stop().await;
    replica.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_only_user_routing() {
    let primary = MockPg::start(MockBehavior::Ok).await;
    let replica = MockPg::start_replica(MockBehavior::Ok).await;
    let config = Arc::new(ArcSwap::from_pointee(test_config_with_replica(
        &primary.addr(),
        &replica.addr(),
    )));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));
    topology.refresh().await;
    let pools = Arc::new(PoolManager::new(
        Arc::clone(&config),
        Arc::clone(&topology),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));

    let (client_tcp, proxy_tcp) = tcp_pair().await;

    // rouser has max_connections = { replica = 10 } (no primary) — is_read_only.
    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "rouser", true);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    let _ = query_collect(&mut client, "SELECT 1").await;
    // The query should have gone to the replica, not the primary.
    assert_eq!(
        replica.connections(),
        2, // 1 topology probe + 1 query checkout
        "read-only user should route to replica"
    );

    client.send(FrontendMessage::Terminate).await.unwrap();
    handle.await.unwrap().unwrap();
    primary.stop().await;
    replica.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_write_override_rejected() {
    let primary = MockPg::start(MockBehavior::Ok).await;
    let replica = MockPg::start_replica(MockBehavior::Ok).await;
    let config = Arc::new(ArcSwap::from_pointee(test_config_with_replica(
        &primary.addr(),
        &replica.addr(),
    )));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));
    topology.refresh().await;
    let pools = Arc::new(PoolManager::new(
        Arc::clone(&config),
        Arc::clone(&topology),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));

    let (client_tcp, proxy_tcp) = tcp_pair().await;

    // read_only = true: should reject BEGIN READ WRITE.
    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", true);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    let msgs = query_collect(&mut client, "BEGIN READ WRITE").await;
    // Expect an ErrorResponse with SQLSTATE 25006.
    let has_error = msgs.iter().any(|m| {
        if let BackendMessage::ErrorResponse(fields) = m {
            fields
                .fields
                .iter()
                .any(|(k, v)| *k == b'C' && v == "25006")
        } else {
            false
        }
    });
    assert!(has_error, "expected ErrorResponse with SQLSTATE 25006");

    // The proxy should still be alive — send a normal query.
    let msgs = query_collect(&mut client, "SELECT 1").await;
    assert!(
        matches!(
            msgs.last(),
            Some(BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        ),
        "proxy should still be functional after rejected override"
    );

    client.send(FrontendMessage::Terminate).await.unwrap();
    handle.await.unwrap().unwrap();
    primary.stop().await;
    replica.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn begin_read_only_routes_to_replica() {
    let primary = MockPg::start(MockBehavior::Ok).await;
    let replica = MockPg::start_replica(MockBehavior::Ok).await;
    let config = Arc::new(ArcSwap::from_pointee(test_config_with_replica(
        &primary.addr(),
        &replica.addr(),
    )));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));
    topology.refresh().await;
    let pools = Arc::new(PoolManager::new(
        Arc::clone(&config),
        Arc::clone(&topology),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));

    let (client_tcp, proxy_tcp) = tcp_pair().await;

    // read_only = false, but BEGIN READ ONLY should still route to replica.
    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    let primary_before = primary.connections();
    let msgs = query_collect(&mut client, "BEGIN READ ONLY").await;
    assert!(
        matches!(
            msgs.last(),
            Some(BackendMessage::ReadyForQuery(
                TransactionStatus::InTransaction
            ))
        ),
        "expected InTransaction after BEGIN READ ONLY"
    );

    // The BEGIN should have checked out from the replica, not the primary.
    assert_eq!(
        primary.connections(),
        primary_before,
        "BEGIN READ ONLY should not open a new primary connection"
    );
    assert!(
        replica.connections() > 1,
        "BEGIN READ ONLY should have opened a replica connection"
    );

    // COMMIT to release.
    let _ = query_collect(&mut client, "COMMIT").await;

    client.send(FrontendMessage::Terminate).await.unwrap();
    handle.await.unwrap().unwrap();
    primary.stop().await;
    replica.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_mode_forward() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let config = Arc::new(ArcSwap::from_pointee(test_config_session(&mock.addr())));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));
    topology.refresh().await;
    let pools = Arc::new(PoolManager::new(
        Arc::clone(&config),
        Arc::clone(&topology),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));

    // Check out a connection for session-mode forwarding.
    let checkout_client = test_client_guard();
    let mut guard = pools
        .checkout(&checkout_client, "testdb", "testuser", false)
        .await
        .unwrap();

    let (client_tcp, proxy_tcp) = tcp_pair().await;
    let mut proxy_client = Framed::new(proxy_tcp, FrontendCodec::post_startup());
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    // Send post-auth startup to the client.
    halephant_core::messages::send_post_auth_startup(&mut proxy_client, guard.conn())
        .await
        .unwrap();

    // Drain the startup messages from the client side.
    loop {
        let msg = client.next().await.unwrap().unwrap();
        if matches!(msg, BackendMessage::ReadyForQuery(_)) {
            break;
        }
    }

    // Run session forwarding in background.
    let pools_for_session = Arc::clone(&pools);
    let handle = tokio::spawn(async move {
        halephant_core::proxy::session::forward(&mut proxy_client, guard.conn(), &pools_for_session)
            .await
    });

    // Send a query through the session.
    let msgs = query_collect(&mut client, "SELECT 1").await;
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::CommandComplete(_))),
        "expected CommandComplete in session mode"
    );

    // Terminate.
    client.send(FrontendMessage::Terminate).await.unwrap();
    handle.await.unwrap().unwrap();
    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_checkouts() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let config = Arc::new(ArcSwap::from_pointee(test_config_max(&mock.addr(), 5)));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));
    topology.refresh().await;
    let pools = Arc::new(PoolManager::new(
        Arc::clone(&config),
        Arc::clone(&topology),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));

    // Check out 5 connections concurrently. Each spawned task owns its
    // own ClientGuard so state transitions don't collide.
    let mut handles = Vec::new();
    for _ in 0..5 {
        let pools = Arc::clone(&pools);
        handles.push(tokio::spawn(async move {
            let client = test_client_guard();
            pools.checkout(&client, "testdb", "testuser", false).await
        }));
    }

    let mut guards = Vec::new();
    for h in handles {
        guards.push(h.await.unwrap().unwrap());
    }

    // All 5 should succeed (+ 1 topology probe = 6 total mock connections).
    assert_eq!(mock.connections(), 6);

    // 6th checkout should time out — pool is at max and all five guards
    // are still held.
    let client = test_client_guard();
    let result = pools.checkout(&client, "testdb", "testuser", false).await;
    assert!(result.is_err(), "6th checkout should fail at max=5");
    assert!(
        result.err().unwrap().to_string().contains("timed out"),
        "6th checkout should time out"
    );

    drop(guards);
    mock.stop().await;
}

// ---------------------------------------------------------------------------
// SCRAM-SHA-256 upstream authentication
// ---------------------------------------------------------------------------

/// Build a pool backed by a SCRAM-auth mock, with a pgpass entry for the password.
fn setup_scram_pool(mock: &MockPg, password: &str) -> (Arc<PoolManager>, Arc<ArcSwap<Config>>) {
    let config = Arc::new(ArcSwap::from_pointee(test_config(&mock.addr())));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));
    seed_primary(&topology, mock.addr());
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(&format!(
        "*:*:*:*:{password}"
    )));
    let pools = Arc::new(PoolManager::new(
        Arc::clone(&config),
        Arc::clone(&topology),
        pgpass,
    ));
    (pools, config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_auth_success() {
    let password = "testpassword";
    let verifier = mock_pg::test_verifier(password);
    let mock = MockPg::start(MockBehavior::ScramAuth(verifier)).await;

    let (pools, _config) = setup_scram_pool(&mock, password);

    // Checkout should succeed — SCRAM exchange completes transparently.
    let client = test_client_guard();
    let guard = pools.checkout(&client, "testdb", "testuser", false).await;
    assert!(
        guard.is_ok(),
        "SCRAM checkout should succeed: {:?}",
        guard.err()
    );

    drop(guard);
    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_auth_wrong_password() {
    let verifier = mock_pg::test_verifier("correct");
    let mock = MockPg::start(MockBehavior::ScramAuth(verifier)).await;

    // Provide the wrong password via pgpass.
    let (pools, _config) = setup_scram_pool(&mock, "wrong");

    let client = test_client_guard();
    let result = pools.checkout(&client, "testdb", "testuser", false).await;
    assert!(
        result.is_err(),
        "SCRAM checkout with wrong password should fail"
    );

    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_auth_no_password() {
    let verifier = mock_pg::test_verifier("password");
    let mock = MockPg::start(MockBehavior::ScramAuth(verifier)).await;

    // Empty pgpass — no password available.
    let config = Arc::new(ArcSwap::from_pointee(test_config(&mock.addr())));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));
    seed_primary(&topology, mock.addr());
    let pools = Arc::new(PoolManager::new(
        Arc::clone(&config),
        Arc::clone(&topology),
        Arc::new(halephant_core::auth::pgpass::Pgpass::parse("")),
    ));

    let client = test_client_guard();
    let result = pools.checkout(&client, "testdb", "testuser", false).await;
    assert!(result.is_err(), "checkout without password should fail");
    let err = result.err().unwrap().to_string();
    assert!(
        err.contains("pgpass") || err.contains("password"),
        "error should mention missing password: {err}"
    );

    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_auth_query_round_trip() {
    let password = "querypass";
    let verifier = mock_pg::test_verifier(password);
    let mock = MockPg::start(MockBehavior::ScramAuth(verifier)).await;

    let (pools, _config) = setup_scram_pool(&mock, password);

    let (client_tcp, proxy_tcp) = tcp_pair().await;
    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    // Run a query through the SCRAM-authenticated connection.
    let msgs = query_collect(&mut client, "SELECT 1").await;
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::CommandComplete(_))),
        "should get CommandComplete"
    );

    client.send(FrontendMessage::Terminate).await.unwrap();
    handle.await.unwrap().unwrap();
    mock.stop().await;
}

// ---------------------------------------------------------------------------
// Scavenger floor preservation
// ---------------------------------------------------------------------------

/// Total idle connections across every pool entry. Helper for scavenger tests.
fn total_idle(pools: &PoolManager) -> u32 {
    pools.pool_stats().iter().map(|(_, s)| s.idle).sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scavenger_preserves_min_connections_floor() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let toml = format!(
        r#"
        [cluster.test]
        nodes = ["{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        idle_timeout = "50ms"
        max_connections = {{ primary = 10 }}

        [cluster.test.pool.testdb.user.testuser]
        min_connections = {{ primary = 3 }}
    "#,
        mock.addr()
    );

    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary(&topology, mock.addr());
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    pools.warm_up().await;
    assert_eq!(total_idle(&pools), 3, "warm-up should produce 3 idle");

    // Sleep past idle_timeout — every connection is now eligible for
    // idle-timeout removal, but the floor must keep all 3 alive.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    pools.scavenge_idle();
    assert_eq!(
        total_idle(&pools),
        3,
        "scavenger must preserve min_connections.primary"
    );

    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scavenger_releases_excess_above_floor() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    // min = 2, but warmup + a transient burst leaves 5 connections idle.
    let toml = format!(
        r#"
        [cluster.test]
        nodes = ["{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        idle_timeout = "50ms"
        max_connections = {{ primary = 10 }}

        [cluster.test.pool.testdb.user.testuser]
        min_connections = {{ primary = 2 }}
    "#,
        mock.addr()
    );

    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary(&topology, mock.addr());
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    pools.warm_up().await;
    assert_eq!(total_idle(&pools), 2, "warm-up populates the floor");

    // Burst: 3 concurrent checkouts. The first 2 reuse warm-up connections;
    // the 3rd opens a fresh one. After every checkin, all 3 return to idle.
    let client = test_client_guard();
    let mut guards = Vec::new();
    for _ in 0..3 {
        guards.push(
            pools
                .checkout(&client, "testdb", "testuser", false)
                .await
                .unwrap(),
        );
    }
    for guard in guards {
        guard.checkin();
    }

    // Wait for the background reset+checkin to drop the burst connections
    // back into the idle queue.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while total_idle(&pools) < 3 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            tokio::time::Instant::now() < deadline,
            "burst connections never returned to idle: idle={}",
            total_idle(&pools)
        );
    }

    // Sleep past idle_timeout. Scavenger should drop the excess (1) but
    // keep the 2-connection floor intact.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    pools.scavenge_idle();
    assert_eq!(
        total_idle(&pools),
        2,
        "scavenger should release excess down to the floor",
    );

    mock.stop().await;
}

// ---------------------------------------------------------------------------
// Per-node min/max with multiple replicas
// ---------------------------------------------------------------------------

/// Seed the `test` cluster with one primary and multiple replicas. Used by
/// the per-node semantic tests so warm-up sees the full topology without
/// running the real probe (which would need each mock to handle the role
/// check query).
fn seed_primary_with_replicas(
    topology: &TopologyManager,
    primary_addr: String,
    replica_addrs: Vec<String>,
) {
    topology.set(
        "test",
        ClusterTopology {
            primary: Some(primary_addr),
            replicas: replica_addrs,
            unreachable: Vec::new(),
        },
    );
}

/// Total idle connections at a specific node across all (db, user) pools.
fn idle_at_node(pools: &PoolManager, node: &str) -> u32 {
    pools
        .pool_stats()
        .iter()
        .filter(|(k, _)| k.node == node)
        .map(|(_, s)| s.idle)
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warmup_per_node_min_with_two_replicas() {
    // min_connections.replica = 3 means 3 PER replica node, not 3
    // round-robined across replicas. With 2 replicas, expect 6 idle in
    // total (3+3), not 3.
    let primary = MockPg::start(MockBehavior::Ok).await;
    let replica_a = MockPg::start_replica(MockBehavior::Ok).await;
    let replica_b = MockPg::start_replica(MockBehavior::Ok).await;
    let toml = format!(
        r#"
        [cluster.test]
        nodes = ["{}", "{}", "{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 10, replica = 10 }}

        [cluster.test.pool.testdb.user.testuser]
        min_connections = {{ primary = 0, replica = 3 }}
    "#,
        primary.addr(),
        replica_a.addr(),
        replica_b.addr(),
    );

    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary_with_replicas(
        &topology,
        primary.addr(),
        vec![replica_a.addr(), replica_b.addr()],
    );
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    pools.warm_up().await;

    assert_eq!(
        total_idle(&pools),
        6,
        "warm-up should produce 3 per replica = 6 total"
    );
    assert_eq!(idle_at_node(&pools, &replica_a.addr()), 3);
    assert_eq!(idle_at_node(&pools, &replica_b.addr()), 3);

    primary.stop().await;
    replica_a.stop().await;
    replica_b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scavenger_preserves_per_replica_floor() {
    let primary = MockPg::start(MockBehavior::Ok).await;
    let replica_a = MockPg::start_replica(MockBehavior::Ok).await;
    let replica_b = MockPg::start_replica(MockBehavior::Ok).await;
    let toml = format!(
        r#"
        [cluster.test]
        nodes = ["{}", "{}", "{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        idle_timeout = "50ms"
        max_connections = {{ primary = 10, replica = 10 }}

        [cluster.test.pool.testdb.user.testuser]
        min_connections = {{ primary = 0, replica = 2 }}
    "#,
        primary.addr(),
        replica_a.addr(),
        replica_b.addr(),
    );

    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary_with_replicas(
        &topology,
        primary.addr(),
        vec![replica_a.addr(), replica_b.addr()],
    );
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    pools.warm_up().await;
    assert_eq!(
        total_idle(&pools),
        4,
        "warm-up should produce 2 per replica"
    );
    assert_eq!(idle_at_node(&pools, &replica_a.addr()), 2);
    assert_eq!(idle_at_node(&pools, &replica_b.addr()), 2);

    // Sleep past idle_timeout. The scavenger must keep the 2-connection
    // floor on each replica intact, not drop one or both replicas to zero.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    pools.scavenge_idle();
    assert_eq!(
        total_idle(&pools),
        4,
        "scavenger must preserve the floor on every replica"
    );
    assert_eq!(idle_at_node(&pools, &replica_a.addr()), 2);
    assert_eq!(idle_at_node(&pools, &replica_b.addr()), 2);

    primary.stop().await;
    replica_a.stop().await;
    replica_b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_only_falls_back_to_primary_when_replicas_full() {
    // max_connections.replica = 2 means 2 PER replica node. With 2
    // replicas, total replica capacity is 4. The 5th read-only
    // checkout falls back to the primary instead of timing out.
    let primary = MockPg::start(MockBehavior::Ok).await;
    let replica_a = MockPg::start_replica(MockBehavior::Ok).await;
    let replica_b = MockPg::start_replica(MockBehavior::Ok).await;
    let toml = format!(
        r#"
        [server]
        checkout_timeout = "{TEST_CHECKOUT_TIMEOUT}"

        [cluster.test]
        nodes = ["{}", "{}", "{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 10, replica = 2 }}

        [cluster.test.pool.testdb.user.testuser]
    "#,
        primary.addr(),
        replica_a.addr(),
        replica_b.addr(),
    );

    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary_with_replicas(
        &topology,
        primary.addr(),
        vec![replica_a.addr(), replica_b.addr()],
    );
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    let client = test_client_guard();

    // Four read-only checkouts fill both replicas (2 each).
    let mut guards = Vec::new();
    for _ in 0..4 {
        let guard = pools
            .checkout(&client, "testdb", "testuser", true)
            .await
            .expect("first 4 read-only checkouts should succeed");
        guards.push(guard);
    }

    // 5th read-only checkout falls back to the primary.
    let guard5 = pools
        .checkout(&client, "testdb", "testuser", true)
        .await
        .expect("5th read-only checkout should fall back to primary");
    assert_eq!(
        guard5.node(),
        primary.addr(),
        "fallback should route to the primary"
    );

    drop(guards);
    drop(guard5);
    primary.stop().await;
    replica_a.stop().await;
    replica_b.stop().await;
}

// ---------------------------------------------------------------------------
// Wait-queue semantics
// ---------------------------------------------------------------------------

/// A waiter queued on an exhausted pool is woken when an active
/// connection is returned via `checkin`, and the returning connection
/// is handed to the waiter instead of staying idle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_wakes_waiter_on_checkin() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    // Long checkout_timeout so the test blocks on wakeup, not timeout.
    let toml = format!(
        r#"
        [server]
        checkout_timeout = "5s"

        [cluster.test]
        nodes = ["{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 1 }}

        [cluster.test.pool.testdb.user.testuser]
    "#,
        mock.addr()
    );
    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary(&topology, mock.addr());
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    // Checkout 1 holds the only slot.
    let client1 = test_client_guard();
    let guard1 = pools
        .checkout(&client1, "testdb", "testuser", false)
        .await
        .expect("first checkout should succeed");

    // Checkout 2 enqueues in a background task and blocks.
    let pools2 = Arc::clone(&pools);
    let waiter = tokio::spawn(async move {
        let client2 = test_client_guard();
        pools2.checkout(&client2, "testdb", "testuser", false).await
    });

    // Give the waiter enough time to enqueue.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Return guard1 — this should wake the waiter.
    guard1.checkin();

    // Waiter should resolve successfully within a reasonable time.
    let guard2 = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
        .await
        .expect("waiter did not wake within 2s")
        .expect("waiter task panicked")
        .expect("waiter checkout should succeed");

    drop(guard2);
    mock.stop().await;
}

/// FIFO: when a slot frees, the waiter who enqueued first gets woken.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_is_fifo() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let mock = MockPg::start(MockBehavior::Ok).await;
    let toml = format!(
        r#"
        [server]
        checkout_timeout = "5s"

        [cluster.test]
        nodes = ["{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 1 }}

        [cluster.test.pool.testdb.user.testuser]
    "#,
        mock.addr()
    );
    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary(&topology, mock.addr());
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    // Initial slot holder.
    let client0 = test_client_guard();
    let guard0 = pools
        .checkout(&client0, "testdb", "testuser", false)
        .await
        .unwrap();

    let order = Arc::new(AtomicU32::new(0));

    // Helper: synchronously wait until the total wait-queue depth
    // reaches `target`. This replaces fixed-duration sleeps so the
    // test's ordering guarantees don't depend on CI scheduler jitter.
    let wait_for_depth = |target: u32| {
        let pools = Arc::clone(&pools);
        async move {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                let depth: u32 = pools.queue_stats().iter().map(|q| q.depth).sum();
                if depth >= target {
                    return;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "wait queue never reached depth {target}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
    };

    let waiter_a = {
        let pools = Arc::clone(&pools);
        let order = Arc::clone(&order);
        tokio::spawn(async move {
            let client = test_client_guard();
            let guard = pools
                .checkout(&client, "testdb", "testuser", false)
                .await
                .unwrap();
            let woke_at = order.fetch_add(1, Ordering::SeqCst);
            (woke_at, guard)
        })
    };
    // Ensure A has fully enqueued before spawning B so the FIFO order
    // is deterministic — without this, a slow-to-schedule A could
    // land after B.
    wait_for_depth(1).await;

    let waiter_b = {
        let pools = Arc::clone(&pools);
        let order = Arc::clone(&order);
        tokio::spawn(async move {
            let client = test_client_guard();
            let guard = pools
                .checkout(&client, "testdb", "testuser", false)
                .await
                .unwrap();
            let woke_at = order.fetch_add(1, Ordering::SeqCst);
            (woke_at, guard)
        })
    };
    // Both waiters now enqueued.
    wait_for_depth(2).await;

    // Release the initial slot. Waiter A should wake first.
    guard0.checkin();
    let (a_order, guard_a) = waiter_a.await.unwrap();
    assert_eq!(a_order, 0, "waiter A (enqueued first) should wake first");

    // Release A's slot. Waiter B should wake next.
    guard_a.checkin();
    let (b_order, guard_b) = waiter_b.await.unwrap();
    assert_eq!(b_order, 1, "waiter B (enqueued second) should wake second");

    drop(guard_b);
    mock.stop().await;
}

/// A checkout that times out while queued returns a classified
/// `checkout_timeout` error and leaves the pool state consistent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_timeout() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let toml = format!(
        r#"
        [server]
        checkout_timeout = "50ms"

        [cluster.test]
        nodes = ["{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 1 }}

        [cluster.test.pool.testdb.user.testuser]
    "#,
        mock.addr()
    );
    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary(&topology, mock.addr());
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    let client = test_client_guard();

    // Take the only slot.
    let guard = pools
        .checkout(&client, "testdb", "testuser", false)
        .await
        .unwrap();

    // Second checkout: should enqueue, hit the 50ms timeout, and fail.
    let start = std::time::Instant::now();
    let result = pools.checkout(&client, "testdb", "testuser", false).await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "second checkout should time out");
    assert!(
        elapsed >= std::time::Duration::from_millis(50),
        "should have waited at least checkout_timeout before failing: {elapsed:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "should have failed quickly after the timeout elapsed: {elapsed:?}"
    );
    assert!(result.err().unwrap().to_string().contains("timed out"));

    drop(guard);
    mock.stop().await;
}

/// When a waiter's receiver is dropped (cancelled), `wake_one_for_node`
/// skips it and wakes the next one. Confirmed by observing that a
/// cancelled waiter does not "steal" wakeups from later waiters.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_skips_cancelled_waiter() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let toml = format!(
        r#"
        [server]
        checkout_timeout = "5s"

        [cluster.test]
        nodes = ["{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 1 }}

        [cluster.test.pool.testdb.user.testuser]
    "#,
        mock.addr()
    );
    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary(&topology, mock.addr());
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    let client0 = test_client_guard();
    let guard0 = pools
        .checkout(&client0, "testdb", "testuser", false)
        .await
        .unwrap();

    // Enqueue a waiter that we'll cancel.
    let pools_cancelled = Arc::clone(&pools);
    let cancelled = tokio::spawn(async move {
        let client = test_client_guard();
        pools_cancelled
            .checkout(&client, "testdb", "testuser", false)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Enqueue a real waiter behind the cancelled one.
    let pools_real = Arc::clone(&pools);
    let real = tokio::spawn(async move {
        let client = test_client_guard();
        pools_real
            .checkout(&client, "testdb", "testuser", false)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Cancel the first waiter by aborting its task. This drops the
    // receiver side of its oneshot.
    cancelled.abort();
    let _ = cancelled.await; // reap cancelled task

    // Release the slot. wake_one_for_node should skip the cancelled
    // sender and wake the real waiter.
    guard0.checkin();

    let guard = tokio::time::timeout(std::time::Duration::from_secs(2), real)
        .await
        .expect("real waiter did not wake within 2s")
        .expect("real waiter task panicked")
        .expect("real waiter checkout should succeed");

    drop(guard);
    mock.stop().await;
}

/// A replica queue serves waiters regardless of which replica returns
/// an idle connection first. Two waiters enqueue when both replicas are
/// full; freeing one replica wakes one waiter, freeing the other wakes
/// the second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_replica_is_shared() {
    let primary = MockPg::start(MockBehavior::Ok).await;
    let replica_a = MockPg::start_replica(MockBehavior::Ok).await;
    let replica_b = MockPg::start_replica(MockBehavior::Ok).await;
    let toml = format!(
        r#"
        [server]
        checkout_timeout = "5s"

        [cluster.test]
        nodes = ["{}", "{}", "{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 10, replica = 1 }}

        [cluster.test.pool.testdb.user.testuser]
    "#,
        primary.addr(),
        replica_a.addr(),
        replica_b.addr(),
    );
    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary_with_replicas(
        &topology,
        primary.addr(),
        vec![replica_a.addr(), replica_b.addr()],
    );
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    // Fill both replica slots. Round-robin distributes to A then B.
    let client = test_client_guard();
    let guard_a = pools
        .checkout(&client, "testdb", "testuser", true)
        .await
        .unwrap();
    let guard_b = pools
        .checkout(&client, "testdb", "testuser", true)
        .await
        .unwrap();

    // Two waiters, each on the shared Replica queue.
    let pools1 = Arc::clone(&pools);
    let waiter1 = tokio::spawn(async move {
        let c = test_client_guard();
        pools1.checkout(&c, "testdb", "testuser", true).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let pools2 = Arc::clone(&pools);
    let waiter2 = tokio::spawn(async move {
        let c = test_client_guard();
        pools2.checkout(&c, "testdb", "testuser", true).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Release replica A. Waiter 1 should wake.
    guard_a.checkin();
    let g1 = tokio::time::timeout(std::time::Duration::from_secs(2), waiter1)
        .await
        .expect("waiter1 did not wake within 2s")
        .unwrap()
        .expect("waiter1 checkout should succeed");

    // Release replica B. Waiter 2 should wake.
    guard_b.checkin();
    let g2 = tokio::time::timeout(std::time::Duration::from_secs(2), waiter2)
        .await
        .expect("waiter2 did not wake within 2s")
        .unwrap()
        .expect("waiter2 checkout should succeed");

    drop(g1);
    drop(g2);
    primary.stop().await;
    replica_a.stop().await;
    replica_b.stop().await;
}

/// `pools.shutdown()` wakes every queued waiter with a classified
/// `"shutting down"` error instead of making them sit until their
/// individual `checkout_timeout` expires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_wakes_queued_waiters() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    // Long checkout_timeout so the test fails clearly if shutdown
    // doesn't wake promptly.
    let toml = format!(
        r#"
        [server]
        checkout_timeout = "10s"

        [cluster.test]
        nodes = ["{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 1 }}

        [cluster.test.pool.testdb.user.testuser]
    "#,
        mock.addr()
    );
    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary(&topology, mock.addr());
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    // Take the only slot.
    let client0 = test_client_guard();
    let _guard0 = pools
        .checkout(&client0, "testdb", "testuser", false)
        .await
        .unwrap();

    // Spawn a waiter and give it time to enqueue.
    let pools_waiter = Arc::clone(&pools);
    let waiter = tokio::spawn(async move {
        let client = test_client_guard();
        pools_waiter
            .checkout(&client, "testdb", "testuser", false)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Shut down the pool. The waiter should wake immediately with a
    // shutting-down error, not wait 10 seconds for `checkout_timeout`.
    let start = std::time::Instant::now();
    pools.shutdown();
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
        .await
        .expect("waiter did not wake within 2s of shutdown")
        .expect("waiter task panicked");
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "shutdown should wake waiters quickly, took: {elapsed:?}"
    );
    let err = result
        .err()
        .expect("waiter should fail with shutting-down error");
    assert!(
        err.to_string().contains("shutting down"),
        "error should mention shutdown, got: {err}"
    );

    mock.stop().await;
}

/// A client's registry entry shows `state == Waiting` and the correct
/// `waiting_for` target while it's blocked on a checkout, and both
/// reset to `Idle` / `None` on wakeup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiting_for_is_populated_during_wait() {
    use halephant_core::clients::{ClientRegistry, ClientState, WaitRole};

    let mock = MockPg::start(MockBehavior::Ok).await;
    let toml = format!(
        r#"
        [server]
        checkout_timeout = "5s"

        [cluster.test]
        nodes = ["{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 1 }}

        [cluster.test.pool.testdb.user.testuser]
    "#,
        mock.addr()
    );
    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary(&topology, mock.addr());
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    // Hold the only slot.
    let client0 = test_client_guard();
    let guard0 = pools
        .checkout(&client0, "testdb", "testuser", false)
        .await
        .unwrap();

    // Spawn the waiter with its own registry so the test can observe
    // its state via `snapshot()`. The spawned task hands back the
    // ClientGuard alongside the result so the entry lives long enough
    // for the post-wakeup assertions.
    let registry = Arc::new(ClientRegistry::new());
    let waiter_registry = Arc::clone(&registry);
    let pools_waiter = Arc::clone(&pools);
    let waiter = tokio::spawn(async move {
        let client = waiter_registry.register("127.0.0.1:0".parse().unwrap());
        let result = pools_waiter
            .checkout(&client, "testdb", "testuser", false)
            .await;
        (client, result)
    });

    // Poll the registry until the waiter is actually in Waiting state.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let snap = loop {
        let snap = registry.snapshot();
        if let Some(entry) = snap.first()
            && entry.state == ClientState::Waiting
        {
            break snap;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "waiter never transitioned to Waiting"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    let entry = &snap[0];
    let target = entry
        .waiting_for
        .as_ref()
        .expect("waiting_for should be populated during wait");
    assert_eq!(target.database, "testdb");
    assert_eq!(target.user, "testuser");
    assert_eq!(target.role, WaitRole::Primary);

    // Release the slot. The waiter should succeed, and after wakeup
    // the registry entry should show state == Idle and
    // waiting_for == None.
    guard0.checkin();
    let (held_client, result) = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
        .await
        .expect("waiter did not wake")
        .expect("waiter task panicked");
    let guard = result.expect("waiter checkout should succeed");

    let after = registry.snapshot();
    let entry = after
        .iter()
        .find(|e| e.id == held_client.id())
        .expect("entry still present");
    assert_eq!(entry.state, ClientState::Idle);
    assert!(
        entry.waiting_for.is_none(),
        "waiting_for should be cleared after wakeup"
    );

    drop(guard);
    drop(held_client);
    mock.stop().await;
}

/// A per-pool `checkout_timeout` override wins over the server-wide
/// default, so a pool can set a short timeout even when the server
/// default is long.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_pool_checkout_timeout_overrides_server_default() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    // Server default 30s; pool override 50ms. The pool override must
    // win — otherwise the test would hang for 30 seconds before
    // failing.
    let toml = format!(
        r#"
        [server]
        checkout_timeout = "30s"

        [cluster.test]
        nodes = ["{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 1 }}
        checkout_timeout = "50ms"

        [cluster.test.pool.testdb.user.testuser]
    "#,
        mock.addr()
    );
    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary(&topology, mock.addr());
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    let client = test_client_guard();

    // Take the only slot.
    let _guard = pools
        .checkout(&client, "testdb", "testuser", false)
        .await
        .unwrap();

    // Second checkout times out using the pool override (50ms), not
    // the server default (30s).
    let start = std::time::Instant::now();
    let result = pools.checkout(&client, "testdb", "testuser", false).await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "second checkout should time out");
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "pool override should short-circuit the 30s server default, took: {elapsed:?}"
    );
    assert!(result.err().unwrap().to_string().contains("timed out"));

    mock.stop().await;
}

// ---------------------------------------------------------------------------
// DEALLOCATE interception
// ---------------------------------------------------------------------------

/// `DEALLOCATE my_stmt` must never reach the upstream, because
/// halephant renames every client Parse to a canonical hashed name
/// before forwarding — the server doesn't know `my_stmt`, only
/// `_hp_<hash>`. The transaction layer intercepts the DEALLOCATE,
/// updates its per-client tracking, and synthesises a
/// `CommandComplete("DEALLOCATE")` + `ReadyForQuery(Idle)` response
/// without touching the server.
///
/// This test verifies both halves: the client sees the synthetic
/// success response, and the mock's query log does not contain the
/// DEALLOCATE string — only the `SELECT` that preceded it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deallocate_named_is_intercepted() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let (pools, _config) = setup_pool(&mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    // Seed the client-side prepared-statement mapping by sending a
    // Parse/Bind/Execute/Sync batch. The actual content of the Parse
    // doesn't matter — we just need `my_stmt` to exist in
    // ClientPrepared so the DEALLOCATE has something to release.
    client
        .send(FrontendMessage::Parse(
            halephant_core::proto::frontend::Parse {
                name: "my_stmt".into(),
                query: "SELECT 1".into(),
                param_types: vec![],
            },
        ))
        .await
        .unwrap();
    client
        .send(FrontendMessage::Bind(
            halephant_core::proto::frontend::Bind {
                portal: String::new(),
                statement: "my_stmt".into(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            },
        ))
        .await
        .unwrap();
    client
        .send(FrontendMessage::Execute(
            halephant_core::proto::frontend::Execute {
                portal: String::new(),
                max_rows: 0,
            },
        ))
        .await
        .unwrap();
    client.send(FrontendMessage::Sync).await.unwrap();

    // Drain the extended-protocol responses up to ReadyForQuery so
    // the connection is idle before we issue the DEALLOCATE query.
    loop {
        let msg = client.next().await.unwrap().unwrap();
        if matches!(msg, BackendMessage::ReadyForQuery(_)) {
            break;
        }
    }

    // Now send the DEALLOCATE. If the intercept works, halephant
    // synthesises the response without forwarding. If it doesn't,
    // halephant would forward `DEALLOCATE my_stmt` verbatim — on a
    // real server this would fail with "prepared statement does not
    // exist" because the server only knows `_hp_<hash>`.
    let msgs = query_collect(&mut client, "DEALLOCATE my_stmt").await;
    assert!(
        msgs.iter().any(|m| matches!(
            m,
            BackendMessage::CommandComplete(tag) if tag == "DEALLOCATE"
        )),
        "expected CommandComplete(\"DEALLOCATE\") in response: {msgs:?}"
    );
    assert!(
        matches!(
            msgs.last(),
            Some(BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        ),
        "expected ReadyForQuery(Idle) as final message: {msgs:?}"
    );

    // The critical assertion: the mock's simple-query log must NOT
    // contain the DEALLOCATE. If it does, the intercept isn't
    // actually absorbing the message.
    let received = mock.received_queries();
    assert!(
        !received
            .iter()
            .any(|q| q.to_ascii_uppercase().contains("DEALLOCATE")),
        "mock should not have received any DEALLOCATE query, got: {received:?}"
    );

    drop(client);
    let _ = handle.await;
    mock.stop().await;
}

/// Same as above for `DEALLOCATE ALL`. Verifies the wildcard path
/// through `ClientPrepared::release_all`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deallocate_all_is_intercepted() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let (pools, _config) = setup_pool(&mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    // Register two prepared statements so `release_all` has
    // something to drain.
    for name in ["stmt_a", "stmt_b"] {
        client
            .send(FrontendMessage::Parse(
                halephant_core::proto::frontend::Parse {
                    name: name.into(),
                    query: format!("SELECT {name}"),
                    param_types: vec![],
                },
            ))
            .await
            .unwrap();
        client
            .send(FrontendMessage::Bind(
                halephant_core::proto::frontend::Bind {
                    portal: String::new(),
                    statement: name.into(),
                    param_formats: vec![],
                    params: vec![],
                    result_formats: vec![],
                },
            ))
            .await
            .unwrap();
        client
            .send(FrontendMessage::Execute(
                halephant_core::proto::frontend::Execute {
                    portal: String::new(),
                    max_rows: 0,
                },
            ))
            .await
            .unwrap();
        client.send(FrontendMessage::Sync).await.unwrap();

        loop {
            let msg = client.next().await.unwrap().unwrap();
            if matches!(msg, BackendMessage::ReadyForQuery(_)) {
                break;
            }
        }
    }

    // DEALLOCATE ALL — absorbed by the intercept.
    let msgs = query_collect(&mut client, "DEALLOCATE ALL").await;
    assert!(
        msgs.iter().any(|m| matches!(
            m,
            BackendMessage::CommandComplete(tag) if tag == "DEALLOCATE"
        )),
        "expected CommandComplete(\"DEALLOCATE\") in response: {msgs:?}"
    );

    let received = mock.received_queries();
    assert!(
        !received
            .iter()
            .any(|q| q.to_ascii_uppercase().contains("DEALLOCATE")),
        "mock should not have received any DEALLOCATE query, got: {received:?}"
    );

    drop(client);
    let _ = handle.await;
    mock.stop().await;
}

// ---------------------------------------------------------------------------
// Prepared statement lifecycle — LRU eviction, unnamed reuse, disconnect
// ---------------------------------------------------------------------------

/// Helper: send Parse + Bind + Execute + Sync and drain until ReadyForQuery.
async fn extended_query(client: &mut Framed<TcpStream, BackendCodec>, name: &str, query: &str) {
    client
        .send(FrontendMessage::Parse(
            halephant_core::proto::frontend::Parse {
                name: name.into(),
                query: query.into(),
                param_types: vec![],
            },
        ))
        .await
        .unwrap();
    client
        .send(FrontendMessage::Bind(
            halephant_core::proto::frontend::Bind {
                portal: String::new(),
                statement: name.into(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            },
        ))
        .await
        .unwrap();
    client
        .send(FrontendMessage::Execute(
            halephant_core::proto::frontend::Execute {
                portal: String::new(),
                max_rows: 0,
            },
        ))
        .await
        .unwrap();
    client.send(FrontendMessage::Sync).await.unwrap();

    loop {
        let msg = client.next().await.unwrap().unwrap();
        if matches!(msg, BackendMessage::ReadyForQuery(_)) {
            break;
        }
    }
}

/// When the per-server LRU cache is full, the oldest prepared statement
/// is evicted with a Close before the new one is prepared.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_lru_eviction_sends_close() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let cfg = Config::parse(&format!(
        r#"
        [server]
        checkout_timeout = "{TEST_CHECKOUT_TIMEOUT}"
        max_prepared_statements = 2

        [cluster.test]
        nodes = ["{}"]

        [cluster.test.pool.testdb.user.testuser]
    "#,
        mock.addr()
    ))
    .unwrap();
    let (pools, _config) = setup_pool_with_config(cfg, &mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    // Wrap all 3 Parses in a single explicit transaction so they hit
    // the same server connection. Without BEGIN, each Parse is its
    // own transaction and the pool may hand out a fresh connection
    // (while the previous is still resetting), preventing the LRU
    // from filling.
    query_collect(&mut client, "BEGIN").await;
    extended_query(&mut client, "s1", "SELECT 1").await;
    extended_query(&mut client, "s2", "SELECT 2").await;
    extended_query(&mut client, "s3", "SELECT 3").await;
    query_collect(&mut client, "COMMIT").await;

    let closes = mock.received_closes();
    assert!(
        !closes.is_empty(),
        "expected at least one Close for LRU eviction, got none"
    );

    // All 3 canonical names should have been Parsed on the server.
    let parses = mock.received_parses();
    assert_eq!(
        parses.len(),
        3,
        "expected 3 Parse messages on server, got {parses:?}"
    );

    drop(client);
    let _ = handle.await;
    mock.stop().await;
}

/// An unnamed Parse("") followed by Bind("") in a later transaction
/// (different server checkout) re-prepares the statement transparently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unnamed_reuse_across_transactions() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let (pools, _config) = setup_pool(&mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    // Transaction 1: Parse("") + Bind + Execute + Sync.
    extended_query(&mut client, "", "SELECT 1").await;

    // Transaction 2: only Bind("") + Execute + Sync (no new Parse).
    // This is the lib/pq pattern that breaks without unnamed support.
    client
        .send(FrontendMessage::Bind(
            halephant_core::proto::frontend::Bind {
                portal: String::new(),
                statement: String::new(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            },
        ))
        .await
        .unwrap();
    client
        .send(FrontendMessage::Execute(
            halephant_core::proto::frontend::Execute {
                portal: String::new(),
                max_rows: 0,
            },
        ))
        .await
        .unwrap();
    client.send(FrontendMessage::Sync).await.unwrap();

    let mut saw_complete = false;
    loop {
        let msg = client.next().await.unwrap().unwrap();
        if matches!(msg, BackendMessage::CommandComplete(_)) {
            saw_complete = true;
        }
        assert!(
            !matches!(msg, BackendMessage::ErrorResponse(_)),
            "Bind on unnamed in new transaction should not fail"
        );
        if matches!(msg, BackendMessage::ReadyForQuery(_)) {
            break;
        }
    }
    assert!(saw_complete, "expected CommandComplete from unnamed reuse");

    drop(client);
    let _ = handle.await;
    mock.stop().await;
}

/// Replacing an unnamed Parse("") with a different query releases the
/// old canonical and the new one is used for subsequent Bind("").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unnamed_replacement_releases_old_canonical() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let (pools, _config) = setup_pool(&mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    extended_query(&mut client, "", "SELECT 1").await;
    extended_query(&mut client, "", "SELECT 2").await;

    let parses = mock.received_parses();
    assert_eq!(parses.len(), 2, "expected 2 Parse messages: {parses:?}");
    assert_ne!(parses[0], parses[1], "canonical names should differ");

    // Bind("") in a new transaction should use the second canonical.
    client
        .send(FrontendMessage::Bind(
            halephant_core::proto::frontend::Bind {
                portal: String::new(),
                statement: String::new(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            },
        ))
        .await
        .unwrap();
    client
        .send(FrontendMessage::Execute(
            halephant_core::proto::frontend::Execute {
                portal: String::new(),
                max_rows: 0,
            },
        ))
        .await
        .unwrap();
    client.send(FrontendMessage::Sync).await.unwrap();

    loop {
        let msg = client.next().await.unwrap().unwrap();
        assert!(
            !matches!(msg, BackendMessage::ErrorResponse(_)),
            "Bind on replaced unnamed should succeed"
        );
        if matches!(msg, BackendMessage::ReadyForQuery(_)) {
            break;
        }
    }

    // The re-prepare should use the second canonical, not the first.
    let all_parses = mock.received_parses();
    let last_parse = all_parses.last().unwrap();
    assert_eq!(last_parse, &parses[1]);

    drop(client);
    let _ = handle.await;
    mock.stop().await;
}

/// When a client disconnects, `PreparedGuard::drop` releases all
/// refcounts. With a single client, the store should be empty after.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_disconnect_releases_all_prepared() {
    use halephant_core::proxy::prepared::canonical_for_test;

    let mock = MockPg::start(MockBehavior::Ok).await;
    let (pools, _config) = setup_pool(&mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    extended_query(&mut client, "s1", "SELECT 1").await;
    extended_query(&mut client, "s2", "SELECT 2").await;
    extended_query(&mut client, "", "SELECT 3").await;

    // Store should have entries while the client is alive.
    let c1 = canonical_for_test("SELECT 1", &[]);
    let c2 = canonical_for_test("SELECT 2", &[]);
    let c3 = canonical_for_test("SELECT 3", &[]);
    assert!(pools.has_prepared(&c1), "SELECT 1 should be in store");
    assert!(pools.has_prepared(&c2), "SELECT 2 should be in store");
    assert!(pools.has_prepared(&c3), "SELECT 3 should be in store");

    // Drop the client — triggers PreparedGuard::drop → release_all.
    drop(client);
    let _ = handle.await;

    // All refcounts should be zero — entries removed.
    assert!(!pools.has_prepared(&c1), "SELECT 1 should be released");
    assert!(!pools.has_prepared(&c2), "SELECT 2 should be released");
    assert!(!pools.has_prepared(&c3), "SELECT 3 should be released");

    mock.stop().await;
}

/// Discriminant tag for asserting backend-message ordering without
/// caring about message payloads.
#[derive(Debug, PartialEq, Eq)]
enum Tag {
    Parse,
    Bind,
    Command,
}

/// A client that pipelines two extended-protocol statements in a single
/// implicit transaction — `Parse`/`Bind`/`Execute` twice, then one
/// trailing `Sync` — must receive every response for BOTH statements, in
/// protocol order.
///
/// This is the shape that drivers such as sqlx emit when they batch
/// statements. The transaction-mode proxy used to re-prepare statements
/// by injecting an out-of-band `Sync` and draining the server up to
/// `ReadyForQuery`, which silently swallowed the earlier statement's
/// `BindComplete`/`CommandComplete` and reordered `ParseComplete`. The
/// client then saw a desynchronized stream. This test pins the correct
/// behavior so the regression cannot return.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipelined_statements_preserve_all_responses() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let (pools, _config) = setup_pool(&mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    // Feed buffers without flushing; the trailing Sync flushes the whole
    // batch at once so the proxy reads a genuinely pipelined stream.
    for (name, query) in [("s1", "SELECT 1"), ("s2", "SELECT 2")] {
        client
            .feed(FrontendMessage::Parse(
                halephant_core::proto::frontend::Parse {
                    name: name.into(),
                    query: query.into(),
                    param_types: vec![],
                },
            ))
            .await
            .unwrap();
        client
            .feed(FrontendMessage::Bind(
                halephant_core::proto::frontend::Bind {
                    portal: String::new(),
                    statement: name.into(),
                    param_formats: vec![],
                    params: vec![],
                    result_formats: vec![],
                },
            ))
            .await
            .unwrap();
        client
            .feed(FrontendMessage::Execute(
                halephant_core::proto::frontend::Execute {
                    portal: String::new(),
                    max_rows: 0,
                },
            ))
            .await
            .unwrap();
    }
    client.send(FrontendMessage::Sync).await.unwrap();

    let mut msgs = Vec::new();
    loop {
        let m = client.next().await.unwrap().unwrap();
        let done = matches!(m, BackendMessage::ReadyForQuery(_));
        msgs.push(m);
        if done {
            break;
        }
    }

    // Both statements' completions must arrive — none swallowed.
    let count = |want: &Tag| {
        msgs.iter()
            .filter(|m| match m {
                BackendMessage::ParseComplete => want == &Tag::Parse,
                BackendMessage::BindComplete => want == &Tag::Bind,
                BackendMessage::CommandComplete(_) => want == &Tag::Command,
                _ => false,
            })
            .count()
    };
    assert_eq!(
        count(&Tag::Command),
        2,
        "both statements' CommandComplete must reach the client: {msgs:?}"
    );
    assert_eq!(
        count(&Tag::Bind),
        2,
        "both statements' BindComplete must reach the client: {msgs:?}"
    );
    assert_eq!(
        count(&Tag::Parse),
        2,
        "both statements' ParseComplete must reach the client: {msgs:?}"
    );

    // And in protocol order: each statement's Parse → Bind → Command,
    // not the reordered/duplicated stream the swallow produced.
    let order: Vec<Tag> = msgs
        .iter()
        .filter_map(|m| match m {
            BackendMessage::ParseComplete => Some(Tag::Parse),
            BackendMessage::BindComplete => Some(Tag::Bind),
            BackendMessage::CommandComplete(_) => Some(Tag::Command),
            _ => None,
        })
        .collect();
    assert_eq!(
        order,
        vec![
            Tag::Parse,
            Tag::Bind,
            Tag::Command,
            Tag::Parse,
            Tag::Bind,
            Tag::Command
        ],
        "responses must be in per-statement protocol order: {msgs:?}"
    );

    drop(client);
    let _ = handle.await;
    mock.stop().await;
}

/// A transparent re-prepare (and the LRU eviction it may trigger) must
/// be invisible to the client: the backend's `ParseComplete` for the
/// injected `Parse` and its `CloseComplete` for the evicted statement
/// are halephant's own traffic, not answers to anything the client sent.
///
/// Forces the path with `max_prepared_statements = 1`: preparing `s2`
/// evicts `s1`, so a later `Bind` on `s1` (sent without a fresh `Parse`,
/// the way a driver reuses a client-cached statement) makes halephant
/// re-`Parse` `s1` and evict `s2`. The client's `Bind` step must see a
/// clean `BindComplete` + `CommandComplete`, with no stray
/// `ParseComplete` or `CloseComplete`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reprepare_and_eviction_replies_are_hidden_from_client() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let cfg = Config::parse(&format!(
        r#"
        [server]
        checkout_timeout = "{TEST_CHECKOUT_TIMEOUT}"
        max_prepared_statements = 1

        [cluster.test]
        nodes = ["{}"]

        [cluster.test.pool.testdb.user.testuser]
    "#,
        mock.addr()
    ))
    .unwrap();
    let (pools, _config) = setup_pool_with_config(cfg, &mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    // Pin everything to one backend with an explicit transaction so the
    // per-connection LRU actually fills and evicts.
    query_collect(&mut client, "BEGIN").await;
    extended_query(&mut client, "s1", "SELECT 1").await; // prepares _hp(s1)
    extended_query(&mut client, "s2", "SELECT 2").await; // evicts _hp(s1), prepares _hp(s2)

    // Reuse s1 with only Bind + Execute + Sync (no Parse) — s1 is no
    // longer on the backend, so halephant must re-prepare it (injected
    // Parse) and evict s2 (injected Close).
    client
        .feed(FrontendMessage::Bind(
            halephant_core::proto::frontend::Bind {
                portal: String::new(),
                statement: "s1".into(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            },
        ))
        .await
        .unwrap();
    client
        .feed(FrontendMessage::Execute(
            halephant_core::proto::frontend::Execute {
                portal: String::new(),
                max_rows: 0,
            },
        ))
        .await
        .unwrap();
    client.send(FrontendMessage::Sync).await.unwrap();

    let mut msgs = Vec::new();
    loop {
        let m = client.next().await.unwrap().unwrap();
        let done = matches!(m, BackendMessage::ReadyForQuery(_));
        msgs.push(m);
        if done {
            break;
        }
    }

    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::BindComplete)),
        "client should see BindComplete for its Bind: {msgs:?}"
    );
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::CommandComplete(_))),
        "client should see CommandComplete for its Execute: {msgs:?}"
    );
    assert!(
        !msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ParseComplete)),
        "injected re-prepare ParseComplete must not leak to the client: {msgs:?}"
    );
    assert!(
        !msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::CloseComplete)),
        "injected eviction CloseComplete must not leak to the client: {msgs:?}"
    );

    drop(client);
    let _ = handle.await;
    mock.stop().await;
}

/// Send `Parse(name, query)` + `Sync` and collect responses through
/// `ReadyForQuery`. Used to drive a single rejected-Parse transaction.
async fn parse_only(
    client: &mut Framed<TcpStream, BackendCodec>,
    name: &str,
    query: &str,
) -> Vec<BackendMessage> {
    client
        .feed(FrontendMessage::Parse(
            halephant_core::proto::frontend::Parse {
                name: name.into(),
                query: query.into(),
                param_types: vec![],
            },
        ))
        .await
        .unwrap();
    client.send(FrontendMessage::Sync).await.unwrap();
    let mut msgs = Vec::new();
    loop {
        let m = client.next().await.unwrap().unwrap();
        let done = matches!(m, BackendMessage::ReadyForQuery(_));
        msgs.push(m);
        if done {
            break;
        }
    }
    msgs
}

/// A `Parse` the backend rejects must not leave a phantom entry in the
/// per-connection prepared-statement cache. halephant inserts the
/// canonical name optimistically before the backend confirms; if the
/// backend answers `ErrorResponse` instead of `ParseComplete`, that
/// insert must be rolled back. Otherwise the pooled (and reused)
/// connection reports the canonical as prepared when it is not, so a
/// later use is wrongly skipped — falsely "succeeding" or drawing
/// "prepared statement does not exist".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_parse_does_not_leave_stale_cache_entry() {
    use halephant_core::proxy::prepared::canonical_for_test;

    let mock = MockPg::start(MockBehavior::Ok).await;
    // max=1 forces the second transaction to reuse the same backend.
    let cfg = Config::parse(&format!(
        r#"
        [server]
        checkout_timeout = "5s"

        [cluster.test]
        nodes = ["{}"]

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 1 }}

        [cluster.test.pool.testdb.user.testuser]
    "#,
        mock.addr()
    ))
    .unwrap();
    let (pools, _config) = setup_pool_with_config(cfg, &mock);
    let (client_tcp, proxy_tcp) = tcp_pair().await;

    let handle = spawn_tx_forward(proxy_tcp, Arc::clone(&pools), "testdb", "testuser", false);
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    // Transaction 1: a Parse the backend rejects.
    let tx1 = parse_only(&mut client, "s1", "SELECT INVALID").await;
    assert!(
        tx1.iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse(_))),
        "tx1 should surface the backend Parse error: {tx1:?}"
    );

    // Transaction 2 reuses the single backend with the SAME query. If the
    // failed canonical were still cached, halephant would synthesize a
    // ParseComplete and skip the backend — falsely reporting success.
    // With rollback it re-forwards the Parse and the backend rejects it
    // again.
    let tx2 = parse_only(&mut client, "s2", "SELECT INVALID").await;
    assert!(
        tx2.iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse(_))),
        "tx2 must re-validate on the backend, not falsely succeed from a stale cache entry: {tx2:?}"
    );

    // The canonical Parse must have reached the backend both times.
    let canon = canonical_for_test("SELECT INVALID", &[]);
    let forwarded = mock
        .received_parses()
        .iter()
        .filter(|n| **n == canon)
        .count();
    assert_eq!(
        forwarded,
        2,
        "the rejected Parse should be re-forwarded on reuse, not skipped: {:?}",
        mock.received_parses()
    );

    client.send(FrontendMessage::Terminate).await.unwrap();
    let _ = handle.await;
    mock.stop().await;
}

/// Run one session-mode "client" against an already-checked-out backend:
/// send `Parse(name, query)` + `Sync`, collect responses through
/// `ReadyForQuery`, then disconnect so `session::forward` returns. The
/// backend connection (`guard`) is reused across calls, modelling pool
/// reuse of one upstream by successive client sessions.
async fn session_prepare(
    guard: &mut halephant_core::pool::ConnGuard,
    pools: &Arc<PoolManager>,
    name: &str,
    query: &str,
) -> Vec<BackendMessage> {
    let (client_tcp, proxy_tcp) = tcp_pair().await;
    let mut proxy = Framed::new(proxy_tcp, FrontendCodec::post_startup());
    let mut client = Framed::new(client_tcp, BackendCodec::new());

    let session = halephant_core::proxy::session::forward(&mut proxy, guard.conn(), pools);
    let driver = async {
        client
            .feed(FrontendMessage::Parse(
                halephant_core::proto::frontend::Parse {
                    name: name.into(),
                    query: query.into(),
                    param_types: vec![],
                },
            ))
            .await
            .unwrap();
        client.send(FrontendMessage::Sync).await.unwrap();
        let mut msgs = Vec::new();
        loop {
            let m = client.next().await.unwrap().unwrap();
            let done = matches!(m, BackendMessage::ReadyForQuery(_));
            msgs.push(m);
            if done {
                break;
            }
        }
        drop(client); // disconnect → session::forward returns
        msgs
    };
    let (session_res, msgs) = tokio::join!(session, driver);
    session_res.unwrap();
    msgs
}

/// Session mode binds one client to one backend, but the backend is
/// reused by later clients. psycopg3-style drivers auto-name prepared
/// statements per connection (`_pg3_0`, `_pg3_1`, …) restarting from 0
/// each connection, so two different clients both prepare `_pg3_0` for
/// different queries. If halephant forwards those names verbatim, the
/// reused backend already has `_pg3_0` and rejects the second with
/// `42P05 duplicate_prepared_statement`.
///
/// Canonicalizing names (`SHA-256(query, oids)`) — the same machinery
/// transaction mode uses — makes the backend see distinct hashes, so no
/// collision. This pins that session mode applies it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_mode_reused_backend_avoids_duplicate_prepared() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let (pools, _config) = setup_pool_with_config(test_config_session(&mock.addr()), &mock);

    let cg = test_client_guard();
    let mut guard = pools
        .checkout(&cg, "testdb", "testuser", false)
        .await
        .unwrap();

    // Client A prepares "_pg3_0" = SELECT 1 and disconnects.
    let a = session_prepare(&mut guard, &pools, "_pg3_0", "SELECT 1").await;
    assert!(
        !a.iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse(_))),
        "client A's prepare should succeed: {a:?}"
    );

    // Client B reuses the SAME backend and prepares "_pg3_0" = SELECT 2
    // (a DIFFERENT query under the same client-supplied name).
    let b = session_prepare(&mut guard, &pools, "_pg3_0", "SELECT 2").await;
    assert!(
        !b.iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse(_))),
        "client B must not collide on the reused backend's prepared name: {b:?}"
    );
    assert!(
        b.iter().any(|m| matches!(m, BackendMessage::ParseComplete)),
        "client B should see a ParseComplete: {b:?}"
    );

    drop(guard);
    mock.stop().await;
}

// ---------------------------------------------------------------------------
// Connection limit enforcement — user-level and pool-level
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_max_connections_enforced() {
    // Pool allows 10 primary connections, but userA is capped at 2.
    // The 3rd checkout for userA must time out.
    let mock = MockPg::start(MockBehavior::Ok).await;
    let cfg = Config::parse(&format!(
        r#"
        [server]
        checkout_timeout = "{TEST_CHECKOUT_TIMEOUT}"

        [cluster.test]
        nodes = ["{}"]

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 10 }}

        [cluster.test.pool.testdb.user.userA]
        max_connections = {{ primary = 2 }}

        [cluster.test.pool.testdb.user.userB]
    "#,
        mock.addr()
    ))
    .unwrap();
    let (pools, _config) = setup_pool_with_config(cfg, &mock);

    // userA: first 2 checkouts succeed.
    let client_a = test_client_guard();
    let g1 = pools
        .checkout(&client_a, "testdb", "userA", false)
        .await
        .expect("userA checkout 1");
    let g2 = pools
        .checkout(&client_a, "testdb", "userA", false)
        .await
        .expect("userA checkout 2");

    // userA: 3rd checkout hits the per-user limit.
    let err = pools.checkout(&client_a, "testdb", "userA", false).await;
    assert!(
        err.is_err(),
        "userA checkout 3 should fail (per-user max = 2)"
    );

    // userB: still has room under the pool-level limit.
    let client_b = test_client_guard();
    let g3 = pools
        .checkout(&client_b, "testdb", "userB", false)
        .await
        .expect("userB should succeed despite userA being full");

    drop((g1, g2, g3));
    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_max_connections_aggregate_across_users() {
    // Pool allows 3 primary connections total. Two users with no
    // per-user limit. The 4th checkout (any user) must time out.
    let mock = MockPg::start(MockBehavior::Ok).await;
    let cfg = Config::parse(&format!(
        r#"
        [server]
        checkout_timeout = "{TEST_CHECKOUT_TIMEOUT}"

        [cluster.test]
        nodes = ["{}"]

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 3 }}

        [cluster.test.pool.testdb.user.userA]
        [cluster.test.pool.testdb.user.userB]
    "#,
        mock.addr()
    ))
    .unwrap();
    let (pools, _config) = setup_pool_with_config(cfg, &mock);

    let client = test_client_guard();
    let g1 = pools
        .checkout(&client, "testdb", "userA", false)
        .await
        .expect("checkout 1");
    let g2 = pools
        .checkout(&client, "testdb", "userB", false)
        .await
        .expect("checkout 2");
    let g3 = pools
        .checkout(&client, "testdb", "userA", false)
        .await
        .expect("checkout 3");

    // 4th checkout: pool aggregate is 3/3 — should time out.
    let err = pools.checkout(&client, "testdb", "userB", false).await;
    assert!(
        err.is_err(),
        "checkout 4 should fail (pool max = 3, 3 already active)"
    );

    drop((g1, g2, g3));
    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_limit_within_pool_limit() {
    // Pool allows 4, userA allows 1, userB allows 3.
    // userA gets 1, userB gets 3, total = 4 = pool max.
    // An extra checkout for either user fails.
    let mock = MockPg::start(MockBehavior::Ok).await;
    let cfg = Config::parse(&format!(
        r#"
        [server]
        checkout_timeout = "{TEST_CHECKOUT_TIMEOUT}"

        [cluster.test]
        nodes = ["{}"]

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 4 }}

        [cluster.test.pool.testdb.user.userA]
        max_connections = {{ primary = 1 }}

        [cluster.test.pool.testdb.user.userB]
        max_connections = {{ primary = 3 }}
    "#,
        mock.addr()
    ))
    .unwrap();
    let (pools, _config) = setup_pool_with_config(cfg, &mock);

    let client = test_client_guard();
    let ga = pools
        .checkout(&client, "testdb", "userA", false)
        .await
        .expect("userA checkout 1");

    // userA is at its per-user cap.
    let err_a = pools.checkout(&client, "testdb", "userA", false).await;
    assert!(
        err_a.is_err(),
        "userA checkout 2 should fail (user max = 1)"
    );

    // userB can still get 3.
    let gb1 = pools
        .checkout(&client, "testdb", "userB", false)
        .await
        .expect("userB checkout 1");
    let gb2 = pools
        .checkout(&client, "testdb", "userB", false)
        .await
        .expect("userB checkout 2");
    let gb3 = pools
        .checkout(&client, "testdb", "userB", false)
        .await
        .expect("userB checkout 3");

    // Pool aggregate is now 4/4. userB's 4th hits the pool limit.
    let err_b = pools.checkout(&client, "testdb", "userB", false).await;
    assert!(
        err_b.is_err(),
        "userB checkout 4 should fail (pool max = 4, all used)"
    );

    drop((ga, gb1, gb2, gb3));
    mock.stop().await;
}

/// When userA holds the last pool-level slot and userB is queued,
/// checkin of userA's connection must wake userB — not just userA's
/// (empty) queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_user_wake_on_pool_level_checkin() {
    let mock = MockPg::start(MockBehavior::Ok).await;
    let cfg = Config::parse(&format!(
        r#"
        [server]
        checkout_timeout = "2s"

        [cluster.test]
        nodes = ["{}"]

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 1 }}

        [cluster.test.pool.testdb.user.userA]
        [cluster.test.pool.testdb.user.userB]
    "#,
        mock.addr()
    ))
    .unwrap();
    let (pools, _config) = setup_pool_with_config(cfg, &mock);

    // userA takes the single slot.
    let client_a = test_client_guard();
    let guard_a = pools
        .checkout(&client_a, "testdb", "userA", false)
        .await
        .expect("userA checkout should succeed");

    // userB's checkout blocks (pool is full).
    let pools2 = Arc::clone(&pools);
    let userb_handle = tokio::spawn(async move {
        let client_b = test_client_guard();
        pools2.checkout(&client_b, "testdb", "userB", false).await
    });

    // Brief yield so userB's checkout enqueues.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Return userA's connection — this must wake userB.
    guard_a.checkin();

    // userB should succeed well within the 2s timeout.
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), userb_handle)
        .await
        .expect("userB should be woken within 1s")
        .expect("task should not panic");

    assert!(
        result.is_ok(),
        "userB checkout should succeed after userA checkin, got: {:?}",
        result.err()
    );

    mock.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_only_user_does_not_fall_back_to_primary() {
    // A user with max_connections = { replica = 2 } (no primary) must
    // NOT fall back to the primary — user_has_capacity returns false
    // for the primary candidate.
    let primary = MockPg::start(MockBehavior::Ok).await;
    let replica = MockPg::start_replica(MockBehavior::Ok).await;
    let toml = format!(
        r#"
        [server]
        checkout_timeout = "{TEST_CHECKOUT_TIMEOUT}"

        [cluster.test]
        nodes = ["{}", "{}"]
        admin_user = "testuser"

        [cluster.test.pool.testdb]
        max_connections = {{ primary = 10, replica = 2 }}

        [cluster.test.pool.testdb.user.rouser]
        max_connections = {{ replica = 2 }}
    "#,
        primary.addr(),
        replica.addr(),
    );

    let config = Arc::new(ArcSwap::from_pointee(Config::parse(&toml).unwrap()));
    let pgpass = Arc::new(halephant_core::auth::pgpass::Pgpass::parse(""));
    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));
    seed_primary_with_replicas(&topology, primary.addr(), vec![replica.addr()]);
    let pools = Arc::new(PoolManager::new(Arc::clone(&config), topology, pgpass));

    let client = test_client_guard();

    // Fill the replica (2 connections).
    let g1 = pools
        .checkout(&client, "testdb", "rouser", true)
        .await
        .expect("checkout 1");
    let g2 = pools
        .checkout(&client, "testdb", "rouser", true)
        .await
        .expect("checkout 2");

    // 3rd checkout: replica full, primary budget is 0 — must time out.
    let result = pools.checkout(&client, "testdb", "rouser", true).await;
    assert!(
        result.is_err(),
        "read-only user should NOT fall back to primary"
    );

    drop((g1, g2));
    primary.stop().await;
    replica.stop().await;
}
