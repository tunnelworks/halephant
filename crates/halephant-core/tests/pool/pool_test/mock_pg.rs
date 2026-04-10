//! Minimal mock PostgreSQL server for integration tests.
//!
//! Speaks enough of the wire protocol to test halephant's proxy behavior:
//! startup handshake (trust auth), simple query responses, and extended query
//! protocol. Configurable to return canned responses or simulate failures.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_util::codec::Framed;

use halephant_core::auth::scram::ScramVerifier;
use halephant_core::auth::scram::server::ScramServer;
use halephant_core::proto::backend::BackendMessage;
use halephant_core::proto::codec::FrontendCodec;
use halephant_core::proto::frontend::FrontendMessage;
use halephant_core::proto::types::TransactionStatus;

/// A mock PostgreSQL server that accepts connections and responds to queries.
pub(crate) struct MockPg {
    addr: SocketAddr,
    shutdown: Arc<Notify>,
    handle: tokio::task::JoinHandle<()>,
    connection_count: Arc<AtomicU32>,
    /// Simple-query protocol strings the mock has received.
    received_queries: Arc<Mutex<Vec<String>>>,
    /// Statement names from Parse messages the mock has received.
    received_parses: Arc<Mutex<Vec<String>>>,
    /// Statement names from Close messages the mock has received.
    received_closes: Arc<Mutex<Vec<String>>>,
}

/// Controls how the mock responds to queries.
#[derive(Clone)]
pub(crate) enum MockBehavior {
    /// Respond to all queries with CommandComplete + ReadyForQuery.
    Ok,
    /// Require SCRAM-SHA-256 authentication with the given verifier.
    ScramAuth(ScramVerifier),
    /// Close the connection after the startup handshake (simulates server crash).
    CloseAfterStartup,
    /// Close the connection mid-query (after receiving Query but before responding).
    CloseOnQuery,
}

/// Build a SCRAM verifier from a plaintext password for testing.
pub(crate) fn test_verifier(password: &str) -> ScramVerifier {
    use hmac::Mac;
    use sha2::{Digest, Sha256};

    let salt = b"testsalt";
    let iterations = 4096;

    let mut salted_password = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted_password);

    let client_key = {
        let mut mac = hmac::Hmac::<Sha256>::new_from_slice(&salted_password)
            .expect("HMAC accepts any key size");
        mac.update(b"Client Key");
        let result: [u8; 32] = mac.finalize().into_bytes().into();
        result
    };
    let stored_key: [u8; 32] = Sha256::digest(client_key).into();
    let server_key = {
        let mut mac = hmac::Hmac::<Sha256>::new_from_slice(&salted_password)
            .expect("HMAC accepts any key size");
        mac.update(b"Server Key");
        let result: [u8; 32] = mac.finalize().into_bytes().into();
        result
    };

    ScramVerifier {
        iterations,
        salt: salt.to_vec(),
        stored_key,
        server_key,
    }
}

impl MockPg {
    /// Start a mock PostgreSQL server on a random port.
    pub(crate) async fn start(behavior: MockBehavior) -> Self {
        Self::start_with_opts(behavior, false).await
    }

    /// Start a mock that advertises itself as a replica (for topology discovery).
    pub(crate) async fn start_replica(behavior: MockBehavior) -> Self {
        Self::start_with_opts(behavior, true).await
    }

    async fn start_with_opts(behavior: MockBehavior, is_replica: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = Arc::new(Notify::new());
        let shutdown_rx = Arc::clone(&shutdown);
        let connection_count = Arc::new(AtomicU32::new(0));
        let count = Arc::clone(&connection_count);
        let received_queries: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let received_parses: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let received_closes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let queries_for_task = Arc::clone(&received_queries);
        let parses_for_task = Arc::clone(&received_parses);
        let closes_for_task = Arc::clone(&received_closes);

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (stream, _) = result.unwrap();
                        stream.set_nodelay(true).unwrap();
                        count.fetch_add(1, Ordering::Relaxed);
                        let behavior = behavior.clone();
                        let queries = Arc::clone(&queries_for_task);
                        let parses = Arc::clone(&parses_for_task);
                        let closes = Arc::clone(&closes_for_task);
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, behavior, is_replica, queries, parses, closes).await {
                                let _ = e;
                            }
                        });
                    }
                    () = shutdown_rx.notified() => break,
                }
            }
        });

        MockPg {
            addr,
            shutdown,
            handle,
            connection_count,
            received_queries,
            received_parses,
            received_closes,
        }
    }

    pub(crate) fn addr(&self) -> String {
        self.addr.to_string()
    }

    pub(crate) fn connections(&self) -> u32 {
        self.connection_count.load(Ordering::Relaxed)
    }

    /// Snapshot of simple-query protocol strings the mock has
    /// received across all connections since it started. Ordered by
    /// receipt time (within a single connection; concurrent
    /// connections may interleave).
    #[allow(dead_code)]
    pub(crate) fn received_queries(&self) -> Vec<String> {
        self.received_queries.lock().clone()
    }

    #[allow(dead_code)]
    pub(crate) fn received_parses(&self) -> Vec<String> {
        self.received_parses.lock().clone()
    }

    #[allow(dead_code)]
    pub(crate) fn received_closes(&self) -> Vec<String> {
        self.received_closes.lock().clone()
    }

    pub(crate) async fn stop(self) {
        self.shutdown.notify_one();
        let _ = self.handle.await;
    }
}

async fn handle_connection(
    stream: TcpStream,
    behavior: MockBehavior,
    is_replica: bool,
    received_queries: Arc<Mutex<Vec<String>>>,
    received_parses: Arc<Mutex<Vec<String>>>,
    received_closes: Arc<Mutex<Vec<String>>>,
) -> anyhow::Result<()> {
    let mut conn = Framed::new(stream, FrontendCodec::new());

    // Read startup message (may be preceded by SSLRequest).
    loop {
        match conn.next().await.transpose()? {
            Some(FrontendMessage::SslRequest) => {
                // Respond 'N' (no SSL).
                use tokio::io::AsyncWriteExt;
                conn.get_mut().write_all(b"N").await?;
            }
            Some(FrontendMessage::Startup(_)) => break,
            _ => return Ok(()),
        }
    }

    // Authenticate.
    match &behavior {
        MockBehavior::ScramAuth(verifier) => {
            scram_server_auth(&mut conn, verifier.clone()).await?;
        }
        _ => {
            conn.feed(BackendMessage::AuthenticationOk).await?;
        }
    }

    conn.feed(BackendMessage::ParameterStatus {
        name: "server_version".into(),
        value: "17.0".into(),
    })
    .await?;
    conn.feed(BackendMessage::ParameterStatus {
        name: "server_encoding".into(),
        value: "UTF8".into(),
    })
    .await?;
    conn.feed(BackendMessage::BackendKeyData {
        process_id: 1,
        secret_key: 1,
    })
    .await?;
    conn.send(BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        .await?;

    if matches!(behavior, MockBehavior::CloseAfterStartup) {
        return Ok(());
    }

    // Main loop: respond to messages.
    let mut in_transaction = false;

    loop {
        let Some(msg) = conn.next().await.transpose()? else {
            return Ok(());
        };

        match msg {
            FrontendMessage::Terminate => return Ok(()),

            FrontendMessage::Query(q) => {
                // Record before early-returning so the `CloseOnQuery`
                // behavior is still observable in `received_queries`.
                received_queries.lock().push(q.clone());

                if matches!(behavior, MockBehavior::CloseOnQuery) {
                    return Ok(());
                }

                let q_upper = q.to_ascii_uppercase();

                // Handle transaction control.
                if q_upper.starts_with("BEGIN") || q_upper.starts_with("START") {
                    in_transaction = true;
                    conn.feed(BackendMessage::CommandComplete("BEGIN".into()))
                        .await?;
                    conn.send(BackendMessage::ReadyForQuery(
                        TransactionStatus::InTransaction,
                    ))
                    .await?;
                } else if q_upper.starts_with("COMMIT") || q_upper.starts_with("END") {
                    in_transaction = false;
                    conn.feed(BackendMessage::CommandComplete("COMMIT".into()))
                        .await?;
                    conn.send(BackendMessage::ReadyForQuery(TransactionStatus::Idle))
                        .await?;
                } else if q_upper.starts_with("ROLLBACK") || q_upper.starts_with("ABORT") {
                    in_transaction = false;
                    conn.feed(BackendMessage::CommandComplete("ROLLBACK".into()))
                        .await?;
                    conn.send(BackendMessage::ReadyForQuery(TransactionStatus::Idle))
                        .await?;
                } else {
                    // Handle multi-statement reset queries (CLOSE ALL; UNLISTEN *; etc.)
                    let stmts: Vec<&str> = q.split(';').filter(|s| !s.trim().is_empty()).collect();
                    for stmt in &stmts {
                        let trimmed = stmt.trim().to_ascii_uppercase();
                        if trimmed.starts_with("SELECT") {
                            conn.feed(BackendMessage::RowDescription(vec![])).await?;
                            // pg_is_in_recovery() — return "t" or "f" as a DataRow.
                            if trimmed.contains("PG_IS_IN_RECOVERY") {
                                let val = if is_replica {
                                    b"t".to_vec()
                                } else {
                                    b"f".to_vec()
                                };
                                conn.feed(BackendMessage::DataRow(vec![Some(val)])).await?;
                            }
                            conn.feed(BackendMessage::CommandComplete("SELECT 1".into()))
                                .await?;
                        } else {
                            let tag = trimmed.split_whitespace().next().unwrap_or("OK").to_owned();
                            conn.feed(BackendMessage::CommandComplete(tag)).await?;
                        }
                    }
                    let status = if in_transaction {
                        TransactionStatus::InTransaction
                    } else {
                        TransactionStatus::Idle
                    };
                    conn.send(BackendMessage::ReadyForQuery(status)).await?;
                }
            }

            FrontendMessage::Parse(ref parse) => {
                received_parses.lock().push(parse.name.clone());
                conn.send(BackendMessage::ParseComplete).await?;
            }
            FrontendMessage::Bind(_) => {
                conn.send(BackendMessage::BindComplete).await?;
            }
            FrontendMessage::Describe(_) => {
                conn.send(BackendMessage::RowDescription(vec![])).await?;
            }
            FrontendMessage::Execute(_) => {
                conn.feed(BackendMessage::CommandComplete("SELECT 0".into()))
                    .await?;
                // ReadyForQuery is sent by the Sync handler that follows.
            }
            FrontendMessage::Sync => {
                let status = if in_transaction {
                    TransactionStatus::InTransaction
                } else {
                    TransactionStatus::Idle
                };
                conn.send(BackendMessage::ReadyForQuery(status)).await?;
            }
            FrontendMessage::Close(ref close) => {
                received_closes.lock().push(close.name.clone());
                conn.send(BackendMessage::CloseComplete).await?;
            }
            _ => {}
        }
    }
}

/// Perform server-side SCRAM-SHA-256 authentication on a mock connection.
async fn scram_server_auth(
    conn: &mut Framed<TcpStream, FrontendCodec>,
    verifier: ScramVerifier,
) -> anyhow::Result<()> {
    use halephant_core::auth::scram::server::parse_sasl_initial_response;

    // Send AuthenticationSASL.
    conn.send(BackendMessage::AuthenticationSasl {
        mechanisms: vec!["SCRAM-SHA-256".into()],
    })
    .await?;

    // Read SASLInitialResponse (sent as PasswordMessage).
    let initial = match conn.next().await.transpose()? {
        Some(FrontendMessage::PasswordMessage(data)) => data,
        other => anyhow::bail!("expected SASLInitialResponse, got {other:?}"),
    };
    let (_mechanism, client_first) = parse_sasl_initial_response(&initial)?;

    // Process client-first, send server-first.
    let mut server = ScramServer::new(verifier);
    let server_first = server.handle_client_first(client_first)?;
    conn.send(BackendMessage::AuthenticationSaslContinue { data: server_first })
        .await?;

    // Read SASLResponse (client-final).
    let client_final = match conn.next().await.transpose()? {
        Some(FrontendMessage::PasswordMessage(data)) => data,
        other => anyhow::bail!("expected SASLResponse, got {other:?}"),
    };

    // Verify and send server-final.
    let server_final = server.handle_client_final(&client_final)?;
    conn.feed(BackendMessage::AuthenticationSaslFinal { data: server_final })
        .await?;
    conn.send(BackendMessage::AuthenticationOk).await?;

    Ok(())
}
