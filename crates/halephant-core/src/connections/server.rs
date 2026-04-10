//! Upstream PostgreSQL connection establishment.
//!
//! Owns the TCP dial + startup handshake that produces a fully
//! authenticated [`ServerConn`]. Used by every path that opens a
//! connection to an upstream PostgreSQL node — pool checkout, pool
//! refill, topology probing, LISTEN/NOTIFY multiplex.
//!
//! The [`ServerConn`] data type and its per-connection prepared
//! statement tracker [`ServerPrepared`] live here because they're
//! "what `connect_server` returns" and travel together through
//! every subsystem that holds an upstream connection.

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::auth;
use crate::config::cluster::pool::user::UserParameters;
use crate::o11y;
use crate::proto::backend::BackendMessage;
use crate::proto::codec::BackendCodec;
use crate::proto::frontend::{FrontendMessage, ProtocolVersion, Startup};

use super::options::escape;

// ---------------------------------------------------------------------------
// ServerConn — a fully initialized upstream connection
// ---------------------------------------------------------------------------

/// A server connection that has completed the PostgreSQL startup sequence and
/// is ready for query forwarding.
pub struct ServerConn {
    pub framed: Framed<TcpStream, BackendCodec>,
    pub params: Vec<(String, String)>,
    pub backend_key: (i32, i32),
    pub created_at: Instant,
    /// Tracks which prepared statements are cached on this server connection.
    pub prepared: ServerPrepared,
    /// GUC variables SET by the client during this session. Only these are
    /// RESET during connection reset, preserving startup parameters.
    pub dirty_vars: HashSet<String>,
}

// ---------------------------------------------------------------------------
// ServerPrepared — per-connection prepared-statement LRU cache
// ---------------------------------------------------------------------------

/// Tracks which canonical prepared statements exist on a specific
/// server connection, with LRU eviction when the configured limit is
/// reached.
///
/// Lives next to [`ServerConn`] because it's "state that follows a
/// connection through its lifetime" — created at `connect_server`
/// time, travels with the connection through checkout and checkin,
/// consulted by the transaction forwarder's
/// prepared-statement-rewriting path.
pub struct ServerPrepared {
    /// Set of canonical names currently prepared on this server.
    prepared: HashSet<String>,
    /// LRU order: front = oldest, back = most recently used.
    lru: VecDeque<String>,
    /// Maximum statements to keep prepared per server connection.
    max: u32,
}

impl ServerPrepared {
    pub fn new(max: u32) -> Self {
        Self {
            prepared: HashSet::new(),
            lru: VecDeque::new(),
            max,
        }
    }

    /// Check whether the canonical statement is already prepared on this server.
    pub fn contains(&self, canon: &str) -> bool {
        self.prepared.contains(canon)
    }

    /// Mark a canonical statement as prepared on this server. Returns the name
    /// of an evicted statement if the cache is full, which the caller must send
    /// a `Close` for before preparing the new one.
    pub fn insert(&mut self, canon: String) -> Option<String> {
        if self.prepared.contains(&canon) {
            // Move to back of LRU.
            self.lru.retain(|n| n != &canon);
            self.lru.push_back(canon);
            return None;
        }

        let evicted = if self.max > 0 && self.prepared.len() as u32 >= self.max {
            self.evict()
        } else {
            None
        };

        self.prepared.insert(canon.clone());
        self.lru.push_back(canon);
        evicted
    }

    /// Touch a statement in the LRU (it was used).
    pub fn touch(&mut self, canon: &str) {
        self.lru.retain(|n| n != canon);
        self.lru.push_back(canon.to_owned());
    }

    /// Evict the least recently used statement. Returns the canonical name of
    /// the evicted statement.
    fn evict(&mut self) -> Option<String> {
        let name = self.lru.pop_front()?;
        self.prepared.remove(&name);
        Some(name)
    }

    /// Clear all tracked statements (called after DISCARD ALL).
    pub fn clear(&mut self) {
        self.prepared.clear();
        self.lru.clear();
    }
}

// ---------------------------------------------------------------------------
// TCP + startup handshake
// ---------------------------------------------------------------------------

/// Connect to a TCP address with a timeout, TCP_NODELAY, and TCP keepalive.
/// Used for all upstream connections (pooled, listener, auth).
pub(crate) async fn connect_tcp(addr: &str, timeout: Duration) -> anyhow::Result<TcpStream> {
    let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
        .await
        .with_context(|| format!("connection to {addr} timed out"))?
        .with_context(|| format!("failed to connect to {addr}"))?;
    stream.set_nodelay(true)?;
    let sock = socket2::SockRef::from(&stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(30))
        .with_interval(Duration::from_secs(10));
    if let Err(e) = sock.set_tcp_keepalive(&keepalive) {
        tracing::debug!(%e, "failed to set TCP keepalive (dead-peer detection degraded)");
    }
    Ok(stream)
}

/// Open a new TCP connection to `upstream_addr` and complete the PostgreSQL
/// startup handshake, including password authentication if the server requests
/// it.
#[tracing::instrument(name = "pool.connect", skip_all, err(Display), fields(
    otel.kind = "client",
    db.system.name = "postgresql",
    db.namespace = %database,
    server.address = %upstream_addr,
    user = %user,
    otel.status_code,
    otel.status_description,
))]
pub(crate) async fn connect_server(
    upstream_addr: &str,
    database: &str,
    user: &str,
    password: Option<&str>,
    params: &UserParameters,
    max_prepared: u32,
    connect_timeout: Duration,
) -> anyhow::Result<ServerConn> {
    let connect_start = Instant::now();
    async {
        let stream = connect_tcp(upstream_addr, connect_timeout).await?;
        let mut framed = Framed::new(stream, BackendCodec::new());

        // Build startup parameters: user and database are required, then append
        // application_name and GUC options from the config.
        let mut startup_params = vec![
            ("user".into(), user.into()),
            ("database".into(), database.into()),
        ];
        if let Some(ref app_name) = params.application_name {
            startup_params.push(("application_name".into(), app_name.clone()));
        }
        if !params.options.is_empty() {
            let options: String = params
                .options
                .iter()
                .map(|(k, v)| format!("-c {}={}", escape(k), escape(v)))
                .collect::<Vec<_>>()
                .join(" ");
            startup_params.push(("options".into(), options));
        }

        // Send startup message.
        framed
            .send(FrontendMessage::Startup(Startup {
                version: ProtocolVersion { major: 3, minor: 0 },
                parameters: startup_params,
            }))
            .await?;

        // Collect startup responses. Named `server_params` to avoid
        // shadowing the `params: &UserParameters` function argument
        // — these are the `ParameterStatus` values the server reports
        // back, not the GUC settings the client sent in the startup
        // message.
        let mut server_params = Vec::new();
        let mut backend_key = (0, 0);

        loop {
            match framed
                .next()
                .await
                .transpose()
                .context("reading startup response")?
            {
                Some(BackendMessage::AuthenticationOk) => {}
                Some(BackendMessage::AuthenticationCleartextPassword) => {
                    let pw = password.context(
                        "upstream requires password auth but no password found in .pgpass",
                    )?;
                    framed
                        .send(FrontendMessage::PasswordMessage(pw.as_bytes().to_vec()))
                        .await?;
                }
                Some(BackendMessage::AuthenticationMd5Password { .. }) => {
                    anyhow::bail!(
                        "upstream {upstream_addr} requested MD5 auth — \
                         use SCRAM-SHA-256 or trust instead"
                    );
                }
                Some(BackendMessage::AuthenticationSasl { mechanisms }) => {
                    if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                        anyhow::bail!(
                            "upstream {upstream_addr} requires unsupported SASL mechanism: {mechanisms:?}"
                        );
                    }
                    let pw = password.context(
                        "upstream requires SCRAM auth but no password found in .pgpass",
                    )?;
                    auth::scram::client::authenticate(&mut framed, pw)
                        .await
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                }
                Some(BackendMessage::ParameterStatus { name, value }) => {
                    server_params.push((name, value));
                }
                Some(BackendMessage::BackendKeyData {
                    process_id,
                    secret_key,
                }) => {
                    backend_key = (process_id, secret_key);
                }
                Some(BackendMessage::ReadyForQuery(_)) => break,
                Some(BackendMessage::ErrorResponse(err)) => {
                    anyhow::bail!(
                        "upstream startup error: {}",
                        err.message().unwrap_or("unknown")
                    );
                }
                Some(other) => {
                    anyhow::bail!("unexpected message during upstream startup: {other:?}");
                }
                None => anyhow::bail!("upstream closed during startup"),
            }
        }

        Ok(ServerConn {
            framed,
            params: server_params,
            backend_key,
            created_at: Instant::now(),
            prepared: ServerPrepared::new(max_prepared),
            dirty_vars: HashSet::new(),
        })
    }
    .await
    .inspect(|_| {
        o11y::metrics::record_connect(connect_start, upstream_addr, database);
        tracing::Span::current().record("otel.status_code", "OK");
    })
    .inspect_err(|_| {
        o11y::metrics::record_error("connect_failed", upstream_addr);
        let span = tracing::Span::current();
        span.record("otel.status_code", "ERROR");
        span.record("otel.status_description", "connect failed");
    })
}

// ---------------------------------------------------------------------------
// Tests — ServerPrepared LRU behaviour
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_insert_and_contains() {
        let mut server = ServerPrepared::new(10);
        assert!(!server.contains("stmt_a"));
        server.insert("stmt_a".into());
        assert!(server.contains("stmt_a"));
    }

    #[test]
    fn server_lru_eviction() {
        let mut server = ServerPrepared::new(2);
        server.insert("a".into());
        server.insert("b".into());

        // Cache is full, inserting c should evict a (oldest).
        let evicted = server.insert("c".into());
        assert_eq!(evicted, Some("a".into()));
        assert!(!server.contains("a"));
        assert!(server.contains("b"));
        assert!(server.contains("c"));
    }

    #[test]
    fn server_lru_touch_prevents_eviction() {
        let mut server = ServerPrepared::new(2);
        server.insert("a".into());
        server.insert("b".into());

        // Touch a — now b is the oldest.
        server.touch("a");

        let evicted = server.insert("c".into());
        assert_eq!(evicted, Some("b".into()));
        assert!(server.contains("a"));
        assert!(server.contains("c"));
    }

    #[test]
    fn server_insert_existing_moves_to_back() {
        let mut server = ServerPrepared::new(2);
        server.insert("a".into());
        server.insert("b".into());

        // Re-insert a — should not evict, just move to back.
        let evicted = server.insert("a".into());
        assert!(evicted.is_none());

        // Now b is oldest — inserting c evicts b.
        let evicted = server.insert("c".into());
        assert_eq!(evicted, Some("b".into()));
    }

    #[test]
    fn server_clear() {
        let mut server = ServerPrepared::new(10);
        server.insert("a".into());
        server.insert("b".into());
        server.clear();
        assert!(!server.contains("a"));
        assert!(!server.contains("b"));
    }
}
