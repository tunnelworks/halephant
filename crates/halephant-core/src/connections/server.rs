//! Upstream PostgreSQL connection establishment.
//!
//! Owns the TCP dial + startup handshake that produces a fully
//! authenticated [`ServerConn`]. Used by every path that opens a
//! connection to an upstream PostgreSQL node — pool checkout, pool
//! refill, topology probing, LISTEN/NOTIFY multiplex.
//!
//! The [`ServerConn`] data type and its per-connection prepared
//! statement state [`PreparedStatements`] live here because they're
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
    /// Per-connection prepared-statement state: which canonical statements
    /// are prepared on this backend, plus the bookkeeping to filter the
    /// backend's `ParseComplete`/`CloseComplete` replies that answer
    /// messages halephant injected on the client's behalf.
    pub statements: PreparedStatements,
    /// GUC variables SET by the client during this session. Only these are
    /// RESET during connection reset, preserving startup parameters.
    pub dirty_vars: HashSet<String>,
    /// Last `ReadyForQuery` status seen on this connection. Updated by
    /// the proxy forwarding loop so `reset_connection` knows whether a
    /// `ROLLBACK` is needed. Starts as `Idle` (freshly connected).
    pub last_tx_status: crate::proto::types::TransactionStatus,
}

// ---------------------------------------------------------------------------
// PreparedStatements — per-connection prepared-statement state
// ---------------------------------------------------------------------------

/// Per-connection prepared-statement state for the proxy: which canonical
/// statements are prepared on this backend (LRU-bounded), plus the
/// bookkeeping to filter the backend's `ParseComplete`/`CloseComplete`
/// replies that answer messages halephant injected on the client's
/// behalf.
///
/// halephant proxies the extended query protocol with statement-name
/// rewriting and transparent (re)preparation, so it emits `Parse`/`Close`
/// messages the client never sent (re-prepares, LRU evictions) and
/// absorbs others (client statement closes). The backend's reply stream
/// therefore carries `ParseComplete`/`CloseComplete` messages that do not
/// match what the client expects, and the proxy must filter them — in
/// FIFO order, correct under pipelining, and rolled back if a `Parse` is
/// rejected.
///
/// The cache and the reply bookkeeping are deliberately one type so the
/// invariant between them — every optimistic insert pairs with a queued
/// reply disposition, and the error path reverts both — is enforced
/// inside these methods rather than by convention across call sites. The
/// methods are pure and return intents; the async proxy layer executes
/// the wire actions. See
/// `docs/superpowers/specs/2026-06-30-halephant-prepared-statement-state-design.md`
/// for the alternatives considered.
///
/// Lives next to [`ServerConn`] because it is state that follows a
/// connection through its lifetime — created at `connect_server` time,
/// travelling through checkout and checkin.
///
/// Construct only via [`PreparedStatements::new`] so the LRU cap is
/// always explicit; there is no `Default` (which would silently mean an
/// unbounded cache). Pass `max = 0` to opt into unlimited caching.
pub struct PreparedStatements {
    /// Canonical statements prepared on this backend, LRU-bounded.
    cache: Lru,
    /// One entry per `Parse` sent to the backend, in send order: the
    /// canonical name (to roll back the optimistic insert on rejection)
    /// and whether the matching `ParseComplete` is forwarded to the
    /// client or suppressed.
    parse_replies: VecDeque<ParseReply>,
    /// One entry per `Close` sent to the backend, in send order: whether
    /// the matching `CloseComplete` is forwarded (a client portal close)
    /// or suppressed (a halephant eviction close).
    close_replies: VecDeque<Reply>,
}

/// A `Parse` awaiting its backend reply.
struct ParseReply {
    reply: Reply,
    canon: String,
}

/// What to do with a backend reply that answers a tracked `Parse`/`Close`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Reply {
    /// Deliver the reply to the client.
    Forward,
    /// Swallow the reply; the client never sent the message it answers.
    Suppress,
}

/// Outcome of a client `Parse`, returned by [`PreparedStatements::note_client_parse`].
#[derive(Debug)]
pub enum ClientParse {
    /// Already prepared on this backend — answer the client with a
    /// synthetic `ParseComplete` and do not forward the `Parse`.
    Synthesize,
    /// Forward the (rewritten) `Parse` to the backend. Send a statement
    /// `Close` for `eviction` first when `Some`.
    Forward { eviction: Option<String> },
}

/// Outcome of an internal re-prepare, returned by [`PreparedStatements::note_reprepare`].
#[derive(Debug)]
pub enum Reprepare {
    /// Already prepared on this backend — nothing to do.
    AlreadyPrepared,
    /// Inject a `Parse`. Send a statement `Close` for `eviction` first
    /// when `Some`.
    Prepare { eviction: Option<String> },
}

impl PreparedStatements {
    pub fn new(max: u32) -> Self {
        Self {
            cache: Lru::new(max),
            parse_replies: VecDeque::new(),
            close_replies: VecDeque::new(),
        }
    }

    /// Handle a client `Parse` for `canon`. On a cache hit the statement
    /// is already on the backend, so the caller answers synthetically; on
    /// a miss the caller forwards the rewritten `Parse` and the backend's
    /// real `ParseComplete` flows back to the client in order.
    pub fn note_client_parse(&mut self, canon: &str) -> ClientParse {
        if self.cache.contains(canon) {
            self.cache.touch(canon);
            ClientParse::Synthesize
        } else {
            let eviction = self.record_prepare(canon, Reply::Forward);
            ClientParse::Forward { eviction }
        }
    }

    /// Handle an internal re-prepare triggered by a `Bind`/`Describe`
    /// whose statement the client never (re-)`Parse`d. The injected
    /// `ParseComplete` is suppressed.
    pub fn note_reprepare(&mut self, canon: &str) -> Reprepare {
        if self.cache.contains(canon) {
            self.cache.touch(canon);
            Reprepare::AlreadyPrepared
        } else {
            let eviction = self.record_prepare(canon, Reply::Suppress);
            Reprepare::Prepare { eviction }
        }
    }

    /// Insert `canon` optimistically and queue the disposition of its
    /// `ParseComplete`. When the cache is full, queue the eviction
    /// `Close`'s `CloseComplete` for suppression and return the evicted
    /// name. Caller must have confirmed `canon` is not already cached.
    fn record_prepare(&mut self, canon: &str, reply: Reply) -> Option<String> {
        let eviction = self.cache.insert(canon.to_owned());
        if eviction.is_some() {
            self.close_replies.push_back(Reply::Suppress);
        }
        self.parse_replies.push_back(ParseReply {
            reply,
            canon: canon.to_owned(),
        });
        eviction
    }

    /// Record that a client portal `Close` is being forwarded, so its
    /// `CloseComplete` is delivered to the client.
    pub fn note_portal_close(&mut self) {
        self.close_replies.push_back(Reply::Forward);
    }

    /// Disposition of the next `ParseComplete` from the backend. Defaults
    /// to forwarding when nothing is tracked, so an unexpected reply
    /// surfaces to the client rather than being silently dropped.
    pub fn next_parse_reply(&mut self) -> Reply {
        self.parse_replies
            .pop_front()
            .map_or(Reply::Forward, |p| p.reply)
    }

    /// Disposition of the next `CloseComplete` from the backend.
    pub fn next_close_reply(&mut self) -> Reply {
        self.close_replies.pop_front().unwrap_or(Reply::Forward)
    }

    /// Reconcile after a backend `ErrorResponse` aborts the current
    /// extended-protocol batch: every message after the error is skipped,
    /// so the still-pending replies never arrive. Roll back the optimistic
    /// inserts of the unconfirmed `Parse`s and drop the orphaned `Close`
    /// bookkeeping.
    pub fn roll_back_after_error(&mut self) {
        self.close_replies.clear();
        while let Some(pending) = self.parse_replies.pop_front() {
            self.cache.remove(&pending.canon);
        }
    }

    /// `DISCARD ALL` deallocated every prepared statement on the backend.
    pub fn discard_all(&mut self) {
        self.cache.clear();
    }

    /// Drop pending reply bookkeeping at a transaction or session
    /// boundary. A client that disconnected mid-batch can leave a
    /// disposition whose reply the backend skipped and will never send;
    /// the cache itself persists across the boundary.
    pub fn reset_pending(&mut self) {
        self.parse_replies.clear();
        self.close_replies.clear();
    }
}

// ---------------------------------------------------------------------------
// Lru — canonical prepared-statement cache with LRU eviction
// ---------------------------------------------------------------------------

/// Tracks which canonical prepared statements exist on a server
/// connection, with LRU eviction when the configured limit is reached.
/// A private component of [`PreparedStatements`]; constructed only via
/// `Lru::new` so the cap is always explicit.
struct Lru {
    /// Set of canonical names currently prepared on this server.
    prepared: HashSet<String>,
    /// LRU order: front = oldest, back = most recently used.
    order: VecDeque<String>,
    /// Maximum statements to keep prepared per server connection.
    max: u32,
}

impl Lru {
    fn new(max: u32) -> Self {
        Self {
            prepared: HashSet::new(),
            order: VecDeque::new(),
            max,
        }
    }

    /// Check whether the canonical statement is already prepared on this server.
    fn contains(&self, canon: &str) -> bool {
        self.prepared.contains(canon)
    }

    /// Mark a canonical statement as prepared on this server. Returns the name
    /// of an evicted statement if the cache is full, which the caller must send
    /// a `Close` for before preparing the new one.
    fn insert(&mut self, canon: String) -> Option<String> {
        if self.prepared.contains(&canon) {
            // Move to back of LRU.
            self.order.retain(|n| n != &canon);
            self.order.push_back(canon);
            return None;
        }

        let evicted = if self.max > 0 && self.prepared.len() as u32 >= self.max {
            self.evict()
        } else {
            None
        };

        self.prepared.insert(canon.clone());
        self.order.push_back(canon);
        evicted
    }

    /// Touch a statement in the LRU (it was used).
    fn touch(&mut self, canon: &str) {
        self.order.retain(|n| n != canon);
        self.order.push_back(canon.to_owned());
    }

    /// Evict the least recently used statement. Returns the canonical name of
    /// the evicted statement.
    fn evict(&mut self) -> Option<String> {
        let name = self.order.pop_front()?;
        self.prepared.remove(&name);
        Some(name)
    }

    /// Clear all tracked statements (called after DISCARD ALL).
    fn clear(&mut self) {
        self.prepared.clear();
        self.order.clear();
    }

    /// Remove a single canonical statement from the cache. Used to roll
    /// back an optimistic insert when the backend rejects the `Parse`.
    fn remove(&mut self, canon: &str) {
        self.prepared.remove(canon);
        self.order.retain(|n| n != canon);
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
            statements: PreparedStatements::new(max_prepared),
            dirty_vars: HashSet::new(),
            last_tx_status: crate::proto::types::TransactionStatus::Idle,
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Lru cache --

    #[test]
    fn lru_insert_and_contains() {
        let mut lru = Lru::new(10);
        assert!(!lru.contains("stmt_a"));
        lru.insert("stmt_a".into());
        assert!(lru.contains("stmt_a"));
    }

    #[test]
    fn lru_eviction() {
        let mut lru = Lru::new(2);
        lru.insert("a".into());
        lru.insert("b".into());

        // Cache is full, inserting c should evict a (oldest).
        let evicted = lru.insert("c".into());
        assert_eq!(evicted, Some("a".into()));
        assert!(!lru.contains("a"));
        assert!(lru.contains("b"));
        assert!(lru.contains("c"));
    }

    #[test]
    fn lru_touch_prevents_eviction() {
        let mut lru = Lru::new(2);
        lru.insert("a".into());
        lru.insert("b".into());

        // Touch a — now b is the oldest.
        lru.touch("a");

        let evicted = lru.insert("c".into());
        assert_eq!(evicted, Some("b".into()));
        assert!(lru.contains("a"));
        assert!(lru.contains("c"));
    }

    #[test]
    fn lru_insert_existing_moves_to_back() {
        let mut lru = Lru::new(2);
        lru.insert("a".into());
        lru.insert("b".into());

        // Re-insert a — should not evict, just move to back.
        let evicted = lru.insert("a".into());
        assert!(evicted.is_none());

        // Now b is oldest — inserting c evicts b.
        let evicted = lru.insert("c".into());
        assert_eq!(evicted, Some("b".into()));
    }

    #[test]
    fn lru_clear() {
        let mut lru = Lru::new(10);
        lru.insert("a".into());
        lru.insert("b".into());
        lru.clear();
        assert!(!lru.contains("a"));
        assert!(!lru.contains("b"));
    }

    #[test]
    fn lru_remove_frees_the_slot() {
        let mut lru = Lru::new(2);
        lru.insert("a".into());
        lru.insert("b".into());
        lru.remove("a");
        assert!(!lru.contains("a"));
        assert!(lru.contains("b"));
        // The slot "a" freed leaves room for "c" without evicting "b".
        assert!(lru.insert("c".into()).is_none());
        assert!(lru.contains("b"));
        assert!(lru.contains("c"));
    }

    // -- PreparedStatements (cache + reply bookkeeping together) --

    #[test]
    fn parse_replies_are_fifo() {
        let mut ps = PreparedStatements::new(0);
        // A forwarded client Parse, then an injected re-prepare (distinct
        // canonicals so the second is a cache miss).
        assert!(matches!(
            ps.note_client_parse("a"),
            ClientParse::Forward { .. }
        ));
        assert!(matches!(ps.note_reprepare("b"), Reprepare::Prepare { .. }));
        assert_eq!(ps.next_parse_reply(), Reply::Forward);
        assert_eq!(ps.next_parse_reply(), Reply::Suppress);
        // Nothing left — default to forwarding so a stray reply surfaces.
        assert_eq!(ps.next_parse_reply(), Reply::Forward);
    }

    #[test]
    fn cache_hit_synthesizes_without_recording() {
        let mut ps = PreparedStatements::new(0);
        ps.note_client_parse("a");
        ps.next_parse_reply(); // confirm "a"
        // Re-parsing "a" is a cache hit — answered synthetically, no reply queued.
        assert!(matches!(ps.note_client_parse("a"), ClientParse::Synthesize));
        assert!(matches!(ps.note_reprepare("a"), Reprepare::AlreadyPrepared));
        assert_eq!(ps.next_parse_reply(), Reply::Forward); // queue empty
    }

    #[test]
    fn eviction_returns_victim_and_suppresses_its_close() {
        let mut ps = PreparedStatements::new(1);
        assert!(matches!(
            ps.note_client_parse("a"),
            ClientParse::Forward { eviction: None }
        ));
        ps.next_parse_reply();
        // Preparing "b" evicts "a".
        assert!(matches!(
            ps.note_client_parse("b"),
            ClientParse::Forward { eviction: Some(v) } if v == "a"
        ));
        // The eviction Close's CloseComplete is suppressed.
        assert_eq!(ps.next_close_reply(), Reply::Suppress);
    }

    #[test]
    fn close_replies_disambiguate_portal_and_eviction() {
        let mut ps = PreparedStatements::new(1);
        ps.note_client_parse("a");
        ps.next_parse_reply();
        // A client portal Close (forward), then a Parse that evicts "a"
        // (suppress) — both are CloseComplete on the wire, so order is all
        // that tells them apart.
        ps.note_portal_close();
        ps.note_client_parse("b");
        assert_eq!(ps.next_close_reply(), Reply::Forward);
        assert_eq!(ps.next_close_reply(), Reply::Suppress);
    }

    #[test]
    fn error_rolls_back_unconfirmed_parses_only() {
        let mut ps = PreparedStatements::new(0);
        // "a" prepared and confirmed by its ParseComplete.
        ps.note_client_parse("a");
        assert_eq!(ps.next_parse_reply(), Reply::Forward);
        // "b" optimistically inserted, not yet confirmed.
        ps.note_reprepare("b");
        // The backend errors: "b" rolls back, "a" stays.
        ps.roll_back_after_error();
        assert!(
            matches!(ps.note_client_parse("a"), ClientParse::Synthesize),
            "confirmed statement should survive the rollback"
        );
        assert!(
            matches!(ps.note_client_parse("b"), ClientParse::Forward { .. }),
            "unconfirmed statement should be rolled back"
        );
    }

    #[test]
    fn discard_all_clears_the_cache() {
        let mut ps = PreparedStatements::new(0);
        ps.note_client_parse("a");
        ps.next_parse_reply();
        ps.discard_all();
        assert!(
            matches!(ps.note_client_parse("a"), ClientParse::Forward { .. }),
            "DISCARD ALL should clear the cache"
        );
    }

    #[test]
    fn reset_pending_clears_dispositions_not_cache() {
        let mut ps = PreparedStatements::new(1);
        ps.note_reprepare("a"); // parse reply: Suppress
        ps.note_reprepare("b"); // evicts "a": close reply Suppress; parse reply Suppress
        ps.reset_pending();
        // Pending dispositions cleared — both default back to Forward.
        assert_eq!(ps.next_parse_reply(), Reply::Forward);
        assert_eq!(ps.next_close_reply(), Reply::Forward);
        // But the cache itself persists across the boundary.
        assert!(matches!(ps.note_reprepare("b"), Reprepare::AlreadyPrepared));
    }
}
