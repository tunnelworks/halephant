use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use moka::Expiry;
use moka::future::Cache;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::debug;

use crate::auth::scram;
use crate::connections::server::connect_tcp;
use crate::errors;
use crate::proto;

// ---------------------------------------------------------------------------
// Verifier cache
// ---------------------------------------------------------------------------

type CacheKey = (String, String);

/// Upper bound on cached verifier entries. Each entry is ~100 bytes of
/// verifier material plus the key strings, so 10k entries is roughly
/// 1–2 MB — comfortably more than any realistic halephant deployment
/// needs (bounded by `users × clusters`), while small enough that
/// hitting the cap indicates configuration drift rather than normal
/// operation. Moka evicts entries via its Window TinyLFU policy when
/// the cap is reached.
const MAX_CACHED_VERIFIERS: u64 = 10_000;

/// Caches SCRAM verifiers fetched via `auth_query`, keyed by
/// (upstream_addr, username).
///
/// Backed by [`moka::future::Cache`], which provides:
///
/// - **Bounded capacity** via LRU-ish W-TinyLFU eviction, so entries
///   for decommissioned users or upstreams don't linger forever.
/// - **Per-entry TTL** via a custom [`Expiry`] impl, so each cluster's
///   configured `auth.cache_ttl` is honoured independently — a setup
///   with one short-lived and one long-lived cluster gets the exact
///   behaviour each configured.
/// - **Concurrent miss coalescing** via `try_get_with`: multiple
///   concurrent callers for the same key share a single upstream
///   fetch, avoiding a thundering herd of auth queries when the first
///   authentication request arrives for a fresh entry.
pub struct VerifierCache {
    entries: Cache<CacheKey, CachedEntry>,
}

/// Value stored in the cache. The TTL travels alongside the verifier
/// so the [`Expiry`] impl can read it at insert time — this is how
/// moka supports per-entry expiry while still using a single `Cache`
/// instance shared across every cluster.
#[derive(Clone)]
struct CachedEntry {
    verifier: scram::ScramVerifier,
    ttl: Duration,
}

/// Reads the per-entry TTL from the stored value. Only
/// `expire_after_create` is overridden — `expire_after_read` and
/// `expire_after_update` default to `None`, meaning "keep the
/// previously-computed expiration." Reads do not extend the lifetime.
struct CachedEntryExpiry;

impl Expiry<CacheKey, CachedEntry> for CachedEntryExpiry {
    fn expire_after_create(
        &self,
        _key: &CacheKey,
        value: &CachedEntry,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.ttl)
    }
}

impl Default for VerifierCache {
    fn default() -> Self {
        Self::new()
    }
}

impl VerifierCache {
    pub fn new() -> Self {
        let expiry: Arc<dyn Expiry<CacheKey, CachedEntry> + Send + Sync + 'static> =
            Arc::new(CachedEntryExpiry);
        Self {
            entries: Cache::builder()
                .max_capacity(MAX_CACHED_VERIFIERS)
                .expire_after(expiry)
                .build(),
        }
    }

    /// Look up a verifier from cache, or fetch it via `auth_query` if
    /// missing or expired. Concurrent callers for the same key share
    /// a single upstream fetch via `moka::future::Cache::try_get_with`.
    pub(crate) async fn get(
        &self,
        user: &str,
        admin: &crate::auth::AdminConn<'_>,
    ) -> Result<scram::ScramVerifier, errors::AuthError> {
        let cache_key: CacheKey = (admin.addr.to_owned(), user.to_owned());

        // Every concurrent caller for `cache_key` that arrives while
        // the future below is still pending shares this single fetch.
        // The owned clones exist because the future must be 'static —
        // moka stores it internally and polls it from a background
        // task. `try_get_with` returns `Arc<E>` on error so the error
        // can be shared among the coalesced waiters; we stringify
        // through `VerifierFetch` rather than cloning because
        // `AuthError` is not `Clone` (it carries `std::io::Error`).
        let admin_user = admin.admin_user.to_owned();
        let admin_database = admin.admin_database.to_owned();
        let admin_pw = admin.password.map(str::to_owned);
        let auth_query = admin.auth_query.to_owned();
        let upstream_addr_owned = admin.addr.to_owned();
        let user_owned = user.to_owned();
        let cache_ttl = admin.cache_ttl;
        let connect_timeout = admin.connect_timeout;

        let entry = self
            .entries
            .try_get_with(cache_key, async move {
                debug!(
                    user = %user_owned,
                    upstream = %upstream_addr_owned,
                    admin_database = %admin_database,
                    "fetching verifier via auth_query",
                );
                let verifier = fetch_verifier(
                    &upstream_addr_owned,
                    &admin_user,
                    &admin_database,
                    admin_pw.as_deref(),
                    &auth_query,
                    &user_owned,
                    connect_timeout,
                )
                .await?;
                Ok::<_, errors::AuthError>(CachedEntry {
                    verifier,
                    ttl: cache_ttl,
                })
            })
            .await
            .map_err(|e: Arc<errors::AuthError>| errors::AuthError::VerifierFetch(e.to_string()))?;

        Ok(entry.verifier)
    }
}

// ---------------------------------------------------------------------------
// auth_query execution
// ---------------------------------------------------------------------------

/// Connect to `upstream_addr` as `admin_user`, authenticate (via SCRAM if
/// required), run the `auth_query` for `target_user`, and parse the returned
/// SCRAM verifier. The startup message selects `admin_database` — `"postgres"`
/// by default, but operators can override when `auth_query` references
/// database-local objects.
///
/// Routes through [`connect_tcp`] so the verifier fetch shares the
/// same connect budget, `TCP_NODELAY`, and keepalive settings as
/// every other upstream connection path.
#[allow(clippy::too_many_arguments)]
async fn fetch_verifier(
    upstream_addr: &str,
    admin_user: &str,
    admin_database: &str,
    admin_password: Option<&str>,
    auth_query_template: &str,
    target_user: &str,
    connect_timeout: Duration,
) -> Result<scram::ScramVerifier, errors::AuthError> {
    let stream = connect_tcp(upstream_addr, connect_timeout)
        .await
        .map_err(|e| {
            errors::AuthError::VerifierFetch(format!("connect to {upstream_addr}: {e}"))
        })?;
    let mut conn = Framed::new(stream, proto::codec::BackendCodec::new());

    let result = run_auth_query(
        &mut conn,
        admin_user,
        admin_database,
        admin_password,
        auth_query_template,
        target_user,
    )
    .await;

    // Always send Terminate to cleanly close the admin connection,
    // regardless of whether the query succeeded or failed.
    let _ = conn.send(proto::frontend::FrontendMessage::Terminate).await;

    result
}

/// Run the startup handshake and auth query on an established connection.
async fn run_auth_query(
    conn: &mut Framed<TcpStream, proto::codec::BackendCodec>,
    admin_user: &str,
    admin_database: &str,
    admin_password: Option<&str>,
    auth_query_template: &str,
    target_user: &str,
) -> Result<scram::ScramVerifier, errors::AuthError> {
    conn.send(proto::frontend::FrontendMessage::Startup(
        proto::frontend::Startup {
            version: proto::frontend::ProtocolVersion { major: 3, minor: 0 },
            parameters: vec![
                ("user".into(), admin_user.into()),
                ("database".into(), admin_database.into()),
            ],
        },
    ))
    .await
    .map_err(|e| errors::AuthError::VerifierFetch(format!("send startup: {e}")))?;

    // Consume startup responses until ReadyForQuery.
    loop {
        match conn
            .next()
            .await
            .transpose()
            .map_err(|e| errors::AuthError::VerifierFetch(format!("admin startup: {e}")))?
        {
            Some(
                proto::backend::BackendMessage::AuthenticationOk
                | proto::backend::BackendMessage::ParameterStatus { .. }
                | proto::backend::BackendMessage::BackendKeyData { .. },
            ) => {}
            Some(proto::backend::BackendMessage::AuthenticationSasl { mechanisms }) => {
                if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                    return Err(errors::AuthError::VerifierFetch(format!(
                        "admin connection requires unsupported SASL mechanism: {mechanisms:?}"
                    )));
                }
                let pw = admin_password.ok_or_else(|| {
                    errors::AuthError::VerifierFetch(
                        "admin connection requires SCRAM auth but no password in .pgpass".into(),
                    )
                })?;
                scram::client::authenticate(conn, pw).await?;
            }
            Some(proto::backend::BackendMessage::AuthenticationCleartextPassword) => {
                let pw = admin_password.ok_or_else(|| {
                    errors::AuthError::VerifierFetch(
                        "admin connection requires password but none in .pgpass".into(),
                    )
                })?;
                conn.send(proto::frontend::FrontendMessage::PasswordMessage(
                    pw.as_bytes().to_vec(),
                ))
                .await
                .map_err(|e| errors::AuthError::VerifierFetch(format!("send password: {e}")))?;
            }
            Some(proto::backend::BackendMessage::ReadyForQuery(_)) => break,
            Some(proto::backend::BackendMessage::ErrorResponse(err)) => {
                return Err(errors::AuthError::VerifierFetch(format!(
                    "admin startup error: {}",
                    err.message().unwrap_or("unknown")
                )));
            }
            Some(other) => {
                return Err(errors::AuthError::VerifierFetch(format!(
                    "unexpected during admin startup: {other:?}"
                )));
            }
            None => {
                return Err(errors::AuthError::VerifierFetch(
                    "upstream closed during admin startup".into(),
                ));
            }
        }
    }

    // Execute the auth_query using the extended query protocol so the username
    // is sent as a typed parameter, avoiding any string-escaping issues.
    conn.feed(proto::frontend::FrontendMessage::Parse(
        proto::frontend::Parse {
            name: String::new(),
            query: auth_query_template.to_owned(),
            param_types: vec![],
        },
    ))
    .await
    .map_err(|e| errors::AuthError::VerifierFetch(format!("send parse: {e}")))?;
    conn.feed(proto::frontend::FrontendMessage::Bind(
        proto::frontend::Bind {
            portal: String::new(),
            statement: String::new(),
            param_formats: vec![],
            params: vec![Some(target_user.as_bytes().to_vec())],
            result_formats: vec![],
        },
    ))
    .await
    .map_err(|e| errors::AuthError::VerifierFetch(format!("send bind: {e}")))?;
    conn.feed(proto::frontend::FrontendMessage::Execute(
        proto::frontend::Execute {
            portal: String::new(),
            max_rows: 0,
        },
    ))
    .await
    .map_err(|e| errors::AuthError::VerifierFetch(format!("send execute: {e}")))?;
    conn.send(proto::frontend::FrontendMessage::Sync)
        .await
        .map_err(|e| errors::AuthError::VerifierFetch(format!("send sync: {e}")))?;

    // Read the result.
    let mut verifier_str: Option<String> = None;

    loop {
        match conn
            .next()
            .await
            .transpose()
            .map_err(|e| errors::AuthError::VerifierFetch(format!("auth_query: {e}")))?
        {
            Some(proto::backend::BackendMessage::DataRow(cols)) => {
                if cols.len() >= 2
                    && let Some(ref data) = cols[1]
                {
                    verifier_str = Some(String::from_utf8_lossy(data).into_owned());
                }
            }
            Some(proto::backend::BackendMessage::ReadyForQuery(_)) => break,
            Some(proto::backend::BackendMessage::ErrorResponse(err)) => {
                return Err(errors::AuthError::VerifierFetch(format!(
                    "auth_query error: {}",
                    err.message().unwrap_or("unknown")
                )));
            }
            None => {
                return Err(errors::AuthError::VerifierFetch(
                    "upstream closed during auth_query".into(),
                ));
            }
            // ParseComplete, BindComplete, RowDescription, CommandComplete, notices, etc.
            Some(_) => {}
        }
    }

    let raw = verifier_str.ok_or_else(|| {
        errors::AuthError::VerifierFetch(format!("no password found for user {target_user:?}"))
    })?;

    scram::parse_verifier(&raw)
}
