pub mod pgpass;
pub mod query;
pub mod scram;

use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, trace, warn};

use crate::config::Config;
use crate::errors;
use crate::messages;
use crate::proto::codec::FrontendCodec;

use query::VerifierCache;

// ---------------------------------------------------------------------------
// Admin upstream connection parameters
// ---------------------------------------------------------------------------

/// Resolved parameters for the upstream admin connection halephant uses
/// to run `auth_query`. Built by [`resolve_admin_conn`] from a config
/// snapshot plus the topology-resolved primary address and pgpass
/// lookup, then passed down to [`VerifierCache::get`] as a single unit
/// instead of six scalar arguments.
pub(crate) struct AdminConn<'a> {
    pub(crate) addr: &'a str,
    pub(crate) admin_user: &'a str,
    pub(crate) admin_database: &'a str,
    pub(crate) password: Option<&'a str>,
    pub(crate) auth_query: &'a str,
    pub(crate) cache_ttl: std::time::Duration,
    pub(crate) connect_timeout: std::time::Duration,
}

/// Resolve the admin connection details for the cluster serving `database`.
/// The `addr` and `password` are supplied by the caller (topology + pgpass
/// lookups); every other field comes from the cluster config.
fn resolve_admin_conn<'a>(
    config: &'a Config,
    database: &str,
    addr: &'a str,
    password: Option<&'a str>,
) -> Result<AdminConn<'a>, errors::AuthError> {
    let (_, cluster, _) = config.find_pool(database).ok_or_else(|| {
        errors::AuthError::VerifierFetch(format!("no pool configured for {database:?}"))
    })?;

    Ok(AdminConn {
        addr,
        admin_user: &cluster.admin_user,
        admin_database: &cluster.admin_database,
        password,
        auth_query: &cluster.auth.query,
        cache_ttl: cluster.auth.cache_ttl,
        connect_timeout: cluster.connect_timeout,
    })
}

// ---------------------------------------------------------------------------
// Authenticator — caches verifiers and drives the auth exchange
// ---------------------------------------------------------------------------

/// Handles client authentication using pool-based user access control and
/// SCRAM-SHA-256.
#[derive(Default)]
pub struct Authenticator {
    cache: VerifierCache,
}

impl Authenticator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Authenticate a client connection. On success, the client has received
    /// `AuthenticationOk`. On failure, the client has received an
    /// `ErrorResponse` and the connection should be closed.
    ///
    /// Responsibilities at this layer:
    /// - Pool-user policy check (is this `(database, user)` pair allowed?)
    /// - Verifier resolution: alias the user to its upstream name and
    ///   fetch the SCRAM verifier from the cache (or via auth query)
    /// - Error classification into PostgreSQL SQLSTATE codes
    /// - Tracing span and error reporting
    ///
    /// The pure SCRAM wire exchange is delegated to
    /// [`scram::server::authenticate`].
    #[tracing::instrument(name = "auth", skip_all, err(Display), fields(
        db.namespace = %database,
        user = %user,
        otel.status_code,
        otel.status_description,
    ))]
    pub async fn authenticate(
        &self,
        client: &mut Framed<TcpStream, FrontendCodec>,
        database: &str,
        user: &str,
        config: &Config,
        admin_addr: &str,
        admin_password: Option<&str>,
    ) -> Result<(), errors::AuthError> {
        async {
            // Policy: reject unknown `(database, user)` pairs before
            // performing any upstream work.
            if !config.is_user_allowed(database, user) {
                debug!(%database, %user, "rejected: not in pool users list");
                messages::error::send_fatal(client, "28000", "authentication failed").await;
                return Err(errors::AuthError::Rejected {
                    database: database.into(),
                    user: user.into(),
                });
            }

            // Resolve the upstream identity and fetch the verifier.
            // `find_user` may map the client-facing name to a
            // different upstream role when `alias` is configured.
            let admin_conn = resolve_admin_conn(config, database, admin_addr, admin_password)?;
            let upstream_user = config
                .find_user(database, user)
                .map_or(user, |u| u.upstream_name(user));

            let verifier = match tokio::time::timeout(
                admin_conn.connect_timeout,
                self.cache.get(upstream_user, &admin_conn),
            )
            .await
            {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    messages::error::send_fatal(client, "28000", "authentication failed").await;
                    return Err(e);
                }
                Err(_) => {
                    messages::error::send_fatal(client, "28000", "authentication failed").await;
                    return Err(errors::AuthError::VerifierFetch(
                        "verifier fetch timed out".into(),
                    ));
                }
            };

            // Delegate the pure wire exchange. The driver has no
            // knowledge of `Config` or halephant-level policy.
            if let Err(e) = scram::server::authenticate(client, verifier).await {
                if matches!(e, errors::AuthError::InvalidCredentials) {
                    warn!(%user, %database, "SCRAM authentication failed");
                }
                let code = match &e {
                    errors::AuthError::InvalidCredentials => "28P01",
                    _ => "28000",
                };
                messages::error::send_fatal(client, code, "authentication failed").await;
                return Err(e);
            }

            trace!(%user, %database, "SCRAM authentication succeeded");
            Ok(())
        }
        .await
        .inspect(|()| {
            tracing::Span::current().record("otel.status_code", "OK");
        })
        .inspect_err(|e| {
            let span = tracing::Span::current();
            span.record("otel.status_code", "ERROR");
            span.record("otel.status_description", e.to_string().as_str());
        })
    }
}
