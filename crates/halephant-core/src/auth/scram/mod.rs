//! SCRAM-SHA-256 authentication (RFC 5802), split by role.
//!
//! The pure state machines live in the `tinyscram` crate; this module owns the
//! PostgreSQL wire framing that wraps them:
//!
//! - [`client::authenticate`] — halephant authenticates *to* an upstream
//!   PostgreSQL server (pool connect, auth-query probe, topology probe). Takes
//!   a `Framed<TcpStream, BackendCodec>` plus a plaintext password.
//! - [`server::authenticate`] — halephant authenticates a downstream client
//!   *connecting to halephant*. Takes a `Framed<TcpStream, FrontendCodec>`
//!   plus a pre-fetched [`ScramVerifier`]. Halephant-level concerns (policy
//!   check, verifier cache, tracing span) stay in
//!   [`crate::auth::Authenticator`] and wrap this call.
//!
//! [`ScramVerifier`] is a type alias over `tinyscram::Credential`, so callers
//! that read its `iterations`, `salt`, `stored_key`, `server_key` fields
//! continue to work unchanged. [`parse_verifier`] decodes PostgreSQL's
//! `pg_authid.rolpassword` string format into one of these.

pub mod client;
pub mod server;
mod verifier;

pub use verifier::{ScramVerifier, parse_verifier};
