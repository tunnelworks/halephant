//! SCRAM-SHA-256 authentication (RFC 5802), split by role.
//!
//! The module is organised so that the client-facing and
//! server-facing sides of the exchange live in separate files and
//! callers must pick one explicitly:
//!
//! - [`client::authenticate`] — halephant authenticates *to* an
//!   upstream PostgreSQL server (pool connect, auth-query probe,
//!   topology probe). Takes a `Framed<TcpStream, BackendCodec>`
//!   plus a plaintext password.
//! - [`server::authenticate`] — halephant authenticates a downstream
//!   client *connecting to halephant*. Takes a
//!   `Framed<TcpStream, FrontendCodec>` plus a pre-fetched
//!   [`ScramVerifier`]. Halephant-level concerns (policy check,
//!   verifier cache, tracing span) stay in
//!   [`crate::auth::Authenticator`] and wrap this call.
//!
//! Shared pieces:
//!
//! - `verifier` — the [`ScramVerifier`] data type (re-exported at
//!   this module's top level).
//! - `crypto` — HMAC-SHA256, SHA256, and the shared SCRAM message
//!   attribute parser.
//!
//! `ScramServer` and `ScramClient` are intentionally NOT re-exported
//! at this level: callers import them through their respective
//! sub-modules (`scram::server::ScramServer`,
//! `scram::client::ScramClient`) so the role is visible at the
//! import site.

pub mod client;
pub mod crypto;
pub mod server;
mod verifier;

pub use verifier::ScramVerifier;
