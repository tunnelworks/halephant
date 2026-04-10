//! Client-facing message constructors + senders.
//!
//! Everything halephant pushes at a downstream client that wasn't
//! triggered by a corresponding upstream response: post-auth
//! startup frames, synthetic intercept responses, and error frames.
//!
//! Split by shape rather than by caller:
//!
//! - [`startup`] — `ParameterStatus` + `BackendKeyData` +
//!   `ReadyForQuery(Idle)` after auth completes.
//! - [`synthetic`] — `CommandComplete(tag)` + `ReadyForQuery(status)`
//!   for intercepted LISTEN/UNLISTEN/DEALLOCATE/Close statements.
//! - [`error`] — FATAL and non-fatal `ErrorResponse` builders,
//!   used from [`crate::auth`] and [`crate::proxy::transaction`].

pub mod error;
pub mod startup;
pub mod synthetic;

pub use startup::send_post_auth_startup;
