//! Client-facing protocol handlers. The pool checks out server
//! connections; this module decides what to do with them — classify
//! queries, intercept session-affecting commands, forward messages,
//! and translate between client-visible prepared statement names and
//! the pool-wide canonical ones.
pub mod frontend;
pub mod intercept;
pub mod prepared;
pub mod session;
pub mod transaction;
