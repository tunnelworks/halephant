//! Upstream connection establishment + per-connection primitives.
//!
//! Everything halephant needs to open a TCP socket to an upstream
//! PostgreSQL node, drive the startup handshake, and produce a
//! fully authenticated connection handle. Consumed by the pool
//! (checkout, refill, reset), topology probing, LISTEN/NOTIFY
//! multiplex, and the admin auth-query path.

pub mod options;
pub mod reset;
pub mod server;
pub mod sock;
