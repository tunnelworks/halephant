//! Client registry — tracks every accepted client connection from accept
//! through cleanup, independent of the server-side connection pool.
//!
//! `ClientRegistry` is the source of truth for "who is currently connected
//! to halephant and what are they doing". It is consumed by:
//!
//! - the admin HTTP API (`/admin/clients`) for operator introspection,
//! - the `halephant.client.connections` observable gauge for dashboards,
//! - (in a future phase) the checkout wait-queue integration, so the
//!   per-client state can flip to [`ClientState::Waiting`] while a client
//!   is blocked on pool capacity.
//!
//! Registration is RAII: the proxy task calls [`ClientRegistry::register`]
//! once, holds the returned [`ClientGuard`] for its entire lifetime, and
//! the entry is removed when the guard drops. State transitions flow
//! through `&ClientGuard` so the guard can be passed by reference down
//! the call stack without ownership gymnastics.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Stable identifier for a client connection within a single halephant
/// process. Monotonically increasing; wraps after 2^64 connections.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClientId(u64);

impl ClientId {
    /// The underlying numeric value. Used for trace span attributes and
    /// admin JSON output.
    #[must_use]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// High-level phase a client is in at a point in time. Used for admin
/// display and for counting clients by state in the metrics gauge.
///
/// `Waiting` and `InTransaction` are defined now but only transitioned to
/// in later phases — Phase 1 only exercises the `Negotiating → Authenticating
/// → Idle` path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientState {
    /// Pre-startup: reading the initial message, handling SSL or cancel
    /// requests.
    Negotiating,
    /// SCRAM exchange is in progress.
    Authenticating,
    /// Connected and ready; either between transactions (transaction mode)
    /// or holding an open session (session mode).
    Idle,
    /// Actively forwarding inside a transaction. Reserved for Phase 2.
    InTransaction,
    /// Blocked on pool checkout waiting for a server connection to become
    /// available. Reserved for Phase 2.
    Waiting,
}

impl ClientState {
    /// Short lowercase identifier used as a metric attribute value and in
    /// admin JSON output.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ClientState::Negotiating => "negotiating",
            ClientState::Authenticating => "authenticating",
            ClientState::Idle => "idle",
            ClientState::InTransaction => "in_transaction",
            ClientState::Waiting => "waiting",
        }
    }
}

/// Routing intent the client is waiting for while in
/// [`ClientState::Waiting`]. Surfaced via `/admin/clients` so operators
/// can see whether a client is blocked on the primary queue or a
/// replica queue for diagnosis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitRole {
    Primary,
    Replica,
}

impl WaitRole {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            WaitRole::Primary => "primary",
            WaitRole::Replica => "replica",
        }
    }
}

/// The `(database, user, role)` triple a client is blocked on. Only
/// populated while the client is in [`ClientState::Waiting`]; cleared
/// on wakeup (whether the wake succeeds or times out).
#[derive(Clone, Debug)]
pub struct WaitTarget {
    pub database: String,
    pub user: String,
    pub role: WaitRole,
}

/// Snapshot of one client's state at a point in time. Returned by
/// [`ClientRegistry::snapshot`]; consumers serialize it themselves
/// (admin JSON, metrics attributes) rather than relying on this crate
/// pulling in serde.
#[derive(Clone, Debug)]
pub struct ClientEntry {
    pub id: ClientId,
    pub remote: SocketAddr,
    pub state: ClientState,
    pub accepted_at: Instant,
    pub state_since: Instant,
    pub database: Option<String>,
    pub user: Option<String>,
    /// Populated only while `state == Waiting` and cleared on wake.
    pub waiting_for: Option<WaitTarget>,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Central tracker for all accepted client connections. Constructed once
/// per halephant process and shared via `Arc`.
pub struct ClientRegistry {
    next_id: AtomicU64,
    entries: Mutex<HashMap<ClientId, ClientEntry>>,
}

impl ClientRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Register a newly accepted client and return the RAII guard that
    /// owns its entry. The initial state is [`ClientState::Negotiating`];
    /// the caller should transition to [`ClientState::Authenticating`]
    /// once the startup message has been parsed.
    pub fn register(self: &Arc<Self>, remote: SocketAddr) -> ClientGuard {
        let id = ClientId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let now = Instant::now();
        let entry = ClientEntry {
            id,
            remote,
            state: ClientState::Negotiating,
            accepted_at: now,
            state_since: now,
            database: None,
            user: None,
            waiting_for: None,
        };
        {
            let mut entries = self.entries.lock();
            entries.insert(id, entry);
        }
        ClientGuard {
            id,
            registry: Arc::clone(self),
        }
    }

    /// Return a clone of every currently-registered client entry. Used
    /// by the admin `/admin/clients` handler. The lock is held only for
    /// the duration of the clone.
    #[must_use]
    pub fn snapshot(&self) -> Vec<ClientEntry> {
        let entries = self.entries.lock();
        entries.values().cloned().collect()
    }

    /// Total number of currently-registered clients.
    #[must_use]
    pub fn count(&self) -> usize {
        self.entries.lock().len()
    }

    /// Count clients by state in a single locked pass.
    ///
    /// Returns a fixed-size array of `(state, count)` pairs covering
    /// every [`ClientState`] variant. Used by the
    /// `halephant.client.connections` observable gauge — reading all
    /// five buckets from the same snapshot keeps the gauge
    /// self-consistent and touches the mutex exactly once per
    /// observation cycle.
    #[must_use]
    pub fn counts_by_state(&self) -> [(ClientState, u64); 5] {
        let entries = self.entries.lock();
        let mut negotiating = 0u64;
        let mut authenticating = 0u64;
        let mut idle = 0u64;
        let mut in_transaction = 0u64;
        let mut waiting = 0u64;
        for entry in entries.values() {
            match entry.state {
                ClientState::Negotiating => negotiating += 1,
                ClientState::Authenticating => authenticating += 1,
                ClientState::Idle => idle += 1,
                ClientState::InTransaction => in_transaction += 1,
                ClientState::Waiting => waiting += 1,
            }
        }
        [
            (ClientState::Negotiating, negotiating),
            (ClientState::Authenticating, authenticating),
            (ClientState::Idle, idle),
            (ClientState::InTransaction, in_transaction),
            (ClientState::Waiting, waiting),
        ]
    }
}

impl Default for ClientRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Guard
// ---------------------------------------------------------------------------

/// RAII handle held by the proxy task for the lifetime of a client
/// connection. Mutating methods take `&self` so the guard can be borrowed
/// down the call stack without propagating `&mut`.
pub struct ClientGuard {
    id: ClientId,
    registry: Arc<ClientRegistry>,
}

impl ClientGuard {
    #[must_use]
    pub fn id(&self) -> ClientId {
        self.id
    }

    /// Update the client's state. Records the transition time so
    /// `state_since` can be reported in admin output.
    pub fn set_state(&self, state: ClientState) {
        let mut entries = self.registry.entries.lock();
        if let Some(entry) = entries.get_mut(&self.id)
            && entry.state != state
        {
            entry.state = state;
            entry.state_since = Instant::now();
        }
    }

    /// Record the database and upstream user for this client, typically
    /// after parsing the PostgreSQL startup message. Both values are
    /// exposed in admin output and can be used by trace span recording.
    pub fn set_database_and_user(&self, database: &str, user: &str) {
        let mut entries = self.registry.entries.lock();
        if let Some(entry) = entries.get_mut(&self.id) {
            entry.database = Some(database.to_owned());
            entry.user = Some(user.to_owned());
        }
    }

    /// Record (or clear) the wait target while this client is blocked
    /// on a pool checkout. Set to `Some` right before transitioning to
    /// [`ClientState::Waiting`] and to `None` on wakeup. The field is
    /// surfaced in `/admin/clients` so operators can see exactly which
    /// role queue a blocked client is sitting on.
    pub fn set_waiting_for(&self, target: Option<WaitTarget>) {
        let mut entries = self.registry.entries.lock();
        if let Some(entry) = entries.get_mut(&self.id) {
            entry.waiting_for = target;
        }
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        let mut entries = self.registry.entries.lock();
        entries.remove(&self.id);
    }
}
