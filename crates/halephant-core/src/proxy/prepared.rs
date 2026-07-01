//! Per-client prepared statement state and server-side rewriting.
//!
//! When a server connection is reassigned between clients in
//! transaction-mode pooling, prepared statements from the previous
//! client do not exist on the new server. This module solves the
//! problem by:
//!
//! 1. Intercepting `Parse` messages and assigning canonical names
//!    based on a hash of the query text and parameter types.
//! 2. Maintaining a per-client mapping from client-side names to
//!    canonical names.
//! 3. Tracking which canonical statements each server connection has
//!    prepared. (Per-connection state lives on
//!    [`crate::connections::server::PreparedStatements`] so it can travel
//!    with the [`crate::connections::server::ServerConn`] it belongs
//!    to, and decides which backend replies to filter.)
//! 4. Transparently re-preparing statements on cache miss.
//!
//! The global, reference-counted store of Parse messages lives in
//! [`crate::pool::prepared::StatementStore`].

use std::collections::HashMap;
use std::sync::Arc;

use futures_util::SinkExt;
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::connections::server::{ClientParse, Reprepare, ServerConn};
use crate::pool::PoolManager;
use crate::pool::prepared::StatementStore;
use crate::proto;
use crate::proto::frontend::Parse;

// ---------------------------------------------------------------------------
// Canonical name generation
// ---------------------------------------------------------------------------

/// Compute the canonical name for a query. Exposed for integration
/// tests that need to predict canonical names.
#[doc(hidden)]
pub fn canonical_for_test(query: &str, param_types: &[u32]) -> String {
    canonical_name(query, param_types)
}

/// Compute a canonical statement name from the query text and parameter types.
/// Identical queries produce the same canonical name, enabling deduplication
/// across clients.
fn canonical_name(query: &str, param_types: &[u32]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(query.as_bytes());
    for &oid in param_types {
        hasher.update(oid.to_le_bytes());
    }
    let hash = hasher.finalize();
    format!(
        "_hp_{:016x}",
        u64::from_le_bytes(
            hash[..8]
                .try_into()
                .expect("SHA-256 produces at least 8 bytes"),
        )
    )
}

// ---------------------------------------------------------------------------
// Per-client state
// ---------------------------------------------------------------------------

/// Tracks the client-side → canonical name mapping for a single client session.
#[derive(Default)]
pub struct ClientPrepared {
    /// client statement name → canonical name (includes `""` for unnamed)
    names: HashMap<String, String>,
}

impl ClientPrepared {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a client-side name for a Parse, returning the canonical name.
    /// Works for both named and unnamed (`""`) statements — unnamed
    /// entries are tracked the same way so `Bind("")` in a subsequent
    /// transaction resolves to the canonical name and gets re-prepared
    /// on a fresh server connection automatically.
    pub fn register(&mut self, parse: &Parse, store: &mut StatementStore) -> String {
        let canon = canonical_name(&parse.query, &parse.param_types);
        match self.names.insert(parse.name.clone(), canon.clone()) {
            Some(ref old_canon) if old_canon == &canon => {
                // Same canonical — no ref-count change needed.
            }
            Some(old_canon) => {
                store.add_ref(canon.clone(), parse.clone());
                store.release(&old_canon);
            }
            None => {
                store.add_ref(canon.clone(), parse.clone());
            }
        }
        canon
    }

    /// Look up the canonical name for a client-side statement name.
    /// Returns `None` if the name isn't tracked (first use, or the
    /// client never sent a `Parse` for it).
    pub fn resolve(&self, client_name: &str) -> Option<&str> {
        self.names.get(client_name).map(String::as_str)
    }

    /// Remove a client-side name mapping (client sent Close for this statement).
    pub fn remove(&mut self, client_name: &str, store: &mut StatementStore) {
        if let Some(canon) = self.names.remove(client_name) {
            store.release(&canon);
        }
    }

    /// Release all references held by this client. Call when the client
    /// disconnects to allow the global store to reclaim memory.
    pub fn release_all(&mut self, store: &mut StatementStore) {
        for canon in self.names.values() {
            store.release(canon);
        }
        self.names.clear();
    }
}

/// RAII holder for a session's [`ClientPrepared`] that releases every
/// statement-store reference on drop — including when the client task is
/// cancelled mid-`await`, where an explicit cleanup after the forward
/// loop would never run. Shared by both pool modes.
pub(super) struct PreparedGuard {
    pub(super) inner: ClientPrepared,
    pub(super) pools: Arc<PoolManager>,
}

impl Drop for PreparedGuard {
    fn drop(&mut self) {
        let mut store = self.pools.stmt_store.lock();
        self.inner.release_all(&mut store);
    }
}

// ---------------------------------------------------------------------------
// Outbound rewriting + server-side preparation
// ---------------------------------------------------------------------------

/// Rewrite outbound (client → server) messages to use canonical prepared
/// statement names. If the server doesn't have a needed statement, prepare it
/// first (including LRU eviction if necessary).
/// Returns `Some(msg)` to forward to server, or `None` if the message was
/// consumed (synthetic response already sent to client).
pub(super) async fn rewrite_outbound(
    msg: proto::frontend::FrontendMessage,
    client_prepared: &mut ClientPrepared,
    server: &mut ServerConn,
    pools: &Arc<PoolManager>,
    client: &mut Framed<TcpStream, proto::codec::FrontendCodec>,
) -> anyhow::Result<Option<proto::frontend::FrontendMessage>> {
    match msg {
        proto::frontend::FrontendMessage::Parse(parse) => {
            let canon = {
                let mut store = pools.stmt_store.lock();
                client_prepared.register(&parse, &mut store)
            };

            match server.statements.note_client_parse(&canon) {
                ClientParse::Synthesize => {
                    // Already prepared on this backend — either the client
                    // re-Parsed a query it already sent, or another client
                    // prepared the same canonical first. Re-Parsing the
                    // canonical name would draw "prepared statement already
                    // exists", so skip the backend round-trip and answer
                    // synthetically.
                    client
                        .send(proto::backend::BackendMessage::ParseComplete)
                        .await?;
                    Ok(None)
                }
                ClientParse::Forward { eviction } => {
                    // Not on this backend yet. Forward the Parse under its
                    // canonical name so the backend's own ParseComplete
                    // flows back in order — no synthetic reply, no
                    // out-of-band Sync. Evict first if the cache was full.
                    send_eviction(eviction, server).await?;
                    let mut parse = parse;
                    parse.name = canon;
                    Ok(Some(proto::frontend::FrontendMessage::Parse(parse)))
                }
            }
        }

        proto::frontend::FrontendMessage::Bind(mut bind) => {
            if let Some(canon) = client_prepared.resolve(&bind.statement) {
                ensure_prepared(canon, server, pools).await?;
                bind.statement = canon.to_owned();
            }
            Ok(Some(proto::frontend::FrontendMessage::Bind(bind)))
        }

        proto::frontend::FrontendMessage::Describe(mut desc) => {
            if desc.kind == proto::frontend::TargetKind::Statement
                && let Some(canon) = client_prepared.resolve(&desc.name)
            {
                ensure_prepared(canon, server, pools).await?;
                desc.name = canon.to_owned();
            }
            Ok(Some(proto::frontend::FrontendMessage::Describe(desc)))
        }

        proto::frontend::FrontendMessage::Close(close) => {
            // Statement closes are absorbed by `handle_close_intercept`
            // before reaching here, so this is a portal close. Forward it
            // and record that its `CloseComplete` belongs to the client —
            // this keeps the `CloseComplete` FIFO aligned with any
            // interleaved eviction closes.
            server.statements.note_portal_close();
            Ok(Some(proto::frontend::FrontendMessage::Close(close)))
        }

        other => Ok(Some(other)),
    }
}

/// Ensure a canonical statement is prepared on the server connection.
///
/// On a cache miss the stored `Parse` is injected into the normal
/// outbound stream — **without** a `Sync` and without draining the
/// backend — and [`PreparedStatements`] records the disposition so the
/// proxy strips the matching `ParseComplete`/`CloseComplete` from the
/// client-facing stream. (The previous implementation injected a `Sync`
/// and drained to `ReadyForQuery`, which discarded responses the backend
/// had buffered for the client's own pipelined messages — corrupting the
/// stream for drivers that batch statements.)
///
/// Used for `Bind`/`Describe`-triggered re-preparation, where the client
/// never sent a `Parse`, so the injected `ParseComplete` is suppressed.
///
/// [`PreparedStatements`]: crate::connections::server::PreparedStatements
async fn ensure_prepared(
    canon: &str,
    server: &mut ServerConn,
    pools: &Arc<PoolManager>,
) -> anyhow::Result<()> {
    match server.statements.note_reprepare(canon) {
        Reprepare::AlreadyPrepared => Ok(()),
        Reprepare::Prepare { eviction } => {
            // Any canonical in `client_prepared` keeps its `Parse` in the
            // store (the ref is held until release), so this lookup is
            // effectively infallible. On the impossible miss we bail and
            // the connection is discarded, so the optimistic insert
            // `note_reprepare` just made never reaches the pool.
            let stored_parse = {
                let store = pools.stmt_store.lock();
                store.get(canon).cloned()
            };
            let Some(mut parse) = stored_parse else {
                anyhow::bail!("no stored Parse for canonical statement {canon}");
            };
            send_eviction(eviction, server).await?;
            parse.name = canon.to_owned();
            server
                .framed
                .send(proto::frontend::FrontendMessage::Parse(parse))
                .await?;
            Ok(())
        }
    }
}

/// Send a statement `Close` for an LRU-evicted canonical, if any. Its
/// `CloseComplete` is already queued for suppression by the
/// [`PreparedStatements`] call that returned `eviction`.
///
/// [`PreparedStatements`]: crate::connections::server::PreparedStatements
async fn send_eviction(eviction: Option<String>, server: &mut ServerConn) -> anyhow::Result<()> {
    if let Some(evicted) = eviction {
        server
            .framed
            .send(proto::frontend::FrontendMessage::Close(
                proto::frontend::Close {
                    kind: proto::frontend::TargetKind::Statement,
                    name: evicted,
                },
            ))
            .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_name_deterministic() {
        let a = canonical_name("SELECT 1", &[]);
        let b = canonical_name("SELECT 1", &[]);
        assert_eq!(a, b);
        assert!(a.starts_with("_hp_"));
    }

    #[test]
    fn canonical_name_differs_by_query() {
        let a = canonical_name("SELECT 1", &[]);
        let b = canonical_name("SELECT 2", &[]);
        assert_ne!(a, b);
    }

    #[test]
    fn canonical_name_differs_by_params() {
        let a = canonical_name("SELECT $1", &[23]); // int4
        let b = canonical_name("SELECT $1", &[25]); // text
        assert_ne!(a, b);
    }

    #[test]
    fn client_register_and_resolve() {
        let mut store = StatementStore::new();
        let mut client = ClientPrepared::new();

        let parse = Parse {
            name: "my_stmt".into(),
            query: "SELECT 1".into(),
            param_types: vec![],
        };

        let canon = client.register(&parse, &mut store);
        assert_eq!(client.resolve("my_stmt"), Some(canon.as_str()));
        assert!(store.get(&canon).is_some());
    }

    #[test]
    fn client_unnamed_tracked() {
        let mut store = StatementStore::new();
        let mut client = ClientPrepared::new();

        let parse = Parse {
            name: String::new(),
            query: "SELECT 1".into(),
            param_types: vec![],
        };

        let canon = client.register(&parse, &mut store);
        assert_eq!(client.resolve(""), Some(canon.as_str()));
        assert!(store.get(&canon).is_some());
    }

    #[test]
    fn client_unnamed_replaced_by_new_parse() {
        let mut store = StatementStore::new();
        let mut client = ClientPrepared::new();

        let parse1 = Parse {
            name: String::new(),
            query: "SELECT 1".into(),
            param_types: vec![],
        };
        let parse2 = Parse {
            name: String::new(),
            query: "SELECT 2".into(),
            param_types: vec![],
        };

        let canon1 = client.register(&parse1, &mut store);
        let canon2 = client.register(&parse2, &mut store);
        assert_ne!(canon1, canon2);

        // The unnamed slot now points to the second query.
        assert_eq!(client.resolve(""), Some(canon2.as_str()));
        // First query's refcount dropped to zero — removed from store.
        assert!(store.get(&canon1).is_none());
        assert!(store.get(&canon2).is_some());
    }

    #[test]
    fn reparse_same_query_does_not_leak_refcount() {
        let mut store = StatementStore::new();
        let mut client = ClientPrepared::new();

        let parse = Parse {
            name: String::new(),
            query: "SELECT 1".into(),
            param_types: vec![],
        };

        let canon = client.register(&parse, &mut store);
        // Re-register the exact same query — refcount must stay at 1.
        client.register(&parse, &mut store);
        client.register(&parse, &mut store);

        // release_all releases one ref — should reach zero.
        client.release_all(&mut store);
        assert!(
            store.get(&canon).is_none(),
            "store entry should be gone after release_all, but refcount leaked"
        );
    }

    #[test]
    fn client_remove() {
        let mut store = StatementStore::new();
        let mut client = ClientPrepared::new();

        let parse = Parse {
            name: "s1".into(),
            query: "SELECT 1".into(),
            param_types: vec![],
        };

        client.register(&parse, &mut store);
        assert!(client.resolve("s1").is_some());
        client.remove("s1", &mut store);
        assert!(client.resolve("s1").is_none());
    }

    #[test]
    fn deduplication_across_clients() {
        let mut store = StatementStore::new();
        let mut client_a = ClientPrepared::new();
        let mut client_b = ClientPrepared::new();

        let parse_a = Parse {
            name: "s1".into(),
            query: "SELECT 1".into(),
            param_types: vec![],
        };
        let parse_b = Parse {
            name: "s1".into(),
            query: "SELECT 1".into(),
            param_types: vec![],
        };

        let canon_a = client_a.register(&parse_a, &mut store);
        let canon_b = client_b.register(&parse_b, &mut store);

        // Same query text → same canonical name.
        assert_eq!(canon_a, canon_b);
    }

    #[test]
    fn no_collision_different_queries_same_client_name() {
        let mut store = StatementStore::new();
        let mut client_a = ClientPrepared::new();
        let mut client_b = ClientPrepared::new();

        let parse_a = Parse {
            name: "s1".into(),
            query: "SELECT 1".into(),
            param_types: vec![],
        };
        let parse_b = Parse {
            name: "s1".into(),
            query: "SELECT 2".into(),
            param_types: vec![],
        };

        let canon_a = client_a.register(&parse_a, &mut store);
        let canon_b = client_b.register(&parse_b, &mut store);

        // Same client name, different queries → different canonical names.
        assert_ne!(canon_a, canon_b);
    }

    #[test]
    fn store_refcount_cleanup() {
        let mut store = StatementStore::new();
        let mut client_a = ClientPrepared::new();
        let mut client_b = ClientPrepared::new();

        let parse = Parse {
            name: "s1".into(),
            query: "SELECT 1".into(),
            param_types: vec![],
        };

        let canon = client_a.register(&parse, &mut store);
        client_b.register(&parse, &mut store);

        // Both clients reference it — store has the entry.
        assert!(store.get(&canon).is_some());

        // Release client A — refcount drops to 1, still in store.
        client_a.release_all(&mut store);
        assert!(store.get(&canon).is_some());

        // Release client B — refcount drops to 0, removed from store.
        client_b.release_all(&mut store);
        assert!(store.get(&canon).is_none());
    }

    #[test]
    fn remove_decrements_refcount() {
        let mut store = StatementStore::new();
        let mut client = ClientPrepared::new();

        let parse = Parse {
            name: "s1".into(),
            query: "SELECT 1".into(),
            param_types: vec![],
        };

        let canon = client.register(&parse, &mut store);
        assert!(store.get(&canon).is_some());

        client.remove("s1", &mut store);
        // Single reference removed — gone from store.
        assert!(store.get(&canon).is_none());
    }
}
