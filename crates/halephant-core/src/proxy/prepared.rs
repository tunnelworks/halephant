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
//!    prepared. (The per-connection LRU cache lives on
//!    [`crate::connections::server::ServerPrepared`] so it can travel
//!    with the [`crate::connections::server::ServerConn`] it belongs
//!    to.)
//! 4. Transparently re-preparing statements on cache miss.
//!
//! The global, reference-counted store of Parse messages lives in
//! [`crate::pool::prepared::StatementStore`].

use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::connections::server::ServerConn;
use crate::pool::PoolManager;
use crate::pool::prepared::StatementStore;
use crate::proto;
use crate::proto::frontend::Parse;

// ---------------------------------------------------------------------------
// Canonical name generation
// ---------------------------------------------------------------------------

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
    /// client statement name → canonical name
    names: HashMap<String, String>,
}

impl ClientPrepared {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a client-side name for a Parse, returning the canonical name.
    pub fn register(&mut self, parse: &Parse, store: &mut StatementStore) -> String {
        let canon = canonical_name(&parse.query, &parse.param_types);
        store.add_ref(canon.clone(), parse.clone());
        if !parse.name.is_empty()
            && let Some(old_canon) = self.names.insert(parse.name.clone(), canon.clone())
            && old_canon != canon
        {
            store.release(&old_canon);
        }
        canon
    }

    /// Look up the canonical name for a client-side statement name. Returns
    /// `None` for unnamed statements (the empty string), which are always
    /// forwarded as-is.
    pub fn resolve(&self, client_name: &str) -> Option<&str> {
        if client_name.is_empty() {
            return None;
        }
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
            if parse.name.is_empty() {
                // Unnamed (anonymous) prepared statements are not cached — forward as-is.
                return Ok(Some(proto::frontend::FrontendMessage::Parse(parse)));
            }

            let canon = {
                let mut store = pools.stmt_store.lock();
                client_prepared.register(&parse, &mut store)
            };

            // Ensure the server has this statement. If it was already there or
            // was just prepared, do NOT forward the Parse again — PostgreSQL
            // rejects duplicate named statements. Send a synthetic ParseComplete
            // to the client instead.
            ensure_prepared(&canon, server, pools).await?;
            client
                .send(proto::backend::BackendMessage::ParseComplete)
                .await?;
            Ok(None)
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

        // Close for named statements is handled by handle_close_intercept
        // before reaching here. If one slips through (for portals or unnamed),
        // forward as-is.
        other => Ok(Some(other)),
    }
}

/// Ensure a canonical statement is prepared on the server connection. If the
/// server doesn't have it, send the stored Parse and consume the ParseComplete.
async fn ensure_prepared(
    canon: &str,
    server: &mut ServerConn,
    pools: &Arc<PoolManager>,
) -> anyhow::Result<()> {
    if server.prepared.contains(canon) {
        server.prepared.touch(canon);
        return Ok(());
    }

    // Look up the stored Parse first — bail if missing.
    let stored_parse = {
        let store = pools.stmt_store.lock();
        store.get(canon).cloned()
    };
    let Some(mut parse) = stored_parse else {
        anyhow::bail!("no stored Parse for canonical statement {canon}");
    };

    // Evict if necessary, then prepare.
    if let Some(evicted) = server.prepared.insert(canon.to_owned()) {
        server
            .framed
            .send(proto::frontend::FrontendMessage::Close(
                proto::frontend::Close {
                    kind: proto::frontend::TargetKind::Statement,
                    name: evicted,
                },
            ))
            .await?;
        server
            .framed
            .send(proto::frontend::FrontendMessage::Sync)
            .await?;
        drain_until_ready(&mut server.framed).await?;
    }

    parse.name = canon.to_owned();
    server
        .framed
        .send(proto::frontend::FrontendMessage::Parse(parse))
        .await?;
    server
        .framed
        .send(proto::frontend::FrontendMessage::Sync)
        .await?;
    drain_until_ready(&mut server.framed).await?;

    Ok(())
}

/// Drain server responses until ReadyForQuery, discarding everything except
/// errors (which are propagated).
async fn drain_until_ready(
    framed: &mut Framed<TcpStream, proto::codec::BackendCodec>,
) -> anyhow::Result<()> {
    loop {
        match framed.next().await.transpose()? {
            Some(proto::backend::BackendMessage::ReadyForQuery(_)) => return Ok(()),
            Some(proto::backend::BackendMessage::ErrorResponse(err)) => {
                anyhow::bail!(
                    "server error during prepared statement management: {}",
                    err.message().unwrap_or("unknown")
                );
            }
            Some(_) => {} // ParseComplete, CloseComplete, etc.
            None => anyhow::bail!("upstream closed during prepared statement management"),
        }
    }
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
    fn client_unnamed_not_tracked() {
        let mut store = StatementStore::new();
        let mut client = ClientPrepared::new();

        let parse = Parse {
            name: String::new(),
            query: "SELECT 1".into(),
            param_types: vec![],
        };

        client.register(&parse, &mut store);
        assert_eq!(client.resolve(""), None);
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
