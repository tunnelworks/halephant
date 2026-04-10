//! Global, reference-counted storage of `Parse` messages keyed by
//! canonical name.
//!
//! The proxy's prepared-statement rewriter
//! ([`crate::proxy::prepared`]) assigns each incoming client `Parse`
//! a canonical name based on a hash of its query text and parameter
//! types, then calls [`StatementStore::add_ref`] so every subsequent
//! client that submits the same statement text shares a single
//! entry. When a server connection is missing a canonical statement,
//! the pool looks up the original `Parse` via [`StatementStore::get`]
//! and replays it; when the last referencing client drops, the entry
//! is reclaimed.

use std::collections::HashMap;

use crate::proto::frontend::Parse;

/// Stores the original Parse messages keyed by canonical name, with reference
/// counting. Entries are removed when no client references them.
#[derive(Default)]
pub struct StatementStore {
    statements: HashMap<String, StoreEntry>,
}

struct StoreEntry {
    parse: Parse,
    refs: usize,
}

impl StatementStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the reference count for a canonical name, inserting the Parse
    /// if this is the first reference.
    pub(crate) fn add_ref(&mut self, canon: String, mut parse: Parse) {
        let entry = self.statements.entry(canon).or_insert_with(|| {
            parse.name = String::new();
            StoreEntry { parse, refs: 0 }
        });
        entry.refs += 1;
    }

    /// Decrement the reference count. Removes the entry when it reaches zero.
    pub(crate) fn release(&mut self, canon: &str) {
        if let Some(entry) = self.statements.get_mut(canon) {
            entry.refs = entry.refs.saturating_sub(1);
            if entry.refs == 0 {
                self.statements.remove(canon);
            }
        }
    }

    /// Get the stored Parse for a canonical name.
    pub fn get(&self, canon: &str) -> Option<&Parse> {
        self.statements.get(canon).map(|e| &e.parse)
    }
}
