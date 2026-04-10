//! Minimal SQL parser for halephant's routing and interception needs.
//!
//! The module is split into three layers:
//!
//! - `lexer`: chumsky character-level tokenizer — handles nested
//!   block comments, dollar-quoted strings, single-quoted strings with
//!   `''` escapes, and double-quoted identifiers with `""` escapes.
//! - `parser`: a tiny hand-written recursive-descent walker over the
//!   token slice, producing a [`Statement`] AST.
//! - `ast`: the AST — one variant per statement shape halephant
//!   cares about, with all relevant fields captured inline so
//!   consumers never need a second extraction helper.
//!
//! Public API is deliberately small: [`parse`] to get a [`Statement`],
//! [`as_boolean`] to interpret a PostgreSQL boolean literal. Everything
//! else is a method on the AST (see [`Statement::is_read_only`] and
//! [`Statement::is_read_write_override`]).

mod ast;
mod lexer;
mod parser;

pub use ast::{
    DeallocateTarget, DiscardTarget, ResetTarget, SetScope, SetValue, Statement,
    TransactionOptions, UnlistenTarget,
};
pub use parser::parse;

/// Interpret a lowercased string as a PostgreSQL boolean value.
///
/// PostgreSQL accepts unambiguous prefixes of `true`, `false`, `yes`,
/// `no`, `on`, `off`, plus the numeric literals `1` and `0`. A bare
/// `o` is rejected because it is ambiguous between `on` and `off`.
///
/// This helper is exposed so callers can interpret a
/// [`SetValue::Literal`] payload — the parser itself never resolves
/// boolean semantics, because a single GUC value might be boolean in
/// one context and a literal string in another.
pub fn as_boolean(val: &str) -> Option<bool> {
    match val {
        "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "on" | "1" => Some(true),
        "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "of" | "off" | "0" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boolean_true_values() {
        for s in ["t", "tr", "tru", "true", "y", "ye", "yes", "on", "1"] {
            assert_eq!(as_boolean(s), Some(true), "{s:?} should be true");
        }
    }

    #[test]
    fn boolean_false_values() {
        for s in [
            "f", "fa", "fal", "fals", "false", "n", "no", "of", "off", "0",
        ] {
            assert_eq!(as_boolean(s), Some(false), "{s:?} should be false");
        }
    }

    #[test]
    fn bare_o_is_ambiguous() {
        assert_eq!(as_boolean("o"), None);
    }

    #[test]
    fn invalid_boolean_rejected() {
        for s in [
            "", "maybe", "2", "256mb", "truth", "nope", "yep", "11", "10",
        ] {
            assert_eq!(as_boolean(s), None, "{s:?} should not parse");
        }
    }
}
