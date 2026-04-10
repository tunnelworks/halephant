//! Minimal SQL statement AST.
//!
//! Each variant captures exactly the fields halephant needs to route,
//! intercept, or classify a statement. Anything outside that set
//! collapses into `Statement::Other`, so consumers can rely on
//! exhaustive pattern matches without a parser that understands the
//! full PostgreSQL grammar.
//!
//! This is deliberately not a faithful AST: nested expressions, join
//! trees, projections, and ordering clauses are all invisible to it.
//! The design goal is "enough structure that a caller never needs a
//! second helper function to pull a value out of the result."

/// A parsed SQL statement.
///
/// The parser only inspects leading tokens, so multi-statement
/// queries are classified by their first statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    /// `LISTEN channel`. The channel name is lowercased for unquoted
    /// identifiers and case-preserved for `"quoted"` identifiers —
    /// matching PostgreSQL's own rules.
    Listen { channel: String },

    /// `UNLISTEN *` or `UNLISTEN channel`.
    Unlisten { target: UnlistenTarget },

    /// `SET [SESSION | LOCAL] parameter (= | TO) value` — a regular
    /// GUC assignment.
    ///
    /// `SET TRANSACTION ...` and `SET SESSION CHARACTERISTICS AS
    /// TRANSACTION ...` are parsed as [`Statement::SetTransaction`]
    /// instead; those configure transaction characteristics, not a
    /// parameter value.
    Set {
        scope: SetScope,
        parameter: String,
        value: SetValue,
    },

    /// `SET TRANSACTION <modes>` (for the current transaction) or
    /// `SET SESSION CHARACTERISTICS AS TRANSACTION <modes>` (session
    /// defaults for future transactions). Both share the same
    /// transaction-mode grammar, so they flatten to a single variant.
    SetTransaction { options: TransactionOptions },

    /// `RESET ALL` or `RESET parameter`.
    Reset { target: ResetTarget },

    /// `DISCARD ALL | PLANS | SEQUENCES | TEMPORARY | TEMP`.
    ///
    /// The subtarget matters: only `DISCARD ALL` drops prepared
    /// statements (via its embedded `DEALLOCATE ALL`) and resets
    /// session GUCs. `PLANS` only invalidates cached query plans —
    /// the prepared statements themselves persist. `SEQUENCES` and
    /// `TEMPORARY` do not touch the pool state halephant tracks.
    /// See [`DiscardTarget`] for the full list.
    Discard { target: DiscardTarget },

    /// `DEALLOCATE [PREPARE] (name | ALL)`.
    ///
    /// The `PREPARE` keyword between `DEALLOCATE` and the target is
    /// optional noise in PostgreSQL's grammar; the parser collapses
    /// both forms to the same variant.
    ///
    /// Halephant intercepts this at the transaction layer: the server
    /// only knows the canonical (hashed) name halephant assigned
    /// during Parse rewriting, so a raw `DEALLOCATE my_stmt` would
    /// error out with "prepared statement does not exist". The
    /// interceptor resolves the client-side name against
    /// `ClientPrepared`, releases the refcount in the global
    /// `StatementStore`, and synthesises a `CommandComplete` response
    /// without forwarding to the server — the actual server-side
    /// statement is eventually reclaimed by `ServerPrepared`'s LRU.
    Deallocate { target: DeallocateTarget },

    /// `BEGIN [transaction_mode ...]` or `START TRANSACTION
    /// [transaction_mode ...]`. Folded to a single variant since
    /// PostgreSQL treats them identically.
    Begin { options: TransactionOptions },

    /// `COMMIT` (or its `END` alias).
    Commit,

    /// `ROLLBACK` or `ABORT` (including `ROLLBACK TO SAVEPOINT ...`).
    Rollback,

    /// `SELECT ...`, `TABLE ...`, `VALUES ...`, or a
    /// `WITH ... SELECT/TABLE/VALUES`.
    Select,

    /// `INSERT ...`, or a `WITH ... INSERT`.
    Insert,

    /// `UPDATE ...`, or a `WITH ... UPDATE`.
    Update,

    /// `DELETE ...`, `TRUNCATE ...`, or a `WITH ... DELETE`.
    Delete,

    /// `COPY ...`.
    Copy,

    /// Anything else: DDL, empty input, bare punctuation, statements
    /// whose leading keyword is not recognised. Halephant forwards
    /// these without further inspection.
    Other,
}

impl Statement {
    /// Returns `true` when the statement itself is read-only — currently
    /// only plain `SELECT`. This is a *statement* property, independent
    /// of any transaction-level read-only mode the caller tracks
    /// separately.
    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::Select)
    }

    /// Returns `true` when the statement attempts to switch a read-only
    /// transaction back to read-write. Covers every variant that can
    /// flip the effective read/write mode:
    ///
    /// - `BEGIN READ WRITE` / `START TRANSACTION READ WRITE`
    /// - `SET TRANSACTION READ WRITE`
    /// - `SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE`
    /// - `SET default_transaction_read_only = <false-ish>` or
    ///   `TO DEFAULT` (the compiled default for that GUC is `off`,
    ///   so resetting it to default means read-write)
    /// - `RESET default_transaction_read_only` (identical effect to
    ///   `SET ... TO DEFAULT` — both revert to the compiled default)
    /// - `RESET ALL` (sweeps every session setting, including
    ///   `default_transaction_read_only`)
    ///
    /// Halephant uses this to reject writes on replica-routed
    /// transactions without having to re-parse the statement. Missing
    /// any of the above would let a client silently restore
    /// read-write on a replica-routed connection.
    pub fn is_read_write_override(&self) -> bool {
        match self {
            Self::Begin { options } | Self::SetTransaction { options } => {
                options.read_only == Some(false)
            }
            Self::Set {
                parameter, value, ..
            } if parameter == "default_transaction_read_only" => match value {
                SetValue::Default => true,
                SetValue::Literal(s) => crate::sql::as_boolean(s) == Some(false),
            },
            Self::Reset {
                target: ResetTarget::Parameter(name),
            } if name == "default_transaction_read_only" => true,
            Self::Reset {
                target: ResetTarget::All,
            } => true,
            _ => false,
        }
    }
}

/// Target of an `UNLISTEN`: a specific channel name or the wildcard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnlistenTarget {
    /// `UNLISTEN *` — drop all subscriptions. Note that a double-quoted
    /// `UNLISTEN "*"` is parsed as `Channel("*")`, not `Star`, because
    /// the quoted form is a literal identifier named `*`.
    Star,
    /// `UNLISTEN channel`. Lowercased for unquoted, case-preserved
    /// for `"quoted"`.
    Channel(String),
}

/// Visibility scope of a `SET` assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetScope {
    /// `SET [SESSION] parameter = value` — applies to the current
    /// session until explicitly reset.
    Session,
    /// `SET LOCAL parameter = value` — applies only within the current
    /// transaction and is discarded on COMMIT/ROLLBACK.
    Local,
}

/// Value on the right-hand side of a `SET parameter (= | TO) ...`.
///
/// Literals are canonicalised by the lexer: unquoted keywords and
/// single-quoted strings are lowercased (so `'Off'` and `off` both
/// arrive as `"off"`), and numeric literals preserve their original
/// digits. Double-quoted identifiers are never accepted as values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetValue {
    /// A concrete literal value. Pass this to [`crate::sql::as_boolean`]
    /// to interpret it as a PostgreSQL boolean.
    Literal(String),
    /// `SET parameter (= | TO) DEFAULT` — reset to the compiled-in
    /// default. This is syntactically distinct from a literal because
    /// the caller often needs to resolve "default" against the GUC's
    /// compiled default.
    Default,
}

/// Target of a `RESET`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResetTarget {
    /// `RESET ALL` — reset every session-level setting.
    All,
    /// `RESET parameter` — reset a single parameter.
    Parameter(String),
}

/// Target of a `DISCARD`. PostgreSQL's `DISCARD ALL` is defined as a
/// macro expansion of the other `DISCARD` variants plus `DEALLOCATE
/// ALL`, `RESET ALL`, `CLOSE ALL`, `UNLISTEN *`, and
/// `pg_advisory_unlock_all()`. The distinction matters because
/// halephant's per-connection pool state tracking needs to react to
/// `ALL` (clear prepared statements and dirty GUCs) but not to the
/// narrower forms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardTarget {
    /// `DISCARD ALL` — full session reset. Drops prepared statements
    /// (DEALLOCATE ALL), resets every GUC (RESET ALL), closes cursors,
    /// and drops temp tables. Halephant must clear both its
    /// `ServerPrepared` tracker and its `dirty_vars` set on the
    /// affected connection.
    All,
    /// `DISCARD PLANS` — releases cached query plans only. The
    /// prepared statements themselves remain registered on the
    /// backend; the next `Bind` will trigger re-planning transparently,
    /// so halephant's pool state is unaffected.
    Plans,
    /// `DISCARD SEQUENCES` — discards cached sequence-related state.
    /// Has no bearing on halephant's pool tracking.
    Sequences,
    /// `DISCARD TEMPORARY` / `DISCARD TEMP` — drops temp tables. Has
    /// no bearing on halephant's pool tracking.
    Temp,
}

/// Target of a `DEALLOCATE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeallocateTarget {
    /// `DEALLOCATE ALL` / `DEALLOCATE PREPARE ALL` — drop every
    /// prepared statement the client has registered.
    All,
    /// `DEALLOCATE name` / `DEALLOCATE PREPARE name` — drop a single
    /// named prepared statement. The name is the client-side
    /// identifier from the original `Parse`; halephant must resolve
    /// it to a canonical name before touching the global store.
    /// Lowercased for unquoted identifiers, case-preserved for
    /// `"quoted"` identifiers, matching PostgreSQL's rules.
    Name(String),
}

/// Transaction-mode options gathered from a `BEGIN`, `START
/// TRANSACTION`, `SET TRANSACTION`, or `SET SESSION CHARACTERISTICS AS
/// TRANSACTION` clause.
///
/// Only the fields halephant actually uses are tracked. Isolation
/// level and deferrable flags are parsed implicitly (the parser walks
/// past them without producing errors) but not surfaced, because
/// nothing in the router needs them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionOptions {
    /// `Some(true)` for an explicit `READ ONLY`, `Some(false)` for
    /// `READ WRITE`, `None` when neither was specified. When both
    /// appear (malformed SQL), the *last* one wins — matching
    /// PostgreSQL's own parser behaviour.
    pub read_only: Option<bool>,
}
