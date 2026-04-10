//! Token-level statement parser.
//!
//! Given a slice of [`Token`]s from [`super::lexer::lex`], walk the
//! leading tokens and build a [`Statement`] AST.
//!
//! This is deliberately a tiny hand-written recursive-descent walker
//! rather than a second chumsky parser layer. Chumsky shines on
//! complex grammars with backtracking and error recovery, neither of
//! which apply here: the grammar is ~10 statement shapes, every
//! unrecognised input falls through to `Statement::Other`, and there
//! are no errors to surface. Imperative cursor-based parsing is
//! clearer and produces less machinery for a reader to digest.

use super::ast::{
    DeallocateTarget, DiscardTarget, ResetTarget, SetScope, SetValue, Statement,
    TransactionOptions, UnlistenTarget,
};
use super::lexer::{Token, lex};

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Parse a SQL string into a [`Statement`].
///
/// The parser inspects only the leading tokens of the query, so for
/// multi-statement input it classifies by the first statement — the
/// same convention every proxy-level parser uses.
pub fn parse(sql: &str) -> Statement {
    let tokens = lex(sql);
    let mut cur = Cursor::new(&tokens);
    parse_statement(&mut cur)
}

// ---------------------------------------------------------------------------
// Cursor
// ---------------------------------------------------------------------------

/// Linear iterator over the token slice with enough lookahead for the
/// shapes we care about (one-token for most branches, two-token for
/// `SET SESSION CHARACTERISTICS`). Everything is `O(1)`: no cloning,
/// no backtracking buffer.
struct Cursor<'a> {
    tokens: &'a [Token],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(tokens: &'a [Token]) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&'a Token> {
        self.tokens.get(self.pos)
    }

    /// Look at the token `offset` positions ahead without advancing.
    /// `peek_at(0)` is equivalent to `peek()`.
    fn peek_at(&self, offset: usize) -> Option<&'a Token> {
        self.tokens.get(self.pos + offset)
    }

    fn advance(&mut self) -> Option<&'a Token> {
        let t = self.tokens.get(self.pos)?;
        self.pos += 1;
        Some(t)
    }

    /// If the next token is a `Word(expected)`, consume it and return
    /// `true`. Otherwise leave the cursor untouched.
    fn eat_word(&mut self, expected: &str) -> bool {
        if matches!(self.peek(), Some(Token::Word(w)) if w == expected) {
            self.pos += 1;
            return true;
        }
        false
    }

    /// Consume the next token as an identifier — either an unquoted
    /// `Word` (lowercased by the lexer) or a `QuotedIdent` (case
    /// preserved). Returns `None` on any other token.
    fn eat_identifier(&mut self) -> Option<String> {
        match self.peek()? {
            Token::Word(name) | Token::QuotedIdent(name) => {
                let s = name.clone();
                self.pos += 1;
                Some(s)
            }
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Statement dispatch
// ---------------------------------------------------------------------------

fn parse_statement(cur: &mut Cursor<'_>) -> Statement {
    let Some(Token::Word(first)) = cur.peek() else {
        return Statement::Other;
    };
    match first.as_str() {
        "listen" => parse_listen(cur),
        "unlisten" => parse_unlisten(cur),
        "set" => parse_set(cur),
        "reset" => parse_reset(cur),
        "discard" => parse_discard(cur),
        "deallocate" => parse_deallocate(cur),
        "begin" | "start" => parse_begin(cur),
        "commit" | "end" => Statement::Commit,
        "rollback" | "abort" => Statement::Rollback,
        "select" | "table" | "values" => Statement::Select,
        "insert" => Statement::Insert,
        "update" => Statement::Update,
        "delete" | "truncate" => Statement::Delete,
        "copy" => Statement::Copy,
        "with" => parse_cte_wrapped(cur),
        _ => Statement::Other,
    }
}

// ---------------------------------------------------------------------------
// LISTEN / UNLISTEN
// ---------------------------------------------------------------------------

fn parse_listen(cur: &mut Cursor<'_>) -> Statement {
    cur.advance(); // LISTEN
    match cur.eat_identifier() {
        Some(channel) => Statement::Listen { channel },
        // `LISTEN` without a following identifier is malformed SQL;
        // collapse to Other rather than fabricate an empty channel.
        None => Statement::Other,
    }
}

fn parse_unlisten(cur: &mut Cursor<'_>) -> Statement {
    cur.advance(); // UNLISTEN

    // Note: the `Token::Star` arm is only reachable for an unquoted
    // `*` in the source text. A double-quoted `"*"` comes in as
    // `Token::QuotedIdent("*")` and falls into the identifier arm
    // below, becoming `Channel("*")` — the right behaviour, since
    // the quoted form is a literal identifier, not the wildcard.
    match cur.peek() {
        Some(Token::Star) => {
            cur.advance();
            Statement::Unlisten {
                target: UnlistenTarget::Star,
            }
        }
        Some(Token::Word(name) | Token::QuotedIdent(name)) => {
            let name = name.clone();
            cur.advance();
            Statement::Unlisten {
                target: UnlistenTarget::Channel(name),
            }
        }
        _ => Statement::Other,
    }
}

// ---------------------------------------------------------------------------
// SET / RESET / DISCARD
// ---------------------------------------------------------------------------

fn parse_set(cur: &mut Cursor<'_>) -> Statement {
    cur.advance(); // SET

    // `SET TRANSACTION <modes>` — transaction characteristics for
    // the current transaction. Must come before the LOCAL/SESSION
    // check because it takes no scope prefix.
    if cur.eat_word("transaction") {
        return Statement::SetTransaction {
            options: parse_transaction_options(cur),
        };
    }

    // Scope prefix: LOCAL or SESSION. SESSION has the extra special
    // case `SET SESSION CHARACTERISTICS AS TRANSACTION <modes>`, which
    // sets session-level transaction defaults and is also a
    // SetTransaction variant — because the grammar after `AS
    // TRANSACTION` is the same.
    let scope = if cur.eat_word("local") {
        SetScope::Local
    } else if cur.eat_word("session") {
        if cur.eat_word("characteristics") {
            if cur.eat_word("as") && cur.eat_word("transaction") {
                return Statement::SetTransaction {
                    options: parse_transaction_options(cur),
                };
            }
            // `SET SESSION CHARACTERISTICS ...` without `AS
            // TRANSACTION` is malformed.
            return Statement::Other;
        }
        SetScope::Session
    } else {
        SetScope::Session
    };

    // `SET [scope] parameter (= | TO) value`
    let Some(parameter) = cur.eat_identifier() else {
        return Statement::Other;
    };

    // Consume the `=` (arrives as `Token::Other` from the lexer) or
    // the `TO` keyword.
    match cur.peek() {
        Some(Token::Word(w)) if w == "to" => {
            cur.advance();
        }
        Some(Token::Other) => {
            cur.advance();
        }
        _ => return Statement::Other,
    }

    let value = match cur.peek() {
        Some(Token::Word(w)) if w == "default" => {
            cur.advance();
            SetValue::Default
        }
        Some(Token::Word(v) | Token::Literal(v) | Token::Number(v)) => {
            let v = v.clone();
            cur.advance();
            SetValue::Literal(v)
        }
        // Signed numeric values like `SET work_mem = -1` or
        // `SET seed = +0.5`: the sign character lexes as
        // `Token::Other` because the lexer collapses unknown
        // punctuation uniformly. If the token immediately after the
        // sign is a Number, consume both and record the parameter
        // as dirty. The actual numeric value is irrelevant for
        // `dirty_vars` tracking (which only keys on the parameter
        // name), so we store the bare number without the sign.
        //
        // Known limitation: `SET default_transaction_read_only = -0`
        // is pathological — PostgreSQL would treat it as
        // `0`/read-write, but halephant's `is_read_write_override`
        // check runs `as_boolean("0")` against the stored literal
        // (sign stripped), so it would correctly flag this. For any
        // other signed value, `as_boolean` on the bare digits
        // returns `None`, so no false read-write override fires.
        Some(Token::Other) if matches!(cur.peek_at(1), Some(Token::Number(_))) => {
            cur.advance(); // sign
            let Some(Token::Number(v)) = cur.peek() else {
                unreachable!("peek_at(1) matched Token::Number above");
            };
            let v = v.clone();
            cur.advance();
            SetValue::Literal(v)
        }
        _ => return Statement::Other,
    };

    Statement::Set {
        scope,
        parameter,
        value,
    }
}

fn parse_reset(cur: &mut Cursor<'_>) -> Statement {
    cur.advance(); // RESET
    match cur.peek() {
        Some(Token::Word(w)) if w == "all" => {
            cur.advance();
            Statement::Reset {
                target: ResetTarget::All,
            }
        }
        Some(Token::Word(_) | Token::QuotedIdent(_)) => {
            // Unwrap is safe: we just matched Word/QuotedIdent above.
            let name = cur.eat_identifier().expect("identifier after RESET");
            Statement::Reset {
                target: ResetTarget::Parameter(name),
            }
        }
        _ => Statement::Other,
    }
}

fn parse_discard(cur: &mut Cursor<'_>) -> Statement {
    cur.advance(); // DISCARD
    let Some(Token::Word(w)) = cur.peek() else {
        // Bare `DISCARD` is not valid SQL — fall through so the
        // server surfaces the syntax error.
        return Statement::Other;
    };
    let target = match w.as_str() {
        "all" => DiscardTarget::All,
        "plans" => DiscardTarget::Plans,
        "sequences" => DiscardTarget::Sequences,
        "temporary" | "temp" => DiscardTarget::Temp,
        // Unknown DISCARD subtarget — PostgreSQL would reject it too.
        _ => return Statement::Other,
    };
    cur.advance();
    Statement::Discard { target }
}

fn parse_deallocate(cur: &mut Cursor<'_>) -> Statement {
    cur.advance(); // DEALLOCATE

    // `PREPARE` is an optional noise keyword between DEALLOCATE and
    // the target; consume it if present and keep going.
    cur.eat_word("prepare");

    match cur.peek() {
        // `DEALLOCATE [PREPARE] ALL` — must be the unquoted `ALL`
        // keyword, not a (case-preserved) quoted identifier named
        // "ALL".
        Some(Token::Word(w)) if w == "all" => {
            cur.advance();
            Statement::Deallocate {
                target: DeallocateTarget::All,
            }
        }
        // `DEALLOCATE [PREPARE] name` — lowercased for unquoted,
        // case-preserved for `"quoted"`.
        Some(Token::Word(_) | Token::QuotedIdent(_)) => {
            // Unwrap is safe: we just matched Word/QuotedIdent.
            let name = cur.eat_identifier().expect("identifier after DEALLOCATE");
            Statement::Deallocate {
                target: DeallocateTarget::Name(name),
            }
        }
        // Bare `DEALLOCATE` or `DEALLOCATE PREPARE` alone is
        // malformed — PostgreSQL requires a target. Fall through so
        // the server surfaces the syntax error rather than silently
        // succeeding with a synthetic response.
        _ => Statement::Other,
    }
}

// ---------------------------------------------------------------------------
// BEGIN / START TRANSACTION
// ---------------------------------------------------------------------------

fn parse_begin(cur: &mut Cursor<'_>) -> Statement {
    // Consume the leading keyword and remember which one it was:
    // `START` requires a following `TRANSACTION`; `BEGIN` accepts an
    // optional `WORK` or `TRANSACTION` keyword.
    let is_start = matches!(cur.peek(), Some(Token::Word(w)) if w == "start");
    cur.advance();

    if is_start {
        // `START` on its own (or `START WORK`, `START READ ONLY`,
        // ...) is not valid SQL — PostgreSQL only accepts `START
        // TRANSACTION [...]`. Fall through to `Other` so the server
        // surfaces the syntax error to the client instead of us
        // fabricating a `Begin` and routing malformed `START READ
        // ONLY` to a replica.
        if !cur.eat_word("transaction") {
            return Statement::Other;
        }
    } else {
        // BEGIN accepts an optional trailing WORK or TRANSACTION
        // noise word; neither changes semantics, both are skipped.
        let _ = cur.eat_word("work") || cur.eat_word("transaction");
    }

    Statement::Begin {
        options: parse_transaction_options(cur),
    }
}

/// Scan the remaining tokens for transaction-mode keywords, collecting
/// `READ ONLY` / `READ WRITE` into [`TransactionOptions`]. Other
/// transaction-mode tokens (`ISOLATION LEVEL SERIALIZABLE`,
/// `DEFERRABLE`, comma separators) are skipped without error — the
/// router only cares about the read/write flag.
///
/// Crucially, a bare `READ` without a following `ONLY`/`WRITE` (as in
/// `ISOLATION LEVEL READ COMMITTED` or `REPEATABLE READ`) does not
/// break the scan: the loop simply continues, so the later `READ ONLY`
/// in `ISOLATION LEVEL REPEATABLE READ READ ONLY` is still picked up.
fn parse_transaction_options(cur: &mut Cursor<'_>) -> TransactionOptions {
    let mut opts = TransactionOptions::default();
    while let Some(tok) = cur.peek() {
        match tok {
            Token::Word(w) if w == "read" => {
                cur.advance();
                match cur.peek() {
                    Some(Token::Word(w)) if w == "only" => {
                        cur.advance();
                        opts.read_only = Some(true);
                    }
                    Some(Token::Word(w)) if w == "write" => {
                        cur.advance();
                        opts.read_only = Some(false);
                    }
                    // `READ` was a decoy from an isolation-level
                    // clause — don't touch read_only, keep scanning.
                    _ => {}
                }
            }
            _ => {
                cur.advance();
            }
        }
    }
    opts
}

// ---------------------------------------------------------------------------
// WITH <cte> ... <statement>
// ---------------------------------------------------------------------------

/// Scan past CTE body parens at depth zero to find the wrapped
/// statement's leading keyword. A `WITH` query that isn't followed by
/// a recognised DML keyword returns `Other` — PostgreSQL would reject
/// it too.
fn parse_cte_wrapped(cur: &mut Cursor<'_>) -> Statement {
    cur.advance(); // WITH
    let mut depth = 0u32;
    while let Some(t) = cur.advance() {
        match t {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            Token::Word(w) if depth == 0 => match w.as_str() {
                "select" | "table" | "values" => return Statement::Select,
                "insert" => return Statement::Insert,
                "update" => return Statement::Update,
                "delete" | "truncate" => return Statement::Delete,
                _ => {}
            },
            _ => {}
        }
    }
    Statement::Other
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- LISTEN ----------------------------------------------------------

    #[test]
    fn listen_unquoted() {
        assert_eq!(
            parse("LISTEN my_channel"),
            Statement::Listen {
                channel: "my_channel".into()
            }
        );
    }

    #[test]
    fn listen_quoted_preserves_case() {
        assert_eq!(
            parse(r#"LISTEN "MyChannel""#),
            Statement::Listen {
                channel: "MyChannel".into()
            }
        );
    }

    #[test]
    fn listen_without_channel_is_other() {
        // Malformed `LISTEN` with no following identifier.
        assert_eq!(parse("LISTEN"), Statement::Other);
    }

    // ---- UNLISTEN --------------------------------------------------------

    #[test]
    fn unlisten_star() {
        assert_eq!(
            parse("UNLISTEN *"),
            Statement::Unlisten {
                target: UnlistenTarget::Star
            }
        );
    }

    #[test]
    fn unlisten_quoted_star_is_channel_not_wildcard() {
        // The quoted form is a literal identifier named `*`, not the
        // wildcard.
        assert_eq!(
            parse(r#"UNLISTEN "*""#),
            Statement::Unlisten {
                target: UnlistenTarget::Channel("*".into())
            }
        );
    }

    #[test]
    fn unlisten_channel_name() {
        assert_eq!(
            parse("UNLISTEN my_channel"),
            Statement::Unlisten {
                target: UnlistenTarget::Channel("my_channel".into())
            }
        );
    }

    // ---- SET -------------------------------------------------------------

    #[test]
    fn set_session_default() {
        assert_eq!(
            parse("SET work_mem = '256MB'"),
            Statement::Set {
                scope: SetScope::Session,
                parameter: "work_mem".into(),
                value: SetValue::Literal("256mb".into()),
            }
        );
    }

    #[test]
    fn set_to_syntax() {
        assert_eq!(
            parse("SET search_path TO public"),
            Statement::Set {
                scope: SetScope::Session,
                parameter: "search_path".into(),
                value: SetValue::Literal("public".into()),
            }
        );
    }

    #[test]
    fn set_local_scope() {
        assert_eq!(
            parse("SET LOCAL work_mem = '1MB'"),
            Statement::Set {
                scope: SetScope::Local,
                parameter: "work_mem".into(),
                value: SetValue::Literal("1mb".into()),
            }
        );
    }

    #[test]
    fn set_session_scope_explicit() {
        assert_eq!(
            parse("SET SESSION work_mem = '1GB'"),
            Statement::Set {
                scope: SetScope::Session,
                parameter: "work_mem".into(),
                value: SetValue::Literal("1gb".into()),
            }
        );
    }

    #[test]
    fn set_default_value() {
        assert_eq!(
            parse("SET default_transaction_read_only TO DEFAULT"),
            Statement::Set {
                scope: SetScope::Session,
                parameter: "default_transaction_read_only".into(),
                value: SetValue::Default,
            }
        );
    }

    #[test]
    fn set_quoted_parameter_preserves_case() {
        assert_eq!(
            parse(r#"SET "MyVar" = 'value'"#),
            Statement::Set {
                scope: SetScope::Session,
                parameter: "MyVar".into(),
                value: SetValue::Literal("value".into()),
            }
        );
    }

    /// Regression guard: a SET whose value has a leading sign
    /// character must still parse as `Statement::Set` with the
    /// parameter name populated — not collapse to `Other`. Otherwise
    /// `track_set_reset` never inserts the parameter into
    /// `dirty_vars` and `reset_connection` fails to RESET it on
    /// checkin, leaking the modified value to the next pool client.
    #[test]
    fn set_negative_number() {
        assert_eq!(
            parse("SET work_mem = -1"),
            Statement::Set {
                scope: SetScope::Session,
                parameter: "work_mem".into(),
                value: SetValue::Literal("1".into()),
            }
        );
    }

    #[test]
    fn set_positive_sign_number() {
        assert_eq!(
            parse("SET work_mem = +5"),
            Statement::Set {
                scope: SetScope::Session,
                parameter: "work_mem".into(),
                value: SetValue::Literal("5".into()),
            }
        );
    }

    #[test]
    fn set_negative_with_to_syntax() {
        assert_eq!(
            parse("SET seed TO -1"),
            Statement::Set {
                scope: SetScope::Session,
                parameter: "seed".into(),
                value: SetValue::Literal("1".into()),
            }
        );
    }

    #[test]
    fn set_local_negative_number() {
        // LOCAL scope must still be tracked; otherwise the
        // transaction-level GUC change leaks the wrong way.
        assert_eq!(
            parse("SET LOCAL work_mem = -1"),
            Statement::Set {
                scope: SetScope::Local,
                parameter: "work_mem".into(),
                value: SetValue::Literal("1".into()),
            }
        );
    }

    // ---- SET TRANSACTION -------------------------------------------------

    #[test]
    fn set_transaction_read_write() {
        assert_eq!(
            parse("SET TRANSACTION READ WRITE"),
            Statement::SetTransaction {
                options: TransactionOptions {
                    read_only: Some(false)
                }
            }
        );
    }

    #[test]
    fn set_session_characteristics() {
        assert_eq!(
            parse("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE"),
            Statement::SetTransaction {
                options: TransactionOptions {
                    read_only: Some(false)
                }
            }
        );
    }

    #[test]
    fn set_session_characteristics_malformed_is_other() {
        assert_eq!(
            parse("SET SESSION CHARACTERISTICS work_mem = '1GB'"),
            Statement::Other
        );
    }

    // ---- RESET -----------------------------------------------------------

    #[test]
    fn reset_all() {
        assert_eq!(
            parse("RESET ALL"),
            Statement::Reset {
                target: ResetTarget::All
            }
        );
    }

    #[test]
    fn reset_parameter() {
        assert_eq!(
            parse("RESET work_mem"),
            Statement::Reset {
                target: ResetTarget::Parameter("work_mem".into())
            }
        );
    }

    // ---- DISCARD ---------------------------------------------------------

    #[test]
    fn discard_variants() {
        assert_eq!(
            parse("DISCARD ALL"),
            Statement::Discard {
                target: DiscardTarget::All
            }
        );
        assert_eq!(
            parse("DISCARD PLANS"),
            Statement::Discard {
                target: DiscardTarget::Plans
            }
        );
        assert_eq!(
            parse("DISCARD SEQUENCES"),
            Statement::Discard {
                target: DiscardTarget::Sequences
            }
        );
        assert_eq!(
            parse("DISCARD TEMPORARY"),
            Statement::Discard {
                target: DiscardTarget::Temp
            }
        );
        assert_eq!(
            parse("DISCARD TEMP"),
            Statement::Discard {
                target: DiscardTarget::Temp
            }
        );
    }

    #[test]
    fn discard_malformed_is_other() {
        assert_eq!(parse("DISCARD"), Statement::Other);
        assert_eq!(parse("DISCARD FOO"), Statement::Other);
    }

    // ---- DEALLOCATE ------------------------------------------------------

    #[test]
    fn deallocate_named() {
        assert_eq!(
            parse("DEALLOCATE my_stmt"),
            Statement::Deallocate {
                target: DeallocateTarget::Name("my_stmt".into())
            }
        );
    }

    #[test]
    fn deallocate_all() {
        assert_eq!(
            parse("DEALLOCATE ALL"),
            Statement::Deallocate {
                target: DeallocateTarget::All
            }
        );
    }

    #[test]
    fn deallocate_prepare_noise_keyword() {
        // The optional `PREPARE` keyword is part of the grammar but
        // contributes no semantics; both forms collapse to the same
        // variant.
        assert_eq!(
            parse("DEALLOCATE PREPARE my_stmt"),
            Statement::Deallocate {
                target: DeallocateTarget::Name("my_stmt".into())
            }
        );
        assert_eq!(
            parse("DEALLOCATE PREPARE ALL"),
            Statement::Deallocate {
                target: DeallocateTarget::All
            }
        );
    }

    #[test]
    fn deallocate_mixed_case() {
        assert_eq!(
            parse("Deallocate Prepare My_Stmt"),
            Statement::Deallocate {
                target: DeallocateTarget::Name("my_stmt".into())
            }
        );
        assert_eq!(
            parse("deallocate all"),
            Statement::Deallocate {
                target: DeallocateTarget::All
            }
        );
    }

    #[test]
    fn deallocate_quoted_identifier_preserves_case() {
        assert_eq!(
            parse(r#"DEALLOCATE "MyStmt""#),
            Statement::Deallocate {
                target: DeallocateTarget::Name("MyStmt".into())
            }
        );
        assert_eq!(
            parse(r#"DEALLOCATE PREPARE "MyStmt""#),
            Statement::Deallocate {
                target: DeallocateTarget::Name("MyStmt".into())
            }
        );
    }

    /// A quoted `"ALL"` is a literal identifier, not the wildcard
    /// keyword — mirrors the `UNLISTEN "*"` case. PostgreSQL itself
    /// treats the unquoted `ALL` as a reserved keyword in this
    /// position.
    #[test]
    fn deallocate_quoted_all_is_name_not_wildcard() {
        assert_eq!(
            parse(r#"DEALLOCATE "ALL""#),
            Statement::Deallocate {
                target: DeallocateTarget::Name("ALL".into())
            }
        );
    }

    #[test]
    fn deallocate_malformed_is_other() {
        // Bare DEALLOCATE — no target.
        assert_eq!(parse("DEALLOCATE"), Statement::Other);
        // DEALLOCATE PREPARE without a target.
        assert_eq!(parse("DEALLOCATE PREPARE"), Statement::Other);
    }

    // ---- BEGIN / START ---------------------------------------------------

    #[test]
    fn begin_plain() {
        assert_eq!(
            parse("BEGIN"),
            Statement::Begin {
                options: TransactionOptions::default()
            }
        );
    }

    #[test]
    fn begin_read_only() {
        assert_eq!(
            parse("BEGIN READ ONLY"),
            Statement::Begin {
                options: TransactionOptions {
                    read_only: Some(true)
                }
            }
        );
    }

    #[test]
    fn begin_read_write() {
        assert_eq!(
            parse("BEGIN READ WRITE"),
            Statement::Begin {
                options: TransactionOptions {
                    read_only: Some(false)
                }
            }
        );
    }

    #[test]
    fn begin_with_isolation_before_read_only() {
        // The `READ` inside `REPEATABLE READ` must not break the scan
        // — the later `READ ONLY` is still picked up.
        assert_eq!(
            parse("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"),
            Statement::Begin {
                options: TransactionOptions {
                    read_only: Some(true)
                }
            }
        );
    }

    #[test]
    fn begin_isolation_read_committed_no_mode() {
        // `READ COMMITTED` has no effect on the read_only flag.
        assert_eq!(
            parse("BEGIN ISOLATION LEVEL READ COMMITTED"),
            Statement::Begin {
                options: TransactionOptions { read_only: None }
            }
        );
    }

    #[test]
    fn start_transaction_read_only() {
        assert_eq!(
            parse("START TRANSACTION READ ONLY"),
            Statement::Begin {
                options: TransactionOptions {
                    read_only: Some(true)
                }
            }
        );
    }

    /// Regression guard: `START` without a following `TRANSACTION`
    /// must parse as `Other`, not `Begin`. Misclassifying it as
    /// `Begin` lets the router send `START READ ONLY` to a replica
    /// where PostgreSQL rejects it with a syntax error the client
    /// doesn't expect.
    #[test]
    fn start_without_transaction_is_other() {
        assert_eq!(parse("START"), Statement::Other);
        assert_eq!(parse("START WORK"), Statement::Other);
        assert_eq!(parse("START READ ONLY"), Statement::Other);
        assert_eq!(parse("START WORK READ ONLY"), Statement::Other);
    }

    // ---- COMMIT / ROLLBACK -----------------------------------------------

    #[test]
    fn commit_aliases() {
        assert_eq!(parse("COMMIT"), Statement::Commit);
        assert_eq!(parse("END"), Statement::Commit);
    }

    #[test]
    fn rollback_aliases() {
        assert_eq!(parse("ROLLBACK"), Statement::Rollback);
        assert_eq!(parse("ABORT"), Statement::Rollback);
    }

    // ---- DML -------------------------------------------------------------

    #[test]
    fn select_variants() {
        assert_eq!(parse("SELECT 1"), Statement::Select);
        assert_eq!(parse("TABLE t"), Statement::Select);
        assert_eq!(parse("VALUES (1)"), Statement::Select);
    }

    #[test]
    fn dml_variants() {
        assert_eq!(parse("INSERT INTO t VALUES (1)"), Statement::Insert);
        assert_eq!(parse("UPDATE t SET x = 1"), Statement::Update);
        assert_eq!(parse("DELETE FROM t"), Statement::Delete);
        assert_eq!(parse("TRUNCATE t"), Statement::Delete);
        assert_eq!(parse("COPY t FROM stdin"), Statement::Copy);
    }

    // ---- WITH CTE --------------------------------------------------------

    #[test]
    fn with_cte_select() {
        assert_eq!(
            parse("WITH cte AS (SELECT 1) SELECT * FROM cte"),
            Statement::Select
        );
    }

    #[test]
    fn with_cte_insert() {
        assert_eq!(
            parse("WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte"),
            Statement::Insert
        );
    }

    #[test]
    fn with_recursive() {
        assert_eq!(
            parse("WITH RECURSIVE tree AS (SELECT 1) SELECT * FROM tree"),
            Statement::Select
        );
    }

    // ---- Degenerate ------------------------------------------------------

    #[test]
    fn empty_input() {
        assert_eq!(parse(""), Statement::Other);
        assert_eq!(parse("   "), Statement::Other);
    }

    #[test]
    fn only_comment() {
        assert_eq!(parse("-- just a comment"), Statement::Other);
    }

    #[test]
    fn ddl_is_other() {
        assert_eq!(parse("CREATE TABLE t (id int)"), Statement::Other);
    }

    // ---- is_read_only / is_read_write_override on the AST ---------------

    #[test]
    fn is_read_only_pattern() {
        assert!(matches!(
            parse("BEGIN READ ONLY"),
            Statement::Begin {
                options: TransactionOptions {
                    read_only: Some(true)
                }
            }
        ));
        assert!(!matches!(
            parse("BEGIN"),
            Statement::Begin {
                options: TransactionOptions {
                    read_only: Some(true)
                }
            }
        ));
    }

    #[test]
    fn is_read_write_override_covers_all_paths() {
        assert!(parse("BEGIN READ WRITE").is_read_write_override());
        assert!(parse("SET TRANSACTION READ WRITE").is_read_write_override());
        assert!(
            parse("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE").is_read_write_override()
        );
        assert!(parse("SET default_transaction_read_only = off").is_read_write_override());
        assert!(parse("SET default_transaction_read_only = 'off'").is_read_write_override());
        assert!(parse("SET default_transaction_read_only TO DEFAULT").is_read_write_override());

        assert!(!parse("BEGIN READ ONLY").is_read_write_override());
        assert!(!parse("BEGIN").is_read_write_override());
        assert!(!parse("SET default_transaction_read_only = on").is_read_write_override());
        assert!(!parse("SET work_mem = '1MB'").is_read_write_override());
        assert!(!parse("SELECT 1").is_read_write_override());
    }

    /// Regression: `RESET default_transaction_read_only` is
    /// semantically identical to `SET ... TO DEFAULT` (both revert
    /// the GUC to the compiled default, which is `off` / read-write)
    /// and must be flagged as a read-write override so the router
    /// rejects it on replica-routed transactions. Without this, a
    /// client could issue RESET to silently flip the connection back
    /// to read-write and leak that state to the next pool user.
    #[test]
    fn reset_default_transaction_read_only_is_override() {
        assert!(parse("RESET default_transaction_read_only").is_read_write_override());
        assert!(parse("reset DEFAULT_TRANSACTION_READ_ONLY").is_read_write_override());
    }

    /// Regression: `RESET ALL` sweeps every session setting including
    /// `default_transaction_read_only`, so it has the same effect as
    /// the targeted RESET above and must be rejected the same way.
    #[test]
    fn reset_all_is_override() {
        assert!(parse("RESET ALL").is_read_write_override());
        assert!(parse("reset all").is_read_write_override());
    }

    /// Narrow check: a RESET of an unrelated GUC is NOT an override.
    /// Only `default_transaction_read_only` and `ALL` matter.
    #[test]
    fn reset_other_parameter_is_not_override() {
        assert!(!parse("RESET work_mem").is_read_write_override());
        assert!(!parse("RESET search_path").is_read_write_override());
    }
}
