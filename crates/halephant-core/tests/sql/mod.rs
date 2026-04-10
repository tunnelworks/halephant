mod listen;
mod unlisten;

use halephant_core::sql::{Statement, parse};

// ---------------------------------------------------------------------------
// Basic statement classification
// ---------------------------------------------------------------------------

#[test]
fn select_variants() {
    assert_eq!(parse("SELECT 1"), Statement::Select);
    assert_eq!(parse("select * from t"), Statement::Select);
    assert_eq!(parse("SELECT DISTINCT x FROM t"), Statement::Select);
    assert_eq!(parse("TABLE my_table"), Statement::Select);
    assert_eq!(parse("VALUES (1, 2), (3, 4)"), Statement::Select);
}

#[test]
fn insert_variants() {
    assert_eq!(parse("INSERT INTO t VALUES (1)"), Statement::Insert);
    assert_eq!(
        parse("INSERT INTO t (a, b) SELECT * FROM s"),
        Statement::Insert
    );
}

#[test]
fn update_variants() {
    assert_eq!(parse("UPDATE t SET x = 1"), Statement::Update);
    assert_eq!(parse("UPDATE t SET x = 1 WHERE id = 5"), Statement::Update);
}

#[test]
fn delete_and_truncate() {
    assert_eq!(parse("DELETE FROM t"), Statement::Delete);
    assert_eq!(parse("DELETE FROM t WHERE id = 1"), Statement::Delete);
    assert_eq!(parse("TRUNCATE t"), Statement::Delete);
    assert_eq!(parse("TRUNCATE TABLE t CASCADE"), Statement::Delete);
}

#[test]
fn transaction_control() {
    use halephant_core::sql::TransactionOptions;

    assert_eq!(
        parse("BEGIN"),
        Statement::Begin {
            options: TransactionOptions::default()
        }
    );
    assert_eq!(
        parse("begin"),
        Statement::Begin {
            options: TransactionOptions::default()
        }
    );
    assert_eq!(
        parse("BEGIN TRANSACTION"),
        Statement::Begin {
            options: TransactionOptions::default()
        }
    );
    assert_eq!(
        parse("BEGIN ISOLATION LEVEL SERIALIZABLE"),
        Statement::Begin {
            options: TransactionOptions::default()
        }
    );
    assert_eq!(
        parse("START TRANSACTION"),
        Statement::Begin {
            options: TransactionOptions::default()
        }
    );
    assert_eq!(parse("COMMIT"), Statement::Commit);
    assert_eq!(parse("END"), Statement::Commit);
    assert_eq!(parse("ROLLBACK"), Statement::Rollback);
    assert_eq!(parse("ABORT"), Statement::Rollback);
    assert_eq!(parse("ROLLBACK TO SAVEPOINT sp1"), Statement::Rollback);
}

#[test]
fn session_commands() {
    use halephant_core::sql::{ResetTarget, SetScope, SetValue};

    assert_eq!(
        parse("SET search_path TO public"),
        Statement::Set {
            scope: SetScope::Session,
            parameter: "search_path".into(),
            value: SetValue::Literal("public".into()),
        }
    );
    assert_eq!(
        parse("SET LOCAL timezone = 'UTC'"),
        Statement::Set {
            scope: SetScope::Local,
            parameter: "timezone".into(),
            value: SetValue::Literal("utc".into()),
        }
    );
    assert_eq!(
        parse("RESET ALL"),
        Statement::Reset {
            target: ResetTarget::All
        }
    );
    assert_eq!(
        parse("RESET search_path"),
        Statement::Reset {
            target: ResetTarget::Parameter("search_path".into())
        }
    );
    assert_eq!(
        parse("DISCARD ALL"),
        Statement::Discard {
            target: halephant_core::sql::DiscardTarget::All
        }
    );
    assert_eq!(
        parse("DISCARD PLANS"),
        Statement::Discard {
            target: halephant_core::sql::DiscardTarget::Plans
        }
    );
}

#[test]
fn copy_command() {
    assert_eq!(parse("COPY t FROM STDIN"), Statement::Copy);
    assert_eq!(parse("COPY t TO STDOUT"), Statement::Copy);
    assert_eq!(
        parse("COPY t (a, b) FROM '/tmp/data.csv' WITH CSV"),
        Statement::Copy
    );
}

#[test]
fn ddl_and_other() {
    assert_eq!(parse("CREATE TABLE t (id int)"), Statement::Other);
    assert_eq!(parse("ALTER TABLE t ADD COLUMN x int"), Statement::Other);
    assert_eq!(parse("DROP TABLE t"), Statement::Other);
    assert_eq!(parse("CREATE INDEX idx ON t (x)"), Statement::Other);
    assert_eq!(parse("GRANT SELECT ON t TO reader"), Statement::Other);
    assert_eq!(parse("REVOKE ALL ON t FROM public"), Statement::Other);
    assert_eq!(parse("VACUUM t"), Statement::Other);
    assert_eq!(parse("ANALYZE t"), Statement::Other);
    assert_eq!(parse("EXPLAIN SELECT 1"), Statement::Other);
    assert_eq!(parse("DO $$ BEGIN NULL; END $$"), Statement::Other);
}

// ---------------------------------------------------------------------------
// Case insensitivity
// ---------------------------------------------------------------------------

#[test]
fn mixed_case() {
    use halephant_core::sql::TransactionOptions;

    assert_eq!(parse("Select 1"), Statement::Select);
    assert_eq!(parse("INSERT into t values (1)"), Statement::Insert);
    assert_eq!(
        parse("Begin Transaction"),
        Statement::Begin {
            options: TransactionOptions::default()
        }
    );
}

// ---------------------------------------------------------------------------
// WITH CTEs
// ---------------------------------------------------------------------------

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
fn with_cte_update() {
    assert_eq!(
        parse("WITH cte AS (SELECT 1) UPDATE t SET x = cte.v FROM cte"),
        Statement::Update
    );
}

#[test]
fn with_cte_delete() {
    assert_eq!(
        parse("WITH cte AS (SELECT 1) DELETE FROM t WHERE id IN (SELECT * FROM cte)"),
        Statement::Delete
    );
}

#[test]
fn with_recursive_cte() {
    assert_eq!(
        parse("WITH RECURSIVE tree AS (SELECT 1 UNION ALL SELECT 1) SELECT * FROM tree"),
        Statement::Select
    );
}

#[test]
fn with_multiple_ctes() {
    assert_eq!(
        parse("WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b"),
        Statement::Select
    );
}

#[test]
fn with_nested_parens() {
    assert_eq!(
        parse("WITH cte AS (SELECT (1 + (2 * 3))) SELECT * FROM cte"),
        Statement::Select
    );
}

// ---------------------------------------------------------------------------
// Comments — MUST NOT affect classification
// ---------------------------------------------------------------------------

#[test]
fn line_comment_before_statement() {
    assert_eq!(parse("-- this is a comment\nSELECT 1"), Statement::Select);
}

#[test]
fn line_comment_with_keyword() {
    assert_eq!(parse("-- LISTEN my_channel\nSELECT 1"), Statement::Select);
    assert_eq!(parse("-- INSERT INTO t\nDELETE FROM t"), Statement::Delete);
}

#[test]
fn block_comment_before_statement() {
    assert_eq!(parse("/* comment */ SELECT 1"), Statement::Select);
}

#[test]
fn block_comment_with_keyword() {
    assert_eq!(parse("/* LISTEN channel */ SELECT 1"), Statement::Select);
    assert_eq!(
        parse("/* DELETE FROM t */ INSERT INTO t VALUES (1)"),
        Statement::Insert
    );
}

#[test]
fn nested_block_comments() {
    assert_eq!(
        parse("/* outer /* LISTEN */ still comment */ SELECT 1"),
        Statement::Select
    );
    assert_eq!(
        parse("/* /* /* deeply nested */ */ */ INSERT INTO t VALUES (1)"),
        Statement::Insert
    );
}

#[test]
fn multiple_comments() {
    assert_eq!(
        parse("-- first\n/* second */\n-- third\nSELECT 1"),
        Statement::Select
    );
}

#[test]
fn comment_between_tokens() {
    // WITH /* surprise! */ cte — the comment between tokens should be skipped.
    assert_eq!(
        parse("WITH /* comment */ cte AS (SELECT 1) SELECT * FROM cte"),
        Statement::Select
    );
}

// ---------------------------------------------------------------------------
// String literals
// ---------------------------------------------------------------------------

#[test]
fn keyword_in_single_quoted_string() {
    assert_eq!(parse("SELECT 'LISTEN my_channel'"), Statement::Select);
    assert_eq!(parse("SELECT 'DELETE FROM t'"), Statement::Select);
}

#[test]
fn escaped_quotes_in_string() {
    assert_eq!(parse("SELECT 'it''s a LISTEN test'"), Statement::Select);
    assert_eq!(parse("SELECT 'can''t DELETE'"), Statement::Select);
}

#[test]
fn keyword_in_dollar_quoted_string() {
    assert_eq!(parse("SELECT $$LISTEN my_channel$$"), Statement::Select);
    assert_eq!(
        parse("SELECT $$INSERT INTO t VALUES (1)$$"),
        Statement::Select
    );
}

#[test]
fn keyword_in_tagged_dollar_string() {
    assert_eq!(
        parse("SELECT $body$LISTEN my_channel$body$"),
        Statement::Select
    );
    assert_eq!(parse("SELECT $fn$DELETE FROM t$fn$"), Statement::Select);
}

#[test]
fn dollar_string_with_nested_quotes() {
    assert_eq!(
        parse("SELECT $$ it's a 'test' with LISTEN $$"),
        Statement::Select
    );
}

// ---------------------------------------------------------------------------
// Whitespace variations
// ---------------------------------------------------------------------------

#[test]
fn leading_whitespace() {
    assert_eq!(parse("   SELECT 1"), Statement::Select);
    assert_eq!(parse("\t\tSELECT 1"), Statement::Select);
    assert_eq!(parse("\n\nSELECT 1"), Statement::Select);
    assert_eq!(parse("\r\n  SELECT 1"), Statement::Select);
}

// ---------------------------------------------------------------------------
// Empty / degenerate input
// ---------------------------------------------------------------------------

#[test]
fn empty_string() {
    assert_eq!(parse(""), Statement::Other);
}

#[test]
fn only_whitespace() {
    assert_eq!(parse("   \t\n  "), Statement::Other);
}

#[test]
fn only_comments() {
    assert_eq!(parse("-- just a comment"), Statement::Other);
    assert_eq!(parse("/* block comment */"), Statement::Other);
    assert_eq!(parse("-- line\n/* block */\n-- another"), Statement::Other);
}

#[test]
fn only_punctuation() {
    assert_eq!(parse(";"), Statement::Other);
    assert_eq!(parse("()"), Statement::Other);
}

#[test]
fn only_string_literal() {
    assert_eq!(parse("'hello'"), Statement::Other);
    assert_eq!(parse("$$body$$"), Statement::Other);
}

// ---------------------------------------------------------------------------
// is_read_only
// ---------------------------------------------------------------------------

#[test]
fn read_only_classification() {
    use halephant_core::sql::{
        ResetTarget, SetScope, SetValue, TransactionOptions, UnlistenTarget,
    };

    assert!(Statement::Select.is_read_only());
    assert!(!Statement::Insert.is_read_only());
    assert!(!Statement::Update.is_read_only());
    assert!(!Statement::Delete.is_read_only());
    assert!(
        !Statement::Begin {
            options: TransactionOptions::default(),
        }
        .is_read_only()
    );
    assert!(!Statement::Commit.is_read_only());
    assert!(!Statement::Rollback.is_read_only());
    assert!(
        !Statement::Set {
            scope: SetScope::Session,
            parameter: "x".into(),
            value: SetValue::Literal("y".into()),
        }
        .is_read_only()
    );
    assert!(!Statement::Copy.is_read_only());
    assert!(
        !Statement::Listen {
            channel: String::new(),
        }
        .is_read_only()
    );
    assert!(
        !Statement::Unlisten {
            target: UnlistenTarget::Star,
        }
        .is_read_only()
    );
    assert!(
        !Statement::Reset {
            target: ResetTarget::All,
        }
        .is_read_only()
    );
    assert!(
        !Statement::Discard {
            target: halephant_core::sql::DiscardTarget::All,
        }
        .is_read_only()
    );
    assert!(!Statement::Other.is_read_only());
}

// ---------------------------------------------------------------------------
// Adversarial / tricky inputs
// ---------------------------------------------------------------------------

#[test]
fn select_star() {
    assert_eq!(parse("SELECT *"), Statement::Select);
    assert_eq!(parse("SELECT * FROM t"), Statement::Select);
}

#[test]
fn keyword_as_identifier_prefix() {
    // "LISTENER" starts with "LISTEN" but is a different word.
    assert_eq!(parse("LISTENER"), Statement::Other);
    assert_eq!(parse("SELECTED"), Statement::Other);
    assert_eq!(parse("INSERTED"), Statement::Other);
    assert_eq!(parse("UPDATED"), Statement::Other);
    assert_eq!(parse("DELETING"), Statement::Other);
    assert_eq!(parse("BEGINNING"), Statement::Other);
    assert_eq!(parse("COMMITTED"), Statement::Other);
    assert_eq!(parse("SETTINGS"), Statement::Other);
}

#[test]
fn keyword_with_underscore_suffix() {
    assert_eq!(parse("SELECT_ALL FROM t"), Statement::Other);
    assert_eq!(parse("LISTEN_CHANNEL x"), Statement::Other);
}

#[test]
fn semicolons_do_not_affect_classification() {
    use halephant_core::sql::TransactionOptions;

    assert_eq!(parse("SELECT 1;"), Statement::Select);
    assert_eq!(
        parse("BEGIN;"),
        Statement::Begin {
            options: TransactionOptions::default()
        }
    );
}

#[test]
fn multiple_statements_classified_by_first() {
    use halephant_core::sql::TransactionOptions;

    assert_eq!(
        parse("SELECT 1; INSERT INTO t VALUES (1)"),
        Statement::Select
    );
    assert_eq!(
        parse("BEGIN; SELECT 1; COMMIT;"),
        Statement::Begin {
            options: TransactionOptions::default()
        }
    );
}

#[test]
fn unterminated_string_does_not_crash() {
    assert_eq!(parse("SELECT 'unterminated"), Statement::Select);
    assert_eq!(parse("SELECT $$unterminated"), Statement::Select);
}

#[test]
fn unterminated_comment_does_not_crash() {
    assert_eq!(parse("/* unterminated comment"), Statement::Other);
    assert_eq!(parse("/* outer /* inner unterminated"), Statement::Other);
}

#[test]
fn very_long_comment_before_keyword() {
    let comment = format!("/* {} */ SELECT 1", "x".repeat(10_000));
    assert_eq!(parse(&comment), Statement::Select);
}

#[test]
fn very_long_string_before_second_statement() {
    let query = format!("SELECT '{}'", "a".repeat(10_000));
    assert_eq!(parse(&query), Statement::Select);
}
