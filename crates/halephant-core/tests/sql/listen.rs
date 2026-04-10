use halephant_core::sql::{Statement, parse};

#[test]
fn listen_simple() {
    assert_eq!(
        parse("LISTEN my_channel"),
        Statement::Listen {
            channel: "my_channel".into()
        }
    );
    assert_eq!(
        parse("listen events"),
        Statement::Listen {
            channel: "events".into()
        }
    );
    assert_eq!(
        parse("Listen My_Channel_123"),
        Statement::Listen {
            channel: "my_channel_123".into()
        }
    );
    assert_eq!(
        parse("LISTEN ch;"),
        Statement::Listen {
            channel: "ch".into()
        }
    );
}

#[test]
fn listen_quoted_channel() {
    assert_eq!(
        parse(r#"LISTEN "my-channel""#),
        Statement::Listen {
            channel: "my-channel".into()
        }
    );
    // Case preserved for quoted identifiers.
    assert_eq!(
        parse(r#"LISTEN "MyChannel""#),
        Statement::Listen {
            channel: "MyChannel".into()
        }
    );
    // Escaped double quotes.
    assert_eq!(
        parse(r#"LISTEN "has""quote""#),
        Statement::Listen {
            channel: r#"has"quote"#.into()
        }
    );
}

#[test]
fn listen_in_line_comment() {
    assert_eq!(parse("-- LISTEN my_channel\nSELECT 1"), Statement::Select);
}

#[test]
fn listen_in_block_comment() {
    assert_eq!(parse("/* LISTEN my_channel */ SELECT 1"), Statement::Select);
}

#[test]
fn listen_in_nested_block_comment() {
    assert_eq!(
        parse("/* outer /* LISTEN */ still comment */ SELECT 1"),
        Statement::Select
    );
}

#[test]
fn listen_in_string_literal() {
    assert_eq!(parse("SELECT 'LISTEN my_channel'"), Statement::Select);
}

#[test]
fn listen_in_dollar_quoted_string() {
    assert_eq!(
        parse("SELECT $$LISTEN my_channel$$ AS x"),
        Statement::Select
    );
}

#[test]
fn listen_in_tagged_dollar_string() {
    assert_eq!(
        parse("SELECT $body$LISTEN my_channel$body$ AS x"),
        Statement::Select
    );
}

#[test]
fn comments_before_listen() {
    assert_eq!(
        parse("-- setup\n/* prep */ LISTEN events"),
        Statement::Listen {
            channel: "events".into()
        }
    );
}

#[test]
fn mixed_case_listen() {
    assert_eq!(
        parse("Listen My_Channel"),
        Statement::Listen {
            channel: "my_channel".into()
        }
    );
    assert_eq!(
        parse("LISTEN EVENTS"),
        Statement::Listen {
            channel: "events".into()
        }
    );
}
