use halephant_core::sql::{Statement, UnlistenTarget, parse};

#[test]
fn unlisten_simple() {
    assert_eq!(
        parse("UNLISTEN my_channel"),
        Statement::Unlisten {
            target: UnlistenTarget::Channel("my_channel".into())
        }
    );
    assert_eq!(
        parse("unlisten events"),
        Statement::Unlisten {
            target: UnlistenTarget::Channel("events".into())
        }
    );
    assert_eq!(
        parse("UNLISTEN *"),
        Statement::Unlisten {
            target: UnlistenTarget::Star
        }
    );
    assert_eq!(
        parse("unlisten *;"),
        Statement::Unlisten {
            target: UnlistenTarget::Star
        }
    );
    assert_eq!(
        parse("  UNLISTEN  *  "),
        Statement::Unlisten {
            target: UnlistenTarget::Star
        }
    );
    assert_eq!(
        parse("UNLISTEN *;"),
        Statement::Unlisten {
            target: UnlistenTarget::Star
        }
    );
}

#[test]
fn unlisten_quoted_channel() {
    assert_eq!(
        parse(r#"UNLISTEN "my-channel""#),
        Statement::Unlisten {
            target: UnlistenTarget::Channel("my-channel".into())
        }
    );
}

/// A double-quoted `"*"` is a literal identifier named `*`, not the
/// wildcard.
#[test]
fn unlisten_quoted_star_is_channel_not_wildcard() {
    assert_eq!(
        parse(r#"UNLISTEN "*""#),
        Statement::Unlisten {
            target: UnlistenTarget::Channel("*".into())
        }
    );
}

#[test]
fn mixed_case_unlisten() {
    assert_eq!(
        parse("Unlisten *"),
        Statement::Unlisten {
            target: UnlistenTarget::Star
        }
    );
    assert_eq!(
        parse("UNLISTEN my_chan"),
        Statement::Unlisten {
            target: UnlistenTarget::Channel("my_chan".into())
        }
    );
}

#[test]
fn excessive_whitespace_between_tokens() {
    assert_eq!(
        parse("UNLISTEN   \t\n   *"),
        Statement::Unlisten {
            target: UnlistenTarget::Star
        }
    );
}
