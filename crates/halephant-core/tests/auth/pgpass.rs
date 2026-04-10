use halephant_core::auth::pgpass::Pgpass;

#[test]
fn basic_lookup() {
    let pgpass = Pgpass::parse("localhost:5432:mydb:myuser:secret");
    assert_eq!(
        pgpass.lookup("localhost", "5432", "mydb", "myuser"),
        Some("secret")
    );
    assert_eq!(pgpass.lookup("localhost", "5432", "mydb", "other"), None);
}

#[test]
fn wildcard_match() {
    let pgpass = Pgpass::parse("*:*:*:*:fallback");
    assert_eq!(pgpass.lookup("any", "1234", "any", "any"), Some("fallback"));
}

#[test]
fn first_match_wins() {
    let pgpass = Pgpass::parse("host:5432:db:user:specific\n*:*:*:*:fallback");
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user"),
        Some("specific")
    );
    assert_eq!(
        pgpass.lookup("other", "5432", "db", "user"),
        Some("fallback")
    );
}

#[test]
fn comments_and_empty_lines() {
    let pgpass = Pgpass::parse("# comment\n\nhost:5432:db:user:pass");
    assert_eq!(pgpass.lookup("host", "5432", "db", "user"), Some("pass"));
}

#[test]
fn escaped_colon_in_password() {
    let pgpass = Pgpass::parse(r"host:5432:db:user:pass\:word");
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user"),
        Some("pass:word")
    );
}

#[test]
fn escaped_backslash() {
    let pgpass = Pgpass::parse(r"host:5432:db:user:pass\\word");
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user"),
        Some(r"pass\word")
    );
}

#[test]
fn partial_wildcard() {
    let pgpass = Pgpass::parse("pg-primary:*:*:app:primarypass\npg-replica:*:*:app:replicapass");
    assert_eq!(
        pgpass.lookup("pg-primary", "5432", "mydb", "app"),
        Some("primarypass")
    );
    assert_eq!(
        pgpass.lookup("pg-replica", "5433", "other", "app"),
        Some("replicapass")
    );
}

#[test]
fn malformed_line_skipped() {
    let pgpass = Pgpass::parse("not:enough:fields\nhost:5432:db:user:pass");
    assert_eq!(pgpass.lookup("host", "5432", "db", "user"), Some("pass"));
}

#[test]
fn empty_file() {
    let pgpass = Pgpass::parse("");
    assert!(pgpass.is_empty());
    assert_eq!(pgpass.lookup("any", "any", "any", "any"), None);
}

#[test]
fn colon_in_password_unescaped() {
    // Password is everything after the 4th colon, including literal colons.
    let pgpass = Pgpass::parse("host:5432:db:user:pass:with:colons");
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user"),
        Some("pass:with:colons")
    );
}

// -----------------------------------------------------------------------
// Partial wildcard patterns (mixed exact and *)
// -----------------------------------------------------------------------

#[test]
fn ip_with_wildcard_port() {
    let pgpass = Pgpass::parse("127.0.0.1:*:*:myuser:pass");
    assert_eq!(
        pgpass.lookup("127.0.0.1", "5432", "any", "myuser"),
        Some("pass")
    );
    assert_eq!(
        pgpass.lookup("127.0.0.1", "5433", "other", "myuser"),
        Some("pass")
    );
    assert_eq!(pgpass.lookup("10.0.0.1", "5432", "any", "myuser"), None);
}

#[test]
fn wildcard_host_specific_user() {
    let pgpass = Pgpass::parse("*:5432:prod:deploy:deploypass");
    assert_eq!(
        pgpass.lookup("any-host", "5432", "prod", "deploy"),
        Some("deploypass")
    );
    assert_eq!(pgpass.lookup("any-host", "5432", "prod", "other"), None);
    assert_eq!(pgpass.lookup("any-host", "5433", "prod", "deploy"), None);
}

#[test]
fn wildcard_database_only() {
    let pgpass = Pgpass::parse("db.internal:5432:*:app:apppass");
    assert_eq!(
        pgpass.lookup("db.internal", "5432", "mydb", "app"),
        Some("apppass")
    );
    assert_eq!(
        pgpass.lookup("db.internal", "5432", "other", "app"),
        Some("apppass")
    );
    assert_eq!(pgpass.lookup("db.internal", "5432", "mydb", "root"), None);
}

// -----------------------------------------------------------------------
// Escaping edge cases
// -----------------------------------------------------------------------

#[test]
fn escaped_colon_in_hostname() {
    let pgpass = Pgpass::parse(r"host\:name:5432:db:user:pass");
    assert_eq!(
        pgpass.lookup("host:name", "5432", "db", "user"),
        Some("pass")
    );
}

#[test]
fn escaped_colon_in_username() {
    let pgpass = Pgpass::parse(r"host:5432:db:user\:name:pass");
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user:name"),
        Some("pass")
    );
}

#[test]
fn escaped_backslash_before_colon() {
    // \\: means a literal backslash followed by a field separator.
    let pgpass = Pgpass::parse(r"host:5432:db:user\\:pass");
    assert_eq!(pgpass.lookup("host", "5432", "db", r"user\"), Some("pass"));
}

#[test]
fn multiple_escapes_in_password() {
    let pgpass = Pgpass::parse(r"host:5432:db:user:p\:a\\s\:s");
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user"),
        Some(r"p:a\s:s")
    );
}

// -----------------------------------------------------------------------
// Negative / no-match cases
// -----------------------------------------------------------------------

#[test]
fn no_match_wrong_host() {
    let pgpass = Pgpass::parse("primary:5432:db:user:pass");
    assert_eq!(pgpass.lookup("replica", "5432", "db", "user"), None);
}

#[test]
fn no_match_wrong_port() {
    let pgpass = Pgpass::parse("host:5432:db:user:pass");
    assert_eq!(pgpass.lookup("host", "5433", "db", "user"), None);
}

#[test]
fn no_match_wrong_database() {
    let pgpass = Pgpass::parse("host:5432:prod:user:pass");
    assert_eq!(pgpass.lookup("host", "5432", "staging", "user"), None);
}

#[test]
fn no_match_wrong_user() {
    let pgpass = Pgpass::parse("host:5432:db:alice:pass");
    assert_eq!(pgpass.lookup("host", "5432", "db", "bob"), None);
}

#[test]
fn wildcard_does_not_apply_to_password() {
    // A literal `*` in the password field is the password, not a wildcard.
    let pgpass = Pgpass::parse("host:5432:db:user:*");
    assert_eq!(pgpass.lookup("host", "5432", "db", "user"), Some("*"));
}

// -----------------------------------------------------------------------
// Ordering and precedence
// -----------------------------------------------------------------------

#[test]
fn specific_before_wildcard() {
    let pgpass = Pgpass::parse(
        "host:5432:db:user:specific\n\
             host:5432:db:*:any_user\n\
             *:*:*:*:catchall",
    );
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user"),
        Some("specific")
    );
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "other"),
        Some("any_user")
    );
    assert_eq!(
        pgpass.lookup("elsewhere", "5432", "db", "user"),
        Some("catchall")
    );
}

// -----------------------------------------------------------------------
// Malformed input
// -----------------------------------------------------------------------

#[test]
fn too_few_fields() {
    let pgpass = Pgpass::parse("host:5432:db:user");
    assert!(pgpass.is_empty());
}

#[test]
fn only_comments() {
    let pgpass = Pgpass::parse("# just a comment\n# another");
    assert!(pgpass.is_empty());
}

#[test]
fn comment_after_valid_line_is_separate() {
    // A # at the start of a line is a comment; mid-line # is part of the field.
    let pgpass = Pgpass::parse("host:5432:db:user:pass#word");
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user"),
        Some("pass#word")
    );
}

#[test]
fn empty_password() {
    let pgpass = Pgpass::parse("host:5432:db:user:");
    assert_eq!(pgpass.lookup("host", "5432", "db", "user"), Some(""));
}

#[test]
fn whitespace_only_lines() {
    let pgpass = Pgpass::parse("   \n\t\nhost:5432:db:user:pass");
    assert_eq!(pgpass.lookup("host", "5432", "db", "user"), Some("pass"));
}

// -----------------------------------------------------------------------
// First-match semantics: wildcards vs. specific lines
// -----------------------------------------------------------------------

#[test]
fn wildcard_before_specific_selects_wildcard() {
    // If the wildcard comes first, it matches even though a more specific
    // line exists later. This is the documented behavior ("first line
    // that matches is used").
    let pgpass = Pgpass::parse(
        "*:*:*:*:wildcard\n\
             host:5432:db:user:specific",
    );
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user"),
        Some("wildcard")
    );
}

#[test]
fn specific_before_wildcard_selects_specific() {
    let pgpass = Pgpass::parse(
        "host:5432:db:user:specific\n\
             *:*:*:*:wildcard",
    );
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user"),
        Some("specific")
    );
    // Non-matching lookup falls through to the wildcard.
    assert_eq!(pgpass.lookup("other", "9999", "x", "y"), Some("wildcard"));
}

#[test]
fn partial_wildcard_before_full_wildcard() {
    let pgpass = Pgpass::parse(
        "host:*:*:*:host_match\n\
             *:*:*:*:catchall",
    );
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user"),
        Some("host_match")
    );
    assert_eq!(
        pgpass.lookup("other", "5432", "db", "user"),
        Some("catchall")
    );
}

#[test]
fn full_wildcard_before_partial_wildcard_shadows_it() {
    // The full wildcard matches everything, so the partial wildcard
    // below it is never reached.
    let pgpass = Pgpass::parse(
        "*:*:*:*:catchall\n\
             host:*:*:*:host_match",
    );
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user"),
        Some("catchall")
    );
}

#[test]
fn two_partial_wildcards_first_wins() {
    let pgpass = Pgpass::parse(
        "*:5432:*:*:port_match\n\
             host:*:*:*:host_match",
    );
    // Both match, but port_match comes first.
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "user"),
        Some("port_match")
    );
    // Only host_match matches here.
    assert_eq!(
        pgpass.lookup("host", "5433", "db", "user"),
        Some("host_match")
    );
}

#[test]
fn duplicate_entries_first_wins() {
    let pgpass = Pgpass::parse(
        "host:5432:db:user:first\n\
             host:5432:db:user:second",
    );
    assert_eq!(pgpass.lookup("host", "5432", "db", "user"), Some("first"));
}

#[test]
fn wildcard_user_then_specific_user() {
    let pgpass = Pgpass::parse(
        "host:5432:db:*:any_user_pass\n\
             host:5432:db:admin:admin_pass",
    );
    // admin matches the wildcard first.
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "admin"),
        Some("any_user_pass")
    );
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "other"),
        Some("any_user_pass")
    );
}

#[test]
fn specific_user_then_wildcard_user() {
    let pgpass = Pgpass::parse(
        "host:5432:db:admin:admin_pass\n\
             host:5432:db:*:any_user_pass",
    );
    // admin gets its own password; others fall through.
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "admin"),
        Some("admin_pass")
    );
    assert_eq!(
        pgpass.lookup("host", "5432", "db", "other"),
        Some("any_user_pass")
    );
}

#[test]
fn no_match_falls_through_all_wildcards() {
    // Wildcards on different fields — none match the lookup.
    let pgpass = Pgpass::parse(
        "host:*:*:*:host_only\n\
             *:5432:*:*:port_only",
    );
    assert_eq!(pgpass.lookup("other", "9999", "db", "user"), None);
}

// -----------------------------------------------------------------------
// IPv6 addresses
// -----------------------------------------------------------------------

#[test]
fn ipv6_loopback() {
    // Bare IPv6 in pgpass (no brackets — pgpass matches the host
    // parameter, which uses unbracketed addresses).
    let pgpass = Pgpass::parse(r"\:\:1:5432:db:user:pass");
    assert_eq!(pgpass.lookup("::1", "5432", "db", "user"), Some("pass"));
}

#[test]
fn ipv6_full_address() {
    let pgpass = Pgpass::parse(r"2001\:db8\:\:1:5432:*:*:v6pass");
    assert_eq!(
        pgpass.lookup("2001:db8::1", "5432", "mydb", "app"),
        Some("v6pass")
    );
}

#[test]
fn ipv6_wildcard_port() {
    let pgpass = Pgpass::parse(r"\:\:1:*:*:*:v6any");
    assert_eq!(pgpass.lookup("::1", "5433", "db", "user"), Some("v6any"));
}

#[test]
fn ipv6_no_match_wrong_host() {
    let pgpass = Pgpass::parse(r"\:\:1:5432:db:user:pass");
    assert_eq!(pgpass.lookup("::2", "5432", "db", "user"), None);
}

#[test]
fn ipv6_with_wildcard_fallback() {
    let pgpass = Pgpass::parse(
        "\\:\\:1:5432:db:user:v6pass\n\
             *:5432:db:user:fallback",
    );
    assert_eq!(pgpass.lookup("::1", "5432", "db", "user"), Some("v6pass"));
    assert_eq!(
        pgpass.lookup("192.168.1.1", "5432", "db", "user"),
        Some("fallback")
    );
}

#[test]
fn split_host_port_then_lookup() {
    // End-to-end: split a bracketed address (as stored in topology),
    // then look up in pgpass (which stores bare IPv6).
    let pgpass = Pgpass::parse(r"\:\:1:5432:*:*:v6pass");
    let (host, port) = halephant_core::addr::split_addr::split_host_port("[::1]:5432");
    assert_eq!(host, "::1");
    assert_eq!(pgpass.lookup(host, port, "mydb", "app"), Some("v6pass"));
}
