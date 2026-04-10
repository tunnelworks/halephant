//! Character-level SQL lexer.
//!
//! Built on chumsky combinators because the grammar at the character
//! level is genuinely gnarly — nested block comments, dollar-quoted
//! strings with arbitrary tags, single-quoted strings with doubled
//! escape sequences, line comments that run until end-of-line. A
//! hand-rolled scanner for any one of these is error-prone; chumsky's
//! combinator style keeps each piece declarative and composable.
//!
//! The token-level statement grammar is handled by `parser.rs`
//! instead — it's simple enough that a tiny recursive-descent walker
//! over `&[Token]` is clearer than a second chumsky parser layer.

use chumsky::prelude::*;

// ---------------------------------------------------------------------------
// Token
// ---------------------------------------------------------------------------

/// A lexer token.
///
/// Words and literals are case-folded to lowercase by the lexer so the
/// statement parser can compare against lowercase keywords directly.
/// Quoted identifiers preserve their case, matching PostgreSQL's
/// behaviour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Token {
    /// An unquoted keyword or identifier, lowercased.
    Word(String),
    /// A double-quoted identifier, stored without its surrounding
    /// quotes and with case preserved.
    QuotedIdent(String),
    /// A single-quoted string literal, stored without its surrounding
    /// quotes and lowercased. Doubled single quotes (`''`) are
    /// collapsed to a single literal quote.
    Literal(String),
    /// A numeric literal (digits only), stored verbatim.
    Number(String),
    /// A bare `*` character, used by `UNLISTEN *`.
    Star,
    /// A `(` grouping token, used by CTE body scanning.
    LParen,
    /// A `)` grouping token.
    RParen,
    /// Any single character the parser does not otherwise distinguish
    /// (punctuation, `;`, `,`, `=`, operators, dollar-quoted string
    /// bodies that have already been consumed). The parser tolerates
    /// these as separators between known tokens.
    Other,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Tokenise a SQL string. On lexer error or any other parse problem
/// returns an empty vec — the statement parser interprets that as
/// [`super::Statement::Other`] downstream, so no error path needs to
/// be surfaced to the caller.
pub(super) fn lex(sql: &str) -> Vec<Token> {
    sql_tokens().parse(sql).into_result().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Combinators
// ---------------------------------------------------------------------------

type E<'src> = extra::Err<Rich<'src, char>>;

/// Top-level parser: any number of tokens separated by noise (whitespace
/// and comments) on both sides.
fn sql_tokens<'src>() -> impl Parser<'src, &'src str, Vec<Token>, E<'src>> {
    let number = any()
        .filter(|c: &char| c.is_ascii_digit())
        .repeated()
        .at_least(1)
        .to_slice()
        .map(|s: &str| Token::Number(s.to_owned()));

    let token = choice((
        word(),
        double_quoted_ident(),
        number,
        just('*').to(Token::Star),
        just('(').to(Token::LParen),
        just(')').to(Token::RParen),
        single_quoted_string().map(Token::Literal),
        dollar_quoted_string().to(Token::Other),
        // Any other single character we don't care about — keeps the
        // parser stream-aligned even when unknown punctuation appears.
        any().to(Token::Other),
    ));

    noise().ignore_then(token.then_ignore(noise()).repeated().collect())
}

/// A SQL keyword or identifier — `[A-Za-z_][A-Za-z0-9_]*` — lowercased.
fn word<'src>() -> impl Parser<'src, &'src str, Token, E<'src>> + Clone {
    any()
        .filter(|c: &char| c.is_ascii_alphabetic() || *c == '_')
        .then(
            any()
                .filter(|c: &char| c.is_ascii_alphanumeric() || *c == '_')
                .repeated(),
        )
        .to_slice()
        .map(|s: &str| Token::Word(s.to_ascii_lowercase()))
}

/// `"..."` identifier with `""` escape sequences. Stored without the
/// surrounding quotes, with case preserved.
fn double_quoted_ident<'src>() -> impl Parser<'src, &'src str, Token, E<'src>> + Clone {
    just('"')
        .ignore_then(custom(|inp| {
            let mut name = String::new();
            loop {
                match inp.next() {
                    Some('"') => {
                        if inp.peek() == Some('"') {
                            inp.skip();
                            name.push('"'); // escaped ""
                        } else {
                            return Ok(name); // end of identifier
                        }
                    }
                    Some(c) => name.push(c),
                    None => return Ok(name), // unterminated — tolerate
                }
            }
        }))
        .map(Token::QuotedIdent)
}

// ---------------------------------------------------------------------------
// Noise: whitespace and comments (skipped between tokens)
// ---------------------------------------------------------------------------

/// Skip any amount of whitespace, line comments, and block comments.
fn noise<'src>() -> impl Parser<'src, &'src str, (), E<'src>> + Clone {
    choice((
        any()
            .filter(|c: &char| c.is_ascii_whitespace())
            .repeated()
            .at_least(1)
            .to(()),
        line_comment(),
        block_comment(),
    ))
    .repeated()
    .to(())
}

/// `-- ...` until end of line (or end of input).
fn line_comment<'src>() -> impl Parser<'src, &'src str, (), E<'src>> + Clone {
    just("--")
        .then(any().filter(|c: &char| *c != '\n').repeated())
        .to(())
}

/// `/* ... */` with nesting support. The opening `/*` is matched by a
/// combinator; the body (including nested comments) is scanned with
/// `custom` because chumsky's built-in combinators don't directly
/// express "balanced delimiters with arbitrary content".
fn block_comment<'src>() -> impl Parser<'src, &'src str, (), E<'src>> + Clone {
    just("/*")
        .ignore_then(custom(|inp| {
            let mut depth = 1u32;
            while depth > 0 {
                match inp.next() {
                    Some('/') => {
                        if inp.peek() == Some('*') {
                            inp.skip();
                            depth += 1;
                        }
                    }
                    Some('*') => {
                        if inp.peek() == Some('/') {
                            inp.skip();
                            depth -= 1;
                        }
                    }
                    Some(_) => {}
                    None => break, // unterminated — tolerate
                }
            }
            Ok(())
        }))
        .to(())
}

// ---------------------------------------------------------------------------
// String literals
// ---------------------------------------------------------------------------

/// `'...'` with `''` escape sequences. Content is lowercased — the
/// statement parser uses this to treat `'Off'` and `off` identically
/// when resolving boolean values.
fn single_quoted_string<'src>() -> impl Parser<'src, &'src str, String, E<'src>> + Clone {
    just('\'').ignore_then(custom(|inp| {
        let mut content = String::new();
        loop {
            match inp.next() {
                Some('\'') => {
                    if inp.peek() == Some('\'') {
                        inp.skip();
                        content.push('\'');
                    } else {
                        return Ok(content.to_ascii_lowercase());
                    }
                }
                Some(c) => content.push(c),
                None => return Ok(content.to_ascii_lowercase()),
            }
        }
    }))
}

/// `$$...$$` or `$tag$...$tag$`. The content is discarded — halephant
/// never needs to inspect the body of a dollar-quoted string, it only
/// needs to make sure the lexer advances past it so keywords inside
/// don't accidentally classify the statement.
fn dollar_quoted_string<'src>() -> impl Parser<'src, &'src str, (), E<'src>> + Clone {
    just('$').ignore_then(custom(|inp| {
        // Read the opening tag up to (and including) its trailing `$`.
        let mut tag = String::from("$");
        loop {
            match inp.next() {
                Some('$') => {
                    tag.push('$');
                    break;
                }
                Some(c) if c.is_ascii_alphanumeric() || c == '_' => {
                    tag.push(c);
                }
                // Not a valid dollar-quoted tag — treat the leading `$`
                // as punctuation and return without consuming more.
                _ => return Ok(()),
            }
        }

        // Scan the body for the matching closing tag using a single
        // `matched` index as the match state — no buffer, no
        // allocations, O(1) memory.
        //
        // A naive `buf.push(c) + buf.ends_with(&tag)` grows `buf`
        // unbounded, which is wasteful for large bodies (PL/pgSQL
        // function sources can be several MB). A byte-level ring
        // buffer would avoid that but has a subtle correctness
        // trap: comparing `c as u8` against tag bytes truncates the
        // low byte of multi-byte code points, and a non-ASCII char
        // whose low byte happens to match a tag byte would trigger
        // a false termination mid-body.
        //
        // Instead, we maintain a "how many tag bytes matched so far"
        // counter. Dollar-quote tags have the form `$[alnum_]*$` —
        // exactly one `$` at each end, alphanumeric interior — so
        // their KMP failure function collapses to "on mismatch, fall
        // back to state 0 and retry the current byte". That's an
        // `if`, not a loop. Non-ASCII chars can never be part of an
        // ASCII tag, so hitting one resets the match state directly.
        let tag_bytes = tag.as_bytes();
        let mut matched = 0usize;
        loop {
            // Explicit type annotation: chumsky's `inp.next()` is
            // generic over the input's token type, and the other
            // functions in this file disambiguate via a literal
            // `Some('c')` pattern. We don't have one here, so pin
            // the type manually.
            let next: Option<char> = inp.next();
            let Some(c) = next else {
                return Ok(()); // unterminated — tolerate
            };

            if !c.is_ascii() {
                // Non-ASCII can't be part of an ASCII-only tag, so
                // it always breaks any in-progress match.
                matched = 0;
                continue;
            }
            let b = c as u8;

            // On mismatch, fall back to state 0 and retry the current
            // byte. Handles cases like tag `$ab$` on content `$$ab$`,
            // where the second `$` fails at position 1 but is itself
            // a valid state-0 match that starts the real run.
            if matched > 0 && b != tag_bytes[matched] {
                matched = 0;
            }
            if b == tag_bytes[matched] {
                matched += 1;
                if matched == tag_bytes.len() {
                    return Ok(());
                }
            }
        }
    }))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn t(sql: &str) -> Vec<Token> {
        lex(sql)
    }

    #[test]
    fn words_are_lowercased() {
        assert_eq!(t("SELECT"), vec![Token::Word("select".into())]);
        assert_eq!(t("Begin"), vec![Token::Word("begin".into())]);
    }

    #[test]
    fn quoted_idents_preserve_case() {
        assert_eq!(
            t(r#""MyTable""#),
            vec![Token::QuotedIdent("MyTable".into())]
        );
        assert_eq!(
            t(r#""has""quote""#),
            vec![Token::QuotedIdent(r#"has"quote"#.into())]
        );
    }

    #[test]
    fn single_quoted_strings_are_lowercased() {
        assert_eq!(t("'Off'"), vec![Token::Literal("off".into())]);
        assert_eq!(t("'it''s ok'"), vec![Token::Literal("it's ok".into())]);
    }

    #[test]
    fn nested_block_comments_are_skipped() {
        assert_eq!(
            t("/* outer /* inner */ still */ SELECT"),
            vec![Token::Word("select".into())]
        );
    }

    #[test]
    fn dollar_quoted_strings_are_skipped() {
        // The entire $body$...$body$ block should be a single Other
        // token and leave nothing from the inner SELECT behind.
        assert_eq!(
            t("$body$SELECT 1$body$ WORD"),
            vec![Token::Other, Token::Word("word".into())]
        );
    }

    /// Regression: a Unicode character whose low byte coincides with
    /// a tag byte must NOT terminate the scan. `Ĥ` is U+0124 (low
    /// byte 0x24 = `$`) and `š` is U+0161 (low byte 0x61 = `a`);
    /// naive `c as u8` byte matching against tag `$a$` would find a
    /// false "terminator" at `Ĥš$` inside the body. The match state
    /// must either work at char granularity or skip non-ASCII chars.
    #[test]
    fn dollar_quoted_body_with_unicode_low_byte_collision() {
        // Tag `$a$`, body `Ĥš$XYZ$a$` — the `$a$` after `XYZ` is the
        // real terminator; the `Ĥš$` prefix must be skipped.
        assert_eq!(
            t("$a$Ĥš$XYZ$a$ WORD"),
            vec![Token::Other, Token::Word("word".into())]
        );
    }

    /// Regression: tag matcher handles "false start" prefixes. With
    /// tag `$ab$` and body `$ab$`, the inner state machine must not
    /// accidentally consume the opening `$` at state 1 when the next
    /// character is also `$` — it should fall back to state 0, match
    /// that `$` as the start of a fresh run, and succeed on the
    /// following `a`, `b`, `$`.
    #[test]
    fn dollar_quoted_tag_with_nested_dollar_prefix() {
        // Body `$$ab$` ends with the real tag `$ab$`; the initial `$`
        // is a decoy that must not break the match.
        assert_eq!(
            t("$ab$$$ab$ WORD"),
            vec![Token::Other, Token::Word("word".into())]
        );
    }

    #[test]
    fn unterminated_inputs_do_not_panic() {
        // These all exercise the `None => tolerate` branches.
        let _ = t("/* unterminated");
        let _ = t("'unterminated");
        let _ = t("$$unterminated");
        let _ = t(r#""unterminated"#);
    }
}
