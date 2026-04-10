//! Escaping helpers for the libpq `options` startup parameter.

/// Escape a GUC value for inclusion in the libpq `options` startup
/// parameter. PostgreSQL's server-side `split_opts()` tokenizes the
/// string on whitespace, treating `\` as the only escape character
/// (see `src/backend/postmaster/postmaster.c`). Any whitespace or
/// backslash in the value is therefore prefixed with `\` so values
/// like `search_path=my schema,public` survive the split intact.
///
/// Note that this is *not* the same as `postgresql.conf` quoting —
/// single quotes have no special meaning in the `options` parameter
/// and are passed through verbatim.
pub(crate) fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch == '\\' || ch.is_ascii_whitespace() {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::escape;

    #[test]
    fn unescaped_values_pass_through() {
        assert_eq!(escape("public"), "public");
        assert_eq!(escape("30s"), "30s");
        assert_eq!(escape("public,reporting"), "public,reporting");
    }

    #[test]
    fn spaces_are_backslash_escaped() {
        assert_eq!(escape("my schema"), r"my\ schema");
        assert_eq!(
            escape("my schema, other schema"),
            r"my\ schema,\ other\ schema"
        );
    }

    #[test]
    fn tabs_and_newlines_are_escaped() {
        assert_eq!(escape("a\tb"), "a\\\tb");
        assert_eq!(escape("a\nb"), "a\\\nb");
    }

    #[test]
    fn backslashes_are_doubled() {
        assert_eq!(escape(r"a\b"), r"a\\b");
        // A backslash immediately before a space — verifies we don't
        // accidentally double-escape the space with the already-added
        // backslash for the preceding `\`.
        assert_eq!(escape(r"a\ b"), r"a\\\ b");
    }

    #[test]
    fn empty_value_stays_empty() {
        assert_eq!(escape(""), "");
    }
}
