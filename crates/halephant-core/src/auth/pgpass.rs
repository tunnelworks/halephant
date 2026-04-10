use std::path::Path;

/// Parsed `.pgpass` file entries for upstream password lookup.
pub struct Pgpass {
    entries: Vec<Entry>,
}

struct Entry {
    hostname: Pattern,
    port: Pattern,
    database: Pattern,
    username: Pattern,
    password: String,
}

enum Pattern {
    Any,
    Exact(String),
}

impl Pattern {
    fn parse(s: &str) -> Self {
        if s == "*" {
            Self::Any
        } else {
            Self::Exact(s.to_owned())
        }
    }

    fn matches(&self, value: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(v) => v == value,
        }
    }
}

impl Pgpass {
    /// Load and parse a `.pgpass` file. Returns an empty set on I/O errors
    /// (matching PostgreSQL behavior — a missing file is not fatal).
    pub fn load(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(content) => Self::parse(&content),
            Err(_) => Self {
                entries: Vec::new(),
            },
        }
    }

    /// Parse `.pgpass` content from a string.
    pub fn parse(content: &str) -> Self {
        // Trim each line before the blank/comment check so a
        // whitespace-only line like "   " is dropped here rather
        // than deep inside `parse_line`'s field-count check. Today
        // both paths happen to reject the line, but making the
        // intent explicit at the filter step means a future
        // tightening of `parse_line` (for example to return an
        // error instead of `None` on malformed input) will not
        // regress on whitespace handling.
        let entries = content
            .lines()
            .filter(|line| {
                let trimmed = line.trim();
                !trimmed.is_empty() && !trimmed.starts_with('#')
            })
            .filter_map(parse_line)
            .collect();
        Self { entries }
    }

    /// Look up a password for the given connection parameters. Returns the
    /// password from the first matching entry, or `None` if no entry matches.
    pub fn lookup(
        &self,
        hostname: &str,
        port: &str,
        database: &str,
        username: &str,
    ) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| {
                e.hostname.matches(hostname)
                    && e.port.matches(port)
                    && e.database.matches(database)
                    && e.username.matches(username)
            })
            .map(|e| e.password.as_str())
    }

    /// Look up a password by `host:port` address (or bracketed IPv6).
    /// Convenience wrapper around [`Pgpass::lookup`] that handles the
    /// host/port split so call sites don't repeat the same three-line
    /// dance.
    pub fn lookup_addr(&self, addr: &str, database: &str, username: &str) -> Option<&str> {
        let (host, port) = crate::addr::split_addr::split_host_port(addr);
        self.lookup(host, port, database, username)
    }

    /// Returns true if no entries are loaded.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Parse a single `.pgpass` line into an entry. The format is:
/// `hostname:port:database:username:password`
///
/// Backslash escapes `:` and `\` within fields. The password field is
/// everything after the fourth unescaped colon (including colons).
fn parse_line(line: &str) -> Option<Entry> {
    let mut fields = Vec::with_capacity(5);
    let mut current = String::new();
    let chars = line.chars();
    let mut escaped = false;

    for ch in chars {
        if escaped {
            current.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == ':' && fields.len() < 4 {
            fields.push(std::mem::take(&mut current));
        } else {
            current.push(ch);
        }
    }
    fields.push(current); // password (5th field)

    if fields.len() != 5 {
        return None;
    }

    Some(Entry {
        hostname: Pattern::parse(&fields[0]),
        port: Pattern::parse(&fields[1]),
        database: Pattern::parse(&fields[2]),
        username: Pattern::parse(&fields[3]),
        password: fields[4].clone(),
    })
}
