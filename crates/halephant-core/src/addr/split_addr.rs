// ---------------------------------------------------------------------------
// Address parsing
// ---------------------------------------------------------------------------

/// Split a PostgreSQL-style address into `(host, port)`. Handles:
/// - `hostname:5432` — simple host and port
/// - `127.0.0.1:5432` — IPv4 and port
/// - `[::1]:5432` — bracketed IPv6 and port
/// - `[::1]` — bracketed IPv6, default port
/// - `hostname` — no port, default to `"5432"`
///
/// Brackets are stripped from the returned host so the value matches `.pgpass`
/// entries.
pub fn split_host_port(addr: &str) -> (&str, &str) {
    if let Some(rest) = addr.strip_prefix('[') {
        // Bracketed IPv6: [host]:port or [host]
        if let Some((host, port)) = rest.split_once("]:") {
            (host, port)
        } else {
            (rest.trim_end_matches(']'), "5432")
        }
    } else if let Some(colon) = addr.rfind(':') {
        // Only split on the last colon if there's exactly one (otherwise it's
        // a bare IPv6 literal like "::1").
        if addr[..colon].contains(':') {
            // Multiple colons without brackets — bare IPv6, no port.
            (addr, "5432")
        } else {
            (&addr[..colon], &addr[colon + 1..])
        }
    } else {
        (addr, "5432")
    }
}

#[cfg(test)]
mod tests {
    use super::split_host_port;

    #[test]
    fn simple_host_port() {
        assert_eq!(split_host_port("localhost:5432"), ("localhost", "5432"));
    }

    #[test]
    fn ipv4_port() {
        assert_eq!(split_host_port("127.0.0.1:5433"), ("127.0.0.1", "5433"));
    }

    #[test]
    fn bracketed_ipv6_port() {
        assert_eq!(split_host_port("[::1]:5432"), ("::1", "5432"));
    }

    #[test]
    fn bracketed_ipv6_no_port() {
        assert_eq!(split_host_port("[::1]"), ("::1", "5432"));
    }

    #[test]
    fn bare_ipv6_no_port() {
        assert_eq!(split_host_port("::1"), ("::1", "5432"));
    }

    #[test]
    fn hostname_no_port() {
        assert_eq!(
            split_host_port("pg-primary.internal"),
            ("pg-primary.internal", "5432")
        );
    }

    #[test]
    fn custom_port() {
        assert_eq!(
            split_host_port("db.example.com:6432"),
            ("db.example.com", "6432")
        );
    }

    #[test]
    fn full_ipv6_bracketed() {
        assert_eq!(
            split_host_port("[2001:db8::1]:5432"),
            ("2001:db8::1", "5432")
        );
    }
}
