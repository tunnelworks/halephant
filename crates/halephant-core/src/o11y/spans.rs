use std::collections::{HashMap, VecDeque};

use tracing::{field, info_span};

use crate::config::otel::QueryText;
use crate::proto::frontend::FrontendMessage;

/// Tracks in-flight statement spans within a transaction. Each `Execute` or
/// `Query` message creates a span that is closed when the corresponding
/// `CommandComplete` arrives from the server.
pub(crate) struct StatementTracker {
    mode: QueryText,
    /// Query text keyed by statement name. The unnamed statement uses an empty
    /// string as the key.
    statements: HashMap<String, String>,
    /// Query text keyed by portal name, populated on `Bind`.
    portals: HashMap<String, String>,
    /// FIFO queue of spans awaiting their `CommandComplete`.
    pending: VecDeque<tracing::Span>,
}

impl StatementTracker {
    pub(crate) fn new(mode: QueryText) -> Self {
        Self {
            mode,
            statements: HashMap::new(),
            portals: HashMap::new(),
            pending: VecDeque::new(),
        }
    }

    /// Observe a client message and create statement spans as needed.
    ///
    /// `resolve_query` is called when a `Bind` references a named statement not
    /// seen in this transaction's `Parse` messages (cross-transaction reuse).
    /// It should return the query text for the given statement name, or `None`.
    pub(crate) fn on_client_msg(
        &mut self,
        msg: &FrontendMessage,
        resolve_query: impl FnOnce(&str) -> Option<String>,
    ) {
        match msg {
            FrontendMessage::Query(sql) => {
                let span = self.new_statement_span();
                record_query_attrs(&span, sql, false, self.mode);
                self.pending.push_back(span);
            }
            FrontendMessage::Parse(parse) if !parse.query.is_empty() => {
                self.statements
                    .insert(parse.name.clone(), parse.query.clone());
            }
            FrontendMessage::Bind(bind) => {
                let query = self.statements.get(&bind.statement).cloned().or_else(|| {
                    if bind.statement.is_empty() {
                        return None;
                    }
                    resolve_query(&bind.statement)
                });
                if let Some(q) = query {
                    self.portals.insert(bind.portal.clone(), q);
                }
            }
            FrontendMessage::Execute(exec) => {
                let span = self.new_statement_span();
                if let Some(query) = self.portals.get(&exec.portal) {
                    record_query_attrs(&span, query, true, self.mode);
                }
                self.pending.push_back(span);
            }
            _ => {}
        }
    }

    /// Create a new statement span and briefly enter it so the OTel layer
    /// records the correct start timestamp.
    #[allow(clippy::unused_self)]
    fn new_statement_span(&self) -> tracing::Span {
        let span = info_span!(
            "proxy.statement",
            db.query.summary = field::Empty,
            db.query.text = field::Empty,
            otel.status_code = field::Empty,
            otel.status_description = field::Empty,
        );
        // Enter and immediately exit: this anchors the OTel start timestamp
        // to the creation time rather than the close time.
        span.in_scope(|| {});
        span
    }

    /// Close the oldest pending statement span on `CommandComplete`.
    pub(crate) fn on_command_complete(&mut self, _tag: &str) {
        // Dropping the span records its end time and exports it.
        if let Some(span) = self.pending.pop_front() {
            span.record("otel.status_code", "OK");
        }
    }

    /// Close the oldest pending statement span with error status.
    pub(crate) fn on_error(&mut self, message: &str) {
        if let Some(span) = self.pending.pop_front() {
            span.record("otel.status_code", "ERROR");
            span.record("otel.status_description", message);
        }
    }

    /// Drain all remaining spans (for example, on `ReadyForQuery` after a
    /// pipeline error where some statements were skipped).
    pub(crate) fn drain(&mut self) {
        self.pending.clear();
    }
}

/// Record `db.query.summary` and (optionally) `db.query.text` on a span.
fn record_query_attrs(span: &tracing::Span, query: &str, is_parameterized: bool, mode: QueryText) {
    span.record("db.query.summary", truncate_str(query, 120));

    match mode {
        QueryText::Off => {}
        QueryText::Raw => {
            span.record("db.query.text", query);
        }
        QueryText::Sanitized => {
            if is_parameterized {
                span.record("db.query.text", query);
            } else {
                span.record(
                    "db.query.text",
                    sql_lexer::sanitize_string(query.to_owned()).as_str(),
                );
            }
        }
    }
}

/// Truncate a string at a UTF-8 boundary.
fn truncate_str(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len {
        return s;
    }
    let mut end = max_len;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}
