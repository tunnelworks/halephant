//! Connection reset — runs before returning a pooled connection to
//! the idle queue, cleaning up session state left behind by the
//! previous client.
//!
//! Only RESETs GUC variables the previous client explicitly SET
//! (tracked in [`super::server::ServerConn::dirty_vars`]); startup
//! parameters like `application_name` and `search_path` are
//! preserved across pool reuse. The [`BASE_RESET`] block runs on
//! every reset regardless of dirty-var state — it covers
//! non-GUC session state (cursors, LISTEN subscriptions, advisory
//! locks, sequence caches, temp tables) that the previous client
//! may have left behind.

use std::fmt::Write;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};

use crate::proto::backend::BackendMessage;
use crate::proto::frontend::FrontendMessage;
use crate::proto::types::TransactionStatus;

use super::server::ServerConn;

/// Base reset commands that clean up non-GUC session state without
/// destroying prepared statements or their cached plans.
///
/// Leads with `ROLLBACK;` so a connection handed back mid-transaction
/// (e.g. the client sent `Terminate` inside a `BEGIN` block) is
/// unwound before the rest of the reset runs — otherwise `CLOSE ALL`
/// and friends would execute *inside* the orphaned transaction and
/// the connection would return to the idle pool still in
/// `InTransaction` state, leaking the previous client's uncommitted
/// work into the next checkout. `ROLLBACK` outside a transaction
/// block emits a harmless `NOTICE` and is otherwise a no-op.
const BASE_RESET: &str = "\
    ROLLBACK;\
    CLOSE ALL;\
    UNLISTEN *;\
    SELECT pg_advisory_unlock_all();\
    DISCARD SEQUENCES;\
    DISCARD TEMP;\
";

/// Reset a pooled server connection, preparing it for a new client
/// session. Only RESETs GUC variables that were explicitly SET by
/// the previous client, preserving startup parameters
/// (application_name, search_path, etc.).
#[tracing::instrument(name = "pool.reset", skip_all, err(Display), fields(
    otel.status_code,
    otel.status_description,
))]
pub(crate) async fn reset_connection(conn: &mut ServerConn) -> anyhow::Result<()> {
    async {
        let mut query = String::new();

        // RESET only the variables the client explicitly SET.
        for var in conn.dirty_vars.drain() {
            let _ = write!(query, "RESET {var};");
        }

        query.push_str(BASE_RESET);

        conn.framed.send(FrontendMessage::Query(query)).await?;

        loop {
            match conn
                .framed
                .next()
                .await
                .transpose()
                .context("reading reset response")?
            {
                // Defence-in-depth against the orphaned-transaction
                // bug the leading `ROLLBACK` guards against: if the
                // reset returns to anything other than `Idle`, the
                // connection is tainted and must not go back to the
                // idle pool. Surfaces the failure as an error so
                // `reset_and_return` discards it instead of silently
                // leaking transaction state to the next checkout.
                Some(BackendMessage::ReadyForQuery(TransactionStatus::Idle)) => return Ok(()),
                Some(BackendMessage::ReadyForQuery(status)) => {
                    anyhow::bail!("reset left connection in non-idle state: {status:?}");
                }
                Some(BackendMessage::ErrorResponse(err)) => {
                    anyhow::bail!("reset failed: {}", err.message().unwrap_or("unknown"));
                }
                None => anyhow::bail!("upstream closed during reset"),
                // CommandComplete, RowDescription (pg_advisory_unlock_all()
                // returns a result set), DataRow, notices, etc.
                Some(_) => {}
            }
        }
    }
    .await
    .inspect(|()| {
        tracing::Span::current().record("otel.status_code", "OK");
    })
    .inspect_err(|e| {
        let span = tracing::Span::current();
        span.record("otel.status_code", "ERROR");
        span.record("otel.status_description", e.to_string().as_str());
    })
}
