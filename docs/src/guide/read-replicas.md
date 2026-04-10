# Read replica load balancing

Halephant routes transactions to primary or replica nodes based on read-only signals from the client.

## How routing works

Halephant detects read-only intent through three mechanisms:

1. **Connection parameters.** The user is configured with `default_transaction_read_only = on`, so all traffic on that
   connection goes to a replica. This is the simplest and most reliable approach.
2. **Transaction begin.** The client sends `BEGIN READ ONLY` or `START TRANSACTION READ ONLY`, routing that single
   transaction to a replica.
3. **Session characteristics.** The client sends `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` (equivalent to
   `SET default_transaction_read_only = on`), switching all subsequent transactions to read-only.

::: info Note on SET TRANSACTION READ ONLY
`SET TRANSACTION READ ONLY` is sent *after* `BEGIN`, at which point halephant has already routed the transaction to a
server. This cannot change the routing for the current transaction. Use `BEGIN READ ONLY` instead, or set the session
default.
:::

## Separate users

The simplest approach is to configure two users per application, one for writes and one for reads:

```toml [halephant.toml]
[cluster.main.pool.myapp]
max_connections = { primary = 20, replica = 80 }

[cluster.main.pool.myapp.user.myapp.parameters]
application_name = "myapp"

[cluster.main.pool.myapp.user.myapp_ro]
alias = "myapp"
max_connections = { replica = 20 }

[cluster.main.pool.myapp.user.myapp_ro.parameters]
application_name = "myapp-ro"
options = { default_transaction_read_only = "on" }
```

`myapp_ro` has only `replica` capacity, so all its traffic routes to replicas. The `alias` field means it authenticates
and connects upstream as `myapp` — no extra PostgreSQL role needed.

## Read-only transactions

If your application uses a single database user, many drivers support read-only transactions by sending
`BEGIN READ ONLY`. Halephant detects this and routes the transaction to a replica automatically.

See the [client pages](/clients/python/psycopg) for framework-specific setup instructions.

## Read-write override protection

Once a transaction is routed to a replica, halephant rejects attempts to switch to read-write mode with a clear error
instead of letting the statement reach the replica. Intercepted statements:

| Statement | Context |
|-----------|---------|
| `SET default_transaction_read_only = off` | User configured as read-only |
| `SET default_transaction_read_only TO DEFAULT` | Resets to `off` (read-write) |
| `SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE` | Session routed to replica |
| `SET TRANSACTION READ WRITE` | Inside a read-only transaction |
| `BEGIN READ WRITE` | Session default is read-only |

Connections on the primary are not affected.
