# psycopg3

[psycopg3](https://www.psycopg.org/psycopg3/) is the recommended PostgreSQL adapter for Python.

## Connection pooling

psycopg3 includes its own connection pool (`psycopg_pool`). Client-side pooling is still useful when connecting through
halephant. It saves you the SCRAM authentication and startup handshake on every request. Size the pool to match your
application's concurrency, not the number of backend connections you expect to need. Halephant handles the server-side
multiplexing.

## Prepared statements

psycopg3 automatically prepares queries after `prepare_threshold` executions (default 5). This works transparently with
halephant's prepared statement cache, so you don't need to change anything.

## SSL

Halephant doesn't support TLS yet. Set `sslmode=disable` in the connection string or as an environment variable:

```python [connection.py]
conn = await psycopg.AsyncConnection.connect(
    "host=halephant port=6432 dbname=myapp user=myapp sslmode=disable"
)
```

## Read-only transactions

Set `read_only=True` on the connection to make all implicit transactions read-only:

```python [connection.py]
conn = await psycopg.AsyncConnection.connect(conninfo, autocommit=False)
conn.read_only = True
# All subsequent queries use BEGIN READ ONLY.
```

For per-transaction control:

```python [connection.py]
async with conn.transaction() as tx:
    # This transaction inherits conn.read_only.
    await conn.execute("SELECT ...")
```

For autocommit connections, set the session default:

```python [connection.py]
conn = await psycopg.AsyncConnection.connect(conninfo, autocommit=True)
await conn.execute("SET default_transaction_read_only = on")
```

Or use a dedicated read-only user with `default_transaction_read_only = on` configured in halephant. See the
[read replica guide](/guide/read-replicas) for details.
