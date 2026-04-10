# asyncpg

[asyncpg](https://magicstack.github.io/asyncpg/) is a high-performance asyncio PostgreSQL driver.

## Prepared statements

asyncpg uses prepared statements by default for all queries. This works transparently with halephant's prepared statement
cache, so you don't need to change anything.

## SSL

Halephant doesn't support TLS yet. Disable SSL in the connection:

```python [connection.py]
conn = await asyncpg.connect(
    host="halephant", port=6432, database="myapp",
    user="myapp", password="...", ssl=False
)
```

## Read-only transactions

Pass `readonly=True` to the transaction context manager:

```python [connection.py]
async with conn.transaction(readonly=True):
    await conn.fetch("SELECT ...")
```

asyncpg uses autocommit by default, so statements outside an explicit transaction don't carry a read-only signal. If you
want connection-level read-only behavior, use a dedicated read-only user. See the
[read replica guide](/guide/read-replicas).
