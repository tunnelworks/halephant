# SQLAlchemy

[SQLAlchemy](https://www.sqlalchemy.org/) is a Python SQL toolkit and ORM.

## Driver

SQLAlchemy connects to PostgreSQL through a DBAPI driver. Use psycopg3 (`postgresql+psycopg://`) for the best compatibility
with halephant.

## SSL

Disable SSL in the connection URL:

```python [db.py]
engine = create_engine(
    "postgresql+psycopg://myapp:password@halephant:6432/myapp?sslmode=disable"
)
```

## Read-only transactions

SQLAlchemy doesn't have a built-in read-only transaction mode. The simplest approach is to use a separate read-only user
configured in halephant:

```python [db.py]
write_engine = create_engine("postgresql+psycopg://myapp:pw@halephant:6432/myapp")
read_engine = create_engine("postgresql+psycopg://myapp_ro:pw@halephant:6432/myapp")
```

Route sessions with a custom `Session` class or SQLAlchemy's horizontal sharding extension.

You can also set `read_only` on the underlying psycopg3 connection with an event listener:

```python [db.py]
from sqlalchemy import event

@event.listens_for(read_engine, "connect")
def set_readonly(dbapi_conn, connection_record):
    dbapi_conn.read_only = True
```

See the [read replica guide](/guide/read-replicas) for halephant configuration.
