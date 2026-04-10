# tokio-postgres

[tokio-postgres](https://docs.rs/tokio-postgres) is an async PostgreSQL client for Rust.

## Read-only transactions

Use `build_transaction().read_only(true)`:

```rust [main.rs]
let tx = client.build_transaction().read_only(true).start().await?;
// Sends: BEGIN READ ONLY
```

Halephant detects `BEGIN READ ONLY` and routes the transaction to a replica.

## Connection parameters

You can pass startup parameters through the `options` config field:

```rust [main.rs]
let mut config = tokio_postgres::Config::new();
config.host("halephant");
config.port(6432);
config.dbname("myapp");
config.user("myapp_ro");
config.options("-c default_transaction_read_only=on");
```

Or use a dedicated read-only user configured in halephant. See the [read replica guide](/guide/read-replicas).
