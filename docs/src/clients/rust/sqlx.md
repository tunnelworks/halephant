# sqlx

[sqlx](https://github.com/launchbadge/sqlx) is a compile-time checked SQL toolkit for Rust.

## Read-only transactions

sqlx doesn't have a `read_only` option on its transaction builder. You can use a raw query to set the session default:

```rust [main.rs]
sqlx::query("SET default_transaction_read_only = on")
    .execute(&readonly_pool)
    .await?;
```

Or use a dedicated read-only user configured in halephant:

```rust [main.rs]
let write_pool = PgPoolOptions::new()
    .connect("postgres://myapp:pw@halephant:6432/myapp")
    .await?;

let read_pool = PgPoolOptions::new()
    .connect("postgres://myapp_ro:pw@halephant:6432/myapp")
    .await?;
```

See the [read replica guide](/guide/read-replicas) for halephant configuration.
