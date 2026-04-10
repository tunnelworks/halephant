# pgx

[pgx](https://github.com/jackc/pgx) is the most widely used PostgreSQL driver for Go.

## Read-only transactions

Use `pgx.TxOptions` with `AccessMode: pgx.ReadOnly`:

```go [main.go]
tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
// Sends: begin read only
```

With `database/sql`:

```go [main.go]
tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
```

Halephant detects `begin read only` and routes the transaction to a replica.

## Separate pools

If you separate read and write traffic at the pool level:

```go [main.go]
writePool, _ := pgxpool.New(ctx, "postgres://myapp:pw@halephant:6432/myapp")
readPool, _ := pgxpool.New(ctx, "postgres://myapp_ro:pw@halephant:6432/myapp")
```

See the [read replica guide](/guide/read-replicas) for halephant configuration.
