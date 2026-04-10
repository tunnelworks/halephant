# JDBC

The [PostgreSQL JDBC driver](https://jdbc.postgresql.org/) (pgjdbc) has strong read-only transaction support.

## Read-only transactions

`Connection.setReadOnly(true)` controls read-only behavior. The SQL it sends depends on the `readOnlyMode` connection
property:

| readOnlyMode | autocommit=true | autocommit=false |
|---|---|---|
| `ignore` | No effect | No effect |
| `transaction` (default) | No effect | `BEGIN READ ONLY` |
| `always` | `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` | `BEGIN READ ONLY` |

For full coverage, use `readOnlyMode=always`:

```
jdbc:postgresql://halephant:6432/myapp?readOnlyMode=always
```

Then in your application or connection pool, such as HikariCP or c3p0:

```java [DataSource.java]
conn.setReadOnly(true);
// All subsequent transactions are read-only.
```

Halephant detects both `BEGIN READ ONLY` and `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` and routes
accordingly.

## Connection pool integration

Most Java connection pools support a `readOnly` property per pool. You can set up two pools, one for reads and one for
writes:

```properties [hikari.properties]
# Write pool
write.jdbcUrl=jdbc:postgresql://halephant:6432/myapp
write.username=myapp

# Read pool
read.jdbcUrl=jdbc:postgresql://halephant:6432/myapp?readOnlyMode=always
read.username=myapp
read.readOnly=true
```

Or use the separate user approach with `myapp_ro`. See the [read replica guide](/guide/read-replicas) for halephant
configuration.
