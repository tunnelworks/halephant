# Configuration

Halephant is configured with a single TOML file, passed via the `-c` flag:

```sh
halephant -c /etc/halephant/halephant.toml
```

## Minimal example

```toml [halephant.toml]
[server]
listen = ["0.0.0.0:6432"]

[logging]
level = "info"

# [admin]
# listen = "0.0.0.0:6433"

# [otel]
# endpoint = "http://localhost:4317"

[cluster.main]
nodes = ["pg-1.internal:5432"]
admin_user = "halephant"

[cluster.main.pool.myapp]
max_connections = { primary = 20, replica = 80 }

[cluster.main.pool.myapp.user.myapp]
min_connections = { primary = 2 }

[cluster.main.pool.myapp.user.myapp.parameters]
application_name = "myapp"
```

## Hot reload

Halephant re-reads the config file and swaps it in atomically on `SIGHUP`:

```sh
kill -HUP $(pidof halephant)
```

After the swap, halephant runs `topology.refresh()` and `warm_up()` so added nodes, added pools, or raised
`min_connections` floors take effect immediately. Each attempt increments the `halephant.config.reloads` counter
with an `outcome` attribute of `success`, `restart_required`, or `parse_failed`.

The following fields require a process restart. A reload that changes any of them is rejected and the old
config stays in place:

- `server.listen` — TCP sockets are bound at startup.
- `server.pgpass` — the `.pgpass` file is loaded once at startup.
- `server.workers` — consumed by the tokio runtime at startup.
- `admin.listen` — the admin API is bound at startup.
- `otel.endpoint`, `otel.service_name` — the OTLP providers are initialized at startup.
- `logging.format`, `logging.level` — the tracing subscriber is initialized at startup.

Everything else hot-reloads, including cluster nodes, pool sizes, user lists, auth settings, topology
intervals, `server.checkout_timeout`, and `server.shutdown_timeout`. Already-connected clients finish their
current session under the config snapshot they started with; new connections observe the swapped config
immediately.

The admin API is intentionally read-only — there is no `/admin/reload` endpoint. Use `SIGHUP`.

## Reference

### `[server]`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen` | string array | `["0.0.0.0:6432"]` | Addresses to listen on. Use multiple entries for dual-stack (IPv4 + IPv6). |
| `pgpass` | string | `~/.pgpass` | Path to a `.pgpass` file for upstream password authentication. Falls back to the `PGPASSFILE` environment variable, then `~/.pgpass`. |
| `workers` | integer | `0` | Worker threads. `0` auto-detects CPU count. |
| `shutdown_timeout` | duration | `"30s"` | Time to wait for in-flight connections to drain on shutdown. |
| `checkout_timeout` | duration | `"30s"` | Maximum time a client waits for a server connection when every candidate pool is at its `max_connections`. Individual pools can override this via `[cluster.<name>.pool.<database>].checkout_timeout`. See [Backpressure and queueing](/guide/backpressure). |
| `max_prepared_statements` | integer | `0` | Maximum unique prepared statements tracked globally. `0` for unlimited. |

### `[logging]`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `format` | string | `"json"` | Log format: `"json"` or `"text"`. |
| `level` | string | `"info"` | Log level: `"trace"`, `"debug"`, `"info"`, `"warn"`, or `"error"`. |

### `[admin]`

Admin HTTP API configuration. When `listen` is absent, the admin API is disabled.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen` | string | none | Admin API listen address (for example, `"0.0.0.0:6433"`). Serves health checks, pool introspection, and an OpenAPI documentation UI at `/docs`. |

### `[otel]`

OpenTelemetry configuration. When `endpoint` is absent, telemetry export is disabled and adds zero overhead.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | none | OTLP gRPC collector endpoint (for example, `"http://localhost:4317"`). When absent, telemetry export is disabled. |
| `service_name` | string | `"halephant"` | Service name reported in traces. |
| `query_text` | string | `"off"` | Controls SQL query text on trace spans: `"off"` (only `db.query.summary`), `"sanitized"` (literals replaced with `?`), or `"raw"` (full SQL verbatim). |

### `[cluster.<name>]`

Each cluster defines a group of PostgreSQL nodes. The `<name>` in the section header is the cluster's identifier.
Halephant automatically discovers which node is the primary and which are replicas by running
`SELECT pg_is_in_recovery()` on each node.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `nodes` | string array | required | PostgreSQL node addresses (`"host:port"`). |
| `admin_user` | string | `"halephant"` | PostgreSQL role used for topology probes and auth queries. |
| `admin_database` | string | `"postgres"` | Database the admin connection selects in its startup message. The default works for the out-of-the-box `auth.query` (which reads `pg_shadow`, a cluster-global catalog). Override when `auth.query` references database-local objects such as a custom role-mapping table. A matching `.pgpass` line must use the same database name, because the pgpass lookup keys on `(host, port, database, user)`. |
| `connect_timeout` | duration | `"5s"` | Timeout for connecting to upstream nodes. |

### `[cluster.<name>.auth]`

Authentication settings for this cluster. The auth query fetches SCRAM-SHA-256 verifiers from the upstream database,
connecting as the cluster's `admin_user`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `query` | string | `"SELECT usename, passwd FROM pg_shadow WHERE usename = $1"` | SQL query to fetch SCRAM verifiers. Use a custom table or security-definer function when superuser access is restricted. |
| `cache_ttl` | duration | `"300s"` | How long to cache verifiers before re-fetching. |

### `[cluster.<name>.topology]`

Controls how often halephant probes this cluster's nodes to detect primary/replica role changes and failovers.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `interval` | duration | `"5s"` | How often to probe nodes for role changes. |
| `timeout` | duration | `"3s"` | Timeout for each role check probe. |

### `[cluster.<name>.pool.<database>]`

Each pool maps a database to its parent cluster and defines pooling behavior. The `<database>` in the section header is
the PostgreSQL database name.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database` | string | same as `<database>` key | PostgreSQL database name. Override when the pool key differs from the actual database name. |
| `mode` | string | `"transaction"` | Pool mode: `"transaction"` or `"session"`. |
| `listen_mode` | string | `"pin"` | LISTEN/NOTIFY handling in transaction mode: `"pin"` or `"multiplex"`. Not applicable to session-mode pools. |
| `max_connections` | `{ primary, replica }` | `{ primary = 100 }` | Maximum server connections **per node**, split by primary and replica. With two replicas and `replica = 80`, total replica capacity is 160. |
| `idle_timeout` | duration | `"5m"` | Close idle connections (beyond `min_connections`) after this duration. |
| `max_lifetime` | duration | `"1h"` | Close connections after this total lifetime. |
| `checkout_timeout` | duration | inherits `[server]` | Per-pool override of the server-wide checkout timeout. Useful for mixing latency-sensitive pools (short timeout) with analytics pools (long timeout) in the same halephant process. |

### `[cluster.<name>.pool.<database>.user.<name>]`

Each user entry defines who can connect and with what settings. The `<name>` in the section header is the username that
clients connect with. At least one user is required per pool.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `alias` | string | none | This user is an alias for another PostgreSQL role. Halephant authenticates using the aliased role's credentials and connects upstream as that role. |
| `max_connections` | `{ primary, replica }` | no per-user limit | Per-user connection limit within the pool. A user with only `replica` (no `primary`) is implicitly read-only. |
| `min_connections` | `{ primary, replica }` | `{ primary = 0, replica = 0 }` | Idle connections to open at startup. |

### `[cluster.<name>.pool.<database>.user.<name>.parameters]`

PostgreSQL parameters sent in the StartupMessage for this user's connections.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `application_name` | string | none | Application name visible in `pg_stat_activity`. |
| `options` | table | `{}` | GUC settings passed via the `options` connection parameter (for example, `search_path`, `statement_timeout`, `default_transaction_read_only`). |

### Duration format

Duration fields accept human-readable strings: `"5s"`, `"30s"`, `"5m"`, `"1h"`, `"1h30m"`.

### Connection limits

Connection limits use a table with `primary` and `replica` keys. Both are optional and default to `0`.

```toml
max_connections = { primary = 20, replica = 80 }
min_connections = { primary = 2, replica = 10 }
```

All connection limits are **per node**. With two replicas and `replica = 80`, total replica capacity is 160.

A user with only `replica` capacity is implicitly read-only. See [Backpressure and queueing](/guide/backpressure)
for what happens when pools are full.
