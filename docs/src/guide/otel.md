# OpenTelemetry

Halephant exports traces and metrics via [OpenTelemetry](https://opentelemetry.io/) (OTLP gRPC). Configure the
`[otel]` section in the [configuration file](./configuration.md#otel) to enable telemetry export. Both signals share
the same endpoint.

## Spans

Halephant emits the following span types:

| Span | Description | Key attributes |
|------|-------------|----------------|
| `proxy.setup` | Connection establishment: authentication and initial server checkout. | `client.address`, `db.namespace`, `user` |
| `proxy.transaction` | A single transaction in transaction mode. Groups `proxy.statement` children. | `db.namespace`, `user`, `server.address` |
| `proxy.statement` | A single SQL statement within a transaction. One per `Execute` or `Query`. | `db.query.summary`, `db.query.text` |
| `proxy.session` | Full session lifetime (session mode). | `client.address`, `db.namespace`, `user`, `server.address` |
| `pool.checkout` | Checking out a server connection from the pool. | `db.namespace`, `user`, `read_only`, `pool.reused` |
| `pool.connect` | Opening a new TCP connection to an upstream PostgreSQL node. | `db.system.name`, `db.namespace`, `server.address`, `user` |
| `pool.reset` | Resetting a connection before returning it to the idle pool. | |
| `pool.warmup` | Opening minimum idle connections at startup. | |
| `auth` | SCRAM-SHA-256 authentication exchange. | `db.namespace`, `user` |
| `topology.refresh` | Periodic topology probe across all cluster nodes. | |

All spans that can fail record `otel.status_code` and `otel.status_description` on error.

The `db.query.text` attribute on `proxy.statement` is controlled by the `query_text`
[configuration option](./configuration.md#otel). In `sanitized` mode, simple-protocol queries have literals replaced
with `?` while extended-protocol queries (already parameterized) are recorded as-is. In `raw` mode, the full SQL is
recorded verbatim. Pipelined extended-protocol queries produce one `proxy.statement` span per `Execute` message.

## Metrics

Halephant exports the following metrics via OTLP alongside traces:

| Metric | Type | Description | Attributes |
|--------|------|-------------|------------|
| `db.client.connection.count` | gauge | Number of connections in the pool. `state` is `active`, `idle`, or `resetting`. | `server.address`, `db.namespace`, `user`, `state` |
| `db.client.connection.max` | gauge | Configured maximum connections. | `db.namespace`, `role` |
| `db.client.connection.errors` | counter | Classified connection errors. `error.type` is one of `checkout_timeout`, `shutting_down`, `unknown_database`, `no_primary`, `no_replica`, or `connect_failed`. | `error.type`, `db.namespace`, `user`, `server.address` |
| `db.client.connection.wait_time` | histogram | Time spent in `pool.checkout` (idle reuse + any wait + connect). | `db.namespace`, `user`, `server.address` |
| `db.client.connection.create_time` | histogram | Time to establish a new upstream connection (seconds). | `server.address`, `db.namespace` |
| `db.server.healthy` | gauge | Whether an upstream node is reachable (1 = healthy, 0 = unreachable). | `server.address`, `cluster` |
| `halephant.client.connections` | gauge | Clients grouped by lifecycle state: `negotiating`, `authenticating`, `idle`, `in_transaction`, `waiting`. See [Backpressure and queueing](/guide/backpressure) for the state machine. | `state` |
| `halephant.client.queue_depth` | gauge | Current depth of each `(database, user, role)` wait queue. Only non-empty queues emit series. | `db.namespace`, `user`, `role` |
| `halephant.client.wait_duration` | histogram | Time a client spent blocked on a wait queue before being served (seconds). Recorded only for checkouts that actually waited. | `db.namespace`, `user`, `role` |
| `halephant.config.reloads` | counter | SIGHUP config reload attempts. See [Hot reload](/guide/configuration#hot-reload). | `outcome` (`success`, `restart_required`, `parse_failed`) |

Gauges are reported every 15 seconds. Counters and histograms are recorded per event and exported on the same interval.
