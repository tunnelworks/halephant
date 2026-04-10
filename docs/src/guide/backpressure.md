# Backpressure and queueing

When every candidate node for a `(database, user, role)` is at its `max_connections`, halephant queues the checkout
instead of returning an immediate error. The client waits until capacity frees up or `checkout_timeout` expires.

## Checkout lifecycle

1. **Pop an idle connection** from a candidate node's pool if available.
2. **Open a new connection** if the node is below `max_connections`.
3. **Enqueue** on the shared `(database, user, role)` wait queue if every candidate is full.

Candidates are tried in least-connections order. Replica queues are shared across every replica node for the same
`(database, user)` — a waiter that enqueued while replica A was full is served by replica B if B frees up first.

Each checkout has a single timeout budget (`checkout_timeout`). Spurious wakeups do not reset the clock. On shutdown,
all waiters are woken immediately so drain time stays bounded.

## Configuration

```toml [halephant.toml]
[server]
checkout_timeout = "30s"    # server-wide default

[cluster.main.pool.api]
checkout_timeout = "2s"     # per-pool override: fail fast for OLTP

[cluster.warehouse.pool.analytics]
checkout_timeout = "5m"     # per-pool override: patient analytics clients
```

## Diagnosing contention

**Queue state:** `GET /admin/queues` returns non-empty queues with depth and oldest wait duration.

**Waiting clients:** `GET /admin/clients` — look for `"state": "waiting"` and the `waiting_for` field.

**Metrics:**

| Metric | What it means |
|---|---|
| `halephant.client.connections{state="waiting"}` | Clients currently blocked. Sustained `> 0` means regular queueing. |
| `halephant.client.queue_depth` | Per-queue depth by `(database, user, role)`. |
| `halephant.client.wait_duration` | Time clients spent blocked (seconds). Only recorded for checkouts that waited. |
| `db.client.connection.errors{error.type="checkout_timeout"}` | Checkout failures from exhausted budget. |
