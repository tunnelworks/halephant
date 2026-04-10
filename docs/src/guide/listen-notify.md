# LISTEN/NOTIFY

In transaction mode, server connections are shared between clients, so LISTEN subscriptions would be lost on checkin.
The `listen_mode` setting controls how halephant handles this. In session mode, LISTEN/NOTIFY works naturally.

## Pin mode

```toml [halephant.toml]
[cluster.main.pool.myapp]
listen_mode = "pin"
```

When a client sends `LISTEN`, halephant pins the server connection to that client for the rest of the session. The
connection stays dedicated until the client disconnects or sends `UNLISTEN *`, at which point it returns to the pool.

The tradeoff is that each listening client holds a dedicated server connection.

## Multiplex mode

```toml [halephant.toml]
[cluster.main.pool.myapp]
listen_mode = "multiplex"
```

Halephant maintains a single shared listener connection per (database, user) pool. When a client sends
`LISTEN channel`, halephant intercepts the command, subscribes the client internally, and returns a synthetic response.
No server connection is checked out for the client.

Notifications from the shared listener are broadcast to all subscribed clients. `UNLISTEN` and `UNLISTEN *` are handled
locally as well.

The tradeoff is that notifications can be missed during a reconnection of the shared listener, and clients that fall
behind the 4096-entry broadcast buffer silently skip missed messages.

## Choosing a mode

| Mode | When to use |
|------|-------------|
| `pin` | Few listeners, simple setup |
| `multiplex` | Many listeners on shared channels |
