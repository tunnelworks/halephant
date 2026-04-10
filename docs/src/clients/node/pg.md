# node-postgres

[node-postgres](https://node-postgres.com/) (`pg`) is the most widely used PostgreSQL client for Node.js.

## Read-only transactions

node-postgres doesn't have built-in read-only transaction support. Use raw SQL instead:

```javascript [db.js]
await client.query("BEGIN READ ONLY");
const result = await client.query("SELECT ...");
await client.query("COMMIT");
```

Halephant detects `BEGIN READ ONLY` and routes the transaction to a replica.

## Separate pools

If you handle routing at the application level, create two pools with different users:

```javascript [db.js]
const { Pool } = require("pg");

const writePool = new Pool({
    host: "halephant",
    port: 6432,
    database: "myapp",
    user: "myapp",
    ssl: false,
});

const readPool = new Pool({
    host: "halephant",
    port: 6432,
    database: "myapp",
    user: "myapp_ro",
    ssl: false,
});
```

See the [read replica guide](/guide/read-replicas) for halephant configuration.
