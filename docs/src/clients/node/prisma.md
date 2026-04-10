# Prisma

[Prisma](https://www.prisma.io/) is a TypeScript/JavaScript ORM with a read replicas extension.

## Read replica routing

Use the `@prisma/extension-read-replicas` extension. Point both the primary and replica at halephant with different users:

```typescript [db.ts]
import { PrismaClient } from "@prisma/client";
import { readReplicas } from "@prisma/extension-read-replicas";

const prisma = new PrismaClient({
    datasourceUrl: "postgresql://myapp:password@halephant:6432/myapp",
}).$extends(
    readReplicas({
        url: "postgresql://myapp_ro:password@halephant:6432/myapp",
    }),
);
```

Prisma sends read operations (`findMany`, `findFirst`, `findUnique`, `count`, `aggregate`, `groupBy`) to the replica URL.
All write operations and `$transaction` go to the primary URL.

Prisma doesn't send `BEGIN READ ONLY`. The separate user approach lets halephant handle routing. See the
[read replica guide](/guide/read-replicas) for halephant configuration.
