# Laravel

[Laravel](https://laravel.com/) has built-in read/write splitting for database connections.

## Read replica routing

Set up read and write connections in `config/database.php`, pointing both at halephant with different users:

```php [config/database.php]
'pgsql' => [
    'read' => [
        'username' => 'myapp_ro',
    ],
    'write' => [
        'username' => 'myapp',
    ],
    'host' => 'halephant',
    'port' => 6432,
    'database' => 'myapp',
    'password' => '...',
    'sslmode' => 'disable',
    'sticky' => true,
],
```

Laravel sends `SELECT` queries to the read connection and writes to the write connection. The `sticky` option makes sure
reads after a write in the same request go through the write connection, avoiding stale reads.

Laravel doesn't send `BEGIN READ ONLY`. The separate user approach lets halephant handle routing. See the
[read replica guide](/guide/read-replicas) for halephant configuration.
