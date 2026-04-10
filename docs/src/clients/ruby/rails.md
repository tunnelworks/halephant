# Ruby on Rails

[Rails](https://rubyonrails.org/) has built-in multi-database support since version 6.

## Read replica routing

Point both roles at halephant with different users in `config/database.yml`:

```yaml [config/database.yml]
production:
  primary:
    host: halephant
    port: 6432
    database: myapp
    username: myapp
    password: ...
  primary_replica:
    host: halephant
    port: 6432
    database: myapp
    username: myapp_ro
    password: ...
    replica: true
```

Enable automatic connection switching in `config/application.rb`:

```ruby [config/application.rb]
config.active_record.database_selector = { delay: 2.seconds }
config.active_record.database_resolver =
  ActiveRecord::Middleware::DatabaseSelector::Resolver
config.active_record.database_resolver_context =
  ActiveRecord::Middleware::DatabaseSelector::Resolver::Session
```

Rails routes GET and HEAD requests to the replica after a configurable delay (default 2 seconds after the last write).
POST, PUT, DELETE, and PATCH requests use the primary.

For manual control:

```ruby [app/models/concerns/read_routing.rb]
ActiveRecord::Base.connected_to(role: :reading) do
  # Queries go to the replica.
end
```

Rails doesn't send `BEGIN READ ONLY`. The separate user approach lets halephant handle routing. See the
[read replica guide](/guide/read-replicas) for halephant configuration.
