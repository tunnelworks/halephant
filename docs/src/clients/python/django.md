# Django

[Django](https://www.djangoproject.com/) is a Python web framework with built-in ORM and database routing.

## SSL

Disable SSL in the database options:

```python [settings.py]
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": "halephant",
        "PORT": 6432,
        "OPTIONS": {"sslmode": "disable"},
        # ...
    },
}
```

## Read replica routing

Django routes reads and writes through its `DATABASE_ROUTERS` system. Point both connections at halephant with different
users:

```python [settings.py]
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": "halephant",
        "PORT": 6432,
        "NAME": "myapp",
        "USER": "myapp",
        "PASSWORD": "...",
        "OPTIONS": {"sslmode": "disable"},
    },
    "replica": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": "halephant",
        "PORT": 6432,
        "NAME": "myapp",
        "USER": "myapp_ro",
        "PASSWORD": "...",
        "OPTIONS": {"sslmode": "disable"},
    },
}
```

Define a router:

```python [routers.py]
class ReadReplicaRouter:
    def db_for_read(self, model, **hints):
        return "replica"

    def db_for_write(self, model, **hints):
        return "default"

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        return db == "default"
```

Register it in settings:

```python [settings.py]
DATABASE_ROUTERS = ["myapp.routers.ReadReplicaRouter"]
```

Django doesn't send `BEGIN READ ONLY` or any in-band read-only signal. The separate user approach lets halephant handle
routing transparently. See the [read replica guide](/guide/read-replicas) for halephant configuration.
