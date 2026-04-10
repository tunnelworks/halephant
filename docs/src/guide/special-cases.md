# Special cases

Some special cases to keep in mind.

## GUC

Do not configure `options` (such as `-c search_path=...`) on the client side.
As of now halephant does not forward client startup parameters to the upstream,
because server connections are shared in transaction mode.

Configure `search_path` and other GUCs in the halephant TOML instead:

```toml [halephant.toml]
[cluster.main.pool.myapp.user.myapp.parameters]
options = { search_path = "myschema,public" }
```
