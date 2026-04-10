---
layout: home

hero:
  name: "Halephant"
  text: "Connection pooling for PostgreSQL"
  tagline: A PostgreSQL connection pooler, proxy, and multiplexer.
  actions:
    - theme: brand
      text: Get started
      link: /guide/configuration
    - theme: alt
      text: Read replica routing
      link: /guide/read-replicas

features:
  - title: Transaction-mode pooling
    details: Multiplex many clients over fewer server connections with prepared statement support, SCRAM authentication, and LISTEN/NOTIFY.
  - title: Read replica routing
    details: Route read-only transactions to replicas automatically based on connection parameters, BEGIN READ ONLY, or session characteristics.
  - title: Single binary
    details: No runtime dependencies. TOML configuration with structured logging. Deploy as a static binary or container image.
---
