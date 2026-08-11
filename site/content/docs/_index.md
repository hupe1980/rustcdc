+++
title = "Documentation"
description = "Guides, reference and operational procedures for rustcdc — embeddable change data capture for PostgreSQL, MySQL, MariaDB and SQL Server."
sort_by = "weight"
template = "docs/section.html"
page_template = "docs/page.html"

# Sidebar navigation, grouped by what the reader is trying to do rather than by how
# the files happen to be named.
[[extra.nav]]
title = "Start"
items = [
  "docs/getting-started.md",
  "docs/architecture.md",
]

[[extra.nav]]
title = "Build"
items = [
  "docs/api.md",
  "docs/config-reference.md",
  "docs/schema-evolution.md",
]

[[extra.nav]]
title = "Extend"
items = [
  "docs/adapter-sdk.md",
  "docs/wasm-transform-sdk.md",
  "docs/wasm-conformance-contract.md",
]

[[extra.nav]]
title = "Operate"
items = [
  "docs/deployment.md",
  "docs/runbook.md",
  "docs/troubleshooting.md",
  "docs/security.md",
]

[[extra.nav]]
title = "Verify"
items = [
  "docs/reliability-testing.md",
  "docs/feature-policy.md",
  "docs/library-parity-matrix.md",
  "docs/snowflake.md",
]
+++

rustcdc is an embeddable change data capture library. It reads the replication log of
PostgreSQL, MySQL, MariaDB or SQL Server and hands you a stream of typed events inside your
own process — no separate service to run, no JVM, no control plane.

## Where to start

**New here?** [Getting started](@/docs/getting-started.md) builds a working pipeline from an
empty project. Then read [Architecture](@/docs/architecture.md) — the commit barrier and the
snapshot-to-stream handoff are the two ideas everything else rests on.

**Integrating?** The [API guide](@/docs/api.md) covers the embedding model, and the
[configuration reference](@/docs/config-reference.md) documents every option in terms of the
failure it prevents rather than just its type.

**Running it?** The [operations runbook](@/docs/runbook.md) has the alert thresholds and
recovery procedures; [troubleshooting](@/docs/troubleshooting.md) is organised by symptom.

## What to read before production

Three pages carry contracts that are easy to get wrong and expensive to discover late:

- **[Partial payloads](@/docs/api.md#partial-payloads-read-this-before-writing-a-sink)** —
  not every event carries a complete row, and applying one as if it did writes `NULL` over
  data that never changed.
- **[Required source configuration](@/docs/config-reference.md)** — several database settings
  cause silently wrong capture rather than an error. `connect()` rejects them, but knowing
  why saves a confusing first run.
- **[Delivery guarantees](@/docs/architecture.md)** — at-least-once, with duplicates after a
  crash. Sinks must be idempotent on a key you control.

## Reference

Item-level API documentation lives on [docs.rs](https://docs.rs/rustcdc). This site covers the
guides, operational procedures and design rationale that do not fit in rustdoc.
