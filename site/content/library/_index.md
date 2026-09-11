+++
title = "Library"
description = "Embedding the rustcdc crate: guides, reference and operational procedures for change data capture inside your own Rust process."
sort_by = "weight"
template = "section.html"
page_template = "page.html"
+++

rustcdc is an embeddable change data capture library. It reads the replication log of
PostgreSQL, MySQL, MariaDB or SQL Server and hands you a stream of typed events inside your
own process — no separate service to run, no JVM, no control plane.

## Where to start

**New here?** [Getting started](@/library/getting-started.md) builds a working pipeline from an
empty project. Then read [Architecture](@/library/architecture.md) — the commit barrier and the
snapshot-to-stream handoff are the two ideas everything else rests on.

**Integrating?** The [API guide](@/library/api.md) covers the embedding model, and the
[configuration reference](@/library/config-reference.md) documents every option in terms of the
failure it prevents rather than just its type.

**Running it?** The [operations runbook](@/library/runbook.md) has the alert thresholds and
recovery procedures; [troubleshooting](@/library/troubleshooting.md) is organised by symptom.

## What to read before production

Three pages carry contracts that are easy to get wrong and expensive to discover late:

- **[Partial payloads](@/library/api.md#partial-payloads-read-this-before-writing-a-sink)** —
  not every event carries a complete row, and applying one as if it did writes `NULL` over
  data that never changed.
- **[Required source configuration](@/library/config-reference.md)** — several database settings
  cause silently wrong capture rather than an error. `connect()` rejects them, but knowing
  why saves a confusing first run.
- **[Delivery guarantees](@/library/architecture.md)** — at-least-once, with duplicates after a
  crash. Sinks must be idempotent on a key you control.

## Reference

Item-level API documentation lives on [docs.rs](https://docs.rs/rustcdc). This site covers the
guides, operational procedures and design rationale that do not fit in rustdoc.
