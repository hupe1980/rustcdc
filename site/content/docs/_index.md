+++
title = "Documentation"
description = "Guides and reference for rustcdc — install it, understand the delivery contracts, configure every field, write WASM transforms, and run it in production."
sort_by = "weight"
template = "section.html"
page_template = "page.html"
+++

Everything here describes behaviour that exists. Where a guarantee has a limit, the limit
is written down next to the guarantee rather than left for you to discover in production.

## Start here

| Guide | What it covers |
|---|---|
| [Getting started](@/docs/getting-started.md) | Install, configure and run a first pipeline in about ten minutes |
| [Core concepts](@/docs/concepts.md) | The event model, delivery contracts, pipeline lifecycle and circuit breaker |

## Reference

| Guide | What it covers |
|---|---|
| [Configuration](@/docs/configuration.md) | Every TOML field, with defaults and worked examples |
| [WASM transforms](@/docs/transforms.md) | Writing, testing and deploying a sandboxed transform module in Rust or AssemblyScript |

## Connectors

| Guide | What it covers |
|---|---|
| [PostgreSQL](@/docs/connectors/postgres.md) | Logical replication, slot lifecycle, TOAST, managed cloud databases |
| [MySQL / MariaDB](@/docs/connectors/mysql.md) | Binlog, GTID, schema history, permissions |
| [SQL Server](@/docs/connectors/sqlserver.md) | CDC change tables, capture instances, Always On availability groups |

## Running it

| Guide | What it covers |
|---|---|
| [Operations](@/docs/operations.md) | CLI commands, health checks, metrics, replay, Kubernetes |
| [Runbook](@/docs/runbook.md) | Incident procedures, disaster recovery, upgrade and rollback |

## How to read the delivery guarantees

Two claims in these docs are load-bearing, and both are narrower than the words usually
imply elsewhere:

- **`at_least_once`** means a crash can replay the events of one uncheckpointed batch.
  Duplicates are possible; loss is not.
- **`effectively_once`** means a batch commits at the sink atomically or not at all. It is
  *not* end-to-end exactly-once — the checkpoint is written outside the sink transaction,
  so a crash in that window replays the batch. [The window is described in
  full](@/docs/concepts.md#3-delivery-contracts) rather than glossed over.

If you need a guarantee this project does not provide, the documentation will say so
instead of implying otherwise.
