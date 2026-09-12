+++
title = "Documentation"
description = "Capture row-level changes from PostgreSQL, MySQL, MariaDB, SQL Server or Snowflake — as a server you run or a Rust crate you embed."
sort_by = "weight"
template = "section.html"
page_template = "page.html"
+++

rustcdc reads a database's replication log and turns every `INSERT`, `UPDATE` and `DELETE`
into a typed event. You can run it as a **server** or embed it as a **Rust library** —
same runtime, same version, one set of guarantees.

These pages cover both. Where something applies to only one, the page says so at the top.

<figure class="diagram">
<svg viewBox="0 0 760 190" role="img" aria-labelledby="pipeline-title" class="dg-text">
  <title id="pipeline-title">A change flows from the database log through capture, transforms and the commit barrier to a sink, with the checkpoint advancing only after the sink accepts it</title>

  <rect x="8"   y="52" width="128" height="58" rx="8" class="dg-box"/>
  <text x="72"  y="76"  text-anchor="middle" class="dg-label">Database</text>
  <text x="72"  y="94"  text-anchor="middle" class="dg-sub">WAL · binlog · CDC tables</text>

  <rect x="172" y="52" width="118" height="58" rx="8" class="dg-box"/>
  <text x="231" y="76"  text-anchor="middle" class="dg-label">Capture</text>
  <text x="231" y="94"  text-anchor="middle" class="dg-sub">snapshot + stream</text>

  <rect x="326" y="52" width="118" height="58" rx="8" class="dg-box"/>
  <text x="385" y="76"  text-anchor="middle" class="dg-label">Transforms</text>
  <text x="385" y="94"  text-anchor="middle" class="dg-sub">mask · route · WASM</text>

  <rect x="480" y="52" width="128" height="58" rx="8" class="dg-accent"/>
  <text x="544" y="76"  text-anchor="middle" class="dg-label">Commit barrier</text>
  <text x="544" y="94"  text-anchor="middle" class="dg-sub">ordering</text>

  <rect x="644" y="52" width="108" height="58" rx="8" class="dg-box"/>
  <text x="698" y="76"  text-anchor="middle" class="dg-label">Sink</text>
  <text x="698" y="94"  text-anchor="middle" class="dg-sub">Kafka · Iceberg · …</text>

  <path d="M136 81 H172" class="dg-flow" marker-end="url(#dg-arrow)"/>
  <path d="M290 81 H326" class="dg-flow" marker-end="url(#dg-arrow)"/>
  <path d="M444 81 H480" class="dg-flow" marker-end="url(#dg-arrow)"/>
  <path d="M608 81 H644" class="dg-flow" marker-end="url(#dg-arrow)"/>

  <path d="M698 110 V146 H544 V110" class="dg-flow" stroke-dasharray="4 3" marker-end="url(#dg-arrow)"/>
  <text x="621" y="166" text-anchor="middle" class="dg-sub">acknowledged → checkpoint advances</text>

  <defs>
    <marker id="dg-arrow" viewBox="0 0 10 10" refX="9" refY="5"
            markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M0 0 L10 5 L0 10 z" fill="currentColor" class="dg-flow" stroke="none"/>
    </marker>
  </defs>
</svg>
<figcaption>The durable position never advances past an event the sink has not accepted.</figcaption>
</figure>

## Start here

| | |
|---|---|
| [Getting started](@/docs/getting-started.md) | Run the server against a database in about ten minutes |
| [Embedding the library](@/docs/embedding.md) | Build a pipeline inside your own Rust binary |
| [Core concepts](@/docs/concepts.md) | The event model, delivery contracts, lifecycle |
| [Architecture](@/docs/architecture.md) | Capture, the commit barrier and checkpointing — how they fit |

## Connect a source

| | |
|---|---|
| [PostgreSQL](@/docs/connectors/postgres.md) | Logical replication, slot lifecycle, TOAST, managed clouds |
| [MySQL / MariaDB](@/docs/connectors/mysql.md) | Binlog, GTID, schema history, permissions |
| [SQL Server](@/docs/connectors/sqlserver.md) | CDC change tables, capture instances, availability groups |
| [Snowflake](@/docs/snowflake.md) | The `CHANGES` clause, and why Streams are unsafe for an external reader |

## Configure

| | |
|---|---|
| [Server configuration](@/docs/configuration.md) | Every TOML field, with defaults and worked examples |
| [Library options](@/docs/config-reference.md) | Every `RuntimeOptions` field, in terms of the failure it prevents |
| [Schema evolution](@/docs/schema-evolution.md) | DDL handling, schema history, registry compatibility |
| [Feature flags](@/docs/feature-policy.md) | What each cargo feature costs and why connectors are opt-in |

## Build on it

| | |
|---|---|
| [API guide](@/docs/api.md) | The embedding model: lifecycle, acknowledgement, transforms, codecs |
| [Transforms](@/docs/transforms.md) | Masking, routing, outbox, and sandboxed WASM modules |
| [WASM transform SDK](@/docs/wasm-transform-sdk.md) | The guest ABI and its limits |
| [Writing a connector](@/docs/adapter-sdk.md) | A source the runtime treats as first-class |

## Run it

| | |
|---|---|
| [Deployment](@/docs/deployment.md) | Kubernetes, health wiring, resource shape |
| [Operations](@/docs/operations.md) | CLI, probes, metrics, replay |
| [Incident runbook](@/docs/runbook.md) | Symptom → diagnosis → resolution, for the server |
| [Embedded pipelines](@/docs/embedded-operations.md) | Alert thresholds and recovery when you own the process |
| [Troubleshooting](@/docs/troubleshooting.md) | Organised by symptom |
| [Security](@/docs/security.md) | Transport defaults, secret handling, known exposure |

## Reading the delivery guarantees

Two claims are load-bearing, and both are narrower than the words usually imply:

- **`at_least_once`** — a crash can replay the events of one uncheckpointed batch.
  Duplicates are possible; loss is not. Sinks must be idempotent on a key you control.
- **`effectively_once`** — a batch's records and its checkpoint commit together, in one
  Kafka transaction. It is *not* end-to-end exactly-once in every configuration, and
  [the limits are written down](@/docs/concepts.md#3-delivery-contracts) rather than
  glossed over.

Where a guarantee has a limit, the limit is next to the guarantee. If rustcdc cannot do
something, these pages say so.
