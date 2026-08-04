+++
title = "Architecture"
description = "How capture, the commit barrier, checkpointing and the snapshot-to-stream handoff fit together in rustcdc."
weight = 20
+++

This document describes the runtime architecture, safety properties, and extension boundaries of rustcdc.

## Design Goals

rustcdc is designed for:

- deterministic change-event delivery
- restart-safe progress tracking
- source-agnostic event processing
- embeddable runtime operation

## System Overview

At a high level, the runtime executes the following pipeline:

1. source connector emits snapshot or stream records
2. runtime converts records into canonical `Event` envelopes
3. consumer receives `EventBatch` values
4. consumer acknowledges durable progress through `AckToken`
5. checkpoint backend persists offsets
6. source confirmation advances only after durable checkpoint commit

This ordering preserves replay safety under failure and restart.

## Component Map

- `src/core/`: runtime lifecycle, event model, commit barrier, errors, observability
- `src/source/`: PostgreSQL, MySQL, SQL Server source implementations
- `src/checkpoint/`: checkpoint traits and concrete persistence backends
- `src/schema_history/`: schema-history abstraction and backends
- `src/transform/`: transform interfaces and transform pipeline logic
- `src/wasm/`: sandboxed WASM transform runtime
- `src/testkit/`: fixtures and conformance harnesses

## Runtime Model

`CdcRuntime` is the orchestrator and owns:

- source connector state
- in-memory delivery buffers
- checkpoint commit coordination
- runtime health and metrics surfaces

The runtime consumer boundary is batch-based and ack-driven.

## Safety Invariants

The following invariants define correctness:

1. no checkpoint advancement without explicit ack
2. no source confirmation beyond checkpointed progress
3. restart begins from persisted checkpoint position
4. unacknowledged deliveries are replayable
5. runtime lifecycle transitions are explicit and validated

## Snapshot And Stream Handoff

There are two ways to load rows that already exist when capture starts.

### Blocking snapshot, then handoff

The classic path: read the tables, then start streaming from a watermark captured during the
read.

- the snapshot phase establishes a handoff watermark
- the stream phase starts at or after that boundary
- the runtime resolves the overlap so no committed change is dropped

This protects correctness during long-running snapshots with concurrent writes, but the stream
does not start until the snapshot finishes.

### Incremental snapshot (DBLog watermarks)

The default for anything large. Chunk reads interleave with the live stream, so the stream
never pauses and no long-held transaction accumulates transaction IDs. Per chunk:

1. capture a **low watermark** position before the `SELECT`
2. read `chunk_size` rows by keyset pagination, outside any transaction
3. capture a **high watermark** position after the `SELECT`
4. keep polling the stream, recording the primary key of every event for that table whose
   position falls in `(low, high]`
5. once the stream passes the high watermark, emit snapshot rows **except** those whose key
   was in that override set

Step 5 is what makes the result independent of interleaving. A row modified between the two
watermarks appears in the chunk at its old value and in the stream at its new one; suppressing
the chunk copy means the stream value wins regardless of the order they were produced in.

The keyset cursor is persisted **inside the checkpoint offset** — the same atomic, fsynced,
checksummed record as the stream position — so a restart resumes at the chunk boundary rather
than re-reading the table. The coupling is deliberate: a chunk cursor is only meaningful
relative to the stream position it was captured against, and two separately written files
could disagree after a crash between them.

This algorithm is implemented **once**, in `IncrementalSnapshotDriver`. A connector supplies
only the database-specific half through `IncrementalSnapshotBackend`: the position type, the
watermark query, the chunk read, event position extraction, and the offset encoding. The three
built-in connectors go through that interface, and so can yours — see
[Incremental snapshot for a custom source](@/docs/api.md#incremental-snapshot-for-a-custom-source).

## Source-Specific Notes

### PostgreSQL

- stream decoding uses `pg_logical_slot_peek_binary_changes` with `pgoutput` format
- each poll call is bounded by a per-call timeout; slow or stalled queries are
  cancelled server-side via `CancelRequest` so the connection is always returned
  to a ready state before the next poll
- runtime tracks in-memory and persisted LSN progress
- replication slot advancement follows durable commit progression
- startup guards detect slot/checkpoint divergence

### MySQL

- runtime tracks binlog or GTID progress through checkpoint offsets
- resume behavior depends on retained binlog/GTID history

### SQL Server

- runtime tracks CDC progression via source-specific offset surfaces
- capture correctness depends on SQL Server CDC retention and job health

## Extension Points

rustcdc is designed to be extended through typed interfaces:

- `Source` for capture connectors the crate does not ship, driven via `register_source`
- `IncrementalSnapshotBackend` for non-blocking DBLog snapshots on a custom source
- `Checkpoint` for offset persistence backends
- `SchemaHistory` for schema state persistence
- `SinkAdapter` for sink-side delivery adapters; `BoxedSink` for type-erased storage; `FanOutSinkAdapter` for concurrent multi-sink fan-out
- WASM transform ABI for sandboxed transform logic

## Observability Model

The runtime provides structured operational state through:

- admin snapshots
- Prometheus-style metric export
- structured logging fields

These surfaces are intended to integrate directly with service control planes and monitoring stacks.

## Failure Semantics

rustcdc provides at-least-once delivery semantics at the runtime boundary.

Operationally:

- ack after durable sink write minimizes data loss risk
- delayed ack may replay previously delivered events
- destination-side idempotency is recommended for strict correctness under retries

## Delivery Guarantees

### At-Least-Once Boundary

The runtime guarantees **at-least-once delivery** between the source connector and the consumer callback. The guarantee boundary works as follows:

1. Events are polled from the source and buffered in `CommitBarrier`.
2. The consumer calls `runtime.commit_ack(token)` after writing all events in the acknowledged batch to the destination.
3. The runtime persists the checkpoint and then calls `stream.confirm_lsn(...)` when the connector supports source-side confirmation.
4. **Failure window**: if source confirmation fails (network partition, connector restart), the source may replay events already delivered to the consumer.

Consumers **must** tolerate duplicate delivery. Monitor replay windows via destination-side deduplication signals and runtime checkpoint age/lag metrics.

### Idempotent Consumer Design Patterns

Recommended patterns for consumers to absorb duplicate events:

- **Event deduplication table**: maintain a `processed_lsn` / `event_id` set in the destination and skip rows already present.
- **Upsert by primary key**: for row-level CDC, use INSERT … ON CONFLICT DO UPDATE semantics so replaying the same row is idempotent.
- **Outbox pattern**: pair rustcdc with a transactional outbox in the destination; the outbox write and the commit become one transaction.
- **Sequence-gated apply**: checkpoint the last-applied LSN in the destination table; skip events with `lsn ≤ last_applied`.

### Exactly-Once Patterns

rustcdc does not provide a built-in exactly-once transport protocol at the runtime boundary.
Exactly-once behavior is achieved by destination-side design, such as transactional outbox,
deduplication keys, or idempotent upserts.

### Two-Phase Commit Patterns

For heterogeneous destinations (e.g., Kafka + relational DB), use two-phase commit:

1. **Prepare phase**: write events to both destinations speculatively.
2. **Commit phase**: call `runtime.commit_ack(token)` only after both destinations confirm durability.
3. **Abort / rollback**: if either destination fails, abort and allow the runtime to replay.

This is not built into rustcdc directly; it requires the consumer to coordinate the two-phase protocol around batch ack and runtime checkpoint commit boundaries.

## Related Documentation

- [API Guide](@/docs/api.md)
- [Configuration Reference](@/docs/config-reference.md)
- [Schema Evolution and DDL Capture](@/docs/schema-evolution.md)
- [Reliability Testing Guide](@/docs/reliability-testing.md)
- [Operator Runbook](@/docs/runbook.md)
- [Troubleshooting Guide](@/docs/troubleshooting.md)
