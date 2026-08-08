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
6. a durable checkpoint never moves the **stream position** backwards

Invariant 6 is enforced by `FileCheckpoint`, independently of any connector, and exists because
the other five are all expressed in terms of the committed-event *count*. A count keeps rising
while a connector hands the checkpoint a position it cannot have reached, which is strictly
worse than forgetting progress: the counters say the pipeline is healthy while the recorded
resume point now sits before data the sink has already accepted. `FileCheckpoint::save`
therefore compares the connector-native coordinate against the record it is replacing and
refuses a regression with an error naming both positions.

**How strictly it can compare depends on what the source guarantees**, and the guard is only as
strict as each one allows:

| Source | Compared | Why not more |
|---|---|---|
| MySQL / MariaDB, file+position | binlog file sequence, then position | Every event in a transaction carries the *commit* position, and the binlog is written in commit order, so the sequence is monotonic |
| MySQL / MariaDB, GTID | nothing | Binlog coordinates are server-local; a promoted replica's are routinely lower, and GTID is what actually resumes the stream |
| SQL Server | the commit LSN only | Both cursor encodings (`{lsn}` and `{lsn}:{seqval}:{op}`) occur in one stream — the bare form is a prefix of the other, so comparing whole strings would read a graceful restart as a rewind |
| PostgreSQL | **only a zero LSN** | pgoutput emits changes in *commit* order while each keeps its own WAL position, so two transactions interleaved in the WAL arrive out of LSN order. The checkpoint legitimately moves backwards; resuming from the lower LSN re-reads the later-positioned change, which is the documented at-least-once behaviour. Zero is not a position the stream can reach, so it can only come from a decode defect |
| anything else | nothing | An unrecognised source type is left alone rather than guessed at |

A renamed replication slot is never comparable to the old one, in any source.

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

Because that record is written on **every** commit — including commits of the live stream
events that flow past while a chunk is in its collect phase — the cursor advances only once a
chunk has been fully handed to the consumer, never when it is read. A restart therefore re-reads
at most one chunk, which is the at-least-once behaviour the rest of the pipeline already
guarantees. Advancing at read time instead would make a cursor durable before its rows existed
anywhere, and a restart would resume *after* rows that were never emitted — up to `chunk_size`
rows missing from the snapshot, with no error and no counter to notice it by.

This algorithm is implemented **once**, in `IncrementalSnapshotDriver`. A connector supplies
only the database-specific half through `IncrementalSnapshotBackend`: the position type, the
watermark query, the chunk read, event position extraction, and the offset encoding. The three
built-in connectors go through that interface, and so can yours — see
[Incremental snapshot for a custom source](@/docs/api.md#incremental-snapshot-for-a-custom-source).

## Source-Specific Notes

### PostgreSQL

- stream decoding uses `pg_logical_slot_peek_binary_changes` with `pgoutput` format, over an
  ordinary connection — **not** the streaming replication protocol. `tokio-postgres` exposes no
  `CopyBoth` / replication-mode API and no published crate supplies one, so `START_REPLICATION`
  is not reachable from here. The consequences are real and worth planning around:
  - the peek is **non-consuming**, and PostgreSQL begins decoding at the slot's `restart_lsn`
    while only *emitting* past `confirmed_flush_lsn`. A long-running transaction on the source
    pins `restart_lsn`, so every poll re-scans the gap between the two. Keep an eye on
    `pg_replication_slots.restart_lsn` versus `confirmed_flush_lsn`, not just on slot lag
  - delivery latency is bounded by the poll interval rather than pushed by the server
  - acknowledging promptly is what keeps the gap small: the slot advances on
    `confirm_lsn`, and a consumer that defers acks makes each subsequent poll more expensive
- each poll call is bounded by a per-call timeout; slow or stalled queries are
  cancelled server-side via `CancelRequest` so the connection is always returned
  to a ready state before the next poll
- a poll that exceeds its budget **halves** the decode window rather than retrying the same
  work, converging on a single change. A timed-out peek is explicitly *not* reported as an idle
  slot — the opposite is true — so it can never be mistaken for permission to advance the slot
- runtime tracks in-memory and persisted LSN progress
- replication slot advancement follows durable commit progression
- startup guards detect slot/checkpoint divergence

### MySQL / MariaDB

- runtime tracks binlog or GTID progress through checkpoint offsets
- resume behavior depends on retained binlog/GTID history
- `binlog_transaction_compression` is read transparently; events unpacked from a
  `Transaction_payload_event` resume at the payload's end position, since they have none of
  their own — see [the config reference](@/docs/config-reference.md#binlog-transaction-compression)
- MariaDB-specific event types (160–164) are handled explicitly rather than skipped: the GTID
  event is decoded, and an encrypted binlog is a hard error rather than an apparently empty one

### SQL Server

- runtime tracks CDC progression via source-specific offset surfaces
- capture correctness depends on SQL Server CDC retention and job health
- one LSN window is read across every capture instance, and the window advances only after the
  whole window has been read — so `max_events_per_poll` truncating one instance costs a retry,
  not a gap
- each capture instance is read from its own capture floor, so enabling CDC on a new table
  while the stream is running is a normal operation rather than a retention error

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
