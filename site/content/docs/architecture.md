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

An event **past** the high watermark is deliberately not suppressed: it committed after the
`SELECT` finished, so the chunk row is still needed as that row's base state. What the
algorithm requires instead is that the chunk goes out **at** the high watermark, ahead of any
later event. DBLog gets that for free by emitting the buffered chunk the moment it reads the
high-watermark marker out of the log; rustcdc reads the log in batches, and one batch
routinely straddles the watermark — an event at LSN 900 and one at 1200 arrive together. Such
a batch is split at the first event past the watermark: head, chunk, tail. Returning it whole
and the chunk afterwards would apply the 1200 value and then the chunk's older value on top,
which is the stale-row resurrection step 5 exists to prevent, moved one step later.

While the tail is held back the driver reports **no** durable position, because the inner
stream has already consumed events the consumer has not been given; the snapshot rows in
between become non-persistent barrier entries and the held-back events carry the position
forward with their own offsets a moment later.

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

Tables can be added to a running snapshot with
`CdcRuntime::request_incremental_snapshot` — see
[on-demand snapshots](@/docs/config-reference.md#on-demand-snapshots). Because such a table is not
in the static config, the driver also **adopts unfinished tables from the checkpoint** on startup:
the config is the initial set, the checkpoint is the record of work actually in flight, and
without that a runtime request would look honoured and then silently stop at the next restart.

This algorithm is implemented **once**, in `IncrementalSnapshotDriver`. A connector supplies
only the database-specific half through `IncrementalSnapshotBackend`: the position type, the
watermark query, the chunk read, event position extraction, and the offset encoding. The three
built-in connectors go through that interface, and so can yours — see
[Incremental snapshot for a custom source](@/docs/api.md#incremental-snapshot-for-a-custom-source).

## Source-Specific Notes

### PostgreSQL

A PostgreSQL stream opens **two** connections, for two different jobs: `tokio-postgres` for
ordinary SQL (slot and publication introspection, snapshot chunk reads, catalog lookups), and
rustcdc's own replication client for the WAL stream. Both derive their TLS configuration from
the same `TransportConfig`, so they cannot disagree about what they verify or whether they are
encrypted.

#### WAL transport

`WalTransport::StreamingReplication` (the default) runs `START_REPLICATION ... LOGICAL` over the
streaming replication protocol — the mechanism PostgreSQL's own subscribers and `pg_recvlogical`
use. The server pushes WAL as it is written, and progress is reported back with Standby Status
Updates.

- rustcdc implements the wire protocol itself (`source::postgres::wire`): startup, the TLS
  upgrade, SCRAM-SHA-256 / MD5 / cleartext authentication, the `CopyBoth` loop, and feedback.
  `tokio-postgres` exposes no `CopyBoth` or replication-mode API, so the protocol is
  unreachable through it; the published crate that does implement it declares `rustls` without
  `default-features = false`, which would force rustls's `aws-lc-rs` provider across the whole
  build beside the `ring` backend this crate standardises on — and Cargo unifies features, so a
  dependent cannot opt out. Roughly 900 lines of stable, well-specified protocol was the cheaper
  side of that trade.
- `proto_version '1'` is negotiated, matching what the pgoutput decoder implements. Asking for
  more would make the server send v2 streaming and v3 two-phase messages the decoder
  deliberately rejects rather than silently mishandles.
- Framing is **buffered**, and a poll's time budget wraps only the socket fill, never frame
  decoding. Reading fields straight off the socket under a timeout is not cancel-safe: a budget
  expiring between a message's tag and its payload discards bytes that have already left the
  kernel, and every later read is then misaligned — a permanent, silent desynchronisation.
- A poll blocks for the **first** record, then takes only what is already buffered. Waiting the
  full budget once data has arrived would make every record wait for the last one, turning a
  push transport back into a polling one.
- `confirm_lsn` sends its Standby Status Update immediately when the position advances rather
  than deferring to the status interval. `confirmed_flush_lsn` is what releases WAL, and that is
  the one thing a replication slot must not sit on.

`WalTransport::SqlPeek` reads the same slot with `pg_logical_slot_peek_binary_changes` over an
ordinary connection. It needs neither the `REPLICATION` role attribute nor a direct connection,
which makes it the fallback where those cannot be arranged — at a real cost:

- the peek is **non-consuming**: `pg_logical_slot_get_changes_guts` begins reading at the slot's
  `restart_lsn` on every call and only *emits* past `confirmed_flush_lsn`, so each poll re-reads
  from the slot's restart point rather than continuing. A long-running transaction on the source
  pins `restart_lsn` and widens that span, so it is worth watching against `pg_current_wal_lsn()`
  — though how expensive the re-read becomes depends on whether that WAL is still cached, which
  [measured performance](@/docs/reliability-testing.md#measured-performance) does not settle
- delivery latency is bounded by the poll interval rather than pushed by the server. **Note
  this does not make it slower in every case:** measured on a small backlog with no
  long-running transaction, `SqlPeek` shows a *lower* p50 than streaming, because it polls in
  tight, small batches. Its disadvantage is structural rather than constant — the re-scan cost
  grows with the gap between `restart_lsn` and `confirmed_flush_lsn`, and that gap is set by
  the source's workload, not by rustcdc. See
  [measured performance](@/docs/reliability-testing.md#measured-performance)
- each poll is bounded by a server-side `statement_timeout` so the connection is always returned
  to a ready state, and a poll that exceeds its budget **halves** the decode window rather than
  repeating identical work. A timed-out peek is explicitly *not* reported as an idle slot — the
  opposite is true — so it can never be mistaken for permission to advance the slot
- startup repairs a checkpoint sitting ahead of `confirmed_flush_lsn` with
  `pg_replication_slot_advance`. Streaming replication needs no equivalent, because the resume
  LSN is a `START_REPLICATION` parameter — and attempting it there would fail anyway, since
  PostgreSQL refuses to advance a slot an active walsender holds

The two transports share the decoder, event construction, table filtering and checkpointing;
only the route the bytes take differs. `tests/postgres_wal_transport_parity_integration.rs`
captures one workload through both and asserts the resulting events — including each change's
LSN — are identical, so their checkpoints stay interchangeable.
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
