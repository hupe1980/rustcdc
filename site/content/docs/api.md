+++
title = "API guide"
description = "The rustcdc embedding model: runtime lifecycle, acknowledgement semantics, transforms, codecs and schema registries."
weight = 30
+++

This document is the primary API reference for embedding rustcdc in Rust applications.

## Audience

This guide is for engineers integrating rustcdc as a library and building custom runtime loops.

## API Surface

The core embedder API is centered on:

- `RuntimeConfig` for runtime construction
- `CdcRuntime` for lifecycle and event delivery
- `RuntimeSourceConfig` for source selection
- `EventBatch` and `AckMode` for loss-safe delivery semantics

## Runtime Construction

`RuntimeConfig` binds four concerns:

- source connector configuration
- checkpoint backend
- schema history backend
- runtime options and observability

Typical shape:

```rust
use rustcdc::{
  checkpoint::InMemoryCheckpoint,
  IdempotencyOptions,
  schema_history::InMemorySchemaHistory,
  RuntimeConfig,
  RuntimeSourceConfig,
};

# fn example() -> rustcdc::Result<()> {
let checkpoint = InMemoryCheckpoint::default();
let schema_history = InMemorySchemaHistory::default();

let config = RuntimeConfig::new(
  RuntimeSourceConfig::Disabled,
  checkpoint,
  schema_history,
)
.with_max_buffer_size(10_000)
.with_idempotency(IdempotencyOptions::new(100_000)?)
.with_max_poll_wait_ms(500);

// InMemoryCheckpoint is for tests and local development only.
// Use FileCheckpoint (or a custom durable backend) in production.

// Runtime duplicate suppression is enabled by default.
// Use this only when you need to opt out explicitly.
let config_without_dedup = RuntimeConfig::new(
  RuntimeSourceConfig::Disabled,
  InMemoryCheckpoint::default(),
  InMemorySchemaHistory::default(),
)
.with_idempotency_disabled();
# let _ = (config, config_without_dedup);
# Ok(())
# }
```

Durable schema history for restart resilience:

```rust
use rustcdc::{
  checkpoint::InMemoryCheckpoint,
  schema_history::FileSchemaHistory,
  RuntimeConfig,
  RuntimeSourceConfig,
};

async fn durable_schema_history_config() -> rustcdc::Result<()> {
  let checkpoint = InMemoryCheckpoint::default();
  let schema_history = FileSchemaHistory::new("/var/lib/rustcdc/schema-history.json").await?;

  let _config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
  Ok(())
}
```

## Runtime Lifecycle

The canonical lifecycle is:

1. create runtime with `CdcRuntime::new`
2. start runtime with `start()`
3. read batches with `poll_event_batch()` or `event_batches()`
4. acknowledge durable progress with `commit_ack()`
5. stop runtime with `stop()`

Minimal lifecycle example:

```rust
use rustcdc::{CdcRuntime, Result, RuntimeConfig, RuntimeSourceConfig};
use rustcdc::checkpoint::InMemoryCheckpoint;
use rustcdc::schema_history::InMemorySchemaHistory;

async fn run_once() -> Result<()> {
  let checkpoint = InMemoryCheckpoint::default();
  let schema_history = InMemorySchemaHistory::default();
  let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);

  let mut runtime = CdcRuntime::new(config)?;
  runtime.start().await?;

  let batch = runtime.poll_event_batch().await?;
  runtime.commit_ack(batch.ack_mode()).await?;

  runtime.stop().await?;
  Ok(())
}
```

## Source Selection

`RuntimeSourceConfig` selects the source connector at runtime:

- `Postgres(PostgresSourceConfig)`
- `Mysql(MysqlSourceConfig)`
- `MariaDb(MariaDbSourceConfig)`
- `SqlServer(SqlServerSourceConfig)`

Prefer the associated constructors when building embedder code for readability:

`RuntimeSourceConfig::postgres(...)`
`RuntimeSourceConfig::mysql(...)`
`RuntimeSourceConfig::mariadb(...)`
`RuntimeSourceConfig::sqlserver(...)`
`RuntimeSourceConfig::disabled()`

Source configuration in library code is explicit and typed; environment parsing
belongs in host applications or examples that map `CDC_RS_*` variables into
connector config values.

The runtime also exposes connector capability metadata via `source_capabilities()` and validates incompatible settings (for example, snapshot tables for a source that does not support snapshots). Capability metadata includes `snapshot_checkpoint_resume`, which is `true` for PostgreSQL, MySQL, and SQL Server. Snapshot checkpoints now resume through connector-native cursor state and keep stream bootstrap aligned with the saved snapshot watermark.

## Event Model

`Event` is the canonical envelope consumed by downstream code.

Key fields include:
- `op`: one of `Insert`, `Update`, `Delete`, `Read`, `SchemaChange`, `Truncate`
- `source`: source metadata and offset context
- `transaction`: optional transaction metadata
- `snapshot`: optional snapshot metadata

`Operation::Truncate` is emitted when a `TRUNCATE` statement removes all rows from a table.
`before` and `after` are always `None` for truncate events. Only connectors that advertise
`ConnectorCapabilities::truncate` emit this variant (PostgreSQL, MySQL, MariaDB, and
SQL Server when `capture_truncate_events` is enabled).
`Operation::to_str()` returns a `&'static str` for zero-allocation display and comparison.

The event envelope is designed to support stable replay and source-agnostic processing.

### Constructing events

`Event`, `SourceMetadata`, `SnapshotMetadata` and `TransactionMetadata` are
`#[non_exhaustive]`, so code outside this crate builds them through constructors rather than
struct literals:

```rust
use rustcdc::{Event, Operation, SourceMetadata};
use serde_json::json;

let event = Event::builder("users", Operation::Insert)
    .source(SourceMetadata::new("postgres", "0/16B2E48", 1_700_000_000_000))
    .schema("public")
    .after(json!({ "id": 1, "email": "a@example.com" }))
    .primary_key(["id"])
    .ts(1_700_000_000_000)
    .build();

assert!(event.validate().is_ok());
```

This is not ceremony for its own sake. Without `#[non_exhaustive]` plus a builder, every field
added to the envelope is a breaking change at every construction site — including the ones in
your code and in this crate's own adapter SDK examples. With them, adding a field is a
minor-version change.

The builder also sets `envelope_version` for you. Writing that constant by hand is not a
compile error but makes the event fail validation at the far end of the pipeline, which is a
poor place to learn about it.

Use `build_validated()` at a source boundary to enforce the envelope contract where the event
is produced:

```rust
# use rustcdc::{Event, Operation, SourceMetadata};
# fn example() -> rustcdc::Result<()> {
let event = Event::builder("users", Operation::Insert)
    .source(SourceMetadata::new("postgres", "0/16B2E48", 1))
    .after(serde_json::json!({ "id": 1 }))
    .ts(1)
    .build_validated()?;
# let _ = event;
# Ok(())
# }
```

### Column values are text, on every connector and every path

**Every scalar column value is a JSON string. SQL `NULL` is JSON `null`.** One rule, with no
exceptions for connector or capture path.

```json
{"id": "42", "amount": "12345678901234.5678", "active": "t", "notes": null}
```

Read them accordingly:

```rust
# use serde_json::Value;
fn as_i64(value: Option<&Value>) -> Option<i64> {
    value.and_then(Value::as_str).and_then(|text| text.parse().ok())
}
# let _ = as_i64(None);
```

#### Why text rather than typed JSON

**It is the lossless form.** A JSON number is an IEEE-754 double by the time most consumers
see it. `numeric(38,4)` loses its low digits and `bigint` above 2^53 is corrupted outright —
silently, in the value rather than in the type, which is the hardest kind of corruption to
notice. The text form carries `9223372036854775807` and `12345678901234.5678` exactly.

**It is the form the source itself produces.** Every value here is rendered by the column
type's own output function — the same function PostgreSQL's `pgoutput` calls, which is why a
`boolean` is `"t"` and not `"true"`. A snapshot and the live stream therefore agree character
for character, not merely on the JSON type.

**It used to differ by path, and that was a defect.** A row backfilled by a PostgreSQL
snapshot arrived as `{"id": 1}` while the same row updated a moment later arrived as
`{"id": "1"}`, because the chunk read went through `row_to_json` and the stream did not. A
sink reaching for `as_i64()` read one and silently saw `None` for the other. MySQL emitted
JSON numbers from both paths, and SQL Server's `FOR JSON PATH` payload was parsed through
`f64` — which is why its type-fidelity assertions had to be written as prefix matches rather
than equalities. All three are now the same rule, and
`tests/postgres_value_representation_integration.rs` asserts it end to end.

Structured columns are the one carve-out: a `json`/`jsonb` column keeps its object or array
structure rather than being flattened to a string, because flattening would destroy
information a sink can use.

### Partial payloads — read this before writing a sink

Some events do not carry a complete row. Applying one as if it were complete is the classic
CDC corruption: you write `NULL` over a column that never changed. **The library gives you an
API that cannot express that write — use it instead of reading `after` directly.**

#### `Event::row_write()` — the safe path

`row_write()` folds the payload, the missing columns and the primary key into the single
write that is correct for the event:

```rust
use rustcdc::{Event, RowWrite};
# struct Sink;
# impl Sink {
#     fn replace(&self, _key: Option<serde_json::Value>, _row: &serde_json::Value) {}
#     fn update_only(&self, _key: serde_json::Value, _columns: &serde_json::Value) {}
#     fn delete(&self, _key: serde_json::Value) {}
#     fn truncate(&self) {}
# }
# fn log_unwritable(_reason: rustcdc::NoRowWrite) {}
# fn example(event: &Event, sink: &Sink) {
match event.row_write() {
    // The payload is complete. Write every column.
    RowWrite::Replace { key, row } => sink.replace(key, row),

    // The payload is INCOMPLETE. Write only `columns`; leave every other column in the
    // target row untouched. In SQL: `UPDATE ... SET <columns> WHERE <key>` — never an
    // upsert built from the full column list.
    RowWrite::Merge { key, columns, .. } => sink.update_only(key, columns),

    RowWrite::Delete { key } => sink.delete(key),
    RowWrite::Truncate => sink.truncate(),

    // DDL, or no addressable row (no primary key). Nothing to write.
    RowWrite::None { reason } => log_unwritable(reason),
    _ => {}
}
# }
```

`RowWrite::Merge` hands you only the columns that are actually present, so there is no
placeholder value available to write by mistake. `RowWrite::is_partial()` returns `true` for
exactly that variant — branch on it if your sink cannot express a partial update (an
append-only file, a whole-document replace).

The enum is `#[non_exhaustive]`; match with a wildcard arm.

#### A partial key is never offered as a key

`Event::primary_key_values()` — the function `row_write()` derives `key` from, and the one that
produces message keys and idempotency fingerprints — is **all-or-nothing**. If any column named
in `primary_key` is missing from the row image, it returns `None` rather than a key built from
the columns that are present.

That matters most for composite keys. Given `primary_key = ["tenant_id", "id"]` and a payload
carrying only `tenant_id`, a truncated key of `{"tenant_id": 7}` looks entirely valid and
addresses **every row of that tenant**. A sink turning it into `DELETE FROM t WHERE tenant_id =
7` deletes the whole tenant; an upsert collapses the tenant onto one row. Both are silent, and
neither is recoverable from the event stream. As a message key it silently merges distinct rows
into one log-compaction group.

So instead you get `RowWrite::None { reason: NoRowWrite::MissingPrimaryKey }`, which your sink
has to handle explicitly. A visible gap beats an invisible over-write.

The transform pipeline enforces the same thing from the other side: a stage that removes,
renames, or rewrites *any* key column — an `include_columns` projection that omits one, a field
rename, `MaskRule::Encrypt` on a key — is rejected with an error naming the stage, rather than
emitting the record unkeyed.

#### The underlying fields

`row_write()` is derived from these. Read them directly only if you need finer control.

##### `unavailable_columns: Vec<String>`

Columns that exist on the table but whose value the source **could not supply** for the
`after` image. They are **absent** from `after` — not `null`. Without this list the two are
indistinguishable.

The concrete case is **PostgreSQL unchanged-TOAST**: when a large value (roughly >8 KB —
`text`, `bytea`, `jsonb`) is not modified by an `UPDATE`, PostgreSQL omits it from the WAL
entirely and pgoutput sends a `'u'` placeholder. The value is unrecoverable; reading it back
out-of-band would race concurrent writes and return a value from a different point in time.

> **One exception, and it is not an out-of-band read.** During an incremental snapshot, an event
> inside a chunk's watermark bracket is repaired from *that chunk's own image* of the row and
> arrives with this list empty — see
> [a complete chunk row suppressed by an incomplete event](@/docs/architecture.md#a-complete-chunk-row-suppressed-by-an-incomplete-event).
> The driver knows the position its read was taken at and knows every write between that read
> and the event, which is precisely what a fresh read does not. Outside that window the
> paragraph above stands, so a sink must still handle `RowWrite::Merge`.

> ⚠️ **`REPLICA IDENTITY FULL` does not avoid this.** Replica identity governs the *old*
> tuple only. The after-image still omits unmodified TOASTed values under every replica
> identity setting. `FULL` gives you a complete before-image (see `before_is_key_only`
> below); it does not make `after` complete.

This is also why the failure mode is so late-breaking: it only begins once rows cross the
TOAST threshold, typically long after the pipeline was validated against small test rows.

##### `before_unavailable_columns: Vec<String>`

The same thing for the `before` image, tracked **separately**, because the two sets are not
the same. A TOASTed column that *was* modified arrives present in `after` and absent from
`before`. Merging the lists would mark a column that genuinely changed as unwritable and
silently drop the update — so they are never merged.

**Every codec carries all three fields.** JSON, Avro, Protobuf and CloudEvents each emit
`before_is_key_only`, `unavailable_columns` and `before_unavailable_columns`, and the Avro and
Protobuf decoders read them back, so a consumer's contract does not depend on which output format
it reads. Until 0.12.0 the CloudEvents encoder omitted `before_unavailable_columns`, which left its
consumers unable to tell a TOASTed before-image column from a genuine `NULL` while consumers of the
same stream in another format could.

Only relevant if you consume the before-image (computing diffs, building compensating
writes). A column listed here had *some* prior value; the source could not report it. Do not
read its absence as "was NULL".

##### `before_is_key_only: bool`

`true` when `before` holds only the primary-key columns rather than a complete pre-image.
Occurs on PostgreSQL `UPDATE`/`DELETE` when the table's `REPLICA IDENTITY` is `DEFAULT` (the
PostgreSQL default). Code that computes row diffs or needs full prior state must check this —
when `true`, `before` is not a row snapshot. Set `REPLICA IDENTITY FULL` on the table to get a
complete before-image, at the cost of larger WAL volume and therefore more replication-slot
retention pressure.

`before_unavailable_columns` is always empty when this is `true`: a key-only before-image
omits its non-key columns by design, not because of TOAST.

## Delivery And Acknowledgement Semantics

`poll_event_batch()` returns an `EventBatch` that contains events and an `AckMode`.

```rust,ignore
// Shape only — `AckMode` is defined by the crate, not by your code.
pub enum AckMode {
    Required(AckToken),   // must commit; skipping risks replay on restart
    NotRequired,          // empty batch or disabled source; commit_ack is a no-op
}
```

Correct processing sequence:

1. consume events in batch order
2. durably commit sink side effects
3. call `commit_ack(batch.ack_mode())`

`commit_ack` accepts `impl Into<AckMode>` — passing `AckMode::NotRequired` is a documented zero-cost no-op. Raw `AckToken` values are also accepted (via `From<AckToken>`).

### Let the runtime drive the loop

If the sink is an `impl SinkAdapter` and the write does not have to be coordinated with
anything the runtime cannot see, register it and hand over the loop:

```rust
use rustcdc::{CdcRuntime, RuntimeConfig, sink::StdoutSink};
use rustcdc::CancellationToken;

# async fn example(config: RuntimeConfig, shutdown: CancellationToken) -> rustcdc::Result<()> {
let mut runtime = CdcRuntime::new(config)?;
runtime.register_sink(StdoutSink::new());
runtime.start().await?;

let delivered = runtime.run_to_completion(shutdown).await?;
println!("delivered {delivered} events");

runtime.stop().await?;   // closes the registered sink
# Ok(())
# }
```

`run_to_completion` is exactly `poll → send → flush → acknowledge`, and the value of having
it in the library is the *order*. Acknowledging before the flush advances the durable
checkpoint past events the sink never accepted; a crash in that gap loses them, with no error
raised anywhere and no way to recover them. It is one line to get wrong and it fails months
later as rows that are simply missing.

It returns when the token is cancelled, and on the first source, transform, sink or
checkpoint error. A batch that failed mid-delivery is not acknowledged, so it is redelivered
by the next poll — retrying is calling `run_to_completion` again.

Every batch is flushed, and that is not tunable: the acknowledgement cannot outrun the flush
without giving up the guarantee, so a rarer flush would mean a rarer acknowledgement and a
growing redelivery window. Batch *inside* the sink instead, and raise `max_buffer_size` to
hand it more events per call.

Keep the manual loop when the write must be coordinated with something the runtime cannot see
— your own transaction, a two-phase commit, per-branch error handling across a fan-out.

### Cancellation: never race the poll

`poll_event_batch` is **not cancel-safe**. Between the source handing over a batch and the
runtime staging it, the events exist only inside the future — it awaits the durable
schema-history write and the transform pipeline first — so dropping it there discards events
that have already left the source. Nothing acknowledges them, so a *restart* replays them, but
a runtime that keeps polling never sees them again; events added by `enqueue_event` have no
source to replay from at all.

`tokio::select!` is the obvious way to add a shutdown signal or a control channel, which is
what makes this worth stating: the hazard is invisible at the call site.

```rust,ignore
// WRONG — drops a batch whenever the token fires mid-poll.
tokio::select! {
    _ = shutdown.cancelled() => break,
    batch = runtime.poll_event_batch() => { /* … */ }
}

// RIGHT — the poll runs to completion; shutdown costs at most `max_poll_wait_ms`.
while !shutdown.is_cancelled() {
    let batch = runtime.poll_event_batch().await?;
    // …
}
```

`run_to_completion` and `event_batches_cancellable` both check the token between polls, so a
cancellation token handed to either is already safe.

### Committing part of a batch

When a sink accepted some of a batch and not the rest, narrow the token:

```rust,ignore
let accepted = token.accept_prefix(written)?;
runtime.commit_ack(accepted).await?;
```

The checkpoint advances exactly `written` events. The tail is **redelivered** by the next
`poll_event_batch` with a fresh token, which is the only way to obtain one.

Each token may be committed **once**. `AckToken` is `Clone` and `EventBatch::ack_mode()`
returns a fresh copy on every call, so a second commit of the same token used to match the
delivery id, see a shorter remaining prefix, and advance the checkpoint over the *next* events
— which the caller had never been handed. That is now refused with an error naming the cause.

### The checkpoint records a boundary, not the last event's position

An event's `source.offset` identifies **the change**. The position a restart resumes from is
the first position **not** consumed. For a log whose decoder filters at transaction
granularity those are different, and conflating them costs a guaranteed duplicate on every
restart rather than a possible one.

PostgreSQL is the case in point. Each change keeps its own WAL position, but
`START_REPLICATION ... X` re-sends every transaction whose *commit record* sits at or after
`X` — and a change's LSN always precedes its own transaction's commit record. Resuming from
one therefore replays that whole transaction, deterministically, with no writes on the source
at all. Nudging the LSN forward does not help; the commit record is still ahead of `X + 1`.

The connector answers this through `StreamHandle::resume_offset_for`, which for PostgreSQL
returns the COMMIT message's `end_lsn` — the position *after* the commit record. The runtime
uses it for the durable checkpoint and for the source-side confirmation, so slot retention and
restart duplicates move together. MySQL and SQL Server need no override: binlog `log_pos` is
already the next event's position, and the SQL Server window query already increments the LSN.

A custom source whose per-event offset is itself a resumable boundary can leave the default
alone. One that overrides it gets the same treatment as the built-in connectors: the runtime
checkpoints whatever it returns, **verbatim**, on every source path rather than only the
PostgreSQL one. That last part was a bug until 0.12.0 — the generic branch read
`event.source.offset` directly, so a third-party connector implementing this correctly had its
answer discarded and took the duplicate-per-restart anyway, with nothing to see it by.

Important semantics:
- not acknowledging after sink durability may replay already-delivered events
- `stop()` fails fast if uncommitted events remain in-flight
- `force_stop()` is intended for emergency drain where replay is acceptable; emits a `WARN` log with `shutdown_mode = "forced"`
- `drain_and_stop()` polls until the source is exhausted then stops cleanly
- process termination without `stop()` can replay the in-flight batch on restart (at-least-once)
- source confirmation failures after durable checkpoint commit now fail fast by default (`PostCommitSourceConfirmPolicy::FailFast`)

To preserve pre-existing availability-biased behavior, opt into continue mode explicitly:

```rust
use rustcdc::PostCommitSourceConfirmPolicy;
# use rustcdc::{checkpoint::InMemoryCheckpoint, schema_history::InMemorySchemaHistory,
#     RuntimeConfig, RuntimeSourceConfig};
# let config = RuntimeConfig::new(
#     RuntimeSourceConfig::Disabled,
#     InMemoryCheckpoint::default(),
#     InMemorySchemaHistory::default(),
# );

let config = config.with_post_commit_source_confirm_policy(
  PostCommitSourceConfirmPolicy::Continue,
);
# let _ = config;
```

### Sink-Side Idempotency Guard

For at-least-once replay tolerance, rustcdc now provides a built-in
`EventIdempotencyGuard` helper for consumer loops.

```rust
use rustcdc::{EventIdempotencyGuard, Result};

async fn process_batch(events: &[rustcdc::Event]) -> Result<usize> {
  let mut guard = EventIdempotencyGuard::new(100_000)?.with_ttl_ms(60_000)?;
  let mut applied = 0usize;

  for event in events {
    if !guard.should_process(event)? {
      continue;
    }
    // apply sink side-effect here
    applied += 1;
  }

  Ok(applied)
}
```

The fingerprint includes source position, transaction sequence metadata, and
payload shape so events that share coarse offsets remain distinguishable.

### Idempotency guard safety

The fingerprint is content-derived, so two genuinely distinct rows that happen to be
byte-identical hash identically. That is not hypothetical: an audit or event-log table with no
primary key can legitimately contain `INSERT INTO pings VALUES ('ok'), ('ok')`, and on a
connector that supplies no intra-transaction sequencing both rows share one source offset.
Suppressing the second is permanent, silent data loss — the checkpoint advances past it and
nothing downstream can recover it.

The guard therefore suppresses only events it can **identify**: those carrying transaction
metadata (`tx_id` + `event_index`) or a primary key whose columns are actually present in the
row image. Everything else passes through and is counted in
`EventIdempotencyGuard::unidentifiable_passthrough_count()` (and
`RuntimeAdminSnapshot::idempotency_unidentifiable_passthrough`).

Passing a duplicate through is at-least-once — the guarantee the pipeline already documents.
Dropping a distinct row is not recoverable by anyone.

Evictions are counted too (`eviction_count()`): a steadily growing value means the window is
too small for this deployment's replay distance, so duplicates older than the window stop being
suppressed. Delivery stays correct; a sink relying on the guard will start seeing repeats.
Raise `IdempotencyOptions::capacity`.

### Restart Replay Window

**The in-memory idempotency guard resets on every process restart.** It provides
within-session deduplication only, not cross-restart deduplication.

On restart the runtime replays all events between the last durable checkpoint and
the current source position. Events in this window that were already delivered
before the crash **will be re-delivered** and will not be detected as duplicates
by the in-memory guard (because its state was lost).

**Implications:**

- The replay window size is bounded by your commit frequency. Committing after
  each batch keeps the window small.
- For sink operations that are naturally idempotent (upsert-by-PK, conditional
  inserts, etc.) this is safe to ignore.
- For non-idempotent sinks (append-only log ingest, payment triggers, etc.) you
  **must** provide cross-restart deduplication at the sink layer.

**Cross-restart deduplication pattern:**

Use `fingerprint_event_stable` (SHA-256 based, deterministic across process
restarts) and persist seen fingerprints in your sink's storage:

```rust
use rustcdc::fingerprint_event_stable;
# use rustcdc::{Event, Result};
# async fn sink_has_seen(_fp: &str) -> Result<bool> { Ok(false) }
# async fn sink_write(_event: &Event) -> Result<()> { Ok(()) }
# async fn sink_mark_seen(_fp: &str) -> Result<()> { Ok(()) }
# async fn example(event: &Event) -> Result<()> {
// On each delivered event:
let fingerprint = fingerprint_event_stable(event)?; // Result<String, FingerprintError>
if !sink_has_seen(&fingerprint).await? {
    sink_write(event).await?;
    sink_mark_seen(&fingerprint).await?;
}
# Ok(())
# }
```

Unlike `fingerprint_event_transient` (which uses a per-process random seed),
`fingerprint_event_stable` produces the same fingerprint for the same event
across restarts, making it safe to persist and check against a durable store.

## Streaming Consumption

`event_batches()` provides a stream-based consumption model for non-empty batches.

```rust
use futures_util::StreamExt;
# use rustcdc::{CdcRuntime, Result};
# async fn example(runtime: &mut CdcRuntime) -> Result<()> {
let mut batches = runtime.event_batches();
while let Some(batch) = batches.next().await {
  let batch = batch?;
  // `event_batches` borrows the runtime for the life of the stream, so the ack
  // goes through the batch's own token rather than through `runtime`.
  let _ = batch;
}
# Ok(())
# }
```

Note the borrow: `event_batches()` holds `&mut runtime` for as long as the stream
lives, so `commit_ack` cannot be called on `runtime` inside the loop. Use
`poll_event_batch()` in a plain loop when you need to acknowledge inline:

```rust
# use rustcdc::{CdcRuntime, Result};
# async fn example(runtime: &mut CdcRuntime) -> Result<()> {
loop {
  let batch = runtime.poll_event_batch().await?;
  if batch.is_empty() {
    break;
  }
  // durably apply sink side effects here, then acknowledge
  runtime.commit_ack(batch.ack_mode()).await?;
}
# Ok(())
# }
```

For cooperative cancellation, use `event_batches_cancellable(token)` with a `CancellationToken`:

```rust
use rustcdc::CancellationToken;
use futures_util::StreamExt;
# use rustcdc::{CdcRuntime, Result};
# async fn example(runtime: &mut CdcRuntime) -> Result<()> {
let cancel = CancellationToken::new();
let mut batches = std::pin::pin!(runtime.event_batches_cancellable(cancel.clone()));
while let Some(batch) = batches.next().await {
  let _batch = batch?;
}
// cancel.cancel() from another task unblocks the stream cleanly
# Ok(())
# }
```

## Incremental Snapshot API (DBLog Pattern)

`CdcRuntime::start()` supports both classic snapshot + stream handoff and
runtime-driven incremental snapshot startup (when configured via
`with_incremental_snapshot(...)`).

If you want connector-managed non-blocking incremental snapshot behavior,
you can also start it directly from a connector connection via
`start_incremental_snapshot(...)`.

Tables can be added to a snapshot **while the pipeline runs** with
`CdcRuntime::request_incremental_snapshot(tables)` — no restart, and no signal table in the
source, so it works against a read-only role. It can also be paused, resumed and stopped while
it runs, and driven from another task through `CdcRuntime::control_handle()`. See
[on-demand snapshots](@/docs/config-reference.md#on-demand-snapshots).

### Observing progress

`CdcRuntime::incremental_snapshot_state()` takes `&self` and returns the live driver state —
snapshot id, and per table the keyset cursor, completion flag, chunk and row counters, plus
whether it is paused. It is also on `RuntimeAdminSnapshot::incremental_snapshot`.

```rust
# use rustcdc::CdcRuntime;
# fn example(runtime: &CdcRuntime) {
if let Some(state) = runtime.incremental_snapshot_state() {
    println!(
        "snapshot {}: {} rows, {} table(s) remaining{}",
        state.snapshot_id,
        state.rows_emitted(),
        state.tables_remaining(),
        if state.paused { " (paused)" } else { "" },
    );
}
# }
```

The same reading is available without `&mut` access at all through
`RuntimeControl::incremental_snapshot_state()`, which is what an admin endpoint running
alongside the poll loop needs.

```rust
# #[cfg(feature = "postgres")]
# mod example {
use rustcdc::{
  IncrementalSnapshotConfig, PostgresConnection, PostgresSourceConfig, Result,
};

pub async fn start_incremental_stream(config: PostgresSourceConfig) -> Result<()> {
  let mut connection = PostgresConnection::new(config);
  connection.connect().await?;

  let incremental = IncrementalSnapshotConfig::new(vec!["public.users".to_string()])
    .with_chunk_size(1_000);

  let mut stream = connection
    .start_incremental_snapshot(incremental, None)
    .await?;

  let _events = stream.next_events(5_000).await?;
  Ok(())
}
# }
```

This connector-level API is also available for MySQL and SQL Server via
`MysqlConnection::start_incremental_snapshot(...)` and
`SqlServerConnection::start_incremental_snapshot(...)`.

### Ordering: the chunk lands at the high watermark

The override window suppresses a chunk row whose key was modified between the two
watermarks, so the newer stream value wins. Events *past* the high watermark are
deliberately not suppressed — they committed after the `SELECT` finished, so they describe a
state the chunk cannot contain and the chunk row is still needed as the row's base state.

What that requires is that the chunk is emitted **at** the high watermark, ahead of any later
log event. DBLog gets this for free by emitting the buffered chunk the moment it reads the
high-watermark marker out of the log. rustcdc reads the log in batches, and one batch
routinely straddles the high watermark — it can carry an event at LSN 900 (inside the window)
and one at 1200 (past it) together.

So a straddling batch is split at the first event past the high watermark: the head is
delivered, the chunk follows, and the tail is delivered after it. Log order is preserved and
the chunk lands exactly where DBLog puts it. While the tail is held back the driver reports
**no** durable position, so those events cannot be marked consumed before they are delivered;
the snapshot rows in between become non-persistent barrier entries and the held-back events
carry the position forward with their own offsets a moment later.

Delivering the batch whole and the chunk afterwards would hand the consumer the 1200 value
first and the chunk's older value second — the exact stale-row resurrection the override
window exists to prevent, moved one step later.

### Resume across restarts

Each table is read in keyset-paginated chunks. The cursor for the last **fully emitted** chunk
is persisted inside the connector checkpoint offset — `PostgresOffset::incremental_snapshot`,
`MysqlOffset::incremental_snapshot`, `SqlServerOffset::incremental_snapshot` — so it becomes
durable in the same atomic, fsynced, checksummed write as the stream position.

That coupling is deliberate. A chunk cursor is only meaningful relative to the stream position
it was captured against; two separately-written records could disagree after a crash between
them.

Pass the loaded checkpoint offset to `start_incremental_snapshot(config, resume_from)` and the
snapshot continues from where it stopped. Resuming re-reads at most one chunk (the one in
flight when the process stopped), which is at-least-once and bounded by `chunk_size` — not by
table size. Without the persisted cursor, every restart re-reads every configured table from
row zero: a duplicate flood proportional to the whole dataset, repeating until a snapshot
happens to finish inside a single process lifetime.

`IncrementalSnapshotState` and `IncrementalSnapshotTableState` are public, so a custom
`Checkpoint` backend can store and return them. If the table's primary key has changed since
the checkpoint was written, the cursor's arity no longer matches and startup fails with a
message naming the remedy, rather than silently skipping rows.

## Transaction Boundaries

A delivered batch is cut on `max_buffer_size`, `max_event_bytes` and the commit barrier's free
capacity. None of those know anything about transactions, so by default a batch can end in the
middle of one: the sink sees rows 1–3 of a five-row transaction, commits them, and only later
receives rows 4–5. Between those two commits the sink holds a state that never existed in the
source database.

For most sinks that is fine, and it is why `TransactionBoundaryPolicy::Split` is the default —
lowest latency, strictly bounded memory. It is not fine for a sink that must apply each source
transaction atomically.

```rust
use rustcdc::{RuntimeOptions, TransactionBoundaryPolicy};

let options = RuntimeOptions::new()
    .with_transaction_boundary(TransactionBoundaryPolicy::PreserveTransactions);
# let _ = options;
```

Under `PreserveTransactions` the runtime withholds a trailing partial transaction from each
batch and delivers it with the next one, so every batch ends on a boundary.

**How the runtime knows a transaction ended.** Two signals count, and nothing else does:
the event declares its own position (`event_index + 1 == total_events`), or a later event
belongs to a different transaction. Absence of a signal is not proof of an ending, so a
transaction whose remaining events have not arrived yet is **withheld** rather than
delivered partially — including when the rest is simply still in flight from the source,
which for a streaming connector is the normal case rather than the exception.


**The one case this cannot honour:** a single transaction larger than `max_buffer_size` does not
fit in any batch. Trimming it would produce an empty batch forever — a silent permanent stall,
strictly worse than the split it is trying to avoid. The runtime delivers such a transaction
split and logs a WARN naming the transaction id and `max_buffer_size`. Raise `max_buffer_size`
above the largest transaction your source produces if the guarantee must hold absolutely.

Events with no transaction metadata (snapshot rows, and connectors that do not report
transaction boundaries) are treated as their own boundary and are never trimmed.

## Custom Sources

`CdcRuntime::register_source` drives the runtime from any `impl Source`, including one this
crate does not ship:

```rust,ignore
let mut runtime = CdcRuntime::new(config)?;
runtime.register_source(Box::new(MyKafkaConnectSource::new(/* ... */)));
runtime.start().await?;
```

`Source::connect` and `Source::close` are trait methods with no-op defaults, so the runtime can
drive connection setup and teardown for a source it has never heard of. Everything else the
runtime provides applies unchanged: the commit barrier, checkpointing, transforms, the
idempotency guard, health verdicts, and metrics.

Two constraints:

* Call `register_source` **before** `start()`; the source is connected during `start()`.
* The runtime derives the checkpoint offset from the delivered event for the connectors it
  knows. For a custom source it persists `Event::source.offset` verbatim, so that field must be
  a complete, resumable position — the same string your `start_stream(resume_from)` can resume
  from. Implement `StreamHandle::position_offset` if you need to carry richer state.

### Incremental snapshot for a custom source

The DBLog watermark algorithm is source-agnostic — only the position type and the SQL dialect
differ — so it lives in one place, `IncrementalSnapshotDriver`, and connectors plug into it
through `IncrementalSnapshotBackend`. The three built-in connectors use exactly this interface;
there is no private path they take that yours cannot.

Implement five required methods and you inherit the state machine, the override window, cursor
persistence and the `StreamHandle` contract. A sixth,
[`event_in_bracket`](#classifying-an-event-against-the-bracket), has a
default — read the next section before accepting it:

```rust
use rustcdc::source::{
    ChunkRow, IncrementalSnapshotBackend, IncrementalSnapshotState, SnapshotTable,
};
use rustcdc::{Event, Offset, Result};
use async_trait::async_trait;
use std::collections::HashSet;

# struct MyBackend;
#[async_trait]
impl IncrementalSnapshotBackend for MyBackend {
    /// Whatever totally-ordered position your log uses.
    type Position = u64;

    async fn describe_table(&mut self, table_ref: &str) -> Result<SnapshotTable> {
        # unimplemented!()
        // Resolve "schema.table" against your catalog. Must reject a table with no
        // primary key: chunking without one cannot resume.
    }

    async fn current_position(&mut self) -> Result<u64> {
        # unimplemented!()
        // Read the log's current head. Called twice per chunk, so keep it cheap.
    }


    async fn fetch_chunk(
        &mut self,
        table: &SnapshotTable,
        cursor: Option<&[serde_json::Value]>,
        limit: usize,
    ) -> Result<Vec<ChunkRow>> {
        # unimplemented!()
        // Keyset-paginated read, ordered by primary key, OUTSIDE any transaction.
    }

    fn position_of_event(&self, event: &Event) -> Option<u64> {
        # unimplemented!()
        // Recover the log position of a live event.
    }

    fn offset_with_snapshot_state(
        &self,
        inner: &dyn Offset,
        state: IncrementalSnapshotState,
    ) -> Option<Box<dyn Offset>> {
        # unimplemented!()
        // Attach the snapshot state to your offset so it is checkpointed atomically
        // with the log position.
    }
}
```

#### Classifying an event against the bracket

The driver does not compare positions itself. It asks the backend:

```rust,ignore
fn event_in_bracket(
    &self,
    event: &Event,
    position: &Self::Position,
    low: &Self::Position,
    high: &Self::Position,
) -> BracketPosition   // Before | Inside | After
```

The default is the ordinal test — `Inside` when the position is past `low` and at or below
`high` — and that is correct when your watermark is a single ordered coordinate. Accepting it is a
real decision, not a formality, and here is why.

**Reaching the log is not the same as becoming visible.** Every engine commits in stages, and the
log position usually advances before the row is visible to a new snapshot:

| Source | Position advances at | Visible at | Consequence |
|---|---|---|---|
| PostgreSQL | WAL commit-record write | proc-array clear, after the flush | A transaction can sit *below* the low watermark and still be invisible to the chunk read |
| MySQL | binlog flush stage | InnoDB engine commit | Same shape |
| SQL Server | capture-job harvest into `cdc.*` | before the harvest | Watermark **lags** visibility — the safe direction, nothing to do |

Where the position leads visibility, the ordinal test answers `Before` for a transaction the chunk
read could not see. The chunk row is not suppressed, and its pre-image is emitted over the newer
value — silently.

**Each shipped connector answers it differently, which is the point of the hook:**

- **PostgreSQL** overrides it with the engine's own visibility rule. A snapshot is
  `(xmin, xmax, xip)`, and a transaction is invisible exactly when `xid >= xmax || xip.contains(xid)`.
  Both halves matter: `xmax` is `latestCompletedXid + 1`, so a lone in-flight transaction sits *at*
  `xmax` and never appears in `xip`. An earlier version tested `xip` alone and therefore missed
  precisely the transactions it was added to catch — `pg_current_snapshot()` on a single-writer
  database reports `733:733:` with 733 in flight.
- **MySQL** overrides it with executed-GTID **set difference**, because `Executed_Gtid_Set` is
  updated after the engine commit while the binlog coordinate advances before it. A GTID set is
  only *partially* ordered, so no `>` comparison could express this. Requires `gtid_mode = ON`;
  without it, the ordinal test and its documented residual window apply.
- **SQL Server** takes the default, because its watermark already lags visibility.

**Both bounds must come from the same notion of order.** Mixing them — a set-based lower bound with
an ordinal upper bound — is unsound in a way that is easy to reach: an event inside the ordinal
high bound but absent from the high watermark's set committed *after* that read, so suppressing it
would discard the newer value.

If you cannot classify a particular event, fall back to the default rather than guess. MySQL does
exactly that for an event with no GTID: treating a missing GTID as "not in `high`" would defer the
event past the chunk on no evidence.

### Health verdict

`RuntimeAdminSnapshot::health` is a `HealthVerdict` — the runtime's own answer to "is this
connector making progress?", which `RuntimeState` cannot give you (`state = Running` covers both
a quiet database and a hung socket).

```rust
use rustcdc::HealthVerdict;
# use rustcdc::CdcRuntime;
# fn example(runtime: &CdcRuntime) {
let snapshot = runtime.admin_snapshot();
match &snapshot.health {
    HealthVerdict::Healthy | HealthVerdict::Idle => { /* serve 200 */ }
    HealthVerdict::Stalled { reason } => {
        // `reason` names the condition and the remedy.
        eprintln!("cdc stalled: {reason}");
    }
    HealthVerdict::NotRunning => { /* not started, or stopped */ }
    _ => {}
}
# }
```

`RuntimeAdminSnapshot` also carries `idempotency_evictions` and
`idempotency_unidentifiable_passthrough`. The first tells you the dedup window is too small for
this deployment's replay distance (older duplicates stop being suppressed). The second counts
events the guard deliberately did **not** deduplicate because they carry neither transaction
metadata nor a resolvable primary key — see [Idempotency guard safety](#idempotency-guard-safety).

`HealthVerdict::is_alertable()` returns `true` for exactly `Stalled`, so a readiness handler can
gate on it without matching every variant. The enum is `#[non_exhaustive]` — match with a
wildcard arm.

`Stalled` is raised for an unconfirmed source position, a poll loop that has not completed within
`max_poll_wait_ms × 6` (floor 30s), or polled-but-uncommitted events with a stale last commit —
that last case meaning the embedder stopped calling `commit_ack`, not a source fault. See
[Operations Runbook](@/docs/runbook.md#health-verdict-idle-vs-stalled) for the alerting rules.

## Connection Retry Policy

For transient source connectivity failures, configure `ConnectionRetryPolicy` via
`RuntimeOptions::with_connection_retry` and pass the options to `RuntimeConfig::with_options`.
The runtime retries only recoverable errors — an unclassified `SourceError`, a `TimeoutError`,
or a classified source error whose `SourceErrorKind` is recoverable. `AuthFailed`,
`SchemaMismatch` and `SlotNotFound` are **not** retried: they need an operator, and retrying
only delays the page. Fatal configuration errors propagate immediately.

```rust
use rustcdc::{ConnectionRetryPolicy, RuntimeOptions};
# use rustcdc::{checkpoint::InMemoryCheckpoint, schema_history::InMemorySchemaHistory,
#     RuntimeConfig, RuntimeSourceConfig};
# let config = RuntimeConfig::new(
#     RuntimeSourceConfig::Disabled,
#     InMemoryCheckpoint::default(),
#     InMemorySchemaHistory::default(),
# );

let policy = ConnectionRetryPolicy {
    max_retries: Some(5),       // None = retry indefinitely
    initial_delay_ms: 300,
    max_delay_ms: 10_000,
};

let config = config.with_options(RuntimeOptions::new().with_connection_retry(policy));
# let _ = config;
```

Defaults: 5 retries, 300 ms initial delay, 10 s cap, truncated exponential backoff.
Set `max_retries: None` for indefinitely-retrying long-running pipelines.

## Transform Configuration

`FilterProjectionTransform::new(config)` returns `Result<Self>` — configuration
errors (for example empty filter values) are caught at construction time rather
than silently at apply time.

```rust
use rustcdc::transform::{
  FilterField, FilterOperator, FilterProjectionConfig, FilterProjectionTransform, FilterRule,
};

# fn example() -> rustcdc::Result<()> {
let transform = FilterProjectionTransform::new(FilterProjectionConfig {
    // `filters` is a Vec combined per `filter_mode` (default: all must match).
    filters: vec![FilterRule::new(FilterField::Op, FilterOperator::Eq, "insert")],
    include_columns: Some(vec!["id".into(), "email".into()]),
    exclude_columns: None,
    ..FilterProjectionConfig::default()
})?;  // returns Err(ConfigError) for invalid filter values
# let _ = transform;
# Ok(())
# }
```

### Content-based filtering

`FilterField::AfterField(path)` and `FilterField::BeforeField(path)` match
against fields inside the event payload using a dot-separated path (e.g. `"user.country"`).

Available operators: `Eq`, `Ne`, `Contains`, `Regex`, `Lt`, `LtEq`, `Gt`, `GtEq`.

```rust
# use rustcdc::transform::{FilterField, FilterOperator, FilterRule};
// Keep only events where after["status"] == "active"
let by_status =
    FilterRule::new(FilterField::AfterField("status".into()), FilterOperator::Eq, "active");

// Keep events where after["amount"] > 100
let by_amount =
    FilterRule::new(FilterField::AfterField("amount".into()), FilterOperator::Gt, "100");

// Keep events matching a regex on after["email"]
let by_email = FilterRule::new(
    FilterField::AfterField("email".into()),
    FilterOperator::Regex,
    r"@example\.com$",
);
# let _ = (by_status, by_amount, by_email);
```

`FilterOperator::Regex` patterns are compiled once at construction; invalid
patterns return `Err(ConfigError)` at `FilterProjectionTransform::new` time.

### Sensitive-data masking (`MaskHashTransform`)

The available rules are `MaskRule::UnsaltedSha256`, `Redact(String)`, `Null`, `Truncate(usize)`
and `Passthrough`, plus `HmacSha256(SecretString)`, `Encrypt(..)` and `Decrypt(..)` behind the
`encryption` feature. (There is no `MaskRule::Hash`.)

> **⚠ GDPR / privacy warning**
>
> `MaskRule::UnsaltedSha256` is, as the name says, **deterministic and unsalted**.  For
> low-cardinality fields (e.g., `gender`, `country_code`, `status`) or any
> field whose values are enumerable, an attacker can reverse the hash via a
> pre-computed lookup table.  **Do not rely on it alone for GDPR
> pseudonymization compliance.**
>
> Recommended approaches for GDPR-compliant pseudonymization:
> - Use **`MaskRule::HmacSha256(secret)`** — a keyed hash with a site-specific secret.
>   This is the shipped, supported way to get salted pseudonymization; do not hand-roll it
>   by pre-hashing `format!("{secret}:{value}")`.
> - Use `MaskRule::Encrypt` (AES-256-GCM) when you need reversible but opaque tokens.
>   Ciphertexts are bound to `table + JSON path` as associated data, so a value relocated to
>   another column or row fails authentication rather than decrypting as authentic. The wire
>   format is `enc:v1:<nonce>:<ciphertext>`; the `v1` generation marker exists so the format
>   can be rotated, but there is still **no per-key id**, so changing the secret orphans
>   existing ciphertexts rather than allowing a rolling migration. Decrypt and re-encrypt
>   out of band if you need to rotate the key.
> - Consider `MaskRule::Redact` or `MaskRule::Null` for fields that must be
>   fully suppressed in the downstream stream.
>
> **Rules match by exact dotted JSON path**, against a `default_rule` of `Passthrough`.
> A typo, an upstream column rename, or a path-mutating transform (`FieldMappingTransform`,
> `UnwrapTransform`) placed *earlier* in the pipeline will therefore cause masking to do
> nothing for that field. Because that failure is invisible in the data, every rule carries a
> hit counter — see [Unmatched rules](#unmatched-rules) below. **A rule with zero hits after
> real traffic means the field is not being masked.**
>
> Rules on object- and array-valued fields **do** apply: a rule on a `jsonb` column masks the
> whole subtree, and `field.*` covers every element of a variable-length array. Order
> `MaskHashTransform` before any path-mutating transform.
>
> **Default behaviour change in 0.2**: `MaskHashConfig::default()` now uses
> `default_rule: MaskRule::Passthrough`, meaning unlisted fields are passed
> through unchanged.  Use `MaskHashConfig::hash_all()` if you need the old
> "hash everything" behaviour.

`MaskHashTransform::new` returns a `Result`: `MaskHashConfig::validate()` rejects rules that
cannot do what they appear to. `Truncate(0)` and `Redact("")` both produce an empty string,
which downstream cannot distinguish from a genuinely empty column — so the masking is
invisible rather than merely useless, and both are almost always typos for `Redact` with a
marker or `Null`. An empty rule path is rejected for the same reason: it can never match.

```rust
use rustcdc::{MaskHashConfig, MaskHashTransform, MaskRule};

# fn main() -> rustcdc::Result<()> {
// Hash only specified PII fields; leave everything else unchanged.
let mut config = MaskHashConfig::default();
config.mask_rules.insert("email".into(), MaskRule::UnsaltedSha256);
config.mask_rules.insert("ssn".into(),   MaskRule::Null);

// Encrypt a field with AES-256-GCM (requires "encryption" feature).
#[cfg(feature = "encryption")]
config.mask_rules.insert("credit_card".into(), MaskRule::Encrypt("my-secret".into()));

let transform = MaskHashTransform::new(config)?;

// Opt-in aggressive mode: SHA-256 every field not explicitly configured.
let aggressive = MaskHashTransform::new(MaskHashConfig::hash_all())?;
# let _ = (transform, aggressive);
# Ok(()) }
```

### Unmatched rules

Masking, filtering and routing all match by **pattern against a permissive default**, so a
typo or a renamed column disables a rule *silently*. Nothing errors — the pipeline keeps
running and produces plausible output while doing the opposite of what was configured:

| Transform | A rule that never fires means |
|---|---|
| `MaskHashTransform` | a column is shipping in **clear text** |
| `FilterProjectionTransform` | rows meant to be excluded are being delivered |
| `RouteTransform` | events are going to the **default destination**, not the configured one |

Failing closed is not the answer — it would refuse to start over a rule for an optional
column, and operators would respond by deleting rules. So every rule carries a hit counter,
and `Transform::unmatched_rules()` returns the ones that never fired:

```rust
use rustcdc::{Transform, UnmatchedRule};

fn report(stage: &dyn Transform) {
    for rule in stage.unmatched_rules() {
        // rule.transform  — the stage name
        // rule.kind       — "mask" | "filter" | "route"
        // rule.rule       — the pattern that never fired
        // rule.consequence — what is silently happening because of it
        eprintln!("{}: {} never matched. {}", rule.transform, rule.rule, rule.consequence);
    }
    // Or log them all at once:
    stage.warn_on_unmatched_rules();
}
```

The runtime aggregates them across the whole pipeline. **Alert on the metric** rather than
reading a log line at shutdown:

```text
rustcdc_transform_rules_unmatched{transform="mask_hash",kind="mask",rule="user.ssn"} 1
```

The series is emitted **only when a rule is unmatched**, so its absence is the healthy state
and `rustcdc_transform_rules_unmatched > 0` is a complete alert rule. The same data is on
`RuntimeAdminSnapshot::unmatched_transform_rules`.

Zero hits is only meaningful **after real traffic**: every rule is unmatched before the first
event, so evaluate against a representative sample rather than at startup.

Filter rules are reported only once they have actually been *evaluated*. `FilterMode::All`
short-circuits on the first `false`, so a rule an earlier one prevented from running has not
failed to match — reporting it would be a false positive that trains operators to ignore the
signal.

## Transforms

Two traits, because most stages do not need to await:

| Trait | Use for | Cost |
|---|---|---|
| `Transform` | pure CPU work over an in-memory event | no allocation per event |
| `AsyncTransform` | a stage that must `await` — WASM, a network lookup | one boxed future per event |

Every transform this crate ships is synchronous. Register with
`CdcRuntime::add_transform`, or `add_async_transform` for the async variant.

```rust
use rustcdc::{Event, Result, Transform};

#[derive(Debug)]
struct TagRows;

impl Transform for TagRows {
    fn apply(&self, event: &mut Event) -> Result<bool> {
        if let Some(serde_json::Value::Object(after)) = &mut event.after {
            after.insert("_pipeline".into(), serde_json::json!("orders"));
        }
        Ok(true) // false drops the event
    }

    fn name(&self) -> &str {
        "tag_rows"
    }
}
```

Both traits also expose `apply_batch`, and the pipeline runs a whole delivery through each
stage in turn rather than each event through the whole pipeline. Override it when a stage
can amortise per-batch setup — the WASM stage uses it to take its instance lock once per
batch rather than once per event, which matters because that lock serialises every caller
for the duration of guest execution.

**A transform must not destroy the message key.** `Event::primary_key` names the key
*columns*; the values live in the row payload. Projecting away, renaming, or re-encrypting a
key column detaches the two and the event is emitted unkeyed — log compaction stops
collapsing it and upsert consumers start inserting duplicates. The pipeline rejects this
rather than letting it through, but the realistic ways to hit it are all ordinary-looking
config: an `include_columns` list that omits the PK, a `FieldMappingTransform` rename, or
`MaskRule::Encrypt` on a key column (a fresh nonce per call gives every event for the same
row a different key).

## Errors and what an operator sees

`Error` carries a coarse [`ErrorKind`](#error-classification) for retry decisions and,
optionally, a chain of context describing what was being attempted.

### Log `report()`, not the error

`Display` shows only the **outermost** layer. That is the `thiserror` convention, and it
means the obvious logging line hides the very thing you need:

```rust
use rustcdc::Error;

let error = Error::CheckpointError("disk full".into())
    .context("saving the commit barrier")
    .context("acknowledging batch 7");

// What `tracing::error!("{error}")` prints — the context, and nothing about the disk.
assert_eq!(error.to_string(), "acknowledging batch 7");

// What an operator needs.
assert_eq!(
    error.report().to_string(),
    "acknowledging batch 7: saving the commit barrier: checkpoint error: disk full",
);
```

Prefer `tracing::error!(error = %err.report(), "…")` at every site that logs an error for a
human. This crate does the same internally.

`Error::chain()` iterates the layers outermost-first if you want to structure them rather
than render one line, and `Error::root_cause()` returns the innermost error.

### Context never changes a retry decision

`kind()` always delegates to the root cause, so wrapping an error in context cannot turn a
retryable failure into a fatal one — or the reverse:

```rust
# use rustcdc::{Error, ErrorKind};
let transient = Error::SourceError("connection reset".into());
assert_eq!(transient.kind(), ErrorKind::Transient);
assert_eq!(
    transient.context("reading the binlog").kind(),
    ErrorKind::Transient,
);
```

### Foreign errors keep their own causes

Several client libraries have a `Display` that names the operation and leaves the real cause
behind `source()`. `tokio_postgres::Error` renders as *"error connecting to server"* whether
the socket was refused, DNS failed, or the handshake timed out — so a connector that
formatted it with `{error}` threw away the one detail that distinguishes them.

`render_error_chain` walks the foreign chain when flattening, and the connectors use it:

```text
before: postgres tls connection failed: error connecting to server
after:  postgres tls connection failed: error connecting to server: Connection refused (os error 61)
```

## Schema Registries

Three backends, behind one encoder surface. All are optional features.

| Backend | Feature | Wire format | Schema identity |
|---|---|---|---|
| Confluent Schema Registry | `schemreg` | 5-byte header (`0x00` + 4-byte BE id) | integer id |
| Apicurio Registry v3 | `apicurio` | 5-byte header | integer id |
| AWS Glue Schema Registry | `glue` | 18-byte header (`0x03` + compression + 16-byte UUID) | version UUID |

Confluent and Apicurio are **interchangeable**: both implement `SchemaRegistryClient`, so the
same `ConfluentAvroEncoder` and `ConfluentJsonSchemaEncoder` work against either, and the
framing on the wire is identical. Glue is not a drop-in swap — different header, different
identity type, optional ZLIB compression — so it has its own encoder pair, and a consumer must
know which framing to expect or call `detect_wire_format` per message.

### Confluent

```rust,ignore
use rustcdc::codec::{ConfluentAvroEncoder, SchemaRegistryConfig};
use std::sync::Arc;

let config = SchemaRegistryConfig::new("http://localhost:8081", "cdc-events");
let registry = Arc::new(config.build()?);
let encoder = ConfluentAvroEncoder::new(registry.as_ref(), &config).await?;
```

### Apicurio (native v3)

Apicurio also exposes a Confluent-compatible endpoint that `SchemaRegistryConfig` can talk
to. Prefer the native API when you need artifact groups or the richer metadata that the
compatibility shim flattens away.

```rust,ignore
use rustcdc::codec::{ApicurioRegistryConfig, ConfluentAvroEncoder};
use std::sync::Arc;

let apicurio = ApicurioRegistryConfig::new("http://localhost:8080", "cdc-events");
let registry = Arc::new(apicurio.build()?);
let encoder =
    ConfluentAvroEncoder::new(registry.as_ref(), &apicurio.as_schema_registry_config()).await?;
```

`as_schema_registry_config()` carries **every** field over — `auth`, both timeouts,
`max_cache_entries`, `pool_max_idle_per_host`, `references` and `retry_policy` included. The
conversion destructures `ApicurioRegistryConfig` exhaustively, so adding a field without
deciding how it maps is a compile error rather than a setting that quietly stops taking
effect. (Through 0.8 five of those were silently dropped; a caller who set a retry policy got
the default with no indication it had been discarded.)

Two things do not carry over. `normalize_schemas` is a Confluent query parameter with no
Apicurio v3 equivalent. And `url` is copied verbatim, which for this type is the Apicurio
**server root** — so do not call `.build()` on the derived config; use
`ApicurioRegistryConfig::build` for the client and the derived config only for the policy
half of an encoder constructor.

### AWS Glue

Glue identifies schemas by **name** rather than by a topic/subject pair, so there is no
`SubjectNameStrategy` on this path — `GlueAvroConfig` takes the schema name directly and
defaults the key schema to `{schema_name}-key`.

```rust,ignore
use rustcdc::codec::{GlueAvroConfig, GlueAvroDecoder, GlueAvroEncoder};
use rustcdc::codec::glue::{AwsGlueSchemaRegistry, CachedGlueSchemaRegistry, GlueCompression};
use std::sync::Arc;

let registry = Arc::new(CachedGlueSchemaRegistry::new(
    AwsGlueSchemaRegistry::builder().registry_name("cdc").build().await?,
));

let config = GlueAvroConfig::new("cdc-events").with_compression(GlueCompression::Zlib);
let encoder = GlueAvroEncoder::new(Arc::clone(&registry), config).await?;

let value = encoder.encode_event(&event)?;
let key = encoder.encode_event_key(&event)?;   // None for a keyless event

let decoded = GlueAvroDecoder::new(registry)?.decode(&value.bytes).await?;
```

The payload is the same `AVRO_SCHEMA` envelope the Confluent Avro encoder writes, so a
consumer that already decodes rustcdc's Avro events needs only the framing changed. The
decoder resolves the **writer** schema by the header's version UUID and uses it for
resolution, so a message written under an older compatible schema decodes correctly rather
than being read positionally against the current one.

`GlueAvroConfig` has no `auto_register = false`: `schemreg`'s Glue client offers no
lookup-by-name API, so the setting could only have been accepted and ignored — which is
exactly the defect the Confluent JSON Schema and Protobuf encoders shipped with through 0.8.
Glue's `register_schema` is idempotent for identical content.

> **Glue is the one backend with no live-service evidence.** It has no self-hostable
> implementation, so there is no container to point an integration suite at. Everything
> rustcdc owns — the Avro conversion, the 18-byte framing, the compression byte, schema
> identity, error classification, the round trip — is covered against an in-memory fake; the
> AWS transport itself is `schemreg`'s. This is stated as an evidence gap rather than
> implied away.

### Retry policy

Registry calls carry a retry policy by default: jittered exponential back-off, honouring
`Retry-After`. This matters more than it sounds. Schema resolution sits on the **encode
path**, so before retries existed a single HTTP 503 or a dropped connection failed the
event — taking the pipeline down for a condition that clears itself in seconds.

Only *transient* conditions are retried: transport failures, HTTP 429, HTTP 5xx. Not-found,
auth failures, and invalid-schema are permanent and fail immediately, so an outer retry loop
cannot spin on them forever.

```rust
use rustcdc::codec::{RetryPolicy, SchemaRegistryConfig};
use std::time::Duration;

let config = SchemaRegistryConfig::new("http://localhost:8081", "cdc-events")
    .with_retry_policy(
        RetryPolicy::new()
            .max_retries(5)
            .base_backoff(Duration::from_millis(100))
            .max_backoff(Duration::from_secs(5)),
    );
# let _ = config;
```

Set `RetryPolicy::none()` if you already retry at a higher layer and do not want the two to
multiply.

### The schema you register must be the schema you write

With `auto_register = false` — the safer-looking setting, and the one a careful operator picks
in a managed Kafka environment — the encoder takes the schema **id** from the registry. It must
also encode the payload with *that* schema, not with its own: if the two differ, every message
says "decode me with schema X" while carrying bytes written under schema Y.

**Avro binary carries no field names or types.** It is positional and untagged, so a
mismatch does not fail to decode. It silently yields shifted fields and plausible-looking
wrong values, arbitrarily far downstream.

`ConfluentAvroEncoder::new` now verifies the registered schema is the one it will write
with, comparing Avro **parsing canonical form** — so a registry copy differing only in
whitespace, docs, or JSON field ordering is accepted, while a structural difference is a
hard error naming the remedy.

### Holding several codecs behind one type

`Codec` and `EventEncoder` are synchronous. `ConfluentAvroEncoder` resolves its schema once
at construction, so it implements `EventEncoder` and reaches `BoxedCodec` through
`EncoderCodec`. `ConfluentJsonSchemaEncoder` and `ConfluentProtobufEncoder` resolve subjects
**lazily** — correctly so, since `RecordName` and `TopicRecordName` exist precisely to give
each type its own subject — so their `encode` is `async` and fits neither trait.

`AsyncCodec` is the async counterpart, with a blanket `impl<T: Codec> AsyncCodec for T`, so a
sink accepts one type for every format instead of hand-rolling the same three-variant
dispatch enum:

```rust,ignore
use rustcdc::codec::{AsyncCodec, BoxedAsyncCodec, JsonCodec};

let codecs: Vec<BoxedAsyncCodec> = vec![
    ConfluentProtobufEncoder::new(registry.clone(), &config)?.boxed_async(),
    ConfluentJsonSchemaEncoder::new(registry.clone(), &config)?.boxed_async(),
    JsonCodec::default().boxed_async(),   // synchronous, via the blanket impl
];

for codec in &codecs {
    let out = codec.encode_async(&event).await?;
}
```

The encode method is `encode_async`, **not** `encode`. A trait blanket-implemented over
another must not reuse its method names: with both `Codec` and `AsyncCodec` in scope,
`codec.encode(..)` would be an `E0034` ambiguity on every synchronous codec — an error the
library would be handing its users on the hottest call in the API. `content_type` does share
a name, because the blanket impl forwards it and both traits return the same value.

Both registry-backed codecs return `key: None` for a keyless event rather than a framed
empty key, so a Kafka producer round-robins them instead of collapsing every keyless event
onto one partition. Call `encode_event_key` directly when you want the framed-always form.

### Decoding

`AvroDecoder` reverses `AvroEncoder` for bare Avro bytes; `ConfluentAvroDecoder` strips the
5-byte header, resolves the writer schema from the registry, and delegates to the same
conversion.

The conversion is hand-written rather than derived, and that is load-bearing: `before` and
`after` are encoded as Avro **`bytes` holding UTF-8 JSON**, which is what keeps the Avro
schema stable regardless of table structure. A blanket serde mapping sees a byte array where
`Event` declares a JSON value and fails. Until a live round trip against a real registry was
added, `ConfluentAvroDecoder` had never successfully decoded an event — the encoder's unit
tests decoded to a raw Avro value and inspected individual fields rather than reconstructing
an `Event`, so nothing exercised the path end to end.

### Registry URLs are not interchangeable

`SchemaRegistryConfig::url` is the **API root that serves `/subjects`**. For Confluent Schema
Registry that is the server root; for Apicurio's Confluent-compatible endpoint it is
`http://apicurio:8080/apis/ccompat/v7`. The value is used as given — nothing is appended.

`ApicurioRegistryConfig::url` is the **server root** (`http://apicurio:8080`); the client
appends `/apis/registry/v3` itself. Passing the full API path there produces a doubled URL
and a 404 from the server.

### Preflight

Schema resolution is on the encode path, so a registry problem does not surface at startup —
it surfaces as a failed event once traffic is flowing. `preflight_schema_registry` turns that
into a startup check:

```rust,ignore
use rustcdc::codec::{preflight_schema_registry, SchemaRegistryConfig, SchemaType};

let config = SchemaRegistryConfig::new("http://localhost:8081", "cdc-events");
let registry = config.build()?;

// Fails here, where an operator can still act on it.
preflight_schema_registry(&registry, &config, SchemaType::Avro).await?;
```

It checks reachability, then — depending on `auto_register` — either that the subjects carry
rustcdc's schema, or that rustcdc's schema is *compatible* with what is already registered,
so an incompatible auto-registration fails with a clear message rather than an opaque HTTP
409 on the first event. A registry that does not implement an optional endpoint reports
`NotSupported`, which is skipped rather than treated as a failure.

`auto_register = false` is also enforced by the encoders themselves, not only by an explicit
preflight call. `ConfluentAvroEncoder` always resolved both subjects itself, so it honoured
the setting; the JSON Schema and Protobuf encoders delegate subject resolution to `schemreg`,
whose resolution path *is* `register_schema` with no lookup-only mode — so through 0.8 the
setting was **silently ignored** by both, and an operator who set it got schemas registered
anyway plus none of the schema-identity checking it exists to buy. All three now verify at
construction that the subjects exist and carry exactly the schema rustcdc will write. The one
thing that cannot be prevented is the later `register_schema` call itself; because the content
is verified identical first, a Confluent-compatible registry answers it with the existing id
rather than a new version.

**Pass the `SchemaType` your codec actually writes.** Each format has different schemas under
different subject names — Avro and JSON Schema derive subjects from the record name
(`io.rustcdc.Event`), Protobuf from the message's fully-qualified name (`rustcdc.Event`).
Through 0.8 preflight checked the Avro schemas unconditionally, so a JSON Schema or Protobuf
deployment with `auto_register = false` failed against a perfectly correct registry, and one
with `auto_register = true` ran an Avro compatibility check against a JSON subject.

Preflight is generic over the client, so an Apicurio deployment gets the same startup check.
`ApicurioRegistryConfig::preflight` is the shortcut:

```rust,ignore
use rustcdc::codec::{ApicurioRegistryConfig, SchemaType};

let apicurio = ApicurioRegistryConfig::new("http://localhost:8080", "cdc-events");
let registry = apicurio.build()?;

apicurio.preflight(&registry, SchemaType::Avro).await?;
```

### Error classification

Registry errors carry the right retryability instead of all collapsing into one kind:

| Registry condition | `ErrorKind` | Why |
|---|---|---|
| transport failure, HTTP 429, HTTP 5xx | `Transient` | resolves on its own |
| subject / version / schema not found | `Terminal` | needs the schema registered |
| auth failure | `Terminal` | needs a credential change |
| malformed Confluent framing | `Terminal` | **these exact bytes will never decode** |
| Avro / JSON deserialisation failure | `Terminal` | same |

The last two matter most. Classified as `Transient` — "safe to retry with backoff" — they would
send an embedder following the crate's own guidance into retrying a message that can never
succeed. Malformed bytes do not become well-formed on the next attempt.

### Protobuf

Confluent Protobuf does **not** use the plain 5-byte header:

```text
[0x00 magic][4-byte BE schema_id][message-index path][protobuf payload]
```

The message-index path locates the message inside its `.proto` file. rustcdc derives it from
the compiled descriptor rather than hardcoding it — for `rustcdc.Event` the correct path is
`[3]`, not the `[0]` a single-message schema would use, and a Confluent deserialiser given
the wrong index misreads the header **without erroring**.

The descriptor is compiled at build time with [`protox`], a pure-Rust protobuf compiler, so
building rustcdc does not require `protoc`.

```rust,ignore
use rustcdc::codec::{ConfluentProtobufEncoder, SchemaRegistryConfig};
use std::sync::Arc;

let config = SchemaRegistryConfig::new("http://localhost:8081", "cdc-events");
let encoder = ConfluentProtobufEncoder::new(Arc::new(config.build()?), &config)?;
let framed = encoder.encode(&event).await?;
let key = encoder.encode_event_key(&event).await?;
```

As with `ProtobufEncoder`, `before` and `after` carry UTF-8 JSON as protobuf `bytes` — the
envelope is typed, the row payload stays schemaless.

`encode_event_key` frames the primary key against `KEY_PROTO_SCHEMA` (`proto/event_key.proto`)
under the key subject, completing the three-format key story: `ConfluentAvroEncoder` has
`encode_key`, `ConfluentJsonSchemaEncoder` has `encode_event_key`, and through 0.8 the
Protobuf encoder had no key path at all — so a fan-out mixing codecs silently paired a
registry-framed value with `ProtobufEncoder`'s unframed compact-JSON key, with nothing in the
API signalling the mismatch. Keyless events (TRUNCATE, SCHEMA_CHANGE, tables with no declared
primary key) produce a message with the `key` field **absent**, not empty, matching the
`{"key": null}` the JSON Schema encoder emits and Debezium's behaviour.

#### `Ok(None)` and `Err` are different outcomes

`EventEncoder::encode_key` returns `Result<Option<Vec<u8>>>`, and the two negative outcomes are
kept apart deliberately:

| Outcome | Meaning | What a keyed sink should do |
|---|---|---|
| `Ok(Some(bytes))` | The event has a key | Publish with it |
| `Ok(None)` | The event genuinely has **no** key — TRUNCATE, SCHEMA_CHANGE, no declared primary key, or a payload missing a key column | Publish unkeyed. Round-robin is correct; collapsing every keyless event onto one partition is not |
| `Err(..)` | Encoding **failed**, for an event that does have a key | Do not publish. Retry or dead-letter it |

Collapsing the third into the second is a silent correctness failure rather than a lost error
message: a keyed sink reads `None` as "unkeyed", so the record is produced without a key,
**ordering for that row is lost**, log compaction stops collapsing it — and the record still
arrives, so nothing looks wrong. Through 0.11 the method returned a bare `Option` and
`ConfluentAvroEncoder::encode_key` swallowed both of its failure paths with `.ok()`, so that
outcome was expressible. It no longer is.

This mirrors what the transform pipeline already does from the other side: a stage that destroys
an event's key is rejected with an error naming the stage, rather than emitting the record
unkeyed. It would have been inconsistent to let an encoder cause the same thing quietly.

### Cache warming

Schema resolution is on the decode path, so a cold cache turns the first message for each
distinct schema id into a synchronous registry call. For a consumer restarting against a
backlog that is a burst of round-trips at exactly the moment throughput matters most.

```rust,ignore
use rustcdc::codec::warm_schema_cache;

warm_schema_cache(&registry, [SchemaId::new(1), SchemaId::new(2)]).await?;
```

Schema ids are immutable — a registry never reassigns one — so a warmed entry is valid for
the process lifetime. That is why the cache warms ids but never `get_latest_schema`, which
can change at any moment.

It accepts **any** `SchemaRegistryClient`, including an erased
`Arc<dyn DynSchemaRegistryClient>`. Through 0.8 it required the concrete
`CachedSchemaRegistry<C>`, which made erasure and warming mutually exclusive — and erasure is
exactly what a deployment with several registry backends needs, since the encoders are
generic over the client and every variant would otherwise exist twice:

```rust,ignore
use rustcdc::codec::{warm_schema_cache, DynSchemaRegistryClient};
use std::sync::Arc;

let erased: Arc<dyn DynSchemaRegistryClient> = Arc::new(config.build()?);
warm_schema_cache(&*erased, ids).await?;
```

Warming fetches through the trait, which is the same cache-populating path
`CachedSchemaRegistry` uses internally. Against a client with **no** cache it issues the
round-trips and retains nothing — a wasted warm rather than a wrong one.

### Debezium key-schema compatibility

Debezium's Avro converter registers a separate key schema per topic (`{topic}-key`) as a
record with a single nullable `key` field. `ConfluentAvroEncoder` mirrors this exactly, so a
consumer written against Debezium's key subject works unchanged.

## Related Documentation

- [Getting Started](@/docs/getting-started.md)
- [Configuration Reference](@/docs/config-reference.md)
- [Architecture](@/docs/architecture.md)
- [Schema Evolution and DDL Capture](@/docs/schema-evolution.md)
- [Reliability Testing Guide](@/docs/reliability-testing.md)
- [Adapter SDK](@/docs/adapter-sdk.md)

---

## MariaDB Support

rustcdc supports **MariaDB 10.5, 10.6, and 10.11** via the MySQL protocol stack. The
`mysql_async` library handles the MariaDB binlog wire protocol transparently.
rustcdc also provides a first-class `MariaDbSourceConfig` wrapper for explicit
runtime source typing (`mariadb`) and separate checkpoint namespace handling.

### Capability Matrix

| Capability                 | PostgreSQL | MySQL 8+ | MariaDB 10.5/10.6/10.11 | SQL Server |
|----------------------------|:----------:|:--------:|:-----------------:|:----------:|
| Full-table snapshot        | ✅          | ✅        | ✅ (validated on 10.5 and 10.6) | ✅          |
| Resumable snapshot (keyset)| ✅        | ✅        | ✅ (validated on 10.5 and 10.6) | ✅          |
| CDC streaming              | ✅          | ✅        | ✅ (validated on 10.5 and 10.6) | ✅          |
| GTID-based position        | —          | ✅        | ✅ (connector support) | —          |
| Binlog position fallback   | —          | ✅        | ✅ (connector support) | —          |
| TLS connections            | ✅          | ✅        | ✅ (connector support) | ✅          |
| Transaction boundaries     | ✅          | ✅        | ✅ (validated on 10.5 and 10.6) | ✅          |
| Schema change events       | ✅          | ✅        | ✅ | ✅          |

MariaDB 10.11 currently has explicit process-crash and replay evidence coverage,
while 10.5/10.6 are validated across the core connection and end-to-end lanes.

**Note on schema change events**: Runtime connectors emit canonical `Operation::SchemaChange` events for supported DDL capture paths. Use `rustcdc::ddl_capture` and `rustcdc::schema_history` together when building schema-aware downstream consumers.

**MariaDB nuance**: MariaDB schema-change behavior follows the MySQL connector path and is exercised in integration coverage, but depth may vary by engine/version-specific DDL semantics.

### Connecting to MariaDB

Use `MysqlSourceConfig` exactly as you would for MySQL:

```rust
use rustcdc::source::mysql::MysqlSourceConfig;

let config = MysqlSourceConfig {
    host: "mariadb-host".into(),
    port: 3306,
    user: "replication_user".into(),
    password: rustcdc::SecretString::new("secret"),
    database: "my_db".into(),
    ..Default::default()
};
```

### Known Limitations

- MariaDB 10.3 and earlier are **not tested** and may work with basic binlog
  events but are unsupported.
- MariaDB Galera Cluster is not tested; CDC from a Galera node may exhibit
  unexpected behaviour due to write-set replication semantics.
- `ROW_FORMAT=COMPRESSED` tables require `binlog_row_image = FULL` on the
  server; partial images are not supported.

MariaDB integration evidence includes dedicated end-to-end suites for snapshot
resume, stream CDC, and snapshot-to-stream handoff on MariaDB 10.5 and 10.6 in
`tests/mariadb_e2e_integration.rs`, plus connection lifecycle coverage in
`tests/mariadb_connection_integration.rs`, and process-crash replay coverage on
MariaDB 10.11 in `tests/runtime_mariadb_process_crash_integration.rs`.

