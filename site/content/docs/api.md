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

This is not ceremony for its own sake. Every field added to the envelope used to be a breaking
change for every construction site — it broke this crate's own published adapter SDK example in
0.7.0. With `#[non_exhaustive]` plus a builder, adding a field is a minor-version change.

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
use tokio_util::sync::CancellationToken;
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

Implement six methods and you inherit the state machine, the override window, cursor
persistence and the `StreamHandle` contract:

```rust
use rustcdc::source::{
    ChunkRow, IncrementalSnapshotBackend, IncrementalSnapshotState, SnapshotTable,
};
use rustcdc::{Event, Offset, Result};
use async_trait::async_trait;

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

Then build the driver in your `Source::start_incremental_snapshot` and return it as the
`StreamHandle`.

**Three contract points decide whether this is correct or silently wrong:**

1. `current_position` and `position_of_event` must be on the **same scale**. If they are not,
   the override window never matches, and stale chunk rows are emitted over newer stream
   values — with no error, no metric, and no log line.
2. `fetch_chunk` must not hold a transaction open. Holding one across chunks reintroduces
   exactly the transaction-ID backlog the incremental snapshot exists to avoid.
3. `offset_with_snapshot_state` returning `None` falls back to the inner stream's own
   `save_position`, which **discards every chunk cursor** — every restart then re-reads each
   table from row zero. Return `None` only if your source genuinely has no typed offset.

The keyset cursor is persisted inside your offset, so it is written by the same atomic, fsynced,
checksummed record as the log position. `snapshot_tables` remains available for a blocking
initial snapshot if you do not want to implement the backend.

## EventBatch Inspection

`EventBatch` provides several inspection methods:

- `batch.len()` / `batch.is_empty()` — event count
- `batch.ack_mode()` — `AckMode::Required(token)` or `AckMode::NotRequired`
- `batch.oldest_event_source_timestamp_ms()` — millisecond timestamp of the oldest event in the batch (for lag monitoring)
- `batch.events()` — iterator over contained `Event` values

## Checkpoint Backends

Checkpoint implementations persist source offsets and determine restart position.

Built-in options include:

- `InMemoryCheckpoint` — zero-config, suitable for tests and short-lived processes. State is lost on restart.
- `FileCheckpoint` — file-backed persistence; recommended for production.

Custom checkpoint backends can be implemented through the `Checkpoint` trait.

## Runtime Introspection

The runtime exposes embeddable control-plane state and metrics surfaces:

- `admin_snapshot()`
- `admin_snapshot_json()`
- `admin_metrics_prometheus()`

Use these methods for health endpoints, diagnostics views, and lightweight observability bridges.

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
> hit counter: `MaskHashTransform::unmatched_rules()` names rules that have never fired and
> `warn_on_unmatched_rules()` logs them. **A rule with zero hits after real traffic means the
> field is not being masked.** Wire it into a health check.
>
> Rules on object- and array-valued fields **do** apply: a rule on a `jsonb` column masks the
> whole subtree, and `field.*` covers every element of a variable-length array. Order
> `MaskHashTransform` before any path-mutating transform.
>
> **Default behaviour change in 0.2**: `MaskHashConfig::default()` now uses
> `default_rule: MaskRule::Passthrough`, meaning unlisted fields are passed
> through unchanged.  Use `MaskHashConfig::hash_all()` if you need the old
> "hash everything" behaviour.

```rust
use rustcdc::{MaskHashConfig, MaskHashTransform, MaskRule};

// Hash only specified PII fields; leave everything else unchanged.
let mut config = MaskHashConfig::default();
config.mask_rules.insert("email".into(), MaskRule::UnsaltedSha256);
config.mask_rules.insert("ssn".into(),   MaskRule::Null);

// Encrypt a field with AES-256-GCM (requires "encryption" feature).
#[cfg(feature = "encryption")]
config.mask_rules.insert("credit_card".into(), MaskRule::Encrypt("my-secret".into()));

// Opt-in aggressive mode: SHA-256 every field not explicitly configured.
let aggressive = MaskHashConfig::hash_all();
```

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
identity type, optional ZLIB compression — so a consumer must know which framing to expect,
or call `detect_wire_format` per message.

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

let apicurio = ApicurioRegistryConfig::new("http://localhost:8080/apis/registry/v3", "cdc-events");
let registry = Arc::new(apicurio.build()?);
let encoder =
    ConfluentAvroEncoder::new(registry.as_ref(), &apicurio.as_schema_registry_config()).await?;
```

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

With `auto_register = false` — the safer-looking setting, and the one a careful operator
picks in a managed Kafka environment — the encoder previously took the registry's schema
**id** and then encoded the payload with rustcdc's own schema. If the two differed, every
message said "decode me with schema X" while carrying bytes written under schema Y.

**Avro binary carries no field names or types.** It is positional and untagged, so a
mismatch does not fail to decode. It silently yields shifted fields and plausible-looking
wrong values, arbitrarily far downstream.

`ConfluentAvroEncoder::new` now verifies the registered schema is the one it will write
with, comparing Avro **parsing canonical form** — so a registry copy differing only in
whitespace, docs, or JSON field ordering is accepted, while a structural difference is a
hard error naming the remedy.

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
use rustcdc::codec::{preflight_schema_registry, SchemaRegistryConfig};

let config = SchemaRegistryConfig::new("http://localhost:8081", "cdc-events");
let registry = config.build()?;

// Fails here, where an operator can still act on it.
preflight_schema_registry(&registry, &config).await?;
```

It checks reachability, then — depending on `auto_register` — either that the subjects carry
rustcdc's schema, or that rustcdc's schema is *compatible* with what is already registered,
so an incompatible auto-registration fails with a clear message rather than an opaque HTTP
409 on the first event. A registry that does not implement an optional endpoint reports
`NotSupported`, which is skipped rather than treated as a failure.

### Error classification

Registry errors carry the right retryability instead of all collapsing into one kind:

| Registry condition | `ErrorKind` | Why |
|---|---|---|
| transport failure, HTTP 429, HTTP 5xx | `Transient` | resolves on its own |
| subject / version / schema not found | `Terminal` | needs the schema registered |
| auth failure | `Terminal` | needs a credential change |
| malformed Confluent framing | `Terminal` | **these exact bytes will never decode** |
| Avro / JSON deserialisation failure | `Terminal` | same |

The last two matter most: framing and deserialisation failures used to surface as
`SourceError`, which classifies as `Transient` — "safe to retry with backoff" — so an
embedder following the crate's own guidance retried a message that cannot ever succeed.

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

let config = SchemaRegistryConfig::new("http://localhost:8081", "cdc-events");
let encoder = ConfluentProtobufEncoder::new(config.build()?, &config)?;
let framed = encoder.encode(&event).await?;
```

As with `ProtobufEncoder`, `before` and `after` carry UTF-8 JSON as protobuf `bytes` — the
envelope is typed, the row payload stays schemaless.

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

