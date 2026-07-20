# rustcdc API Guide

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

### Partial payloads — read this before writing a sink

Some events do not carry a complete row. Applying one as if it were complete is the classic
CDC corruption: you write `NULL` over a column that never changed. **The library gives you an
API that cannot express that write — use it instead of reading `after` directly.**

#### `Event::row_write()` — the safe path

`row_write()` folds the payload, the missing columns and the primary key into the single
write that is correct for the event:

```rust
use rustcdc::{Event, RowWrite};

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

```rust
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

let config = config.with_post_commit_source_confirm_policy(
  PostCommitSourceConfirmPolicy::Continue,
);
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
use rustcdc::idempotency::fingerprint_event_stable;

// On each delivered event:
let fingerprint = fingerprint_event_stable(&event)?; // returns Result<String, FingerprintError>
if !sink_has_seen(&fingerprint).await? {
    sink_write(&event).await?;
    sink_mark_seen(&fingerprint).await?;
}
```

Unlike `fingerprint_event_transient` (which uses a per-process random seed),
`fingerprint_event_stable` produces the same fingerprint for the same event
across restarts, making it safe to persist and check against a durable store.

## Streaming Consumption

`event_batches()` provides a stream-based consumption model for non-empty batches.

```rust
use futures_util::StreamExt;

let mut batches = runtime.event_batches();
while let Some(batch) = batches.next().await {
  let batch = batch?;
  runtime.commit_ack(batch.ack_mode()).await?;
}
```

For cooperative cancellation, use `event_batches_cancellable(token)` with a `CancellationToken`:

```rust
use tokio_util::sync::CancellationToken;
use futures_util::StreamExt;

let cancel = CancellationToken::new();
let mut batches = runtime.event_batches_cancellable(cancel.clone());
while let Some(batch) = batches.next().await {
  let batch = batch?;
  runtime.commit_ack(batch.ack_mode()).await?;
}
// cancel.cancel() from another task unblocks the stream cleanly
```

## Incremental Snapshot API (DBLog Pattern)

`CdcRuntime::start()` supports both classic snapshot + stream handoff and
runtime-driven incremental snapshot startup (when configured via
`with_incremental_snapshot(...)`).

If you want connector-managed non-blocking incremental snapshot behavior,
you can also start it directly from a connector connection via
`start_incremental_snapshot(...)`.

```rust
use rustcdc::{
  IncrementalSnapshotConfig, PostgresConnection, PostgresSourceConfig, Result,
};

async fn start_incremental_stream(config: PostgresSourceConfig) -> Result<()> {
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
```

This connector-level API is also available for MySQL and SQL Server via
`MysqlConnection::start_incremental_snapshot(...)` and
`SqlServerConnection::start_incremental_snapshot(...)`.

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
```

`HealthVerdict::is_alertable()` returns `true` for exactly `Stalled`, so a readiness handler can
gate on it without matching every variant. The enum is `#[non_exhaustive]` — match with a
wildcard arm.

`Stalled` is raised for an unconfirmed source position, a poll loop that has not completed within
`max_poll_wait_ms × 6` (floor 30s), or polled-but-uncommitted events with a stale last commit —
that last case meaning the embedder stopped calling `commit_ack`, not a source fault. See
[Operations Runbook](runbook.md#health-verdict--idle-vs-stalled) for the alerting rules.

## Connection Retry Policy

For transient source connectivity failures, configure `ConnectionRetryPolicy` via
`RuntimeConfig::with_connection_retry`. The runtime retries only recoverable errors
(`SourceError`, `TimeoutError`); fatal configuration errors propagate immediately.

```rust
use rustcdc::core::ConnectionRetryPolicy;

let policy = ConnectionRetryPolicy {
    max_retries: Some(5),       // None = retry indefinitely
    initial_delay_ms: 300,
    max_delay_ms: 10_000,
};

let config = config.with_connection_retry(policy);
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

let transform = FilterProjectionTransform::new(FilterProjectionConfig {
  filter: Some(FilterRule::new(FilterField::Op, FilterOperator::Eq, "insert")),
    include_columns: Some(vec!["id".into(), "email".into()]),
    exclude_columns: None,
})?;  // returns Err(ConfigError) for invalid filter values
```

### Content-based filtering

`FilterField::AfterField(path)` and `FilterField::BeforeField(path)` match
against fields inside the event payload using a dot-separated path (e.g. `"user.country"`).

Available operators: `Eq`, `Ne`, `Contains`, `Regex`, `Lt`, `LtEq`, `Gt`, `GtEq`.

```rust
// Keep only events where after["status"] == "active"
FilterRule::new(FilterField::AfterField("status".into()), FilterOperator::Eq, "active")

// Keep events where after["amount"] > 100
FilterRule::new(FilterField::AfterField("amount".into()), FilterOperator::Gt, "100")

// Keep events matching a regex on after["email"]
FilterRule::new(FilterField::AfterField("email".into()), FilterOperator::Regex, r"@example\.com$")
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
>   Note the current wire format is `enc:<nonce>:<ciphertext>` with no key id, so key rotation
>   is not yet a graceful migration, and ciphertexts are not bound to their column or row.
> - Consider `MaskRule::Redact` or `MaskRule::Null` for fields that must be
>   fully suppressed in the downstream stream.
>
> **Rules match by exact dotted JSON path**, against a `default_rule` of `Passthrough`.
> A typo, an upstream column rename, or a path-mutating transform (`FieldMappingTransform`,
> `UnwrapTransform`) placed *earlier* in the pipeline will therefore cause masking to
> silently do nothing. Rules targeting object- or array-valued fields are currently not
> applied — only scalars are matched. Order `MaskHashTransform` before any path-mutating
> transform, and verify masking with a test.
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

## Related Documentation

- [Getting Started](getting_started.md)
- [Configuration Reference](config_reference.md)
- [Architecture](architecture.md)
- [Schema Evolution and DDL Capture](schema_evolution.md)
- [Reliability Testing Guide](reliability_testing.md)
- [Adapter SDK](adapter_sdk.md)

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

