# cdc-rs API Guide

This document is the primary API reference for embedding cdc-rs in Rust applications.

## Audience

This guide is for engineers integrating cdc-rs as a library and building custom runtime loops.

## API Surface

The core embedder API is centered on:

- `RuntimeConfig` for runtime construction
- `CdcRuntime` for lifecycle and event delivery
- `RuntimeSourceConfig` for source selection
- `EventBatch` and `AckToken` for loss-safe delivery semantics

## Runtime Construction

`RuntimeConfig` binds four concerns:

- source connector configuration
- checkpoint backend
- schema history backend
- runtime options and observability

Typical shape:

```rust
use cdc_rs::{
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
use cdc_rs::{
  checkpoint::InMemoryCheckpoint,
  schema_history::FileSchemaHistory,
  RuntimeConfig,
  RuntimeSourceConfig,
};

async fn durable_schema_history_config() -> cdc_rs::Result<()> {
  let checkpoint = InMemoryCheckpoint::default();
  let schema_history = FileSchemaHistory::new("/var/lib/cdc-rs/schema-history.json").await?;

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
use cdc_rs::{CdcRuntime, Result, RuntimeConfig, RuntimeSourceConfig};
use cdc_rs::checkpoint::InMemoryCheckpoint;
use cdc_rs::schema_history::InMemorySchemaHistory;

async fn run_once() -> Result<()> {
  let checkpoint = InMemoryCheckpoint::default();
  let schema_history = InMemorySchemaHistory::default();
  let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);

  let mut runtime = CdcRuntime::new(config)?;
  runtime.start().await?;

  let batch = runtime.poll_event_batch().await?;
  if let Some(token) = batch.ack_token() {
    runtime.commit_ack(token).await?;
  }

  runtime.stop().await?;
  Ok(())
}
```

## Source Selection

`RuntimeSourceConfig` selects the source connector at runtime:

- `Postgres(PostgresSourceConfig)`
- `Mysql(MysqlSourceConfig)`
- `SqlServer(SqlServerSourceConfig)`
- `Disabled`

The runtime also exposes connector capability metadata via `source_capabilities()` and validates incompatible settings (for example, snapshot tables for a source that does not support snapshots). Capability metadata includes `snapshot_checkpoint_resume`, which is `true` for PostgreSQL, MySQL, and SQL Server. Snapshot checkpoints now resume through connector-native cursor state and keep stream bootstrap aligned with the saved snapshot watermark.

## Event Model

`Event` is the canonical envelope consumed by downstream code.

Key fields include:
- `op`: one of `Insert`, `Update`, `Delete`, `Read`, `SchemaChange`
- `source`: source metadata and offset context
- `transaction`: optional transaction metadata
- `snapshot`: optional snapshot metadata

The event envelope is designed to support stable replay and source-agnostic processing.

## Delivery And Acknowledgement Semantics

`poll_event_batch()` returns an `EventBatch` that contains events and an optional `AckToken`.

Correct processing sequence:

1. consume events in batch order
2. durably commit sink side effects
3. call `commit_ack(token)`

Important semantics:
- not acknowledging after sink durability may replay already-delivered events
- `stop()` fails fast if uncommitted events remain in-flight
- `force_stop()` is intended for emergency drain where replay is acceptable
- source confirmation failures after durable checkpoint commit now fail fast by default (`PostCommitSourceConfirmPolicy::FailFast`)

To preserve pre-existing availability-biased behavior, opt into continue mode explicitly:

```rust
use cdc_rs::PostCommitSourceConfirmPolicy;

let config = config.with_post_commit_source_confirm_policy(
  PostCommitSourceConfirmPolicy::Continue,
);
```

### Sink-Side Idempotency Guard

For at-least-once replay tolerance, cdc-rs now provides a built-in
`EventIdempotencyGuard` helper for consumer loops.

```rust
use cdc_rs::{EventIdempotencyGuard, Result};

async fn process_batch(events: &[cdc_rs::Event]) -> Result<usize> {
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

## Streaming Consumption

`event_batches()` provides a stream-based consumption model for non-empty batches.

```rust
use futures_util::StreamExt;

let mut batches = runtime.event_batches();
while let Some(batch) = batches.next().await {
  let batch = batch?;
  if let Some(token) = batch.ack_token() {
    runtime.commit_ack(token).await?;
  }
}
```

## Checkpoint Backends

Checkpoint implementations persist source offsets and determine restart position.

Built-in options include:

- in-memory checkpoint storage (tests)
- file-backed checkpoint storage
- PostgreSQL-backed checkpoint storage

Custom checkpoint backends can be implemented through the `Checkpoint` trait.

## Runtime Introspection

The runtime exposes embeddable control-plane state and metrics surfaces:

- `admin_snapshot()`
- `admin_snapshot_json()`
- `admin_metrics_prometheus()`

Use these methods for health endpoints, diagnostics views, and lightweight observability bridges.

## Related Documentation

- [Getting Started](getting_started.md)
- [Configuration Reference](config_reference.md)
- [Architecture](architecture.md)
- [Schema Evolution and DDL Capture](schema_evolution.md)
- [Reliability Testing Guide](reliability_testing.md)
- [Adapter SDK](adapter_sdk.md)

---

## MariaDB Support

cdc-rs supports **MariaDB 10.5 and 10.6** via the MySQL connector. The
`mysql_async` library handles the MariaDB binlog wire protocol transparently;
no separate connector type is needed.

### Capability Matrix

| Capability                 | PostgreSQL | MySQL 8+ | MariaDB 10.5/10.6 | SQL Server |
|----------------------------|:----------:|:--------:|:-----------------:|:----------:|
| Full-table snapshot        | ✅          | ✅        | ✅ (validated on 10.5 and 10.6) | ✅          |
| Resumable snapshot (keyset)| ✅        | ✅        | ✅ (validated on 10.5 and 10.6) | ✅          |
| CDC streaming              | ✅          | ✅        | ✅ (validated on 10.5 and 10.6) | ✅          |
| GTID-based position        | —          | ✅        | ✅ (connector support) | —          |
| Binlog position fallback   | —          | ✅        | ✅ (connector support) | —          |
| TLS connections            | ✅          | ✅        | ✅ (connector support) | ✅          |
| Transaction boundaries     | ✅          | ✅        | ✅ (validated on 10.5 and 10.6) | ✅          |
| Schema change events       | ✅          | ✅        | ✅ | ✅          |

**Note on schema change events**: Runtime connectors emit canonical `Operation::SchemaChange` events for supported DDL capture paths. Use `cdc_rs::ddl_capture` and `cdc_rs::schema_history` together when building schema-aware downstream consumers.

**MariaDB nuance**: MariaDB schema-change behavior follows the MySQL connector path and is exercised in integration coverage, but depth may vary by engine/version-specific DDL semantics.

### Connecting to MariaDB

Use `MysqlSourceConfig` exactly as you would for MySQL:

```rust
use cdc_rs::source::mysql::MysqlSourceConfig;

let config = MysqlSourceConfig {
    host: "mariadb-host".into(),
    port: 3306,
    user: "replication_user".into(),
    password: cdc_rs::SecretString::new("secret"),
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
`tests/mariadb_connection_integration.rs`.

