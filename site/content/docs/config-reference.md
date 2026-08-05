+++
title = "Configuration reference"
description = "Every rustcdc runtime and connector option, with the failure each one prevents."
weight = 40
+++

**Version:** v0.1+  
**Audience:** Platform engineers and application developers embedding rustcdc

---

## Table of Contents

1. [RuntimeConfig](#runtimeconfig)
2. [Runtime Consumption Model](#runtime-consumption-model)
3. [Transaction boundaries](#transaction-boundaries)
4. [Connector Capabilities](#connector-capabilities)
5. [Column type mapping](#column-type-mapping)
6. [PostgreSQL Source Configuration](#postgresql-source-configuration)
7. [MySQL Source Configuration](#mysql-source-configuration)
8. [MariaDB Source Configuration](#mariadb-source-configuration)
9. [SQL Server Source Configuration](#sql-server-source-configuration)
10. [Checkpoint Configuration](#checkpoint-configuration)
11. [Observability Configuration](#observability-configuration)
12. [Production Recommendations](#production-recommendations)

---

## RuntimeConfig

Core runtime configuration for CDC operations.

### Fields

| Field | Type | Purpose |
|---|---|---|
| `source` | `RuntimeSourceConfig` | Typed source connector configuration; see [Source selection](#postgresql-source-configuration) below. |
| `snapshot_tables` | `Vec<String>` | Tables for the **classic blocking snapshot**, applied on first run when no checkpoint exists. `"schema.table"` format. Empty means stream-only. |
| `incremental_snapshot` | `Option<IncrementalSnapshotConfig>` | Tables for the **non-blocking DBLog snapshot**. Supersedes `snapshot_tables`; set one or the other, never both. |
| `checkpoint` | `C: Checkpoint` | Durable position store. `InMemoryCheckpoint` for tests, `FileCheckpoint` or your own backend otherwise. |
| `schema_history` | `H: SchemaHistory` | Schema/DDL history store, with the same in-memory-vs-durable choice. |
| `options` | `RuntimeOptions` | Operational knobs; see the table below. |

### RuntimeOptions

`RuntimeOptions` carries the operational knobs. Every default below is chosen to make a
failure visible rather than to keep the pipeline running through one.

| Field | Type | Default | Purpose |
|---|---|---|---|
| `observability` | `RuntimeObservability` | no-op | Metrics collector and event tracer. Nothing is exported until you set these. |
| `max_buffer_size` | `usize` | 10 000 | Maximum events per delivered batch. |
| `max_poll_wait_ms` | `u64` | 5 000 | How long `poll_event_batch` waits before returning an empty batch. |
| `max_event_bytes` | `Option<usize>` | `None` | Upper bound on serialized bytes per batch. `None` relies on `max_buffer_size` alone — which is a poor proxy when row sizes vary by orders of magnitude. |
| `transform_error_policy` | `TransformErrorPolicy` | `Halt` | What a failing transform does. `Halt` preserves failure visibility; `Skip` requires a `dead_letter_handler`. |
| `dead_letter_handler` | `Option<Arc<dyn Fn(Event, Error)>>` | `None` | Invoked for events discarded under `Skip`. Mandatory with that policy — a skipped event is otherwise unrecoverable. |
| `post_commit_source_confirm_policy` | `PostCommitSourceConfirmPolicy` | `FailFast` | Behaviour when source confirmation fails *after* the durable checkpoint commit. `FailFast` surfaces the divergence; `Continue` is the availability-biased opt-in. |
| `idempotency` | `Option<IdempotencyOptions>` | on, 100 000 keys | Runtime duplicate suppression. Disable with `with_idempotency_disabled()`. |
| `validate_events` | `bool` | `true` | Enforce the event envelope contract on every event. |
| `schema_history_retention` | `Option<SchemaHistoryRetention>` | `keep_last(256)` | Bounds unbounded schema-history growth. |
| `connection_retry` | `Option<ConnectionRetryPolicy>` | enabled | Jittered exponential back-off for recoverable source connection errors. `None` propagates immediately. |
| `sink_close_timeout_ms` | `Option<u64>` | `None` | Timeout applied to a registered sink's `close` during orderly shutdown. |
| `transaction_boundary` | `TransactionBoundaryPolicy` | `Split` | Whether a delivered batch may end mid-transaction. See [Transaction boundaries](#transaction-boundaries). |

### RuntimeConfig Builder Example

```rust
use rustcdc::{
  checkpoint::InMemoryCheckpoint,
  schema_history::InMemorySchemaHistory,
  PostgresSourceConfig,
  RuntimeConfig,
  RuntimeSourceConfig,
  SecretString,
};

let checkpoint = InMemoryCheckpoint::default();
let schema_history = InMemorySchemaHistory::default();
let source = PostgresSourceConfig {
  host: "localhost".into(),
  port: 5432,
  user: "postgres".into(),
  password: SecretString::from_callback("postgres-password", || {
    std::env::var("CDC_RS_POSTGRES_PASSWORD")
      .map_err(|error| rustcdc::Error::ConfigError(error.to_string()))
  }),
  database: "mydb".into(),
  replication_slot_name: "rustcdc_slot".into(),
  publication_name: "rustcdc_publication".into(),
  conn_timeout_secs: 30,
  ..PostgresSourceConfig::default()
};

let config = RuntimeConfig::new(RuntimeSourceConfig::postgres(source), checkpoint, schema_history)
    .with_snapshot_tables(vec!["public.users".to_string(), "public.orders".to_string()])
    .with_max_buffer_size(50_000)
    .with_max_poll_wait_ms(2_000)
    .with_transform_error_policy(rustcdc::TransformErrorPolicy::Halt);
```

For env-driven bootstrapping, use explicit argument parsing in your host
application and map values into typed source configs.

Prefer the associated constructors when selecting a source in embedder code:

- `RuntimeSourceConfig::postgres(...)`
- `RuntimeSourceConfig::mysql(...)`
- `RuntimeSourceConfig::mariadb(...)`
- `RuntimeSourceConfig::sqlserver(...)`
- `RuntimeSourceConfig::disabled()`

## Runtime Consumption Model

The preferred embedder surface is now batch-oriented rather than count-oriented.

`poll_event_batch()` returns an `EventBatch` containing the delivered events plus an `AckMode`. Re-polling before acknowledgement redelivers the same in-flight batch, which keeps retry behavior loss-safe.

```rust
use rustcdc::{CdcRuntime, Result};

async fn consume_once(runtime: &mut CdcRuntime) -> Result<()> {
  let batch = runtime.poll_event_batch().await?;
  if batch.is_empty() {
    return Ok(());
  }

  runtime.commit_ack(batch.ack_mode()).await?;

  Ok(())
}
```

For partial acknowledgement, split the token and commit only the accepted prefix. The remaining suffix will be re-delivered on the next poll.

```rust
use rustcdc::AckMode;
# use rustcdc::{CdcRuntime, Result};
# async fn example(runtime: &mut CdcRuntime) -> Result<()> {
let batch = runtime.poll_event_batch().await?;
if let AckMode::Required(token) = batch.ack_mode() {
  let (accepted, _retry_later) = token.split_at(10)?;
  runtime.commit_ack(accepted).await?;
}
# Ok(())
# }
```

`event_batches()` exposes the same model as a stream of non-empty `EventBatch` values.

```rust
use futures_util::StreamExt;
# use rustcdc::{CdcRuntime, Result};
# async fn example(runtime: &mut CdcRuntime) -> Result<()> {
let mut batches = runtime.event_batches();
while let Some(batch) = batches.next().await {
  let batch = batch?;
  let _ = batch;
}
# Ok(())
# }
```

`poll_event_batch()` + `commit_ack(batch.ack_mode())` is now the canonical runtime acknowledgement API.

## Transaction boundaries

By default a delivered batch may end in the middle of a source transaction. Batches are cut on
`max_buffer_size`, `max_event_bytes` and free commit-barrier capacity — none of which know
anything about transactions. That is `TransactionBoundaryPolicy::Split`, and for most sinks it
is the right trade: lowest latency, strictly bounded memory, and a transaction of any size is
delivered across as many batches as needed.

It is the wrong trade when your sink must apply each source transaction atomically — a ledger,
a materialized view with cross-row invariants, anything where a half-applied transaction is a
state that never existed upstream:

```rust
use rustcdc::{RuntimeOptions, TransactionBoundaryPolicy};

let options = RuntimeOptions::new()
    .with_transaction_boundary(TransactionBoundaryPolicy::PreserveTransactions);
# let _ = options;
```

Under `PreserveTransactions` the runtime withholds the trailing partial transaction from each
batch and delivers it with the next one, so every batch ends on a transaction boundary.

**How the runtime knows a transaction ended.** Two signals count, and nothing else does:
the event declares its own position (`event_index + 1 == total_events`), or a later event
belongs to a different transaction. Absence of a signal is not proof of an ending, so a
transaction whose remaining events have not arrived yet is **withheld** rather than
delivered partially — including when the rest is simply still in flight from the source,
which for a streaming connector is the normal case rather than the exception.


**The one case it cannot honour.** A single transaction larger than `max_buffer_size` does not
fit in any batch. Trimming it would produce an empty batch forever — a silent, permanent stall,
strictly worse than the split it is trying to avoid. The runtime therefore delivers such a
transaction split and logs a `WARN` naming the transaction id and `max_buffer_size`. If the
guarantee has to hold absolutely, raise `max_buffer_size` above the largest transaction the
source produces.

Events with no transaction metadata — snapshot rows, and connectors that do not report
transaction boundaries — are treated as their own boundary and are never trimmed.

## Connector Capabilities

Runtime source selection now exposes explicit connector capabilities through `ConnectorCapabilities`.

```rust
use rustcdc::{ConnectorCapabilities, RuntimeSourceConfig};

let source = RuntimeSourceConfig::Disabled;
let caps: ConnectorCapabilities = source.capabilities();
assert!(!caps.snapshot);
assert!(!caps.handoff);
assert!(!caps.ddl_capture);
```

When running a runtime instance, the same view is available from `source_capabilities()`:

```rust
# use rustcdc::CdcRuntime;
# fn example(runtime: &CdcRuntime) {
let caps = runtime.source_capabilities();
if !caps.snapshot {
  // Guard feature wiring in embedders before attempting snapshot mode.
}
# }
```

For configured PostgreSQL/MySQL/MariaDB/SQL Server sources, the runtime advertises
`snapshot=true`, `handoff=true`, `ddl_capture=true`, `heartbeat=true`, and
`schema_introspection=true`.

The runtime now also provides an embeddable admin/introspection surface that includes
capabilities, readiness/liveness, buffer depth, and delivery counters.

```rust
# use rustcdc::{CdcRuntime, Result};
# fn example(runtime: &CdcRuntime) -> Result<()> {
let admin = runtime.admin_snapshot();
assert_eq!(admin.state, "running");

let json = runtime.admin_snapshot_json()?;
let prometheus = runtime.admin_metrics_prometheus();
# let _ = (json, prometheus);
# Ok(())
# }
```

`admin_snapshot_json()` is intended for control-plane APIs, and
`admin_metrics_prometheus()` emits Prometheus-friendly text for embedding in
lightweight health endpoints.

The runtime constructor enforces capability guards. For example, configuring `snapshot_tables` with a source that does not support snapshots is rejected at construction time.

---

## PostgreSQL Source Configuration

| Field | Type | Default | Purpose |
|---|---|---|---|
| `host` | `String` | — | Host FQDN or IP. |
| `port` | `u16` | 5432 | |
| `user` | `String` | — | Needs the `REPLICATION` role. |
| `password` | `SecretString` | — | Build with `SecretString::new`, `from_provider`, or `from_callback`. |
| `auth_mode` | `DatabaseAuthMode` | `Password` | `AwsIamToken` switches to short-lived IAM token semantics and requires TLS. |
| `database` | `String` | — | Database to replicate from. |
| `replication_slot_name` | `String` | — | e.g. `"rustcdc_slot"`. |
| `publication_name` | `String` | — | Publication used by pgoutput. |
| `create_replication_slot_if_missing` | `bool` | `false` | **Read the note below before setting this.** |
| `failover_slot` | `bool` | `false` | Create the slot with `failover = true` (PostgreSQL 17+) so it survives a promotion. Only applies when this connector creates the slot. |
| `table_include_list` | `Vec<String>` | `[]` | Allowlist in `"schema.table"` form. Non-empty means *only* these tables; takes precedence over the exclude list. Empty means all tables the publication carries. |
| `table_exclude_list` | `Vec<String>` | `[]` | Blocklist in `"schema.table"` form. Ignored when the include list is non-empty. |
| `transport` | `TransportConfig` | TLS | TLS by default when the `tls` feature is on. |
| `conn_timeout_secs` | `u64` | 30 | Range 1–300. |
| `stream_poll_interval_ms` | `u64` | 50 | Range 1–60 000. |
| `max_events_per_poll` | `usize` | 1 000 | Range 1–100 000. |
| `slot_idle_advance_interval_ms` | `u64` | 30 000 | See "Idle slots retain WAL" below. `0` disables. |

**`create_replication_slot_if_missing` is not a convenience flag.** A slot that vanishes
mid-life — dropped by an operator, lost to a failover onto a replica that never had it, or
invalidated by `max_slot_wal_keep_size` — is a *data-loss event*: the WAL it was retaining is
gone. Recreating it silently restarts capture at the current WAL position and skips everything
in between, which looks exactly like healthy operation. Set it `true` only for first-time
provisioning or ephemeral test databases; otherwise create the slot out of band:

```sql
SELECT pg_create_logical_replication_slot('rustcdc_slot', 'pgoutput');
```

**Idle slots retain WAL.** When no committed events are delivered — an idle database, or a
burst of rolled-back transactions — the slot's `confirmed_flush_lsn` stays pinned and
PostgreSQL cannot recycle WAL segments. `slot_idle_advance_interval_ms` makes the connector
periodically call `pg_replication_slot_advance(pg_current_wal_lsn())` after that much time
without events. Disabling it on a long-lived stream is how a disk fills up.


### Secret Loading Patterns

Connector passwords are now modeled as `SecretString`, not raw `String` values.

```rust
use rustcdc::{SecretProvider, SecretString};
use std::sync::Arc;

struct VaultProvider;

impl SecretProvider for VaultProvider {
  fn resolve_secret(&self, reference: &str) -> rustcdc::Result<String> {
    Ok(format!("vault://{reference}"))
  }
}

let inline_secret = SecretString::new("postgres");
let provider_secret = SecretString::from_provider(
  "vault",
  "database/postgres/password",
  Arc::new(VaultProvider),
);
let callback_secret = SecretString::from_callback("runtime-refresh", || {
  std::env::var("CDC_RS_ROTATED_PASSWORD")
    .map_err(|error| rustcdc::Error::ConfigError(error.to_string()))
});
```

Deferred secrets are resolved at validation/connect time and remain redacted in `Debug`/`Display` output.

### Feature-Gated Encryption Transforms

Enable the `encryption` feature to use field-level AES-GCM encryption and decryption through the existing `MaskHashTransform` surface.

```rust
use rustcdc::{MaskHashConfig, MaskHashTransform, MaskRule, SecretString};
// `MaskHashConfig::mask_rules` is an `ahash::AHashMap`, re-exported by the crate's
// dependency — `std::collections::HashMap` will not coerce.
use ahash::AHashMap;

let mut encrypt_rules = AHashMap::new();
encrypt_rules.insert(
  "profile.phone".to_string(),
  MaskRule::Encrypt(SecretString::from_callback("field-key", || {
    std::env::var("CDC_RS_FIELD_KEY")
      .map_err(|error| rustcdc::Error::ConfigError(error.to_string()))
  })),
);

let encrypt_transform = MaskHashTransform::new(MaskHashConfig {
  mask_rules: encrypt_rules,
  default_rule: MaskRule::Null,
});

let mut decrypt_rules = AHashMap::new();
decrypt_rules.insert(
  "profile.phone".to_string(),
  MaskRule::Decrypt(SecretString::from_callback("field-key", || {
    std::env::var("CDC_RS_FIELD_KEY")
      .map_err(|error| rustcdc::Error::ConfigError(error.to_string()))
  })),
);

let decrypt_transform = MaskHashTransform::new(MaskHashConfig {
  mask_rules: decrypt_rules,
  default_rule: MaskRule::Null,
});
```

Encrypted fields are emitted as `enc:<nonce_b64>:<ciphertext_b64>` strings and decrypted back into their original JSON values with the matching key.

Format/KDF contract for current unversioned payloads:
- AEAD: AES-256-GCM
- Nonce: 12 random bytes (base64 encoded)
- KDF: HKDF-SHA-256, 32-byte output, no salt
- HKDF info label: `b"rustcdc-field-encryption"`

Future backward-compatibility rollout plan (when versioning becomes necessary):
- phase 1: decrypt supports both legacy unversioned and new versioned payloads
- phase 2: encrypt emits only the new versioned payload format
- phase 3: after migration window, remove legacy decrypt support with release-note callout

### Field Mapping Transform

Use `FieldMappingTransform` for high-value schema-alignment operations without
custom code:

- copy fields (`copy`)
- rename/move fields (`rename`)
- inject static literals (`set_literals`)
- remove fields (`remove`)

Paths use dot notation (`profile.email`, `meta.source`).

```rust
use rustcdc::{FieldMappingConfig, FieldMappingTransform};
use serde_json::json;

# fn example() -> rustcdc::Result<()> {
let transform = FieldMappingTransform::new(FieldMappingConfig {
  copy: vec![("user.email".into(), "email".into())],
  rename: vec![("user.name".into(), "user.full_name".into())],
  set_literals: vec![("meta.pipeline".into(), json!("orders"))],
  remove: vec!["legacy_flag".into()],
  strict: true,
})?;
# Ok(())
# }
```

`strict = true` fails fast when copy/rename/remove source paths are missing,
which helps catch drift during schema evolution and replay.

**Replay determinism caveat (important):**
- `MaskRule::Encrypt` is intentionally nonce-based and therefore non-deterministic.
- Replaying the same logical event will produce different ciphertext bytes.
- Use encryption rules only when your downstream dedup/idempotency logic does not depend on byte-identical payload replay.
- For replay-sensitive pipelines, prefer deterministic masking rules — `UnsaltedSha256`,
  `HmacSha256` (keyed, and the GDPR-appropriate choice), `Redact`, `Truncate`, `Null` — on
  fields that participate in replay comparisons. (There is no `MaskRule::Hash`.)

**Transport Selection:**
- `TransportConfig::tls()` (default with `tls` feature): TLS with system trust store
- `TransportConfig::tls_with_ca_cert_path(path)`: TLS with explicit CA bundle
- `TransportConfig::tls_insecure_skip_verify()`: TLS with certificate/hostname verification disabled (testing or tightly controlled air-gapped environments only)
- `TransportConfig::plaintext()`: unencrypted transport — credentials and data transmitted in the clear

Use TLS transport for all production connector configurations.
`TransportConfig::plaintext()` is provided as an explicit escape hatch for trusted
private networks and local integration testing only — never use it in production.

**Connection Retry Policy:**

Set `RuntimeOptions.connection_retry` to automatically retry recoverable source
connection failures with truncated exponential backoff:

```rust
use rustcdc::{ConnectionRetryPolicy, RuntimeOptions};
# use rustcdc::{checkpoint::InMemoryCheckpoint, schema_history::InMemorySchemaHistory,
#     RuntimeConfig, RuntimeSourceConfig};
# let (source, checkpoint, schema_history) = (
#     RuntimeSourceConfig::Disabled,
#     InMemoryCheckpoint::default(),
#     InMemorySchemaHistory::default(),
# );
// `with_connection_retry` lives on `RuntimeOptions`, not on `RuntimeConfig`.
let config = RuntimeConfig::new(source, checkpoint, schema_history)
    .with_options(RuntimeOptions::new().with_connection_retry(
        ConnectionRetryPolicy::new()
            .with_max_retries(Some(5))    // None retries indefinitely
            .with_initial_delay_ms(300)   // first retry after 300 ms
            .with_max_delay_ms(10_000),   // backoff capped at 10 s
    ));
# let _ = config;
```

Retry applies to recoverable errors only: an unclassified `SourceError`, a `TimeoutError`,
or a classified source error whose `SourceErrorKind` is recoverable
(`NetworkTransient`, `QuotaExceeded`, `Unknown`). `AuthFailed`, `SchemaMismatch` and
`SlotNotFound` are **not** retried — they need an operator, and retrying only delays the page.
Fatal errors (`ConfigError`, `ValidationError`, `Unrecoverable`) propagate immediately.

> **Operational warning — `max_retries: None` (indefinite retry):**
> Setting `max_retries: None` causes the runtime to retry failed source
> connections forever. This is appropriate for highly-available deployments
> where the source database is expected to recover (e.g., failover, transient
> network blips), but it **masks dead source connections indefinitely**.
> If your monitoring relies on `poll_event_batch` returning an error to
> trigger alerts or circuit-breaking logic, indefinite retry will prevent
> that signal from surfacing.
>
> **Recommendations for `max_retries: None`:**
> - Set a `replication_lag_ms` alert threshold in your observability stack;
>   rising lag indicates the source is unreachable even when the runtime
>   does not surface an error.
> - Emit a dead-man's-switch metric: if `total_events_polled` stops growing
>   for an unexpectedly long window, treat the pipeline as stalled.
> - Consider bounded retry (`max_retries: Some(N)`) with external restart
>   orchestration (e.g., Kubernetes pod restart policy) so stalled pipelines
>   surface cleanly rather than silently burning CPU in a backoff loop.

### Connector-Specific Post-Commit Confirmation Semantics

`commit_ack()` has a uniform API but connector confirmation semantics are intentionally connector-specific:

- PostgreSQL:
  - Runtime confirms durable progress via replication-slot LSN confirmation.
  - Post-commit confirmation failures are governed by `PostCommitSourceConfirmPolicy`.
- MySQL:
  - Runtime durability is checkpoint-first.
  - `confirm_lsn` is a connector compatibility hook and does not provide PostgreSQL-style slot advancement semantics.
- SQL Server:
  - Runtime durability is checkpoint-first.
  - `confirm_lsn` is a connector compatibility hook and does not provide PostgreSQL-style slot advancement semantics.

Operationally, all connectors remain at-least-once at the runtime boundary; downstream idempotency remains mandatory.

**Resumable Snapshot Cursoring:**
- Snapshot resume uses primary-key keyset cursoring (not `ctid`).
- Tables configured for resumable snapshots must expose a primary key.
- Tables without a primary key are rejected for resumable snapshots.
- This prevents physical tuple cursor instability during long-running snapshots with concurrent writes.

---

## Column type mapping

Every event payload is JSON, so each source type is rendered into a JSON value. Where the
mapping is not obvious — or where getting it wrong produces a *plausible* wrong value rather
than an error — it is pinned by the type-fidelity integration suites
(`tests/{postgres,mysql,sqlserver}_type_fidelity_integration.rs`), which assert exact decoded
values against real databases.

### The general rules

| Source shape | JSON | Note |
|---|---|---|
| Integers within `i64`/`u64` | number | |
| Exact numerics (`DECIMAL`, `NUMERIC`, `MONEY`) | string | Rendering as a JSON number would round-trip through a float and lose the low digits. |
| Floating point | number | |
| Booleans | boolean or number | PostgreSQL sends `t`/`f`; MySQL `TINYINT(1)` is a number. |
| Text | string | |
| Binary (`BYTEA`, `VARBINARY`, `BLOB`) | string | Hex-encoded. Lossy UTF-8 transcoding would deliver a replacement character as though it were the stored value. |
| JSON / JSONB | string | The source's own serialization, preserved verbatim. |
| `NULL` | `null` | Present as a key with a null value — **not** an absent key. |
| Value the source could not supply | *key absent* | Listed in `unavailable_columns`; see [partial payloads](@/docs/api.md#partial-payloads-read-this-before-writing-a-sink). |

The last two rows are the distinction that matters most: a missing key means "no information",
a `null` means "the value is NULL". Collapsing them is the classic CDC corruption.

### MySQL and MariaDB temporal and enumerated types

The binlog stores these in encodings that do not survive a naive read, so the connector
consults the table-map metadata rather than the value alone.

| Column type | JSON | Why it needs the metadata |
|---|---|---|
| `DATE` | `"2026-07-20"` | The binlog value is a full timestamp tuple shared with `DATETIME`. Rendering by value alone reports a midnight time the source never carried; truncating whenever the time is zero would instead strip the time from a `DATETIME` that genuinely falls at midnight. Only the column type separates them. |
| `DATETIME`, `TIMESTAMP` | `"2026-07-20T12:34:56"` or `...T12:34:56.789012` | Fractional seconds appear only when the column declares them; a fixed width would fabricate or truncate precision. |
| `TIME` | `"d:hh:mm:ss.uuuuuu"` | MySQL `TIME` can exceed 24 hours, so it is not a clock time. |
| `ENUM` | the **label**, e.g. `"happy"` | The binlog carries the 1-based ordinal. Forwarding it delivers `1` where the row holds `'happy'` — a plausible integer that silently means something different as soon as the enum's declaration order changes. The labels come from the table-map optional metadata. |
| `SET` | comma-joined labels, e.g. `"read,write"` | The binlog carries a little-endian bitmask in raw bytes. Reading those bytes as text yields control characters that are valid UTF-8, so the wrong reading fails *silently*. |

Both `ENUM` and `SET` labels require `binlog_row_metadata=FULL`, which rustcdc already demands
for column names and key flags. Without it the values fall back to the raw ordinal and mask.

> **ENUM ordinal `0`** is MySQL's "invalid value" slot, produced by a non-strict-mode insert of
> an unlisted value. It maps to the empty string, as MySQL itself displays it — not to the
> first variant.

### SQL Server

`DECIMAL`, `NUMERIC`, `MONEY`, `DATETIME2`, `DATETIMEOFFSET`, `TIME`, `UNIQUEIDENTIFIER`,
`VARBINARY` and `XML` all decode to their exact values. This is called out explicitly because
an earlier version of the connector returned `null` for five of them — indistinguishable from a
genuine SQL NULL, delivered as an authentic value, with no error anywhere. The fidelity suite
now asserts non-null on every `NOT NULL` column precisely to catch a regression of that shape.

## MySQL Source Configuration

### Required server configuration

`MysqlConnection::connect()` validates these and **fails loud** if they are unsuitable. Each
unsuitable value would otherwise cause *silent* corruption rather than an error at decode time,
so none of them can be downgraded to a warning.

| Variable | Required | MySQL 8 default | Why |
|---|---|---|---|
| `log_bin` | `ON` | `ON` | No binlog, no CDC. |
| `binlog_format` | `ROW` | `ROW` | Statement-based logging cannot identify which rows changed. |
| `binlog_row_metadata` | **`FULL`** | ⚠️ **`MINIMAL`** | Under `MINIMAL` the binlog carries **no column names and no primary-key flags**. Events would be emitted with positional placeholder keys (`@0`, `@1`, …) instead of real column names, and `primary_key: None` — which additionally disables snapshot/stream duplicate suppression and incremental-snapshot override suppression. |
| `binlog_row_image` | **`FULL`** | `FULL` | Under `MINIMAL`/`NOBLOB` the binlog records only a subset of columns, so UPDATE after-images are emitted as if complete while silently missing columns. A consumer performing an upsert would erase them. |
| `binlog_row_value_options` | **empty** | empty | `PARTIAL_JSON` makes the server write JSON *diffs* instead of complete values. rustcdc cannot apply those diffs, and the failure recurs on every restart because it precedes the checkpoint advance — stalling the pipeline permanently. |

```sql
SET GLOBAL binlog_row_metadata     = FULL;
SET GLOBAL binlog_row_image        = FULL;
SET GLOBAL binlog_row_value_options = '';
```

Persist these in `my.cnf` so they survive a restart. Note `binlog_row_metadata` only affects binlog
events written **after** the change — existing binlog content keeps the old encoding.

> **MariaDB:** `binlog_row_metadata` and `binlog_row_value_options` do not exist. The connector
> detects their absence and skips those two checks rather than failing.

### GTID positioning

When `gtid_mode_enabled` is set, the connector resumes by **GTID set** rather than by
binlog file+position. This matters for failover: binlog coordinates are *server-local*, so
`binlog.000042:88371` addresses an unrelated point on a promoted replica. GTIDs are globally
meaningful, which is the reason they exist.

The checkpoint accumulates a full executed set (`uuid:1-500,uuid2:1-7`), coalescing adjacent
intervals so it stays compact over a long-running stream. Encoding of the
`COM_BINLOG_DUMP_GTID` packet is delegated to `mysql_common`, so the text form written to the
checkpoint and the bytes sent to the server cannot drift apart.

> **The checkpoint must be a set, never a single GTID.** Resuming from a bare `uuid:501`
> tells the server the replica has executed only transaction 501, and it replays 1–500.

### Binlog retention and resume safety

On resume, the connector verifies the server still retains everything the checkpointed
position has not consumed, using `GTID_SUBSET(@@GLOBAL.gtid_purged, <checkpoint position>)`.
If the check fails it stops with an `Unrecoverable` error naming the exact purged-but-unread
transactions, rather than letting the server fail with a generic *"could not find first log
file"* that says nothing about how much was lost.

Set `binlog_expire_logs_seconds` so retention comfortably exceeds your maximum expected
connector downtime. (Note `expire_logs_days` was **removed** in MySQL 8.4 — using it now
raises an error at startup.)

> The subset direction matters and is easy to invert. The correct test is "everything the
> server purged, I already consumed". The intuitive inverse — "my position is a subset of
> what the server executed" — **fails open**: it reports available in precisely the gap case.

| Field | Type | Default | Purpose |
|---|---|---|---|
| `host` | `String` | — | Host FQDN or IP. |
| `port` | `u16` | 3306 | |
| `user` | `String` | — | Needs `REPLICATION CLIENT` and `SELECT`. |
| `password` | `SecretString` | — | |
| `auth_mode` | `DatabaseAuthMode` | `Password` | `AwsIamToken` requires TLS. |
| `database` | `String` | — | Database to replicate from. |
| `server_id` | `u32` | `0` — **invalid on purpose** | Replication server id for the binlog client. MySQL treats `0` as unassigned and `validate()` rejects it, so you must set a unique id per connector instance. |
| `server_flavor` | `ServerFlavor` | `Mysql` | Set `MariaDb` when connecting to MariaDB: `source_type()` then returns `"mariadb"` and checkpoints use a separate `checkpoint_mariadb.json`. |
| `gtid_mode_enabled` | `bool` | `false` | Whether GTID mode is enabled on the server. |
| `binlog_format_check` | `bool` | `true` | Validate `binlog_format = ROW` before streaming. |
| `table_include_list` | `Vec<String>` | `[]` | Allowlist in `"schema.table"` form; takes precedence over the exclude list. |
| `table_exclude_list` | `Vec<String>` | `[]` | Blocklist in `"schema.table"` form. |
| `transport` | `TransportConfig` | TLS | |
| `conn_timeout_secs` | `u64` | 30 | Range 1–300. |
| `stream_poll_interval_ms` | `u64` | 50 | Range 1–60 000. |
| `max_events_per_poll` | `usize` | 1 000 | Range 1–100 000. |
| `handoff_overlap_drain_budget_ms` | `u64` | `stream_poll_interval_ms * 8` | Wall-clock budget for draining overlap events during snapshot-to-stream handoff. `0` disables the budget (unlimited drain). |

**Why `server_id` has no auto-generated default.** Auto-generation was removed because
PID-hash collisions caused silent event loss in multi-instance deployments: two readers
sharing a `server_id` cause the server to evict one, and the eviction surfaces only as a
generic disconnect. A deliberately invalid default forces the decision to be made once,
explicitly.

**Why the handoff drain has a time budget.** The previous implementation capped overlap
draining at a hard-coded eight polls. On a high-traffic table with large batches that cap was
exhausted before the overlap was drained, and the connector silently delivered duplicate rows.
The budget is wall-clock instead, and exhausting it emits a `WARN` naming the residual count
rather than passing the duplicates off as normal.


### MySQL GTID String Format

```text
GTID Set Format: "source_id:interval[, ...]"
Example: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
```

---

## MariaDB Source Configuration

MariaDB uses the same MySQL-protocol transport stack, but rustcdc exposes it as a first-class source identity through [`MariaDbSourceConfig`] and `RuntimeSourceConfig::mariadb(...)`.

Use MariaDB when you need distinct checkpoint naming, source labeling, or runtime routing while keeping the same underlying binlog transport semantics as MySQL.

```rust
use rustcdc::{MariaDbSourceConfig, RuntimeSourceConfig};

// `MariaDbSourceConfig` is a newtype over `MysqlSourceConfig` that forces
// `server_flavor = MariaDb`, so it is built with the `with_*` builders rather than
// struct-literal syntax.
let source = MariaDbSourceConfig::default()
    .with_host("localhost")
    .with_port(3306)
    .with_user("cdc_user")
    .with_password("cdc_password") // prefer SecretString::from_callback in production
    .with_database("events");

let runtime_source = RuntimeSourceConfig::mariadb(source);
# let _ = runtime_source;
```

MariaDB supports the same startup, snapshot, and streaming modes as MySQL, but emits `source_type() == "mariadb"` and uses MariaDB-specific checkpoint identifiers.

> **GTID Format Warning:** MariaDB uses a distinct GTID format — `domain_id-server_id-sequence_no`
> (e.g. `0-1-12345`) — that is **incompatible** with MySQL's `uuid:interval` format
> (e.g. `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5`). Never mix checkpoint files between
> MySQL and MariaDB instances, even if the schemas are identical. Doing so will produce
> invalid GTID resume positions and cause the connector to silently restart replication
> from the beginning or raise a fatal position error. Always use
> `RuntimeSourceConfig::mariadb(...)` (not `RuntimeSourceConfig::mysql(...)`) when
> connecting to a MariaDB server to ensure correct checkpoint namespace isolation.

---

## SQL Server Source Configuration

| Field | Type | Default | Purpose |
|---|---|---|---|
| `host` | `String` | — | Host FQDN or IP. |
| `port` | `u16` | 1433 | |
| `user` | `String` | — | Needs the `CDC_ADMIN` role. |
| `password` | `SecretString` | — | |
| `database` | `String` | — | CDC must be enabled on this database. |
| `instance_name` | `Option<String>` | `None` | Named instance; `None` uses the default instance. |
| `cdc_enabled` | `bool` | `true` | Require CDC to be enabled on the database, and fail connect if it is not. |
| `cdc_schema` | `String` | `"cdc"` | Schema holding the CDC capture tables. |
| `capture_truncate_events` | `bool` | `false` | Capture `TRUNCATE TABLE` via a DDL trigger; see below. |
| `table_include_list` | `Vec<String>` | `[]` | Allowlist in `"schema.table"` form; takes precedence over the exclude list. |
| `table_exclude_list` | `Vec<String>` | `[]` | Blocklist in `"schema.table"` form. |
| `transport` | `TransportConfig` | TLS | |
| `conn_timeout_secs` | `u64` | 30 | Range 1–300. |
| `prereq_pool_size` | `usize` | 4 | Concurrent connections used by prerequisite checks. Range 1–64. |
| `stream_poll_interval_ms` | `u64` | 5 000 | Range 1–60 000. **See the latency note below.** |
| `max_events_per_poll` | `usize` | 10 000 | Range 1–100 000. |

> **⚠️ SQL Server CDC is polling-based, not event-driven.** p99 latency is approximately
> `stream_poll_interval_ms` plus the CDC capture agent's own delay. Reduce the interval to
> 500–1 000 ms for latency-sensitive workloads, and do not compare SQL Server latency numbers
> against the log-based connectors as though they measured the same thing.

**Capturing TRUNCATE.** SQL Server's `cdc.fn_cdc_get_all_changes_*` cannot see `TRUNCATE
TABLE`, because TRUNCATE bypasses row-level logging. With `capture_truncate_events = true`,
rustcdc creates a shadow table (`[<cdc_schema>].[rustcdc_truncate_events]`) and a
database-level DDL trigger (`rustcdc_truncate_capture`) on first connect. The trigger records
the affected schema and table along with the current CDC maximum LSN from
`sys.fn_cdc_get_max_lsn()`; rustcdc polls that shadow table alongside the change tables and
emits `Operation::Truncate` positioned after all DML at or before that LSN.

The connected user needs `db_owner`, `db_ddladmin` or `sysadmin` to create those objects —
already required for CDC administration. They are created idempotently and survive restarts.
Ordering is **best-effort**: the truncate lands after every DML change whose commit LSN is at
or before the LSN captured when the trigger fired, which is as precise as SQL Server allows
for an operation that bypasses row-level logging.


### AWS IAM Auth Mode (MySQL/PostgreSQL)

For RDS-style IAM database auth, use connector `auth_mode = AwsIamToken` and
resolve the token through `SecretString::from_callback` (or provider) so each
new connection can fetch a fresh short-lived token.

TLS is mandatory when `auth_mode = AwsIamToken`.

### SQL Server Connection String Format

```text
sqlserver://user:password@host:port;database=dbname;TrustServerCertificate=no;Encrypt=yes
```

---

## Checkpoint Configuration

### InMemoryCheckpoint

**Use Case:** Development, testing, single-machine deployments (volatile)

```rust
use rustcdc::checkpoint::InMemoryCheckpoint;

let checkpoint = InMemoryCheckpoint::default();
// Keeps checkpoint in memory; lost on process restart
```

### FileCheckpoint

**Use Case:** Local machine deployments; single-machine production (persistent but not HA)

```rust
use rustcdc::checkpoint::FileCheckpoint;

// Default: 0o600 (owner read/write only — enforced at load time).
let checkpoint = FileCheckpoint::new("/var/rustcdc/checkpoints");
// Stores checkpoint in JSON file; atomically updated via write-rename.
```

File permissions are enforced at load time: if the checkpoint file on disk has
mode bits accessible to group or other (e.g. 0o644), the load is rejected with
a `CheckpointError`. This protects connection credentials embedded in the
checkpoint from unauthorized access. Do not set a mode wider than 0o600.

**File Location Format:**
```text
/var/rustcdc/checkpoints/checkpoint_postgres.json
/var/rustcdc/checkpoints/checkpoint_mysql.json
/var/rustcdc/checkpoints/checkpoint_sqlserver.json
```

**File Content Example:**
```json
{
  "checkpoint_format_version": 1,
  "source_type": "postgres",
  "committed_event_count": 12345,
  "offset": {
    "lsn": 281474976711680,
    "slot_name": "rustcdc_postgres_abc123"
  },
  "content_checksum": "9f2b...(SHA-256 over the four fields above)"
}
```

**Checkpoint Format Version Policy:**
- `checkpoint_format_version = 1` is the current write format.
- `checkpoint_format_version` is required for all file checkpoints.
- Unknown or missing versions are rejected at load time.
- rustcdc intentionally enforces fail-closed checkpoint decoding for format safety.

**Integrity:**

`content_checksum` is a SHA-256 over the other fields. It is verified on every load, and a
mismatch is a hard error rather than a resume.

This matters because checkpoint corruption is otherwise **silent**. A flipped bit in an LSN
or binlog position does not fail to parse — it resumes capture from a *wrong* position,
skipping events with no error raised anywhere.

The practical consequence: **checkpoint files cannot be edited or generated by hand.** For
disaster recovery use the bundled seeding tool, which computes the checksum, writes
atomically, applies the required file mode and fsyncs the parent directory:

```bash
cargo run --example seed_checkpoint --features postgres -- \
  --dir /var/rustcdc/checkpoints \
  --source-type postgres \
  --committed-event-count 0 \
  --offset '{"lsn": 281474976711680, "slot_name": "rustcdc_postgres_new"}'
```

Programmatically, the same operation is `FileCheckpoint::restore_from_record`.

### Custom Durable Checkpoint Backend

**Use Case:** High-availability or centralized checkpoint management

rustcdc currently ships with `FileCheckpoint` and `InMemoryCheckpoint`.
For HA or centralized state, implement the `Checkpoint` trait against your
own storage backend (for example PostgreSQL, Redis, object storage, or a
platform metadata service).

---

## Observability Configuration

### NoOp Observability (Default)

```rust,ignore
use rustcdc::{RuntimeConfig, RuntimeObservability};

// Metrics and tracing are disabled by default via explicit runtime observability options.
let config = RuntimeConfig::new(...)
  .with_observability(RuntimeObservability::default());
```

### OpenTelemetry Observability

```rust,ignore
// Requires --features metrics. `RuntimeConfig::new(...)` stands in for your own
// source/checkpoint/schema-history arguments.
use rustcdc::{OTelConfig, OTelEventTracer, OTelMetricsCollector, RuntimeConfig, RuntimeObservability};
use std::sync::Arc;

let otel_config = OTelConfig::new(
    "http://otel-collector:4317",  // OTLP gRPC endpoint
    "rustcdc",                        // Service name
    "0.8.0",                         // Service version
    "production",                    // Environment
);

let metrics = Arc::new(OTelMetricsCollector::with_otlp_exporter(otel_config.clone())?);
let tracer = Arc::new(OTelEventTracer::with_otlp_exporter(otel_config)?);

let config = RuntimeConfig::new(...)
  .with_observability(
    RuntimeObservability::default()
      .with_metrics(metrics)
      .with_tracer(tracer)
  );
```

### Runtime Admin Metrics (`CdcRuntime::admin_metrics_prometheus()`)

| Metric | Type | Description |
|--------|------|-------------|
| `rustcdc_runtime_readiness` | Gauge | Runtime readiness (1 ready, 0 not ready) |
| `rustcdc_runtime_liveness` | Gauge | Runtime liveness (1 alive, 0 stopped) |
| `rustcdc_runtime_buffer_depth` | Gauge | Buffered events waiting for delivery |
| `rustcdc_runtime_in_flight_events` | Gauge | Delivered but uncommitted events |
| `rustcdc_runtime_events_polled_total` | Counter | Total events delivered by runtime batches |
| `rustcdc_runtime_events_committed_total` | Counter | Total acknowledged and checkpointed events |
| `rustcdc_runtime_events_deduplicated_total` | Counter | Total events suppressed by idempotency guard |
| `rustcdc_runtime_events_skipped_total` | Counter | Events permanently dropped by `TransformErrorPolicy::Skip`. **Any non-zero value means data was lost** — the checkpoint advances past skipped events, so they are never replayed. Alert on any increase. |
| `rustcdc_runtime_idempotency_evictions_total` | Counter | Fingerprints evicted because the idempotency window filled. Growing steadily means the window is too small for this deployment's replay distance; raise `IdempotencyOptions::capacity`. |
| `rustcdc_runtime_idempotency_unidentifiable_total` | Counter | Events passed through undeduplicated because they carry neither transaction metadata nor a resolvable primary key. Expected for keyless tables. |
| `rustcdc_runtime_health` | Gauge | Derived health verdict, one series per `verdict` label. **`rustcdc_runtime_health{verdict="stalled"} == 1` is the alert rule** — `state` alone cannot distinguish healthy-idle from stalled. |
| `rustcdc_runtime_checkpoint_age_ms` | Gauge | Age of last durable checkpoint |
| `rustcdc_runtime_replication_lag_ms` | Gauge | Estimated source lag in milliseconds |
| `rustcdc_replication_slot_lag_bytes` | Gauge | PostgreSQL replication slot WAL lag (`pg_current_wal_lsn - confirmed_flush_lsn`). **The single most operationally critical PostgreSQL signal**: a monotonically growing value means the slot is pinning WAL on the primary until the disk fills. Page on sustained growth. |
| `rustcdc_runtime_source_capability` | Gauge | Connector capability flags, one series per `capability` label |

### OpenTelemetry Exported Metrics (`OTelMetricsCollector`)

| Metric | Type | Description |
|--------|------|-------------|
| `rustcdc.events.processed` | Counter | Total events successfully processed |
| `rustcdc.events.filtered` | Counter | Events dropped by transform pipeline |
| `rustcdc.errors` | Counter | Total errors encountered |
| `rustcdc.checkpoint.committed_count` | Counter | Total events committed to checkpoint |
| `rustcdc.replication_lag_ms` | Gauge | Estimated replication lag in milliseconds |
| `rustcdc.replication_lag_events` | Gauge | Estimated events not yet consumed |
| `rustcdc.checkpoint_offset` | Gauge | Current checkpoint offset (source-specific encoding) |
| `rustcdc.buffer_size` | Gauge | Current buffered event count |
| `rustcdc.snapshot_progress` | Gauge | Current snapshot completion percentage |
| `rustcdc.event_processing_duration` | Histogram | Event processing latency (ms) |
| `rustcdc.checkpoint_commit_duration` | Histogram | Checkpoint commit latency (ms) |

### Structured Log Fields

All logs include:
- `source_type`: Connector type (postgres, mysql, sqlserver)
- `timestamp`: ISO 8601 timestamp
- `level`: ERROR, WARN, INFO, DEBUG, TRACE
- `message`: Human-readable description
- Context fields (when applicable):
  - `table`: Table name
  - `event_count`: Number of events
  - `offset`: Source-specific position
  - `error`: Error details (sanitized)

**Enable Logging:**

```bash
# Set environment variable
export RUST_LOG=rustcdc=info,rustcdc::source=debug

# Run with structured JSON output
export RUST_LOG_FORMAT=json
```

---

## Production Recommendations

### Checkpoint Store Selection

| Scenario | Recommendation | Rationale |
|----------|---|----------|
| Single machine, restarts acceptable | FileCheckpoint | Simple, no external dependencies |
| HA cluster, centralized state | Custom `Checkpoint` backend | Integrates with your existing HA metadata store |
| Development/testing | InMemoryCheckpoint | Fast iteration; ephemeral OK |

### Buffer Size Tuning

```text
Throughput-Focused (High Latency Acceptable):
  max_buffer_size = 100_000
  max_poll_wait_ms = 5_000
  → Batches large groups; fewer commits

Latency-Focused (Lower Throughput):
  max_buffer_size = 10_000
  max_poll_wait_ms = 1_000
  → Frequent commits; sub-second latency

Balanced (Recommended):
  max_buffer_size = 50_000
  max_poll_wait_ms = 2_000
  → ~50-100ms latency; 1K-2K commits/sec
```

### Connector Scaling Envelopes

Use these as baseline production profiles, then tune with real workload evidence.

**SQL Server connector tuning (`SqlServerSourceConfig`):**

| Profile | `prereq_pool_size` | `stream_poll_interval_ms` | `max_events_per_poll` | Suggested Use |
|---|---:|---:|---:|---|
| Low-latency | 4 | 250 | 5000 | Near-real-time dashboards, lower throughput |
| Balanced (default-ish) | 4-8 | 1000 | 10000-20000 | General production workloads |
| Throughput-heavy | 8-16 | 2000-5000 | 20000-50000 | Backfills, bursty write workloads |

**PostgreSQL connector tuning (`PostgresSourceConfig`):**

| Profile | `stream_poll_interval_ms` | `max_events_per_poll` | Suggested Use |
|---|---:|---:|---|
| Low-latency | 10-25 | 250-500 | Interactive workloads where update freshness is prioritized |
| Balanced (default-ish) | 50-250 | 1000-5000 | General production workloads |
| Throughput-heavy | 250-1000 | 5000-20000 | Backfills, high sustained ingest |

**MySQL connector tuning (`MysqlSourceConfig`):**

| Profile | `stream_poll_interval_ms` | `max_events_per_poll` | Suggested Use |
|---|---:|---:|---|
| Low-latency | 10-25 | 250-500 | Interactive workloads where update freshness is prioritized |
| Balanced (default-ish) | 50-250 | 1000-5000 | General production workloads |
| Throughput-heavy | 250-1000 | 5000-20000 | Backfills, high sustained ingest |

For sustained saturation, combine connector tuning with runtime delivery controls (`RuntimeOptions.max_buffer_size`, `RuntimeOptions.max_poll_wait_ms`) and horizontal partitioning.

### TLS Best Practices

```rust
use rustcdc::TransportConfig;

// Recommended: explicit CA bundle in production.
let transport =
    TransportConfig::tls_with_ca_cert_path(Some("/etc/ssl/certs/company-ca.pem".to_string()));

// Also valid: rely on system trust store.
let transport = TransportConfig::tls();

// Testing/air-gapped fallback only: disable certificate + hostname verification.
let transport = TransportConfig::tls_insecure_skip_verify();

// Plaintext: only for trusted private networks or local integration testing.
// Credentials and event data are transmitted unencrypted.
let transport = TransportConfig::plaintext();
```

Connector config helpers now provide explicit transport selection APIs:

```rust,ignore
// Requires the mysql, postgres and sqlserver features together.
let mysql_cfg = MysqlSourceConfig::default().with_plaintext_transport();
let pg_cfg = PostgresSourceConfig::default().with_plaintext_transport();
let mssql_cfg = SqlServerSourceConfig::default().with_plaintext_transport();

let mysql_tls = mysql_cfg.with_tls_transport();
```

### Monitoring Checklist

- [ ] Alert on `rustcdc_runtime_replication_lag_ms > 30000` (30s)
- [ ] Alert on `rustcdc_runtime_liveness == 0`
- [ ] Alert on `rustcdc_runtime_checkpoint_age_ms > 10000`
- [ ] Alert on `rustcdc_runtime_events_polled_total` trend deviation > 20%
- [ ] Dashboard: Replication lag trend over 24h
- [ ] Dashboard: Event processing rate (events/sec)
- [ ] Dashboard: Checkpoint commit latency distribution

---

**Last Updated:** May 25, 2026  
**Version:** Configuration Reference v0.1+
