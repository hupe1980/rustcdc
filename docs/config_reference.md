# rustcdc Configuration Reference

**Version:** v0.1+  
**Audience:** Platform engineers and application developers embedding rustcdc

---

## Table of Contents

1. [RuntimeConfig](#runtimeconfig)
2. [Runtime Consumption Model](#runtime-consumption-model)
3. [Connector Capabilities](#connector-capabilities)
4. [PostgreSQL Source Configuration](#postgresql-source-configuration)
5. [MySQL Source Configuration](#mysql-source-configuration)
6. [SQL Server Source Configuration](#sql-server-source-configuration)
7. [Checkpoint Configuration](#checkpoint-configuration)
8. [Observability Configuration](#observability-configuration)
9. [Production Recommendations](#production-recommendations)

---

## RuntimeConfig

Core runtime configuration for CDC operations.

```rust
pub struct RuntimeConfig<C, H> {
  /// Typed source connector configuration
  pub source: RuntimeSourceConfig,
    
    /// List of tables to snapshot on initial run (when no checkpoint exists)
    /// Format: ["schema.table", "schema.table2"]
    /// Leave empty to start in stream-only mode on first run
    pub snapshot_tables: Vec<String>,
    
    /// Checkpoint backend (pluggable trait implementation)
    pub checkpoint: C,
    
    /// Schema history backend (pluggable trait implementation)
    pub schema_history: H,

    /// Explicit runtime options surface including observability and tuning.
    pub options: RuntimeOptions,
}
```

  `RuntimeOptions` contains the operational knobs that used to live as top-level runtime fields:

  ```rust
  pub struct RuntimeOptions {
    pub observability: RuntimeObservability,
    pub max_buffer_size: usize,
    pub max_poll_wait_ms: u64,
    pub transform_error_policy: TransformErrorPolicy,
    pub post_commit_source_confirm_policy: PostCommitSourceConfirmPolicy,
    pub idempotency: Option<IdempotencyOptions>,
    pub validate_events: bool,
    pub schema_history_retention: Option<SchemaHistoryRetention>,
    /// Exponential-backoff retry policy for recoverable source connection errors.
    /// `None` disables retry (errors propagate immediately).
    pub connection_retry: Option<ConnectionRetryPolicy>,
  }
  ```

Default runtime safety posture:
- `transform_error_policy = Halt`
- `post_commit_source_confirm_policy = FailFast`
- `validate_events = true`
- `schema_history_retention = Some(SchemaHistoryRetention::keep_last(256))`

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
  password: SecretString::from_env("CDC_RS_POSTGRES_PASSWORD"),
  database: "mydb".into(),
  replication_slot_name: "rustcdc_slot".into(),
  publication_name: "rustcdc_publication".into(),
  conn_timeout_secs: 30,
  ..PostgresSourceConfig::default()
};

let config = RuntimeConfig::new(RuntimeSourceConfig::Postgres(source), checkpoint, schema_history)
    .with_snapshot_tables(vec!["public.users".to_string(), "public.orders".to_string()])
    .with_max_buffer_size(50_000)
    .with_max_poll_wait_ms(2_000)
    .with_transform_error_policy(rustcdc::TransformErrorPolicy::Halt);
```

## Runtime Consumption Model

The preferred embedder surface is now batch-oriented rather than count-oriented.

`poll_event_batch()` returns an `EventBatch` containing the delivered events plus an opaque `AckToken`. Re-polling before acknowledgement redelivers the same in-flight batch, which keeps retry behavior loss-safe.

```rust
use rustcdc::{CdcRuntime, EventBatch, Result};

async fn consume_once<C, H>(runtime: &mut CdcRuntime<C, H>) -> Result<()>
where
  C: rustcdc::checkpoint::Checkpoint + Send + Sync + 'static,
  H: rustcdc::schema_history::SchemaHistory + Send + Sync + 'static,
{
  let batch = runtime.poll_event_batch().await?;
  if batch.is_empty() {
    return Ok(());
  }

  if let Some(token) = batch.ack_token() {
    runtime.commit_ack(token).await?;
  }

  Ok(())
}
```

For partial acknowledgement, split the token and commit only the accepted prefix. The remaining suffix will be re-delivered on the next poll.

```rust
let batch = runtime.poll_event_batch().await?;
if let Some(token) = batch.ack_token() {
  let (accepted, _retry_later) = token.split_at(10)?;
  runtime.commit_ack(accepted).await?;
}
```

`event_batches()` exposes the same model as a stream of non-empty `EventBatch` values.

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

`poll_event_batch()` + `commit_ack(token)` is now the canonical runtime acknowledgement API.

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
let caps = runtime.source_capabilities();
if !caps.snapshot {
  // Guard feature wiring in embedders before attempting snapshot mode.
}
```

For configured PostgreSQL/MySQL/SQL Server sources, the runtime advertises
`snapshot=true`, `handoff=true`, `ddl_capture=true`, `heartbeat=true`, and
`schema_introspection=true`.

The runtime now also provides an embeddable admin/introspection surface that includes
capabilities, readiness/liveness, buffer depth, and delivery counters.

```rust
let admin = runtime.admin_snapshot();
assert_eq!(admin.state, "running");

let json = runtime.admin_snapshot_json()?;
let prometheus = runtime.admin_metrics_prometheus();
```

`admin_snapshot_json()` is intended for control-plane APIs, and
`admin_metrics_prometheus()` emits Prometheus-friendly text for embedding in
lightweight health endpoints.

The runtime constructor enforces capability guards. For example, configuring `snapshot_tables` with a source that does not support snapshots is rejected at construction time.

---

## PostgreSQL Source Configuration

```rust
pub struct PostgresSourceConfig {
    /// PostgreSQL host (FQDN or IP)
    pub host: String,
    
    /// PostgreSQL port
    /// Default: 5432
    pub port: u16,
    
    /// PostgreSQL username (should have REPLICATION role)
    pub user: String,
    
    /// PostgreSQL password material.
    /// Use `SecretString::new`, `SecretString::from_env`,
    /// `SecretString::from_provider`, or `SecretString::from_callback`.
    pub password: SecretString,

    /// Database authentication mode.
    /// - `Password` (default): static password semantics
    /// - `AwsIamToken`: short-lived IAM token semantics (requires TLS transport)
    pub auth_mode: DatabaseAuthMode,
    
    /// Database name to replicate from
    pub database: String,
    
    /// Logical replication slot name
    /// Example: "rustcdc_slot"
    pub replication_slot_name: String,

    /// Publication name used by pgoutput
    /// Example: "rustcdc_publication"
    pub publication_name: String,
    
    /// Transport mode (`TransportConfig::tls()` by default when `tls` feature is enabled).
    pub transport: TransportConfig,
    
    /// Connection timeout in seconds
    /// Default: 30
    /// Range: 1 - 300
    pub conn_timeout_secs: u64,

    /// Stream poll interval in milliseconds
    /// Default: 50
    /// Range: 1 - 60000
    pub stream_poll_interval_ms: u64,

    /// Maximum events yielded per stream poll
    /// Default: 1000
    /// Range: 1 - 100000
    pub max_events_per_poll: usize,
    
}
```

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
let env_secret = SecretString::from_env("CDC_RS_POSTGRES_PASSWORD");
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
use std::collections::HashMap;

let mut encrypt_rules = HashMap::new();
encrypt_rules.insert(
  "profile.phone".to_string(),
  MaskRule::Encrypt(SecretString::from_env("CDC_RS_FIELD_KEY")),
);

let encrypt_transform = MaskHashTransform::new(MaskHashConfig {
  mask_rules: encrypt_rules,
  default_rule: MaskRule::Null,
});

let mut decrypt_rules = HashMap::new();
decrypt_rules.insert(
  "profile.phone".to_string(),
  MaskRule::Decrypt(SecretString::from_env("CDC_RS_FIELD_KEY")),
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

let transform = FieldMappingTransform::new(FieldMappingConfig {
  copy: vec![("user.email".into(), "email".into())],
  rename: vec![("user.name".into(), "user.full_name".into())],
  set_literals: vec![("meta.pipeline".into(), json!("orders-v2"))],
  remove: vec!["legacy_flag".into()],
  strict: true,
})?;
```

`strict = true` fails fast when copy/rename/remove source paths are missing,
which helps catch drift during schema evolution and replay.

**Replay determinism caveat (important):**
- `MaskRule::Encrypt` is intentionally nonce-based and therefore non-deterministic.
- Replaying the same logical event will produce different ciphertext bytes.
- Use encryption rules only when your downstream dedup/idempotency logic does not depend on byte-identical payload replay.
- For replay-sensitive pipelines, prefer deterministic masking rules (`Hash`, `Redact`, `Truncate`, `Null`) on fields that participate in replay comparisons.

**Transport Selection:**
- `TransportConfig::tls()` (default with `tls` feature): TLS with system trust store
- `TransportConfig::tls_with_ca_cert_path(path)`: TLS with explicit CA bundle
- `TransportConfig::plaintext()`: unencrypted transport — credentials and data transmitted in the clear

Use TLS transport for all production connector configurations.
`TransportConfig::plaintext()` is provided as an explicit escape hatch for trusted
private networks and local integration testing only — never use it in production.

**Connection Retry Policy:**

Set `RuntimeOptions.connection_retry` to automatically retry recoverable source
connection failures with truncated exponential backoff:

```rust
use rustcdc::core::ConnectionRetryPolicy;

let config = RuntimeConfig::new(source, checkpoint, schema_history)
    .with_connection_retry(ConnectionRetryPolicy {
        max_retries: Some(5),    // None retries indefinitely
        initial_delay_ms: 300,   // first retry after 300 ms
        max_delay_ms: 10_000,    // backoff capped at 10 s
    });
```

Only `SourceError` and `TimeoutError` trigger retry. Fatal errors (`ConfigError`,
`ValidationError`, etc.) propagate immediately regardless of this policy.

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

## MySQL Source Configuration

```rust
pub struct MysqlSourceConfig {
    /// MySQL host (FQDN or IP)
    pub host: String,
    
    /// MySQL port
    /// Default: 3306
    pub port: u16,
    
    /// MySQL username (should have REPLICATION CLIENT and SELECT privileges)
    pub user: String,
    
    /// MySQL password material as `SecretString`
    pub password: SecretString,

    /// Database authentication mode.
    /// - `Password` (default): static password semantics
    /// - `AwsIamToken`: short-lived IAM token semantics (requires TLS transport)
    pub auth_mode: DatabaseAuthMode,
    
    /// Database name to replicate from
    pub database: String,
    
    /// Replication server id used by binlog stream client
    /// Default: 1
    pub server_id: u32,

    /// Whether GTID mode is enabled in your deployment.
    /// Default: false
    pub gtid_mode_enabled: bool,

    /// Validate that source binlog format is ROW before streaming.
    /// Default: true
    pub binlog_format_check: bool,
    
    /// Transport mode (`TransportConfig::tls()` by default when `tls` feature is enabled).
    pub transport: TransportConfig,
    
    /// Connection timeout in seconds
    /// Default: 30
    /// Range: 1 - 300
    pub conn_timeout_secs: u64,

    /// Stream poll interval in milliseconds
    /// Default: 50
    /// Range: 1 - 60000
    pub stream_poll_interval_ms: u64,

    /// Maximum events yielded per stream poll
    /// Default: 1000
    /// Range: 1 - 100000
    pub max_events_per_poll: usize,
    
}
```

### MySQL GTID String Format

```
GTID Set Format: "source_id:interval[, ...]"
Example: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
```

---

## SQL Server Source Configuration

```rust
pub struct SqlServerSourceConfig {
    /// SQL Server host (FQDN or IP)
    pub host: String,
    
    /// SQL Server port
    /// Default: 1433
    pub port: u16,
    
    /// SQL Server username (should have CDC_ADMIN role)
    pub user: String,
    
    /// SQL Server password material as `SecretString`
    pub password: SecretString,
    
    /// Database name to replicate from (CDC must be enabled on database)
    pub database: String,
    
    /// Named instance (if using non-default instance)
    /// Example: Some("INSTANCE_NAME")
    /// Default: None (default instance)
    pub instance_name: Option<String>,
    
    /// Transport mode (`TransportConfig::tls()` by default when `tls` feature is enabled).
    pub transport: TransportConfig,
    
    /// Connection timeout in seconds
    /// Default: 30
    /// Range: 1 - 300
    pub conn_timeout_secs: u64,
    
    /// Require CDC to be enabled on database.
    /// Default: true
    pub cdc_enabled: bool,
    
    /// CDC schema name (usually "cdc")
    /// Default: "cdc"
    pub cdc_schema: String,

    /// Maximum concurrent SQL Server connections used by prereq checks
    /// Default: 4
    /// Range: 1 - 64
    pub prereq_pool_size: usize,

    /// Stream poll interval in milliseconds
    /// Default: 5000
    /// Range: 1 - 60000
    ///
    /// ⚠️ LATENCY NOTE: SQL Server CDC is polling-based, not event-driven.
    /// p99 latency ≈ stream_poll_interval_ms + CDC capture agent delay.
    /// Reduce this to 500–1000ms for latency-sensitive workloads.
    pub stream_poll_interval_ms: u64,

    /// Maximum events yielded per stream poll
    /// Default: 10000
    /// Range: 1 - 100000
    pub max_events_per_poll: usize,
    
}
```

### AWS IAM Auth Mode (MySQL/PostgreSQL)

For RDS-style IAM database auth, use connector `auth_mode = AwsIamToken` and
resolve the token through `SecretString::from_callback` (or provider) so each
new connection can fetch a fresh short-lived token.

TLS is mandatory when `auth_mode = AwsIamToken`.

### SQL Server Connection String Format

```
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
```
/var/rustcdc/checkpoints/checkpoint_postgres.json
/var/rustcdc/checkpoints/checkpoint_mysql.json
/var/rustcdc/checkpoints/checkpoint_sqlserver.json
```

**File Content Example:**
```json
{
  "checkpoint_format_version": 2,
  "source_type": "postgres",
  "committed_event_count": 12345,
  "offset": {
    "lsn": 281474976711680,
    "slot_name": "rustcdc_postgres_abc123"
  }
}
```

**Checkpoint Format Version Policy:**
- `checkpoint_format_version = 2` is the current write format.
- `checkpoint_format_version` is required for all file checkpoints.
- Unknown or missing versions are rejected at load time.
- rustcdc intentionally enforces fail-closed checkpoint decoding for format safety.

### Custom Durable Checkpoint Backend

**Use Case:** High-availability or centralized checkpoint management

rustcdc currently ships with `FileCheckpoint` and `InMemoryCheckpoint`.
For HA or centralized state, implement the `Checkpoint` trait against your
own storage backend (for example PostgreSQL, Redis, object storage, or a
platform metadata service).

---

## Observability Configuration

### NoOp Observability (Default)

```rust
use rustcdc::{RuntimeConfig, RuntimeObservability};

// Metrics and tracing are disabled by default via explicit runtime observability options.
let config = RuntimeConfig::new(...)
  .with_observability(RuntimeObservability::default());
```

### OpenTelemetry Observability

```rust
use rustcdc::{OTelConfig, OTelEventTracer, OTelMetricsCollector, RuntimeConfig, RuntimeObservability};
use std::sync::Arc;

let otel_config = OTelConfig::new(
    "http://otel-collector:4317",  // OTLP gRPC endpoint
    "rustcdc",                        // Service name
    "0.1.2",                         // Service version
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
| `cdc_runtime_readiness` | Gauge | Runtime readiness (1 ready, 0 not ready) |
| `cdc_runtime_liveness` | Gauge | Runtime liveness (1 alive, 0 stopped) |
| `cdc_runtime_buffer_depth` | Gauge | Buffered events waiting for delivery |
| `cdc_runtime_in_flight_events` | Gauge | Delivered but uncommitted events |
| `cdc_runtime_events_polled_total` | Counter | Total events delivered by runtime batches |
| `cdc_runtime_events_committed_total` | Counter | Total acknowledged and checkpointed events |
| `cdc_runtime_events_deduplicated_total` | Counter | Total events suppressed by idempotency guard |
| `cdc_runtime_checkpoint_age_ms` | Gauge | Age of last durable checkpoint |
| `cdc_runtime_replication_lag_ms` | Gauge | Estimated source lag in milliseconds |

### OpenTelemetry Exported Metrics (`OTelMetricsCollector`)

| Metric | Type | Description |
|--------|------|-------------|
| `cdc.events.processed` | Counter | Total events successfully processed |
| `cdc.events.filtered` | Counter | Events dropped by transform pipeline |
| `cdc.errors` | Counter | Total errors encountered |
| `cdc.checkpoint.committed_count` | Counter | Total events committed to checkpoint |
| `cdc.replication_lag_ms` | Gauge | Estimated replication lag in milliseconds |
| `cdc.replication_lag_events` | Gauge | Estimated events not yet consumed |
| `cdc.checkpoint_offset` | Gauge | Current checkpoint offset (source-specific encoding) |
| `cdc.buffer_size` | Gauge | Current buffered event count |
| `cdc.snapshot_progress` | Gauge | Current snapshot completion percentage |
| `cdc.event_processing_duration` | Histogram | Event processing latency (ms) |
| `cdc.checkpoint_commit_duration` | Histogram | Checkpoint commit latency (ms) |

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

```
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
let transport = TransportConfig::tls_with_ca_cert_path("/etc/ssl/certs/company-ca.pem");

// Also valid: rely on system trust store.
let transport = TransportConfig::tls();

// Plaintext: only for trusted private networks or local integration testing.
// Credentials and event data are transmitted unencrypted.
let transport = TransportConfig::plaintext();
```

Connector config helpers now provide explicit transport selection APIs:

```rust
let mysql_cfg = MysqlSourceConfig::default().with_plaintext_transport();
let pg_cfg = PostgresSourceConfig::default().with_plaintext_transport();
let mssql_cfg = SqlServerSourceConfig::default().with_plaintext_transport();

let mysql_tls = mysql_cfg.with_tls_transport();
```

### Monitoring Checklist

- [ ] Alert on `cdc_runtime_replication_lag_ms > 30000` (30s)
- [ ] Alert on `cdc_runtime_liveness == 0`
- [ ] Alert on `cdc_runtime_checkpoint_age_ms > 10000`
- [ ] Alert on `cdc_runtime_events_polled_total` trend deviation > 20%
- [ ] Dashboard: Replication lag trend over 24h
- [ ] Dashboard: Event processing rate (events/sec)
- [ ] Dashboard: Checkpoint commit latency distribution

---

**Last Updated:** May 25, 2026  
**Version:** Configuration Reference v0.1+
