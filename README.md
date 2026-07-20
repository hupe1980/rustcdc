# rustcdc

rustcdc is an embeddable CDC library for Rust with a correctness-first design.
The repository includes canonical event contracts, checkpoint safety primitives, schema history abstractions, an embedded runtime, and PostgreSQL/MySQL/MariaDB/SQL Server source connectors.

## Status 🚀

**Pre-1.0.** Core connector and runtime paths are implemented and validated by 765 unit tests
plus 16 integration suites running against real PostgreSQL 16, MySQL 8.0/8.1, MariaDB 10.5/10.6
and SQL Server 2022 containers.

Current crate release: 0.7.0.

### Handling partial payloads

Not every event carries a complete row. Applying one as if it were complete writes `NULL`
over a column that never changed — the classic CDC corruption. Rather than asking you to
remember that, the API will not express the bad write:

```rust
use rustcdc::RowWrite;

match event.row_write() {
    RowWrite::Replace { key, row } => sink.replace(key, row),  // complete row
    RowWrite::Merge { key, columns, .. } => sink.update_only(key, columns), // partial: SET only these
    RowWrite::Delete { key } => sink.delete(key),
    RowWrite::Truncate => sink.truncate(),
    RowWrite::None { reason } => log_unwritable(reason),       // DDL, or no addressable row
    _ => {}
}
```

`Merge` hands you only the columns the source actually supplied, so there is no placeholder
left to write by accident. It arises from PostgreSQL unchanged-TOAST: a large value not
modified by an `UPDATE` is omitted from the WAL and is unrecoverable. `REPLICA IDENTITY FULL`
does **not** fix it — replica identity governs the before-image only.

See [docs/api.md](docs/api.md#partial-payloads--read-this-before-writing-a-sink) for the
underlying fields (`unavailable_columns`, `before_unavailable_columns`, `before_is_key_only`).

### Required source-database configuration

Some server settings cause **silent** corruption rather than an error, so `connect()` validates
them and fails loud. Check these before your first run:

- **MySQL:** `binlog_row_metadata=FULL` (⚠️ MySQL 8 defaults to `MINIMAL`, under which the binlog
  carries no column names or primary-key flags), `binlog_row_image=FULL`,
  `binlog_row_value_options=''`, `binlog_format=ROW`, and a unique non-zero `server_id`.
  See [docs/config_reference.md](docs/config_reference.md#mysql-source-configuration).
- **PostgreSQL:** the replication slot must exist. rustcdc will **not** create it automatically —
  a slot that disappeared mid-life is a data-loss event, and recreating it silently restarts
  capture at the current WAL position. Provision it out of band, or set
  `create_replication_slot_if_missing = true` for first-time setup.

### Knowing whether it is actually running

`RuntimeState` cannot tell you: a connector streaming from a quiet database and one hung on a dead
socket both report `Running`. `runtime.admin_snapshot().health` gives a `HealthVerdict` —
`Healthy`, `Idle`, `Stalled { reason }` or `NotRunning` — where `reason` names both the condition
and the remedy. `HealthVerdict::is_alertable()` is true for exactly `Stalled`, and the same verdict
is on the Prometheus surface as `rustcdc_runtime_health{verdict="stalled"} == 1`.

See [docs/runbook.md](docs/runbook.md#health-verdict--idle-vs-stalled).

## MSRV 🛠️

This crate targets Rust 1.92 or newer, matching the `rust-version` declared in `Cargo.toml`.

## Build 📦

```bash
cargo build
cargo build --features postgres
```

Default profile enables `postgres` + `tls`. WASM transforms are **opt-in** (`--features wasm`).

## Feature Profiles ⚙️

- default profile: `postgres` + `tls` (lean, no JIT runtime in the default binary)
- `--features wasm`: WASM transform sandbox via wasmtime (~15 MB release binary overhead; opt-in by design)
- `--features postgres`: PostgreSQL connector profile (TLS transport is required and enabled transitively)
- `--features mysql`: MySQL connector profile (TLS transport is required and enabled transitively)
- `--features mariadb`: MariaDB connector profile (reuses the MySQL transport stack with MariaDB source identity)
- `--features sqlserver`: SQL Server connector profile (TLS transport is required and enabled transitively)
- `--features tls`: explicit TLS transport surface (already included by relational connector features)
- `--features outbox`: enables outbox helpers and transforms
- `--features encryption`: enables encryption-oriented transforms and helpers
- `--features metrics`: enables OpenTelemetry metrics/tracing integrations
- `--no-default-features`: foundation-only validation without source connectors
- `--all-features`: validates the full additive feature surface

For self-signed or private-CA deployments, configure TLS directly with `TransportConfig::tls_with_ca_cert_path(...)` or `TransportConfig::mtls(...)`. No Cargo feature is required for those production-safe paths.

## License

Licensed under either of:
- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

Run local quality checks:

```bash
cargo check --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
bash scripts/ci-policy-gate.sh
```

Run full connector-backed evidence locally (requires Docker daemon):

```bash
bash scripts/ci-benchmark-gate.sh
bash scripts/run_full_integration_matrix_evidence.sh
```

To validate the foundation profile without source-specific features:

```bash
cargo test --lib --no-default-features
```

## Benchmark Evidence Policy

Benchmark evidence is produced via `scripts/ci-benchmark-gate.sh`.
Local runs are allowed, but are classified as non-release evidence unless strict release-policy inputs are satisfied.

Example local run (non-release classification expected):

```bash
bash scripts/ci-benchmark-gate.sh
```

Release-grade benchmark classification requires commit-pinned metadata plus a named Criterion baseline:

```bash
BENCHMARK_STRICT=1 \
BENCHMARK_MAX_REGRESSION_PERCENT=5 \
BENCHMARK_BASELINE_COMMIT="$(git rev-parse HEAD)" \
BENCHMARK_BASELINE_ARTIFACT="commit:$(git rev-parse HEAD)" \
CRITERION_BASELINE="ci-baseline" \
bash scripts/ci-benchmark-gate.sh
```

Use the same `CRITERION_BASELINE=ci-baseline` value in CI so release evidence and local reports compare against the same named baseline.

> **⚠️ Benchmarks measure in-process work only.** Both bench targets are microbenchmarks with no
> connector I/O — they do not measure end-to-end CDC throughput or latency. Treat a local run as
> a regression signal for the transform/codec paths, not as a performance claim.
>
> Regenerate `BENCHMARK_REPORT.md` on a clean tree before citing any number from it, and pin
> `BENCHMARK_BASELINE_COMMIT` to a known-good SHA so comparisons are like-for-like.

## Quick Start ✅

```rust
use rustcdc::{checkpoint::InMemoryCheckpoint, schema_history::InMemorySchemaHistory, RuntimeConfig, RuntimeSourceConfig};

let checkpoint = InMemoryCheckpoint::default();
let schema_history = InMemorySchemaHistory::default();
let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);

let _config = config;
```

## Delivery Guarantees 🔁

- Runtime delivery contract is at-least-once.
- Duplicate event delivery is possible after crashes, restart boundaries, and partial ack/commit windows.
- Ordering is preserved within committed ack prefixes, but consumers must still tolerate duplicates.
- Downstream systems should apply idempotency using stable keys (for example: source + table + primary key + source offset/transaction metadata).

Operational expectation:
- Treat rustcdc as correctness-first at-least-once transport, not exactly-once.
- Validate sink-side deduplication in staging before production rollout.

## Runtime Transform Error Policy 🧯

`RuntimeConfig` defaults to halting on transform failures via `TransformErrorPolicy::Halt`.
For best-effort pipelines, switch to `TransformErrorPolicy::Skip`:

```rust
use rustcdc::{
	checkpoint::InMemoryCheckpoint,
	schema_history::InMemorySchemaHistory,
	PostgresSourceConfig,
	RuntimeConfig,
	RuntimeSourceConfig,
	TransformErrorPolicy,
};
let checkpoint = InMemoryCheckpoint::default();
let schema_history = InMemorySchemaHistory::default();
let source = PostgresSourceConfig {
	host: "localhost".into(),
	port: 5432,
	user: "postgres".into(),
	password: "postgres".into(),
	database: "app".into(),
	replication_slot_name: "rustcdc_slot".into(),
	publication_name: "rustcdc_publication".into(),
	conn_timeout_secs: 30,
	..PostgresSourceConfig::default()
};

let config = RuntimeConfig::new(RuntimeSourceConfig::Postgres(source), checkpoint, schema_history)
	.with_transform_error_policy(TransformErrorPolicy::Skip);
```

`Halt` is the safe default because it preserves strict failure visibility.

## Post-Commit Confirmation Policy

`RuntimeConfig` now defaults to `PostCommitSourceConfirmPolicy::FailFast`.
If source confirmation fails after durable checkpoint commit, runtime returns an error by default to surface confirmation divergence immediately.

For availability-biased pipelines, opt into continue behavior explicitly:

```rust
use rustcdc::PostCommitSourceConfirmPolicy;

let config = config.with_post_commit_source_confirm_policy(
	PostCommitSourceConfirmPolicy::Continue,
);
```

## TRUNCATE Event Support

`TRUNCATE` statements are surfaced as `Operation::Truncate` events on PostgreSQL, MySQL and
MariaDB, and on SQL Server when `capture_truncate_events` is enabled. `before` and `after` are both
`None` for truncate events. Connectors that support them advertise `ConnectorCapabilities::truncate`.

## Connection Retry 🔄

Automatic reconnection on transient source failures is **enabled by default**
(`RuntimeOptions::connection_retry` defaults to `Some(ConnectionRetryPolicy::default())`). To tune it:

```rust
use rustcdc::{ConnectionRetryPolicy, RuntimeOptions};

// ConnectionRetryPolicy is #[non_exhaustive], so use the builder rather than a
// struct literal — struct-literal syntax is not available outside the crate.
let options = RuntimeOptions::new().with_connection_retry(Some(
    ConnectionRetryPolicy::new()
        .with_max_retries(Some(5)) // None = retry indefinitely
        .with_initial_delay_ms(300)
        .with_max_delay_ms(10_000),
));
```

Only recoverable errors (`SourceError`, `TimeoutError`) trigger retry. Fatal errors propagate immediately.

> **Note:** `with_connection_retry` lives on `RuntimeOptions`, not on `RuntimeConfig`. Pass the
> options via `RuntimeConfig::with_options(...)`.

## Transport Configuration 🔒

All connectors default to TLS. For trusted private networks or local testing only, use the explicit plaintext escape hatch:

```rust
use rustcdc::TransportConfig;

let transport = TransportConfig::plaintext(); // ⚠️ never use in production
```

## PostgreSQL Example 🐘

Build and run the PostgreSQL example:

```bash
cargo build --example pg_to_stdout --features postgres
./target/debug/examples/pg_to_stdout --host localhost --port 5432 --database testdb --snapshot-tables public.users
```

The example also accepts environment variables (`CDC_RS_HOST`, `CDC_RS_PORT`, `CDC_RS_DB`, `CDC_RS_SNAPSHOT_TABLES`, and related settings) and commits every 100 events by default.

## MariaDB Example 🐬

Build and run the MariaDB example:

```bash
cargo build --example mariadb_to_stdout --features mariadb
./target/debug/examples/mariadb_to_stdout --host localhost --port 3306 --database testdb --snapshot-tables public.users
```

The MariaDB example uses the same runtime loop as the PostgreSQL example, but it starts from `MariaDbSourceConfig` and a MariaDB-specific source identity.

## Docker Compose Example 🐳

Bring up the local PostgreSQL + `pg_to_stdout` demo stack:

```bash
docker compose up --build
```

The compose setup initializes `public.users` and publication `rustcdc_example_pub` automatically.

Stop and clean up:

```bash
docker compose down -v
```

## Disaster Recovery: seeding a checkpoint 🩹

Checkpoint files carry a SHA-256 integrity checksum that is verified on every load, so they
cannot be written by hand. Silent checkpoint corruption is otherwise unrecoverable — a
flipped bit in an LSN still parses, and capture resumes from a *wrong* position with no
error raised anywhere.

To seed one during recovery (connector stopped):

```bash
cargo run --example seed_checkpoint --features postgres -- \
  --dir /var/rustcdc/checkpoints \
  --source-type postgres \
  --committed-event-count 0 \
  --offset '{"lsn": 281474976711680, "slot_name": "rustcdc_postgres_new"}'
```

Programmatically this is `FileCheckpoint::restore_from_record`. Seeding a position *ahead*
of what was actually delivered skips everything in between, permanently — when in doubt,
seed behind and rely on at-least-once tolerance downstream.

See [docs/runbook.md](docs/runbook.md) for the full procedure.

## Documentation Map 📚

### Operational Documentation

- [Getting Started Guide](docs/getting_started.md) - Setup and quick start
- [Configuration Reference](docs/config_reference.md) - Complete configuration options
- [Troubleshooting Guide](docs/troubleshooting.md) - Diagnosis and resolution procedures
- [Operations Runbook](docs/runbook.md) - Production procedures, disaster recovery, alerting
- [Security Posture](docs/security.md) - Transport defaults, dependency policy, known exposure
- [Documentation Index](docs/documentation.md) - Cross-referenced documentation map

### Developer Documentation

- [API Documentation](docs/api.md) - Rust SDK documentation
- [Adapter SDK](docs/adapter_sdk.md) - Building custom adapters
- [WASM Transform SDK](docs/wasm_transform_sdk.md) - WASM transform runtime

### Project Documentation

- Architecture: [docs/architecture.md](docs/architecture.md)
- Library parity matrix (scope-aware release gating): [docs/library_parity_matrix.md](docs/library_parity_matrix.md)
