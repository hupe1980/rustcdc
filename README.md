# rustcdc

rustcdc is an embeddable CDC library for Rust with a correctness-first design.
The repository includes canonical event contracts, checkpoint safety primitives, schema history abstractions, an embedded runtime, and PostgreSQL/MySQL/MariaDB/SQL Server source connectors.

## Status 🚀

Active development. Core connector/runtime library paths are implemented and validated by unit and integration suites.

Current crate release: 0.1.4.

## MSRV 🛠️

This crate targets Rust 1.88 or newer, matching the `rust-version` declared in `Cargo.toml`.

## Build 📦

```bash
cargo build
cargo build --features postgres
```

Default profile enables `postgres` + `tls`.

## Feature Profiles ⚙️

- default profile: secure-by-default build with `postgres` + `tls`
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

Release-grade benchmark classification now requires commit-pinned metadata plus a named Criterion baseline:

```bash
BENCHMARK_STRICT=1 \
BENCHMARK_MAX_REGRESSION_PERCENT=5 \
BENCHMARK_BASELINE_COMMIT="$(git rev-parse HEAD)" \
BENCHMARK_BASELINE_ARTIFACT="BENCHMARK_REPORT.md" \
CRITERION_BASELINE="ci-baseline" \
bash scripts/ci-benchmark-gate.sh
```

Use the same `CRITERION_BASELINE=ci-baseline` value in CI so release evidence and local reports compare against the same named baseline.

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

PostgreSQL `TRUNCATE` statements are surfaced as `Operation::Truncate` events. `before` and `after` are both `None` for truncate events. Connectors that support truncate events advertise `ConnectorCapabilities::truncate`.

## Connection Retry 🔄

Configure `ConnectionRetryPolicy` for automatic reconnection on transient source failures:

```rust
use rustcdc::core::ConnectionRetryPolicy;

let config = config.with_connection_retry(ConnectionRetryPolicy {
    max_retries: Some(5),    // None = retry indefinitely
    initial_delay_ms: 300,
    max_delay_ms: 10_000,
});
```

Only recoverable errors (`SourceError`, `TimeoutError`) trigger retry. Fatal errors propagate immediately.

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

## Documentation Map 📚

### Operational Documentation

- [Getting Started Guide](docs/getting_started.md) - Setup and quick start
- [Configuration Reference](docs/config_reference.md) - Complete configuration options
- [Troubleshooting Guide](docs/troubleshooting.md) - Diagnosis and resolution procedures
- [Operations Runbook](docs/runbook.md) - Production procedures, disaster recovery, alerting
- [Documentation Index](docs/documentation.md) - Cross-referenced documentation map

### Developer Documentation

- [API Documentation](docs/api.md) - Rust SDK documentation
- [Adapter SDK](docs/adapter_sdk.md) - Building custom adapters
- [WASM Transform SDK](docs/wasm_transform_sdk.md) - WASM transform runtime

### Project Documentation

- Architecture: [docs/architecture.md](docs/architecture.md)
- Library parity matrix (scope-aware release gating): [docs/library_parity_matrix.md](docs/library_parity_matrix.md)
