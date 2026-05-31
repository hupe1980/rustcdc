# Getting Started

## Prerequisites

- Rust 1.88 or newer
- Cargo
- Docker if you want to run connector-backed integration tests with testcontainers

## Development Setup

```bash
cargo check --no-default-features
cargo test --lib
```

## Feature Profiles

- `--no-default-features`: foundation-only validation without source connectors
- default profile: secure-by-default build with `postgres` + `tls`
- relational connector features (`postgres`, `mysql`, `mariadb`, `sqlserver`) require and enable `tls` transitively
- `mariadb`: MariaDB profile with first-class MariaDB source identity on the MySQL transport stack
- `outbox`: enables outbox helpers and transforms
- `encryption`: enables encryption-oriented transforms and helpers
- `metrics`: enables OpenTelemetry metrics/tracing integrations
- `--all-features`: validates the full additive feature surface

For self-signed or private-CA deployments, use `TransportConfig::tls_with_ca_cert_path(...)` or `TransportConfig::mtls(...)`. Those production-safe TLS paths do not require a special Cargo feature.

For local testing or tightly controlled air-gapped environments where CA distribution is not practical, an explicit insecure opt-in is available: `TransportConfig::tls_insecure_skip_verify()`. This disables certificate and hostname verification and should not be used in production.

## Useful Commands

```bash
cargo build --no-default-features
cargo build
cargo build --all-features
cargo test --lib
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```

## Current Scope

- Canonical event envelope and validation
- Error and observability abstractions
- Checkpoint trait and commit barrier
- In-memory schema history and validator
- Embedded runtime with batch/ack delivery model
- PostgreSQL, MySQL, MariaDB, and SQL Server source connectors
- Fixture replay and conformance harness foundations

## Known Limits

- Integration-heavy connector suites require Docker/testcontainers execution.
- Full integration matrix evidence may be expensive to rerun locally in constrained environments.
