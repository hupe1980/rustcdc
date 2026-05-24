# Getting Started

## Prerequisites

- Rust 1.80 or newer
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
- relational connector features (`postgres`, `mysql`, `sqlserver`) require and enable `tls` transitively
- `outbox`: enables outbox helpers and transforms
- `encryption`: enables encryption-oriented transforms and helpers
- `metrics`: enables OpenTelemetry metrics/tracing integrations
- `insecure-test-overrides`: test-only insecure connector toggle support; do not enable in production
- `--all-features`: validates the full additive feature surface

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
- PostgreSQL, MySQL, and SQL Server source connectors
- Fixture replay and conformance harness foundations

## Known Limits

- Integration-heavy connector suites require Docker/testcontainers execution.
- Full integration matrix evidence may be expensive to rerun locally in constrained environments.
