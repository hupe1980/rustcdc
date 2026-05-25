# Contributing to rustcdc

Thank you for your interest in contributing to rustcdc!

## Prerequisites

- Rust (MSRV 1.80, edition 2021)
- Docker and Docker Compose (for integration tests against live databases)

## Building

```bash
cargo build
cargo build --all-features
```

## Running Tests

Unit and integration tests for library code (no database required):

```bash
cargo test --lib
```

Run all tests including integration tests (requires running database containers — see `docker-compose.yml`):

```bash
docker compose up -d
cargo test
```

## Feature Gates

The crate uses feature flags to opt into connector and observability support:

| Feature       | Description                                |
|---------------|--------------------------------------------|
| `postgres`    | PostgreSQL WAL connector                   |
| `mysql`       | MySQL binlog connector                     |
| `sqlserver`   | SQL Server CDC connector                   |
| `tls`         | TLS support for all connectors             |
| `wasm`        | WASM transform runtime                     |
| `metrics`     | Prometheus metrics endpoint                |
| `testkit`     | Test utilities (for downstream crate tests)|

Pass `--all-features` to build and test with all connectors enabled.

## Implementing a Transform

The `Transform` trait uses RPITIT — no `#[async_trait]` needed:

```rust
use rustcdc::transform::{Transform, BoxTransform};
use rustcdc::core::Event;

struct MyTransform;

impl Transform for MyTransform {
    fn transform(&self, event: Event) -> impl Future<Output = Option<Event>> + Send + '_ {
        async move { Some(event) }
    }
}

// For dynamic dispatch, wrap with BoxTransform:
let t: BoxTransform = BoxTransform::new(MyTransform);
```

## Code Style

- `cargo clippy --all-features` must produce zero warnings before submitting a PR.
- `#[forbid(unsafe_code)]` is set at the crate root — no unsafe code.
- Prefer `is_some_and` over `map_or(false, ...)`.
- All public API changes should be reflected in `docs/` and `CHANGELOG.md`.

## Security

- Do not use string interpolation to build SQL queries; use parameterised queries exclusively.
- Raw WHERE clause overrides (e.g., `snapshot_select_overrides`) must never be derived from untrusted user input at runtime.
- Run `cargo audit` before submitting a PR that adds or updates dependencies.

## Submitting a Pull Request

1. Fork the repository and create a feature branch.
2. Run `cargo test --lib` and ensure all tests pass.
3. Run `cargo clippy --all-features` and fix any warnings.
4. Open a PR with a clear description of the change and its motivation.
