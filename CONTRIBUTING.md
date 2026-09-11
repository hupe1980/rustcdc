# Contributing to rustcdc

Thank you for your interest in contributing to rustcdc!

## What is in this repository

One Cargo workspace with two members that ship together:

| Member | What it is |
|---|---|
| **`.`** — `rustcdc` | The library. Published to crates.io; its rustdoc embeds `site/content/library/*.md`. |
| **`server/`** — `rustcdc-server` | The configured binary and container image. Not published to crates.io. |
| `xtask/` | Crash-worker binaries the process-crash integration suites spawn as separate processes (a test cannot `SIGKILL` itself). Not run directly. |
| `server/fuzz/` | libFuzzer targets. Excluded from the workspace: they need nightly and `cargo fuzz`. |

**Every command below runs from the repository root**, including the server's. `cargo build`
builds both members; `-p rustcdc` and `-p rustcdc-server` address one.

## Prerequisites

- Rust 1.94.1+, edition 2024 — must match `rust-version` in `[workspace.package]`. CI reads
  the number from the manifest rather than restating it.
- A C toolchain for the server's dependency graph (`aws-lc-sys`, `parquet`, `wasmtime`):
  `cmake`, `clang`, `perl`, `pkg-config`.
- Docker and Docker Compose, for the integration tests that manage their own containers.
- [Zola](https://www.getzola.org) **0.23.4** for the documentation site. Content files are
  not Tera-templated (`skip_content_templating`), so code samples containing `{{` are safe
  and shortcodes do not exist — see `site/README.md` before editing a page.

## Building

```bash
cargo build                              # both members
cargo build -p rustcdc --all-features    # the library alone
cargo build -p rustcdc-server --all-features
```

## Running tests

Unit tests, no database required:

```bash
cargo test -p rustcdc --lib --all-features
cargo test -p rustcdc-server --lib --all-features
```

Documentation samples — every Rust block in `README.md` and under `site/content/library/` is
compiled and run, not just the rustdoc examples:

```bash
cargo test -p rustcdc --doc --all-features
```

The container-backed suites start and stop their own containers, so a local run is the same
command CI runs:

```bash
RUSTCDC_INTEGRATION=1 cargo test -p rustcdc-server --all-features \
  --test integration_postgres -- --test-threads=1
```

The library's example stack and the server's demo stack are separate:

```bash
docker compose -f docker/compose.example.yml up --build   # library example
docker compose -f demo/compose.yml up --build             # the server itself
```

## Benchmarks

```bash
cargo bench -p rustcdc            # library
RUSTFLAGS='--cfg rustcdc_optimised_test_harnesses' cargo bench -p rustcdc-server
```

The server's benches reach `rustcdc/test-harnesses` through a dev-dependency, and that feature
refuses to compile without `debug_assertions` — a guard against shipping test harnesses in a
production binary. The `--cfg` is the documented, deliberately awkward escape hatch. It exists
so the alternative (turning `debug-assertions` on for the whole workspace's bench profile) does
not quietly degrade the *library's* throughput numbers, which are the ones that get quoted.

## Feature gates

| Feature | Description |
|---|---|
| `postgres` | PostgreSQL WAL connector |
| `mysql` / `mariadb` | MySQL and MariaDB binlog connectors |
| `sqlserver` | SQL Server CDC connector |
| `snowflake` | Snowflake `CHANGES` source (adds no dependencies) |
| `tls` | TLS transport for the connectors |
| `wasm` | WASM transform runtime |
| `metrics` | OpenTelemetry metrics and tracing |
| `schemreg` / `apicurio` / `glue` | Schema-registry backends |
| `cloudevents` / `avro` / `protobuf` | Wire formats |
| `encryption` / `outbox` | Transform helpers |
| `test-harnesses` | Fault injection and mock sources. **Never enable in production.** |

`server/` has its own narrower set — `postgres` (default), `mysql`, `sqlserver` — so a
PostgreSQL-only deployment does not link `tiberius` and its second TLS stack.

## Code style

- `cargo fmt --all` before every commit. CI runs `cargo fmt --all --check` and fails on any
  difference, including in `tests/` and `server/` — formatting is not checked by the compiler,
  so a tree that builds and passes every test can still fail this gate.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` must be clean.
- `unsafe_code` is `deny` (not `forbid`) at the workspace level, with an allowlist enforced by
  `server/tests/architecture.rs`. The two exceptions are the Win32 PID-liveness probe in
  `checkpoint::owner_lease` and `std::env::set_var` in the server's test harness. Adding a
  third needs a safety comment and an allowlist entry.
- Every public item in the library carries documentation; `#![deny(missing_docs)]` enforces it.
- Prefer `is_some_and` over `map_or(false, ...)`, and let-chains over nested `if let`.
- Public API changes belong in `site/content/library/` and `CHANGELOG.md`.

## Implementing a Transform

The `Transform` trait uses RPITIT — no `#[async_trait]` needed:

```rust
use rustcdc::transform::{BoxTransform, Transform};
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

## Security

- Never build SQL with string interpolation; use parameterised queries exclusively.
- Raw `WHERE` overrides (`snapshot_select_overrides`) must never be derived from untrusted
  input at runtime.
- `cargo deny check` before a PR that adds or updates a dependency. One policy covers the whole
  workspace, and it resolves the graph with `all-features = true` — without that, the optional
  subtrees (the wasmtime JIT, the crypto stack, every connector) are invisible to the advisory,
  licence and ban checks.

## Submitting a pull request

1. Fork the repository and create a feature branch.
2. `cargo fmt --all`
3. `cargo test -p rustcdc --lib --all-features && cargo test -p rustcdc-server --lib --all-features`
4. `cargo clippy --workspace --all-targets --all-features -- -D warnings`
5. `bash scripts/ci-policy-gate.sh` — it catches drift CI would otherwise reject: docs that no
   longer match config, a `tests/*.rs` no workflow runs, a workflow that lost a required job,
   a Cargo profile that turns on `debug-assertions`, and markdown links that are broken —
   including links that resolve on your machine but point at a gitignored path, which is broken
   for everyone else.
6. Open a PR with a clear description of the change and its motivation.

Steps 2, 4 and 5 are the local equivalents of CI's `quality` and `policy-gate` jobs; running
them is much faster than learning their result from a failed build.
