# rustcdc

[![crates.io](https://img.shields.io/crates/v/rustcdc.svg)](https://crates.io/crates/rustcdc)
[![docs.rs](https://img.shields.io/docsrs/rustcdc)](https://docs.rs/rustcdc)
[![CI](https://github.com/hupe1980/rustcdc/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/rustcdc/actions/workflows/ci.yml)
[![Rust 1.94.1+](https://img.shields.io/badge/rust-1.94.1%2B-orange?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/crates/l/rustcdc.svg)](#license)

**Change data capture in Rust.** PostgreSQL, MySQL, MariaDB, SQL Server and Snowflake
behind one `Source` trait, one event envelope and one checkpoint model — with no JVM, no
sidecar and no control plane.

📖 **[Documentation](https://hupe1980.github.io/rustcdc/)** ·
🚀 **[Server quickstart](https://hupe1980.github.io/rustcdc/docs/getting-started/)** ·
🔧 **[Library guide](https://hupe1980.github.io/rustcdc/docs/embedding/)** ·
📦 **[API reference](https://docs.rs/rustcdc)**

## Two ways to run it

Both are built and tested from this repository, against each other, at one version.

| | | |
|---|---|---|
| **[`rustcdc`](crates/rustcdc/)** | A **library** you link into your own binary. You own the loop, the sink and the process. | [crates.io](https://crates.io/crates/rustcdc) · [guide](https://hupe1980.github.io/rustcdc/docs/) · [README](crates/rustcdc/README.md) |
| **[`rustcdc-server`](crates/rustcdc-server/)** | A configured **binary and container image**: TOML in, Kafka / Iceberg / Snowflake / HTTP / files out, with an admin API, Prometheus metrics and OTLP traces. | [ghcr.io](https://github.com/hupe1980/rustcdc/pkgs/container/rustcdc-server) · [docs](https://hupe1980.github.io/rustcdc/docs/) · [README](crates/rustcdc-server/README.md) |

If you want change data capture **running**, take the server. If you want it **inside
something you are building**, take the crate. The server is the crate plus configuration,
sinks, state backends and an operational surface — not a second implementation.

```bash
# The server, against a seeded PostgreSQL, streaming to your terminal
docker compose -f demo/compose.yml up --build

# The library, in your own project
cargo add rustcdc --features postgres
```

## Repository layout

| Path | What it is |
|---|---|
| [`crates/rustcdc/`](crates/rustcdc/) | The library. Published to crates.io |
| [`crates/rustcdc-server/`](crates/rustcdc-server/) | The server binary and container image. Not published to crates.io |
| [`crates/crash-workers/`](crates/crash-workers/) | Binaries the process-crash suites spawn as separate processes; a test cannot `SIGKILL` itself |
| [`crates/xtask/`](crates/xtask/) | Repository automation. `cargo xtask <task>` is the one entry point |
| [`site/`](site/) | The documentation site. `/docs/` is the server, `/library/` is the crate |
| [`demo/`](demo/) | A self-contained Compose stack running the server against a seeded database |
| [`docker/`](docker/) | The library's example stack and its Dockerfile |
| [`monitoring/`](monitoring/) | Prometheus SLO alert rules, checked against the metrics the server actually emits |
| [`scripts/`](scripts/) | CI gates, including `ci-policy-gate.sh` |

Every command runs from the repository root. `cargo build` builds both crates;
`-p rustcdc` and `-p rustcdc-server` address one.

## Delivery guarantees

- **`at_least_once`** — a crash can replay the events of one uncheckpointed batch.
  Duplicates are possible; loss is not. Sinks must be idempotent on a key you control.
- **`effectively_once`** — a batch's records and its checkpoint commit in one Kafka
  transaction, so a crash keeps both or neither. It is *not* end-to-end exactly-once in
  every configuration, and [the window is described in full](https://hupe1980.github.io/rustcdc/docs/concepts/#3-delivery-contracts)
  rather than glossed over.

Where a guarantee has a limit, the limit is written next to it.

## Status

**Pre-1.0.** The public API may still change; minor versions may contain breaking changes,
and each one is listed in [CHANGELOG.md](CHANGELOG.md) with the migration it requires.

Validated by 1 168 library and 446 server unit tests, 139 compiled documentation samples,
41 deterministic-replay golden fixtures and 42 integration suites against real PostgreSQL,
MySQL, MariaDB, SQL Server, Apicurio, Redpanda and Apache Kafka.

## Building it

Rust 1.94.1+, and a C toolchain for the server's dependency graph (`cmake`, `clang`,
`perl`, `pkg-config`). Every command runs from the repository root.

```bash
cargo build                                        # both crates
cargo test -p rustcdc --lib --all-features         # the library
cargo test -p rustcdc-server --lib --all-features  # the server
cargo xtask                                        # the repository's gates, listed
cargo xtask policy-gate                            # the one a pull request must pass
cargo xtask bench                                  # benchmarks (never plain `cargo bench`)
```

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.
