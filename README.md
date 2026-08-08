# rustcdc

[![crates.io](https://img.shields.io/crates/v/rustcdc.svg)](https://crates.io/crates/rustcdc)
[![docs.rs](https://img.shields.io/docsrs/rustcdc)](https://docs.rs/rustcdc)
[![CI](https://github.com/hupe1980/rustcdc/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/rustcdc/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.94-blue)](https://blog.rust-lang.org/)
[![License](https://img.shields.io/crates/l/rustcdc.svg)](#license)

**Change data capture you embed, not deploy.** PostgreSQL, MySQL, MariaDB and SQL Server
behind one `Source` trait, one event envelope and one checkpoint model — as an ordinary Rust
crate that links into your binary and runs on your Tokio runtime.

📖 **[Documentation](https://hupe1980.github.io/rustcdc/docs/)** ·
🚀 **[Getting started](https://hupe1980.github.io/rustcdc/docs/getting-started/)** ·
🔧 **[API reference](https://docs.rs/rustcdc)**

## Why this exists

Embedding multi-database CDC today means embedding a JVM: Debezium's engine is a Java library,
and Materialize and RisingWave are platforms rather than crates. The Rust alternatives are
single-database primitives, and the most complete of them cannot be published to crates.io at
all because it depends on a fork of a fork of `rust-postgres`.

rustcdc writes its own pgoutput parser against stock `tokio-postgres`. That is why it ships as
a normal dependency with no sidecar to supervise and no control plane to operate.

The trade-off is stated up front: stock `tokio-postgres` exposes no replication-mode API, so
PostgreSQL decoding goes through `pg_logical_slot_peek_binary_changes` rather than
`START_REPLICATION`. Latency is bounded by the poll interval, and a long-running transaction that
pins the slot's `restart_lsn` makes each poll re-scan the gap to `confirmed_flush_lsn`. See
[known architectural limits](https://hupe1980.github.io/rustcdc/docs/library-parity-matrix/#known-architectural-limits)
before choosing it for a latency-critical PostgreSQL workload.

## Status

**Pre-1.0.** Latest published release is 0.7.0; 0.10.0 is in development and is a breaking
release — see [CHANGELOG.md](CHANGELOG.md). Core connector and runtime paths are implemented
and validated by 929 unit tests, 109 documentation samples compiled as doctests, and 54
integration suites, the container-backed ones running against real PostgreSQL 16,
MySQL 8.0/8.1, MariaDB 10.5/10.6, SQL Server 2022 and Apicurio Registry 3.

0.10.0 is a correctness release: an audit of each connector's **resume coordinate** found three
silent-data-loss defects — MySQL transaction compression recording an unusable binlog position,
an incremental snapshot cursor becoming durable before its rows were delivered, and a truncated
SQL Server LSN window dropping 55 of 60 rows across two capture instances — plus a false
data-loss alarm when a table was added to a running SQL Server pipeline. Each has a regression
test that fails without its fix, three of them against a live server.

The public API may still change. Delivery is **at-least-once**; see
[Delivery guarantees](#delivery-guarantees).

## Install

```toml
[dependencies]
rustcdc = { version = "0.10", features = ["postgres"] }
```

The default profile is `postgres` + `tls`. WASM transforms and every non-PostgreSQL connector
are opt-in — see [Feature flags](#feature-flags).

## Quick start

```rust
use rustcdc::{
    checkpoint::InMemoryCheckpoint, schema_history::InMemorySchemaHistory,
    PostgresSourceConfig, RuntimeConfig, RuntimeSourceConfig,
};

let source = PostgresSourceConfig {
    host: "localhost".into(),
    port: 5432,
    user: "postgres".into(),
    password: "postgres".into(),
    database: "app".into(),
    replication_slot_name: "rustcdc_slot".into(),
    publication_name: "rustcdc_publication".into(),
    ..PostgresSourceConfig::default()
};

let config = RuntimeConfig::new(
    RuntimeSourceConfig::Postgres(source),
    InMemoryCheckpoint::default(),   // use FileCheckpoint in production
    InMemorySchemaHistory::default(),
);
# let _ = config;
```

Then drive the runtime: consume a batch, apply it, and acknowledge. The full loop — including
why the acknowledgement is a separate step — is in the
**[getting started guide](https://hupe1980.github.io/rustcdc/docs/getting-started/)**.

## Read this before writing a sink

Not every event carries a complete row. Applying one as if it were complete writes `NULL` over
a column that never changed — the classic CDC corruption. Rather than asking you to remember
that, the API will not express the bad write:

```rust
use rustcdc::RowWrite;
# use rustcdc::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
# struct Sink;
# impl Sink {
#     fn replace(&self, _key: Option<serde_json::Value>, _row: &serde_json::Value) {}
#     fn update_only(&self, _key: serde_json::Value, _columns: &serde_json::Value) {}
#     fn delete(&self, _key: serde_json::Value) {}
#     fn truncate(&self) {}
# }
# fn log_unwritable(_reason: rustcdc::NoRowWrite) {}
# fn example(event: &Event, sink: &Sink) {
match event.row_write() {
    RowWrite::Replace { key, row } => sink.replace(key, row),  // complete row
    RowWrite::Merge { key, columns, .. } => sink.update_only(key, columns), // partial: SET only these
    RowWrite::Delete { key } => sink.delete(key),
    RowWrite::Truncate => sink.truncate(),
    RowWrite::None { reason } => log_unwritable(reason),       // DDL, or no addressable row
    _ => {}
}
# }
```

`Merge` hands you only the columns the source actually supplied, so there is no placeholder
left to write by accident. It arises from PostgreSQL unchanged-TOAST: a large value not
modified by an `UPDATE` is omitted from the WAL and is unrecoverable. `REPLICA IDENTITY FULL`
does **not** fix it — replica identity governs the before-image only.

The underlying fields (`unavailable_columns`, `before_unavailable_columns`,
`before_is_key_only`) are documented in the
[API guide](https://hupe1980.github.io/rustcdc/docs/api/#partial-payloads-read-this-before-writing-a-sink).

## Required source-database configuration

Some server settings cause **silent** corruption rather than an error, so `connect()` validates
them and fails loud. Check these before your first run:

- **MySQL / MariaDB:** `binlog_row_metadata=FULL` (⚠️ MySQL 8 defaults to `MINIMAL`, under which
  the binlog carries no column names or primary-key flags), `binlog_row_image=FULL`,
  `binlog_row_value_options=''`, `binlog_format=ROW`, and a unique non-zero `server_id`.
- **PostgreSQL:** the replication slot must exist. rustcdc will **not** create it automatically —
  a slot that disappeared mid-life is a data-loss event, and recreating it silently restarts
  capture at the current WAL position. Provision it out of band, or set
  `create_replication_slot_if_missing = true` for first-time setup.
- **SQL Server:** CDC enabled on the database and on each captured table. Adding a table later
  with `sys.sp_cdc_enable_table` is supported while the stream is running.

Settings that need **no** change, but whose behaviour is worth knowing:

- **MySQL `binlog_transaction_compression = ON`** is read transparently. Rows inside a compressed
  transaction share one resume coordinate — the payload event's end position — because the
  unpacked events carry none of their own.
- **`max_events_per_poll` on SQL Server** is per *capture instance*, so one LSN window can buffer
  more events than a single poll returns. The window is never advanced before it has been read in
  full.

Full matrix: [configuration reference](https://hupe1980.github.io/rustcdc/docs/config-reference/).

## Delivery guarantees

- The runtime delivery contract is **at-least-once**. There is no exactly-once claim anywhere
  in this crate.
- Duplicates are possible after crashes, restarts, and partial ack/commit windows.
- Ordering is preserved within committed ack prefixes.
- Deduplicate sink-side on a stable key — source + table + primary key + source offset — and
  validate that dedup in staging before production rollout.

By default a delivered batch may end mid-transaction, because batches are cut on
`max_buffer_size`, `max_event_bytes` and commit-barrier capacity, none of which know anything
about transactions. For a sink that must apply each source transaction atomically — a ledger,
a materialized view with cross-row invariants — set
`TransactionBoundaryPolicy::PreserveTransactions` and every delivered batch ends on a
transaction boundary — including when the rest of a transaction is still in flight from the
source, which for a streaming connector is the normal case. A transaction larger than
`max_buffer_size` is still delivered split, with a WARN naming the transaction id, because a
permanent silent stall would be worse.

## What's in the box

| | |
|---|---|
| **Connectors** | PostgreSQL (logical replication / pgoutput), MySQL and MariaDB (binlog, GTID), SQL Server (CDC capture tables) |
| **Snapshots** | DBLog-style resumable incremental snapshot, implemented once and shared by every connector — including yours; chunk cursors persist inside the checkpoint offset, so a restart resumes at the chunk boundary |
| **Checkpoints** | In-memory and file-backed; file writes are atomic, fsynced and SHA-256 checksummed, with a single-writer lease |
| **Transforms** | Masking, filtering, projection, field mapping, routing, unwrapping, outbox — plus a sandboxed WASM stage |
| **Codecs** | JSON, Avro, Protobuf; Confluent, Apicurio and AWS Glue schema registries |
| **Observability** | Prometheus text exposition, OpenTelemetry metrics and tracing, structured logs, `HealthVerdict` |
| **Testing** | Deterministic replay, fault injection, adapter conformance harness |

Three details worth knowing up front:

**Bring your own source.** `CdcRuntime::register_source` drives the runtime from any
`impl Source`, including one this crate does not ship. The commit barrier, checkpointing,
transforms, idempotency guard, health verdicts and metrics all apply unchanged — and
implementing `IncrementalSnapshotBackend` gets you non-blocking DBLog snapshots too, since the
watermark algorithm lives in one shared driver rather than once per connector. See
[custom sources](https://hupe1980.github.io/rustcdc/docs/api/#custom-sources).

**Transforms don't pay for async they don't use.** Every shipped transform is pure CPU work
over an in-memory event, so `Transform::apply` is a plain `fn`. A stage that genuinely must
await implements `AsyncTransform` and is registered with `add_async_transform`. The pipeline
runs a whole delivery through each stage in turn rather than each event through the whole
pipeline, so a stage can amortise per-batch setup — the WASM stage takes its instance lock
once per batch instead of once per event.

**Knowing whether it is actually running.** `RuntimeState` cannot tell you: a connector
streaming from a quiet database and one hung on a dead socket both report `Running`.
`runtime.admin_snapshot().health` returns a `HealthVerdict` — `Healthy`, `Idle`,
`Stalled { reason }` or `NotRunning` — where `reason` names both the condition and the remedy.
`HealthVerdict::is_alertable()` is true for exactly `Stalled`, and the same verdict is exported
as `rustcdc_runtime_health{verdict="stalled"} == 1`. See the
[runbook](https://hupe1980.github.io/rustcdc/docs/runbook/#health-verdict-idle-vs-stalled).

## Feature flags

| Flag | Enables |
|---|---|
| `postgres` *(default)* | PostgreSQL connector (pulls in `tls`) |
| `tls` *(default)* | TLS transport surface |
| `mysql` / `mariadb` | MySQL and MariaDB connectors (shared transport stack, distinct source identity) |
| `sqlserver` | SQL Server connector. **Brings a second, older TLS stack** — see the note below |
| `wasm` | WASM transform sandbox via wasmtime (~15 MB release overhead; opt-in by design) |
| `outbox` | Outbox pattern helpers and transforms |
| `encryption` | Encryption-oriented transforms and helpers |
| `metrics` | OpenTelemetry metrics and tracing |
| `schemreg` | Confluent Schema Registry — Avro, JSON Schema, Protobuf |
| `apicurio` | Apicurio Registry v3 native REST API |
| `glue` | AWS Glue Schema Registry — `GlueAvroEncoder`/`Decoder` (18-byte wire header, UUID schema identity) |
| `test-harnesses` | Replay, fault injection and conformance harnesses (dev/test only) |

`--no-default-features` builds the foundation without any connector; `--all-features` validates
the full additive surface.

TLS is the default for every connector and needs no feature flag for private-CA or mutual-TLS
deployments — configure `TransportConfig::tls_with_ca_cert_path(...)` or
`TransportConfig::mtls(...)` directly.

> **`sqlserver` negotiates TLS with a different stack than the rest of the crate.**
> Everything else in rustcdc — every other connector, every sink — is on `rustls 0.23`.
> `tiberius 0.12.3` hard-pins `tokio-rustls 0.24`, so enabling `sqlserver` adds a second,
> older copy: `rustls 0.21` / `rustls-webpki 0.101.7`, carrying RUSTSEC-2026-0098, -0099
> and -0104, plus the unmaintained `rustls-pemfile 1.0` via `rustls-native-certs 0.6`.
> It is not deduplicable and not fixable from here — the fix needs a tiberius release
> built against `rustls 0.23`. Two of the three advisories are unreachable on rustcdc's
> code paths and the third needs CA misissuance to exploit; the per-advisory reachability
> analysis, the `cargo deny` suppressions and the mitigations are in
> [security](https://hupe1980.github.io/rustcdc/docs/security/#known-exposure-sqlserver-feature).
> Deployments that cannot accept it should leave the feature off — it is not a default.

## Examples

```bash
# PostgreSQL → stdout
cargo run --example pg_to_stdout --features postgres -- \
  --host localhost --port 5432 --database testdb --snapshot-tables public.users

# MariaDB → stdout (same runtime loop, MariaDB source identity)
cargo run --example mariadb_to_stdout --features mariadb -- \
  --host localhost --port 3306 --database testdb --snapshot-tables app.users

# Full local stack: PostgreSQL + pg_to_stdout
docker compose up --build
docker compose down -v
```

The examples also read `CDC_RS_HOST`, `CDC_RS_PORT`, `CDC_RS_DB`, `CDC_RS_SNAPSHOT_TABLES` and
related variables, and commit every 100 events by default.

## How this is verified

**Every public item is documented.** `#![deny(missing_docs)]` is enforced at the crate root and
gated in CI. For a library whose public surface *is* the product, an undocumented `pub fn` on a
checkpoint or connector type is a reader guessing at a correctness contract.

**The documentation compiles.** Every Rust block in this README and under `site/content/docs/`
is compiled and run by `cargo test --doc --all-features`, gated in CI. Wiring the Markdown into
the doctest run immediately surfaced 36 broken samples out of 96, including wrong field names
and methods that had moved between types. Blocks that cannot run in a doctest — they need a
live database, or they document the shape of a type — are marked `ignore` with a one-line
reason; an unmarked block that fails to compile is a defect.

**Failure paths are exercised, not assumed.** Deterministic replay, fault injection and
process-kill crash tests are part of the suite, and are available to *your* tests too via the
`test-harnesses` feature. See
[reliability testing](https://hupe1980.github.io/rustcdc/docs/reliability-testing/).

**Suites run against the configurations that break things, not the defaults.** Every
silent-data-loss defect fixed in 0.10.0 was invisible under the existing suite's configuration —
compression off, one capture instance, a chunk that happened to drain in the same poll — and
appeared immediately under one a real deployment would choose. The resume-coordinate suites now
pin those options explicitly, and each was confirmed to fail with its fix reverted before being
committed.

## Development

```bash
cargo check --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
bash scripts/ci-policy-gate.sh
```

Foundation-only profile, without any connector:

```bash
cargo test --lib --no-default-features
```

Connector-backed evidence (requires a running Docker daemon):

```bash
bash scripts/run_full_integration_matrix_evidence.sh
bash scripts/ci-benchmark-gate.sh
```

Benchmarks are in-process microbenchmarks with no connector I/O — a regression signal for the
transform and codec paths, not a throughput claim. The evidence policy, including release-grade
classification, is documented under
[benchmark evidence](https://hupe1980.github.io/rustcdc/docs/reliability-testing/#benchmark-evidence).

The documentation site lives in [`site/`](site/) and is built with [Zola](https://www.getzola.org/):

```bash
zola --root site serve
```

## Documentation

| | |
|---|---|
| [Getting started](https://hupe1980.github.io/rustcdc/docs/getting-started/) | First pipeline, from an empty project to committed events |
| [Architecture](https://hupe1980.github.io/rustcdc/docs/architecture/) | Capture, commit barrier and checkpointing — how they fit and why |
| [API guide](https://hupe1980.github.io/rustcdc/docs/api/) | The embedding model: lifecycle, acknowledgement, transforms, codecs |
| [Configuration reference](https://hupe1980.github.io/rustcdc/docs/config-reference/) | Every option, with the failure it prevents |
| [Schema evolution](https://hupe1980.github.io/rustcdc/docs/schema-evolution/) | DDL handling, schema history, registry compatibility |
| [Adapter SDK](https://hupe1980.github.io/rustcdc/docs/adapter-sdk/) | Writing a connector the runtime treats as first-class |
| [WASM transform SDK](https://hupe1980.github.io/rustcdc/docs/wasm-transform-sdk/) | Sandboxed transforms, ABI and limits |
| [Deployment](https://hupe1980.github.io/rustcdc/docs/deployment/) | Running it in production |
| [Runbook](https://hupe1980.github.io/rustcdc/docs/runbook/) | Alert thresholds, recovery procedures, disaster recovery |
| [Troubleshooting](https://hupe1980.github.io/rustcdc/docs/troubleshooting/) | Symptom → diagnosis → resolution |
| [Security](https://hupe1980.github.io/rustcdc/docs/security/) | Transport defaults, secret handling, known exposure |
| [Reliability testing](https://hupe1980.github.io/rustcdc/docs/reliability-testing/) | Replay, fault injection, conformance |
| [Library parity matrix](https://hupe1980.github.io/rustcdc/docs/library-parity-matrix/) | Scope-aware comparison against alternatives |

## MSRV

Rust 1.94 or newer, matching the `rust-version` in `Cargo.toml`. Raising it is a
minor-version change. CI verifies it on exactly that toolchain.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.
