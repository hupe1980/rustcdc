<div align="center">

# 🔄 rustcdc-server

**Change Data Capture — built in Rust.**

Capture every row-level change from your databases and stream it anywhere,  
with configurable delivery semantics, a pluggable WASM transform pipeline, and a cryptographically signed audit trail.

[![CI](https://github.com/hupe1980/rustcdc-server/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/rustcdc-server/actions/workflows/ci.yml)
[![Docker](https://github.com/hupe1980/rustcdc-server/actions/workflows/publish.yml/badge.svg)](https://github.com/hupe1980/rustcdc-server/actions/workflows/publish.yml)
[![GHCR](https://img.shields.io/badge/ghcr.io-hupe1980%2Frustcdc--server-blue?logo=docker)](https://github.com/hupe1980/rustcdc-server/pkgs/container/rustcdc-server)
[![Rust 1.94+](https://img.shields.io/badge/rust-1.94%2B-orange?logo=rust)](https://www.rust-lang.org)
[![License: Apache 2.0 / MIT](https://img.shields.io/badge/license-Apache%202.0%20%2F%20MIT-green)](#-license)

</div>

---

## ✨ Why rustcdc-server?

| | |
|---|---|
| 🚀 **Zero-copy streaming** | Low-latency WAL/binlog tailing with back-pressure across all sources |
| 🔌 **Four sources, five sinks** | Postgres · MySQL · MariaDB · SQL Server → stdout · JSONL · HTTP · Kafka · Iceberg |
| 📐 **Nine wire formats** | JSON · CloudEvents 1.0 · Avro · Protobuf, plus Confluent framing (Avro / JSON Schema / Protobuf) against Confluent **or** Apicurio registries, and AWS Glue framing |
| 🌊 **Non-blocking backfill** | DBLog watermark incremental snapshots interleave with the live stream and resume mid-chunk after a restart — no held replication slot, no re-read from row zero |
| 🧩 **Transform pipeline** | Native rules — masking (redact / HMAC / AES-GCM), field mapping, transactional outbox, routing — plus sandboxed WASM modules in any language. A rule that never matches is a metric, not a silent no-op |
| 📦 **Pluggable state** | Checkpoint anywhere: local FS · Kafka topic · Redis · PostgreSQL |
| ☠️ **Sink-agnostic dead-letter queue** | Permanently undeliverable events are quarantined to a file or Kafka topic with their source offset and cause, so a poison record cannot crash-loop the pipeline. Opt-in, because advancing past an undelivered event is data loss and should be a decision |
| 🧮 **Failures classified on two axes** | Permanent and *this record's fault* (`MessageTooLarge`) is quarantined; permanent and *environmental* (a revoked ACL) halts the pipeline instead of draining the change stream into the DLQ one event at a time; transient is retried. Conflating the first two is how a dead-letter queue becomes the data loss it exists to prevent |
| 🎯 **End-to-end exactly-once** | `effectively_once` writes the checkpoint *inside* the sink's Kafka transaction, so the data and the position that describes it commit together — there is no crash window in which one survives without the other. Plus `at_least_once`, and an optional `preserve_transactions` boundary so a sink never commits half a source transaction |
| 🔭 **First-class observability** | Prometheus `/metrics` + OTLP traces & metrics (gRPC/HTTP), a one-hot runtime health verdict (`healthy · idle · stalled · not_running`) that distinguishes a quiet database from a dead socket, and a data-loss tripwire counter |
| 🧬 **Partial-image safety** | PostgreSQL unchanged-TOAST holes are tracked per image (`unavailable_columns` / `before_unavailable_columns`) and survive transforms, sinks, and the Iceberg schema — absent is never conflated with `NULL` |
| 🔒 **Security by default** | Kafka SASL (PLAIN · SCRAM · OAUTHBEARER with a built-in OIDC provider · AWS MSK IAM), mTLS with hot certificate reload, Ed25519-signed audit trail, token-manifest auth, per-IP rate limiting, IP pseudonymisation (GDPR) |
| 🐳 **Distroless multi-arch image** | `linux/amd64` + `linux/arm64`, SLSA provenance + SBOM, no shell inside |

---

## 📚 Documentation

**📖 [hupe1980.github.io/rustcdc-server](https://hupe1980.github.io/rustcdc-server)** — full documentation, searchable.

| Guide | Description |
|---|---|
| [🚀 Getting started](https://hupe1980.github.io/rustcdc-server/docs/getting-started/) | Up and running in 10 minutes |
| [💡 Core concepts](https://hupe1980.github.io/rustcdc-server/docs/concepts/) | Event model, delivery contracts, circuit breaker |
| [⚙️ Configuration reference](https://hupe1980.github.io/rustcdc-server/docs/configuration/) | Every TOML field, with examples |
| [🛠️ Operations guide](https://hupe1980.github.io/rustcdc-server/docs/operations/) | CLI, health checks, replay, K8s deployment |
| [🚨 Runbook](https://hupe1980.github.io/rustcdc-server/docs/runbook/) | Incident procedures, disaster recovery, upgrade and rollback |
| [🧩 WASM transforms](https://hupe1980.github.io/rustcdc-server/docs/transforms/) | Rust + AssemblyScript walkthroughs |
| [🔌 PostgreSQL connector](https://hupe1980.github.io/rustcdc-server/docs/connectors/postgres/) | WAL, replication slots, cloud databases |
| [🔌 MySQL / MariaDB connector](https://hupe1980.github.io/rustcdc-server/docs/connectors/mysql/) | Binlog, GTID, schema history |
| [🔌 SQL Server connector](https://hupe1980.github.io/rustcdc-server/docs/connectors/sqlserver/) | CDC change tables, Always On AG |

The site is built with [Zola](https://www.getzola.org) from `site/`; edit the Markdown
under `site/content/` and open a pull request.

---

## ⚡ Quick start

### Option A — Runnable demo (fastest)

A self-contained Docker Compose demo that streams PostgreSQL changes to your terminal in under two minutes — no config required:

```bash
git clone https://github.com/hupe1980/rustcdc-server
cd rustcdc-server/demo
docker compose up
```

See [demo/README.md](demo/README.md) for what to expect and how to explore the admin API.

### Option B — Docker with your own config

```bash
# Pull the latest multi-arch image
docker pull ghcr.io/hupe1980/rustcdc-server:latest

docker run --rm \
  -e POSTGRES_PASSWORD=mysecret \
  -v "$PWD/config.toml:/etc/rustcdc/config.toml:ro" \
  ghcr.io/hupe1980/rustcdc-server:latest
```

### Option C — Build from source

```bash
# Requires Rust 1.94+ and cmake / clang / perl (for aws-lc-sys)
git clone https://github.com/hupe1980/rustcdc-server
cd rustcdc-server
cargo build --release --all-features

export POSTGRES_PASSWORD="mysecret"
./target/release/rustcdc run --config-file config.toml
# (only the kafka_topic state backend needs a one-time `rustcdc init-state` first)
```

### Minimal `config.toml`

```toml
api_version = "v1"

[source.postgres]
host        = "localhost"
port        = 5432
user        = "cdc_user"
password    = { env = "POSTGRES_PASSWORD" }
database    = "mydb"
publication_name      = "cdc_pub"
replication_slot_name = "cdc_slot"
table_include_list = ["public.orders", "public.customers"]
table_exclude_list = []
create_replication_slot_if_missing = true   # quickstart only — provision out of band in production
conn_timeout_secs       = 10
stream_poll_interval_ms = 100
max_events_per_poll     = 1000

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "./state"
```

> 📖 See the [getting started guide](https://hupe1980.github.io/rustcdc-server/docs/getting-started/) for PostgreSQL setup steps, Docker Compose, and a production checklist.

---

## 🗺️ Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│                            rustcdc-server                            │
│                                                                      │
│   ┌──────────────┐                   ┌──────────────────────────┐    │
│   │  PostgreSQL  │                   │        CDC Source        │    │
│   │    MySQL     │ ─ WAL / binlog ─▶ │                          │    │
│   │   MariaDB    │                   │   snapshot → streaming   │    │
│   │  SQL Server  │                   └──────────────────────────┘    │
│   └──────────────┘                                                   │
│                                                    │                 │
│                                                    ▼                 │
│                                    ┌──────────────────────────────┐  │
│                                    │      Transform Pipeline      │  │
│                                    │                              │  │
│                                    │     native rules + WASM      │  │
│                                    └──────────────────────────────┘  │
│                                                    │                 │
│                                                    ▼                 │
│                                    ┌──────────────────────────────┐  │
│                                    │         Sink Router          │  │
│                                    │                              │  │
│                                    │   table glob → named sinks   │  │
│                                    └──────────────────────────────┘  │
│                                                    │                 │
│             ┌────────────┬────────────┬────────────┼                 │
│             ▼            ▼            ▼            ▼                 │
│        ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│        │  Kafka  │  │ Iceberg │  │   HTTP  │  │  JSONL  │            │
│        └─────────┘  └─────────┘  └─────────┘  └─────────┘            │
│                                                                      │
│    ┌────────────────────────────────────────────────────────────┐    │
│    │     State: local FS · Kafka topic · Redis · PostgreSQL     │    │
│    └────────────────────────────────────────────────────────────┘    │
│                                                                      │
│      ┌──────────────┐  ┌────────────────┐  ┌──────────────────────┐  │
│      │   /metrics   │  │  OTLP traces   │  │   Admin API + TLS    │  │
│      │  Prometheus  │  │  gRPC / HTTP   │  │  signed audit trail  │  │
│      └──────────────┘  └────────────────┘  └──────────────────────┘  │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 🔌 Sources

| Source | Mechanism | Snapshot | Schema changes |
|---|---|---|---|
| **PostgreSQL** | Logical replication (WAL) | blocking + incremental | pgoutput |
| **MySQL** | Binlog (row-based) | blocking + incremental | Schema history |
| **MariaDB** | Binlog (row-based) | blocking + incremental | Schema history |
| **SQL Server** | CDC change tables | blocking + incremental | Column tracking |

**Incremental snapshots** use the DBLog watermark algorithm: chunks interleave
with the live stream instead of gating it, so capture starts immediately and a
large table does not hold the replication slot open while it is read. Chunk
cursors live inside the connector checkpoint — the same atomic, fsynced,
checksummed write as the stream position — so a restart resumes mid-backfill
rather than re-reading from row zero.

```toml
[incremental_snapshot]
tables     = ["public.orders", "public.customers"]
chunk_size = 5000
```

---

## 📤 Sinks

| Sink | Delivery contracts | Notes |
|---|---|---|
| **stdout** | — | Development / debugging |
| **file_jsonl** | at_least_once | Rotatable, append-only |
| **HTTP** | at_least_once | Batched, configurable retry |
| **Apache Kafka** | at_least_once · effectively_once | Pure-Rust client (krafka), idempotent + transactional producers, pipelined sends with per-record delivery confirmation and per-partition ordering, PK-keyed partitioning with per-table fallback, SASL + mTLS + OIDC |
| **Apache Iceberg** | at_least_once | REST catalog, S3/GCS/ABS storage, zstd Parquet, periodic snapshot expiry |

---

## 📐 Codecs

Set per sink under `[sink.codec]`.

| `type` | Framing | Registry |
|---|---|---|
| `json` *(default)* | Raw JSON; compact-JSON primary key as the message key | — |
| `json_pretty` | Raw JSON, indented — for reading by eye | — |
| `avro` | Plain Avro binary | — |
| `protobuf` | Plain protobuf | — |
| `avro_confluent` | Confluent 5-byte header + Avro | ✅ |
| `json_schema_confluent` | Confluent 5-byte header + JSON, validated on encode | ✅ |
| `protobuf_confluent` | Confluent header + message-index path + protobuf | ✅ |
| `glue_avro` | AWS Glue 18-byte header + Avro | AWS Glue |
| `cloud_events` | CloudEvents 1.0 JSON envelope | — |

Registry-backed codecs speak the **Confluent Schema Registry** API (Confluent
Platform/Cloud, Karapace, Redpanda) or **Apicurio Registry v3**'s native API —
both emit Confluent framing, so consumers do not need to know which produced the
message. Define a registry once under `[registries.<name>]` and reference it with
`registry_ref`. With `auto_register = false` the encoder verifies that the
registered schema is the one it will write: Avro binary is positional and
untagged, so an id resolving to a different schema yields plausible-looking
wrong values rather than an error.

---

## 🎯 Delivery contracts

```toml
delivery_contract = "effectively_once"  # at_least_once | effectively_once
```

| Contract | Guarantee | Kafka | HTTP |
|---|---|---|---|
| `at_least_once` | Delivered ≥ 1×; checkpoint advances only after durable delivery | ✅ | ✅ |
| `effectively_once` | **Exactly-once, end to end.** The batch's records and its checkpoint are written in one Kafka transaction, so a crash discards both or keeps both — never one. Requires a transactional Kafka sink and `state.offset.backend = "kafka_topic"` on the same cluster; any other combination is rejected at load rather than silently degraded. [How it works](https://hupe1980.github.io/rustcdc-server/docs/concepts/#3-delivery-contracts). | ✅ | ❌ |

---

## 🧩 Transforms

Rules run in order, gated by a `when` predicate on table / schema / operation.
Native actions cover the common shapes without leaving the config file:

| Action | What it does |
|---|---|
| `mask` | Redact, truncate, SHA-256, keyed **HMAC-SHA256**, or **AES-256-GCM encrypt/decrypt** fields by dotted path (`emails.*` covers a whole array) |
| `field_mapping` | Copy, rename, set literals, remove — optionally `strict`, so a renamed-away column is an error instead of a silently missing field |
| `outbox` | Unwrap the transactional-outbox pattern: an insert carrying `aggregate_id` / `event_type` / `payload` becomes the domain event it represents, keyed per aggregate |
| `unwrap` / `flatten` | Lift a nested object into the row |
| `filter` / `route` | Drop events, or rewrite `schema` / `table` to fan out |
| `metadata_projection` / `key_shaping` | Materialise source metadata or a deterministic key into the payload |

```toml
[[pipeline.transforms]]
name = "redact_pii"
  [pipeline.transforms.when]
  tables = ["users"]

  [[pipeline.transforms.actions]]
  type = "mask"
    [pipeline.transforms.actions.rules]
    email = { type = "redact", placeholder = "***" }
    ssn   = { type = "hmac_sha256", key = { env = "PII_HMAC_KEY" } }
```

Mask rules match by **exact** path, so a typo or a renamed column disables one
silently and the field keeps flowing in clear text. Every rule carries a hit
counter and the ones that never fired are named in a WARN at shutdown. Masking
keys must be `{ env = "VAR" }` references — a literal in the config file is
rejected at load, because it makes every value it masked re-identifiable for as
long as that file exists.

### WASM modules

For anything the native actions do not cover, rustcdc embeds a **WebAssembly sandbox** directly in the transform pipeline. Write your logic in any language that compiles to `.wasm` — the runtime loads the module once at startup and calls it for every event across a pool of parallel instances.

**What you can do:**

| Pattern | Example |
|---|---|
| 🔏 Redact PII | Replace `email`, `ssn`, `card_number` with `***` |
| 🗑️ Drop events | Discard rows from `sessions` or `audit_log` tables |
| ➕ Enrich | Derive fields: `total = qty × unit_price` |
| 🔀 Re-route | Rewrite `schema`/`table` to fan out to different topics |
| 📦 Flatten JSONB | Expand a nested column into top-level fields |

**Supported languages:** Rust · AssemblyScript · TinyGo · C/C++ · any `wasm32-unknown-unknown` target

**Minimal Rust transform:**

```rust
#[no_mangle]
pub extern "C" fn transform(ptr: i32, len: i32) -> i64 {
    let mut event = read_json(ptr, len);
    // redact a field
    event["after"]["email"] = json!("***");
    // return 0 to drop, or packed (out_ptr << 32 | out_len) to emit
    write_json(event)
}
```

**Enable in `cdc.toml`:**

```toml
[pipeline.transform_runtime]
mode = "wasm"

  [pipeline.transform_runtime.wasm]
  module_path        = "/etc/rustcdc/redact_pii.wasm"
  timeout_ms         = 50
  max_memory_bytes   = 8388608   # 8 MiB
  instance_pool_size = 4         # parallel instances
```

Every module runs in a **deterministic sandbox** — no network, no filesystem, no side effects. A configurable fuel budget prevents runaway transforms from stalling the pipeline.

→ [WASM transforms guide](https://hupe1980.github.io/rustcdc-server/docs/transforms/)

---

## 🛠️ CLI

```
rustcdc --config-file <FILE> <COMMAND>

  init                 Generate a starter config file (dev/prod profile)
  run                  Start the CDC pipeline
  dry-run              Validate config and connectivity without writing
  init-state           Seed the kafka_topic state backend (run once before first start)
  migrate-state        Migrate state from one backend to another
  validate-config      Parse and validate the config file; exit 0 if valid
  inspect-checkpoint   Print the current checkpoint offset
  replay               Replay events from a saved JSONL file
  status               Query runtime status via the admin API
```

---

## 🐳 Docker

```bash
# Latest (multi-arch: linux/amd64 + linux/arm64)
docker pull ghcr.io/hupe1980/rustcdc-server:latest

# Pin to a specific version
docker pull ghcr.io/hupe1980/rustcdc-server:1.2.3
```

Images are built on distroless/cc (no shell, no package manager), signed with SLSA provenance, and include a software bill of materials (SBOM). [See the Dockerfile](Dockerfile) for build details.

---

## 🔭 Observability

| Signal | How |
|---|---|
| **Prometheus metrics** | `GET /metrics` on the admin port (requires the admin read token) |
| **OTLP traces** | `otlp_endpoint` in `[observability]` (gRPC or HTTP) |
| **OTLP metrics** | Same or separate `otlp_metrics_endpoint` |
| **Structured logs** | `log_format = "json"` → ships to any log aggregator |
| **SLO alert rules** | [monitoring/rustcdc_slo_alerts.yml](monitoring/rustcdc_slo_alerts.yml) — 26 rules, `promtool`-validated |
| **Kafka OAuth health** | `rustcdc_sink_kafka_oauth_token_fetch_failures_total` and `..._expiry_epoch_ms` — a failing OIDC refresh loop is otherwise indistinguishable from an unreachable broker |

---

## 🔒 Security

- **Ed25519-signed audit trail** — every admin action is signed; tamper detection is built in
- **Token-manifest auth** — rotate tokens without restarting
- **Per-IP rate limiting** — configurable RPS + burst on the admin API
- **IP pseudonymisation** — GDPR-compliant SHA-256 pseudonymisation of actor IPs (on by default)
- **Field-level masking** — keyed HMAC-SHA256 pseudonymisation and AES-256-GCM encryption bound to `table + path`, so a value relocated to another column fails authentication instead of decrypting as authentic
- **Kafka SASL** — PLAIN, SCRAM-SHA-256/512, OAUTHBEARER (with a built-in OIDC `client_credentials` provider that refreshes per connection), AWS MSK IAM — every mechanism composes with TLS, so `sasl_ssl` + SCRAM-SHA-512 (the default on Redpanda Cloud, Aiven and Strimzi) is a supported combination
- **mTLS with hot reload** — client certificates re-read on an interval (KIP-1288), so a rotated cert does not need a restart
- **Secrets are references, never literals** — registry credentials, SASL passwords and masking keys must be `{ env = "VAR" }`; a literal in the config file is rejected at load
- **TLS everywhere** — admin API and all outbound connections use rustls (no OpenSSL)
- **`#![deny(unsafe_code)]`** — enforced workspace-wide

---

## 🧪 Development

```bash
# Check everything (warnings-as-errors, all targets, all features)
RUSTFLAGS='-D warnings' cargo check --all-targets --all-features

# Run tests
cargo test --all-features

# Lint
cargo clippy --all-targets --all-features -- -D warnings

# Audit licenses and duplicate dependencies
cargo deny check

# Specific integration suites
cargo test --test config_roundtrip
cargo test --test sink_conformance
cargo test --test wasm_transform
```

---

## 📄 License

Licensed under either of

- [Apache License 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.
