<div align="center">

# 🔄 rustcdc-server

**Change Data Capture — built in Rust.**

Capture every row-level change from your databases and stream it anywhere,  
with configurable delivery semantics, a pluggable WASM transform pipeline, and a cryptographically signed audit trail.

[![CI](https://github.com/hupe1980/rustcdc-server/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/rustcdc-server/actions/workflows/ci.yml)
[![Docker](https://github.com/hupe1980/rustcdc-server/actions/workflows/publish.yml/badge.svg)](https://github.com/hupe1980/rustcdc-server/actions/workflows/publish.yml)
[![GHCR](https://img.shields.io/badge/ghcr.io-hupe1980%2Frustcdc--server-blue?logo=docker)](https://github.com/hupe1980/rustcdc-server/pkgs/container/rustcdc-server)
[![Rust 1.93+](https://img.shields.io/badge/rust-1.93%2B-orange?logo=rust)](https://www.rust-lang.org)
[![License: Apache 2.0 / MIT](https://img.shields.io/badge/license-Apache%202.0%20%2F%20MIT-green)](#-license)

</div>

---

## ✨ Why rustcdc-server?

| | |
|---|---|
| 🚀 **Zero-copy streaming** | Low-latency WAL/binlog tailing with back-pressure across all sources |
| 🔌 **Four sources, five sinks** | Postgres · MySQL · MariaDB · SQL Server → stdout · JSONL · HTTP · Kafka · Iceberg |
| 🧩 **WASM transform pipeline** | Sandboxed, fuel-limited modules in any language. Drop, enrich, redact, or re-route events. |
| 📦 **Pluggable state** | Checkpoint anywhere: local FS · Kafka topic · Redis · PostgreSQL |
| 🎯 **Three delivery contracts** | `at_least_once` · `at_most_once` · `effectively_once` (Kafka transactional) |
| 🔭 **First-class observability** | Prometheus `/metrics` + OTLP traces & metrics (gRPC/HTTP), a one-hot runtime health verdict (`healthy · idle · stalled · not_running`) that distinguishes a quiet database from a dead socket, and a data-loss tripwire counter |
| 🧬 **Partial-image safety** | PostgreSQL unchanged-TOAST holes are tracked per image (`unavailable_columns` / `before_unavailable_columns`) and survive transforms, sinks, and the Iceberg schema — absent is never conflated with `NULL` |
| 🔒 **Security by default** | Ed25519-signed audit trail · token-manifest auth · per-IP rate limiting · IP pseudonymisation (GDPR) |
| 🐳 **Distroless multi-arch image** | `linux/amd64` + `linux/arm64`, SLSA provenance + SBOM, no shell inside |

---

## 📚 Documentation

| Guide | Description |
|---|---|
| [🚀 Getting started](docs/getting-started.md) | Up and running in 10 minutes |
| [💡 Core concepts](docs/concepts.md) | Event model, delivery contracts, circuit breaker |
| [⚙️ Configuration reference](docs/configuration.md) | Every TOML field, with examples |
| [🛠️ Operations guide](docs/operations.md) | CLI, health checks, replay, K8s deployment |
| [🧩 Writing WASM transforms](docs/transforms.md) | Rust + AssemblyScript walkthroughs |
| [🔌 PostgreSQL connector](docs/connectors/postgres.md) | WAL, replication slots, cloud databases |
| [🔌 MySQL / MariaDB connector](docs/connectors/mysql.md) | Binlog, GTID, schema history |
| [🔌 SQL Server connector](docs/connectors/sqlserver.md) | CDC change tables, Always On AG |

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
# Requires Rust 1.93+ and cmake / clang / perl (for aws-lc-sys)
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

> 📖 See the [getting started guide](docs/getting-started.md) for PostgreSQL setup steps, Docker Compose, and a production checklist.

---

## 🗺️ Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                          rustcdc-server                          │
│                                                                  │
│  ┌─────────────┐   WAL/binlog   ┌────────────────────────────┐  │
│  │  PostgreSQL │──────────────▶ │                            │  │
│  │  MySQL      │                │       CDC Source           │  │
│  │  MariaDB    │                │  (snapshot → streaming)    │  │
│  │  SQL Server │                └────────────┬───────────────┘  │
│  └─────────────┘                             │                  │
│                                              ▼                  │
│                              ┌───────────────────────────────┐  │
│                              │     Transform Pipeline        │  │
│                              │  native rules + WASM module   │  │
│                              └───────────────┬───────────────┘  │
│                                              │                  │
│                              ┌───────────────▼───────────────┐  │
│                              │         Sink Router           │  │
│                              │  (table-glob → named sinks)   │  │
│                              └──┬──────────┬──────────┬──────┘  │
│                                 │          │          │         │
│                     ┌───────────▼─┐ ┌──────▼───┐ ┌───▼──────┐  │
│                     │   Kafka     │ │ Iceberg  │ │  HTTP /  │  │
│                     │   Sink      │ │  Sink    │ │  JSONL   │  │
│                     └─────────────┘ └──────────┘ └──────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  State backend: local FS · Kafka topic · Redis · PG      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────┐  ┌──────────────────────┐  ┌───────────────────┐  │
│  │ /metrics │  │  OTLP traces/metrics │  │  Admin API + TLS  │  │
│  │(Prometheus│  │  (gRPC / HTTP)       │  │  audit trail      │  │
│  └──────────┘  └──────────────────────┘  └───────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🔌 Sources

| Source | Mechanism | Snapshot | Schema changes |
|---|---|---|---|
| **PostgreSQL** | Logical replication (WAL) | ✅ | pgoutput |
| **MySQL** | Binlog (row-based) | ✅ | Schema history |
| **MariaDB** | Binlog (row-based) | ✅ | Schema history |
| **SQL Server** | CDC change tables | ✅ | Column tracking |

---

## 📤 Sinks

| Sink | Delivery contracts | Notes |
|---|---|---|
| **stdout** | — | Development / debugging |
| **file_jsonl** | at_least_once | Rotatable, append-only |
| **HTTP** | at_least_once | Batched, configurable retry |
| **Apache Kafka** | at_least_once · effectively_once | Transactional producer |
| **Apache Iceberg** | at_least_once | REST catalog, S3/GCS/ABS storage |

---

## 🎯 Delivery contracts

```toml
delivery_contract = "effectively_once"  # at_least_once | at_most_once | effectively_once
```

| Contract | Guarantee | Kafka | HTTP |
|---|---|---|---|
| `at_least_once` | Delivered ≥ 1× | ✅ | ✅ |
| `at_most_once` | Delivered ≤ 1× | ✅ | ✅ |
| `effectively_once` | Exactly-once via transactional barrier | ✅ | ❌ |

---

## 🧩 WASM transforms

rustcdc embeds a **WebAssembly sandbox** directly in the transform pipeline. Write your logic in any language that compiles to `.wasm` — the runtime loads the module once at startup and calls it for every event across a pool of parallel instances.

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

→ [Writing WASM transforms guide](docs/transforms.md)

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
| **SLO alert rules** | [monitoring/rustcdc_slo_alerts.yml](monitoring/rustcdc_slo_alerts.yml) (Prometheus) |

---

## 🔒 Security

- **Ed25519-signed audit trail** — every admin action is signed; tamper detection is built in
- **Token-manifest auth** — rotate tokens without restarting
- **Per-IP rate limiting** — configurable RPS + burst on the admin API
- **IP pseudonymisation** — GDPR-compliant SHA-256 pseudonymisation of actor IPs (on by default)
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
