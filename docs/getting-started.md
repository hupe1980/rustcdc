# Getting Started with rustcdc

> 🎥 **Want to see it in action first?** Run the [self-contained demo](../demo/README.md) — streams live PostgreSQL changes to your terminal in under two minutes, no config required:
> ```bash
> cd demo && docker compose up
> ```

**rustcdc** streams every row-level change (`INSERT`, `UPDATE`, `DELETE`) from
your database — PostgreSQL, MySQL, MariaDB, or SQL Server — to HTTP, Kafka,
Iceberg, or files. Ordering and delivery guarantees are built in.

This guide gets you to a working pipeline in under 10 minutes.

---

## Choose your install path

| Path | Best for |
|---|---|
| [Binary](#install-the-binary) | local dev, bare-metal, VMs |
| [Docker](#run-with-docker) | containers, Kubernetes, CI |

---

## Install the binary

### Build from source (recommended)

Requires Rust ≥ 1.93 (`rustup update stable`).

```bash
git clone https://github.com/hupe1980/rustcdc-server
cd rustcdc-server
cargo build --release --locked
sudo cp target/release/rustcdc /usr/local/bin/
rustcdc --version
```

### Quick smoke-test

```bash
rustcdc --help
```

---

## Run with Docker

The official image is multi-arch (`linux/amd64`, `linux/arm64`), runs as a
non-root user, and is based on `distroless/cc` — no shell, no package manager.

### Pull

```bash
docker pull ghcr.io/hupe1980/rustcdc-server:latest
```

### Verify

```bash
docker run --rm ghcr.io/hupe1980/rustcdc-server:latest --version
```

---

## Prepare PostgreSQL

rustcdc reads from the PostgreSQL logical replication stream. You need
`wal_level = logical` and a replication user.

```sql
-- 1. Enable logical replication (requires superuser; needs a server restart
--    if wal_level was not already "logical")
ALTER SYSTEM SET wal_level = logical;
SELECT pg_reload_conf();   -- confirm with: SHOW wal_level;

-- 2. Create a dedicated replication user
CREATE USER cdc_user WITH REPLICATION LOGIN PASSWORD 'changeme';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;

-- 3. Create a publication for the tables you want to capture
CREATE PUBLICATION cdc_pub FOR TABLE public.orders, public.customers;
```

> **Cloud databases:** RDS, Azure Database for PostgreSQL, and Cloud SQL all
> support logical replication — see the
> [PostgreSQL connector guide](connectors/postgres.md#cloud-databases).

---

## Create your config

Generate a starter file:

```bash
rustcdc init --output cdc.toml --profile dev
```

Then edit `cdc.toml` to match your environment. The minimal working config:

```toml
api_version = "v1"

[source.postgres]
host        = "localhost"
port        = 5432
user        = "cdc_user"
password    = { env = "POSTGRES_PASSWORD" }   # never hardcode secrets
database    = "mydb"
publication_name      = "cdc_pub"
replication_slot_name = "cdc_slot"
table_include_list = ["public.orders", "public.customers"]
table_exclude_list = []
conn_timeout_secs       = 10
stream_poll_interval_ms = 100
max_events_per_poll     = 1000

# Quickstart convenience: create the replication slot on first connect.
# In production, provision the slot out of band and set this to false —
# a slot that vanishes mid-life is a data-loss event, and recreating it
# silently would skip everything in between.
create_replication_slot_if_missing = true

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"   # swap for http, kafka, or iceberg when ready

[state]
dir = "./state"
```

Validate before running:

```bash
rustcdc validate-config --config-file cdc.toml
```

---

## First run

### Binary

```bash
export POSTGRES_PASSWORD="changeme"

# (Kafka-topic state backend only) seed the state topic first:
# rustcdc init-state --config-file cdc.toml

# Start streaming — the slot is created on first connect thanks to
# create_replication_slot_if_missing = true
rustcdc run --config-file cdc.toml
```

### Docker

```bash
docker run --rm \
  -e POSTGRES_PASSWORD="changeme" \
  -v "$PWD/cdc.toml:/etc/rustcdc/config.toml:ro" \
  -v "$PWD/state:/var/lib/rustcdc/state" \
  -p 8080:8080 \
  ghcr.io/hupe1980/rustcdc-server:latest \
  run --config-file /etc/rustcdc/config.toml
```

> **Tip:** add `--network host` (Linux) or configure `host.docker.internal` so
> the container can reach your local Postgres.

### Docker Compose (development)

```yaml
# compose.yml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: changeme
      POSTGRES_DB: mydb
    command: ["postgres", "-c", "wal_level=logical"]
    ports: ["5432:5432"]

  rustcdc:
    image: ghcr.io/hupe1980/rustcdc-server:latest
    depends_on: [postgres]
    environment:
      POSTGRES_PASSWORD: changeme
    volumes:
      - ./cdc.toml:/etc/rustcdc/config.toml:ro
      - rustcdc-state:/var/lib/rustcdc/state
    ports: ["8080:8080"]
    command: ["run", "--config-file", "/etc/rustcdc/config.toml"]

volumes:
  rustcdc-state:
```

```bash
docker compose up
```

---

## Verify it's working

Events stream to stdout as JSON lines — the full rustcdc event envelope
(abridged; real events also carry `source`, `ts`, `transaction`, and
`envelope_version`):

```json
{"before":null,"after":{"id":42,"amount":"99.99","status":"pending"},"op":"insert",
 "schema":"public","table":"orders","primary_key":["id"],"before_is_key_only":false}
```

Check the admin API (available on port 8080). With the dev profile, `/healthz`,
`/livez`, and `/readyz` need no token; `/status` and `/metrics` always require a
read token — configure `[admin] read_token_env` and export the variable first:

```bash
# Liveness / readiness (no token needed with the dev profile)
curl http://localhost:8080/livez
curl http://localhost:8080/readyz

# Pipeline status and Prometheus metrics (read token required)
export RUSTCDC_ADMIN_READ_TOKEN="my-dev-read-token"   # matches [admin] read_token_env
curl -H "Authorization: Bearer $RUSTCDC_ADMIN_READ_TOKEN" http://localhost:8080/status
curl -H "Authorization: Bearer $RUSTCDC_ADMIN_READ_TOKEN" http://localhost:8080/metrics
```

---

## Add a transform (optional)

rustcdc can modify, filter, or enrich every event before it reaches the sink.
Two modes are available:

**Native rules** — zero-code, declared in TOML. Good for filtering and simple
field operations:

```toml
# Drop audit-log events and stamp provenance metadata onto customer events
[[pipeline.transforms]]
name = "drop-audit-log"

  [[pipeline.transforms.actions]]
  type = "filter"
  include_tables = ["customers", "orders"]   # everything else is dropped

[[pipeline.transforms]]
name = "stamp-metadata"

  [pipeline.transforms.when]
  tables = ["customers"]
  ops    = ["insert", "update", "read"]

  [[pipeline.transforms.actions]]
  type         = "metadata_projection"
  target_field = "_meta"
  fields       = ["source_name", "offset", "table", "operation"]
```

Available native actions: `unwrap`, `flatten`, `filter`, `route`,
`metadata_projection`, `key_shaping`. For column redaction/masking, use a WASM
module (below).

**WASM module** — arbitrary logic compiled from Rust, AssemblyScript, TinyGo,
or any `wasm32` target. Use this for enrichment, complex routing, custom
pseudonymisation, and anything native rules cannot express:

```toml
[pipeline.transform_runtime]
mode = "wasm"

  [pipeline.transform_runtime.wasm]
  module_path        = "/etc/rustcdc/my_transform.wasm"
  timeout_ms         = 50
  max_memory_bytes   = 8388608
  instance_pool_size = 4
```

→ [Writing WASM transforms](transforms.md) for build steps, patterns, testing,
and Kubernetes deployment.

---

## Production checklist

Before going to production, work through these items:

- [ ] **Sink** — swap `stdout` for `kafka`, `http`, or `iceberg`
  ([sink configuration](configuration.md#sinks))
- [ ] **Delivery contract** — set `delivery_contract = "at_least_once"` (default)
  or `"effectively_once"` for Kafka
  ([delivery contracts](concepts.md#delivery-contracts))
- [ ] **State backend** — replace `local_fs` with `kafka_topic`, `redis`, or
  `postgresql` for HA ([state backends](configuration.md#4-state-backends-state))
- [ ] **Secrets** — all passwords via `{ env = "VAR" }`, never inline
- [ ] **Audit trail** — enable signing and IP pseudonymisation
  ([security hardening](configuration.md#security-hardening))
- [ ] **Health probes** — wire `/livez` and `/readyz` to your orchestrator
  ([admin API](configuration.md#admin-api))
- [ ] **SLO alerts** — deploy `monitoring/rustcdc_slo_alerts.yml` to Prometheus

---

## Next steps

| Guide | What it covers |
|---|---|
| [PostgreSQL connector](connectors/postgres.md) | Publications, slots, replica identity, RDS/Azure/GCP, HA failover |
| [MySQL / MariaDB connector](connectors/mysql.md) | `binlog_format`, `server_id`, grants, GTID |
| [SQL Server connector](connectors/sqlserver.md) | `sp_cdc_enable_db`, permissions, polling interval |
| [Core concepts](concepts.md) | Event model, pipeline lifecycle, delivery contracts, circuit breaker |
| [Configuration reference](configuration.md) | Every TOML field with defaults and examples |
| [Operations guide](operations.md) | Replay, migrate-state, graceful shutdown, performance tuning |
| [Writing WASM transforms](transforms.md) | Write custom transforms in Rust, AssemblyScript, or any WASM language |
