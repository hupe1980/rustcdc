# Configuration reference

Every rustcdc setting is documented here. All configuration lives in a single
TOML file (default path: `cdc.toml`). Secrets are **never** stored inline — use
`{ env = "VAR" }` for any sensitive value.

> **Tip:** run `rustcdc validate-config --config-file cdc.toml --print-json` to see
> the fully parsed and redacted config as JSON after applying environment
> variables and defaults.

---

## Table of contents

1. [Top-level fields](#1-top-level-fields)
2. [Source (`[source]`)](#2-source-source)
3. [Sink (`[sink]` / `[[sinks]]`)](#3-sink-sink--sinks)
4. [State backends (`[state]`)](#4-state-backends-state)
5. [Pipeline (`[pipeline]`)](#5-pipeline-pipeline)
6. [Runtime (`[runtime]`)](#6-runtime-runtime)
7. [Admin API (`[admin]`)](#7-admin-api-admin)
8. [Observability (`[observability]`)](#8-observability-observability)
9. [Environment variables](#9-environment-variables)

---

## 1. Top-level fields

```toml
api_version       = "v1"              # required; must be "v1"
delivery_contract = "at_least_once"   # at_least_once | effectively_once | at_most_once
```

| Field | Required | Default | Description |
|---|---|---|---|
| `api_version` | yes | — | Schema version. Must be `"v1"`. |
| `delivery_contract` | no | `"at_least_once"` | When the checkpoint advances relative to delivery. See [delivery contracts](concepts.md#3-delivery-contracts). |

---

## 2. Source (`[source]`)

The `[source]` section is **required**. Exactly one source type is supported per
pipeline.

### PostgreSQL

```toml
[source]
require_primary = true               # reject startup if connected to a replica

[source.postgres]
host        = "localhost"
port        = 5432
user        = "cdc_user"
password    = { env = "POSTGRES_PASSWORD" }
database    = "mydb"
conn_timeout_secs = 10
publication_name      = "cdc_pub"
replication_slot_name = "cdc_slot"

# Slot lifecycle — see docs/connectors/postgres.md for the full story
create_replication_slot_if_missing = false   # true only for first-time provisioning
failover_slot                      = false   # PostgreSQL 17+ failover-enabled slots
slot_idle_advance_interval_ms      = 30000   # idle WAL advance; 0 disables

stream_poll_interval_ms = 100
max_events_per_poll     = 1000

# Exact "schema.table" names; include takes precedence over exclude
table_include_list = ["public.orders", "public.customers"]
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"                   # plaintext | tls (ca_cert_path, client_cert_path, client_key_path)
```

See the [PostgreSQL connector guide](connectors/postgres.md#7-configuration-reference) for field-by-field documentation.

### MySQL / MariaDB

```toml
[source]
type      = "mysql"            # or "mariadb"
host      = "localhost"
port      = 3306
user      = "cdc_user"
password  = { env = "MYSQL_PASSWORD" }
database  = "mydb"
server_id = 1001               # unique among all replication clients
conn_timeout_secs = 10

gtid_mode_enabled   = false    # true when the server runs gtid_mode = ON
binlog_format_check = true     # verify binlog_format = ROW at connect time

stream_poll_interval_ms = 100
max_events_per_poll     = 1000

# Exact "database.table" names; include takes precedence over exclude
table_include_list = ["mydb.orders"]
table_exclude_list = []

[source.transport]
mode = "plaintext"             # plaintext | tls
```

See the [MySQL connector guide](connectors/mysql.md#7-configuration-reference).

### SQL Server

```toml
[source]
type     = "sqlserver"    # or "mssql"
host     = "localhost"
port     = 1433
user     = "cdc_user"
password = { env = "MSSQL_PASSWORD" }
database = "mydb"

cdc_enabled             = true      # verify database-level CDC at connect time
cdc_schema              = "cdc"
stream_poll_interval_ms = 500
max_events_per_poll     = 1000
conn_timeout_secs       = 15
prereq_pool_size        = 4
capture_truncate_events = false     # opt-in TRUNCATE capture via DDL trigger

# Exact "schema.table" names; include takes precedence over exclude
table_include_list = ["dbo.orders"]
table_exclude_list = []

[source.transport]
mode = "tls"                        # SQL Server negotiates TLS by default
# allow_invalid_certificates = true # dev/test only (self-signed certs)
```

See the [SQL Server connector guide](connectors/sqlserver.md#6-configuration-reference).

---

## 3. Sink (`[sink]` / `[[sinks]]`)

Use `[sink]` for a single sink, or `[[sinks]]` with `[[pipeline.routes]]` for
fan-out to multiple named sinks.

### stdout (development only)

```toml
[sink]
type = "stdout"
```

### JSONL file

```toml
[sink]
type              = "file_jsonl"
path              = "/var/log/cdc/events.jsonl"
rotate_size_bytes = 104857600   # 100 MiB; 0 = no rotation
```

| Field | Default | Description |
|---|---|---|
| `path` | — | File path. The parent directory must already exist. |
| `rotate_size_bytes` | `104857600` (100 MiB) | Rotate when the file exceeds this size. `0` disables rotation. |
| `fsync_every` | `1` | fsync after every N events (`1` = every event) |

### HTTP

```toml
[sink]
type   = "http"
url    = "https://ingest.example.com/events"

# Authentication (choose one or none)
bearer_token = { env = "INGEST_TOKEN" }

# Batching
batch_max_events   = 256     # flush after N events
batch_max_delay_ms = 250     # flush after N ms, even if batch is not full

# Retries
max_retries                = 5
backoff_initial_ms         = 200
backoff_max_ms             = 5000
batch_retry_time_budget_ms = 30000   # hard budget per batch

# Connection pool
pool_max_idle_per_host = 8
tcp_keepalive_secs     = 30    # null to disable

# Dead-letter queue (terminal failures)
dlq_path      = "/var/log/cdc/dlq.jsonl"
dlq_max_bytes = 134217728   # 128 MiB

# TLS
verify_tls = true   # set false only in dev environments

# Custom headers
[sink.headers]
X-Tenant-Id  = "acme"
Content-Type = "application/x-ndjson"
```

| Field | Default | Description |
|---|---|---|
| `url` | — | HTTP endpoint URL |
| `bearer_token` | — | Bearer token sent as `Authorization: Bearer <token>` |
| `batch_max_events` | `256` | Maximum events per POST body |
| `batch_max_delay_ms` | `250` | Maximum time to wait before flushing a partial batch |
| `max_retries` | `5` | Retry attempts for retriable errors (4xx excluding 429, 5xx) |
| `backoff_initial_ms` | `200` | Initial retry backoff |
| `backoff_max_ms` | `5000` | Maximum retry backoff (exponential with jitter) |
| `batch_retry_time_budget_ms` | `30000` | Per-batch hard retry deadline |
| `pool_max_idle_per_host` | `8` | HTTP connection pool size |
| `tcp_keepalive_secs` | `30` | TCP keepalive interval; `null` disables |
| `dlq_path` | — | Path to dead-letter queue JSONL file for terminal failures |
| `dlq_max_bytes` | `134217728` | Maximum DLQ file size before new entries are dropped |
| `verify_tls` | `true` | Verify TLS certificates |

### Apache Kafka

```toml
[sink]
type    = "kafka"
brokers = "broker1:9092,broker2:9092"
topic   = "cdc.events"

# Delivery mode
delivery_mode = "at_least_once_idempotent"   # at_least_once_idempotent | transactional
# transactional_id = "rustcdc-pipeline-1"    # required (and only valid) for transactional mode

# Compression
compression = "zstd"   # none | gzip | snappy | lz4 | zstd

# Transport security (SASL is not supported — use TLS + network ACLs)
[sink.security]
protocol        = "tls"           # plaintext | tls
ssl_ca_location = "/etc/ssl/ca.pem"
verify_peer     = true

# Avro encoding (Confluent Schema Registry)
[sink.codec]
type = "avro_confluent"

[sink.codec.registry]
url          = "https://registry.example.com"
username_env = "SCHEMA_REGISTRY_KEY"      # env var names — resolved at startup
password_env = "SCHEMA_REGISTRY_SECRET"
```

| Field | Default | Description |
|---|---|---|
| `brokers` | — | Comma-separated `host:port` list |
| `topic` | — | Destination Kafka topic |
| `client_id` | `"rustcdc"` | Kafka client identifier |
| `delivery_mode` | `"at_least_once_idempotent"` | `at_least_once_idempotent` \| `transactional` |
| `transactional_id` | — | Required for `"transactional"` mode; must be unique per pipeline |
| `compression` | `"none"` | `none` \| `gzip` \| `snappy` \| `lz4` \| `zstd` |
| `ack_timeout_ms` / `retry_backoff_ms` / `retry_max_attempts` | — | Producer retry tuning |
| `security.protocol` | `"plaintext"` | `plaintext` \| `tls` (no SASL support) |

### Apache Iceberg

```toml
[sink]
type       = "iceberg"
namespace  = "cdc"
table_name = "events"
table_path = "/var/lib/rustcdc/iceberg/events"   # local staging/table path (required)
write_mode = "append"                            # append is the only mode

[sink.catalog.rest]
uri       = "https://rest-catalog.example.com"
warehouse = "s3://my-warehouse/cdc"
token     = { env = "ICEBERG_CATALOG_TOKEN" }

[sink.storage]
type = "s3"   # local_fs | s3 | gcs | adls

# Buffer limits (flush when either is exceeded)
max_pending_events = 100000     # default 100,000 events
max_pending_bytes  = 268435456  # default 256 MiB

# Schema mode
schema_mode = "normalized_with_raw"   # normalized | normalized_with_raw
```

Both schema modes materialize a `has_complete_after_image` boolean column: `false`
marks events whose `after` payload was partial (PostgreSQL unchanged-TOAST), so
data-quality checks can find them with a columnar predicate. The per-column
`unavailable_columns` lists are preserved inside `event_json` when
`schema_mode = "normalized_with_raw"`; with `normalized` they are dropped along
with the rest of the payload.

**Storage backends:**

| `type` | Authentication |
|---|---|
| `local_fs` | None |
| `s3` | `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`, or instance profile |
| `gcs` | `GOOGLE_APPLICATION_CREDENTIALS`, or Workload Identity |
| `adls` | `AZURE_STORAGE_ACCOUNT_KEY`, or managed identity |

### Fan-out (multiple sinks)

Named sinks flatten their sink fields next to `name` (no nested sub-table).
Each route maps one glob pattern to one named sink:

```toml
# Default sink — receives events that match no route
[sink]
type = "stdout"

[[sinks]]
name    = "kafka_all"
type    = "kafka"
brokers = "broker:9092"
topic   = "cdc.all"

[[sinks]]
name       = "iceberg_orders"
type       = "iceberg"
namespace  = "cdc"
table_name = "orders"
table_path = "/var/lib/rustcdc/iceberg/orders"

  [sinks.catalog.rest]
  uri       = "https://rest-catalog.example.com"
  warehouse = "s3://my-warehouse/cdc"

[[pipeline.routes]]
table_pattern = "public.orders"
sink          = "iceberg_orders"

[[pipeline.routes]]
table_pattern = "public.*"
sink          = "kafka_all"
```

Routes are evaluated top-to-bottom; the first match wins. Route patterns support
`*` globs (unlike source-side `table_include_list`, which is exact-match).

---

## 4. State backends (`[state]`)

```toml
[state]
backend = "local_fs"   # local_fs | kafka_topic | redis | postgresql
```

### Local filesystem (default)

```toml
# Short form — one directory for checkpoint + schema history
[state]
dir = "/var/lib/rustcdc/state"

# Canonical form — per-artifact directories
# [state.offset]
# dir = "/var/lib/rustcdc/state"
# [state.schema_history]
# dir = "/var/lib/rustcdc/state"
```

> Do not combine the flat `[state] dir/backend` keys with explicit
> `[state.offset]` / `[state.schema_history]` tables — the flat form takes
> precedence and rebuilds the per-artifact sections.

### Kafka compacted topic

```toml
[state]
dir = "/var/lib/rustcdc/state"        # local mirror for crash recovery

[state.backend.kafka_topic]
brokers            = "broker1:9092,broker2:9092"
topic              = "__rustcdc_state"
durability_profile = "production"     # production | development
# Optional (with defaults):
# client_id               = "rustcdc"
# request_timeout_ms      = 30000
# readback_poll_timeout_ms = 5000
# min_replication_factor  = 3          # enforced under the production profile
# min_insync_replicas     = 2
# [state.backend.kafka_topic.security]
# protocol = "tls"                     # plaintext | tls
```

Seed the topic once before first start: `rustcdc init-state --config-file cdc.toml`.

### PostgreSQL

```toml
[state]
dir = "/var/lib/rustcdc/state"        # local mirror for crash recovery

[state.backend.postgresql]
url = { env = "STATE_POSTGRES_URL" }  # e.g. postgres://user:pass@pg.example.com:5432/rustcdc_state
# Optional (with defaults):
# checkpoint_table     = "rustcdc_checkpoint"
# schema_history_table = "rustcdc_schema_history"
```

### Redis

```toml
[state]
[state.backend.redis]
url = { env = "REDIS_URL" }   # e.g. redis://localhost:6379
```

---

## 5. Pipeline (`[pipeline]`)

### Transform rules

```toml
[[pipeline.transforms]]
name = "my-rule"

  [pipeline.transforms.when]
  tables  = ["orders"]          # exact table names (case-insensitive)
  schemas = ["public"]
  ops     = ["insert", "update"]

  [[pipeline.transforms.actions]]
  type           = "filter"
  include_tables = ["orders"]

  [[pipeline.transforms.actions]]
  type         = "metadata_projection"
  target_field = "_meta"
  fields       = ["schema", "table", "operation", "source_timestamp", "offset"]
```

See [transform pipeline](concepts.md#4-transform-pipeline) for all action types.

### WASM transform runtime

```toml
[pipeline.transform_runtime]
mode = "wasm"

  [pipeline.transform_runtime.wasm]
  module_path          = "/etc/rustcdc/transform.wasm"
  timeout_ms           = 50
  max_memory_bytes     = 8388608   # 8 MiB
  max_event_bytes      = 1048576   # 1 MiB
  instance_pool_size   = 4
  fuel_yield_interval  = 10000     # null = no fuel limit
```

### Routes (fan-out)

```toml
[[pipeline.routes]]
table_pattern = "public.orders"   # glob pattern; first match wins
sink          = "iceberg_orders"  # must match a [[sinks]] name
```

---

## 6. Runtime (`[runtime]`)

```toml
[runtime]
# Buffering
max_buffer_size            = 1000    # max events held in memory before forced flush
sink_flush_interval_events = 100     # flush after N events (whichever comes first)
max_poll_wait_ms           = 100     # max wait for source events before flushing partial batch

# Parallelism
prepare_parallelism = 8

# Timeouts
sink_send_timeout_ms  = 15000   # per-request sink timeout
sink_flush_timeout_ms = 60000   # per-batch flush timeout (allow this long before SIGKILL)

# Event size + delivery queue
max_event_bytes              = 1048576   # reject events larger than this (bytes)
sink_delivery_queue_capacity = 128       # prepared-event queue between transform and sink

# Transform error policy
transform_error_policy = "halt"   # halt | skip — skip drops the event AND advances
                                  # the checkpoint past it (counted in
                                  # rustcdc_runtime_events_skipped_total: data loss)

# Post-commit source confirmation
post_commit_source_confirm_policy = "fail_fast"   # continue | fail_fast

# Circuit breaker (see concepts.md#6-circuit-breaker)
recoverable_error_breaker_consecutive_threshold = 10
recoverable_error_breaker_cooldown_ms           = 30000
recoverable_error_breaker_max_open_cycles       = 3
recoverable_error_backoff_initial_ms            = 100
recoverable_error_backoff_max_ms                = 5000
recoverable_error_backoff_multiplier            = 2.0
recoverable_error_backoff_jitter_ratio          = 0.2

# Source reconnects
[runtime.source_connection_retry]
enabled          = true
max_retries      = 5
initial_delay_ms = 300
max_delay_ms     = 10000
```

| Field | Default | Description |
|---|---|---|
| `max_buffer_size` | `1000` | In-memory event buffer; backpressure kicks in at this threshold |
| `sink_flush_interval_events` | `100` | Flush after this many buffered events |
| `max_poll_wait_ms` | `100` | Maximum source poll wait before a partial batch flush |
| `prepare_parallelism` | `8` | Concurrent sink prepare operations |
| `sink_send_timeout_ms` | `15000` | Per-request timeout |
| `sink_flush_timeout_ms` | `60000` | Per-batch flush deadline; set K8s `terminationGracePeriodSeconds` higher |
| `max_event_bytes` | `1048576` | Maximum serialized event size |
| `sink_delivery_queue_capacity` | `128` | Prepared-event queue capacity |
| `transform_error_policy` | `"halt"` | `halt` = stop pipeline on error; `skip` = drop event, advance the checkpoint past it (**data loss**, counted in `rustcdc_runtime_events_skipped_total`) |
| `post_commit_source_confirm_policy` | `"fail_fast"` | Behaviour when the post-commit source confirmation fails |
| `correctness_dedup_window_size` | `50000` | Fingerprint window for duplicate/reorder detection metrics |

---

## 7. Admin API (`[admin]`)

```toml
[admin]
bind = "127.0.0.1:8080"   # default (loopback)

# Probe auth: /livez and /healthz are always unauthenticated; /readyz needs the
# read token unless this is set AND bind is loopback
probe_auth_mode = "require_read_token"   # require_read_token | allow_unauthenticated_loopback

# Simple bearer token auth (/status, /metrics, /readyz always require a read token)
read_token_env  = "RUSTCDC_READ_TOKEN"    # env var containing the read token
write_token_env = "RUSTCDC_WRITE_TOKEN"   # env var containing the write token

# OR: token manifest (supports rotation + per-token expiry) — replaces the
# simple token envs above; the manifest keys are only valid together
# token_manifest_file                    = "/etc/rustcdc/tokens.json"
# token_manifest_trusted_public_keys_hex = ["<ed25519-pubkey-hex>"]
# token_manifest_refresh_ms              = 30000
# token_manifest_max_staleness_ms        = 120000

# Write-capable signaling requires a durable notification channel
notification_log_file = "/var/lib/rustcdc/admin-notifications.jsonl"

# TLS
[admin.tls]
cert_file           = "/etc/rustcdc/tls/server.pem"
key_file            = "/etc/rustcdc/tls/server-key.pem"
require_client_cert = false                       # true enables mTLS…
client_ca_file      = "/etc/rustcdc/tls/ca.pem"   # …validated against this CA

# Audit trail
audit_log_file            = "/var/log/rustcdc/audit.jsonl"
audit_signing_key_env     = "RUSTCDC_AUDIT_SIGNING_KEY_HEX"
audit_ip_pseudonymise     = true      # default: true (GDPR-compliant)
audit_ip_salt_env         = "RUSTCDC_AUDIT_IP_SALT_HEX"   # optional; random salt if unset

# Rate limiting (per client IP; defaults shown)
metrics_rate_limit_rps   = 20
metrics_rate_limit_burst = 40
readyz_rate_limit_rps    = 20
readyz_rate_limit_burst  = 40
status_rate_limit_rps    = 20
status_rate_limit_burst  = 40

# Trusted proxy IPs (for X-Forwarded-For)
trusted_proxy_ips = ["10.0.0.1", "10.0.0.2"]
```

> **Non-loopback binds are locked down.** Setting `bind` to anything other than
> loopback requires **all** of: `read_token_env`, `write_token_env` (or a token
> manifest), `[admin.tls]`, a notification channel for write signaling, and
> `probe_auth_mode = "require_read_token"`. The config loader rejects anything
> less at startup.

### Admin API endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/healthz` | none | Always 200; confirms HTTP server is alive |
| `GET` | `/livez` | none | 200 = alive; 503 = Error state or source degraded > 5 min |
| `GET` | `/readyz` | read | 200 = ready to serve traffic |
| `GET` | `/metrics` | read | Prometheus text-format metrics |
| `GET` | `/status` | read | JSON status snapshot (state, counters, audit trail) |
| `POST` | `/signals` | write | Send a control-plane signal |
| `GET` | `/notifications` | read | Recent CloudEvents notification log |
| `GET` | `/notifications/stream` | read | Server-sent events stream |

### Control-plane signals

```bash
# Log a deployment marker
curl -X POST http://localhost:8080/signals \
  -H "Authorization: Bearer $RUSTCDC_WRITE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"action_type":"log_marker","message":"deploy v2.4.1"}'

# Trigger an ad-hoc snapshot
curl -X POST http://localhost:8080/signals \
  -H "Authorization: Bearer $RUSTCDC_WRITE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"action_type":"execute_snapshot","tables":["public.orders"]}'
```

**Available actions:**

| `action_type` | Description |
|---|---|
| `log_marker` | Insert a named marker in the audit trail and logs |
| `execute_snapshot` | Trigger a new snapshot for specified tables |
| `pause_snapshot` | Pause an in-progress snapshot |
| `resume_snapshot` | Resume a paused snapshot |
| `stop_snapshot` | Abort the current snapshot |

---

## 8. Observability (`[observability]`)

```toml
[observability]
otlp_endpoint              = "http://otel-collector:4317"   # gRPC (default)
otlp_metrics_endpoint      = "http://otel-collector:4317"   # separate endpoint (optional)
otlp_metrics_interval_secs = 30
otlp_protocol              = "grpc"    # grpc | http
service_name               = "rustcdc-server"
```

| Field | Default | Description |
|---|---|---|
| `otlp_endpoint` | — | OTLP gRPC or HTTP endpoint for traces and metrics |
| `otlp_metrics_endpoint` | same as `otlp_endpoint` | Override metrics endpoint |
| `otlp_metrics_interval_secs` | `30` | Metrics export interval |
| `otlp_protocol` | `"grpc"` | `"grpc"` or `"http"` |
| `service_name` | `"rustcdc-server"` | Service name in telemetry |

---

## 9. Environment variables

All `RUSTCDC_*` variables override their config-file equivalents:

| Variable | Overrides | Description |
|---|---|---|
| `RUSTCDC_LOG_LEVEL` | `--log-level` | Log level: `trace` \| `debug` \| `info` \| `warn` \| `error` |
| `RUSTCDC_LOG_FORMAT` | `--log-format` | Log format: `text` \| `json` |
| `RUSTCDC_STATE_DIR` | `state.offset.dir` | State directory override |
| `RUSTCDC_ADMIN_URL` | `--admin-url` | Admin API base URL for `rustcdc status` |
| `RUSTCDC_READ_TOKEN` | — | Admin API read bearer token |
| `RUSTCDC_WRITE_TOKEN` | — | Admin API write bearer token |
| `RUSTCDC_AUDIT_SIGNING_KEY_HEX` | — | 32-byte hex-encoded Ed25519 private key for audit trail signing |
| `RUSTCDC_AUDIT_IP_SALT_HEX` | — | 16-byte hex-encoded pseudonymisation salt for audit IPs |

---

## See also

- [Getting started](getting-started.md)
- [Core concepts](concepts.md)
- [PostgreSQL connector](connectors/postgres.md)
- [MySQL / MariaDB connector](connectors/mysql.md)
- [SQL Server connector](connectors/sqlserver.md)
- [Operations guide](operations.md)
