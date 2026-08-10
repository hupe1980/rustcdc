+++
title = "Configuration reference"
description = "Every rustcdc TOML field: sources, sinks, codecs, state backends, transforms, the dead-letter queue, runtime tuning, the admin API and observability."
weight = 30
+++

Every rustcdc setting is documented here. All configuration lives in a single
TOML file (default path: `cdc.toml`). Secrets are **never** stored inline — use
`{ env = "VAR" }` for any sensitive value.

> **Tip:** run `rustcdc validate-config --config-file cdc.toml --print-json` to see
> the fully parsed and redacted config as JSON after applying environment
> variables and defaults.


## 1. Top-level fields

```toml
api_version       = "v1"              # required; must be "v1"
delivery_contract = "at_least_once"   # at_least_once | effectively_once
```

| Field | Required | Default | Description |
|---|---|---|---|
| `api_version` | yes | — | Schema version. Must be `"v1"`. |
| `delivery_contract` | no | `"at_least_once"` | When the checkpoint advances relative to delivery. `"effectively_once"` additionally requires a transactional Kafka sink **and** `state.offset.backend = "kafka_topic"` on the same cluster, because it writes the checkpoint inside the sink's transaction; any other combination is rejected at load. See [delivery contracts](@/docs/concepts.md#3-delivery-contracts). |

### Unknown keys are rejected

A key the schema does not recognise fails the load, naming its full path. This is
not tidiness — a misspelled `table_include_lst` leaves the include list empty,
which captures **every table in the database**, and a key indented one table too
far parses cleanly and does nothing. Both failure modes are silent by nature: the
setting appears to be there and the default stays in force.

The check covers the config file. Environment-overlay variables are not included,
for the reason given in [§9](#9-environment-variables).


## 2. Source (`[source]`)

The `[source]` section is **required**. Exactly one source type is supported per
pipeline.

The source `password` **must** be a deferred secret reference (`{ env = "VAR" }`);
a literal string is rejected at load. A replication credential is not an ordinary
password — it grants read access to every table the pipeline captures, and writing
it into the config file hands that access to anyone who can read the file, plus
every backup, image layer and version-control history the file passes through.
The rejection is deliberate rather than a warning: a warning at startup scrolls
past, and the credential stays in the file.

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

# How the WAL stream is read — see the connector guide before changing this
wal_transport = "streaming_replication"   # streaming_replication | sql_peek

stream_poll_interval_ms = 100
max_events_per_poll     = 1000

# Exact "schema.table" names; include takes precedence over exclude
table_include_list = ["public.orders", "public.customers"]
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"                   # plaintext | tls (ca_cert_path, client_cert_path, client_key_path)
```

### Table patterns

`table_include_list` and `table_exclude_list` take **glob patterns**, and so do
`pipeline.routes[].table_pattern`. One matcher serves all three.

| Pattern | Matches |
|---|---|
| `public.orders` | exactly that table |
| `public.*` | every table in `public` |
| `public.order_?` | `public.order_1`, not `public.order_10` |
| `orders` | `public.orders` **and** `tenant_private.orders` — an unqualified entry is schema-agnostic |

`*` and `?` work inside a segment and do not cross the `.`, so `public.*` never matches
`other.orders`. Blank entries are ignored rather than treated as catch-alls.

> **These were exact-match before rustcdc 0.12.** `table_exclude_list = ["public.tmp_*"]`
> excluded nothing, which is indistinguishable from a set of tables that never changed;
> an include list matching nothing is indistinguishable from an idle database. If either
> list carried a literal `*` as a no-op placeholder, **it is now a catch-all** — audit
> both before upgrading. Entries containing neither `*` nor `?` behave exactly as before.

An unqualified entry on the *include* side widens the very thing the list exists to bound,
so the connector logs a WARN naming each one at startup. Qualify them unless you mean the
pattern to span schemas.

`wal_transport` defaults to `streaming_replication`: `START_REPLICATION ... LOGICAL`
over the streaming replication protocol, which is what `pg_recvlogical` and
PostgreSQL's own subscribers use. The server pushes WAL as it is written, so delivery
latency is not bounded by `stream_poll_interval_ms`. It requires the `REPLICATION`
role attribute and a **direct** connection — a pooler in transaction-pooling mode
cannot carry a replication stream.

`sql_peek` is the fallback for environments that cannot provide either. It is slower
by construction and its cost grows with the source's longest-running transaction, so
selecting it logs a warning at startup and from `validate-config`. See
[WAL transport](@/docs/connectors/postgres.md#wal-transport-how-the-stream-is-read).

`mode = "tls"` now **requires** TLS: a connector pointed at a server with `ssl = off`
fails to connect instead of silently downgrading to an unencrypted connection.

See the [PostgreSQL connector guide](@/docs/connectors/postgres.md#7-configuration-reference) for field-by-field documentation.

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

See the [MySQL connector guide](@/docs/connectors/mysql.md#7-configuration-reference).

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

See the [SQL Server connector guide](@/docs/connectors/sqlserver.md#6-configuration-reference).


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

# Dead-letter queue: see [dlq] — it is pipeline-wide, not per-sink, and also
# supports a Kafka topic target.

# TLS
verify_tls = true   # set false only in dev environments

# Custom headers
[sink.headers]
X-Tenant-Id  = "acme"
Content-Type = "application/x-ndjson"
```

| Field | Default | Description |
|---|---|---|
| `url` | — | HTTP endpoint URL. **Must not embed credentials** — a `user:pass@` component is rejected at load, because it travels into the `/config` snapshot that a read-scoped token can read. A credential in the query string (`?api_key=`) is redacted there rather than rejected, since that is the only form some endpoints accept — but prefer `bearer_token` or an `authorization` header. |
| `bearer_token` | — | Bearer token sent as `Authorization: Bearer <token>` |
| `batch_max_events` | `256` | Maximum events per POST body |
| `batch_max_delay_ms` | `250` | Maximum time to wait before flushing a partial batch |
| `max_retries` | `5` | Retry attempts for retriable errors (4xx excluding 429, 5xx) |
| `backoff_initial_ms` | `200` | Initial retry backoff |
| `backoff_max_ms` | `5000` | Maximum retry backoff (exponential with jitter) |
| `batch_retry_time_budget_ms` | `30000` | Per-batch hard retry deadline |
| `pool_max_idle_per_host` | `8` | HTTP connection pool size |
| `tcp_keepalive_secs` | `30` | TCP keepalive interval; `null` disables |
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

# Compression + batching
compression       = "zstd"   # none | gzip | snappy | lz4 | zstd
compression_level = 6        # gzip 0–9, zstd 1–22; rejected for codecs without a level
batch_size        = 16384    # bytes accumulated per partition batch
linger_ms           = 0      # ordinary Kafka linger; amortised across the pipelining window
max_pipelined_sends = 128    # records accepted before waiting for an acknowledgement


# Transport security
[sink.security]
protocol        = "sasl_ssl"      # plaintext | tls | sasl_plaintext | sasl_ssl
ssl_ca_location = "/etc/ssl/ca.pem"   # omit to use the platform trust store
verify_peer     = true

[sink.security.sasl]
mechanism = "plain"                       # plain | scram_sha_256 | scram_sha_512 | oauth_bearer | aws_msk_iam
username  = "CONFLUENT_API_KEY"
password  = { env = "CONFLUENT_API_SECRET" }

# Avro encoding (Confluent Schema Registry)
[sink.codec]
type         = "avro_confluent"
registry_ref = "prod"          # or an inline [sink.codec.registry] table
```

| Field | Default | Description |
|---|---|---|
| `brokers` | — | Comma-separated `host:port` list |
| `topic` | — | Destination Kafka topic |
| `client_id` | `"rustcdc-server"` | Kafka client identifier |
| `delivery_mode` | `"at_least_once_idempotent"` | `at_least_once_idempotent` \| `transactional` |
| `transactional_id` | — | Required for `"transactional"` mode; must be unique per pipeline |
| `compression` | `"none"` | `none` \| `gzip` \| `snappy` \| `lz4` \| `zstd` |
| `compression_level` | — | Gzip `0–9`, Zstd `1–22`. Setting one on a codec that has no level is a startup error, not a silent no-op. Available on both delivery modes. |
| `batch_size` | `16384` | Bytes per partition batch before a send |
| `linger_ms` | `0` | Ordinary Kafka linger: how long a partially-filled batch waits for more records. Because up to `max_pipelined_sends` records are in flight at once, the wait is amortised across all of them rather than paid per record. The default stays `0` because CDC consumers are usually latency-sensitive, not because linger is harmful. |
| `max_pipelined_sends` | `128` | Records accepted for delivery before the sink waits for an acknowledgement — the pipelining window. `1` restores one broker round-trip per record. Per-partition ordering is unaffected: records reach the broker in submission order regardless of the depth. Two other settings cap the effective depth, so raising this alone past either has no effect: `runtime.sink_flush_interval_events` (a flush drains the window) and `runtime.sink_delivery_queue_capacity` (bounds how far the prepare stage runs ahead). |
| `ack_timeout_ms` / `retry_backoff_ms` / `retry_max_attempts` | — | Producer retry tuning (the TCP connect timeout follows `ack_timeout_ms` down, capped at 10 s) |

#### `[sink.security]`

| Field | Default | Description |
|---|---|---|
| `protocol` | `"plaintext"` | `plaintext` \| `tls` \| `sasl_plaintext` \| `sasl_ssl` |
| `ssl_ca_location` | — | CA bundle for verification. Omitted, the **platform trust store** is used — which is what a managed broker with a publicly-issued certificate needs. |
| `ssl_certificate_location` / `ssl_key_location` | — | Client certificate chain + key for mTLS. Setting one without the other is a startup error: the handshake would silently fall back to server-only auth and the broker would reject the client with an error naming neither field. |
| `sni_hostname` | — | Override the SNI name, for brokers reached through a load balancer or port-forward whose address does not match their certificate. |
| `verify_peer` | `true` | Must stay `true` for any TLS protocol. |

#### `[sink.security.sasl]`

| Field | Default | Description |
|---|---|---|
| `mechanism` | `"plain"` | `plain` \| `scram_sha_256` \| `scram_sha_512` \| `oauth_bearer` \| `aws_msk_iam` |
| `username` / `password` | — | Required for `plain` and both SCRAM mechanisms. |
| `token` | — | A pre-issued OAuth 2 bearer token for `oauth_bearer`. Static tokens expire and the broker then rejects every reconnect; prefer `[.oidc]`. |
| `extensions` | `{}` | SASL extensions sent with an OAUTHBEARER token — Confluent Cloud uses `logicalCluster` and `identityPoolId`. |
| `region` | — | AWS region for `aws_msk_iam`. Omitted, the region comes from `AWS_REGION` / `AWS_DEFAULT_REGION`. Credentials always come from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`; the session token is read whether or not a region is set, so assumed roles, instance profiles and EKS web identities work. |

**Every mechanism composes with TLS.** `sasl_ssl` + SCRAM-SHA-256/512 — the
default listener on Redpanda Cloud, Aiven and most Strimzi installs — is
supported, as is `sasl_ssl` with any other mechanism. The CA path, client
certificate and SNI override configured under `[sink.security]` apply to all of
them.

Combinations the loader **rejects**, each because accepting it would be worse
than failing:

* `sasl_plaintext` + `plain` — PLAIN puts the password on the wire verbatim.
* `aws_msk_iam` on anything but `sasl_ssl` — MSK IAM listeners are TLS-only.
* TLS material under a non-TLS protocol, or `[.sasl]` under a non-SASL protocol —
  in both cases the settings read as active and do nothing.

#### `[sink.security.sasl.oidc]` — OAUTHBEARER via `client_credentials` (KIP-768)

```toml
[sink.security.sasl]
mechanism = "oauth_bearer"
extensions = { logicalCluster = "lkc-abc123", identityPoolId = "pool-xyz" }

[sink.security.sasl.oidc]
token_endpoint  = "https://idp.example.com/oauth2/v1/token"   # https only
client_id       = "rustcdc-prod"
client_secret   = { env = "OIDC_CLIENT_SECRET" }
scope           = "kafka"
form_parameters = { audience = "kafka-cluster" }
request_timeout_ms = 10000
```

The provider is called on **every new broker connection**, including automatic
reconnects, so tokens are always fresh — the failure mode a static `token` has
is that it expires and every subsequent reconnect is rejected.

#### `[sink.transport]` — connection-level tuning

```toml
[sink.transport]
tcp_keepalive_ms        = 30000    # 0 = OS default
connections_max_idle_ms = 540000   # 0 = never evict idle connections
max_connections         = 0        # 0 = unlimited
max_in_flight           = 10       # unacknowledged requests per connection
tls_reload_interval_ms  = 300000   # 0 = never re-read the certificate (KIP-1288)
socks5_proxy            = "bastion.internal:1080"   # omit for a direct connection
```

`max_in_flight` **moved here from `[sink.kafka]`, and its cap is gone.** It used to be
rejected above 5, on the usual reasoning that an idempotent producer preserves ordering
only up to `max.in.flight.requests.per.connection = 5` (KIP-679) and that a retried batch
could otherwise land after one produced later.

That rule protects against several batches for the *same partition* being on the wire at
once, which krafka's record accumulator does not do: it holds one in-flight slot per
partition and only dispatches partitions whose slot is idle, so batches reach the wire in
seal order and sequence order cannot diverge from wire order. krafka 0.18 removed its own
producer-level knob for that reason, leaving the per-*connection* ceiling — which is a
transport concern, hence the move. Raising it lets more partitions have requests in flight
over one connection; it does not let a partition get ahead of itself.

`socks5_proxy` tunnels **every** broker connection, and the proxy performs the DNS
resolution — which is the point in a VPN or bastion topology where broker hostnames do
not resolve outside the tunnel. Give it as `host:port`; a value without a colon is a
startup error.

`tls_reload_interval_ms` matters wherever certificates rotate on their own
schedule (cert-manager, Vault): a long-lived producer that read them once keeps
presenting an expired client certificate until it is restarted. Setting it
without a TLS protocol is a startup error — there would be nothing to reload.

**Message keys and ordering.** Events with a primary key are keyed by the
compact-JSON primary-key values (e.g. `{"id":42}`), so all changes to one row
land on one partition in order. Events from tables **without** a primary key
are keyed by the qualified table name (`schema.table`), preserving per-table
ordering while spreading tables across partitions. Kafka guarantees ordering
only within a partition — consumers that need cross-table ordering must use a
single-partition topic.

**Durability.** Every producer runs with `acks = all` and idempotence enabled,
and the sink verifies the broker's per-record delivery confirmation — a send
that the broker did not acknowledge durably fails the batch instead of being
treated as delivered. All compression codecs listed above (including `zstd`)
are compiled into the binary.


### Codecs (`[sink.codec]`)

| `type` | Framing | Registry |
|---|---|---|
| `json` *(default)* | Raw JSON; compact-JSON primary key as the message key | — |
| `json_pretty` | Raw JSON, indented. For reading by eye, not for throughput. | — |
| `avro` | Plain Avro binary, no header. The reader must hold the schema out of band. | — |
| `protobuf` | Plain protobuf; `before`/`after` carry UTF-8 JSON in `bytes` fields | — |
| `avro_confluent` | Confluent 5-byte header + Avro | required |
| `json_schema_confluent` | Confluent 5-byte header + JSON | required |
| `protobuf_confluent` | Confluent header + message-index path + protobuf | required |
| `glue_avro` | AWS Glue 18-byte header + Avro | AWS Glue (`glue` feature) |
| `cloud_events` | CloudEvents 1.0 JSON envelope | — |

Prefer `avro_confluent` over bare `avro` whenever a registry is available. Avro
binary is positional and untagged, so a reader resolving the wrong schema does
not get an error — it gets shifted fields and plausible-looking wrong values.

`json_schema_confluent` validates every event against rustcdc's published JSON
Schema before encoding; set `validate = false` to skip that, at the cost of the
guarantee that what leaves the process matches the schema its header advertises.

```toml
[sink.codec]
type     = "json_schema_confluent"
validate = true
registry_ref = "prod"
```

### Schema registries (`[registries.<name>]`)

Define a registry once and reference it by name from every codec that uses it.
Repeating the block per sink is how two copies drift apart.

```toml
[registries.prod]
url    = "https://registry.example.com"
flavor = "confluent"          # confluent | apicurio

# Basic auth (Confluent Cloud API key/secret) …
username_env = "SCHEMA_REGISTRY_KEY"
password_env = "SCHEMA_REGISTRY_SECRET"
# … or an OAuth/IAM bearer token (mutually exclusive with username_env)
# token_env  = "SCHEMA_REGISTRY_TOKEN"

subject_name_strategy = "topic_name"   # topic_name | record_name | topic_record_name
auto_register         = true
normalize_schemas     = false
preflight             = true

request_timeout_ms     = 30000
connect_timeout_ms     = 0     # 0 = client default
max_cache_entries      = 0     # 0 = client default (1000)
pool_max_idle_per_host = 0     # 0 = reqwest default

[registries.prod.retry]
enabled         = true
max_retries     = 3
base_backoff_ms = 100
max_backoff_ms  = 5000
```

| Field | Default | Description |
|---|---|---|
| `url` | — | For `confluent`, the API root that serves `/subjects`. For `apicurio`, the **server root** — the client appends `/apis/registry/v3` itself, and a URL that already carries an API path is rejected. |
| `flavor` | `"confluent"` | `confluent` (Confluent Platform/Cloud, Karapace, Redpanda, or Apicurio's `/apis/ccompat/v7`) \| `apicurio` (native v3 API). Both emit Confluent framing, so consumers do not need to know which produced the message. |
| `auto_register` | `true` | With `false`, the subject must already carry **rustcdc's** schema. The encoder compares the registered schema against what it will write and refuses to start if they differ — taking the id without checking is the silent-corruption path, and `auto_register = false` is the setting a careful operator picks. |
| `normalize_schemas` | `false` | Ask the registry to normalise on registration, preventing schema-id churn from field-ordering differences. |
| `preflight` | `true` | Verify reachability and schema compatibility at **startup**. Schema resolution sits on the encode path, so without this a registry problem surfaces as a failed event mid-pipeline rather than a failed start. Confluent flavour only. |
| `retry.*` | on, 3 attempts | Jittered exponential back-off honouring `Retry-After`, for transient failures only (transport, 429, 5xx). Not-found, auth and invalid-schema fail immediately, so an outer loop cannot spin on them. |
| `references` | `[]` | Confluent schema references (`name` / `subject` / `version`), for deployments that register rustcdc's envelope in a shared subject namespace. |

### AWS Glue Schema Registry (`type = "glue_avro"`)

Glue is not Confluent-compatible, so it does not use `[registries.*]`: the framing
is an 18-byte header (`0x03`, a compression byte, a 16-byte schema-version UUID)
rather than Confluent's 5-byte magic + integer id, and schema identity is a UUID
rather than a monotonic id. The payload is the same `io.rustcdc.Event` Avro
envelope, so a consumer that already decodes rustcdc's Avro events needs only the
framing changed.

```toml
[sink.codec]
type            = "glue_avro"
registry_name   = "default-registry"
schema_name     = "cdc-events"
# key_schema_name = "cdc-events-key"   # defaults to {schema_name}-key
zlib_compression = false
auto_register    = true
```

| Field | Default | Description |
|---|---|---|
| `registry_name` | `"default-registry"` | Glue registry to register into. |
| `schema_name` | — | Required. Glue schema name for the event envelope. |
| `key_schema_name` | `{schema_name}-key` | Glue schema name for the primary-key envelope. |
| `zlib_compression` | `false` | Glue's header carries a compression byte; Confluent's does not. |
| `auto_register` | `true` | **Cannot be `false`.** `schemreg`'s Glue client has no lookup-by-name API, so the setting could only be accepted and ignored — the exact failure mode this config surface exists to avoid. Glue's `register_schema` is idempotent for identical content, so leaving it on is safe even when the schemas are provisioned out of band. |

Credentials and region come from the standard AWS chain (environment,
`~/.aws/credentials`, instance/task role, EKS web identity) — the same chain the
Kafka sink's `aws_msk_iam` mechanism uses, so one IAM identity covers both.

**Requires the `glue` cargo feature.** It is compiled into the published container
image; a source build needs `cargo build --features glue`, because Glue pulls the
AWS SDK (~45 crates) that no other deployment needs. Without it, a `glue_avro`
codec is rejected at startup naming the remedy rather than falling back to another
framing.

> Glue is the one backend with no live-service evidence anywhere in the stack —
> there is no self-hostable implementation to test against. rustcdc covers the
> Avro conversion, framing, compression byte, schema identity, error
> classification and round trip against an in-memory fake; cdc-server covers the
> configuration surface. Neither has run against real Glue.

### Apache Iceberg

**The Iceberg table is an append-only change log, not a mirror of the source table.** Each
event becomes a row carrying `operation`, `source_offset`, `fingerprint_hex` and the row
image; nothing merges updates or deletes into a current-state view. Answering "what does
`public.orders` look like now?" is a `MERGE`/window query the consumer writes.

There is no `write_mode` setting. There used to be one whose only legal value was
`"append"` — a knob with a single value is documentation pretending to be configuration,
and it invited the belief that an upsert mode existed. Upsert via Iceberg v2 equality
deletes is the tracked gap; when it lands the setting returns with two real values.

Because nothing merges, **enable `snapshot_expiry`**: the sink commits a snapshot per
flush, and unbounded snapshot metadata is read in full on every planning pass.

```toml
[sink]
type       = "iceberg"
namespace  = "cdc"
table_name = "events"
table_path = "/var/lib/rustcdc/iceberg/events"   # local staging/table path (required)

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

# Parquet output
parquet_compression    = "zstd"     # uncompressed | snappy | zstd | gzip | lz4
parquet_row_group_rows = 1048576

# Snapshot expiry (off by default)
[sink.snapshot_expiry]
enabled       = true
older_than_ms = 604800000   # 7 days — the time-travel window readers keep
retain_last   = 10          # always keep at least this many snapshots
interval_ms   = 3600000     # minimum gap between expiry runs
```

| Field | Default | Description |
|---|---|---|
| `parquet_compression` | `"zstd"` | CDC payloads are JSON-shaped and repetitive, so the default typically cuts stored bytes several-fold at negligible write cost. `uncompressed` is parquet's own default and is almost never what you want here. |
| `parquet_row_group_rows` | `1048576` | Row groups are the unit a reader can skip with statistics; smaller groups prune better on selective scans and cost more metadata. |
| `snapshot_expiry.enabled` | `false` | A CDC sink commits on every flush, so the table gains a snapshot per flush and its metadata is read in full on every planning pass. Nothing prunes that on its own. |
| `snapshot_expiry.older_than_ms` | 7 days | Snapshots older than this are dropped. This **is** the time-travel window — size it for the longest query or rollback you must support. |
| `snapshot_expiry.retain_last` | `10` | Floor on retained snapshots regardless of age. |
| `snapshot_expiry.interval_ms` | 1 hour | Expiry rewrites table metadata, so running it per flush would multiply catalog traffic for no benefit. |

Expiry failures are logged, never propagated: it is housekeeping, and the events
it runs after are already durably committed.

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

Routes are evaluated top-to-bottom; the first match wins. Route patterns and the
source-side `table_include_list` / `table_exclude_list` use the **same** matcher — see
[Table patterns](#table-patterns) below.


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
# checkpoint_table     = "rustcdc_state_checkpoint"
# schema_history_table = "rustcdc_state_schema_history"
```

**Two tables, deliberately.** The checkpoint and the schema history have different
lifecycles: truncating the checkpoint table to force a re-snapshot is a routine recovery
step, and the schema history is what MySQL and SQL Server need to decode their logs at
all. Losing it to a checkpoint reset would turn a re-snapshot into a re-provision.

Both are created on first write by the OpenDAL PostgreSQL service; the role needs
`CREATE` on the schema, or you can pre-create them and grant only `SELECT`/`INSERT`/
`UPDATE`/`DELETE`.

### Redis

```toml
[state]
[state.backend.redis]
url = { env = "REDIS_URL" }   # e.g. redis://localhost:6379
```


### Single-writer enforcement

Exactly one process may own a pipeline's state. How that is enforced depends on the
backend, and the guarantees differ:

| Backend | Mechanism | Strength |
|---|---|---|
| `local_fs` | `HOSTNAME:PID` owner-lease file, plus a PID lock on the state dir | Strong on a shared path; a second process is refused |
| `kafka_topic` | Kafka transactional id + `init_transactions()` (KIP-447) | **Strongest.** The broker fences the previous producer by epoch. No TTL, no race |
| `redis`, `postgresql` | TTL owner lease beside the checkpoint, renewed on a heartbeat and re-verified before each write | Good. Closes the rolling-update overlap; see the caveat below |

A second instance that tries to start against a held lease refuses to run and names the
current owner. A crashed owner's lease expires by itself after 60 s, so recovery needs
no manual step.

**Caveat for `redis` / `postgresql`:** acquisition is read-then-write, not
compare-and-swap, because OpenDAL exposes no uniform CAS across those services. Two
instances starting within the same read-write window can both observe a free slot. That
window is milliseconds; the case this protects — a rolling update where the incumbent's
lease is live — is fully covered. If you need a hard guarantee, use `kafka_topic`.

**This is not a substitute for `strategy: Recreate`.** See the
[Kubernetes deployment](@/docs/operations.md#11-kubernetes-deployment) section: a Deployment
without it surges to two pods on every rollout, and the second will now refuse to start
rather than corrupt state — which turns a silent data problem into a stuck rollout. Set
`Recreate` and neither happens.

Source-level exclusivity is uneven and should not be relied on: a PostgreSQL
replication slot admits one connection and MySQL rejects a duplicate `server_id`, but
**SQL Server CDC capture tables are ordinary reads with no exclusivity at all**. That
connector depends entirely on the state lease.


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

See [transform pipeline](@/docs/concepts.md#4-transform-pipeline) for all action types.

#### `mask` — redact, hash or encrypt fields

```toml
[[pipeline.transforms]]
name = "redact_pii"

  [[pipeline.transforms.actions]]
  type              = "mask"
  warn_on_unmatched = true          # default

    [pipeline.transforms.actions.rules]
    email       = { type = "redact", placeholder = "***" }
    card_number = { type = "truncate", keep = 4 }
    "emails.*"  = { type = "redact" }
    ssn         = { type = "hmac_sha256", key = { env = "PII_HMAC_KEY" } }
    iban        = { type = "encrypt",     key = { env = "PII_AES_KEY" } }
```

| Rule `type` | Behaviour |
|---|---|
| `passthrough` | Leave unchanged (the default for unlisted fields) |
| `unsalted_sha256` | Deterministic SHA-256. Obfuscation only — a low-cardinality field falls to a rainbow table in seconds. |
| `redact` | Replace with `placeholder` (default `"***"`) |
| `null` | Replace with JSON `null`. Indistinguishable downstream from a genuine `NULL`; use `redact` when a consumer must tell them apart. |
| `truncate` | Keep the first `keep` characters of a string |
| `hmac_sha256` | Keyed, deterministic pseudonymisation. GDPR-safe while the key stays secret, and stable — joins and dedup on the masked field still work. |
| `encrypt` / `decrypt` | AES-256-GCM to `enc:v1:<nonce>:<ciphertext>`, bound to `table + path` as associated data. **Non-deterministic** — never apply to a primary key or any field a downstream deduplicates on. |

Paths are exact and dotted. A trailing `.*` matches every child one level down,
which is the only way to cover a variable-length array — enumerating `emails.0`,
`emails.1`, … leaks whatever you did not guess. A rule on an object- or
array-valued field masks the whole subtree.

Keys **must** be deferred secret references (`{ env = "VAR" }`); a literal in the
config file is rejected at load, because a key written there makes every value it
masked re-identifiable for as long as that file exists.

`warn_on_unmatched` (default `true`) reports every rule that never matched. Rules
match by pattern, so a typo or a renamed column disables one *silently* and
nothing errors — the report is the difference between a rule that is off and one
that looks on.

Two surfaces carry it, and the metric is the one to alert on:

* **`rustcdc_transform_rules_unmatched`**, labelled `sink` / `transform` / `kind` /
  `rule`. Emitted **only** for rules that are unmatched, so its absence is the
  healthy state and `> 0` is a complete alert rule with no threshold to pick. The
  `transform` label is `<your rule name>/<stage>`, so it points at the rule you
  wrote rather than the internal stage.
* A WARN per rule at shutdown, naming the **consequence** — which differs per
  stage: a mask rule that never fired means that column shipped in clear text; a
  route rule that never fired means events went to the default destination.

#### `field_mapping` — copy, rename, set, remove

```toml
  [[pipeline.transforms.actions]]
  type   = "field_mapping"
  rename = [["region", "location"]]
  copy   = [["id", "legacy_id"]]
  remove = ["internal_notes"]
  strict = true                      # missing source/removal path is an error
  [pipeline.transforms.actions.set]
  source_system = "crm"
```

With `strict = false` a renamed-away column silently produces no output field;
with `true` it becomes a transform error routed through
`runtime.transform_error_policy`.

#### `outbox` — unwrap the transactional-outbox pattern

```toml
  [[pipeline.transforms.actions]]
  type  = "outbox"
  table = "outbox_events"
```

An `INSERT` into `table` whose row carries `aggregate_id`, `event_type` and
`payload` is rewritten into the domain event it represents: `table` becomes the
`event_type`, `after` becomes the `payload`, and `aggregate_id` becomes the key —
so each aggregate keeps its own ordering downstream. `UPDATE` and `DELETE`
against the outbox table are a cleanup job's housekeeping and pass through
untouched.

### WASM transform runtime

```toml
[pipeline.transform_runtime]
mode = "wasm"

  [pipeline.transform_runtime.wasm]
  module_path          = "/etc/rustcdc/transform.wasm"
  timeout_ms           = 50
  max_memory_bytes     = 8388608   # 8 MiB
  max_event_bytes      = 1048576   # 1 MiB
  instance_pool_size   = 4         # concurrent guest instances; bounds transform parallelism
  fuel_yield_interval  = 10000     # null = no fuel limit
```

### Routes (fan-out)

```toml
[[pipeline.routes]]
table_pattern = "public.orders"   # glob pattern; first match wins
sink          = "iceberg_orders"  # must match a [[sinks]] name
```


## 5b. Dead-letter queue (`[dlq]`)

Where events that can **never** be delivered are quarantined so the pipeline can make
forward progress instead of halting on them.

```toml
[dlq]
enabled   = true
type      = "file"                          # file | kafka
path      = "/var/lib/rustcdc/dlq.jsonl"
max_bytes = 134217728                       # refuse writes beyond this, do not grow unbounded
```

```toml
[dlq]
enabled = true
type    = "kafka"
brokers = "kafka:9092"
topic   = "cdc.dlq"

[dlq.security]                              # same shape as [sink.kafka.security]
protocol = "sasl_ssl"
mechanism = "scram-sha-512"
```

**It is off by default, and that is deliberate.** Quarantining an event advances the
checkpoint past something that was never delivered. That is data loss — recorded rather
than silent, but loss. Without `[dlq]` a permanently undeliverable event **halts the
pipeline**, which is the right answer when the contents matter more than the uptime.
Enabling this is choosing the opposite trade, and you should know you are choosing it.

### What gets quarantined

A failure is classified along two independent axes, because "permanent" and "this event's
fault" are different questions and only their conjunction justifies quarantine.

**Quarantined** — permanent *and* attributable to the record. The same bytes fail
identically forever, so skipping is the only way forward:

* an encoded payload over `runtime.max_event_bytes`
* a broker rejection of the record itself — `MessageTooLarge`, `InvalidRecord`,
  `RecordListTooLarge`, `InvalidTimestamp`
* a codec or schema-registry rejection
* an envelope-contract violation after a transform

**Retried, never quarantined** — transient. Broker unavailable, leader election, timeout,
connection reset. These are retried under the `[runtime]` backoff and circuit-breaker
policy; dead-lettering them would throw away good data because a dependency blinked.

**Halts the pipeline, never quarantined** — permanent but *not* the record's fault: bad
SASL credentials, a revoked topic ACL, a missing topic, an unparseable configuration.
Quarantining these would drain the entire change stream into the dead-letter queue one
event at a time while every health check still reported the pipeline as running — the
silent data loss the DLQ exists to prevent, delivered by the DLQ itself. The pipeline
stops instead, so the real cause is what you see.

### Operating it

Each record is one JSON object per line carrying the timestamp, sink, table, **source
offset**, the error, and the full event. The source offset is what tells you which
upstream position was skipped, and the full event is what makes a replay possible:

```bash
jq -c '.event' /var/lib/rustcdc/dlq.jsonl > replay.jsonl
rustcdc replay replay.jsonl --config-file cdc.toml
```

The `kafka` target additionally attaches the triage fields as **record headers**, so a
dead-letter topic can be filtered without deserialising every payload — which is what you
want at 3 a.m. with a console consumer:

| Header | Value |
|---|---|
| `__rustcdc.dlq.source.table` | Fully-qualified `"schema.table"` (also the record key) |
| `__rustcdc.dlq.source.offset` | The upstream position that was skipped |
| `__rustcdc.dlq.sink` | The sink that refused the event |
| `__rustcdc.dlq.exception.message` | The rendered error chain, truncated to 2 KiB |

The namespace mirrors krafka's `__krafka.dlq.*` convention without reusing its names:
those describe a Kafka record that failed to *produce*, these describe a change event no
sink would accept, and the source table is not a Kafka topic. The exception header is
truncated because a broker rejecting an oversized record would lose the dead letter
entirely — the untruncated text is always in the body.

Alert on `rustcdc_dlq_events_total`. Any non-zero rate is a data incident. The shipped
rules include `RUSTCDCEventsQuarantined` for this. See the
[runbook](@/docs/runbook.md#5-events-are-being-quarantined).

On a container filesystem the `file` target is lost with the pod — which is exactly when
you want to read it. Mount a volume, or use the `kafka` target.


## 6. Runtime (`[runtime]`)

### `max_event_bytes` measures the encoded payload

The limit applies to the bytes the sink actually transmits — for a Kafka or HTTP sink,
the codec output (key + value); for stdout, `file_jsonl` and Iceberg, which serialise the
event themselves, the event's JSON.

It previously measured a JSON rendering of the event in **all** cases, which for Avro or
Protobuf is a payload that is never sent — usually several times larger than the real
one. If you set this against a broker's `max.message.bytes`, you were calibrating against
the wrong number and rejecting events that would have fitted.

An event over the limit fails the batch and is **never retried**: it is the same size on
every attempt, so retrying makes no progress and never reaches the events behind it.

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
                                         # measured on the ENCODED payload — see below
sink_delivery_queue_capacity = 128       # prepared-event queue between transform and sink

# Transform error policy
transform_error_policy = "halt"   # halt | skip — skip drops the event AND advances
                                  # the checkpoint past it (counted in
                                  # rustcdc_runtime_events_skipped_total: data loss)

# Post-commit source confirmation
post_commit_source_confirm_policy = "fail_fast"   # continue | fail_fast

# Where a delivered batch may end
transaction_boundary = "split"   # split | preserve_transactions

# Envelope validation and shutdown deadline
validate_events       = true
sink_close_timeout_ms = 30000    # 0 = wait indefinitely

# Schema-history retention (0 = keep every version)
schema_history_max_versions_per_table = 0

# Runtime duplicate suppression
[runtime.idempotency]
enabled  = true
capacity = 100000    # fingerprints retained in the sliding window
ttl_ms   = 0         # 0 = evict by capacity only

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
| `prepare_parallelism` | `8` | How many events are transformed and encoded concurrently. **Not a general throughput knob** — see below |
| `sink_send_timeout_ms` | `15000` | Per-request timeout |
| `sink_flush_timeout_ms` | `60000` | Per-batch flush deadline; set K8s `terminationGracePeriodSeconds` higher |
| `max_event_bytes` | `1048576` | Maximum serialized event size |
| `sink_delivery_queue_capacity` | `128` | Prepared-event queue capacity |
| `transform_error_policy` | `"halt"` | `halt` = stop pipeline on error; `skip` = drop event, advance the checkpoint past it (**data loss**, counted in `rustcdc_runtime_events_skipped_total`) |
| `post_commit_source_confirm_policy` | `"fail_fast"` | Behaviour when the post-commit source confirmation fails |
| `correctness_dedup_window_size` | `50000` | Fingerprint window for duplicate/reorder detection metrics |
| `transaction_boundary` | `"split"` | `split` cuts batches wherever the buffer limits fall — lowest latency, bounded memory. `preserve_transactions` never ends a batch mid-transaction: batches are otherwise cut on `max_buffer_size`, `max_event_bytes` and barrier capacity, none of which know anything about transactions, so a sink can commit rows 1–3 of a five-row transaction — a state that never existed in the source. A transaction larger than `max_buffer_size` is still delivered split, with a WARN, because a permanent silent stall would be worse. |
| `validate_events` | `true` | Validate every envelope inside the runtime. Turning it off removes the check that catches a self-contradictory envelope (a column both listed unavailable and present in the payload) before a sink acts on it. |
| `sink_close_timeout_ms` | `30000` | Deadline for the sink's `close()` at shutdown; `0` waits indefinitely. A sink wedged on an unreachable broker otherwise hangs past the supervisor's grace period, turning an orderly drain into a SIGKILL. |
| `schema_history_max_versions_per_table` | `0` (unbounded) | Historical schema versions retained per table. Unbounded history grows for the deployment's lifetime while only the versions spanning the replay window are ever read. |
| `idempotency.enabled` | `true` | Sliding-window duplicate suppression across restarts. The guard suppresses only events it can *identify* — one carrying transaction metadata, or a primary key whose columns are present in the row image. Everything else passes through and is counted: at-least-once is the documented contract, while dropping a distinct row is not recoverable by anyone. |
| `idempotency.capacity` | `100000` | Fingerprints retained. Size for the deployment's **replay distance**, not its event rate: once the window fills, older duplicates stop being suppressed. Evictions are exported as `rustcdc_runtime_idempotency_evictions_total`. |
| `idempotency.ttl_ms` | `0` | Optional fingerprint lifetime; `0` keeps one until capacity evicts it. |


### When `prepare_parallelism` actually helps

Only the **prepare** stage — transform and encode — is parallel. Delivery is a single
sequential loop, and that is deliberate: per-partition ordering is the contract CDC
consumers depend on, and the sequential consumer is what provides it.

So the knob buys throughput only when preparing an event is expensive *relative to
delivering* it. Measured on the reference machine (`benches/BASELINES.md`):

| Stage | Cost per event |
|---|---|
| Transform, no rules | 0.42 µs |
| Transform, one mask rule | 0.92 µs |
| Full batch through a `file_jsonl` sink | 47.6 µs |

Delivery is roughly **100×** the transform, so raising `prepare_parallelism` from 1 to 16
moves end-to-end throughput by less than measurement noise — it is tuning 1 % of the work.

Raise it when the prepare stage is genuinely expensive:

* **WASM transforms** — guest execution is CPU-bound and pooled; this is the knob that feeds
  the pool, and it should be set alongside `transform_runtime.wasm.instance_pool_size`.
* **A heavy codec** — Avro or Protobuf with schema-registry lookups.

Leave it at the default for a JSON codec and a network or disk sink. For Kafka set it to
`1`: the producer is already async and pipelined, so the prepare stage is never the
constraint.


## 6b. Snapshots

Two bootstrapping paths exist and **exactly one** may be configured; setting both
is a startup error, because every listed table would be read twice and the
duplicate would look like genuine change data downstream.

```toml
# Blocking: the stream does not start until the snapshot finishes.
snapshot_tables = ["public.orders", "public.customers"]

# Non-blocking (DBLog watermark): chunks interleave with the live stream.
[incremental_snapshot]
tables     = ["public.orders", "public.customers"]
chunk_size = 5000

# Optional: restrict which *rows* each table's backfill reads.
[incremental_snapshot.table_conditions]
"public.orders" = "t.created_at >= '2026-01-01'"
```

Prefer `[incremental_snapshot]` for anything large enough that the wait matters:
capture starts immediately and a big table does not hold the replication slot
open while it is read. Chunk cursors are persisted inside the connector
checkpoint — in the same atomic, fsynced, checksummed write as the stream
position — so a restart resumes mid-backfill instead of re-reading from row zero.

| Field | Default | Description |
|---|---|---|
| `tables` | `[]` | `"schema.table"` entries, processed in order. Empty disables incremental snapshotting — but see below: an empty table declared section still enables on-demand snapshots. |
| `chunk_size` | `5000` | Rows per chunk. Each chunk is one keyset-paginated `SELECT` bracketed by watermarks: bigger chunks backfill faster and hold the override window open longer, smaller ones interleave more finely with the stream. |
| `table_conditions` | `{}` | Per-table row filter, keyed by `"schema.table"` — Debezium's `additional-condition`. See below. |

### Filtering which rows a backfill reads

`table_conditions` appends a SQL boolean expression to a table's chunk `SELECT`, so a
backfill can cover one tenant or one date range instead of the whole table. It does
**not** restrict the live stream, which keeps carrying every change to the table.

```toml
[incremental_snapshot]
tables = ["public.orders"]

[incremental_snapshot.table_conditions]
"public.orders" = "t.region = 'eu' AND t.created_at >= '2026-01-01'"
```

Alias the table as `t` to qualify a column; that is the alias every connector's chunk
read uses.

> **This is raw SQL, and it is trusted input.** The expression is interpolated into
> the chunk `SELECT` — a filter that could only be a bound parameter could not express
> the predicates this exists for. It carries the same trust level as the connection
> string.
>
> **It is not a tenancy boundary.** Do not accept one over an API, do not build one
> from user input, and do not treat it as an access control. It is a backfill scope.

A filter that fails to parse surfaces as a chunk-read error naming the table, at the
first chunk — not as a silently empty backfill.

A condition may be pre-declared for a table that is not in `tables` — that is how you
scope a backfill you intend to request later through `execute_snapshot`. Since rustcdc
0.12 all three resolution paths (startup, checkpoint resume, on-demand request) go through
one function, so a configured filter applies wherever the table is resolved.

That was not always true. Until 0.12 the on-demand path could not see `table_conditions`
at all, and worse, the two paths disagreed with *each other*: a runtime-requested table ran
unfiltered and a restart then adopted it **with** the filter, so the rows delivered
corresponded to no single predicate and where the split fell depended on when the process
happened to restart. We reported it; this project refused the combination outright until
the fix landed.

### Snapshotting a table without a restart

With `[incremental_snapshot]` configured, tables can be added to a **running**
pipeline through the admin API:

```bash
curl -X POST http://localhost:8080/signals \
  -H "Authorization: Bearer $RUSTCDC_WRITE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"action_type":"execute_snapshot","tables":["public.invoices"]}'
```

The live stream is not paused and the request survives a restart. Declare the section
even when the initial set is empty if you want this available:

```toml
[incremental_snapshot]
tables = []          # nothing at startup; execute_snapshot still works
```

See [control-plane signals](#on-demand-snapshots-execute-snapshot) for the semantics
of an already-tracked or already-completed table.


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

**How the buckets behave.** Each endpoint has its own token bucket **per client key** —
the peer IP, or the `X-Forwarded-For` client when the peer is in `trusted_proxy_ips`. One
noisy peer cannot exhaust another's budget. A client seen for the first time starts with
the full configured `burst`, and the bucket refills at `rps`.

The exception is deliberate: once the limiter is tracking a large number of distinct
client keys — the signature of an attacker rotating source addresses — a *new* key is
admitted with a single token instead of the full burst, so rotation cannot multiply the
attacker's allowance by the burst size. Steady-state clients are unaffected.

Rate limiting is evaluated **before** authentication, so an unauthenticated flood is
cheap to refuse. A `429` therefore does not imply your credentials are wrong.

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
| `GET` | `/config` | read | The running configuration, every credential redacted |
| `POST` | `/signals` | write | Send a control-plane signal |
| `GET` | `/notifications` | read | Recent CloudEvents notification log |
| `GET` | `/notifications/stream` | read | Server-sent events stream (long-lived) |

#### `/config`

The configuration this instance is **actually** running — after environment-variable
layering and any config migration — with every credential redacted:

```bash
curl -s -H "Authorization: Bearer $RUSTCDC_READ_TOKEN" \
  http://localhost:8080/config | jq .
```

A separate endpoint rather than a field on `/status`, because `/status` is polled on a
short interval by dashboards and a whole configuration document in every response is
bandwidth nobody asked for. This is the question you ask once, during an incident.

Redaction removes credentials, not topology: hosts, topics, table lists and file paths are
all present, which is what makes the answer useful. See
[what redaction covers](@/docs/operations.md#what-status-redaction-actually-covers).

#### `/notifications/stream`

A real `text/event-stream`: it replays the recent notification backlog, then stays open and
pushes each new notification as it is raised. It does not terminate on its own.

| Behaviour | Detail |
|---|---|
| Event `id` | The audit-trail sequence — monotonic and gapless |
| Event types | `notification` (a CloudEvent payload), `lagged`, `error` |
| Resumption | Send `Last-Event-ID`; only notifications *newer* than that id are delivered. `EventSource` does this automatically on reconnect. |
| Keep-alive | A comment frame every 15 s, so proxies do not close an idle connection |
| Slow clients | A subscriber more than 256 notifications behind receives a `lagged` event carrying the number missed, rather than a silently incomplete sequence |

```bash
# Follow the stream; -N disables curl's output buffering
curl -N -H "Authorization: Bearer $RUSTCDC_READ_TOKEN" \
  http://localhost:8080/notifications/stream

# Resume after the last event you processed
curl -N -H "Authorization: Bearer $RUSTCDC_READ_TOKEN" \
  -H "Last-Event-ID: 417" \
  http://localhost:8080/notifications/stream
```

Use `/notifications` instead if you want a point-in-time snapshot rather than a feed.

### Control-plane signals

```bash
# Log a deployment marker
curl -X POST http://localhost:8080/signals \
  -H "Authorization: Bearer $RUSTCDC_WRITE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"action_type":"log_marker","message":"deploy v2.4.1"}'

# Snapshot tables on the running pipeline — no restart, no pause.
# `rustcdc snapshot public.orders --admin-write-token-env RUSTCDC_WRITE_TOKEN`
# is the same request, with the table names validated before they are sent.
curl -X POST http://localhost:8080/signals \
  -H "Authorization: Bearer $RUSTCDC_WRITE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"action_type":"execute_snapshot","tables":["public.orders"]}'

# Backfill one tenant, without touching the config file or restarting.
curl -X POST http://localhost:8080/signals \
  -H "Authorization: Bearer $RUSTCDC_WRITE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "action_type": "execute_snapshot",
        "tables": ["public.orders", "public.order_items"],
        "conditions": {
          "public.orders":      "tenant_id = 42",
          "public.order_items": "tenant_id = 42"
        }
      }'
```

**Available actions:**

| `action_type` | Description |
|---|---|
| `log_marker` | Insert a named marker in the audit trail and logs. `message` is required |
| `execute_snapshot` | Snapshot the named `tables` on the running pipeline. `tables` is required; `conditions` optionally scopes them |
| `pause_snapshot` / `resume_snapshot` | Suspend or resume chunk reading. The live stream is unaffected |
| `stop_snapshot` | Abandon the remaining tables. Survives a restart |

#### Per-request row filters (`conditions`)

`conditions` is Debezium's `additional-conditions`: a SQL boolean expression per table,
applied to that table's chunk `SELECT` for **this request only**. It overrides
`incremental_snapshot.table_conditions` for the tables it names; a table without an
override keeps its configured filter.

The filter belongs on the request because that is what it is. "Backfill tenant 42's
orders" is a one-off, and routing it through static configuration means editing a file
and restarting a process to run something that was meant to be a signal.

Every key must name a table in the same request's `tables` list. A key that does not is
rejected with `400` rather than accepted and ignored — an ignored filter reads the
**whole** table, and the only symptom is volume, which is indistinguishable from a large
table.

> **Raw SQL, and trusted input.** The expression is interpolated into the chunk `SELECT`
> and carries the same trust level as the connection string. It is **not a tenancy
> boundary**: do not proxy an end user's input into it. The write-scope token that reaches
> this endpoint can already `stop_snapshot`; treat it accordingly.

Whether a filter took effect is observable: `/status` reports the effective expression per
table under `.incremental_snapshot`, and `/metrics` exports
`rustcdc_incremental_snapshot_table_filtered{table="…"}` as 0 or 1. The expression itself
is deliberately absent from `/metrics` — it can carry column names and literal values that
have no business in a scrape.

Unknown fields are rejected with `400`, over HTTP and through the file and Kafka
ingress channels alike — so a mistyped key fails rather than being dropped.

#### On-demand snapshots (`execute_snapshot`)

This is the equivalent of Debezium's `execute-snapshot` signal, and it needs none of
the machinery: there is **no signal table in the source**, so it works against a
read-only role and a read replica.

Use it to backfill a table just added to the publication, rebuild a downstream store,
or re-run history through a corrected transform. The live stream is never paused — the
new tables are chunked into it exactly like the ones in `[incremental_snapshot]`,
under the same DBLog watermark suppression.

| Case | Behaviour |
|---|---|
| Table not currently tracked | Added and read from the start |
| Table already in progress | No-op, so retrying a request is safe |
| Table already complete | Rewound and read again |

Every name is resolved against the catalog **before anything is mutated**, so one bad
reference fails the whole request rather than half-applying it. Requests are durable:
an enqueued table reaches the checkpoint with the next commit and is picked up again
after a restart, even though it is not in the configured list.

**Requires `[incremental_snapshot]`.** The request adds tables to an existing
incremental snapshot; with no such section there is nothing to add to, and the signal
is refused with a terminal state of `ABORTED` naming the missing configuration rather
than reporting a backfill that never happens. An empty `incremental_snapshot.tables`
is enough to enable it.

Check before firing rather than after — `/status` and `/metrics` both report the
capability, because the `POST` answers `STARTED` whether or not the pipeline can
service it:

```bash
curl -s -H "Authorization: Bearer $RUSTCDC_READ_TOKEN" \
  http://localhost:8080/status | jq .slo.snapshot_requests
# { "available": true, "accepted_total": 3, "refused_total": 0, "tables_enqueued_total": 7 }
```

`rustcdc_snapshot_requests_available` is the same value as a gauge, and
`rustcdc_snapshot_requests_refused_total` backs the shipped
`RUSTCDCSnapshotRequestsRefused` alert.

The signal is asynchronous: the `POST` answers `STARTED`, and the outcome — including
`tables_enqueued` — arrives on the audit trail and the notification stream. The
pipeline picks requests up between polls, so expect a delay of up to
`runtime.max_poll_wait_ms`.

```bash
# Watch the outcome
curl -N -H "Authorization: Bearer $RUSTCDC_READ_TOKEN" \
  http://localhost:8080/notifications/stream
```

#### Snapshot pause / resume / stop

`pause_snapshot`, `resume_snapshot` and `stop_snapshot` suspend chunk reading, resume
it, and abandon the remaining tables respectively. The live stream is unaffected in all
three cases — pausing a backfill to relieve pressure on the source does not pause
capture.

Pause and resume report whether they changed anything, so a redundant call is
distinguishable from an effective one. `stop_snapshot` reports how many tables it
dropped. A paused snapshot survives a restart: the pause flag is part of the snapshot
state carried in the checkpoint.

These endpoints answered `501 Not Implemented` until rustcdc 0.11 shipped the controls.
Before that they answered `200 OK` and recorded `PAUSED` / `RESUMED` / `ABORTED` while
doing nothing at all — an operator pausing a snapshot would have watched it keep running
under a green audit trail saying it had stopped. Refusing outright was the interim fix;
this is the real one.

Progress is observable while any of this is happening:

```bash
curl -s -H "Authorization: Bearer $RUSTCDC_READ_TOKEN" \
  http://localhost:8080/status | jq .incremental_snapshot
```

and on `/metrics` as `rustcdc_incremental_snapshot_active`,
`rustcdc_incremental_snapshot_paused`, `rustcdc_incremental_snapshot_stopped`,
`rustcdc_incremental_snapshot_generation`,
`rustcdc_incremental_snapshot_tables_remaining`,
`rustcdc_incremental_snapshot_rows_emitted`, and the per-table
`rustcdc_incremental_snapshot_table_rows_emitted{table="…"}` /
`rustcdc_incremental_snapshot_table_complete{table="…"}` /
`rustcdc_incremental_snapshot_table_filtered{table="…"}`.

`stopped` is worth alerting on separately from `active`: a stopped snapshot is a
deliberate operator action, and it must stay stopped across a deploy. `generation` counts
how many times snapshot work has been requested, which is what makes a deliberate
re-snapshot distinguishable from a replay — without it the two are byte-identical and the
idempotency guard drops the re-snapshot, so an operator re-requesting a table gets
`enqueued: 1` and no rows. All of them are absent
entirely when no snapshot is in flight, rather than reported as zero — a stale zero and
"nothing running" are different states and an alert should not have to guess which it is
looking at.

### Signal ingress channels

Signals can also arrive without an HTTP call, which is what you want when the admin API
is not reachable from wherever the automation lives — a Kubernetes `Job`, a CI pipeline,
a control-plane topic other systems already publish to.

```toml
[admin]
# Append-only JSONL. One signal per line, same shape as the POST body.
signal_ingress_file = "/var/lib/rustcdc/signals.jsonl"

[admin.signal_ingress_kafka]
brokers  = "kafka:9092"
topic    = "cdc.signals"
group_id = "rustcdc-signals"

[admin.signal_ingress_kafka.security]   # same shape as [sink.kafka.security]
protocol = "sasl_ssl"
mechanism = "scram-sha-512"
```

Both accept exactly the payload `POST /signals` takes, both reject unknown fields, and
both are subject to the same validation — an `execute_snapshot` without `tables` is
refused on every channel.

**Neither replays history on startup, and this matters.** A signal is a *command*, not
state: re-running one is not idempotent, because rustcdc rewinds an already-complete
table and reads it again. A channel that replayed its backlog would re-snapshot every
table ever requested on every restart.

| Channel | On startup | A signal sent while the server is down |
|---|---|---|
| `signal_ingress_file` | Resumes at the **current end of file** | Not processed — the read offset is process-local, so there is nothing to resume from |
| `signal_ingress_kafka` | Resumes from the **committed group offset**; a group with none starts at the end (`auto.offset.reset = latest`) | Processed, as long as the group's offsets have not aged out of `offsets.retention.minutes` |

Use the Kafka channel when signals must survive a restart. The file channel is a live
tail, not a queue.

The audit trail deduplicates a repeated `signal_id`, but it holds 512 entries in memory
and starts empty on every boot — so it is a convenience for retries within one process
lifetime, not the mechanism that makes the above safe.


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
| `otlp_endpoint` | — | Collector endpoint for traces and metrics. Port `4317` for gRPC, `4318` for HTTP |
| `otlp_metrics_endpoint` | same as `otlp_endpoint` | Override metrics endpoint |
| `otlp_metrics_interval_secs` | `30` | Metrics export interval |
| `otlp_protocol` | `"grpc"` | `"grpc"` or `"http"`. Anything else is **rejected at load** — a wrong value is invisible at runtime, because the exporter simply talks the other protocol at the collector and nothing arrives |
| `service_name` | `"rustcdc-server"` | Service name in telemetry |

`otlp_protocol = "http"` selects OTLP/HTTP with protobuf encoding — the collector's
`:4318` listener. Give the **base** URL and the signal path is appended for you
(`/v1/traces`, `/v1/metrics`), matching the OTLP specification's rule for
`OTEL_EXPORTER_OTLP_ENDPOINT`:

```toml
[observability]
otlp_endpoint = "http://otel-collector:4318"   # POSTs to /v1/traces and /v1/metrics
otlp_protocol = "http"
```

An endpoint that already carries a path is used exactly as written, which is the escape
hatch for a collector behind a prefix (`https://gw.example.com/otlp/v1/traces`).

The plaintext guard is about the **transport**, not the protocol: `http://` to a
non-loopback host is refused for either protocol unless `OTLP_ALLOW_INSECURE=1` is set.
`otlp_protocol = "http"` against an `https://` endpoint is the normal production shape.


## 9. Environment variables

Three mechanisms share the environment, and conflating them is the usual source of
"I set the variable and nothing happened".

### Fixed variables

These names are built in — each backs a CLI flag:

| Variable | Flag | Description |
|---|---|---|
| `RUSTCDC_LOG_LEVEL` | `--log-level` | Log level: `trace` \| `debug` \| `info` \| `warn` \| `error` |
| `RUSTCDC_LOG_FORMAT` | `--log-format` | Log format: `text` \| `json` |
| `RUSTCDC_STATE_DIR` | `--state-dir` | State directory override (`run`, `init-state`) |
| `RUSTCDC_ADMIN_URL` | `--admin-url` | Admin API base URL for `rustcdc status` |
| `CDC_AUDIT_SIGNING_KEY_HEX` | — | 64-char hex Ed25519 private key for audit-trail signing. Unset means audit records are unsigned. |

### Operator-named variables

Secrets are **not** read from fixed names. You choose the name and the config points
at it, so one deployment's naming scheme does not have to bend to ours:

| Config key | Holds |
|---|---|
| `source.password = { env = "…" }` | Replication credential — **required** to be a reference, never a literal |
| `admin.read_token_env` / `admin.write_token_env` | Admin API bearer tokens |
| `admin.audit_ip_salt_env` | 32-char hex salt for audit-IP pseudonymisation. Unset means a random salt per process, so pseudonyms do not correlate across restarts. |
| `[sink.*]` codec / registry / SASL credentials | See the sink sections above |

### The `RUSTCDC_` config overlay

Any other `RUSTCDC_`-prefixed variable is merged over the config file, with `__`
as the table separator — `RUSTCDC_SOURCE__POSTGRES__HOST` sets `source.postgres.host`.

This namespace is **shared** with the fixed variables above, which are read by name
and are not config keys. That is why unknown-key rejection (see [Top-level
fields](#1-top-level-fields)) applies to the config *file* only: rejecting every
prefixed variable that is not a config field would reject `RUSTCDC_LOG_LEVEL`. The
practical consequence is that a typo in an overlay variable is **not** caught — it
lands as an ignored key. Prefer the config file for anything you want validated.
