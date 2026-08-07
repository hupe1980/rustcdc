+++
title = "Operations"
description = "Run rustcdc in production: CLI commands, health and readiness probes, Prometheus metrics, OpenTelemetry, replay, tuning profiles and Kubernetes deployment."
weight = 60
+++

Day-2 operations for rustcdc — keeping it running, diagnosing problems,
and recovering from failures.


## 1. CLI reference

```
rustcdc [OPTIONS] <COMMAND>

Options:
  -V, --version                  Print version and exit
  -c, --config-file <FILE>            Path to TOML config file
      --log-level <LEVEL>        trace|debug|info|warn|error [env: RUSTCDC_LOG_LEVEL]
      --log-format <FORMAT>      text|json [env: RUSTCDC_LOG_FORMAT]

Commands:
  init                 Generate a starter config file
  run                  Start the CDC pipeline
  validate-config      Parse and validate the config without starting
  status               Query status via the admin API
  dry-run              Test sink delivery with synthetic events
  inspect-checkpoint   Show the stored checkpoint and schema history
  replay               Replay events from a JSONL file
  migrate-state        Migrate state between backends
  init-state           Seed the kafka_topic state backend (errors for other backends)
```

### `rustcdc init`

```
Options:
  --output <FILE>       Output config path (default: cdc.toml)
  --force               Overwrite an existing file
  --profile <PROFILE>   dev | prod (default: dev)
  --state-dir <DIR>     State directory in generated config
  --admin-bind <ADDR>   Admin bind address in generated config
```

### `rustcdc run`

```
Options:
  --state-dir <DIR>                 Override state directory [env: RUSTCDC_STATE_DIR]
  --snapshot-table <SCHEMA.TABLE>   Tables for initial snapshot (repeatable)
  --checkpoint-parity-mode <MODE>   auto | enabled | disabled (default: auto)
```

### `rustcdc status`

```
Options:
  --admin-url <URL>               Admin API base URL (default: http://127.0.0.1:8080)
                                  [env: RUSTCDC_ADMIN_URL]
  --require-running               Exit 1 if not in Running state
  --admin-ca-file <FILE>          Custom CA for TLS
  --admin-client-cert-file <FILE> Client cert for mTLS
  --admin-client-key-file <FILE>  Client key for mTLS
  --admin-read-token <TOKEN>      Read bearer token
  --admin-read-token-env <VAR>    Env var containing the read token
```

### `rustcdc replay`

```
rustcdc replay <FILE> [OPTIONS]

Options:
  --sink <SINK>              stdout | file_jsonl | http | kafka | iceberg
  --limit <N>                Stop after N events
  --max-file-bytes <BYTES>   Reject replay files larger than this
  --skip-before-offset <O>   Skip events before this WAL LSN or hex u64
  --checkpoint-parity-mode   auto | enabled | disabled
```


## 2. Health and status

### Admin API probes

```bash
# Is the HTTP server alive?
curl http://localhost:8080/healthz   # always 200

# Is the pipeline healthy?
curl http://localhost:8080/livez     # 200 = running; 503 = Error/degraded

# Is the pipeline ready for traffic?
# Unauthenticated only with probe_auth_mode = "allow_unauthenticated_loopback"
# (requires a loopback bind); the default require_read_token needs the bearer header.
curl http://localhost:8080/readyz    # 200 = ready; 503 = still initialising

# Full status JSON
curl -H "Authorization: Bearer $RUSTCDC_READ_TOKEN" http://localhost:8080/status

# Prometheus metrics
curl -H "Authorization: Bearer $RUSTCDC_READ_TOKEN" http://localhost:8080/metrics
```

### `rustcdc status`

`status` talks to the admin API directly — it does not read the config file:

```bash
rustcdc status --admin-url http://127.0.0.1:8080 \
  --admin-read-token-env RUSTCDC_READ_TOKEN
# Exit code 0 = Running; 1 = not Running (use with --require-running in CI)
rustcdc status --admin-read-token-env RUSTCDC_READ_TOKEN --require-running
```

### Key metrics to watch

| Metric | Alert threshold | Description |
|---|---|---|
| `rustcdc_runtime_health{verdict="stalled"}` | == 1 | Runtime is running but progress stopped (dead socket, commit divergence, lag growth). One-hot gauge: exactly one verdict label is 1. `verdict="idle"` is deliberately not alertable — a quiet database is not an incident. The stall reason is logged at WARN |
| `rustcdc_runtime_events_skipped_total` | any increase | Events permanently dropped by `transform_error_policy = "skip"` — the checkpoint advances past them, so **any increase is confirmed data loss** |
| `rustcdc_runtime_replication_slot_lag_bytes` | > 1 GiB sustained | PostgreSQL slot WAL retention; growth risks slot invalidation (`max_slot_wal_keep_size`) or a full WAL volume |
| `rustcdc_slo_checkpoint_age_seconds` | > 300 s | Checkpoint has not advanced — sink or source issue |
| `rustcdc_source_consecutive_poll_errors` | > 0 | Source connection degraded |
| `rustcdc_runtime_recoverable_breaker_open_total` | increasing | Circuit breaker firing repeatedly |
| `rustcdc_slo_readiness_ready_total / rustcdc_slo_readiness_checks_total` | < 0.99 | Readiness probe failures |
| `rustcdc_audit_log_drop_total` | > 0 | Audit log queue saturated |
| `rustcdc_end_to_end_ack_lag_seconds` | p95 > 30 s | **Freshness** — source commit to sink durability, the number a data consumer actually experiences. A histogram, so read it with `histogram_quantile`; see below |

#### Freshness: read the percentile, not the average

```promql
histogram_quantile(
  0.95,
  sum by (le, sink) (rate(rustcdc_end_to_end_ack_lag_seconds_bucket[5m]))
)
```

`rustcdc_end_to_end_ack_lag_seconds_avg` and `_last` are also exported and are useful on a
dashboard, but neither can express a freshness SLO. A pipeline delivering 99% of events in
200 ms and 1% ten minutes late reports a healthy average and a `_last` that depends on
which event happened to be most recent. The histogram bounds run from 100 ms to one hour,
so the quantile stays meaningful while a pipeline works through a backlog rather than
collapsing into `+Inf`.

A recovering pipeline breaches this legitimately and clears on its own; sustained breach
with no restart behind it means the sink cannot keep up with the source.

SLO alert rules for Prometheus are in
[`monitoring/rustcdc_slo_alerts.yml`](https://github.com/hupe1980/rustcdc-server/blob/main/monitoring/rustcdc_slo_alerts.yml).


## 3. Graceful shutdown

rustcdc handles `SIGTERM` and `SIGINT` gracefully:

1. Stops accepting new events from the source
2. Flushes the current in-flight batch to the sink
3. Advances the checkpoint
4. Exits with code 0

**Kubernetes `terminationGracePeriodSeconds`** must be at least as long as
`runtime.sink_flush_timeout_ms` (default: 60 s) plus headroom:

```yaml
spec:
  terminationGracePeriodSeconds: 90   # 60s flush + 30s headroom
```

**Do not** send `SIGKILL` before the grace period expires — the current batch
will not be checkpointed and will be redelivered on the next start.


## 4. Inspect checkpoint

View the stored checkpoint and (optionally) the schema history without starting
the pipeline:

```bash
# Show current checkpoint offset
rustcdc inspect-checkpoint --config-file cdc.toml

# Include schema history (MySQL / MariaDB only)
rustcdc inspect-checkpoint --config-file cdc.toml --schema-history
```

Example output:

```
Checkpoint
  offset:     0/1B2C3D4E
  at:         2026-06-07T12:00:00Z
  sequence:   4821

Schema history
  entries:    12
  last DDL:   2026-06-05T09:30:00Z
  tables:     public.orders, public.customers
```

### Checkpoint integrity

Checkpoint files carry a `content_checksum` (SHA-256 over the other fields,
rustcdc ≥ 0.7.0) that is verified on every load, and must be owner-only
(`chmod 600`). This closes a silent-corruption path: a flipped bit in an LSN
does not fail to parse — it resumes capture from a *wrong* position, skipping
events with no error raised anywhere.

Consequences:

- **Never edit a checkpoint file by hand.** A hand-edited file fails the
  integrity check and the runtime refuses to start — by design.
- `inspect-checkpoint` surfaces integrity failures as
  `Checkpoint error: … integrity check …` instead of printing a value.
- To seed or repair a checkpoint (disaster recovery), use the official restore
  path, which computes the checksum and writes the file `0600` + fsynced —
  from a checkout of the `rustcdc` crate:

  ```bash
  cargo run --example seed_checkpoint --features postgres -- \
    --dir /var/lib/rustcdc/checkpoint \
    --source-type postgres \
    --committed-event-count 0 \
    --offset '{"lsn": 281474976711680, "slot_name": "cdc_slot"}'
  ```

  (Embedders: `FileCheckpoint::restore_from_record`.) `migrate-state` and the
  Redis/PostgreSQL state mirrors use this same path internally, and
  `migrate-state` verifies every migrated checkpoint by loading it through the
  runtime's own checksum/permission/offset gates before reporting success.


## 5. Replay events

If you have captured a JSONL event file (from a `file_jsonl` sink, a DLQ, or a
previous `stdout` run), you can replay it through the full transform + sink pipeline
without connecting to the source database:

```bash
# Replay to stdout
rustcdc replay events.jsonl --config-file cdc.toml

# Replay to the sink defined in cdc.toml
rustcdc replay events.jsonl --config-file cdc.toml --sink http

# Replay only the first 1000 events
rustcdc replay events.jsonl --config-file cdc.toml --limit 1000

# Skip events before a specific WAL LSN
rustcdc replay events.jsonl --config-file cdc.toml \
  --skip-before-offset 0/1A2B3C4D

# Reject large files (safety guard)
rustcdc replay events.jsonl --config-file cdc.toml \
  --max-file-bytes 104857600   # 100 MiB
```

Replay does not advance the checkpoint of a running pipeline. It is safe to run
while the pipeline is stopped.


## 6. Migrate state

Move the checkpoint and schema history between backends without losing position:

```bash
rustcdc migrate-state \
  --source-backend local_fs \
  --source-dir /var/lib/rustcdc/old-state \
  --target-dir /var/lib/rustcdc/new-state \
  --output migration-report.json
```

`migrate-state` writes **file-backed state** (`--target-dir`); it reads from
`local_fs` or, with `--source-backend kafka --kafka-brokers … --kafka-topic …`,
drains a Kafka state topic into files. Migrated checkpoints are re-materialized
through the official restore path (checksum computed, `0600`, fsynced) and
verified by loading them through the runtime's own integrity gates.

**Procedure: Kafka-topic state → local files (e.g. decommissioning Kafka state):**

1. Stop the pipeline (`SIGTERM`; wait for clean exit).
2. `rustcdc migrate-state --source-backend kafka --kafka-brokers broker:9092 \
      --kafka-topic cdc-state --target-dir /var/lib/rustcdc/state --output report.json`
3. Update `cdc.toml` to the `local_fs` state backend pointing at the target dir.
4. Start the pipeline. It resumes from the migrated checkpoint.

**Procedure: local files → Kafka-topic state:**

1. Stop the pipeline.
2. Update `cdc.toml` to the `kafka_topic` state backend and run
   `rustcdc init-state --config-file cdc.toml` to seed the topic.
3. Start the pipeline — on first start with seeded Kafka state it restores from
   the topic and continues from the checkpoint it finds there.
5. Verify with `rustcdc inspect-checkpoint --config-file cdc.toml` and the
   Prometheus metrics.


## 7. Dry run

Test the full transform + sink pipeline with synthetic events, without connecting
to the source database:

```bash
rustcdc dry-run --config-file cdc.toml --event-count 50
```

Synthetic events cover all `op` types (`insert`, `update`, `delete`, `read`) and
use the schema of the first table in `table_include_list`. Use this to:

- Verify sink connectivity before go-live
- Test WASM transforms without a live database
- Benchmark throughput


## 8. Circuit-breaker recovery

When the source becomes intermittently unavailable, rustcdc opens the circuit
breaker and enters exponential backoff. See
[concepts → circuit breaker](@/docs/concepts.md#6-circuit-breaker) for the state diagram.

### Manual recovery

If the pipeline is stuck in Error state:

1. Resolve the underlying issue (network, database, credentials).
2. Restart the pipeline:
   ```bash
   # Kubernetes
   kubectl rollout restart deployment/rustcdc

   # Docker
   docker restart rustcdc

   # Systemd
   systemctl restart rustcdc
   ```

### Tuning the breaker

For a source with frequent short-lived outages (e.g., cloud DB maintenance
windows), relax the thresholds to avoid unnecessary restarts:

```toml
[runtime]
recoverable_error_breaker_consecutive_threshold = 30     # was 10
recoverable_error_breaker_cooldown_ms           = 60000  # 1 min was 30s
recoverable_error_breaker_max_open_cycles       = 10     # was 3
```


## 9. Performance tuning

### Baseline metrics

Before tuning, establish a baseline:

```bash
# Events per second delivered to the sink:
rate(rustcdc_runtime_events_committed_total[1m])

# Checkpoint age (how far behind real-time):
rustcdc_slo_checkpoint_age_seconds
```

### High-throughput HTTP (target: 50 k events/s)

```toml
[runtime]
max_buffer_size            = 5000
sink_flush_interval_events = 500
prepare_parallelism        = 16

[sink]
type                   = "http"
batch_max_events       = 1000
batch_max_delay_ms     = 50
pool_max_idle_per_host = 32
tcp_keepalive_secs     = 30
```

### High-throughput Kafka

```toml
[runtime]
max_buffer_size             = 10000
sink_flush_interval_events  = 1000   # a flush drains the pipelining window — keep it >= max_pipelined_sends
sink_delivery_queue_capacity = 1024  # how far the prepare stage may run ahead of delivery
prepare_parallelism         = 1      # Kafka producer is already async

[sink]
type                = "kafka"
brokers             = "broker1:9092,broker2:9092,broker3:9092"
compression         = "zstd"
delivery_mode       = "at_least_once_idempotent"
max_pipelined_sends = 512   # records accepted before waiting for an acknowledgement
linger_ms           = 5     # amortised across the window; trades latency for larger batches
```

**The three settings interact, and the smallest wins.** The effective pipelining depth is
`min(max_pipelined_sends, sink_flush_interval_events, sink_delivery_queue_capacity)`,
because a flush drains the window and the delivery queue bounds how far the prepare stage
runs ahead. Raising `max_pipelined_sends` alone past either of the others does nothing.

Depth is what makes `linger_ms` worth setting: with records in flight concurrently, a batch
fills from many sends and the linger is paid once for the batch rather than once per
record. Measured against an in-process broker, 300 records: a depth-1 window took 712 ms
and 300 produce requests; a depth-256 window took 7.9 ms and 3. Per-partition ordering is
identical either way.

Memory cost is bounded by the window: `max_pipelined_sends` encoded payloads held for
retry, per Kafka sink.

### Iceberg (batch-oriented)

Iceberg is optimised for large batches. Flush less frequently for bigger,
more efficient Parquet files:

```toml
[sink]
type               = "iceberg"
max_pending_events = 500000
max_pending_bytes  = 536870912   # 512 MiB
```

### Memory pressure

If the container OOMs:

1. Reduce `max_buffer_size` and `sink_flush_interval_events` to flush more often.
2. If using Iceberg, reduce `max_pending_bytes`.
3. If using WASM transforms, reduce `instance_pool_size` or `max_memory_bytes`.


## 10. Security hardening

### Credentials

All secrets support inline env-var references:

```toml
password    = { env = "POSTGRES_PASSWORD" }
bearer_token = { env = "INGEST_TOKEN" }
```

Credentials backed by `SecretString` are never emitted in logs, config snapshots
(`/status`), or support bundles.

### Metric units

Every duration and latency family is exported in **seconds**, per the Prometheus
base-unit convention, and captured at microsecond resolution.

This was not always true. Latency was previously captured with millisecond truncation
against histogram bounds starting at 1 ms — so per-event work, which is measured in
microseconds, truncated to `0`, landed entirely in the `le="1"` bucket, and made every
quantile identical. A tenfold regression produced no visible change. If you have
dashboards predating this, the families were renamed:

| Old | New |
|---|---|
| `rustcdc_*_latency_ms` / `_ms_avg` / `_ms_last` | `rustcdc_*_latency_seconds` / `_seconds_avg` / `_seconds_last` |
| `rustcdc_end_to_end_ack_lag_ms_*` | `rustcdc_end_to_end_ack_lag_seconds_*` |
| `rustcdc_sink_http_retry_delay_ms_*` | `rustcdc_sink_http_retry_delay_seconds_*` |
| `rustcdc_sink_http_batch_retry_duration_ms_*` | `rustcdc_sink_http_batch_retry_duration_seconds_*` |
| `rustcdc_slo_admin_api_latency_ms_histogram` | `rustcdc_slo_admin_api_latency_seconds_histogram` |
| `rustcdc_admin_rate_limiter_*_decision_latency_ms_*` | `..._decision_latency_seconds_*` |

`rustcdc_runtime_checkpoint_age_ms` is deliberately unchanged; use
`rustcdc_slo_checkpoint_age_seconds` for alerting, which is what the shipped rules do.

### Alert rules

`monitoring/rustcdc_slo_alerts.yml` is validated two ways, and both matter:

* `promtool check rules` — PromQL **syntax**.
* `tests/alert_rules.rs` — that every metric a rule references is one the server
  actually **emits**.

The second exists because promtool cannot know which metrics exist. Eighteen of the
twenty-nine metrics the rules referenced turned out not to exist at all, so two thirds of
the alerting was silent while looking configured — including the checkpoint-age,
delivery-latency, revoked-token and crash-window-detection alerts. A rule that cannot
fire is worse than no rule: its silence reads as health.

If you add a rule, run `cargo test --test alert_rules`.

### Audit trail

Configure Ed25519 signing and GDPR-compliant IP pseudonymisation:

```toml
[admin]
audit_log_file         = "/var/log/rustcdc/audit.jsonl"
audit_signing_key_env  = "RUSTCDC_AUDIT_SIGNING_KEY_HEX"
audit_ip_pseudonymise  = true    # default: true
audit_ip_salt_env      = "RUSTCDC_AUDIT_IP_SALT_HEX"
```

Generate an Ed25519 signing key:

```bash
# Generate a 32-byte key and encode as hex
openssl rand -hex 32
# Export the result as RUSTCDC_AUDIT_SIGNING_KEY_HEX
```

For forensic IP correlation across sessions, set a stable salt:

```bash
openssl rand -hex 16
# Export as RUSTCDC_AUDIT_IP_SALT_HEX
```

When `audit_ip_salt_env` is unset, a random salt is generated at startup —
pseudonyms are not correlatable across restarts.

**Verify signing is actually on.** Both settings degrade to "no signature" /
"ephemeral salt" rather than refusing to start, because an audit-trail
misconfiguration should not take a running pipeline down. That makes the failure
quiet by design, so check for it: an unset or malformed
`audit_signing_key_env` variable logs at `warn` on startup, and exports carry no
signature. Grep the first seconds of the log after any change to these settings —
an unsigned audit trail found during an incident is an audit trail you do not have.

### Admin API hardening

```toml
[admin]
# Bind to localhost only (default); expose externally via ingress/proxy
bind = "127.0.0.1:8080"

# TLS between ingress and rustcdc
[admin.tls]
cert_file = "/etc/rustcdc/tls/server.pem"
key_file  = "/etc/rustcdc/tls/server-key.pem"

# mTLS (require client cert)
ca_file = "/etc/rustcdc/tls/ca.pem"

# Rate limits
metrics_rate_limit_rps   = 10
metrics_rate_limit_burst = 20
```

### Token rotation

Use the token manifest for zero-downtime token rotation:

```toml
[admin]
token_manifest_file                    = "/etc/rustcdc/tokens.json"
token_manifest_trusted_public_keys_hex = ["<ed25519-pubkey-hex>"]
token_manifest_refresh_ms              = 30000       # poll interval
token_manifest_max_staleness_ms        = 120000      # fail closed after 2 min stale
```


## 11. Kubernetes deployment

### Minimal Deployment + Service

The admin API must be reachable by the kubelet (pod IP, not loopback), and the
server only allows a non-loopback bind with **tokens + TLS** — so the ConfigMap
below binds `0.0.0.0:8080` with `[admin.tls]`, the probes use `scheme: HTTPS`,
and `/readyz` sends the read token (`/livez` needs no auth). Mount the TLS
key pair from a Secret (e.g. cert-manager).

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rustcdc
spec:
  replicas: 1
  # `Recreate`, not the default `RollingUpdate` — this is load-bearing, not tidiness.
  #
  # A Deployment with replicas: 1 and no strategy gets maxSurge 25% (→ 1) and
  # maxUnavailable 25% (→ 0). Kubernetes therefore starts the new pod and waits for it
  # to become Ready *before* terminating the old one, so every `kubectl rollout restart`
  # runs two instances against one pipeline's state. With a remote state backend they
  # both write checkpoints and the durable position can move backwards; with local_fs
  # the owner lease correctly refuses the second pod, which never becomes Ready, and the
  # rollout deadlocks because maxUnavailable is 0.
  #
  # `Recreate` terminates the old pod first. That means a visible gap in capture during
  # the rollout — bounded by terminationGracePeriodSeconds plus startup. For a
  # single-writer system that gap is the correct trade, and the source retains the
  # changes: the pipeline resumes from its checkpoint.
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: rustcdc
  template:
    metadata:
      labels:
        app: rustcdc
    spec:
      terminationGracePeriodSeconds: 90
      containers:
        - name: rustcdc
          image: ghcr.io/hupe1980/rustcdc-server:latest
          args: ["run", "--config-file", "/etc/rustcdc/config.toml"]
          ports:
            - containerPort: 8080
              name: admin
          env:
            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: rustcdc-secrets
                  key: postgres-password
            - name: RUSTCDC_WRITE_TOKEN
              valueFrom:
                secretKeyRef:
                  name: rustcdc-secrets
                  key: write-token
            - name: RUSTCDC_READ_TOKEN
              valueFrom:
                secretKeyRef:
                  name: rustcdc-secrets
                  key: read-token
          volumeMounts:
            - name: config
              mountPath: /etc/rustcdc
              readOnly: true
            - name: admin-tls
              mountPath: /etc/rustcdc/tls
              readOnly: true
            - name: state
              mountPath: /var/lib/rustcdc/state
          livenessProbe:
            httpGet:
              path: /livez
              port: 8080
              scheme: HTTPS   # kubelet skips certificate verification
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8080
              scheme: HTTPS
              httpHeaders:
                - name: Authorization
                  # /readyz requires the read token (probe_auth_mode default).
                  # Kubelet probes cannot read Secrets into headers — either
                  # template this value in via your deploy tooling, or use an
                  # exec probe with curl reading $RUSTCDC_READ_TOKEN.
                  value: "Bearer <read-token>"
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 3
          resources:
            requests:
              cpu:    "250m"
              memory: "256Mi"
            limits:
              cpu:    "1000m"
              memory: "1Gi"
      volumes:
        - name: config
          configMap:
            name: rustcdc-config
        - name: admin-tls
          secret:
            secretName: rustcdc-admin-tls   # tls.crt / tls.key (e.g. cert-manager)
        - name: state
          persistentVolumeClaim:
            claimName: rustcdc-state
---
apiVersion: v1
kind: Service
metadata:
  name: rustcdc
spec:
  selector:
    app: rustcdc
  ports:
    - port: 8080
      name: admin
```

### PersistentVolumeClaim (for `local_fs` state)

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: rustcdc-state
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 5Gi
```

> For production, prefer `kafka_topic` or `postgres` state backends over
> `local_fs` so the state survives pod rescheduling without a PVC.
>
> **Whichever you choose, `strategy: Recreate` above is mandatory.** The remote
> backends hold an owner lease that refuses a second concurrent writer, and a rolling
> update would make the new pod fail its lease acquisition and crash-loop until the old
> pod exits — which, with `maxUnavailable: 0`, it never does.

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: rustcdc-config
data:
  config.toml: |
    api_version = "v1"

    [source.postgres]
    host        = "postgres.default.svc.cluster.local"
    port        = 5432
    user        = "cdc_user"
    password    = { env = "POSTGRES_PASSWORD" }
    database    = "mydb"
    publication_name      = "cdc_pub"
    replication_slot_name = "cdc_slot"
    # Production posture: the slot is provisioned out of band (default false).
    create_replication_slot_if_missing = false
    conn_timeout_secs       = 10
    stream_poll_interval_ms = 100
    max_events_per_poll     = 1000
    # Name the tables. An EMPTY include list does not mean "capture nothing" — it means
    # capture EVERY table in the database, including ones added later, and every column
    # in them. Enumerate what you intend to publish.
    table_include_list = ["public.orders", "public.customers"]
    table_exclude_list = []

    [source.postgres.transport]
    mode = "plaintext"

    [sink]
    type    = "kafka"
    brokers = "kafka.default.svc.cluster.local:9092"
    topic   = "cdc.events"

    [state]
    dir = "/var/lib/rustcdc/state"

    [admin]
    # Non-loopback bind so the kubelet and Service can reach the admin API.
    # This requires tokens + TLS (enforced at config load).
    bind            = "0.0.0.0:8080"
    read_token_env  = "RUSTCDC_READ_TOKEN"
    write_token_env = "RUSTCDC_WRITE_TOKEN"
    notification_log_file = "/var/lib/rustcdc/state/admin-notifications.jsonl"

    [admin.tls]
    cert_file = "/etc/rustcdc/tls/tls.crt"
    key_file  = "/etc/rustcdc/tls/tls.key"
```


## 12. Upgrading

### Patch / minor version upgrades

1. Update the image tag in the Deployment.
2. `kubectl rollout restart deployment/rustcdc` (triggers graceful shutdown).
3. The connector resumes from the last checkpoint automatically.

With `strategy: Recreate` (see the manifest above — it is required, not optional) the
old pod is fully terminated before the new one starts. Capture pauses for roughly
`terminationGracePeriodSeconds` plus startup time; the source retains the changes and
the new pod resumes from the checkpoint. Watch `rustcdc_slo_checkpoint_age_seconds`
return to baseline to confirm the resume.

### State format changes

If a new version introduces an incompatible state format, the release notes will
say so explicitly. In that case:

1. Run `rustcdc migrate-state` with the old binary to export state.
2. Update the binary.
3. Import state with the new binary.

### Replication slot recreation

If a new version requires a fresh replication slot (rare; only after major
connector reworks):

1. Stop the pipeline.
2. Drop the existing slot: `SELECT pg_drop_replication_slot('cdc_slot');`
3. Re-create it: `SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');`
   (or start once with `create_replication_slot_if_missing = true`, then revert).
4. Start the pipeline. A full snapshot will run.
