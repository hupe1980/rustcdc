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
  snapshot             Backfill tables on a running instance via the admin API
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

### `rustcdc snapshot`

```
rustcdc snapshot <SCHEMA.TABLE>... [OPTIONS]

Options:
  --admin-url <URL>                 Admin API base URL [env: RUSTCDC_ADMIN_URL]
  --signal-id <ID>                  Correlate with an incident or change id
  --message <TEXT>                  Note recorded in the audit trail
  --admin-write-token <TOKEN>       Write bearer token
  --admin-write-token-env <VAR>     Env var containing the write token
  --admin-ca-file <FILE>            Custom CA for TLS
  --admin-client-cert-file <FILE>   Client cert for mTLS
  --admin-client-key-file <FILE>    Client key for mTLS
```

Requires `[incremental_snapshot]` on the running instance. Returns as soon as the signal
is accepted — see [§6b](#6b-backfill-a-table-without-a-restart).

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
curl http://localhost:8080/livez     # 200 = alive; 503 = restart this pod

# Is the pipeline ready for traffic?
# Unauthenticated only with probe_auth_mode = "allow_unauthenticated_loopback"
# (requires a loopback bind); the default require_read_token needs the bearer header.
curl http://localhost:8080/readyz    # 200 = ready; 503 = initialising, degraded or stalled

# Full status JSON
curl -H "Authorization: Bearer $RUSTCDC_READ_TOKEN" http://localhost:8080/status

# The configuration this instance is actually running, credentials redacted
curl -H "Authorization: Bearer $RUSTCDC_READ_TOKEN" http://localhost:8080/config

# Prometheus metrics
curl -H "Authorization: Bearer $RUSTCDC_READ_TOKEN" http://localhost:8080/metrics

# The API's own contract — unauthenticated, and the right place to start
curl http://localhost:8080/openapi.json
```

### What the probes actually decide

The three answer different questions, and the difference is the whole design:

| Probe | Question | 503 means |
|---|---|---|
| `/healthz` | Is the HTTP server up? | Nothing useful; it is a smoke test |
| `/livez` | Should this process be **restarted**? | `error`, `source-degraded-timeout` or `poll-loop-stalled` |
| `/readyz` | Should this replica take **traffic**, and should a rollout proceed? | `not ready`, `source-degraded` or `stalled:<cause>` |

Both probes read the runtime's health verdict. That matters for one failure in
particular: **a poll blocked inside the source produces no error.** A TCP connection that
was accepted and then went silent, a database that stopped answering mid-query, a future
that never completes — nothing fails, nothing changes state, and the two error-driven
conditions stay quiet. Before the verdict was wired in, `/livez` answered `200 alive` for
the life of such a process. The pipeline was dead and every signal Kubernetes had said it
was fine.

**Liveness restarts on one stall cause only, and the exclusions are deliberate:**

| `StallCause` | `/livez` | `/readyz` | Why |
|---|---|---|---|
| `poll_loop_not_turning` | **503** after `LIVEZ_STALL_TIMEOUT` (120 s) | 503 | The process is wedged; a restart is the remedy |
| `unconfirmed_source_position` | 200 | 503 | A restart replays from the same checkpoint and fails identically, while the source keeps retaining log — crash-looping makes a full `pg_wal` volume arrive *sooner*. Page, do not reboot |
| `consumer_not_acknowledging` | 200 | 503 | The sink is not draining; restarting thrashes against an already-unhealthy downstream |

Readiness excludes no cause because it costs nothing — it takes the pod out of rotation
and stops a rollout rather than destroying in-flight work, and a stalled replica that
reports itself ready is how a broken deploy reaches every pod. Its body carries the cause
(`stalled:poll_loop_not_turning`), which is what `kubectl describe` shows you.

**An idle pipeline is ready and alive.** A quiet source is the most common reason for a
pipeline to be producing nothing, and treating it as a fault would take every healthy
deployment out of rotation the moment its database went quiet.

`/livez` waits out `LIVEZ_STALL_TIMEOUT` on top of the runtime's own 30 s threshold, so
with the recommended `failureThreshold: 3` / `periodSeconds: 10` a wedged pod restarts at
roughly three minutes. Restarting is destructive — it drops the in-flight batch and
replays from the last checkpoint — so the delay is intentional.

### Control-plane liveness

`/healthz` and `/livez` answer for the HTTP surface and the pipeline task. Neither answers
for the **signal-action worker** — the single background task that executes every
`execute_snapshot`, `pause_snapshot`, `resume_snapshot` and `stop_snapshot`.

That distinction matters because the failure is quiet. If the worker exits, the pipeline
keeps capturing, `/status` keeps reporting `snapshot_requests_available: true`, and `POST
/signals` keeps answering `STARTED` — while every queued action is silently never executed.

Two metrics make it visible:

| Metric | Meaning |
|---|---|
| `rustcdc_admin_signal_worker_alive` | `0` means the worker is gone. Nothing recovers short of a process restart — alert on it |
| `rustcdc_admin_signal_worker_panics_total` | A signal action panicked and was **recovered**. Not an outage: the worker caught it, released the signal's in-flight guard so it can be retried, and carried on. Treat it as a defect report |

A panicking action used to terminate the worker for the process lifetime. It now unwinds
into the worker's recovery path, which writes the panic message to the audit trail under
`action=signal_action_panicked` with the signal id, increments the counter, and continues.

Both alerts ship in `monitoring/rustcdc_slo_alerts.yml`.

### The OpenAPI document

`GET /openapi.json` serves an OpenAPI 3.1 description of every admin endpoint: the two
token scopes, the signal vocabulary, the request and response shapes, and which status
codes mean what. Point a generator at it and you have a typed client:

```bash
curl -s http://localhost:8080/openapi.json > admin.json
npx @openapitools/openapi-generator-cli generate -i admin.json -g go -o ./client
```

It is unauthenticated by design — it describes the *shape* of the API rather than any of
this instance's state, and requiring a credential to discover how to authenticate is a
loop. It carries no configuration; `/config` is the endpoint for that, and it needs a read
token.

The document is **generated from the same crate as the handlers**, and
`tests/architecture.rs` fails the build if the router serves a path the document omits, or
documents one the router no longer serves. A hand-written spec file would drift a commit
later with nothing to catch it, and a specification nobody verifies is one that lies in the
direction that costs most — a consumer generates a client, it is missing an endpoint or
calls one that 404s, and the mismatch surfaces in their codebase rather than ours.

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
| `rustcdc_runtime_health{verdict="stalled"}` | == 1 | Runtime is running but progress stopped. One-hot gauge: exactly one verdict label is 1. `verdict="idle"` is deliberately not alertable — a quiet database is not an incident, and is a verdict a healthy pipeline reaches whenever its source goes quiet. Logged once at WARN on entry, once at INFO on recovery |
| `rustcdc_runtime_stall_cause{cause=…}` | present | Which signal stalled: `unconfirmed_source_position` (the database is retaining log), `poll_loop_not_turning` (this process or the source socket), `consumer_not_acknowledging` (the sink). Emitted only while stalled, so presence is the condition — route pages on this rather than fanning one alert out to every team |
| `rustcdc_runtime_poll_age_ms` | see runbook | Milliseconds since `poll_event_batch` last returned, empty batches included. Stays low on an idle source; growth means the loop itself has stopped turning |
| `rustcdc_runtime_delivery_age_ms` | **do not alert** | Milliseconds since events last arrived. High on its own is a quiet database. A diagnostic to read beside the poll age, never a condition |
| `rustcdc_runtime_events_skipped_total` | any increase | Events permanently dropped by `transform_error_policy = "skip"` — the checkpoint advances past them, so **any increase is confirmed data loss** |
| `rustcdc_runtime_replication_slot_lag_bytes` | > 1 GiB sustained | PostgreSQL slot WAL retention; growth risks slot invalidation (`max_slot_wal_keep_size`) or a full WAL volume |
| `rustcdc_slo_checkpoint_age_seconds` | > 300 s | Checkpoint has not advanced — sink or source issue |
| `rustcdc_source_consecutive_poll_errors` | > 0 | Source connection degraded |
| `rustcdc_runtime_recoverable_breaker_open_total` | increasing | Circuit breaker firing repeatedly |
| `rustcdc_slo_readiness_ready_total / rustcdc_slo_readiness_checks_total` | < 0.99 | Readiness probe failures |
| `rustcdc_audit_log_drop_total` | > 0 | Audit log queue saturated |
| `rustcdc_snapshot_requests_refused_total` | any increase | An `execute_snapshot` request never reached the pipeline. The request is asynchronous, so the caller was answered `STARTED` and only the notification stream carries the refusal — without this metric a backfill that never ran looks identical to one that did |
| `rustcdc_snapshot_tables_enqueued_total` | — | Tables accepted for on-demand backfill. Not the request count: one request carries many tables, and a table already in progress is a no-op the runtime does not re-enqueue |
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
[`monitoring/rustcdc_slo_alerts.yml`](https://github.com/hupe1980/rustcdc/blob/main/monitoring/rustcdc_slo_alerts.yml).


## 3. Graceful shutdown

rustcdc handles `SIGTERM` and `SIGINT` gracefully:

1. Stops accepting new events from the source
2. Flushes the current in-flight batch to the sink
3. Advances the checkpoint
4. Releases the owner lease, so a replacement starts immediately instead of waiting
   out the lease TTL
5. Exits with code 0

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


## 6b. Backfill a table without a restart

A table added to the publication after the pipeline started has no history
downstream. `execute_snapshot` reads it into the live stream — no restart, and the
stream is never paused:

```bash
rustcdc snapshot public.invoices \
  --admin-url https://localhost:8080 \
  --admin-write-token-env RUSTCDC_WRITE_TOKEN
```

`rustcdc snapshot` takes the same `--admin-*` TLS and token flags as `rustcdc status`,
validates the table names before sending, and prints the `signal_id` to follow. Several
tables in one request are applied atomically — one bad name fails the whole call rather
than half-applying it:

```bash
rustcdc snapshot public.invoices public.invoice_lines \
  --message "backfill for INC-4821" --signal-id INC-4821 \
  --admin-write-token-env RUSTCDC_WRITE_TOKEN
```

The equivalent HTTP call, if you would rather not shell out to the binary:

```bash
curl -X POST https://localhost:8080/signals \
  -H "Authorization: Bearer $RUSTCDC_WRITE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"action_type":"execute_snapshot","tables":["public.invoices"]}'
```

Requires `[incremental_snapshot]` in the config; an empty `tables` list is enough,
and is what `rustcdc init --profile prod` writes. Without it the signal is refused
with a terminal state of `ABORTED` naming the missing section — it does not silently
report success.

The call answers `STARTED` immediately. Watch the outcome:

```bash
curl -N -H "Authorization: Bearer $RUSTCDC_READ_TOKEN" \
  https://localhost:8080/notifications/stream
```

The terminal notification carries `tables_enqueued` — the number of tables the
runtime actually took. Expect a delay of up to `runtime.max_poll_wait_ms` before it
arrives: the pipeline services requests between polls rather than interrupting one,
because cancelling a poll mid-transform would drop the events already taken from the
source buffer.

| Situation | Result |
|---|---|
| Table not tracked | Added, read from the start |
| Table in progress | No-op — retrying a request is safe |
| Table already complete | Rewound and read again |
| Any name unknown or without a primary key | The **whole request** fails; nothing is mutated |

Requests are durable: an enqueued table reaches the checkpoint with the next commit
and resumes after a restart.

A backfill in flight can be steered without stopping the pipeline:

| Signal | Effect |
|---|---|
| `pause_snapshot` | Suspends chunk reading. The live stream keeps flowing. Reports whether it changed anything, so a redundant call is distinguishable |
| `resume_snapshot` | Resumes chunk reading, same reporting |
| `stop_snapshot` | Abandons the remaining tables, reporting how many it dropped |

The pause flag is part of the snapshot state carried in the checkpoint, so a paused
backfill stays paused across a restart. Progress is visible throughout under
`.incremental_snapshot` on `/status` and as `rustcdc_incremental_snapshot_*` on `/metrics`.

Use it also to rebuild a downstream store, or to re-run history through a corrected
transform.


## 6c. Running the integration suites

Capture is verified end to end against a real PostgreSQL **and a real MySQL**. Each suite
manages its own container, so a local run and CI are the same command against the same
fixture:

```bash
RUSTCDC_INTEGRATION=1 cargo test --all-features --test integration_postgres -- --test-threads=1
RUSTCDC_INTEGRATION=1 cargo test --all-features --test integration_mysql    -- --test-threads=1
```

`--test-threads=1` is required: each suite binds a fixed host port and a fixed container
name, deliberately, so a killed run leaves exactly one thing to clean up.

Requires a working Docker daemon. Without `RUSTCDC_INTEGRATION=1` every test returns
immediately — and `tests/architecture.rs::every_integration_suite_is_run_by_ci` enumerates
`tests/integration_*.rs` from disk and asserts each one is named in the workflow. A suite
that CI does not run is not merely unrun: it is env-gated, so it *reports success*. Adding
a suite and forgetting to wire it up fails the unit suite.

What it covers:

| Test | Property |
|---|---|
| `insert_update_and_delete_are_captured_in_order` | Commit order and full before-images, across **both** `wal_transport` values |
| `the_resume_position_does_not_lose_events` | Rows committed while the pipeline is down are captured after a restart, in order, with a bounded replay window |
| `a_tls_transport_refuses_a_server_without_tls` | `mode = "tls"` fails against `ssl = off` rather than silently downgrading |
| `a_replica_identity_full_table_reports_only_its_real_primary_key` | The key is the table's key, not every column — the Kafka message key is derived from it |
| `an_on_demand_snapshot_backfills_rows_the_stream_could_never_deliver` | `execute_snapshot` reaches a live runtime and produces rows |
| `a_startup_backfill_reads_only_the_rows_its_filter_selects` | `table_conditions` is actually applied |

And for MySQL:

| Test | Property |
|---|---|
| `insert_update_and_delete_are_captured_in_order` | Commit order, and a `DELETE` carrying a usable before-image |
| `the_primary_key_is_the_declared_key_not_every_column` | `binlog_row_metadata=FULL` sends every column; the key must not widen to match |
| `the_resume_position_does_not_lose_events` | MySQL has no server-side slot, so this exercises **this project's** checkpoint rather than the server's bookkeeping |
| `a_clean_restart_redelivers_nothing` | Exact zero replay across a clean restart |
| `capture_works_without_gtid` | The file+position resume path, for servers with GTID off |

> **MySQL needs two binlog settings, not one.** `binlog_row_image=FULL` decides which
> *columns* are logged; `binlog_row_metadata=FULL` decides whether column names and
> primary-key flags travel with them. MySQL 8 defaults the latter to `MINIMAL` and MariaDB
> to `NO_LOG`, under which events carry positional placeholders (`@0`, `@1`, …) and no
> primary key — so the connector refuses to start rather than emit that. The error names
> the setting and the fix. This fixture was written with only the first setting and hit it
> on its first run.

The resume tests assert **no gaps and no replay**, both exactly.

They did not always. This suite found a real defect on its first run: a *clean* shutdown
redelivered the last transaction on every restart, deterministically. The tests were
written to assert "no gaps exactly, duplicates within a bound" — which was the honest
assertion while the defect stood, since at-least-once is the contract and asserting "no
duplicates" would have been asserting exactly-once.

The diagnosis we reported upstream was wrong, and the correction is worth recording:
we said `START_REPLICATION` was inclusive and suggested resuming at `lsn + 1`. PostgreSQL
logical decoding filters at *transaction* granularity, and a change's own LSN always
precedes its transaction's commit record, so either position replays the whole transaction.
rustcdc 0.11 fixed it properly with `StreamHandle::resume_offset_for`, and the bound was
tightened to zero.

If a run is interrupted, clean up with
`docker rm -f rustcdc-integration-postgres rustcdc-integration-mysql`.


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

### Measured baselines

Throughput is measured, not asserted. `benches/throughput.rs` drives the real batch path;
`benches/BASELINES.md` carries the numbers and what they mean.

```bash
cargo bench --bench throughput -- --save-baseline main
# …change something…
cargo bench --bench throughput -- --baseline main
```

The figures are hardware-specific and the full-pipeline one is fsync-dominated, so the
number that matters is the **ratio to a baseline taken on the same machine**, not the
absolute value. Two results worth knowing before you tune anything:

* the transform stage costs 0.42-0.92 µs/event, and full batch delivery ~47.6 µs/event — so
  **delivery dominates by ~100×**;
* consequently `prepare_parallelism` moves nothing for a JSON codec and a durable sink. See
  [when it actually helps](@/docs/configuration.md#when-prepare-parallelism-actually-helps).

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

### What `/status` redaction actually covers

The configuration snapshot on `/config` is readable by any **read**-scoped token, so
redaction is a boundary, not a courtesy. Three independent rules apply, deliberately — a secret that
slips past one is usually caught by another:

| Rule | Catches |
|---|---|
| **Named paths** | An enumerated list (`source.password`, `state.backend.postgres.url`, …) redacted whole, because a credential is not always in the userinfo |
| **Key names** | Any key containing `password`, `secret`, `token`, `credential`, `api_key`, `authorization`, `private_key`, `key_file` — at any depth. Separators are normalised, so `x-api-key` and `client.secret` match the same tokens as `api_key` |
| **URL values** | Any string that parses as `scheme://…`, under *any* key: userinfo is replaced, and query parameters whose **name** looks secret (`?api_key=`, `?token=`) have their values replaced |

The URL rule is value-driven on purpose. `sink.http.url` was named like nothing sensitive
and was on no list, so a webhook URL carrying its credential came back verbatim. The
loader now also rejects userinfo in that field outright, which leaves the query string as
the only way a credential can reach it — hence the second half of the rule.

A property test in `tests/fuzz_properties.rs` generates key and parameter names rather
than listing them, which is what found the hyphenated spelling the enumerated tests all
missed.

### Control-plane panic guard

Every admin handler runs behind a panic guard: a panic becomes a logged `500` rather than
an aborted task, which is what a client would otherwise see as a bare connection reset
with no status, no body and nothing in the log. The panic payload is logged but never
returned — it can carry file paths and internal state.

This is defence in depth, not a licence to panic. It exists because a panic *was* reachable
from outside: the audit-trail detail was capped with a byte slice, which aborted the task
whenever the budget landed inside a multi-byte character. Any write-scope token could
trigger it, and doing so killed the signal-action worker for the life of the process —
after which every asynchronous signal was accepted, answered `STARTED`, and never run.

### Transport encryption

`transport.mode = "tls"` is enforced on **every** connection the server opens to the
source, including the replication-slot lag sampler behind
`rustcdc_runtime_replication_slot_lag_bytes`.

That sampler used to connect with TLS disabled regardless of the configured
transport, so a TLS-configured PostgreSQL deployment put its replication password on
the wire in the clear once every 15 seconds for the life of the process, with nothing
in the logs to say so. It now builds its TLS client from the same
`ca_cert_path` / `client_cert_path` / `client_key_path` as the capture connection, and
a transport it cannot honour fails the sample rather than downgrading it.

The connector itself no longer accepts `sslmode=prefer` semantics either: a TLS
transport against a server with `ssl = off` fails to connect. If you need plaintext,
say so — the configuration is the audit record.

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

**Only name a proxy in `trusted_proxy_ips` if the admin listener is genuinely behind it.**
That list is what enables `X-Forwarded-For` parsing; while it is empty (the default) no
request header can influence the rate-limit key at all. When it is populated, the header is
read right to left past known proxies — see
[Admin API](@/docs/configuration.md#7-admin-api-admin) in the configuration reference for
why the direction matters.

### Sink delivery metrics

The `rustcdc_sink_*` and `rustcdc_iceberg_*` families report the transport's own view of
delivery — HTTP status classes, batch-size and retry-delay histograms, pending bytes,
Iceberg orphaned files and flush-lock contention, Kafka OAUTHBEARER token health. They are
sampled once per batch from the sink bindings directly, not through the routing layer,
because the router's generic adapter interface has room for four counters and these are
thirty. With `[[pipeline.routes]]` configured they are the **sum across every route**, so a
per-sink breakdown needs one process per sink.

Four of these back shipped alert rules — `CDCIcebergOrphanedFiles`,
`RUSTCDCHttpBatchOldestEventAgeHigh`, `RUSTCDCKafkaOAuthTokenFetchFailing` and
`RUSTCDCKafkaOAuthTokenNearExpiry` — so a dashboard showing them flat at zero under real
traffic is a bug report, not a healthy pipeline.

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
> **Whichever you choose, `strategy: Recreate` above is mandatory.** *Every* backend now
> holds an owner lease that refuses a second concurrent writer, and a rolling update would
> make the new pod fail its lease acquisition and crash-loop until the old pod exits —
> which, with `maxUnavailable: 0`, it never does.

### State ownership

One process owns a pipeline's state. Every backend enforces that, and each enforces it
with whatever its store makes available:

| Backend | Fenced by |
|---|---|
| `local_fs` | An owner+epoch lease in `<state.dir>/owner_lease.json`, re-asserted before durable writes |
| `redis`, `postgresql` | The same lease, held as a key in the remote store |
| `kafka_topic` | The broker, by transactional id — a second producer with the same id fences the first |

A second instance starting against a live lease refuses to run and names the holder. A
crashed owner's lease expires after 60 s, so recovery needs no manual step, and an owner
whose lease is stolen while it was partitioned discovers this on its next heartbeat and
stops writing rather than continuing blind.

`local_fs` gets one refinement the others cannot: if the recorded owner is a process **on
this host** whose PID is no longer running, the lease is taken over immediately rather than
after the TTL. That is the ordinary crash-and-restart case — systemd or a container runtime
bringing the process back seconds later — and waiting a minute for a directory nobody holds
would be worse than the problem the lease solves. A lease from another host is never
second-guessed this way: a foreign PID means nothing locally, and treating it as stale is
precisely how two hosts came to share one NFS state directory.

**What this does not do.** Acquisition is read-then-write, not compare-and-swap, so two
instances starting inside the same millisecond-scale window can both see a free slot. It
converts the common silent interleave — two writers, last-write-wins, the durable position
sliding backwards — into a loud refusal. It is not a substitute for a store with real CAS,
and on a filesystem that does not honour write visibility across hosts it guarantees
nothing. `strategy: Recreate` and `ReadWriteOnce` remain the right things to configure;
they are simply no longer the *only* thing standing between you and a corrupted checkpoint.

Deleting `owner_lease.json` by hand forces a takeover. Do that only when you are certain no
other process is running, because it is exactly the safety this file provides.

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
