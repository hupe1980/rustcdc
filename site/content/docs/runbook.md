+++
title = "Operations runbook"
description = "Alert thresholds, disaster recovery, secret rotation and per-connector maintenance for rustcdc."
weight = 100
+++

**Audience:** Platform operators and SREs managing rustcdc in production  
**Version:** Current  
**Last Updated:** May 25, 2026

---

## Table of Contents

1. [PostgreSQL Source Management](#postgresql-source-management)
2. [MySQL Source Management](#mysql-source-management)
3. [SQL Server Source Management](#sql-server-source-management)
4. [Metric Alerting and Monitoring](#metric-alerting-and-monitoring)
5. [Troubleshooting Common Failures](#troubleshooting-common-failures)
6. [Secret Rotation](#secret-rotation)
7. [Disaster Recovery](#disaster-recovery)

---

## Integration Scaffolding Assumptions

This runbook assumes rustcdc is embedded into an application/runtime wrapper that provides:

- A service manager command for start/stop/restart (examples use `systemctl`)
- A metrics endpoint path and port (examples use `http://localhost:9090/metrics`)
- A deployment-specific checkpoint storage path (examples use `/var/rustcdc/...`)

Replace these placeholders with your environment equivalents:

- Service manager: `systemctl` or `docker compose` or Kubernetes rollout/exec commands
- Metrics endpoint: your runtime/admin endpoint bound by the embedder
- Checkpoint path: your configured persistent volume or mount path

If your deployment does not provide these wrappers, see [Deployment](@/docs/deployment.md) first and wire health/metrics/service controls before applying this runbook verbatim.

> **rustcdc ships no binary.** There is no `[[bin]]` target and no `src/main.rs`; `systemctl stop
> rustcdc` and `curl localhost:9090/metrics` refer to **your** wrapper, not to anything this crate
> installs. In particular:
>
> - **Nothing flushes on SIGTERM unless you implement it.** Graceful drain is
>   `CdcRuntime::drain_and_stop()` — which returns the drained events and commits them, so **you
>   must consume the returned `Vec<Event>`**; dropping it is unrecoverable data loss. Use
>   `stop()` (refuses while events are uncommitted) or `force_stop()` (discards explicitly, returns
>   them for replay) if you do not intend to process them.
> - **No HTTP server is provided.** The crate exposes `admin_snapshot_json()` and a Prometheus text
>   *renderer*; binding a port is the embedder's job.
> - **Restart does not drain automatically.** Any "will drain pending events before restart"
>   behaviour is a property of your supervisor.

---

## PostgreSQL Source Management

> **The default WAL transport holds the replication slot.** `WalTransport::StreamingReplication`
> keeps a walsender attached for the life of the stream, and PostgreSQL refuses
> `pg_replication_slot_advance` and `pg_drop_replication_slot` on an active slot
> (`SQLSTATE 55006`, *"replication slot is active for PID N"*). Every slot procedure below
> therefore stops the pipeline first — that ordering is load-bearing, not tidiness. The server
> reaps the walsender a moment after the socket closes, so retry briefly if the first attempt
> still reports the slot active. `SELECT slot_name, active, active_pid FROM
> pg_replication_slots;` shows the holder.
>
> The connecting role also needs the **`REPLICATION`** attribute and a direct connection; a
> pooler in transaction-pooling mode cannot carry a replication stream. Where neither can be
> arranged, `WalTransport::SqlPeek` reads the same slot over an ordinary connection — see
> [`wal_transport`](@/docs/config-reference.md#wal-transport) for what that costs.


### Replication Slot Setup

**Prerequisites:**
- PostgreSQL 10+ (recommended 16+)
- Logical replication enabled: `wal_level = logical` in postgresql.conf
- Sufficient WAL retention (at least 1GB, preferably 10GB+)

**Initial Setup:**

```bash
# On PostgreSQL server
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD '<provision-from-secret-manager>';
GRANT CONNECT ON DATABASE your_database TO cdc_user;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
```

**rustcdc Connector Fields (PostgreSQL):**

- `host`, `port`, `user`, `password`, `database`
- `replication_slot_name`, `publication_name`
- `conn_timeout_secs`
- `stream_poll_interval_ms` (poll cadence; lower for latency, higher for throughput batching)
- `max_events_per_poll` (per-poll event budget)
- transport selection: `transport = TransportConfig::tls()` (default with `tls` feature) or `TransportConfig::tls_with_ca_cert_path(...)`

### Replication Slot Lifecycle

**Creation:**
- rustcdc automatically creates a replication slot on first `start_stream()` call
- Slot name: taken from `PostgresSourceConfig.replication_slot_name`
- Slot is logical replication type (pgoutput plugin)

**Monitoring Slot Health:**

```sql
-- Check slot status
SELECT slot_name, slot_type, active, restart_lsn, confirmed_flush_lsn 
FROM pg_replication_slots;

-- Check lag in bytes
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
FROM pg_replication_slots WHERE slot_name = 'your_slot_name';
```

### Replication Slot Divergence Recovery

**Symptom:** Error message similar to:
```text
ERROR: source error: postgres checkpoint/slot divergence for slot '...'
```

**Root Causes:**
1. **Slot was dropped manually** → Operator accidentally dropped the slot
2. **WAL was pruned** → checkpoint_lsn is older than current oldest WAL available
3. **Slot became inactive** → rustcdc didn't consume for >24 hours (typical WAL retention)

**Recovery Steps:**

**Option A: Manual Slot Recovery (Recommended)**

```bash
# 1. Stop rustcdc instance gracefully
systemctl stop rustcdc
# or send SIGTERM to the process

# 2. Verify checkpoint is readable
cat /var/rustcdc/checkpoint_postgres.json
# Should be valid JSON and contain postgres offset state

# If the checkpoint/slot alignment no longer matches, reset the pair together
# instead of forcing a resume attempt.

# 3. Check current WAL position on PostgreSQL
psql -U cdc_user -d your_database -c "SELECT pg_current_wal_lsn();"

# 4. If checkpoint LSN is older than current WAL minus retention:
#    a) Create a replacement checkpoint using the runtime file format envelope:
CURRENT_LSN_HEX=$(psql -U cdc_user -d your_database -Atc "
SELECT
  (('x' || split_part(pg_current_wal_lsn()::text, '/', 1))::bit(32)::bigint * 4294967296) +
  (('x' || split_part(pg_current_wal_lsn()::text, '/', 2))::bit(32)::bigint);
")

#    Seed a replacement checkpoint. Checkpoint files carry an integrity checksum, so
#    they cannot be written correctly by hand — use the bundled tool, which also writes
#    atomically, applies the required 0600 mode, and fsyncs the directory.
#
#    Do this only while the connector is STOPPED. Seeding a position AHEAD of what was
#    actually delivered downstream skips every event in between, permanently. When in
#    doubt seed behind: the delivery contract is at-least-once, so downstream must
#    already tolerate duplicates.
cargo run --example seed_checkpoint --features postgres -- \
  --dir /var/rustcdc \
  --source-type postgres \
  --committed-event-count 0 \
  --offset "{\"lsn\": $CURRENT_LSN_HEX, \"slot_name\": \"rustcdc_postgres_new\"}"

# Confirm the runtime will accept it before restarting the service. A checkpoint that
# fails its integrity check is rejected at load, so verify now rather than at startup.
jq -e '.checkpoint_format_version == 1 and .content_checksum != null' \
  /var/rustcdc/checkpoint_postgres.json

#    b) Optionally create new replication slot on PostgreSQL
psql -U cdc_user -d your_database -c "SELECT * FROM pg_create_logical_replication_slot('rustcdc_postgres_new', 'pgoutput');"

# 5. Restart rustcdc
systemctl start rustcdc

# 6. Verify slot is active and consuming
psql -U cdc_user -d your_database -c "SELECT slot_name, active, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'rustcdc_postgres_new';"
```

**Option B: Force Reset (Data Loss Risk)**

⚠️ **WARNING:** This discards uncommitted events and may cause data loss if not coordinated with downstream systems.

Before executing force reset, record and confirm this checklist in the incident/change ticket:

- Change ticket created (for example `INC-12345`) with operator + reviewer names.
- Current checkpoint/offset snapshot archived.
- Downstream consumers paused or explicitly verified dedup-safe.
- Replication-slot/binlog retention and catch-up impact reviewed.
- Rollback plan prepared and on-call ownership confirmed.

```bash
# 1. Stop rustcdc
systemctl stop rustcdc

# 2. Drop old slot
psql -U cdc_user -d your_database -c "SELECT pg_drop_replication_slot('rustcdc_postgres_old');"

# 3. Delete ALL checkpoint files for this source family to force a fresh start.
#    Deleting only `checkpoint_postgres.json` leaves `checkpoint_postgres_snapshot.json`
#    behind, and `load()` picks the record with the highest committed count across the
#    directory — so the snapshot checkpoint is resumed and the outcome is not "fresh".
rm -f /var/rustcdc/checkpoint_postgres.json \
      /var/rustcdc/checkpoint_postgres_snapshot.json

# 4. Restart rustcdc (will start fresh from current WAL position)
systemctl start rustcdc
```

### Preventive Maintenance

**Daily Checks:**

```bash
#!/bin/bash
# Check runtime lag every hour (milliseconds)
LAG_MS=$(curl -s http://localhost:9090/metrics | awk '/^rustcdc_runtime_replication_lag_ms / {print $2; exit}')

if [ -n "$LAG_MS" ] && [ "$LAG_MS" -gt 30000 ]; then  # 30 seconds
  echo "WARNING: rustcdc replication lag exceeds 30s" | mail -s "rustcdc Alert" ops@company.com
fi
```

---

## MySQL Source Management

### Binlog Configuration

**Prerequisites:**
- MySQL 8.0+ (MariaDB 10.5+)
- Binlog enabled: `log_bin = ON` in my.cnf
- GTID enabled (recommended): `gtid_mode = ON`
- Binlog retention: `binlog_expire_logs_auto_purge = 0` (manual management recommended)

**Configuration (my.cnf):**

```ini
[mysqld]
log_bin = /var/log/mysql/mysql-bin
binlog_format = ROW
gtid_mode = ON
enforce_gtid_consistency = ON
log_slave_updates = ON
binlog_expire_logs_auto_purge = 0
# Retention: Keep 7 days of binlogs (adjust per your needs)
# FLUSH BINARY LOGS EVERY 24 HOURS via cron is recommended
```

**User Setup:**

```sql
CREATE USER 'cdc_user'@'%' IDENTIFIED BY '<provision-from-secret-manager>';
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
```

**rustcdc Connector Fields (MySQL):**

- `host`, `port`, `user`, `password`, `database`
- `server_id`, `gtid_mode_enabled`, `binlog_format_check`
- `conn_timeout_secs`
- `stream_poll_interval_ms` (poll cadence; lower for latency, higher for throughput batching)
- `max_events_per_poll` (per-poll event budget)
- transport selection: `transport = TransportConfig::tls()` (default with `tls` feature) or `TransportConfig::tls_with_ca_cert_path(...)`

### Binlog Retention Strategy

**Recommended: Manual Cleanup with Monitoring**

```bash
#!/bin/bash
# Run daily via cron
MYSQL_USER="cdc_user"
MYSQL_HOST="localhost"
MYSQL_CLIENT_CNF="/etc/rustcdc/mysql-client.cnf"  # file contains credentials with 0600 perms

# Get current replication position from rustcdc checkpoint wrapper
CHECKPOINT=$(cat /var/rustcdc/checkpoint_mysql.json | jq -r '.offset.gtid')

# Log checkpoint for audit
echo "$(date): Current checkpoint: $CHECKPOINT" >> /var/log/rustcdc-binlog-retention.log

# Purge binlogs older than 7 days, but preserve current GTID
mysql --defaults-extra-file="$MYSQL_CLIENT_CNF" -h "$MYSQL_HOST" -u "$MYSQL_USER" -e "PURGE BINARY LOGS BEFORE DATE_SUB(NOW(), INTERVAL 7 DAY);"

# Verify retention
mysql --defaults-extra-file="$MYSQL_CLIENT_CNF" -h "$MYSQL_HOST" -u "$MYSQL_USER" -e "SHOW BINARY LOGS;" >> /var/log/rustcdc-binlog-retention.log
```

### GTID Mode Verification

```sql
-- Check GTID status
SHOW VARIABLES LIKE 'gtid_mode';
-- Should output: gtid_mode | ON

-- Check replication position (used by rustcdc)
SHOW MASTER STATUS\G
-- Note: GTID set for checkpoint tracking
```

### MysqlOffset Resume Priority

`MysqlOffset` tracks two parallel position fields: `gtid` (a GTID set string), and `binlog_file` + `binlog_pos` (a traditional file/position pair). Understanding which takes precedence on restart is important for recovery operations.

**Resume order:**

1. **GTID-mode servers** — When the server has `gtid_mode=ON` and the stored `gtid` field is non-empty, the connector resumes using the GTID set. This is the preferred path because GTID positions are server-globally unique and survive binlog rotation without ambiguity.

2. **Non-GTID or empty GTID field** — When `gtid` is empty (GTID mode off, or a legacy checkpoint written before GTID support), the connector falls back to `binlog_file` + `binlog_pos`. This requires the named binlog file to still be present on the server (see [Binlog Retention Strategy](#binlog-retention-strategy)).

**Operational implications:**

- If you migrate a server from non-GTID to GTID mode, existing checkpoints will have an empty `gtid` field. The runtime will use the file/position fallback until at least one new checkpoint is written in GTID mode.
- If binlog files have been purged and the checkpoint references a rotated-away file, restart will fail with a `SourceError` indicating the position is unavailable. Remedy: reset the checkpoint to an empty offset and trigger a fresh snapshot.
- For cross-server failover (primary → replica promotion), GTID-mode checkpoints are portable; file/position checkpoints are not — they are specific to the binlog sequence of the original primary.

---

## SQL Server Source Management

### CDC Setup on SQL Server

**Prerequisites:**
- SQL Server 2016+ (2019 recommended)
- SQL Server Agent running
- Database recovery model: FULL (not SIMPLE)

**Enable CDC on Database:**

```sql
-- Connect as sa or db_owner
USE your_database;
GO

-- Enable CDC on database
EXEC sys.sp_cdc_enable_db;
GO

-- Enable CDC on specific table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'users',
    @role_name = N'cdc_role',
    @supports_net_changes = 0;
GO

-- Verify CDC enabled
SELECT name FROM sys.databases WHERE database_id = DB_ID() AND is_cdc_enabled = 1;
```

**Create CDC User (Recommended):**

```sql
-- Create login
CREATE LOGIN cdc_user WITH PASSWORD = '<provision-from-secret-manager>';

-- Create user in database
USE your_database;
CREATE USER cdc_user FOR LOGIN cdc_user;

-- Grant minimal required permissions
GRANT SELECT ON sys.cdc_lsn_time_mapping TO cdc_user;
GRANT SELECT ON cdc.lsn_time_mapping TO cdc_user;
GRANT SELECT ON cdc.fn_cdc_get_all_changes_dbo_users TO cdc_user;  -- Per table
ALTER ROLE cdc_admin ADD MEMBER cdc_user;  -- Or custom role
```

### LSN Progression Monitoring

```sql
-- Check current LSN
SELECT @@DBTS AS current_lsn;

-- Check change table progress (used by rustcdc)
SELECT TOP (10)
    CAST(start_lsn AS VARCHAR(32)) AS start_lsn,
    CAST(end_lsn AS VARCHAR(32)) AS end_lsn
FROM cdc.lsn_time_mapping
ORDER BY start_lsn DESC;
```

### SQL Server CDC Cleanup

```sql
-- Cleanup old CDC tables (keep last 7 days of LSN)
EXEC sys.sp_cdc_cleanup_change_tables
    @capture_instance = N'dbo_users',
    @low_water_mark = NULL;  -- Use default retention
GO
```

### SQL Server TRUNCATE Capture — DDL Trigger Management

`SqlServerSourceConfig::capture_truncate_events` controls whether `TRUNCATE TABLE` operations are captured as `Operation::Truncate` events. SQL Server's native CDC change tables do **not** record `TRUNCATE TABLE`; capture requires an opt-in DDL trigger that `rustcdc` installs automatically.

#### Required permissions

The connecting user must have the following permissions to install the trigger:

```sql
-- Verify permissions
SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'ALTER') AS can_alter_database;
SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TRIGGER') AS can_create_trigger;
```

If either returns `0`, grant the permissions to the CDC login:

```sql
GRANT ALTER ON DATABASE::[your_database] TO [cdc_login];
GRANT CREATE TRIGGER TO [cdc_login];
```

#### Verify the trigger is installed

After connecting with `capture_truncate_events: true`, verify the trigger exists:

```sql
SELECT
    t.name AS trigger_name,
    te.type_desc AS event_type,
    t.create_date,
    t.modify_date
FROM sys.triggers t
JOIN sys.trigger_events te ON t.object_id = te.object_id
WHERE t.parent_class_desc = 'DATABASE'
  AND t.name LIKE 'rustcdc_%';
```

Expected: one row with `trigger_name = 'rustcdc_truncate_capture'` (or similar) and `event_type = 'ALTER_TABLE'`.

#### Behaviour when trigger is absent

- If the trigger installation fails (insufficient permissions, quota), `connect()` returns an error. No truncate events are captured, and the connector does not start.
- If the trigger is deleted after startup while the connector is running, subsequent `TRUNCATE TABLE` statements are silently missed. No error is surfaced at runtime. Re-connect to reinstall.

#### Cleanup on decommission

When removing a `rustcdc` deployment, drop the DDL trigger to avoid orphaned objects:

```sql
-- List all rustcdc DDL triggers
SELECT name FROM sys.triggers WHERE parent_class_desc = 'DATABASE' AND name LIKE 'rustcdc_%';

-- Drop the truncate capture trigger
DROP TRIGGER IF EXISTS rustcdc_truncate_capture ON DATABASE;
GO
```

Also verify no capture instances remain from the CDC setup:

```sql
SELECT capture_instance, source_schema, source_table
FROM cdc.change_tables
WHERE capture_instance LIKE 'rustcdc_%';
```

Drop them with `sys.sp_cdc_disable_table` if needed.

### SQL Server Connection and Poll Tuning

`SqlServerSourceConfig` now exposes explicit concurrency/throughput controls:

- `prereq_pool_size`
- `stream_poll_interval_ms`
- `max_events_per_poll`

Recommended starting profiles:

| Profile | prereq_pool_size | stream_poll_interval_ms | max_events_per_poll |
|---|---:|---:|---:|
| Low-latency | 4 | 250 | 5000 |
| Balanced | 4-8 | 1000 | 10000-20000 |
| Throughput-heavy | 8-16 | 2000-5000 | 20000-50000 |

Rollout guidance:

1. Change one knob set at a time.
2. Observe `rustcdc_runtime_replication_lag_ms`, checkpoint progression, and source CPU.
3. Revert if lag drops but source CPU or lock contention spikes.

### SQL Server Tail-Latency Watch (p99)

For SQL Server, watch the p99/p95 spread for poll latency in evidence runs.
Large sustained spread indicates burstiness or source-side pressure even when p95 stays low.

Operator policy:

- Warning: p99 > 10x p95 for 3 consecutive evidence runs.
- Escalate: p99 > 50x p95 with user-visible lag growth.

First response actions:

1. Increase `max_events_per_poll` for burst absorption.
2. Increase `stream_poll_interval_ms` modestly (for example, 1000 -> 2000) to reduce poll churn.
3. Validate source indexing and CDC capture table growth on SQL Server.

---

## Structured Log Field Schema

All connector events emitted by `StructuredLogger` use the `tracing` framework and include a consistent set of structured fields. This schema is stable and suitable for log aggregation pipeline alert rules.

### Common fields (present on every log record)

| Field | Type | Description |
|-------|------|-------------|
| `source_type` | `string` | Connector type (`postgres`, `mysql`, `mariadb`, `sqlserver`) |
| `event` | `string` | Event name (see table below) |

### Event names and additional fields

| Event name (`event =`) | Level | Additional fields | Description |
|---|---|---|---|
| `source_connected` | INFO | — | Source database connection established |
| `source_disconnected` | INFO | — | Source database connection closed |
| `insecure_transport` | WARN | `mode`, `details` | TLS verification is disabled |
| `connection_error` | ERROR | `error` | Connection-level error |
| `snapshot_started` | INFO | `table` | Snapshot phase started for table |
| `snapshot_chunk_received` | DEBUG | `table`, `chunk_size` | Snapshot batch received |
| `snapshot_complete` | INFO | `table` | Snapshot phase completed for table |
| `stream_started` | INFO | `offset` | Streaming replication started at offset |
| `stream_events_received` | DEBUG | `table`, `event_count`, `offset` | Batch of stream events received |
| `stream_error` | ERROR | `error` | Streaming-level error |
| `checkpoint_saved` | INFO | `offset`, `committed_count` | Checkpoint durably persisted |
| `checkpoint_loaded` | INFO | `offset`, `committed_count` | Checkpoint loaded on startup |
| `checkpoint_error` | WARN | `error` | Checkpoint operation warning |
| `transform_applied` | DEBUG | `transform`, `table`, `offset` | Transform stage applied to event |
| `transform_error` | WARN | `transform`, `error` | Transform stage returned an error |

> **Note:** `error` fields are sanitized by `sanitize_context()` — DSN credentials and common key=value secrets are redacted before logging. You will see `***redacted***` in place of password/token values.

### Example log-aggregation filter (Loki)

```logql
{app="rustcdc"} | json | event = "checkpoint_saved" | committed_count > 0
```

### Alert rule guidance

- Alert on `event = "source_disconnected"` sustained for > 30s with no `source_connected` following.
- Alert on `event = "stream_error"` rate > 1/min.
- Alert on `event = "checkpoint_error"` — any occurrence warrants investigation.
- Use `committed_count` from `checkpoint_saved` to derive event throughput rate.

---

## Metric Alerting and Monitoring

### Recommended Alert Thresholds

**Critical (Page On-Call):**

| Metric | Threshold | Action |
|--------|-----------|--------|
| **`rustcdc_replication_slot_lag_bytes`** (PostgreSQL) | **Sustained growth over 15 min, or > 25% of free `pg_wal` volume** | **The single most operationally critical PostgreSQL CDC signal.** A slot pins WAL *and* catalog xmin; unbounded growth ends in a full `pg_wal` volume or, in the extreme, a transaction-ID-wraparound shutdown of the **primary**. See [PostgreSQL WAL retention](#postgresql-source-management). Distinguish *idle-nonzero* (normal) from *monotonically growing* (act now) — alert on the derivative, not the level. |
| `rustcdc_runtime_replication_lag_ms` | > 30000 ms | Investigate source/database lag, downstream throughput, and checkpoint commits |
| `rustcdc_runtime_events_committed_total` | No increase for 5 min | Check stream connectivity; may indicate stalled progress |
| `rustcdc_runtime_liveness` | == 0 | Runtime stopped or unhealthy; investigate process and startup logs |
| `rustcdc_runtime_health{verdict="stalled"}` | == 1 | The runtime's own verdict that progress has stopped, with the reason and remedy in the log line. See [Health verdict](#health-verdict-idle-vs-stalled). |
| **`rustcdc_runtime_events_skipped_total`** | **Any increase** | **Data was lost.** `TransformErrorPolicy::Skip` drops the event *and* the checkpoint advances past it, so it is never replayed. Recover it from the dead-letter handler; if none was configured the runtime refuses to start under `Skip`, so one exists. |

**Warning (ticket, not a page):**

| Metric | Threshold | Action |
|--------|-----------|--------|
| `rustcdc_transform_rules_unmatched{kind="mask"}` | Present at all, after real traffic | **A configured column is shipping in clear text.** The rule's path does not match any field: a typo, a column renamed upstream, or a path-mutating transform (`FieldMappingTransform`, `UnwrapTransform`) ordered *before* the mask stage. Nothing errors, so this metric is the only signal. Fix the path or the stage order — do not delete the rule. |
| `rustcdc_transform_rules_unmatched{kind="route"}` | Present at all, after real traffic | Events that rule was meant to route are going to `default_destination`. Routing fails open, so the only symptom is a destination that stays empty. Check the table name against the source, and remember exact-table rules win over patterns. |
| `rustcdc_transform_rules_unmatched{kind="filter"}` | Present at all, after real traffic | A filter predicate has been evaluated and never matched, so it is contributing nothing to the `FilterMode`. Usually a typo in a field path or a value that never occurs. Rules that were never *reached* (short-circuited under `FilterMode::All`) are deliberately not reported. |
| `rustcdc_runtime_idempotency_evictions_total` | Sustained growth | The dedup window is too small for this deployment's replay distance, so duplicates older than the window stop being suppressed. Delivery stays at-least-once, but a sink relying on the guard will start seeing repeats. Raise `IdempotencyOptions::capacity`. |
| `rustcdc_runtime_idempotency_unidentifiable_total` | Growth on a table you expected to be keyed | Events with neither transaction metadata nor a resolvable primary key are deliberately **not** deduplicated — suppressing them could drop distinct rows. Expected for keyless tables; unexpected growth means a primary key is missing or its columns are absent from the row image (check `REPLICA IDENTITY` on PostgreSQL, `binlog_row_image` on MySQL). |
| `rustcdc_runtime_buffer_depth` | At `max_buffer_size` for > 5 min | The embedder is not acknowledging. The runtime is applying backpressure (`ErrorKind::Backpressure`), which is flow control, not a failure — but sustained saturation means the sink cannot keep up. |

### Health verdict: idle vs stalled

`RuntimeState` alone cannot answer whether a connector is *healthy*. It has only
`Idle | Running | Stopping | Stopped`, and `Idle` there means *not yet started*. A connector
streaming from a quiet database and one hung on a dead socket both report `state=running`,
`readiness=true`, and flat counters.

`RuntimeAdminSnapshot::health` resolves that ambiguity. It is a `HealthVerdict`:

| Verdict | Meaning | Alert? |
|---|---|---|
| `Healthy` | The poll loop is turning and committed progress is current. | No |
| `Idle` | The loop is turning, but the source has produced nothing. Normal for a quiet database. | No |
| `Stalled { reason }` | Progress has stopped for a reason the runtime can name. | **Yes** |
| `NotRunning` | The runtime has not been started, or has stopped. | No — but check it was intentional |

`HealthVerdict::is_alertable()` returns `true` for exactly `Stalled`, so an embedder's health
endpoint can gate on it directly. The verdict is derived from three independent signals, checked
in this order, and `reason` names both the condition and the remedy:

1. **Unconfirmed source position** — a checkpoint committed but the source-side confirmation
   (`confirmed_flush_lsn` and equivalents) failed repeatedly. Retention keeps growing at the
   source even though the consumer is making progress.
2. **Poll loop stuck** — `now - last_poll_at_ms` exceeds `max_poll_wait_ms × 6` (floor 30s).
   The connector is blocked in the source, typically a dead socket.
3. **Consumer stall** — events were polled but not committed, and `last_commit_at_ms` is stale.
   The embedder has stopped calling `commit_ack`; this is *not* a source problem.

The same verdict is exposed on the Prometheus surface as a set of gauges of which exactly one
is `1`, so an alert rule is unambiguous:

```promql
rustcdc_runtime_health{verdict="stalled"} == 1
```

Alert on that expression. Do not alert on flat `events_committed_total` alone — it fires on every
quiet period.

Pair it with `rustcdc_runtime_events_skipped_total`: any non-zero value means events were dropped
by the transform error policy rather than delivered, which is silent data loss unless a
dead-letter handler is recording them.

**Warning (Alert, No Page):**

| Metric | Threshold | Action |
|--------|-----------|--------|
| `rustcdc_runtime_replication_lag_ms` | > 10000 ms | Monitor; lag is growing and may approach retention risk window |
| `rustcdc_runtime_checkpoint_age_ms` | > 10000 ms | Commit progression is stale; check checkpoint backend and consumer ack flow |
| `rustcdc_runtime_events_polled_total` | Deviation > 20% from 1h baseline | Throughput anomaly; check source and transform paths |

**Informational (Dashboard Only):**

| Metric | Baseline |
|--------|----------|
| `rustcdc_runtime_events_polled_total` | Should be monotonically increasing |
| `rustcdc_runtime_in_flight_events` | Should remain bounded; sustained growth indicates ack stalls |
| `rustcdc_runtime_buffer_depth` | Should remain bounded relative to workload |

### Prometheus Example Configuration

```yaml
groups:
  - name: rustcdc
    interval: 30s
    rules:
      - alert: CdcReplicationLagCritical
        expr: rustcdc_runtime_replication_lag_ms > 30000  # 30s
        for: 5m
        annotations:
          summary: "rustcdc replication lag critical ({{ $value }} ms)"
          action: "Check source database; verify checkpoint commits; investigate network/storage"

      - alert: CdcRuntimeStopped
        expr: rustcdc_runtime_liveness == 0
        for: 1m
        annotations:
          summary: "rustcdc runtime is not live"
          action: "Check process health, startup logs, and source connectivity"

      - alert: CdcCheckpointStalled
        expr: increase(rustcdc_runtime_events_committed_total[5m]) == 0
        for: 5m
        annotations:
          summary: "rustcdc checkpoint not advancing"
          action: "Check connectivity to source; verify no transform errors"
```

---

## Troubleshooting Common Failures

See [troubleshooting.md](@/docs/troubleshooting.md) for detailed diagnosis procedures.

### Quick Diagnosis

```bash
# 1. Check rustcdc process health
systemctl status rustcdc
journalctl -u rustcdc -f  # Live logs

# 2. Check checkpoint state
ls -lh /var/rustcdc/checkpoint_*.json
cat /var/rustcdc/checkpoint_postgres.json | jq .
cat /var/rustcdc/.rustcdc_checkpoint.owner 2>/dev/null || true

# 3. Check source database connectivity
# PostgreSQL
psql -h $PG_HOST -U cdc_user -d your_database -c "SELECT 1;"

# MySQL
mysql --defaults-extra-file=/etc/rustcdc/mysql-client.cnf -h "$MYSQL_HOST" -u cdc_user -e "SELECT 1;"

# SQL Server
SQLCMDPASSWORD="${SQLCMDPASSWORD:?set from secret manager}" sqlcmd -S "$SQLSERVER_HOST" -U cdc_user -Q "SELECT 1;"

# 4. Check recent errors in logs
journalctl -u rustcdc -n 50 --no-pager | grep -i "error\|warn"

# 5. Verify metrics are flowing
curl -s http://localhost:9090/metrics | grep rustcdc_ | head -20
```

### Checkpoint Owner-Lease Conflict Recovery

Symptom example:

```text
checkpoint owner lease conflict for '/var/rustcdc': lock owned by pid ...
```

Safe recovery steps:

```bash
# 1. Confirm rustcdc process is not running.
systemctl status rustcdc

# 2. Inspect owner-lease file (if present).
cat /var/rustcdc/.rustcdc_checkpoint.owner

# 3. Verify the listed PID is not active.
#    The lease file contains `HOSTNAME:PID`, so split it before calling `ps` —
#    `ps -p "$(cat ...)"` errors out on the whole string.
LEASE=$(cat /var/rustcdc/.rustcdc_checkpoint.owner)
LEASE_HOST="${LEASE%:*}"
LEASE_PID="${LEASE##*:}"
echo "lease held by host=$LEASE_HOST pid=$LEASE_PID (this host: $(hostname))"

if [ "$LEASE_HOST" = "$(hostname)" ]; then
  ps -p "$LEASE_PID"        # exit 0 = still alive, do NOT remove the lease
else
  echo "Lease belongs to a DIFFERENT host. Do not remove it from here."
  echo "Confirm no runtime is running on $LEASE_HOST first — two writers against one"
  echo "checkpoint directory destroy each other's records."
fi

# 4. Only if the PID is genuinely dead on THIS host: remove the stale lease.
#    A live process normally clears its own lease on exit, so a leftover file means
#    the process was killed with SIGKILL or the host lost power.
rm -f /var/rustcdc/.rustcdc_checkpoint.owner
systemctl start rustcdc
```

> The runtime fences writes against this file: it re-reads the lease before every durable
> write and refuses to write if the token is no longer its own. Removing the lease while a
> runtime is live therefore stops that runtime with a named error rather than silently
> allowing two writers — but it still costs you an outage, so confirm liveness first.

---

## Secret Rotation

### PostgreSQL Credential Rotation

**Procedure (Zero-Downtime):**

```bash
# 1. Create new credential in PostgreSQL (value supplied from secret manager)
psql -U postgres -d your_database -v new_password="$NEW_CDC_PASSWORD" -c "ALTER ROLE cdc_user WITH PASSWORD :'new_password';"

# 2. Update rustcdc configuration (new connection string with new password)
# Edit: /etc/rustcdc/config.toml or environment variable
# Update configured secret source for `PostgresSourceConfig.password`

# 3. Gracefully restart rustcdc (will drain pending events before restart)
systemctl restart rustcdc

# 4. Verify new connection is active
journalctl -u rustcdc -n 10 | grep "source_connected\|connection"

# 5. Old password can now be revoked (after verification)
psql -U postgres -d your_database -c "ALTER ROLE cdc_user WITH PASSWORD NULL;" # Disable old password
```

### MySQL Credential Rotation

```bash
# 1. Create new user with password supplied via secret manager
mysql --defaults-extra-file=/etc/rustcdc/mysql-admin.cnf -e "CREATE USER 'cdc_user_new'@'%' IDENTIFIED BY '${NEW_CDC_PASSWORD}'; GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'cdc_user_new'@'%';"

# 2. Update rustcdc config
# Update configured secret source for `MysqlSourceConfig.password`

# 3. Restart
systemctl restart rustcdc

# 4. Verify
journalctl -u rustcdc -n 10 | grep "source_connected"

# 5. Revoke old user
mysql --defaults-extra-file=/etc/rustcdc/mysql-admin.cnf -e "DROP USER 'cdc_user'@'%';"
```

---

## Disaster Recovery

### Scenario 0: Forcing a Re-Snapshot

Several procedures below end in "re-snapshot the affected tables". This is that procedure.
It is needed whenever the source can no longer supply the changes the checkpoint says we
still need — the connector detects each of these and stops with an `Unrecoverable` error
naming the cause:

- PostgreSQL: replication slot dropped or invalidated (`invalidation_reason` non-NULL).
- MySQL/MariaDB: binlogs purged past the checkpointed GTID position.
- SQL Server: CDC cleanup purged change rows past the checkpointed LSN (error 313).

**Steps:**

1. **Stop the pipeline.** Do not skip this — a running connector will re-create state
   underneath you.

2. **Remove BOTH checkpoint files for the source.** A stream checkpoint and a snapshot
   checkpoint coexist, and deleting only the stream one leaves the snapshot checkpoint to
   be picked up on restart:
   ```bash
   rm -f /var/rustcdc/checkpoint_<source>.json \
         /var/rustcdc/checkpoint_<source>_snapshot.json
   ```
   `<source>` is `postgres`, `mysql`, `mariadb`, or `sqlserver`. **Note MariaDB writes
   `checkpoint_mariadb.json`, not `checkpoint_mysql.json`.**

3. **Re-provision source-side capture state** where it was lost:
   ```sql
   -- PostgreSQL: recreate the slot (add failover for PG17+ multi-node clusters)
   SELECT pg_create_logical_replication_slot('rustcdc_slot', 'pgoutput');
   SELECT pg_create_logical_replication_slot('rustcdc_slot', 'pgoutput', false, false, true);

   -- SQL Server: verify the capture instance still exists and the Agent jobs are running
   SELECT capture_instance, start_lsn FROM cdc.change_tables;
   EXEC sys.sp_cdc_help_jobs;
   ```

4. **Restart with `snapshot_tables` configured** for the affected tables. The connector
   performs a fresh snapshot, then hands off to streaming at the snapshot watermark.

5. **Expect duplicates downstream, not gaps.** The re-snapshot re-emits every row of the
   affected tables. Sinks must be idempotent (upsert on primary key); this is the
   at-least-once contract, not a bug.

> **Prevention is retention.** Every trigger above is "the source discarded data before we
> read it". Size retention against your worst-case downtime:
> PostgreSQL `max_slot_wal_keep_size` (and monitor `rustcdc_replication_slot_lag_bytes`),
> MySQL `binlog_expire_logs_seconds`, SQL Server
> `sys.sp_cdc_change_job @job_type='cleanup', @retention = ...`.

### Scenario 1: Source Database Becomes Unavailable

**Recovery Steps:**

1. **Graceful Shutdown**

   ```bash
   systemctl stop rustcdc
   ```

   > **This does not flush anything by itself.** rustcdc ships no binary and installs no
   > signal handler; a plain `SIGTERM` kills the process with the in-flight batch
   > uncommitted, which replays on restart (at-least-once — correct, but noisy).
   >
   > Flushing is a property of **your** wrapper. To get it, handle the shutdown signal and
   > call `CdcRuntime::drain_and_stop()`, which polls until the source is exhausted and
   > commits — and **returns the drained events**, because dropping them after the
   > checkpoint has advanced past them is unrecoverable. `stop()` refuses to run while
   > events are in flight; `force_stop()` discards them and logs
   > `shutdown_mode = "forced"`.

2. **Verify Last Checkpoint**
   ```bash
   cat /var/rustcdc/checkpoint_postgres.json | jq .
   ```

3. **Source Recovery**
   - Wait for source database to recover
   - Verify replication slot still exists (if PostgreSQL)
   - Verify WAL/binlog is available for resume position

4. **Resume**
   ```bash
   systemctl start rustcdc
   # Will resume from last committed checkpoint
   ```

### Scenario 2: Checkpoint Corruption

**Diagnosis:**
```bash
# Attempt to parse checkpoint
cat /var/rustcdc/checkpoint_postgres.json | jq . 2>&1
# If error: checkpoint file is corrupted
```

**Recovery:**

```bash
# 1. Stop rustcdc
systemctl stop rustcdc

# 2. Backup corrupted checkpoint
cp /var/rustcdc/checkpoint_postgres.json /var/rustcdc/checkpoint_postgres.json.corrupt.$(date +%s)

# 3. Delete ALL checkpoint files for this source family to force a full rescan.
#    See the note above: a leftover `_snapshot.json` is still a resumable checkpoint.
rm -f /var/rustcdc/checkpoint_postgres.json \
      /var/rustcdc/checkpoint_postgres_snapshot.json

# 4. Restart
systemctl start rustcdc

# ⚠️ WARNING: This may cause duplicate events if consumer is already processing data beyond this point
# Coordinate with downstream systems to handle duplicates
```

### Scenario 3: Metric Exporter Unavailable

If metrics are critical for operations:

```bash
# Verify metrics endpoint is responding
curl -v http://localhost:9090/metrics

# If OTel collector is unreachable, rustcdc will:
# 1. Log warning message
# 2. Continue processing (metrics are not critical to CDC correctness)
# 3. Retry connection periodically

# No action needed; CDC processing continues
```

---

## Maintenance Windows

### Planned Maintenance Schedule

**Weekly (off-hours):**
- [ ] Verify checkpoint files are readable
- [ ] Check replication lag is healthy (< 10000 ms steady-state target)
- [ ] Confirm no errors in recent logs

**Monthly:**
- [ ] Rotate credentials (if policy requires)
- [ ] Verify backup/disaster recovery procedure
- [ ] Review metric alert thresholds vs. actual baseline

**Quarterly:**
- [ ] Test failover to secondary source (if applicable)
- [ ] Review and update this runbook
- [ ] Capacity planning based on data growth

### Backfill load during business hours

**Symptom:** an incremental snapshot of a large table is adding read load to a production
primary at a bad time.

**Do not** stop the pipeline and clear the checkpoint. That stops capture as well, and
restarting rebuilds the snapshot from wherever the cursors were lost.

**Instead**, pause chunk reading and leave capture running:

```rust
# use rustcdc::CdcRuntime;
# async fn example(runtime: &mut CdcRuntime) -> rustcdc::Result<()> {
runtime.pause_incremental_snapshot().await?;   // idempotent
# Ok(())
# }
```

The change stream is unaffected — replication-slot lag keeps draining, and the checkpoint keeps
advancing. Resume in the evening with `resume_incremental_snapshot()`, which continues from the
chunk it stopped at rather than restarting the table.

The pause is written into the checkpoint, so a deploy during the paused window does **not**
silently restart the backfill. That also means a pause left in place is invisible unless you
look: check `admin_snapshot().incremental_snapshot`, whose `paused` flag and per-table
`rows_emitted` / `is_complete` are the progress readout. From an admin task that does not hold
`&mut CdcRuntime`, use `control_handle()` — `RuntimeControl::incremental_snapshot_state()` is
non-blocking and cannot hang behind a stalled pipeline.

To abandon the backfill entirely, `stop_incremental_snapshot()` discards the cursors and
returns how many tables still had work. Capture continues. Note that stop becomes durable only
with the next checkpoint write, so a crash in that window resumes the snapshot — stop it again.

---

## Contacts and Escalation

| Role | Contact | Escalation |
|------|---------|-----------|
| On-Call SRE | Page via PagerDuty | Escalate to Platform Lead if unresolved in 30 min |
| Database Admin | Slack #dba-oncall | Create incident ticket if source DB issue confirmed |
| CDC Maintainer | GitHub Issues or #rustcdc Slack | Create critical incident if data loss risk detected |

---

**Last Updated:** May 25, 2026  
**Version:** Current Runbook
