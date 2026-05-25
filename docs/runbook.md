# rustcdc Operator Runbook

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

If your deployment does not provide these wrappers, see `docs/deployment.md` first and wire health/metrics/service controls before applying this runbook verbatim.

---

## PostgreSQL Source Management

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
```
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

cat > /var/rustcdc/checkpoint_postgres.json.new <<EOF
{
  "checkpoint_format_version": 2,
  "source_type": "postgres",
  "committed_event_count": 0,
  "offset": {
    "lsn": $CURRENT_LSN_HEX,
    "slot_name": "rustcdc_postgres_new"
  }
}
EOF

# Validate checkpoint schema before swapping it in.
cat /var/rustcdc/checkpoint_postgres.json.new | jq -e '
  .checkpoint_format_version == 2 and
  .source_type == "postgres" and
  (.committed_event_count | type == "number") and
  (.offset.lsn | type == "number") and
  (.offset.slot_name | type == "string")
'

# Atomically replace the active checkpoint file.
mv /var/rustcdc/checkpoint_postgres.json.new /var/rustcdc/checkpoint_postgres.json
#    b) Optionally create new replication slot on PostgreSQL
psql -U cdc_user -d your_database -c "SELECT * FROM pg_create_logical_replication_slot('rustcdc_postgres_new', 'pgoutput');"

# 5. Restart rustcdc
systemctl start rustcdc

# 6. Verify slot is active and consuming
psql -U cdc_user -d your_database -c "SELECT slot_name, active, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'rustcdc_postgres_new';"
```

**Option B: Force Reset (Data Loss Risk)**

⚠️ **WARNING:** This discards uncommitted events and may cause data loss if not coordinated with downstream systems.

Before executing force reset, run the checklist guardrail script:

```bash
./scripts/runbook-force-reset-checklist.sh \
  --change-ticket INC-12345 \
  --snapshot-backed-up \
  --downstream-paused \
  --slot-impact-reviewed \
  --rollback-plan-ready
```

```bash
# 1. Stop rustcdc
systemctl stop rustcdc

# 2. Drop old slot
psql -U cdc_user -d your_database -c "SELECT pg_drop_replication_slot('rustcdc_postgres_old');"

# 3. Delete checkpoint file to force restart from current position
rm /var/rustcdc/checkpoint_postgres.json

# 4. Restart rustcdc (will start fresh from current WAL position)
systemctl start rustcdc
```

### Preventive Maintenance

**Daily Checks:**

```bash
#!/bin/bash
# Check runtime lag every hour (milliseconds)
LAG_MS=$(curl -s http://localhost:9090/metrics | awk '/^cdc_runtime_replication_lag_ms / {print $2; exit}')

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
2. Observe `cdc_runtime_replication_lag_ms`, checkpoint progression, and source CPU.
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

## Metric Alerting and Monitoring

### Recommended Alert Thresholds

**Critical (Page On-Call):**

| Metric | Threshold | Action |
|--------|-----------|--------|
| `cdc_runtime_replication_lag_ms` | > 30000 ms | Investigate source/database lag, downstream throughput, and checkpoint commits |
| `cdc_runtime_events_committed_total` | No increase for 5 min | Check stream connectivity; may indicate stalled progress |
| `cdc_runtime_liveness` | == 0 | Runtime stopped or unhealthy; investigate process and startup logs |

**Warning (Alert, No Page):**

| Metric | Threshold | Action |
|--------|-----------|--------|
| `cdc_runtime_replication_lag_ms` | > 10000 ms | Monitor; lag is growing and may approach retention risk window |
| `cdc_runtime_checkpoint_age_ms` | > 10000 ms | Commit progression is stale; check checkpoint backend and consumer ack flow |
| `cdc_runtime_events_polled_total` | Deviation > 20% from 1h baseline | Throughput anomaly; check source and transform paths |

**Informational (Dashboard Only):**

| Metric | Baseline |
|--------|----------|
| `cdc_runtime_events_polled_total` | Should be monotonically increasing |
| `cdc_runtime_in_flight_events` | Should remain bounded; sustained growth indicates ack stalls |
| `cdc_runtime_buffer_depth` | Should remain bounded relative to workload |

### Prometheus Example Configuration

```yaml
groups:
  - name: rustcdc
    interval: 30s
    rules:
      - alert: CdcReplicationLagCritical
        expr: cdc_runtime_replication_lag_ms > 30000  # 30s
        for: 5m
        annotations:
          summary: "rustcdc replication lag critical ({{ $value }} ms)"
          action: "Check source database; verify checkpoint commits; investigate network/storage"

      - alert: CdcRuntimeStopped
        expr: cdc_runtime_liveness == 0
        for: 1m
        annotations:
          summary: "rustcdc runtime is not live"
          action: "Check process health, startup logs, and source connectivity"

      - alert: CdcCheckpointStalled
        expr: increase(cdc_runtime_events_committed_total[5m]) == 0
        for: 5m
        annotations:
          summary: "rustcdc checkpoint not advancing"
          action: "Check connectivity to source; verify no transform errors"
```

---

## Troubleshooting Common Failures

See [troubleshooting.md](troubleshooting.md) for detailed diagnosis procedures.

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
curl -s http://localhost:9090/metrics | grep cdc_ | head -20
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

# 3. Verify listed PID is not active.
ps -p "$(cat /var/rustcdc/.rustcdc_checkpoint.owner)"

# 4. If PID is not running, remove stale lease and restart.
rm -f /var/rustcdc/.rustcdc_checkpoint.owner
systemctl start rustcdc
```

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
mysql --defaults-extra-file=/etc/rustcdc/mysql-admin.cnf -e "CREATE USER 'cdc_user_v2'@'%' IDENTIFIED BY '${NEW_CDC_PASSWORD}'; GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'cdc_user_v2'@'%';"

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

### Scenario 1: Source Database Becomes Unavailable

**Recovery Steps:**

1. **Graceful Shutdown**
   ```bash
   systemctl stop rustcdc  # Flushes pending events to checkpoint
   ```

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

# 3. Delete checkpoint to force full rescan from source
rm /var/rustcdc/checkpoint_postgres.json

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
