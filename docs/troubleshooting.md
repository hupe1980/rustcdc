# rustcdc Troubleshooting Guide

**Version:** v0.1+  
**Audience:** Operators and developers debugging rustcdc issues

---

## Table of Contents

1. [Connection Issues](#connection-issues)
2. [Checkpoint and Recovery Issues](#checkpoint-and-recovery-issues)
3. [Performance and Throughput Issues](#performance-and-throughput-issues)
4. [Data Quality Issues](#data-quality-issues)
5. [Transform and Filter Issues](#transform-and-filter-issues)
6. [Diagnostics Toolkit](#diagnostics-toolkit)

---

## Integration Scaffolding Assumptions

Command examples in this guide assume your embedder/deployment provides:

- Service controls (for example `systemctl`, container orchestration commands, or custom supervisor)
- Runtime/admin metrics endpoint (examples use `http://localhost:9090/metrics`)
- Connector/client CLIs installed for ad-hoc diagnostics (`psql`, `mysql`, `sqlcmd`)

Adapt commands to your runtime model. If your deployment does not expose these controls yet,
establish them first using the deployment guidance in `docs/deployment.md`.

---

## Connection Issues

### Symptom: "connection refused" or timeout on startup

**Error Examples:**
```
ERROR source error: failed to connect to postgres: connection refused
ERROR source error: failed to connect to mysql: timeout
ERROR source error: connection to sqlserver closed unexpectedly
```

**Diagnosis Checklist:**

1. **Verify network connectivity:**
   ```bash
   ping -c 3 <database_host>
   telnet <database_host> <port>  # or: nc -zv <host> <port>
   ```
   ✅ Should respond; ❌ if not, check network/firewall

2. **Verify database is running:**
   ```bash
   # PostgreSQL
   pg_isready -h <host> -p 5432
   
   # MySQL
   mysql --defaults-extra-file=<mysql-client.cnf> -h <host> -u <user> -e "SELECT 1;"
   
   # SQL Server
   SQLCMDPASSWORD="${SQLCMDPASSWORD:?set from secret manager}" sqlcmd -S <host> -U <user> -Q "SELECT 1;"
   ```
   ✅ Should return connection OK; ❌ if not, restart database

3. **Verify credentials:**
   ```bash
   # PostgreSQL
   psql "postgresql://<user>@<host>:5432/<database>"
   
   # MySQL
   mysql --defaults-extra-file=<mysql-client.cnf> -h <host> -u <user> <database>
   
   # SQL Server
   SQLCMDPASSWORD="${SQLCMDPASSWORD:?set from secret manager}" sqlcmd -S <host> -U <user> -d <database>
   ```
   ✅ Should authenticate; ❌ if not, verify configured secret source and connector credentials

4. **Check user permissions:**
   ```sql
   -- PostgreSQL: verify REPLICATION role
   SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'cdc_user';
   -- Should show: cdc_user | t
   
   -- MySQL: verify privileges
   SHOW GRANTS FOR 'cdc_user'@'%';
   -- Should include: REPLICATION CLIENT, REPLICATION SLAVE, SELECT
   
   -- SQL Server: verify CDC role
   SELECT is_member('cdc_admin');
   -- Should return: 1
   ```

5. **Check connection string format:**
   - PostgreSQL: `postgresql://user:pass@host:port/database?sslmode=require`
   - MySQL: `mysql://user:pass@host:port/database`
   - SQL Server: `sqlserver://user:pass@host:port;database=name;Encrypt=yes`

**Resolution:**

| Root Cause | Action |
|------------|--------|
| Network blocked | Whitelist rustcdc server IP on database firewall |
| Database not running | Restart database service |
| Invalid credentials | Verify in config file; check for special characters |
| Missing REPLICATION role (PG) | Run: `ALTER ROLE cdc_user WITH REPLICATION;` |
| Missing CDC admin (SQL Server) | Run: `ALTER ROLE cdc_user ADD MEMBER cdc_admin;` |
| TLS/Certificate error | Verify `transport` is set to `TransportConfig::tls()` or `TransportConfig::tls_with_ca_cert_path(...)` |

---

### Symptom: "TLS handshake failed" or certificate validation error

**Error Examples:**
```
ERROR source error: tls error: certificate verify failed
ERROR source error: tls error: x509: certificate signed by unknown authority
```

**Diagnosis:**

```bash
# 1. Check if database requires TLS
openssl s_client -connect <host>:<port>
# Should show certificate chain; if connection fails, TLS may be required

# 2. Verify CA certificate (if using custom CA)
openssl x509 -in /path/to/ca.pem -text -noout
# Should show certificate details; verify Subject and Issuer match your CA

# 3. Test TLS connection manually
# PostgreSQL
psql "postgresql://user:pass@host/db?sslmode=require&sslcert=/path/to/cert.pem&sslkey=/path/to/key.pem"

# MySQL
mysql -h <host> --ssl-mode=REQUIRED --ssl-ca=/path/to/ca.pem -u <user> -p<pass>
```

**Resolution:**

| Issue | Action |
|-------|--------|
| Certificate not trusted | Use `TransportConfig::tls_with_ca_cert_path(...)` with the CA bundle path |
| TLS handshake fails behind proxy | Verify proxy certificates are rooted in the CA bundle configured via `TransportConfig::tls_with_ca_cert_path(...)` |
| Expired certificate | Request new certificate from database admin |
| Wrong CA certificate | Verify CA cert matches database server certificate issuer |

---

## Checkpoint and Recovery Issues

### Symptom: "checkpoint error" or "replication slot diverged"

**Error Examples:**
```
ERROR: source error: postgres checkpoint/slot divergence for slot '...'
ERROR checkpoint error: checkpoint file does not exist
ERROR checkpoint error: failed to read checkpoint: invalid JSON
```

**Diagnosis Checklist:**

1. **Verify checkpoint file exists and is readable:**
   ```bash
   ls -lh /var/rustcdc/checkpoint_*.json
   ```
   ✅ Should list checkpoint files; ❌ if not, check directory permissions

2. **Verify checkpoint is valid JSON:**
   ```bash
   cat /var/rustcdc/checkpoint_postgres.json | jq .
   # Should pretty-print JSON; ❌ if error, checkpoint is corrupted
   ```

3. **For PostgreSQL: verify replication slot exists:**
   ```sql
   SELECT slot_name, active, restart_lsn FROM pg_replication_slots WHERE slot_name = 'rustcdc_postgres_*';
   -- Should return: slot_name | t | <LSN>
   ```
   ✅ Slot exists and active; ❌ if not, may have been dropped manually

4. **For PostgreSQL: check LSN divergence:**
   ```sql
   -- Get checkpoint LSN from checkpoint file
   cat /var/rustcdc/checkpoint_postgres.json | jq '.offset.lsn'
   -- Should output: 281474976711680 (example)
   
   -- Get current WAL position
   SELECT pg_current_wal_lsn();
   -- Should return: 0/11000000 (example)
   
   -- Calculate gap
   -- If checkpoint LSN differs from the slot's confirmed_flush_lsn, rustcdc now fails closed
   ```

**Resolution:**

| Root Cause | Action |
|------------|--------|
| Checkpoint corrupted | Stop rustcdc; delete checkpoint file; restart (will scan from current position) |
| Replication slot dropped | Stop rustcdc; recreate checkpoint with current LSN; restart |
| WAL/binlog purged | See [Replication Slot Divergence Recovery](runbook.md#replication-slot-divergence-recovery) |
| Checkpoint permissions | Verify `/var/rustcdc/` is writable by rustcdc process owner |

---

### Symptom: "buffer full" error or frequent checkpoint pauses

**Error Examples:**
```
ERROR checkpoint error: commit barrier buffer is full
WARNING checkpoint latency exceeding 1s
```

**Diagnosis:**

1. **Check buffer utilization:**
   ```bash
   # From logs
   grep "buffer_size" /var/log/rustcdc/structured.log | tail -20
   
   # From runtime admin metrics
   curl http://localhost:9090/metrics | grep "cdc_runtime_buffer_depth"
   ```

2. **Check checkpoint commit latency:**
   ```bash
   # From runtime admin metrics
   curl http://localhost:9090/metrics | grep "cdc_runtime_checkpoint_age_ms"
   # p95 should be < 1s
   ```

3. **Check checkpoint store I/O:**
   ```bash
   # If using FileCheckpoint
   iostat -x 1 5 | grep sda  # Watch %util and await

   # If using a custom external checkpoint backend (for example PostgreSQL),
   # measure write latency of the backend-specific checkpoint upsert/update path.
   ```

**Resolution:**

| Root Cause | Action |
|------------|--------|
| Checkpoint store slow (disk I/O) | 1. Switch to FileCheckpoint on faster disk; 2. Increase max_buffer_size to batch more events |
| Checkpoint store slow (external backend) | Optimize backend-specific checkpoint writes and indexes; monitor write latency and contention |
| Buffer size too small | Increase `max_buffer_size` in RuntimeConfig (e.g., 50_000 → 100_000) |
| Transform errors causing queue buildup | Check transform error logs; fix failing transforms or set `transform_error_policy = Skip` |

---

## Performance and Throughput Issues

### Symptom: SQL Server high p99 latency (bursty / non-uniform)

**Expected behavior — not a bug:**

SQL Server CDC is **polling-based**.  rustcdc calls `cdc.fn_cdc_get_all_changes_*`
at a configurable interval (`stream_poll_interval_ms`, default 5 000 ms).  Unlike
PostgreSQL logical replication (push-based, near-zero propagation), SQL Server
events are only visible after the next poll cycle **and** after the SQL Server CDC
capture agent has written them to the change tables (typically < 5 s on an idle server).

Expected latency profile:

| Percentile | Typical value |
|------------|---------------|
| p50        | ≈ stream_poll_interval_ms / 2 |
| p99        | ≈ stream_poll_interval_ms + capture agent delay |
| p99.9      | ≈ 2 × stream_poll_interval_ms (poll jitter under load) |

A p99/p50 ratio of 1 000× is **normal** (e.g. p50 = 0.3 ms measured within a poll
window, p99 = 318 ms = one poll cycle).

**Tuning for lower latency:**

```rust
SqlServerSourceConfig {
    stream_poll_interval_ms: 500,  // default 5000; 500–1000 ms for latency-sensitive
    ..SqlServerSourceConfig::default()
}
```

Lower values increase SQL Server query load.  500 ms is the practical lower bound
for most production SQL Server deployments.

---

### Symptom: Low event throughput or high latency

**Error Examples:**
```
WARNING events processed per second dropping below baseline (was 10K/sec, now 5K/sec)
WARNING snapshot progress stalled (no new chunks for 30s)
```

**Diagnosis Checklist:**

1. **Check replication lag:**
   ```bash
   # From runtime admin metrics
   curl http://localhost:9090/metrics | grep "cdc_runtime_replication_lag_ms"
   # Should usually stay < 10000 ms; sustained > 30000 ms is critical
   ```

2. **Check source database load:**
   ```bash
   # PostgreSQL
   SELECT query, calls, mean_exec_time FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 5;
   
   # MySQL
   SHOW FULL PROCESSLIST;
   
   # SQL Server
   SELECT command, status, sql_text FROM sys.dm_exec_requests;
   ```
   ✅ Should show normal query activity; ❌ if high, source DB is overloaded

3. **Check network latency:**
   ```bash
   ping -c 10 <database_host> | tail -1
   # Should show avg < 10ms; if > 50ms, network may be congested
   ```

4. **Check rustcdc resource utilization:**
   ```bash
   # CPU
   top -p <rustcdc_pid> | grep CPU
   # Should be 25-75% for 1 core; > 90% indicates bottleneck
   
   # Memory
   ps aux | grep rustcdc | grep -v grep | awk '{print $6}'
   # Should grow to ~300-500 MB, then stabilize
   
   # File descriptors
   lsof -p <rustcdc_pid> | wc -l
   # Should be < 100 per source
   ```

5. **Check transform pipeline overhead:**
   ```bash
   # From metrics
   curl http://otel-collector:9090/metrics | grep "cdc_transform_duration"
   # p95 should be < 1ms per event
   ```

**Resolution:**

| Root Cause | Action |
|------------|--------|
| Source DB overloaded | Reduce rustcdc poll frequency; scale source DB; check for long-running queries |
| Network congested | Verify network MTU (1500 default); check for packet loss (ping -c 100) |
| rustcdc CPU maxed | Increase max_poll_wait_ms (batches more events per poll); reduce transform complexity |
| rustcdc memory growing | Check for transform memory leaks; verify checkpoint is committing (check committed_count) |
| Transform pipeline slow | Profile individual transforms; consider removing non-critical transforms |

---

### Symptom: Snapshot taking too long

**Error Examples:**
```
WARNING snapshot progress: 5% complete (10 hours in, estimated 200 hours remaining)
```

**Diagnosis:**

1. **Check snapshot progress:**
   ```bash
   # From logs
   grep "snapshot_chunk_received\|snapshot_complete" /var/log/rustcdc/structured.log | tail -20
   ```

2. **Check source table sizes:**
   ```sql
   -- PostgreSQL
   SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) 
   FROM pg_tables WHERE tablename IN ('users', 'orders', ...);
   
   -- MySQL
   SELECT table_schema, table_name, ROUND(((data_length + index_length) / 1024 / 1024), 2) 
   FROM information_schema.tables WHERE table_name IN ('users', 'orders', ...);
   
   -- SQL Server
   SELECT OBJECT_NAME(ps.object_id), SUM(ps.row_count) 
   FROM sys.dm_db_partition_stats ps 
   WHERE OBJECT_NAME(ps.object_id) IN ('users', 'orders', ...)
   GROUP BY ps.object_id;
   ```

3. **Check snapshot query performance:**
   ```bash
   # Manually run a snapshot query to measure time
   time psql -U cdc_user -d mydb -c "SELECT * FROM public.users LIMIT 10000;"
   # Should be < 100ms for 10K rows
   ```

**Resolution:**

| Root Cause | Action |
|------------|--------|
| Table too large to snapshot | 1. Reduce `snapshot_tables` list; 2. Increase `snapshot_chunk_size` (e.g., 10K → 50K); 3. Add index on clustering key |
| Source DB query slow | Add index on clustering/primary key; schedule snapshot during low-activity window |
| Network bandwidth limited | Verify network bandwidth (iperf); consider moving rustcdc to same datacenter |
| rustcdc CPU bottleneck | Scale to additional rustcdc instances; profile hot path in transform pipeline |

---

## Data Quality Issues

### Symptom: Missing events or duplicate events in output

**Error Examples:**
```
WARNING event_id=12345 received duplicate after checkpoint restart
ERROR detected missing event (event_id=12346 skipped)
```

**Diagnosis Checklist:**

1. **Verify checkpoint is committing:**
   ```bash
   # From runtime admin metrics
   curl http://localhost:9090/metrics | grep "cdc_runtime_events_committed_total"
   # Should be monotonically increasing; if stalled, commits have stopped
   ```

2. **Check for buffered events during shutdown:**
   ```bash
   # From logs
   grep "drain_pending\|final_checkpoint" /var/log/rustcdc/structured.log
   # Should show events flushed before shutdown
   ```

3. **Verify consumer is calling commit callbacks:**
   ```bash
   # Consumer code should call notify_consumer_accepted() + commit()
   # If not, events may not be marked as committed
   # Check consumer logs for these callbacks
   ```

4. **Check for transform filtering:**
   ```bash
   # From metrics
   curl http://otel-collector:9090/metrics | grep "cdc_events_filtered"
   # Should be intentional drops; not unexpected
   ```

**Resolution:**

| Root Cause | Action |
|------------|--------|
| Consumer not calling commit | Verify consumer code calls CommitBarrier::notify_consumer_accepted() + commit() |
| Checkpoint not persisted | Verify checkpoint store is writable; check FileCheckpoint path permissions |
| Process killed without graceful shutdown | Implement SIGTERM handler to flush pending events before exit |
| Transform filtering unintentional | Review transform configuration; verify filter rules are correct |

---

### Symptom: Events with incorrect data or wrong schema

**Error Examples:**
```
ERROR validation error: field 'before' is None but operation is Update
ERROR schema error: table schema not found for public.users
```

**Diagnosis:**

1. **Verify source schema is correct:**
   ```sql
   -- PostgreSQL
   \d public.users
   
   -- MySQL
   DESC users;
   
   -- SQL Server
   EXEC sp_help 'dbo.users';
   ```

2. **Check event envelope validation:**
   ```bash
   # Enable debug logging
   export RUST_LOG=rustcdc::core::event=debug
   
   # Check for validation errors
   grep "validation error\|ValidationError" /var/log/rustcdc/structured.log
   ```

3. **Verify transform rules are correct:**
   ```bash
   # Review transform configuration
   grep -A 10 "transform" /etc/rustcdc/config.toml
   # Verify mask/filter rules apply to correct tables/columns
   ```

**Resolution:**

| Root Cause | Action |
|------------|--------|
| Source schema changed (DDL) | 1. Update rustcdc snapshot_tables list; 2. Manually trigger schema refresh in SchemaHistory |
| Transform filter too broad | Review transform rules; test in development first |
| Event validation rule violated | Check docs/api.md and src/core/event.rs validation contract; verify source is generating events correctly |

---

## Transform and Filter Issues

### Symptom: Transform errors or events filtered unexpectedly

**Error Examples:**
```
ERROR transform error: route: no matching output for table public.unknown_table
ERROR transform error: mask: regex compilation failed for pattern '(?P<invalid>)'
WARNING events filtered by transform (count=50)
```

**Diagnosis:**

1. **Enable debug logging for transforms:**
   ```bash
   export RUST_LOG=rustcdc::transform=debug
   # See: transform_applied, transform_failed, events_filtered
   ```

2. **Verify transform configuration:**
   ```bash
   # Review config file for each transform
   grep -A 10 "transform\|route\|filter\|mask" /etc/rustcdc/config.toml
   
   # Common issues:
   # - Route table name doesn't match source
   # - Filter regex syntax invalid
   # - Mask column doesn't exist
   ```

3. **Test transforms in isolation:**
   ```bash
   # Enable test mode (if available in SDK)
   # Or manually run transform on sample event
   ```

**Resolution:**

| Root Cause | Action |
|------------|--------|
| Route table not found | Update route transform to include all source tables |
| Regex invalid | Use online regex tester (regex101.com); test pattern before deploying |
| Transform policy = Halt | If acceptable data loss, change to `transform_error_policy = Skip` |
| Column doesn't exist | Verify column name matches source schema exactly (case-sensitive) |

---

## Diagnostics Toolkit

### Essential Commands

```bash
# 1. Health check
curl http://localhost:9090/metrics | grep -E "cdc_runtime_events_polled_total|cdc_runtime_events_committed_total" | head -5

# 2. Recent errors
journalctl -u rustcdc -f | grep -i "error\|warn"

# 3. Checkpoint status
cat /var/rustcdc/checkpoint_postgres.json | jq .

# 4. Source connectivity test
psql "postgresql://user:pass@host/db" -c "SELECT 1;"
mysql -h host -u user -ppass db -e "SELECT 1;"
sqlcmd -S host -U user -P pass -d db -Q "SELECT 1;"

# 5. Network diagnostics
ping -c 10 <source_host>
telnet <source_host> <port>
iperf -c <source_host>  # Bandwidth test

# 6. System resource check
top -p $(pgrep -f rustcdc)
ps aux | grep rustcdc | awk '{print $2, $3, $4, $6}'

# 7. Detailed metrics
curl http://otel-collector:9090/metrics | grep cdc_ | sort

# 8. OTel trace export check
curl -s http://otel-collector:4317/...  # Check exporter is responding
```

### Log Analysis

```bash
# Count errors by type
grep "ERROR\|error" /var/log/rustcdc/structured.log | cut -d: -f2- | sort | uniq -c | sort -rn

# Find slow operations
grep "duration\|latency" /var/log/rustcdc/structured.log | sort -k3 -rn | head -20

# Timeline of events
grep "timestamp" /var/log/rustcdc/structured.log | head -1
grep "timestamp" /var/log/rustcdc/structured.log | tail -1
# Calculates duration of log file

# Export metrics trend
journalctl -u rustcdc -S "2 hours ago" | grep "cdc_" > metrics_export.log
```

### Interactive Debugging

```bash
# Start rustcdc with maximum logging
export RUST_LOG=rustcdc=trace
export RUST_LOG_FORMAT=json
cargo run --release

# Attach debugger (if debug build)
rust-gdb --args ./target/debug/rustcdc --config config.toml

# Health endpoint (if embedded in app)
curl -v http://localhost:8080/health
```

---

**Last Updated:** May 25, 2026  
**Version:** Troubleshooting Guide v0.1+  
**Contributing:** Found a new troubleshooting scenario? File an issue on GitHub!
