+++
title = "MySQL / MariaDB connector"
description = "Capture changes from MySQL and MariaDB via the binary log: GTID positioning, row image settings, schema history, required grants and managed cloud databases."
weight = 52
+++

The rustcdc MySQL and MariaDB connector reads row-level changes from the database
binary log (binlog) and streams them as structured events to your configured sink.
It acts as a **replica** — connecting to the server using the MySQL replication
protocol — so it captures changes in real time with no polling, triggers, or
schema modifications required.

Both MySQL (5.7+) and MariaDB (10.3+) are supported. The configuration is
identical except for `type = "mysql"` vs `type = "mariadb"`.


## 1. Overview

MySQL's **binary log** (binlog) records every committed data change. With
`binlog_format = ROW`, the binlog contains full before/after row images for every
`INSERT`, `UPDATE`, and `DELETE`. rustcdc reads this stream by registering as a
replica with a unique `server_id`.

### What is captured

| Change type | Captured | Notes |
|---|---|---|
| `INSERT` | yes | Full `after` row |
| `UPDATE` | yes | `before` + `after` when `binlog_row_image = FULL` |
| `DELETE` | yes | `before` when `binlog_row_image = FULL`; otherwise primary key only |
| DDL changes | yes | Schema history is maintained for correct event decoding after column additions/renames |
| `TRUNCATE` | yes | Parsed from the `TRUNCATE TABLE` query event in the binlog and emitted as a `truncate` operation (respects the table include/exclude lists) |

### Minimum requirements

| Requirement | MySQL | MariaDB |
|---|---|---|
| Version | 5.7+ | 10.3+ |
| `binlog_format` | `ROW` | `ROW` |
| `binlog_row_image` | `FULL` | `FULL` |
| User privilege | `REPLICATION SLAVE`, `REPLICATION CLIENT`, `SELECT` | same |


## 2. How the connector works

### Schema history

Unlike PostgreSQL's logical decoding (which carries schema information inline),
the MySQL binlog only contains column positions — not column names or types. The
connector maintains a **schema history** in the state backend, which it uses to
decode binlog events correctly.

On the first run, the connector:

1. Connects to MySQL and reads the current schema for all tables in scope.
2. Records the schema and the current binlog position as the checkpoint.
3. Starts streaming the binlog from that position.

Whenever the connector encounters a DDL event (e.g., `ALTER TABLE`), it updates
the schema history so subsequent events can be decoded correctly.

> **Important:** if you delete the schema history (e.g., by clearing the state
> backend), the connector cannot decode previously captured binlog events
> reliably. Always include the schema history in any backup/migration of the
> state backend.

### GTID mode

If the MySQL server is configured with GTID (Global Transaction Identifiers), the
connector can use GTIDs instead of binlog file/position offsets. GTIDs make it
significantly easier to resume replication after a failover because the new
primary has the same GTID set regardless of binlog filename.

Enable GTID-based offsets with `gtid_mode_enabled = true` in `[source]` (the
server must have `gtid_mode = ON` / `enforce_gtid_consistency = ON`). With
`gtid_mode_enabled = false` the connector uses binlog file/position offsets.

### Binlog position vs. GTID

| Mode | `source_offset` format | Failover support |
|---|---|---|
| Binlog position | `mysql-bin.000001:1234` | Manual (update host + offset) |
| GTID | `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5` | Automatic (GTID set is server-global) |


## 3. Setting up MySQL

### Step 1 — configure `my.cnf`

```ini
[mysqld]
# Binlog
log_bin               = mysql-bin
binlog_format         = ROW
binlog_row_image      = FULL
expire_logs_days      = 7          # retain at least as long as max connector downtime

# Server identity (must be unique across all servers in the topology)
server_id             = 1

# GTID (strongly recommended for HA setups)
gtid_mode             = ON
enforce_gtid_consistency = ON

# Optional: reduce replica lag
sync_binlog           = 1
innodb_flush_log_at_trx_commit = 1
```

Restart MySQL after changing `my.cnf`:

```bash
systemctl restart mysqld
```

Verify:

```sql
SHOW VARIABLES LIKE 'binlog_format';      -- must be ROW
SHOW VARIABLES LIKE 'binlog_row_image';   -- must be FULL
SHOW VARIABLES LIKE 'gtid_mode';          -- ON if using GTIDs
SHOW MASTER STATUS;                       -- shows current binlog position
```

### Step 2 — create a replication user

```sql
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'changeme';

-- Minimum required privileges:
GRANT REPLICATION SLAVE    ON *.* TO 'cdc_user'@'%';
GRANT REPLICATION CLIENT   ON *.* TO 'cdc_user'@'%';
GRANT SELECT               ON mydb.* TO 'cdc_user'@'%';

-- If you want schema history to survive DDL changes on all databases:
GRANT SELECT               ON *.* TO 'cdc_user'@'%';

FLUSH PRIVILEGES;
```

### Step 3 — configure `cdc.toml`

```toml
[source]
type      = "mysql"
host      = "localhost"
port      = 3306
user      = "cdc_user"
password  = { env = "MYSQL_PASSWORD" }
database  = "mydb"

# Must be unique among all replication clients of this server
server_id = 1001

gtid_mode_enabled   = false   # true when the server runs gtid_mode = ON
binlog_format_check = true    # verify binlog_format = ROW at connect time

conn_timeout_secs       = 10
stream_poll_interval_ms = 100
max_events_per_poll     = 1000

# Exact "database.table" names (case-insensitive); no glob patterns
table_include_list = ["mydb.orders", "mydb.customers"]
table_exclude_list = []

[source.transport]
mode = "plaintext"   # plaintext | tls
```

### Step 4 — run

```bash
export MYSQL_PASSWORD="changeme"
rustcdc run --config cdc.toml
# (only the kafka_topic state backend needs a one-time `rustcdc init-state` first)
```


## 4. Setting up MariaDB

MariaDB is configured identically to MySQL. Use `type = "mariadb"` in `cdc.toml`.

### MariaDB-specific notes

- **GTID:** MariaDB uses a different GTID format (`domain_id-server_id-sequence_nr`).
  rustcdc handles both MySQL and MariaDB GTID formats transparently.
- **Binlog row annotations:** if `binlog_annotate_row_events = ON`, rustcdc logs
  the SQL statement that triggered each row event in structured log output.
- **TLS:** some MariaDB test/development images do not have SSL capability.
  Use `[source.transport] mode = "plaintext"` in those environments.

```ini
[mysqld]
# my.cnf for MariaDB
log_bin                    = mariadb-bin
binlog_format              = ROW
binlog_row_image           = FULL
server_id                  = 1
gtid_strict_mode           = ON
binlog_annotate_row_events = ON
```

```toml
# cdc.toml
[source]
type      = "mariadb"
host      = "localhost"
port      = 3306
user      = "cdc_user"
password  = { env = "MARIADB_PASSWORD" }
database  = "mydb"
server_id = 1002
gtid_mode_enabled   = true    # MariaDB GTID (domain_id-server_id-sequence_nr)
binlog_format_check = true
conn_timeout_secs       = 10
stream_poll_interval_ms = 100
max_events_per_poll     = 1000
table_include_list = ["mydb.orders"]
table_exclude_list = []

[source.transport]
mode = "plaintext"
```


## 5. Cloud databases

### Amazon RDS for MySQL

1. Enable automated backups (required for binary logging on RDS).
2. Set `binlog_format = ROW` in a custom parameter group.
3. Grant `AmazonRDSReplicationRole` or manually:
   ```sql
   CALL mysql.rds_set_configuration('binlog retention hours', 24);
   GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'cdc_user'@'%';
   ```
4. In `cdc.toml`, use the RDS endpoint as `source.host`.

> GTID is available on RDS MySQL 5.7.23+ and 8.0. Enable it via the parameter
> group (`gtid_mode = ON`, `enforce_gtid_consistency = ON`).

### Amazon Aurora MySQL

Aurora MySQL uses a different binlog implementation. Set:
- `binlog_format = ROW` in the cluster parameter group
- `binlog_row_image = full` (Aurora uses lowercase)

Aurora requires a cluster-level parameter group change, not an instance-level one.

### Cloud SQL for MySQL (GCP)

Enable binary logging in the Cloud SQL instance settings. GTID is available on
Cloud SQL MySQL 5.7+. Grant standard replication privileges as shown above.


## 6. Supported topologies

### Standalone server

Single MySQL instance. The connector uses one `server_id` and connects directly.

### Primary + replicas

The connector must connect to the **primary**. Physical replicas share the same
binlog stream but binlog positions may differ. Use GTID mode to simplify failover:

1. Update `source.host` to the new primary's endpoint.
2. Restart the connector. The GTID set in the checkpoint is resolved on the new
   primary automatically.

### Multiple pipelines against the same server

Each pipeline must use a **unique** `server_id`. MySQL does not prevent multiple
replicas from sharing a `server_id`, but doing so causes undefined behaviour and
silent data loss.

```toml
# Pipeline A — server_id 1001
[source]
server_id = 1001

# Pipeline B (separate config) — server_id 1002
[source]
server_id = 1002
```


## 7. Configuration reference

```toml
[source]
type     = "mysql"      # or "mariadb"

# Connection
host     = "localhost"
port     = 3306
user     = "cdc_user"
password = { env = "MYSQL_PASSWORD" }
database = "mydb"
conn_timeout_secs = 10

# Replication identity — must be unique across all replicas of this server
server_id = 1001

# Offsets and safety checks
gtid_mode_enabled   = false     # true when the server runs gtid_mode = ON
binlog_format_check = true      # verify binlog_format = ROW at connect time

# Polling
stream_poll_interval_ms = 100
max_events_per_poll     = 1000

# Table filtering (exact "database.table" names, case-insensitive;
# include takes precedence — no glob patterns)
table_include_list = ["mydb.orders", "mydb.customers"]
table_exclude_list = []

# Transport
[source.transport]
mode = "plaintext"    # plaintext | tls
# For mode = "tls":
# ca_cert_path               = "/etc/ssl/ca.pem"       # custom CA; system store if absent
# client_cert_path           = "/etc/ssl/client.pem"   # mTLS (with client_key_path)
# client_key_path            = "/etc/ssl/client-key.pem"
# allow_invalid_certificates = false                   # local testing only
# allow_invalid_hostnames    = false                   # local testing only
```

### Field details

| Field | Default | Description |
|---|---|---|
| `type` | — | `"mysql"` or `"mariadb"` |
| `host` / `port` / `user` / `password` / `database` | — | Connection parameters; `password` accepts `{ env = "VAR" }` |
| `conn_timeout_secs` | — | Connection timeout in seconds |
| `server_id` | — | **Required.** Unique replica server ID. Must not collide with any other replica. |
| `gtid_mode_enabled` | — | **Required.** Use GTID offsets (`true`) or binlog file/position (`false`) |
| `binlog_format_check` | — | **Required.** Verify `binlog_format = ROW` at connect time |
| `stream_poll_interval_ms` | — | Binlog poll interval |
| `max_events_per_poll` | — | Maximum events yielded per poll cycle |
| `table_include_list` | empty (= all) | Exact `database.table` names; takes precedence over the exclude list |
| `table_exclude_list` | empty | Exact `database.table` names to suppress; ignored when the include list is non-empty |
| `auth_mode` | `password` | `password` or `aws_iam_token` (short-lived IAM auth; requires TLS) |


## 8. Monitoring

| Metric | Type | Description |
|---|---|---|
| `rustcdc_runtime_health{verdict=…}` | gauge (one-hot) | `healthy` \| `idle` \| `stalled` \| `not_running` — alert on `stalled` |
| `rustcdc_source_consecutive_poll_errors` | gauge | Consecutive binlog read errors |
| `rustcdc_runtime_checkpoint_age_ms` | gauge | Age of last durable checkpoint (milliseconds) |
| `rustcdc_runtime_events_committed_total` | counter | Total events acknowledged and checkpointed |


## 9. Behavior when things go wrong

### Binlog not available for stored offset

**Symptom:** connector fails with an error indicating the binlog position is no
longer available (e.g., `Could not find first log file name in binary log index file`).

**Cause:** the binlog was rotated/purged before the connector could consume it.
This happens when `expire_logs_days` is too short or the connector was stopped
for longer than the retention window.

**Resolution:** the stored offset is unrecoverable — the connector must
re-snapshot. Remove the checkpoint state (for `local_fs`: delete the
`checkpoint/` directory under `state.dir`; for `kafka_topic`: re-seed with
`rustcdc init-state --config cdc.toml --force`), then start with
`snapshot_tables` configured so the tables are re-read before streaming.
Increase `binlog_expire_logs_seconds` to be longer than your maximum expected
connector downtime.

### `server_id` conflict

**Symptom:** connector connects successfully but then gets disconnected with
`A slave with the same server_uuid/server_id as this slave has connected to the master`.

**Resolution:** assign a unique `server_id` to each connector instance. The value
must not match the primary server's `server_id` or any replica's `server_id`.

### Schema history corruption

**Symptom:** events for a table decode incorrectly (wrong column names or types)
after an `ALTER TABLE`.

**Cause:** the schema history in the state backend is out of sync with the actual
table schema.

**Resolution:** clear the schema history and re-snapshot. For the `local_fs`
state backend, stop the pipeline and delete the `schema_history` file under
`state.dir`; for `kafka_topic`, re-seed the state topic with
`rustcdc init-state --config-file cdc.toml --force`. Then start with
`snapshot_tables` configured so the affected tables are re-read.
