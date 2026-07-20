# SQL Server connector

The rustcdc SQL Server connector reads row-level changes from Microsoft SQL
Server's built-in **CDC feature** and streams them as structured events to your
configured sink. No third-party plugins or server extensions are required —
SQL Server CDC is a native enterprise feature available in SQL Server 2016+.

---

## Table of contents

1. [Overview](#1-overview)
2. [How the connector works](#2-how-the-connector-works)
3. [Setting up SQL Server](#3-setting-up-sql-server)
4. [Cloud databases](#4-cloud-databases)
5. [Supported topologies](#5-supported-topologies)
6. [Configuration reference](#6-configuration-reference)
7. [Monitoring](#7-monitoring)
8. [Behavior when things go wrong](#9-behavior-when-things-go-wrong)

---

## 1. Overview

SQL Server's built-in CDC feature captures row-level changes from the transaction
log into dedicated **change tables** (one per tracked source table). The connector
polls these change tables at a configurable interval and converts the captured
rows into the rustcdc event envelope.

### What is captured

| Change type | Captured | Notes |
|---|---|---|
| `INSERT` | yes | Full `after` row |
| `UPDATE` | yes | Full `before` + `after` |
| `DELETE` | yes | Full `before` |
| DDL changes | partial | Column additions captured; column drops may require re-enabling CDC on the table |
| `TRUNCATE` | opt-in | SQL Server CDC alone cannot log TRUNCATE. With `capture_truncate_events = true` the connector installs a database-level DDL trigger + shadow table and emits a `truncate` event positioned after all DML at the captured LSN |

### Minimum requirements

| Requirement | Value |
|---|---|
| SQL Server version | 2016 (13.x) or later |
| Edition | Enterprise, Developer, Standard, Web (not Express) |
| SQL Server Agent | must be running (drives the CDC cleanup and capture jobs) |
| User privilege | `db_datareader` on CDC change tables + `EXECUTE` on CDC system procedures |

---

## 2. How the connector works

### Change tables

When CDC is enabled on a table, SQL Server creates a corresponding change table
in the `cdc` schema (e.g., `cdc.dbo_orders_CT`). The table has one row for every
captured change, with columns:

| Column | Description |
|---|---|
| `__$start_lsn` | Log Sequence Number of the committed transaction |
| `__$end_lsn` | Always `NULL` for now (reserved) |
| `__$seqval` | Sequence within a transaction |
| `__$operation` | `1` = delete, `2` = insert, `3` = before-update, `4` = after-update |
| `__$update_mask` | Bitmask of changed columns |
| _all source columns_ | Actual column values |

### Polling

The connector periodically polls each change table using
`cdc.fn_cdc_get_all_changes_<capture_instance>()`. On each poll:

1. Reads changes since the last confirmed LSN
2. Converts them into rustcdc events
3. Delivers them to the sink
4. Advances the checkpoint to the highest LSN processed

The polling interval is controlled by `stream_poll_interval_ms` (default: 500 ms).

### Initial snapshot

On first run, the connector:

1. Records the current maximum LSN from `sys.fn_cdc_get_max_lsn()`
2. Scans all tracked tables and emits `read` events
3. Sets the checkpoint to the LSN recorded in step 1
4. Begins polling for changes from that LSN

---

## 3. Setting up SQL Server

### Step 1 — enable SQL Server Agent

SQL Server CDC requires the SQL Server Agent service to run the capture and
cleanup jobs. Start it if not already running:

```sql
-- Check Agent status
SELECT name, status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%';
```

On Windows:
```
Services → SQL Server Agent → Start
```

On Linux (SQL Server on Linux):
```bash
sudo /opt/mssql/bin/mssql-conf set sqlagent.enabled true
sudo systemctl restart mssql-server
```

### Step 2 — enable CDC on the database

```sql
USE mydb;
GO

EXEC sys.sp_cdc_enable_db;
GO

-- Verify:
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'mydb';
```

### Step 3 — enable CDC on each table

```sql
USE mydb;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'orders',
    @role_name     = NULL,           -- NULL = no gating role; restrict via user grants instead
    @supports_net_changes = 1;       -- enables net-changes queries (optional)
GO

-- Repeat for each table:
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'customers',
    @role_name     = NULL;
GO

-- Verify:
SELECT s.name AS schema_name, t.name AS table_name, t.is_tracked_by_cdc
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_tracked_by_cdc = 1;
```

### Step 4 — create a CDC user

```sql
-- Create login and user
CREATE LOGIN cdc_user WITH PASSWORD = 'changeme';
CREATE USER  cdc_user FOR LOGIN cdc_user;

-- Grant minimum required privileges:

-- Read CDC change tables
ALTER ROLE db_datareader ADD MEMBER cdc_user;

-- Execute CDC table-valued functions
GRANT EXECUTE ON SCHEMA::cdc TO cdc_user;

-- Read system LSN functions
GRANT VIEW DATABASE STATE TO cdc_user;

-- If the connector needs to query sys.columns for schema introspection:
GRANT VIEW DEFINITION ON SCHEMA::dbo TO cdc_user;
```

### Step 5 — configure `cdc.toml`

```toml
[source]
type     = "sqlserver"      # alias: "mssql"
host     = "localhost"
port     = 1433
user     = "cdc_user"
password = { env = "MSSQL_PASSWORD" }
database = "mydb"

cdc_enabled             = true
cdc_schema              = "cdc"
conn_timeout_secs       = 15
stream_poll_interval_ms = 500
max_events_per_poll     = 1000
prereq_pool_size        = 4

# Exact "schema.table" names (case-insensitive); no glob patterns
table_include_list = ["dbo.orders", "dbo.customers"]
table_exclude_list = []

[source.transport]
mode = "tls"                          # SQL Server negotiates TLS by default
allow_invalid_certificates = true     # dev/test only — self-signed server certs
```

### Step 6 — run

```bash
export MSSQL_PASSWORD="changeme"
rustcdc run --config-file cdc.toml
# (only the kafka_topic state backend needs a one-time `rustcdc init-state` first)
```

---

## 4. Cloud databases

### Azure SQL Database

Azure SQL Database has CDC available in the General Purpose and Business Critical
tiers. It is not available in the Basic/Standard tiers.

Enable CDC the same way as on-premises:

```sql
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders', @role_name = NULL;
```

> **Note:** SQL Server Agent is managed by Azure and is always running. You do
> not need to start it manually.

**Connection string:** use the Azure SQL fully qualified domain name as `host`:
```toml
host = "myserver.database.windows.net"
port = 1433
```

**Authentication:** Azure SQL supports SQL authentication (username/password) and
Azure AD authentication. rustcdc currently supports SQL authentication.

### Amazon RDS for SQL Server

CDC is available on RDS SQL Server Enterprise and Standard editions. Enable it via
RDS-specific stored procedures:

```sql
EXEC msdb.dbo.rds_cdc_enable_db 'mydb';
```

Then enable per-table CDC using the standard `sp_cdc_enable_table` procedure.

> **Note:** on RDS, the SQL Server Agent jobs are managed by AWS. You do not need
> to start them manually.

---

## 5. Supported topologies

### Standalone instance

Standard setup. One connector per database.

### Always On Availability Groups

SQL Server CDC change tables exist on the primary replica only. The connector
must connect to the primary listener endpoint.

After a failover:
1. The new primary already has the CDC change tables (they are replicated as part
   of the AG database).
2. Update `source.host` to the AG listener DNS name (this usually stays constant
   across failovers).
3. Restart the connector. It resumes from the last confirmed LSN.

### Multiple pipelines against the same database

Each pipeline must use distinct `table_include_list` entries or distinct CDC
capture instances. Two connectors polling the same change table with the same LSN
will produce duplicate events.

---

## 6. Configuration reference

```toml
[source]
type     = "sqlserver"   # or "mssql"

# Connection
host     = "localhost"
port     = 1433
user     = "cdc_user"
password = { env = "MSSQL_PASSWORD" }
database = "mydb"
instance_name = "SQLEXPRESS"       # optional — named instances (host\instance)
conn_timeout_secs = 15

# CDC objects
cdc_enabled = true                 # verify CDC is enabled on the database at connect time
cdc_schema  = "cdc"                # schema where CDC change tables and functions live

# Polling
stream_poll_interval_ms = 500
max_events_per_poll     = 1000
prereq_pool_size        = 4        # metadata connection pool size

# TRUNCATE capture (opt-in): installs a database-level DDL trigger + shadow
# table, because SQL Server CDC alone cannot log TRUNCATE
capture_truncate_events = false

# Table filtering (exact "schema.table" names; include takes precedence — no globs)
table_include_list = ["dbo.orders", "dbo.customers"]
table_exclude_list = []

# Transport — SQL Server negotiates TLS by default
[source.transport]
mode = "tls"
# ca_cert_path               = "/etc/ssl/ca.pem"   # custom CA; system store if absent
# allow_invalid_certificates = false               # dev/test only (self-signed certs)
# allow_invalid_hostnames    = false               # dev/test only
```

### Field details

| Field | Default | Description |
|---|---|---|
| `host` / `port` / `user` / `password` / `database` | — | Connection parameters; `password` accepts `{ env = "VAR" }` |
| `instance_name` | none | Named SQL Server instance (`host\instance`) |
| `conn_timeout_secs` | — | TCP connection timeout in seconds |
| `cdc_enabled` | — | **Required.** Verify database-level CDC is enabled at connect time |
| `cdc_schema` | — | Schema where CDC change tables and functions live (usually `cdc`) |
| `stream_poll_interval_ms` | — | Milliseconds between change table polls |
| `max_events_per_poll` | — | Maximum rows fetched per poll per table |
| `prereq_pool_size` | — | Number of concurrent metadata connections |
| `capture_truncate_events` | `false` | Opt-in TRUNCATE capture via DDL trigger + shadow table; the connector reports the `truncate` capability only when enabled |
| `table_include_list` | empty (= all CDC-enabled) | Exact `schema.table` names; takes precedence over the exclude list |
| `table_exclude_list` | empty | Exact `schema.table` names to suppress; ignored when the include list is non-empty |

---

## 7. Monitoring

| Metric | Type | Description |
|---|---|---|
| `rustcdc_runtime_health{verdict=…}` | gauge (one-hot) | `healthy` \| `idle` \| `stalled` \| `not_running` — alert on `stalled` |
| `rustcdc_source_consecutive_poll_errors` | gauge | CDC polling errors since last success |
| `rustcdc_runtime_checkpoint_age_ms` | gauge | Age of last durable checkpoint (milliseconds) |
| `rustcdc_runtime_events_committed_total` | counter | Total events acknowledged and checkpointed |

---

## 8. Behavior when things go wrong

### CDC not enabled on table

**Symptom:** connector starts but no events appear for a specific table, or
startup fails with a message about a missing capture instance.

**Resolution:**
```sql
USE mydb;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders', @role_name = NULL;
```

### CDC cleanup has removed old LSNs

**Symptom:** connector fails or skips events because the stored LSN is older than
the oldest available LSN in the change tables.

**Cause:** the SQL Server CDC cleanup job has purged old change table rows, and
the connector's checkpoint points to a LSN that no longer exists.

**Resolution:** the stored LSN is unrecoverable — re-snapshot to get back in
sync. Remove the checkpoint state (for `local_fs`: delete the `checkpoint/`
directory under `state.dir`; for `kafka_topic`: re-seed with
`rustcdc init-state --config-file cdc.toml --force`), then start with
`snapshot_tables` configured so the tables are re-read before streaming.

To prevent this, adjust the CDC retention period:
```sql
EXEC sys.sp_cdc_change_job
    @job_type = N'cleanup',
    @retention = 14400;   -- 14400 minutes = 10 days (default: 4320 = 3 days)
```

### SQL Server Agent not running

**Symptom:** CDC change tables exist but contain no rows even though the source
table is being modified.

**Cause:** the SQL Server Agent (which runs the CDC capture job) is stopped.

**Resolution:** start SQL Server Agent (see [Step 1](#step-1--enable-sql-server-agent)).

### TLS certificate errors in dev environments

**Symptom:** connection fails with a TLS certificate error in a local or
containerised environment (self-signed server certificate).

**Resolution:** for development only:
```toml
[source.transport]
mode = "tls"
allow_invalid_certificates = true
```

Do not set this in production — provide the CA instead via
`transport.ca_cert_path`.

---

## See also

- [Getting started](../getting-started.md)
- [Core concepts](../concepts.md)
- [Configuration reference](../configuration.md)
- [Operations guide](../operations.md)
