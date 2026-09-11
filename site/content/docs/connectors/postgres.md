+++
title = "PostgreSQL connector"
description = "Capture PostgreSQL changes with logical replication: publications, replication slots, REPLICA IDENTITY, unchanged TOAST and managed cloud databases."
weight = 51
+++

The rustcdc PostgreSQL connector reads every committed row-level change from a
PostgreSQL database and streams it as a structured event to your configured sink.
It uses **logical replication** — PostgreSQL's native change-data-capture
mechanism — so it captures changes in real time without polling, table triggers,
or additional plugins.


## 1. Overview

PostgreSQL's **logical decoding** feature (introduced in PostgreSQL 9.4) streams
committed transactions from the write-ahead log (WAL) in a structured format.
rustcdc attaches to a **replication slot** and reads events via the built-in
`pgoutput` logical decoding plugin (available in every PostgreSQL ≥ 10 with no
additional installation required).

### What is captured

| Change type | Captured | Notes |
|---|---|---|
| `INSERT` | yes | Full `after` row image |
| `UPDATE` | yes | `after` always; `before` requires `REPLICA IDENTITY FULL` or `USING INDEX` |
| `DELETE` | yes | `before` requires `REPLICA IDENTITY FULL` or `USING INDEX`; otherwise primary key only |
| `TRUNCATE` | yes | PostgreSQL ≥ 11; `pgoutput` only |
| DDL changes | no | Logical decoding does not expose DDL; schema changes must be handled out-of-band |

### Minimum requirements

| Requirement | Minimum |
|---|---|
| PostgreSQL | 12 |
| `wal_level` | `logical` |
| User privilege | `REPLICATION` + `SELECT` on target tables |


## 2. How the connector works

### Initial snapshot

When `snapshot_tables` is configured (top-level in `cdc.toml`, or via
`rustcdc run --snapshot-table schema.table`), the connector performs an initial
snapshot before streaming:

1. Opens a consistent `REPEATABLE READ` transaction
2. Records the current WAL LSN as the snapshot boundary
3. Performs a full table scan for each table in `snapshot_tables`, emitting
   one `read` event per row
4. Commits the transaction and records the snapshot LSN as the checkpoint
5. Switches to streaming mode from that LSN (snapshot-to-stream handoff)

Without `snapshot_tables`, the connector starts streaming immediately from the
slot position — existing rows are not re-read.

If the connector is interrupted during the snapshot, it restarts the snapshot
from the beginning. Only a fully completed snapshot advances the checkpoint.

### Incremental snapshot (non-blocking)

`[incremental_snapshot]` backfills with the **DBLog watermark algorithm** instead
of a blocking scan:

```toml
[incremental_snapshot]
tables     = ["public.orders", "public.customers"]
chunk_size = 5000
```

Streaming starts immediately, and each chunk is a keyset-paginated `SELECT`
bracketed by low/high watermarks written into the WAL. Rows the stream also
delivered inside the override window are reconciled by row identity, so a row
changed mid-chunk is not delivered twice with stale content.

Two consequences matter operationally:

* **The replication slot is not held open for the length of a table scan.** A
  blocking snapshot of a large table keeps the slot from advancing, so WAL
  accumulates on the primary for the whole scan.
* **A restart resumes mid-backfill.** Chunk cursors travel inside the connector
  checkpoint offset, in the same atomic, fsynced, checksummed write as the stream
  position — a cursor is only meaningful relative to the position it was captured
  against, and two separately-written records could disagree after a crash.

`snapshot_tables` and `incremental_snapshot.tables` are mutually exclusive; the
loader rejects both, because every listed table would otherwise be read twice and
the duplicate would look like genuine change data downstream.

### Streaming

After the snapshot, the connector streams changes by reading from the replication
slot:

- Each committed transaction's WAL records are decoded by `pgoutput` and
  forwarded to the connector
- The connector converts them into the rustcdc event envelope (see
  [event model](@/docs/concepts.md#1-event-model))
- After a batch is delivered to the sink, the connector acknowledges the LSN to
  PostgreSQL, allowing WAL segments to be reclaimed

### WAL transport — how the stream is read

PostgreSQL offers two ways to consume a logical replication slot, and they are not
equivalent. `source.postgres.wal_transport` selects between them.

| Value | Mechanism | When |
|---|---|---|
| `"streaming_replication"` *(default)* | `START_REPLICATION ... LOGICAL` over the streaming replication protocol — what `pg_recvlogical` and PostgreSQL's own subscribers use | Always, unless one of the constraints below applies |
| `"sql_peek"` | `pg_logical_slot_peek_binary_changes()` over an ordinary SQL connection | Fallback for environments that cannot grant a replication connection |

Under `streaming_replication` the server **pushes** WAL as it is written over a
long-lived connection, and progress is reported with Standby Status Updates. Latency
is not bounded by `stream_poll_interval_ms`.

`sql_peek` is slower by construction, and the cost grows with the workload rather
than staying constant. The peek is non-consuming: PostgreSQL begins decoding at the
slot's `restart_lsn` and only emits past `confirmed_flush_lsn`, so **any long-running
transaction on the source pins `restart_lsn` and every poll re-reads the WAL between
the two**. Delivery latency is also bounded by the poll interval rather than pushed
by the server. Selecting it logs a warning at startup — including from
`validate-config` and `dry-run`, so a config review catches it before a deploy does.

Reach for `sql_peek` only when you cannot fix the environment:

- a managed service that withholds the `REPLICATION` attribute from the application role
- a connection routed through a pooler in transaction-pooling mode, which cannot carry
  a replication stream

```toml
[source.postgres]
wal_transport = "streaming_replication"   # or "sql_peek"
```

> **Out-of-band slot operations need the pipeline stopped.** Under
> `streaming_replication` a walsender holds the slot for the life of the stream, and
> PostgreSQL refuses `pg_replication_slot_advance` or `pg_drop_replication_slot` on an
> active slot. Stop the pipeline first; an operator script that runs alongside a live
> one fails with *"replication slot is active for PID N"*. This did not apply under
> `sql_peek`, where nothing held the slot persistently.

> **TLS is now enforced, not preferred.** A connector configured with
> `transport.mode = "tls"` against a server with `ssl = off` fails to connect instead
> of silently falling back to an unencrypted connection. Either enable TLS on the
> server or set `mode = "plaintext"` explicitly.

### Offset / LSN

The checkpoint stored by rustcdc is the WAL **Log Sequence Number (LSN)**, for
example `0/1B2C3D4E`. On restart, the connector sends this LSN to the replication
slot. PostgreSQL will replay all uncommitted WAL segments from that LSN forward.

> **Important:** never drop a replication slot while the connector is stopped.
> Doing so discards WAL and the connector cannot resume without a new snapshot.

### Replica identity

`REPLICA IDENTITY` controls how much of the old row is included in `UPDATE` and
`DELETE` events:

| Setting | `before` for UPDATE | `before` for DELETE | Notes |
|---|---|---|---|
| `DEFAULT` | primary key columns only | primary key columns only | Default for all tables |
| `FULL` | all columns | all columns | Highest fidelity; increases WAL size |
| `USING INDEX` | indexed columns | indexed columns | Compromise: specific unique key |
| `NOTHING` | empty | empty | Avoid unless truly needed |

Change it with:

```sql
ALTER TABLE public.orders REPLICA IDENTITY FULL;
```

### Unchanged TOAST columns (partial after-images)

PostgreSQL stores large values (roughly > 8 KB: `text`, `bytea`, `jsonb`) out of
line ("TOAST"). When an `UPDATE` does not modify such a value, PostgreSQL omits it
from the WAL record entirely — **under every `REPLICA IDENTITY` setting**. Replica
identity governs the *old* tuple only; it does not and cannot make the after-image
complete.

The connector surfaces this precisely instead of papering over it: affected column
names are listed in the event's `unavailable_columns` (holes in `after`) and
`before_unavailable_columns` (holes in `before`). An absent column is **not**
`NULL` — consumers must exclude listed columns from any write they build from the
payload. See [Core concepts — partial row images](@/docs/concepts.md#partial-row-images-unchanged-toast).


## 3. Setting up PostgreSQL

### Step 1 — enable logical replication

Add to `postgresql.conf` (requires a server restart if changing from a lower level):

```ini
# postgresql.conf
wal_level            = logical
max_replication_slots = 4     # one slot per rustcdc pipeline
max_wal_senders      = 4      # one per replication connection
```

Verify:

```sql
SHOW wal_level;       -- must return 'logical'
SELECT pg_reload_conf();
```

> **Note:** `ALTER SYSTEM SET wal_level = logical;` + `SELECT pg_reload_conf();`
> is not sufficient — changing `wal_level` always requires a **server restart**.

### Step 2 — create a replication user

Create a dedicated user with the minimum required privileges:

```sql
-- Create the CDC user
CREATE USER cdc_user WITH REPLICATION LOGIN PASSWORD 'changeme';

-- Grant SELECT on current and future tables in the schema
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO cdc_user;

```

> **Principle of least privilege:** `REPLICATION` and `SELECT` are the only
> privileges the connector needs. Do not grant `SUPERUSER`.

### Step 3 — configure `pg_hba.conf`

Allow the replication connection from the rustcdc host:

```
# pg_hba.conf
# TYPE   DATABASE    USER       ADDRESS         METHOD
host     replication cdc_user   10.0.0.0/24     scram-sha-256
```

Reload after changes:

```bash
pg_ctl reload
# or
psql -c "SELECT pg_reload_conf();"
```

### Step 4 — create the publication

A **publication** defines which tables are included in the replication stream.
Create it manually for maximum control:

```sql
-- Capture specific tables:
CREATE PUBLICATION cdc_pub FOR TABLE public.orders, public.customers;

-- Or capture all tables in a schema (requires PostgreSQL 15+):
CREATE PUBLICATION cdc_pub FOR TABLES IN SCHEMA public;

-- Or capture all tables in the database:
CREATE PUBLICATION cdc_pub FOR ALL TABLES;
```

> The publication must exist before the connector starts — rustcdc validates it
> at connect time and does not create publications. Creating it manually keeps
> the CDC user at `REPLICATION` + `SELECT` privileges.

### Step 5 — set replica identity (optional but recommended)

For full `before` images on updates and deletes:

```sql
-- Apply to specific tables:
ALTER TABLE public.orders     REPLICA IDENTITY FULL;
ALTER TABLE public.customers  REPLICA IDENTITY FULL;

-- Bulk-apply to all tables in a schema (PostgreSQL 15+, as superuser):
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' LOOP
    EXECUTE format('ALTER TABLE public.%I REPLICA IDENTITY FULL', r.tablename);
  END LOOP;
END $$;
```

### Step 6 — validate

Create the replication slot out of band (the production posture — the connector
only creates it when `create_replication_slot_if_missing = true` is explicitly
set):

```sql
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');
-- PostgreSQL 17+, for HA setups (see failover_slot below):
-- SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput', false, false, true);
```

```bash
# Confirm the slot exists:
SELECT slot_name, active FROM pg_replication_slots;

# Confirm publication exists:
SELECT pubname, puballtables FROM pg_publication;

# Confirm user has replication privilege:
SELECT usename, userepl FROM pg_user WHERE usename = 'cdc_user';
```


## 4. Cloud databases

### Amazon RDS for PostgreSQL

1. Set `rds.logical_replication = 1` in the parameter group (triggers a restart).
2. Verify `SHOW wal_level;` returns `logical`.
3. Grant the `rds_replication` role to the CDC user:
   ```sql
   GRANT rds_replication TO cdc_user;
   ```
4. In `cdc.toml`, no special settings are needed beyond normal PostgreSQL config.
   The connector uses `pgoutput`, which is available on all RDS versions ≥ 10.

> **RDS Multi-AZ:** replication slots are available on the primary only. After a
> failover the slot is gone on the new primary — a data-loss event. Re-create the
> slot (`SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');`) and
> re-snapshot to repair the gap. On PostgreSQL 17+ consider failover-enabled
> slots (`failover_slot = true`) with slot synchronization configured.

### Azure Database for PostgreSQL — Flexible Server

1. Enable logical replication via the Azure Portal or CLI:
   ```bash
   az postgres flexible-server parameter set \
     --resource-group mygroup \
     --server-name myserver \
     --name wal_level \
     --value logical
   az postgres flexible-server restart --resource-group mygroup --name myserver
   ```
2. Grant the `azure_pg_admin` role, or use a user with `REPLICATION` privilege
   (available in Flexible Server; not available in Single Server).

### Cloud SQL for PostgreSQL (GCP)

1. Enable the `cloudsql.logical_decoding` flag in Cloud SQL:
   ```bash
   gcloud sql instances patch myinstance --database-flags cloudsql.logical_decoding=on
   ```
   This automatically sets `wal_level = logical`.
2. Create a replication user in the `cloudsqlsuperuser` role:
   ```sql
   CREATE USER cdc_user WITH REPLICATION IN ROLE cloudsqlsuperuser LOGIN PASSWORD 'changeme';
   ```


## 5. Supported topologies and HA

### Standalone primary

The simplest topology. The connector maintains one replication slot on the
primary. No special configuration is needed.

### Primary + replicas (streaming replication)

Logical replication slots exist only on the **primary**. The connector must
always connect to the primary. Physical standby servers do not have independent
logical slots.

If the primary fails:

1. Promote a standby to primary.
2. Re-create the replication slot (`rustcdc init-state --drop-existing`).
3. Update `source.host` in `cdc.toml` and restart the connector.

> Some managed services (RDS Multi-AZ, Azure HA) replicate slots automatically.
> Verify with your provider before assuming automatic slot failover.

### Multiple pipelines against the same database

Each pipeline instance must use a **unique** `slot_name` and a dedicated
`publication`. Sharing a replication slot between two connector instances causes
silent data loss (each event is delivered to only one consumer).

```toml
# Pipeline A
[source]
slot_name   = "cdc_slot_pipeline_a"
publication_name      = "cdc_pub_pipeline_a"

# Pipeline B (separate config file)
[source]
slot_name   = "cdc_slot_pipeline_b"
publication_name      = "cdc_pub_pipeline_b"
```


## 6. WAL disk space

Replication slots cause PostgreSQL to **retain WAL segments** until the slot
consumer has acknowledged them. An idle or stopped connector causes unbounded WAL
growth.

### Monitoring

```sql
SELECT slot_name,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal,
       active,
       confirmed_flush_lsn
FROM pg_replication_slots;
```

Alert when `retained_wal` exceeds a threshold (e.g., 10 GiB).

### Mitigation

1. **Keep the connector running** — the most effective protection. The connector
   also advances the slot periodically while the database is idle
   (`slot_idle_advance_interval_ms`, default 30 s), so a quiet database does not
   pin WAL.
2. **Cap slot WAL retention** (PostgreSQL 13+) in `postgresql.conf`:
   ```ini
   max_slot_wal_keep_size = 10GB
   ```
   When the limit is exceeded, PostgreSQL **invalidates the slot** rather than
   filling the disk. Treat that as a data-loss event: the retained WAL is gone,
   and the pipeline must re-snapshot. Alert well before the cap using the
   `rustcdc_runtime_replication_slot_lag_bytes` gauge (see
   `RUSTCDCReplicationSlotLagHigh` in the shipped alert rules).
3. **Drop idle slots** if a pipeline is permanently retired:
   ```sql
   SELECT pg_drop_replication_slot('cdc_slot');
   ```

### High-traffic vs. low-traffic databases

If the connector captures a low-traffic database on a server that also hosts a
high-traffic database, the LSN may advance slowly for the monitored database,
causing WAL retention even when the connector is running. Use a heartbeat table
to force regular LSN advancement:

```sql
CREATE TABLE public._cdc_heartbeat (ts timestamptz DEFAULT now());
```

```toml
# cdc.toml
[source]
table_include_list = ["public.orders", "public._cdc_heartbeat"]
```

Periodically insert into the heartbeat table from a cron job or the admin API's
`log_marker` signal.


## 7. Configuration reference

```toml
[source]
require_primary = true           # reject startup if connected to a replica (default: true)

[source.postgres]
# Connection
host     = "localhost"                       # required
port     = 5432                              # required
user     = "cdc_user"                        # required
password = { env = "POSTGRES_PASSWORD" }     # required; never hardcode
database = "mydb"                            # required
conn_timeout_secs = 10

# Replication identifiers (both required)
publication_name      = "cdc_pub"       # name of the PostgreSQL publication
replication_slot_name = "cdc_slot"      # name of the replication slot

# Slot lifecycle
create_replication_slot_if_missing = false   # true only for first-time provisioning / ephemeral envs
failover_slot                      = false   # PostgreSQL 17+: create failover-enabled slots
slot_idle_advance_interval_ms      = 30000   # idle slot advance; 0 disables (not recommended)

# WAL transport
wal_transport = "streaming_replication"   # streaming_replication | sql_peek

# Polling
stream_poll_interval_ms = 100
max_events_per_poll     = 1000

# Table filtering (exact "schema.table" names; include takes precedence)
table_include_list = ["public.orders", "public.customers"]
table_exclude_list = []

# Transport
[source.postgres.transport]
mode = "plaintext"    # plaintext | tls
# For mode = "tls":
# ca_cert_path               = "/etc/ssl/ca.pem"       # custom CA; system store if absent
# client_cert_path           = "/etc/ssl/client.pem"   # mTLS (with client_key_path)
# client_key_path            = "/etc/ssl/client-key.pem"
# allow_invalid_certificates = false                   # local testing only
```

### Field details

| Field | Default | Description |
|---|---|---|
| `host` / `port` / `user` / `password` / `database` | — | Connection parameters; `password` accepts `{ env = "VAR" }` or a literal string (not recommended) |
| `publication_name` | — | Publication name; must exist before startup |
| `replication_slot_name` | — | Replication slot name; provision out of band, or set `create_replication_slot_if_missing = true` |
| `create_replication_slot_if_missing` | `false` | Allow the connector to create a missing slot. Keep `false` in production: a vanished slot is a data-loss event, and recreating it silently resumes from "now" and skips everything in between |
| `failover_slot` | `false` | PostgreSQL 17+: create the slot with `failover = true` so it is synchronized to standbys and capture survives promotion. Only applies when the connector creates the slot; requires cluster-side sync configuration |
| `slot_idle_advance_interval_ms` | `30000` | Advance the slot when no committed events arrive so PostgreSQL can recycle WAL. `0` disables (not recommended for long-lived streams) |
| `wal_transport` | `"streaming_replication"` | How the WAL stream is read. `"sql_peek"` is the fallback for a role without `REPLICATION` or a connection that must route through a pooler — see [WAL transport](#wal-transport-how-the-stream-is-read) for the cost |
| `stream_poll_interval_ms` | — | Stream poll interval; under `streaming_replication` the server pushes, so this bounds the idle backstop rather than delivery latency |
| `max_events_per_poll` | — | Maximum events yielded per poll cycle |
| `table_include_list` | empty (= all) | Exact `schema.table` names to capture; takes precedence over the exclude list |
| `table_exclude_list` | empty | Exact `schema.table` names to suppress; ignored when the include list is non-empty |
| `require_primary` | `true` | (`[source]` level) Fail at startup if the server is a replica |


## 8. Monitoring

The admin API (`/metrics`) exposes Prometheus metrics. Key PostgreSQL-specific
metrics:

| Metric | Type | Description |
|---|---|---|
| `rustcdc_runtime_health{verdict=…}` | gauge (one-hot) | Derived health verdict: `healthy` \| `idle` \| `stalled` \| `not_running`. Exactly one series is 1. Alert on `verdict="stalled"` — `idle` is deliberately not alertable (a quiet database is not an incident) |
| `rustcdc_runtime_stall_cause{cause=…}` | gauge | Present only while stalled: `unconfirmed_source_position` \| `poll_loop_not_turning` \| `consumer_not_acknowledging`. Route the page on this |
| `rustcdc_runtime_poll_age_ms` | gauge | Milliseconds since the last poll returned, empty batches included — the poll loop's own liveness |
| `rustcdc_runtime_delivery_age_ms` | gauge | Milliseconds since events last arrived. High on its own just means the source is quiet — do not alert on it |
| `rustcdc_runtime_replication_slot_lag_bytes` | gauge | Slot WAL lag (`pg_current_wal_lsn - confirmed_flush_lsn`). Unbounded growth risks slot invalidation |
| `rustcdc_runtime_events_skipped_total` | counter | Events permanently dropped by `transform_error_policy = "skip"`. **Any increase is data loss** |
| `rustcdc_source_consecutive_poll_errors` | gauge | Replication stream errors since last success; resets on reconnect |
| `rustcdc_runtime_checkpoint_age_ms` | gauge | Age of the last durable checkpoint (milliseconds) |
| `rustcdc_runtime_events_committed_total` | counter | Total events acknowledged and checkpointed |
| `rustcdc_runtime_recoverable_breaker_open_total` | counter | Number of times the circuit breaker has opened |

SLO alert rules are in
[`monitoring/rustcdc_slo_alerts.yml`](https://github.com/hupe1980/rustcdc/blob/main/monitoring/rustcdc_slo_alerts.yml).
`RUSTCDCRuntimeStalled`, `RUSTCDCReplicationSlotLagHigh`,
`RUSTCDCEventsSkippedDataLoss`, `RUSTCDCCheckpointAgeHigh`, and
`RUSTCDCReadinessRateLow` are most relevant for PostgreSQL replication health.


## 9. Behavior when things go wrong

### Replication slot missing on restart

**Symptom:** connector fails at startup with an error like
`replication slot "cdc_slot" does not exist`.

**Cause:** the slot was dropped (manually or by a DBA), lost in a failover to a
standby that never had it, or invalidated by `max_slot_wal_keep_size`.

**This is a data-loss event** — the WAL the slot was retaining is gone, and this
failure is deliberate: recreating the slot silently would resume capture at the
current WAL position and skip everything in between, which looks exactly like
healthy operation.

**Resolution:**
1. Re-create the slot: `SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');`
   (or start once with `create_replication_slot_if_missing = true`, then revert it).
2. Re-snapshot the affected tables so the gap is repaired, or accept the gap
   explicitly if the data is reproducible downstream.
3. Verify the slot exists: `SELECT * FROM pg_replication_slots WHERE slot_name = 'cdc_slot';`
4. To survive failovers on PostgreSQL 17+, create the slot with `failover = true`
   (`failover_slot = true` when the connector provisions it) and configure slot
   synchronization on the standby.

### WAL lag / disk pressure

**Symptom:** disk usage grows continuously; `pg_replication_slots.retained_wal`
is large.

**Cause:** the connector is stopped or lagging behind.

**Resolution:**
1. Ensure the connector is running.
2. Check consumer throughput: if the sink is slow, increase `batch_max_events`
   or `prepare_parallelism`.
3. If the connector is permanently retired, drop the slot:
   ```sql
   SELECT pg_drop_replication_slot('cdc_slot');
   ```

### `require_primary` rejection

**Symptom:** connector exits immediately with
`source requires a primary server, but connected to a replica`.

**Cause:** `source.host` points to a replica, and `require_primary = true` (default).

**Resolution:** update `source.host` to point to the primary, or set
`require_primary = false` if you intentionally want to capture from a replica
(read-only; WAL LSN may lag).

### `publication` not found

**Symptom:** connector fails with `publication "cdc_pub" does not exist`.

**Resolution:** create it — the connector never creates publications:
```sql
CREATE PUBLICATION cdc_pub FOR TABLE public.orders, public.customers;
```
