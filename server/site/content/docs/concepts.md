+++
title = "Core concepts"
description = "The rustcdc event model, delivery contracts, checkpoints and circuit breaker — what each guarantees, and exactly where the limits are."
weight = 20
+++

Four ideas carry most of the weight, and the rest of the documentation assumes them: the
**event envelope** every source produces, the **delivery contract** that says what a crash
can and cannot do to your data, the **checkpoint** that makes restarts resumable, and the
**circuit breaker** that decides when to stop trying.

Each section below states the guarantee first and its limits immediately after. Where a
limit exists it is named here rather than left for production to reveal.


## 1. Event model

Every database change — an `INSERT`, `UPDATE`, `DELETE`, snapshot read, or
schema change — is represented as a single JSON event. Events are ordered within
a pipeline and carry enough information for consumers to reconstruct the full
change history.

### Event envelope

```json
{
  "before":  { "id": 42, "status": "pending" },
  "after":   { "id": 42, "status": "shipped" },
  "op":      "update",
  "source":  { "source_name": "postgres", "offset": "0/1B2C3D4E", "timestamp": 1784583185586 },
  "ts":      1784583185586,
  "schema":  "public",
  "table":   "orders",
  "primary_key": ["id"],
  "snapshot": null,
  "transaction": { "tx_id": 3084, "total_events": 1, "event_index": 0 },
  "envelope_version": 1,
  "before_is_key_only": false
}
```

### Field reference

| Field | Type | Always present | Description |
|---|---|---|---|
| `before` | object \| null | no | Row image **before** the change — present for `update` and `delete` when the source is configured for full replica identity |
| `after` | object \| null | no | Row image **after** the change — present for `insert`, `update`, and `read` |
| `op` | string | yes | `insert` \| `update` \| `delete` \| `read` \| `schema_change` \| `truncate` |
| `source` | object | yes | Source identity and durable position: `source_name`, `offset` (WAL LSN / binlog position — used for checkpointing), `timestamp` (source-side ms since epoch) |
| `ts` | u64 | yes | Event timestamp — milliseconds since epoch |
| `schema` | string \| null | no | Source schema (PostgreSQL) or database (MySQL/MSSQL), when the source provides one |
| `table` | string | yes | Source table name |
| `primary_key` | string[] \| null | no | Primary-key column names, when available |
| `snapshot` | object \| null | no | Snapshot metadata present only during an initial table scan |
| `transaction` | object \| null | no | Transaction metadata (`tx_id`, `total_events`, `event_index`) when the event belongs to a transaction |
| `envelope_version` | u16 | yes | Canonical envelope version for compatibility checks |
| `before_is_key_only` | bool | yes | `true` when `before` holds only primary-key columns (PostgreSQL `REPLICA IDENTITY DEFAULT`) — `before` is then not a complete row snapshot |
| `unavailable_columns` | string[] | omitted when empty | Columns that exist on the table but are **absent** from `after` because the source could not supply them (PostgreSQL unchanged-TOAST). Absent ≠ `NULL` — see below |
| `before_unavailable_columns` | string[] | omitted when empty | Same, for the `before` image. The two sets are tracked separately and are **not** the same: a TOASTed column that *was* modified is present in `after` but absent from `before` |

### Operation types

| `op` | Source | Description |
|---|---|---|
| `insert` | WAL / binlog | A new row was inserted |
| `update` | WAL / binlog | An existing row was updated |
| `delete` | WAL / binlog | A row was deleted |
| `read` | snapshot | Row captured during initial snapshot; logically equivalent to `insert` for consumers |
| `schema_change` | DDL log | A table or column definition changed (MySQL/MSSQL only) |
| `truncate` | WAL / binlog / DDL trigger | The table was truncated. PostgreSQL (pgoutput) and MySQL/MariaDB (binlog query event) emit it natively; SQL Server requires `capture_truncate_events = true` |

### `before` field availability

The `before` field requires full row images at the source:

- **PostgreSQL** — `REPLICA IDENTITY FULL` on the table, or `USING INDEX` for a unique key. The default (`DEFAULT`) only includes primary key columns in `before` (the event then carries `before_is_key_only: true`).
- **MySQL / MariaDB** — `binlog_row_image = FULL` in `my.cnf`.
- **SQL Server** — full before-image is always available because CDC captures both old and new values.

### Partial row images (unchanged TOAST)

PostgreSQL omits large values (roughly > 8 KB: `text`, `bytea`, `jsonb`) from the WAL
record when an `UPDATE` does not modify them. The value cannot be recovered — reading it
back out-of-band would race concurrent writes — so the event ships without those columns
and lists their names in `unavailable_columns` (for `after`) and
`before_unavailable_columns` (for `before`).

> **This is the classic CDC corruption footgun.** An absent column is *not* `NULL`.
> A consumer that upserts full rows from `after` will write `NULL` (or the column
> default) over a value that never changed. Consumers must exclude the listed columns
> from the write — in SQL terms, `UPDATE … SET <present columns> WHERE <key>`, never an
> upsert built from the full column list.
>
> `REPLICA IDENTITY FULL` does **not** avoid this: replica identity governs the old
> tuple only, and the after-image omits unmodified TOASTed values under every setting.

The two lists are tracked per image and are never merged: a TOASTed column that *was*
modified arrives present in `after` and absent from `before`. Embedders consuming
events in Rust should prefer `Event::row_write()` (rustcdc ≥ 0.7.0), which folds the
availability lists and primary key into a single `Replace | Merge | Delete | Truncate |
None` decision so the corrupting write is not expressible.

Within `rustcdc-server`, sinks forward the full envelope (the lists travel with the
event), the Iceberg sink additionally materializes a `has_complete_after_image` column
for cheap data-quality queries, and transforms that add or rename payload columns
automatically reconcile the lists (see [Transforms](@/docs/transforms.md)).


## 2. Pipeline lifecycle

```
┌─────────────┐    ┌─────────────┐    ┌───────────────┐    ┌──────────┐
│  Source DB  │───▶│   Capture   │───▶│   Transform   │───▶│  Router  │
└─────────────┘    └─────────────┘    └───────────────┘    └────┬─────┘
                                                                  │
                         ┌────────────────────────────────────────┘
                         ▼
                   ┌──────────┐    ┌───────────────────┐
                   │   Sink   │───▶│  State backend    │
                   └──────────┘    │ (checkpoint +     │
                                   │  schema history)  │
                                   └───────────────────┘
```

### 1. Capture

Reads from the source database change stream:

- **PostgreSQL** — connects to a logical replication slot; receives events via the `pgoutput` plugin
- **MySQL / MariaDB** — acts as a replica and reads the binlog
- **SQL Server** — polls the SQL Server CDC change tables

The capture layer translates native change records into the rustcdc event envelope described above.

### 2. Transform

Optional processing applied to each event before delivery. Two modes:

- **Native rules** — declared in TOML; compiled into the binary; zero allocation overhead
- **WASM module** — arbitrary logic compiled to `.wasm`; runs in a bounded sandbox

See the [transform pipeline section](#4-transform-pipeline) for details.

### 3. Router

Matches each event's `(schema, table)` against the `[[pipeline.routes]]` configuration and
selects a named sink. Routes are evaluated top-to-bottom; the first match wins.
If no route matches, the event is delivered to the default sink (`[sink]`).

### 4. Sink

Delivers batches to the target system. The sink:

- Buffers events until either `sink_flush_interval_events` or `sink_flush_interval_events` / the sink's own batching knobs (e.g. HTTP `batch_max_delay_ms`) is reached
- Applies the configured delivery contract (see below)
- Records the source offset in the state backend after successful delivery

### 5. Checkpoint

After a batch is successfully delivered, rustcdc advances the checkpoint in the
state backend. On restart, capture resumes exactly from that checkpoint — no events
are missed, and the number of duplicates is bounded by the batch size.


## 3. Delivery contracts

The delivery contract controls exactly **when** the checkpoint advances relative
to sink delivery. Set at the top level of `cdc.toml`:

```toml
delivery_contract = "at_least_once"   # default
```

### `at_least_once` (default)

| Property | Value |
|---|---|
| Checkpoint advances | **after** successful delivery |
| Duplicates on restart | possible (one batch worth) |
| Data gaps | impossible |
| Compatible sinks | all |

This is the safe default. A crash between delivery and checkpoint write causes
the last batch to be redelivered. Idempotent consumers (Kafka deduplication,
upsert sinks) handle this transparently.

### `effectively_once`

| Property | Value |
|---|---|
| Batch atomicity at the sink | **guaranteed** — all of a batch commits, or none |
| Duplicates during steady-state retries | impossible |
| Duplicates after a crash | impossible |
| Data gaps | impossible |
| Compatible sinks | Kafka only |

Requires all of:

- `sink.type = "kafka"` with `sink.delivery_mode = "transactional"`
- a unique `sink.transactional_id` per pipeline
- `state.offset.backend = "kafka_topic"`, on the **same** Kafka cluster as the sink

The loader rejects any other combination rather than accepting it and degrading — see
[why the state backend is not optional](#why-the-checkpoint-has-to-live-in-kafka).

#### How it works

Each batch is one Kafka transaction, and the checkpoint is a record *inside* it:

1. begin the transaction
2. deliver the batch's records
3. write the checkpoint record — into the **same** transaction
4. commit

One `commit_transaction` makes the data and the position that describes it durable
together. A crash before step 4 discards both, so the batch replays cleanly from the
previous position. A crash after it keeps both, so the batch is not replayed at all. There
is no ordering in which one survives without the other, which is what makes this
exactly-once end to end and not merely atomic at the sink.

A delivery failure aborts the transaction, so a `read_committed` consumer never sees a
partial batch.

#### Why the checkpoint has to live in Kafka

The source position is a WAL LSN or a binlog coordinate, not a Kafka consumer offset, so
`sendOffsetsToTransaction` — the mechanism a Kafka-to-Kafka pipeline would use — does not
apply. The only way to make the position atomic with the data is to write it as an
ordinary record in the same transaction, and that requires it to live on the same cluster,
produced by the same producer.

Any other state backend is a second durability domain. The batch and the position would
commit separately, and a crash between the two would replay the batch — the window this
contract exists to avoid. That combination used to be accepted with the residual window
documented as a caveat; it is now a configuration error, because a guarantee an operator
has to read a paragraph to qualify is not a guarantee.

> [!NOTE]
> One consequence worth planning for: the checkpoint topic must be a compacted topic on
> the sink's own cluster. If your sink cluster and your operational-state cluster are
> different, `effectively_once` is not available and `at_least_once` is the honest choice.

#### What you still need to know

- **Ordering across a restart.** Exactly-once does not mean the pipeline cannot pause. A
  crash mid-batch replays that batch from the source; the records are produced again under
  a new producer epoch, but the aborted transaction means no consumer ever saw the first
  attempt.
- **Consumers must read `read_committed`.** A `read_uncommitted` consumer sees aborted
  records, and no producer-side guarantee can prevent that.
- **The reconciliation marker still runs.** It is written before delivery and cleared after
  the commit, and a marker surviving a restart tells you the crash landed mid-batch. It is
  now purely diagnostic — the transaction, not the marker, is what protects the data.


## 4. Transform pipeline

Transforms modify events in-flight before they reach the sink. They are applied
sequentially; all matching rules are applied in order.

### Native rules

Declared as `[[pipeline.transforms]]` in TOML. Each rule has an optional
`when` predicate (table / schema / op filter) and one or more `actions`.

**Available actions:**

| Action | Purpose |
|---|---|
| `filter` | Drop events that don't match `include_ops` / `include_tables` / `include_schemas` |
| `unwrap` | Replace `after` with `after[field]` |
| `flatten` | Merge keys from `after[field]` into `after` (with optional `prefix`) |
| `route` | Rewrite the event's routing `schema` and/or `table` |
| `metadata_projection` | Copy source metadata into `after[target_field]` |
| `key_shaping` | Materialise a deterministic key from `primary_key` or `fingerprint` into `after[target_field]` |
| `mask` | Redact, truncate, SHA-256, keyed HMAC-SHA256, or AES-256-GCM encrypt/decrypt fields by dotted path |
| `field_mapping` | Copy / rename / set / remove fields, optionally `strict` |
| `outbox` | Unwrap the transactional-outbox pattern into the domain event it carries |

**`when` predicate fields:**

| Field | Type | Example |
|---|---|---|
| `tables` | string[] | `["public.orders", "public.cust*"]` (glob) |
| `schemas` | string[] | `["public", "billing"]` |
| `ops` | string[] | `["insert", "update"]` |

An empty `when` block matches every event.

**Example — redact a column and add metadata:**

```toml
[[pipeline.transforms]]
name = "redact-email"

  [pipeline.transforms.when]
  tables = ["public.customers"]
  ops    = ["insert", "update", "read"]

  [[pipeline.transforms.actions]]
  type           = "filter"
  include_tables = ["public.customers"]

[[pipeline.transforms]]
name = "add-meta"

  [[pipeline.transforms.actions]]
  type         = "metadata_projection"
  target_field = "_meta"
  fields       = ["schema", "table", "operation", "source_timestamp", "offset"]
```

### WASM transforms

Compile any language to WebAssembly to express transforms that native rules
cannot cover. Supported languages include **Rust, AssemblyScript, TinyGo**, and
any other `wasm32-unknown-unknown` target.

**When to use WASM over native rules:**

| Use case | Native rules | WASM |
|---|---|---|
| Drop / filter events | ✅ | ✅ |
| Redact / rename fields | ✅ | ✅ |
| Keyed pseudonymisation (HMAC) / field encryption (AES-GCM) | ✅ | ✅ |
| Transactional-outbox unwrapping | ✅ | ✅ |
| Arbitrary enrichment logic | ❌ | ✅ |
| External data lookups (in-memory cache) | ❌ | ✅ |
| Complex routing decisions | ❌ | ✅ |

```toml
[pipeline.transform_runtime]
mode = "wasm"

  [pipeline.transform_runtime.wasm]
  module_path        = "/etc/rustcdc/transform.wasm"
  timeout_ms         = 50
  max_memory_bytes   = 8388608   # 8 MiB
  instance_pool_size = 4

  # Optional: cooperative yield (fuel budget before yielding)
  fuel_yield_interval = 10000
```

The module must export:

```
transform(ptr: i32, len: i32) -> i64
```

- Input: JSON event bytes at `ptr`/`len`
- Output: `0` to drop the event, or `(out_ptr << 32) | out_len` to emit a modified event
- See [Writing WASM transforms](@/docs/transforms.md) for the full ABI contract and authoring guide

**Transform error policy:**

```toml
[runtime]
transform_error_policy = "halt"   # halt (default) | skip
```

- `halt` — pipeline stops on transform error (safe default; prevents silent data loss)
- `skip` — logs the error, drops the event, continues


## 5. State backends

rustcdc persists two things in the state backend:

| Item | Purpose |
|---|---|
| **Checkpoint** | Last confirmed source offset (WAL LSN / binlog position). Used to resume after a restart. |
| **Schema history** | Historical DDL snapshots. Needed by MySQL/MariaDB to reconstruct event schemas after restarts. |

### Backends at a glance

| Backend | HA | Notes |
|---|---|---|
| `local_fs` | no | Default. Simple files under a configurable directory. |
| `kafka_topic` | yes | Compacted Kafka topic. Suitable for Kubernetes. |
| `postgresql` | yes | Stores state in a dedicated PostgreSQL table. |
| `redis` | yes | Fast; suitable for low-latency pipelines. |

### Migrating between backends

Use `rustcdc migrate-state` to move state without restarting from scratch:

```bash
rustcdc migrate-state \
  --source-backend local_fs \
  --source-dir /var/lib/rustcdc/old \
  --target-dir /var/lib/rustcdc/new \
  --output migration-report.json
```

See the [operations guide](@/docs/operations.md#6-migrate-state) for the full procedure.


## 6. Circuit breaker

When the source database becomes intermittently unavailable, rustcdc enters
exponential backoff and opens a circuit breaker:

```
          ┌────────────┐   consecutive errors ≥ threshold
          │   Closed   │──────────────────────────────────▶│
          │ (running)  │                                    │
          └────────────┘                              ┌─────▼──────┐
               ▲                                      │    Open    │
               │   probe succeeds                     │ (cooldown) │
               │                                      └─────┬──────┘
          ┌────┴────────┐ probe fails (max cycles)          │
          │  Half-open  │◀──────────────────────────────────┘
          │  (probing)  │
          └─────────────┘
                │ max_open_cycles exceeded
                ▼
           Error state → /livez → 503 → Kubernetes restart
```

**Key parameters** (all under `[runtime]`):

| Parameter | Default | Description |
|---|---|---|
| `recoverable_error_breaker_consecutive_threshold` | `10` | Errors before opening |
| `recoverable_error_breaker_cooldown_ms` | `30000` | Cooldown before probing |
| `recoverable_error_breaker_max_open_cycles` | `3` | Open cycles before escalating to Error state |
| `recoverable_error_backoff_initial_ms` | `100` | Initial retry backoff |
| `recoverable_error_backoff_max_ms` | `5000` | Maximum retry backoff |

After `max_open_cycles` open cycles, the pipeline transitions to `Error` state.
`/livez` returns 503, triggering a container restart in Kubernetes.

**These parameters govern both halves of the pipeline.** Source poll failures and sink
delivery failures share one policy and one breaker. That was not always true: sink
failures used to be unconditionally terminal, so a few-second broker leader election
became a process exit and a full replay from the last checkpoint while these settings
looked like they applied.

A sink retry re-delivers the **same batch**. Under `at_least_once` that is duplication,
which the contract permits; under `effectively_once` the aborted transaction discards
the partial batch, so a `read_committed` consumer sees only the successful attempt.

Two classes of failure are deliberately **not** retried:

* **Events over `runtime.max_event_bytes`.** The same event is the same size on every
  attempt, so retrying is a loop that never makes progress and never reaches the events
  behind it.
* **Configuration errors.** Deterministic by nature.

One gap worth knowing: an error surfacing from a sink **flush** rather than a send is
currently treated as terminal even when it is transient. The router collapses per-sink
flush errors into a single string, which loses the classification. Sends are unaffected.
