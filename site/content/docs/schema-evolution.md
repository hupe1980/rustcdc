+++
title = "Schema evolution"
description = "How rustcdc captures DDL, records schema history, and what a consumer must tolerate when a table changes."
weight = 50
+++

This guide documents how rustcdc captures DDL, tracks schema history, and emits schema-change events.

## Audience

- Connector and runtime maintainers
- Integrators who rely on schema-aware downstream pipelines
- Operators planning schema migration rollouts

## Components

Schema evolution behavior spans two modules:

1. `rustcdc::ddl_capture` for source-specific DDL extraction and normalized parsing.
2. `rustcdc::schema_history` for versioned schema persistence and lookup.

## DDL Capture

### Supported Dialects

- PostgreSQL (`DdlDialect::Postgres`)
- MySQL (`DdlDialect::Mysql`)
- SQL Server (`DdlDialect::SqlServer`)

### Supported Operations

- `CREATE TABLE`
- `ALTER TABLE`
- `DROP TABLE`

### Core Types

- `CapturedDdl`
- `ParsedDdlStatement`
- `DdlOperation`
- `SchemaDiff` and `SchemaDiffOperation`
- `MysqlDdlExtractor`, `PostgresDdlExtractor`, `SqlServerDdlExtractor`

### Normalization Flow

1. Extract source DDL from connector message format.
2. Parse dialect-specific statement into a normalized representation.
3. Build `CapturedDdl` with operation, schema, table, statement, and optional `result_schema`/`schema_diff`.
4. Convert to canonical CDC event (`Operation::SchemaChange`) when needed.

## Schema History

### SchemaHistory Trait

`SchemaHistory` defines the storage contract:

- `record_ddl(ddl_id, ddl)` to append a schema mutation and return its version
- `get_schema_at_version` and `get_schema_at_timestamp` for point-in-time lookup
- `latest_schema` for current view
- `apply_retention` to prune old versions using explicit retention policy

#### `record_ddl` is idempotent on `ddl_id`

`ddl_id` is a stable identity for the entry, and a redelivered DDL returns the version it was
already assigned rather than appending a second one. Pass an empty string to opt out.

The runtime chooses it by kind:

| Entry | Identity | Why |
|---|---|---|
| A captured statement (`CREATE_TABLE`, `ALTER_TABLE`, `DROP_TABLE`) | the source log position | A table altered A → B → A has three entries, and the third is not the first |
| A row's shape | `shape:<schema>.<table>:<offset of that row>` | Per occurrence, so A → B → A records A again; stable under replay |

This is load-bearing, not a nicety. The runtime records a schema change *before* it enqueues
the event announcing it, so a crash between the record and the checkpoint commit replays the
DDL on restart. Without the identity check, replaying an `AlterTableDiff` re-applied its
operations to a schema that already had them — `ADD COLUMN` on a column that now exists —
which returns `SchemaError`, failed the poll, and failed identically on every subsequent
restart from the same checkpoint. The pipeline never started again.

A custom `SchemaHistory` implementation must honour the same contract.

#### Row shapes enter the history from the rows

A table that never undergoes DDL is in the history through its rows. The runtime records a
row's [shape](#every-row-carries-its-shape) when it differs from the table's latest entry: it
caches the id it last recorded per table, and on a miss — a fresh process — compares with
`latest_schema`, so a restart records nothing for an unchanged table. A statement for the
table clears the cache, and the next row is compared with the history again.

Any error while recording a row's shape stops the pipeline with `Error::Unrecoverable`: the
batch was already taken from the source, so a retried poll would read past it. A restart
redelivers it from the checkpoint. `FileSchemaHistory` rolls its in-memory state back when
persisting fails, so a retried record is written rather than answered from memory.

#### A rejected entry does not stop capture

If the history genuinely cannot accept a statement — an `ALTER TABLE` diff for a table the
store has never seen, which is what an `InMemorySchemaHistory` looks like after any restart —
the runtime logs at ERROR with the remedy and continues. Propagating it would fail the poll,
and fail identically on every restart, for a gap in an auxiliary index; the event itself is
self-describing and still reaches the consumer. A store that is *broken* rather than merely
inconsistent — an I/O failure, a lost owner lease — still fails the poll. Recording a row's
shape is stricter; see above.

Runtime-managed retention is available through `RuntimeConfig::with_schema_history_retention(...)`.
When configured, rustcdc applies the retention policy automatically after each persisted DDL mutation.
Runtime defaults enable bounded retention (`keep_last(256)` per table) to prevent unbounded growth.

### Built-In Backends

- `InMemorySchemaHistory`
  - Intended for tests, local development, and embedders that keep state in process memory
  - Tracks versioned schema state, timestamp lookup, and drop-table tombstones
  - Supports explicit retention pruning to bound history growth per table

- `FileSchemaHistory`
  - Durable local JSON backend for long-lived deployments
  - Uses write-rename persistence with file and directory fsync for crash-safe single-process durability
  - Every filesystem call, including both `fsync`s, runs on a blocking worker rather than on the caller's async executor
  - A recognised replay writes nothing, so a redelivered DDL costs no file rewrite
  - Writes with restrictive file permissions by default and uses unique temp-file creation with collision retries before atomic rename
  - Reloads schema versions on process restart from configured history file
  - Persists retention-pruned state after applying retention policy

Embedders can still provide custom `SchemaHistory` implementations for external stores (for example, object storage or relational metadata catalogs).

## Runtime Emission Contract

When converted to canonical events, DDL records use:

- `op = Operation::SchemaChange`
- `schema` set to the affected namespace
- `table` encoded as `<table>__ddl_events`
- `after` payload with `ddl_type`, `schema`, `table`, `statement`
- Optional `result_schema` and `schema_diff` for richer evolution metadata

### Every row carries its shape

Each row event carries `shape`: the table's columns (name, declared type, nullability) in
ordinal order, its primary key, and a content `id`. A consumer parses a row's text values
with the types in the row itself — no other event, topic ordering or registry is needed.

```json
{
  "op": "insert",
  "table": "orders",
  "after": { "id": "9", "amount": "12.5000" },
  "shape": {
    "id": "3f1c…",
    "schema": "public",
    "table": "orders",
    "columns": [
      { "name": "id",     "data_type": "bigint",        "nullable": false },
      { "name": "amount", "data_type": "numeric(12,4)", "nullable": false }
    ],
    "primary_key": ["id"]
  }
}
```

`id` is the lowercase hex SHA-256 of the bytes `rustcdc/v2/row-shape` followed by, each
string as a 4-byte big-endian length and its UTF-8 bytes and each count as 4 bytes
big-endian: `schema`, `table`, the column count, then per column `name`, `data_type` and one
byte (`1` nullable, `0` not), then the key count and each key column. It names a shape, not a
point in history: a table altered from A to B and back to A carries A's id again. A *compact*
shape (the server's `runtime.row_shape = "id"`) carries only `id`, `schema` and `table`.
CloudEvents output also sets `dataschema` to `urn:rustcdc:row-shape:<id>`, and the Kafka sink
sets the `__rustcdc.row_shape` header.

No separate event describes a table: its shape is on its rows. A schema-change event is
emitted for captured DDL only, and carries the shape the statement leaves where it is known.

`data_type` is the **source's own type syntax**, modifier included, read from the server:

| Source | Read from | Example |
|---|---|---|
| PostgreSQL | `pg_catalog.format_type()` | `numeric(12,4)`, `character varying(64)`, enum and domain names |
| MySQL / MariaDB | `information_schema.COLUMNS.COLUMN_TYPE` | `decimal(12,4)`, `int unsigned`, `enum('a','b')` |
| SQL Server | `sys.columns` via the capture instance | `decimal(12,4)`, `nvarchar(255)`, `datetime2(3)` |

`nullable` is read from the catalogue too, not derived from the primary key. A type that
cannot be read is the literal `unknown` — never a guess.

| Connector | Snapshot rows | Stream rows |
|---|---|---|
| PostgreSQL | the catalogue read at snapshot start | types from each `RELATION` message; nullability and key from the catalogue (below) |
| MySQL / MariaDB | `information_schema` read at snapshot start | `information_schema`, stamped only when it agrees with the row's binlog table map (below) |
| SQL Server | `INFORMATION_SCHEMA` read at snapshot start | the capture instance's columns; a new capture instance brings a new shape |

**PostgreSQL.** Each column's declared type is `format_type` of the type OID and modifier the
`RELATION` message carries, so a row resumed from before an `ALTER COLUMN … TYPE` carries the
type it was written under. pgoutput carries neither nullability nor the primary key; both are
re-read from the catalogue when a `RELATION` changes. *Limit:* those two are the catalogue's
at the moment of that read, not necessarily at the row's position.

**MySQL and MariaDB.** The catalogue is read now; a row may be older — after a resume, or
while the stream catches up past an `ALTER`. So a row carries the catalogue's shape only when
its table map agrees on column count, names, each column's binlog type family, nullability and
primary key (`binlog_row_metadata = FULL` supplies names and key). A DDL statement naming the
table, and a table map the held shape disagrees with, trigger a re-read; until one agrees the
rows carry no shape. *Limit:* a change within one type family (`varchar(64)` to
`varchar(128)`) is invisible to the table map, so a row captured before such a change and
read after it carries the newer declaration.

**Incremental snapshots.** The table is described (one catalogue query) before each chunk,
and the chunk's rows carry that shape only when the columns the chunk read are exactly the
described ones. *Limit:* a type change between that describe and the chunk's read is not
detected.

**Transforms.** A projection that only drops columns narrows the shape to the remaining
columns, with a new id; dropping a primary-key column removes it. Anything else that changes
the column set or what values mean removes the shape: a rename, copy or literal in a field
mapping, a mask or hash, an unwrap or flatten, the outbox router, and every WASM module.
`before` and `after` are both compared. A route that renames `table` keeps the shape, which
still names the source table.

**No shape** means unknown, never "no columns". These paths carry none: an offline snapshot
(no catalogue), the Snowflake source, a truncate, MySQL/MariaDB rows whose table map
disagrees with the catalogue, an incremental snapshot chunk whose columns differ from the shape
described before it, and rows a transform reshaped.

## Operational Guidance

1. Treat DDL streams as first-class data for downstream compatibility checks.
2. Validate `ALTER TABLE` changes in staging before production rollouts.
3. Keep schema history durable when replay or recovery windows are large.
4. Use replay and fault-injection tests around major schema migration campaigns.

## Known Boundaries

1. Parsing covers common CREATE/ALTER/DROP table shapes; exotic vendor-specific syntax can require parser extension.
2. `DROP_TABLE` emits a schema-history tombstone and does not include `result_schema`.
3. `FileSchemaHistory` is a single-process local durability backend; multi-process or externally replicated durability still requires a custom implementation.
4. A PostgreSQL row's nullability and primary key are the catalogue's when its `RELATION`
   was last read, not necessarily at the row's position.
5. An offline snapshot — one with no live connection — has no catalogue to read, so its rows
   carry no `shape`.
6. A MySQL/MariaDB type change within one binlog type family is not detected by the
   table-map comparison; see [Every row carries its shape](#every-row-carries-its-shape).

## Related Documentation

- [Configuration Reference](@/docs/config-reference.md)
- [Architecture](@/docs/architecture.md)
- [API Guide](@/docs/api.md)
- [Reliability Testing Guide](@/docs/reliability-testing.md)
