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

Plus `READ_SCHEMA` — not a statement, but a table's shape announced before its first row.
See [Every table is announced before its first row](#every-table-is-announced-before-its-first-row).

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
| An observation (`READ_SCHEMA`) | a digest of the schema it carries | Announced every run, so an offset would append a version per table per restart |

An observation of a table that changed while the pipeline was down still records.

This is load-bearing, not a nicety. The runtime records a schema change *before* it enqueues
the event announcing it, so a crash between the record and the checkpoint commit replays the
DDL on restart. Without the identity check, replaying an `AlterTableDiff` re-applied its
operations to a schema that already had them — `ADD COLUMN` on a column that now exists —
which returns `SchemaError`, failed the poll, and failed identically on every subsequent
restart from the same checkpoint. The pipeline never started again.

A custom `SchemaHistory` implementation must honour the same contract.

#### A rejected entry does not stop capture

If the history genuinely cannot accept a statement — an `ALTER TABLE` diff for a table the
store has never seen, which is what an `InMemorySchemaHistory` looks like after any restart —
the runtime logs at ERROR with the remedy and continues. Propagating it would fail the poll,
and fail identically on every restart, for a gap in an auxiliary index; the event itself is
self-describing and still reaches the consumer. A store that is *broken* rather than merely
inconsistent — an I/O failure, a lost owner lease — still fails the poll.

Runtime-managed retention is available through `RuntimeConfig::with_schema_history_retention(...)`.
When configured, rustcdc applies the retention policy automatically after each persisted DDL mutation.
Runtime defaults now enable bounded retention (`keep_last(256)` per table) to prevent unbounded growth.

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

### Every table is announced before its first row

**A consumer reading a stream from the start of a session sees each table's column types
before any of that table's rows, in both the snapshot and the stream.**

Column values are text
([Column values are text](@/docs/api.md#column-values-are-text-on-every-connector-and-every-path)),
and parsing text requires knowing what to parse it as — nothing in `{"id": "9"}` says whether
`"9"` is a `bigint` or a `text` holding a digit.

The announcement carries `ddl_type = "READ_SCHEMA"` and a complete `result_schema`. It is
distinct from `CREATE_TABLE`, which reports a table that was actually created:

```json
{
  "ddl_type": "READ_SCHEMA",
  "schema": "public",
  "table": "orders",
  "result_schema": {
    "schema": "public",
    "table": "orders",
    "primary_keys": ["id"],
    "columns": [
      { "name": "id",     "data_type": "bigint",        "nullable": false, "constraints": ["primary_key"] },
      { "name": "amount", "data_type": "numeric(12,4)", "nullable": false, "constraints": [] },
      { "name": "note",   "data_type": "varchar(64)",   "nullable": true,  "constraints": [] }
    ]
  }
}
```

`data_type` is the **source's own type syntax**, modifier included, read from the catalogue:

| Source | Read from | Example |
|---|---|---|
| PostgreSQL | `pg_catalog.format_type()` | `numeric(12,4)`, `character varying(64)`, enum and domain names |
| MySQL / MariaDB | `information_schema.COLUMNS.COLUMN_TYPE` | `decimal(12,4)`, `int unsigned`, `enum('a','b')` |
| SQL Server | `sys.columns` via the capture instance | `decimal(12,4)`, `nvarchar(255)`, `datetime2(3)` |

`nullable` is read from the catalogue too, not derived from the primary key.

A type that cannot be read is the literal `unknown` — never a guess. See each connector page
for when that happens.

**Cadence.** One announcement per table per run, before that table's first row. A restart
re-announces, because a reconnecting consumer needs the schema before the rows it is about to
receive. The schema history de-duplicates the repeat, so no extra schema version is recorded.

## Operational Guidance

1. Treat DDL streams as first-class data for downstream compatibility checks.
2. Validate `ALTER TABLE` changes in staging before production rollouts.
3. Keep schema history durable when replay or recovery windows are large.
4. Use replay and fault-injection tests around major schema migration campaigns.

## Known Boundaries

1. Parsing covers common CREATE/ALTER/DROP table shapes; exotic vendor-specific syntax can require parser extension.
2. `DROP_TABLE` emits a schema-history tombstone and does not include `result_schema`.
3. `FileSchemaHistory` is a single-process local durability backend; multi-process or externally replicated durability still requires a custom implementation.
4. A PostgreSQL table added to the publication after the stream started is not in the
   stream-start catalogue read, so its announcement reports `data_type` from the pgoutput
   type OID — no modifier, and `unknown` for a type outside the built-in set. Its first DDL
   carries the full declaration.
5. An offline snapshot — one with no live connection — has no catalogue to read, so it
   announces nothing rather than describing columns it did not read.

## Related Documentation

- [Configuration Reference](@/docs/config-reference.md)
- [Architecture](@/docs/architecture.md)
- [API Guide](@/docs/api.md)
- [Reliability Testing Guide](@/docs/reliability-testing.md)
