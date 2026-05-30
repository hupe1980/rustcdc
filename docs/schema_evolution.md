# Schema Evolution And DDL Capture

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

- `record_ddl` to append schema mutations and return version
- `get_schema_at_version` and `get_schema_at_timestamp` for point-in-time lookup
- `latest_schema` for current view
- `apply_retention` to prune old versions using explicit retention policy

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

## Operational Guidance

1. Treat DDL streams as first-class data for downstream compatibility checks.
2. Validate `ALTER TABLE` changes in staging before production rollouts.
3. Keep schema history durable when replay or recovery windows are large.
4. Use replay and fault-injection tests around major schema migration campaigns.

## Known Boundaries

1. Parsing covers common CREATE/ALTER/DROP table shapes; exotic vendor-specific syntax can require parser extension.
2. `DROP_TABLE` emits a schema-history tombstone and does not include `result_schema`.
3. `FileSchemaHistory` is a single-process local durability backend; multi-process or externally replicated durability still requires a custom implementation.

## Related Documentation

- [Configuration Reference](config_reference.md)
- [Architecture](architecture.md)
- [API Guide](api.md)
- [Reliability Testing Guide](reliability_testing.md)
