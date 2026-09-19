//! DDL (Data Definition Language) capture and schema evolution support.
//!
//! This module provides abstractions and implementations for capturing CREATE/ALTER/DROP
//! statements from different database sources (PostgreSQL, MySQL, SQL Server) and
//! converting them into canonical schema change events.

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::core::{BeforeImage, Event, Operation, SourceMetadata};
use crate::schema_history::{ColumnDef, DDLEvent, TableSchema};

pub mod mysql;
pub mod postgres;
pub mod sqlserver;

pub use mysql::MysqlDdlExtractor;
pub use postgres::PostgresDdlExtractor;
pub use sqlserver::SqlServerDdlExtractor;

pub(crate) mod parsing;
use self::parsing::*;
pub use self::parsing::{
    extract_columns_from_create, extract_primary_keys, extract_qualified_name,
    extract_qualified_name_with_default, normalize_identifier,
};
#[cfg(test)]
mod tests;

/// `ddl_type` for a schema a connector **observed**, as distinct from one that changed.
///
/// A consumer needs to tell "here is the shape of this table, before its first row" from
/// "this table was altered". Both carry a complete `result_schema`; only the second is a
/// change to react to. Reusing `CREATE_TABLE` for the first would tell a consumer a table
/// had just been created on every pipeline restart.
///
/// Named rather than inlined because three connectors and the runtime compare against it,
/// where `"CREATE_TABLE"` and its siblings appear at one site each. Public so a caller that
/// has to build the announcement a connector would emit, such as a sink checking its topics
/// at startup, uses the same value rather than restating it.
pub const DDL_TYPE_READ_SCHEMA: &str = "READ_SCHEMA";

/// Database dialect used for DDL parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum DdlDialect {
    /// PostgreSQL DDL grammar.
    Postgres,
    /// MySQL and MariaDB DDL grammar.
    Mysql,
    /// SQL Server (T-SQL) DDL grammar.
    SqlServer,
}

impl DdlDialect {
    fn default_schema(self) -> &'static str {
        match self {
            Self::Postgres => "public",
            Self::Mysql => "default",
            Self::SqlServer => "dbo",
        }
    }
}

/// Normalized operation parsed from a DDL statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum DdlOperation {
    /// `CREATE TABLE`.
    CreateTable,
    /// `ALTER TABLE`.
    AlterTable,
    /// `DROP TABLE`.
    DropTable,
}

/// Normalized ALTER TABLE schema-diff operations for replay-grade metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SchemaDiffOperation {
    /// A column was added.
    AddColumn {
        /// The new column's definition.
        column: ColumnDef,
    },
    /// A column was removed.
    DropColumn {
        /// Name of the removed column.
        name: String,
    },
    /// A column was renamed.
    ///
    /// Renames matter downstream well beyond the schema: a mask rule, a field mapping, or
    /// a filter that names the old path silently stops matching.
    RenameColumn {
        /// Previous column name.
        from: String,
        /// New column name.
        to: String,
    },
    /// A clause this parser does not model.
    ///
    /// Surfaced rather than dropped, so a consumer can decide whether the unparsed change
    /// is one it must react to.
    Unsupported {
        /// The clause text as written.
        clause: String,
    },
}

/// Canonical schema diff extracted from a DDL statement when available.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaDiff {
    /// Ordered operations taking the previous schema to the new one.
    pub operations: Vec<SchemaDiffOperation>,
}

impl DdlOperation {
    fn as_ddl_type(self) -> &'static str {
        match self {
            Self::CreateTable => "CREATE_TABLE",
            Self::AlterTable => "ALTER_TABLE",
            Self::DropTable => "DROP_TABLE",
        }
    }
}

/// Dialect-aware normalized DDL parse result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedDdlStatement {
    /// Grammar the statement was parsed with.
    pub dialect: DdlDialect,
    /// Top-level DDL operation.
    pub operation: DdlOperation,
    /// Schema the statement targets.
    pub schema: String,
    /// Table the statement targets.
    pub table: String,
    /// The DDL statement as written.
    pub statement: String,
    /// Table schema after the statement, when the parser could derive it.
    pub result_schema: Option<TableSchema>,
    /// Column-level diff, when the parser could derive one.
    pub schema_diff: Option<SchemaDiff>,
}

impl ParsedDdlStatement {
    /// Convert the parsed statement to a captured DDL envelope.
    pub fn into_captured(self) -> CapturedDdl {
        CapturedDdl {
            ddl_type: self.operation.as_ddl_type().to_string(),
            schema: self.schema,
            table: self.table,
            statement: self.statement,
            result_schema: self.result_schema,
            schema_diff: self.schema_diff,
            ts: 0,
        }
    }
}

/// Metadata about a DDL statement captured from the source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedDdl {
    /// The type of DDL: CREATE_TABLE, ALTER_TABLE, DROP_TABLE, etc.
    pub ddl_type: String,
    /// Schema name (namespace) affected by the DDL.
    pub schema: String,
    /// Table name affected by the DDL.
    pub table: String,
    /// The raw DDL statement as received from the source.
    pub statement: String,
    /// Current schema after applying DDL (if available).
    pub result_schema: Option<TableSchema>,
    /// Canonical schema diff metadata for ALTER-style evolution events.
    pub schema_diff: Option<SchemaDiff>,
    /// Timestamp when the DDL was applied at the source.
    pub ts: u64,
}

impl CapturedDdl {
    /// Reconstruct a [`CapturedDdl`] from the `after` payload of a schema-change event.
    ///
    /// This is the inverse of [`CapturedDdl::to_event`] and exists so the runtime can
    /// record connector-synthesized schema-change events into the durable schema
    /// history. The connectors build these events directly rather than going through
    /// [`CapturedDdl`], so the payload is the only common representation.
    ///
    /// Returns `None` when the payload is not a schema-change envelope — i.e. when
    /// `ddl_type`, `schema`, `table` or `statement` is missing or not a string.
    pub fn from_event_payload(after: &serde_json::Value) -> Option<Self> {
        let object = after.as_object()?;
        let string_field = |key: &str| object.get(key).and_then(serde_json::Value::as_str);

        Some(Self {
            ddl_type: string_field("ddl_type")?.to_string(),
            schema: string_field("schema").unwrap_or_default().to_string(),
            table: string_field("table")?.to_string(),
            statement: string_field("statement").unwrap_or_default().to_string(),
            result_schema: object
                .get("result_schema")
                .and_then(|value| serde_json::from_value(value.clone()).ok()),
            schema_diff: object
                .get("schema_diff")
                .and_then(|value| serde_json::from_value(value.clone()).ok()),
            ts: object
                .get("ts")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
        })
    }

    /// Whether this is a schema **observation** rather than a captured statement.
    ///
    /// The two are recorded under different identities; see [`Self::history_identity`].
    #[must_use]
    pub fn is_observation(&self) -> bool {
        self.ddl_type == DDL_TYPE_READ_SCHEMA
    }

    /// The identity the schema history records this under, given the source offset.
    ///
    /// `record_ddl` is idempotent on this value, and the two kinds of entry want two
    /// different idempotency windows:
    ///
    /// * **A captured statement** is identified by its `offset` — the source log position
    ///   it was read at. That is stable under at-least-once replay, which is the case that
    ///   matters: a crash between recording the DDL and committing the checkpoint replays
    ///   the event, and without the identity check the replay re-applies it. It is
    ///   deliberately *not* content-derived, because a table altered from shape A to B and
    ///   back to A has three entries in its history and the third is not the first.
    ///
    /// * **An observation** is identified by its content. Every connector announces each
    ///   table before that table's first row, in every run, so a pipeline that restarts
    ///   twice a day would otherwise append two schema versions per table per day that
    ///   record nothing having happened. Keyed by content, the second and every later
    ///   observation of an unchanged table resolve to the version already stored and
    ///   append nothing — while an observation of a table that *did* change while the
    ///   pipeline was down still records, which is exactly the case a restart must not
    ///   lose.
    #[must_use]
    pub fn history_identity(&self, offset: &str) -> String {
        if !self.is_observation() {
            return offset.to_string();
        }

        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(b"rustcdc/v1/schema-observation\x00");
        digest.update(self.schema.as_bytes());
        digest.update(b"\x00");
        digest.update(self.table.as_bytes());
        digest.update(b"\x00");
        if let Some(schema) = &self.result_schema {
            for column in &schema.columns {
                digest.update(column.name.as_bytes());
                digest.update(b"\x1f");
                digest.update(column.data_type.as_bytes());
                digest.update(b"\x1f");
                digest.update(if column.nullable { b"1" } else { b"0" });
                digest.update(b"\x1f");
                for constraint in &column.constraints {
                    digest.update(constraint.as_bytes());
                    digest.update(b"\x1e");
                }
                digest.update(b"\x00");
            }
            digest.update(b"\x00keys\x00");
            for key in &schema.primary_keys {
                digest.update(key.as_bytes());
                digest.update(b"\x1f");
            }
        }
        // RustCrypto 0.11 returns a `hybrid-array::Array`, which does not implement
        // `LowerHex`; format the bytes explicitly, as `fingerprint_event_stable` does.
        let hex: String = digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        format!("observed:{hex}")
    }

    /// Convert a captured DDL into a SchemaHistory DDLEvent for persistence.
    pub fn to_schema_event(&self) -> Option<DDLEvent> {
        match self.ddl_type.as_str() {
            // An observation records the table's shape as a **set**, not a diff.
            //
            // `CreateTable` and `AlterTable` are the same operation in the store — both
            // append the schema whole — and a set is the only form that can be applied to a
            // table the history has never seen, which is what an `InMemorySchemaHistory`
            // looks like after any restart. A diff there is the `SchemaError` that used to
            // be logged and dropped.
            DDL_TYPE_READ_SCHEMA | "CREATE_TABLE" => {
                self.result_schema.clone().map(DDLEvent::CreateTable)
            }
            "ALTER_TABLE" => {
                if let Some(schema) = self
                    .result_schema
                    .as_ref()
                    .filter(|schema| !schema.columns.is_empty())
                {
                    Some(DDLEvent::AlterTable(schema.clone()))
                } else {
                    self.schema_diff
                        .clone()
                        .map(|diff| DDLEvent::AlterTableDiff {
                            schema: self.schema.clone(),
                            table: self.table.clone(),
                            diff,
                        })
                }
            }
            "DROP_TABLE" => Some(DDLEvent::DropTable {
                schema: self.schema.clone(),
                table: self.table.clone(),
            }),
            _ => None,
        }
    }

    /// Convert a captured DDL into a canonical Event for stream emission.
    pub fn to_event(&self, source_name: &str, offset: String, ts_ms: u64) -> Event {
        let mut after = json!({
            "ddl_type": self.ddl_type,
            "schema": self.schema,
            "table": self.table,
            "statement": self.statement,
        });

        // Include result schema if available
        if let Some(schema) = &self.result_schema
            && let Ok(schema_json) = serde_json::to_value(schema)
        {
            after
                .as_object_mut()
                .unwrap()
                .insert("result_schema".into(), schema_json);
        }

        if let Some(diff) = &self.schema_diff
            && let Ok(diff_json) = serde_json::to_value(diff)
        {
            after
                .as_object_mut()
                .unwrap()
                .insert("schema_diff".into(), diff_json);
        }

        Event {
            before: BeforeImage::Unavailable,
            after: Some(after),
            op: Operation::SchemaChange,
            source: SourceMetadata {
                source_name: source_name.to_string(),
                offset,
                timestamp: self.ts,
            },
            ts: ts_ms,
            schema: Some(self.schema.clone()),
            table: format!("{}__ddl_events", self.table),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: crate::core::EVENT_ENVELOPE_VERSION,
            unavailable_columns: Vec::new(),
        }
    }
}

/// Trait for extracting DDL from source-specific message formats.
pub trait DdlExtractor: Send + Sync {
    /// Extract DDL from a source message if it contains a DDL statement.
    /// Returns None if the message is not DDL-related (e.g., DML or control message).
    fn extract_ddl(&self, message: &str) -> Option<CapturedDdl>;

    /// Parse a DDL statement to extract schema/table names.
    fn parse_schema_table(&self, statement: &str) -> Option<(String, String)>;

    /// Parse a CREATE TABLE statement to extract schema information.
    fn parse_create_table_schema(&self, statement: &str) -> Option<TableSchema>;
}

/// Parse a source statement into a normalized DDL shape for a specific dialect.
pub fn parse_ddl_statement(dialect: DdlDialect, statement: &str) -> Option<ParsedDdlStatement> {
    let statement = statement.trim().to_string();
    let upper = statement.to_uppercase();

    let operation = if upper.starts_with("CREATE TABLE") {
        DdlOperation::CreateTable
    } else if upper.starts_with("ALTER TABLE") {
        DdlOperation::AlterTable
    } else if upper.starts_with("DROP TABLE") {
        DdlOperation::DropTable
    } else {
        return None;
    };

    let (schema, table) = parse_schema_table_for_dialect(dialect, &statement)?;
    let result_schema = match operation {
        DdlOperation::CreateTable => parse_create_table_schema_for_dialect(dialect, &statement),
        DdlOperation::AlterTable => None,
        DdlOperation::DropTable => None,
    };
    let schema_diff = match operation {
        DdlOperation::AlterTable => parse_alter_table_diff_for_dialect(dialect, &statement),
        _ => None,
    };

    Some(ParsedDdlStatement {
        dialect,
        operation,
        schema,
        table,
        statement,
        result_schema,
        schema_diff,
    })
}

/// Extract a captured DDL object from a source statement using a dialect parser.
pub fn extract_captured_ddl(dialect: DdlDialect, message: &str) -> Option<CapturedDdl> {
    parse_ddl_statement(dialect, message).map(ParsedDdlStatement::into_captured)
}

/// Parse schema/table names from CREATE/ALTER/DROP TABLE statements for a dialect.
pub fn parse_schema_table_for_dialect(
    dialect: DdlDialect,
    statement: &str,
) -> Option<(String, String)> {
    let upper = statement.to_uppercase();

    let target = if upper.starts_with("CREATE TABLE") {
        statement[12..].trim_start()
    } else if upper.starts_with("ALTER TABLE") {
        let mut target = statement[11..].trim_start();
        target = strip_alter_target_modifiers(target);
        target
    } else if upper.starts_with("DROP TABLE") {
        let after_drop = statement[10..].trim_start();
        if after_drop.to_uppercase().starts_with("IF EXISTS") {
            after_drop[9..].trim_start()
        } else {
            after_drop
        }
    } else {
        return None;
    };

    extract_qualified_name_with_default(target, dialect.default_schema())
}

/// Parse a CREATE/ALTER statement into a normalized schema model for a dialect.
pub fn parse_create_table_schema_for_dialect(
    dialect: DdlDialect,
    statement: &str,
) -> Option<TableSchema> {
    let (schema, table) = parse_schema_table_for_dialect(dialect, statement)?;
    let upper = statement.trim_start().to_uppercase();

    // ALTER statements do not provide a full table snapshot in this parser path.
    // Callers must rely on schema_diff metadata or an external introspection step.
    if upper.starts_with("ALTER TABLE") {
        return None;
    }

    let columns = extract_columns_from_create(statement);
    let primary_keys = extract_primary_keys(statement);

    Some(TableSchema {
        schema,
        table,
        columns,
        primary_keys,
        version: 0,
    })
}

/// Parse ALTER TABLE clauses into normalized schema-diff operations.
pub fn parse_alter_table_diff_for_dialect(
    _dialect: DdlDialect,
    statement: &str,
) -> Option<SchemaDiff> {
    let upper = statement.to_uppercase();
    if !upper.starts_with("ALTER TABLE") {
        return None;
    }

    let after_alter = strip_alter_target_modifiers(statement[11..].trim_start());

    let clauses = split_alter_table_clauses(after_alter)?;
    if clauses.is_empty() {
        return None;
    }

    let mut operations = Vec::new();
    for clause in split_sql_clauses(clauses) {
        if let Some(op) = parse_alter_clause(&clause) {
            if let SchemaDiffOperation::Unsupported {
                clause: ref unsupported_clause,
            } = op
            {
                tracing::warn!(
                    clause = %unsupported_clause,
                    "unsupported ALTER TABLE clause; schema history will not reflect this change"
                );
            }
            operations.push(op);
        }
    }

    if operations.is_empty() {
        None
    } else {
        Some(SchemaDiff { operations })
    }
}
