//! DDL (Data Definition Language) capture and schema evolution support.
//!
//! This module provides abstractions and implementations for capturing CREATE/ALTER/DROP
//! statements from different database sources (PostgreSQL, MySQL, SQL Server) and
//! converting them into canonical schema change events.

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::core::{Event, Operation, SourceMetadata};
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

/// Database dialect used for DDL parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DdlDialect {
    Postgres,
    Mysql,
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
pub enum DdlOperation {
    CreateTable,
    AlterTable,
    DropTable,
}

/// Normalized ALTER TABLE schema-diff operations for replay-grade metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaDiffOperation {
    AddColumn { column: ColumnDef },
    DropColumn { name: String },
    RenameColumn { from: String, to: String },
    Unsupported { clause: String },
}

/// Canonical schema diff extracted from a DDL statement when available.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaDiff {
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
    pub dialect: DdlDialect,
    pub operation: DdlOperation,
    pub schema: String,
    pub table: String,
    pub statement: String,
    pub result_schema: Option<TableSchema>,
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
    /// Convert a captured DDL into a SchemaHistory DDLEvent for persistence.
    pub fn to_schema_event(&self) -> Option<DDLEvent> {
        match self.ddl_type.as_str() {
            "CREATE_TABLE" => self.result_schema.clone().map(DDLEvent::CreateTable),
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
        if let Some(schema) = &self.result_schema {
            if let Ok(schema_json) = serde_json::to_value(schema) {
                after
                    .as_object_mut()
                    .unwrap()
                    .insert("result_schema".into(), schema_json);
            }
        }

        if let Some(diff) = &self.schema_diff {
            if let Ok(diff_json) = serde_json::to_value(diff) {
                after
                    .as_object_mut()
                    .unwrap()
                    .insert("schema_diff".into(), diff_json);
            }
        }

        Event {
            before: None,
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
