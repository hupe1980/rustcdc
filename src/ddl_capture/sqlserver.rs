//! SQL Server DDL extraction from CDC events.

use super::{
    extract_captured_ddl, parse_create_table_schema_for_dialect, parse_schema_table_for_dialect,
    CapturedDdl, DdlDialect, DdlExtractor,
};
use crate::schema_history::TableSchema;

/// SQL Server-specific DDL statement extractor.
///
/// Handles CREATE/ALTER/DROP TABLE statements from SQL Server CDC.
#[derive(Default)]
pub struct SqlServerDdlExtractor;

impl SqlServerDdlExtractor {
    /// Create a new SQL Server DDL extractor.
    pub fn new() -> Self {
        Self
    }
}

impl DdlExtractor for SqlServerDdlExtractor {
    fn extract_ddl(&self, message: &str) -> Option<CapturedDdl> {
        extract_captured_ddl(DdlDialect::SqlServer, message)
    }

    fn parse_schema_table(&self, statement: &str) -> Option<(String, String)> {
        parse_schema_table_for_dialect(DdlDialect::SqlServer, statement)
    }

    fn parse_create_table_schema(&self, statement: &str) -> Option<TableSchema> {
        parse_create_table_schema_for_dialect(DdlDialect::SqlServer, statement)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlserver_ddl_extractor_parses_create_table() {
        let extractor = SqlServerDdlExtractor::new();
        let sql = "CREATE TABLE [dbo].[users] ([id] INT PRIMARY KEY, [name] VARCHAR(255) NOT NULL)";

        let ddl = extractor.extract_ddl(sql);
        assert!(ddl.is_some());

        let ddl = ddl.unwrap();
        assert_eq!(ddl.ddl_type, "CREATE_TABLE");
        assert_eq!(ddl.table, "users");
    }

    #[test]
    fn sqlserver_ddl_extractor_parses_drop_table() {
        let extractor = SqlServerDdlExtractor::new();
        let sql = "DROP TABLE [dbo].[users]";

        let ddl = extractor.extract_ddl(sql);
        assert!(ddl.is_some());

        let ddl = ddl.unwrap();
        assert_eq!(ddl.ddl_type, "DROP_TABLE");
        assert_eq!(ddl.table, "users");
    }

    #[test]
    fn sqlserver_ddl_extractor_ignores_non_ddl() {
        let extractor = SqlServerDdlExtractor::new();
        let sql = "INSERT INTO users VALUES (1, 'Alice')";

        let ddl = extractor.extract_ddl(sql);
        assert!(ddl.is_none());
    }

    #[test]
    fn sqlserver_primary_key_extraction() {
        let ddl = SqlServerDdlExtractor::new()
            .extract_ddl("CREATE TABLE [users] ([id] INT PRIMARY KEY, [name] VARCHAR(255))")
            .unwrap();
        let schema = ddl.result_schema.unwrap();
        assert_eq!(schema.primary_keys, vec!["id"]);
    }
}
