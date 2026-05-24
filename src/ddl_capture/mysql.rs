//! MySQL DDL extraction from binlog events.

use super::{
    extract_captured_ddl, parse_create_table_schema_for_dialect, parse_schema_table_for_dialect,
    CapturedDdl, DdlDialect, DdlExtractor,
};
use crate::schema_history::TableSchema;

/// MySQL-specific DDL statement extractor.
///
/// Handles CREATE/ALTER/DROP TABLE statements from MySQL binary log.
#[derive(Default)]
pub struct MysqlDdlExtractor;

impl MysqlDdlExtractor {
    /// Create a new MySQL DDL extractor.
    pub fn new() -> Self {
        Self
    }
}

impl DdlExtractor for MysqlDdlExtractor {
    fn extract_ddl(&self, message: &str) -> Option<CapturedDdl> {
        extract_captured_ddl(DdlDialect::Mysql, message)
    }

    fn parse_schema_table(&self, statement: &str) -> Option<(String, String)> {
        parse_schema_table_for_dialect(DdlDialect::Mysql, statement)
    }

    fn parse_create_table_schema(&self, statement: &str) -> Option<TableSchema> {
        parse_create_table_schema_for_dialect(DdlDialect::Mysql, statement)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_ddl_extractor_parses_create_table() {
        let extractor = MysqlDdlExtractor::new();
        let sql =
            "CREATE TABLE `mydb`.`users` (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL) ENGINE=InnoDB";

        let ddl = extractor.extract_ddl(sql);
        assert!(ddl.is_some());

        let ddl = ddl.unwrap();
        assert_eq!(ddl.ddl_type, "CREATE_TABLE");
        assert_eq!(ddl.table, "users");
    }

    #[test]
    fn mysql_ddl_extractor_parses_drop_table() {
        let extractor = MysqlDdlExtractor::new();
        let sql = "DROP TABLE IF EXISTS `mydb`.`users`";

        let ddl = extractor.extract_ddl(sql);
        assert!(ddl.is_some());

        let ddl = ddl.unwrap();
        assert_eq!(ddl.ddl_type, "DROP_TABLE");
        assert_eq!(ddl.table, "users");
    }

    #[test]
    fn mysql_ddl_extractor_ignores_non_ddl() {
        let extractor = MysqlDdlExtractor::new();
        let sql = "INSERT INTO users VALUES (1, 'Alice')";

        let ddl = extractor.extract_ddl(sql);
        assert!(ddl.is_none());
    }

    #[test]
    fn mysql_primary_key_extraction() {
        let ddl = MysqlDdlExtractor::new()
            .extract_ddl("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))")
            .unwrap();
        let schema = ddl.result_schema.unwrap();
        assert_eq!(schema.primary_keys, vec!["id"]);
    }
}
