use super::*;
#[test]
fn extract_qualified_name_handles_simple_names() {
    let (schema, table) = extract_qualified_name("users").unwrap();
    assert_eq!(table, "users");
    assert_eq!(schema, "public");
}

#[test]
fn extract_qualified_name_handles_qualified_names() {
    let (schema, table) = extract_qualified_name("myschema.users").unwrap();
    assert_eq!(schema, "myschema");
    assert_eq!(table, "users");
}

#[test]
fn extract_qualified_name_decodes_escaped_quoted_schema_and_table() {
    let (schema, table) =
        extract_qualified_name_with_default("\"odd\"\"schema\".\"na\"\"me\"", "public").unwrap();
    assert_eq!(schema, "odd\"schema");
    assert_eq!(table, "na\"me");

    let (schema, table) =
        extract_qualified_name_with_default("`odd``schema`.`na``me`", "default").unwrap();
    assert_eq!(schema, "odd`schema");
    assert_eq!(table, "na`me");

    let (schema, table) =
        extract_qualified_name_with_default("[odd]]schema].[na]]me]", "dbo").unwrap();
    assert_eq!(schema, "odd]schema");
    assert_eq!(table, "na]me");
}

#[test]
fn parse_schema_table_for_dialect_handles_escaped_schema_qualified_names() {
    let postgres = parse_schema_table_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE \"odd\"\"schema\".\"na\"\"me\" ADD COLUMN id INT",
    )
    .unwrap();
    assert_eq!(postgres, ("odd\"schema".to_string(), "na\"me".to_string()));

    let mysql = parse_schema_table_for_dialect(
        DdlDialect::Mysql,
        "ALTER TABLE `odd``schema`.`na``me` ADD COLUMN id INT",
    )
    .unwrap();
    assert_eq!(mysql, ("odd`schema".to_string(), "na`me".to_string()));

    let sqlserver = parse_schema_table_for_dialect(
        DdlDialect::SqlServer,
        "ALTER TABLE [odd]]schema].[na]]me] ADD [id] INT",
    )
    .unwrap();
    assert_eq!(sqlserver, ("odd]schema".to_string(), "na]me".to_string()));
}

#[test]
fn extract_qualified_name_handles_quoted_identifiers_with_literal_dots() {
    // Postgres: identifier contains a literal dot inside quotes
    let (schema, table) =
        extract_qualified_name_with_default("\"my.schema\".\"table.v2\"", "public").unwrap();
    assert_eq!(schema, "my.schema");
    assert_eq!(table, "table.v2");

    // MySQL: backtick identifiers with dots
    let (schema, table) =
        extract_qualified_name_with_default("`my.db`.`table.name`", "default").unwrap();
    assert_eq!(schema, "my.db");
    assert_eq!(table, "table.name");

    // SQL Server: bracket identifiers with dots
    let (schema, table) =
        extract_qualified_name_with_default("[my.schema].[table.v2]", "dbo").unwrap();
    assert_eq!(schema, "my.schema");
    assert_eq!(table, "table.v2");
}

#[test]
fn parse_schema_table_for_dialect_handles_dotted_identifiers() {
    let postgres = parse_schema_table_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE \"tenant.prod\".\"users.v2\" ADD COLUMN id INT",
    )
    .unwrap();
    assert_eq!(
        postgres,
        ("tenant.prod".to_string(), "users.v2".to_string())
    );

    let mysql = parse_schema_table_for_dialect(
        DdlDialect::Mysql,
        "CREATE TABLE `app.events`.`log.history` (id INT PRIMARY KEY)",
    )
    .unwrap();
    assert_eq!(mysql, ("app.events".to_string(), "log.history".to_string()));

    let sqlserver = parse_schema_table_for_dialect(
        DdlDialect::SqlServer,
        "ALTER TABLE [dbo.legacy].[snapshot.v1] ADD [timestamp] DATETIME",
    )
    .unwrap();
    assert_eq!(
        sqlserver,
        ("dbo.legacy".to_string(), "snapshot.v1".to_string())
    );
}

#[test]
fn parse_schema_table_for_dialect_handles_sqlserver_three_part_names() {
    // SQL Server three-part names: catalog.schema.table
    // Parser should extract only schema.table for canonical relation metadata
    let (schema, table) = parse_schema_table_for_dialect(
        DdlDialect::SqlServer,
        "ALTER TABLE [master].[dbo].[users] ADD [id] INT",
    )
    .unwrap();
    // Should take the final two parts: dbo.users
    assert_eq!(schema, "dbo");
    assert_eq!(table, "users");

    // Three-part with dotted identifiers in the final pair
    let (schema, table) = parse_schema_table_for_dialect(
        DdlDialect::SqlServer,
        "ALTER TABLE [mydb].[dbo.legacy].[snapshot.v2] ADD [ts] DATETIME",
    )
    .unwrap();
    assert_eq!(schema, "dbo.legacy");
    assert_eq!(table, "snapshot.v2");
}

#[test]
fn extract_columns_handles_quoted_identifiers_with_dots() {
    // PostgreSQL quoted column name with dot
    let cols = extract_columns_from_create("CREATE TABLE t (\"created.at\" TIMESTAMP, id INT)");
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name, "created.at");
    assert_eq!(cols[0].data_type, "TIMESTAMP");

    // MySQL backtick column with dot
    let cols = extract_columns_from_create("CREATE TABLE t (`updated.ts` DATETIME, id INT)");
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name, "updated.ts");
    assert_eq!(cols[0].data_type, "DATETIME");

    // SQL Server bracket column with dot
    let cols = extract_columns_from_create("CREATE TABLE t ([event.time] DATETIME2, id INT)");
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name, "event.time");
    assert_eq!(cols[0].data_type, "DATETIME2");
}

#[test]
fn extract_columns_handles_column_types_with_parentheses() {
    let cols = extract_columns_from_create(
        "CREATE TABLE t (name VARCHAR(255), id INT PRIMARY KEY, email VARCHAR(512))",
    );
    assert_eq!(cols.len(), 3);
    assert_eq!(cols[0].name, "name");
    assert_eq!(cols[0].data_type, "VARCHAR(255)");
    assert_eq!(cols[1].name, "id");
    assert_eq!(cols[2].name, "email");
    assert_eq!(cols[2].data_type, "VARCHAR(512)");
}

#[test]
fn extract_columns_handles_not_null_constraint() {
    let cols = extract_columns_from_create(
        "CREATE TABLE t (id INT NOT NULL, name VARCHAR(100), archived BOOL NOT NULL)",
    );
    assert_eq!(cols.len(), 3);
    assert!(!cols[0].nullable);
    assert!(cols[1].nullable);
    assert!(!cols[2].nullable);
}

#[test]
fn extract_columns_handles_mysql_generated_columns() {
    let cols = extract_columns_from_create("CREATE TABLE t (id INT, age INT GENERATED ALWAYS AS (YEAR(now()) - birth_year) STORED, birth_year INT)");
    assert!(cols
        .iter()
        .any(|c| c.name == "age" && c.data_type == "COMPUTED"));
}

#[test]
fn extract_columns_handles_sqlserver_persisted_computed() {
    let cols = extract_columns_from_create("CREATE TABLE t (id INT, full_name AS first_name + ' ' + last_name PERSISTED, first_name VARCHAR(50), last_name VARCHAR(50))");
    assert!(cols
        .iter()
        .any(|c| c.name == "full_name" && c.data_type == "COMPUTED"));
}

#[test]
fn normalize_identifier_removes_quotes() {
    assert_eq!(normalize_identifier("\"MyTable\""), "mytable");
    assert_eq!(normalize_identifier("`my_table`"), "my_table");
    assert_eq!(normalize_identifier("[My Table]"), "my table");
    assert_eq!(normalize_identifier("\"na\"\"me\""), "na\"me");
    assert_eq!(normalize_identifier("`na``me`"), "na`me");
    assert_eq!(normalize_identifier("[na]]me]"), "na]me");
}

#[test]
fn parse_ddl_statement_uses_dialect_default_schema() {
    let parsed = parse_ddl_statement(
        DdlDialect::SqlServer,
        "CREATE TABLE users (id INT PRIMARY KEY)",
    )
    .unwrap();
    assert_eq!(parsed.schema, "dbo");
    assert_eq!(parsed.table, "users");
    assert_eq!(parsed.operation, DdlOperation::CreateTable);
}

#[test]
fn extract_primary_keys_handles_constraint_and_inline_forms() {
    let constraint = extract_primary_keys("CREATE TABLE users (id INT, PRIMARY KEY (id))");
    assert_eq!(constraint, vec!["id"]);

    let inline = extract_primary_keys("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)");
    assert_eq!(inline, vec!["id"]);
}

#[test]
fn parse_alter_table_diff_extracts_add_drop_rename() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users ADD COLUMN email TEXT, DROP COLUMN nickname, RENAME COLUMN name TO full_name",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 3);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::AddColumn { column } if column.name == "email"
    ));
    assert!(matches!(
        &diff.operations[1],
        SchemaDiffOperation::DropColumn { name } if name == "nickname"
    ));
    assert!(matches!(
        &diff.operations[2],
        SchemaDiffOperation::RenameColumn { from, to } if from == "name" && to == "full_name"
    ));
}

#[test]
fn parse_ddl_statement_includes_alter_schema_diff_metadata() {
    let parsed = parse_ddl_statement(
        DdlDialect::Mysql,
        "ALTER TABLE users ADD COLUMN age INT, DROP COLUMN legacy",
    )
    .unwrap();
    assert_eq!(parsed.operation, DdlOperation::AlterTable);
    assert!(parsed.result_schema.is_none());
    let diff = parsed.schema_diff.expect("schema diff should be present");
    assert_eq!(diff.operations.len(), 2);
}

#[test]
fn parse_ddl_statement_handles_alter_if_exists_modifiers() {
    let parsed = parse_ddl_statement(
        DdlDialect::Postgres,
        "ALTER TABLE IF EXISTS ONLY public.users DROP COLUMN IF EXISTS legacy",
    )
    .unwrap();

    assert_eq!(parsed.schema, "public");
    assert_eq!(parsed.table, "users");

    let diff = parsed.schema_diff.expect("schema diff should be present");
    assert_eq!(diff.operations.len(), 1);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::DropColumn { name } if name == "legacy"
    ));
}

#[test]
fn captured_ddl_to_schema_event_prefers_diff_for_alter_without_full_snapshot() {
    let event = CapturedDdl {
        ddl_type: "ALTER_TABLE".into(),
        schema: "public".into(),
        table: "users".into(),
        statement: "ALTER TABLE public.users ADD COLUMN email TEXT".into(),
        result_schema: None,
        schema_diff: Some(SchemaDiff {
            operations: vec![SchemaDiffOperation::AddColumn {
                column: ColumnDef {
                    name: "email".into(),
                    data_type: "TEXT".into(),
                    nullable: true,
                    constraints: Vec::new(),
                },
            }],
        }),
        ts: 0,
    }
    .to_schema_event()
    .expect("alter ddl should convert to schema event");

    assert!(matches!(
        event,
        DDLEvent::AlterTableDiff { schema, table, .. }
            if schema == "public" && table == "users"
    ));
}

#[test]
fn parse_alter_table_diff_handles_add_if_not_exists_and_trailing_semicolon() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Mysql,
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS `Email` VARCHAR(255);",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 1);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::AddColumn { column }
            if column.name == "email" && column.data_type == "VARCHAR(255)"
    ));
}

#[test]
fn parse_alter_table_diff_handles_quoted_rename_and_drop_if_exists() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::SqlServer,
        "ALTER TABLE [dbo].[Users] RENAME COLUMN [UserName] TO [DisplayName], DROP COLUMN IF EXISTS [Legacy];",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 2);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::RenameColumn { from, to }
            if from == "username" && to == "displayname"
    ));
    assert!(matches!(
        &diff.operations[1],
        SchemaDiffOperation::DropColumn { name } if name == "legacy"
    ));
}

#[test]
fn parse_alter_table_diff_keeps_unsupported_clause_metadata() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users REPLICA IDENTITY FULL",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 1);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::Unsupported { clause }
            if clause.eq_ignore_ascii_case("REPLICA IDENTITY FULL")
    ));
}

#[test]
fn parse_alter_table_diff_mixes_supported_and_unsupported_clauses() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users ADD COLUMN age INT, REPLICA IDENTITY FULL, DROP COLUMN legacy",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 3);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::AddColumn { column } if column.name == "age"
    ));
    assert!(matches!(
        &diff.operations[1],
        SchemaDiffOperation::Unsupported { clause }
            if clause.eq_ignore_ascii_case("REPLICA IDENTITY FULL")
    ));
    assert!(matches!(
        &diff.operations[2],
        SchemaDiffOperation::DropColumn { name } if name == "legacy"
    ));
}

#[test]
fn parse_alter_table_diff_normalizes_unsupported_clause_whitespace() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users   REPLICA    IDENTITY   FULL   ;",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 1);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "REPLICA IDENTITY FULL"
    ));
}

#[test]
fn parse_alter_table_diff_normalizes_unsupported_clause_casing() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users set   tablespace fastspace",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 1);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "SET TABLESPACE FASTSPACE"
    ));
}

#[test]
fn parse_alter_table_diff_treats_add_constraint_as_unsupported() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users ADD CONSTRAINT users_pk PRIMARY KEY (id)",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 1);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "ADD CONSTRAINT USERS_PK PRIMARY KEY (ID)"
    ));
}

#[test]
fn parse_alter_table_diff_treats_drop_constraint_as_unsupported() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users DROP CONSTRAINT users_pk",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 1);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "DROP CONSTRAINT USERS_PK"
    ));
}

#[test]
fn parse_alter_table_diff_allows_quoted_keyword_column_name() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users DROP COLUMN \"constraint\"",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 1);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::DropColumn { name } if name == "constraint"
    ));
}

#[test]
fn parse_alter_table_diff_treats_add_fulltext_index_as_unsupported() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Mysql,
        "ALTER TABLE users ADD FULLTEXT INDEX idx_name (name)",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 1);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "ADD FULLTEXT INDEX IDX_NAME (NAME)"
    ));
}

#[test]
fn parse_alter_table_diff_allows_add_with_quoted_keyword_name() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Mysql,
        "ALTER TABLE users ADD COLUMN `fulltext` INT",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 1);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::AddColumn { column }
            if column.name == "fulltext" && column.data_type == "INT"
    ));
}

#[test]
fn parse_alter_table_diff_keeps_comma_inside_single_quoted_literal() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users SET (note = 'a, b'), REPLICA IDENTITY FULL",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 2);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "SET (NOTE = 'a, b')"
    ));
    assert!(matches!(
        &diff.operations[1],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "REPLICA IDENTITY FULL"
    ));
}

#[test]
fn parse_alter_table_diff_preserves_whitespace_inside_quoted_literal() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users SET (note = 'a   b')",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 1);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "SET (NOTE = 'a   b')"
    ));
}

#[test]
fn parse_alter_table_diff_preserves_escaped_double_quotes_in_identifier() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users ATTACH PARTITION \"m\"\"2024\" FOR VALUES IN (1)",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 1);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "ATTACH PARTITION \"m\"\"2024\" FOR VALUES IN (1)"
    ));
}

#[test]
fn parse_alter_table_diff_preserves_sqlserver_bracket_and_quoted_literals() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::SqlServer,
        "ALTER TABLE [dbo].[orders] set (lock_escalation = 'auto, manual'), nocheck constraint [ck_Orders]]State];",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 2);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "SET (LOCK_ESCALATION = 'auto, manual')"
    ));
    assert!(matches!(
        &diff.operations[1],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "NOCHECK CONSTRAINT [ck_Orders]]State]"
    ));
}

#[test]
fn parse_alter_table_diff_preserves_mixed_operation_order_with_quoted_literal() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Mysql,
        "ALTER TABLE inventory.products ADD COLUMN status VARCHAR(32), COMMENT='alpha, beta'' gamma', DROP COLUMN IF EXISTS legacy_status",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 3);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::AddColumn { column }
            if column.name == "status" && column.data_type == "VARCHAR(32)"
    ));
    assert!(matches!(
        &diff.operations[1],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "COMMENT='alpha, beta'' gamma'"
    ));
    assert!(matches!(
        &diff.operations[2],
        SchemaDiffOperation::DropColumn { name } if name == "legacy_status"
    ));
}

#[test]
fn parse_alter_table_diff_preserves_sqlserver_mixed_order_with_quoted_literal() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::SqlServer,
        "ALTER TABLE [dbo].[orders] ADD [priority] INT, SET (lock_escalation = 'auto, manual'), DROP COLUMN IF EXISTS [legacy_status]",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 3);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::AddColumn { column }
            if column.name == "priority" && column.data_type == "INT"
    ));
    assert!(matches!(
        &diff.operations[1],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "SET (LOCK_ESCALATION = 'auto, manual')"
    ));
    assert!(matches!(
        &diff.operations[2],
        SchemaDiffOperation::DropColumn { name } if name == "legacy_status"
    ));
}

#[test]
fn parse_alter_table_diff_preserves_postgres_quoted_keyword_columns() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users ADD COLUMN \"select\" TEXT, REPLICA IDENTITY FULL, DROP COLUMN IF EXISTS \"from\"",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 3);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::AddColumn { column }
            if column.name == "select" && column.data_type == "TEXT"
    ));
    assert!(matches!(
        &diff.operations[1],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "REPLICA IDENTITY FULL"
    ));
    assert!(matches!(
        &diff.operations[2],
        SchemaDiffOperation::DropColumn { name } if name == "from"
    ));
}

#[test]
fn parse_alter_table_diff_preserves_mysql_quoted_keyword_columns() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Mysql,
        "ALTER TABLE inventory.products ADD COLUMN `key` INT, COMMENT='x, y', DROP COLUMN IF EXISTS `order`",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 3);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::AddColumn { column }
            if column.name == "key" && column.data_type == "INT"
    ));
    assert!(matches!(
        &diff.operations[1],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "COMMENT='x, y'"
    ));
    assert!(matches!(
        &diff.operations[2],
        SchemaDiffOperation::DropColumn { name } if name == "order"
    ));
}

#[test]
fn parse_alter_table_diff_preserves_sqlserver_escaped_identifier_mixed_order() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::SqlServer,
        "ALTER TABLE [dbo].[orders] ADD [select] INT, NOCHECK CONSTRAINT [ck_Orders]]State], DROP COLUMN IF EXISTS [from]",
    )
    .unwrap();

    assert_eq!(diff.operations.len(), 3);
    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::AddColumn { column }
            if column.name == "select" && column.data_type == "INT"
    ));
    assert!(matches!(
        &diff.operations[1],
        SchemaDiffOperation::Unsupported { clause }
            if clause == "NOCHECK CONSTRAINT [ck_Orders]]State]"
    ));
    assert!(matches!(
        &diff.operations[2],
        SchemaDiffOperation::DropColumn { name } if name == "from"
    ));
}

#[test]
fn parse_alter_table_diff_decodes_postgres_escaped_identifier_names() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Postgres,
        "ALTER TABLE public.users ADD COLUMN \"na\"\"me\" TEXT, REPLICA IDENTITY FULL, DROP COLUMN IF EXISTS \"fr\"\"om\"",
    )
    .unwrap();

    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::AddColumn { column }
            if column.name == "na\"me" && column.data_type == "TEXT"
    ));
    assert!(matches!(
        &diff.operations[2],
        SchemaDiffOperation::DropColumn { name } if name == "fr\"om"
    ));
}

#[test]
fn parse_alter_table_diff_decodes_mysql_escaped_identifier_names() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::Mysql,
        "ALTER TABLE inventory.products ADD COLUMN `na``me` INT, COMMENT='alpha, beta'' gamma', DROP COLUMN IF EXISTS `or``der`",
    )
    .unwrap();

    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::AddColumn { column }
            if column.name == "na`me" && column.data_type == "INT"
    ));
    assert!(matches!(
        &diff.operations[2],
        SchemaDiffOperation::DropColumn { name } if name == "or`der"
    ));
}

#[test]
fn parse_alter_table_diff_decodes_sqlserver_escaped_identifier_names() {
    let diff = parse_alter_table_diff_for_dialect(
        DdlDialect::SqlServer,
        "ALTER TABLE [dbo].[orders] ADD [na]]me] INT, SET (lock_escalation = 'auto'' mode, manual'), DROP COLUMN IF EXISTS [fr]]om]",
    )
    .unwrap();

    assert!(matches!(
        &diff.operations[0],
        SchemaDiffOperation::AddColumn { column }
            if column.name == "na]me" && column.data_type == "INT"
    ));
    assert!(matches!(
        &diff.operations[2],
        SchemaDiffOperation::DropColumn { name } if name == "fr]om"
    ));
}

#[test]
fn captured_ddl_to_event_emits_schema_change() {
    let ddl = CapturedDdl {
        ddl_type: "CREATE_TABLE".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        statement: "CREATE TABLE users (id INT PRIMARY KEY)".to_string(),
        result_schema: None,
        schema_diff: None,
        ts: 1000,
    };

    let event = ddl.to_event("postgres", "0/16B6A70".to_string(), 1000);
    assert_eq!(event.op, Operation::SchemaChange);
    assert_eq!(event.source.source_name, "postgres");
    assert_eq!(event.ts, 1000);
    assert!(event.after.is_some());
}
