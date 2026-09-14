//! Loader tests for **source** settings the configuration reference documents with a default.
//!
//! Split from `loader_tests.rs`, which is at the per-file line budget `tests/architecture.rs`
//! enforces. Every field the reference lists with a default must load when omitted, and take
//! that default. Expected values come from the library's `Default`, so the reference, the
//! serde attribute and `Default` are pinned to one answer.
//!
//! Each test writes a real TOML document and runs the real `load()`.

use super::load;

const SECRET_VAR: &str = "CDC_TEST_SOURCE_PASSWORD";

fn write_config(dir: &std::path::Path, source: &str) -> std::path::PathBuf {
    let config_path = dir.join("cdc.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1"

{source}
[sink]
type = "stdout"

[state]
dir = "{}"
"#,
            dir.join("state").display()
        ),
    )
    .expect("write config");
    config_path
}

#[test]
#[cfg(feature = "postgres")]
fn postgres_fields_with_documented_defaults_may_be_omitted() {
    let _env = crate::test_env::EnvGuard::set(&[(SECRET_VAR, "pg-secret")]);
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(
        dir.path(),
        r#"[source.postgres]
host = "localhost"
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
"#,
    );

    let cfg = load(&config_path).expect("a config omitting defaulted fields should load");
    let pg = match &cfg.source.driver {
        crate::config::schema::SourceDriver::Postgres(pg) => pg,
        #[allow(unreachable_patterns)]
        other => panic!("expected a postgres source, got {other:?}"),
    };
    let expected = rustcdc::PostgresSourceConfig::default();
    assert_eq!(pg.port, expected.port);
    assert_eq!(pg.transport, expected.transport);
    assert_eq!(pg.conn_timeout_secs, expected.conn_timeout_secs);
    assert_eq!(pg.stream_poll_interval_ms, expected.stream_poll_interval_ms);
    assert_eq!(pg.max_events_per_poll, expected.max_events_per_poll);
    assert_eq!(pg.table_include_list, expected.table_include_list);
    assert_eq!(pg.table_exclude_list, expected.table_exclude_list);
}

#[test]
#[cfg(feature = "mysql")]
fn mysql_fields_with_documented_defaults_may_be_omitted() {
    let _env = crate::test_env::EnvGuard::set(&[(SECRET_VAR, "mysql-secret")]);
    let dir = tempfile::tempdir().expect("tempdir");
    // `server_id` stays required: its default of 0 is a deliberate tripwire.
    let config_path = write_config(
        dir.path(),
        r#"[source.mysql]
host = "localhost"
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
server_id = 101
"#,
    );

    let cfg = load(&config_path).expect("a config omitting defaulted fields should load");
    let mysql = match &cfg.source.driver {
        crate::config::schema::SourceDriver::Mysql(mysql) => mysql,
        #[allow(unreachable_patterns)]
        other => panic!("expected a mysql source, got {other:?}"),
    };
    let expected = rustcdc::MysqlSourceConfig::default();
    assert_eq!(mysql.port, expected.port);
    assert_eq!(mysql.gtid_mode_enabled, expected.gtid_mode_enabled);
    assert_eq!(mysql.binlog_format_check, expected.binlog_format_check);
    assert_eq!(mysql.transport, expected.transport);
    assert_eq!(mysql.conn_timeout_secs, expected.conn_timeout_secs);
    assert_eq!(
        mysql.stream_poll_interval_ms,
        expected.stream_poll_interval_ms
    );
    assert_eq!(mysql.max_events_per_poll, expected.max_events_per_poll);
    assert_eq!(mysql.table_include_list, expected.table_include_list);
    assert_eq!(mysql.table_exclude_list, expected.table_exclude_list);
}

#[test]
#[cfg(feature = "sqlserver")]
fn sqlserver_fields_with_documented_defaults_may_be_omitted() {
    let _env = crate::test_env::EnvGuard::set(&[(SECRET_VAR, "mssql-secret")]);
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(
        dir.path(),
        r#"[source.sqlserver]
host = "localhost"
user = "sa"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
"#,
    );

    let cfg = load(&config_path).expect("a config omitting defaulted fields should load");
    let profile = match &cfg.source.driver {
        crate::config::schema::SourceDriver::Sqlserver(profile) => profile,
        #[allow(unreachable_patterns)]
        other => panic!("expected a sqlserver source, got {other:?}"),
    };
    let loaded = profile.to_runtime_config();
    let expected = rustcdc::SqlServerSourceConfig::default();
    assert_eq!(loaded.port, expected.port);
    assert_eq!(loaded.transport, expected.transport);
    assert_eq!(loaded.conn_timeout_secs, expected.conn_timeout_secs);
    assert_eq!(loaded.cdc_enabled, expected.cdc_enabled);
    assert_eq!(loaded.cdc_schema, expected.cdc_schema);
    assert_eq!(loaded.prereq_pool_size, expected.prereq_pool_size);
    assert_eq!(
        loaded.stream_poll_interval_ms,
        expected.stream_poll_interval_ms
    );
    assert_eq!(loaded.max_events_per_poll, expected.max_events_per_poll);
}
