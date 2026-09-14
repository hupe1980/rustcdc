//! Loader tests for **source** settings the configuration reference documents with a default.
//!
//! Split from `loader_tests.rs`, which is at the per-file line budget `tests/architecture.rs`
//! enforces. Every field the reference lists with a default must load when omitted, and take
//! that default. Expected values come from the library's `Default`, so the reference, the
//! serde attribute and `Default` are pinned to one answer.
//!
//! Each test writes a real TOML document and runs the real `load()`.
//!
//! The assertions compare **whole structs** rather than field by field. That pins every field,
//! including ones added later, and it keeps `.field` reads out of this file:
//! `every_configuration_setting_is_read_by_something` matches readers by field name, so reading
//! `pg.table_include_list` here would make the SQL Server profile's field of the same name
//! look consumed, and hide a dead mapping in `SqlServerProfileConfig::to_runtime_config`.

use rustcdc::SecretString;

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
host = "db.internal"
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
    let expected = rustcdc::PostgresSourceConfig {
        host: "db.internal".into(),
        user: "cdc_user".into(),
        password: SecretString::new("pg-secret"),
        database: "mydb".into(),
        replication_slot_name: "cdc_slot".into(),
        publication_name: "cdc_pub".into(),
        ..Default::default()
    };
    assert_eq!(pg, &expected);
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
host = "db.internal"
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
    let expected = rustcdc::MysqlSourceConfig {
        host: "db.internal".into(),
        user: "cdc_user".into(),
        password: SecretString::new("mysql-secret"),
        database: "mydb".into(),
        server_id: 101,
        ..Default::default()
    };
    assert_eq!(mysql, &expected);
}

#[cfg(feature = "sqlserver")]
fn load_sqlserver(source: &str) -> rustcdc::SqlServerSourceConfig {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(dir.path(), source);
    let cfg = load(&config_path).expect("the sqlserver config should load");
    match &cfg.source.driver {
        crate::config::schema::SourceDriver::Sqlserver(profile) => profile.to_runtime_config(),
        #[allow(unreachable_patterns)]
        other => panic!("expected a sqlserver source, got {other:?}"),
    }
}

#[test]
#[cfg(feature = "sqlserver")]
fn sqlserver_fields_with_documented_defaults_may_be_omitted() {
    let _env = crate::test_env::EnvGuard::set(&[(SECRET_VAR, "mssql-secret")]);
    let loaded = load_sqlserver(
        r#"[source.sqlserver]
host = "db.internal"
user = "sa"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
"#,
    );

    let expected = rustcdc::SqlServerSourceConfig {
        host: "db.internal".into(),
        user: "sa".into(),
        password: SecretString::new("mssql-secret"),
        database: "mydb".into(),
        ..Default::default()
    };
    assert_eq!(loaded, expected);
}

// The defaults test above cannot catch a mapping that drops a field and substitutes its
// default, because the default is what it expects. Here every field the profile carries is
// set away from its default, and the expected struct is spelled out in full with no
// `..Default::default()`, so a field added to either side has to be mapped here too.
#[test]
#[cfg(feature = "sqlserver")]
fn sqlserver_profile_maps_every_setting_to_the_runtime_config() {
    let _env = crate::test_env::EnvGuard::set(&[(SECRET_VAR, "mssql-secret")]);
    let loaded = load_sqlserver(
        r#"[source.sqlserver]
host = "db.internal"
port = 14330
user = "sa"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
instance_name = "REPORTING"
conn_timeout_secs = 45
cdc_enabled = false
cdc_schema = "capture"
prereq_pool_size = 8
stream_poll_interval_ms = 250
max_events_per_poll = 500
capture_truncate_events = true
table_include_list = ["dbo.orders"]
table_exclude_list = ["dbo.audit"]
"#,
    );

    let expected = rustcdc::SqlServerSourceConfig {
        host: "db.internal".into(),
        port: 14330,
        user: "sa".into(),
        password: SecretString::new("mssql-secret"),
        database: "mydb".into(),
        instance_name: Some("REPORTING".into()),
        transport: rustcdc::TransportConfig::default(),
        conn_timeout_secs: 45,
        cdc_enabled: false,
        cdc_schema: "capture".into(),
        prereq_pool_size: 8,
        stream_poll_interval_ms: 250,
        max_events_per_poll: 500,
        capture_truncate_events: true,
        table_include_list: vec!["dbo.orders".into()],
        table_exclude_list: vec!["dbo.audit".into()],
    };
    assert_eq!(loaded, expected);
}
