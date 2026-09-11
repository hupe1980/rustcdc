//! Tests for the configuration loader.
//!
//! Split out of `loader.rs`, where the fixtures were the larger half of the file. Each
//! test writes a real TOML document and runs the real `load()`, so a rule that is not
//! actually enforced fails here rather than in production.

use std::path::{Path, PathBuf};

use super::{load, load_and_migrate};
use crate::config::schema::AppConfig;
use crate::token_manifest_policy::{
    TokenManifestFile, TokenManifestSignature, TokenManifestToken, TokenManifestUnsigned,
    canonical_signing_payload,
};
use ed25519_dalek::{Signer, SigningKey};

/// Destructure a `SourceDriver` known to be PostgreSQL, under every feature set.
///
/// `let SourceDriver::Postgres(pg) = … else { panic!() }` reads better but does not
/// compile both ways: in a default (PostgreSQL-only) build the enum has one variant, so
/// the pattern is irrefutable and `-D irrefutable-let-patterns` rejects the `else`. With
/// `--all-features` there are four variants and the `else` is required. A `match` with an
/// allowed-unreachable arm is the one shape that satisfies both.
#[track_caller]
fn expect_postgres(config: &crate::config::AppConfig) -> &rustcdc::PostgresSourceConfig {
    match &config.source.driver {
        crate::config::schema::SourceDriver::Postgres(pg) => pg,
        #[allow(unreachable_patterns)]
        other => panic!("expected a postgres source, got {other:?}"),
    }
}

fn write_signed_manifest(path: &Path, tokens: Vec<TokenManifestToken>) -> String {
    let signing_key = SigningKey::from_bytes(&[0x11; 32]);
    let public_key_hex = hex::encode(signing_key.verifying_key().to_bytes());

    let unsigned = TokenManifestUnsigned {
        tokens: tokens.clone(),
    };
    let unsigned_bytes = canonical_signing_payload(&unsigned).expect("serialize unsigned manifest");
    let signature = signing_key.sign(&unsigned_bytes);

    let manifest = TokenManifestFile {
        tokens,
        signature: TokenManifestSignature {
            algorithm: "ed25519".to_string(),
            public_key_hex: public_key_hex.clone(),
            signature_hex: hex::encode(signature.to_bytes()),
        },
    };

    std::fs::write(
        path,
        serde_json::to_vec_pretty(&manifest).expect("serialize signed manifest"),
    )
    .expect("write manifest");

    public_key_hex
}

#[test]
fn loads_valid_config_without_admin_or_observability_sections() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state_dir = dir.path().join("state");
    let config_path = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"
delivery_contract = "at_least_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc-events"

[state]
dir = "{}"
"#,
            state_dir.display()
        ),
    )
    .expect("write config");

    let cfg = load(&config_path).expect("config should load");
    assert_eq!(cfg.admin.bind, "127.0.0.1:8080");
    assert_eq!(cfg.observability.service_name, "rustcdc-server");
    assert!(cfg.observability.otlp_endpoint.is_none());
}

#[test]
fn rejects_unsupported_older_api_version() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha0"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("unsupported older api version should fail closed");
    assert!(
        err.to_string()
            .contains("api_version must be \"v1\", got \"v1alpha0\""),
        "unexpected error: {err}"
    );
}

#[test]
fn rejects_unknown_api_version_when_no_migration_path_exists() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v9"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("unknown api version should fail closed");
    assert!(
        err.to_string()
            .contains("api_version must be \"v1\", got \"v9\""),
        "unexpected error: {err}"
    );
}

#[test]
fn load_and_migrate_uses_supported_target_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let cfg = load_and_migrate(&config_path)
        .expect("load_and_migrate should succeed for supported version");
    assert_eq!(cfg.api_version, AppConfig::SUPPORTED_API_VERSION);
}

#[test]
fn accepts_non_loopback_plaintext_postgres_source_transport() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "postgres.internal"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    load(&config_path).expect("non-loopback plaintext should be allowed for DX");
}

#[test]
fn accepts_non_loopback_postgres_source_with_tls_transport() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "postgres.internal"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "tls"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    load(&config_path).expect("tls transport should pass for non-loopback source host");
}

#[test]
#[cfg(feature = "mysql")]
fn accepts_mysql_source_profile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.mysql]
host = "localhost"
port = 3306
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
server_id = 101
gtid_mode_enabled = false
binlog_format_check = true
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.mysql.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    load(&config_path).expect("mysql source should be accepted");
}

#[test]
#[cfg(feature = "mysql")]
fn accepts_mariadb_source_profile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.mariadb]
host = "localhost"
port = 3306
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
server_id = 202
gtid_mode_enabled = false
binlog_format_check = true
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.mariadb.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    load(&config_path).expect("mariadb source should be accepted");
}

#[test]
#[cfg(feature = "sqlserver")]
fn accepts_mssql_source_profile_alias() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.mssql]
host = "localhost"
port = 1433
user = "sa"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
instance_name = "SQLEXPRESS"
conn_timeout_secs = 10
cdc_enabled = true
cdc_schema = "cdc"
prereq_pool_size = 2
stream_poll_interval_ms = 1000
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.mssql.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    load(&config_path).expect("mssql alias source should be accepted");
}

#[test]
fn rejects_multiple_source_profiles() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"
delivery_contract = "at_least_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[source.mysql]
host = "mysql.internal"
port = 3306

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("multiple sources should fail closed");
    assert!(
        err.to_string()
            .contains("exactly one source block must be configured"),
        "unexpected error: {err}"
    );
}

#[test]
fn rejects_effectively_once_contract_for_non_idempotent_sink() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"
delivery_contract = "effectively_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected delivery contract rejection");
    assert!(
        err.to_string().contains(
            "delivery_contract='effectively_once' is incompatible with sink.type='stdout'"
        ),
        "unexpected error: {err}"
    );
}

#[test]
fn rejects_effectively_once_contract_for_non_transactional_kafka_sink() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"
delivery_contract = "effectively_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc-events"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path)
        .expect_err("expected effectively_once contract rejection without transactional mode");
    assert!(
        err.to_string().contains(
            "delivery_contract='effectively_once' is incompatible with sink.type='kafka': missing idempotent delivery and transactional checkpoint barrier coupling"
        ) || err.to_string().contains(
            "delivery_contract='effectively_once' is incompatible with sink.type='kafka': missing transactional checkpoint barrier coupling"
        ),
        "unexpected error: {err}"
    );
}

#[test]
fn accepts_effectively_once_contract_with_transactional_kafka_sink() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"
delivery_contract = "effectively_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc-events"
delivery_mode = "transactional"
transactional_id = "cdc-eos-1"

[state]
dir = "/tmp/cdc-state"

[state.backend]
kafka_topic = { brokers = "localhost:9092", topic = "cdc-state", min_replication_factor = 1, min_insync_replicas = 1, durability_profile = "development" }
"#,
    )
    .expect("write config");

    load(&config_path).expect("transactional kafka sink should satisfy effectively_once");
}

#[test]
fn rejects_transactional_kafka_sink_when_delivery_contract_is_at_least_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"
delivery_contract = "at_least_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc-events"
delivery_mode = "transactional"
transactional_id = "cdc-eos-1"

[state]
dir = "/tmp/cdc-state"

[state.backend]
kafka_topic = { brokers = "localhost:9092", topic = "cdc-state", min_replication_factor = 1, min_insync_replicas = 1, durability_profile = "development" }
"#,
    )
    .expect("write config");

    let err = load(&config_path)
        .expect_err("expected delivery_contract mismatch for transactional kafka mode");
    assert!(
        err.to_string().contains(
            "delivery_contract must be \"effectively_once\" when sink.kafka.delivery_mode=\"transactional\""
        ),
        "unexpected error: {err}"
    );
}

#[test]
fn accepts_source_kind_when_matching_configured_profile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source]
kind = "postgres"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    load(&config_path).expect("matching source.kind should load");
}

/// A `[source.mysql]` block under `kind = "postgres"` must not load.
///
/// Gated on `mysql` because the *reason* it fails differs by build: with the connector
/// compiled in, deserialization fails on the mismatch this test is about; without it, the
/// config is rejected earlier by `reject_uncompiled_source_driver`, which is a different
/// property with its own tests in `tests/config_roundtrip.rs`.
#[test]
#[cfg(feature = "mysql")]
fn rejects_source_kind_when_not_matching_configured_profile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source]
kind = "postgres"

[source.mysql]
host = "mysql.internal"
port = 3306

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("mismatched source.kind should fail");
    assert!(
        err.to_string().contains("missing field")
            || err.to_string().contains("failed to deserialize"),
        "unexpected error: {err}"
    );
}

/// `write_mode` was a single-variant enum whose validator could never fire — serde
/// rejected everything but `"append"` before `validate()` ever ran. It has been removed:
/// a configuration knob with exactly one legal value is documentation pretending to be
/// configuration, and it invited operators to believe an upsert mode existed.
///
/// The key is now simply unrecognised, which produces a better message — it names the
/// key and points at the reference — and this test pins that, so reintroducing the field
/// when the Iceberg sink actually gains upsert is a deliberate act.
#[test]
fn rejects_iceberg_write_mode_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let table_path = dir.path().join("iceberg-table");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "iceberg"
table_path = "{}"
write_mode = "upsert"

[sink.catalog.rest]
uri = "http://127.0.0.1:8181"
warehouse = "file:///tmp/cdc-iceberg-warehouse"

[state]
dir = "/tmp/cdc-state"
"#,
            table_path.display()
        ),
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid iceberg sink config");
    let rendered = err.to_string();
    assert!(
        rendered.contains("unrecognised configuration key") && rendered.contains("write_mode"),
        "the error must name the key so an operator can find it: {rendered}"
    );
}

#[test]
fn rejects_wrong_api_version() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v9"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid api version error");
    assert!(err.to_string().contains("v9"));
}

#[test]
fn rejects_non_loopback_admin_without_tls() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"
delivery_contract = "at_least_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
bind = "0.0.0.0:8080"
read_token_env = "RUSTCDC_ADMIN_READ_TOKEN"
write_token_env = "RUSTCDC_ADMIN_WRITE_TOKEN"
notification_log_file = "/tmp/cdc-test-notifications.jsonl"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected non-loopback TLS requirement failure");
    assert!(err.to_string().contains("admin.tls is required"));
}

#[test]
fn rejects_empty_admin_audit_log_file_when_configured() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
audit_log_file = ""
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected empty audit log file rejection");
    assert!(
        err.to_string()
            .contains("admin.audit_log_file must not be empty when configured"),
        "unexpected error: {err}"
    );
}

#[test]
fn rejects_admin_audit_log_file_when_parent_directory_is_missing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let missing_parent = dir.path().join("missing");
    let audit_log_file = missing_parent.join("admin-audit.jsonl");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
audit_log_file = "{}"
"#,
            audit_log_file.display()
        ),
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected missing audit log parent rejection");
    assert!(
        err.to_string()
            .contains("admin.audit_log_file parent directory does not exist"),
        "unexpected error: {err}"
    );
}

#[test]
fn accepts_admin_audit_log_file_when_parent_directory_exists() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let log_dir = dir.path().join("logs");
    let audit_log_file = log_dir.join("admin-audit.jsonl");
    std::fs::create_dir_all(&log_dir).expect("create log directory");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
audit_log_file = "{}"
"#,
            audit_log_file.display()
        ),
    )
    .expect("write config");

    load(&config_path).expect("expected valid audit log file config");
}

#[test]
fn rejects_empty_admin_notification_log_file_when_configured() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
notification_log_file = ""
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected empty notification log file rejection");
    assert!(
        err.to_string()
            .contains("admin.notification_log_file must not be empty when configured"),
        "unexpected error: {err}"
    );
}

#[test]
fn rejects_admin_notification_log_file_when_parent_directory_is_missing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let missing_parent = dir.path().join("missing");
    let notification_log_file = missing_parent.join("admin-notifications.jsonl");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
notification_log_file = "{}"
"#,
            notification_log_file.display()
        ),
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected missing notification log parent rejection");
    assert!(
        err.to_string()
            .contains("admin.notification_log_file parent directory does not exist"),
        "unexpected error: {err}"
    );
}

#[test]
fn accepts_admin_notification_log_file_when_parent_directory_exists() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let log_dir = dir.path().join("logs");
    let notification_log_file = log_dir.join("admin-notifications.jsonl");
    std::fs::create_dir_all(&log_dir).expect("create log directory");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
notification_log_file = "{}"
"#,
            notification_log_file.display()
        ),
    )
    .expect("write config");

    load(&config_path).expect("expected valid notification log file config");
}

#[test]
fn rejects_admin_signal_ingress_file_when_parent_directory_is_missing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let missing_parent = dir.path().join("missing");
    let signal_ingress_file = missing_parent.join("admin-signal-ingress.jsonl");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
signal_ingress_file = "{}"
"#,
            signal_ingress_file.display()
        ),
    )
    .expect("write config");

    let err =
        load(&config_path).expect_err("expected missing signal ingress parent directory rejection");
    assert!(
        err.to_string()
            .contains("admin.signal_ingress_file parent directory does not exist"),
        "unexpected error: {err}"
    );
}

#[test]
fn accepts_admin_signal_ingress_file_when_parent_directory_exists() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let ingress_dir = dir.path().join("ingress");
    let signal_ingress_file = ingress_dir.join("admin-signal-ingress.jsonl");
    std::fs::create_dir_all(&ingress_dir).expect("create ingress directory");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
signal_ingress_file = "{}"
"#,
            signal_ingress_file.display()
        ),
    )
    .expect("write config");

    load(&config_path).expect("expected valid signal ingress file config");
}

#[test]
fn rejects_admin_notification_kafka_with_empty_brokers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin.notification_kafka]
brokers = ""
topic = "cdc-admin-notifications"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid admin notification kafka config");
    assert!(
        err.to_string()
            .contains("admin.notification_kafka.brokers must not be empty"),
        "unexpected error: {err}"
    );
}

#[test]
fn accepts_admin_notification_kafka_with_valid_plaintext_config() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin.notification_kafka]
brokers = "kafka-1:9092"
topic = "cdc-admin-notifications"
client_id = "cdc-admin-test"
"#,
    )
    .expect("write config");

    load(&config_path).expect("expected valid admin notification kafka config");
}

#[test]
fn rejects_admin_signal_ingress_kafka_with_empty_brokers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin.signal_ingress_kafka]
brokers = ""
topic = "cdc-admin-signals"
group_id = "cdc-admin-signals-group"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid admin signal ingress kafka config");
    assert!(
        err.to_string()
            .contains("admin.signal_ingress_kafka.brokers must not be empty"),
        "unexpected error: {err}"
    );
}

#[test]
fn accepts_admin_signal_ingress_kafka_with_valid_plaintext_config() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin.signal_ingress_kafka]
brokers = "localhost:9092"
topic = "cdc-admin-signals"
group_id = "cdc-admin-signals-group"
"#,
    )
    .expect("write config");

    load(&config_path).expect("expected valid admin signal ingress kafka config");
}

#[test]
fn rejects_write_capable_admin_without_non_admin_notification_channels() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
write_token_env = "RUSTCDC_ADMIN_WRITE_TOKEN"
"#,
    )
    .expect("write config");

    let err = load(&config_path)
        .expect_err("expected rejection for write-capable admin without notification channels");
    assert!(
        err.to_string().contains(
            "admin.notification_log_file or admin.notification_kafka is required when write-capable admin signaling is enabled"
        ),
        "unexpected error: {err}"
    );
}

#[test]
fn accepts_write_capable_admin_with_non_admin_notification_log_channel() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let log_dir = dir.path().join("logs");
    let notification_log_file = log_dir.join("admin-notifications.jsonl");
    std::fs::create_dir_all(&log_dir).expect("create log directory");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
write_token_env = "RUSTCDC_ADMIN_WRITE_TOKEN"
notification_log_file = "{}"
"#,
            notification_log_file.display()
        ),
    )
    .expect("write config");

    load(&config_path).expect("expected valid write-capable admin with notification channel");
}

#[test]
fn rejects_admin_tls_mtls_without_client_ca_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let cert_path = dir.path().join("admin-cert.pem");
    let key_path = dir.path().join("admin-key.pem");

    std::fs::write(&cert_path, "dummy cert").expect("write cert");
    std::fs::write(&key_path, "dummy key").expect("write key");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
bind = "127.0.0.1:8080"

[admin.tls]
cert_file = "{}"
key_file = "{}"
require_client_cert = true
"#,
            cert_path.display(),
            key_path.display()
        ),
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected mTLS client CA requirement failure");
    assert!(
        err.to_string()
            .contains("admin.tls.client_ca_file is required")
    );
}

#[test]
fn rejects_invalid_admin_token_manifest_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let manifest_path = dir.path().join("admin-tokens.json");

    let trusted_public_key_hex = write_signed_manifest(
        &manifest_path,
        vec![TokenManifestToken {
            id: "ops".to_string(),
            token_sha256_hex: "bad".to_string(),
            scopes: vec!["read".to_string()],
            not_before: None,
            expires_at: None,
            revoked: false,
        }],
    );

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
token_manifest_file = "{}"
token_manifest_trusted_public_keys_hex = ["{}"]
token_manifest_max_staleness_ms = 60000
notification_log_file = "/tmp/cdc-test-notifications.jsonl"
"#,
            manifest_path.display(),
            trusted_public_key_hex
        ),
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid manifest hash failure");
    assert!(err.to_string().contains("token_sha256_hex"));
}

#[test]
fn rejects_empty_admin_token_manifest_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let manifest_path = dir.path().join("admin-tokens.json");

    let trusted_public_key_hex = write_signed_manifest(&manifest_path, vec![]);

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
token_manifest_file = "{}"
token_manifest_trusted_public_keys_hex = ["{}"]
token_manifest_max_staleness_ms = 60000
notification_log_file = "/tmp/cdc-test-notifications.jsonl"
"#,
            manifest_path.display(),
            trusted_public_key_hex
        ),
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected empty manifest rejection");
    assert!(
        err.to_string()
            .contains("must contain at least one token entry")
    );
}

#[test]
fn rejects_admin_token_manifest_without_trusted_public_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let manifest_path = dir.path().join("admin-tokens.json");

    write_signed_manifest(
        &manifest_path,
        vec![TokenManifestToken {
            id: "ops".to_string(),
            token_sha256_hex: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
            scopes: vec!["read".to_string()],
            not_before: None,
            expires_at: None,
            revoked: false,
        }],
    );

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
token_manifest_file = "{}"
token_manifest_max_staleness_ms = 60000
notification_log_file = "/tmp/cdc-test-notifications.jsonl"
"#,
            manifest_path.display()
        ),
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected trusted key requirement failure");
    assert!(
        err.to_string()
            .contains("token_manifest_trusted_public_keys_hex")
    );
}

#[test]
fn rejects_non_loopback_admin_without_write_token_env() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let cert_path = dir.path().join("admin-cert.pem");
    let key_path = dir.path().join("admin-key.pem");

    std::fs::write(&cert_path, "dummy cert").expect("write cert");
    std::fs::write(&key_path, "dummy key").expect("write key");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
bind = "0.0.0.0:8080"
read_token_env = "RUSTCDC_ADMIN_READ_TOKEN"

[admin.tls]
cert_file = "{}"
key_file = "{}"
"#,
            cert_path.display(),
            key_path.display()
        ),
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected write token requirement failure");
    assert!(err.to_string().contains("admin.write_token_env"));
}

#[test]
fn accepts_non_loopback_admin_with_token_manifest_and_tls() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let manifest_path = dir.path().join("admin-tokens.json");
    let cert_path = dir.path().join("admin-cert.pem");
    let key_path = dir.path().join("admin-key.pem");

    let trusted_public_key_hex = write_signed_manifest(
        &manifest_path,
        vec![TokenManifestToken {
            id: "ops-read".to_string(),
            token_sha256_hex: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
            scopes: vec!["read".to_string(), "write".to_string()],
            not_before: None,
            expires_at: Some(
                chrono::DateTime::parse_from_rfc3339("2099-01-01T00:00:00Z")
                    .expect("parse expires_at")
                    .with_timezone(&chrono::Utc),
            ),
            revoked: false,
        }],
    );
    std::fs::write(&cert_path, "dummy cert").expect("write cert");
    std::fs::write(&key_path, "dummy key").expect("write key");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
bind = "0.0.0.0:8080"
token_manifest_file = "{}"
token_manifest_trusted_public_keys_hex = ["{}"]
token_manifest_max_staleness_ms = 60000
notification_log_file = "/tmp/cdc-test-notifications.jsonl"

[admin.tls]
cert_file = "{}"
key_file = "{}"
"#,
            manifest_path.display(),
            trusted_public_key_hex,
            cert_path.display(),
            key_path.display()
        ),
    )
    .expect("write config");

    let cfg = load(&config_path).expect("expected config to load");
    assert_eq!(cfg.admin.bind, "0.0.0.0:8080");
    assert_eq!(cfg.admin.token_manifest_file, Some(manifest_path));
}

#[test]
fn rejects_zero_admin_token_manifest_refresh_interval() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"
delivery_contract = "effectively_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
token_manifest_refresh_ms = 0
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid manifest refresh interval");
    assert!(err.to_string().contains("token_manifest_refresh_ms"));
}

#[test]
fn rejects_admin_token_manifest_without_max_staleness_policy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let manifest_path = dir.path().join("admin-tokens.json");

    let trusted_public_key_hex = write_signed_manifest(
        &manifest_path,
        vec![TokenManifestToken {
            id: "ops".to_string(),
            token_sha256_hex: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
            scopes: vec!["read".to_string(), "write".to_string()],
            not_before: None,
            expires_at: None,
            revoked: false,
        }],
    );

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
token_manifest_file = "{}"
token_manifest_trusted_public_keys_hex = ["{}"]
notification_log_file = "/tmp/cdc-test-notifications.jsonl"
"#,
            manifest_path.display(),
            trusted_public_key_hex
        ),
    )
    .expect("write config");

    let err = load(&config_path)
        .expect_err("expected staleness policy requirement for manifest-backed auth");
    assert!(
        err.to_string()
            .contains("token_manifest_max_staleness_ms is required")
    );
}

#[test]
fn rejects_admin_manifest_max_staleness_without_manifest_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"
delivery_contract = "effectively_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
token_manifest_max_staleness_ms = 1000
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected staleness policy to require manifest file");
    assert!(
        err.to_string()
            .contains("token_manifest_max_staleness_ms requires admin.token_manifest_file")
    );
}

#[test]
fn accepts_loopback_admin_with_unauthenticated_probe_mode() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
probe_auth_mode = "allow_unauthenticated_loopback"
"#,
    )
    .expect("write config");

    let cfg = load(&config_path).expect("expected config to load");
    assert_eq!(cfg.admin.bind, "127.0.0.1:8080");
}

#[test]
fn rejects_non_loopback_admin_with_unauthenticated_probe_mode() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let cert_path = dir.path().join("admin-cert.pem");
    let key_path = dir.path().join("admin-key.pem");

    std::fs::write(&cert_path, "dummy cert").expect("write cert");
    std::fs::write(&key_path, "dummy key").expect("write key");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
bind = "0.0.0.0:8080"
read_token_env = "RUSTCDC_ADMIN_READ_TOKEN"
write_token_env = "RUSTCDC_ADMIN_WRITE_TOKEN"
probe_auth_mode = "allow_unauthenticated_loopback"

[admin.tls]
cert_file = "{}"
key_file = "{}"
"#,
            cert_path.display(),
            key_path.display()
        ),
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected loopback-only probe auth mode enforcement");
    assert!(err.to_string().contains(
        "admin.probe_auth_mode=allow_unauthenticated_loopback requires loopback admin.bind"
    ));
}

#[test]
fn rejects_invalid_admin_rate_limit_configuration() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
metrics_rate_limit_rps = 10
metrics_rate_limit_burst = 5
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected admin rate-limit validation failure");
    assert!(
        err.to_string()
            .contains("admin.metrics_rate_limit_burst must be >= admin.metrics_rate_limit_rps")
    );
}

#[test]
fn rejects_invalid_admin_trusted_proxy_ip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[admin]
trusted_proxy_ips = ["not-an-ip"]
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid trusted proxy ip");
    assert!(err.to_string().contains("admin.trusted_proxy_ips"));
}

#[test]
fn rejects_http_sink_with_empty_url() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = ""

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid http sink URL");
    assert!(err.to_string().contains("sink.http.url"));
}

#[test]
fn rejects_http_sink_with_invalid_backoff_range() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://localhost:8080/events"
backoff_initial_ms = 5000
backoff_max_ms = 100

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid backoff range");
    assert!(err.to_string().contains("backoff_initial_ms"));
}

#[test]
fn rejects_runtime_zero_max_event_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[runtime]
max_event_bytes = 0
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid runtime max_event_bytes");
    assert!(err.to_string().contains("runtime.max_event_bytes"));
}

#[test]
fn rejects_runtime_flush_interval_above_max_buffer_size() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[runtime]
max_buffer_size = 50
sink_flush_interval_events = 100
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid runtime flush interval");
    assert!(
        err.to_string()
            .contains("runtime.sink_flush_interval_events must be <= runtime.max_buffer_size")
    );
}

#[test]
fn rejects_kafka_sink_with_empty_brokers_or_topic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = " , "
topic = ""

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid kafka sink config");
    assert!(
        err.to_string().contains("sink.kafka.brokers")
            || err.to_string().contains("sink.kafka.topic")
    );
}

#[test]
fn rejects_kafka_topic_state_backend_with_invalid_thresholds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[state.backend]
kafka_topic = { brokers = "kafka-1:9092", topic = "cdc-state", min_replication_factor = 1, min_insync_replicas = 2, durability_profile = "development" }
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid kafka topic state config");
    assert!(err.to_string().contains("min_insync_replicas"));
}

#[test]
fn rejects_avro_sink_with_empty_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "avro"
path = ""

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid avro sink config");
    assert!(
        err.to_string().contains("avro") && err.to_string().contains("no longer supported"),
        "unexpected error: {err}"
    );
}

#[test]
fn rejects_wasm_runtime_without_module_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[transform_runtime]
mode = "wasm"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid wasm runtime config");
    assert!(
        err.to_string()
            .contains("transform_runtime.wasm.module_path")
    );
}

#[test]
fn rejects_transform_rule_with_empty_actions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"

[[transforms]]
name = "bad"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid transform config");
    assert!(err.to_string().contains("at least one action"));
}

#[test]
fn rejects_kafka_tls_with_verify_peer_disabled_and_missing_ca_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let missing_ca = dir.path().join("missing-ca.pem");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc-events"

[sink.security]
protocol = "tls"
verify_peer = false
ssl_ca_location = "{}"

[state]
dir = "/tmp/cdc-state"
"#,
            missing_ca.display()
        ),
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid kafka tls config");
    let message = err.to_string();
    assert!(
        message.contains("verify_peer") || message.contains("ssl_ca_location"),
        "unexpected error: {message}"
    );
}

#[test]
fn accepts_transactional_kafka_sink_mode() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"
delivery_contract = "effectively_once"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc-events"
delivery_mode = "transactional"
transactional_id = "cdc-eos-1"

[state]
dir = "/tmp/cdc-state"

[state.backend]
kafka_topic = { brokers = "localhost:9092", topic = "cdc-state", min_replication_factor = 1, min_insync_replicas = 1, durability_profile = "development" }
"#,
    )
    .expect("write config");

    load(&config_path).expect("transactional kafka sink config should load");
}

#[test]
fn rejects_http_sink_with_verify_tls_disabled() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://127.0.0.1:8081/events"
verify_tls = false

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid insecure http sink config");
    assert!(
        err.to_string()
            .contains("sink.http.verify_tls must be true")
    );
}

#[test]
fn rejects_http_sink_with_non_https_non_loopback_url() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://example.com/events"
verify_tls = true

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected invalid non-https http sink url");
    assert!(
        err.to_string()
            .contains("sink.http.url must use https except for localhost loopback testing")
    );
}

#[test]
fn rejects_http_sink_with_inline_bearer_secret_literal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://127.0.0.1:8080/events"
bearer_token = "top-secret-token"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected inline bearer token literal to be rejected");
    assert!(
        err.to_string()
            .contains("sink.http.bearer_token must use deferred secret references")
    );
}

/// `{ env = "VAR" }` references resolve to the environment value for every
/// secret-bearing field — the pattern the docs and `rustcdc init` templates
/// use must actually load.
#[test]
fn resolves_env_secret_references_at_load_time() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    // Unique names to avoid collisions with parallel tests.
    let _env = crate::test_env::EnvGuard::set(&[
        ("CDC_LOADER_TEST_PG_PASSWORD", "pg-secret-from-env"),
        ("CDC_LOADER_TEST_BEARER", "bearer-from-env"),
    ]);

    std::fs::write(
        &config_path,
        r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_LOADER_TEST_PG_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://127.0.0.1:8080/events"
bearer_token = { env = "CDC_LOADER_TEST_BEARER" }

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let config = load(&config_path).expect("env-referenced secrets must load");

    let pg = expect_postgres(&config);
    assert_eq!(
        pg.password.resolve().expect("resolve password"),
        "pg-secret-from-env"
    );
    let crate::config::schema::SinkConfig::Http(http) = &config.sink else {
        panic!("expected http sink");
    };
    assert_eq!(
        http.bearer_token
            .as_ref()
            .expect("bearer token present")
            .resolve()
            .expect("resolve bearer token"),
        "bearer-from-env"
    );
}

/// An unset env reference must fail loudly, naming the variable and the
/// config path — not default to an empty credential.
#[test]
fn rejects_unset_env_secret_reference() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_LOADER_TEST_DEFINITELY_UNSET_VAR" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("unset env reference must be rejected");
    let message = err.to_string();
    assert!(
        message.contains("CDC_LOADER_TEST_DEFINITELY_UNSET_VAR"),
        "error must name the variable: {message}"
    );
    assert!(
        message.contains("source.password"),
        "error must name the config path: {message}"
    );
}

#[test]
fn rejects_iceberg_sink_with_inline_catalog_token_literal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "iceberg"
table_path = "/tmp/iceberg-table"
namespace = "cdc"
table_name = "events"

[sink.catalog.rest]
uri = "http://127.0.0.1:8181"
warehouse = "file:///tmp/iceberg-warehouse"
token = "inline-token-literal"

[state]
dir = "/tmp/cdc-state"
"#,
    )
    .expect("write config");

    let err = load(&config_path).expect_err("expected inline iceberg token literal to be rejected");
    assert!(
        err.to_string()
            .contains("sink.iceberg.catalog.rest.token must use deferred secret references")
    );
}

/// AWS S3 Tables is selected by the catalog table key, and needs no warehouse URI.
///
/// The catalog is an enum over `[sink.catalog.rest]` and `[sink.catalog.s3tables]` rather
/// than a struct with two optional sub-tables, so "both" and "neither" cannot be written
/// down — they are parse errors rather than validation errors.
#[test]
fn accepts_iceberg_sink_with_an_s3tables_catalog() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");

    let toml = |arn: &str| {
        format!(
            r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "iceberg"
table_path = "/tmp/iceberg-table"
namespace = "cdc"
table_name = "events"

[sink.catalog.s3tables]
table_bucket_arn = "{arn}"

[state]
dir = "/tmp/cdc-state"
"#
        )
    };

    let _env = crate::test_env::EnvGuard::set(&[("CDC_TEST_SOURCE_PASSWORD", "pg-secret")]);

    std::fs::write(
        &config_path,
        toml("arn:aws:s3tables:eu-central-1:123456789012:bucket/cdc"),
    )
    .expect("write config");
    let config = load(&config_path).expect("an s3tables catalog must load");
    let crate::config::sink::SinkConfig::Iceberg(iceberg) = &config.sink else {
        panic!("expected an iceberg sink");
    };
    assert_eq!(iceberg.catalog.kind(), "s3tables");
    assert_eq!(
        iceberg.catalog.location(),
        "arn:aws:s3tables:eu-central-1:123456789012:bucket/cdc"
    );

    // A malformed ARN is refused at load. Left to the AWS SDK it surfaces as an opaque
    // signing failure on the first flush, which is during an incident rather than during
    // `validate-config`.
    std::fs::write(&config_path, toml("my-bucket")).expect("write config");
    let err = load(&config_path).expect_err("a non-ARN must be refused");
    assert!(
        err.to_string().contains("is not an S3 Tables ARN"),
        "unexpected: {err}"
    );
}

/// The Snowflake durability wait must fit inside the runtime's flush timeout.
///
/// Both defaults were 60 000 ms, so `runtime.sink_flush_timeout_ms` raced
/// `sink.snowflake.commit_timeout_ms` and usually won — cancelling the wait after the rows
/// had been appended. The sink reconciles so no duplicate reaches Snowflake either way;
/// this rule stops the situation arising, because a bound the caller abandons is not a
/// bound.
#[test]
fn rejects_a_snowflake_commit_wait_that_outlives_the_runtime_flush_timeout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("cdc.toml");
    let key_path = dir.path().join("key.pem");
    std::fs::write(
        &key_path,
        include_str!("../../tests/fixtures/snowflake_test_key.pem"),
    )
    .expect("write key");

    let toml = |commit_timeout_ms: u64, flush_timeout_ms: u64| {
        format!(
            r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[runtime]
sink_flush_timeout_ms = {flush_timeout_ms}

[sink]
type = "snowflake"
account_url = "https://myorg-myaccount.snowflakecomputing.com"
account = "MYORG-MYACCOUNT"
user = "CDC_SVC"
database = "CDC"
schema = "PUBLIC"
pipe = "EVENTS_PIPE"
commit_timeout_ms = {commit_timeout_ms}

[sink.auth]
type = "key_pair"
private_key = {{ env = "CDC_TEST_SNOWFLAKE_KEY" }}

[state]
dir = "/tmp/cdc-state"
"#
        )
    };

    let _env = crate::test_env::EnvGuard::set(&[
        ("CDC_TEST_SOURCE_PASSWORD", "pg-secret"),
        (
            "CDC_TEST_SNOWFLAKE_KEY",
            include_str!("../../tests/fixtures/snowflake_test_key.pem"),
        ),
    ]);

    // Equal timeouts — the shape that shipped, and the one that races.
    std::fs::write(&config_path, toml(60_000, 60_000)).expect("write config");
    let err = load(&config_path).expect_err("an equal pair must be refused");
    assert!(
        err.to_string().contains("commit_timeout_ms"),
        "unexpected: {err}"
    );

    // With headroom it loads.
    std::fs::write(&config_path, toml(45_000, 60_000)).expect("write config");
    load(&config_path).expect("a commit wait with headroom must load");
}

/// `wal_transport` reaches the connector, and its default is the streaming protocol.
///
/// The field is part of rustcdc's own `PostgresSourceConfig`, so it arrives here by
/// `#[serde(flatten)]` rather than through any code in this crate — which is exactly why
/// it is worth pinning: nothing in cdc-server would fail if a rename upstream made the
/// documented key silently inert, and the difference between the two transports is a
/// per-poll WAL re-read.
#[test]
fn postgres_wal_transport_round_trips_and_defaults_to_streaming_replication() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state_dir = dir.path().join("state");

    let default_cfg = write_postgres_config(dir.path(), "default.toml", &state_dir, "");
    let pg = expect_postgres(&default_cfg);
    assert_eq!(
        pg.wal_transport,
        rustcdc::WalTransport::StreamingReplication,
        "the default must be the protocol PostgreSQL's own subscribers use"
    );

    let peek_cfg = write_postgres_config(
        dir.path(),
        "peek.toml",
        &state_dir,
        "wal_transport = \"sql_peek\"\n",
    );
    let pg = expect_postgres(&peek_cfg);
    assert_eq!(
        pg.wal_transport,
        rustcdc::WalTransport::SqlPeek,
        "the documented fallback must actually select the fallback"
    );
}

fn write_postgres_config(
    dir: &Path,
    name: &str,
    state_dir: &Path,
    extra_source_lines: &str,
) -> AppConfig {
    let config_path = dir.join(name);
    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []
{extra_source_lines}
[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "{}"
"#,
            state_dir.display()
        ),
    )
    .expect("write config");

    // Scoped to the `load` call that needs it. The old code set this variable and never
    // removed it, so every later test in the binary ran with it present — which is how a
    // test that depends on an ambient variable passes locally and fails in isolation.
    let _env = crate::test_env::EnvGuard::set(&[("CDC_TEST_SOURCE_PASSWORD", "pg-secret")]);
    load(&config_path).expect("config should load")
}
