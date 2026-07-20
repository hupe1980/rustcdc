//! Config loading and round-trip integration tests.
//!
//! These tests exercise the layered configuration pipeline
//! (`src/config/loader.rs`): TOML parsing, schema migrations, field defaults,
//! environment-variable overrides, and validation.  They do not require any
//! external services.

use std::io::Write as _;

use rustcdc_server::config::schema::{KafkaDeliveryMode, KafkaSecurityProtocol, SinkConfig};
use rustcdc_server::config::AppConfig;
use tempfile::NamedTempFile;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn load_from_str(toml: &str) -> Result<AppConfig, Box<dyn std::error::Error>> {
    let mut file = NamedTempFile::new()?;
    file.write_all(toml.as_bytes())?;
    file.flush()?;
    let path = file.path().to_path_buf();
    // keep file alive until load returns
    let cfg = rustcdc_server::config::load(&path)?;
    drop(file);
    Ok(cfg)
}

// ─────────────────────────────────────────────────────────────────────────────
// Minimal valid config round-trip
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn minimal_postgres_stdout_config_loads() {
    let cfg = load_from_str(
        r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-test"
"#,
    )
    .expect("minimal config must load");

    assert!(matches!(cfg.sink, SinkConfig::Stdout(_)));
}

#[test]
fn mysql_source_config_loads() {
    let cfg = load_from_str(
        r#"
api_version = "v1"

[source.mysql]
host = "localhost"
port = 3306
user = "cdc"
password = "secret"
database = "mydb"
server_id = 42
gtid_mode_enabled = false
binlog_format_check = true
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.mysql.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-test"
"#,
    )
    .expect("mysql source config must load");

    // Source field is present (we just verify it loaded without panic).
    let _ = cfg;
}

// ─────────────────────────────────────────────────────────────────────────────
// Sink variant parsing
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn file_jsonl_sink_config_loads_with_defaults() {
    let cfg = load_from_str(
        r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "file_jsonl"
path = "/tmp/cdc-events.jsonl"

[state]
dir = "/tmp/cdc-test"
"#,
    )
    .expect("file_jsonl config must load");

    let SinkConfig::FileJsonl(file_cfg) = &cfg.sink else {
        panic!("expected FileJsonl sink, got {:?}", cfg.sink);
    };
    assert_eq!(file_cfg.path.to_string_lossy(), "/tmp/cdc-events.jsonl");
}

#[test]
fn http_sink_config_loads_with_retry_settings() {
    let cfg = load_from_str(
        r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "https://ingest.example.com/cdc"
timeout_ms = 5000
batch_max_events = 100
batch_max_delay_ms = 250
max_pending_bytes = 10485760
max_retries = 5
batch_retry_time_budget_ms = 60000
backoff_initial_ms = 100
backoff_max_ms = 5000
backoff_multiplier = 2.0

[state]
dir = "/tmp/cdc-test"
"#,
    )
    .expect("http sink config must load");

    let SinkConfig::Http(http_cfg) = &cfg.sink else {
        panic!("expected Http sink, got {:?}", cfg.sink);
    };
    assert_eq!(http_cfg.url, "https://ingest.example.com/cdc");
    assert_eq!(http_cfg.max_retries, 5);
    assert_eq!(http_cfg.backoff_multiplier, 2.0);
}

#[test]
fn kafka_sink_config_with_idempotent_delivery_loads() {
    let cfg = load_from_str(
        r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "broker1:9092,broker2:9092"
topic = "cdc-events"
client_id = "cdc-server"
ack_timeout_ms = 5000
delivery_mode = "at_least_once_idempotent"

[state]
dir = "/tmp/cdc-test"
"#,
    )
    .expect("kafka sink config must load");

    let SinkConfig::Kafka(kafka_cfg) = &cfg.sink else {
        panic!("expected Kafka sink, got {:?}", cfg.sink);
    };
    assert_eq!(kafka_cfg.topic, "cdc-events");
    assert!(matches!(
        kafka_cfg.delivery_mode,
        KafkaDeliveryMode::AtLeastOnceIdempotent
    ));
}

// ─────────────────────────────────────────────────────────────────────────────
// Config migration: v1alpha1 → v1
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn v1alpha1_config_is_migrated_to_v1() {
    let cfg = load_from_str(
        r#"
api_version = "v1alpha1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-test"
"#,
    )
    .expect("v1alpha1 config must be accepted via migration");

    // After migration, the loaded config should behave as v1.
    assert!(matches!(cfg.sink, SinkConfig::Stdout(_)));
}

// ─────────────────────────────────────────────────────────────────────────────
// Validation — unsupported API version is rejected
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn unknown_api_version_is_rejected() {
    let err = load_from_str(
        r#"
api_version = "v99"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-test"
"#,
    )
    .expect_err("unknown api_version must be rejected");

    assert!(
        err.to_string().contains("v99") || err.to_string().contains("api_version"),
        "error must mention the bad version: {err}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP sink URL policy
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn http_sink_rejects_plaintext_remote_url() {
    let err = load_from_str(
        r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://remote-host.example.com/ingest"
timeout_ms = 5000
batch_max_events = 100
batch_max_delay_ms = 250
max_pending_bytes = 10485760
max_retries = 3
batch_retry_time_budget_ms = 30000
backoff_initial_ms = 100
backoff_max_ms = 1000
backoff_multiplier = 2.0

[state]
dir = "/tmp/cdc-test"
"#,
    )
    .expect_err("plaintext remote URL must be rejected");

    assert!(
        err.to_string().contains("https"),
        "error must mention HTTPS requirement: {err}"
    );
}

#[test]
fn http_sink_accepts_loopback_plaintext_url() {
    load_from_str(
        r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "http"
url = "http://127.0.0.1:8080/ingest"
timeout_ms = 5000
batch_max_events = 100
batch_max_delay_ms = 250
max_pending_bytes = 10485760
max_retries = 3
batch_retry_time_budget_ms = 30000
backoff_initial_ms = 100
backoff_max_ms = 1000
backoff_multiplier = 2.0

[state]
dir = "/tmp/cdc-test"
"#,
    )
    .expect("loopback http URL must be allowed");
}

// ─────────────────────────────────────────────────────────────────────────────
// Kafka TLS security protocol
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kafka_sink_tls_security_protocol_loads() {
    let cfg = load_from_str(
        r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "kafka"
brokers = "broker1:9093"
topic = "cdc-events-tls"
client_id = "cdc-server"
ack_timeout_ms = 5000
delivery_mode = "at_least_once_idempotent"

[sink.security]
protocol = "tls"

[state]
dir = "/tmp/cdc-test"
"#,
    )
    .expect("kafka TLS config must load");

    let SinkConfig::Kafka(kafka_cfg) = &cfg.sink else {
        panic!("expected Kafka sink");
    };
    assert!(matches!(
        kafka_cfg.security.protocol,
        KafkaSecurityProtocol::Tls
    ));
}

// ─────────────────────────────────────────────────────────────────────────────
// Transform config — WASM runtime
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn wasm_transform_config_loads() {
    // Write a minimal valid ABI-v2 WASM module so the path-validation passes.
    let dir = tempfile::tempdir().expect("tempdir");
    let wasm_path = dir.path().join("transform.wasm");
    let wasm_bytes = wat::parse_str(
        r#"(module
          (memory (export "memory") 1 1)
          (global $heap (mut i32) (i32.const 8))
          (func (export "alloc") (param i32) (result i32)
            global.get $heap)
          (func (export "dealloc") (param i32) (param i32))
          (func (export "cdc_abi_version") (result i32) i32.const 2)
          (func (export "transform") (param i32) (param i32) (result i64) i64.const 0))"#,
    )
    .expect("valid WAT");
    std::fs::write(&wasm_path, wasm_bytes).expect("write wasm");

    let toml = format!(
        r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = "secret"
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
conn_timeout_secs = 10
stream_poll_interval_ms = 200
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "/tmp/cdc-test"

[pipeline.transform_runtime]
mode = "wasm"

[pipeline.transform_runtime.wasm]
module_path = "{}"
entrypoint = "transform"
instance_pool_size = 2
max_memory_bytes = 1048576
max_event_bytes = 65536
max_fuel = 10000000
"#,
        wasm_path.display()
    );

    let cfg = load_from_str(&toml).expect("wasm transform config must load");

    let tr = &cfg.pipeline.transform_runtime;

    assert!(matches!(
        tr.mode,
        rustcdc_server::config::schema::TransformRuntimeMode::Wasm
    ));
    assert_eq!(tr.wasm.instance_pool_size, 2);
}
