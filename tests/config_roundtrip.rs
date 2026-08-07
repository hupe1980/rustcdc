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
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
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
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
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
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
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
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
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
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
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
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
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
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
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
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
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
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
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
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
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
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
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
instance_pool_size = 2
max_memory_bytes = 1048576
max_event_bytes = 65536
timeout_ms = 75
fuel_yield_interval = 10000
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
    assert_eq!(tr.wasm.max_memory_bytes, 1_048_576);
    assert_eq!(tr.wasm.max_event_bytes, 65_536);
    assert_eq!(tr.wasm.timeout_ms, 75);
    assert_eq!(tr.wasm.fuel_yield_interval, Some(10_000));
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema registry pool + registry_ref resolution
// ─────────────────────────────────────────────────────────────────────────────

/// A shared `[registries.<name>]` entry must actually reach the codec. Before
/// the pool was wired up it parsed and was then ignored, so every sink had to
/// repeat the URL and credentials — and the copies are what drift apart.
#[test]
fn registry_ref_is_resolved_from_the_shared_pool() {
    let cfg = load_from_str(&format!(
        r#"{SOURCE_AND_STATE}

[registries.prod]
url = "https://registry.example.com"
subject_name_strategy = "topic_record_name"
auto_register = false

[sink]
type = "kafka"
brokers = "broker:9092"
topic = "cdc-events"

[sink.codec]
type = "avro_confluent"
registry_ref = "prod"
"#
    ))
    .expect("registry_ref config must load");

    let SinkConfig::Kafka(kafka) = &cfg.sink else {
        panic!("expected Kafka sink");
    };
    let binding = kafka
        .codec
        .as_ref()
        .expect("codec")
        .binding()
        .expect("registry binding");
    let registry = binding.resolved().expect("registry resolved by the loader");
    assert_eq!(registry.url, "https://registry.example.com");
    assert!(!registry.auto_register);
}

#[test]
fn unknown_registry_ref_is_rejected_with_the_known_names() {
    let err = load_from_str(&format!(
        r#"{SOURCE_AND_STATE}

[registries.prod]
url = "https://registry.example.com"

[sink]
type = "kafka"
brokers = "broker:9092"
topic = "cdc-events"

[sink.codec]
type = "avro_confluent"
registry_ref = "staging"
"#
    ))
    .expect_err("unknown registry_ref must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("staging"), "unexpected: {msg}");
    assert!(msg.contains("known: prod"), "unexpected: {msg}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Kafka SASL
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kafka_sasl_ssl_plain_config_loads() {
    let cfg = load_from_str(&format!(
        r#"{SOURCE_AND_STATE}

[sink]
type = "kafka"
brokers = "broker:9093"
topic = "cdc-events"
compression = "zstd"
compression_level = 6

[sink.security]
protocol = "sasl_ssl"

[sink.security.sasl]
mechanism = "plain"
username = "api-key"
password = "api-secret"

[sink.transport]
tcp_keepalive_ms = 30000
tls_reload_interval_ms = 300000
"#
    ))
    .expect("SASL_SSL config must load");

    let SinkConfig::Kafka(kafka) = &cfg.sink else {
        panic!("expected Kafka sink");
    };
    assert!(kafka.security.protocol.uses_sasl());
    assert!(kafka.security.protocol.uses_tls());
    assert_eq!(kafka.compression_level, Some(6));
    assert_eq!(kafka.transport.tls_reload_interval_ms, 300_000);
    kafka
        .security
        .to_auth_config()
        .expect("auth config must build");
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshots
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn incremental_snapshot_config_loads() {
    let cfg = load_from_str(&format!(
        r#"{SOURCE_AND_STATE}

[sink]
type = "stdout"

[incremental_snapshot]
tables = ["public.orders"]
chunk_size = 2500
"#
    ))
    .expect("incremental snapshot config must load");

    assert!(cfg.incremental_snapshot.is_enabled());
    assert_eq!(cfg.incremental_snapshot.chunk_size, 2500);
}

/// Both paths bootstrap the same tables; accepting both would read every table
/// twice and the duplicate would look like genuine change data downstream.
#[test]
fn blocking_and_incremental_snapshots_are_mutually_exclusive() {
    let err = load_from_str(&format!(
        r#"snapshot_tables = ["public.orders"]
{SOURCE_AND_STATE}

[sink]
type = "stdout"

[incremental_snapshot]
tables = ["public.orders"]
"#
    ))
    .expect_err("both snapshot paths must be rejected");
    assert!(err.to_string().contains("exactly one"), "unexpected: {err}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Masking secrets
// ─────────────────────────────────────────────────────────────────────────────

/// A key written into the config file makes every value it masked
/// re-identifiable for as long as that file exists.
#[test]
fn inline_mask_key_literal_is_rejected() {
    let err = load_from_str(&format!(
        r#"{SOURCE_AND_STATE}

[sink]
type = "stdout"

[[pipeline.transforms]]
name = "redact"

[[pipeline.transforms.actions]]
type = "mask"

[pipeline.transforms.actions.rules]
ssn = {{ type = "hmac_sha256", key = "hardcoded-key" }}
"#
    ))
    .expect_err("an inline masking key must be rejected");
    assert!(
        err.to_string().contains("deferred secret reference"),
        "unexpected: {err}"
    );
}

#[test]
fn mask_transform_with_env_key_loads() {
    // Safety: single-threaded test setup; no other thread reads this variable.
    unsafe { std::env::set_var("CDC_TEST_MASK_KEY", "s3cret") };
    let cfg = load_from_str(&format!(
        r#"{SOURCE_AND_STATE}

[sink]
type = "stdout"

[[pipeline.transforms]]
name = "redact"

[[pipeline.transforms.actions]]
type = "mask"

[pipeline.transforms.actions.rules]
email = {{ type = "redact", placeholder = "***" }}
ssn = {{ type = "hmac_sha256", key = {{ env = "CDC_TEST_MASK_KEY" }} }}
"#
    ));
    unsafe { std::env::remove_var("CDC_TEST_MASK_KEY") };

    let cfg = cfg.expect("mask config with an env key must load");
    assert_eq!(cfg.pipeline.transforms.len(), 1);
    // The pipeline must also *compile*: secrets resolve at startup, not on the
    // first event that happens to carry the field.
    rustcdc_server::pipeline::transform::TransformPipeline::from_config(
        cfg.pipeline.transform_runtime.clone(),
        cfg.pipeline.transforms.clone(),
    )
    .expect("mask pipeline must compile");
}

/// Shared source + state preamble for the configs above.
const SOURCE_AND_STATE: &str = r#"api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = { env = "CDC_TEST_SOURCE_PASSWORD" }
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

[state]
dir = "/tmp/cdc-test""#;

// ─────────────────────────────────────────────────────────────────────────────
// Removed / rejected configuration
// ─────────────────────────────────────────────────────────────────────────────

/// `at_most_once` was accepted, labelled and validated but never acted on — the code
/// that would have advanced the checkpoint before delivery was defined and never
/// called, so every deployment that selected it silently received at_least_once.
/// It must now fail loudly, naming the replacement.
#[test]
fn removed_at_most_once_contract_is_rejected_with_the_replacement() {
    let err = load_from_str(&format!(
        r#"delivery_contract = "at_most_once"
{SOURCE_AND_STATE}

[sink]
type = "stdout"
"#
    ))
    .expect_err("at_most_once must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("was removed"), "unexpected: {msg}");
    assert!(
        msg.contains("at_least_once"),
        "the error must name the replacement: {msg}"
    );
}

/// A misspelled key is not a typo-ergonomics problem: `table_include_lst` leaves the
/// include list empty, which captures **every table in the database**.
#[test]
fn unknown_config_keys_are_rejected_with_their_path() {
    let err = load_from_str(&format!(
        r#"{SOURCE_AND_STATE}

[sink]
type = "stdout"

[runtime]
max_buffer_sze = 500
"#
    ))
    .expect_err("an unknown key must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("runtime.max_buffer_sze"),
        "the error must name the full path: {msg}"
    );
}

/// Unknown-key detection must diff the config **file**, never the env-merged document.
///
/// `Env::prefixed("RUSTCDC_")` turns every variable in that namespace into a top-level
/// config key, but the namespace is shared: `RUSTCDC_ADMIN_READ_TOKEN` is read by name
/// to authorise the admin bind and `RUSTCDC_LOG_LEVEL` is applied before the config is
/// parsed. Neither is a field of `AppConfig`. Diffing the merged document rejected the
/// project's own documented environment variables — it broke the demo, which sets both.
///
/// `.cargo/config.toml` exports them for the whole test run, so this asserts the
/// condition directly rather than mutating the process environment mid-suite.
#[test]
fn env_vars_sharing_the_prefix_are_not_mistaken_for_unknown_keys() {
    assert!(
        std::env::var("RUSTCDC_ADMIN_READ_TOKEN").is_ok()
            && std::env::var("RUSTCDC_LOG_LEVEL").is_ok(),
        "this test is only meaningful with the prefixed non-config variables set; \
         see the [env] block in .cargo/config.toml"
    );

    load_from_str(&format!(
        r#"{SOURCE_AND_STATE}

[sink]
type = "stdout"
"#
    ))
    .expect("a prefixed variable that is not a config field must not be a typo");
}

/// The real defect this guard was built from: a key indented under the wrong table
/// parses cleanly and does nothing.
#[test]
fn a_key_under_the_wrong_table_is_rejected() {
    let err = load_from_str(&format!(
        r#"{SOURCE_AND_STATE}
snapshot_tables = ["public.orders"]

[sink]
type = "stdout"
"#
    ))
    .expect_err("snapshot_tables under [state] must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("[state]"), "unexpected: {msg}");
    assert!(msg.contains("snapshot_tables"), "unexpected: {msg}");
    assert!(
        msg.contains("above the [state] header"),
        "the error must say how to fix it: {msg}"
    );
}

/// A replication credential written as a literal is readable by anyone who can read
/// the config file, and it grants read access to every captured table. Sink tokens,
/// Iceberg credentials and masking keys were already required to be deferred; the
/// source password was the asymmetry.
#[test]
fn literal_source_password_is_rejected() {
    let err = load_from_str(
        r#"api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc"
password = "hardcoded-in-the-file"
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
    .expect_err("a literal source password must be rejected");
    assert!(
        err.to_string().contains("deferred secret reference"),
        "unexpected: {err}"
    );
}

/// A real `[dlq]` block must survive the unknown-key guard.
///
/// `DlqConfig` uses `#[serde(flatten)]` for the target, and flatten is exactly what
/// makes `deny_unknown_fields` unusable — which is why the guard is a post-parse
/// round-trip diff instead. A flattened, internally-tagged enum is the shape most
/// likely to round-trip differently from its input, so it needs its own test rather
/// than trust.
#[test]
fn a_file_dead_letter_queue_configuration_loads() {
    let config = load_from_str(&format!(
        r#"{SOURCE_AND_STATE}

[sink]
type = "stdout"

[dlq]
enabled   = true
type      = "file"
path      = "/var/lib/rustcdc/dlq.jsonl"
max_bytes = 134217728
"#
    ))
    .expect("a documented dlq block must load");

    assert!(config.dlq.enabled);
    match &config.dlq.target {
        rustcdc_server::config::dlq::DlqTarget::File(file) => {
            assert_eq!(file.path, "/var/lib/rustcdc/dlq.jsonl");
            assert_eq!(file.max_bytes, 134_217_728);
        }
        other => panic!("expected the file target, got {other:?}"),
    }
}

#[test]
fn a_kafka_dead_letter_queue_configuration_loads() {
    let config = load_from_str(&format!(
        r#"{SOURCE_AND_STATE}

[sink]
type = "stdout"

[dlq]
enabled = true
type    = "kafka"
brokers = "kafka:9092"
topic   = "cdc.dlq"
"#
    ))
    .expect("a documented kafka dlq block must load");

    match &config.dlq.target {
        rustcdc_server::config::dlq::DlqTarget::Kafka(kafka) => {
            assert_eq!(kafka.topic, "cdc.dlq");
        }
        other => panic!("expected the kafka target, got {other:?}"),
    }
}

/// The removed per-sink DLQ keys must be rejected, not ignored.
///
/// Silently dropping them would be the worst outcome: an operator who configured a
/// dead-letter queue would get none, and would only discover it when the first poison
/// event took the pipeline down.
#[test]
fn the_relocated_http_dlq_keys_are_rejected_with_the_replacement() {
    let err = load_from_str(&format!(
        r#"{SOURCE_AND_STATE}

[sink]
type = "http"
url  = "https://api.example.com/events"
dlq_path = "/var/log/cdc/dlq.jsonl"
"#
    ))
    .expect_err("the removed key must be rejected");

    let message = err.to_string();
    assert!(
        message.contains("[dlq]"),
        "must name the new section: {message}"
    );
    assert!(
        message.contains("opt-in") || message.contains("halts"),
        "must say the behaviour changed, not just the key name: {message}"
    );
}
