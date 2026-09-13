//! Loader tests for **sink** configuration.
//!
//! Split from `loader_tests.rs`, which passed the per-file line budget
//! `tests/architecture.rs` enforces. The seam is by concern rather than by size: these
//! exercise how a *sink* is configured — Kafka topic templates, delete tombstones, record
//! headers, Standard Webhooks signing — while `loader_tests.rs` keeps the document-level
//! rules: api_version, migrations, secrets, typo detection, source drivers.
//!
//! Every test here writes a real TOML document and runs the real `load()`, for the same
//! reason as its sibling: a rule that only the struct-level `validate()` knows about, and
//! that `load()` never reaches, is a rule no operator is protected by.

use std::path::{Path, PathBuf};

use super::load;

// ─────────────────────────────────────────────────────────────────────────────
// Kafka topic templates
// ─────────────────────────────────────────────────────────────────────────────

/// One PostgreSQL source plus whatever sink section the test needs.
///
/// The topic-template rules are enforced by the real `load()`, so these write real
/// documents rather than constructing an `AppConfig` by hand — a rule that only the
/// struct-level `validate()` knows about, and that `load()` never reaches, is a rule no
/// operator is protected by.
fn topic_fixture(dir: &Path, sink: &str) -> PathBuf {
    topic_fixture_with(dir, sink, "")
}

fn topic_fixture_with(dir: &Path, sink: &str, extra: &str) -> PathBuf {
    let state_dir = dir.join("state");
    let config_path = dir.join("cdc.toml");
    std::fs::write(
        &config_path,
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

{sink}

[state]
dir = "{}"
{extra}
"#,
            state_dir.display()
        ),
    )
    .expect("write config");
    config_path
}

#[test]
fn a_topic_template_loads_and_survives_the_round_trip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.${schema}.${table}"
"#,
    );

    let cfg = load(&path).expect("a templated topic is valid configuration");
    let crate::config::schema::SinkConfig::Kafka(kafka) = &cfg.sink else {
        panic!("expected a kafka sink");
    };
    assert_eq!(kafka.topic, "cdc.${schema}.${table}");
    assert!(kafka.topic_template().expect("parses").is_templated());
    // The default policy is the conservative one.
    assert_eq!(
        kafka.topic_naming.invalid_characters,
        crate::topic::InvalidCharacterPolicy::Reject
    );
}

/// The proposal's own test plan: an unknown placeholder must fail at config load, not at
/// send time. `${db}` is the near-miss an operator arriving from Debezium writes.
#[test]
fn an_unknown_placeholder_is_rejected_at_config_load() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.${db}.${table}"
"#,
    );

    let error = load(&path).expect_err("unknown placeholder").to_string();
    assert!(error.contains("${db}"), "{error}");
    assert!(error.contains("${schema}"), "{error}");
}

#[test]
fn a_topic_with_characters_kafka_forbids_is_rejected_at_config_load() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc events"
"#,
    );

    let error = load(&path).expect_err("space in a topic name").to_string();
    assert!(error.contains("not legal"), "{error}");
}

#[test]
fn the_replacement_character_is_validated_at_config_load() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.${schema}.${table}"

[sink.topic_naming]
invalid_characters = "replace"
replacement = "."
"#,
    );

    let error = load(&path).expect_err("dot replacement").to_string();
    assert!(error.contains("replacement"), "{error}");
}

/// The gap the draft proposal missed entirely: a registry-backed codec derives its
/// subject from the topic *string*, and the codec is built once. A template plus the
/// default `topic_name` strategy would register every schema under the literal
/// `cdc.${schema}.${table}-value`.
#[test]
fn a_templated_topic_with_a_topic_derived_subject_strategy_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.${schema}.${table}"

[sink.codec]
type = "avro_confluent"

[sink.codec.registry]
url = "http://localhost:8081"
"#,
    );

    let error = load(&path).expect_err("topic_name strategy").to_string();
    assert!(error.contains("subject_name_strategy"), "{error}");
    assert!(error.contains("record_name"), "{error}");
}

#[test]
fn a_templated_topic_is_accepted_with_the_record_name_subject_strategy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.${schema}.${table}"

[sink.codec]
type = "avro_confluent"

[sink.codec.registry]
url = "http://localhost:8081"
allow_insecure = true
subject_name_strategy = "record_name"
"#,
    );

    load(&path).expect("record_name names no subject from the topic");
}

/// A **literal** topic keeps the Confluent default, because the subject it derives is a
/// real one. The check must not over-reach.
#[test]
fn a_literal_topic_still_accepts_the_default_subject_strategy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.events"

[sink.codec]
type = "avro_confluent"

[sink.codec.registry]
url = "http://localhost:8081"
allow_insecure = true
"#,
    );

    load(&path).expect("a literal topic is a subject");
}

/// `[[sinks]]` entries reached the codec check but never `KafkaSinkConfig::validate`, so
/// every Kafka rule — timeouts, compression levels, and now topic templates — was
/// enforced for the default sink only.
#[test]
fn a_named_kafka_sink_is_validated_like_the_default_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture_with(
        dir.path(),
        r#"
[sink]
type = "stdout"
"#,
        r#"
[[sinks]]
name = "warehouse"
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.${db}.${table}"

[[pipeline.routes]]
table_pattern = "public.*"
sink = "warehouse"
"#,
    );

    let error = load(&path)
        .expect_err("named sink must be validated")
        .to_string();
    assert!(error.contains("${db}"), "{error}");
    assert!(error.contains("sinks.warehouse"), "{error}");
}

/// Fan-out children were in the same blind spot.
#[test]
fn a_fan_out_kafka_child_is_validated_like_the_default_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "fan"

[[sink.sinks]]
type = "stdout"

[[sink.sinks]]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.${db}.${table}"
"#,
    );

    let error = load(&path)
        .expect_err("fan-out child must be validated")
        .to_string();
    assert!(error.contains("${db}"), "{error}");
    assert!(error.contains("sink.sinks[1]"), "{error}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Delete tombstones
// ─────────────────────────────────────────────────────────────────────────────

/// The default is what most deployments get, so it is asserted through the real loader
/// rather than read off the struct: a `#[serde(default)]` pointing at the wrong function
/// would still compile.
#[test]
fn tombstones_on_delete_defaults_to_on() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.events"
"#,
    );

    let cfg = load(&path).expect("config should load");
    let crate::config::schema::SinkConfig::Kafka(kafka) = &cfg.sink else {
        panic!("expected a kafka sink");
    };
    assert!(
        kafka.tombstones_on_delete,
        "the default must match Debezium's tombstones.on.delete"
    );
}

#[test]
fn tombstones_on_delete_can_be_turned_off_in_config() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.events"
tombstones_on_delete = false
"#,
    );

    let cfg = load(&path).expect("config should load");
    let crate::config::schema::SinkConfig::Kafka(kafka) = &cfg.sink else {
        panic!("expected a kafka sink");
    };
    assert!(!kafka.tombstones_on_delete);
}

/// `reject_unknown_config_keys` diffs the parsed config against the raw document, so a
/// field that fails to round-trip turns every config that sets it into a load error.
#[test]
fn tombstones_on_delete_survives_the_unknown_key_round_trip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture_with(
        dir.path(),
        r#"
[sink]
type = "stdout"
"#,
        r#"
[[sinks]]
name = "warehouse"
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.events"
tombstones_on_delete = false

[[pipeline.routes]]
table_pattern = "public.*"
sink = "warehouse"
"#,
    );

    let cfg = load(&path).expect("a named sink may set it too");
    let crate::config::schema::SinkConfig::Kafka(kafka) = &cfg.sinks[0].sink else {
        panic!("expected a kafka sink");
    };
    assert!(!kafka.tombstones_on_delete);
}

// ─────────────────────────────────────────────────────────────────────────────
// Record headers
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn record_headers_defaults_to_cdc() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.events"
"#,
    );

    let cfg = load(&path).expect("config should load");
    let crate::config::schema::SinkConfig::Kafka(kafka) = &cfg.sink else {
        panic!("expected a kafka sink");
    };
    assert_eq!(
        kafka.record_headers,
        crate::config::sink::KafkaRecordHeaders::Cdc
    );
}

#[test]
fn record_headers_can_be_turned_off_in_config() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "kafka"
brokers = "localhost:9092"
topic = "cdc.events"
record_headers = "none"
"#,
    );

    let cfg = load(&path).expect("config should load");
    let crate::config::schema::SinkConfig::Kafka(kafka) = &cfg.sink else {
        panic!("expected a kafka sink");
    };
    assert_eq!(
        kafka.record_headers,
        crate::config::sink::KafkaRecordHeaders::None
    );
}

/// The reason for the name. `sink.http.headers` is a map of user-supplied request headers
/// whose credential-bearing entries `redaction.rs` redacts by key, and both sink configs
/// flatten into `sink.*`. Sharing the name would put two unrelated meanings at one config
/// path — and the redaction rule is keyed on exactly that path.
#[test]
fn the_kafka_record_header_setting_does_not_collide_with_http_request_headers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "http"
url = "https://example.com/cdc"

[sink.headers]
authorization = "Bearer not-a-real-token"
x-tenant = "acme"
"#,
    );

    let cfg = load(&path).expect("config should load");
    let json = serde_json::to_string(&cfg).expect("serialize");
    let rendered = crate::redaction::redact_secrets(&json);
    assert!(
        !rendered.contains("not-a-real-token"),
        "an http `authorization` header must still be redacted: {rendered}"
    );
    assert!(
        rendered.contains("acme"),
        "a non-credential http header is topology, not a secret: {rendered}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Standard Webhooks signing
// ─────────────────────────────────────────────────────────────────────────────

/// A signing key written as a literal is a signing key in the config file, in the
/// `/status` snapshot, and in whatever backs them up — and a leaked signing key lets
/// anyone forge events *as this pipeline*.
#[test]
fn a_literal_signing_key_is_rejected_at_load() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "http"
url = "https://example.com/cdc"

[sink.signing]
scheme = "hmac_sha256"
key = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
"#,
    );

    let error = load(&path).expect_err("a literal signing key").to_string();
    assert!(error.contains("signing.key"), "{error}");
    assert!(error.contains("deferred"), "{error}");
}

#[test]
fn a_literal_rotating_signing_key_is_rejected_too() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "http"
url = "https://example.com/cdc"

[sink.signing]
scheme = "hmac_sha256"
key = { env = "CDC_TEST_SOURCE_PASSWORD" }
previous_keys = ["whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"]
"#,
    );

    let error = load(&path).expect_err("a literal rotating key").to_string();
    assert!(error.contains("previous_keys"), "{error}");
}

/// A key that cannot be parsed must fail at startup, not become a request every receiver
/// rejects while this side reports success.
#[test]
fn an_unparseable_signing_key_fails_at_load() {
    let dir = tempfile::tempdir().expect("tempdir");
    // `CDC_TEST_SOURCE_PASSWORD` is "test-only-not-a-real-credential" — not base64, and
    // therefore not a usable HMAC secret.
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "http"
url = "https://example.com/cdc"

[sink.signing]
scheme = "ed25519"
key = { env = "CDC_TEST_SOURCE_PASSWORD" }
"#,
    );

    let error = load(&path)
        .expect_err("not a valid ed25519 seed")
        .to_string();
    assert!(error.contains("sink.http.signing.key"), "{error}");
}

/// The defect found while adding signing: HTTP validation ran only for the default
/// `[sink]`, so a named entry or a fan-out child skipped the URL policy entirely —
/// `verify_tls = false` was rejected in one position and accepted in another.
#[test]
fn a_named_http_sink_is_validated_like_the_default_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture_with(
        dir.path(),
        r#"
[sink]
type = "stdout"
"#,
        r#"
[[sinks]]
name = "webhook"
type = "http"
url = "https://example.com/cdc"
verify_tls = false

[[pipeline.routes]]
table_pattern = "public.*"
sink = "webhook"
"#,
    );

    let error = load(&path)
        .expect_err("named http sink must be validated")
        .to_string();
    assert!(error.contains("verify_tls"), "{error}");
    assert!(error.contains("sinks.webhook"), "{error}");
}

#[test]
fn a_fan_out_http_child_is_validated_like_the_default_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "fan"

[[sink.sinks]]
type = "stdout"

[[sink.sinks]]
type = "http"
url = "http://not-localhost.example.com/cdc"
"#,
    );

    let error = load(&path)
        .expect_err("fan-out http child must be validated")
        .to_string();
    assert!(error.contains("https"), "{error}");
    assert!(error.contains("sink.sinks[1]"), "{error}");
}

#[test]
fn a_deferred_signing_key_loads() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "http"
url = "https://example.com/cdc"

[sink.signing]
scheme = "hmac_sha256"
key = { env = "CDC_TEST_WEBHOOK_SIGNING_KEY" }
"#,
    );

    let _env = crate::test_env::EnvGuard::set(&[(
        "CDC_TEST_WEBHOOK_SIGNING_KEY",
        "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw",
    )]);
    let cfg = load(&path).expect("a deferred signing key is the supported form");

    let crate::config::schema::SinkConfig::Http(http) = &cfg.sink else {
        panic!("expected an http sink");
    };
    let signing = http.signing.as_ref().expect("signing configured");
    assert_eq!(
        signing.scheme,
        crate::webhook::WebhookSignatureScheme::HmacSha256
    );
    assert!(signing.previous_keys.is_empty());
}

/// `/status` serves a config snapshot to any holder of a *read*-scoped admin token, so a
/// signing key reaching it would be readable by someone who should not be able to forge
/// events. `SecretString` redacts on `Serialize`, but that is a property of a type three
/// crates away — asserted here against the real snapshot rather than reasoned about.
#[test]
fn signing_keys_never_reach_the_config_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = topic_fixture(
        dir.path(),
        r#"
[sink]
type = "http"
url = "https://example.com/cdc"

[sink.signing]
scheme = "ed25519"
key = { env = "CDC_TEST_WEBHOOK_ACTIVE" }
previous_keys = [{ env = "CDC_TEST_WEBHOOK_PREVIOUS" }]
"#,
    );

    let active = "whsk_YWN0aXZlLWtleS1leGFjdGx5LTMyLWJ5dGVzLWxvbmc=";
    let previous = "whsk_cHJldmlvdXMta2V5LWV4YWN0bHktMzItYnl0ZXNsbmc=";
    let _env = crate::test_env::EnvGuard::set(&[
        ("CDC_TEST_WEBHOOK_ACTIVE", active),
        ("CDC_TEST_WEBHOOK_PREVIOUS", previous),
    ]);

    let cfg = load(&path).expect("config should load");
    let json = serde_json::to_string(&cfg).expect("serialize");
    let rendered = crate::redaction::redact_secrets(&json);

    for key in [active, previous] {
        assert!(
            !rendered.contains(key),
            "a signing key must never appear in the config snapshot: {rendered}"
        );
    }
    // The rotating key is inside a `Vec`, which is the case a per-field redaction rule
    // would miss — it is redacted because `SecretString` itself refuses to serialise, not
    // because anything enumerated the path.
    assert!(
        !rendered.contains("cHJldmlvdXMta2V5"),
        "the rotating key must be redacted inside the array too: {rendered}"
    );
    // The scheme is topology, not a secret, and an operator reading `/status` needs it.
    assert!(rendered.contains("ed25519"), "{rendered}");
}
