//! Loader tests for the **admin** surface: listener TLS, tokens and their manifest, audit,
//! notification and signal-ingress channels, rate limits and trusted proxies.
//!
//! Split from `loader_tests.rs` by concern, at the per-file line budget
//! `tests/architecture.rs` enforces. Like its siblings, each test runs the real `load()`.

use std::path::{Path, PathBuf};

use super::load;
use crate::token_manifest_policy::{
    TokenManifestFile, TokenManifestSignature, TokenManifestToken, TokenManifestUnsigned,
    canonical_signing_payload,
};
use ed25519_dalek::{Signer, SigningKey};

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
