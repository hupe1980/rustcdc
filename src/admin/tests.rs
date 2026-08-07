//! Tests for the admin API.
//!
//! In their own file because they were the larger half of `admin/mod.rs` — roughly 3 700
//! lines against 3 800 of implementation — and the mixture made the module hard to
//! navigate for either purpose. `super::` still resolves to `admin`, so nothing about how
//! they reach the code under test changed.

use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::{
    body::to_bytes,
    extract::{ConnectInfo, State},
    http::{header::AUTHORIZATION, header::RETRY_AFTER, HeaderMap, HeaderValue, StatusCode},
    Json,
};
use chrono::Duration as ChronoDuration;
use chrono::Utc;
use ed25519_dalek::{Signer, SigningKey};
use krafka::consumer::{AutoOffsetReset, Consumer};
use tempfile::tempdir;
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;

use super::QueuedSignalAction;

use super::auth::{file_modified_time, load_auth_tokens_from_manifest, AuthToken};
use super::{
    collect_control_notifications, healthz, notifications_authed, notifications_cloudevents_authed,
    notifications_stream_authed, readyz, runtime_metrics_prometheus, signal_action, slo_json,
    slo_prometheus, token_sha256_hex, AbuseLimitScope, AdminAbuseGuard, AdminScope, AdminState,
    AdminStateData, AuditTrailEntry, AuthSource, AuthState, InstanceState, SignalActionRequest,
    SignalActionType, SignalIngressRecord, SignalIngressSource,
};
use crate::admin::rate_limit::EndpointRateLimiter;
use crate::config;
use crate::config::schema::{
    AdminNotificationKafkaConfig, AdminProbeAuthMode, AdminSignalIngressKafkaConfig,
    KafkaSecurityConfig, KafkaSecurityProtocol,
};
use crate::token_manifest_policy::{
    canonical_signing_payload, TokenManifestFile, TokenManifestSignature, TokenManifestToken,
    TokenManifestUnsigned,
};

fn write_signed_manifest(path: &Path, signing_key: &SigningKey, tokens: Vec<TokenManifestToken>) {
    let unsigned = TokenManifestUnsigned {
        tokens: tokens.clone(),
    };
    let unsigned_bytes =
        canonical_signing_payload(&unsigned).expect("serialize canonical manifest payload");
    let signature = signing_key.sign(&unsigned_bytes);

    let manifest = TokenManifestFile {
        tokens,
        signature: TokenManifestSignature {
            algorithm: "ed25519".to_string(),
            public_key_hex: hex::encode(signing_key.verifying_key().to_bytes()),
            signature_hex: hex::encode(signature.to_bytes()),
        },
    };

    std::fs::write(
        path,
        serde_json::to_vec_pretty(&manifest).expect("serialize signed manifest"),
    )
    .expect("write signed manifest");
}

fn auth_token_manifest(id: &str, token: &str, scopes: &[&str]) -> TokenManifestToken {
    TokenManifestToken {
        id: id.to_string(),
        token_sha256_hex: token_sha256_hex(token),
        scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
        not_before: None,
        expires_at: None,
        revoked: false,
    }
}

/// `admin.audit_signing_key_env` must actually be read.
///
/// It was accepted, echoed in `/status` and ignored: the loader read a hardcoded
/// `CDC_AUDIT_SIGNING_KEY_HEX` instead, so a correctly-configured deployment
/// produced unsigned audit records with no error anywhere. Nothing failed, because
/// nothing asserted the key was resolved from the configured name.
#[test]
fn the_configured_audit_signing_key_variable_is_the_one_that_is_read() {
    let mut admin = crate::config::schema::AdminConfig::default();
    assert!(
        super::load_audit_signing_key(&admin).is_none(),
        "no configured variable means no signing key"
    );

    // `.cargo/config.toml` exports this for the whole test run, so the assertion
    // does not race other tests through a process-global `set_var`.
    admin.audit_signing_key_env = Some("CDC_TEST_AUDIT_SIGNING_KEY_HEX".to_string());
    assert!(
        super::load_audit_signing_key(&admin).is_some(),
        "the key named by admin.audit_signing_key_env must be resolved"
    );

    admin.audit_signing_key_env = Some("CDC_TEST_SOURCE_PASSWORD".to_string());
    assert!(
        super::load_audit_signing_key(&admin).is_none(),
        "a variable that is set but is not 32 hex-encoded bytes must not yield a key"
    );
}

fn sample_config() -> crate::config::AppConfig {
    let dir = tempdir().expect("tempdir");
    let state_dir = dir.path().join("state");
    let notification_log =
        std::env::temp_dir().join(format!("cdc-admin-notifications-{}.jsonl", test_suffix()));
    let config_path = dir.path().join("cdc.toml");

    std::fs::write(
        &config_path,
        format!(
            r#"
api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "rustcdc_user"
password = {{ env = "CDC_TEST_SOURCE_PASSWORD" }}
database = "mydb"
replication_slot_name = "rustcdc_slot"
publication_name = "rustcdc_pub"
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
dir = "{}"

[admin]
notification_log_file = "{}"
"#,
            state_dir.display(),
            notification_log.display()
        ),
    )
    .expect("write config");

    config::load(&config_path).expect("config should load")
}

async fn wait_for_signal_state(
    admin: &AdminState,
    signal_id: &str,
    action_type: &str,
    expected_state: &str,
) {
    for _ in 0..100 {
        let data = admin.data.read().await;
        let notifications = collect_control_notifications(&data);
        let observed = notifications
            .iter()
            .filter(|n| n.signal_id == signal_id && n.action_type == action_type)
            .any(|n| n.state == expected_state);
        if observed {
            return;
        }
        drop(data);
        sleep(Duration::from_millis(20)).await;
    }

    panic!("timed out waiting for {signal_id}/{action_type} state {expected_state}");
}

/// Poll until the async signal-action queue has fully drained.
///
/// The worker decrements `signal_action_queue_depth` *after* emitting the
/// terminal lifecycle notification, so observing COMPLETED does not imply
/// the depth has reached zero yet — a fixed sleep here is a CI flake.
async fn wait_for_signal_queue_drained(admin: &AdminState) {
    for _ in 0..250 {
        if admin.data.read().await.signal_action_queue_depth == 0 {
            return;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for signal action queue to drain");
}

/// Poll a file until it contains at least `expected_lines` non-empty lines,
/// returning them.  Needed because the AuditLogWriter is async (mpsc channel
/// to a tokio task), so writes may lag a few milliseconds behind the API call.
async fn wait_for_file_lines(path: &std::path::Path, expected_lines: usize) -> Vec<String> {
    for _ in 0..100 {
        if let Ok(text) = std::fs::read_to_string(path) {
            let lines: Vec<String> = text
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(String::from)
                .collect();
            if lines.len() >= expected_lines {
                return lines;
            }
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for {expected_lines} lines in {}",
        path.display()
    );
}

fn test_suffix() -> String {
    format!(
        "{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    )
}

fn append_signal_ingress_line(path: &Path, payload: &str) {
    use std::io::Write as _;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open signal ingress file");
    writeln!(file, "{payload}").expect("append signal ingress line");
}

async fn wait_for_signal_ingress_rejection_counters(
    admin: &AdminState,
    parse_rejections: u64,
    line_too_large_rejections: u64,
    validation_rejections: u64,
) {
    for _ in 0..200 {
        let data = admin.data.read().await;
        let observed = data.signal_ingress_parse_rejections_total == parse_rejections
            && data.signal_ingress_line_too_large_rejections_total == line_too_large_rejections
            && data.signal_ingress_validation_rejections_total == validation_rejections;
        if observed {
            return;
        }
        drop(data);
        sleep(Duration::from_millis(20)).await;
    }

    panic!(
        "timed out waiting for ingress counters parse={parse_rejections} too_large={line_too_large_rejections} validation={validation_rejections}"
    );
}

fn kafka_auth_from_env() -> krafka::auth::AuthConfig {
    let security = KafkaSecurityConfig {
        protocol: match std::env::var("CDC_TEST_KAFKA_PROTOCOL") {
            Ok(protocol) if protocol.eq_ignore_ascii_case("tls") => KafkaSecurityProtocol::Tls,
            _ => KafkaSecurityProtocol::Plaintext,
        },
        ssl_ca_location: std::env::var("CDC_TEST_KAFKA_CA").ok().map(PathBuf::from),
        ..KafkaSecurityConfig::default()
    };

    security
        .to_auth_config()
        .expect("kafka auth config should be valid")
}

async fn consume_notification_states_until_seen(
    consumer: &Consumer,
    signal_id: &str,
    expected_states: &[&str],
    attempts: usize,
) -> HashSet<String> {
    let expected = expected_states
        .iter()
        .map(|state| state.to_string())
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();

    for _ in 0..attempts {
        let records = consumer
            .poll(Duration::from_millis(250))
            .await
            .expect("consumer poll should succeed");

        for record in records {
            let Some(value) = &record.value else {
                continue;
            };
            let Ok(event) = serde_json::from_slice::<serde_json::Value>(value.as_ref()) else {
                continue;
            };
            if event["signalid"].as_str() != Some(signal_id) {
                continue;
            }
            if event["source"] != "urn:cdc-server:kafka-notifications" {
                continue;
            }
            if let Some(state) = event["data"]["state"].as_str() {
                seen.insert(state.to_string());
            }
        }

        if expected.is_subset(&seen) {
            break;
        }

        sleep(Duration::from_millis(100)).await;
    }

    seen
}

fn configure_test_read_write_tokens(admin: &AdminState) {
    let mut auth = admin.auth_state.write().expect("auth lock");
    auth.read_tokens = vec![AuthToken {
        id: "read-test".to_string(),
        token_sha256_hex: token_sha256_hex("read-secret"),
        not_before: None,
        expires_at: None,
        revoked: false,
    }];
    auth.write_tokens = vec![AuthToken {
        id: "write-test".to_string(),
        token_sha256_hex: token_sha256_hex("write-secret"),
        not_before: None,
        expires_at: None,
        revoked: false,
    }];
}

/// The admin workers used to be detached OS threads running private tokio
/// runtimes in unconditional loops, with no handle kept and no exit condition — they
/// could not be stopped by any means short of process exit.
///
/// This asserts the property that was missing rather than the mechanism: after
/// `shutdown_workers` returns, every worker task has finished. `JoinHandle::is_finished`
/// is the direct observation; a test that merely called the method and asserted it
/// returned would have passed against the old code too, since the old code had nothing
/// to call.
#[tokio::test]
async fn admin_workers_stop_when_asked() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    let tracked = {
        let handles = admin.workers.handles.lock().expect("worker handles");
        assert!(
            !handles.is_empty(),
            "the admin state must have spawned supervised workers to shut down"
        );
        handles.len()
    };

    // The whole call must complete well inside the per-worker budget; a worker that
    // ignored the watch would sit here for `ADMIN_WORKER_SHUTDOWN_TIMEOUT` each.
    tokio::time::timeout(Duration::from_secs(5), admin.shutdown_workers())
        .await
        .expect("shutdown_workers must not hang");

    assert!(
        admin
            .workers
            .handles
            .lock()
            .expect("worker handles")
            .is_empty(),
        "shutdown must consume the handles it joined ({tracked} were tracked)"
    );

    // Idempotent: shutdown runs on the pipeline's exit path, which is also reached
    // from error paths that may already have run it.
    tokio::time::timeout(Duration::from_secs(1), admin.shutdown_workers())
        .await
        .expect("a second shutdown must be a no-op, not a hang");
}

#[tokio::test]
async fn status_surface_exposes_slo_snapshot() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    admin
        .record_readiness_probe(true, Duration::from_millis(12))
        .await;
    admin.set_state(InstanceState::Running).await;

    {
        let mut data = admin.data.write().await;
        data.runtime_metrics = "runtime_metric 1\n".to_string();
        data.checkpoint_age_seconds = Some(15.0);
        data.last_admin_api_latency_us = Some(12);
        data.restart_recovery_seconds = Some(2.5);
        let started_at = data.started_at;
        data.first_ready_at = Some(started_at + ChronoDuration::milliseconds(900));
        data.first_checkpoint_advanced_at = Some(started_at + ChronoDuration::milliseconds(2_100));
    }

    let data = admin.data.read().await;
    let slo = slo_json(&data);

    assert_eq!(slo["readiness_checks_total"].as_u64(), Some(1));
    assert_eq!(slo["readiness_ready_total"].as_u64(), Some(1));
    assert_eq!(slo["source_lag_seconds"].as_f64(), Some(15.0));
    assert_eq!(slo["admin_api_latency_us"].as_u64(), Some(12));
    assert_eq!(slo["restart_recovery_seconds"].as_f64(), Some(2.5));
    assert_eq!(slo["cold_start_to_ready_ms"].as_u64(), Some(900));
    assert_eq!(
        slo["cold_start_to_checkpoint_advance_ms"].as_u64(),
        Some(2100)
    );
    assert!(slo["reasons"].as_array().expect("array").is_empty());

    let metrics = slo_prometheus(&data);
    assert!(metrics.contains("rustcdc_slo_readiness_checks_total 1"));
    assert!(metrics.contains("rustcdc_source_lag_seconds 15"));
    assert!(metrics.contains("rustcdc_slo_restart_recovery_seconds 2.5"));
    assert!(metrics.contains("rustcdc_slo_cold_start_to_ready_ms 900"));
    assert!(metrics.contains("rustcdc_slo_cold_start_to_checkpoint_advance_ms 2100"));
    assert!(metrics.contains("rustcdc_admin_control_plane_up 1"));
    assert!(metrics.contains("rustcdc_admin_control_plane_heartbeat_unix_seconds"));
    assert!(metrics.contains("rustcdc_admin_control_plane_state_code 1"));
    assert!(metrics.contains("rustcdc_admin_shutdown_requests_os_signal_total 0"));
    assert!(metrics.contains("rustcdc_admin_shutdown_completions_stopped_total 0"));
    assert!(metrics.contains("rustcdc_admin_shutdown_completions_error_total 0"));
    assert!(metrics.contains("rustcdc_admin_rate_limited_metrics_total 0"));
    assert!(metrics.contains("rustcdc_admin_rate_limiter_metrics_decisions_total 0"));
    assert!(metrics.contains("rustcdc_admin_last_terminal_reason_code{reason=\"none\"} 1"));

    let runtime_metrics = runtime_metrics_prometheus(&data);
    assert!(runtime_metrics.contains("runtime_metric 1"));
}

#[tokio::test]
async fn shutdown_lifecycle_counters_surface_in_slo_snapshot_and_metrics() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    admin.record_shutdown_request_os_signal().await;
    admin
        .record_rate_limited_request(AbuseLimitScope::Metrics)
        .await;
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Metrics, Duration::from_millis(2))
        .await;
    admin.set_state(InstanceState::Stopping).await;
    admin.set_state(InstanceState::Stopped).await;
    admin.set_state(InstanceState::Stopping).await;
    admin.set_state(InstanceState::Error).await;
    admin
        .record_reconciliation_recovery_proof(true, "checkpoint age observed")
        .await;

    let data = admin.data.read().await;
    let slo = slo_json(&data);
    let shutdown = &slo["shutdown"];
    assert_eq!(shutdown["requests_os_signal_total"].as_u64(), Some(1));
    assert_eq!(shutdown["completions_stopped_total"].as_u64(), Some(1));
    assert_eq!(shutdown["completions_error_total"].as_u64(), Some(1));
    assert_eq!(slo["abuse"]["rate_limited_metrics_total"].as_u64(), Some(1));
    assert_eq!(
        slo["abuse"]["rate_limiter_metrics_decisions_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        slo["abuse"]["rate_limiter_metrics_decision_latency_seconds_avg"].as_f64(),
        Some(0.002),
        "the rate-limiter decision latency is denominated in seconds like every \
         other duration the server exports"
    );

    let metrics = slo_prometheus(&data);
    assert!(metrics.contains("rustcdc_admin_shutdown_requests_os_signal_total 1"));
    assert!(metrics.contains("rustcdc_admin_shutdown_completions_stopped_total 1"));
    assert!(metrics.contains("rustcdc_admin_shutdown_completions_error_total 1"));
    assert!(metrics.contains("rustcdc_admin_rate_limited_metrics_total 1"));
    assert!(metrics.contains("rustcdc_admin_rate_limiter_metrics_decisions_total 1"));
    assert!(
        metrics.contains("rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_avg 0.002")
    );
    assert!(metrics.contains("rustcdc_admin_reconciliation_recoveries_total 0"));
    assert!(metrics.contains("rustcdc_admin_reconciliation_recovery_proof_last_ok 1"));
}

#[tokio::test]
async fn audit_entries_are_persisted_when_admin_audit_log_file_is_configured() {
    let mut cfg = sample_config();
    let dir = tempdir().expect("tempdir");
    let audit_log_path = dir.path().join("admin-audit.jsonl");
    cfg.admin.audit_log_file = Some(audit_log_path.clone());

    let admin = AdminState::new(&cfg).await.expect("admin state");

    admin.set_state(InstanceState::Running).await;
    admin.record_shutdown_request_os_signal().await;

    // AuditLogWriter is async (mpsc → tokio task); poll until both writes land.
    let lines = wait_for_file_lines(&audit_log_path, 2).await;
    assert_eq!(lines.len(), 2, "expected two persisted audit entries");

    let persisted_entries: Vec<AuditTrailEntry> = lines
        .iter()
        .map(|line| serde_json::from_str::<AuditTrailEntry>(line).expect("valid audit entry"))
        .collect();

    assert_eq!(persisted_entries[0].sequence, 1);
    assert_eq!(persisted_entries[1].sequence, 2);
    assert_eq!(persisted_entries[0].action, "admin_state_transition");
    assert_eq!(persisted_entries[1].action, "shutdown_requested");
    assert_eq!(persisted_entries[0].actor_source_ip.as_deref(), None);
    assert_eq!(persisted_entries[0].actor_token_id.as_deref(), None);
    assert_eq!(persisted_entries[1].actor_source_ip.as_deref(), None);
    assert_eq!(persisted_entries[1].actor_token_id.as_deref(), None);
    assert_eq!(
        persisted_entries[1].prev_hash_hex,
        persisted_entries[0].entry_hash_hex
    );
}

#[tokio::test]
async fn shutdown_audit_entry_persists_actor_metadata() {
    let mut cfg = sample_config();
    let dir = tempdir().expect("tempdir");
    let audit_log_path = dir.path().join("admin-audit-actor.jsonl");
    cfg.admin.audit_log_file = Some(audit_log_path.clone());

    let admin = AdminState::new(&cfg).await.expect("admin state");
    admin.record_shutdown_request_os_signal().await;

    // AuditLogWriter is async; poll until the write lands.
    let lines = wait_for_file_lines(&audit_log_path, 1).await;
    let entry = serde_json::from_str::<AuditTrailEntry>(&lines[0]).expect("valid audit entry");

    assert_eq!(entry.action, "shutdown_requested");
    assert_eq!(entry.actor_source_ip.as_deref(), None);
    assert_eq!(entry.actor_token_id.as_deref(), None);
}

#[tokio::test]
async fn signal_action_persists_non_admin_notification_log_events_when_configured() {
    let mut cfg = sample_config();
    let dir = tempdir().expect("tempdir");
    let notification_log_path = dir.path().join("admin-notifications.jsonl");
    cfg.admin.notification_log_file = Some(notification_log_path.clone());

    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9011))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-log-1".to_string()),
            correlation_id: Some("corr-log-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            message: Some("run snapshot".to_string()),
            additional_data: Some(serde_json::json!({"operator": "unit-test"})),
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    // AuditLogWriter is async; poll until all 3 notification writes land.
    let lines = wait_for_file_lines(&notification_log_path, 3).await;
    assert_eq!(
        lines.len(),
        3,
        "expected started/in-progress/terminal events"
    );

    let events: Vec<serde_json::Value> = lines
        .iter()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("valid event"))
        .collect();
    assert!(events.iter().all(|event| event["specversion"] == "1.0"));
    assert!(events
        .iter()
        .all(|event| event["source"] == "urn:cdc-server:notification-log"));

    let data = admin.data.read().await;
    assert_eq!(data.notification_log_emitted_total, 3);
    assert_eq!(data.notification_log_emit_failures_total, 0);
    assert_eq!(
        data.notification_channel_emitted_total.get("file").copied(),
        Some(3)
    );
    assert_eq!(
        data.notification_channel_emit_failures_total
            .get("file")
            .copied(),
        None
    );

    let slo = slo_json(&data);
    assert_eq!(slo["notification_log"]["emitted_total"].as_u64(), Some(3));
    assert_eq!(
        slo["notification_log"]["emit_failures_total"].as_u64(),
        Some(0)
    );
    assert_eq!(
        slo["notification_log"]["channels"]["emitted_total"]["file"].as_u64(),
        Some(3)
    );

    let metrics = slo_prometheus(&data);
    assert!(metrics.contains("rustcdc_signal_notification_log_emitted_total 3"));
    assert!(metrics.contains("rustcdc_signal_notification_log_emit_failures_total 0"));
    assert!(
        metrics.contains("rustcdc_signal_notification_channel_emitted_total{channel=\"file\"} 3")
    );
}

#[tokio::test]
async fn signal_action_emits_non_admin_notification_kafka_events_when_configured() {
    let Ok(brokers) = std::env::var("CDC_TEST_KAFKA_BROKERS") else {
        eprintln!("skipping admin notification kafka test (CDC_TEST_KAFKA_BROKERS is not set)");
        return;
    };
    let Ok(topic) = std::env::var("CDC_TEST_KAFKA_TOPIC") else {
        eprintln!("skipping admin notification kafka test (CDC_TEST_KAFKA_TOPIC is not set)");
        return;
    };

    let suffix = test_suffix();
    let mut cfg = sample_config();
    let security = KafkaSecurityConfig {
        protocol: match std::env::var("CDC_TEST_KAFKA_PROTOCOL") {
            Ok(protocol) if protocol.eq_ignore_ascii_case("tls") => KafkaSecurityProtocol::Tls,
            _ => KafkaSecurityProtocol::Plaintext,
        },
        ssl_ca_location: std::env::var("CDC_TEST_KAFKA_CA").ok().map(PathBuf::from),
        ..KafkaSecurityConfig::default()
    };
    cfg.admin.notification_kafka = Some(AdminNotificationKafkaConfig {
        brokers: brokers.clone(),
        topic: topic.clone(),
        client_id: format!("cdc-admin-notifications-{suffix}"),
        ack_timeout_ms: 1_000,
        retry_backoff_ms: 100,
        retry_max_attempts: 3,
        compression: Default::default(),
        security,
    });

    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let signal_id = format!("sig-kafka-{suffix}");
    let correlation_id = format!("corr-kafka-{suffix}");
    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9012))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some(signal_id.clone()),
            correlation_id: Some(correlation_id),
            action_type: SignalActionType::ExecuteSnapshot,
            message: Some("run snapshot".to_string()),
            additional_data: Some(serde_json::json!({"operator": "kafka-test"})),
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    let consumer = Consumer::builder()
        .bootstrap_servers(brokers)
        .group_id(format!("cdc-admin-notifications-{suffix}"))
        .client_id(format!("cdc-admin-notifications-consumer-{suffix}"))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .request_timeout(Duration::from_millis(1_000))
        .connect_timeout(crate::sink::kafka_connect_timeout(Duration::from_millis(
            1_000,
        )))
        .auth(kafka_auth_from_env())
        .build()
        .await
        .expect("consumer should build");
    consumer
        .subscribe(&[topic.as_str()])
        .await
        .expect("consumer should subscribe");

    let seen_states = consume_notification_states_until_seen(
        &consumer,
        &signal_id,
        &["STARTED", "IN_PROGRESS", "COMPLETED"],
        60,
    )
    .await;

    assert!(
        ["STARTED", "IN_PROGRESS", "COMPLETED"]
            .iter()
            .all(|state| seen_states.contains(*state)),
        "consumer did not observe expected kafka notification lifecycle states: {seen_states:?}"
    );

    let data = admin.data.read().await;
    assert_eq!(
        data.notification_channel_emitted_total
            .get("kafka")
            .copied(),
        Some(3)
    );
    assert_eq!(data.notification_log_emit_failures_total, 0);
}

#[tokio::test]
async fn signal_ingress_kafka_processes_execute_snapshot_actions() {
    let Ok(brokers) = std::env::var("CDC_TEST_KAFKA_BROKERS") else {
        eprintln!("skipping admin signal ingress kafka test (CDC_TEST_KAFKA_BROKERS is not set)");
        return;
    };
    let Ok(topic) = std::env::var("CDC_TEST_KAFKA_SIGNAL_INGRESS_TOPIC") else {
        eprintln!(
            "skipping admin signal ingress kafka test (CDC_TEST_KAFKA_SIGNAL_INGRESS_TOPIC is not set)"
        );
        return;
    };

    let suffix = test_suffix();
    let mut cfg = sample_config();
    let security = KafkaSecurityConfig {
        protocol: match std::env::var("CDC_TEST_KAFKA_PROTOCOL") {
            Ok(protocol) if protocol.eq_ignore_ascii_case("tls") => KafkaSecurityProtocol::Tls,
            _ => KafkaSecurityProtocol::Plaintext,
        },
        ssl_ca_location: std::env::var("CDC_TEST_KAFKA_CA").ok().map(PathBuf::from),
        ..KafkaSecurityConfig::default()
    };

    cfg.admin.signal_ingress_kafka = Some(AdminSignalIngressKafkaConfig {
        brokers: brokers.clone(),
        topic: topic.clone(),
        group_id: format!("cdc-admin-signal-ingress-{suffix}"),
        client_id: format!("cdc-admin-signal-ingress-{suffix}"),
        poll_timeout_ms: 250,
        security: security.clone(),
    });

    let admin = AdminState::new(&cfg).await.expect("admin state");

    let producer = krafka::producer::Producer::builder()
        .bootstrap_servers(brokers)
        .client_id(format!("cdc-admin-signal-ingress-producer-{suffix}"))
        .acks(krafka::producer::Acks::All)
        .request_timeout(Duration::from_millis(1_000))
        .connect_timeout(crate::sink::kafka_connect_timeout(Duration::from_millis(
            1_000,
        )))
        .auth(kafka_auth_from_env())
        .build()
        .await
        .expect("signal ingress producer should build");

    let signal_id = format!("sig-ingress-kafka-{suffix}");
    let correlation_id = format!("corr-ingress-kafka-{suffix}");
    let payload = serde_json::json!({
        "signal_id": signal_id,
        "correlation_id": correlation_id,
        "action_type": "execute_snapshot",
        "message": "kafka ingress execute",
        "additional_data": {"ingress": "kafka"},
    });

    let record = krafka::producer::ProducerRecord::new(
        topic,
        serde_json::to_vec(&payload).expect("serialize kafka ingress payload"),
    );
    let _metadata = producer
        .send_record(record)
        .await
        .expect("signal ingress payload send should succeed");
    producer
        .flush()
        .await
        .expect("producer flush should succeed");

    wait_for_signal_state(&admin, &signal_id, "execute_snapshot", "COMPLETED").await;

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    assert!(notifications.iter().any(|notification| {
        notification.signal_id == signal_id
            && notification.action_type == "execute_snapshot"
            && notification.state == "COMPLETED"
    }));
}

#[tokio::test]
async fn admin_state_new_rejects_audit_log_file_that_points_to_directory() {
    let mut cfg = sample_config();
    let dir = tempdir().expect("tempdir");
    cfg.admin.audit_log_file = Some(dir.path().to_path_buf());

    let err = match AdminState::new(&cfg).await {
        Ok(_) => panic!("directory path must fail as audit log file"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("failed to open admin.audit_log_file"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn healthz_is_unauthenticated_liveness() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    let response = healthz(State(admin), HeaderMap::new()).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn readyz_returns_ok_while_stopping_when_probe_auth_allows_loopback() {
    let mut cfg = sample_config();
    cfg.admin.probe_auth_mode = AdminProbeAuthMode::AllowUnauthenticatedLoopback;
    let admin = AdminState::new(&cfg).await.expect("admin state");

    admin.set_state(InstanceState::Stopping).await;

    let response = readyz(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 8080))),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let data = admin.data.read().await;
    assert_eq!(data.readiness_checks_total, 1);
    assert_eq!(data.readiness_ready_total, 1);
}

#[tokio::test]
async fn signal_action_requires_write_token() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer read-secret"),
    );

    let response = signal_action(
        State(admin),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 8080))),
        headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-read-only".to_string()),
            correlation_id: Some("corr-read-only".to_string()),
            action_type: SignalActionType::LogMarker,
            message: Some("hello".to_string()),
            additional_data: None,
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn signal_action_emits_started_and_completed_notifications() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );
    write_headers.insert(
        "traceparent",
        HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
    );

    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9000))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-1".to_string()),
            correlation_id: Some("corr-1".to_string()),
            action_type: SignalActionType::LogMarker,
            message: Some("checkpoint marker".to_string()),
            additional_data: Some(serde_json::json!({"operator": "unit-test"})),
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    assert_eq!(notifications.len(), 2);
    assert!(notifications.iter().any(|n| n.state == "STARTED"));
    assert!(notifications.iter().any(|n| n.state == "COMPLETED"));
    assert!(notifications.iter().all(|n| n.signal_id == "sig-1"));
    assert!(notifications.iter().all(|n| n.correlation_id == "corr-1"));

    let slo = slo_json(&data);
    assert_eq!(
        slo["signal_notifications"]["without_terminal_total"].as_u64(),
        Some(0)
    );
    assert!(
        slo["signal_notifications"]["lag_seconds"]
            .as_f64()
            .unwrap_or(-1.0)
            >= 0.0
    );

    drop(data);

    let mut read_headers = HeaderMap::new();
    read_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer read-secret"),
    );
    let notifications_response = notifications_authed(
        State(admin),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 8081))),
        read_headers,
    )
    .await;
    assert_eq!(notifications_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn collect_control_notifications_ignores_missing_or_unknown_action_type() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    {
        let mut data = admin.data.write().await;
        data.audit_entries_total = 3;
        data.audit_recent_entries = vec![
            AuditTrailEntry {
                sequence: 1,
                at: Utc::now() - ChronoDuration::seconds(3),
                action: "signal_log_marker_started".to_string(),
                result: "accepted".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-malformed-1",
                    "correlation_id": "corr-malformed-1",
                    "state": "STARTED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "genesis".to_string(),
                entry_hash_hex: "hash-1".to_string(),
                ed25519_signature_hex: None,
            },
            AuditTrailEntry {
                sequence: 2,
                at: Utc::now() - ChronoDuration::seconds(2),
                action: "signal_log_marker_started".to_string(),
                result: "accepted".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-malformed-2",
                    "correlation_id": "corr-malformed-2",
                    "action_type": "unknown_action",
                    "state": "STARTED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "hash-1".to_string(),
                entry_hash_hex: "hash-2".to_string(),
                ed25519_signature_hex: None,
            },
            AuditTrailEntry {
                sequence: 3,
                at: Utc::now() - ChronoDuration::seconds(1),
                action: "signal_log_marker_started".to_string(),
                result: "accepted".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-valid-1",
                    "correlation_id": "corr-valid-1",
                    "action_type": "log_marker",
                    "state": "STARTED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "hash-2".to_string(),
                entry_hash_hex: "hash-3".to_string(),
                ed25519_signature_hex: None,
            },
        ];
    }

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    assert_eq!(notifications.len(), 1);
    assert_eq!(notifications[0].signal_id, "sig-valid-1");
    assert_eq!(notifications[0].action_type, "log_marker");
}

#[tokio::test]
async fn notifications_stream_requires_read_token() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let response = notifications_stream_authed(
        State(admin),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 8082))),
        HeaderMap::new(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Drive a signal action through the write API so a real notification is produced by
/// the same path production uses, rather than hand-inserting an audit entry.
async fn raise_test_signal(admin: &AdminState, signal_id: &str, correlation_id: &str) {
    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9000))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some(signal_id.to_string()),
            correlation_id: Some(correlation_id.to_string()),
            action_type: SignalActionType::LogMarker,
            message: Some("checkpoint marker".to_string()),
            additional_data: Some(serde_json::json!({"operator": "unit-test"})),
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

async fn open_notification_stream(
    admin: &AdminState,
    last_event_id: Option<u64>,
) -> axum::response::Response {
    let mut read_headers = HeaderMap::new();
    read_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer read-secret"),
    );
    if let Some(id) = last_event_id {
        read_headers.insert(
            "last-event-id",
            HeaderValue::from_str(&id.to_string()).expect("valid header"),
        );
    }
    notifications_stream_authed(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 8083))),
        read_headers,
    )
    .await
}

/// Read from an open SSE response until `min_events` `event:` lines have arrived.
///
/// Necessarily bounded: the endpoint no longer terminates, so `to_bytes` would block
/// forever. Keep-alive comments are counted as data but not as events, so a quiet
/// stream still times out rather than passing on heartbeats alone.
async fn read_sse_frames(response: axum::response::Response, min_events: usize) -> String {
    use futures::StreamExt as _;

    let mut stream = response.into_body().into_data_stream();
    let mut body = String::new();

    let collected = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(chunk)) = stream.next().await {
            body.push_str(&String::from_utf8_lossy(chunk.as_ref()));
            if body.matches("event: ").count() >= min_events {
                break;
            }
        }
        body
    })
    .await;

    collected.unwrap_or_else(|_| {
        panic!("timed out waiting for {min_events} SSE event(s) on an open stream")
    })
}

#[tokio::test]
async fn notifications_stream_replays_the_backlog_and_stays_open() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    raise_test_signal(&admin, "sig-sse-1", "corr-sse-1").await;

    let stream_response = open_notification_stream(&admin, None).await;
    assert_eq!(stream_response.status(), StatusCode::OK);
    let content_type = stream_response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    assert!(content_type.contains("text/event-stream"));

    // Bounded read: the stream is now genuinely open-ended, so reading it to
    // completion — as this test used to — would never return.
    let body = read_sse_frames(stream_response, 2).await;
    assert!(
        body.contains("event: notification"),
        "backlog must be replayed on connect: {body}"
    );
    assert!(
        body.contains("sig-sse-1"),
        "the raised signal must appear in the backlog: {body}"
    );
    assert!(
        body.contains("id: "),
        "each event needs an id for Last-Event-ID resumption: {body}"
    );
    assert!(
        !body.contains("event: stream_end"),
        "the stream must stay open; `stream_end` was the marker of the snapshot \
         implementation that closed after one burst: {body}"
    );
}

/// **The defect this closes.** The endpoint advertised `text/event-stream` but wrote
/// a snapshot of the ring buffer and closed. A client that connected and waited never
/// saw anything else; `EventSource` turned it into a reconnect-per-notification poll
/// with full re-delivery each time.
///
/// This asserts the property that was missing: an event raised *after* the client
/// connected arrives on the open connection.
#[tokio::test]
async fn notifications_stream_delivers_events_raised_after_connecting() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let stream_response = open_notification_stream(&admin, None).await;
    assert_eq!(stream_response.status(), StatusCode::OK);

    let reader = tokio::spawn(async move { read_sse_frames(stream_response, 1).await });

    // Raised only now — nothing was in the backlog when the client connected.
    raise_test_signal(&admin, "sig-live-1", "corr-live-1").await;

    let body = tokio::time::timeout(Duration::from_secs(5), reader)
        .await
        .expect("a live notification must reach an already-open stream")
        .expect("stream reader task");

    assert!(
        body.contains("sig-live-1"),
        "an event raised after the client connected must be pushed to it: {body}"
    );
}

/// `Last-Event-ID` is what `EventSource` replays on reconnect. Ignoring it — as the
/// snapshot implementation did — re-delivered the whole ring buffer every time a
/// browser reconnected.
#[tokio::test]
async fn notifications_stream_resumes_after_the_last_event_id() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    raise_test_signal(&admin, "sig-old-1", "corr-old-1").await;

    // Everything already recorded is at or below this sequence.
    let high_water = admin.data.read().await.audit_entries_total;

    let stream_response = open_notification_stream(&admin, Some(high_water)).await;
    let reader = tokio::spawn(async move { read_sse_frames(stream_response, 1).await });

    raise_test_signal(&admin, "sig-new-1", "corr-new-1").await;

    let body = tokio::time::timeout(Duration::from_secs(5), reader)
        .await
        .expect("resumed stream must deliver the newer notification")
        .expect("stream reader task");

    assert!(
        body.contains("sig-new-1"),
        "a notification newer than Last-Event-ID must be delivered: {body}"
    );
    assert!(
        !body.contains("sig-old-1"),
        "a notification the client already acknowledged must not be replayed: {body}"
    );
}

#[tokio::test]
async fn notifications_cloudevents_requires_read_token() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let response = notifications_cloudevents_authed(
        State(admin),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 8084))),
        HeaderMap::new(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn notifications_cloudevents_renders_required_fields() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );
    write_headers.insert(
        "traceparent",
        HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
    );

    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9007))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-ce-1".to_string()),
            correlation_id: Some("corr-ce-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            message: None,
            additional_data: Some(serde_json::json!({"operator": "unit-test"})),
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let mut read_headers = HeaderMap::new();
    read_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer read-secret"),
    );
    let response = notifications_cloudevents_authed(
        State(admin),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 8085))),
        read_headers,
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read cloudevents body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("valid cloudevents json");
    assert_eq!(payload["format"], "cloudevents");

    let events = payload["events"].as_array().expect("events array");
    assert!(!events.is_empty());
    let first = &events[0];
    assert_eq!(first["specversion"], "1.0");
    assert_eq!(first["source"], "urn:cdc-server:admin:notifications");
    assert!(first["type"]
        .as_str()
        .is_some_and(|v| v.starts_with("cdc.signal.")));
    assert!(first["id"].as_str().is_some());
    assert!(first["time"].as_str().is_some());
    assert_eq!(first["datacontenttype"], "application/json");
}

#[tokio::test]
async fn signal_notification_timeout_and_lag_metrics_surface_in_slo() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    {
        let mut data = admin.data.write().await;
        let stale_started_at = Utc::now() - ChronoDuration::seconds(90);
        data.notification_log_enabled = false;
        data.notification_kafka_enabled = false;
        data.audit_entries_total = 5;
        data.audit_recent_entries = vec![
            AuditTrailEntry {
                sequence: 1,
                at: stale_started_at,
                action: "signal_execute_snapshot_started".to_string(),
                result: "accepted".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-stale-1",
                    "correlation_id": "corr-stale-1",
                    "action_type": "execute_snapshot",
                    "state": "STARTED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "genesis".to_string(),
                entry_hash_hex: "hash-1".to_string(),
                ed25519_signature_hex: None,
            },
            AuditTrailEntry {
                sequence: 2,
                at: Utc::now() - ChronoDuration::seconds(40),
                action: "signal_pause_snapshot_started".to_string(),
                result: "accepted".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-lag-1",
                    "correlation_id": "corr-lag-1",
                    "action_type": "pause_snapshot",
                    "state": "STARTED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "hash-1".to_string(),
                entry_hash_hex: "hash-2".to_string(),
                ed25519_signature_hex: None,
            },
            AuditTrailEntry {
                sequence: 3,
                at: Utc::now() - ChronoDuration::seconds(37),
                action: "signal_pause_snapshot_completed".to_string(),
                result: "completed".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-lag-1",
                    "correlation_id": "corr-lag-1",
                    "action_type": "pause_snapshot",
                    "state": "COMPLETED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "hash-2".to_string(),
                entry_hash_hex: "hash-3".to_string(),
                ed25519_signature_hex: None,
            },
            AuditTrailEntry {
                sequence: 4,
                at: Utc::now() - ChronoDuration::seconds(34),
                action: "signal_pause_snapshot_completed".to_string(),
                result: "completed".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-lag-1",
                    "correlation_id": "corr-lag-1",
                    "action_type": "pause_snapshot",
                    "state": "COMPLETED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "hash-3".to_string(),
                entry_hash_hex: "hash-4".to_string(),
                ed25519_signature_hex: None,
            },
        ];
    }

    let data = admin.data.read().await;
    let slo = slo_json(&data);
    assert_eq!(
        slo["signal_notifications"]["without_terminal_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        slo["signal_notifications"]["duplicate_terminal_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        slo["signal_notifications"]["terminal_timeout_budget_seconds"].as_i64(),
        Some(30)
    );
    assert!(
        slo["signal_notifications"]["lag_seconds"]
            .as_f64()
            .expect("lag metric")
            >= 3.0
    );

    let metrics = slo_prometheus(&data);
    assert!(metrics.contains("rustcdc_signal_without_terminal_notification_total 1"));
    assert!(metrics.contains("rustcdc_signal_duplicate_terminal_notification_total 1"));
    assert!(metrics.contains("rustcdc_signal_notification_lag_seconds"));
    assert!(metrics.contains("rustcdc_signal_terminal_timeout_budget_seconds 30"));
    assert!(metrics.contains("rustcdc_signal_notification_log_emitted_total"));
    assert!(metrics.contains("rustcdc_signal_notification_log_emit_failures_total"));
    assert!(metrics.contains("rustcdc_signal_notification_channel_enabled{channel=\"file\"} 0"));
    assert!(metrics.contains("rustcdc_signal_notification_channel_enabled{channel=\"kafka\"} 0"));
    let reasons = slo["reasons"]
        .as_array()
        .expect("reasons array")
        .iter()
        .filter_map(|entry| entry.as_str())
        .collect::<Vec<_>>();
    assert!(reasons
        .iter()
        .any(|reason| reason.contains("duplicate terminal lifecycle notifications")));
    assert!(reasons
        .iter()
        .any(|reason| reason.contains("no non-admin notification channels are enabled")));
    assert_eq!(
        slo["notification_channels"]["enabled_total"].as_u64(),
        Some(0)
    );
}

#[tokio::test]
async fn signal_notification_emit_failures_surface_in_slo_reasons() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    {
        let mut data = admin.data.write().await;
        data.state = InstanceState::Running;
        data.notification_log_enabled = true;
        data.notification_kafka_enabled = true;
        data.notification_log_emit_failures_total = 2;
        data.notification_channel_emit_failures_total
            .insert("file".to_string(), 2);
    }

    let data = admin.data.read().await;
    let slo = slo_json(&data);
    let reasons = slo["reasons"]
        .as_array()
        .expect("reasons array")
        .iter()
        .filter_map(|entry| entry.as_str())
        .collect::<Vec<_>>();
    assert!(reasons.iter().any(|reason| {
        reason.contains("2 non-admin notification lifecycle events failed to emit")
    }));

    let metrics = slo_prometheus(&data);
    assert!(metrics.contains("rustcdc_signal_notification_log_emit_failures_total 2"));
    assert!(metrics
        .contains("rustcdc_signal_notification_channel_emit_failures_total{channel=\"file\"} 2"));
}

#[tokio::test]
async fn signal_notification_health_keeps_earliest_started_timestamp() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    {
        let mut data = admin.data.write().await;
        let early_started_at = Utc::now() - ChronoDuration::seconds(90);
        let late_started_at = Utc::now() - ChronoDuration::seconds(10);
        data.notification_log_enabled = true;
        data.notification_kafka_enabled = true;
        data.audit_entries_total = 2;
        data.audit_recent_entries = vec![
            AuditTrailEntry {
                sequence: 1,
                at: early_started_at,
                action: "signal_execute_snapshot_started".to_string(),
                result: "accepted".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-stale-dup-1",
                    "correlation_id": "corr-stale-dup-1",
                    "action_type": "execute_snapshot",
                    "state": "STARTED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "genesis".to_string(),
                entry_hash_hex: "hash-1".to_string(),
                ed25519_signature_hex: None,
            },
            AuditTrailEntry {
                sequence: 2,
                at: late_started_at,
                action: "signal_execute_snapshot_started".to_string(),
                result: "accepted".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-stale-dup-1",
                    "correlation_id": "corr-stale-dup-1",
                    "action_type": "execute_snapshot",
                    "state": "STARTED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "hash-1".to_string(),
                entry_hash_hex: "hash-2".to_string(),
                ed25519_signature_hex: None,
            },
        ];
    }

    let data = admin.data.read().await;
    let slo = slo_json(&data);
    assert_eq!(
        slo["signal_notifications"]["without_terminal_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        slo["signal_notifications"]["duplicate_terminal_total"].as_u64(),
        Some(0)
    );
    assert!(
        slo["signal_notifications"]["lag_seconds"]
            .as_f64()
            .expect("lag metric")
            >= 0.0
    );
    assert_eq!(
        slo["notification_channels"]["enabled"]["file"].as_bool(),
        Some(true)
    );
    assert_eq!(
        slo["notification_channels"]["enabled"]["kafka"].as_bool(),
        Some(true)
    );
    assert_eq!(
        slo["notification_channels"]["enabled_total"].as_u64(),
        Some(2)
    );

    let metrics = slo_prometheus(&data);
    assert!(metrics.contains("rustcdc_signal_notification_channel_enabled{channel=\"file\"} 1"));
    assert!(metrics.contains("rustcdc_signal_notification_channel_enabled{channel=\"kafka\"} 1"));
}

#[tokio::test]
async fn signal_notification_health_ignores_unknown_action_types() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    {
        let mut data = admin.data.write().await;
        data.audit_entries_total = 3;
        data.audit_recent_entries = vec![
            AuditTrailEntry {
                sequence: 1,
                at: Utc::now() - ChronoDuration::seconds(40),
                action: "signal_execute_snapshot_started".to_string(),
                result: "accepted".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-unknown-action-1",
                    "correlation_id": "corr-unknown-action-1",
                    "action_type": "unknown_action",
                    "state": "STARTED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "genesis".to_string(),
                entry_hash_hex: "hash-1".to_string(),
                ed25519_signature_hex: None,
            },
            AuditTrailEntry {
                sequence: 2,
                at: Utc::now() - ChronoDuration::seconds(30),
                action: "signal_execute_snapshot_completed".to_string(),
                result: "completed".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-unknown-action-1",
                    "correlation_id": "corr-unknown-action-1",
                    "action_type": "unknown_action",
                    "state": "COMPLETED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "hash-1".to_string(),
                entry_hash_hex: "hash-2".to_string(),
                ed25519_signature_hex: None,
            },
            AuditTrailEntry {
                sequence: 3,
                at: Utc::now() - ChronoDuration::seconds(50),
                action: "signal_log_marker_started".to_string(),
                result: "accepted".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-valid-action-1",
                    "correlation_id": "corr-valid-action-1",
                    "action_type": "log_marker",
                    "state": "STARTED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "hash-2".to_string(),
                entry_hash_hex: "hash-3".to_string(),
                ed25519_signature_hex: None,
            },
        ];
    }

    let data = admin.data.read().await;
    let slo = slo_json(&data);
    assert_eq!(
        slo["signal_notifications"]["without_terminal_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        slo["signal_notifications"]["duplicate_terminal_total"].as_u64(),
        Some(0)
    );
}

#[tokio::test]
async fn signal_action_accepts_execute_snapshot_and_emits_lifecycle_notifications() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9002))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-exec-1".to_string()),
            correlation_id: Some("corr-exec-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            message: None,
            additional_data: Some(serde_json::json!({
                "data_collections": ["public.accounts", "public.orders"],
                "snapshot_mode": "incremental"
            })),
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    wait_for_signal_state(&admin, "sig-exec-1", "execute_snapshot", "COMPLETED").await;
    wait_for_signal_queue_drained(&admin).await;

    let data = admin.data.read().await;
    assert_eq!(data.signal_action_queue_depth, 0);
    let notifications = collect_control_notifications(&data);
    let execute_notifications: Vec<_> = notifications
        .iter()
        .filter(|n| n.action_type == "execute_snapshot")
        .collect();

    assert_eq!(execute_notifications.len(), 3);
    assert!(execute_notifications.iter().any(|n| n.state == "STARTED"));
    assert!(execute_notifications
        .iter()
        .any(|n| n.state == "IN_PROGRESS"));
    assert!(execute_notifications.iter().any(|n| n.state == "COMPLETED"));
    assert!(execute_notifications
        .iter()
        .all(|n| n.signal_id == "sig-exec-1" && n.correlation_id == "corr-exec-1"));
}

#[tokio::test]
async fn signal_action_emits_action_specific_terminal_states() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    for (signal_id, action_type, expected_terminal) in [
        ("sig-pause-1", SignalActionType::PauseSnapshot, "PAUSED"),
        ("sig-resume-1", SignalActionType::ResumeSnapshot, "RESUMED"),
        ("sig-stop-1", SignalActionType::StopSnapshot, "ABORTED"),
    ] {
        let response = signal_action(
            State(admin.clone()),
            ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9010))),
            write_headers.clone(),
            Json(SignalActionRequest {
                signal_id: Some(signal_id.to_string()),
                correlation_id: Some(format!("corr-{signal_id}")),
                action_type,
                message: None,
                additional_data: Some(serde_json::json!({"operator": "test"})),
            }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);

        wait_for_signal_state(&admin, signal_id, action_type.as_str(), expected_terminal).await;
        sleep(Duration::from_millis(500)).await;

        let data = admin.data.read().await;
        let notifications = collect_control_notifications(&data);
        let scoped: Vec<_> = notifications
            .iter()
            .filter(|n| n.signal_id == signal_id)
            .collect();
        assert!(scoped.iter().any(|n| n.state == "STARTED"));
        assert!(scoped.iter().any(|n| n.state == expected_terminal));
        drop(data);
    }
}

#[tokio::test]
async fn signal_lifecycle_conformance_matrix_for_http_file_kafka_and_source_ingress() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9020))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-matrix-http-1".to_string()),
            correlation_id: Some("corr-matrix-http-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            message: Some("matrix http execute".to_string()),
            additional_data: Some(serde_json::json!({"ingress": "http"})),
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    admin
        .process_file_signal_ingress_record(SignalIngressRecord {
            signal_id: Some("sig-matrix-file-exec-1".to_string()),
            correlation_id: Some("corr-matrix-file-exec-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            message: Some("matrix file execute".to_string()),
            additional_data: Some(serde_json::json!({"ingress": "file"})),
            traceparent: None,
        })
        .await;

    admin
        .process_file_signal_ingress_record(SignalIngressRecord {
            signal_id: Some("sig-matrix-file-log-1".to_string()),
            correlation_id: Some("corr-matrix-file-log-1".to_string()),
            action_type: SignalActionType::LogMarker,
            message: Some("matrix file log".to_string()),
            additional_data: Some(serde_json::json!({"ingress": "file"})),
            traceparent: None,
        })
        .await;

    let kafka_exec_payload = serde_json::to_vec(&serde_json::json!({
        "signal_id": "sig-matrix-kafka-exec-1",
        "correlation_id": "corr-matrix-kafka-exec-1",
        "action_type": "execute_snapshot",
        "message": "matrix kafka execute",
        "additional_data": {"ingress": "kafka"},
    }))
    .expect("serialize kafka matrix execute payload");
    assert!(
        admin
            .process_signal_ingress_payload(
                kafka_exec_payload.as_slice(),
                SignalIngressSource::Kafka,
            )
            .await
    );

    let kafka_log_payload = serde_json::to_vec(&serde_json::json!({
        "signal_id": "sig-matrix-kafka-log-1",
        "correlation_id": "corr-matrix-kafka-log-1",
        "action_type": "log_marker",
        "message": "matrix kafka log",
        "additional_data": {"ingress": "kafka"},
    }))
    .expect("serialize kafka matrix log payload");
    assert!(
        admin
            .process_signal_ingress_payload(
                kafka_log_payload.as_slice(),
                SignalIngressSource::Kafka,
            )
            .await
    );

    let source_exec_payload = serde_json::to_vec(&serde_json::json!({
        "signal_id": "sig-matrix-source-exec-1",
        "correlation_id": "corr-matrix-source-exec-1",
        "action_type": "execute_snapshot",
        "message": "matrix source execute",
        "additional_data": {"ingress": "source"},
    }))
    .expect("serialize source matrix execute payload");
    assert!(
        admin
            .process_source_signal_ingress_payload(source_exec_payload.as_slice())
            .await
    );

    let source_log_payload = serde_json::to_vec(&serde_json::json!({
        "signal_id": "sig-matrix-source-log-1",
        "correlation_id": "corr-matrix-source-log-1",
        "action_type": "log_marker",
        "message": "matrix source log",
        "additional_data": {"ingress": "source"},
    }))
    .expect("serialize source matrix log payload");
    assert!(
        admin
            .process_source_signal_ingress_payload(source_log_payload.as_slice())
            .await
    );

    wait_for_signal_state(&admin, "sig-matrix-http-1", "execute_snapshot", "COMPLETED").await;
    wait_for_signal_state(
        &admin,
        "sig-matrix-file-exec-1",
        "execute_snapshot",
        "COMPLETED",
    )
    .await;
    wait_for_signal_state(&admin, "sig-matrix-file-log-1", "log_marker", "COMPLETED").await;
    wait_for_signal_state(
        &admin,
        "sig-matrix-kafka-exec-1",
        "execute_snapshot",
        "COMPLETED",
    )
    .await;
    wait_for_signal_state(&admin, "sig-matrix-kafka-log-1", "log_marker", "COMPLETED").await;
    wait_for_signal_state(
        &admin,
        "sig-matrix-source-exec-1",
        "execute_snapshot",
        "COMPLETED",
    )
    .await;
    wait_for_signal_state(&admin, "sig-matrix-source-log-1", "log_marker", "COMPLETED").await;
    sleep(Duration::from_millis(500)).await;

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);

    let http_exec: Vec<_> = notifications
        .iter()
        .filter(|n| n.signal_id == "sig-matrix-http-1" && n.action_type == "execute_snapshot")
        .collect();
    assert!(http_exec.iter().any(|n| n.state == "STARTED"));
    assert!(http_exec.iter().any(|n| n.state == "IN_PROGRESS"));
    assert!(http_exec.iter().any(|n| n.state == "COMPLETED"));
    assert_eq!(
        http_exec.iter().filter(|n| n.state == "COMPLETED").count(),
        1,
        "http execute must emit exactly one terminal notification"
    );

    let file_exec: Vec<_> = notifications
        .iter()
        .filter(|n| n.signal_id == "sig-matrix-file-exec-1" && n.action_type == "execute_snapshot")
        .collect();
    assert!(file_exec.iter().any(|n| n.state == "STARTED"));
    assert!(file_exec.iter().any(|n| n.state == "IN_PROGRESS"));
    assert!(file_exec.iter().any(|n| n.state == "COMPLETED"));
    assert_eq!(
        file_exec.iter().filter(|n| n.state == "COMPLETED").count(),
        1,
        "file execute must emit exactly one terminal notification"
    );

    let file_log: Vec<_> = notifications
        .iter()
        .filter(|n| n.signal_id == "sig-matrix-file-log-1" && n.action_type == "log_marker")
        .collect();
    assert!(file_log.iter().any(|n| n.state == "STARTED"));
    assert!(file_log.iter().any(|n| n.state == "COMPLETED"));
    assert!(
        file_log.iter().all(|n| n.state != "IN_PROGRESS"),
        "log marker lifecycle must not emit IN_PROGRESS"
    );
    assert_eq!(
        file_log.iter().filter(|n| n.state == "COMPLETED").count(),
        1,
        "file log marker must emit exactly one terminal notification"
    );

    let kafka_exec: Vec<_> = notifications
        .iter()
        .filter(|n| n.signal_id == "sig-matrix-kafka-exec-1" && n.action_type == "execute_snapshot")
        .collect();
    assert!(kafka_exec.iter().any(|n| n.state == "STARTED"));
    assert!(kafka_exec.iter().any(|n| n.state == "IN_PROGRESS"));
    assert!(kafka_exec.iter().any(|n| n.state == "COMPLETED"));
    assert_eq!(
        kafka_exec.iter().filter(|n| n.state == "COMPLETED").count(),
        1,
        "kafka execute must emit exactly one terminal notification"
    );

    let kafka_log: Vec<_> = notifications
        .iter()
        .filter(|n| n.signal_id == "sig-matrix-kafka-log-1" && n.action_type == "log_marker")
        .collect();
    assert!(kafka_log.iter().any(|n| n.state == "STARTED"));
    assert!(kafka_log.iter().any(|n| n.state == "COMPLETED"));
    assert!(
        kafka_log.iter().all(|n| n.state != "IN_PROGRESS"),
        "kafka log marker lifecycle must not emit IN_PROGRESS"
    );
    assert_eq!(
        kafka_log.iter().filter(|n| n.state == "COMPLETED").count(),
        1,
        "kafka log marker must emit exactly one terminal notification"
    );

    let source_exec: Vec<_> = notifications
        .iter()
        .filter(|n| {
            n.signal_id == "sig-matrix-source-exec-1" && n.action_type == "execute_snapshot"
        })
        .collect();
    assert!(source_exec.iter().any(|n| n.state == "STARTED"));
    assert!(source_exec.iter().any(|n| n.state == "IN_PROGRESS"));
    assert!(source_exec.iter().any(|n| n.state == "COMPLETED"));
    assert_eq!(
        source_exec
            .iter()
            .filter(|n| n.state == "COMPLETED")
            .count(),
        1,
        "source execute must emit exactly one terminal notification"
    );

    let source_log: Vec<_> = notifications
        .iter()
        .filter(|n| n.signal_id == "sig-matrix-source-log-1" && n.action_type == "log_marker")
        .collect();
    assert!(source_log.iter().any(|n| n.state == "STARTED"));
    assert!(source_log.iter().any(|n| n.state == "COMPLETED"));
    assert!(
        source_log.iter().all(|n| n.state != "IN_PROGRESS"),
        "source log marker lifecycle must not emit IN_PROGRESS"
    );
    assert_eq!(
        source_log.iter().filter(|n| n.state == "COMPLETED").count(),
        1,
        "source log marker must emit exactly one terminal notification"
    );

    let slo = slo_json(&data);
    assert_eq!(
        slo["signal_notifications"]["without_terminal_total"].as_u64(),
        Some(0)
    );
    assert_eq!(
        slo["signal_notifications"]["duplicate_terminal_total"].as_u64(),
        Some(0)
    );
}

#[tokio::test]
async fn signal_action_rejects_empty_log_marker_message() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let response = signal_action(
        State(admin),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9003))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: None,
            correlation_id: None,
            action_type: SignalActionType::LogMarker,
            message: Some("   ".to_string()),
            additional_data: None,
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn signal_action_is_idempotent_for_duplicate_terminal_signal() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let req = SignalActionRequest {
        signal_id: Some("sig-idempotent-1".to_string()),
        correlation_id: Some("corr-idempotent-1".to_string()),
        action_type: SignalActionType::ExecuteSnapshot,
        message: None,
        additional_data: Some(serde_json::json!({"snapshot_mode": "incremental"})),
    };

    let first = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9004))),
        write_headers.clone(),
        Json(req),
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);

    wait_for_signal_state(&admin, "sig-idempotent-1", "execute_snapshot", "COMPLETED").await;
    sleep(Duration::from_millis(500)).await;

    let second = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9005))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-idempotent-1".to_string()),
            correlation_id: Some("corr-idempotent-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            message: None,
            additional_data: Some(serde_json::json!({"snapshot_mode": "incremental"})),
        }),
    )
    .await;
    assert_eq!(second.status(), StatusCode::OK);

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    let execute_notifications: Vec<_> = notifications
        .iter()
        .filter(|n| n.action_type == "execute_snapshot" && n.signal_id == "sig-idempotent-1")
        .collect();
    assert_eq!(execute_notifications.len(), 3);
    assert!(execute_notifications.iter().any(|n| n.state == "STARTED"));
    assert!(execute_notifications
        .iter()
        .any(|n| n.state == "IN_PROGRESS"));
    assert!(execute_notifications.iter().any(|n| n.state == "COMPLETED"));
}

#[tokio::test]
async fn signal_action_fails_closed_when_queue_is_saturated() {
    let cfg = sample_config();
    let mut admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let (tx, rx) = mpsc::channel(1);
    admin.signal_action_tx = tx;
    let _keep_receiver_alive = rx;
    {
        let mut data = admin.data.write().await;
        data.signal_action_queue_depth = 1;
    }

    admin
        .signal_action_tx
        .try_send(QueuedSignalAction {
            signal_id: "sig-fill-1".to_string(),
            correlation_id: "corr-fill-1".to_string(),
            action_type: SignalActionType::ExecuteSnapshot,
            message: String::new(),
            traceparent: None,
            additional_data: serde_json::Value::Null,
            actor_source_ip: None,
            actor_token_id: "write-test".to_string(),
        })
        .expect("queue prefill");

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9006))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-saturated-1".to_string()),
            correlation_id: Some("corr-saturated-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            message: None,
            additional_data: Some(serde_json::json!({"snapshot_mode": "incremental"})),
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read queue saturation response body");
    let payload: serde_json::Value =
        serde_json::from_slice(&body).expect("valid queue saturation response json");
    assert_eq!(payload["current_state"], "ABORTED");
    assert_eq!(payload["action_type"], "execute_snapshot");
    assert_eq!(payload["signal_id"], "sig-saturated-1");

    let data = admin.data.read().await;
    assert_eq!(data.signal_action_queue_rejections_total, 1);
    assert_eq!(data.signal_action_queue_depth, 1);
    assert!(!admin.signal_inflight.contains_key(&(
        "sig-saturated-1".to_string(),
        "execute_snapshot".to_string()
    )));

    let slo = slo_json(&data);
    assert_eq!(
        slo["signal_actions"]["queue_rejections_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        slo["signal_notifications"]["without_terminal_total"].as_u64(),
        Some(0)
    );
    let reasons = slo["reasons"].as_array().expect("reasons array");
    assert!(reasons.iter().any(|reason| {
        reason
            .as_str()
            .expect("reason string")
            .contains("worker queue was saturated or unavailable")
    }));

    let metrics = slo_prometheus(&data);
    assert!(metrics.contains("rustcdc_signal_action_queue_rejections_total 1"));
}

#[tokio::test]
async fn signal_action_fails_closed_without_non_admin_notification_channels() {
    let mut cfg = sample_config();
    cfg.admin.notification_log_file = None;
    cfg.admin.notification_kafka = None;
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9010))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-no-channel-1".to_string()),
            correlation_id: Some("corr-no-channel-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            message: None,
            additional_data: Some(serde_json::json!({"snapshot_mode": "incremental"})),
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("valid response json");
    assert_eq!(payload["current_state"], "ABORTED");
    assert!(payload["error"]
        .as_str()
        .is_some_and(|msg| msg.contains("no non-admin notification channels are enabled")));

    assert!(!admin.signal_inflight.contains_key(&(
        "sig-no-channel-1".to_string(),
        "execute_snapshot".to_string()
    )));

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    assert!(notifications.iter().any(|notification| {
        notification.signal_id == "sig-no-channel-1"
            && notification.action_type == "execute_snapshot"
            && notification.state == "ABORTED"
    }));
}

#[tokio::test]
async fn signal_action_fails_closed_when_notification_flags_are_stale() {
    let cfg = sample_config();
    let mut admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    {
        let mut data = admin.data.write().await;
        data.notification_log_enabled = true;
        data.notification_kafka_enabled = false;
    }
    admin.notification_log = None;
    admin.notification_kafka = None;

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9011))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-started-notify-fail-1".to_string()),
            correlation_id: Some("corr-started-notify-fail-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            message: None,
            additional_data: Some(serde_json::json!({"snapshot_mode": "incremental"})),
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("valid response json");
    assert_eq!(payload["current_state"], "ABORTED");
    assert!(payload["error"]
        .as_str()
        .is_some_and(|msg| { msg.contains("no non-admin notification channels are enabled") }));

    assert!(!admin.signal_inflight.contains_key(&(
        "sig-started-notify-fail-1".to_string(),
        "execute_snapshot".to_string()
    )));

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    assert!(notifications.iter().any(|notification| {
        notification.signal_id == "sig-started-notify-fail-1"
            && notification.action_type == "execute_snapshot"
            && notification.state == "ABORTED"
    }));
}

#[tokio::test]
async fn log_marker_fails_closed_when_started_notification_cannot_emit() {
    let cfg = sample_config();
    let mut admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    {
        let mut data = admin.data.write().await;
        data.notification_log_enabled = true;
        data.notification_kafka_enabled = false;
    }
    admin.notification_log = None;
    admin.notification_kafka = None;

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9012))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-log-started-notify-fail-1".to_string()),
            correlation_id: Some("corr-log-started-notify-fail-1".to_string()),
            action_type: SignalActionType::LogMarker,
            message: Some("strict-log-marker".to_string()),
            additional_data: Some(serde_json::json!({"operator": "unit-test"})),
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("valid response json");
    assert_eq!(payload["current_state"], "ABORTED");
    assert!(payload["error"]
        .as_str()
        .is_some_and(|msg| { msg.contains("failed to emit STARTED lifecycle notification") }));

    let data = admin.data.read().await;
    assert_eq!(data.signal_action_started_notification_rejections_total, 1);
    let notifications = collect_control_notifications(&data);
    assert!(notifications.iter().any(|notification| {
        notification.signal_id == "sig-log-started-notify-fail-1"
            && notification.action_type == "log_marker"
            && notification.state == "ABORTED"
    }));

    let slo = slo_json(&data);
    assert_eq!(
        slo["signal_actions"]["started_notification_rejections_total"].as_u64(),
        Some(1)
    );
    let reasons = slo["reasons"]
        .as_array()
        .expect("reasons array")
        .iter()
        .filter_map(|entry| entry.as_str())
        .collect::<Vec<_>>();
    assert!(reasons
        .iter()
        .any(|reason| { reason.contains("STARTED lifecycle notifications could not be emitted") }));

    let metrics = slo_prometheus(&data);
    assert!(metrics.contains("rustcdc_signal_action_started_notification_rejections_total 1"));
}

#[tokio::test]
async fn paused_signal_is_treated_as_terminal_for_timeout_health() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    {
        let mut data = admin.data.write().await;
        let started_at = Utc::now() - ChronoDuration::seconds(40);
        data.audit_entries_total = 3;
        data.audit_recent_entries = vec![
            AuditTrailEntry {
                sequence: 1,
                at: started_at,
                action: "signal_pause_snapshot_started".to_string(),
                result: "accepted".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-pause-health-1",
                    "correlation_id": "corr-pause-health-1",
                    "action_type": "pause_snapshot",
                    "state": "STARTED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "genesis".to_string(),
                entry_hash_hex: "hash-1".to_string(),
                ed25519_signature_hex: None,
            },
            AuditTrailEntry {
                sequence: 2,
                at: Utc::now() - ChronoDuration::seconds(35),
                action: "signal_pause_snapshot_paused".to_string(),
                result: "paused".to_string(),
                detail: serde_json::json!({
                    "signal_id": "sig-pause-health-1",
                    "correlation_id": "corr-pause-health-1",
                    "action_type": "pause_snapshot",
                    "state": "PAUSED"
                })
                .to_string(),
                actor_source_ip: Some("127.0.0.1".to_string()),
                actor_token_id: Some("write-env".to_string()),
                prev_hash_hex: "hash-1".to_string(),
                entry_hash_hex: "hash-2".to_string(),
                ed25519_signature_hex: None,
            },
        ];
    }

    let data = admin.data.read().await;
    let slo = slo_json(&data);
    assert_eq!(
        slo["signal_notifications"]["without_terminal_total"].as_u64(),
        Some(0)
    );
}

#[tokio::test]
async fn signal_ingress_file_processes_execute_snapshot_and_emits_notifications() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    admin
        .process_file_signal_ingress_record(SignalIngressRecord {
            signal_id: Some("sig-ingress-1".to_string()),
            correlation_id: Some("corr-ingress-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            message: Some("ingress execute".to_string()),
            additional_data: Some(serde_json::json!({"source": "file-ingress"})),
            traceparent: Some(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".to_string(),
            ),
        })
        .await;

    wait_for_signal_state(&admin, "sig-ingress-1", "execute_snapshot", "COMPLETED").await;

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    assert!(notifications.iter().any(|notification| {
        notification.signal_id == "sig-ingress-1"
            && notification.action_type == "execute_snapshot"
            && notification.state == "COMPLETED"
    }));

    assert!(data.audit_recent_entries.iter().any(|entry| {
        entry.action == "signal_execute_snapshot_completed"
            && entry.actor_token_id.as_deref() == Some("signal-ingress-file")
    }));
}

#[tokio::test]
async fn signal_ingress_file_fails_closed_without_non_admin_notification_channels() {
    let mut cfg = sample_config();
    cfg.admin.notification_log_file = None;
    cfg.admin.notification_kafka = None;

    let admin = AdminState::new(&cfg).await.expect("admin state");
    admin
        .process_file_signal_ingress_record(SignalIngressRecord {
            signal_id: Some("sig-ingress-no-channel-1".to_string()),
            correlation_id: Some("corr-ingress-no-channel-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            message: Some("ingress execute".to_string()),
            additional_data: None,
            traceparent: None,
        })
        .await;

    wait_for_signal_state(
        &admin,
        "sig-ingress-no-channel-1",
        "execute_snapshot",
        "ABORTED",
    )
    .await;

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    assert!(notifications.iter().any(|notification| {
        notification.signal_id == "sig-ingress-no-channel-1"
            && notification.action_type == "execute_snapshot"
            && notification.state == "ABORTED"
    }));
}

#[test]
fn signal_ingress_file_record_rejects_unknown_fields() {
    let parsed = serde_json::from_str::<SignalIngressRecord>(
        r#"{
            "signal_id": "sig-ingress-unknown-1",
            "action_type": "log_marker",
            "message": "ok",
            "unexpected": true
        }"#,
    );

    assert!(parsed.is_err(), "unknown ingress fields must be rejected");
}

#[tokio::test]
async fn signal_ingress_file_worker_surfaces_parse_rejections_in_slo_and_metrics() {
    let mut cfg = sample_config();
    let dir = tempfile::tempdir().expect("tempdir");
    let ingress_path = dir.path().join("signal-ingress.jsonl");
    std::fs::write(&ingress_path, "").expect("create ingress file");
    cfg.admin.signal_ingress_file = Some(ingress_path.clone());

    let admin = AdminState::new(&cfg).await.expect("admin state");
    append_signal_ingress_line(&ingress_path, "{invalid json}");

    wait_for_signal_ingress_rejection_counters(&admin, 1, 0, 0).await;

    let data = admin.data.read().await;
    let slo = slo_json(&data);
    assert_eq!(
        slo["signal_ingress"]["parse_rejections_total"].as_u64(),
        Some(1)
    );
    let reasons = slo["reasons"].as_array().expect("reasons array");
    assert!(reasons.iter().any(|reason| {
        reason
            .as_str()
            .expect("reason string")
            .contains("malformed payloads")
    }));

    let metrics = slo_prometheus(&data);
    assert!(metrics.contains("rustcdc_signal_ingress_parse_rejections_total 1"));
}

#[tokio::test]
async fn signal_ingress_file_worker_rejects_oversized_lines() {
    let mut cfg = sample_config();
    let dir = tempfile::tempdir().expect("tempdir");
    let ingress_path = dir.path().join("signal-ingress.jsonl");
    std::fs::write(&ingress_path, "").expect("create ingress file");
    cfg.admin.signal_ingress_file = Some(ingress_path.clone());

    let admin = AdminState::new(&cfg).await.expect("admin state");
    let oversized_payload = "a".repeat(super::SIGNAL_INGRESS_MAX_LINE_BYTES + 128);
    append_signal_ingress_line(&ingress_path, &oversized_payload);

    wait_for_signal_ingress_rejection_counters(&admin, 0, 1, 0).await;

    let data = admin.data.read().await;
    let slo = slo_json(&data);
    assert_eq!(
        slo["signal_ingress"]["line_too_large_rejections_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        slo["signal_ingress"]["max_line_bytes"].as_u64(),
        Some(super::SIGNAL_INGRESS_MAX_LINE_BYTES as u64)
    );

    let metrics = slo_prometheus(&data);
    assert!(metrics.contains("rustcdc_signal_ingress_line_too_large_rejections_total 1"));
    assert!(metrics.contains(&format!(
        "rustcdc_signal_ingress_max_line_bytes {}",
        super::SIGNAL_INGRESS_MAX_LINE_BYTES
    )));
}

#[tokio::test]
async fn signal_ingress_log_marker_fails_closed_when_started_notification_cannot_emit() {
    let cfg = sample_config();
    let mut admin = AdminState::new(&cfg).await.expect("admin state");

    {
        let mut data = admin.data.write().await;
        data.notification_log_enabled = true;
        data.notification_kafka_enabled = false;
    }
    admin.notification_log = None;
    admin.notification_kafka = None;

    admin
        .process_file_signal_ingress_record(SignalIngressRecord {
            signal_id: Some("sig-ingress-log-fail-1".to_string()),
            correlation_id: Some("corr-ingress-log-fail-1".to_string()),
            action_type: SignalActionType::LogMarker,
            message: Some("ingress marker".to_string()),
            additional_data: None,
            traceparent: None,
        })
        .await;

    wait_for_signal_state(&admin, "sig-ingress-log-fail-1", "log_marker", "ABORTED").await;

    let data = admin.data.read().await;
    assert_eq!(data.signal_action_started_notification_rejections_total, 1);
    assert_eq!(data.signal_ingress_validation_rejections_total, 0);
    let notifications = collect_control_notifications(&data);
    assert!(notifications.iter().any(|notification| {
        notification.signal_id == "sig-ingress-log-fail-1"
            && notification.action_type == "log_marker"
            && notification.state == "ABORTED"
    }));
}

#[tokio::test]
async fn signal_ingress_log_marker_empty_message_increments_validation_rejections() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    admin
        .process_file_signal_ingress_record(SignalIngressRecord {
            signal_id: Some("sig-ingress-log-empty-1".to_string()),
            correlation_id: Some("corr-ingress-log-empty-1".to_string()),
            action_type: SignalActionType::LogMarker,
            message: Some("   ".to_string()),
            additional_data: None,
            traceparent: None,
        })
        .await;

    wait_for_signal_ingress_rejection_counters(&admin, 0, 0, 1).await;
    let data = admin.data.read().await;
    let slo = slo_json(&data);
    assert_eq!(
        slo["signal_ingress"]["validation_rejections_total"].as_u64(),
        Some(1)
    );

    let metrics = slo_prometheus(&data);
    assert!(metrics.contains("rustcdc_signal_ingress_validation_rejections_total 1"));
}

#[tokio::test]
async fn signal_ingress_payload_processor_preserves_kafka_actor_metadata() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    let payload = serde_json::json!({
        "signal_id": "sig-ingress-kafka-actor-1",
        "correlation_id": "corr-ingress-kafka-actor-1",
        "action_type": "execute_snapshot",
        "message": "kafka ingress"
    });
    let payload_bytes = serde_json::to_vec(&payload).expect("serialize ingress payload");

    assert!(
        admin
            .process_signal_ingress_payload(payload_bytes.as_slice(), SignalIngressSource::Kafka,)
            .await
    );

    wait_for_signal_state(
        &admin,
        "sig-ingress-kafka-actor-1",
        "execute_snapshot",
        "COMPLETED",
    )
    .await;

    let data = admin.data.read().await;
    assert!(data.audit_recent_entries.iter().any(|entry| {
        entry.action == SignalActionType::ExecuteSnapshot.started_audit_action()
            && entry.actor_source_ip.as_deref() == Some("signal_ingress_kafka")
            && entry.actor_token_id.as_deref() == Some("signal-ingress-kafka")
            && entry
                .detail
                .contains("\"signal_id\":\"sig-ingress-kafka-actor-1\"")
    }));
}

#[tokio::test]
async fn admin_state_stores_redacted_config_json() {
    let mut cfg = sample_config();
    cfg.sink = crate::config::schema::SinkConfig::Http(crate::config::schema::HttpSinkConfig {
        url: "https://example.invalid/ingest".to_string(),
        timeout_ms: 5_000,
        batch_max_events: 64,
        batch_max_delay_ms: 250,
        max_pending_bytes: 1024 * 1024,
        max_retries: 3,
        batch_retry_time_budget_ms: 30_000,
        backoff_initial_ms: 100,
        backoff_max_ms: 1_000,
        backoff_multiplier: 2.0,
        headers: std::collections::HashMap::from([(
            "Authorization".to_string(),
            "Bearer top-secret".to_string(),
        )]),
        bearer_token: Some(rustcdc::SecretString::new("top-secret-token")),
        verify_tls: true,
        pool_max_idle_per_host: 10,
        pool_idle_timeout_secs: None,
        tcp_keepalive_secs: None,
        codec: None,
    });

    let admin = AdminState::new(&cfg).await.expect("admin state");
    let data = admin.data.read().await;

    assert!(data.config_json.contains("[REDACTED]"));
    assert!(!data.config_json.contains("top-secret"));
    let snapshot: serde_json::Value =
        serde_json::from_str(&data.config_json).expect("redacted snapshot is valid json");
    assert_eq!(snapshot["sink"]["headers"]["Authorization"], "[REDACTED]");
}

#[test]
fn endpoint_rate_limiter_is_per_client_and_burst_bounded() {
    let limiter = EndpointRateLimiter::new(1, 1);

    assert!(limiter.allow("client-a"));
    assert!(!limiter.allow("client-a"));
    assert!(limiter.allow("client-b"));
}

#[test]
fn abuse_guard_uses_peer_ip_when_proxy_not_trusted() {
    let guard = AdminAbuseGuard::new(&crate::config::schema::AdminConfig::default());
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-for", HeaderValue::from_static("203.0.113.7"));

    let peer_addr: SocketAddr = "10.0.0.10:7777".parse().expect("peer addr");
    assert_eq!(
        guard.client_key(&headers, Some(peer_addr)),
        "peer:10.0.0.10"
    );
}

#[test]
fn abuse_guard_honors_forwarded_ip_from_trusted_proxy() {
    let config = crate::config::schema::AdminConfig {
        trusted_proxy_ips: vec!["10.0.0.10".to_string()],
        ..Default::default()
    };
    let guard = AdminAbuseGuard::new(&config);

    let mut headers = HeaderMap::new();
    // Use globally-routable IPs (not documentation ranges like 203.0.113.x
    // which are filtered by is_globally_routable).
    headers.insert(
        "x-forwarded-for",
        HeaderValue::from_static("1.2.3.4, 5.6.7.8"),
    );

    let peer_addr: SocketAddr = "10.0.0.10:7777".parse().expect("peer addr");
    assert_eq!(guard.client_key(&headers, Some(peer_addr)), "xff:1.2.3.4");
}

#[test]
fn alert_rules_file_contains_expected_slo_gates() {
    let rules = include_str!("../../monitoring/rustcdc_slo_alerts.yml");
    assert!(rules.contains("CDCReadinessRateLow"));
    assert!(rules.contains("CDCCheckpointAgeHigh"));
    assert!(rules.contains("CDCAdminAPILatencyHigh"));
    assert!(rules.contains("CDCRestartRecoverySlow"));
    assert!(rules.contains("CDCAdminManifestReloadFailed"));
    assert!(rules.contains("CDCAdminControlPlaneDown"));
    assert!(rules.contains("CDCAdminManifestStaleBlocked"));
    assert!(rules.contains("CDCAdminRevokedTokenUseDetected"));
    assert!(rules.contains("CDCAdminShutdownCompletionErrors"));
    assert!(rules.contains("CDCReconciliationRecoveryDetected"));
    assert!(rules.contains("CDCReconciliationRecoveryProofFailed"));
    assert!(rules.contains("CDCAdminAuthAbuse"));
    assert!(rules.contains("CDCAdminRateLimiterContentionHigh"));
    assert!(rules.contains("CDCDataCorrectnessDegradation"));
    assert!(rules.contains("CDCHttpBatchOldestEventAgeHigh"));
    assert!(rules.contains("CDCRuntimeRecoverableBreakerOpen"));
    assert!(rules.contains("CDCRuntimeRecoverableBreakerEscalationRisk"));
}

#[test]
fn read_auth_requires_matching_bearer_when_token_configured() {
    let admin = AdminState {
        data: Arc::new(RwLock::new(AdminStateData {
            state: InstanceState::Starting,
            started_at: Utc::now(),
            first_ready_at: None,
            first_checkpoint_advanced_at: None,
            last_batch_at: None,
            events_processed: 0,
            batches_processed: 0,
            readiness_checks_total: 0,
            readiness_ready_total: 0,
            last_admin_api_latency_us: None,
            admin_api_latency_us_buckets: [0u64;
                super::ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US.len()],
            admin_api_latency_us_sum: 0.0,
            admin_api_latency_us_count: 0,
            checkpoint_age_seconds: None,
            replication_slot_lag_bytes: None,
            restart_recovery_seconds: None,
            source_consecutive_errors: 0,
            degraded_since: None,
            shutdown_requests_os_signal_total: 0,
            shutdown_completions_stopped_total: 0,
            shutdown_completions_error_total: 0,
            admin_rate_limited_readyz_total: 0,
            admin_rate_limited_status_total: 0,
            admin_rate_limited_metrics_total: 0,
            admin_rate_limiter_readyz_decisions_total: 0,
            admin_rate_limiter_readyz_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_readyz_decision_latency_seconds_max: 0.0,
            admin_rate_limiter_status_decisions_total: 0,
            admin_rate_limiter_status_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_status_decision_latency_seconds_max: 0.0,
            admin_rate_limiter_metrics_decisions_total: 0,
            admin_rate_limiter_metrics_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_metrics_decision_latency_seconds_max: 0.0,
            last_terminal_reason_code: None,
            reconciliation_recoveries_total: 0,
            reconciliation_recovery_last_unix_seconds: None,
            reconciliation_recovery_last_parse_ok: None,
            reconciliation_recovery_last_detail: None,
            reconciliation_recovery_proof_last_unix_seconds: None,
            reconciliation_recovery_proof_last_ok: None,
            reconciliation_recovery_proof_last_detail: None,
            signal_action_queue_rejections_total: 0,
            signal_action_started_notification_rejections_total: 0,
            signal_ingress_parse_rejections_total: 0,
            signal_ingress_line_too_large_rejections_total: 0,
            signal_ingress_validation_rejections_total: 0,
            signal_action_queue_depth: 0,
            notification_log_enabled: false,
            notification_kafka_enabled: false,
            notification_log_emitted_total: 0,
            notification_log_emit_failures_total: 0,
            notification_channel_emitted_total: HashMap::new(),
            notification_channel_emit_failures_total: HashMap::new(),
            audit_entries_total: 0,
            audit_recent_entries: Vec::new(),
            audit_signing_key: None,
            audit_ip_pseudonymise: false,
            audit_ip_salt: [0u8; 16],
            runtime_metrics: String::new(),
            runtime_metrics_generation: 0,
            config_json: String::new(),
        })),
        abuse_guard: Arc::new(AdminAbuseGuard::new(
            &crate::config::schema::AdminConfig::default(),
        )),
        readiness_auth_mode: AdminProbeAuthMode::RequireReadToken,
        auth_state: Arc::new(std::sync::RwLock::new(AuthState {
            read_tokens: vec![AuthToken {
                id: "read-test".to_string(),
                token_sha256_hex: token_sha256_hex("read-secret"),
                not_before: None,
                expires_at: None,
                revoked: false,
            }],
            write_tokens: vec![AuthToken {
                id: "write-test".to_string(),
                token_sha256_hex: token_sha256_hex("write-secret"),
                not_before: None,
                expires_at: None,
                revoked: false,
            }],
            source: AuthSource::Env,
            manifest_version: 0,
            manifest_reload_ok: true,
            manifest_stale_blocked: false,
            last_reload_at: None,
            last_reload_error: None,
            last_observed_mtime: None,
            last_refresh_attempt: None,
            revoked_token_hits_total: 0,
        })),
        audit_log: None,
        notification_log: None,
        notification_kafka: None,
        signal_action_tx: AdminState::dormant_signal_action_tx(),
        signal_inflight: Arc::new(DashMap::new()),
        workers: super::AdminWorkers::new(),
        notification_broadcast: Arc::new(
            tokio::sync::broadcast::channel(super::NOTIFICATION_STREAM_BUFFER).0,
        ),
        audit_log_drop_counter: None,
    };

    let mut read_headers = HeaderMap::new();
    read_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer read-secret"),
    );
    assert!(admin.authorize_read(&read_headers));
    assert!(admin.authorize_write_token_id(&read_headers).is_none());

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );
    assert_eq!(
        admin.authorize_write_token_id(&write_headers).as_deref(),
        Some("write-test")
    );

    let mut wrong_headers = HeaderMap::new();
    wrong_headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer nope"));
    assert!(!admin.authorize_read(&wrong_headers));
    assert!(admin.authorize_write_token_id(&wrong_headers).is_none());
}

#[test]
fn write_scope_fails_closed_when_only_read_tokens_are_configured() {
    let admin = AdminState {
        data: Arc::new(RwLock::new(AdminStateData {
            state: InstanceState::Starting,
            started_at: Utc::now(),
            first_ready_at: None,
            first_checkpoint_advanced_at: None,
            last_batch_at: None,
            events_processed: 0,
            batches_processed: 0,
            readiness_checks_total: 0,
            readiness_ready_total: 0,
            last_admin_api_latency_us: None,
            admin_api_latency_us_buckets: [0u64;
                super::ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US.len()],
            admin_api_latency_us_sum: 0.0,
            admin_api_latency_us_count: 0,
            checkpoint_age_seconds: None,
            replication_slot_lag_bytes: None,
            restart_recovery_seconds: None,
            source_consecutive_errors: 0,
            degraded_since: None,
            shutdown_requests_os_signal_total: 0,
            shutdown_completions_stopped_total: 0,
            shutdown_completions_error_total: 0,
            admin_rate_limited_readyz_total: 0,
            admin_rate_limited_status_total: 0,
            admin_rate_limited_metrics_total: 0,
            admin_rate_limiter_readyz_decisions_total: 0,
            admin_rate_limiter_readyz_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_readyz_decision_latency_seconds_max: 0.0,
            admin_rate_limiter_status_decisions_total: 0,
            admin_rate_limiter_status_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_status_decision_latency_seconds_max: 0.0,
            admin_rate_limiter_metrics_decisions_total: 0,
            admin_rate_limiter_metrics_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_metrics_decision_latency_seconds_max: 0.0,
            last_terminal_reason_code: None,
            reconciliation_recoveries_total: 0,
            reconciliation_recovery_last_unix_seconds: None,
            reconciliation_recovery_last_parse_ok: None,
            reconciliation_recovery_last_detail: None,
            reconciliation_recovery_proof_last_unix_seconds: None,
            reconciliation_recovery_proof_last_ok: None,
            reconciliation_recovery_proof_last_detail: None,
            signal_action_queue_rejections_total: 0,
            signal_action_started_notification_rejections_total: 0,
            signal_ingress_parse_rejections_total: 0,
            signal_ingress_line_too_large_rejections_total: 0,
            signal_ingress_validation_rejections_total: 0,
            signal_action_queue_depth: 0,
            notification_log_enabled: false,
            notification_kafka_enabled: false,
            notification_log_emitted_total: 0,
            notification_log_emit_failures_total: 0,
            notification_channel_emitted_total: HashMap::new(),
            notification_channel_emit_failures_total: HashMap::new(),
            audit_entries_total: 0,
            audit_recent_entries: Vec::new(),
            audit_signing_key: None,
            audit_ip_pseudonymise: false,
            audit_ip_salt: [0u8; 16],
            runtime_metrics: String::new(),
            runtime_metrics_generation: 0,
            config_json: String::new(),
        })),
        abuse_guard: Arc::new(AdminAbuseGuard::new(
            &crate::config::schema::AdminConfig::default(),
        )),
        readiness_auth_mode: AdminProbeAuthMode::RequireReadToken,
        auth_state: Arc::new(std::sync::RwLock::new(AuthState {
            read_tokens: vec![AuthToken {
                id: "read-only".to_string(),
                token_sha256_hex: token_sha256_hex("read-secret"),
                not_before: None,
                expires_at: None,
                revoked: false,
            }],
            write_tokens: vec![],
            source: AuthSource::Env,
            manifest_version: 0,
            manifest_reload_ok: true,
            manifest_stale_blocked: false,
            last_reload_at: None,
            last_reload_error: None,
            last_observed_mtime: None,
            last_refresh_attempt: None,
            revoked_token_hits_total: 0,
        })),
        audit_log: None,
        notification_log: None,
        notification_kafka: None,
        signal_action_tx: AdminState::dormant_signal_action_tx(),
        signal_inflight: Arc::new(DashMap::new()),
        workers: super::AdminWorkers::new(),
        notification_broadcast: Arc::new(
            tokio::sync::broadcast::channel(super::NOTIFICATION_STREAM_BUFFER).0,
        ),
        audit_log_drop_counter: None,
    };

    let mut read_headers = HeaderMap::new();
    read_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer read-secret"),
    );
    assert!(admin.authorize_read(&read_headers));
    assert!(admin.authorize_write_token_id(&read_headers).is_none());

    let mut wrong_headers = HeaderMap::new();
    wrong_headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer nope"));
    assert!(admin.authorize_write_token_id(&wrong_headers).is_none());
}

#[test]
fn read_scope_fails_closed_when_no_read_tokens_are_configured() {
    let admin = AdminState {
        data: Arc::new(RwLock::new(AdminStateData {
            state: InstanceState::Starting,
            started_at: Utc::now(),
            first_ready_at: None,
            first_checkpoint_advanced_at: None,
            last_batch_at: None,
            events_processed: 0,
            batches_processed: 0,
            readiness_checks_total: 0,
            readiness_ready_total: 0,
            last_admin_api_latency_us: None,
            admin_api_latency_us_buckets: [0u64;
                super::ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US.len()],
            admin_api_latency_us_sum: 0.0,
            admin_api_latency_us_count: 0,
            checkpoint_age_seconds: None,
            replication_slot_lag_bytes: None,
            restart_recovery_seconds: None,
            source_consecutive_errors: 0,
            degraded_since: None,
            shutdown_requests_os_signal_total: 0,
            shutdown_completions_stopped_total: 0,
            shutdown_completions_error_total: 0,
            admin_rate_limited_readyz_total: 0,
            admin_rate_limited_status_total: 0,
            admin_rate_limited_metrics_total: 0,
            admin_rate_limiter_readyz_decisions_total: 0,
            admin_rate_limiter_readyz_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_readyz_decision_latency_seconds_max: 0.0,
            admin_rate_limiter_status_decisions_total: 0,
            admin_rate_limiter_status_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_status_decision_latency_seconds_max: 0.0,
            admin_rate_limiter_metrics_decisions_total: 0,
            admin_rate_limiter_metrics_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_metrics_decision_latency_seconds_max: 0.0,
            last_terminal_reason_code: None,
            reconciliation_recoveries_total: 0,
            reconciliation_recovery_last_unix_seconds: None,
            reconciliation_recovery_last_parse_ok: None,
            reconciliation_recovery_last_detail: None,
            reconciliation_recovery_proof_last_unix_seconds: None,
            reconciliation_recovery_proof_last_ok: None,
            reconciliation_recovery_proof_last_detail: None,
            signal_action_queue_rejections_total: 0,
            signal_action_started_notification_rejections_total: 0,
            signal_ingress_parse_rejections_total: 0,
            signal_ingress_line_too_large_rejections_total: 0,
            signal_ingress_validation_rejections_total: 0,
            signal_action_queue_depth: 0,
            notification_log_enabled: false,
            notification_kafka_enabled: false,
            notification_log_emitted_total: 0,
            notification_log_emit_failures_total: 0,
            notification_channel_emitted_total: HashMap::new(),
            notification_channel_emit_failures_total: HashMap::new(),
            audit_entries_total: 0,
            audit_recent_entries: Vec::new(),
            audit_signing_key: None,
            audit_ip_pseudonymise: false,
            audit_ip_salt: [0u8; 16],
            runtime_metrics: String::new(),
            runtime_metrics_generation: 0,
            config_json: String::new(),
        })),
        abuse_guard: Arc::new(AdminAbuseGuard::new(
            &crate::config::schema::AdminConfig::default(),
        )),
        readiness_auth_mode: AdminProbeAuthMode::RequireReadToken,
        auth_state: Arc::new(std::sync::RwLock::new(AuthState {
            read_tokens: vec![],
            write_tokens: vec![AuthToken {
                id: "write-only".to_string(),
                token_sha256_hex: token_sha256_hex("write-secret"),
                not_before: None,
                expires_at: None,
                revoked: false,
            }],
            source: AuthSource::Env,
            manifest_version: 0,
            manifest_reload_ok: true,
            manifest_stale_blocked: false,
            last_reload_at: None,
            last_reload_error: None,
            last_observed_mtime: None,
            last_refresh_attempt: None,
            revoked_token_hits_total: 0,
        })),
        audit_log: None,
        notification_log: None,
        notification_kafka: None,
        signal_action_tx: AdminState::dormant_signal_action_tx(),
        signal_inflight: Arc::new(DashMap::new()),
        workers: super::AdminWorkers::new(),
        notification_broadcast: Arc::new(
            tokio::sync::broadcast::channel(super::NOTIFICATION_STREAM_BUFFER).0,
        ),
        audit_log_drop_counter: None,
    };

    assert!(!admin.authorize_read(&HeaderMap::new()));
}

#[test]
fn token_lifecycle_enforces_expiry_and_revocation() {
    let now = Utc::now();

    let admin = AdminState {
        data: Arc::new(RwLock::new(AdminStateData {
            state: InstanceState::Starting,
            started_at: now,
            first_ready_at: None,
            first_checkpoint_advanced_at: None,
            last_batch_at: None,
            events_processed: 0,
            batches_processed: 0,
            readiness_checks_total: 0,
            readiness_ready_total: 0,
            last_admin_api_latency_us: None,
            admin_api_latency_us_buckets: [0u64;
                super::ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US.len()],
            admin_api_latency_us_sum: 0.0,
            admin_api_latency_us_count: 0,
            checkpoint_age_seconds: None,
            replication_slot_lag_bytes: None,
            restart_recovery_seconds: None,
            source_consecutive_errors: 0,
            degraded_since: None,
            shutdown_requests_os_signal_total: 0,
            shutdown_completions_stopped_total: 0,
            shutdown_completions_error_total: 0,
            admin_rate_limited_readyz_total: 0,
            admin_rate_limited_status_total: 0,
            admin_rate_limited_metrics_total: 0,
            admin_rate_limiter_readyz_decisions_total: 0,
            admin_rate_limiter_readyz_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_readyz_decision_latency_seconds_max: 0.0,
            admin_rate_limiter_status_decisions_total: 0,
            admin_rate_limiter_status_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_status_decision_latency_seconds_max: 0.0,
            admin_rate_limiter_metrics_decisions_total: 0,
            admin_rate_limiter_metrics_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_metrics_decision_latency_seconds_max: 0.0,
            last_terminal_reason_code: None,
            reconciliation_recoveries_total: 0,
            reconciliation_recovery_last_unix_seconds: None,
            reconciliation_recovery_last_parse_ok: None,
            reconciliation_recovery_last_detail: None,
            reconciliation_recovery_proof_last_unix_seconds: None,
            reconciliation_recovery_proof_last_ok: None,
            reconciliation_recovery_proof_last_detail: None,
            signal_action_queue_rejections_total: 0,
            signal_action_started_notification_rejections_total: 0,
            signal_ingress_parse_rejections_total: 0,
            signal_ingress_line_too_large_rejections_total: 0,
            signal_ingress_validation_rejections_total: 0,
            signal_action_queue_depth: 0,
            notification_log_enabled: false,
            notification_kafka_enabled: false,
            notification_log_emitted_total: 0,
            notification_log_emit_failures_total: 0,
            notification_channel_emitted_total: HashMap::new(),
            notification_channel_emit_failures_total: HashMap::new(),
            audit_entries_total: 0,
            audit_recent_entries: Vec::new(),
            audit_signing_key: None,
            audit_ip_pseudonymise: false,
            audit_ip_salt: [0u8; 16],
            runtime_metrics: String::new(),
            runtime_metrics_generation: 0,
            config_json: String::new(),
        })),
        abuse_guard: Arc::new(AdminAbuseGuard::new(
            &crate::config::schema::AdminConfig::default(),
        )),
        readiness_auth_mode: AdminProbeAuthMode::RequireReadToken,
        auth_state: Arc::new(std::sync::RwLock::new(AuthState {
            read_tokens: vec![
                AuthToken {
                    id: "expired".to_string(),
                    token_sha256_hex: token_sha256_hex("expired-token"),
                    not_before: None,
                    expires_at: Some(now),
                    revoked: false,
                },
                AuthToken {
                    id: "revoked".to_string(),
                    token_sha256_hex: token_sha256_hex("revoked-token"),
                    not_before: None,
                    expires_at: None,
                    revoked: true,
                },
                AuthToken {
                    id: "active".to_string(),
                    token_sha256_hex: token_sha256_hex("active-token"),
                    not_before: None,
                    expires_at: Some(now + chrono::Duration::minutes(10)),
                    revoked: false,
                },
            ],
            write_tokens: vec![],
            source: AuthSource::Env,
            manifest_version: 0,
            manifest_reload_ok: true,
            manifest_stale_blocked: false,
            last_reload_at: None,
            last_reload_error: None,
            last_observed_mtime: None,
            last_refresh_attempt: None,
            revoked_token_hits_total: 0,
        })),
        audit_log: None,
        notification_log: None,
        notification_kafka: None,
        signal_action_tx: AdminState::dormant_signal_action_tx(),
        signal_inflight: Arc::new(DashMap::new()),
        workers: super::AdminWorkers::new(),
        notification_broadcast: Arc::new(
            tokio::sync::broadcast::channel(super::NOTIFICATION_STREAM_BUFFER).0,
        ),
        audit_log_drop_counter: None,
    };

    let mut expired_headers = HeaderMap::new();
    expired_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer expired-token"),
    );
    assert!(!admin.authorize_read(&expired_headers));

    let mut revoked_headers = HeaderMap::new();
    revoked_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer revoked-token"),
    );
    assert!(!admin.authorize_read(&revoked_headers));

    let mut active_headers = HeaderMap::new();
    active_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer active-token"),
    );
    assert!(admin.authorize_read(&active_headers));

    let revoked_hits = admin
        .auth_state
        .read()
        .expect("auth lock")
        .revoked_token_hits_total;
    assert_eq!(revoked_hits, 1);
}

#[test]
fn stale_manifest_policy_blocks_authorization() {
    let now = Utc::now();
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer active-token"),
    );

    let admin = AdminState {
        data: Arc::new(RwLock::new(AdminStateData {
            state: InstanceState::Starting,
            started_at: now,
            first_ready_at: None,
            first_checkpoint_advanced_at: None,
            last_batch_at: None,
            events_processed: 0,
            batches_processed: 0,
            readiness_checks_total: 0,
            readiness_ready_total: 0,
            last_admin_api_latency_us: None,
            admin_api_latency_us_buckets: [0u64;
                super::ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US.len()],
            admin_api_latency_us_sum: 0.0,
            admin_api_latency_us_count: 0,
            checkpoint_age_seconds: None,
            replication_slot_lag_bytes: None,
            restart_recovery_seconds: None,
            source_consecutive_errors: 0,
            degraded_since: None,
            shutdown_requests_os_signal_total: 0,
            shutdown_completions_stopped_total: 0,
            shutdown_completions_error_total: 0,
            admin_rate_limited_readyz_total: 0,
            admin_rate_limited_status_total: 0,
            admin_rate_limited_metrics_total: 0,
            admin_rate_limiter_readyz_decisions_total: 0,
            admin_rate_limiter_readyz_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_readyz_decision_latency_seconds_max: 0.0,
            admin_rate_limiter_status_decisions_total: 0,
            admin_rate_limiter_status_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_status_decision_latency_seconds_max: 0.0,
            admin_rate_limiter_metrics_decisions_total: 0,
            admin_rate_limiter_metrics_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_metrics_decision_latency_seconds_max: 0.0,
            last_terminal_reason_code: None,
            reconciliation_recoveries_total: 0,
            reconciliation_recovery_last_unix_seconds: None,
            reconciliation_recovery_last_parse_ok: None,
            reconciliation_recovery_last_detail: None,
            reconciliation_recovery_proof_last_unix_seconds: None,
            reconciliation_recovery_proof_last_ok: None,
            reconciliation_recovery_proof_last_detail: None,
            signal_action_queue_rejections_total: 0,
            signal_action_started_notification_rejections_total: 0,
            signal_ingress_parse_rejections_total: 0,
            signal_ingress_line_too_large_rejections_total: 0,
            signal_ingress_validation_rejections_total: 0,
            signal_action_queue_depth: 0,
            notification_log_enabled: false,
            notification_kafka_enabled: false,
            notification_log_emitted_total: 0,
            notification_log_emit_failures_total: 0,
            notification_channel_emitted_total: HashMap::new(),
            notification_channel_emit_failures_total: HashMap::new(),
            audit_entries_total: 0,
            audit_recent_entries: Vec::new(),
            audit_signing_key: None,
            audit_ip_pseudonymise: false,
            audit_ip_salt: [0u8; 16],
            runtime_metrics: String::new(),
            runtime_metrics_generation: 0,
            config_json: String::new(),
        })),
        abuse_guard: Arc::new(AdminAbuseGuard::new(
            &crate::config::schema::AdminConfig::default(),
        )),
        readiness_auth_mode: AdminProbeAuthMode::RequireReadToken,
        auth_state: Arc::new(std::sync::RwLock::new(AuthState {
            read_tokens: vec![AuthToken {
                id: "active".to_string(),
                token_sha256_hex: token_sha256_hex("active-token"),
                not_before: None,
                expires_at: Some(now + chrono::Duration::minutes(10)),
                revoked: false,
            }],
            write_tokens: vec![],
            source: AuthSource::Manifest {
                path: std::path::PathBuf::from("/tmp/non-existent-token-manifest.json"),
                trusted_public_keys: vec![],
                refresh_interval: Duration::from_millis(1),
                max_staleness: Some(Duration::from_millis(1)),
            },
            manifest_version: 1,
            manifest_reload_ok: true,
            manifest_stale_blocked: false,
            last_reload_at: Some(SystemTime::UNIX_EPOCH),
            last_reload_error: None,
            last_observed_mtime: None,
            last_refresh_attempt: None,
            revoked_token_hits_total: 0,
        })),
        audit_log: None,
        notification_log: None,
        notification_kafka: None,
        signal_action_tx: AdminState::dormant_signal_action_tx(),
        signal_inflight: Arc::new(DashMap::new()),
        workers: super::AdminWorkers::new(),
        notification_broadcast: Arc::new(
            tokio::sync::broadcast::channel(super::NOTIFICATION_STREAM_BUFFER).0,
        ),
        audit_log_drop_counter: None,
    };

    assert!(admin
        .authorize_scope_token_id(&headers, AdminScope::Read)
        .is_none());
}

#[test]
fn manifest_refresh_supports_signer_key_rotation() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("admin-token-manifest.json");

    let old_key = SigningKey::from_bytes(&[0x31; 32]);
    let new_key = SigningKey::from_bytes(&[0x32; 32]);

    write_signed_manifest(
        &path,
        &old_key,
        vec![auth_token_manifest(
            "ops-old",
            "read-old",
            &["read", "write"],
        )],
    );

    let trusted_keys = vec![old_key.verifying_key(), new_key.verifying_key()];
    let (read_tokens, write_tokens) =
        load_auth_tokens_from_manifest(&path, &trusted_keys).expect("initial manifest load");

    let mut state = AuthState {
        read_tokens,
        write_tokens,
        source: AuthSource::Manifest {
            path: path.clone(),
            trusted_public_keys: trusted_keys,
            refresh_interval: Duration::from_millis(1),
            max_staleness: Some(Duration::from_secs(300)),
        },
        manifest_version: 1,
        manifest_reload_ok: true,
        manifest_stale_blocked: false,
        last_reload_at: Some(SystemTime::now()),
        last_reload_error: None,
        last_observed_mtime: file_modified_time(&path)
            .expect("stat manifest")
            .or(Some(SystemTime::UNIX_EPOCH)),
        last_refresh_attempt: None,
        revoked_token_hits_total: 0,
    };

    write_signed_manifest(
        &path,
        &new_key,
        vec![auth_token_manifest(
            "ops-new",
            "read-new",
            &["read", "write"],
        )],
    );

    state.maybe_refresh_manifest();

    assert!(
        state.manifest_reload_ok,
        "reload should succeed after signer rotation"
    );
    assert_eq!(
        state.manifest_version, 2,
        "successful reload must increment manifest version"
    );
    assert!(state.read_tokens.iter().any(|token| token.id == "ops-new"));
    assert!(state.write_tokens.iter().any(|token| token.id == "ops-new"));
    assert!(
        !state.manifest_stale_blocked,
        "fresh rotated manifest must not be stale-blocked"
    );
}

#[test]
fn manifest_refresh_failure_keeps_cached_tokens_until_staleness_threshold() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("admin-token-manifest.json");

    let signing_key = SigningKey::from_bytes(&[0x41; 32]);
    write_signed_manifest(
        &path,
        &signing_key,
        vec![auth_token_manifest(
            "ops-cached",
            "read-cached",
            &["read", "write"],
        )],
    );

    let trusted_keys = vec![signing_key.verifying_key()];
    let (read_tokens, write_tokens) =
        load_auth_tokens_from_manifest(&path, &trusted_keys).expect("initial manifest load");

    let mut state = AuthState {
        read_tokens,
        write_tokens,
        source: AuthSource::Manifest {
            path: path.clone(),
            trusted_public_keys: trusted_keys,
            refresh_interval: Duration::from_millis(1),
            max_staleness: Some(Duration::from_secs(300)),
        },
        manifest_version: 1,
        manifest_reload_ok: true,
        manifest_stale_blocked: false,
        last_reload_at: Some(SystemTime::now()),
        last_reload_error: None,
        last_observed_mtime: file_modified_time(&path)
            .expect("stat manifest")
            .or(Some(SystemTime::UNIX_EPOCH)),
        last_refresh_attempt: None,
        revoked_token_hits_total: 0,
    };

    std::fs::write(&path, "{ invalid json }").expect("write corrupted manifest");

    state.maybe_refresh_manifest();

    assert!(
        !state.manifest_reload_ok,
        "reload should report failure on corrupted manifest"
    );
    assert!(
        state
            .read_tokens
            .iter()
            .any(|token| token.id == "ops-cached"),
        "cached read token set should remain available after reload failure"
    );
    assert!(
        state
            .write_tokens
            .iter()
            .any(|token| token.id == "ops-cached"),
        "cached write token set should remain available after reload failure"
    );
    assert!(
        !state.manifest_stale_blocked,
        "reload failures should not immediately block auth while manifest age is within threshold"
    );
}

#[test]
fn manifest_refresh_failure_blocks_when_staleness_budget_is_exceeded() {
    let mut state = AuthState {
        read_tokens: vec![AuthToken {
            id: "cached-read".to_string(),
            token_sha256_hex: token_sha256_hex("cached-read"),
            not_before: None,
            expires_at: None,
            revoked: false,
        }],
        write_tokens: vec![AuthToken {
            id: "cached-write".to_string(),
            token_sha256_hex: token_sha256_hex("cached-write"),
            not_before: None,
            expires_at: None,
            revoked: false,
        }],
        source: AuthSource::Manifest {
            path: std::path::PathBuf::from("/tmp/non-existent-token-manifest.json"),
            trusted_public_keys: vec![],
            refresh_interval: Duration::from_millis(1),
            max_staleness: Some(Duration::from_millis(1)),
        },
        manifest_version: 1,
        manifest_reload_ok: true,
        manifest_stale_blocked: false,
        last_reload_at: Some(SystemTime::UNIX_EPOCH),
        last_reload_error: None,
        last_observed_mtime: Some(SystemTime::UNIX_EPOCH),
        last_refresh_attempt: None,
        revoked_token_hits_total: 0,
    };

    state.maybe_refresh_manifest();

    assert!(
        !state.manifest_reload_ok,
        "reload should fail when manifest cannot be read"
    );
    assert!(
        state.manifest_stale_blocked,
        "staleness policy must fail closed once age budget is exceeded"
    );
}

#[test]
fn readyz_auth_mode_can_allow_loopback_unauthenticated_probes() {
    let admin = AdminState {
        data: Arc::new(RwLock::new(AdminStateData {
            state: InstanceState::Starting,
            started_at: Utc::now(),
            first_ready_at: None,
            first_checkpoint_advanced_at: None,
            last_batch_at: None,
            events_processed: 0,
            batches_processed: 0,
            readiness_checks_total: 0,
            readiness_ready_total: 0,
            last_admin_api_latency_us: None,
            admin_api_latency_us_buckets: [0u64;
                super::ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US.len()],
            admin_api_latency_us_sum: 0.0,
            admin_api_latency_us_count: 0,
            checkpoint_age_seconds: None,
            replication_slot_lag_bytes: None,
            restart_recovery_seconds: None,
            source_consecutive_errors: 0,
            degraded_since: None,
            shutdown_requests_os_signal_total: 0,
            shutdown_completions_stopped_total: 0,
            shutdown_completions_error_total: 0,
            admin_rate_limited_readyz_total: 0,
            admin_rate_limited_status_total: 0,
            admin_rate_limited_metrics_total: 0,
            admin_rate_limiter_readyz_decisions_total: 0,
            admin_rate_limiter_readyz_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_readyz_decision_latency_seconds_max: 0.0,
            admin_rate_limiter_status_decisions_total: 0,
            admin_rate_limiter_status_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_status_decision_latency_seconds_max: 0.0,
            admin_rate_limiter_metrics_decisions_total: 0,
            admin_rate_limiter_metrics_decision_latency_seconds_sum: 0.0,
            admin_rate_limiter_metrics_decision_latency_seconds_max: 0.0,
            last_terminal_reason_code: None,
            reconciliation_recoveries_total: 0,
            reconciliation_recovery_last_unix_seconds: None,
            reconciliation_recovery_last_parse_ok: None,
            reconciliation_recovery_last_detail: None,
            reconciliation_recovery_proof_last_unix_seconds: None,
            reconciliation_recovery_proof_last_ok: None,
            reconciliation_recovery_proof_last_detail: None,
            signal_action_queue_rejections_total: 0,
            signal_action_started_notification_rejections_total: 0,
            signal_ingress_parse_rejections_total: 0,
            signal_ingress_line_too_large_rejections_total: 0,
            signal_ingress_validation_rejections_total: 0,
            signal_action_queue_depth: 0,
            notification_log_enabled: false,
            notification_kafka_enabled: false,
            notification_log_emitted_total: 0,
            notification_log_emit_failures_total: 0,
            notification_channel_emitted_total: HashMap::new(),
            notification_channel_emit_failures_total: HashMap::new(),
            audit_entries_total: 0,
            audit_recent_entries: Vec::new(),
            audit_signing_key: None,
            audit_ip_pseudonymise: false,
            audit_ip_salt: [0u8; 16],
            runtime_metrics: String::new(),
            runtime_metrics_generation: 0,
            config_json: String::new(),
        })),
        abuse_guard: Arc::new(AdminAbuseGuard::new(
            &crate::config::schema::AdminConfig::default(),
        )),
        readiness_auth_mode: AdminProbeAuthMode::AllowUnauthenticatedLoopback,
        auth_state: Arc::new(std::sync::RwLock::new(AuthState {
            read_tokens: vec![AuthToken {
                id: "read-test".to_string(),
                token_sha256_hex: token_sha256_hex("read-secret"),
                not_before: None,
                expires_at: None,
                revoked: false,
            }],
            write_tokens: vec![],
            source: AuthSource::Env,
            manifest_version: 0,
            manifest_reload_ok: true,
            manifest_stale_blocked: false,
            last_reload_at: None,
            last_reload_error: None,
            last_observed_mtime: None,
            last_refresh_attempt: None,
            revoked_token_hits_total: 0,
        })),
        audit_log: None,
        notification_log: None,
        notification_kafka: None,
        signal_action_tx: AdminState::dormant_signal_action_tx(),
        signal_inflight: Arc::new(DashMap::new()),
        workers: super::AdminWorkers::new(),
        notification_broadcast: Arc::new(
            tokio::sync::broadcast::channel(super::NOTIFICATION_STREAM_BUFFER).0,
        ),
        audit_log_drop_counter: None,
    };

    assert!(admin.authorize_readyz(&HeaderMap::new()));
}
