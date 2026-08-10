//! Tests for the admin API.
//!
//! In their own file because they were the larger half of `admin/mod.rs` — roughly 3 700
//! lines against 3 800 of implementation — and the mixture made the module hard to
//! navigate for either purpose. `super::` still resolves to `admin`, so nothing about how
//! they reach the code under test changed.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use axum::{
    body::to_bytes,
    extract::{ConnectInfo, State},
    http::{header::AUTHORIZATION, header::RETRY_AFTER, HeaderMap, HeaderValue, StatusCode},
    Json,
};
use chrono::Duration as ChronoDuration;
use chrono::Utc;
use ed25519_dalek::{Signer, SigningKey};
use krafka::consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use tempfile::tempdir;
use tokio::sync::mpsc;
use tokio::time::sleep;

use super::QueuedSignalAction;

use super::auth::AuthToken;
use super::{
    collect_control_notifications, healthz, notifications_authed, notifications_cloudevents_authed,
    notifications_stream_authed, readyz, runtime_metrics_prometheus, signal_action, slo_json,
    slo_prometheus, token_sha256_hex, AbuseLimitScope, AdminAbuseGuard, AdminState,
    AuditTrailEntry, InstanceState, SignalActionRequest, SignalActionType, SignalIngressRecord,
    SignalIngressSource,
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

pub(super) fn write_signed_manifest(
    path: &Path,
    signing_key: &SigningKey,
    tokens: Vec<TokenManifestToken>,
) {
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

pub(super) fn auth_token_manifest(id: &str, token: &str, scopes: &[&str]) -> TokenManifestToken {
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
pub(super) fn the_configured_audit_signing_key_variable_is_the_one_that_is_read() {
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

pub(super) fn sample_config() -> crate::config::AppConfig {
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

pub(super) async fn wait_for_signal_state(
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
pub(super) async fn wait_for_signal_queue_drained(admin: &AdminState) {
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
pub(super) async fn wait_for_file_lines(
    path: &std::path::Path,
    expected_lines: usize,
) -> Vec<String> {
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

pub(super) fn test_suffix() -> String {
    format!(
        "{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    )
}

pub(super) fn append_signal_ingress_line(path: &Path, payload: &str) {
    use std::io::Write as _;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open signal ingress file");
    writeln!(file, "{payload}").expect("append signal ingress line");
}

pub(super) async fn wait_for_signal_ingress_rejection_counters(
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

pub(super) fn kafka_auth_from_env() -> krafka::auth::AuthConfig {
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

pub(super) async fn consume_notification_states_until_seen(
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

pub(super) fn configure_test_read_write_tokens(admin: &AdminState) {
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
pub(super) async fn admin_workers_stop_when_asked() {
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
pub(super) async fn audit_entries_are_persisted_when_admin_audit_log_file_is_configured() {
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
pub(super) async fn shutdown_audit_entry_persists_actor_metadata() {
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
pub(super) async fn admin_state_new_rejects_audit_log_file_that_points_to_directory() {
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
pub(super) async fn healthz_is_unauthenticated_liveness() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    let response = healthz(State(admin), HeaderMap::new()).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
pub(super) async fn readyz_returns_ok_while_stopping_when_probe_auth_allows_loopback() {
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

/// Drive a signal action through the write API so a real notification is produced by
/// the same path production uses, rather than hand-inserting an audit entry.
pub(super) async fn raise_test_signal(admin: &AdminState, signal_id: &str, correlation_id: &str) {
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
            tables: None,
            conditions: None,
            message: Some("checkpoint marker".to_string()),
            additional_data: Some(serde_json::json!({"operator": "unit-test"})),
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

pub(super) async fn open_notification_stream(
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
pub(super) async fn read_sse_frames(
    response: axum::response::Response,
    min_events: usize,
) -> String {
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

/// `/config` requires a read token, exactly like `/status`.
///
/// The endpoint serves a configuration document: hosts, topics, table lists, file paths.
/// Redaction removes the credentials, not the topology.
#[tokio::test]
pub(super) async fn the_config_endpoint_requires_a_read_token() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let unauthenticated = super::config_authed(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9012))),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let mut read_headers = HeaderMap::new();
    read_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer read-secret"),
    );
    let authorized = super::config_authed(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9013))),
        read_headers,
    )
    .await;
    assert_eq!(authorized.status(), StatusCode::OK);

    let body = to_bytes(authorized.into_body(), usize::MAX)
        .await
        .expect("read body");
    let rendered: serde_json::Value =
        serde_json::from_slice(&body).expect("the endpoint must serve parseable JSON");
    assert!(
        rendered.get("source").is_some(),
        "the snapshot must be the actual configuration: {rendered}"
    );
}

/// A panicking handler answers `500`, and does not leak the panic payload.
///
/// Without the guard a panic unwinds into tokio, which aborts the task: the client gets a
/// connection reset with no status and no body, and nothing reaches the log — so the
/// failure is indistinguishable from a network fault and gets diagnosed as one. This is
/// not hypothetical; see `a_multi_byte_message_across_the_audit_budget_does_not_panic`
/// for the defect that reached this path from outside.
///
/// The guard is exercised through `panic_guard()` — the same constructor `router()`
/// applies — rather than through a route that exists in production, because none of them
/// should ever panic.
#[tokio::test]
pub(super) async fn a_panicking_admin_handler_answers_500_without_leaking_the_payload() {
    use axum::routing::get;
    use tower::ServiceExt as _;

    let app = axum::Router::new()
        .route(
            "/boom",
            get(|| async {
                panic!("secret-bearing panic detail: /etc/rustcdc/key.pem");
                #[allow(unreachable_code)]
                StatusCode::OK
            }),
        )
        .layer(super::panic_guard());

    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/boom")
                .body(axum::body::Body::empty())
                .expect("request"),
        )
        .await
        .expect("the guard must answer rather than propagate the panic");

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let rendered = String::from_utf8_lossy(&body);
    assert!(
        !rendered.contains("key.pem"),
        "a panic payload can carry paths and internal state; it must not reach the \
         caller: {rendered}"
    );
    assert!(
        rendered.contains("internal server error"),
        "the caller still needs a usable error body: {rendered}"
    );
}

/// A signal whose message straddles the audit-detail byte budget must not panic.
///
/// `append_audit_entry` capped the detail with `detail[..AUDIT_DETAIL_MAX_BYTES]`, a byte
/// slice. The detail embeds the caller's `message` verbatim and `serde_json` does not
/// escape non-ASCII, so any write-scope token could place a multi-byte character across
/// byte 4096 and panic the task doing the append.
///
/// That is worse than a failed request. The same lifecycle entries are appended by the
/// signal-action worker, and a panic there kills the worker for the life of the process —
/// after which every asynchronous signal is accepted, answered `STARTED`, and never
/// processed, with nothing in the metrics to say so.
///
/// The message is built so the boundary lands mid-character for *some* offset in the
/// window, whatever the surrounding JSON contributes.
#[tokio::test]
pub(super) async fn a_multi_byte_message_across_the_audit_budget_does_not_panic() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    // Three-byte characters: every byte offset that is not a multiple of three splits one.
    // Sweeping a small window guarantees at least one request whose budget lands inside a
    // character regardless of the fixed JSON around it.
    for padding in 0..6usize {
        let message = format!(
            "{}{}",
            "x".repeat(padding),
            "世".repeat(super::AUDIT_DETAIL_MAX_BYTES)
        );

        let response = signal_action(
            State(admin.clone()),
            ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9010))),
            write_headers.clone(),
            Json(SignalActionRequest {
                signal_id: Some(format!("sig-wide-{padding}")),
                correlation_id: None,
                action_type: SignalActionType::LogMarker,
                tables: None,
                conditions: None,
                message: Some(message),
                additional_data: None,
            }),
        )
        .await;

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "an oversized multi-byte message must be truncated, not fatal"
        );
    }

    // The entries are still well-formed and bounded — truncation happened, and what it
    // produced is still valid UTF-8 by construction (it is a `String`).
    let data = admin.data.read().await;
    // Two per signal: `log_marker` is synchronous, so STARTED and the terminal entry are
    // both appended inline.
    assert_eq!(data.audit_entries_total, 12);
    for entry in &data.audit_recent_entries {
        assert!(
            entry.detail.len() <= super::AUDIT_DETAIL_MAX_BYTES + " [truncated]".len(),
            "detail exceeded its budget: {} bytes",
            entry.detail.len()
        );
    }
}

#[tokio::test]
pub(super) async fn admin_state_stores_redacted_config_json() {
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
pub(super) fn endpoint_rate_limiter_is_per_client_and_burst_bounded() {
    let limiter = EndpointRateLimiter::new(1, 1);

    assert!(limiter.allow("client-a"));
    assert!(!limiter.allow("client-a"));
    assert!(limiter.allow("client-b"));
}

#[test]
pub(super) fn abuse_guard_uses_peer_ip_when_proxy_not_trusted() {
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
pub(super) fn abuse_guard_honors_forwarded_ip_from_trusted_proxy() {
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
pub(super) fn alert_rules_file_contains_expected_slo_gates() {
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

// ─────────────────────────────────────────────────────────────────────────────
// Kafka signal ingress: commit after the action, not before it
// ─────────────────────────────────────────────────────────────────────────────

pub(super) fn kafka_signal_record(
    topic: &str,
    partition: i32,
    offset: i64,
    payload: &str,
) -> ConsumerRecord {
    ConsumerRecord::new(
        topic,
        partition,
        offset,
        None,
        Some(bytes::Bytes::copy_from_slice(payload.as_bytes())),
    )
}

/// An unparseable record is settled, not retried forever.
///
/// A malformed payload will be just as malformed on redelivery. Holding the offset for it
/// would wedge the channel behind a record that can never succeed.
#[tokio::test]
pub(super) async fn an_unparseable_signal_record_still_lets_the_offset_advance() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    let outcome = admin
        .ingest_kafka_signal_batch(vec![kafka_signal_record(
            "cdc.signals",
            0,
            1,
            "{ not json at all",
        )])
        .await;
    admin.shutdown_workers().await;

    assert_eq!(outcome.ingested, 0, "nothing was dispatched");
    assert!(
        outcome.commit,
        "a permanently invalid record must not block the ingress channel"
    );
}

/// An action still in flight past its budget must hold the offset **and** stay unledgered.
///
/// This is the ordering that separates the fix from the defect. If the ledger entry were
/// written when the record was dispatched rather than when its action settled, this record
/// would be marked processed while still running — and a crash here would leave it
/// committed-in-the-ledger and never executed, which is the original at-most-once loss with
/// extra steps.
///
/// The stall is constructed rather than waited for: a `STARTED` lifecycle entry with no
/// terminal entry is exactly what an in-flight action looks like to
/// `latest_signal_action_state`, and the in-flight guard makes the dispatch return
/// `ExistingState` instead of starting a second one.
#[tokio::test]
pub(super) async fn an_action_still_running_holds_the_offset_and_is_not_ledgered() {
    let cfg = sample_config();
    let mut admin = AdminState::new(&cfg).await.expect("admin state");
    // Milliseconds instead of the 30 s production budget; the property is the same.
    admin.signal_terminal_budget = Duration::from_millis(80);

    let signal_id = "sig-still-running";
    // Make it look in-flight: a STARTED entry and no terminal one, plus the guard entry
    // that stops a second dispatch.
    admin
        .append_signal_lifecycle_entry(
            signal_id,
            signal_id,
            SignalActionType::ExecuteSnapshot,
            "signal_execute_snapshot_started",
            "accepted",
            "STARTED",
            "stalled",
            None,
            serde_json::Value::Null,
            None,
            None,
        )
        .await;
    admin
        .signal_inflight
        .insert((signal_id.to_string(), "execute_snapshot".to_string()), ());

    let payload = serde_json::json!({
        "signal_id": signal_id,
        "action_type": "execute_snapshot",
        "tables": ["public.orders"],
    })
    .to_string();

    let outcome = admin
        .ingest_kafka_signal_batch(vec![kafka_signal_record("cdc.signals", 0, 99, &payload)])
        .await;
    admin.shutdown_workers().await;

    assert!(
        !outcome.commit,
        "an action that has not reported a terminal state must hold the ingress offset — \
         committing past it is how the command gets lost"
    );
    assert!(
        !admin.signal_ingress_already_processed(&super::signal_ledger::kafka_ingress_key(
            "cdc.signals",
            0,
            99
        )),
        "an unfinished action must not be recorded as processed; a restart has to redeliver \
         it, and a ledger entry would make the restart skip it"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// execute_snapshot row filters (rustcdc 0.12, F-8)
// ─────────────────────────────────────────────────────────────────────────────

/// A `conditions` entry naming a table the request does not snapshot is rejected.
///
/// The failure it prevents is silent and in the dangerous direction: the filter never
/// applies, so the backfill reads the *whole* table while the operator believes it is
/// scoped to one tenant. The only symptom is volume, which is indistinguishable from a
/// large table. A typo in the key is the usual cause, and here the two lists arrive in the
/// same request, so a mismatch is unambiguous rather than a guess.
#[test]
pub(super) fn a_condition_for_a_table_outside_the_request_is_rejected() {
    let tables = vec!["public.orders".to_string()];
    let mut conditions = std::collections::BTreeMap::new();
    conditions.insert("public.ordres".to_string(), "tenant_id = 42".to_string());

    let error = super::validate_signal_conditions(
        SignalActionType::ExecuteSnapshot,
        &tables,
        Some(conditions),
    )
    .expect_err("a condition outside the request's tables must be rejected");

    assert!(
        error.contains("public.ordres") && error.contains("whole table"),
        "the error must name the key and say what goes wrong: {error}"
    );
}

/// A blank filter is rejected rather than treated as "no filter".
#[test]
pub(super) fn a_blank_condition_is_rejected() {
    let tables = vec!["public.orders".to_string()];
    let mut conditions = std::collections::BTreeMap::new();
    conditions.insert("public.orders".to_string(), "   ".to_string());

    let error = super::validate_signal_conditions(
        SignalActionType::ExecuteSnapshot,
        &tables,
        Some(conditions),
    )
    .expect_err("a blank filter must be rejected");
    assert!(error.contains("empty"), "unexpected: {error}");
}

/// Conditions are only meaningful for `execute_snapshot`.
#[test]
pub(super) fn conditions_on_a_non_snapshot_action_are_rejected() {
    let mut conditions = std::collections::BTreeMap::new();
    conditions.insert("public.orders".to_string(), "tenant_id = 42".to_string());

    let error =
        super::validate_signal_conditions(SignalActionType::LogMarker, &[], Some(conditions))
            .expect_err("conditions must not be silently ignored on another action");
    assert!(error.contains("log_marker"), "unexpected: {error}");
}

/// A valid request keeps its filters, trimmed.
#[test]
pub(super) fn a_valid_condition_survives_validation() {
    let tables = vec!["public.orders".to_string()];
    let mut conditions = std::collections::BTreeMap::new();
    conditions.insert(
        "  public.orders  ".to_string(),
        "  tenant_id = 42  ".to_string(),
    );

    let normalized = super::validate_signal_conditions(
        SignalActionType::ExecuteSnapshot,
        &tables,
        Some(conditions),
    )
    .expect("a matching condition must be accepted");

    assert_eq!(
        normalized.get("public.orders").map(String::as_str),
        Some("tenant_id = 42"),
        "the SQL must reach the runtime trimmed but otherwise intact"
    );
}

/// `POST /signals` accepts the Debezium-shaped payload end to end.
///
/// `deny_unknown_fields` is on both wire structs, so an unrecognised `conditions` key
/// would be a 400 rather than a silently dropped filter — which is exactly how `tables`
/// used to be lost over HTTP while the file and Kafka paths accepted it.
#[tokio::test]
pub(super) async fn the_signals_endpoint_accepts_a_row_filter() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);
    admin.advertise_snapshot_capability().await;

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let mut conditions = std::collections::BTreeMap::new();
    conditions.insert("public.orders".to_string(), "tenant_id = 42".to_string());

    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9010))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-filtered-1".to_string()),
            correlation_id: None,
            action_type: SignalActionType::ExecuteSnapshot,
            tables: Some(vec!["public.orders".to_string()]),
            conditions: Some(conditions),
            message: None,
            additional_data: None,
        }),
    )
    .await;

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a filtered execute_snapshot must be accepted"
    );
    admin.shutdown_workers().await;
}

/// …and rejects a filter that names a table outside the request.
#[tokio::test]
pub(super) async fn the_signals_endpoint_rejects_a_mismatched_row_filter() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);
    admin.advertise_snapshot_capability().await;

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let mut conditions = std::collections::BTreeMap::new();
    conditions.insert("public.customers".to_string(), "tenant_id = 42".to_string());

    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9011))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-filtered-2".to_string()),
            correlation_id: None,
            action_type: SignalActionType::ExecuteSnapshot,
            tables: Some(vec!["public.orders".to_string()]),
            conditions: Some(conditions),
            message: None,
            additional_data: None,
        }),
    )
    .await;

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "a filter that could never apply must be refused, not accepted and ignored"
    );
    admin.shutdown_workers().await;
}

/// `GET /openapi.json` serves the document, without a credential.
///
/// Unauthenticated on purpose: it describes the shape of the API, not its state, and
/// requiring a token to discover how to authenticate is a loop. The assertion that it
/// carries no configuration is the one that matters — this endpoint is reachable by
/// anything that can reach the port.
#[tokio::test]
pub(super) async fn the_openapi_document_is_served_unauthenticated() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");

    let response = super::openapi_document(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9020))),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("body");
    let doc: serde_json::Value = serde_json::from_slice(&body).expect("valid JSON");

    assert_eq!(doc["openapi"], "3.1.0");
    assert_eq!(
        doc["info"]["version"],
        env!("CARGO_PKG_VERSION"),
        "a generated client must be able to tell which build it was made against"
    );
    assert!(doc["paths"]["/signals"]["post"].is_object());

    // Nothing about this instance beyond the version: no bind address, no token ids, no
    // source host. `/config` is the endpoint for configuration, and it requires a token.
    let rendered = String::from_utf8_lossy(&body);
    for leak in ["rustcdc_user", "cdc_slot", "mydb", "password"] {
        assert!(
            !rendered.contains(leak),
            "the unauthenticated document must not carry configuration: found {leak:?}"
        );
    }

    admin.shutdown_workers().await;
}

/// The document must be built once, not per request.
///
/// It depends only on the crate version, so rebuilding a `json!` tree and re-serialising
/// ~10 KB on every call was pure waste — and on an unauthenticated route it let a caller
/// choose how much work the process did. `Bytes` makes the response body a refcount bump
/// rather than a copy, so this asserts pointer identity: two calls must hand back the same
/// buffer, not two equal ones.
#[tokio::test]
pub(super) async fn the_openapi_document_is_built_once_and_shared() {
    let first = super::openapi::cached_document();
    let second = super::openapi::cached_document();

    assert_eq!(first, second, "the cached document must be stable");
    assert!(
        first.as_ptr() == second.as_ptr(),
        "each call must share one buffer; a fresh allocation per request is the cost this \
         cache exists to remove"
    );
}

/// `/openapi.json` must be rate-limited, like every other endpoint that does real work.
///
/// It is deliberately unauthenticated — it describes the shape of the API, not its state —
/// but unauthenticated and unmetered are different things, and it was briefly both.
#[tokio::test]
pub(super) async fn the_openapi_endpoint_is_rate_limited() {
    let mut cfg = sample_config();
    cfg.admin.status_rate_limit_rps = 1;
    cfg.admin.status_rate_limit_burst = 1;
    let admin = AdminState::new(&cfg).await.expect("admin state");

    let peer = ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9021)));
    let mut saw_limited = false;
    for _ in 0..8 {
        let response = super::openapi_document(State(admin.clone()), peer, HeaderMap::new()).await;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            saw_limited = true;
            break;
        }
    }
    admin.shutdown_workers().await;

    assert!(
        saw_limited,
        "an unauthenticated endpoint serving ~10 KB must be metered; without a limiter the \
         cost of a request is the caller's to choose"
    );
}

/// A panicking signal action must not take the control plane with it.
///
/// Before this, a panic anywhere in `run_signal_action` terminated the worker task for the
/// process lifetime. The pipeline kept capturing, `/status` kept reporting
/// `snapshot_requests_available: true`, and `POST /signals` kept answering `STARTED` —
/// while every subsequent action was queued and silently never executed. The SLO caught it
/// 30 s later by a different name, and nothing recovered short of a restart.
///
/// Three properties, and all three matter:
///
/// 1. the worker survives and processes the *next* action;
/// 2. the panicking signal's in-flight guard is released, or that
///    `(signal_id, action_type)` is permanently unusable — `execute_signal_action_envelope`
///    reads a present entry as "already running" and refuses every retry;
/// 3. the panic is counted, so a recovered bug is visible rather than invisible.
#[tokio::test]
pub(super) async fn a_panicking_signal_action_does_not_kill_the_worker() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    // `pause_snapshot` rather than `log_marker`: only async actions reach the worker.
    // `signal_action_requires_async_worker` excludes `log_marker`, which completes inline
    // in `execute_signal_action_envelope` and would never exercise the recovery path.
    admin
        .process_source_signal_ingress_payload(
            serde_json::json!({
                "signal_id": "sig-panics",
                "action_type": "pause_snapshot",
                "message": super::PANIC_PROBE_MESSAGE,
            })
            .to_string()
            .as_bytes(),
        )
        .await;

    // The panic is recovered asynchronously; wait for the counter rather than sleeping.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if admin.data.read().await.signal_worker_panics_total > 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the panic was never recorded; the worker probably died instead of recovering"
        );
        sleep(Duration::from_millis(25)).await;
    }

    assert!(
        !admin
            .signal_inflight
            .contains_key(&("sig-panics".to_string(), "pause_snapshot".to_string())),
        "the in-flight guard must be released, or this signal id can never be retried"
    );

    assert!(
        admin.signal_worker_alive(),
        "the worker must still be running after a recovered panic"
    );

    // And it must still do work: a following action reaches a terminal state.
    admin
        .process_source_signal_ingress_payload(
            serde_json::json!({
                "signal_id": "sig-after-panic",
                "action_type": "log_marker",
                "message": "still alive",
            })
            .to_string()
            .as_bytes(),
        )
        .await;
    wait_for_signal_state(&admin, "sig-after-panic", "log_marker", "COMPLETED").await;

    let metrics = super::slo_prometheus(&*admin.data.read().await, admin.signal_worker_alive());
    assert!(
        metrics.contains("rustcdc_admin_signal_worker_alive 1"),
        "liveness must be exported: a dead worker is otherwise only inferable from a \
         different metric 30 seconds later"
    );
    assert!(
        metrics.contains("rustcdc_admin_signal_worker_panics_total 1"),
        "a recovered panic must be counted, not silently swallowed"
    );

    admin.shutdown_workers().await;
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

    let metrics = slo_prometheus(&data, true);
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

    let metrics = slo_prometheus(&data, true);
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
            tables: Some(vec!["public.orders".to_string()]),
            conditions: None,
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

    let metrics = slo_prometheus(&data, true);
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
            tables: Some(vec!["public.orders".to_string()]),
            conditions: None,
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
        &["STARTED", "IN_PROGRESS", "ABORTED"],
        60,
    )
    .await;

    assert!(
        ["STARTED", "IN_PROGRESS", "ABORTED"]
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
        "tables": ["public.orders"],
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

    wait_for_signal_state(&admin, &signal_id, "execute_snapshot", "ABORTED").await;

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    assert!(notifications.iter().any(|notification| {
        notification.signal_id == signal_id
            && notification.action_type == "execute_snapshot"
            && notification.state == "ABORTED"
    }));
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
            tables: None,
            conditions: None,
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
            tables: None,
            conditions: None,
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
            tables: Some(vec!["public.orders".to_string()]),
            conditions: None,
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
                    "tables": ["public.orders"],
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

    let metrics = slo_prometheus(&data, true);
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

    let metrics = slo_prometheus(&data, true);
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
                    "tables": ["public.orders"],
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
                    "tables": ["public.orders"],
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

    let metrics = slo_prometheus(&data, true);
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
    // Advertises the capability without a runtime. `RuntimeControl` is not constructible
    // outside rustcdc, so these tests assert the signal **lifecycle** — STARTED, terminal
    // state, notifications, idempotency — which is identical whether the terminal state is
    // COMPLETED or ABORTED. The success path is covered end to end, against a real runtime,
    // by `tests/integration_postgres.rs`.
    admin.advertise_snapshot_capability().await;
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
            tables: Some(vec!["public.orders".to_string()]),
            conditions: None,
            message: None,
            additional_data: Some(serde_json::json!({
                "data_collections": ["public.accounts", "public.orders"],
                "snapshot_mode": "incremental"
            })),
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    wait_for_signal_state(&admin, "sig-exec-1", "execute_snapshot", "ABORTED").await;
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
    assert!(execute_notifications.iter().any(|n| n.state == "ABORTED"));
    assert!(execute_notifications
        .iter()
        .all(|n| n.signal_id == "sig-exec-1" && n.correlation_id == "corr-exec-1"));
}

/// Snapshot progress renders as gauges, and is absent when nothing is running.
///
/// The absence matters as much as the presence: a gauge reporting `0` while no backfill
/// exists is indistinguishable from one reporting a stalled backfill, so `absent()` is the
/// honest PromQL for "no snapshot" and these series must not be emitted at all.
#[test]
fn snapshot_progress_renders_only_while_a_snapshot_is_in_flight() {
    use rustcdc::{IncrementalSnapshotState, IncrementalSnapshotTableState};

    assert_eq!(
        super::snapshot_progress_prometheus(None),
        "",
        "with no snapshot in flight nothing is emitted"
    );

    let state = IncrementalSnapshotState {
        snapshot_id: "snap-1".to_string(),
        paused: true,
        stopped: false,
        generation: 1,
        tables: vec![
            IncrementalSnapshotTableState {
                table: "public.orders".to_string(),
                rows_emitted: 1200,
                is_complete: false,
                ..Default::default()
            },
            IncrementalSnapshotTableState {
                table: "public.customers".to_string(),
                rows_emitted: 90,
                is_complete: true,
                ..Default::default()
            },
        ],
    };

    let rendered = super::snapshot_progress_prometheus(Some(state));
    assert!(rendered.contains("rustcdc_incremental_snapshot_active 1"));
    assert!(rendered.contains("rustcdc_incremental_snapshot_paused 1"));
    assert!(rendered.contains("rustcdc_incremental_snapshot_tables_remaining 1"));
    assert!(rendered.contains("rustcdc_incremental_snapshot_rows_emitted 1290"));
    assert!(rendered
        .contains("rustcdc_incremental_snapshot_table_rows_emitted{table=\"public.orders\"} 1200"));
    assert!(rendered
        .contains("rustcdc_incremental_snapshot_table_complete{table=\"public.customers\"} 1"));
}

/// A table name containing a quote must not break the exposition format.
#[test]
fn snapshot_progress_escapes_label_values() {
    use rustcdc::{IncrementalSnapshotState, IncrementalSnapshotTableState};

    let state = IncrementalSnapshotState {
        snapshot_id: "snap-1".to_string(),
        paused: false,
        stopped: false,
        generation: 1,
        tables: vec![IncrementalSnapshotTableState {
            table: "public.we\"ird".to_string(),
            rows_emitted: 1,
            ..Default::default()
        }],
    };

    let rendered = super::snapshot_progress_prometheus(Some(state));
    assert!(
        rendered.contains(r#"table="public.we\"ird""#),
        "an unescaped quote would terminate the label and corrupt the scrape: {rendered}"
    );
}

/// Snapshot pause / resume / stop are dispatched to the runtime, not acknowledged blindly.
///
/// This assertion has been rewritten twice, and the history is the point. Originally these
/// answered `200 OK` and recorded `PAUSED` / `RESUMED` / `ABORTED` while doing **nothing** —
/// an operator pausing a snapshot to relieve a primary would have watched it keep running
/// with a green audit trail. We then refused them with `501`, because rustcdc 0.10 had no
/// such control and an honest refusal beats a false success. rustcdc 0.11 added
/// `pause_incremental_snapshot` / `resume` / `stop`, so they are now real.
///
/// With no runtime attached the terminal state is `ABORTED` naming the missing
/// configuration — which is the same path `execute_snapshot` takes, and is what
/// distinguishes "dispatched and refused" from "acknowledged and ignored". The success path
/// runs against a real runtime in `tests/integration_postgres.rs`.
#[tokio::test]
async fn snapshot_controls_are_dispatched_to_the_runtime() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    for (signal_id, action_type) in [
        ("sig-pause-1", SignalActionType::PauseSnapshot),
        ("sig-resume-1", SignalActionType::ResumeSnapshot),
        ("sig-stop-1", SignalActionType::StopSnapshot),
    ] {
        let response = signal_action(
            State(admin.clone()),
            ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9010))),
            write_headers.clone(),
            Json(SignalActionRequest {
                signal_id: Some(signal_id.to_string()),
                correlation_id: Some(format!("corr-{signal_id}")),
                action_type,
                tables: None,
                conditions: None,
                message: None,
                additional_data: Some(serde_json::json!({"operator": "test"})),
            }),
        )
        .await;

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "the control must be accepted and dispatched, not refused at the API"
        );

        wait_for_signal_state(&admin, signal_id, action_type.as_str(), "ABORTED").await;

        let data = admin.data.read().await;
        let terminal = data
            .audit_recent_entries
            .iter()
            .rev()
            .find(|entry| entry.detail.contains(signal_id) && entry.detail.contains("ABORTED"))
            .expect("a terminal entry");
        assert!(
            terminal.detail.contains("incremental_snapshot"),
            "the refusal must name the missing configuration rather than claiming success: {}",
            terminal.detail
        );
        drop(data);
    }
}

/// `execute_snapshot` with no usable table names has nothing to snapshot.
///
/// Accepting it produced a `COMPLETED` record for a no-op, which is the same failure shape
/// as the acknowledged-but-unimplemented controls above.
#[tokio::test]
async fn execute_snapshot_without_tables_is_rejected() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);
    admin.advertise_snapshot_capability().await;

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
            signal_id: Some("sig-empty-tables".to_string()),
            correlation_id: None,
            action_type: SignalActionType::ExecuteSnapshot,
            tables: Some(vec!["   ".to_string()]),
            conditions: None,
            message: None,
            additional_data: None,
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// Without an executor the signal aborts and says why.
///
/// This is the no-`[incremental_snapshot]` case: there is no snapshot for
/// the requested tables to join, and the terminal state has to report that rather than
/// `COMPLETED`.
#[tokio::test]
async fn execute_snapshot_without_a_pipeline_executor_aborts() {
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
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9010))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-no-executor".to_string()),
            correlation_id: None,
            action_type: SignalActionType::ExecuteSnapshot,
            tables: Some(vec!["public.orders".to_string()]),
            conditions: None,
            message: None,
            additional_data: None,
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK, "the signal is accepted");

    wait_for_signal_state(&admin, "sig-no-executor", "execute_snapshot", "ABORTED").await;

    let data = admin.data.read().await;
    let terminal = data
        .audit_recent_entries
        .iter()
        .rev()
        .find(|entry| entry.detail.contains("sig-no-executor") && entry.detail.contains("ABORTED"))
        .expect("an aborted terminal entry");
    assert!(
        terminal.detail.contains("incremental_snapshot"),
        "the failure must name the missing configuration: {}",
        terminal.detail
    );
}

/// The snapshot counters move, so the alert rule built on them can fire.
///
/// The audit trail is bounded to 512 in-memory entries, so it is not a durable record of
/// how a pipeline has been driven. Without these counters a scrape cannot distinguish a
/// pipeline that has been asked to backfill four tables from one that has been asked and
/// refused every time — and `RUSTCDCSnapshotRequestsRefused` would never fire, which reads
/// as health.
#[tokio::test]
async fn snapshot_requests_are_counted_for_metrics_and_status() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let request = |signal_id: &str| SignalActionRequest {
        signal_id: Some(signal_id.to_string()),
        correlation_id: None,
        action_type: SignalActionType::ExecuteSnapshot,
        tables: Some(vec![
            "public.orders".to_string(),
            "public.customers".to_string(),
        ]),
        conditions: None,
        message: None,
        additional_data: None,
    };

    // Both requests are refused: `RuntimeControl` is not constructible outside rustcdc, so
    // a unit test cannot produce an accepted one. What it *can* pin is that every outcome
    // is counted — a refusal that is not counted looks exactly like no request at all, and
    // `RUSTCDCSnapshotRequestsRefused` would never fire. The accepted counters are asserted
    // against a real runtime in `tests/integration_postgres.rs`.
    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9010))),
        write_headers.clone(),
        Json(request("sig-count-refused")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    wait_for_signal_state(&admin, "sig-count-refused", "execute_snapshot", "ABORTED").await;

    admin.advertise_snapshot_capability().await;
    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9010))),
        write_headers,
        Json(request("sig-count-accepted")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    wait_for_signal_state(&admin, "sig-count-accepted", "execute_snapshot", "ABORTED").await;

    let data = admin.data.read().await;
    assert_eq!(
        data.snapshot_requests_refused_total, 2,
        "every refusal must be counted"
    );
    assert_eq!(data.snapshot_requests_accepted_total, 0);

    let metrics = slo_prometheus(&data, true);
    assert!(
        metrics.contains("rustcdc_snapshot_requests_available 1"),
        "the capability gauge must report that this pipeline advertises the capability"
    );
    assert!(metrics.contains("rustcdc_snapshot_requests_refused_total 2"));

    let slo = slo_json(&data);
    assert_eq!(slo["snapshot_requests"]["refused_total"].as_u64(), Some(2));
    assert_eq!(
        slo["snapshot_requests"]["available"].as_bool(),
        Some(true),
        "an operator must be able to see the capability before firing a signal"
    );
}

/// The outcome — whatever it is — reaches the audit trail alongside the caller's own data.
///
/// Without that an operator can see a snapshot was requested but not what happened to it.
/// With no runtime attached the outcome is the refusal, and the assertion is that the
/// *cause* survives the merge into `additional_data` rather than being flattened away. The
/// success shape (`tables_enqueued`) is asserted against a real runtime in
/// `tests/integration_postgres.rs`.
#[tokio::test]
async fn an_execute_snapshot_outcome_reaches_the_terminal_record() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);
    admin.advertise_snapshot_capability().await;

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
            signal_id: Some("sig-enqueued".to_string()),
            correlation_id: None,
            action_type: SignalActionType::ExecuteSnapshot,
            tables: Some(vec![
                "public.orders".to_string(),
                "public.customers".to_string(),
            ]),
            conditions: None,
            message: None,
            additional_data: Some(serde_json::json!({"operator": "unit-test"})),
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    wait_for_signal_state(&admin, "sig-enqueued", "execute_snapshot", "ABORTED").await;

    let data = admin.data.read().await;
    let terminal = data
        .audit_recent_entries
        .iter()
        .rev()
        .find(|entry| entry.detail.contains("sig-enqueued") && entry.detail.contains("ABORTED"))
        .expect("a terminal entry");
    assert!(
        terminal.detail.contains("incremental_snapshot"),
        "the outcome must reach the terminal record and name the cause: {}",
        terminal.detail
    );
    assert!(
        terminal.detail.contains("unit-test"),
        "the caller's own additional_data must survive the merge: {}",
        terminal.detail
    );
}

#[tokio::test]
async fn signal_lifecycle_conformance_matrix_for_http_file_kafka_and_source_ingress() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    // Advertises the capability without a runtime. `RuntimeControl` is not constructible
    // outside rustcdc, so these tests assert the signal **lifecycle** — STARTED, terminal
    // state, notifications, idempotency — which is identical whether the terminal state is
    // COMPLETED or ABORTED. The success path is covered end to end, against a real runtime,
    // by `tests/integration_postgres.rs`.
    admin.advertise_snapshot_capability().await;
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
            tables: Some(vec!["public.orders".to_string()]),
            conditions: None,
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
            tables: Some(vec!["public.orders".to_string()]),
            conditions: Default::default(),
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
            tables: None,
            conditions: Default::default(),
            message: Some("matrix file log".to_string()),
            additional_data: Some(serde_json::json!({"ingress": "file"})),
            traceparent: None,
        })
        .await;

    let kafka_exec_payload = serde_json::to_vec(&serde_json::json!({
        "signal_id": "sig-matrix-kafka-exec-1",
        "correlation_id": "corr-matrix-kafka-exec-1",
        "action_type": "execute_snapshot",
        "tables": ["public.orders"],
        "message": "matrix kafka execute",
        "additional_data": {"ingress": "kafka"},
    }))
    .expect("serialize kafka matrix execute payload");
    assert!(admin
        .process_signal_ingress_payload(kafka_exec_payload.as_slice(), SignalIngressSource::Kafka,)
        .await
        .is_some());

    let kafka_log_payload = serde_json::to_vec(&serde_json::json!({
        "signal_id": "sig-matrix-kafka-log-1",
        "correlation_id": "corr-matrix-kafka-log-1",
        "action_type": "log_marker",
        "message": "matrix kafka log",
        "additional_data": {"ingress": "kafka"},
    }))
    .expect("serialize kafka matrix log payload");
    assert!(admin
        .process_signal_ingress_payload(kafka_log_payload.as_slice(), SignalIngressSource::Kafka,)
        .await
        .is_some());

    let source_exec_payload = serde_json::to_vec(&serde_json::json!({
        "signal_id": "sig-matrix-source-exec-1",
        "correlation_id": "corr-matrix-source-exec-1",
        "action_type": "execute_snapshot",
        "tables": ["public.orders"],
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

    wait_for_signal_state(&admin, "sig-matrix-http-1", "execute_snapshot", "ABORTED").await;
    wait_for_signal_state(
        &admin,
        "sig-matrix-file-exec-1",
        "execute_snapshot",
        "ABORTED",
    )
    .await;
    wait_for_signal_state(&admin, "sig-matrix-file-log-1", "log_marker", "COMPLETED").await;
    wait_for_signal_state(
        &admin,
        "sig-matrix-kafka-exec-1",
        "execute_snapshot",
        "ABORTED",
    )
    .await;
    wait_for_signal_state(&admin, "sig-matrix-kafka-log-1", "log_marker", "COMPLETED").await;
    wait_for_signal_state(
        &admin,
        "sig-matrix-source-exec-1",
        "execute_snapshot",
        "ABORTED",
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
    assert!(http_exec.iter().any(|n| n.state == "ABORTED"));
    assert_eq!(
        http_exec.iter().filter(|n| n.state == "ABORTED").count(),
        1,
        "http execute must emit exactly one terminal notification"
    );

    let file_exec: Vec<_> = notifications
        .iter()
        .filter(|n| n.signal_id == "sig-matrix-file-exec-1" && n.action_type == "execute_snapshot")
        .collect();
    assert!(file_exec.iter().any(|n| n.state == "STARTED"));
    assert!(file_exec.iter().any(|n| n.state == "IN_PROGRESS"));
    assert!(file_exec.iter().any(|n| n.state == "ABORTED"));
    assert_eq!(
        file_exec.iter().filter(|n| n.state == "ABORTED").count(),
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
    assert!(kafka_exec.iter().any(|n| n.state == "ABORTED"));
    assert_eq!(
        kafka_exec.iter().filter(|n| n.state == "ABORTED").count(),
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
    assert!(source_exec.iter().any(|n| n.state == "ABORTED"));
    assert_eq!(
        source_exec.iter().filter(|n| n.state == "ABORTED").count(),
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
            tables: None,
            conditions: None,
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
    // Advertises the capability without a runtime. `RuntimeControl` is not constructible
    // outside rustcdc, so these tests assert the signal **lifecycle** — STARTED, terminal
    // state, notifications, idempotency — which is identical whether the terminal state is
    // COMPLETED or ABORTED. The success path is covered end to end, against a real runtime,
    // by `tests/integration_postgres.rs`.
    admin.advertise_snapshot_capability().await;
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
        tables: Some(vec!["public.orders".to_string()]),
        conditions: None,
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

    wait_for_signal_state(&admin, "sig-idempotent-1", "execute_snapshot", "ABORTED").await;
    sleep(Duration::from_millis(500)).await;

    let second = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9005))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some("sig-idempotent-1".to_string()),
            correlation_id: Some("corr-idempotent-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            tables: Some(vec!["public.orders".to_string()]),
            conditions: None,
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
    assert!(execute_notifications.iter().any(|n| n.state == "ABORTED"));
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
            tables: vec!["public.orders".to_string()],
            conditions: Default::default(),
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
            tables: Some(vec!["public.orders".to_string()]),
            conditions: None,
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

    let metrics = slo_prometheus(&data, true);
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
            tables: Some(vec!["public.orders".to_string()]),
            conditions: None,
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
            tables: Some(vec!["public.orders".to_string()]),
            conditions: None,
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
            tables: None,
            conditions: None,
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

    let metrics = slo_prometheus(&data, true);
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
    // Advertises the capability without a runtime. `RuntimeControl` is not constructible
    // outside rustcdc, so these tests assert the signal **lifecycle** — STARTED, terminal
    // state, notifications, idempotency — which is identical whether the terminal state is
    // COMPLETED or ABORTED. The success path is covered end to end, against a real runtime,
    // by `tests/integration_postgres.rs`.
    admin.advertise_snapshot_capability().await;
    admin
        .process_file_signal_ingress_record(SignalIngressRecord {
            signal_id: Some("sig-ingress-1".to_string()),
            correlation_id: Some("corr-ingress-1".to_string()),
            action_type: SignalActionType::ExecuteSnapshot,
            tables: Some(vec!["public.orders".to_string()]),
            conditions: Default::default(),
            message: Some("ingress execute".to_string()),
            additional_data: Some(serde_json::json!({"source": "file-ingress"})),
            traceparent: Some(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".to_string(),
            ),
        })
        .await;

    wait_for_signal_state(&admin, "sig-ingress-1", "execute_snapshot", "ABORTED").await;

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    assert!(notifications.iter().any(|notification| {
        notification.signal_id == "sig-ingress-1"
            && notification.action_type == "execute_snapshot"
            && notification.state == "ABORTED"
    }));

    assert!(data.audit_recent_entries.iter().any(|entry| {
        entry.action == "signal_execute_snapshot_aborted"
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
            tables: Some(vec!["public.orders".to_string()]),
            conditions: Default::default(),
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

    let metrics = slo_prometheus(&data, true);
    assert!(metrics.contains("rustcdc_signal_ingress_parse_rejections_total 1"));
}

/// A restart must not re-execute the signals already in the file.
///
/// The read offset is process-local, so starting at byte 0 meant every restart re-ran
/// the file's whole history. That was survivable while `execute_snapshot` did nothing;
/// now it snapshots, and rustcdc *rewinds an already-complete table and reads it again* —
/// so a restart re-scanned every table ever requested through this channel and re-emitted
/// every row downstream.
///
/// Restarting is modelled by building a second `AdminState` over the same file, which is
/// what a redeploy does. Changing `spawn_signal_ingress_file_worker` back to `offset = 0`
/// fails this test.
#[tokio::test]
async fn a_restart_does_not_re_execute_the_signals_already_in_the_ingress_file() {
    let mut cfg = sample_config();
    let dir = tempfile::tempdir().expect("tempdir");
    let ingress_path = dir.path().join("signal-ingress.jsonl");
    std::fs::write(&ingress_path, "").expect("create ingress file");
    cfg.admin.signal_ingress_file = Some(ingress_path.clone());

    // A history of signals, as an operator's file accumulates over months.
    let first = AdminState::new(&cfg).await.expect("admin state");
    first.advertise_snapshot_capability().await;
    append_signal_ingress_line(
        &ingress_path,
        &serde_json::json!({
            "signal_id": "sig-history-1",
            "action_type": "execute_snapshot",
            "tables": ["public.orders"],
        })
        .to_string(),
    );
    wait_for_signal_state(&first, "sig-history-1", "execute_snapshot", "ABORTED").await;
    first.shutdown_workers().await;

    // The restart. The same file is still on disk, still holding that request.
    let second = AdminState::new(&cfg).await.expect("restarted admin state");
    second.advertise_snapshot_capability().await;
    tokio::time::sleep(Duration::from_millis(750)).await;

    let data = second.data.read().await;
    assert!(
        data.audit_recent_entries.is_empty(),
        "a restart must not re-execute historical signals; it replayed: {:?}",
        data.audit_recent_entries
    );
    drop(data);

    // The channel is still live — a signal appended *after* the restart runs.
    append_signal_ingress_line(
        &ingress_path,
        &serde_json::json!({
            "signal_id": "sig-after-restart",
            "action_type": "execute_snapshot",
            "tables": ["public.orders"],
        })
        .to_string(),
    );
    wait_for_signal_state(&second, "sig-after-restart", "execute_snapshot", "ABORTED").await;
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

    let metrics = slo_prometheus(&data, true);
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
            tables: None,
            conditions: Default::default(),
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
            tables: None,
            conditions: Default::default(),
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

    let metrics = slo_prometheus(&data, true);
    assert!(metrics.contains("rustcdc_signal_ingress_validation_rejections_total 1"));
}

#[tokio::test]
async fn signal_ingress_payload_processor_preserves_kafka_actor_metadata() {
    let cfg = sample_config();
    let admin = AdminState::new(&cfg).await.expect("admin state");
    // Advertises the capability without a runtime. `RuntimeControl` is not constructible
    // outside rustcdc, so these tests assert the signal **lifecycle** — STARTED, terminal
    // state, notifications, idempotency — which is identical whether the terminal state is
    // COMPLETED or ABORTED. The success path is covered end to end, against a real runtime,
    // by `tests/integration_postgres.rs`.
    admin.advertise_snapshot_capability().await;

    let payload = serde_json::json!({
        "signal_id": "sig-ingress-kafka-actor-1",
        "correlation_id": "corr-ingress-kafka-actor-1",
        "action_type": "execute_snapshot",
        "tables": ["public.orders"],
        "message": "kafka ingress"
    });
    let payload_bytes = serde_json::to_vec(&payload).expect("serialize ingress payload");

    assert!(admin
        .process_signal_ingress_payload(payload_bytes.as_slice(), SignalIngressSource::Kafka,)
        .await
        .is_some());

    wait_for_signal_state(
        &admin,
        "sig-ingress-kafka-actor-1",
        "execute_snapshot",
        "ABORTED",
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

/// A redelivered record whose action already completed must not run a second time.
///
/// This is the property that lets the ingress loop commit *after* an action rather than
/// before it. Without a durable ledger the only dedup was `audit_recent_entries` — 512
/// entries, in memory, empty after a restart — so the loop had to choose between losing a
/// command to a crash and re-running one on redelivery. `execute_snapshot` is not
/// idempotent: rustcdc rewinds an already-complete table and reads it again.
///
/// The second `AdminState` shares the first's state directory, which is exactly what a
/// restart looks like: same durable state, empty in-memory state.
#[tokio::test]
async fn a_redelivered_signal_record_is_not_executed_twice() {
    let cfg = sample_config();
    let payload = serde_json::json!({
        "signal_id": "sig-redelivery-1",
        "action_type": "log_marker",
        "message": "redelivery guard",
    })
    .to_string();
    let record = || kafka_signal_record("cdc.signals", 0, 11, &payload);

    let first_batch = {
        let admin = AdminState::new(&cfg).await.expect("admin state");
        let outcome = admin.ingest_kafka_signal_batch(vec![record()]).await;
        assert_eq!(outcome.ingested, 1, "the first delivery must be executed");
        assert!(
            outcome.commit,
            "a completed action must let the offset advance"
        );
        admin.shutdown_workers().await;
        outcome
    };
    let _ = first_batch;

    // Restart: new in-memory state, same state directory. The offset commit is assumed
    // lost, so the broker redelivers.
    let admin = AdminState::new(&cfg)
        .await
        .expect("admin state after restart");
    let outcome = admin.ingest_kafka_signal_batch(vec![record()]).await;
    admin.shutdown_workers().await;

    assert_eq!(
        outcome.ingested, 0,
        "a record whose action already reached a terminal state must be skipped, not \
         re-executed — for execute_snapshot that would be a full table re-scan"
    );
    assert!(
        outcome.commit,
        "a skipped record is settled, so the offset must still advance past it"
    );
}

/// A record that was never decided must be re-executed after a restart.
///
/// The mirror of the test above, and the half that catches an over-eager ledger: if
/// `mark_signal_ingress_processed` were called before the action ran rather than after it,
/// this record would be skipped and the command lost — which is the original defect
/// wearing the fix's clothes.
#[tokio::test]
async fn an_undelivered_signal_record_still_runs_after_a_restart() {
    let cfg = sample_config();
    let payload = serde_json::json!({
        "signal_id": "sig-redelivery-2",
        "action_type": "log_marker",
        "message": "never decided",
    })
    .to_string();

    {
        // A different offset was processed; ours was not.
        let admin = AdminState::new(&cfg).await.expect("admin state");
        admin
            .ingest_kafka_signal_batch(vec![kafka_signal_record("cdc.signals", 0, 5, &payload)])
            .await;
        admin.shutdown_workers().await;
    }

    let admin = AdminState::new(&cfg)
        .await
        .expect("admin state after restart");
    let outcome = admin
        .ingest_kafka_signal_batch(vec![kafka_signal_record("cdc.signals", 0, 6, &payload)])
        .await;
    admin.shutdown_workers().await;

    assert_eq!(
        outcome.ingested, 1,
        "a record that never reached a terminal state must run after the restart"
    );
}
