//! Authentication and token-manifest tests for the admin API.
//!
//! Split out of `admin/tests.rs` for the same reason that file was split out of
//! `admin/mod.rs`: this is one self-contained concern — bearer-token scopes, manifest
//! signature verification, expiry and revocation, staleness policy and signer rotation —
//! and it is roughly a quarter of the suite. Keeping it beside the lifecycle and
//! notification tests meant neither could be read on its own.
//!
//! `super::` still resolves to `admin`, so nothing about how these reach the code under
//! test changed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
use chrono::Utc;
use dashmap::DashMap;
use ed25519_dalek::SigningKey;
use tempfile::tempdir;
use tokio::sync::RwLock;

use super::auth::{AuthToken, file_modified_time, load_auth_tokens_from_manifest};
use super::tests::{auth_token_manifest, write_signed_manifest};
use super::{
    AdminAbuseGuard, AdminScope, AdminState, AdminStateData, AuthSource, AuthState, InstanceState,
    token_sha256_hex,
};
use crate::config::schema::AdminProbeAuthMode;

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
            snapshot_requests_accepted_total: 0,
            snapshot_requests_refused_total: 0,
            snapshot_tables_enqueued_total: 0,
            snapshot_requests_available: false,
            notification_log_enabled: false,
            notification_kafka_enabled: false,
            notification_log_emitted_total: 0,
            notification_log_emit_failures_total: 0,
            notification_channel_emitted_total: HashMap::new(),
            notification_channel_emit_failures_total: HashMap::new(),
            audit_entries_total: 0,
            audit_recent_entries: Vec::new(),
            signal_worker_panics_total: 0,
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
        runtime_control: Arc::new(std::sync::OnceLock::new()),
        signal_inflight: Arc::new(DashMap::new()),
        signal_terminal_budget: std::time::Duration::from_secs(30),
        // No state directory in a hand-built state: these tests exercise the auth layer,
        // not signal ingress, and the ledger is only consulted by the Kafka ingress loop.
        signal_ledger: None,
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
            snapshot_requests_accepted_total: 0,
            snapshot_requests_refused_total: 0,
            snapshot_tables_enqueued_total: 0,
            snapshot_requests_available: false,
            notification_log_enabled: false,
            notification_kafka_enabled: false,
            notification_log_emitted_total: 0,
            notification_log_emit_failures_total: 0,
            notification_channel_emitted_total: HashMap::new(),
            notification_channel_emit_failures_total: HashMap::new(),
            audit_entries_total: 0,
            audit_recent_entries: Vec::new(),
            signal_worker_panics_total: 0,
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
        runtime_control: Arc::new(std::sync::OnceLock::new()),
        signal_inflight: Arc::new(DashMap::new()),
        signal_terminal_budget: std::time::Duration::from_secs(30),
        // No state directory in a hand-built state: these tests exercise the auth layer,
        // not signal ingress, and the ledger is only consulted by the Kafka ingress loop.
        signal_ledger: None,
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
            snapshot_requests_accepted_total: 0,
            snapshot_requests_refused_total: 0,
            snapshot_tables_enqueued_total: 0,
            snapshot_requests_available: false,
            notification_log_enabled: false,
            notification_kafka_enabled: false,
            notification_log_emitted_total: 0,
            notification_log_emit_failures_total: 0,
            notification_channel_emitted_total: HashMap::new(),
            notification_channel_emit_failures_total: HashMap::new(),
            audit_entries_total: 0,
            audit_recent_entries: Vec::new(),
            signal_worker_panics_total: 0,
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
        runtime_control: Arc::new(std::sync::OnceLock::new()),
        signal_inflight: Arc::new(DashMap::new()),
        signal_terminal_budget: std::time::Duration::from_secs(30),
        // No state directory in a hand-built state: these tests exercise the auth layer,
        // not signal ingress, and the ledger is only consulted by the Kafka ingress loop.
        signal_ledger: None,
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
            snapshot_requests_accepted_total: 0,
            snapshot_requests_refused_total: 0,
            snapshot_tables_enqueued_total: 0,
            snapshot_requests_available: false,
            notification_log_enabled: false,
            notification_kafka_enabled: false,
            notification_log_emitted_total: 0,
            notification_log_emit_failures_total: 0,
            notification_channel_emitted_total: HashMap::new(),
            notification_channel_emit_failures_total: HashMap::new(),
            audit_entries_total: 0,
            audit_recent_entries: Vec::new(),
            signal_worker_panics_total: 0,
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
        runtime_control: Arc::new(std::sync::OnceLock::new()),
        signal_inflight: Arc::new(DashMap::new()),
        signal_terminal_budget: std::time::Duration::from_secs(30),
        // No state directory in a hand-built state: these tests exercise the auth layer,
        // not signal ingress, and the ledger is only consulted by the Kafka ingress loop.
        signal_ledger: None,
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
            snapshot_requests_accepted_total: 0,
            snapshot_requests_refused_total: 0,
            snapshot_tables_enqueued_total: 0,
            snapshot_requests_available: false,
            notification_log_enabled: false,
            notification_kafka_enabled: false,
            notification_log_emitted_total: 0,
            notification_log_emit_failures_total: 0,
            notification_channel_emitted_total: HashMap::new(),
            notification_channel_emit_failures_total: HashMap::new(),
            audit_entries_total: 0,
            audit_recent_entries: Vec::new(),
            signal_worker_panics_total: 0,
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
        runtime_control: Arc::new(std::sync::OnceLock::new()),
        signal_inflight: Arc::new(DashMap::new()),
        signal_terminal_budget: std::time::Duration::from_secs(30),
        // No state directory in a hand-built state: these tests exercise the auth layer,
        // not signal ingress, and the ledger is only consulted by the Kafka ingress loop.
        signal_ledger: None,
        workers: super::AdminWorkers::new(),
        notification_broadcast: Arc::new(
            tokio::sync::broadcast::channel(super::NOTIFICATION_STREAM_BUFFER).0,
        ),
        audit_log_drop_counter: None,
    };

    assert!(
        admin
            .authorize_scope_token_id(&headers, AdminScope::Read)
            .is_none()
    );
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
            snapshot_requests_accepted_total: 0,
            snapshot_requests_refused_total: 0,
            snapshot_tables_enqueued_total: 0,
            snapshot_requests_available: false,
            notification_log_enabled: false,
            notification_kafka_enabled: false,
            notification_log_emitted_total: 0,
            notification_log_emit_failures_total: 0,
            notification_channel_emitted_total: HashMap::new(),
            notification_channel_emit_failures_total: HashMap::new(),
            audit_entries_total: 0,
            audit_recent_entries: Vec::new(),
            signal_worker_panics_total: 0,
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
        runtime_control: Arc::new(std::sync::OnceLock::new()),
        signal_inflight: Arc::new(DashMap::new()),
        signal_terminal_budget: std::time::Duration::from_secs(30),
        // No state directory in a hand-built state: these tests exercise the auth layer,
        // not signal ingress, and the ledger is only consulted by the Kafka ingress loop.
        signal_ledger: None,
        workers: super::AdminWorkers::new(),
        notification_broadcast: Arc::new(
            tokio::sync::broadcast::channel(super::NOTIFICATION_STREAM_BUFFER).0,
        ),
        audit_log_drop_counter: None,
    };

    assert!(admin.authorize_readyz(&HeaderMap::new()));
}
