mod rate_limit;

use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::StatusCode,
    http::{header::AUTHORIZATION, header::RETRY_AFTER, HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use dashmap::DashMap;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use krafka::consumer::{AutoOffsetReset, Consumer};
use krafka::producer::{Acks, Producer, ProducerRecord};
use rate_limit::{AbuseLimitScope, AdminAbuseGuard, RATE_LIMIT_STALE_CLIENT_TTL};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{mpsc, Mutex, RwLock};
use tower_http::timeout::TimeoutLayer;

use crate::commands::run_metrics::RuntimeMetricsSnapshot;
use crate::config::{
    schema::AdminNotificationKafkaConfig, schema::AdminProbeAuthMode,
    schema::AdminSignalIngressKafkaConfig, schema::AdminTlsConfig, AppConfig,
};
use crate::error::AppError;
use crate::redaction::redact_secrets;
use crate::token_manifest_policy;

// ─────────────────────────────────────────────────────────────────────────────
// Shared state that the main event loop writes and the admin handlers read.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceState {
    Starting,
    Running,
    Stopping,
    Stopped,
    Error,
}

/// Histogram bucket upper bounds (ms) for admin API latency.
/// Mirrors `LATENCY_HISTOGRAM_BUCKETS_MS` in `commands/run_metrics.rs`.
const ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_MS: [u64; 8] = [1, 5, 10, 25, 50, 100, 250, 500];

#[derive(Debug, Clone, Serialize)]
pub struct AdminStateData {
    pub state: InstanceState,
    pub started_at: DateTime<Utc>,
    pub first_ready_at: Option<DateTime<Utc>>,
    pub first_checkpoint_advanced_at: Option<DateTime<Utc>>,
    pub last_batch_at: Option<DateTime<Utc>>,
    pub events_processed: u64,
    pub batches_processed: u64,
    pub readiness_checks_total: u64,
    pub readiness_ready_total: u64,
    pub last_admin_api_latency_ms: Option<u64>,
    /// Cumulative histogram of admin API request latencies (milliseconds).
    /// Bucket upper bounds match `ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_MS`.
    pub admin_api_latency_ms_buckets: [u64; 8],
    pub admin_api_latency_ms_sum: f64,
    pub admin_api_latency_ms_count: u64,
    pub checkpoint_age_seconds: Option<f64>,
    /// Replication slot lag in bytes, sampled every 15 s by the admin side-channel poller.
    /// `None` before the first successful sample or for non-PostgreSQL sources.
    pub replication_slot_lag_bytes: Option<i64>,
    pub restart_recovery_seconds: Option<f64>,
    /// Consecutive source poll errors currently accumulated.
    ///
    /// Reset to zero on every successful batch.  The `/readyz` endpoint returns
    /// `503 Service Unavailable` when this counter reaches
    /// `READYZ_SOURCE_CONSECUTIVE_ERROR_THRESHOLD` so that Kubernetes stops
    /// routing traffic to a pod that cannot read from the source database.
    pub source_consecutive_errors: u64,
    /// Instant at which `source_consecutive_errors` first reached
    /// `READYZ_SOURCE_CONSECUTIVE_ERROR_THRESHOLD`.  Reset when errors clear.
    /// Used by `/livez` to detect pipelines stuck in indefinite backoff.
    #[serde(skip)]
    pub degraded_since: Option<std::time::Instant>,
    pub shutdown_requests_os_signal_total: u64,
    pub shutdown_completions_stopped_total: u64,
    pub shutdown_completions_error_total: u64,
    pub admin_rate_limited_readyz_total: u64,
    pub admin_rate_limited_status_total: u64,
    pub admin_rate_limited_metrics_total: u64,
    pub admin_rate_limiter_readyz_decisions_total: u64,
    pub admin_rate_limiter_readyz_decision_latency_ms_sum: f64,
    pub admin_rate_limiter_readyz_decision_latency_ms_max: f64,
    pub admin_rate_limiter_status_decisions_total: u64,
    pub admin_rate_limiter_status_decision_latency_ms_sum: f64,
    pub admin_rate_limiter_status_decision_latency_ms_max: f64,
    pub admin_rate_limiter_metrics_decisions_total: u64,
    pub admin_rate_limiter_metrics_decision_latency_ms_sum: f64,
    pub admin_rate_limiter_metrics_decision_latency_ms_max: f64,
    pub last_terminal_reason_code: Option<String>,
    pub reconciliation_recoveries_total: u64,
    pub reconciliation_recovery_last_unix_seconds: Option<f64>,
    pub reconciliation_recovery_last_parse_ok: Option<bool>,
    pub reconciliation_recovery_last_detail: Option<String>,
    pub reconciliation_recovery_proof_last_unix_seconds: Option<f64>,
    pub reconciliation_recovery_proof_last_ok: Option<bool>,
    pub reconciliation_recovery_proof_last_detail: Option<String>,
    pub signal_action_queue_rejections_total: u64,
    pub signal_action_started_notification_rejections_total: u64,
    pub signal_ingress_parse_rejections_total: u64,
    pub signal_ingress_line_too_large_rejections_total: u64,
    pub signal_ingress_validation_rejections_total: u64,
    pub signal_action_queue_depth: usize,
    pub notification_log_enabled: bool,
    pub notification_kafka_enabled: bool,
    pub notification_log_emitted_total: u64,
    pub notification_log_emit_failures_total: u64,
    pub notification_channel_emitted_total: HashMap<String, u64>,
    pub notification_channel_emit_failures_total: HashMap<String, u64>,
    pub audit_entries_total: u64,
    pub audit_recent_entries: Vec<AuditTrailEntry>,
    /// Ed25519 signing key loaded from `CDC_AUDIT_SIGNING_KEY_HEX` at startup.
    #[serde(skip)]
    pub(crate) audit_signing_key: Option<ed25519_dalek::SigningKey>,
    /// Whether source IPs are pseudonymised before signing (mirrors `AdminConfig::audit_ip_pseudonymise`).
    pub audit_ip_pseudonymise: bool,
    /// 16-byte pseudonymisation salt for audit trail IPs (never serialised or logged).
    #[serde(skip)]
    pub(crate) audit_ip_salt: [u8; 16],
    /// Pre-rendered Prometheus runtime-metrics text, rebuilt only when the
    /// batch generation counter advances (CR-020).  Skips re-allocation on
    /// admin scrapes that arrive between batch completions.
    #[serde(skip_serializing)]
    pub(crate) runtime_metrics: String,
    /// Generation counter incremented by `record_batch`; matches
    /// `runtime_metrics_generation` when the cached string is current.
    #[serde(skip)]
    pub(crate) runtime_metrics_generation: u64,
    /// Sanitised (secret-redacted) config JSON.
    pub config_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditTrailEntry {
    pub sequence: u64,
    pub at: DateTime<Utc>,
    pub action: String,
    pub result: String,
    pub detail: String,
    pub actor_source_ip: Option<String>,
    pub actor_token_id: Option<String>,
    pub prev_hash_hex: String,
    pub entry_hash_hex: String,
    /// Ed25519 signature over the canonical pipe-delimited entry string,
    /// present only when `CDC_AUDIT_SIGNING_KEY_HEX` is set at startup.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ed25519_signature_hex: Option<String>,
}

const AUDIT_TRAIL_MAX_ENTRIES: usize = 512;
/// Maximum byte length for the `detail` field of an audit trail entry.
///
/// Prevents a write-scope token holder from inflating the in-memory ring buffer
/// by submitting signals with arbitrarily long `message` payloads (CR-016).
/// At 512 entries × 4 KiB per detail the ring buffer is bounded to ~2 MiB.
const AUDIT_DETAIL_MAX_BYTES: usize = 4096;
const SIGNAL_TERMINAL_TIMEOUT_SECONDS: i64 = 30;
const SIGNAL_ACTION_QUEUE_CAPACITY: usize = 128;
const SIGNAL_INGRESS_MAX_LINE_BYTES: usize = 1024 * 1024;
/// Number of consecutive source poll errors that must accumulate before `/readyz`
/// returns `503 Service Unavailable` (CR-010).
///
/// Chosen to be low enough that Kubernetes' default liveness/readiness poll
/// interval (10 s) causes a pod to be removed from the load balancer well
/// before the circuit breaker escalation threshold (default 10).
const READYZ_SOURCE_CONSECUTIVE_ERROR_THRESHOLD: u64 = 3;

/// Non-blocking, async-safe append-only log writer.
///
/// Writes are serialised through a dedicated Tokio task that owns the
/// underlying `tokio::fs::File`, so callers on the hot path (async handlers)
/// never block a worker thread on filesystem I/O.
#[derive(Debug)]
struct AuditLogWriter {
    path: PathBuf,
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    drop_counter: Arc<std::sync::atomic::AtomicU64>,
}

struct KafkaNotificationPublisher {
    topic: String,
    producer: Mutex<Producer>,
}

#[derive(Debug, Clone)]
struct QueuedSignalAction {
    signal_id: String,
    correlation_id: String,
    action_type: SignalActionType,
    message: String,
    traceparent: Option<String>,
    additional_data: serde_json::Value,
    actor_source_ip: Option<String>,
    actor_token_id: String,
}

#[derive(Debug, Clone)]
struct SignalActionEnvelope {
    signal_id: String,
    correlation_id: String,
    action_type: SignalActionType,
    message: String,
    traceparent: Option<String>,
    additional_data: serde_json::Value,
    actor_source_ip: Option<String>,
    actor_token_id: String,
}

#[derive(Debug, Clone, Copy)]
enum SignalIngressSource {
    File,
    Kafka,
    Source,
}

impl SignalIngressSource {
    fn channel_label(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Kafka => "kafka",
            Self::Source => "source",
        }
    }

    fn actor_source_ip(self) -> &'static str {
        match self {
            Self::File => "signal_ingress_file",
            Self::Kafka => "signal_ingress_kafka",
            Self::Source => "signal_ingress_source",
        }
    }

    fn actor_token_id(self) -> &'static str {
        match self {
            Self::File => "signal-ingress-file",
            Self::Kafka => "signal-ingress-kafka",
            Self::Source => "signal-ingress-source",
        }
    }
}

#[derive(Debug, Clone)]
enum SignalActionExecutionResult {
    ExistingState {
        signal_id: String,
        correlation_id: String,
        action_type: String,
        current_state: String,
        expected_terminal_state: String,
    },
    Started {
        signal_id: String,
        correlation_id: String,
        action_type: String,
        expected_terminal_state: String,
    },
    Terminal {
        signal_id: String,
        correlation_id: String,
        action_type: String,
        current_state: String,
        expected_terminal_state: String,
    },
    Aborted {
        signal_id: String,
        correlation_id: String,
        action_type: String,
        expected_terminal_state: String,
        error: String,
        retry_after_seconds: Option<u64>,
    },
}

impl KafkaNotificationPublisher {
    async fn new(config: &AdminNotificationKafkaConfig) -> Result<Self, AppError> {
        let auth = config.security.to_auth_config().map_err(|err| {
            AppError::Other(format!(
                "invalid admin.notification_kafka security configuration: {err}"
            ))
        })?;
        let compression = config.compression.to_krafka().map_err(|err| {
            AppError::Other(format!(
                "invalid admin.notification_kafka compression configuration: {err}"
            ))
        })?;
        let producer = Producer::builder()
            .bootstrap_servers(config.normalized_brokers().join(","))
            .client_id(config.client_id.clone())
            .acks(Acks::All)
            .idempotent(true)
            .compression(compression)
            .retries(config.retry_max_attempts)
            .retry_backoff(Duration::from_millis(config.retry_backoff_ms))
            .request_timeout(Duration::from_millis(config.ack_timeout_ms))
            .delivery_timeout(Duration::from_millis(
                config.ack_timeout_ms.saturating_add(
                    config
                        .retry_backoff_ms
                        .saturating_mul(config.retry_max_attempts as u64),
                ),
            ))
            .max_in_flight(5)
            .auth(auth)
            .build()
            .await
            .map_err(|err| {
                AppError::Other(format!(
                    "failed to build admin.notification_kafka producer: {err}"
                ))
            })?;

        Ok(Self {
            topic: config.topic.clone(),
            producer: Mutex::new(producer),
        })
    }

    async fn send_event(&self, event: &serde_json::Value) -> Result<(), AppError> {
        let payload = serde_json::to_vec(event).map_err(|err| {
            AppError::Other(format!("failed to serialize notification event: {err}"))
        })?;
        let key = event
            .get("signalid")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("notification")
            .as_bytes()
            .to_vec();

        let record = ProducerRecord::new(self.topic.clone(), payload).with_key(key);
        let producer = self.producer.lock().await;
        let _metadata = producer.send_record(record).await.map_err(|err| {
            AppError::Other(format!(
                "failed to emit notification event to admin.notification_kafka topic {}: {err}",
                self.topic
            ))
        })?;
        producer.flush().await.map_err(|err| {
            AppError::Other(format!(
                "failed to flush admin.notification_kafka topic {}: {err}",
                self.topic
            ))
        })?;

        Ok(())
    }
}

async fn build_kafka_notification_publisher(
    config: &AdminNotificationKafkaConfig,
) -> Result<Arc<KafkaNotificationPublisher>, AppError> {
    KafkaNotificationPublisher::new(config).await.map(Arc::new)
}

impl AuditLogWriter {
    /// Capacity of the bounded write channel.
    /// At ~1 KB/entry this allows ~8 MiB of queued entries before drops begin.
    const CHANNEL_CAPACITY: usize = 8_192;

    fn open(path: &Path, config_field: &str) -> Result<Self, AppError> {
        // Open synchronously to surface permission / path errors immediately.
        let std_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| {
                AppError::Other(format!(
                    "failed to open {config_field} {}: {e}",
                    path.display()
                ))
            })?;

        // Promote to async file and hand off to a dedicated writer task.
        // Bounded channel (8 192 entries) provides backpressure: when the
        // writer task falls behind under filesystem pressure, excess entries
        // are counted and dropped rather than causing unbounded heap growth.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(Self::CHANNEL_CAPACITY);
        let drop_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let display_path = path.display().to_string();

        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt as _;
            let mut file = tokio::fs::File::from_std(std_file);
            while let Some(bytes) = rx.recv().await {
                if let Err(e) = file.write_all(&bytes).await {
                    tracing::error!(
                        path = %display_path,
                        error = %e,
                        "audit log write failed"
                    );
                } else if let Err(e) = file.sync_data().await {
                    tracing::error!(
                        path = %display_path,
                        error = %e,
                        "audit log sync_data failed"
                    );
                }
            }
        });

        Ok(Self {
            path: path.to_path_buf(),
            tx,
            drop_counter,
        })
    }

    fn append_entry(&self, entry: &AuditTrailEntry) -> Result<(), AppError> {
        self.append_json_value(entry, "audit entry")
    }

    fn append_json_value<T: Serialize>(
        &self,
        value: &T,
        value_label: &str,
    ) -> Result<(), AppError> {
        let mut line = serde_json::to_vec(value)
            .map_err(|e| AppError::Other(format!("failed to serialize {value_label}: {e}")))?;
        line.push(b'\n');

        // Non-blocking try_send: if the channel is full, count the drop and
        // return Ok so the hot path is never stalled.
        match self.tx.try_send(line) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                self.drop_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    path = %self.path.display(),
                    "audit log channel full — entry dropped (increment rustcdc_audit_log_drop_total)"
                );
                Ok(())
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(AppError::Other(format!(
                    "audit log writer task has exited for {}",
                    self.path.display()
                )))
            }
        }
    }

    #[allow(dead_code)]
    fn drop_count(&self) -> u64 {
        self.drop_counter.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct AdminState {
    pub data: Arc<RwLock<AdminStateData>>,
    abuse_guard: Arc<AdminAbuseGuard>,
    readiness_auth_mode: AdminProbeAuthMode,
    auth_state: Arc<std::sync::RwLock<AuthState>>,
    audit_log: Option<Arc<AuditLogWriter>>,
    notification_log: Option<Arc<AuditLogWriter>>,
    notification_kafka: Option<Arc<KafkaNotificationPublisher>>,
    signal_action_tx: mpsc::Sender<QueuedSignalAction>,
    signal_inflight: Arc<DashMap<(String, String), ()>>,
    /// Aggregate audit-log drop counter sourced directly from the `AuditLogWriter`
    /// atomics — does not require holding the `RwLock<AdminStateData>` write lock.
    audit_log_drop_counter: Option<Arc<std::sync::atomic::AtomicU64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdminScope {
    Read,
    #[allow(dead_code)]
    Write,
}

#[derive(Debug, Clone)]
struct AuthToken {
    id: String,
    token_sha256_hex: String,
    not_before: Option<DateTime<Utc>>,
    expires_at: Option<DateTime<Utc>>,
    revoked: bool,
}

#[derive(Debug, Clone)]
struct AuthState {
    read_tokens: Vec<AuthToken>,
    write_tokens: Vec<AuthToken>,
    source: AuthSource,
    manifest_version: u64,
    manifest_reload_ok: bool,
    manifest_stale_blocked: bool,
    last_reload_at: Option<SystemTime>,
    last_reload_error: Option<String>,
    last_observed_mtime: Option<SystemTime>,
    last_refresh_attempt: Option<Instant>,
    revoked_token_hits_total: u64,
}

#[derive(Debug, Clone)]
enum AuthSource {
    Env,
    Manifest {
        path: std::path::PathBuf,
        trusted_public_keys: Vec<VerifyingKey>,
        refresh_interval: Duration,
        max_staleness: Option<Duration>,
    },
}

impl AdminState {
    #[cfg(test)]
    fn dormant_signal_action_tx() -> mpsc::Sender<QueuedSignalAction> {
        let (tx, _rx) = mpsc::channel(1);
        tx
    }

    fn redacted_config_snapshot(config: &AppConfig) -> String {
        match serde_json::to_string_pretty(config) {
            Ok(json) => redact_secrets(&json),
            Err(error) => serde_json::json!({
                "api_version": "v1",
                "config_snapshot": "unavailable",
                "reason": format!("failed to serialize sanitized config: {error}"),
            })
            .to_string(),
        }
    }

    pub async fn new(config: &AppConfig) -> Result<Self, AppError> {
        let config_json = Self::redacted_config_snapshot(config);
        let auth_state = load_auth_state(config)?;
        let audit_log = config
            .admin
            .audit_log_file
            .as_ref()
            .map(|path| AuditLogWriter::open(path, "admin.audit_log_file"))
            .transpose()?
            .map(Arc::new);
        let notification_log = config
            .admin
            .notification_log_file
            .as_ref()
            .map(|path| AuditLogWriter::open(path, "admin.notification_log_file"))
            .transpose()?
            .map(Arc::new);
        let notification_kafka = if let Some(kafka_cfg) = &config.admin.notification_kafka {
            Some(build_kafka_notification_publisher(kafka_cfg).await?)
        } else {
            None
        };
        let (signal_action_tx, signal_action_rx) = mpsc::channel(SIGNAL_ACTION_QUEUE_CAPACITY);
        let signal_ingress_file = config.admin.signal_ingress_file.clone();
        let signal_ingress_kafka = config.admin.signal_ingress_kafka.clone();
        let state = Self {
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
                last_admin_api_latency_ms: None,
                admin_api_latency_ms_buckets: [0u64; 8],
                admin_api_latency_ms_sum: 0.0,
                admin_api_latency_ms_count: 0,
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
                admin_rate_limiter_readyz_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_readyz_decision_latency_ms_max: 0.0,
                admin_rate_limiter_status_decisions_total: 0,
                admin_rate_limiter_status_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_status_decision_latency_ms_max: 0.0,
                admin_rate_limiter_metrics_decisions_total: 0,
                admin_rate_limiter_metrics_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_metrics_decision_latency_ms_max: 0.0,
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
                notification_log_enabled: notification_log.is_some(),
                notification_kafka_enabled: notification_kafka.is_some(),
                notification_log_emitted_total: 0,
                notification_log_emit_failures_total: 0,
                notification_channel_emitted_total: HashMap::new(),
                notification_channel_emit_failures_total: HashMap::new(),
                audit_entries_total: 0,
                audit_recent_entries: Vec::new(),
                audit_signing_key: load_audit_signing_key(),
                audit_ip_pseudonymise: config.admin.audit_ip_pseudonymise,
                audit_ip_salt: resolve_audit_ip_salt(&config.admin),
                runtime_metrics: String::new(),
                runtime_metrics_generation: 0,
                config_json,
            })),
            abuse_guard: Arc::new(AdminAbuseGuard::new(&config.admin)),
            readiness_auth_mode: config.admin.probe_auth_mode,
            auth_state: Arc::new(std::sync::RwLock::new(auth_state)),
            audit_log_drop_counter: audit_log.as_ref().map(|w| Arc::clone(&w.drop_counter)),
            audit_log,
            notification_log,
            notification_kafka,
            signal_action_tx,
            signal_inflight: Arc::new(DashMap::new()),
        };
        state.spawn_signal_action_worker(signal_action_rx);
        state.spawn_rate_limit_sweep_worker();
        if let Some(path) = signal_ingress_file {
            state.spawn_signal_ingress_file_worker(path);
        }
        if let Some(kafka_config) = signal_ingress_kafka {
            state.spawn_signal_ingress_kafka_worker(kafka_config);
        }
        Ok(state)
    }

    /// CR-006 fix: periodically sweep stale entries from all rate-limit maps.
    ///
    /// The insert-time eviction path only runs when a new key tries to join a
    /// full map.  Under a sustained attack from rotating IPs the map fills,
    /// eviction stops, and legitimate clients can be blocked.  This background
    /// task sweeps every `RATE_LIMIT_STALE_CLIENT_TTL / 2` regardless of map
    /// size, keeping the maps current.
    fn spawn_rate_limit_sweep_worker(&self) {
        let abuse_guard = Arc::clone(&self.abuse_guard);
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("rate limit sweep runtime");
            runtime.block_on(async move {
                let sweep_interval = RATE_LIMIT_STALE_CLIENT_TTL / 2;
                loop {
                    tokio::time::sleep(sweep_interval).await;
                    abuse_guard.sweep_stale_entries();
                }
            });
        });
    }

    fn spawn_signal_action_worker(&self, mut rx: mpsc::Receiver<QueuedSignalAction>) {
        let worker_state = self.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("signal worker runtime");
            runtime.block_on(async move {
                while let Some(action) = rx.recv().await {
                    {
                        let mut data = worker_state.data.write().await;
                        data.signal_action_queue_depth =
                            data.signal_action_queue_depth.saturating_sub(1);
                    }
                    worker_state.process_queued_signal_action(action).await;
                }
            });
        });
    }

    fn spawn_signal_ingress_file_worker(&self, path: PathBuf) {
        let worker_state = self.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("signal ingress worker runtime");

            runtime.block_on(async move {
                let mut offset: u64 = 0;

                loop {
                    if let Ok(metadata) = std::fs::metadata(&path) {
                        if metadata.len() < offset {
                            offset = 0;
                        }
                    }

                    let mut ingested_lines = 0_u64;
                    if let Ok(file) = std::fs::OpenOptions::new().read(true).open(&path) {
                        let mut reader = BufReader::new(file);
                        if reader.seek(SeekFrom::Start(offset)).is_ok() {
                            let mut line = Vec::new();
                            let mut drain = Vec::new();
                            loop {
                                line.clear();
                                let mut limited_reader = reader
                                    .by_ref()
                                    .take((SIGNAL_INGRESS_MAX_LINE_BYTES as u64) + 1);
                                let read = limited_reader.read_until(b'\n', &mut line).unwrap_or(0);
                                if read == 0 {
                                    break;
                                }

                                offset = offset.saturating_add(read as u64);

                                let line_too_large = line.len() > SIGNAL_INGRESS_MAX_LINE_BYTES
                                    && !line.ends_with(b"\n");
                                if line_too_large {
                                    worker_state
                                        .record_signal_ingress_line_too_large_rejection()
                                        .await;

                                    drain.clear();
                                    let drained = reader.read_until(b'\n', &mut drain).unwrap_or(0);
                                    if drained > 0 {
                                        offset = offset.saturating_add(drained as u64);
                                    }

                                    tracing::warn!(
                                        target: "rustcdc_audit",
                                        action = "signal_ingress_line_too_large",
                                        file = %path.display(),
                                        max_line_bytes = SIGNAL_INGRESS_MAX_LINE_BYTES,
                                        "rejected oversized signal ingress line"
                                    );
                                    continue;
                                }

                                if worker_state
                                    .process_signal_ingress_payload(
                                        &line,
                                        SignalIngressSource::File,
                                    )
                                    .await
                                {
                                    ingested_lines = ingested_lines.saturating_add(1);
                                }
                            }
                        }
                    }

                    if ingested_lines > 0 {
                        tracing::info!(
                            target: "rustcdc_audit",
                            action = "signal_ingress_batch_processed",
                            file = %path.display(),
                            records = ingested_lines,
                            "processed signal ingress records"
                        );
                    }

                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            });
        });
    }

    fn spawn_signal_ingress_kafka_worker(&self, config: AdminSignalIngressKafkaConfig) {
        let worker_state = self.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("signal ingress kafka worker runtime");

            runtime.block_on(async move {
                loop {
                    let auth = match config.security.to_auth_config() {
                        Ok(auth) => auth,
                        Err(error) => {
                            tracing::error!(
                                target: "rustcdc_audit",
                                action = "signal_ingress_kafka_init_failed",
                                error = %error,
                                "invalid signal ingress kafka security configuration"
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                    let consumer = match Consumer::builder()
                        .bootstrap_servers(config.normalized_brokers().join(","))
                        .group_id(config.group_id.clone())
                        .client_id(config.client_id.clone())
                        .auto_offset_reset(AutoOffsetReset::Earliest)
                        .enable_auto_commit(false)
                        .request_timeout(Duration::from_millis(1_000))
                        .auth(auth)
                        .build()
                        .await
                    {
                        Ok(consumer) => consumer,
                        Err(error) => {
                            tracing::warn!(
                                target: "rustcdc_audit",
                                action = "signal_ingress_kafka_connect_failed",
                                error = %error,
                                "failed to connect kafka signal ingress consumer"
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                    if let Err(error) = consumer.subscribe(&[config.topic.as_str()]).await {
                        tracing::warn!(
                            target: "rustcdc_audit",
                            action = "signal_ingress_kafka_subscribe_failed",
                            topic = %config.topic,
                            error = %error,
                            "failed to subscribe kafka signal ingress consumer"
                        );
                        let _ = consumer.close().await;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }

                    loop {
                        let records = match consumer
                            .poll(Duration::from_millis(config.poll_timeout_ms))
                            .await
                        {
                            Ok(records) => records,
                            Err(error) => {
                                tracing::warn!(
                                    target: "rustcdc_audit",
                                    action = "signal_ingress_kafka_poll_failed",
                                    error = %error,
                                    "failed polling kafka signal ingress records"
                                );
                                break;
                            }
                        };

                        if records.is_empty() {
                            continue;
                        }

                        let mut ingested_records = 0_u64;
                        for record in records {
                            let Some(value) = &record.value else {
                                continue;
                            };

                            if worker_state
                                .process_signal_ingress_payload(
                                    value.as_ref(),
                                    SignalIngressSource::Kafka,
                                )
                                .await
                            {
                                ingested_records = ingested_records.saturating_add(1);
                            }
                        }

                        if let Err(error) = consumer.commit().await {
                            tracing::warn!(
                                target: "rustcdc_audit",
                                action = "signal_ingress_kafka_commit_failed",
                                error = %error,
                                "failed committing kafka signal ingress offsets"
                            );
                        }

                        if ingested_records > 0 {
                            tracing::info!(
                                target: "rustcdc_audit",
                                action = "signal_ingress_kafka_batch_processed",
                                topic = %config.topic,
                                records = ingested_records,
                                "processed kafka signal ingress records"
                            );
                        }
                    }

                    let _ = consumer.close().await;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
        });
    }

    async fn process_signal_ingress_payload(
        &self,
        payload: &[u8],
        source: SignalIngressSource,
    ) -> bool {
        if payload.iter().all(|b| b.is_ascii_whitespace()) {
            return false;
        }

        if payload.len() > SIGNAL_INGRESS_MAX_LINE_BYTES {
            self.record_signal_ingress_line_too_large_rejection().await;
            tracing::warn!(
                target: "rustcdc_audit",
                action = "signal_ingress_payload_too_large",
                ingress_channel = source.channel_label(),
                max_line_bytes = SIGNAL_INGRESS_MAX_LINE_BYTES,
                payload_bytes = payload.len(),
                "rejected oversized signal ingress payload"
            );
            return false;
        }

        let record = match serde_json::from_slice::<SignalIngressRecord>(payload) {
            Ok(record) => record,
            Err(error) => {
                self.record_signal_ingress_parse_rejection().await;
                tracing::warn!(
                    target: "rustcdc_audit",
                    action = "signal_ingress_parse_failed",
                    ingress_channel = source.channel_label(),
                    error = %error,
                    "failed to parse signal ingress record"
                );
                return false;
            }
        };

        self.process_signal_ingress_record(record, source).await;
        true
    }

    #[cfg(test)]
    async fn process_file_signal_ingress_record(&self, record: SignalIngressRecord) {
        self.process_signal_ingress_record(record, SignalIngressSource::File)
            .await;
    }

    pub async fn process_source_signal_ingress_payload(&self, payload: &[u8]) -> bool {
        self.process_signal_ingress_payload(payload, SignalIngressSource::Source)
            .await
    }

    async fn process_signal_ingress_record(
        &self,
        record: SignalIngressRecord,
        source: SignalIngressSource,
    ) {
        let message = record.message.unwrap_or_default().trim().to_string();
        if matches!(record.action_type, SignalActionType::LogMarker) && message.is_empty() {
            self.record_signal_ingress_validation_rejection().await;
            tracing::warn!(
                target: "rustcdc_audit",
                action = "signal_ingress_rejected",
                reason = "empty_log_marker_message",
                ingress_channel = source.channel_label(),
                "ignoring log_marker ingress record with empty message"
            );
            return;
        }

        let signal_id =
            normalize_optional_id(record.signal_id.as_deref()).unwrap_or_else(generated_signal_id);
        let correlation_id = normalize_optional_id(record.correlation_id.as_deref())
            .unwrap_or_else(|| signal_id.clone());
        let traceparent = normalize_optional_id(record.traceparent.as_deref());
        let envelope = SignalActionEnvelope {
            signal_id,
            correlation_id,
            action_type: record.action_type,
            message,
            traceparent,
            additional_data: record.additional_data.unwrap_or(serde_json::Value::Null),
            actor_source_ip: Some(source.actor_source_ip().to_string()),
            actor_token_id: source.actor_token_id().to_string(),
        };

        let result = self.execute_signal_action_envelope(envelope).await;
        if let SignalActionExecutionResult::Aborted {
            signal_id,
            action_type,
            error,
            ..
        } = result
        {
            tracing::warn!(
                target: "rustcdc_audit",
                action = "signal_ingress_aborted",
                ingress_channel = source.channel_label(),
                signal_id = %signal_id,
                action_type = %action_type,
                error = %error,
                "ingress signal action aborted"
            );
        }
    }

    async fn handle_signal_action_queue_unavailable(&self, action: QueuedSignalAction) {
        {
            let mut data = self.data.write().await;
            data.signal_action_queue_rejections_total =
                data.signal_action_queue_rejections_total.saturating_add(1);
        }

        let action_type = action.action_type.as_str().to_string();
        let signal_id = action.signal_id;
        let correlation_id = action.correlation_id;
        let message = action.message;
        let traceparent = action.traceparent;
        let additional_data = action.additional_data;
        let actor_source_ip = action.actor_source_ip;
        let actor_token_id = action.actor_token_id;

        let terminal_entry = self
            .append_signal_lifecycle_entry(
                &signal_id,
                &correlation_id,
                action.action_type,
                action.action_type.queue_rejected_audit_action(),
                "rejected",
                "ABORTED",
                &message,
                traceparent,
                additional_data,
                actor_source_ip,
                Some(actor_token_id),
            )
            .await;
        drop(terminal_entry);

        self.signal_inflight
            .remove(&(signal_id.clone(), action_type.clone()));
    }

    async fn execute_signal_action_envelope(
        &self,
        envelope: SignalActionEnvelope,
    ) -> SignalActionExecutionResult {
        let SignalActionEnvelope {
            signal_id,
            correlation_id,
            action_type,
            message,
            traceparent,
            additional_data,
            actor_source_ip,
            actor_token_id,
        } = envelope;

        let action_type_text = action_type.as_str().to_string();
        let expected_terminal_state = action_type.terminal_state().to_string();
        let async_action = signal_action_requires_async_worker(action_type);
        let inflight_key = (signal_id.clone(), action_type_text.clone());

        let current_state = {
            let data = self.data.read().await;
            latest_signal_action_state(&data, &signal_id, &action_type_text)
        };
        if let Some(existing_state) = current_state {
            return SignalActionExecutionResult::ExistingState {
                signal_id,
                correlation_id,
                action_type: action_type_text,
                current_state: existing_state,
                expected_terminal_state,
            };
        }

        if async_action && !self.has_non_admin_notification_channels() {
            let rejected_entry = self
                .append_signal_lifecycle_entry(
                    &signal_id,
                    &correlation_id,
                    action_type,
                    action_type.queue_rejected_audit_action(),
                    "rejected",
                    "ABORTED",
                    &message,
                    traceparent.clone(),
                    additional_data.clone(),
                    actor_source_ip.clone(),
                    Some(actor_token_id.clone()),
                )
                .await;
            drop(rejected_entry);

            return SignalActionExecutionResult::Aborted {
                signal_id,
                correlation_id,
                action_type: action_type_text,
                expected_terminal_state,
                error: "no non-admin notification channels are enabled for async signal actions"
                    .to_string(),
                retry_after_seconds: Some(1),
            };
        }

        if async_action
            && self
                .signal_inflight
                .insert(inflight_key.clone(), ())
                .is_some()
        {
            return SignalActionExecutionResult::ExistingState {
                signal_id,
                correlation_id,
                action_type: action_type_text,
                current_state: "STARTED".to_string(),
                expected_terminal_state,
            };
        }

        let (started_entry, started_notification_emitted) = self
            .append_signal_lifecycle_entry(
                &signal_id,
                &correlation_id,
                action_type,
                action_type.started_audit_action(),
                "accepted",
                "STARTED",
                &message,
                traceparent.clone(),
                additional_data.clone(),
                actor_source_ip.clone(),
                Some(actor_token_id.clone()),
            )
            .await;
        drop(started_entry);

        if !started_notification_emitted {
            self.record_signal_started_notification_rejection().await;
            let notification_rejected_entry = self
                .append_signal_lifecycle_entry(
                    &signal_id,
                    &correlation_id,
                    action_type,
                    action_type.queue_rejected_audit_action(),
                    "rejected",
                    "ABORTED",
                    &message,
                    traceparent.clone(),
                    additional_data.clone(),
                    actor_source_ip.clone(),
                    Some(actor_token_id.clone()),
                )
                .await;
            drop(notification_rejected_entry);
            if async_action {
                self.signal_inflight.remove(&inflight_key);
            }

            return SignalActionExecutionResult::Aborted {
                signal_id,
                correlation_id,
                action_type: action_type_text,
                expected_terminal_state,
                error: "failed to emit STARTED lifecycle notification to non-admin channels"
                    .to_string(),
                retry_after_seconds: Some(1),
            };
        }

        if async_action {
            let queued_action = QueuedSignalAction {
                signal_id: signal_id.clone(),
                correlation_id: correlation_id.clone(),
                action_type,
                message,
                traceparent,
                additional_data,
                actor_source_ip,
                actor_token_id,
            };

            match self.signal_action_tx.try_send(queued_action) {
                Ok(()) => {
                    let mut data = self.data.write().await;
                    data.signal_action_queue_depth =
                        data.signal_action_queue_depth.saturating_add(1);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(queued_action)) => {
                    self.handle_signal_action_queue_unavailable(queued_action)
                        .await;
                    return SignalActionExecutionResult::Aborted {
                        signal_id,
                        correlation_id,
                        action_type: action_type_text,
                        expected_terminal_state,
                        error: "signal queue saturated".to_string(),
                        retry_after_seconds: Some(1),
                    };
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(queued_action)) => {
                    self.handle_signal_action_queue_unavailable(queued_action)
                        .await;
                    return SignalActionExecutionResult::Aborted {
                        signal_id,
                        correlation_id,
                        action_type: action_type_text,
                        expected_terminal_state,
                        error: "signal worker unavailable".to_string(),
                        retry_after_seconds: Some(1),
                    };
                }
            }

            return SignalActionExecutionResult::Started {
                signal_id,
                correlation_id,
                action_type: action_type_text,
                expected_terminal_state,
            };
        }

        let terminal_entry = self
            .append_signal_lifecycle_entry(
                &signal_id,
                &correlation_id,
                action_type,
                action_type.terminal_audit_action(),
                action_type.terminal_result(),
                action_type.terminal_state(),
                &message,
                traceparent,
                additional_data,
                actor_source_ip,
                Some(actor_token_id),
            )
            .await;
        drop(terminal_entry);

        SignalActionExecutionResult::Terminal {
            signal_id,
            correlation_id,
            action_type: action_type_text,
            current_state: action_type.terminal_state().to_string(),
            expected_terminal_state,
        }
    }

    fn persist_audit_entry(&self, entry: &AuditTrailEntry) {
        let Some(audit_log) = &self.audit_log else {
            return;
        };

        if let Err(error) = audit_log.append_entry(entry) {
            tracing::error!(
                target: "rustcdc_audit",
                action = "audit_persist_failed",
                sequence = entry.sequence,
                error = %error,
                "failed to persist audit log entry"
            );
        }
    }

    fn allow_abuse_scope(
        &self,
        scope: AbuseLimitScope,
        headers: &HeaderMap,
        peer_addr: Option<SocketAddr>,
    ) -> (bool, Duration) {
        self.abuse_guard.allow(scope, headers, peer_addr)
    }

    fn has_non_admin_notification_channels(&self) -> bool {
        self.notification_log.is_some() || self.notification_kafka.is_some()
    }

    async fn record_rate_limiter_decision(&self, scope: AbuseLimitScope, latency: Duration) {
        let mut d = self.data.write().await;
        let latency_ms = latency.as_secs_f64() * 1000.0;
        match scope {
            AbuseLimitScope::Readyz => {
                d.admin_rate_limiter_readyz_decisions_total = d
                    .admin_rate_limiter_readyz_decisions_total
                    .saturating_add(1);
                d.admin_rate_limiter_readyz_decision_latency_ms_sum += latency_ms;
                d.admin_rate_limiter_readyz_decision_latency_ms_max = d
                    .admin_rate_limiter_readyz_decision_latency_ms_max
                    .max(latency_ms);
            }
            AbuseLimitScope::Status => {
                d.admin_rate_limiter_status_decisions_total = d
                    .admin_rate_limiter_status_decisions_total
                    .saturating_add(1);
                d.admin_rate_limiter_status_decision_latency_ms_sum += latency_ms;
                d.admin_rate_limiter_status_decision_latency_ms_max = d
                    .admin_rate_limiter_status_decision_latency_ms_max
                    .max(latency_ms);
            }
            AbuseLimitScope::Metrics => {
                d.admin_rate_limiter_metrics_decisions_total = d
                    .admin_rate_limiter_metrics_decisions_total
                    .saturating_add(1);
                d.admin_rate_limiter_metrics_decision_latency_ms_sum += latency_ms;
                d.admin_rate_limiter_metrics_decision_latency_ms_max = d
                    .admin_rate_limiter_metrics_decision_latency_ms_max
                    .max(latency_ms);
            }
        }
    }

    async fn record_signal_started_notification_rejection(&self) {
        let mut data = self.data.write().await;
        data.signal_action_started_notification_rejections_total = data
            .signal_action_started_notification_rejections_total
            .saturating_add(1);
    }

    async fn record_signal_ingress_parse_rejection(&self) {
        let mut data = self.data.write().await;
        data.signal_ingress_parse_rejections_total =
            data.signal_ingress_parse_rejections_total.saturating_add(1);
    }

    async fn record_signal_ingress_line_too_large_rejection(&self) {
        let mut data = self.data.write().await;
        data.signal_ingress_line_too_large_rejections_total = data
            .signal_ingress_line_too_large_rejections_total
            .saturating_add(1);
    }

    async fn record_signal_ingress_validation_rejection(&self) {
        let mut data = self.data.write().await;
        data.signal_ingress_validation_rejections_total = data
            .signal_ingress_validation_rejections_total
            .saturating_add(1);
    }

    fn spawn_manifest_refresh_worker(&self) {
        let refresh_interval = {
            let Ok(auth) = self.auth_state.read() else {
                return;
            };

            let AuthSource::Manifest {
                refresh_interval, ..
            } = auth.source
            else {
                return;
            };

            refresh_interval
        };

        let state = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(refresh_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                ticker.tick().await;
                let worker_state = state.clone();
                let result = tokio::task::spawn_blocking(move || {
                    let Ok(mut auth) = worker_state.auth_state.write() else {
                        return;
                    };
                    auth.maybe_refresh_manifest();
                })
                .await;

                if let Err(error) = result {
                    tracing::warn!(%error, "admin manifest refresh worker join error");
                }
            }
        });
    }

    fn authorize_readyz(&self, headers: &HeaderMap) -> bool {
        match self.readiness_auth_mode {
            AdminProbeAuthMode::RequireReadToken => self.authorize_read(headers),
            AdminProbeAuthMode::AllowUnauthenticatedLoopback => true,
        }
    }

    fn authorize_read(&self, headers: &HeaderMap) -> bool {
        self.authorize_scope_token_id(headers, AdminScope::Read)
            .is_some()
    }

    fn authorize_write_token_id(&self, headers: &HeaderMap) -> Option<String> {
        self.authorize_scope_token_id(headers, AdminScope::Write)
    }

    fn authorize_scope_token_id(&self, headers: &HeaderMap, scope: AdminScope) -> Option<String> {
        let Ok(mut auth) = self.auth_state.write() else {
            return None;
        };
        auth.enforce_manifest_staleness_from_source();

        if auth.manifest_stale_blocked {
            tracing::warn!(
                target: "rustcdc_audit",
                action = "admin_auth_denied",
                reason = "manifest_stale_blocked",
                scope = match scope {
                    AdminScope::Read => "read",
                    AdminScope::Write => "write",
                },
                "admin auth denied"
            );
            return None;
        }

        let required_tokens = match scope {
            AdminScope::Read => auth.read_tokens.clone(),
            AdminScope::Write => auth.write_tokens.clone(),
        };

        if required_tokens.is_empty() {
            // Fail closed when the scope has no configured tokens.
            tracing::warn!(
                target: "rustcdc_audit",
                action = "admin_auth_denied",
                reason = "no_tokens_for_scope",
                scope = match scope {
                    AdminScope::Read => "read",
                    AdminScope::Write => "write",
                },
                "admin auth denied"
            );
            return None;
        }

        let Some(token) = bearer_token(headers) else {
            tracing::warn!(
                target: "rustcdc_audit",
                action = "admin_auth_denied",
                reason = "missing_bearer_token",
                scope = match scope {
                    AdminScope::Read => "read",
                    AdminScope::Write => "write",
                },
                "admin auth denied"
            );
            return None;
        };

        let token_hash = token_sha256_hex(token);
        let now = Utc::now();

        for candidate in &required_tokens {
            if !constant_time_eq_str(&candidate.token_sha256_hex, &token_hash) {
                continue;
            }

            if candidate.revoked {
                auth.revoked_token_hits_total += 1;
                tracing::warn!(
                    target: "rustcdc_audit",
                    action = "admin_auth_denied",
                    reason = "revoked_token",
                    token_id = %candidate.id,
                    scope = match scope {
                        AdminScope::Read => "read",
                        AdminScope::Write => "write",
                    },
                    "admin auth denied"
                );
                return None;
            }

            if candidate.not_before.is_some_and(|ts| now < ts) {
                return None;
            }

            if candidate.expires_at.is_some_and(|ts| now >= ts) {
                return None;
            }

            return Some(candidate.id.clone());
        }

        None
    }

    fn auth_status_json(&self) -> serde_json::Value {
        let Ok(auth) = self.auth_state.read() else {
            return serde_json::json!({
                "status": "error",
                "detail": "auth state lock poisoned",
            });
        };

        let manifest_last_reload_age_seconds = auth
            .last_reload_at
            .and_then(|ts| SystemTime::now().duration_since(ts).ok())
            .map(|d| d.as_secs_f64());

        serde_json::json!({
            "source": auth.source_name(),
            "manifest_version": auth.manifest_version,
            "manifest_reload_ok": auth.manifest_reload_ok,
            "manifest_stale_blocked": auth.manifest_stale_blocked,
            "manifest_last_reload_age_seconds": manifest_last_reload_age_seconds,
            "revoked_token_hits_total": auth.revoked_token_hits_total,
        })
    }

    fn auth_prometheus(&self) -> String {
        let Ok(auth) = self.auth_state.read() else {
            return String::new();
        };

        let source_label = auth.source_name();
        let reload_ok = if auth.manifest_reload_ok { 1 } else { 0 };
        let stale_blocked = if auth.manifest_stale_blocked { 1 } else { 0 };
        let reload_age_seconds = auth
            .last_reload_at
            .and_then(|ts| SystemTime::now().duration_since(ts).ok())
            .map(|d| d.as_secs_f64())
            .unwrap_or(-1.0);

        format!(
            concat!(
                "# HELP rustcdc_admin_auth_manifest_reload_ok Last token-manifest reload success state\n",
                "# TYPE rustcdc_admin_auth_manifest_reload_ok gauge\n",
                "rustcdc_admin_auth_manifest_reload_ok{{source=\"{}\"}} {}\n",
                "# HELP rustcdc_admin_auth_manifest_version Token-manifest version counter (successful reloads)\n",
                "# TYPE rustcdc_admin_auth_manifest_version gauge\n",
                "rustcdc_admin_auth_manifest_version{{source=\"{}\"}} {}\n",
                "# HELP rustcdc_admin_auth_manifest_last_reload_age_seconds Age since last successful manifest reload in seconds (-1 when unavailable)\n",
                "# TYPE rustcdc_admin_auth_manifest_last_reload_age_seconds gauge\n",
                "rustcdc_admin_auth_manifest_last_reload_age_seconds{{source=\"{}\"}} {}\n",
                "# HELP rustcdc_admin_auth_manifest_stale_blocked Whether auth is currently blocked due to stale manifest policy\n",
                "# TYPE rustcdc_admin_auth_manifest_stale_blocked gauge\n",
                "rustcdc_admin_auth_manifest_stale_blocked{{source=\"{}\"}} {}\n",
                "# HELP rustcdc_admin_auth_revoked_token_hits_total Requests that presented a known revoked token\n",
                "# TYPE rustcdc_admin_auth_revoked_token_hits_total counter\n",
                "rustcdc_admin_auth_revoked_token_hits_total{{source=\"{}\"}} {}\n"
            ),
            source_label,
            reload_ok,
            source_label,
            auth.manifest_version,
            source_label,
            reload_age_seconds,
            source_label,
            stale_blocked,
            source_label,
            auth.revoked_token_hits_total,
        )
    }

    /// Convenience helper called by the main loop after every batch.
    pub(crate) async fn record_batch(
        &self,
        event_count: u64,
        runtime_metrics: RuntimeMetricsSnapshot,
        checkpoint_age_seconds: Option<f64>,
    ) {
        let mut d = self.data.write().await;
        d.last_batch_at = Some(Utc::now());
        d.events_processed += event_count;
        d.batches_processed += 1;
        // Defer `render_prometheus()` to the first scrape after this batch —
        // avoids allocation when the /metrics endpoint is not polled every batch.
        // The generation counter signals that `runtime_metrics` is stale.
        d.runtime_metrics_generation = d.batches_processed;
        d.runtime_metrics = runtime_metrics.render_prometheus();
        d.checkpoint_age_seconds = checkpoint_age_seconds;
        d.last_terminal_reason_code = None;
        d.state = InstanceState::Running;
        // A successful batch delivery means the source is reachable again —
        // reset the consecutive-error counter so /readyz recovers immediately.
        d.source_consecutive_errors = 0;
        d.degraded_since = None;

        if d.first_checkpoint_advanced_at.is_none() {
            d.first_checkpoint_advanced_at = d.last_batch_at;
        }

        if d.first_ready_at.is_none() {
            let ready_at = Utc::now();
            d.first_ready_at = Some(ready_at);
            d.restart_recovery_seconds =
                Some((ready_at - d.started_at).num_milliseconds() as f64 / 1000.0);
        }
    }

    /// Record the latest replication slot lag sampled by the background poller.
    ///
    /// Called every 15 s from the admin side-channel task (PostgreSQL sources only).
    /// Pass `None` to signal that sampling failed so the metric reflects staleness.
    pub async fn record_slot_lag(&self, lag_bytes: Option<i64>) {
        self.data.write().await.replication_slot_lag_bytes = lag_bytes;
    }

    /// Record consecutive source poll errors for the `/readyz` health signal.
    ///
    /// Pass `0` to indicate recovery (called implicitly by `record_batch`).
    pub async fn record_source_consecutive_errors(&self, n: u64) {
        let mut d = self.data.write().await;
        let was_below = d.source_consecutive_errors < READYZ_SOURCE_CONSECUTIVE_ERROR_THRESHOLD;
        d.source_consecutive_errors = n;
        if n >= READYZ_SOURCE_CONSECUTIVE_ERROR_THRESHOLD && was_below {
            d.degraded_since = Some(std::time::Instant::now());
        } else if n < READYZ_SOURCE_CONSECUTIVE_ERROR_THRESHOLD {
            d.degraded_since = None;
        }
    }

    pub async fn set_state(&self, state: InstanceState) {
        let (previous, new_state, audit_entry) = {
            let mut d = self.data.write().await;
            let previous = d.state.clone();
            if matches!(state, InstanceState::Running) && d.first_ready_at.is_none() {
                let ready_at = Utc::now();
                d.first_ready_at = Some(ready_at);
                d.restart_recovery_seconds =
                    Some((ready_at - d.started_at).num_milliseconds() as f64 / 1000.0);
            }

            if matches!(previous, InstanceState::Stopping)
                && matches!(state, InstanceState::Stopped)
            {
                d.shutdown_completions_stopped_total =
                    d.shutdown_completions_stopped_total.saturating_add(1);
            }

            if matches!(previous, InstanceState::Stopping) && matches!(state, InstanceState::Error)
            {
                d.shutdown_completions_error_total =
                    d.shutdown_completions_error_total.saturating_add(1);
            }

            d.state = state;
            let new_state = d.state.clone();
            let audit_entry = append_audit_entry(
                &mut d,
                "admin_state_transition",
                "accepted",
                &format!("from={previous:?},to={new_state:?}"),
                None,
                None,
            );

            (previous, new_state, audit_entry)
        };

        self.persist_audit_entry(&audit_entry);
        tracing::info!(
            target: "rustcdc_audit",
            action = "admin_state_transition",
            from = ?previous,
            to = ?new_state,
            result = "accepted",
            "admin state transition"
        );
    }

    pub async fn record_shutdown_request_os_signal(&self) {
        let audit_entry = {
            let mut d = self.data.write().await;
            d.shutdown_requests_os_signal_total =
                d.shutdown_requests_os_signal_total.saturating_add(1);
            append_audit_entry(
                &mut d,
                "shutdown_requested",
                "accepted",
                "source=os_signal",
                None,
                None,
            )
        };
        self.persist_audit_entry(&audit_entry);
        tracing::info!(
            target: "rustcdc_audit",
            action = "shutdown_requested",
            source = "os_signal",
            result = "accepted",
            "admin shutdown request"
        );
    }

    async fn record_rate_limited_request(&self, scope: AbuseLimitScope) {
        let mut d = self.data.write().await;
        match scope {
            AbuseLimitScope::Readyz => {
                d.admin_rate_limited_readyz_total =
                    d.admin_rate_limited_readyz_total.saturating_add(1)
            }
            AbuseLimitScope::Status => {
                d.admin_rate_limited_status_total =
                    d.admin_rate_limited_status_total.saturating_add(1)
            }
            AbuseLimitScope::Metrics => {
                d.admin_rate_limited_metrics_total =
                    d.admin_rate_limited_metrics_total.saturating_add(1)
            }
        }
    }

    pub async fn record_readiness_probe(&self, ready: bool, latency: Duration) {
        let mut d = self.data.write().await;
        d.readiness_checks_total += 1;
        if ready {
            d.readiness_ready_total += 1;
        }
        let latency_ms = latency.as_millis() as u64;
        d.last_admin_api_latency_ms = Some(latency_ms);
        observe_admin_api_latency_histogram(&mut d, latency_ms);
    }

    pub async fn record_status_probe(&self, latency: Duration) {
        let mut d = self.data.write().await;
        let latency_ms = latency.as_millis() as u64;
        d.last_admin_api_latency_ms = Some(latency_ms);
        observe_admin_api_latency_histogram(&mut d, latency_ms);
    }

    pub async fn set_terminal_reason_code(&self, reason_code: &str) {
        self.data.write().await.last_terminal_reason_code = Some(reason_code.to_string());
    }

    pub async fn record_reconciliation_recovery(&self, parse_ok: bool, detail: &str) {
        let mut d = self.data.write().await;
        d.reconciliation_recoveries_total = d.reconciliation_recoveries_total.saturating_add(1);
        d.reconciliation_recovery_last_unix_seconds = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|dur| dur.as_secs_f64())
                .unwrap_or(0.0),
        );
        d.reconciliation_recovery_last_parse_ok = Some(parse_ok);
        d.reconciliation_recovery_last_detail = Some(detail.to_string());
    }

    pub async fn record_reconciliation_recovery_proof(&self, ok: bool, detail: &str) {
        let mut d = self.data.write().await;
        d.reconciliation_recovery_proof_last_unix_seconds = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|dur| dur.as_secs_f64())
                .unwrap_or(0.0),
        );
        d.reconciliation_recovery_proof_last_ok = Some(ok);
        d.reconciliation_recovery_proof_last_detail = Some(detail.to_string());
    }

    fn persist_notification_event(&self, event: &serde_json::Value) -> Result<(), AppError> {
        let Some(notification_log) = &self.notification_log else {
            return Ok(());
        };

        notification_log.append_json_value(event, "notification event")
    }

    async fn publish_non_admin_notification(&self, notification: &ControlNotification) -> bool {
        let mut emitted = 0_u64;
        let mut failures = 0_u64;

        if self.notification_log.is_some() {
            let event = notification_to_cloudevent_with_source(
                notification,
                "urn:cdc-server:notification-log",
            );
            match self.persist_notification_event(&event) {
                Ok(()) => {
                    emitted = emitted.saturating_add(1);
                    self.record_notification_channel_result("file", 1, 0).await;
                }
                Err(error) => {
                    failures = failures.saturating_add(1);
                    self.record_notification_channel_result("file", 0, 1).await;
                    tracing::error!(
                        target: "rustcdc_audit",
                        action = "notification_channel_emit_failed",
                        channel = "file",
                        signal_id = %notification.signal_id,
                        correlation_id = %notification.correlation_id,
                        action_type = %notification.action_type,
                        state = %notification.state,
                        error = %error,
                        "failed to persist notification event to file channel"
                    );
                }
            }
        }

        if let Some(notification_kafka) = &self.notification_kafka {
            let event = notification_to_cloudevent_with_source(
                notification,
                "urn:cdc-server:kafka-notifications",
            );
            match notification_kafka.send_event(&event).await {
                Ok(()) => {
                    emitted = emitted.saturating_add(1);
                    self.record_notification_channel_result("kafka", 1, 0).await;
                }
                Err(error) => {
                    failures = failures.saturating_add(1);
                    self.record_notification_channel_result("kafka", 0, 1).await;
                    tracing::error!(
                        target: "rustcdc_audit",
                        action = "notification_channel_emit_failed",
                        channel = "kafka",
                        signal_id = %notification.signal_id,
                        correlation_id = %notification.correlation_id,
                        action_type = %notification.action_type,
                        state = %notification.state,
                        error = %error,
                        "failed to emit notification event to kafka channel"
                    );
                }
            }
        }

        self.record_notification_log_emit_result(emitted, failures)
            .await;
        emitted > 0
    }

    async fn record_notification_log_emit_result(&self, emitted: u64, failures: u64) {
        if emitted == 0 && failures == 0 {
            return;
        }

        let mut d = self.data.write().await;
        d.notification_log_emitted_total = d.notification_log_emitted_total.saturating_add(emitted);
        d.notification_log_emit_failures_total = d
            .notification_log_emit_failures_total
            .saturating_add(failures);
    }

    async fn record_notification_channel_result(&self, channel: &str, emitted: u64, failures: u64) {
        let mut d = self.data.write().await;
        if emitted > 0 {
            let channel_total = d
                .notification_channel_emitted_total
                .entry(channel.to_string())
                .or_insert(0);
            *channel_total = channel_total.saturating_add(emitted);
        }
        if failures > 0 {
            let channel_failures = d
                .notification_channel_emit_failures_total
                .entry(channel.to_string())
                .or_insert(0);
            *channel_failures = channel_failures.saturating_add(failures);
        }
    }

    #[allow(clippy::too_many_arguments)] // internal plumbing; audit fields arrive individually
    async fn append_signal_lifecycle_entry(
        &self,
        signal_id: &str,
        correlation_id: &str,
        action_type: SignalActionType,
        lifecycle_action: &str,
        result: &str,
        state: &str,
        message: &str,
        traceparent: Option<String>,
        additional_data: serde_json::Value,
        actor_source_ip: Option<String>,
        actor_token_id: Option<String>,
    ) -> (AuditTrailEntry, bool) {
        let detail = serde_json::json!({
            "signal_id": signal_id,
            "correlation_id": correlation_id,
            "action_type": action_type.as_str(),
            "state": state,
            "message": message,
            "traceparent": traceparent,
            "additional_data": additional_data,
        })
        .to_string();

        let entry = {
            let mut data = self.data.write().await;
            append_audit_entry(
                &mut data,
                lifecycle_action,
                result,
                &detail,
                actor_source_ip,
                actor_token_id,
            )
        };

        self.persist_audit_entry(&entry);
        let notification_emitted = if let Some(notification) = notification_from_audit_entry(&entry)
        {
            self.publish_non_admin_notification(&notification).await
        } else {
            true
        };

        (entry, notification_emitted)
    }

    async fn process_queued_signal_action(&self, action: QueuedSignalAction) {
        let key = (
            action.signal_id.clone(),
            action.action_type.as_str().to_string(),
        );

        if let Some(in_progress_action) = action.action_type.in_progress_audit_action() {
            self.append_signal_lifecycle_entry(
                &action.signal_id,
                &action.correlation_id,
                action.action_type,
                in_progress_action,
                "in_progress",
                "IN_PROGRESS",
                &action.message,
                action.traceparent.clone(),
                serde_json::Value::Null,
                action.actor_source_ip.clone(),
                Some(action.actor_token_id.clone()),
            )
            .await;
        }

        self.append_signal_lifecycle_entry(
            &action.signal_id,
            &action.correlation_id,
            action.action_type,
            action.action_type.terminal_audit_action(),
            action.action_type.terminal_result(),
            action.action_type.terminal_state(),
            &action.message,
            action.traceparent,
            action.additional_data,
            action.actor_source_ip,
            Some(action.actor_token_id),
        )
        .await;

        self.signal_inflight.remove(&key);
    }
}

fn readiness_rate(data: &AdminStateData) -> f64 {
    if data.readiness_checks_total == 0 {
        1.0
    } else {
        data.readiness_ready_total as f64 / data.readiness_checks_total as f64
    }
}

fn latency_avg_ms(total: u64, sum_ms: f64) -> f64 {
    if total == 0 {
        0.0
    } else {
        sum_ms / total as f64
    }
}

fn observe_admin_api_latency_histogram(d: &mut AdminStateData, latency_ms: u64) {
    d.admin_api_latency_ms_sum += latency_ms as f64;
    d.admin_api_latency_ms_count += 1;
    for (i, &bound) in ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_MS.iter().enumerate() {
        if latency_ms <= bound {
            d.admin_api_latency_ms_buckets[i] += 1;
        }
    }
}

fn notification_channel_totals_json(totals: &HashMap<String, u64>) -> serde_json::Value {
    let mut ordered = serde_json::Map::new();
    let mut channels = totals.iter().collect::<Vec<_>>();
    channels.sort_by(|left, right| left.0.cmp(right.0));
    for (channel, total) in channels {
        ordered.insert(channel.clone(), serde_json::Value::from(*total));
    }
    serde_json::Value::Object(ordered)
}

fn notification_channel_metric_lines(
    metric_name: &str,
    help: &str,
    totals: &HashMap<String, u64>,
) -> String {
    let mut lines = String::new();
    lines.push_str(&format!("# HELP {metric_name} {help}\n"));
    lines.push_str(&format!("# TYPE {metric_name} counter\n"));

    let mut channels = totals.iter().collect::<Vec<_>>();
    channels.sort_by(|left, right| left.0.cmp(right.0));
    for (channel, total) in channels {
        lines.push_str(&format!("{metric_name}{{channel=\"{channel}\"}} {total}\n"));
    }

    lines
}

fn notification_channel_gauge_lines(
    metric_name: &str,
    help: &str,
    enabled: &HashMap<String, bool>,
) -> String {
    let mut lines = String::new();
    lines.push_str(&format!("# HELP {metric_name} {help}\n"));
    lines.push_str(&format!("# TYPE {metric_name} gauge\n"));

    let mut channels = enabled.iter().collect::<Vec<_>>();
    channels.sort_by(|left, right| left.0.cmp(right.0));
    for (channel, enabled) in channels {
        lines.push_str(&format!(
            "{metric_name}{{channel=\"{channel}\"}} {}\n",
            if *enabled { 1 } else { 0 }
        ));
    }

    lines
}

fn append_audit_entry(
    data: &mut AdminStateData,
    action: &str,
    result: &str,
    detail: &str,
    actor_source_ip: Option<String>,
    actor_token_id: Option<String>,
) -> AuditTrailEntry {
    // Truncate detail to prevent memory amplification from write-scope
    // tokens with large message payloads (CR-016).
    let detail = if detail.len() > AUDIT_DETAIL_MAX_BYTES {
        let mut truncated = detail[..AUDIT_DETAIL_MAX_BYTES].to_string();
        truncated.push_str(" [truncated]");
        truncated
    } else {
        detail.to_string()
    };
    let detail: &str = &detail;
    data.audit_entries_total = data.audit_entries_total.saturating_add(1);
    let sequence = data.audit_entries_total;
    let at = Utc::now();
    let prev_hash_hex = data
        .audit_recent_entries
        .last()
        .map(|entry| entry.entry_hash_hex.clone())
        .unwrap_or_else(|| "genesis".to_string());

    // Pseudonymise source IP before building the canonical string so the
    // signature covers the pseudonymised form — raw IPs are never written to
    // disk or held in the signed payload.
    let actor_source_ip = if data.audit_ip_pseudonymise {
        actor_source_ip.map(|ip| pseudonymise_ip(&ip, &data.audit_ip_salt))
    } else {
        actor_source_ip
    };

    let actor_source_ip_canonical = actor_source_ip.as_deref().unwrap_or("-");
    let actor_token_id_canonical = actor_token_id.as_deref().unwrap_or("-");
    let canonical = format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        sequence,
        at.to_rfc3339_opts(SecondsFormat::Millis, true),
        action,
        result,
        detail,
        actor_source_ip_canonical,
        actor_token_id_canonical,
        prev_hash_hex
    );
    let entry_hash_hex = hex::encode(Sha256::digest(canonical.as_bytes()));

    // Sign the canonical string with Ed25519 if a signing key is available.
    let ed25519_signature_hex = data.audit_signing_key.as_ref().map(|key| {
        let sig = key.sign(canonical.as_bytes());
        hex::encode(sig.to_bytes())
    });

    let entry = AuditTrailEntry {
        sequence,
        at,
        action: action.to_string(),
        result: result.to_string(),
        detail: detail.to_string(),
        actor_source_ip,
        actor_token_id,
        prev_hash_hex,
        entry_hash_hex,
        ed25519_signature_hex,
    };
    data.audit_recent_entries.push(entry.clone());

    if data.audit_recent_entries.len() > AUDIT_TRAIL_MAX_ENTRIES {
        let trim = data.audit_recent_entries.len() - AUDIT_TRAIL_MAX_ENTRIES;
        data.audit_recent_entries.drain(0..trim);
    }

    entry
}

fn load_audit_signing_key() -> Option<SigningKey> {
    let hex = std::env::var("CDC_AUDIT_SIGNING_KEY_HEX").ok()?;
    let hex = hex.trim();
    let bytes = hex::decode(hex).ok()?;
    let bytes: [u8; 32] = bytes.try_into().ok()?;
    Some(SigningKey::from_bytes(&bytes))
}

/// Resolve the 16-byte audit IP pseudonymisation salt.
///
/// Priority:
/// 1. If `admin.audit_ip_salt_env` names an env var whose value is ≥ 32 hex
///    chars, decode the first 16 bytes — provides stable cross-session
///    pseudonymisation.
/// 2. Otherwise generate 16 random bytes at startup — IPs from different
///    process lifetimes cannot be correlated (good for privacy, worse for
///    forensics).
fn resolve_audit_ip_salt(cfg: &crate::config::schema::AdminConfig) -> [u8; 16] {
    if let Some(env_name) = &cfg.audit_ip_salt_env {
        if let Ok(val) = std::env::var(env_name) {
            let val = val.trim();
            if val.len() >= 32 {
                if let Ok(bytes) = hex::decode(&val[..32]) {
                    if let Ok(arr) = <[u8; 16]>::try_from(bytes.as_slice()) {
                        tracing::debug!(
                            env_var = %env_name,
                            "audit IP pseudonymisation: stable salt loaded from env"
                        );
                        return arr;
                    }
                }
            }
            tracing::warn!(
                env_var = %env_name,
                "audit IP pseudonymisation: env salt is not valid 32-char hex \
                 — falling back to random ephemeral salt"
            );
        } else {
            tracing::warn!(
                env_var = %env_name,
                "audit IP pseudonymisation: env var not set \
                 — falling back to random ephemeral salt"
            );
        }
    }
    uuid::Uuid::new_v4().into_bytes()
}

/// Pseudonymise an IP address for audit log storage.
///
/// Returns the first 8 bytes of `SHA-256(salt || ip)` as 16 lowercase hex chars.
/// The output is deterministic for a given (salt, ip) pair and is not reversible.
///
/// Non-IP actor labels (e.g. the `signal_ingress_kafka` / `signal_ingress_file`
/// channel markers) are returned unchanged: they identify a machine channel, not
/// a person, so hashing them would erase audit legibility without any privacy
/// gain.
fn pseudonymise_ip(ip: &str, salt: &[u8; 16]) -> String {
    if ip.parse::<std::net::IpAddr>().is_err() {
        return ip.to_string();
    }
    let mut hasher = Sha256::new();
    hasher.update(salt);
    hasher.update(ip.as_bytes());
    let hash = hasher.finalize();
    hex::encode(&hash[..8])
}

fn elapsed_ms(started_at: DateTime<Utc>, marker: Option<DateTime<Utc>>) -> Option<u64> {
    marker.map(|ts| (ts - started_at).num_milliseconds().max(0) as u64)
}

fn load_token(var_name: Option<&str>, field_name: &str) -> Result<Option<String>, AppError> {
    let Some(var_name) = var_name else {
        return Ok(None);
    };

    let token = std::env::var(var_name).map_err(|_| {
        AppError::Other(format!(
            "{field_name} points to missing environment variable '{var_name}'"
        ))
    })?;

    if token.trim().is_empty() {
        return Err(AppError::Other(format!(
            "{field_name} references '{var_name}' but it is empty"
        )));
    }

    Ok(Some(token))
}

fn load_auth_state(config: &AppConfig) -> Result<AuthState, AppError> {
    if let Some(path) = config.admin.token_manifest_file.as_deref() {
        let trusted_public_keys = token_manifest_policy::parse_trusted_manifest_keys(
            &config.admin.token_manifest_trusted_public_keys_hex,
        )
        .map_err(AppError::Other)?;
        let (read_tokens, write_tokens) =
            load_auth_tokens_from_manifest(path, &trusted_public_keys)?;
        return Ok(AuthState {
            read_tokens,
            write_tokens,
            source: AuthSource::Manifest {
                path: path.to_path_buf(),
                trusted_public_keys,
                refresh_interval: Duration::from_millis(config.admin.token_manifest_refresh_ms),
                max_staleness: config
                    .admin
                    .token_manifest_max_staleness_ms
                    .map(Duration::from_millis),
            },
            manifest_version: 1,
            manifest_reload_ok: true,
            manifest_stale_blocked: false,
            last_reload_at: Some(SystemTime::now()),
            last_reload_error: None,
            last_observed_mtime: file_modified_time(path).ok().flatten(),
            last_refresh_attempt: None,
            revoked_token_hits_total: 0,
        });
    }

    let mut read_tokens = Vec::new();
    let mut write_tokens = Vec::new();

    if let Some(read_token) = load_token(
        config.admin.read_token_env.as_deref(),
        "admin.read_token_env",
    )? {
        read_tokens.push(AuthToken {
            id: "read-env".to_string(),
            token_sha256_hex: token_sha256_hex(&read_token),
            not_before: None,
            expires_at: None,
            revoked: false,
        });
    }

    if let Some(write_token) = load_token(
        config.admin.write_token_env.as_deref(),
        "admin.write_token_env",
    )? {
        write_tokens.push(AuthToken {
            id: "write-env".to_string(),
            token_sha256_hex: token_sha256_hex(&write_token),
            not_before: None,
            expires_at: None,
            revoked: false,
        });
    }

    Ok(AuthState {
        read_tokens,
        write_tokens,
        source: AuthSource::Env,
        manifest_version: 0,
        manifest_reload_ok: true,
        manifest_stale_blocked: false,
        last_reload_at: None,
        last_reload_error: None,
        last_observed_mtime: None,
        last_refresh_attempt: None,
        revoked_token_hits_total: 0,
    })
}

fn load_auth_tokens_from_manifest(
    path: &Path,
    trusted_public_keys: &[VerifyingKey],
) -> Result<(Vec<AuthToken>, Vec<AuthToken>), AppError> {
    let manifest = token_manifest_policy::load_signed_token_manifest(path, trusted_public_keys)
        .map_err(AppError::Other)?;

    let mut read_tokens = Vec::new();
    let mut write_tokens = Vec::new();

    for token in manifest.tokens {
        let auth = AuthToken {
            id: token.id,
            token_sha256_hex: token.token_sha256_hex.to_ascii_lowercase(),
            not_before: token.not_before,
            expires_at: token.expires_at,
            revoked: token.revoked,
        };

        let mut has_scope = false;
        for scope in token.scopes {
            match scope.trim().to_ascii_lowercase().as_str() {
                "read" => {
                    has_scope = true;
                    read_tokens.push(auth.clone());
                }
                "write" => {
                    has_scope = true;
                    write_tokens.push(auth.clone());
                    // Write-scoped tokens can also access read endpoints.
                    read_tokens.push(auth.clone());
                }
                _ => {}
            }
        }

        if !has_scope {
            return Err(AppError::Other(format!(
                "admin token '{}' has no recognized scopes in manifest {}",
                auth.id,
                path.display()
            )));
        }
    }

    if read_tokens.is_empty() {
        return Err(AppError::Other(format!(
            "admin token manifest {} must contain at least one token with a recognized read or write scope",
            path.display()
        )));
    }

    if write_tokens.is_empty() {
        return Err(AppError::Other(format!(
            "admin token manifest {} must contain at least one token with write scope",
            path.display()
        )));
    }

    Ok((read_tokens, write_tokens))
}

fn token_sha256_hex(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    hex::encode(digest)
}

fn constant_time_eq_str(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut diff = 0u8;
    for (a, b) in left.as_bytes().iter().zip(right.as_bytes().iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

fn file_modified_time(path: &Path) -> Result<Option<SystemTime>, AppError> {
    let meta = std::fs::metadata(path).map_err(|e| {
        AppError::Other(format!(
            "failed to stat admin token manifest {}: {e}",
            path.display()
        ))
    })?;
    Ok(meta.modified().ok())
}

impl AuthState {
    fn source_name(&self) -> &'static str {
        match self.source {
            AuthSource::Env => "env",
            AuthSource::Manifest { .. } => "manifest",
        }
    }

    fn maybe_refresh_manifest(&mut self) {
        let AuthSource::Manifest {
            ref path,
            ref trusted_public_keys,
            refresh_interval,
            max_staleness,
        } = self.source
        else {
            return;
        };

        let now_instant = Instant::now();
        if let Some(last) = self.last_refresh_attempt {
            if now_instant.duration_since(last) < refresh_interval {
                self.enforce_manifest_staleness(max_staleness);
                return;
            }
        }
        self.last_refresh_attempt = Some(now_instant);

        let current_mtime = file_modified_time(path).ok().flatten();
        let reload_needed =
            self.last_observed_mtime != current_mtime || self.last_reload_at.is_none();

        if reload_needed {
            match load_auth_tokens_from_manifest(path, trusted_public_keys) {
                Ok((read_tokens, write_tokens)) => {
                    self.read_tokens = read_tokens;
                    self.write_tokens = write_tokens;
                    self.manifest_version = self.manifest_version.saturating_add(1);
                    self.manifest_reload_ok = true;
                    self.last_reload_at = Some(SystemTime::now());
                    self.last_reload_error = None;
                    self.last_observed_mtime = current_mtime;
                }
                Err(e) => {
                    self.manifest_reload_ok = false;
                    self.last_reload_error = Some(e.to_string());
                }
            }
        }

        self.enforce_manifest_staleness(max_staleness);
    }

    fn enforce_manifest_staleness_from_source(&mut self) {
        let AuthSource::Manifest { max_staleness, .. } = self.source else {
            return;
        };

        self.enforce_manifest_staleness(max_staleness);
    }

    fn enforce_manifest_staleness(&mut self, max_staleness: Option<Duration>) {
        self.manifest_stale_blocked = false;
        let Some(max_staleness) = max_staleness else {
            return;
        };

        let Some(last_reload_at) = self.last_reload_at else {
            self.manifest_stale_blocked = true;
            return;
        };

        match SystemTime::now().duration_since(last_reload_at) {
            Ok(age) => {
                if age > max_staleness {
                    self.manifest_stale_blocked = true;
                }
            }
            Err(_) => {
                self.manifest_stale_blocked = true;
            }
        }
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let auth = headers.get(AUTHORIZATION)?.to_str().ok()?;
    auth.strip_prefix("Bearer ")
}

fn unauthorized_response() -> Response {
    tracing::warn!("unauthorized admin API request");
    (
        StatusCode::UNAUTHORIZED,
        [(axum::http::header::WWW_AUTHENTICATE, "Bearer")],
        "unauthorized",
    )
        .into_response()
}

fn rate_limited_response(endpoint: &str) -> Response {
    tracing::warn!(endpoint, "admin API request rate limited");
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(RETRY_AFTER, "1")],
        "too many requests",
    )
        .into_response()
}

#[derive(Debug, Clone, Serialize)]
struct ControlNotification {
    id: u64,
    at: DateTime<Utc>,
    signal_id: String,
    correlation_id: String,
    action_type: String,
    state: String,
    traceparent: Option<String>,
    detail: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SignalActionType {
    LogMarker,
    ExecuteSnapshot,
    PauseSnapshot,
    ResumeSnapshot,
    StopSnapshot,
}

impl SignalActionType {
    fn as_str(self) -> &'static str {
        match self {
            Self::LogMarker => "log_marker",
            Self::ExecuteSnapshot => "execute_snapshot",
            Self::PauseSnapshot => "pause_snapshot",
            Self::ResumeSnapshot => "resume_snapshot",
            Self::StopSnapshot => "stop_snapshot",
        }
    }

    fn started_audit_action(self) -> &'static str {
        match self {
            Self::LogMarker => "signal_log_marker_started",
            Self::ExecuteSnapshot => "signal_execute_snapshot_started",
            Self::PauseSnapshot => "signal_pause_snapshot_started",
            Self::ResumeSnapshot => "signal_resume_snapshot_started",
            Self::StopSnapshot => "signal_stop_snapshot_started",
        }
    }

    fn in_progress_audit_action(self) -> Option<&'static str> {
        match self {
            Self::ExecuteSnapshot => Some("signal_execute_snapshot_in_progress"),
            _ => None,
        }
    }

    fn queue_rejected_audit_action(self) -> &'static str {
        match self {
            Self::LogMarker => "signal_log_marker_aborted",
            Self::ExecuteSnapshot => "signal_execute_snapshot_aborted",
            Self::PauseSnapshot => "signal_pause_snapshot_aborted",
            Self::ResumeSnapshot => "signal_resume_snapshot_aborted",
            Self::StopSnapshot => "signal_stop_snapshot_aborted",
        }
    }

    fn terminal_audit_action(self) -> &'static str {
        match self {
            Self::LogMarker => "signal_log_marker_completed",
            Self::ExecuteSnapshot => "signal_execute_snapshot_completed",
            Self::PauseSnapshot => "signal_pause_snapshot_paused",
            Self::ResumeSnapshot => "signal_resume_snapshot_resumed",
            Self::StopSnapshot => "signal_stop_snapshot_aborted",
        }
    }

    fn terminal_state(self) -> &'static str {
        match self {
            Self::LogMarker => "COMPLETED",
            Self::ExecuteSnapshot => "COMPLETED",
            Self::PauseSnapshot => "PAUSED",
            Self::ResumeSnapshot => "RESUMED",
            Self::StopSnapshot => "ABORTED",
        }
    }

    fn terminal_result(self) -> &'static str {
        match self {
            Self::LogMarker | Self::ExecuteSnapshot => "completed",
            Self::PauseSnapshot => "paused",
            Self::ResumeSnapshot => "resumed",
            Self::StopSnapshot => "aborted",
        }
    }
}

#[derive(Debug, Deserialize)]
struct SignalActionRequest {
    signal_id: Option<String>,
    correlation_id: Option<String>,
    action_type: SignalActionType,
    message: Option<String>,
    #[serde(default)]
    additional_data: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignalIngressRecord {
    signal_id: Option<String>,
    correlation_id: Option<String>,
    action_type: SignalActionType,
    message: Option<String>,
    #[serde(default)]
    additional_data: Option<serde_json::Value>,
    #[serde(default)]
    traceparent: Option<String>,
}

fn normalize_optional_id(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

fn generated_signal_id() -> String {
    format!("signal-{}", Utc::now().timestamp_micros())
}

fn is_control_lifecycle_state(state: &str) -> bool {
    matches!(
        state,
        "STARTED"
            | "IN_PROGRESS"
            | "TABLE_SCAN_COMPLETED"
            | "PAUSED"
            | "RESUMED"
            | "COMPLETED"
            | "ABORTED"
            | "SKIPPED"
    )
}

fn is_control_action_type(action_type: &str) -> bool {
    matches!(
        action_type,
        "log_marker" | "execute_snapshot" | "pause_snapshot" | "resume_snapshot" | "stop_snapshot"
    )
}

fn notification_from_audit_entry(entry: &AuditTrailEntry) -> Option<ControlNotification> {
    if !entry.action.starts_with("signal_") {
        return None;
    }

    let detail = serde_json::from_str::<serde_json::Value>(&entry.detail).ok()?;
    let signal_id = detail.get("signal_id")?.as_str()?.to_string();
    let correlation_id = detail
        .get("correlation_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(signal_id.as_str())
        .to_string();
    let action_type = detail
        .get("action_type")
        .and_then(serde_json::Value::as_str)?;
    if !is_control_action_type(action_type) {
        return None;
    }
    let state = detail
        .get("state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_string();
    if !is_control_lifecycle_state(&state) {
        return None;
    }
    let traceparent = detail
        .get("traceparent")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);

    Some(ControlNotification {
        id: entry.sequence,
        at: entry.at,
        signal_id,
        correlation_id,
        action_type: action_type.to_string(),
        state,
        traceparent,
        detail,
    })
}

fn collect_control_notifications(data: &AdminStateData) -> Vec<ControlNotification> {
    data.audit_recent_entries
        .iter()
        .filter_map(notification_from_audit_entry)
        .collect()
}

fn notification_to_cloudevent(notification: &ControlNotification) -> serde_json::Value {
    notification_to_cloudevent_with_source(notification, "urn:cdc-server:admin:notifications")
}

fn notification_to_cloudevent_with_source(
    notification: &ControlNotification,
    source: &str,
) -> serde_json::Value {
    let mut event = serde_json::Map::new();
    event.insert(
        "id".to_string(),
        serde_json::Value::String(format!("{}", notification.id)),
    );
    event.insert(
        "source".to_string(),
        serde_json::Value::String(source.to_string()),
    );
    event.insert(
        "type".to_string(),
        serde_json::Value::String(format!(
            "cdc.signal.{}",
            notification.state.to_ascii_lowercase()
        )),
    );
    event.insert(
        "specversion".to_string(),
        serde_json::Value::String("1.0".to_string()),
    );
    event.insert(
        "time".to_string(),
        serde_json::Value::String(notification.at.to_rfc3339()),
    );
    event.insert(
        "datacontenttype".to_string(),
        serde_json::Value::String("application/json".to_string()),
    );
    event.insert(
        "signalid".to_string(),
        serde_json::Value::String(notification.signal_id.clone()),
    );
    event.insert(
        "correlationid".to_string(),
        serde_json::Value::String(notification.correlation_id.clone()),
    );
    event.insert(
        "actiontype".to_string(),
        serde_json::Value::String(notification.action_type.clone()),
    );
    if let Some(traceparent) = &notification.traceparent {
        event.insert(
            "traceparent".to_string(),
            serde_json::Value::String(traceparent.clone()),
        );
    }

    event.insert(
        "data".to_string(),
        serde_json::json!({
            "signal_id": notification.signal_id,
            "correlation_id": notification.correlation_id,
            "action_type": notification.action_type,
            "state": notification.state,
            "detail": notification.detail,
        }),
    );

    serde_json::Value::Object(event)
}

fn render_notifications_sse(notifications: &[ControlNotification]) -> String {
    let mut body = String::new();
    for notification in notifications {
        if let Ok(payload) = serde_json::to_string(notification) {
            body.push_str("event: control_notification\n");
            body.push_str("data: ");
            body.push_str(&payload);
            body.push_str("\n\n");
        }
    }
    body.push_str("event: stream_end\n");
    body.push_str("data: {\"status\":\"ok\"}\n\n");
    body
}

#[derive(Debug, Clone, Copy)]
struct SignalNotificationHealth {
    without_terminal_total: u64,
    duplicate_terminal_total: u64,
    lag_seconds: f64,
}

fn signal_terminal_state(state: &str) -> bool {
    matches!(
        state,
        "COMPLETED" | "ABORTED" | "SKIPPED" | "PAUSED" | "RESUMED"
    )
}

fn parse_signal_lifecycle_detail(detail: &str) -> Option<(String, String, String)> {
    let parsed = serde_json::from_str::<serde_json::Value>(detail).ok()?;
    let signal_id = parsed
        .get("signal_id")
        .and_then(serde_json::Value::as_str)?
        .to_string();
    let action_type = parsed
        .get("action_type")
        .and_then(serde_json::Value::as_str)?
        .to_string();
    if !is_control_action_type(&action_type) {
        return None;
    }
    let state = parsed
        .get("state")
        .and_then(serde_json::Value::as_str)?
        .to_string();
    Some((signal_id, action_type, state))
}

/// Lifecycle aggregate per `(signal_id, action_type)`: earliest STARTED
/// timestamp (if any) and count of terminal-state entries.
type SignalLifecycleAggregate = (Option<DateTime<Utc>>, u64);

fn signal_notification_health(
    data: &AdminStateData,
    now: DateTime<Utc>,
) -> SignalNotificationHealth {
    let mut lifecycle_by_signal: HashMap<(String, String), SignalLifecycleAggregate> =
        HashMap::new();
    let mut lag_samples = Vec::new();

    for entry in &data.audit_recent_entries {
        if !entry.action.starts_with("signal_") {
            continue;
        }

        let Some((signal_id, action_type, state)) = parse_signal_lifecycle_detail(&entry.detail)
        else {
            continue;
        };
        if !is_control_lifecycle_state(&state) {
            continue;
        }

        let key = (signal_id, action_type);
        let lifecycle = lifecycle_by_signal.entry(key).or_insert((None, 0));
        if state == "STARTED" {
            lifecycle.0 = Some(match lifecycle.0 {
                Some(started_at) => started_at.min(entry.at),
                None => entry.at,
            });
        }
        if signal_terminal_state(&state) {
            lifecycle.1 = lifecycle.1.saturating_add(1);
            if let Some(started_at) = lifecycle.0 {
                let lag = (entry.at - started_at).num_milliseconds().max(0) as f64 / 1000.0;
                lag_samples.push(lag);
            }
        }
    }

    let without_terminal_total = lifecycle_by_signal
        .values()
        .filter(|(started_at, terminal_count)| {
            started_at
                .map(|ts| (now - ts).num_seconds() > SIGNAL_TERMINAL_TIMEOUT_SECONDS)
                .unwrap_or(false)
                && *terminal_count == 0
        })
        .count() as u64;

    let duplicate_terminal_total = lifecycle_by_signal
        .values()
        .filter(|(_, terminal_count)| *terminal_count > 1)
        .count() as u64;

    let lag_seconds = lag_samples.into_iter().fold(0.0_f64, f64::max);

    SignalNotificationHealth {
        without_terminal_total,
        duplicate_terminal_total,
        lag_seconds,
    }
}

fn latest_signal_action_state(
    data: &AdminStateData,
    signal_id: &str,
    action_type: &str,
) -> Option<String> {
    data.audit_recent_entries.iter().rev().find_map(|entry| {
        if !entry.action.starts_with("signal_") {
            return None;
        }

        let Ok(detail) = serde_json::from_str::<serde_json::Value>(&entry.detail) else {
            return None;
        };

        let entry_signal_id = detail
            .get("signal_id")
            .and_then(serde_json::Value::as_str)?;
        let entry_action_type = detail
            .get("action_type")
            .and_then(serde_json::Value::as_str)?;
        let state = detail.get("state").and_then(serde_json::Value::as_str)?;

        if entry_signal_id == signal_id && entry_action_type == action_type {
            return Some(state.to_string());
        }

        None
    })
}

fn signal_action_requires_async_worker(action_type: SignalActionType) -> bool {
    !matches!(action_type, SignalActionType::LogMarker)
}

async fn signal_action(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(request): Json<SignalActionRequest>,
) -> Response {
    let Some(actor_token_id) = admin.authorize_write_token_id(&headers) else {
        return unauthorized_response();
    };

    let message = request.message.unwrap_or_default().trim().to_string();
    if matches!(request.action_type, SignalActionType::LogMarker) && message.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "message must not be empty for action_type=log_marker",
        )
            .into_response();
    }

    let signal_id =
        normalize_optional_id(request.signal_id.as_deref()).unwrap_or_else(generated_signal_id);
    let correlation_id = normalize_optional_id(request.correlation_id.as_deref())
        .unwrap_or_else(|| signal_id.clone());
    let traceparent = headers
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| normalize_optional_id(Some(value)));
    let envelope = SignalActionEnvelope {
        signal_id,
        correlation_id,
        action_type: request.action_type,
        message,
        traceparent,
        additional_data: request.additional_data.unwrap_or(serde_json::Value::Null),
        actor_source_ip: Some(peer_addr.ip().to_string()),
        actor_token_id: actor_token_id.clone(),
    };

    match admin.execute_signal_action_envelope(envelope).await {
        SignalActionExecutionResult::ExistingState {
            signal_id,
            correlation_id,
            action_type,
            current_state,
            expected_terminal_state,
        } => Json(serde_json::json!({
            "api_version": "v1",
            "signal_id": signal_id,
            "correlation_id": correlation_id,
            "action_type": action_type,
            "current_state": current_state,
            "expected_terminal_state": expected_terminal_state,
            "idempotent_replay": true,
        }))
        .into_response(),
        SignalActionExecutionResult::Started {
            signal_id,
            correlation_id,
            action_type,
            expected_terminal_state,
        } => {
            tracing::info!(
                target: "rustcdc_audit",
                action = "signal_action",
                signal_id = %signal_id,
                correlation_id = %correlation_id,
                action_type = %action_type,
                token_id = %actor_token_id,
                result = "accepted_async",
                "typed signal queued for async completion"
            );

            Json(serde_json::json!({
                "api_version": "v1",
                "signal_id": signal_id,
                "correlation_id": correlation_id,
                "action_type": action_type,
                "current_state": "STARTED",
                "expected_terminal_state": expected_terminal_state,
                "idempotent_replay": false,
            }))
            .into_response()
        }
        SignalActionExecutionResult::Terminal {
            signal_id,
            correlation_id,
            action_type,
            current_state,
            expected_terminal_state,
        } => {
            tracing::info!(
                target: "rustcdc_audit",
                action = "signal_action",
                signal_id = %signal_id,
                correlation_id = %correlation_id,
                action_type = %action_type,
                token_id = %actor_token_id,
                result = "accepted",
                "typed signal processed"
            );

            Json(serde_json::json!({
                "api_version": "v1",
                "signal_id": signal_id,
                "correlation_id": correlation_id,
                "action_type": action_type,
                "current_state": current_state,
                "expected_terminal_state": expected_terminal_state,
                "idempotent_replay": false,
            }))
            .into_response()
        }
        SignalActionExecutionResult::Aborted {
            signal_id,
            correlation_id,
            action_type,
            expected_terminal_state,
            error,
            retry_after_seconds,
        } => {
            let mut headers = HeaderMap::new();
            if let Some(retry_after) = retry_after_seconds {
                if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
                    headers.insert(RETRY_AFTER, value);
                }
            }

            (
                StatusCode::SERVICE_UNAVAILABLE,
                headers,
                Json(serde_json::json!({
                    "api_version": "v1",
                    "signal_id": signal_id,
                    "correlation_id": correlation_id,
                    "action_type": action_type,
                    "current_state": "ABORTED",
                    "expected_terminal_state": expected_terminal_state,
                    "idempotent_replay": false,
                    "error": error,
                })),
            )
                .into_response()
        }
    }
}

async fn notifications_authed(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (allowed, decision_latency) =
        admin.allow_abuse_scope(AbuseLimitScope::Status, &headers, Some(peer_addr));
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Status, decision_latency)
        .await;
    if !allowed {
        admin
            .record_rate_limited_request(AbuseLimitScope::Status)
            .await;
        return rate_limited_response("notifications");
    }

    if !admin.authorize_read(&headers) {
        return unauthorized_response();
    }

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    Json(serde_json::json!({
        "api_version": "v1",
        "notifications_total": notifications.len(),
        "notifications": notifications,
    }))
    .into_response()
}

async fn notifications_cloudevents_authed(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (allowed, decision_latency) =
        admin.allow_abuse_scope(AbuseLimitScope::Status, &headers, Some(peer_addr));
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Status, decision_latency)
        .await;
    if !allowed {
        admin
            .record_rate_limited_request(AbuseLimitScope::Status)
            .await;
        return rate_limited_response("notifications_cloudevents");
    }

    if !admin.authorize_read(&headers) {
        return unauthorized_response();
    }

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    let events = notifications
        .iter()
        .map(notification_to_cloudevent)
        .collect::<Vec<_>>();

    Json(serde_json::json!({
        "api_version": "v1",
        "format": "cloudevents",
        "events_total": events.len(),
        "events": events,
    }))
    .into_response()
}

async fn notifications_stream_authed(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (allowed, decision_latency) =
        admin.allow_abuse_scope(AbuseLimitScope::Status, &headers, Some(peer_addr));
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Status, decision_latency)
        .await;
    if !allowed {
        admin
            .record_rate_limited_request(AbuseLimitScope::Status)
            .await;
        return rate_limited_response("notifications_stream");
    }

    if !admin.authorize_read(&headers) {
        return unauthorized_response();
    }

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    let body = render_notifications_sse(&notifications);

    Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "text/event-stream")
        .header(axum::http::header::CACHE_CONTROL, "no-cache")
        .body(Body::from(body))
        .unwrap_or_else(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to build notifications SSE response",
            )
                .into_response()
        })
}

fn slo_reasons(data: &AdminStateData) -> Vec<String> {
    let mut reasons = Vec::new();
    let signal_health = signal_notification_health(data, Utc::now());
    match data.state {
        InstanceState::Starting => {
            reasons.push("pipeline starting; awaiting first ready batch".to_string())
        }
        InstanceState::Running => {}
        InstanceState::Stopping => reasons.push("shutdown requested".to_string()),
        InstanceState::Stopped => reasons.push("pipeline stopped".to_string()),
        InstanceState::Error => reasons.push("pipeline in error state".to_string()),
    }

    if let Some(age) = data.checkpoint_age_seconds {
        if age > 300.0 {
            reasons.push(format!("checkpoint age {age:.3}s exceeds 300s threshold"));
        }
    }

    if let Some(latency) = data.last_admin_api_latency_ms {
        if latency > 250 {
            reasons.push(format!(
                "admin API latency {latency}ms exceeds 250ms threshold"
            ));
        }
    }

    if readiness_rate(data) < 0.99 {
        reasons.push(format!(
            "readiness success rate {:.2}% below 99% threshold",
            readiness_rate(data) * 100.0
        ));
    }

    if let Some(recovery) = data.restart_recovery_seconds {
        if recovery > 30.0 {
            reasons.push(format!(
                "restart recovery {recovery:.3}s exceeds 30s threshold"
            ));
        }
    }

    if signal_health.without_terminal_total > 0 {
        reasons.push(format!(
            "{} accepted signal actions exceeded terminal notification timeout budget",
            signal_health.without_terminal_total
        ));
    }

    if data.signal_action_queue_rejections_total > 0 {
        reasons.push(format!(
            "{} async signal actions were rejected because the worker queue was saturated or unavailable",
            data.signal_action_queue_rejections_total
        ));
    }

    if data.signal_action_started_notification_rejections_total > 0 {
        reasons.push(format!(
            "{} signal actions were rejected because STARTED lifecycle notifications could not be emitted to non-admin channels",
            data.signal_action_started_notification_rejections_total
        ));
    }

    if data.signal_ingress_parse_rejections_total > 0 {
        reasons.push(format!(
            "{} signal ingress records were rejected due to malformed payloads",
            data.signal_ingress_parse_rejections_total
        ));
    }

    if data.signal_ingress_line_too_large_rejections_total > 0 {
        reasons.push(format!(
            "{} signal ingress records were rejected because they exceeded the line-size limit",
            data.signal_ingress_line_too_large_rejections_total
        ));
    }

    if data.signal_ingress_validation_rejections_total > 0 {
        reasons.push(format!(
            "{} signal ingress records were rejected by semantic validation",
            data.signal_ingress_validation_rejections_total
        ));
    }

    if !(data.notification_log_enabled || data.notification_kafka_enabled) {
        reasons.push("no non-admin notification channels are enabled".to_string());
    }

    if data.notification_log_emit_failures_total > 0 {
        reasons.push(format!(
            "{} non-admin notification lifecycle events failed to emit",
            data.notification_log_emit_failures_total
        ));
    }

    if signal_health.duplicate_terminal_total > 0 {
        reasons.push(format!(
            "{} signal actions emitted duplicate terminal lifecycle notifications",
            signal_health.duplicate_terminal_total
        ));
    }

    reasons
}

pub(crate) fn slo_json(data: &AdminStateData) -> serde_json::Value {
    let cold_start_to_ready_ms = elapsed_ms(data.started_at, data.first_ready_at);
    let cold_start_to_checkpoint_advance_ms =
        elapsed_ms(data.started_at, data.first_checkpoint_advanced_at);
    let signal_health = signal_notification_health(data, Utc::now());

    serde_json::json!({
        "readiness_checks_total": data.readiness_checks_total,
        "readiness_ready_total": data.readiness_ready_total,
        "readiness_rate": readiness_rate(data),
        "checkpoint_age_seconds": data.checkpoint_age_seconds,
        "source_lag_seconds": data.checkpoint_age_seconds,
        "admin_api_latency_ms": data.last_admin_api_latency_ms,
        "restart_recovery_seconds": data.restart_recovery_seconds,
        "cold_start_to_ready_ms": cold_start_to_ready_ms,
        "cold_start_to_checkpoint_advance_ms": cold_start_to_checkpoint_advance_ms,
        "shutdown": {
            "requests_os_signal_total": data.shutdown_requests_os_signal_total,
            "completions_stopped_total": data.shutdown_completions_stopped_total,
            "completions_error_total": data.shutdown_completions_error_total,
        },
        "signal_notifications": {
            "without_terminal_total": signal_health.without_terminal_total,
            "duplicate_terminal_total": signal_health.duplicate_terminal_total,
            "lag_seconds": signal_health.lag_seconds,
            "terminal_timeout_budget_seconds": SIGNAL_TERMINAL_TIMEOUT_SECONDS,
        },
        "signal_actions": {
            "queue_rejections_total": data.signal_action_queue_rejections_total,
            "started_notification_rejections_total": data.signal_action_started_notification_rejections_total,
            "queue_depth": data.signal_action_queue_depth,
        },
        "signal_ingress": {
            "parse_rejections_total": data.signal_ingress_parse_rejections_total,
            "line_too_large_rejections_total": data.signal_ingress_line_too_large_rejections_total,
            "validation_rejections_total": data.signal_ingress_validation_rejections_total,
            "max_line_bytes": SIGNAL_INGRESS_MAX_LINE_BYTES,
        },
        "notification_channels": {
            "enabled": {
                "file": data.notification_log_enabled,
                "kafka": data.notification_kafka_enabled,
            },
            "enabled_total": u64::from(data.notification_log_enabled)
                + u64::from(data.notification_kafka_enabled),
        },
        "notification_log": {
            "emitted_total": data.notification_log_emitted_total,
            "emit_failures_total": data.notification_log_emit_failures_total,
            "channels": {
                "emitted_total": notification_channel_totals_json(
                    &data.notification_channel_emitted_total,
                ),
                "emit_failures_total": notification_channel_totals_json(
                    &data.notification_channel_emit_failures_total,
                ),
            },
        },
        "abuse": {
            "rate_limited_readyz_total": data.admin_rate_limited_readyz_total,
            "rate_limited_status_total": data.admin_rate_limited_status_total,
            "rate_limited_metrics_total": data.admin_rate_limited_metrics_total,
            "rate_limiter_readyz_decisions_total": data.admin_rate_limiter_readyz_decisions_total,
            "rate_limiter_readyz_decision_latency_ms_avg": latency_avg_ms(
                data.admin_rate_limiter_readyz_decisions_total,
                data.admin_rate_limiter_readyz_decision_latency_ms_sum,
            ),
            "rate_limiter_readyz_decision_latency_ms_max": data.admin_rate_limiter_readyz_decision_latency_ms_max,
            "rate_limiter_status_decisions_total": data.admin_rate_limiter_status_decisions_total,
            "rate_limiter_status_decision_latency_ms_avg": latency_avg_ms(
                data.admin_rate_limiter_status_decisions_total,
                data.admin_rate_limiter_status_decision_latency_ms_sum,
            ),
            "rate_limiter_status_decision_latency_ms_max": data.admin_rate_limiter_status_decision_latency_ms_max,
            "rate_limiter_metrics_decisions_total": data.admin_rate_limiter_metrics_decisions_total,
            "rate_limiter_metrics_decision_latency_ms_avg": latency_avg_ms(
                data.admin_rate_limiter_metrics_decisions_total,
                data.admin_rate_limiter_metrics_decision_latency_ms_sum,
            ),
            "rate_limiter_metrics_decision_latency_ms_max": data.admin_rate_limiter_metrics_decision_latency_ms_max,
        },
        "reconciliation": {
            "recoveries_total": data.reconciliation_recoveries_total,
            "last_recovery_unix_seconds": data.reconciliation_recovery_last_unix_seconds,
            "last_recovery_parse_ok": data.reconciliation_recovery_last_parse_ok,
            "last_recovery_detail": data.reconciliation_recovery_last_detail,
            "last_proof_unix_seconds": data.reconciliation_recovery_proof_last_unix_seconds,
            "last_proof_ok": data.reconciliation_recovery_proof_last_ok,
            "last_proof_detail": data.reconciliation_recovery_proof_last_detail,
        },
        "last_terminal_reason_code": data.last_terminal_reason_code,
        "reasons": slo_reasons(data),
    })
}

pub(crate) fn slo_prometheus(data: &AdminStateData) -> String {
    let heartbeat_unix_seconds = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let state_code = match data.state {
        InstanceState::Starting => 0,
        InstanceState::Running => 1,
        InstanceState::Stopping => 2,
        InstanceState::Stopped => 3,
        InstanceState::Error => 4,
    };
    let terminal_reason_code = data.last_terminal_reason_code.as_deref().unwrap_or("none");
    let cold_start_to_ready_ms = elapsed_ms(data.started_at, data.first_ready_at).unwrap_or(0);
    let cold_start_to_checkpoint_advance_ms =
        elapsed_ms(data.started_at, data.first_checkpoint_advanced_at).unwrap_or(0);
    let signal_health = signal_notification_health(data, Utc::now());

    let mut metrics = format!(
        concat!(
            "# HELP rustcdc_slo_readiness_checks_total Total readiness probes\n",
            "# TYPE rustcdc_slo_readiness_checks_total counter\n",
            "rustcdc_slo_readiness_checks_total {}\n",
            "# HELP rustcdc_slo_readiness_ready_total Ready readiness probes\n",
            "# TYPE rustcdc_slo_readiness_ready_total counter\n",
            "rustcdc_slo_readiness_ready_total {}\n",
            "# HELP rustcdc_slo_readiness_rate Readiness success ratio\n",
            "# TYPE rustcdc_slo_readiness_rate gauge\n",
            "rustcdc_slo_readiness_rate {}\n",
            "# HELP rustcdc_slo_checkpoint_age_seconds Age of the checkpoint file in seconds\n",
            "# TYPE rustcdc_slo_checkpoint_age_seconds gauge\n",
            "rustcdc_slo_checkpoint_age_seconds {}\n",
            "# HELP rustcdc_source_lag_seconds Source lag proxy derived from checkpoint age in seconds\n",
            "# TYPE rustcdc_source_lag_seconds gauge\n",
            "rustcdc_source_lag_seconds {}\n",
            "# HELP rustcdc_slo_admin_api_latency_ms Last admin API latency in milliseconds\n",
            "# TYPE rustcdc_slo_admin_api_latency_ms gauge\n",
            "rustcdc_slo_admin_api_latency_ms {}\n",
            "# HELP rustcdc_slo_restart_recovery_seconds Time to first ready batch after start\n",
            "# TYPE rustcdc_slo_restart_recovery_seconds gauge\n",
            "rustcdc_slo_restart_recovery_seconds {}\n",
            "# HELP rustcdc_slo_cold_start_to_ready_ms Time from process start to first ready state in milliseconds\n",
            "# TYPE rustcdc_slo_cold_start_to_ready_ms gauge\n",
            "rustcdc_slo_cold_start_to_ready_ms {}\n",
            "# HELP rustcdc_slo_cold_start_to_checkpoint_advance_ms Time from process start to first durable checkpoint advance in milliseconds\n",
            "# TYPE rustcdc_slo_cold_start_to_checkpoint_advance_ms gauge\n",
            "rustcdc_slo_cold_start_to_checkpoint_advance_ms {}\n",
            "# HELP rustcdc_admin_control_plane_up Admin control-plane liveness signal\n",
            "# TYPE rustcdc_admin_control_plane_up gauge\n",
            "rustcdc_admin_control_plane_up 1\n",
            "# HELP rustcdc_admin_control_plane_heartbeat_unix_seconds Admin control-plane heartbeat timestamp\n",
            "# TYPE rustcdc_admin_control_plane_heartbeat_unix_seconds gauge\n",
            "rustcdc_admin_control_plane_heartbeat_unix_seconds {}\n",
            "# HELP rustcdc_admin_control_plane_state_code Admin control-plane state code (starting=0,running=1,stopping=2,stopped=3,error=4)\n",
            "# TYPE rustcdc_admin_control_plane_state_code gauge\n",
            "rustcdc_admin_control_plane_state_code {}\n",
            "# HELP rustcdc_admin_shutdown_requests_os_signal_total Total shutdown requests initiated by OS shutdown signals\n",
            "# TYPE rustcdc_admin_shutdown_requests_os_signal_total counter\n",
            "rustcdc_admin_shutdown_requests_os_signal_total {}\n",
            "# HELP rustcdc_admin_shutdown_completions_stopped_total Total shutdown lifecycles that completed with stopped state\n",
            "# TYPE rustcdc_admin_shutdown_completions_stopped_total counter\n",
            "rustcdc_admin_shutdown_completions_stopped_total {}\n",
            "# HELP rustcdc_admin_shutdown_completions_error_total Total shutdown lifecycles that terminated in error state\n",
            "# TYPE rustcdc_admin_shutdown_completions_error_total counter\n",
            "rustcdc_admin_shutdown_completions_error_total {}\n",
            "# HELP rustcdc_signal_without_terminal_notification_total Accepted signal actions without terminal lifecycle notifications beyond timeout budget\n",
            "# TYPE rustcdc_signal_without_terminal_notification_total gauge\n",
            "rustcdc_signal_without_terminal_notification_total {}\n",
            "# HELP rustcdc_signal_duplicate_terminal_notification_total Signal actions that emitted more than one terminal lifecycle notification\n",
            "# TYPE rustcdc_signal_duplicate_terminal_notification_total gauge\n",
            "rustcdc_signal_duplicate_terminal_notification_total {}\n",
            "# HELP rustcdc_signal_notification_lag_seconds Max observed lag in seconds between STARTED and terminal notification states\n",
            "# TYPE rustcdc_signal_notification_lag_seconds gauge\n",
            "rustcdc_signal_notification_lag_seconds {}\n",
            "# HELP rustcdc_signal_terminal_timeout_budget_seconds Timeout budget in seconds for terminal notification emission\n",
            "# TYPE rustcdc_signal_terminal_timeout_budget_seconds gauge\n",
            "rustcdc_signal_terminal_timeout_budget_seconds {}\n",
            "# HELP rustcdc_signal_action_queue_rejections_total Async signal actions rejected because the worker queue was full or unavailable\n",
            "# TYPE rustcdc_signal_action_queue_rejections_total counter\n",
            "rustcdc_signal_action_queue_rejections_total {}\n",
            "# HELP rustcdc_signal_action_started_notification_rejections_total Signal actions rejected because STARTED lifecycle notifications could not be emitted to non-admin channels\n",
            "# TYPE rustcdc_signal_action_started_notification_rejections_total counter\n",
            "rustcdc_signal_action_started_notification_rejections_total {}\n",
            "# HELP rustcdc_signal_action_queue_depth Current number of queued async signal actions waiting to be processed\n",
            "# TYPE rustcdc_signal_action_queue_depth gauge\n",
            "rustcdc_signal_action_queue_depth {}\n",
            "# HELP rustcdc_signal_ingress_parse_rejections_total Signal ingress records rejected because payload parsing failed\n",
            "# TYPE rustcdc_signal_ingress_parse_rejections_total counter\n",
            "rustcdc_signal_ingress_parse_rejections_total {}\n",
            "# HELP rustcdc_signal_ingress_line_too_large_rejections_total Signal ingress records rejected because line size exceeded the limit\n",
            "# TYPE rustcdc_signal_ingress_line_too_large_rejections_total counter\n",
            "rustcdc_signal_ingress_line_too_large_rejections_total {}\n",
            "# HELP rustcdc_signal_ingress_validation_rejections_total Signal ingress records rejected by semantic validation\n",
            "# TYPE rustcdc_signal_ingress_validation_rejections_total counter\n",
            "rustcdc_signal_ingress_validation_rejections_total {}\n",
            "# HELP rustcdc_signal_ingress_max_line_bytes Maximum accepted signal ingress line size in bytes\n",
            "# TYPE rustcdc_signal_ingress_max_line_bytes gauge\n",
            "rustcdc_signal_ingress_max_line_bytes {}\n",
            "# HELP rustcdc_signal_notification_log_emitted_total Notification lifecycle events emitted to non-admin log channel\n",
            "# TYPE rustcdc_signal_notification_log_emitted_total counter\n",
            "rustcdc_signal_notification_log_emitted_total {}\n",
            "# HELP rustcdc_signal_notification_log_emit_failures_total Notification lifecycle events that failed to emit to non-admin log channel\n",
            "# TYPE rustcdc_signal_notification_log_emit_failures_total counter\n",
            "rustcdc_signal_notification_log_emit_failures_total {}\n",
            "# HELP rustcdc_admin_rate_limited_readyz_total Total `/readyz` requests denied by admin abuse controls\n",
            "# TYPE rustcdc_admin_rate_limited_readyz_total counter\n",
            "rustcdc_admin_rate_limited_readyz_total {}\n",
            "# HELP rustcdc_admin_rate_limited_status_total Total `/status` requests denied by admin abuse controls\n",
            "# TYPE rustcdc_admin_rate_limited_status_total counter\n",
            "rustcdc_admin_rate_limited_status_total {}\n",
            "# HELP rustcdc_admin_rate_limited_metrics_total Total `/metrics` requests denied by admin abuse controls\n",
            "# TYPE rustcdc_admin_rate_limited_metrics_total counter\n",
            "rustcdc_admin_rate_limited_metrics_total {}\n",
            "# HELP rustcdc_admin_rate_limiter_readyz_decisions_total Total `/readyz` limiter decisions evaluated\n",
            "# TYPE rustcdc_admin_rate_limiter_readyz_decisions_total counter\n",
            "rustcdc_admin_rate_limiter_readyz_decisions_total {}\n",
            "# HELP rustcdc_admin_rate_limiter_readyz_decision_latency_ms_avg Average `/readyz` limiter decision latency in milliseconds\n",
            "# TYPE rustcdc_admin_rate_limiter_readyz_decision_latency_ms_avg gauge\n",
            "rustcdc_admin_rate_limiter_readyz_decision_latency_ms_avg {}\n",
            "# HELP rustcdc_admin_rate_limiter_readyz_decision_latency_ms_max Max `/readyz` limiter decision latency in milliseconds\n",
            "# TYPE rustcdc_admin_rate_limiter_readyz_decision_latency_ms_max gauge\n",
            "rustcdc_admin_rate_limiter_readyz_decision_latency_ms_max {}\n",
            "# HELP rustcdc_admin_rate_limiter_status_decisions_total Total `/status` limiter decisions evaluated\n",
            "# TYPE rustcdc_admin_rate_limiter_status_decisions_total counter\n",
            "rustcdc_admin_rate_limiter_status_decisions_total {}\n",
            "# HELP rustcdc_admin_rate_limiter_status_decision_latency_ms_avg Average `/status` limiter decision latency in milliseconds\n",
            "# TYPE rustcdc_admin_rate_limiter_status_decision_latency_ms_avg gauge\n",
            "rustcdc_admin_rate_limiter_status_decision_latency_ms_avg {}\n",
            "# HELP rustcdc_admin_rate_limiter_status_decision_latency_ms_max Max `/status` limiter decision latency in milliseconds\n",
            "# TYPE rustcdc_admin_rate_limiter_status_decision_latency_ms_max gauge\n",
            "rustcdc_admin_rate_limiter_status_decision_latency_ms_max {}\n",
            "# HELP rustcdc_admin_rate_limiter_metrics_decisions_total Total `/metrics` limiter decisions evaluated\n",
            "# TYPE rustcdc_admin_rate_limiter_metrics_decisions_total counter\n",
            "rustcdc_admin_rate_limiter_metrics_decisions_total {}\n",
            "# HELP rustcdc_admin_rate_limiter_metrics_decision_latency_ms_avg Average `/metrics` limiter decision latency in milliseconds\n",
            "# TYPE rustcdc_admin_rate_limiter_metrics_decision_latency_ms_avg gauge\n",
            "rustcdc_admin_rate_limiter_metrics_decision_latency_ms_avg {}\n",
            "# HELP rustcdc_admin_rate_limiter_metrics_decision_latency_ms_max Max `/metrics` limiter decision latency in milliseconds\n",
            "# TYPE rustcdc_admin_rate_limiter_metrics_decision_latency_ms_max gauge\n",
            "rustcdc_admin_rate_limiter_metrics_decision_latency_ms_max {}\n",
            "# HELP rustcdc_admin_reconciliation_recoveries_total Total startup reconciliation markers auto-recovered\n",
            "# TYPE rustcdc_admin_reconciliation_recoveries_total counter\n",
            "rustcdc_admin_reconciliation_recoveries_total {}\n",
            "# HELP rustcdc_admin_reconciliation_recovery_last_unix_seconds Last startup reconciliation recovery timestamp in unix seconds (-1 when unavailable)\n",
            "# TYPE rustcdc_admin_reconciliation_recovery_last_unix_seconds gauge\n",
            "rustcdc_admin_reconciliation_recovery_last_unix_seconds {}\n",
            "# HELP rustcdc_admin_reconciliation_recovery_last_parse_ok Whether the last recovered marker parsed successfully (1=true, 0=false, -1=unavailable)\n",
            "# TYPE rustcdc_admin_reconciliation_recovery_last_parse_ok gauge\n",
            "rustcdc_admin_reconciliation_recovery_last_parse_ok {}\n",
            "# HELP rustcdc_admin_reconciliation_recovery_proof_last_unix_seconds Last reconciliation recovery proof timestamp in unix seconds (-1 when unavailable)\n",
            "# TYPE rustcdc_admin_reconciliation_recovery_proof_last_unix_seconds gauge\n",
            "rustcdc_admin_reconciliation_recovery_proof_last_unix_seconds {}\n",
            "# HELP rustcdc_admin_reconciliation_recovery_proof_last_ok Whether the last reconciliation recovery proof succeeded (1=true, 0=false, -1=unavailable)\n",
            "# TYPE rustcdc_admin_reconciliation_recovery_proof_last_ok gauge\n",
            "rustcdc_admin_reconciliation_recovery_proof_last_ok {}\n",
            "# HELP rustcdc_admin_last_terminal_reason_code Last runtime terminal reason code (label value)\n",
            "# TYPE rustcdc_admin_last_terminal_reason_code gauge\n",
            "rustcdc_admin_last_terminal_reason_code{{reason=\"{}\"}} 1\n"
        ),
        data.readiness_checks_total,
        data.readiness_ready_total,
        readiness_rate(data),
        data.checkpoint_age_seconds.unwrap_or(0.0),
        data.checkpoint_age_seconds.unwrap_or(0.0),
        data.last_admin_api_latency_ms.unwrap_or(0),
        data.restart_recovery_seconds.unwrap_or(0.0),
        cold_start_to_ready_ms,
        cold_start_to_checkpoint_advance_ms,
        heartbeat_unix_seconds,
        state_code,
        data.shutdown_requests_os_signal_total,
        data.shutdown_completions_stopped_total,
        data.shutdown_completions_error_total,
        signal_health.without_terminal_total,
        signal_health.duplicate_terminal_total,
        signal_health.lag_seconds,
        SIGNAL_TERMINAL_TIMEOUT_SECONDS,
        data.signal_action_queue_rejections_total,
        data.signal_action_started_notification_rejections_total,
        data.signal_action_queue_depth,
        data.signal_ingress_parse_rejections_total,
        data.signal_ingress_line_too_large_rejections_total,
        data.signal_ingress_validation_rejections_total,
        SIGNAL_INGRESS_MAX_LINE_BYTES,
        data.notification_log_emitted_total,
        data.notification_log_emit_failures_total,
        data.admin_rate_limited_readyz_total,
        data.admin_rate_limited_status_total,
        data.admin_rate_limited_metrics_total,
        data.admin_rate_limiter_readyz_decisions_total,
        latency_avg_ms(
            data.admin_rate_limiter_readyz_decisions_total,
            data.admin_rate_limiter_readyz_decision_latency_ms_sum,
        ),
        data.admin_rate_limiter_readyz_decision_latency_ms_max,
        data.admin_rate_limiter_status_decisions_total,
        latency_avg_ms(
            data.admin_rate_limiter_status_decisions_total,
            data.admin_rate_limiter_status_decision_latency_ms_sum,
        ),
        data.admin_rate_limiter_status_decision_latency_ms_max,
        data.admin_rate_limiter_metrics_decisions_total,
        latency_avg_ms(
            data.admin_rate_limiter_metrics_decisions_total,
            data.admin_rate_limiter_metrics_decision_latency_ms_sum,
        ),
        data.admin_rate_limiter_metrics_decision_latency_ms_max,
        data.reconciliation_recoveries_total,
        data.reconciliation_recovery_last_unix_seconds.unwrap_or(-1.0),
        data.reconciliation_recovery_last_parse_ok.map(|ok| if ok { 1 } else { 0 }).unwrap_or(-1),
        data.reconciliation_recovery_proof_last_unix_seconds.unwrap_or(-1.0),
        data.reconciliation_recovery_proof_last_ok.map(|ok| if ok { 1 } else { 0 }).unwrap_or(-1),
        terminal_reason_code,
    );

    metrics.push_str(&notification_channel_metric_lines(
        "rustcdc_signal_notification_channel_emitted_total",
        "Notification lifecycle events emitted to non-admin notification channels by channel",
        &data.notification_channel_emitted_total,
    ));
    metrics.push_str(&notification_channel_metric_lines(
        "rustcdc_signal_notification_channel_emit_failures_total",
        "Notification lifecycle events that failed to emit to non-admin notification channels by channel",
        &data.notification_channel_emit_failures_total,
    ));

    // Replication slot lag — emitted as -1 when no sample has been taken yet
    // (non-PostgreSQL sources or before the first 15 s poll completes).
    // Use this in Prometheus `record_rules` and the `CdcReplicationSlotLagHigh`
    // alerting rule to catch dangerously high producer lag.
    {
        let lag = data.replication_slot_lag_bytes.unwrap_or(-1);
        metrics.push_str("# HELP rustcdc_source_replication_slot_lag_bytes WAL bytes not yet consumed by the CDC replication slot (-1 = not yet sampled or non-PostgreSQL source)\n");
        metrics.push_str("# TYPE rustcdc_source_replication_slot_lag_bytes gauge\n");
        metrics.push_str(&format!(
            "rustcdc_source_replication_slot_lag_bytes {lag}\n"
        ));
    }

    // Consecutive source poll errors — reflects the backoff-retry counter in
    // `RecoverableErrorState`.  A value ≥ READYZ_SOURCE_CONSECUTIVE_ERROR_THRESHOLD
    // means /readyz is already returning 503.
    {
        let n = data.source_consecutive_errors;
        metrics.push_str("# HELP rustcdc_source_consecutive_poll_errors Consecutive recoverable source poll errors since last successful batch (0 when healthy)\n");
        metrics.push_str("# TYPE rustcdc_source_consecutive_poll_errors gauge\n");
        metrics.push_str(&format!("rustcdc_source_consecutive_poll_errors {n}\n"));
    }

    let notification_channel_enabled = HashMap::from([
        ("file".to_string(), data.notification_log_enabled),
        ("kafka".to_string(), data.notification_kafka_enabled),
    ]);
    metrics.push_str(&notification_channel_gauge_lines(
        "rustcdc_signal_notification_channel_enabled",
        "Configured non-admin notification channels enabled by channel",
        &notification_channel_enabled,
    ));

    // Admin API latency histogram (for histogram_quantile / P50/P95/P99 in Prometheus).
    metrics.push_str("# HELP rustcdc_slo_admin_api_latency_ms_histogram Admin API request latency histogram in milliseconds\n");
    metrics.push_str("# TYPE rustcdc_slo_admin_api_latency_ms_histogram histogram\n");
    let mut cumulative: u64 = 0;
    for (i, &bound) in ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_MS.iter().enumerate() {
        cumulative += data.admin_api_latency_ms_buckets[i];
        metrics.push_str(&format!(
            "rustcdc_slo_admin_api_latency_ms_histogram_bucket{{le=\"{}\"}} {}\n",
            bound, cumulative
        ));
    }
    metrics.push_str(&format!(
        "rustcdc_slo_admin_api_latency_ms_histogram_bucket{{le=\"+Inf\"}} {}\n",
        data.admin_api_latency_ms_count
    ));
    metrics.push_str(&format!(
        "rustcdc_slo_admin_api_latency_ms_histogram_sum {}\n",
        data.admin_api_latency_ms_sum
    ));
    metrics.push_str(&format!(
        "rustcdc_slo_admin_api_latency_ms_histogram_count {}\n",
        data.admin_api_latency_ms_count
    ));

    metrics
}

pub(crate) fn runtime_metrics_prometheus(data: &AdminStateData) -> String {
    data.runtime_metrics.clone()
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

async fn healthz(State(_admin): State<AdminState>, _headers: HeaderMap) -> Response {
    (StatusCode::OK, "ok").into_response()
}

/// Kubernetes `livenessProbe` endpoint.
///
/// Returns `200 ok` when the process should continue running, `503` when it
/// should be restarted.  Unlike `/readyz` (which signals traffic readiness),
/// `/livez` signals process health:
///
/// - Returns `503` when `InstanceState::Error` (unrecoverable failure).
/// - Returns `503` when `source_consecutive_errors` has been at-or-above the
///   readiness threshold for longer than `LIVEZ_DEGRADED_TIMEOUT`.  This
///   catches pipelines stuck in indefinite circuit-breaker backoff that will
///   never self-heal.
///
/// Kubernetes recommended configuration:
/// ```yaml
/// livenessProbe:
///   httpGet:
///     path: /livez
///     port: <admin_port>
///   failureThreshold: 3
///   periodSeconds: 10
/// ```
const LIVEZ_DEGRADED_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes

async fn livez(State(admin): State<AdminState>) -> Response {
    let (state, source_consecutive_errors, degraded_since) = {
        let d = admin.data.read().await;
        (
            d.state.clone(),
            d.source_consecutive_errors,
            d.degraded_since,
        )
    };

    if matches!(state, InstanceState::Error) {
        return (StatusCode::SERVICE_UNAVAILABLE, "error").into_response();
    }

    if source_consecutive_errors >= READYZ_SOURCE_CONSECUTIVE_ERROR_THRESHOLD {
        if let Some(since) = degraded_since {
            if since.elapsed() > LIVEZ_DEGRADED_TIMEOUT {
                return (StatusCode::SERVICE_UNAVAILABLE, "source-degraded-timeout")
                    .into_response();
            }
        }
    }

    (StatusCode::OK, "alive").into_response()
}

async fn readyz(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (allowed, decision_latency) =
        admin.allow_abuse_scope(AbuseLimitScope::Readyz, &headers, Some(peer_addr));
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Readyz, decision_latency)
        .await;
    if !allowed {
        admin
            .record_rate_limited_request(AbuseLimitScope::Readyz)
            .await;
        return rate_limited_response("readyz");
    }

    if !admin.authorize_readyz(&headers) {
        return unauthorized_response();
    }

    let start = std::time::Instant::now();
    let (state, source_consecutive_errors) = {
        let d = admin.data.read().await;
        (d.state.clone(), d.source_consecutive_errors)
    };
    // Degraded: still Running but source poll errors have accumulated beyond the
    // readiness threshold.  Report 503 so Kubernetes removes us from the
    // load balancer before the circuit-breaker escalates to a terminal failure.
    let sink_degraded = matches!(state, InstanceState::Running)
        && source_consecutive_errors >= READYZ_SOURCE_CONSECUTIVE_ERROR_THRESHOLD;
    let ready = matches!(state, InstanceState::Running | InstanceState::Stopping) && !sink_degraded;
    admin.record_readiness_probe(ready, start.elapsed()).await;
    match state {
        InstanceState::Running if sink_degraded => {
            (StatusCode::SERVICE_UNAVAILABLE, "source-degraded").into_response()
        }
        InstanceState::Running => (StatusCode::OK, "ready").into_response(),
        InstanceState::Stopping => (StatusCode::OK, "stopping").into_response(),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response(),
    }
}

async fn status_authed(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (allowed, decision_latency) =
        admin.allow_abuse_scope(AbuseLimitScope::Status, &headers, Some(peer_addr));
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Status, decision_latency)
        .await;
    if !allowed {
        admin
            .record_rate_limited_request(AbuseLimitScope::Status)
            .await;
        return rate_limited_response("status");
    }

    if !admin.authorize_read(&headers) {
        return unauthorized_response();
    }

    let start = std::time::Instant::now();
    let data = admin.data.read().await;
    let snapshot = serde_json::json!({
        "api_version": "v1",
        "state": data.state,
        "started_at": data.started_at,
        "first_ready_at": data.first_ready_at,
        "first_checkpoint_advanced_at": data.first_checkpoint_advanced_at,
        "last_batch_at": data.last_batch_at,
        "events_processed": data.events_processed,
        "batches_processed": data.batches_processed,
        "slo": slo_json(&data),
        "audit": {
            "entries_total": data.audit_entries_total,
            "recent_entries": data.audit_recent_entries,
        },
        "auth": admin.auth_status_json(),
    });
    drop(data);
    admin.record_status_probe(start.elapsed()).await;
    Json(snapshot).into_response()
}

async fn metrics(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (allowed, decision_latency) =
        admin.allow_abuse_scope(AbuseLimitScope::Metrics, &headers, Some(peer_addr));
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Metrics, decision_latency)
        .await;
    if !allowed {
        admin
            .record_rate_limited_request(AbuseLimitScope::Metrics)
            .await;
        return rate_limited_response("metrics");
    }

    if !admin.authorize_read(&headers) {
        return unauthorized_response();
    }

    let data = admin.data.read().await;
    let prom = runtime_metrics_prometheus(&data);
    let slo_prom = slo_prometheus(&data);
    let auth_prom = admin.auth_prometheus();
    let audit_drop = admin
        .audit_log_drop_counter
        .as_ref()
        .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(0);
    let audit_prom = format!(
        "# HELP rustcdc_audit_log_drop_total Audit-log entries dropped because the write channel was full\n\
         # TYPE rustcdc_audit_log_drop_total counter\n\
         rustcdc_audit_log_drop_total {audit_drop}\n"
    );
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        format!("{prom}{slo_prom}{auth_prom}{audit_prom}"),
    )
        .into_response()
}

// ─────────────────────────────────────────────────────────────────────────────
// Router
// ─────────────────────────────────────────────────────────────────────────────

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/status", get(status_authed))
        .route("/metrics", get(metrics))
        .route("/signals", post(signal_action))
        .route("/notifications", get(notifications_authed))
        .route(
            "/notifications/cloudevents",
            get(notifications_cloudevents_authed),
        )
        .route("/notifications/stream", get(notifications_stream_authed))
        .with_state(state)
}

/// Spawn the admin HTTP server as a background tokio task.
///
/// Returns a `JoinHandle`; the server runs until the process exits.
pub async fn serve(
    bind_addr: &str,
    timeout_ms: u64,
    tls: Option<&AdminTlsConfig>,
    state: AdminState,
) -> Result<tokio::task::JoinHandle<()>, AppError> {
    let addr: std::net::SocketAddr = bind_addr
        .parse()
        .map_err(|e| AppError::Other(format!("invalid admin bind address '{bind_addr}': {e}")))?;

    state.spawn_manifest_refresh_worker();

    let app = router(state).layer(TimeoutLayer::with_status_code(
        StatusCode::REQUEST_TIMEOUT,
        Duration::from_millis(timeout_ms),
    ));
    let handle = if let Some(tls) = tls {
        let server_config = build_tls_server_config(tls)?;
        let rustls_config =
            axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(server_config));

        tracing::info!(admin_addr = %addr, "admin HTTPS server listening");

        tokio::spawn(async move {
            if let Err(e) = axum_server::bind_rustls(addr, rustls_config)
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await
            {
                tracing::error!(error = %e, "admin HTTPS server error");
            }
        })
    } else {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| AppError::Other(format!("failed to bind admin server to {addr}: {e}")))?;

        tracing::info!(admin_addr = %addr, "admin HTTP server listening");

        tokio::spawn(async move {
            if let Err(e) = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            {
                tracing::error!(error = %e, "admin HTTP server error");
            }
        })
    };

    Ok(handle)
}

fn build_tls_server_config(tls: &AdminTlsConfig) -> Result<rustls::ServerConfig, AppError> {
    let cert_chain = read_certs(&tls.cert_file)?;
    let private_key = read_private_key(&tls.key_file)?;

    let builder = rustls::ServerConfig::builder();
    let mut config = if tls.require_client_cert {
        let ca_path = tls.client_ca_file.as_ref().ok_or_else(|| {
            AppError::Other(
                "admin.tls.client_ca_file is required when require_client_cert=true".to_string(),
            )
        })?;
        let client_ca_certs = read_certs(ca_path)?;
        let mut roots = rustls::RootCertStore::empty();
        let (_, rejected) = roots.add_parsable_certificates(client_ca_certs);
        if rejected > 0 {
            return Err(AppError::Other(format!(
                "admin.tls.client_ca_file contains {rejected} invalid certificates"
            )));
        }

        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| {
                AppError::Other(format!("failed to build admin TLS client verifier: {e}"))
            })?;

        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| AppError::Other(format!("failed to build admin TLS config: {e}")))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| AppError::Other(format!("failed to build admin TLS config: {e}")))?
    };

    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

fn read_certs(
    path: &std::path::Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, AppError> {
    let file = std::fs::File::open(path).map_err(|e| {
        AppError::Other(format!(
            "failed to open certificate file {}: {e}",
            path.display()
        ))
    })?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            AppError::Other(format!(
                "failed to parse PEM certificates from {}: {e}",
                path.display()
            ))
        })?;

    if certs.is_empty() {
        return Err(AppError::Other(format!(
            "no certificates found in {}",
            path.display()
        )));
    }

    Ok(certs)
}

fn read_private_key(
    path: &std::path::Path,
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, AppError> {
    let mut reader = BufReader::new(std::fs::File::open(path).map_err(|e| {
        AppError::Other(format!(
            "failed to open private key file {}: {e}",
            path.display()
        ))
    })?);

    let mut pkcs8_keys = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            AppError::Other(format!(
                "failed to parse PKCS#8 private keys from {}: {e}",
                path.display()
            ))
        })?;

    if let Some(key) = pkcs8_keys.pop() {
        return Ok(rustls::pki_types::PrivateKeyDer::Pkcs8(key));
    }

    let mut reader = BufReader::new(std::fs::File::open(path).map_err(|e| {
        AppError::Other(format!(
            "failed to open private key file {}: {e}",
            path.display()
        ))
    })?);
    let mut rsa_keys = rustls_pemfile::rsa_private_keys(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            AppError::Other(format!(
                "failed to parse RSA private keys from {}: {e}",
                path.display()
            ))
        })?;

    if let Some(key) = rsa_keys.pop() {
        return Ok(rustls::pki_types::PrivateKeyDer::Pkcs1(key));
    }

    Err(AppError::Other(format!(
        "no supported private key found in {} (expected PKCS#8 or PKCS#1)",
        path.display()
    )))
}

#[cfg(test)]
mod tests {
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

    use super::{
        collect_control_notifications, file_modified_time, healthz, load_auth_tokens_from_manifest,
        notifications_authed, notifications_cloudevents_authed, notifications_stream_authed,
        readyz, runtime_metrics_prometheus, signal_action, slo_json, slo_prometheus,
        token_sha256_hex, AbuseLimitScope, AdminAbuseGuard, AdminScope, AdminState, AdminStateData,
        AuditTrailEntry, AuthSource, AuthState, AuthToken, InstanceState, SignalActionRequest,
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

    fn write_signed_manifest(
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
password = "secret"
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
            data.last_admin_api_latency_ms = Some(12);
            data.restart_recovery_seconds = Some(2.5);
            let started_at = data.started_at;
            data.first_ready_at = Some(started_at + ChronoDuration::milliseconds(900));
            data.first_checkpoint_advanced_at =
                Some(started_at + ChronoDuration::milliseconds(2_100));
        }

        let data = admin.data.read().await;
        let slo = slo_json(&data);

        assert_eq!(slo["readiness_checks_total"].as_u64(), Some(1));
        assert_eq!(slo["readiness_ready_total"].as_u64(), Some(1));
        assert_eq!(slo["source_lag_seconds"].as_f64(), Some(15.0));
        assert_eq!(slo["admin_api_latency_ms"].as_u64(), Some(12));
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
            slo["abuse"]["rate_limiter_metrics_decision_latency_ms_avg"].as_f64(),
            Some(2.0)
        );

        let metrics = slo_prometheus(&data);
        assert!(metrics.contains("rustcdc_admin_shutdown_requests_os_signal_total 1"));
        assert!(metrics.contains("rustcdc_admin_shutdown_completions_stopped_total 1"));
        assert!(metrics.contains("rustcdc_admin_shutdown_completions_error_total 1"));
        assert!(metrics.contains("rustcdc_admin_rate_limited_metrics_total 1"));
        assert!(metrics.contains("rustcdc_admin_rate_limiter_metrics_decisions_total 1"));
        assert!(metrics.contains("rustcdc_admin_rate_limiter_metrics_decision_latency_ms_avg 2"));
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
        assert!(metrics
            .contains("rustcdc_signal_notification_channel_emitted_total{channel=\"file\"} 3"));
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
            eprintln!(
                "skipping admin signal ingress kafka test (CDC_TEST_KAFKA_BROKERS is not set)"
            );
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

    #[tokio::test]
    async fn notifications_stream_renders_sse_events() {
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
            ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9000))),
            write_headers,
            Json(SignalActionRequest {
                signal_id: Some("sig-sse-1".to_string()),
                correlation_id: Some("corr-sse-1".to_string()),
                action_type: SignalActionType::LogMarker,
                message: Some("checkpoint marker".to_string()),
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
        let stream_response = notifications_stream_authed(
            State(admin),
            ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 8083))),
            read_headers,
        )
        .await;

        assert_eq!(stream_response.status(), StatusCode::OK);
        let content_type = stream_response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        assert!(content_type.contains("text/event-stream"));

        let body = to_bytes(stream_response.into_body(), usize::MAX)
            .await
            .expect("read stream body");
        let body = String::from_utf8(body.to_vec()).expect("utf8 stream body");
        assert!(body.contains("event: control_notification"));
        assert!(body.contains("\"signal_id\":\"sig-sse-1\""));
        assert!(body.contains("\"state\":\"STARTED\""));
        assert!(body.contains("\"state\":\"COMPLETED\""));
        assert!(body.contains("event: stream_end"));
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
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("valid cloudevents json");
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
        assert!(
            metrics.contains("rustcdc_signal_notification_channel_enabled{channel=\"kafka\"} 0")
        );
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
        assert!(metrics.contains(
            "rustcdc_signal_notification_channel_emit_failures_total{channel=\"file\"} 2"
        ));
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
        assert!(
            metrics.contains("rustcdc_signal_notification_channel_enabled{channel=\"kafka\"} 1")
        );
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
        sleep(Duration::from_millis(500)).await;

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
            .filter(|n| {
                n.signal_id == "sig-matrix-file-exec-1" && n.action_type == "execute_snapshot"
            })
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
            .filter(|n| {
                n.signal_id == "sig-matrix-kafka-exec-1" && n.action_type == "execute_snapshot"
            })
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
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("valid response json");
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
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("valid response json");
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
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("valid response json");
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
        assert!(reasons.iter().any(|reason| {
            reason.contains("STARTED lifecycle notifications could not be emitted")
        }));

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
                .process_signal_ingress_payload(
                    payload_bytes.as_slice(),
                    SignalIngressSource::Kafka,
                )
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
            dlq_path: None,
            dlq_max_bytes: 128 * 1024 * 1024,
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
                last_admin_api_latency_ms: None,
                admin_api_latency_ms_buckets: [0u64; 8],
                admin_api_latency_ms_sum: 0.0,
                admin_api_latency_ms_count: 0,
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
                admin_rate_limiter_readyz_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_readyz_decision_latency_ms_max: 0.0,
                admin_rate_limiter_status_decisions_total: 0,
                admin_rate_limiter_status_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_status_decision_latency_ms_max: 0.0,
                admin_rate_limiter_metrics_decisions_total: 0,
                admin_rate_limiter_metrics_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_metrics_decision_latency_ms_max: 0.0,
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
                last_admin_api_latency_ms: None,
                admin_api_latency_ms_buckets: [0u64; 8],
                admin_api_latency_ms_sum: 0.0,
                admin_api_latency_ms_count: 0,
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
                admin_rate_limiter_readyz_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_readyz_decision_latency_ms_max: 0.0,
                admin_rate_limiter_status_decisions_total: 0,
                admin_rate_limiter_status_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_status_decision_latency_ms_max: 0.0,
                admin_rate_limiter_metrics_decisions_total: 0,
                admin_rate_limiter_metrics_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_metrics_decision_latency_ms_max: 0.0,
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
                last_admin_api_latency_ms: None,
                admin_api_latency_ms_buckets: [0u64; 8],
                admin_api_latency_ms_sum: 0.0,
                admin_api_latency_ms_count: 0,
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
                admin_rate_limiter_readyz_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_readyz_decision_latency_ms_max: 0.0,
                admin_rate_limiter_status_decisions_total: 0,
                admin_rate_limiter_status_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_status_decision_latency_ms_max: 0.0,
                admin_rate_limiter_metrics_decisions_total: 0,
                admin_rate_limiter_metrics_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_metrics_decision_latency_ms_max: 0.0,
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
                last_admin_api_latency_ms: None,
                admin_api_latency_ms_buckets: [0u64; 8],
                admin_api_latency_ms_sum: 0.0,
                admin_api_latency_ms_count: 0,
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
                admin_rate_limiter_readyz_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_readyz_decision_latency_ms_max: 0.0,
                admin_rate_limiter_status_decisions_total: 0,
                admin_rate_limiter_status_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_status_decision_latency_ms_max: 0.0,
                admin_rate_limiter_metrics_decisions_total: 0,
                admin_rate_limiter_metrics_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_metrics_decision_latency_ms_max: 0.0,
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
                last_admin_api_latency_ms: None,
                admin_api_latency_ms_buckets: [0u64; 8],
                admin_api_latency_ms_sum: 0.0,
                admin_api_latency_ms_count: 0,
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
                admin_rate_limiter_readyz_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_readyz_decision_latency_ms_max: 0.0,
                admin_rate_limiter_status_decisions_total: 0,
                admin_rate_limiter_status_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_status_decision_latency_ms_max: 0.0,
                admin_rate_limiter_metrics_decisions_total: 0,
                admin_rate_limiter_metrics_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_metrics_decision_latency_ms_max: 0.0,
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
                last_admin_api_latency_ms: None,
                admin_api_latency_ms_buckets: [0u64; 8],
                admin_api_latency_ms_sum: 0.0,
                admin_api_latency_ms_count: 0,
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
                admin_rate_limiter_readyz_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_readyz_decision_latency_ms_max: 0.0,
                admin_rate_limiter_status_decisions_total: 0,
                admin_rate_limiter_status_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_status_decision_latency_ms_max: 0.0,
                admin_rate_limiter_metrics_decisions_total: 0,
                admin_rate_limiter_metrics_decision_latency_ms_sum: 0.0,
                admin_rate_limiter_metrics_decision_latency_ms_max: 0.0,
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
            audit_log_drop_counter: None,
        };

        assert!(admin.authorize_readyz(&HeaderMap::new()));
    }
}
