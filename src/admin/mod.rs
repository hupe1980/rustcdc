mod auth;
mod rate_limit;
mod tls;

use auth::{
    bearer_token, constant_time_eq_str, load_auth_state, rate_limited_response, token_sha256_hex,
    unauthorized_response, AdminScope, AuthSource, AuthState,
};

use axum::{
    extract::{ConnectInfo, State},
    http::StatusCode,
    http::{header::RETRY_AFTER, HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use dashmap::DashMap;
use ed25519_dalek::{Signer, SigningKey};
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
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, Mutex, RwLock};
use tower_http::timeout::TimeoutLayer;

use crate::commands::run_metrics::RuntimeMetricsSnapshot;
use crate::config::{
    schema::AdminNotificationKafkaConfig, schema::AdminProbeAuthMode,
    schema::AdminSignalIngressKafkaConfig, schema::AdminTlsConfig, AppConfig,
};
use crate::error::AppError;
use crate::redaction::redact_secrets;

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
/// Mirrors `LATENCY_HISTOGRAM_BUCKETS_US` in `commands/run_metrics.rs`.
/// Admin API latency bounds in **microseconds**, exported as seconds.
///
/// A `/readyz` handler returns in tens of microseconds. Against the previous
/// millisecond bounds every request landed in `le="1"` and the histogram could not
/// distinguish a healthy probe from one 100x slower — the same defect as the pipeline
/// latency histograms.
const ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US: [u64; 10] = [
    100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000,
];

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
    pub last_admin_api_latency_us: Option<u64>,
    /// Cumulative histogram of admin API request latencies (microseconds).
    /// Bucket upper bounds match `ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US`.
    pub admin_api_latency_us_buckets: [u64; ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US.len()],
    pub admin_api_latency_us_sum: f64,
    pub admin_api_latency_us_count: u64,
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
    pub admin_rate_limiter_readyz_decision_latency_seconds_sum: f64,
    pub admin_rate_limiter_readyz_decision_latency_seconds_max: f64,
    pub admin_rate_limiter_status_decisions_total: u64,
    pub admin_rate_limiter_status_decision_latency_seconds_sum: f64,
    pub admin_rate_limiter_status_decision_latency_seconds_max: f64,
    pub admin_rate_limiter_metrics_decisions_total: u64,
    pub admin_rate_limiter_metrics_decision_latency_seconds_sum: f64,
    pub admin_rate_limiter_metrics_decision_latency_seconds_max: f64,
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
    /// batch generation counter advances. Skips re-allocation on
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
/// by submitting signals with arbitrarily long `message` payloads.
/// At 512 entries × 4 KiB per detail the ring buffer is bounded to ~2 MiB.
const AUDIT_DETAIL_MAX_BYTES: usize = 4096;
const SIGNAL_TERMINAL_TIMEOUT_SECONDS: i64 = 30;
const SIGNAL_ACTION_QUEUE_CAPACITY: usize = 128;
const SIGNAL_INGRESS_MAX_LINE_BYTES: usize = 1024 * 1024;
/// Number of consecutive source poll errors that must accumulate before `/readyz`
/// returns `503 Service Unavailable`.
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
            .connect_timeout(crate::sink::kafka_connect_timeout(Duration::from_millis(
                config.ack_timeout_ms,
            )))
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
        let metadata = producer.send_record(record).await.map_err(|err| {
            AppError::Other(format!(
                "failed to emit notification event to admin.notification_kafka topic {}: {err}",
                self.topic
            ))
        })?;
        crate::sink::enforce_durable_confirmation(&metadata, "notification")
            .map_err(|e| AppError::Other(e.to_string()))?;
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
    workers: AdminWorkers,
    /// Live notification fan-out for `/notifications/stream`.
    ///
    /// Bounded: a subscriber that falls behind is told how many events it missed
    /// (`RecvError::Lagged`) rather than being allowed to pin them in memory. The stream
    /// forwards that as an explicit `lagged` SSE event, so a slow client learns it has a
    /// gap instead of silently receiving an incomplete sequence.
    notification_broadcast: Arc<tokio::sync::broadcast::Sender<ControlNotification>>,
}

/// How many notifications a slow `/notifications/stream` subscriber may fall behind
/// before it is told it has lost events.
const NOTIFICATION_STREAM_BUFFER: usize = 256;

/// Interval between SSE keep-alive comments.
///
/// Load-bearing for anything behind a proxy: an idle `text/event-stream` with no traffic
/// is indistinguishable from a dead connection, and most proxies close it after 30–60 s.
const NOTIFICATION_STREAM_KEEPALIVE: Duration = Duration::from_secs(15);

/// How long each admin worker gets to observe the shutdown watch and return before it is
/// abandoned. Generous enough to cover a Kafka poll at its default timeout, short enough
/// that a wedged broker cannot hold the process open.
const ADMIN_WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Lifecycle handle for the admin background workers.
///
/// Each of the four workers used to be a `std::thread::spawn` running a private
/// `current_thread` tokio runtime in an unconditional `loop`. That cost four OS threads
/// and four independent timer and I/O drivers per process, and — because nothing held a
/// handle and no loop had an exit condition — none of them could be stopped. They ran
/// until the process died.
///
/// Two consequences beyond the waste. Shutdown was not orderly: the signal-action worker
/// could still be mutating `AdminStateData` while the pipeline was finalising, so the
/// counters reported at exit raced the workers producing them. And every test that
/// constructed an `AdminState` leaked four OS threads and four runtimes for the lifetime
/// of the test binary, which is why the suite's thread count grew with the number of
/// admin tests rather than with its concurrency.
///
/// They are now ordinary `tokio::spawn` tasks on the ambient runtime, each selecting on a
/// shutdown watch, with their handles retained so [`AdminState::shutdown_workers`] can
/// stop them deterministically. In tests they need no shutdown at all: dropping the test
/// runtime cancels them.
#[derive(Clone)]
struct AdminWorkers {
    shutdown_tx: Arc<tokio::sync::watch::Sender<bool>>,
    handles: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl AdminWorkers {
    fn new() -> Self {
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Self {
            shutdown_tx: Arc::new(shutdown_tx),
            handles: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn subscribe(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    fn track(&self, handle: tokio::task::JoinHandle<()>) {
        if let Ok(mut handles) = self.handles.lock() {
            handles.push(handle);
        }
    }
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
                last_admin_api_latency_us: None,
                admin_api_latency_us_buckets: [0u64; ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US.len()],
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
                notification_log_enabled: notification_log.is_some(),
                notification_kafka_enabled: notification_kafka.is_some(),
                notification_log_emitted_total: 0,
                notification_log_emit_failures_total: 0,
                notification_channel_emitted_total: HashMap::new(),
                notification_channel_emit_failures_total: HashMap::new(),
                audit_entries_total: 0,
                audit_recent_entries: Vec::new(),
                audit_signing_key: load_audit_signing_key(&config.admin),
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
            workers: AdminWorkers::new(),
            notification_broadcast: Arc::new(
                tokio::sync::broadcast::channel(NOTIFICATION_STREAM_BUFFER).0,
            ),
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

    /// Periodically sweep stale entries from all rate-limit maps.
    ///
    /// The insert-time eviction path only runs when a new key tries to join a
    /// full map.  Under a sustained attack from rotating IPs the map fills,
    /// eviction stops, and legitimate clients can be blocked.  This background
    /// task sweeps every `RATE_LIMIT_STALE_CLIENT_TTL / 2` regardless of map
    /// size, keeping the maps current.
    /// Stop the background workers and wait for them to finish.
    ///
    /// Called once, on the pipeline's way out. Ordering matters: the signal-action worker
    /// mutates `AdminStateData`, so leaving it running while shutdown counters are
    /// written means the numbers reported at exit race the worker producing them.
    ///
    /// Each worker is given `AdminWorkerShutdownTimeout` to observe the watch and return;
    /// one that does not — a Kafka poll wedged on an unresponsive broker — is aborted
    /// rather than allowed to hold up process exit. Nothing here is on a durability path:
    /// the workers ingest signals and sweep rate-limit maps, so an abort loses at most an
    /// unprocessed signal, which the sender retries.
    pub async fn shutdown_workers(&self) {
        // A send failure means every receiver is already gone, which is the state we want.
        let _ = self.workers.shutdown_tx.send(true);

        let handles: Vec<_> = match self.workers.handles.lock() {
            Ok(mut handles) => std::mem::take(&mut *handles),
            Err(_) => return,
        };

        for handle in handles {
            match tokio::time::timeout(ADMIN_WORKER_SHUTDOWN_TIMEOUT, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(join_error)) if join_error.is_cancelled() => {}
                Ok(Err(join_error)) => {
                    tracing::warn!(error = %join_error, "admin worker panicked before shutdown")
                }
                Err(_) => tracing::warn!(
                    timeout_ms = ADMIN_WORKER_SHUTDOWN_TIMEOUT.as_millis(),
                    "admin worker did not stop within its shutdown budget; abandoning it"
                ),
            }
        }
    }

    fn spawn_rate_limit_sweep_worker(&self) {
        let abuse_guard = Arc::clone(&self.abuse_guard);
        let mut shutdown = self.workers.subscribe();
        self.workers.track(tokio::spawn(async move {
            let sweep_interval = RATE_LIMIT_STALE_CLIENT_TTL / 2;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(sweep_interval) => abuse_guard.sweep_stale_entries(),
                    _ = shutdown.changed() => break,
                }
            }
        }));
    }

    fn spawn_signal_action_worker(&self, mut rx: mpsc::Receiver<QueuedSignalAction>) {
        let worker_state = self.clone();
        let mut shutdown = self.workers.subscribe();
        self.workers.track(tokio::spawn(async move {
            loop {
                let action = tokio::select! {
                    action = rx.recv() => match action {
                        Some(action) => action,
                        None => break,
                    },
                    _ = shutdown.changed() => break,
                };
                {
                    let mut data = worker_state.data.write().await;
                    data.signal_action_queue_depth =
                        data.signal_action_queue_depth.saturating_sub(1);
                }
                worker_state.process_queued_signal_action(action).await;
            }
        }));
    }

    fn spawn_signal_ingress_file_worker(&self, path: PathBuf) {
        let worker_state = self.clone();
        let mut shutdown = self.workers.subscribe();
        self.workers.track(tokio::spawn(async move {
            {
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

                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                        _ = shutdown.changed() => break,
                    }
                }
            }
        }));
    }

    fn spawn_signal_ingress_kafka_worker(&self, config: AdminSignalIngressKafkaConfig) {
        let worker_state = self.clone();
        let mut shutdown = self.workers.subscribe();
        self.workers.track(tokio::spawn(async move {
            {
                // Every reconnect backoff races the shutdown watch, so a stop request is
                // honoured within the current poll rather than after a full backoff.
                macro_rules! backoff_or_stop {
                    ($d:expr) => {
                        tokio::select! {
                            _ = tokio::time::sleep($d) => {}
                            _ = shutdown.changed() => break,
                        }
                    };
                }

                loop {
                    if *shutdown.borrow_and_update() {
                        break;
                    }
                    let auth = match config.security.to_auth_config() {
                        Ok(auth) => auth,
                        Err(error) => {
                            tracing::error!(
                                target: "rustcdc_audit",
                                action = "signal_ingress_kafka_init_failed",
                                error = %error,
                                "invalid signal ingress kafka security configuration"
                            );
                            backoff_or_stop!(Duration::from_secs(1));
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
                        .connect_timeout(crate::sink::kafka_connect_timeout(Duration::from_millis(
                            1_000,
                        )))
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
                            backoff_or_stop!(Duration::from_secs(1));
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
                        backoff_or_stop!(Duration::from_secs(1));
                        continue;
                    }

                    loop {
                        if *shutdown.borrow_and_update() {
                            break;
                        }
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
                    backoff_or_stop!(Duration::from_secs(1));
                }
            }
        }));
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

            // Reserve the queue-depth slot BEFORE handing the action to the
            // worker. The worker decrements on dequeue from a separate thread;
            // incrementing after `try_send` races it — the worker's
            // `saturating_sub(1)` can land on a still-zero counter, floor at 0,
            // and the late increment then strands the gauge at 1 forever.
            {
                let mut data = self.data.write().await;
                data.signal_action_queue_depth = data.signal_action_queue_depth.saturating_add(1);
            }

            match self.signal_action_tx.try_send(queued_action) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(queued_action)) => {
                    // The action never entered the queue — release the slot.
                    {
                        let mut data = self.data.write().await;
                        data.signal_action_queue_depth =
                            data.signal_action_queue_depth.saturating_sub(1);
                    }
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
                    // The action never entered the queue — release the slot.
                    {
                        let mut data = self.data.write().await;
                        data.signal_action_queue_depth =
                            data.signal_action_queue_depth.saturating_sub(1);
                    }
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
        // Every audit append funnels through here, which makes it the one place that sees
        // a notification the instant it exists. `/notifications/stream` subscribes to this
        // broadcast; without it the endpoint could only ever re-read the ring buffer.
        if let Some(notification) = notification_from_audit_entry(entry) {
            // `send` fails only when there are no subscribers, which is the normal case.
            let _ = self.notification_broadcast.send(notification);
        }

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
        let latency_seconds = latency.as_secs_f64();
        match scope {
            AbuseLimitScope::Readyz => {
                d.admin_rate_limiter_readyz_decisions_total = d
                    .admin_rate_limiter_readyz_decisions_total
                    .saturating_add(1);
                d.admin_rate_limiter_readyz_decision_latency_seconds_sum += latency_seconds;
                d.admin_rate_limiter_readyz_decision_latency_seconds_max = d
                    .admin_rate_limiter_readyz_decision_latency_seconds_max
                    .max(latency_seconds);
            }
            AbuseLimitScope::Status => {
                d.admin_rate_limiter_status_decisions_total = d
                    .admin_rate_limiter_status_decisions_total
                    .saturating_add(1);
                d.admin_rate_limiter_status_decision_latency_seconds_sum += latency_seconds;
                d.admin_rate_limiter_status_decision_latency_seconds_max = d
                    .admin_rate_limiter_status_decision_latency_seconds_max
                    .max(latency_seconds);
            }
            AbuseLimitScope::Metrics => {
                d.admin_rate_limiter_metrics_decisions_total = d
                    .admin_rate_limiter_metrics_decisions_total
                    .saturating_add(1);
                d.admin_rate_limiter_metrics_decision_latency_seconds_sum += latency_seconds;
                d.admin_rate_limiter_metrics_decision_latency_seconds_max = d
                    .admin_rate_limiter_metrics_decision_latency_seconds_max
                    .max(latency_seconds);
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
        let latency_us = latency.as_micros() as u64;
        d.last_admin_api_latency_us = Some(latency_us);
        observe_admin_api_latency_histogram(&mut d, latency_us);
    }

    pub async fn record_status_probe(&self, latency: Duration) {
        let mut d = self.data.write().await;
        let latency_us = latency.as_micros() as u64;
        d.last_admin_api_latency_us = Some(latency_us);
        observe_admin_api_latency_histogram(&mut d, latency_us);
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

fn latency_avg_seconds(total: u64, sum_ms: f64) -> f64 {
    if total == 0 {
        0.0
    } else {
        sum_ms / total as f64
    }
}

fn observe_admin_api_latency_histogram(d: &mut AdminStateData, latency_us: u64) {
    d.admin_api_latency_us_sum += latency_us as f64;
    d.admin_api_latency_us_count += 1;
    for (i, &bound) in ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US.iter().enumerate() {
        if latency_us <= bound {
            d.admin_api_latency_us_buckets[i] += 1;
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
    // tokens with large message payloads.
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

/// Resolve the Ed25519 key used to sign audit-trail exports.
///
/// `admin.audit_signing_key_env` names the variable; it used to be ignored entirely
/// while this function read a hardcoded `CDC_AUDIT_SIGNING_KEY_HEX`. An operator who
/// configured the documented key got **unsigned audit records and no error** — the
/// setting was accepted, serialised into `/status`, and never consulted. Unsigned
/// audit trails are exactly what an audit trail exists to rule out, so every way this
/// can end up without a key now says so at `warn`.
fn load_audit_signing_key(cfg: &crate::config::schema::AdminConfig) -> Option<SigningKey> {
    let env_name = cfg.audit_signing_key_env.as_deref()?;
    let Ok(hex) = std::env::var(env_name) else {
        tracing::warn!(
            env_var = %env_name,
            "admin.audit_signing_key_env names an unset variable — audit trail exports \
             will be unsigned"
        );
        return None;
    };

    let decoded = hex::decode(hex.trim())
        .ok()
        .and_then(|bytes| <[u8; 32]>::try_from(bytes.as_slice()).ok());

    match decoded {
        Some(bytes) => Some(SigningKey::from_bytes(&bytes)),
        None => {
            // Deliberately no length or prefix in the message: a diagnostic that
            // describes the shape of a private key is a diagnostic that leaks it.
            tracing::warn!(
                env_var = %env_name,
                "audit signing key is not 64 hex characters (32 bytes) — audit trail \
                 exports will be unsigned"
            );
            None
        }
    }
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

    // Subscribe *before* reading the backlog. The other order has a hole: a notification
    // raised between the snapshot and the subscription belongs to neither, and the client
    // never sees it.
    let live = admin.notification_broadcast.subscribe();

    // `Last-Event-ID` is how EventSource resumes after a dropped connection — the browser
    // replays it automatically. Ignoring it, as the previous handler did, meant every
    // reconnect re-delivered the entire ring buffer.
    let resume_after = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok());

    let backlog: Vec<ControlNotification> = {
        let data = admin.data.read().await;
        collect_control_notifications(&data)
            .into_iter()
            .filter(|notification| resume_after.is_none_or(|after| notification.id > after))
            .collect()
    };

    let stream = futures::stream::unfold(
        (backlog.into_iter(), live, resume_after),
        |(mut backlog, mut live, mut last_id)| async move {
            if let Some(notification) = backlog.next() {
                last_id = Some(notification.id);
                let event = notification_sse_event(&notification);
                return Some((
                    Ok::<_, std::convert::Infallible>(event),
                    (backlog, live, last_id),
                ));
            }

            loop {
                match live.recv().await {
                    Ok(notification) => {
                        // The backlog and the live channel overlap by construction: a
                        // notification raised between the subscribe and the snapshot read
                        // appears in both. Suppressing anything not newer than the last id
                        // emitted is what makes the stream exactly-once for the client.
                        if last_id.is_some_and(|seen| notification.id <= seen) {
                            continue;
                        }
                        last_id = Some(notification.id);
                        let event = notification_sse_event(&notification);
                        return Some((Ok(event), (backlog, live, last_id)));
                    }
                    // Tell the client it has a gap rather than let it believe the sequence
                    // is complete. A silent gap in a control-plane feed is worse than a
                    // slow one.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                        let event = axum::response::sse::Event::default()
                            .event("lagged")
                            .data(missed.to_string());
                        return Some((Ok(event), (backlog, live, last_id)));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    );

    axum::response::sse::Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::new().interval(NOTIFICATION_STREAM_KEEPALIVE))
        .into_response()
}

/// Render one notification as an SSE event.
///
/// The `id` is the audit sequence, which is what makes `Last-Event-ID` resumption work:
/// it is monotonic, gapless and already the identity the audit trail uses.
fn notification_sse_event(notification: &ControlNotification) -> axum::response::sse::Event {
    axum::response::sse::Event::default()
        .id(notification.id.to_string())
        .event("notification")
        .json_data(notification_to_cloudevent(notification))
        .unwrap_or_else(|error| {
            axum::response::sse::Event::default()
                .event("error")
                .data(format!("failed to encode notification: {error}"))
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

    if let Some(latency_us) = data.last_admin_api_latency_us {
        // 250 ms, now expressed in the unit the field actually carries. The threshold
        // was `latency > 250` against a value that used to be milliseconds; leaving it
        // after the switch to microseconds would have fired on every request over
        // 250 us — i.e. permanently.
        if latency_us > 250_000 {
            let latency_ms = latency_us as f64 / 1_000.0;
            reasons.push(format!(
                "admin API latency {latency_ms:.1}ms exceeds 250ms threshold"
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
        "admin_api_latency_us": data.last_admin_api_latency_us,
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
            "rate_limiter_readyz_decision_latency_seconds_avg": latency_avg_seconds(
                data.admin_rate_limiter_readyz_decisions_total,
                data.admin_rate_limiter_readyz_decision_latency_seconds_sum,
            ),
            "rate_limiter_readyz_decision_latency_seconds_max": data.admin_rate_limiter_readyz_decision_latency_seconds_max,
            "rate_limiter_status_decisions_total": data.admin_rate_limiter_status_decisions_total,
            "rate_limiter_status_decision_latency_seconds_avg": latency_avg_seconds(
                data.admin_rate_limiter_status_decisions_total,
                data.admin_rate_limiter_status_decision_latency_seconds_sum,
            ),
            "rate_limiter_status_decision_latency_seconds_max": data.admin_rate_limiter_status_decision_latency_seconds_max,
            "rate_limiter_metrics_decisions_total": data.admin_rate_limiter_metrics_decisions_total,
            "rate_limiter_metrics_decision_latency_seconds_avg": latency_avg_seconds(
                data.admin_rate_limiter_metrics_decisions_total,
                data.admin_rate_limiter_metrics_decision_latency_seconds_sum,
            ),
            "rate_limiter_metrics_decision_latency_seconds_max": data.admin_rate_limiter_metrics_decision_latency_seconds_max,
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
            "# HELP rustcdc_slo_admin_api_latency_seconds Last admin API latency in seconds\n",
            "# TYPE rustcdc_slo_admin_api_latency_seconds gauge\n",
            "rustcdc_slo_admin_api_latency_seconds {}\n",
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
            "# HELP rustcdc_admin_rate_limiter_readyz_decision_latency_seconds_avg Average `/readyz` limiter decision latency in seconds\n",
            "# TYPE rustcdc_admin_rate_limiter_readyz_decision_latency_seconds_avg gauge\n",
            "rustcdc_admin_rate_limiter_readyz_decision_latency_seconds_avg {}\n",
            "# HELP rustcdc_admin_rate_limiter_readyz_decision_latency_seconds_max Max `/readyz` limiter decision latency in seconds\n",
            "# TYPE rustcdc_admin_rate_limiter_readyz_decision_latency_seconds_max gauge\n",
            "rustcdc_admin_rate_limiter_readyz_decision_latency_seconds_max {}\n",
            "# HELP rustcdc_admin_rate_limiter_status_decisions_total Total `/status` limiter decisions evaluated\n",
            "# TYPE rustcdc_admin_rate_limiter_status_decisions_total counter\n",
            "rustcdc_admin_rate_limiter_status_decisions_total {}\n",
            "# HELP rustcdc_admin_rate_limiter_status_decision_latency_seconds_avg Average `/status` limiter decision latency in seconds\n",
            "# TYPE rustcdc_admin_rate_limiter_status_decision_latency_seconds_avg gauge\n",
            "rustcdc_admin_rate_limiter_status_decision_latency_seconds_avg {}\n",
            "# HELP rustcdc_admin_rate_limiter_status_decision_latency_seconds_max Max `/status` limiter decision latency in seconds\n",
            "# TYPE rustcdc_admin_rate_limiter_status_decision_latency_seconds_max gauge\n",
            "rustcdc_admin_rate_limiter_status_decision_latency_seconds_max {}\n",
            "# HELP rustcdc_admin_rate_limiter_metrics_decisions_total Total `/metrics` limiter decisions evaluated\n",
            "# TYPE rustcdc_admin_rate_limiter_metrics_decisions_total counter\n",
            "rustcdc_admin_rate_limiter_metrics_decisions_total {}\n",
            "# HELP rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_avg Average `/metrics` limiter decision latency in seconds\n",
            "# TYPE rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_avg gauge\n",
            "rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_avg {}\n",
            "# HELP rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_max Max `/metrics` limiter decision latency in seconds\n",
            "# TYPE rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_max gauge\n",
            "rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_max {}\n",
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
        data.last_admin_api_latency_us.unwrap_or(0) as f64 / 1_000_000.0,
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
        latency_avg_seconds(
            data.admin_rate_limiter_readyz_decisions_total,
            data.admin_rate_limiter_readyz_decision_latency_seconds_sum,
        ),
        data.admin_rate_limiter_readyz_decision_latency_seconds_max,
        data.admin_rate_limiter_status_decisions_total,
        latency_avg_seconds(
            data.admin_rate_limiter_status_decisions_total,
            data.admin_rate_limiter_status_decision_latency_seconds_sum,
        ),
        data.admin_rate_limiter_status_decision_latency_seconds_max,
        data.admin_rate_limiter_metrics_decisions_total,
        latency_avg_seconds(
            data.admin_rate_limiter_metrics_decisions_total,
            data.admin_rate_limiter_metrics_decision_latency_seconds_sum,
        ),
        data.admin_rate_limiter_metrics_decision_latency_seconds_max,
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
    metrics.push_str("# HELP rustcdc_slo_admin_api_latency_seconds_histogram Admin API request latency histogram in seconds\n");
    metrics.push_str("# TYPE rustcdc_slo_admin_api_latency_seconds_histogram histogram\n");
    let mut cumulative: u64 = 0;
    for (i, &bound) in ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US.iter().enumerate() {
        cumulative += data.admin_api_latency_us_buckets[i];
        metrics.push_str(&format!(
            "rustcdc_slo_admin_api_latency_seconds_histogram_bucket{{le=\"{}\"}} {}\n",
            bound as f64 / 1_000_000.0,
            cumulative
        ));
    }
    metrics.push_str(&format!(
        "rustcdc_slo_admin_api_latency_seconds_histogram_bucket{{le=\"+Inf\"}} {}\n",
        data.admin_api_latency_us_count
    ));
    metrics.push_str(&format!(
        "rustcdc_slo_admin_api_latency_seconds_histogram_sum {}\n",
        data.admin_api_latency_us_sum / 1_000_000.0
    ));
    metrics.push_str(&format!(
        "rustcdc_slo_admin_api_latency_seconds_histogram_count {}\n",
        data.admin_api_latency_us_count
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
        let server_config = tls::build_tls_server_config(tls)?;
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

#[cfg(test)]
mod tests;
