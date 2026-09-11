mod auth;
mod notify;
mod openapi;
mod probes;
mod prometheus;
mod rate_limit;
mod signal_ledger;
mod signals;
mod tls;

// The probe handlers live in `probes` but are referenced unqualified by `router()`
// and by the test modules, which is how they were written when they sat in this file.
use probes::{healthz, livez, readyz};

use auth::{
    AdminScope, AuthSource, AuthState, bearer_token, constant_time_eq_str, load_auth_state,
    rate_limited_response, token_sha256_hex, unauthorized_response,
};

use axum::{
    Json, Router,
    extract::{ConnectInfo, State},
    http::StatusCode,
    http::{HeaderMap, HeaderValue, header::RETRY_AFTER},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, SecondsFormat, Utc};
use dashmap::DashMap;
use ed25519_dalek::{Signer, SigningKey};
use futures::FutureExt as _;
use krafka::consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use krafka::producer::{Acks, Producer, ProducerRecord};
use notify::*;
use prometheus::*;
use rate_limit::{AbuseLimitScope, AdminAbuseGuard, RATE_LIMIT_STALE_CLIENT_TTL};
use rustcdc::core::StallCause;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use signals::*;
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock, mpsc};
use tower_http::timeout::TimeoutLayer;

use crate::config::{
    AppConfig, schema::AdminNotificationKafkaConfig, schema::AdminProbeAuthMode,
    schema::AdminSignalIngressKafkaConfig, schema::AdminTlsConfig,
};
use crate::error::AppError;
use crate::redaction::redact_secrets;
use crate::runtime::metrics::RuntimeMetricsSnapshot;

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
    /// The runtime's latest health verdict, as its stable label.
    ///
    /// Kept structured rather than only rendered into `runtime_metrics`: the probes below
    /// have to *act* on this, and a Prometheus text blob is not something to parse in a
    /// liveness handler.
    pub health_verdict: Option<String>,
    /// Which signal produced a `stalled` verdict, or `None` for any other verdict.
    pub health_stall_cause: Option<String>,
    /// Instant at which the runtime first reported the *current* stall.
    ///
    /// Reset whenever the verdict or its cause changes, so the elapsed time below always
    /// describes one continuous condition rather than an accumulation of unrelated ones.
    #[serde(skip)]
    pub stalled_since: Option<std::time::Instant>,
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
    /// On-demand snapshot requests the runtime accepted.
    ///
    /// Counted separately from the signal lifecycle because the audit trail is bounded to
    /// 512 entries and lives only in memory: without these an operator scraping metrics
    /// cannot tell a pipeline that has been asked to backfill four tables from one that
    /// has been asked and refused every time.
    pub snapshot_requests_accepted_total: u64,
    /// On-demand snapshot requests the runtime refused, for any reason.
    pub snapshot_requests_refused_total: u64,
    /// Tables the runtime actually enqueued across all accepted requests.
    ///
    /// Not the same as the request count: one request carries many tables, and a table
    /// already in progress is a no-op that the runtime does not re-enqueue.
    pub snapshot_tables_enqueued_total: u64,
    /// Whether this pipeline can service an on-demand snapshot at all.
    ///
    /// Reported so an operator can find out *before* firing a signal rather than by
    /// reading an `ABORTED` notification afterwards — the request is asynchronous, so the
    /// `POST` answers `STARTED` either way. It is `false` for any pipeline with no
    /// `[incremental_snapshot]` section.
    pub snapshot_requests_available: bool,
    pub notification_log_enabled: bool,
    pub notification_kafka_enabled: bool,
    pub notification_log_emitted_total: u64,
    pub notification_log_emit_failures_total: u64,
    pub notification_channel_emitted_total: HashMap<String, u64>,
    pub notification_channel_emit_failures_total: HashMap<String, u64>,
    pub audit_entries_total: u64,
    pub audit_recent_entries: Vec<AuditTrailEntry>,
    /// Signal actions that panicked and were recovered.
    ///
    /// Non-zero means a bug was hit *and* survived. It must be visible: the alternative is
    /// inferring it from `signal_actions_without_terminal_total`, which fires 30 s later
    /// and names a different thing.
    pub signal_worker_panics_total: u64,
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

/// How often the Kafka ingress loop re-reads a dispatched signal's lifecycle state.
///
/// Short enough that a fast action (the common case — `execute_snapshot` returns once the
/// tables are enqueued, not once they are read) does not add perceptible latency to the
/// offset commit, long enough not to spin on the state lock.
const SIGNAL_TERMINAL_POLL_INTERVAL: Duration = Duration::from_millis(50);
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

#[derive(Debug, Clone)]
struct QueuedSignalAction {
    signal_id: String,
    correlation_id: String,
    action_type: SignalActionType,
    message: String,
    traceparent: Option<String>,
    additional_data: serde_json::Value,
    /// Fully-qualified `"schema.table"` names an `execute_snapshot` applies to.
    tables: Vec<String>,
    /// Per-request row filters, overriding `incremental_snapshot.table_conditions`.
    conditions: BTreeMap<String, String>,
    actor_source_ip: Option<String>,
    actor_token_id: String,
}

/// How long a queued `execute_snapshot` waits for the event loop to answer.
///
/// The loop picks requests up between polls, so the healthy wait is bounded by
/// `runtime.max_poll_wait_ms` plus one batch's delivery — well under a second in any
/// ordinary configuration. This ceiling exists so a wedged pipeline reports a timeout
/// instead of pinning a signal-worker slot forever.
///
/// **Derived from [`SIGNAL_TERMINAL_TIMEOUT_SECONDS`] rather than chosen.** That constant
/// is the budget after which a `STARTED` signal with no terminal state is counted as
/// unhealthy and surfaces in the SLO reasons. A request timeout *above* it would make an
/// ordinary in-flight snapshot look like a stuck worker, and the operator would be paged
/// for a request that was about to answer. Staying inside it means the timeout always
/// produces an explicit `ABORTED` — with a diagnostic naming the pipeline — before the
/// health check has anything to complain about.
const SNAPSHOT_REQUEST_TIMEOUT: Duration =
    Duration::from_secs(SIGNAL_TERMINAL_TIMEOUT_SECONDS as u64 - 5);

const _: () = assert!(
    SNAPSHOT_REQUEST_TIMEOUT.as_secs() < SIGNAL_TERMINAL_TIMEOUT_SECONDS as u64,
    "a snapshot request must time out before an unterminated signal is reported unhealthy"
);

#[derive(Debug, Clone)]
struct SignalActionEnvelope {
    signal_id: String,
    correlation_id: String,
    action_type: SignalActionType,
    message: String,
    traceparent: Option<String>,
    additional_data: serde_json::Value,
    tables: Vec<String>,
    /// Per-request row filters, already validated against `tables`.
    conditions: BTreeMap<String, String>,
    actor_source_ip: Option<String>,
    actor_token_id: String,
}

#[derive(Debug, Clone, Copy)]
enum SignalIngressSource {
    File,
    Kafka,
    Source,
}

impl AdminStateData {
    /// Latch the runtime's verdict, tracking how long the *current* stall has lasted.
    ///
    /// `stalled_since` restarts whenever the verdict or its cause changes, because the
    /// probes below ask "has this one condition persisted?" — not "how long has something
    /// been wrong?". A pipeline flapping between two different stalls is a different
    /// situation from one wedged in a single state, and only the second is a restart
    /// candidate.
    fn record_health_verdict(&mut self, health: &rustcdc::core::HealthVerdict) {
        let verdict = health.as_str();
        let cause = health.stall_cause().map(|cause| cause.as_str());

        let unchanged = self.health_verdict.as_deref() == Some(verdict)
            && self.health_stall_cause.as_deref() == cause;
        if !unchanged {
            self.stalled_since = cause.map(|_| std::time::Instant::now());
        }

        self.health_verdict = Some(verdict.to_owned());
        self.health_stall_cause = cause.map(str::to_owned);
    }

    /// How long the current stall has lasted, or `None` when not stalled.
    fn stalled_for(&self) -> Option<Duration> {
        self.stalled_since.map(|since| since.elapsed())
    }
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

/// What one polled batch of Kafka signal records produced.
struct KafkaIngestOutcome {
    /// Records whose action reached a terminal state.
    ingested: u64,
    /// Whether the consumer may advance its committed offset past this batch.
    ///
    /// `false` means an action is still in flight past its terminal budget, and losing it
    /// to a crash is worse than redelivering it — which the ledger makes a no-op anyway.
    commit: bool,
}

/// A signal handed to the action pipeline, identified well enough to wait on.
///
/// The pair is the same key `signal_inflight` and `latest_signal_action_state` use — a
/// signal id alone is not enough, because one id can carry several action types.
#[derive(Debug, Clone)]
struct DispatchedSignal {
    signal_id: String,
    action_type: String,
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
    /// The runtime's control handle, installed once the runtime exists.
    ///
    /// A `OnceLock` rather than a constructor argument because `AdminState` is built
    /// before `CdcRuntime` — the admin server must be answering `/healthz` while the
    /// source connection is still being established. Absent means "this process cannot
    /// snapshot", which any pipeline without `[incremental_snapshot]` legitimately is,
    /// and which the signal path reports rather than papers over.
    ///
    /// Every control operation on `CdcRuntime` takes `&mut self` and the event loop owns
    /// that borrow for its lifetime, so reaching the runtime from a handler needs a
    /// bridge. `RuntimeControl` is that bridge, defined upstream where the invariants
    /// live rather than hand-built here out of a request type and a oneshot reply.
    runtime_control: Arc<std::sync::OnceLock<rustcdc::core::RuntimeControl>>,
    signal_inflight: Arc<DashMap<(String, String), ()>>,
    /// How long the Kafka ingress loop waits for a dispatched signal to reach a terminal
    /// state before holding the offset.
    ///
    /// A field rather than the bare constant so tests can exercise the timeout path in
    /// milliseconds. At the production budget of 30 s the case where this matters — an
    /// action still in flight, so the offset must *not* advance and the record must *not*
    /// be ledgered — is not something a unit test can wait for, and it is exactly the
    /// ordering that distinguishes this from the at-most-once version.
    signal_terminal_budget: Duration,
    /// Signal-ingress records whose action already reached a terminal state.
    ///
    /// Durable, unlike `signal_inflight` and the audit ring, so the Kafka ingress channel
    /// can commit its offsets *after* an action completes rather than before it starts.
    /// `None` only where the state directory is unavailable — in hand-built test states.
    signal_ledger: Option<Arc<signal_ledger::ProcessedSignalLedger>>,
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
    /// The signal-action worker, for liveness reporting only.
    ///
    /// An `AbortHandle` rather than a `JoinHandle` because the latter is owned by
    /// `handles` for shutdown, and `is_finished` is all liveness needs.
    signal_worker: Arc<std::sync::Mutex<Option<tokio::task::AbortHandle>>>,
}

impl AdminWorkers {
    fn new() -> Self {
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Self {
            shutdown_tx: Arc::new(shutdown_tx),
            handles: Arc::new(std::sync::Mutex::new(Vec::new())),
            signal_worker: Arc::new(std::sync::Mutex::new(None)),
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
        // Built unconditionally rather than only when the Kafka channel is configured: the
        // ledger is cheap when unused, and building it lazily would mean the one path that
        // needs durability is the one that could silently fail to get it.
        let signal_ledger = Some(Arc::new(signal_ledger::ProcessedSignalLedger::open(
            &config.state.offset.dir,
        )?));
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
                restart_recovery_seconds: None,
                source_consecutive_errors: 0,
                degraded_since: None,
                health_verdict: None,
                health_stall_cause: None,
                stalled_since: None,
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
                notification_log_enabled: notification_log.is_some(),
                notification_kafka_enabled: notification_kafka.is_some(),
                notification_log_emitted_total: 0,
                notification_log_emit_failures_total: 0,
                notification_channel_emitted_total: HashMap::new(),
                notification_channel_emit_failures_total: HashMap::new(),
                audit_entries_total: 0,
                audit_recent_entries: Vec::new(),
                signal_worker_panics_total: 0,
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
            runtime_control: Arc::new(std::sync::OnceLock::new()),
            signal_inflight: Arc::new(DashMap::new()),
            signal_terminal_budget: Duration::from_secs(SIGNAL_TERMINAL_TIMEOUT_SECONDS as u64),
            signal_ledger,
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

    /// Is the signal-action worker still running?
    ///
    /// `JoinHandle::is_finished` is the direct observation. Anything derived — a heartbeat
    /// timestamp, a queue depth — reports "no work happened recently", which a quiet
    /// control plane also reports. Those are different states and an operator needs to tell
    /// them apart.
    fn signal_worker_alive(&self) -> bool {
        self.workers
            .signal_worker
            .lock()
            .ok()
            .and_then(|handle| handle.as_ref().map(|h| !h.is_finished()))
            // No handle means the worker was never spawned, which only happens in
            // hand-built test states. Reporting "alive" there avoids a false alarm about a
            // worker that was never meant to exist.
            .unwrap_or(true)
    }

    fn spawn_signal_action_worker(&self, mut rx: mpsc::Receiver<QueuedSignalAction>) {
        let worker_state = self.clone();
        let mut shutdown = self.workers.subscribe();
        let handle = tokio::spawn(async move {
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
                // **A panic here must not take the control plane with it.**
                //
                // Without this the task dies and never returns. The pipeline keeps
                // capturing, `/status` keeps reporting `snapshot_requests_available:
                // true`, and `POST /signals` keeps answering `STARTED` — while every
                // queued action is silently never executed. The SLO does catch it 30 s
                // later, but it names the symptom ("actions are not reaching a terminal
                // state") rather than the cause, and nothing recovers short of a process
                // restart. A live-looking process with a dead subsystem is the worst
                // shape an on-call can be handed.
                //
                // Note the asymmetry this removes: `panic_guard()` already wraps the HTTP
                // layer in a `CatchPanicLayer`, so a panicking *handler* returns 500 and
                // the server survives. The workers behind those handlers had no
                // equivalent.
                let signal_id = action.signal_id.clone();
                let inflight_key = (
                    action.signal_id.clone(),
                    action.action_type.as_str().to_string(),
                );
                let outcome =
                    std::panic::AssertUnwindSafe(worker_state.process_queued_signal_action(action))
                        .catch_unwind()
                        .await;

                if let Err(panic) = outcome {
                    // Release the in-flight guard, or this (signal_id, action_type) is
                    // permanently unusable: `execute_signal_action_envelope` reads a
                    // present entry as "already running" and refuses every retry.
                    worker_state.signal_inflight.remove(&inflight_key);
                    worker_state.record_signal_worker_panic().await;
                    tracing::error!(
                        target: "rustcdc_audit",
                        action = "signal_action_panicked",
                        signal_id = %signal_id,
                        panic = %panic_message(&panic),
                        "a signal action panicked; the worker has recovered and the signal \
                         is released for retry"
                    );
                }
            }
        });
        // Tracked twice on purpose: `workers.handles` owns shutdown, and `signal_worker`
        // holds a second handle purely so liveness can be observed without draining the
        // shutdown list.
        if let Ok(mut slot) = self.workers.signal_worker.lock() {
            *slot = Some(handle.abort_handle());
        }
        self.workers.track(handle);
    }

    /// Count a recovered worker panic, for `rustcdc_admin_signal_worker_panics_total`.
    async fn record_signal_worker_panic(&self) {
        let mut data = self.data.write().await;
        data.signal_worker_panics_total = data.signal_worker_panics_total.saturating_add(1);
    }

    fn spawn_signal_ingress_file_worker(&self, path: PathBuf) {
        let worker_state = self.clone();
        let mut shutdown = self.workers.subscribe();

        // **Start at the end of the file, not the beginning.**
        //
        // This offset is process-local — nothing persists it — so starting at 0 meant
        // every restart re-read the whole file and **re-executed every signal in it**.
        // While `execute_snapshot` was a no-op that only produced duplicate audit
        // entries; now it snapshots, and rustcdc rewinds an already-complete table and
        // reads it again, so a restart re-scanned every table ever requested through this
        // channel. A file that accumulates signals over months turns each restart into a
        // full re-read of all of them.
        //
        // The file is a live channel, not a durable queue: a signal appended while the
        // server is down is not processed. That is the safe direction — the operator gets
        // no `STARTED` notification and can see it did not run, whereas a replay is
        // indistinguishable from normal operation. Use the Kafka ingress channel when
        // signals must survive a restart; it has real committed offsets.
        //
        // Read **here**, not inside the task: the spawn only queues the future, so taking
        // the length on first poll would race anything appended between `AdminState::new`
        // returning and the scheduler getting to it — and lose exactly the signals sent
        // immediately after startup, non-deterministically.
        let mut offset: u64 = std::fs::metadata(&path).map_or(0, |m| m.len());

        self.workers.track(tokio::spawn(async move {
            {
                loop {
                    if let Ok(metadata) = std::fs::metadata(&path) {
                        // Truncated or rotated: the bytes this offset described are gone,
                        // so re-read from the start of whatever replaced them.
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

                                // The file channel has no durable cursor to protect —
                                // it deliberately starts at EOF and never replays — so it
                                // only counts what it dispatched, and does not wait for a
                                // terminal state the way the Kafka loop must.
                                if worker_state
                                    .process_signal_ingress_payload(
                                        &line,
                                        SignalIngressSource::File,
                                    )
                                    .await
                                    .is_some()
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
                    ($d:expr_2021) => {
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
                        // **A signal is a command, not state.** `Earliest` meant that a
                        // group with no committed offset — a first start, a renamed
                        // `group_id`, or a group whose offsets aged out of
                        // `offsets.retention.minutes` — replayed the entire signal topic
                        // and **re-executed every `execute_snapshot` in its history**.
                        // rustcdc rewinds an already-complete table and reads it again, so
                        // that is a full re-scan of every table ever requested, plus the
                        // duplicate `read` events downstream.
                        //
                        // The in-memory idempotency guard does not cover it: replayed
                        // signal ids are deduplicated against `audit_recent_entries`, which
                        // holds 512 entries and starts empty on every boot.
                        //
                        // `Latest` keeps the useful case — a committed offset still wins,
                        // so signals sent during a brief restart are processed — and drops
                        // only commands issued to a consumer that had never run.
                        .auto_offset_reset(AutoOffsetReset::Latest)
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

                        let outcome = worker_state.ingest_kafka_signal_batch(records).await;
                        let ingested_records = outcome.ingested;

                        if outcome.commit
                            && let Err(error) = consumer.commit().await
                        {
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

    /// Feed one ingress payload through the signal pipeline.
    ///
    /// Returns the `(signal_id, action_type)` that was dispatched, so a caller that owns a
    /// durable cursor — the Kafka ingress loop — can wait for the action to reach a
    /// terminal state before committing past it. `None` means nothing was dispatched:
    /// blank, oversized, unparseable, or rejected by validation. Those are settled
    /// decisions, not pending work.
    async fn process_signal_ingress_payload(
        &self,
        payload: &[u8],
        source: SignalIngressSource,
    ) -> Option<DispatchedSignal> {
        if payload.iter().all(|b| b.is_ascii_whitespace()) {
            return None;
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
            return None;
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
                return None;
            }
        };

        self.process_signal_ingress_record(record, source).await
    }

    #[cfg(test)]
    async fn process_file_signal_ingress_record(&self, record: SignalIngressRecord) {
        self.process_signal_ingress_record(record, SignalIngressSource::File)
            .await;
    }

    pub async fn process_source_signal_ingress_payload(&self, payload: &[u8]) -> bool {
        self.process_signal_ingress_payload(payload, SignalIngressSource::Source)
            .await
            .is_some()
    }

    async fn process_signal_ingress_record(
        &self,
        record: SignalIngressRecord,
        source: SignalIngressSource,
    ) -> Option<DispatchedSignal> {
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
            return None;
        }

        let tables = match validate_signal_tables(record.action_type, record.tables) {
            Ok(tables) => tables,
            Err(error) => {
                self.record_signal_ingress_validation_rejection().await;
                tracing::warn!(
                    target: "rustcdc_audit",
                    action = "signal_ingress_rejected",
                    reason = "invalid_tables",
                    ingress_channel = source.channel_label(),
                    error = %error,
                    "ignoring ingress record with an unusable table list"
                );
                return None;
            }
        };

        let signal_id =
            normalize_optional_id(record.signal_id.as_deref()).unwrap_or_else(generated_signal_id);
        let correlation_id = normalize_optional_id(record.correlation_id.as_deref())
            .unwrap_or_else(|| signal_id.clone());
        let traceparent = normalize_optional_id(record.traceparent.as_deref());
        let conditions =
            match validate_signal_conditions(record.action_type, &tables, record.conditions) {
                Ok(conditions) => conditions,
                Err(error) => {
                    self.record_signal_ingress_validation_rejection().await;
                    tracing::warn!(
                        target: "rustcdc_audit",
                        action = "signal_ingress_rejected",
                        reason = "invalid_conditions",
                        ingress_channel = source.channel_label(),
                        error = %error,
                        "ignoring ingress record with an unusable row filter"
                    );
                    return None;
                }
            };

        let envelope = SignalActionEnvelope {
            signal_id,
            correlation_id,
            action_type: record.action_type,
            message,
            traceparent,
            additional_data: record.additional_data.unwrap_or(serde_json::Value::Null),
            tables,
            conditions,
            actor_source_ip: Some(source.actor_source_ip().to_string()),
            actor_token_id: source.actor_token_id().to_string(),
        };

        let result = self.execute_signal_action_envelope(envelope).await;
        match result {
            SignalActionExecutionResult::Aborted {
                signal_id,
                action_type,
                error,
                ..
            } => {
                tracing::warn!(
                    target: "rustcdc_audit",
                    action = "signal_ingress_aborted",
                    ingress_channel = source.channel_label(),
                    signal_id = %signal_id,
                    action_type = %action_type,
                    error = %error,
                    "ingress signal action aborted"
                );
                // Terminal already: the ABORTED lifecycle entry is written. Reporting it
                // as still-pending would make the ingress loop wait out its whole terminal
                // budget for a state that will never change.
                None
            }
            SignalActionExecutionResult::ExistingState {
                signal_id,
                action_type,
                ..
            }
            | SignalActionExecutionResult::Started {
                signal_id,
                action_type,
                ..
            }
            | SignalActionExecutionResult::Terminal {
                signal_id,
                action_type,
                ..
            } => Some(DispatchedSignal {
                signal_id,
                action_type,
            }),
        }
    }

    /// Ingest one polled batch of Kafka signal records, reporting whether to commit.
    ///
    /// Extracted from the ingress worker so a test can drive it directly. The worker's
    /// remaining job is polling and committing; everything that decides *whether* the
    /// offset may advance lives here, which is the part with a correctness argument.
    async fn ingest_kafka_signal_batch(&self, records: Vec<ConsumerRecord>) -> KafkaIngestOutcome {
        // **Commit after the action is decided, never before it starts.**
        //
        // This loop used to call `commit()` unconditionally once every
        // record had been *queued*. An async action — every
        // `execute_snapshot` — had not run at that point, so a crash
        // between the commit and the worker draining the queue lost the
        // command permanently: a `STARTED` entry in a 512-entry in-memory
        // ring that the restart then emptied, and no terminal state. The
        // channel documented as the durable one was at-most-once.
        //
        // The reason it was written that way is that the opposite trade is
        // also bad: redelivering an `execute_snapshot` re-runs it, and
        // rustcdc rewinds an already-complete table and reads it again.
        // `ProcessedSignalLedger` removes the dilemma — a record whose
        // action reached a terminal state is durably marked, so redelivery
        // is a skip rather than a re-execution.
        let mut ingested_records = 0_u64;
        let mut commit_batch = true;
        for record in records {
            let ledger_key =
                signal_ledger::kafka_ingress_key(&record.topic, record.partition, record.offset);
            if self.signal_ingress_already_processed(&ledger_key) {
                // Decided in a previous life; the offset simply had not
                // been committed before the process went away.
                continue;
            }

            let Some(value) = &record.value else {
                self.mark_signal_ingress_processed(&ledger_key);
                continue;
            };

            let dispatched = self
                .process_signal_ingress_payload(value.as_ref(), SignalIngressSource::Kafka)
                .await;

            match dispatched {
                // Rejected outright — blank, oversized, unparseable, or
                // invalid. That verdict will not change on redelivery, so
                // it is settled and the offset may advance past it.
                None => self.mark_signal_ingress_processed(&ledger_key),
                Some(dispatched) => {
                    if self.await_signal_terminal(&dispatched).await {
                        ingested_records = ingested_records.saturating_add(1);
                        self.mark_signal_ingress_processed(&ledger_key);
                    } else {
                        // Still running past its terminal budget. Hold the
                        // offset: the next poll redelivers this record and
                        // the in-flight guard makes that a no-op until it
                        // finishes. Committing here is the loss this whole
                        // block exists to prevent.
                        tracing::warn!(
                            target: "rustcdc_audit",
                            action = "signal_ingress_kafka_commit_deferred",
                            topic = %record.topic,
                            partition = record.partition,
                            offset = record.offset,
                            signal_id = %dispatched.signal_id,
                            action_type = %dispatched.action_type,
                            "signal action has not reported a terminal state; \
                             holding the ingress offset rather than risking \
                             the command being lost"
                        );
                        commit_batch = false;
                        break;
                    }
                }
            }
        }

        KafkaIngestOutcome {
            ingested: ingested_records,
            commit: commit_batch,
        }
    }

    /// Has this ingress record's action already been decided in an earlier process?
    ///
    /// Without a ledger this is always `false`, which is the pre-existing behaviour: the
    /// caller then relies on the offset commit alone.
    fn signal_ingress_already_processed(&self, key: &str) -> bool {
        self.signal_ledger
            .as_ref()
            .is_some_and(|ledger| ledger.contains(key))
    }

    /// Durably mark an ingress record as decided, before its offset is committed.
    ///
    /// A write failure is logged and not propagated. The ledger makes redelivery *safe*;
    /// it is not what makes delivery happen, and refusing to commit because a dedup cache
    /// could not be written would stall the channel over a recoverable condition. The cost
    /// of the failure is a possible duplicate after a crash, which is the behaviour this
    /// channel had unconditionally before the ledger existed.
    fn mark_signal_ingress_processed(&self, key: &str) {
        let Some(ledger) = self.signal_ledger.as_ref() else {
            return;
        };
        if let Err(error) = ledger.record(key) {
            tracing::warn!(
                target: "rustcdc_audit",
                action = "signal_ingress_ledger_write_failed",
                error = %error,
                key = %key,
                "could not durably record a processed signal; a crash before the offset \
                 commit could redeliver and re-execute it"
            );
        }
    }

    /// Wait for a dispatched signal to reach a terminal lifecycle state.
    ///
    /// Returns `false` on timeout, which the Kafka ingress loop reads as "do not commit
    /// past this record yet". A slow action therefore holds the consumer offset instead of
    /// risking the command being lost, and the next poll redelivers the same record — the
    /// in-flight guard makes that a no-op while it is still running.
    ///
    /// Bounded by [`SIGNAL_TERMINAL_TIMEOUT_SECONDS`], the same budget the SLO uses for
    /// "accepted but never reported a terminal state", so a signal that trips this is
    /// already visible as `signal_actions_without_terminal_total`.
    async fn await_signal_terminal(&self, dispatched: &DispatchedSignal) -> bool {
        let deadline = tokio::time::Instant::now() + self.signal_terminal_budget;
        loop {
            {
                let data = self.data.read().await;
                match latest_signal_action_state(
                    &data,
                    &dispatched.signal_id,
                    &dispatched.action_type,
                ) {
                    // Absent means the audit ring has already evicted it under load, which
                    // this loop cannot distinguish from "never recorded". Treating it as
                    // terminal is the safe reading: the alternative is holding the offset
                    // forever on a signal whose outcome is unknowable from here.
                    None => return true,
                    Some(state) if state != "STARTED" => return true,
                    Some(_) => {}
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(SIGNAL_TERMINAL_POLL_INTERVAL).await;
        }
    }

    /// Hand the admin API the channel the event loop listens on for snapshot requests.
    ///
    /// Called once, after `CdcRuntime::start()` has confirmed the pipeline can actually
    /// service one. A second call is ignored: the first executor is the live one, and
    /// silently replacing it would leave requests queued on a receiver nobody polls.
    pub async fn attach_runtime_control(&self, control: rustcdc::core::RuntimeControl) {
        if self.runtime_control.set(control).is_err() {
            tracing::warn!("a runtime control handle is already attached; ignoring the second");
            return;
        }
        self.data.write().await.snapshot_requests_available = true;
    }

    /// Advertise the snapshot capability without a runtime behind it.
    ///
    /// `RuntimeControl` is not constructible outside rustcdc, so the signal-path tests
    /// assert the *dispatch* and reporting behaviour with the capability advertised and no
    /// runtime attached. The end-to-end path — a real runtime servicing a real request —
    /// is covered by `tests/integration_postgres.rs`, which is where it belongs.
    #[cfg(test)]
    async fn advertise_snapshot_capability(&self) {
        self.data.write().await.snapshot_requests_available = true;
    }

    /// Ask the running pipeline to snapshot `tables`, returning how many it enqueued.
    ///
    /// The error is a rendered string rather than a typed error because every caller puts
    /// it straight into an audit record and a notification payload, and the distinctions
    /// that matter to an operator ("no such table", "snapshots are not configured") are
    /// already in the message rustcdc produced.
    async fn request_incremental_snapshot(
        &self,
        tables: Vec<String>,
        conditions: BTreeMap<String, String>,
    ) -> Result<usize, String> {
        let outcome = self.dispatch_incremental_snapshot(tables, conditions).await;

        // Counted here rather than at each `return`, so a future early exit cannot escape
        // the accounting — a refusal that is not counted looks exactly like no request.
        let mut data = self.data.write().await;
        match &outcome {
            Ok(enqueued) => {
                data.snapshot_requests_accepted_total =
                    data.snapshot_requests_accepted_total.saturating_add(1);
                data.snapshot_tables_enqueued_total = data
                    .snapshot_tables_enqueued_total
                    .saturating_add(*enqueued as u64);
            }
            Err(_) => {
                data.snapshot_requests_refused_total =
                    data.snapshot_requests_refused_total.saturating_add(1);
            }
        }

        outcome
    }

    /// Build the rustcdc request and hand it to the runtime.
    ///
    /// The per-request conditions override the configured `table_conditions` for the same
    /// table; a table with no override keeps its configured filter. That merge happens
    /// inside rustcdc, in one place, which is the fix for the defect where a runtime
    /// request ran unfiltered and a restart then adopted the same table *with* the filter
    /// — producing a table whose rows corresponded to no single predicate.
    async fn dispatch_incremental_snapshot(
        &self,
        tables: Vec<String>,
        conditions: BTreeMap<String, String>,
    ) -> Result<usize, String> {
        self.with_control("execute_snapshot", |control| async move {
            let mut request = rustcdc::source::SnapshotRequest::new(tables);
            for (table, condition) in conditions {
                request = request.with_condition(table, condition);
            }
            control.request_incremental_snapshot_filtered(request).await
        })
        .await
    }

    /// Live incremental-snapshot progress, or `None` when nothing is in flight.
    ///
    /// Non-blocking by construction: rustcdc republishes this snapshot every poll and
    /// `RuntimeControl::incremental_snapshot_state` reads the published copy, so a busy
    /// pipeline cannot starve it and a stalled one cannot hang it. It is stale by at most
    /// one poll, which is the right trade for a number an operator refreshes in a
    /// dashboard.
    ///
    /// Without it an operator who fires `execute_snapshot` learns how many tables were
    /// accepted and nothing after that, which for a multi-hour backfill is the entire
    /// operational experience.
    fn incremental_snapshot_progress(&self) -> Option<rustcdc::IncrementalSnapshotState> {
        self.runtime_control
            .get()
            .and_then(|control| control.incremental_snapshot_state())
    }

    /// Run a control operation against the runtime, with the shared budget and messaging.
    ///
    /// The timeout is ours, not rustcdc's: commands are applied between polls, so a loop
    /// that has stopped turning leaves one waiting, and the crate documents that a caller
    /// with an SLO should impose one. Ours is derived from
    /// [`SIGNAL_TERMINAL_TIMEOUT_SECONDS`] so a request always resolves before an
    /// unterminated signal is reported unhealthy.
    async fn with_control<T, F, Fut>(&self, action: &str, call: F) -> Result<T, String>
    where
        F: FnOnce(rustcdc::core::RuntimeControl) -> Fut,
        Fut: std::future::Future<Output = rustcdc::core::Result<T>>,
    {
        let Some(control) = self.runtime_control.get() else {
            return Err(format!(
                "this pipeline cannot service '{action}': no [incremental_snapshot] section \
                 is configured, so there is no snapshot to act on. Configure \
                 incremental_snapshot.tables (it may be empty) and restart."
            ));
        };

        match tokio::time::timeout(SNAPSHOT_REQUEST_TIMEOUT, call(control.clone())).await {
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(_) => Err(format!(
                "the pipeline did not answer '{action}' within {}s; control commands are \
                 applied between polls, so it may be blocked on the source or the sink",
                SNAPSHOT_REQUEST_TIMEOUT.as_secs()
            )),
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
            tables,
            conditions,
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
                tables,
                conditions,
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
        // Latch the verdict *before* the snapshot is consumed by the renderer, so
        // `/livez` and `/readyz` can act on it. See `AdminStateData::health_verdict`.
        d.record_health_verdict(&runtime_metrics.runtime_admin.health);
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

        // Do the work the signal names, then report what happened.
        //
        // This block used to be absent: every queued action went straight to its terminal
        // audit entry, so `execute_snapshot` recorded `COMPLETED` and emitted a
        // notification saying so **without snapshotting anything**. The API reported
        // success for an operation that did not exist, which is worse than not offering it
        // — an operator watching the notification stream had no way to tell.
        let outcome = self.run_signal_action(&action).await;

        let (lifecycle_action, result, state, detail) = match outcome {
            Ok(detail) => (
                action.action_type.terminal_audit_action(),
                action.action_type.terminal_result(),
                action.action_type.terminal_state(),
                detail,
            ),
            Err(error) => (
                action.action_type.queue_rejected_audit_action(),
                "failed",
                "ABORTED",
                serde_json::json!({ "error": error }),
            ),
        };

        let additional_data = merge_signal_detail(action.additional_data, detail);

        self.append_signal_lifecycle_entry(
            &action.signal_id,
            &action.correlation_id,
            action.action_type,
            lifecycle_action,
            result,
            state,
            &action.message,
            action.traceparent,
            additional_data,
            action.actor_source_ip,
            Some(action.actor_token_id),
        )
        .await;

        self.signal_inflight.remove(&key);
    }

    /// Perform a queued signal action, returning detail to attach to its terminal record.
    async fn run_signal_action(
        &self,
        action: &QueuedSignalAction,
    ) -> Result<serde_json::Value, String> {
        // A deliberate panic, so the worker's recovery path can be tested through the real
        // code rather than a restatement of it. There is no way to make a *real* action
        // panic on demand, and a recovery path that has never actually unwound is a
        // recovery path nobody has tested.
        #[cfg(test)]
        if action.message == PANIC_PROBE_MESSAGE {
            panic!("deliberate panic from the signal-action panic probe");
        }

        match action.action_type {
            SignalActionType::ExecuteSnapshot => {
                let enqueued = self
                    .request_incremental_snapshot(action.tables.clone(), action.conditions.clone())
                    .await?;
                tracing::info!(
                    target: "rustcdc_audit",
                    action = "signal_execute_snapshot",
                    signal_id = %action.signal_id,
                    enqueued,
                    tables = ?action.tables,
                    "incremental snapshot enqueued on the running pipeline"
                );
                Ok(serde_json::json!({
                    "tables": action.tables,
                    "tables_enqueued": enqueued,
                }))
            }
            // `LogMarker` is the marker; writing the audit entry *is* the work.
            SignalActionType::LogMarker => Ok(serde_json::Value::Null),

            // Real operations, routed through `RuntimeControl`. The live change stream is
            // untouched in every case: only chunk reading is
            // affected, so a backfill loading a production primary during business hours
            // can be held until the evening without stopping capture.
            SignalActionType::PauseSnapshot => {
                let already_paused = self
                    .with_control("pause_snapshot", |control| async move {
                        control.pause_incremental_snapshot().await
                    })
                    .await?;
                Ok(serde_json::json!({ "already_paused": already_paused }))
            }
            SignalActionType::ResumeSnapshot => {
                let was_paused = self
                    .with_control("resume_snapshot", |control| async move {
                        control.resume_incremental_snapshot().await
                    })
                    .await?;
                Ok(serde_json::json!({ "was_paused": was_paused }))
            }
            SignalActionType::StopSnapshot => {
                let tables_abandoned = self
                    .with_control("stop_snapshot", |control| async move {
                        control.stop_incremental_snapshot().await
                    })
                    .await?;
                Ok(serde_json::json!({ "tables_abandoned": tables_abandoned }))
            }
        }
    }
}

/// Fold execution detail into the caller's `additional_data`.
///
/// The operator's own payload is preserved: it is what correlates the signal with
/// whatever raised it, and overwriting it to report an outcome would trade one useful
/// field for another.
fn merge_signal_detail(caller: serde_json::Value, detail: serde_json::Value) -> serde_json::Value {
    match (caller, detail) {
        (caller, serde_json::Value::Null) => caller,
        (serde_json::Value::Object(mut caller), serde_json::Value::Object(detail)) => {
            for (key, value) in detail {
                caller.insert(key, value);
            }
            serde_json::Value::Object(caller)
        }
        (serde_json::Value::Null, detail) => detail,
        (caller, detail) => serde_json::json!({ "request": caller, "result": detail }),
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
    // Truncate detail to prevent memory amplification from write-scope tokens with large
    // message payloads.
    //
    // Through `crate::text`, because this string embeds the caller's `message` and
    // `additional_data` verbatim and `serde_json` does not escape non-ASCII: the previous
    // `detail[..AUDIT_DETAIL_MAX_BYTES]` panicked whenever the budget landed inside a
    // multi-byte character. From the HTTP handler that failed one request; from the
    // signal-action worker it killed the task, and every asynchronous signal afterwards
    // was silently never processed.
    let detail = crate::text::truncate_utf8(detail, AUDIT_DETAIL_MAX_BYTES, " [truncated]");
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
            // `get` rather than `&val[..32]`: the index is a byte offset, so a value that
            // is not ASCII — a pasted passphrase rather than hex — would panic at startup
            // instead of taking the documented warning path below.
            if let Some(prefix) = val.get(..32)
                && let Ok(bytes) = hex::decode(prefix)
                && let Ok(arr) = <[u8; 16]>::try_from(bytes.as_slice())
            {
                tracing::debug!(
                    env_var = %env_name,
                    "audit IP pseudonymisation: stable salt loaded from env"
                );
                return arr;
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

    if let Some(age) = data.checkpoint_age_seconds
        && age > 300.0
    {
        reasons.push(format!("checkpoint age {age:.3}s exceeds 300s threshold"));
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

    if let Some(recovery) = data.restart_recovery_seconds
        && recovery > 30.0
    {
        reasons.push(format!(
            "restart recovery {recovery:.3}s exceeds 30s threshold"
        ));
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

/// The runtime's health verdict, for the top level of `/status`.
///
/// Deliberately not inside the `slo` object: a verdict is not a service-level indicator,
/// it is the headline answer, and §1 of the runbook opens by telling an operator to curl
/// this endpoint. Until it was added, the verdict was reachable only by scraping
/// `/metrics` and grepping a one-hot gauge — for the single field that says whether the
/// pipeline is working.
///
/// `stall_cause` is the stable discriminant rather than the prose: it is what routes a
/// page and what `/readyz` reports. `stalled_for_seconds` is how long *this* condition
/// has held, which is what decides whether `/livez` is about to restart the pod.
pub(crate) fn health_json(data: &AdminStateData) -> serde_json::Value {
    serde_json::json!({
        "verdict": data.health_verdict,
        "stall_cause": data.health_stall_cause,
        "stalled_for_seconds": data.stalled_for().map(|elapsed| elapsed.as_secs_f64()),
    })
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
        "snapshot_requests": {
            "available": data.snapshot_requests_available,
            "accepted_total": data.snapshot_requests_accepted_total,
            "refused_total": data.snapshot_requests_refused_total,
            "tables_enqueued_total": data.snapshot_tables_enqueued_total,
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

pub(crate) fn runtime_metrics_prometheus(data: &AdminStateData) -> String {
    data.runtime_metrics.clone()
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

/// The running configuration, with every credential redacted.
///
/// `AdminState` has built and redacted this snapshot on every startup since the beginning
/// — and nothing served it. The field was written, the redaction rules were written, the
/// property tests over those rules were written, and the value was unreachable. Two docs
/// pages nevertheless described "the `/status` config snapshot a read-scoped token can
/// read", which is how the gap survived: the documentation described the intent and
/// everyone read it as the behaviour.
///
/// A separate endpoint rather than a field on `/status`. `/status` is polled by dashboards
/// on a short interval; a whole configuration document in every response is bandwidth
/// nobody asked for, and the question this answers — "what is this instance *actually*
/// running, after env-var layering and config migration?" — is asked once during an
/// incident, not continuously.
///
/// Read scope, and rate-limited on the `Status` budget, because it is the same class of
/// disclosure: everything here has been through `redact_secrets`, but a configuration
/// still describes hosts, topics, table lists and file paths.
async fn config_authed(
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
        return rate_limited_response("config");
    }

    if !admin.authorize_read(&headers) {
        return unauthorized_response();
    }

    let start = std::time::Instant::now();
    let config_json = admin.data.read().await.config_json.clone();
    admin.record_status_probe(start.elapsed()).await;

    // Served as `application/json` from the stored string rather than re-parsed: it was
    // produced by `serde_json::to_string_pretty` and then redacted, so re-parsing would
    // cost an allocation to produce the same bytes.
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "application/json; charset=utf-8",
        )],
        config_json,
    )
        .into_response()
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
    let snapshot_progress = admin.incremental_snapshot_progress();
    let data = admin.data.read().await;
    let snapshot = serde_json::json!({
        "api_version": "v1",
        "state": data.state,
        // `state` says the runtime is running; `health` says whether it is working. The
        // pair is the point — `state = "running"` covers a quiet database and a dead
        // socket equally.
        "health": health_json(&data),
        "started_at": data.started_at,
        "first_ready_at": data.first_ready_at,
        "first_checkpoint_advanced_at": data.first_checkpoint_advanced_at,
        "last_batch_at": data.last_batch_at,
        "events_processed": data.events_processed,
        "batches_processed": data.batches_processed,
        "slo": slo_json(&data),
        // Read from the runtime rather than from `AdminStateData`, because it is the
        // runtime's live view: per-table cursors, completion flags and row counters that
        // no admin-side counter could reconstruct. `None` when no snapshot is in flight.
        "incremental_snapshot": snapshot_progress,
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

    let snapshot_prom = snapshot_progress_prometheus(admin.incremental_snapshot_progress());
    let data = admin.data.read().await;
    let prom = runtime_metrics_prometheus(&data);
    let slo_prom = slo_prometheus(&data, admin.signal_worker_alive());
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
        format!("{prom}{slo_prom}{auth_prom}{audit_prom}{snapshot_prom}"),
    )
        .into_response()
}

// ─────────────────────────────────────────────────────────────────────────────
// Router
// ─────────────────────────────────────────────────────────────────────────────

/// Turn a panic in any admin handler into a `500`, logged, instead of a dropped socket.
///
/// Defence in depth for the control plane, not a substitute for not panicking. Without
/// it, a panicking handler unwinds into tokio, which aborts that task: the client sees a
/// connection reset with **no status and no body**, and nothing is written to the log —
/// so the failure looks like a network problem and is diagnosed as one.
///
/// This is not hypothetical here. The audit-trail detail was truncated with a byte slice,
/// which panicked whenever the budget landed inside a multi-byte character, and any
/// write-scope token could reach it. That specific defect is fixed (`crate::text`), and
/// the next one of its kind should surface as a `500` with a stack trace in the log rather
/// than as an unexplained reset.
///
/// The panic payload is deliberately **not** returned to the caller: a panic message can
/// carry file paths and fragments of internal state, and the admin API is reachable by any
/// read-scoped token. It goes to the log, where it is already trusted with more than that.
fn panic_guard()
-> tower_http::catch_panic::CatchPanicLayer<fn(Box<dyn std::any::Any + Send + 'static>) -> Response>
{
    fn on_panic(panic: Box<dyn std::any::Any + Send + 'static>) -> Response {
        let detail = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&'static str>().copied())
            .unwrap_or("<non-string panic payload>");

        tracing::error!(
            target: "rustcdc_audit",
            action = "admin_handler_panic",
            detail = %detail,
            "an admin API handler panicked; returning 500. This is a defect — the \
             handler should have produced an error response"
        );

        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "api_version": "v1",
                "error": "internal server error",
            })),
        )
            .into_response()
    }

    tower_http::catch_panic::CatchPanicLayer::custom(on_panic as fn(_) -> _)
}

/// Serve the admin API's OpenAPI 3.1 document.
///
/// Unauthenticated on purpose: it describes the *shape* of the API, not its state, and
/// requiring a credential to discover how to authenticate is a loop. It contains no
/// configuration, no secrets and nothing about this instance beyond the crate version.
async fn openapi_document(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    // Rate-limited like every other endpoint that does real work.
    //
    // It stays **unauthenticated** — it describes the shape of the API rather than any of
    // this instance's state, carries no configuration, and requiring a credential to
    // discover how to authenticate is a loop. But unauthenticated and unmetered are
    // different things, and this was briefly both: it was the only route that was neither
    // authorised nor limited while serving ~10 KB of freshly-built JSON, which is a cheap
    // asymmetric-cost request against a process that is also running the pipeline.
    //
    // Shares the `Status` scope rather than adding a fourth limiter: the budget an operator
    // tunes for "read-only admin surface" is the right one, and a separate knob for a
    // constant document would be a setting nobody has a reason to set.
    let (allowed, decision_latency) =
        admin.allow_abuse_scope(AbuseLimitScope::Status, &headers, Some(peer_addr));
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Status, decision_latency)
        .await;
    if !allowed {
        admin
            .record_rate_limited_request(AbuseLimitScope::Status)
            .await;
        return rate_limited_response("openapi");
    }

    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        axum::body::Body::from(openapi::cached_document()),
    )
        .into_response()
}

/// Message that makes a signal action panic, for the worker-recovery test.
#[cfg(test)]
pub(super) const PANIC_PROBE_MESSAGE: &str = "__panic_probe__";

/// Render a caught panic payload as text.
///
/// `Box<dyn Any>` carries a `&str` for `panic!("literal")` and a `String` for a formatted
/// one; anything else is opaque. Losing the message would make the audit record useless
/// for the one thing it is for.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/status", get(status_authed))
        .route("/config", get(config_authed))
        .route("/metrics", get(metrics))
        .route("/signals", post(signal_action))
        .route("/notifications", get(notifications_authed))
        .route(
            "/notifications/cloudevents",
            get(notifications_cloudevents_authed),
        )
        .route("/notifications/stream", get(notifications_stream_authed))
        .route("/openapi.json", get(openapi_document))
        .with_state(state)
        .layer(panic_guard())
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
mod auth_tests;
#[cfg(test)]
mod probe_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod kafka_tests;
