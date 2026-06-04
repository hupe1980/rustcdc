//! Runtime orchestration for embedded CDC operation.

use std::{collections::VecDeque, sync::Arc};

use futures_util::{stream, stream::BoxStream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    checkpoint::{CommitBarrier, GenericOffset},
    ddl_capture::{parse_ddl_statement, DdlDialect},
    schema_history::{SchemaHistory, SchemaHistoryRetention},
    source::{
        ConnectorCapabilities, HandoffResult, IncrementalSnapshotConfig, SnapshotHandle,
        StreamHandle,
    },
    transform::TransformPipeline,
};

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
use crate::source::Source;

#[cfg(feature = "sqlserver")]
use crate::source::{SqlServerConnection, SqlServerSourceConfig};
#[cfg(feature = "mysql")]
use crate::{
    checkpoint::MysqlOffset,
    source::{MysqlConnection, MysqlSourceConfig},
};
#[cfg(feature = "postgres")]
use crate::{
    checkpoint::PostgresOffset,
    source::{PostgresConnection, PostgresSourceConfig},
};

#[cfg(feature = "mysql")]
use super::runtime_offsets::parse_mysql_stream_offset;
#[cfg(any(feature = "postgres", test))]
use super::runtime_offsets::parse_postgres_lsn;
use super::runtime_utils::{normalize_source_timestamp_ms, now_millis};
use super::{
    Error, Event, EventIdempotencyGuard, EventTracer, MetricsCollector, NoOpEventTracer,
    NoOpMetricsCollector, Offset, Result,
};

mod runtime_commit;

const DEFAULT_RUNTIME_IDEMPOTENCY_CAPACITY: usize = 100_000;
const DEFAULT_SCHEMA_HISTORY_MAX_VERSIONS_PER_TABLE: usize = 256;

/// Explicit observability configuration for runtime construction.
#[derive(Clone)]
#[non_exhaustive]
pub struct RuntimeObservability {
    /// Metrics collector used by runtime operations.
    pub metrics: Arc<dyn MetricsCollector>,
    /// Tracer used for runtime-level events.
    pub tracer: Arc<dyn EventTracer>,
}

impl Default for RuntimeObservability {
    fn default() -> Self {
        Self {
            metrics: Arc::new(NoOpMetricsCollector),
            tracer: Arc::new(NoOpEventTracer),
        }
    }
}

impl RuntimeObservability {
    /// Override the metrics collector.
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsCollector>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Override the tracer.
    pub fn with_tracer(mut self, tracer: Arc<dyn EventTracer>) -> Self {
        self.tracer = tracer;
        self
    }
}

/// Explicit runtime tuning and operational options.
#[derive(Clone)]
#[non_exhaustive]
pub struct RuntimeOptions {
    /// Observability configuration for runtime instrumentation.
    pub observability: RuntimeObservability,
    /// Maximum number of in-memory buffered events.
    pub max_buffer_size: usize,
    /// Poll wait budget in milliseconds.
    pub max_poll_wait_ms: u64,
    /// Runtime behavior when transform execution fails.
    pub transform_error_policy: TransformErrorPolicy,
    /// Runtime behavior when source confirmation fails after durable checkpoint commit.
    pub post_commit_source_confirm_policy: PostCommitSourceConfirmPolicy,
    /// Optional runtime-level sink-side duplicate suppression guard.
    pub idempotency: Option<IdempotencyOptions>,
    /// Whether to enforce canonical event-envelope validation before buffering.
    pub validate_events: bool,
    /// Optional schema-history retention policy applied after DDL persistence.
    pub schema_history_retention: Option<SchemaHistoryRetention>,
    /// Optional retry policy applied when a recoverable source error occurs during streaming.
    ///
    /// When `None`, recoverable source errors surface immediately to the caller.
    /// When `Some`, the runtime retries the failing poll with exponential backoff before
    /// surfacing the error.
    pub connection_retry: Option<ConnectionRetryPolicy>,
    /// Optional callback invoked when an event is discarded due to a transform error
    /// under [`TransformErrorPolicy::Skip`].
    ///
    /// The handler receives the original (pre-transform) [`Event`] and the
    /// [`Error`](crate::core::Error) that caused the skip. Use this to route discarded
    /// events to a dead-letter queue, external error store, or alerting system.
    ///
    /// # Hard constraints
    ///
    /// **The callback is invoked synchronously inside the runtime poll loop.**
    /// It **must not block** (no `std::thread::sleep`, no synchronous I/O, no
    /// blocking locks) and **must not panic**. A blocking handler will stall
    /// the entire CDC pipeline for as long as the call takes.
    ///
    /// If you need to write to a slow external system, enqueue the event into
    /// an internal channel or `VecDeque` inside the callback and drain it from
    /// a separate thread or async task.
    pub dead_letter_handler:
        Option<std::sync::Arc<dyn Fn(Event, crate::core::Error) + Send + Sync>>,
    /// Optional upper bound on serialized event bytes per batch.
    ///
    /// When set, the runtime will not flush a batch whose total serialized size
    /// exceeds this value. Set to `None` (the default) to disable byte-level
    /// throttling and rely only on `max_buffer_size`.
    pub max_event_bytes: Option<usize>,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            observability: RuntimeObservability::default(),
            max_buffer_size: 10_000,
            max_poll_wait_ms: 5_000,
            transform_error_policy: TransformErrorPolicy::Halt,
            // Correctness-first default: fail fast if source confirmation fails
            // after durable checkpoint commit so operators see divergence immediately.
            post_commit_source_confirm_policy: PostCommitSourceConfirmPolicy::FailFast,
            idempotency: Some(IdempotencyOptions {
                capacity: DEFAULT_RUNTIME_IDEMPOTENCY_CAPACITY,
                ttl_ms: None,
            }),
            validate_events: true,
            // Correctness-first + operability default: keep bounded schema history
            // to prevent unbounded growth in long-lived DDL-heavy deployments.
            schema_history_retention: Some(
                SchemaHistoryRetention::keep_last(DEFAULT_SCHEMA_HISTORY_MAX_VERSIONS_PER_TABLE)
                    .expect("default schema history retention policy must be valid"),
            ),
            connection_retry: Some(ConnectionRetryPolicy::default()),
            dead_letter_handler: None,
            max_event_bytes: None,
        }
    }
}

impl RuntimeOptions {
    /// Replace the observability configuration.
    pub fn with_observability(mut self, observability: RuntimeObservability) -> Self {
        self.observability = observability;
        self
    }

    /// Override the maximum buffer size.
    pub fn with_max_buffer_size(mut self, max_buffer_size: usize) -> Self {
        self.max_buffer_size = max_buffer_size;
        self
    }

    /// Override the poll wait budget in milliseconds.
    pub fn with_max_poll_wait_ms(mut self, max_poll_wait_ms: u64) -> Self {
        self.max_poll_wait_ms = max_poll_wait_ms;
        self
    }

    /// Configure transform failure behavior.
    pub fn with_transform_error_policy(mut self, policy: TransformErrorPolicy) -> Self {
        self.transform_error_policy = policy;
        self
    }

    /// Configure post-commit source confirmation behavior.
    pub fn with_post_commit_source_confirm_policy(
        mut self,
        policy: PostCommitSourceConfirmPolicy,
    ) -> Self {
        self.post_commit_source_confirm_policy = policy;
        self
    }

    /// Configure runtime-level duplicate suppression for source events.
    ///
    /// Duplicate detection runs before transform stages, so dedupe decisions
    /// are stable even when downstream transforms are nondeterministic.
    pub fn with_idempotency(mut self, idempotency: IdempotencyOptions) -> Self {
        self.idempotency = Some(idempotency);
        self
    }

    /// Explicitly disable runtime-level duplicate suppression.
    pub fn with_idempotency_disabled(mut self) -> Self {
        self.idempotency = None;
        self
    }

    /// Enable or disable canonical event-envelope validation at runtime ingress.
    pub fn with_event_validation(mut self, enabled: bool) -> Self {
        self.validate_events = enabled;
        self
    }

    /// Apply retention automatically after each persisted schema-history mutation.
    pub fn with_schema_history_retention(mut self, retention: SchemaHistoryRetention) -> Self {
        self.schema_history_retention = Some(retention);
        self
    }

    /// Configure automatic retry with exponential backoff for recoverable source errors.
    ///
    /// Without a retry policy every recoverable source error surfaces immediately
    /// to the caller. With a policy the runtime retries the failing stream poll
    /// up to `max_retries` times, sleeping between attempts, before propagating.
    pub fn with_connection_retry(mut self, policy: ConnectionRetryPolicy) -> Self {
        self.connection_retry = Some(policy);
        self
    }

    /// Set an upper bound on serialized event bytes per batch.
    ///
    /// The runtime will not flush a batch whose total serialized size exceeds
    /// this value. Pass `None` to remove the limit (the default).
    pub fn with_max_event_bytes(mut self, max_bytes: impl Into<Option<usize>>) -> Self {
        self.max_event_bytes = max_bytes.into();
        self
    }
}

/// Runtime-level idempotency guard configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct IdempotencyOptions {
    pub capacity: usize,
    pub ttl_ms: Option<u64>,
}

impl IdempotencyOptions {
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::ConfigError(
                "idempotency capacity must be greater than zero".into(),
            ));
        }
        Ok(Self {
            capacity,
            ttl_ms: None,
        })
    }

    pub fn with_ttl_ms(mut self, ttl_ms: u64) -> Result<Self> {
        if ttl_ms == 0 {
            return Err(Error::ConfigError(
                "idempotency ttl_ms must be greater than zero".into(),
            ));
        }
        self.ttl_ms = Some(ttl_ms);
        Ok(self)
    }
}

/// Retry policy for recoverable source connection errors.
///
/// When a stream poll fails with a recoverable [`Error::SourceError`], the runtime
/// retries up to `max_retries` times (or indefinitely when `None`) using truncated
/// exponential backoff clamped to `max_delay_ms`.
///
/// # Example
/// ```
/// use rustcdc::core::ConnectionRetryPolicy;
///
/// let policy = ConnectionRetryPolicy {
///     max_retries: Some(5),
///     initial_delay_ms: 300,
///     max_delay_ms: 10_000,
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectionRetryPolicy {
    /// Maximum number of consecutive retries before the error is surfaced.
    /// `None` means retry indefinitely.
    pub max_retries: Option<u32>,
    /// Initial retry delay in milliseconds.
    pub initial_delay_ms: u64,
    /// Maximum retry delay cap in milliseconds (exponential backoff clamp).
    pub max_delay_ms: u64,
}

impl Default for ConnectionRetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: Some(5),
            initial_delay_ms: 300,
            max_delay_ms: 10_000,
        }
    }
}

impl ConnectionRetryPolicy {
    /// Set the maximum number of retries (`None` = retry indefinitely).
    pub fn max_retries(mut self, n: Option<u32>) -> Self {
        self.max_retries = n;
        self
    }

    /// Set the initial delay between retries in milliseconds.
    pub fn initial_delay_ms(mut self, ms: u64) -> Self {
        self.initial_delay_ms = ms;
        self
    }

    /// Set the maximum delay cap for exponential backoff in milliseconds.
    pub fn max_delay_ms(mut self, ms: u64) -> Self {
        self.max_delay_ms = ms;
        self
    }

    /// Validate the policy fields, returning an error for obviously wrong configurations.
    ///
    /// Constraints:
    /// - `initial_delay_ms` must be greater than zero.
    /// - `max_delay_ms` must be ≥ `initial_delay_ms` (the backoff cap cannot be
    ///   lower than the starting delay or the cap is meaningless).
    pub fn validate(self) -> Result<Self> {
        if self.initial_delay_ms == 0 {
            return Err(Error::ConfigError(
                "connection_retry.initial_delay_ms must be greater than zero".into(),
            ));
        }
        if self.max_delay_ms < self.initial_delay_ms {
            return Err(Error::ConfigError(format!(
                "connection_retry.max_delay_ms ({}) must be ≥ initial_delay_ms ({})",
                self.max_delay_ms, self.initial_delay_ms
            )));
        }
        Ok(self)
    }
}

/// Source configuration for runtime construction.
#[derive(Clone)]
pub enum RuntimeSourceConfig {
    #[cfg(feature = "postgres")]
    Postgres(PostgresSourceConfig),
    #[cfg(feature = "mysql")]
    Mysql(MysqlSourceConfig),
    #[cfg(feature = "mariadb")]
    MariaDb(crate::source::MariaDbSourceConfig),
    #[cfg(feature = "sqlserver")]
    SqlServer(SqlServerSourceConfig),
    Disabled,
}

impl RuntimeSourceConfig {
    /// Construct a disabled source configuration.
    pub const fn disabled() -> Self {
        Self::Disabled
    }

    /// Construct a PostgreSQL source configuration.
    #[cfg(feature = "postgres")]
    pub fn postgres(source: PostgresSourceConfig) -> Self {
        Self::Postgres(source)
    }

    /// Construct a MySQL source configuration.
    #[cfg(feature = "mysql")]
    pub fn mysql(source: MysqlSourceConfig) -> Self {
        Self::Mysql(source)
    }

    /// Construct a MariaDB source configuration.
    #[cfg(feature = "mariadb")]
    pub fn mariadb(source: crate::source::MariaDbSourceConfig) -> Self {
        Self::MariaDb(source)
    }

    /// Construct a SQL Server source configuration.
    #[cfg(feature = "sqlserver")]
    pub fn sqlserver(source: SqlServerSourceConfig) -> Self {
        Self::SqlServer(source)
    }

    /// Connector identifier when a real source is configured.
    ///
    /// For MySQL and MariaDB, this reflects the `server_flavor` field in the
    /// config, so `RuntimeSourceConfig::Mysql(config_with_mariadb_flavor)` and
    /// `RuntimeSourceConfig::MariaDb(...)` both return `Some("mariadb")`.
    pub fn source_type(&self) -> Option<&'static str> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => Some("postgres"),
            #[cfg(feature = "mysql")]
            Self::Mysql(config) => Some(config.source_type()),
            #[cfg(feature = "mariadb")]
            Self::MariaDb(_) => Some("mariadb"),
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(_) => Some("sqlserver"),
            Self::Disabled => None,
        }
    }

    /// Capabilities advertised by the selected source connector.
    pub fn capabilities(&self) -> ConnectorCapabilities {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => Self::postgres_connector_capabilities(),
            #[cfg(feature = "mysql")]
            Self::Mysql(_) => Self::mysql_connector_capabilities(),
            #[cfg(feature = "mariadb")]
            Self::MariaDb(_) => Self::mysql_connector_capabilities(),
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(config) => {
                Self::sqlserver_connector_capabilities(config.capture_truncate_events)
            }
            Self::Disabled => ConnectorCapabilities::none(),
        }
    }

    /// Capabilities for the MySQL and MariaDB connectors.
    ///
    /// Both connectors capture `TRUNCATE TABLE` from the binlog `QueryEvent`
    /// and emit `Operation::Truncate` events, so `truncate` is always `true`.
    #[cfg(any(feature = "mysql", feature = "mariadb"))]
    const fn mysql_connector_capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities {
            snapshot: true,
            snapshot_checkpoint_resume: true,
            handoff: true,
            ddl_capture: true,
            heartbeat: true,
            tls: cfg!(feature = "tls"),
            schema_introspection: true,
            truncate: true,
            incremental_snapshot: true,
        }
    }

    /// Capabilities for the SQL Server connector.
    ///
    /// `truncate` reflects `SqlServerSourceConfig::capture_truncate_events`:
    /// SQL Server CDC change tables do not record `TRUNCATE TABLE` natively;
    /// truncate capture requires an opt-in DDL trigger
    /// (`capture_truncate_events: true`).
    #[cfg(feature = "sqlserver")]
    const fn sqlserver_connector_capabilities(
        capture_truncate_events: bool,
    ) -> ConnectorCapabilities {
        ConnectorCapabilities {
            snapshot: true,
            snapshot_checkpoint_resume: true,
            handoff: true,
            ddl_capture: true,
            heartbeat: true,
            tls: cfg!(feature = "tls"),
            schema_introspection: true,
            truncate: capture_truncate_events,
            incremental_snapshot: true,
        }
    }

    #[cfg(feature = "postgres")]
    const fn postgres_connector_capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities {
            snapshot: true,
            snapshot_checkpoint_resume: true,
            handoff: true,
            ddl_capture: true,
            heartbeat: true,
            tls: cfg!(feature = "tls"),
            schema_introspection: true,
            truncate: true,
            incremental_snapshot: true,
        }
    }
}

/// Runtime lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeState {
    Idle,
    Running,
    Stopping,
    Stopped,
}

impl std::fmt::Display for RuntimeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        })
    }
}

/// Embeddable admin snapshot for runtime introspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeAdminSnapshot {
    /// Connector type name (e.g. `"postgres"`, `"mysql"`). `None` when source is disabled.
    pub source_type: Option<String>,
    /// Current lifecycle state: `"idle"`, `"running"`, `"stopping"`, or `"stopped"`.
    pub state: String,
    /// `true` when the runtime is ready to serve events (Running + healthy source).
    pub readiness: bool,
    /// `true` when the runtime process is alive (not permanently failed).
    pub liveness: bool,
    /// Set of capabilities reported by the active connector.
    pub capabilities: ConnectorCapabilities,
    /// Number of events currently held in the in-memory event buffer.
    pub buffer_depth: usize,
    /// Number of events delivered to the caller but not yet acknowledged via `commit_ack`.
    pub in_flight_events: usize,
    /// `true` while a snapshot phase is active (initial bulk copy in progress).
    pub snapshot_active: bool,
    /// `true` while a CDC change-stream connection is open.
    pub stream_active: bool,
    /// `true` once the snapshot-to-stream handoff has been completed at least once.
    pub handoff_complete: bool,
    /// Cumulative count of events polled from the source since `start()`. Never resets.
    pub total_events_polled: u64,
    /// Cumulative count of events committed (acknowledged) since `start()`. Never resets.
    pub total_events_committed: u64,
    /// Cumulative count of events suppressed by the idempotency guard since `start()`. Never resets.
    pub total_events_deduplicated: u64,
    /// Unix epoch milliseconds when `start()` was last called. `None` before first start.
    pub started_at_ms: Option<u64>,
    /// Unix epoch milliseconds of the last successful `poll_event_batch` call. `None` if never polled.
    pub last_poll_at_ms: Option<u64>,
    /// Unix epoch milliseconds of the last successful `commit_ack` call. `None` if never committed.
    pub last_commit_at_ms: Option<u64>,
    /// Age of the last durable checkpoint in milliseconds (None if never committed).
    pub checkpoint_age_ms: Option<u64>,
    /// Estimated replication lag from source in milliseconds (None if not available).
    pub replication_lag_ms: Option<u64>,
}

/// Opaque token representing an in-flight batch prefix that may be committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckToken {
    delivery_id: u64,
    event_count: usize,
}

impl AckToken {
    /// Number of events covered by this token.
    pub const fn len(&self) -> usize {
        self.event_count
    }

    /// Whether the token covers zero events.
    pub const fn is_empty(&self) -> bool {
        self.event_count == 0
    }

    /// Split a token into an accepted prefix and an optional remainder token.
    pub fn split_at(self, accepted_count: usize) -> Result<(Self, Option<Self>)> {
        if accepted_count == 0 || accepted_count > self.event_count {
            return Err(Error::CheckpointError(
                "ack token split must accept between 1 and the token length".into(),
            ));
        }

        let accepted = Self {
            delivery_id: self.delivery_id,
            event_count: accepted_count,
        };
        let remaining = self.event_count - accepted_count;
        let remainder = if remaining == 0 {
            None
        } else {
            Some(Self {
                delivery_id: self.delivery_id,
                event_count: remaining,
            })
        };

        Ok((accepted, remainder))
    }
}

/// Describes whether an [`EventBatch`] requires an explicit checkpoint commit.
///
/// `commit_ack()` on [`CdcRuntime`] accepts either `AckMode` or an [`AckToken`]
/// directly (via [`From<AckToken>`]).
///
/// # Contract
///
/// | Variant | When returned | What caller must do |
/// |---|---|---|
/// | `Required(token)` | Non-empty batch with at-least-once delivery active | Call `runtime.commit_ack(mode)` or `runtime.commit_ack(token)`; omitting it stalls the commit barrier and blocks further checkpoint progress. |
/// | `NotRequired` | Empty batch, or source configured without at-least-once delivery (e.g. `RuntimeSourceConfig::Disabled`) | No action needed — omitting the call is safe and correct. |
///
/// # Example
///
/// ```no_run
/// # use rustcdc::{CdcRuntime, AckMode};
/// # async fn example(runtime: &mut CdcRuntime) -> rustcdc::Result<()> {
/// let batch = runtime.poll_event_batch().await?;
/// // Process events ...
/// // Then commit regardless of whether the batch was empty:
/// runtime.commit_ack(batch.ack_mode()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckMode {
    /// The batch must be acknowledged; `token` carries the delivery reference.
    Required(AckToken),
    /// No acknowledgement is needed for this batch.
    NotRequired,
}

impl AckMode {
    /// Return the inner token if acknowledgement is required.
    pub fn token(self) -> Option<AckToken> {
        match self {
            Self::Required(token) => Some(token),
            Self::NotRequired => None,
        }
    }

    /// Return `true` when the batch must be acknowledged.
    pub fn is_required(&self) -> bool {
        matches!(self, Self::Required(_))
    }
}

impl From<AckToken> for AckMode {
    fn from(token: AckToken) -> Self {
        Self::Required(token)
    }
}

impl From<Option<AckToken>> for AckMode {
    fn from(opt: Option<AckToken>) -> Self {
        match opt {
            Some(token) => Self::Required(token),
            None => Self::NotRequired,
        }
    }
}

///
/// Internally the events vector is reference-counted so that the runtime can
/// keep a copy in `pending_delivery` for replay without an O(n) clone per
/// delivery.  All public accessors expose the same slice/vec API as before.
#[derive(Debug, Clone, PartialEq)]
pub struct EventBatch {
    events: Arc<Vec<Event>>,
    ack_token: Option<AckToken>,
}

impl EventBatch {
    fn empty() -> Self {
        Self {
            events: Arc::new(Vec::new()),
            ack_token: None,
        }
    }

    /// Borrow the delivered events.
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Consume the batch and return its events.
    ///
    /// If the runtime has already committed and dropped its internal reference
    /// (via `commit_ack`) this is zero-copy; otherwise the vector is cloned.
    pub fn into_events(self) -> Vec<Event> {
        Arc::try_unwrap(self.events).unwrap_or_else(|arc| (*arc).clone())
    }

    /// Return the acknowledgement mode for this batch.
    ///
    /// - [`AckMode::Required`] — the batch contains events and at-least-once delivery is
    ///   active. You **must** call `runtime.commit_ack(batch.ack_mode())` to advance the
    ///   commit barrier. Omitting the call stalls checkpoint progress indefinitely.
    /// - [`AckMode::NotRequired`] — the batch is empty, or the source is configured without
    ///   at-least-once delivery. Calling `commit_ack` is a safe no-op in this case.
    ///
    /// Passing the return value directly to `commit_ack` is always correct:
    /// ```no_run
    /// # use rustcdc::CdcRuntime;
    /// # async fn example(runtime: &mut CdcRuntime) -> rustcdc::Result<()> {
    /// let batch = runtime.poll_event_batch().await?;
    /// runtime.commit_ack(batch.ack_mode()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn ack_mode(&self) -> AckMode {
        match &self.ack_token {
            Some(token) => AckMode::Required(token.clone()),
            None => AckMode::NotRequired,
        }
    }

    /// Number of events in the batch.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Returns the smallest `ts` (milliseconds since epoch) across all events in this batch.
    ///
    /// Returns `None` when the batch is empty.
    pub fn oldest_event_source_timestamp_ms(&self) -> Option<u64> {
        self.events.iter().map(|e| e.ts).min()
    }
}

#[derive(Clone)]
struct PendingDelivery {
    delivery_id: u64,
    events: Arc<Vec<Event>>,
    /// Number of events from the front of `events` that have already been committed.
    committed_prefix: usize,
}

/// Behavior when a transform stage returns an error for an event.
///
/// Controls how the runtime handles transformation failures during event processing.
/// This is a critical operational toggle for balancing reliability (halt on corruption)
/// against availability (skip and continue on transient errors).
///
/// **Default:** `Halt` — Fail-safe by default; embedders must explicitly opt-in to skip behavior.
///
/// # Variants
///
/// - **`Halt`** (default): Stop polling and immediately return an error to the caller.
///   Use this when data integrity is non-negotiable (e.g., fraud detection pipelines).
///   Errors are surfaced as `[`Error::TransformError`] with transform stage context.
///
/// - **`Skip`**: Log a warning and silently skip the failed event, continuing to the next event.
///   Use this for best-effort enrichment (e.g., adding geo-location tags). Dropped events
///   are counted in metrics (`transform_error_skipped_count`).
///
/// # Observability
///
/// Both policies emit structured logs and runtime error telemetry through
/// `MetricsCollector::record_error`, differing only in downstream runtime behavior.
///
/// # Example Configuration
///
/// ```ignore
/// # Halt on any transform error (production default)
/// config.with_transform_error_policy(TransformErrorPolicy::Halt)
///
/// # Skip failing events (dev/testing or lenient pipelines)
/// config.with_transform_error_policy(TransformErrorPolicy::Skip)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransformErrorPolicy {
    Halt,
    Skip,
}

impl TransformErrorPolicy {
    /// Human-readable description of the policy.
    pub fn description(&self) -> &'static str {
        match self {
            Self::Halt => "halt on transform error and return to caller",
            Self::Skip => "skip failing event, log warning, and continue",
        }
    }
}

impl std::fmt::Display for TransformErrorPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.description())
    }
}

/// Behavior when source confirmation fails after checkpoint durability is already guaranteed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PostCommitSourceConfirmPolicy {
    /// Keep ack successful once checkpoint commit is durable and emit warning telemetry.
    Continue,
    /// Return an error even though checkpoint durability already succeeded.
    FailFast,
}

impl PostCommitSourceConfirmPolicy {
    /// Human-readable description of the policy.
    pub fn description(&self) -> &'static str {
        match self {
            Self::Continue => "keep ack successful and emit warning",
            Self::FailFast => "return error after durable commit on confirmation failure",
        }
    }
}

impl std::fmt::Display for PostCommitSourceConfirmPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.description())
    }
}

/// Runtime configuration for embedded execution.
pub struct RuntimeConfig {
    /// Source configuration used by the runtime.
    pub source: RuntimeSourceConfig,
    /// Snapshot table list used on first run when no checkpoint exists.
    pub snapshot_tables: Vec<String>,
    /// Optional incremental (non-blocking) snapshot configuration.
    ///
    /// When set, runtime startup initializes stream ingestion through the connector's
    /// watermark-based incremental snapshot handle instead of the classic
    /// snapshot + handoff path.
    pub incremental_snapshot: Option<IncrementalSnapshotConfig>,
    /// Checkpoint backend owned by the runtime.
    pub checkpoint: Box<dyn crate::checkpoint::Checkpoint>,
    /// Schema history backend owned by the runtime.
    pub schema_history: Box<dyn SchemaHistory>,
    /// Explicit runtime options including observability and tuning defaults.
    pub options: RuntimeOptions,
}

impl RuntimeConfig {
    /// Create a config boxing the provided checkpoint and schema history implementations.
    pub fn new<C, H>(source: RuntimeSourceConfig, checkpoint: C, schema_history: H) -> Self
    where
        C: crate::checkpoint::Checkpoint + 'static,
        H: SchemaHistory + 'static,
    {
        Self {
            source,
            snapshot_tables: Vec::new(),
            incremental_snapshot: None,
            checkpoint: Box::new(checkpoint),
            schema_history: Box::new(schema_history),
            options: RuntimeOptions::default(),
        }
    }

    /// Replace the full runtime options surface.
    pub fn with_options(mut self, options: RuntimeOptions) -> Self {
        self.options = options;
        self
    }

    /// Replace the observability configuration.
    pub fn with_observability(mut self, observability: RuntimeObservability) -> Self {
        self.options = self.options.with_observability(observability);
        self
    }

    /// Override the metrics collector.
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsCollector>) -> Self {
        self.options.observability.metrics = metrics;
        self
    }

    /// Override the tracer.
    pub fn with_tracer(mut self, tracer: Arc<dyn EventTracer>) -> Self {
        self.options.observability.tracer = tracer;
        self
    }

    /// Override the maximum buffer size.
    pub fn with_max_buffer_size(mut self, max_buffer_size: usize) -> Self {
        self.options = self.options.with_max_buffer_size(max_buffer_size);
        self
    }

    /// Override the poll wait budget in milliseconds.
    pub fn with_max_poll_wait_ms(mut self, max_poll_wait_ms: u64) -> Self {
        self.options = self.options.with_max_poll_wait_ms(max_poll_wait_ms);
        self
    }

    /// Configure transform failure behavior. **Defaults to [`TransformErrorPolicy::Halt`].**
    ///
    /// # Operational Guidance
    ///
    /// - **Production:** Use `Halt` (default) to fail fast on data corruption.
    /// - **Staging/Testing:** Use `Skip` for tolerant evaluation (e.g., optional enrichment).
    /// - **Change at Runtime:** Policy is set at config time; to change behavior, recreate runtime.
    ///
    /// # Error Context
    ///
    /// Errors during transform execution include the transform's name and the event ID,
    /// enabling quick diagnosis. All failed events are logged regardless of policy.
    pub fn with_transform_error_policy(mut self, policy: TransformErrorPolicy) -> Self {
        self.options = self.options.with_transform_error_policy(policy);
        self
    }

    /// Configure post-commit source confirmation behavior.
    pub fn with_post_commit_source_confirm_policy(
        mut self,
        policy: PostCommitSourceConfirmPolicy,
    ) -> Self {
        self.options = self.options.with_post_commit_source_confirm_policy(policy);
        self
    }

    /// Configure runtime-level idempotency guard options.
    ///
    /// Duplicate detection runs before transform stages, so dedupe decisions
    /// are stable even when downstream transforms are nondeterministic.
    pub fn with_idempotency(mut self, idempotency: IdempotencyOptions) -> Self {
        self.options = self.options.with_idempotency(idempotency);
        self
    }

    /// Explicitly disable runtime-level duplicate suppression.
    pub fn with_idempotency_disabled(mut self) -> Self {
        self.options = self.options.with_idempotency_disabled();
        self
    }

    /// Enable or disable canonical event-envelope validation at runtime ingress.
    pub fn with_event_validation(mut self, enabled: bool) -> Self {
        self.options = self.options.with_event_validation(enabled);
        self
    }

    /// Configure runtime-managed schema-history retention after DDL persistence.
    pub fn with_schema_history_retention(mut self, retention: SchemaHistoryRetention) -> Self {
        self.options = self.options.with_schema_history_retention(retention);
        self
    }

    /// Configure snapshot tables for initial snapshot mode.
    pub fn with_snapshot_tables(mut self, snapshot_tables: Vec<String>) -> Self {
        self.snapshot_tables = snapshot_tables;
        self
    }

    /// Configure runtime startup to use incremental (non-blocking) snapshot mode.
    ///
    /// This supersedes the classic `with_snapshot_tables` bootstrapping path.
    /// Do not set both at once.
    pub fn with_incremental_snapshot(mut self, config: IncrementalSnapshotConfig) -> Self {
        self.incremental_snapshot = Some(config);
        self
    }
}

enum RuntimeSource {
    #[cfg(feature = "postgres")]
    Postgres(PostgresConnection),
    #[cfg(feature = "mysql")]
    Mysql(MysqlConnection),
    #[cfg(feature = "sqlserver")]
    SqlServer(SqlServerConnection),
    Disabled,
    #[cfg(test)]
    Mock(Box<dyn crate::source::Source>),
}

impl RuntimeSource {
    async fn connect(&self) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(source) => source.connect().await,
            #[cfg(feature = "mysql")]
            Self::Mysql(source) => source.connect().await,
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(source) => source.connect().await,
            Self::Disabled => Err(Error::ConfigError(
                "runtime source is disabled in this build".into(),
            )),
            #[cfg(test)]
            Self::Mock(_) => Ok(()),
        }
    }

    async fn close(&self) {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(source) => source.close().await,
            #[cfg(feature = "mysql")]
            Self::Mysql(source) => source.close().await,
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(source) => source.close().await,
            Self::Disabled => {}
            #[cfg(test)]
            Self::Mock(_) => {}
        }
    }

    #[allow(unused_variables)]
    async fn start_snapshot(&mut self, tables: &[String]) -> Result<Box<dyn SnapshotHandle>> {
        let refs = tables.iter().map(String::as_str).collect::<Vec<_>>();
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(source) => source.start_snapshot(&refs).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(source) => source.start_snapshot(&refs).await,
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(source) => source.start_snapshot(&refs).await,
            Self::Disabled => Err(Error::ConfigError(
                "runtime source is disabled in this build".into(),
            )),
            #[cfg(test)]
            Self::Mock(source) => source.start_snapshot(&refs).await,
        }
    }

    #[allow(unused_variables)]
    async fn start_snapshot_from_checkpoint(
        &mut self,
        tables: &[String],
        resume_from: &dyn Offset,
    ) -> Result<Box<dyn SnapshotHandle>> {
        let refs = tables.iter().map(String::as_str).collect::<Vec<_>>();
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(source) => {
                source
                    .start_snapshot_from_checkpoint(&refs, Some(resume_from))
                    .await
            }
            #[cfg(feature = "mysql")]
            Self::Mysql(source) => {
                source
                    .start_snapshot_from_checkpoint(&refs, Some(resume_from))
                    .await
            }
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(source) => {
                source
                    .start_snapshot_from_checkpoint(&refs, Some(resume_from))
                    .await
            }
            Self::Disabled => Err(Error::ConfigError(
                "runtime source is disabled in this build".into(),
            )),
            #[cfg(test)]
            Self::Mock(source) => {
                source
                    .start_snapshot_from_checkpoint(&refs, Some(resume_from))
                    .await
            }
        }
    }

    #[allow(unused_variables)]
    async fn start_stream(
        &mut self,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(source) => source.start_stream(resume_from).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(source) => source.start_stream(resume_from).await,
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(source) => source.start_stream(resume_from).await,
            Self::Disabled => Err(Error::ConfigError(
                "runtime source is disabled in this build".into(),
            )),
            #[cfg(test)]
            Self::Mock(source) => source.start_stream(resume_from).await,
        }
    }

    #[allow(unused_variables)]
    async fn start_incremental_snapshot(
        &mut self,
        config: IncrementalSnapshotConfig,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(source) => source.start_incremental_snapshot(config, resume_from).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(source) => source.start_incremental_snapshot(config, resume_from).await,
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(source) => source.start_incremental_snapshot(config, resume_from).await,
            Self::Disabled => Err(Error::ConfigError(
                "runtime source is disabled in this build".into(),
            )),
            #[cfg(test)]
            Self::Mock(_) => Err(Error::ConfigError(
                "incremental snapshot startup is unsupported for mock runtime source".into(),
            )),
        }
    }

    #[allow(unused_variables)]
    async fn perform_handoff(
        &mut self,
        snapshot: &mut dyn SnapshotHandle,
        stream: &mut dyn StreamHandle,
    ) -> Result<HandoffResult> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(source) => source.perform_handoff(snapshot, stream).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(source) => source.perform_handoff(snapshot, stream).await,
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(source) => source.perform_handoff(snapshot, stream).await,
            Self::Disabled => Err(Error::ConfigError(
                "runtime source is disabled in this build".into(),
            )),
            #[cfg(test)]
            Self::Mock(source) => source.perform_handoff(snapshot, stream).await,
        }
    }
}

/// Embedded runtime for source orchestration.
pub struct CdcRuntime {
    config: RuntimeConfig,
    state: RuntimeState,
    injected_events: VecDeque<Event>,
    pending_source_events: VecDeque<Event>,
    buffered_events: VecDeque<Event>,
    delivered_not_committed: usize,
    next_delivery_id: u64,
    pending_delivery: Option<PendingDelivery>,
    commit_barrier: CommitBarrier,
    source: RuntimeSource,
    snapshot: Option<Box<dyn SnapshotHandle>>,
    stream: Option<Box<dyn StreamHandle>>,
    handoff_complete: bool,
    started_at_ms: Option<u64>,
    last_poll_at_ms: Option<u64>,
    last_source_event_ts_ms: Option<u64>,
    last_commit_at_ms: Option<u64>,
    total_events_polled: u64,
    total_events_committed: u64,
    total_events_deduplicated: u64,
    last_checkpoint_saved_at_ms: Option<u64>,
    transform_pipeline: TransformPipeline,
    idempotency_guard: Option<EventIdempotencyGuard>,
}

impl CdcRuntime {
    fn observability(&self) -> &RuntimeObservability {
        &self.config.options.observability
    }

    fn record_runtime_error(&self, context: &str, error: &Error) {
        self.observability().metrics.record_error(error, context);
    }

    fn record_replication_lag_metric(&self) {
        if let Some(lag_ms) = self.estimate_replication_lag_ms() {
            let lag_events = self
                .buffered_events
                .len()
                .saturating_add(self.injected_events.len())
                .saturating_add(
                    self.pending_delivery
                        .as_ref()
                        .map_or(0, |pending| pending.events.len()),
                ) as u64;
            self.observability()
                .metrics
                .record_replication_lag_ms(lag_ms, lag_events);
        }
    }

    fn event_trace_id(event: &Event) -> String {
        format!(
            "{}:{}:{}:{}",
            event.source.source_name, event.table, event.source.offset, event.ts
        )
    }

    /// Create a new runtime.
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        if config.options.max_buffer_size == 0 {
            return Err(Error::ConfigError(
                "max_buffer_size must be greater than zero".into(),
            ));
        }

        if !config.snapshot_tables.is_empty() && config.incremental_snapshot.is_some() {
            return Err(Error::ConfigError(
                "snapshot_tables and incremental_snapshot are mutually exclusive — use one or the other, not both".into(),
            ));
        }

        let capabilities = config.source.capabilities();
        // Skip capability checks for Disabled sources (used in tests with mock sources).
        if !matches!(config.source, RuntimeSourceConfig::Disabled) {
            if !config.snapshot_tables.is_empty() && !capabilities.snapshot {
                return Err(Error::ConfigError(
                    "configured source does not support snapshot mode".into(),
                ));
            }
            if !config.snapshot_tables.is_empty() && !capabilities.handoff {
                return Err(Error::ConfigError(
                    "configured source does not support snapshot-to-stream handoff".into(),
                ));
            }
        }

        // Validate the retry policy early so callers get a clear error at construction
        // time rather than a subtle misconfiguration silently surviving into the poll loop.
        if let Some(retry) = config.options.connection_retry {
            retry.validate()?;
        }

        let source = Self::build_source(&config)?;
        let idempotency_guard = Self::build_idempotency_guard(&config.options)?;
        Ok(Self {
            commit_barrier: CommitBarrier::new(config.options.max_buffer_size),
            config,
            state: RuntimeState::Idle,
            injected_events: VecDeque::new(),
            pending_source_events: VecDeque::new(),
            buffered_events: VecDeque::new(),
            delivered_not_committed: 0,
            next_delivery_id: 1,
            pending_delivery: None,
            source,
            snapshot: None,
            stream: None,
            handoff_complete: false,
            started_at_ms: None,
            last_poll_at_ms: None,
            last_source_event_ts_ms: None,
            last_commit_at_ms: None,
            total_events_polled: 0,
            total_events_committed: 0,
            total_events_deduplicated: 0,
            last_checkpoint_saved_at_ms: None,
            transform_pipeline: TransformPipeline::default(),
            idempotency_guard,
        })
    }

    fn build_idempotency_guard(options: &RuntimeOptions) -> Result<Option<EventIdempotencyGuard>> {
        let Some(idempotency) = options.idempotency else {
            return Ok(None);
        };

        let guard = EventIdempotencyGuard::new(idempotency.capacity)?;
        let guard = if let Some(ttl_ms) = idempotency.ttl_ms {
            guard.with_ttl_ms(ttl_ms)?
        } else {
            guard
        };

        Ok(Some(guard))
    }

    fn build_source(config: &RuntimeConfig) -> Result<RuntimeSource> {
        match &config.source {
            #[cfg(feature = "postgres")]
            RuntimeSourceConfig::Postgres(source) => Ok(RuntimeSource::Postgres(
                PostgresConnection::new(source.clone()),
            )),
            #[cfg(feature = "mysql")]
            RuntimeSourceConfig::Mysql(source) => {
                Ok(RuntimeSource::Mysql(MysqlConnection::new(source.clone())))
            }
            #[cfg(feature = "mariadb")]
            RuntimeSourceConfig::MariaDb(source) => Ok(RuntimeSource::Mysql(MysqlConnection::new(
                source.clone().into_inner(),
            ))),
            #[cfg(feature = "sqlserver")]
            RuntimeSourceConfig::SqlServer(source) => Ok(RuntimeSource::SqlServer(
                SqlServerConnection::new(source.clone()),
            )),
            RuntimeSourceConfig::Disabled => Ok(RuntimeSource::Disabled),
        }
    }

    /// Add a transform stage applied to polled events.
    pub fn add_transform(&mut self, transform: Box<dyn crate::transform::Transform>) {
        self.transform_pipeline.add_transform(transform);
    }

    /// Replace the runtime source with a mock for testing.
    #[cfg(test)]
    pub(crate) fn inject_mock_source(&mut self, source: Box<dyn crate::source::Source>) {
        self.source = RuntimeSource::Mock(source);
    }
}

mod runtime_admin;
mod runtime_lifecycle;
mod runtime_poll;

#[cfg(test)]
mod tests {
    #[cfg(feature = "encryption")]
    use ahash::AHashMap as HashMap;
    use async_trait::async_trait;
    use futures_util::StreamExt;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    #[cfg(feature = "encryption")]
    use crate::transform::{MaskHashConfig, MaskHashTransform, MaskRule};
    use crate::{
        checkpoint::{Checkpoint, InMemoryCheckpoint},
        core::{
            Event, EventTracer, MetricsCollector, NoOpEventTracer, NoOpMetricsCollector, Operation,
            SnapshotMetadata, SourceMetadata, EVENT_ENVELOPE_VERSION,
        },
        ddl_capture::DdlDialect,
        schema_history::{InMemorySchemaHistory, SchemaHistoryRetention},
        transform::Transform,
    };

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
    use crate::checkpoint::FileCheckpoint;

    use super::{
        AckMode, CdcRuntime, ConnectionRetryPolicy, IdempotencyOptions, RuntimeConfig,
        RuntimeObservability, RuntimeSourceConfig, RuntimeState, TransformErrorPolicy,
    };

    #[cfg(feature = "postgres")]
    use super::{PostCommitSourceConfirmPolicy, RuntimeSource};

    fn event() -> Event {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or_default();
        Event {
            before: None,
            after: Some(json!({"id": 1})),
            op: Operation::Read,
            source: SourceMetadata {
                source_name: "mock".into(),
                offset: "1".into(),
                timestamp: now,
            },
            ts: now,
            schema: Some("public".into()),
            table: "users".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    #[derive(Debug, Default)]
    struct RecordingMetricsState {
        event_processed_calls: usize,
        checkpoint_commits: usize,
        replication_lag_calls: usize,
        error_contexts: Vec<String>,
    }

    #[derive(Clone)]
    struct RecordingMetrics {
        state: Arc<Mutex<RecordingMetricsState>>,
    }

    impl RecordingMetrics {
        fn new(state: Arc<Mutex<RecordingMetricsState>>) -> Self {
            Self { state }
        }
    }

    impl MetricsCollector for RecordingMetrics {
        fn record_event_processed(&self, _op: Operation, _latency_ms: u64) {
            let mut state = self
                .state
                .lock()
                .expect("recording metrics mutex should not be poisoned");
            state.event_processed_calls += 1;
        }

        fn record_checkpoint_committed(&self, _event_count: u64, _latency_ms: u64) {
            let mut state = self
                .state
                .lock()
                .expect("recording metrics mutex should not be poisoned");
            state.checkpoint_commits += 1;
        }

        fn record_replication_lag_ms(&self, _lag_ms: u64, _lag_events: u64) {
            let mut state = self
                .state
                .lock()
                .expect("recording metrics mutex should not be poisoned");
            state.replication_lag_calls += 1;
        }

        fn record_error(&self, _error: &crate::core::Error, context: &str) {
            let mut state = self
                .state
                .lock()
                .expect("recording metrics mutex should not be poisoned");
            state.error_contexts.push(context.to_string());
        }
    }

    #[derive(Debug, Default)]
    struct RecordingTracerState {
        event_starts: Vec<String>,
        event_ends: Vec<(String, String)>,
        checkpoint_states: Vec<String>,
    }

    #[derive(Clone)]
    struct RecordingTracer {
        state: Arc<Mutex<RecordingTracerState>>,
    }

    impl RecordingTracer {
        fn new(state: Arc<Mutex<RecordingTracerState>>) -> Self {
            Self { state }
        }
    }

    impl EventTracer for RecordingTracer {
        fn trace_event_start(&self, event_id: &str) {
            let mut state = self
                .state
                .lock()
                .expect("recording tracer mutex should not be poisoned");
            state.event_starts.push(event_id.to_string());
        }

        fn trace_event_end(&self, event_id: &str, status: &str) {
            let mut state = self
                .state
                .lock()
                .expect("recording tracer mutex should not be poisoned");
            state
                .event_ends
                .push((event_id.to_string(), status.to_string()));
        }

        fn trace_checkpoint_barrier(&self, state_label: &str) {
            let mut state = self
                .state
                .lock()
                .expect("recording tracer mutex should not be poisoned");
            state.checkpoint_states.push(state_label.to_string());
        }
    }

    #[test]
    fn runtime_config_defaults_to_explicit_noop_observability() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);

        let default_metrics: Arc<dyn MetricsCollector> = Arc::new(NoOpMetricsCollector);
        let default_tracer: Arc<dyn EventTracer> = Arc::new(NoOpEventTracer);

        assert_eq!(
            Arc::strong_count(&config.options.observability.metrics),
            Arc::strong_count(&default_metrics)
        );
        assert_eq!(
            Arc::strong_count(&config.options.observability.tracer),
            Arc::strong_count(&default_tracer)
        );
        assert_eq!(config.options.max_buffer_size, 10_000);
        assert_eq!(config.options.max_poll_wait_ms, 5_000);
        assert_eq!(
            config.options.transform_error_policy,
            TransformErrorPolicy::Halt
        );
        let idempotency = config
            .options
            .idempotency
            .expect("default idempotency enabled");
        assert_eq!(
            idempotency.capacity,
            super::DEFAULT_RUNTIME_IDEMPOTENCY_CAPACITY
        );
        assert!(idempotency.ttl_ms.is_none());
    }

    #[test]
    fn runtime_config_can_disable_default_idempotency() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_idempotency_disabled();

        assert!(config.options.idempotency.is_none());
    }

    #[test]
    fn runtime_config_can_replace_observability_explicitly() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let observability = RuntimeObservability::default()
            .with_metrics(Arc::new(NoOpMetricsCollector))
            .with_tracer(Arc::new(NoOpEventTracer));
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_observability(observability.clone());

        assert!(Arc::ptr_eq(
            &config.options.observability.metrics,
            &observability.metrics
        ));
        assert!(Arc::ptr_eq(
            &config.options.observability.tracer,
            &observability.tracer
        ));
    }

    #[test]
    fn runtime_source_capabilities_are_exposed_programmatically() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let runtime = CdcRuntime::new(config).unwrap();
        let caps = runtime.source_capabilities();

        assert!(!caps.snapshot);
        assert!(!caps.snapshot_checkpoint_resume);
        assert!(!caps.handoff);
        assert!(!caps.ddl_capture);
        assert!(!caps.heartbeat);
        assert!(!caps.tls);
        assert!(!caps.schema_introspection);
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_runtime_source_capabilities_report_ddl_capture() {
        let caps = RuntimeSourceConfig::Postgres(crate::source::PostgresSourceConfig::default())
            .capabilities();

        assert!(caps.snapshot);
        assert!(caps.snapshot_checkpoint_resume);
        assert!(caps.handoff);
        assert!(caps.ddl_capture);
        assert!(caps.heartbeat);
        assert!(caps.schema_introspection);
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_runtime_source_capabilities_report_ddl_capture() {
        let caps =
            RuntimeSourceConfig::Mysql(crate::source::MysqlSourceConfig::default()).capabilities();

        assert!(caps.snapshot);
        assert!(caps.snapshot_checkpoint_resume);
        assert!(caps.handoff);
        assert!(caps.ddl_capture);
        assert!(caps.heartbeat);
        assert!(caps.schema_introspection);
        assert!(
            caps.truncate,
            "MySQL connector must report truncate support (binlog QueryEvent)"
        );
    }

    #[cfg(feature = "sqlserver")]
    #[test]
    fn sqlserver_runtime_source_capabilities_report_ddl_capture() {
        let caps = RuntimeSourceConfig::SqlServer(crate::source::SqlServerSourceConfig::default())
            .capabilities();

        assert!(caps.snapshot);
        assert!(caps.snapshot_checkpoint_resume);
        assert!(caps.handoff);
        assert!(caps.ddl_capture);
        assert!(caps.heartbeat);
        assert!(caps.schema_introspection);
        assert!(
            !caps.truncate,
            "SQL Server connector reports truncate false by default (requires capture_truncate_events: true)"
        );
    }

    #[cfg(feature = "sqlserver")]
    #[test]
    fn sqlserver_runtime_source_capabilities_truncate_enabled_when_opt_in() {
        let config = crate::source::SqlServerSourceConfig {
            capture_truncate_events: true,
            ..Default::default()
        };
        let caps = RuntimeSourceConfig::SqlServer(config).capabilities();
        assert!(
            caps.truncate,
            "SQL Server connector must report truncate when capture_truncate_events is enabled"
        );
    }

    #[test]
    fn runtime_admin_snapshot_exposes_capabilities_and_health_flags() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let runtime = CdcRuntime::new(config).unwrap();

        let admin = runtime.admin_snapshot();
        assert_eq!(admin.state, "idle");
        assert!(!admin.readiness);
        assert!(admin.liveness);
        assert!(!admin.capabilities.snapshot);
        assert_eq!(admin.total_events_polled, 0);
        assert_eq!(admin.total_events_committed, 0);
    }

    #[tokio::test]
    async fn runtime_admin_json_and_prometheus_outputs_include_runtime_state() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.inject_mock_source(Box::new(MockSource::with_snapshot(Vec::new(), Vec::new())));

        runtime.start().await.unwrap();
        runtime.enqueue_event(event()).unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        runtime.commit_ack(batch.ack_mode()).await.unwrap();

        let json = runtime.admin_snapshot_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["state"], "running");
        assert_eq!(parsed["readiness"], true);
        assert_eq!(parsed["total_events_polled"], 1);
        assert_eq!(parsed["total_events_committed"], 1);

        let prometheus = runtime.admin_metrics_prometheus();
        assert!(prometheus.contains("cdc_runtime_readiness"));
        assert!(prometheus.contains("cdc_runtime_events_polled_total"));
        assert!(prometheus.contains("source_type=\""));
        assert!(prometheus.contains("} 1"));
        assert!(prometheus.contains("capability=\"snapshot\""));
    }

    #[test]
    fn runtime_allows_snapshot_tables_on_disabled_source_for_testing() {
        // Disabled sources are placeholder sources used in tests with mock sources.
        // They don't enforce capability constraints since the mock will be injected after construction.
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_snapshot_tables(vec!["public.users".to_string()]);

        let result = CdcRuntime::new(config);
        // Disabled sources allow snapshot_tables; capability checks are skipped for them.
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn runtime_rejects_double_start() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.start().await.unwrap();
        assert!(runtime.start().await.is_err());
    }

    #[tokio::test]
    async fn runtime_enqueue_poll_commit_stop_cycle() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();

        assert_eq!(runtime.state(), RuntimeState::Idle);
        runtime.enqueue_event(event()).unwrap();

        let events = runtime.poll_event_batch().await.unwrap_err();
        assert!(matches!(events, crate::core::Error::StateError(_)));

        runtime.state = RuntimeState::Running;
        let batch = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch.len(), 1);

        runtime.commit_ack(batch.ack_mode()).await.unwrap();
        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            1
        );
        runtime.state = RuntimeState::Stopped;
    }

    #[tokio::test]
    async fn runtime_start_hydrates_committed_count_from_checkpoint() {
        let checkpoint = InMemoryCheckpoint::default();

        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(
            RuntimeSourceConfig::Disabled,
            checkpoint.clone(),
            schema_history,
        )
        .with_idempotency_disabled();
        let mut first_runtime = CdcRuntime::new(config).unwrap();

        first_runtime.start().await.unwrap();
        first_runtime.enqueue_event(event()).unwrap();
        first_runtime.enqueue_event(event()).unwrap();

        let first_batch = first_runtime.poll_event_batch().await.unwrap();
        assert_eq!(first_batch.len(), 2);
        first_runtime
            .commit_ack(first_batch.ack_mode())
            .await
            .unwrap();
        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 2);

        first_runtime.stop().await.unwrap();

        let second_schema_history = InMemorySchemaHistory::default();
        let second_config = RuntimeConfig::new(
            RuntimeSourceConfig::Disabled,
            checkpoint.clone(),
            second_schema_history,
        )
        .with_idempotency_disabled();
        let mut second_runtime = CdcRuntime::new(second_config).unwrap();

        second_runtime.start().await.unwrap();
        second_runtime.enqueue_event(event()).unwrap();

        let second_batch = second_runtime.poll_event_batch().await.unwrap();
        assert_eq!(second_batch.len(), 1);
        second_runtime
            .commit_ack(second_batch.ack_mode())
            .await
            .unwrap();

        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn runtime_observability_emits_delivery_commit_and_barrier_signals() {
        let metrics_state = Arc::new(Mutex::new(RecordingMetricsState::default()));
        let tracer_state = Arc::new(Mutex::new(RecordingTracerState::default()));
        let observability = RuntimeObservability::default()
            .with_metrics(Arc::new(RecordingMetrics::new(Arc::clone(&metrics_state))))
            .with_tracer(Arc::new(RecordingTracer::new(Arc::clone(&tracer_state))));

        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_observability(observability)
            .with_idempotency_disabled();
        let mut runtime = CdcRuntime::new(config).unwrap();

        runtime.start().await.unwrap();
        runtime.enqueue_event(event()).unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        runtime.commit_ack(batch.ack_mode()).await.unwrap();

        let metrics = metrics_state
            .lock()
            .expect("recording metrics mutex should not be poisoned");
        assert_eq!(metrics.event_processed_calls, 1);
        assert_eq!(metrics.checkpoint_commits, 1);
        assert!(metrics.replication_lag_calls >= 1);
        drop(metrics);

        let tracer = tracer_state
            .lock()
            .expect("recording tracer mutex should not be poisoned");
        assert_eq!(tracer.event_starts.len(), 1);
        assert_eq!(tracer.event_ends.len(), 1);
        assert_eq!(tracer.event_ends[0].1, "committed");
        assert!(tracer.checkpoint_states.iter().any(|state| state == "open"));
        assert!(tracer
            .checkpoint_states
            .iter()
            .any(|state| state == "accepting"));
        assert!(tracer
            .checkpoint_states
            .iter()
            .any(|state| state == "flushing"));
        assert!(tracer
            .checkpoint_states
            .iter()
            .any(|state| state == "committed"));
    }

    #[tokio::test]
    async fn runtime_observability_records_poll_state_errors() {
        let metrics_state = Arc::new(Mutex::new(RecordingMetricsState::default()));
        let observability = RuntimeObservability::default()
            .with_metrics(Arc::new(RecordingMetrics::new(Arc::clone(&metrics_state))))
            .with_tracer(Arc::new(NoOpEventTracer));

        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_observability(observability)
            .with_idempotency_disabled();
        let mut runtime = CdcRuntime::new(config).unwrap();

        let error = runtime.poll_event_batch().await.unwrap_err();
        assert!(matches!(error, crate::core::Error::StateError(_)));

        let metrics = metrics_state
            .lock()
            .expect("recording metrics mutex should not be poisoned");
        assert!(metrics
            .error_contexts
            .iter()
            .any(|context| context == "runtime.poll.state"));
    }

    #[tokio::test]
    async fn runtime_rejects_reusing_ack_token() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.state = RuntimeState::Running;
        runtime.enqueue_event(event()).unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        let AckMode::Required(token) = batch.ack_mode() else {
            panic!("expected ack token")
        };
        runtime.commit_ack(token.clone()).await.unwrap();

        let error = runtime.commit_ack(token).await.unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));
    }

    #[derive(Debug)]
    struct FailTransform;
    #[derive(Debug)]
    struct NonDeterministicTransform;

    #[async_trait]
    impl Transform for FailTransform {
        async fn apply(&self, _event: &mut Event) -> crate::core::Result<bool> {
            Err(crate::core::Error::TransformError("boom".into()))
        }

        fn name(&self) -> &str {
            "fail_transform"
        }
    }

    #[async_trait]
    impl Transform for NonDeterministicTransform {
        async fn apply(&self, event: &mut Event) -> crate::core::Result<bool> {
            static NEXT_NONCE: AtomicU64 = AtomicU64::new(1);
            let nonce = NEXT_NONCE.fetch_add(1, Ordering::Relaxed);

            if let Some(serde_json::Value::Object(after)) = &mut event.after {
                after.insert("nondeterministic_nonce".into(), serde_json::json!(nonce));
            }

            Ok(true)
        }

        fn name(&self) -> &str {
            "non_deterministic_transform"
        }
    }

    #[tokio::test]
    async fn transform_error_policy_halt_returns_error() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_transform_error_policy(TransformErrorPolicy::Halt);
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.add_transform(Box::new(FailTransform));

        let error = runtime.apply_transforms(vec![event()]).await.unwrap_err();
        assert!(matches!(error, crate::core::Error::TransformError(_)));
    }

    #[tokio::test]
    async fn transform_error_policy_skip_drops_failing_event() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_transform_error_policy(TransformErrorPolicy::Skip);
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.add_transform(Box::new(FailTransform));

        let events = runtime.apply_transforms(vec![event()]).await.unwrap();
        assert!(events.is_empty());
    }

    // ─── Mock source infrastructure ─────────────────────────────────────────

    use std::collections::VecDeque as TestDeque;

    struct MockStreamHandle {
        batches: TestDeque<Vec<Event>>,
        confirmed_lsns: Arc<Mutex<Vec<u64>>>,
        confirm_lsn_error: Option<String>,
    }

    impl MockStreamHandle {
        fn new(
            batches: Vec<Vec<Event>>,
            confirmed_lsns: Arc<Mutex<Vec<u64>>>,
            confirm_lsn_error: Option<String>,
        ) -> Self {
            Self {
                batches: batches.into_iter().collect(),
                confirmed_lsns,
                confirm_lsn_error,
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::source::StreamHandle for MockStreamHandle {
        async fn next_events(&mut self, _timeout_ms: u64) -> crate::core::Result<Vec<Event>> {
            Ok(self.batches.pop_front().unwrap_or_default())
        }

        async fn save_position(
            &self,
            _checkpoint: &mut dyn crate::checkpoint::Checkpoint,
        ) -> crate::core::Result<()> {
            Ok(())
        }

        async fn confirm_lsn(&mut self, lsn: u64) -> crate::core::Result<()> {
            if let Some(message) = &self.confirm_lsn_error {
                return Err(crate::core::Error::SourceError(message.clone()));
            }
            self.confirmed_lsns
                .lock()
                .map_err(|_| {
                    crate::core::Error::StateError("mock confirm_lsn mutex poisoned".into())
                })?
                .push(lsn);
            Ok(())
        }
    }

    struct MockSnapshotHandle {
        chunks: TestDeque<Vec<Event>>,
        done: bool,
        checkpoint_error: Option<String>,
        checkpoint_payload: Option<Vec<u8>>,
        checkpoint_source_type: String,
    }

    impl MockSnapshotHandle {
        fn new(
            chunks: Vec<Vec<Event>>,
            checkpoint_error: Option<String>,
            checkpoint_payload: Option<Vec<u8>>,
            checkpoint_source_type: String,
        ) -> Self {
            Self {
                chunks: chunks.into_iter().collect(),
                done: false,
                checkpoint_error,
                checkpoint_payload,
                checkpoint_source_type,
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::source::SnapshotHandle for MockSnapshotHandle {
        async fn next_chunk(&mut self, _chunk_size: usize) -> crate::core::Result<Vec<Event>> {
            if let Some(chunk) = self.chunks.pop_front() {
                Ok(chunk)
            } else {
                self.done = true;
                Ok(vec![])
            }
        }

        async fn checkpoint(
            &self,
            checkpoint: &mut dyn crate::checkpoint::Checkpoint,
            committed_event_count: u64,
        ) -> crate::core::Result<()> {
            if let Some(message) = &self.checkpoint_error {
                return Err(crate::core::Error::CheckpointError(message.clone()));
            }
            if let Some(payload) = &self.checkpoint_payload {
                checkpoint
                    .save(
                        &crate::checkpoint::GenericOffset::new(
                            &self.checkpoint_source_type,
                            payload.clone(),
                        ),
                        committed_event_count,
                    )
                    .await?;
            }
            Ok(())
        }

        async fn finish(&mut self) -> crate::core::Result<crate::source::SnapshotEnd> {
            self.done = true;
            Ok(crate::source::SnapshotEnd { snapshot_end_ts: 1 })
        }
    }

    struct MockSource {
        stream_batches: Vec<Vec<Event>>,
        snapshot_chunks: Vec<Vec<Event>>,
        confirmed_lsns: Arc<Mutex<Vec<u64>>>,
        last_snapshot_resume_source: Arc<Mutex<Option<String>>>,
        last_snapshot_resume_payload: Arc<Mutex<Option<Vec<u8>>>>,
        last_stream_resume_source: Arc<Mutex<Option<String>>>,
        confirm_lsn_error: Option<String>,
        snapshot_checkpoint_error: Option<String>,
        snapshot_checkpoint_payload: Option<Vec<u8>>,
        snapshot_checkpoint_source_type: String,
    }

    impl MockSource {
        fn stream_only(batches: Vec<Vec<Event>>) -> Self {
            Self {
                stream_batches: batches,
                snapshot_chunks: vec![],
                confirmed_lsns: Arc::new(Mutex::new(Vec::new())),
                last_snapshot_resume_source: Arc::new(Mutex::new(None)),
                last_snapshot_resume_payload: Arc::new(Mutex::new(None)),
                last_stream_resume_source: Arc::new(Mutex::new(None)),
                confirm_lsn_error: None,
                snapshot_checkpoint_error: None,
                snapshot_checkpoint_payload: None,
                snapshot_checkpoint_source_type: "mock_snapshot".to_string(),
            }
        }

        fn with_snapshot(
            snapshot_chunks: Vec<Vec<Event>>,
            stream_batches: Vec<Vec<Event>>,
        ) -> Self {
            Self {
                stream_batches,
                snapshot_chunks,
                confirmed_lsns: Arc::new(Mutex::new(Vec::new())),
                last_snapshot_resume_source: Arc::new(Mutex::new(None)),
                last_snapshot_resume_payload: Arc::new(Mutex::new(None)),
                last_stream_resume_source: Arc::new(Mutex::new(None)),
                confirm_lsn_error: None,
                snapshot_checkpoint_error: None,
                snapshot_checkpoint_payload: None,
                snapshot_checkpoint_source_type: "mock_snapshot".to_string(),
            }
        }

        #[cfg(feature = "postgres")]
        fn with_confirm_lsn_error(mut self, message: impl Into<String>) -> Self {
            self.confirm_lsn_error = Some(message.into());
            self
        }

        fn with_snapshot_checkpoint_error(mut self, message: impl Into<String>) -> Self {
            self.snapshot_checkpoint_error = Some(message.into());
            self
        }

        fn with_snapshot_checkpoint_payload(mut self, payload: Vec<u8>) -> Self {
            self.snapshot_checkpoint_payload = Some(payload);
            self
        }

        #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
        fn with_snapshot_checkpoint_source_type(mut self, source_type: impl Into<String>) -> Self {
            self.snapshot_checkpoint_source_type = source_type.into();
            self
        }

        #[cfg(feature = "postgres")]
        fn confirmed_lsns(&self) -> Arc<Mutex<Vec<u64>>> {
            Arc::clone(&self.confirmed_lsns)
        }

        #[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlserver"))]
        fn last_stream_resume_source(&self) -> Arc<Mutex<Option<String>>> {
            Arc::clone(&self.last_stream_resume_source)
        }

        #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
        fn last_snapshot_resume_source(&self) -> Arc<Mutex<Option<String>>> {
            Arc::clone(&self.last_snapshot_resume_source)
        }

        #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
        fn last_snapshot_resume_payload(&self) -> Arc<Mutex<Option<Vec<u8>>>> {
            Arc::clone(&self.last_snapshot_resume_payload)
        }
    }
    #[async_trait::async_trait]
    impl crate::source::Source for MockSource {
        async fn start_snapshot(
            &mut self,
            _tables: &[&str],
        ) -> crate::core::Result<Box<dyn crate::source::SnapshotHandle>> {
            Ok(Box::new(MockSnapshotHandle::new(
                self.snapshot_chunks.clone(),
                self.snapshot_checkpoint_error.clone(),
                self.snapshot_checkpoint_payload.clone(),
                self.snapshot_checkpoint_source_type.clone(),
            )))
        }

        async fn start_snapshot_from_checkpoint(
            &mut self,
            _tables: &[&str],
            resume_from: Option<&dyn crate::core::Offset>,
        ) -> crate::core::Result<Box<dyn crate::source::SnapshotHandle>> {
            let resume_source = resume_from.map(|offset| offset.source_type().to_string());
            let resume_payload = if let Some(offset) = resume_from {
                Some(offset.encode()?)
            } else {
                None
            };

            *self.last_snapshot_resume_source.lock().map_err(|_| {
                crate::core::Error::StateError(
                    "mock snapshot resume source mutex should not be poisoned".into(),
                )
            })? = resume_source;
            *self.last_snapshot_resume_payload.lock().map_err(|_| {
                crate::core::Error::StateError(
                    "mock snapshot resume payload mutex should not be poisoned".into(),
                )
            })? = resume_payload;

            Ok(Box::new(MockSnapshotHandle::new(
                self.snapshot_chunks.clone(),
                self.snapshot_checkpoint_error.clone(),
                self.snapshot_checkpoint_payload.clone(),
                self.snapshot_checkpoint_source_type.clone(),
            )))
        }

        async fn start_stream(
            &mut self,
            resume_from: Option<&dyn crate::core::Offset>,
        ) -> crate::core::Result<Box<dyn crate::source::StreamHandle>> {
            let resume_source = resume_from.map(|offset| offset.source_type().to_string());
            *self.last_stream_resume_source.lock().map_err(|_| {
                crate::core::Error::StateError(
                    "mock resume source mutex should not be poisoned".into(),
                )
            })? = resume_source;

            Ok(Box::new(MockStreamHandle::new(
                self.stream_batches.clone(),
                Arc::clone(&self.confirmed_lsns),
                self.confirm_lsn_error.clone(),
            )))
        }

        async fn perform_handoff(
            &mut self,
            _snapshot: &mut dyn crate::source::SnapshotHandle,
            _stream: &mut dyn crate::source::StreamHandle,
        ) -> crate::core::Result<crate::source::HandoffResult> {
            Ok(crate::source::HandoffResult {
                snapshot_end_ts: Some(1),
                stream_start_ts: Some(2),
                overlap_events_dropped: None,
                stream_watermark_gap: None,
            })
        }

        fn source_type(&self) -> &str {
            "mock"
        }

        fn capabilities(&self) -> crate::source::ConnectorCapabilities {
            crate::source::ConnectorCapabilities {
                snapshot: true,
                snapshot_checkpoint_resume: true,
                handoff: true,
                ddl_capture: false,
                heartbeat: false,
                tls: false,
                schema_introspection: true,
                truncate: false,
                incremental_snapshot: false,
            }
        }
    }

    fn make_runtime_with_mock_source(
        source: MockSource,
        snapshot_tables: Vec<String>,
    ) -> CdcRuntime {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = crate::schema_history::InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_snapshot_tables(snapshot_tables)
            // Keep mock source cycle tests focused on ack/redelivery semantics.
            .with_idempotency_disabled();
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.inject_mock_source(Box::new(source));
        runtime
    }

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
    fn make_file_checkpoint_runtime_with_mock_source(
        source_config: RuntimeSourceConfig,
        checkpoint_dir: &std::path::Path,
        source: MockSource,
        snapshot_tables: Vec<String>,
    ) -> CdcRuntime {
        let checkpoint = FileCheckpoint::new(checkpoint_dir);
        let schema_history = crate::schema_history::InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(source_config, checkpoint, schema_history)
            .with_snapshot_tables(snapshot_tables)
            .with_idempotency_disabled();
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.inject_mock_source(Box::new(source));
        runtime
    }

    // ─── Mock source cycle tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn mock_source_stream_only_full_cycle() {
        let batch = vec![event(), event(), event()];
        let mut runtime =
            make_runtime_with_mock_source(MockSource::stream_only(vec![batch.clone()]), vec![]);

        // Inject a checkpoint so runtime skips snapshot and goes directly to stream.
        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new("mock", b"stream-offset".to_vec()),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Running);

        let batch = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch.len(), 3);

        runtime.commit_ack(batch.ack_mode()).await.unwrap();
        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            3
        );

        runtime.stop().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Stopped);
    }

    #[tokio::test]
    async fn snapshot_commit_preserves_structured_snapshot_checkpoint_payload() {
        let mut snapshot_event = event();
        snapshot_event.snapshot = Some(SnapshotMetadata {
            snapshot_id: "snap-1".into(),
            chunk_index: 0,
            is_last_chunk: true,
        });
        snapshot_event.source.offset = "users:cursor:0".into();

        let expected_payload = serde_json::to_vec(&serde_json::json!({
            "snapshot_id": "snap-1",
            "table": "users",
            "cursor": [0]
        }))
        .unwrap();

        let source = MockSource::with_snapshot(vec![vec![snapshot_event]], vec![])
            .with_snapshot_checkpoint_payload(expected_payload.clone());
        let mut runtime = make_runtime_with_mock_source(source, vec!["public.users".into()]);

        runtime.start().await.unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        let AckMode::Required(token) = batch.ack_mode() else {
            panic!("expected ack token")
        };
        runtime.commit_ack(token).await.unwrap();

        let loaded = runtime.config.checkpoint.load().await.unwrap().unwrap();
        assert_eq!(loaded.source_type(), "mock_snapshot");
        assert_eq!(loaded.encode().unwrap(), expected_payload);
        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn mock_source_oversized_stream_batch_is_staged_and_drained() {
        let oversized_batch = vec![event(), event(), event(), event(), event()];
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = crate::schema_history::InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_max_buffer_size(2)
            .with_idempotency_disabled();
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.inject_mock_source(Box::new(MockSource::stream_only(vec![oversized_batch])));

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new("mock", b"stream-offset".to_vec()),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();

        let batch1 = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch1.len(), 2);
        runtime.commit_ack(batch1.ack_mode()).await.unwrap();

        let batch2 = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch2.len(), 2);
        runtime.commit_ack(batch2.ack_mode()).await.unwrap();

        let batch3 = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch3.len(), 1);
        runtime.commit_ack(batch3.ack_mode()).await.unwrap();

        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            5
        );
    }

    #[tokio::test]
    async fn runtime_idempotency_guard_suppresses_duplicate_delivery() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let idempotency = IdempotencyOptions::new(128).unwrap();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_idempotency(idempotency);
        let mut runtime = CdcRuntime::new(config).unwrap();

        runtime.start().await.unwrap();
        runtime.enqueue_event(event()).unwrap();
        runtime.enqueue_event(event()).unwrap();

        let batch = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch.len(), 1);

        runtime.commit_ack(batch.ack_mode()).await.unwrap();
        let admin = runtime.admin_snapshot();
        assert_eq!(admin.total_events_deduplicated, 1);
    }

    #[tokio::test]
    async fn runtime_idempotency_deduplicates_before_nondeterministic_transform() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let idempotency = IdempotencyOptions::new(128).unwrap();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_idempotency(idempotency);
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.add_transform(Box::new(NonDeterministicTransform));

        runtime.start().await.unwrap();
        runtime.enqueue_event(event()).unwrap();
        runtime.enqueue_event(event()).unwrap();

        let batch = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch.len(), 1);

        let nonce = batch.events()[0].after.as_ref().unwrap()["nondeterministic_nonce"]
            .as_u64()
            .unwrap();
        assert_eq!(nonce, 1);

        runtime.commit_ack(batch.ack_mode()).await.unwrap();
        let admin = runtime.admin_snapshot();
        assert_eq!(admin.total_events_deduplicated, 1);
    }

    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn runtime_idempotency_deduplicates_before_encryption_transform() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let idempotency = IdempotencyOptions::new(128).unwrap();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_idempotency(idempotency);
        let mut runtime = CdcRuntime::new(config).unwrap();

        let mut rules = HashMap::new();
        rules.insert(
            "id".to_string(),
            MaskRule::Encrypt(crate::core::SecretString::new("state-of-the-art-test-key")),
        );
        runtime.add_transform(Box::new(MaskHashTransform::new(MaskHashConfig {
            mask_rules: rules,
            default_rule: MaskRule::UnsaltedSha256,
        })));

        runtime.start().await.unwrap();
        runtime.enqueue_event(event()).unwrap();
        runtime.enqueue_event(event()).unwrap();

        let batch = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch.len(), 1);

        let encrypted_id = batch.events()[0].after.as_ref().unwrap()["id"]
            .as_str()
            .expect("encrypted payload should be string");
        assert!(encrypted_id.starts_with("enc:"));

        runtime.commit_ack(batch.ack_mode()).await.unwrap();
        let admin = runtime.admin_snapshot();
        assert_eq!(admin.total_events_deduplicated, 1);
    }

    #[tokio::test]
    async fn mock_source_snapshot_then_stream_handoff() {
        let snap_events = vec![event(), event()];
        let stream_events = vec![event()];
        let mut runtime = make_runtime_with_mock_source(
            MockSource::with_snapshot(vec![snap_events], vec![stream_events]),
            vec!["users".to_string()],
        );

        runtime.start().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Running);

        // Snapshot chunk.
        let chunk = runtime.poll_event_batch().await.unwrap();
        assert_eq!(chunk.len(), 2);
        runtime.commit_ack(chunk.ack_mode()).await.unwrap();

        // Handoff (snapshot done, stream continues).
        let stream_chunk = runtime.poll_event_batch().await.unwrap();
        assert_eq!(stream_chunk.len(), 1);
        runtime.commit_ack(stream_chunk.ack_mode()).await.unwrap();

        runtime.stop().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Stopped);
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_snapshot_checkpoint_starts_with_resume_offset() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(
            RuntimeSourceConfig::Postgres(crate::source::PostgresSourceConfig::default()),
            checkpoint,
            schema_history,
        )
        .with_snapshot_tables(vec!["users".to_string()])
        .with_idempotency_disabled();
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.inject_mock_source(Box::new(MockSource::with_snapshot(
            vec![vec![event()]],
            vec![vec![event()]],
        )));

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new(
                    "postgres_snapshot",
                    br#"{"snapshot_id":"s","snapshot_start_ts":1,"snapshot_end_ts":0,"snapshot_watermark":42,"current_table":0,"next_chunk_index":0,"tables":[]}"#.to_vec(),
                ),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Running);
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_runtime_source_capabilities_report_resumable_snapshot_checkpoints() {
        let postgres = crate::source::PostgresSourceConfig {
            user: "cdc".into(),
            password: crate::core::SecretString::new("cdc"),
            database: "cdc".into(),
            replication_slot_name: "slot_cdc".into(),
            publication_name: "pub_cdc".into(),
            ..Default::default()
        };

        let caps = RuntimeSourceConfig::Postgres(postgres).capabilities();
        assert!(caps.snapshot);
        assert!(caps.snapshot_checkpoint_resume);
    }

    #[cfg(feature = "mysql")]
    #[tokio::test]
    async fn mysql_snapshot_checkpoint_resumes_stream_from_mysql_offset() {
        let mut snapshot_event = event();
        snapshot_event.snapshot = Some(crate::core::SnapshotMetadata {
            snapshot_id: "snap-1".into(),
            chunk_index: 0,
            is_last_chunk: false,
        });

        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(
            RuntimeSourceConfig::Mysql(crate::source::MysqlSourceConfig::default()),
            checkpoint,
            schema_history,
        )
        .with_snapshot_tables(vec!["users".to_string()]);
        let mut runtime = CdcRuntime::new(config).unwrap();
        let source = MockSource::with_snapshot(vec![vec![snapshot_event]], vec![vec![event()]]);
        let resume_source = source.last_stream_resume_source();
        runtime.inject_mock_source(Box::new(source));

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new(
                    "mysql_snapshot",
                    br#"{"snapshot_id":"s","snapshot_start_ts":1,"binlog_file":"mysql-bin.000123","binlog_pos":789,"gtid":"uuid:8-9","current_table":0,"next_chunk_index":0,"tables":[]}"#.to_vec(),
                ),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();
        let first = runtime.poll_event_batch().await.unwrap();
        assert_eq!(first.len(), 1);

        let resume_source = resume_source
            .lock()
            .expect("resume source mutex should not be poisoned")
            .clone();
        assert_eq!(resume_source.as_deref(), Some("mysql"));
    }

    #[cfg(feature = "mariadb")]
    #[tokio::test]
    async fn mariadb_snapshot_checkpoint_resumes_stream_from_mariadb_offset() {
        let mut snapshot_event = event();
        snapshot_event.snapshot = Some(crate::core::SnapshotMetadata {
            snapshot_id: "snap-1".into(),
            chunk_index: 0,
            is_last_chunk: false,
        });

        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(
            RuntimeSourceConfig::MariaDb(crate::source::MariaDbSourceConfig::default()),
            checkpoint,
            schema_history,
        )
        .with_snapshot_tables(vec!["users".to_string()]);
        let mut runtime = CdcRuntime::new(config).unwrap();
        let source = MockSource::with_snapshot(vec![vec![snapshot_event]], vec![vec![event()]]);
        let resume_source = source.last_stream_resume_source();
        runtime.inject_mock_source(Box::new(source));

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new(
                    "mariadb_snapshot",
                    br#"{"snapshot_id":"s","snapshot_start_ts":1,"binlog_file":"mariadb-bin.000123","binlog_pos":789,"gtid":"uuid:8-9","current_table":0,"next_chunk_index":0,"tables":[]}"#.to_vec(),
                ),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();
        let first = runtime.poll_event_batch().await.unwrap();
        assert_eq!(first.len(), 1);

        let resume_source = resume_source
            .lock()
            .expect("resume source mutex should not be poisoned")
            .clone();
        assert_eq!(resume_source.as_deref(), Some("mariadb"));
    }

    #[cfg(any(
        feature = "postgres",
        feature = "mysql",
        feature = "mariadb",
        feature = "sqlserver"
    ))]
    fn snapshot_checkpoint_payload_for_source(snapshot_source_type: &str) -> Vec<u8> {
        match snapshot_source_type {
            "postgres_snapshot" => br#"{"snapshot_id":"snap","snapshot_start_ts":1,"snapshot_end_ts":0,"snapshot_watermark":4242,"current_table":0,"next_chunk_index":1,"tables":[]}"#.to_vec(),
            "mysql_snapshot" => br#"{"snapshot_id":"snap","snapshot_start_ts":1,"binlog_file":"mysql-bin.000123","binlog_pos":789,"gtid":"uuid:8-9","current_table":0,"next_chunk_index":1,"tables":[]}"#.to_vec(),
            "mariadb_snapshot" => br#"{"snapshot_id":"snap","snapshot_start_ts":1,"binlog_file":"mariadb-bin.000123","binlog_pos":789,"gtid":"uuid:8-9","current_table":0,"next_chunk_index":1,"tables":[]}"#.to_vec(),
            "sqlserver_snapshot" => br#"{"snapshot_id":"snap","lsn_start":[0,0,0,42,0,0,1,155,0,16],"current_table":0,"next_chunk_index":1,"tables":[]}"#.to_vec(),
            other => panic!("unsupported snapshot source type in test fixture: {other}"),
        }
    }

    #[cfg(any(
        feature = "postgres",
        feature = "mysql",
        feature = "mariadb",
        feature = "sqlserver"
    ))]
    async fn assert_runtime_snapshot_resume_through_commit_ack(
        source_config: RuntimeSourceConfig,
        snapshot_source_type: &str,
    ) {
        let expected_stream_source = snapshot_source_type
            .strip_suffix("_snapshot")
            .expect("snapshot source type should end with '_snapshot'")
            .to_string();

        let mut snapshot_event = event();
        snapshot_event.snapshot = Some(SnapshotMetadata {
            snapshot_id: "snap".into(),
            chunk_index: 0,
            is_last_chunk: true,
        });
        snapshot_event.source.offset = "table:cursor:0".into();

        let expected_payload = snapshot_checkpoint_payload_for_source(snapshot_source_type);
        let checkpoint_dir = tempfile::tempdir().expect("tempdir should be created");

        let source_first = MockSource::with_snapshot(vec![vec![snapshot_event]], vec![])
            .with_snapshot_checkpoint_payload(expected_payload.clone())
            .with_snapshot_checkpoint_source_type(snapshot_source_type);
        let mut runtime = make_file_checkpoint_runtime_with_mock_source(
            source_config.clone(),
            checkpoint_dir.path(),
            source_first,
            vec!["users".to_string()],
        );

        runtime.start().await.unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch.len(), 1);
        runtime.commit_ack(batch.ack_mode()).await.unwrap();
        drop(runtime);

        let checkpoint = FileCheckpoint::new(checkpoint_dir.path());
        let persisted = checkpoint
            .load()
            .await
            .unwrap()
            .expect("snapshot checkpoint should persist after commit_ack");
        assert_eq!(persisted.source_type(), snapshot_source_type);
        let persisted_payload: serde_json::Value =
            serde_json::from_slice(&persisted.encode().unwrap()).unwrap();
        let expected_payload_json: serde_json::Value =
            serde_json::from_slice(&expected_payload).unwrap();
        assert_eq!(persisted_payload, expected_payload_json);
        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 1);

        let source_resume = MockSource::with_snapshot(vec![], vec![]);
        let snapshot_resume_source = source_resume.last_snapshot_resume_source();
        let snapshot_resume_payload = source_resume.last_snapshot_resume_payload();
        let stream_resume_source = source_resume.last_stream_resume_source();

        let mut resumed_runtime = make_file_checkpoint_runtime_with_mock_source(
            source_config,
            checkpoint_dir.path(),
            source_resume,
            vec!["users".to_string()],
        );

        resumed_runtime.start().await.unwrap();

        let resumed_snapshot_source = snapshot_resume_source
            .lock()
            .expect("snapshot resume source mutex should not be poisoned")
            .clone();
        assert_eq!(
            resumed_snapshot_source.as_deref(),
            Some(snapshot_source_type)
        );

        let resumed_snapshot_payload = snapshot_resume_payload
            .lock()
            .expect("snapshot resume payload mutex should not be poisoned")
            .clone();
        let resumed_snapshot_payload =
            resumed_snapshot_payload.expect("snapshot resume payload should be present");
        let resumed_snapshot_payload: serde_json::Value =
            serde_json::from_slice(&resumed_snapshot_payload).unwrap();
        let expected_payload_json: serde_json::Value =
            serde_json::from_slice(&expected_payload).unwrap();
        assert_eq!(resumed_snapshot_payload, expected_payload_json);

        let resumed_stream_source = stream_resume_source
            .lock()
            .expect("stream resume source mutex should not be poisoned")
            .clone();
        assert_eq!(
            resumed_stream_source.as_deref(),
            Some(expected_stream_source.as_str())
        );
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_snapshot_checkpoint_commit_ack_survives_restart_and_resumes_runtime() {
        assert_runtime_snapshot_resume_through_commit_ack(
            RuntimeSourceConfig::Postgres(crate::source::PostgresSourceConfig::default()),
            "postgres_snapshot",
        )
        .await;
    }

    #[cfg(feature = "mysql")]
    #[tokio::test]
    async fn mysql_snapshot_checkpoint_commit_ack_survives_restart_and_resumes_runtime() {
        assert_runtime_snapshot_resume_through_commit_ack(
            RuntimeSourceConfig::Mysql(crate::source::MysqlSourceConfig::default()),
            "mysql_snapshot",
        )
        .await;
    }

    #[cfg(feature = "mariadb")]
    #[tokio::test]
    async fn mariadb_snapshot_checkpoint_commit_ack_survives_restart_and_resumes_runtime() {
        assert_runtime_snapshot_resume_through_commit_ack(
            RuntimeSourceConfig::MariaDb(crate::source::MariaDbSourceConfig::default()),
            "mariadb_snapshot",
        )
        .await;
    }

    #[cfg(feature = "sqlserver")]
    #[tokio::test]
    async fn sqlserver_snapshot_checkpoint_commit_ack_survives_restart_and_resumes_runtime() {
        assert_runtime_snapshot_resume_through_commit_ack(
            RuntimeSourceConfig::SqlServer(crate::source::SqlServerSourceConfig::default()),
            "sqlserver_snapshot",
        )
        .await;
    }

    #[tokio::test]
    async fn stop_rejects_uncommitted_events_by_default() {
        let mut runtime =
            make_runtime_with_mock_source(MockSource::stream_only(vec![vec![event()]]), vec![]);

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new("mock", b"offset".to_vec()),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        assert!(!batch.is_empty());

        let error = runtime.stop().await.unwrap_err();
        assert!(matches!(error, crate::core::Error::StateError(_)));
        assert_eq!(runtime.state(), RuntimeState::Running);

        let drained = runtime.force_stop().await.unwrap();
        assert_eq!(drained.len(), batch.len());
        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            0
        );
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn commit_ack_confirms_postgres_lsn_when_available() {
        let mut event = event();
        event.source.source_name = "postgres".into();
        event.source.offset = "16/B374D848".into();

        let source = MockSource::stream_only(vec![vec![event]]);
        let confirmed = source.confirmed_lsns();
        let mut runtime = make_runtime_with_mock_source(source, vec![]);

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new("mock", b"offset".to_vec()),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        runtime.commit_ack(batch.ack_mode()).await.unwrap();

        let lsns = confirmed
            .lock()
            .expect("confirmed lsn mutex should not be poisoned")
            .clone();
        assert_eq!(lsns, vec![0x16_00000000 + 0xB374D848]);
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn commit_ack_fails_when_confirm_lsn_fails_post_commit_by_default() {
        let mut event = event();
        event.source.source_name = "postgres".into();
        event.source.offset = "16/B374D848".into();

        let mut runtime = make_runtime_with_mock_source(
            MockSource::stream_only(vec![vec![event]])
                .with_confirm_lsn_error("simulated confirm_lsn failure"),
            vec![],
        );

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new("mock", b"offset".to_vec()),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        let error = runtime.commit_ack(batch.ack_mode()).await.expect_err(
            "default fail-fast policy should return an error after durable checkpoint commit",
        );

        assert!(matches!(error, crate::core::Error::SourceError(_)));

        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            1
        );
        assert_eq!(runtime.admin_snapshot().in_flight_events, 0);
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn commit_ack_can_continue_when_confirm_lsn_fails_post_commit() {
        let mut event = event();
        event.source.source_name = "postgres".into();
        event.source.offset = "16/B374D848".into();

        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_post_commit_source_confirm_policy(PostCommitSourceConfirmPolicy::Continue);
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.source = RuntimeSource::Mock(Box::new(
            MockSource::stream_only(vec![vec![event]])
                .with_confirm_lsn_error("simulated confirm_lsn failure"),
        ));

        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            0
        );

        runtime.start().await.unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        runtime
            .commit_ack(batch.ack_mode())
            .await
            .expect("continue policy should keep ack successful after durable checkpoint commit");

        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            1
        );
        assert_eq!(runtime.admin_snapshot().in_flight_events, 0);
    }

    #[tokio::test]
    async fn commit_ack_fails_when_snapshot_checkpoint_fails_pre_commit() {
        let mut snapshot_event = event();
        snapshot_event.snapshot = Some(SnapshotMetadata {
            snapshot_id: "snap-1".into(),
            chunk_index: 0,
            is_last_chunk: false,
        });

        let mut runtime = make_runtime_with_mock_source(
            MockSource::with_snapshot(vec![vec![snapshot_event]], vec![])
                .with_snapshot_checkpoint_error("simulated snapshot checkpoint failure"),
            vec!["users".to_string()],
        );

        runtime.start().await.unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        let error = runtime
            .commit_ack(batch.ack_mode())
            .await
            .expect_err("ack should fail before durable commit when snapshot checkpoint fails");

        assert!(matches!(error, crate::core::Error::CheckpointError(_)));

        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            0
        );
        assert_eq!(runtime.admin_snapshot().in_flight_events, 1);
    }

    #[tokio::test]
    async fn mock_source_poll_event_batch_redelivers_until_acknowledged() {
        let mut runtime = make_runtime_with_mock_source(
            MockSource::stream_only(vec![vec![event(), event()]]),
            vec![],
        );

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new("mock", b"offset".to_vec()),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();

        let first = runtime.poll_event_batch().await.unwrap();
        let AckMode::Required(first_token) = first.ack_mode() else {
            panic!("expected first ack token")
        };
        let second = runtime.poll_event_batch().await.unwrap();
        let AckMode::Required(second_token) = second.ack_mode() else {
            panic!("expected second ack token")
        };

        assert_eq!(first.events(), second.events());
        assert_eq!(first_token, second_token);

        runtime.commit_ack(first_token).await.unwrap();
        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn mock_source_commit_ack_supports_partial_ack_and_retry() {
        let mut runtime = make_runtime_with_mock_source(
            MockSource::stream_only(vec![vec![event(), event(), event()]]),
            vec![],
        );

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new("mock", b"offset".to_vec()),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();

        let batch = runtime.poll_event_batch().await.unwrap();
        let AckMode::Required(token) = batch.ack_mode() else {
            panic!("expected ack token")
        };
        let (accepted, remainder) = token.split_at(2).unwrap();

        runtime.commit_ack(accepted).await.unwrap();
        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            2
        );

        let retried = runtime.poll_event_batch().await.unwrap();
        assert_eq!(retried.len(), 1);
        assert_eq!(AckMode::from(remainder), retried.ack_mode());

        runtime.commit_ack(retried.ack_mode()).await.unwrap();
        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            3
        );
    }

    #[tokio::test]
    async fn runtime_event_batches_stream_yields_non_empty_batches() {
        let mut runtime =
            make_runtime_with_mock_source(MockSource::stream_only(vec![vec![event()]]), vec![]);

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new("mock", b"offset".to_vec()),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();

        let batch = {
            let mut batches = runtime.event_batches();
            batches.next().await.unwrap().unwrap()
        };

        assert_eq!(batch.len(), 1);
        runtime.commit_ack(batch.ack_mode()).await.unwrap();
    }

    #[tokio::test]
    async fn mock_source_state_transitions_are_valid() {
        let mut runtime = make_runtime_with_mock_source(MockSource::stream_only(vec![]), vec![]);

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new("mock", b"offset".to_vec()),
                0,
            )
            .await
            .unwrap();

        assert_eq!(runtime.state(), RuntimeState::Idle);
        runtime.start().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Running);
        assert!(runtime.start().await.is_err()); // double-start fails
        runtime.stop().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Stopped);
        // Restart from Stopped is allowed.
        runtime.start().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Running);
        runtime.stop().await.unwrap();
    }

    #[test]
    fn parse_postgres_lsn_accepts_valid_hex() {
        let parsed = super::parse_postgres_lsn("16/B374D848").unwrap();
        assert_eq!(parsed, 0x16_00000000 + 0xB374D848);
    }

    #[test]
    fn parse_postgres_lsn_rejects_invalid_inputs() {
        assert!(super::parse_postgres_lsn("missing-slash").is_err());
        assert!(super::parse_postgres_lsn("GG/1").is_err());
        assert!(super::parse_postgres_lsn("1/GG").is_err());
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn parse_mysql_stream_offset_supports_gtid_suffix() {
        let parsed = super::parse_mysql_stream_offset("binlog.000001:123#gtid=uuid:1-20").unwrap();
        assert_eq!(parsed.0, "binlog.000001");
        assert_eq!(parsed.1, 123);
        assert_eq!(parsed.2, "uuid:1-20");
    }

    #[cfg(feature = "mysql")]
    #[tokio::test]
    async fn mysql_checkpoint_offset_preserves_gtid_from_event_offset() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(
            RuntimeSourceConfig::Mysql(crate::source::MysqlSourceConfig::default()),
            checkpoint,
            schema_history,
        );
        let mut runtime = CdcRuntime::new(config).unwrap();
        let mut ev = event();
        ev.source.source_name = "mysql".into();
        ev.source.offset = "binlog.000002:432#gtid=uuid:3-9".into();
        runtime.inject_mock_source(Box::new(MockSource::stream_only(vec![vec![ev]])));

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new(
                    "mysql",
                    br#"{"gtid":"","binlog_file":"binlog.000001","binlog_pos":4}"#.to_vec(),
                ),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        runtime.commit_ack(batch.ack_mode()).await.unwrap();

        let saved = runtime
            .config
            .checkpoint
            .load()
            .await
            .unwrap()
            .expect("mysql checkpoint should be present");
        let decoded = crate::checkpoint::MysqlOffset::from_bytes(&saved.encode().unwrap()).unwrap();
        assert_eq!(decoded.gtid, "uuid:3-9");
        assert_eq!(decoded.binlog_file, "binlog.000002");
        assert_eq!(decoded.binlog_pos, 432);
    }

    #[cfg(feature = "mariadb")]
    #[tokio::test]
    async fn mariadb_checkpoint_offset_preserves_gtid_from_event_offset() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(
            RuntimeSourceConfig::MariaDb(crate::source::MariaDbSourceConfig::default()),
            checkpoint,
            schema_history,
        );
        let mut runtime = CdcRuntime::new(config).unwrap();
        let mut ev = event();
        ev.source.source_name = "mariadb".into();
        ev.source.offset = "mariadb-bin.000002:432#gtid=uuid:3-9".into();
        runtime.inject_mock_source(Box::new(MockSource::stream_only(vec![vec![ev]])));

        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new(
                    "mariadb",
                    br#"{"gtid":"","binlog_file":"mariadb-bin.000001","binlog_pos":4}"#.to_vec(),
                ),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        runtime.commit_ack(batch.ack_mode()).await.unwrap();

        let saved = runtime
            .config
            .checkpoint
            .load()
            .await
            .unwrap()
            .expect("mariadb checkpoint should be present");
        assert_eq!(saved.source_type(), "mariadb");
        let decoded = crate::checkpoint::MysqlOffset::from_bytes(&saved.encode().unwrap()).unwrap();
        assert_eq!(decoded.gtid, "uuid:3-9");
        assert_eq!(decoded.binlog_file, "mariadb-bin.000002");
        assert_eq!(decoded.binlog_pos, 432);
    }

    #[tokio::test]
    async fn disabled_runtime_source_constructor_is_empty() {
        let source = RuntimeSourceConfig::disabled();
        assert_eq!(source.source_type(), None);
        assert!(!source.capabilities().snapshot);
    }

    #[cfg(feature = "mariadb")]
    #[tokio::test]
    async fn mariadb_runtime_source_constructor_keeps_mariadb_identity() {
        let source = RuntimeSourceConfig::mariadb(crate::source::MariaDbSourceConfig::default());
        assert_eq!(source.source_type(), Some("mariadb"));
        assert!(source.capabilities().snapshot);
    }

    #[tokio::test]
    async fn stop_on_idle_runtime_is_idempotent() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();

        let drained_first = runtime.stop().await.unwrap();
        let drained_second = runtime.stop().await.unwrap();
        assert!(drained_first.is_empty());
        assert!(drained_second.is_empty());
        assert_eq!(runtime.state(), RuntimeState::Stopped);
    }

    #[tokio::test]
    async fn admin_snapshot_tracks_checkpoint_age() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();

        // Before any checkpoint, age should be None.
        let admin = runtime.admin_snapshot();
        assert!(admin.checkpoint_age_ms.is_none());

        // After commit, checkpoint_age_ms should be set.
        runtime.start().await.unwrap();
        runtime.enqueue_event(event()).unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        runtime.commit_ack(batch.ack_mode()).await.unwrap();

        let admin = runtime.admin_snapshot();
        assert!(admin.checkpoint_age_ms.is_some());
        assert!(admin.checkpoint_age_ms.unwrap() < 100); // Should be recently committed.
    }

    #[tokio::test]
    async fn admin_snapshot_tracks_replication_lag() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();

        // Before any poll, lag should be None.
        let admin = runtime.admin_snapshot();
        assert!(admin.replication_lag_ms.is_none());

        // After poll, lag should be set (estimated from last poll time).
        runtime.start().await.unwrap();
        runtime.enqueue_event(event()).unwrap();
        let _batch = runtime.poll_event_batch().await.unwrap();

        let admin = runtime.admin_snapshot();
        assert!(admin.replication_lag_ms.is_some());
        assert!(admin.replication_lag_ms.unwrap() < 100); // Should be recent.
    }

    #[tokio::test]
    async fn admin_snapshot_lag_normalizes_seconds_source_timestamps() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();

        runtime.start().await.unwrap();
        let mut ev = event();
        ev.source.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or_default();
        runtime.enqueue_event(ev).unwrap();
        let _batch = runtime.poll_event_batch().await.unwrap();

        let admin = runtime.admin_snapshot();
        assert!(admin.replication_lag_ms.is_some());
        assert!(admin.replication_lag_ms.unwrap() < 1_500);
    }

    #[tokio::test]
    async fn admin_metrics_prometheus_includes_checkpoint_age_and_lag() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();

        runtime.start().await.unwrap();
        runtime.enqueue_event(event()).unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        runtime.commit_ack(batch.ack_mode()).await.unwrap();

        let prometheus = runtime.admin_metrics_prometheus();
        assert!(prometheus.contains("cdc_runtime_checkpoint_age_ms"));
        assert!(prometheus.contains("cdc_runtime_replication_lag_ms"));
    }

    #[tokio::test]
    async fn admin_snapshot_json_serializes_all_fields() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();

        runtime.start().await.unwrap();
        runtime.enqueue_event(event()).unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        runtime.commit_ack(batch.ack_mode()).await.unwrap();

        let json = runtime.admin_snapshot_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(parsed.get("checkpoint_age_ms").is_some());
        assert!(parsed.get("replication_lag_ms").is_some());
        assert_eq!(parsed["state"], "running");
        assert!(parsed["checkpoint_age_ms"].is_number());
    }

    #[tokio::test]
    async fn capture_ddl_statement_records_schema_history_and_enqueues_event() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();

        runtime.start().await.unwrap();

        let event = runtime
            .capture_ddl_statement(
                DdlDialect::Postgres,
                "CREATE TABLE public.users (id INT PRIMARY KEY, name TEXT NOT NULL)",
                "postgres",
                "0/16B6A70".to_string(),
                1,
            )
            .await
            .unwrap()
            .expect("ddl should be captured");

        assert_eq!(event.op, Operation::SchemaChange);
        assert_eq!(event.table, "users__ddl_events");

        let schema = runtime
            .config
            .schema_history
            .latest_schema("public.users")
            .await
            .unwrap()
            .expect("schema should be persisted");
        assert_eq!(schema.table, "users");

        let batch = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.events()[0].op, Operation::SchemaChange);
    }

    #[tokio::test]
    async fn capture_alter_ddl_applies_schema_diff_without_erasing_schema_history() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();

        runtime.start().await.unwrap();

        runtime
            .capture_ddl_statement(
                DdlDialect::Postgres,
                "CREATE TABLE public.users (id INT PRIMARY KEY, name TEXT NOT NULL)",
                "postgres",
                "0/16B6A70".to_string(),
                1,
            )
            .await
            .unwrap();

        let event = runtime
            .capture_ddl_statement(
                DdlDialect::Postgres,
                "ALTER TABLE public.users ADD COLUMN email TEXT, RENAME COLUMN name TO full_name",
                "postgres",
                "0/16B6A71".to_string(),
                2,
            )
            .await
            .unwrap()
            .expect("alter ddl should be captured");

        let after = event
            .after
            .as_ref()
            .and_then(|value| value.as_object())
            .unwrap();
        assert!(after.get("result_schema").is_none());
        assert_eq!(after.get("schema_version"), Some(&serde_json::json!(2)));

        let schema = runtime
            .config
            .schema_history
            .latest_schema("public.users")
            .await
            .unwrap()
            .expect("alter should preserve schema history");
        assert_eq!(schema.version, 2);
        assert!(schema.columns.iter().any(|column| column.name == "email"));
        assert!(schema
            .columns
            .iter()
            .any(|column| column.name == "full_name"));
        assert!(!schema.columns.iter().any(|column| column.name == "name"));
    }

    #[tokio::test]
    async fn capture_ddl_statement_applies_runtime_schema_history_retention_policy() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let retention = SchemaHistoryRetention::keep_last(2).unwrap();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_schema_history_retention(retention);
        let mut runtime = CdcRuntime::new(config).unwrap();

        runtime.start().await.unwrap();

        runtime
            .capture_ddl_statement(
                DdlDialect::Postgres,
                "CREATE TABLE public.users (id INT PRIMARY KEY, name TEXT NOT NULL)",
                "postgres",
                "0/16B6A70".to_string(),
                1,
            )
            .await
            .unwrap();
        runtime
            .capture_ddl_statement(
                DdlDialect::Postgres,
                "ALTER TABLE public.users ADD COLUMN email TEXT",
                "postgres",
                "0/16B6A71".to_string(),
                2,
            )
            .await
            .unwrap();
        runtime
            .capture_ddl_statement(
                DdlDialect::Postgres,
                "ALTER TABLE public.users ADD COLUMN phone TEXT",
                "postgres",
                "0/16B6A72".to_string(),
                3,
            )
            .await
            .unwrap();

        let v1 = runtime
            .config
            .schema_history
            .get_schema_at_version("public.users", 1)
            .await
            .unwrap();
        let latest = runtime
            .config
            .schema_history
            .latest_schema("public.users")
            .await
            .unwrap()
            .unwrap();

        assert!(v1.is_none(), "retention should prune oldest schema version");
        assert_eq!(latest.version, 3);
        assert!(latest.columns.iter().any(|column| column.name == "phone"));
    }

    #[tokio::test]
    async fn capture_alter_ddl_rejects_unsupported_schema_diff_clauses() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();

        runtime.start().await.unwrap();

        runtime
            .capture_ddl_statement(
                DdlDialect::Postgres,
                "CREATE TABLE public.users (id INT PRIMARY KEY, name TEXT NOT NULL)",
                "postgres",
                "0/16B6A70".to_string(),
                1,
            )
            .await
            .unwrap();

        let error = runtime
            .capture_ddl_statement(
                DdlDialect::Postgres,
                "ALTER TABLE public.users ADD COLUMN email TEXT, REPLICA IDENTITY FULL",
                "postgres",
                "0/16B6A71".to_string(),
                2,
            )
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("unsupported clause 'REPLICA IDENTITY FULL'"));

        let schema = runtime
            .config
            .schema_history
            .latest_schema("public.users")
            .await
            .unwrap()
            .expect("schema should remain at create-table version");
        assert_eq!(schema.version, 1);

        let batch = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.events()[0].op, Operation::SchemaChange);
    }

    // ─── Reconnect recovery test ─────────────────────────────────────────────

    /// Verifies that a recoverable `SourceError` from a live stream triggers
    /// the reconnect-and-resume path: the runtime must close the old stream,
    /// call `start_stream` again, and continue delivering events — without
    /// surfacing the transient error to the caller.
    #[tokio::test]
    async fn recoverable_stream_error_triggers_reconnect_and_resumes_delivery() {
        use std::collections::VecDeque;
        use std::sync::atomic::{AtomicU32, Ordering as AOrdering};

        // ── Mini StreamHandle ─────────────────────────────────────────────
        // Returns queued event batches, then optionally emits one recoverable
        // error, then returns empty batches indefinitely.
        struct FailOnceStream {
            events: VecDeque<Vec<Event>>,
            error_pending: bool,
        }

        #[async_trait]
        impl crate::source::StreamHandle for FailOnceStream {
            async fn next_events(&mut self, _timeout_ms: u64) -> crate::core::Result<Vec<Event>> {
                if let Some(batch) = self.events.pop_front() {
                    return Ok(batch);
                }
                if self.error_pending {
                    self.error_pending = false;
                    return Err(crate::core::Error::SourceError(
                        "simulated TCP reset by peer".into(),
                    ));
                }
                Ok(vec![])
            }

            async fn save_position(
                &self,
                _checkpoint: &mut dyn crate::checkpoint::Checkpoint,
            ) -> crate::core::Result<()> {
                Ok(())
            }

            async fn confirm_lsn(&mut self, _lsn: u64) -> crate::core::Result<()> {
                Ok(())
            }
        }

        // ── Mini Source ───────────────────────────────────────────────────
        // Counts `start_stream` invocations so the test can verify reconnect
        // happened. First stream: 1 event then a recoverable error.
        // Second stream (after reconnect): 2 events.
        struct ReconnectableSource {
            call_count: Arc<AtomicU32>,
        }

        #[async_trait]
        impl crate::source::Source for ReconnectableSource {
            async fn start_snapshot(
                &mut self,
                _tables: &[&str],
            ) -> crate::core::Result<Box<dyn crate::source::SnapshotHandle>> {
                unreachable!("reconnect test does not use snapshot")
            }

            async fn start_stream(
                &mut self,
                _resume_from: Option<&dyn crate::core::Offset>,
            ) -> crate::core::Result<Box<dyn crate::source::StreamHandle>> {
                let call = self.call_count.fetch_add(1, AOrdering::SeqCst);
                let (events, error_pending) = if call == 0 {
                    // First stream: yield one event, then fail with recoverable error.
                    (vec![vec![event()]], true)
                } else {
                    // Reconnected stream: yield two events normally.
                    (vec![vec![event(), event()]], false)
                };
                Ok(Box::new(FailOnceStream {
                    events: events.into_iter().collect(),
                    error_pending,
                }))
            }

            async fn perform_handoff(
                &mut self,
                _snapshot: &mut dyn crate::source::SnapshotHandle,
                _stream: &mut dyn crate::source::StreamHandle,
            ) -> crate::core::Result<crate::source::HandoffResult> {
                unreachable!("no handoff in reconnect test")
            }

            fn source_type(&self) -> &str {
                "mock"
            }
        }

        // ── Setup ─────────────────────────────────────────────────────────
        let call_count = Arc::new(AtomicU32::new(0));
        let source = ReconnectableSource {
            call_count: Arc::clone(&call_count),
        };

        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = crate::schema_history::InMemorySchemaHistory::default();

        let mut config =
            RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
                .with_idempotency_disabled();
        // Use aggressive retry timing so the test completes quickly.
        config.options.connection_retry = Some(ConnectionRetryPolicy {
            max_retries: Some(3),
            initial_delay_ms: 1,
            max_delay_ms: 10,
        });

        let mut runtime: CdcRuntime = CdcRuntime::new(config).unwrap();
        runtime.inject_mock_source(Box::new(source));

        // Pre-populate a checkpoint offset so the runtime enters stream mode
        // directly rather than starting a full snapshot phase.
        runtime
            .config
            .checkpoint
            .save(
                &crate::checkpoint::GenericOffset::new("mock", b"stream-offset-0".to_vec()),
                0,
            )
            .await
            .unwrap();

        runtime.start().await.unwrap();

        // ── First poll: delivers 1 event from the first stream ────────────
        let batch1 = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch1.len(), 1, "first batch should have 1 event");
        runtime.commit_ack(batch1.ack_mode()).await.unwrap();

        // ── Second poll: first stream raises a recoverable error,  ────────
        //    the runtime reconnects, and the second stream delivers 2 events.
        let batch2 = runtime.poll_event_batch().await.unwrap();
        assert_eq!(
            batch2.len(),
            2,
            "reconnected stream should deliver the remaining 2 events"
        );

        // ── Invariant: start_stream must have been called exactly twice ───
        assert_eq!(
            call_count.load(AOrdering::SeqCst),
            2,
            "source.start_stream must be invoked once on initial connect \
             and once more after the recoverable error triggers reconnect"
        );

        runtime.force_stop().await.unwrap();
    }

    // ─── ConnectionRetryPolicy validation ────────────────────────────────

    #[test]
    fn connection_retry_policy_default_is_valid() {
        assert!(ConnectionRetryPolicy::default().validate().is_ok());
    }

    #[test]
    fn connection_retry_policy_rejects_zero_initial_delay() {
        let policy = ConnectionRetryPolicy {
            initial_delay_ms: 0,
            max_delay_ms: 10_000,
            max_retries: Some(5),
        };
        let err = policy.validate().unwrap_err();
        assert!(
            matches!(err, crate::core::Error::ConfigError(_)),
            "expected ConfigError, got {err:?}"
        );
        assert!(
            err.to_string().contains("initial_delay_ms"),
            "error message should mention initial_delay_ms"
        );
    }

    #[test]
    fn connection_retry_policy_rejects_max_delay_below_initial() {
        let policy = ConnectionRetryPolicy {
            initial_delay_ms: 500,
            max_delay_ms: 100, // less than initial
            max_retries: Some(3),
        };
        let err = policy.validate().unwrap_err();
        assert!(
            matches!(err, crate::core::Error::ConfigError(_)),
            "expected ConfigError, got {err:?}"
        );
        assert!(
            err.to_string().contains("max_delay_ms"),
            "error message should mention max_delay_ms"
        );
    }

    #[test]
    fn connection_retry_policy_allows_equal_initial_and_max_delay() {
        // initial == max is valid (no exponential growth, fixed delay)
        let policy = ConnectionRetryPolicy {
            initial_delay_ms: 300,
            max_delay_ms: 300,
            max_retries: None,
        };
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn runtime_new_rejects_invalid_connection_retry_policy() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let mut config =
            RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        config.options.connection_retry = Some(ConnectionRetryPolicy {
            initial_delay_ms: 0,
            max_delay_ms: 10_000,
            max_retries: Some(3),
        });
        let err = CdcRuntime::new(config)
            .err()
            .expect("CdcRuntime::new should reject an invalid retry policy");
        assert!(
            matches!(err, crate::core::Error::ConfigError(_)),
            "expected ConfigError, got {err:?}"
        );
    }
}
