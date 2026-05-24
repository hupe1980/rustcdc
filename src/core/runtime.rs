//! Runtime orchestration for embedded CDC operation.

use std::{collections::VecDeque, sync::Arc};

use futures_util::{stream, stream::BoxStream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    checkpoint::{CommitBarrier, GenericOffset},
    ddl_capture::{parse_ddl_statement, DdlDialect},
    schema_history::{SchemaHistory, SchemaHistoryRetention},
    source::{ConnectorCapabilities, HandoffResult, SnapshotHandle, StreamHandle},
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
use super::runtime_utils::{format_capability_metric, normalize_source_timestamp_ms, now_millis};
use super::{
    Error, Event, EventIdempotencyGuard, EventTracer, MetricsCollector, NoOpEventTracer,
    NoOpMetricsCollector, Offset, Result,
};

mod runtime_commit;

const DEFAULT_RUNTIME_IDEMPOTENCY_CAPACITY: usize = 100_000;

/// Explicit observability configuration for runtime construction.
#[derive(Clone)]
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
            schema_history_retention: None,
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
}

/// Runtime-level idempotency guard configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Source configuration for runtime construction.
#[derive(Clone)]
pub enum RuntimeSourceConfig {
    #[cfg(feature = "postgres")]
    Postgres(PostgresSourceConfig),
    #[cfg(feature = "mysql")]
    Mysql(MysqlSourceConfig),
    #[cfg(feature = "sqlserver")]
    SqlServer(SqlServerSourceConfig),
    Disabled,
}

impl RuntimeSourceConfig {
    /// Connector identifier when a real source is configured.
    pub const fn source_type(&self) -> Option<&'static str> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => Some("postgres"),
            #[cfg(feature = "mysql")]
            Self::Mysql(_) => Some("mysql"),
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(_) => Some("sqlserver"),
            Self::Disabled => None,
        }
    }

    /// Capabilities advertised by the selected source connector.
    pub const fn capabilities(&self) -> ConnectorCapabilities {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => Self::postgres_connector_capabilities(),
            #[cfg(feature = "mysql")]
            Self::Mysql(_) => Self::full_connector_capabilities(),
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(_) => Self::full_connector_capabilities(),
            Self::Disabled => ConnectorCapabilities::none(),
        }
    }

    #[cfg(any(feature = "mysql", feature = "sqlserver"))]
    const fn full_connector_capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities {
            snapshot: true,
            snapshot_checkpoint_resume: true,
            handoff: true,
            ddl_capture: true,
            heartbeat: true,
            tls: cfg!(feature = "tls"),
            schema_introspection: true,
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
        }
    }
}

/// Runtime lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeState {
    Idle,
    Running,
    Stopping,
    Stopped,
}

/// Embeddable admin snapshot for runtime introspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeAdminSnapshot {
    pub source_type: Option<String>,
    pub state: String,
    pub readiness: bool,
    pub liveness: bool,
    pub capabilities: ConnectorCapabilities,
    pub buffer_depth: usize,
    pub in_flight_events: usize,
    pub snapshot_active: bool,
    pub stream_active: bool,
    pub handoff_complete: bool,
    pub total_events_polled: u64,
    pub total_events_committed: u64,
    pub total_events_deduplicated: u64,
    pub started_at_ms: Option<u64>,
    pub last_poll_at_ms: Option<u64>,
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

/// Delivered runtime events paired with an opaque acknowledgement token.
#[derive(Debug, Clone, PartialEq)]
pub struct EventBatch {
    events: Vec<Event>,
    ack_token: Option<AckToken>,
}

impl EventBatch {
    fn empty() -> Self {
        Self {
            events: Vec::new(),
            ack_token: None,
        }
    }

    /// Borrow the delivered events.
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Consume the batch and return its events.
    pub fn into_events(self) -> Vec<Event> {
        self.events
    }

    /// Return the acknowledgement token for this delivery, if any events were delivered.
    pub fn ack_token(&self) -> Option<AckToken> {
        self.ack_token.clone()
    }

    /// Number of events in the batch.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[derive(Clone)]
struct PendingDelivery {
    delivery_id: u64,
    events: Vec<Event>,
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

/// Behavior when source confirmation fails after checkpoint durability is already guaranteed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Runtime configuration for embedded execution.
#[derive(Clone)]
pub struct RuntimeConfig<C, H> {
    /// Source configuration used by the runtime.
    pub source: RuntimeSourceConfig,
    /// Snapshot table list used on first run when no checkpoint exists.
    pub snapshot_tables: Vec<String>,
    /// Checkpoint backend owned by the runtime.
    pub checkpoint: C,
    /// Schema history backend owned by the runtime.
    pub schema_history: H,
    /// Explicit runtime options including observability and tuning defaults.
    pub options: RuntimeOptions,
}

impl<C, H> RuntimeConfig<C, H> {
    /// Create a config with explicit runtime options using no-op observability defaults.
    pub fn new(source: RuntimeSourceConfig, checkpoint: C, schema_history: H) -> Self {
        Self {
            source,
            snapshot_tables: Vec::new(),
            checkpoint,
            schema_history,
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
pub struct CdcRuntime<C, H> {
    config: RuntimeConfig<C, H>,
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

impl<C, H> CdcRuntime<C, H>
where
    C: crate::checkpoint::Checkpoint + Send + Sync + 'static,
    H: SchemaHistory + Send + Sync + 'static,
{
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
    pub fn new(config: RuntimeConfig<C, H>) -> Result<Self> {
        if config.options.max_buffer_size == 0 {
            return Err(Error::ConfigError(
                "max_buffer_size must be greater than zero".into(),
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

    fn build_source(config: &RuntimeConfig<C, H>) -> Result<RuntimeSource> {
        match &config.source {
            #[cfg(feature = "postgres")]
            RuntimeSourceConfig::Postgres(source) => Ok(RuntimeSource::Postgres(
                PostgresConnection::new(source.clone()),
            )),
            #[cfg(feature = "mysql")]
            RuntimeSourceConfig::Mysql(source) => {
                Ok(RuntimeSource::Mysql(MysqlConnection::new(source.clone())))
            }
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

    /// Start the runtime and initialize source handles.
    pub async fn start(&mut self) -> Result<()> {
        match self.state {
            RuntimeState::Idle | RuntimeState::Stopped => {}
            RuntimeState::Running => {
                let error = Error::StateError("runtime already started".into());
                self.record_runtime_error("runtime.start.state", &error);
                return Err(error);
            }
            RuntimeState::Stopping => {
                let error = Error::StateError("runtime is currently stopping".into());
                self.record_runtime_error("runtime.start.state", &error);
                return Err(error);
            }
        }

        let committed_event_count = self
            .config
            .checkpoint
            .get_committed_count()
            .await
            .inspect_err(|error| self.record_runtime_error("runtime.start.committed_count", error))?;
        self.commit_barrier
            .hydrate_committed_event_count(committed_event_count)
            .inspect_err(|error| self.record_runtime_error("runtime.start.barrier_hydrate", error))?;

        if matches!(self.source, RuntimeSource::Disabled) {
            self.state = RuntimeState::Running;
            self.observability().tracer.trace_checkpoint_barrier("open");
            return Ok(());
        }

        let mut checkpoint_offset = self.config.checkpoint.load().await?;
        if let Some(offset) = checkpoint_offset.as_ref() {
            if self.is_snapshot_checkpoint(offset.as_ref()) {
                if !self.source_capabilities().snapshot_checkpoint_resume {
                    tracing::warn!(
                        target: "cdc_rs::runtime",
                        source = self.config.source.source_type().unwrap_or("unknown"),
                        "snapshot checkpoint resume is unsupported by connector; restarting snapshot from scratch"
                    );
                    checkpoint_offset = None;
                }

                if checkpoint_offset.is_some() && self.config.snapshot_tables.is_empty() {
                    return Err(Error::ConfigError(
                        "snapshot_tables must not be empty when resuming from a snapshot checkpoint"
                            .into(),
                    ));
                }
            }
        }

        self.source
            .connect()
            .await
            .inspect_err(|error| self.record_runtime_error("runtime.start.connect", error))?;

        if let Some(offset) = checkpoint_offset.as_ref() {
            if self.is_snapshot_checkpoint(offset.as_ref()) {
                self.snapshot = Some(
                    self.source
                        .start_snapshot_from_checkpoint(
                            &self.config.snapshot_tables,
                            offset.as_ref(),
                        )
                        .await?,
                );
                let stream_resume_from =
                    self.stream_resume_offset_for_snapshot_checkpoint(offset.as_ref())?;
                self.stream = Some(
                    self.source
                        .start_stream(stream_resume_from.as_deref())
                        .await?,
                );
                self.handoff_complete = false;
            } else {
                self.stream = Some(self.source.start_stream(Some(offset.as_ref())).await?);
                self.snapshot = None;
                self.handoff_complete = true;
            }
        } else if self.config.snapshot_tables.is_empty() {
            self.snapshot = None;
            self.stream = Some(self.source.start_stream(None).await?);
            self.handoff_complete = true;
        } else {
            self.snapshot = Some(
                self.source
                    .start_snapshot(&self.config.snapshot_tables)
                    .await?,
            );
            self.stream = Some(self.source.start_stream(None).await?);
            self.handoff_complete = false;
        }

        self.state = RuntimeState::Running;
        self.observability().tracer.trace_checkpoint_barrier("open");
        self.started_at_ms = Some(now_millis());
        self.last_poll_at_ms = None;
        self.last_source_event_ts_ms = None;
        self.last_commit_at_ms = None;
        self.total_events_polled = 0;
        self.total_events_committed = 0;
        self.total_events_deduplicated = 0;
        Ok(())
    }

    fn is_snapshot_checkpoint(&self, offset: &dyn Offset) -> bool {
        let Some(source_type) = self.config.source.source_type() else {
            return false;
        };
        let expected_snapshot_source = format!("{source_type}_snapshot");
        offset.source_type() == expected_snapshot_source
    }

    #[allow(unused_variables)]
    fn stream_resume_offset_for_snapshot_checkpoint(
        &self,
        snapshot_checkpoint: &dyn Offset,
    ) -> Result<Option<Box<dyn Offset>>> {
        #[cfg(feature = "postgres")]
        if matches!(&self.config.source, RuntimeSourceConfig::Postgres(_)) {
            return Ok(Some(Box::new(
                self.postgres_stream_offset_from_snapshot_checkpoint(snapshot_checkpoint)?,
            )));
        }

        #[cfg(feature = "mysql")]
        if matches!(&self.config.source, RuntimeSourceConfig::Mysql(_)) {
            return Ok(Some(Box::new(
                Self::mysql_stream_offset_from_snapshot_checkpoint(snapshot_checkpoint)?,
            )));
        }

        #[cfg(feature = "sqlserver")]
        if matches!(&self.config.source, RuntimeSourceConfig::SqlServer(_)) {
            return Ok(Some(Box::new(
                Self::sqlserver_stream_offset_from_snapshot_checkpoint(snapshot_checkpoint)?,
            )));
        }

        Ok(None)
    }

    #[cfg(feature = "postgres")]
    fn postgres_stream_offset_from_snapshot_checkpoint(
        &self,
        snapshot_checkpoint: &dyn Offset,
    ) -> Result<PostgresOffset> {
        let payload = snapshot_checkpoint.encode()?;
        let value: serde_json::Value = serde_json::from_slice(&payload)
            .map_err(|error| Error::CheckpointError(error.to_string()))?;

        let lsn = value
            .get("snapshot_watermark")
            .and_then(|entry| entry.as_u64())
            .ok_or_else(|| {
                Error::CheckpointError(
                    "postgres snapshot checkpoint is missing 'snapshot_watermark'".into(),
                )
            })?;

        let slot_name = match &self.config.source {
            RuntimeSourceConfig::Postgres(cfg) => cfg.replication_slot_name.clone(),
            _ => {
                return Err(Error::StateError(
                    "postgres stream resume conversion called for non-postgres runtime source"
                        .into(),
                ));
            }
        };

        Ok(PostgresOffset { lsn, slot_name })
    }

    #[cfg(feature = "mysql")]
    fn mysql_stream_offset_from_snapshot_checkpoint(
        snapshot_checkpoint: &dyn Offset,
    ) -> Result<MysqlOffset> {
        let payload = snapshot_checkpoint.encode()?;
        let value: serde_json::Value = serde_json::from_slice(&payload)
            .map_err(|error| Error::CheckpointError(error.to_string()))?;

        let binlog_file = value
            .get("binlog_file")
            .and_then(|entry| entry.as_str())
            .ok_or_else(|| {
                Error::CheckpointError("mysql snapshot checkpoint is missing 'binlog_file'".into())
            })?
            .to_string();
        let binlog_pos = value
            .get("binlog_pos")
            .and_then(|entry| entry.as_u64())
            .ok_or_else(|| {
                Error::CheckpointError("mysql snapshot checkpoint is missing 'binlog_pos'".into())
            })?;
        let binlog_pos = u32::try_from(binlog_pos).map_err(|_| {
            Error::CheckpointError("mysql snapshot checkpoint binlog_pos exceeds u32".into())
        })?;
        let gtid = value
            .get("gtid")
            .and_then(|entry| entry.as_str())
            .unwrap_or_default()
            .to_string();

        Ok(MysqlOffset {
            gtid,
            binlog_file,
            binlog_pos,
        })
    }

    #[cfg(feature = "sqlserver")]
    fn sqlserver_stream_offset_from_snapshot_checkpoint(
        snapshot_checkpoint: &dyn Offset,
    ) -> Result<GenericOffset> {
        let payload = snapshot_checkpoint.encode()?;
        let value: serde_json::Value = serde_json::from_slice(&payload)
            .map_err(|error| Error::CheckpointError(error.to_string()))?;

        let lsn_start = value
            .get("lsn_start")
            .and_then(|entry| entry.as_array())
            .ok_or_else(|| {
                Error::CheckpointError(
                    "sqlserver snapshot checkpoint is missing 'lsn_start'".into(),
                )
            })?;

        if lsn_start.len() != 10 {
            return Err(Error::CheckpointError(
                "sqlserver snapshot checkpoint lsn_start must contain exactly 10 bytes".into(),
            ));
        }

        let mut bytes = Vec::with_capacity(10);
        for value in lsn_start {
            let byte = value.as_u64().ok_or_else(|| {
                Error::CheckpointError(
                    "sqlserver snapshot checkpoint lsn_start contains non-byte value".into(),
                )
            })?;
            let byte = u8::try_from(byte).map_err(|_| {
                Error::CheckpointError(
                    "sqlserver snapshot checkpoint lsn_start contains out-of-range byte".into(),
                )
            })?;
            bytes.push(byte);
        }

        Ok(GenericOffset::new(
            "sqlserver",
            serde_json::to_vec(&format!(
                "0x{}",
                bytes
                    .iter()
                    .map(|byte| format!("{byte:02X}"))
                    .collect::<String>()
            ))
            .map_err(|error| Error::SerializationError(error.to_string()))?,
        ))
    }

    /// Stop the runtime.
    ///
    /// This is safe-by-default and will fail if there are uncommitted in-memory
    /// events. Callers must acknowledge deliveries first, or use `force_stop()`
    /// to explicitly drain pending events.
    pub async fn stop(&mut self) -> Result<Vec<Event>> {
        match self.state {
            RuntimeState::Idle | RuntimeState::Stopped => {
                self.state = RuntimeState::Stopped;
                return Ok(Vec::new());
            }
            RuntimeState::Stopping => {
                let error = Error::StateError("runtime already stopping".into());
                self.record_runtime_error("runtime.stop.state", &error);
                return Err(error);
            }
            RuntimeState::Running => {}
        }

        let pending_events = self
            .commit_barrier
            .pending_count()
            .saturating_add(self.injected_events.len())
            .saturating_add(self.pending_source_events.len());
        if pending_events > 0 {
            let error = Error::StateError(format!(
                "runtime has {pending_events} uncommitted events; commit acknowledgements before stop or call force_stop()"
            ));
            self.record_runtime_error("runtime.stop.uncommitted", &error);
            return Err(error);
        }

        self.state = RuntimeState::Stopping;
        self.delivered_not_committed = 0;
        self.pending_delivery = None;
        self.source.close().await;

        self.snapshot = None;
        self.stream = None;
        self.started_at_ms = None;
        self.last_source_event_ts_ms = None;
        self.observability()
            .tracer
            .trace_checkpoint_barrier("stopped");
        self.state = RuntimeState::Stopped;
        Ok(Vec::new())
    }

    /// Force stop the runtime and drain all pending in-memory events.
    ///
    /// This is intended for emergency shutdown paths where replay/duplication
    /// handling is delegated to the embedder.
    pub async fn force_stop(&mut self) -> Result<Vec<Event>> {
        match self.state {
            RuntimeState::Idle | RuntimeState::Stopped => {
                self.state = RuntimeState::Stopped;
                return Ok(Vec::new());
            }
            RuntimeState::Stopping => {
                let error = Error::StateError("runtime already stopping".into());
                self.record_runtime_error("runtime.force_stop.state", &error);
                return Err(error);
            }
            RuntimeState::Running => {}
        }

        self.state = RuntimeState::Stopping;
        let mut drained = std::mem::take(&mut self.injected_events)
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(pending) = self.pending_delivery.take() {
            drained.extend(pending.events);
        }
        drained.extend(self.buffered_events.drain(..));
        drained.extend(self.pending_source_events.drain(..));
        self.commit_barrier.clear_pending();
        for event in &drained {
            self.observability()
                .tracer
                .trace_event_end(&Self::event_trace_id(event), "force_stopped");
        }
        self.delivered_not_committed = 0;
        self.source.close().await;

        self.snapshot = None;
        self.stream = None;
        self.started_at_ms = None;
        self.last_source_event_ts_ms = None;
        self.observability()
            .tracer
            .trace_checkpoint_barrier("stopped");
        self.state = RuntimeState::Stopped;
        Ok(drained)
    }

    /// Return the current lifecycle state.
    pub fn state(&self) -> RuntimeState {
        self.state
    }

    /// Report capabilities for the configured source.
    pub const fn source_capabilities(&self) -> ConnectorCapabilities {
        self.config.source.capabilities()
    }

    /// Return an embeddable admin snapshot for runtime health and capabilities introspection.
    pub fn admin_snapshot(&self) -> RuntimeAdminSnapshot {
        let now_ms = now_millis();
        let checkpoint_age_ms = self
            .last_checkpoint_saved_at_ms
            .map(|checkpoint_time| now_ms.saturating_sub(checkpoint_time));

        RuntimeAdminSnapshot {
            source_type: self.config.source.source_type().map(str::to_string),
            state: runtime_state_label(self.state).to_string(),
            readiness: self.state == RuntimeState::Running
                && (matches!(self.config.source, RuntimeSourceConfig::Disabled)
                    || self.stream.is_some()
                    || self.snapshot.is_some()),
            liveness: self.state != RuntimeState::Stopped,
            capabilities: self.source_capabilities(),
            buffer_depth: self.buffered_events.len()
                + self.injected_events.len()
                + self.pending_source_events.len(),
            in_flight_events: self
                .pending_delivery
                .as_ref()
                .map_or(0, |pending| pending.events.len()),
            snapshot_active: self.snapshot.is_some(),
            stream_active: self.stream.is_some(),
            handoff_complete: self.handoff_complete,
            total_events_polled: self.total_events_polled,
            total_events_committed: self.total_events_committed,
            total_events_deduplicated: self.total_events_deduplicated,
            started_at_ms: self.started_at_ms,
            last_poll_at_ms: self.last_poll_at_ms,
            last_commit_at_ms: self.last_commit_at_ms,
            checkpoint_age_ms,
            replication_lag_ms: self.estimate_replication_lag_ms(),
        }
    }

    /// Estimate replication lag from source event timestamps when available.
    /// Falls back to poll recency until a source timestamp is observed.
    fn estimate_replication_lag_ms(&self) -> Option<u64> {
        let now = now_millis();
        if let Some(source_ts) = self.last_source_event_ts_ms {
            return Some(now.saturating_sub(source_ts.min(now)));
        }
        self.last_poll_at_ms
            .map(|poll_time| now.saturating_sub(poll_time))
    }

    /// Render the current admin snapshot as JSON.
    pub fn admin_snapshot_json(&self) -> Result<String> {
        serde_json::to_string(&self.admin_snapshot())
            .map_err(|error| Error::SerializationError(error.to_string()))
    }

    /// Render runtime admin metrics in a Prometheus-friendly text exposition format.
    pub fn admin_metrics_prometheus(&self) -> String {
        let admin = self.admin_snapshot();
        let mut out = String::new();

        out.push_str("# HELP cdc_runtime_readiness Runtime readiness (1=ready, 0=not ready).\n");
        out.push_str("# TYPE cdc_runtime_readiness gauge\n");
        out.push_str(&format!(
            "cdc_runtime_readiness{{state=\"{}\"}} {}\n",
            admin.state,
            if admin.readiness { 1 } else { 0 }
        ));

        out.push_str("# HELP cdc_runtime_liveness Runtime liveness (1=alive, 0=stopped).\n");
        out.push_str("# TYPE cdc_runtime_liveness gauge\n");
        out.push_str(&format!(
            "cdc_runtime_liveness{{state=\"{}\"}} {}\n",
            admin.state,
            if admin.liveness { 1 } else { 0 }
        ));

        out.push_str(
            "# HELP cdc_runtime_buffer_depth Number of buffered events waiting for delivery.\n",
        );
        out.push_str("# TYPE cdc_runtime_buffer_depth gauge\n");
        out.push_str(&format!(
            "cdc_runtime_buffer_depth {}\n",
            admin.buffer_depth
        ));

        out.push_str(
            "# HELP cdc_runtime_in_flight_events Number of delivered but uncommitted events.\n",
        );
        out.push_str("# TYPE cdc_runtime_in_flight_events gauge\n");
        out.push_str(&format!(
            "cdc_runtime_in_flight_events {}\n",
            admin.in_flight_events
        ));

        out.push_str(
            "# HELP cdc_runtime_events_polled_total Total events delivered by runtime batches.\n",
        );
        out.push_str("# TYPE cdc_runtime_events_polled_total counter\n");
        out.push_str(&format!(
            "cdc_runtime_events_polled_total {}\n",
            admin.total_events_polled
        ));

        out.push_str("# HELP cdc_runtime_events_committed_total Total events acknowledged and checkpointed.\n");
        out.push_str("# TYPE cdc_runtime_events_committed_total counter\n");
        out.push_str(&format!(
            "cdc_runtime_events_committed_total {}\n",
            admin.total_events_committed
        ));

        out.push_str(
            "# HELP cdc_runtime_events_deduplicated_total Total events suppressed by runtime idempotency guard.\n",
        );
        out.push_str("# TYPE cdc_runtime_events_deduplicated_total counter\n");
        out.push_str(&format!(
            "cdc_runtime_events_deduplicated_total {}\n",
            admin.total_events_deduplicated
        ));

        if let Some(checkpoint_age_ms) = admin.checkpoint_age_ms {
            out.push_str("# HELP cdc_runtime_checkpoint_age_ms Age of last durable checkpoint in milliseconds.\n");
            out.push_str("# TYPE cdc_runtime_checkpoint_age_ms gauge\n");
            out.push_str(&format!(
                "cdc_runtime_checkpoint_age_ms {}\n",
                checkpoint_age_ms
            ));
        }

        if let Some(lag_ms) = admin.replication_lag_ms {
            out.push_str("# HELP cdc_runtime_replication_lag_ms Estimated replication lag in milliseconds (source event timestamp preferred; poll recency fallback).\n");
            out.push_str("# TYPE cdc_runtime_replication_lag_ms gauge\n");
            out.push_str(&format!("cdc_runtime_replication_lag_ms {}\n", lag_ms));
        }

        out.push_str("# HELP cdc_runtime_source_capability Connector capability flags.\n");
        out.push_str("# TYPE cdc_runtime_source_capability gauge\n");
        out.push_str(&format_capability_metric(
            "snapshot",
            admin.capabilities.snapshot,
        ));
        out.push_str(&format_capability_metric(
            "handoff",
            admin.capabilities.handoff,
        ));
        out.push_str(&format_capability_metric(
            "ddl_capture",
            admin.capabilities.ddl_capture,
        ));
        out.push_str(&format_capability_metric(
            "heartbeat",
            admin.capabilities.heartbeat,
        ));
        out.push_str(&format_capability_metric("tls", admin.capabilities.tls));
        out.push_str(&format_capability_metric(
            "schema_introspection",
            admin.capabilities.schema_introspection,
        ));

        out
    }

    /// Poll the next event batch with an opaque acknowledgement token.
    pub async fn poll_event_batch(&mut self) -> Result<EventBatch> {
        if self.state != RuntimeState::Running {
            let error = Error::StateError("runtime must be running before polling".into());
            self.record_runtime_error("runtime.poll.state", &error);
            return Err(error);
        }

        if let Some(batch) = self.current_pending_batch() {
            return Ok(batch);
        }

        let metrics = Arc::clone(&self.observability().metrics);

        if !self.buffered_events.is_empty() {
            return Ok(self.deliver_buffered_batch());
        }

        if !self.pending_source_events.is_empty() {
            return self.flush_pending_source_events();
        }

        if !self.injected_events.is_empty() {
            let mut chunk = Vec::new();
            while chunk.len() < self.config.options.max_buffer_size {
                let Some(event) = self.injected_events.pop_front() else {
                    break;
                };
                chunk.push(event);
            }

            // Deduplicate source events before transform stages mutate payloads.
            let deduplicated = self.filter_idempotent_events(chunk)?;
            let transformed = self.apply_transforms(deduplicated).await?;
            self.enqueue_pending_source_events(transformed);
            return self.flush_pending_source_events();
        }

        if let Some(snapshot) = self.snapshot.as_mut() {
            let chunk = snapshot
                .next_chunk(self.config.options.max_buffer_size)
                .await
                .inspect_err(|error| metrics.record_error(error, "runtime.poll.snapshot_chunk"))?;
            if !chunk.is_empty() {
                // Deduplicate source events before transform stages mutate payloads.
                let deduplicated = self.filter_idempotent_events(chunk)?;
                let transformed = self.apply_transforms(deduplicated).await?;
                self.enqueue_pending_source_events(transformed);
                return self.flush_pending_source_events();
            }

            if !self.handoff_complete {
                let stream = self.stream.as_mut().ok_or_else(|| {
                    Error::StateError("snapshot-to-stream handoff requires active stream".into())
                })?;
                self.source
                    .perform_handoff(snapshot.as_mut(), stream.as_mut())
                    .await
                    .inspect_err(|error| metrics.record_error(error, "runtime.poll.handoff"))?;
                self.handoff_complete = true;
            }
            self.snapshot = None;
        }

        if let Some(stream) = self.stream.as_mut() {
            let events = stream
                .next_events(self.config.options.max_poll_wait_ms)
                .await
                .inspect_err(|error| metrics.record_error(error, "runtime.poll.stream_events"))?;
            if events.is_empty() {
                return Ok(EventBatch::empty());
            }
            // Deduplicate source events before transform stages mutate payloads.
            let deduplicated = self.filter_idempotent_events(events)?;
            let transformed = self.apply_transforms(deduplicated).await?;
            self.enqueue_pending_source_events(transformed);
            return self.flush_pending_source_events();
        }

        Ok(EventBatch::empty())
    }

    /// Expose the runtime as a batch stream that yields non-empty deliveries.
    pub fn event_batches(&mut self) -> BoxStream<'_, Result<EventBatch>> {
        stream::unfold(self, |runtime| async move {
            loop {
                match runtime.poll_event_batch().await {
                    Ok(batch) if batch.is_empty() => continue,
                    Ok(batch) => return Some((Ok(batch), runtime)),
                    Err(error) => return Some((Err(error), runtime)),
                }
            }
        })
        .boxed()
    }

    async fn apply_transforms(&self, events: Vec<Event>) -> Result<Vec<Event>> {
        let mut out = Vec::with_capacity(events.len());
        for event in events {
            let table = event.table.clone();
            let offset = event.source.offset.clone();
            match self.transform_pipeline.apply(event).await {
                Ok(Some(event)) => out.push(event),
                Ok(None) => {}
                Err(error) => match self.config.options.transform_error_policy {
                    TransformErrorPolicy::Halt => {
                        self.record_runtime_error("runtime.transform.halt", &error);
                        return Err(error);
                    }
                    TransformErrorPolicy::Skip => {
                        self.record_runtime_error("runtime.transform.skip", &error);
                        tracing::warn!(
                            target: "cdc_rs::core::runtime",
                            table = %table,
                            offset = %offset,
                            error = %error,
                            "runtime transform error; skipping event",
                        );
                        continue;
                    }
                },
            }
        }
        Ok(out)
    }

    fn filter_idempotent_events(&mut self, events: Vec<Event>) -> Result<Vec<Event>> {
        let Some(guard) = self.idempotency_guard.as_mut() else {
            return Ok(events);
        };

        let mut out = Vec::with_capacity(events.len());
        for event in events {
            if guard.should_process(&event)? {
                out.push(event);
            } else {
                self.total_events_deduplicated = self.total_events_deduplicated.saturating_add(1);
            }
        }

        Ok(out)
    }

    fn enqueue_pending_source_events(&mut self, events: Vec<Event>) {
        self.pending_source_events.extend(events);
    }

    fn flush_pending_source_events(&mut self) -> Result<EventBatch> {
        if self.pending_source_events.is_empty() {
            return Ok(EventBatch::empty());
        }

        let available = self
            .config
            .options
            .max_buffer_size
            .saturating_sub(self.commit_barrier.pending_count());

        if available == 0 {
            let error = Error::StateError(
                "runtime commit barrier is full; commit acknowledgements before polling more events"
                    .into(),
            );
            self.record_runtime_error("runtime.poll.buffer_full", &error);
            return Err(error);
        }

        let mut chunk = Vec::with_capacity(available.min(self.pending_source_events.len()));
        while chunk.len() < available {
            let Some(event) = self.pending_source_events.pop_front() else {
                break;
            };
            chunk.push(event);
        }

        self.buffer_and_deliver(chunk)
    }

    fn buffer_and_deliver(&mut self, events: Vec<Event>) -> Result<EventBatch> {
        for event in events {
            if self.config.options.validate_events {
                event.validate_or_error()?;
            }
            if event.snapshot.is_some() {
                // Snapshot checkpoints are persisted via SnapshotHandle::checkpoint
                // using connector-native structured state; avoid clobbering them
                // with per-event offsets at commit barrier flush time.
                self.commit_barrier.add_non_persistent_event()?;
            } else {
                let offset = self.build_checkpoint_offset(&event)?;
                self.commit_barrier.add_event(offset)?;
            }
            self.buffered_events.push_back(event);
        }
        Ok(self.deliver_buffered_batch())
    }

    fn build_checkpoint_offset(&self, event: &Event) -> Result<GenericOffset> {
        let source_type = self
            .config
            .source
            .source_type()
            .unwrap_or(event.source.source_name.as_str());

        #[cfg(feature = "postgres")]
        if let RuntimeSourceConfig::Postgres(config) = &self.config.source {
            let lsn = parse_postgres_lsn(&event.source.offset)?;
            let slot_name = config.replication_slot_name.clone();
            let offset = PostgresOffset { lsn, slot_name };
            return Ok(GenericOffset::new(
                "postgres",
                offset
                    .encode()
                    .map_err(|error| Error::CheckpointError(error.to_string()))?,
            ));
        }

        #[cfg(feature = "mysql")]
        if matches!(&self.config.source, RuntimeSourceConfig::Mysql(_)) {
            let (binlog_file, binlog_pos, gtid) = parse_mysql_stream_offset(&event.source.offset)?;
            let offset = MysqlOffset {
                gtid,
                binlog_file,
                binlog_pos,
            };
            return Ok(GenericOffset::new(
                "mysql",
                offset
                    .encode()
                    .map_err(|error| Error::CheckpointError(error.to_string()))?,
            ));
        }

        Ok(GenericOffset::new(
            source_type.to_string(),
            serde_json::to_vec(&event.source.offset)
                .map_err(|error| Error::SerializationError(error.to_string()))?,
        ))
    }

    fn current_pending_batch(&self) -> Option<EventBatch> {
        let pending = self.pending_delivery.as_ref()?;
        Some(EventBatch {
            events: pending.events.clone(),
            ack_token: Some(AckToken {
                delivery_id: pending.delivery_id,
                event_count: pending.events.len(),
            }),
        })
    }

    fn deliver_buffered_batch(&mut self) -> EventBatch {
        let mut events = Vec::new();
        while events.len() < self.config.options.max_buffer_size {
            let Some(event) = self.buffered_events.pop_front() else {
                break;
            };
            events.push(event);
        }

        if events.is_empty() {
            return EventBatch::empty();
        }

        self.total_events_polled = self.total_events_polled.saturating_add(events.len() as u64);
        self.last_poll_at_ms = Some(now_millis());
        let now_ms = now_millis();
        for event in &events {
            self.observability()
                .tracer
                .trace_event_start(&Self::event_trace_id(event));
            let source_ts = normalize_source_timestamp_ms(event.source.timestamp).min(now_ms);
            let latency_ms = now_ms.saturating_sub(source_ts);
            self.observability()
                .metrics
                .record_event_processed(event.op, latency_ms);
        }
        if let Some(latest_source_ts) = events
            .iter()
            .map(|event| normalize_source_timestamp_ms(event.source.timestamp))
            .max()
        {
            self.last_source_event_ts_ms = Some(
                self.last_source_event_ts_ms
                    .map_or(latest_source_ts, |previous| previous.max(latest_source_ts)),
            );
        }
        self.record_replication_lag_metric();

        let delivery_id = self.next_delivery_id;
        self.next_delivery_id = self.next_delivery_id.saturating_add(1);
        self.delivered_not_committed = self.delivered_not_committed.saturating_add(events.len());
        self.pending_delivery = Some(PendingDelivery {
            delivery_id,
            events: events.clone(),
        });

        EventBatch {
            events,
            ack_token: Some(AckToken {
                delivery_id,
                event_count: self
                    .pending_delivery
                    .as_ref()
                    .map_or(0, |pending| pending.events.len()),
            }),
        }
    }

    /// Inject a test event directly into the runtime buffer.
    pub fn enqueue_event(&mut self, event: Event) -> Result<()> {
        let queued_events = self.buffered_events.len() + self.injected_events.len();
        if queued_events >= self.config.options.max_buffer_size {
            return Err(Error::StateError("runtime buffer is full".into()));
        }

        self.injected_events.push_back(event);
        Ok(())
    }

    /// Parse and persist a DDL statement, then emit a canonical `schema_change` event.
    ///
    /// Returns `Ok(None)` when the statement is not a supported DDL command.
    pub async fn capture_ddl_statement(
        &mut self,
        dialect: DdlDialect,
        statement: &str,
        source_name: &str,
        offset: String,
        ts_ms: u64,
    ) -> Result<Option<Event>> {
        let Some(parsed) = parse_ddl_statement(dialect, statement) else {
            return Ok(None);
        };

        let mut captured = parsed.into_captured();
        captured.ts = ts_ms;

        let schema_version = match captured.to_schema_event() {
            Some(schema_event) => {
                let version = self.config.schema_history.record_ddl(schema_event).await?;
                if let Some(retention) = self.config.options.schema_history_retention {
                    self.config.schema_history.apply_retention(retention).await?;
                }
                Some(version)
            }
            None => None,
        };

        let mut event = captured.to_event(source_name, offset, ts_ms);
        if let Some(version) = schema_version {
            if let Some(after) = event.after.as_mut().and_then(|value| value.as_object_mut()) {
                after.insert("schema_version".into(), serde_json::json!(version));
            }
        }

        self.enqueue_event(event.clone())?;
        Ok(Some(event))
    }
}

fn runtime_state_label(state: RuntimeState) -> &'static str {
    match state {
        RuntimeState::Idle => "idle",
        RuntimeState::Running => "running",
        RuntimeState::Stopping => "stopping",
        RuntimeState::Stopped => "stopped",
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use futures_util::StreamExt;
    use serde_json::json;
    #[cfg(feature = "encryption")]
    use std::collections::HashMap;
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
        schema_history::{InMemorySchemaHistory, SchemaHistory, SchemaHistoryRetention},
        transform::Transform,
    };

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
    use crate::checkpoint::FileCheckpoint;

    use super::{
        CdcRuntime, IdempotencyOptions, RuntimeConfig, RuntimeObservability,
        RuntimeSourceConfig, RuntimeState, TransformErrorPolicy,
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
        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();

        let json = runtime.admin_snapshot_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["state"], "running");
        assert_eq!(parsed["readiness"], true);
        assert_eq!(parsed["total_events_polled"], 1);
        assert_eq!(parsed["total_events_committed"], 1);

        let prometheus = runtime.admin_metrics_prometheus();
        assert!(prometheus.contains("cdc_runtime_readiness"));
        assert!(prometheus.contains("cdc_runtime_events_polled_total 1"));
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

        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();
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
            .commit_ack(first_batch.ack_token().unwrap())
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
            .commit_ack(second_batch.ack_token().unwrap())
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
        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();

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
        let token = batch.ack_token().unwrap();
        runtime.commit_ack(token.clone()).await.unwrap();

        let error = runtime.commit_ack(token).await.unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));
    }

    struct FailTransform;
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
                overlap_events_dropped: 0,
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
            }
        }
    }

    fn make_runtime_with_mock_source(
        source: MockSource,
        snapshot_tables: Vec<String>,
    ) -> CdcRuntime<InMemoryCheckpoint, crate::schema_history::InMemorySchemaHistory> {
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
    ) -> CdcRuntime<FileCheckpoint, crate::schema_history::InMemorySchemaHistory> {
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

        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();
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
        let token = batch.ack_token().unwrap();
        runtime.commit_ack(token).await.unwrap();

        let loaded = runtime.config.checkpoint.load().await.unwrap().unwrap();
        assert_eq!(loaded.source_type(), "mock_snapshot");
        assert_eq!(loaded.encode().unwrap(), expected_payload);
        assert_eq!(runtime.config.checkpoint.get_committed_count().await.unwrap(), 1);
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
        runtime.commit_ack(batch1.ack_token().unwrap()).await.unwrap();

        let batch2 = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch2.len(), 2);
        runtime.commit_ack(batch2.ack_token().unwrap()).await.unwrap();

        let batch3 = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch3.len(), 1);
        runtime.commit_ack(batch3.ack_token().unwrap()).await.unwrap();

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

        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();
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

        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();
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
            default_rule: MaskRule::Hash,
        })));

        runtime.start().await.unwrap();
        runtime.enqueue_event(event()).unwrap();
        runtime.enqueue_event(event()).unwrap();

        let batch = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch.len(), 1);

        let encrypted_id = batch.events()[0].after.as_ref().unwrap()["id"]
            .as_str()
            .expect("encrypted payload should be string");
        assert!(encrypted_id.starts_with("enc:v1:"));

        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();
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
        runtime
            .commit_ack(chunk.ack_token().unwrap())
            .await
            .unwrap();

        // Handoff (snapshot done, stream continues).
        let stream_chunk = runtime.poll_event_batch().await.unwrap();
        assert_eq!(stream_chunk.len(), 1);
        runtime
            .commit_ack(stream_chunk.ack_token().unwrap())
            .await
            .unwrap();

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

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
    fn snapshot_checkpoint_payload_for_source(snapshot_source_type: &str) -> Vec<u8> {
        match snapshot_source_type {
            "postgres_snapshot" => br#"{"snapshot_id":"snap","snapshot_start_ts":1,"snapshot_end_ts":0,"snapshot_watermark":4242,"current_table":0,"next_chunk_index":1,"tables":[]}"#.to_vec(),
            "mysql_snapshot" => br#"{"snapshot_id":"snap","snapshot_start_ts":1,"binlog_file":"mysql-bin.000123","binlog_pos":789,"gtid":"uuid:8-9","current_table":0,"next_chunk_index":1,"tables":[]}"#.to_vec(),
            "sqlserver_snapshot" => br#"{"snapshot_id":"snap","lsn_start":[0,0,0,42,0,0,1,155,0,16],"current_table":0,"next_chunk_index":1,"tables":[]}"#.to_vec(),
            other => panic!("unsupported snapshot source type in test fixture: {other}"),
        }
    }

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
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
        runtime.commit_ack(batch.ack_token().unwrap()).await.unwrap();
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
        assert_eq!(resumed_snapshot_source.as_deref(), Some(snapshot_source_type));

        let resumed_snapshot_payload = snapshot_resume_payload
            .lock()
            .expect("snapshot resume payload mutex should not be poisoned")
            .clone();
        let resumed_snapshot_payload = resumed_snapshot_payload
            .expect("snapshot resume payload should be present");
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
        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();

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
        let error = runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .expect_err("default fail-fast policy should return an error after durable checkpoint commit");

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
            .commit_ack(batch.ack_token().unwrap())
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
            .commit_ack(batch.ack_token().unwrap())
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
        let first_token = first.ack_token().unwrap();
        let second = runtime.poll_event_batch().await.unwrap();
        let second_token = second.ack_token().unwrap();

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
        let token = batch.ack_token().unwrap();
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
        assert_eq!(remainder, retried.ack_token());

        runtime
            .commit_ack(retried.ack_token().unwrap())
            .await
            .unwrap();
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
        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();
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
        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();

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
        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();

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
        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();

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
        runtime
            .commit_ack(batch.ack_token().unwrap())
            .await
            .unwrap();

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
}
