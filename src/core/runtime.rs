//! Runtime orchestration for embedded CDC operation.

use std::{collections::VecDeque, sync::Arc};

use futures_util::{stream, stream::BoxStream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    checkpoint::{CommitBarrier, GenericOffset},
    ddl_capture::{parse_ddl_statement, DdlDialect},
    schema_history::{SchemaHistory, SchemaHistoryRetention},
    sink::{BoxedSink, SinkAdapter},
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
    /// Timeout applied to sink close during orderly runtime shutdown.
    ///
    /// When a sink is registered via [`CdcRuntime::register_sink`], this
    /// timeout is enforced automatically during [`CdcRuntime::stop`],
    /// [`CdcRuntime::force_stop`], and [`CdcRuntime::drain_and_stop`].
    ///
    /// If the sink does not close within `sink_close_timeout_ms` milliseconds,
    /// the shutdown path surfaces [`crate::core::Error::TimeoutError`] to the
    /// operator rather than blocking indefinitely.
    ///
    /// Set to `None` (default) to leave close duration unbounded.
    pub sink_close_timeout_ms: Option<u64>,
    /// Whether a delivered batch may end in the middle of a source transaction.
    ///
    /// Defaults to [`TransactionBoundaryPolicy::Split`], which matches the behaviour
    /// of every comparable CDC library. See the enum docs for when to change it.
    pub transaction_boundary: TransactionBoundaryPolicy,
}

/// Whether a delivered batch may end in the middle of a source transaction.
///
/// The runtime cuts batches on `max_buffer_size`, `max_event_bytes` and the commit
/// barrier's free capacity — none of which know anything about transactions. A cut
/// that lands inside one means the sink sees rows 1–3 of a five-row transaction,
/// commits them, and only later receives rows 4–5. Between those two commits the
/// sink holds a state that never existed in the source database.
///
/// For most sinks that is fine and is the reason the default is [`Split`]: it keeps
/// latency low and memory bounded. It is *not* fine for a sink that must apply each
/// source transaction atomically — a ledger, a materialized view with cross-row
/// invariants, or any consumer that publishes "the database as of transaction N".
///
/// [`Split`]: TransactionBoundaryPolicy::Split
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum TransactionBoundaryPolicy {
    /// Cut batches wherever the buffer limits fall (default).
    ///
    /// Lowest latency and strictly bounded memory. A transaction of any size is
    /// delivered across as many batches as needed.
    #[default]
    Split,
    /// Never end a delivered batch in the middle of a source transaction.
    ///
    /// The runtime trims the trailing partial transaction off each batch and delivers
    /// it with the next one, so every batch ends on a transaction boundary.
    ///
    /// # The one case this cannot honour
    ///
    /// A single transaction larger than `max_buffer_size` does not fit in any batch.
    /// Trimming it would produce an empty batch forever — a silent, permanent stall,
    /// which is strictly worse than the split it is trying to avoid. The runtime
    /// therefore delivers such a transaction split, and logs a WARN naming the
    /// transaction id and `max_buffer_size`. Raise `max_buffer_size` above the
    /// largest transaction the source produces if the guarantee must hold absolutely.
    ///
    /// Events with no transaction metadata (snapshot rows, and connectors that do not
    /// report transaction boundaries) are treated as their own boundary and are never
    /// trimmed.
    PreserveTransactions,
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
            sink_close_timeout_ms: None,
            transaction_boundary: TransactionBoundaryPolicy::Split,
        }
    }
}

impl RuntimeOptions {
    /// Runtime options with every setting at its default.
    ///
    /// `RuntimeOptions` is `#[non_exhaustive]`, so struct-literal syntax is unavailable
    /// outside this crate. Start here (or from [`RuntimeOptions::default`], which is
    /// equivalent) and chain the `with_*` builders.
    ///
    /// ```
    /// use rustcdc::RuntimeOptions;
    ///
    /// let options = RuntimeOptions::new().with_max_buffer_size(4096);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

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

    /// Register a dead-letter handler invoked when an event is skipped under
    /// [`TransformErrorPolicy::Skip`].
    ///
    /// The handler receives the original (pre-transform) [`Event`] and the
    /// [`Error`](crate::core::Error) that caused the skip. Use this to route
    /// discarded events to a DLQ, external error store, or alerting system.
    ///
    /// # Hard constraints
    ///
    /// **The handler runs synchronously in the runtime poll loop.** It must not
    /// block (no `sleep`, no synchronous I/O, no blocking locks) and must not
    /// panic. Buffer the event into an internal channel and drain asynchronously
    /// if you need slow I/O, or use [`RuntimeOptions::with_dead_letter_handler_async`]
    /// to automatically spawn the handler as a detached Tokio task.
    pub fn with_dead_letter_handler(
        mut self,
        handler: impl Fn(Event, Error) + Send + Sync + 'static,
    ) -> Self {
        self.dead_letter_handler = Some(std::sync::Arc::new(handler));
        self
    }

    /// Register an **async** dead-letter handler invoked when an event is skipped
    /// under [`TransformErrorPolicy::Skip`].
    ///
    /// Unlike [`with_dead_letter_handler`](Self::with_dead_letter_handler), this
    /// variant spawns the handler as a **detached [`tokio::task`]** so the async
    /// future can await slow I/O (network writes, channel sends, file appends)
    /// without blocking the CDC poll loop.
    ///
    /// # Ordering
    ///
    /// Because each invocation is spawned as an independent task, handler calls
    /// for different events may execute concurrently and may complete out-of-order
    /// relative to each other. If strict ordering matters, use a bounded channel
    /// inside the handler and drain it sequentially from a single background task.
    ///
    /// # Panics
    ///
    /// The spawned task is detached (`tokio::spawn`); a panic inside the handler
    /// future will abort only that task, not the CDC runtime.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use rustcdc::{core::RuntimeOptions, TransformErrorPolicy};
    ///
    /// let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    /// let options = RuntimeOptions::default()
    ///     .with_transform_error_policy(TransformErrorPolicy::Skip)
    ///     .with_dead_letter_handler_async(move |event, error| {
    ///         let tx = tx.clone();
    ///         async move {
    ///             // Can await here safely — runs in a separate task.
    ///             let _ = tx.send((event, error));
    ///         }
    ///     });
    /// // Drive rx in a background task to drain the DLQ.
    /// ```
    pub fn with_dead_letter_handler_async<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Event, Error) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.dead_letter_handler = Some(std::sync::Arc::new(move |event, error| {
            tokio::spawn(handler(event, error));
        }));
        self
    }

    /// Set a timeout for sink close during orderly runtime shutdown.
    ///
    /// When set, the shutdown path should call [`crate::sink::SinkAdapter::close_with_timeout`]
    /// with this value so a hung sink (e.g. a Kafka producer waiting for broker
    /// acknowledgement) cannot prevent the process from exiting. Returns
    /// [`Error::TimeoutError`] if the deadline is exceeded.
    ///
    /// Pass `None` to leave the close duration unbounded (the default).
    pub fn with_sink_close_timeout_ms(mut self, timeout_ms: impl Into<Option<u64>>) -> Self {
        self.sink_close_timeout_ms = timeout_ms.into();
        self
    }

    /// Choose whether a delivered batch may end mid-transaction.
    ///
    /// ```
    /// use rustcdc::{RuntimeOptions, TransactionBoundaryPolicy};
    ///
    /// // A sink that must apply each source transaction atomically.
    /// let options = RuntimeOptions::new()
    ///     .with_transaction_boundary(TransactionBoundaryPolicy::PreserveTransactions);
    /// ```
    #[must_use]
    pub fn with_transaction_boundary(mut self, policy: TransactionBoundaryPolicy) -> Self {
        self.transaction_boundary = policy;
        self
    }
}

/// Runtime-level idempotency guard configuration.
///
/// # Both constructors return `Result`, so the chain needs a `?` per step
///
/// `capacity` and `ttl_ms` are validated where they are set rather than at
/// `CdcRuntime::new`, because a zero for either silently disables the guard rather than
/// configuring it — a window of zero suppresses nothing, and a TTL of zero expires every
/// fingerprint before the next event. That makes both methods fallible, which means the
/// natural-looking chain does **not** compile:
///
/// ```compile_fail
/// use rustcdc::IdempotencyOptions;
/// # fn main() -> rustcdc::Result<()> {
/// // error[E0599]: no method named `with_ttl_ms` found for enum `Result`
/// let options = IdempotencyOptions::new(100_000).with_ttl_ms(60_000);
/// # Ok(()) }
/// ```
///
/// Write it with a `?` after each step:
///
/// ```
/// use rustcdc::IdempotencyOptions;
/// # fn main() -> rustcdc::Result<()> {
/// let options = IdempotencyOptions::new(100_000)?.with_ttl_ms(60_000)?;
/// assert_eq!(options.capacity, 100_000);
/// assert_eq!(options.ttl_ms, Some(60_000));
/// # Ok(()) }
/// ```
///
/// Outside a `Result`-returning function, unwrap the constant once:
///
/// ```
/// use rustcdc::IdempotencyOptions;
///
/// let options = IdempotencyOptions::new(100_000)
///     .and_then(|options| options.with_ttl_ms(60_000))
///     .expect("literal capacity and TTL are both non-zero");
/// # let _ = options;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdempotencyOptions {
    /// Maximum fingerprints retained in the sliding window.
    ///
    /// Sized for the replay distance of the deployment, not for the event rate: once the
    /// window fills, duplicates older than it stop being suppressed. Evictions are
    /// counted in `RuntimeAdminSnapshot::idempotency_evictions`.
    pub capacity: usize,
    /// Optional fingerprint lifetime in milliseconds.
    ///
    /// `None` keeps a fingerprint until capacity evicts it. A TTL admits an expected
    /// long-tail replay after a retention window while still suppressing immediate
    /// duplicates.
    pub ttl_ms: Option<u64>,
}

impl IdempotencyOptions {
    /// Build options with the given window capacity and no TTL.
    ///
    /// Returns a `Result`, so chaining [`with_ttl_ms`](Self::with_ttl_ms) onto it needs a
    /// `?` in between — see the type-level docs.
    ///
    /// ```
    /// use rustcdc::IdempotencyOptions;
    /// # fn main() -> rustcdc::Result<()> {
    /// let options = IdempotencyOptions::new(100_000)?.with_ttl_ms(60_000)?;
    /// # let _ = options;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] if `capacity` is zero. A zero-capacity window
    /// suppresses nothing, so accepting it would present a disabled guard as an enabled
    /// one.
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

    /// Set a fingerprint lifetime in milliseconds.
    ///
    /// Takes and returns `Self`, but is called on the `Ok` value of
    /// [`new`](Self::new) — `IdempotencyOptions::new(n)?.with_ttl_ms(ms)?`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] if `ttl_ms` is zero. A zero TTL expires every
    /// fingerprint before the next event, disabling the guard rather than tuning it.
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
/// // Build with the typed constructor
/// let policy = ConnectionRetryPolicy::new()
///     .with_max_retries(Some(5))
///     .with_initial_delay_ms(300)
///     .with_max_delay_ms(10_000);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Construct a `ConnectionRetryPolicy` starting from the default values.
    ///
    /// This is the canonical constructor when struct-literal syntax is not
    /// available (e.g. outside the crate due to `#[non_exhaustive]`).
    ///
    /// ```
    /// use rustcdc::core::ConnectionRetryPolicy;
    ///
    /// let policy = ConnectionRetryPolicy::new()
    ///     .with_max_retries(Some(10))
    ///     .with_initial_delay_ms(500)
    ///     .with_max_delay_ms(30_000);
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of retries (`None` = retry indefinitely).
    pub fn with_max_retries(mut self, n: Option<u32>) -> Self {
        self.max_retries = n;
        self
    }

    /// Set the initial delay between retries in milliseconds.
    pub fn with_initial_delay_ms(mut self, ms: u64) -> Self {
        self.initial_delay_ms = ms;
        self
    }

    /// Set the maximum delay cap for exponential backoff in milliseconds.
    pub fn with_max_delay_ms(mut self, ms: u64) -> Self {
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
///
/// `#[non_exhaustive]`: this enum gains a variant with every connector the crate
/// adds, which makes it the single most certain future source of breakage for an
/// embedder that matches on it exhaustively. Add a `_` arm.
#[derive(Clone)]
#[non_exhaustive]
pub enum RuntimeSourceConfig {
    #[cfg(feature = "postgres")]
    /// PostgreSQL logical replication via pgoutput.
    Postgres(PostgresSourceConfig),
    #[cfg(feature = "mysql")]
    /// MySQL binlog replication.
    Mysql(MysqlSourceConfig),
    #[cfg(feature = "mariadb")]
    /// MariaDB binlog replication. Shares the MySQL transport with MariaDB source
    /// identity, so checkpoints land in their own namespace and GTID formats do not mix.
    MariaDb(crate::source::MariaDbSourceConfig),
    #[cfg(feature = "sqlserver")]
    /// SQL Server CDC capture tables.
    SqlServer(SqlServerSourceConfig),
    /// No source. The runtime accepts injected events and exercises the full
    /// buffer/commit path, which is what the tests and examples use.
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
    /// Constructed but never started.
    ///
    /// Note this does **not** mean "running with nothing to do" — for that, and for the
    /// distinction from a stalled connector, read
    /// [`RuntimeAdminSnapshot::health`] instead. `RuntimeState` alone cannot tell a quiet
    /// database from a hung socket: both report `Running`.
    Idle,
    /// Started and polling.
    Running,
    /// A shutdown is in progress.
    Stopping,
    /// Stopped. May be started again.
    Stopped,
}

/// Derived health verdict — what an operator actually needs to page on.
///
/// [`RuntimeState`] alone cannot answer the question that matters during an incident.
/// A connector streaming normally from a quiet database and one hung on a dead socket
/// both report `state = running` with flat counters and `readiness = true`; the two are
/// indistinguishable, so "no events for 10 minutes" is either completely fine or a
/// production outage and the operator cannot tell which.
///
/// This composes the three signals that *do* distinguish them — poll recency,
/// polled-versus-committed divergence, and source-side lag growth — into one verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[non_exhaustive]
pub enum HealthVerdict {
    /// Running and making progress: events polled and committed recently.
    Healthy,
    /// Running correctly with nothing to do — the source genuinely has no changes.
    ///
    /// Distinguished from [`Stalled`](HealthVerdict::Stalled) by the poll loop still
    /// completing on schedule and source-side lag not growing.
    Idle,
    /// Running, but something is wrong and progress has stopped or is degrading.
    ///
    /// `reason` names which signal fired, so an alert can route without a human first
    /// correlating three metrics by hand.
    Stalled {
        /// Human-readable description of the specific stall condition detected.
        reason: String,
    },
    /// Not running: never started, stopping, or stopped.
    NotRunning,
}

impl HealthVerdict {
    /// Whether this verdict warrants operator attention.
    ///
    /// [`Idle`](HealthVerdict::Idle) is deliberately **not** alertable: a quiet database
    /// is the single most common cause of "no events", and paging on it trains operators
    /// to ignore the alert.
    pub fn is_alertable(&self) -> bool {
        matches!(self, Self::Stalled { .. })
    }

    /// Short stable label for metrics and dashboards.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Idle => "idle",
            Self::Stalled { .. } => "stalled",
            Self::NotRunning => "not_running",
        }
    }
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
///
/// This struct is `#[non_exhaustive]`: new fields may be added in minor releases.
/// Use `..` in struct patterns and do not rely on exhaustive construction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
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
    /// Fingerprints the idempotency guard evicted because its window filled.
    ///
    /// Growing steadily means the window is too small for this deployment's replay
    /// distance: older duplicates stop being suppressed. Delivery stays at-least-once,
    /// but a sink relying on the guard will begin seeing repeats. Raise
    /// `IdempotencyOptions::capacity`. `None` when the guard is disabled.
    pub idempotency_evictions: Option<u64>,
    /// Events the idempotency guard passed through because it could not identify them.
    ///
    /// These come from tables with no primary key on connectors that supply no
    /// intra-transaction sequencing. The guard deliberately does not deduplicate them
    /// — dropping a distinct row is unrecoverable, whereas a duplicate is the
    /// documented at-least-once contract. `None` when the guard is disabled.
    pub idempotency_unidentifiable_passthrough: Option<u64>,
    /// Events permanently dropped by [`TransformErrorPolicy::Skip`].
    ///
    /// **Any non-zero value means data was lost.** A skipped event is dropped *and* the
    /// checkpoint advances past it, so it is never replayed. Alert on any increase.
    pub total_events_skipped: u64,
    /// Transform rules that have never matched anything since `start()`.
    ///
    /// A masking rule that never fires means a column is shipping in clear text; a
    /// routing rule that never fires means events are going to the default destination.
    /// Neither errors, so this is the only signal. **Non-empty is only meaningful after
    /// real traffic** — every rule is unmatched before the first event.
    ///
    /// See [`UnmatchedRule`](crate::transform::UnmatchedRule).
    pub unmatched_transform_rules: Vec<crate::transform::UnmatchedRule>,
    /// Derived health verdict.
    ///
    /// Use this for alerting rather than composing `state`, counters and timestamps by
    /// hand — `state` alone cannot distinguish a healthy idle connector from a stalled
    /// one. See [`HealthVerdict::is_alertable`].
    pub health: HealthVerdict,
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
    /// Replication slot WAL lag in bytes (`pg_current_wal_lsn - confirmed_flush_lsn`).
    ///
    /// Only populated for PostgreSQL sources after the first idle-advance call.
    /// `None` means the lag has not yet been measured (the slot may still be behind).
    /// `Some(0)` means the slot is fully caught up to the current WAL write position.
    pub replication_slot_lag_bytes: Option<u64>,
}

/// Opaque token representing an in-flight batch prefix that may be committed.
///
/// Dropping an `AckToken` without passing it to [`CdcRuntime::commit_ack`] will
/// stall checkpoint progress indefinitely. The `#[must_use]` attribute ensures
/// the compiler emits a warning if the token is silently discarded.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "AckToken must be passed to CdcRuntime::commit_ack(); dropping it silently stalls the commit barrier"]
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
#[must_use = "AckMode::Required must be passed to CdcRuntime::commit_ack(); ignoring it stalls the commit barrier"]
#[non_exhaustive]
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

/// A batch of CDC events delivered from [`CdcRuntime::poll_event_batch`].
///
/// Internally the events vector is reference-counted so that the runtime can
/// keep a copy in `pending_delivery` for replay without an O(n) clone per
/// delivery.  All public accessors expose the same slice/vec API as before.
///
/// Implements [`IntoIterator`] for both owned and borrowed use:
/// ```no_run
/// # use rustcdc::CdcRuntime;
/// # async fn example(runtime: &mut CdcRuntime) -> rustcdc::Result<()> {
/// let batch = runtime.poll_event_batch().await?;
/// for event in &batch {           // borrow
///     println!("{}", event.table);
/// }
/// let mode = batch.ack_mode();
/// runtime.commit_ack(mode).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq)]
#[must_use = "poll_event_batch() returns an EventBatch that must be acknowledged via commit_ack()"]
pub struct EventBatch {
    events: Arc<Vec<Event>>,
    /// Index of the first event in this batch within `events`.
    ///
    /// A redelivered batch is the *uncommitted suffix* of an in-flight delivery. Taking
    /// that suffix used to deep-clone the whole slice on every re-poll; sharing the
    /// `Arc` and carrying an offset makes redelivery allocation-free. The repository's
    /// own `benches/cdc_perf.rs` benchmarked exactly this trade-off and showed the
    /// shared-view variant faster — the result was simply never applied to production.
    offset: usize,
    ack_token: Option<AckToken>,
}

impl EventBatch {
    fn empty() -> Self {
        Self {
            events: Arc::new(Vec::new()),
            offset: 0,
            ack_token: None,
        }
    }

    /// Borrow the delivered events.
    pub fn events(&self) -> &[Event] {
        &self.events[self.offset..]
    }

    /// Consume the batch and return its events.
    ///
    /// Zero-copy when this batch covers the whole buffer and the runtime has already
    /// dropped its internal reference (via `commit_ack`); otherwise the events are
    /// cloned out of the shared buffer.
    pub fn into_events(self) -> Vec<Event> {
        if self.offset == 0 {
            Arc::try_unwrap(self.events).unwrap_or_else(|arc| (*arc).clone())
        } else {
            self.events[self.offset..].to_vec()
        }
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
        self.events.len() - self.offset
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the smallest `ts` (milliseconds since epoch) across all events in this batch.
    ///
    /// Returns `None` when the batch is empty.
    pub fn oldest_event_source_timestamp_ms(&self) -> Option<u64> {
        self.events().iter().map(|e| e.ts).min()
    }

    /// Returns the largest `ts` (milliseconds since epoch) across all events in this batch.
    ///
    /// Returns `None` when the batch is empty.
    pub fn latest_event_source_timestamp_ms(&self) -> Option<u64> {
        self.events().iter().map(|e| e.ts).max()
    }

    /// Returns `true` if any event in this batch has `before_is_key_only == true`.
    ///
    /// Use this to decide whether to fetch full pre-images from the source before
    /// computing row diffs. When this returns `true`, at least one UPDATE or DELETE
    /// event in the batch carries only primary-key columns in `before`.
    pub fn has_key_only_befores(&self) -> bool {
        self.events().iter().any(|e| e.before_is_key_only)
    }

    /// Returns an iterator over references to events in this batch.
    ///
    /// Equivalent to `batch.events().iter()`.
    pub fn iter(&self) -> std::slice::Iter<'_, Event> {
        self.events().iter()
    }

    /// Returns a deduplicated, sorted list of table names present in this batch.
    ///
    /// Useful for routing decisions, per-table metrics, and conditional sink selection.
    ///
    /// ```no_run
    /// # use rustcdc::CdcRuntime;
    /// # async fn example(runtime: &mut CdcRuntime) -> rustcdc::Result<()> {
    /// let batch = runtime.poll_event_batch().await?;
    /// for table in batch.tables() {
    ///     println!("batch contains events for table: {table}");
    /// }
    /// # runtime.commit_ack(batch.ack_mode()).await
    /// # }
    /// ```
    pub fn tables(&self) -> Vec<&str> {
        let mut tables: Vec<&str> = self.events().iter().map(|e| e.table.as_str()).collect();
        tables.sort_unstable();
        tables.dedup();
        tables
    }

    /// Returns a deduplicated, sorted list of fully-qualified table names
    /// (`"schema.table"` or `"table"` when no schema is set).
    ///
    /// Useful when routing events to Kafka topics or per-table sinks where
    /// tables from different schemas must be distinguished.
    pub fn qualified_tables(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .events
            .iter()
            .map(|e| e.qualified_table_name())
            .collect();
        names.sort_unstable();
        names.dedup();
        names
    }

    /// Number of events in this batch that belong to the given table.
    ///
    /// The `table` parameter is matched against the unqualified `event.table` field.
    /// Use [`qualified_tables`](Self::qualified_tables) and filter `event.qualified_table_name()`
    /// when schema disambiguation is needed.
    pub fn event_count_for_table(&self, table: &str) -> usize {
        self.events().iter().filter(|e| e.table == table).count()
    }
}

impl<'a> IntoIterator for &'a EventBatch {
    type Item = &'a Event;
    type IntoIter = std::slice::Iter<'a, Event>;

    fn into_iter(self) -> Self::IntoIter {
        self.events().iter()
    }
}

impl IntoIterator for EventBatch {
    type Item = Event;
    type IntoIter = std::vec::IntoIter<Event>;

    fn into_iter(self) -> Self::IntoIter {
        Arc::try_unwrap(self.events)
            .unwrap_or_else(|arc| (*arc).clone())
            .into_iter()
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
    /// Surface the error to the caller and stop. The default, and the safe choice.
    Halt,
    /// Drop the failing event and continue.
    ///
    /// **This loses data.** A skipped event never reaches the commit barrier, so it gets
    /// no offset — but the events after it do, and the checkpoint persists the last
    /// accepted offset, which is *past* the skipped one. It is therefore never replayed.
    ///
    /// Selecting this requires a `dead_letter_handler`, so the loss is a deliberate,
    /// captured routing decision rather than a `warn!` line, and every skip increments
    /// `RuntimeAdminSnapshot::total_events_skipped`. Alert on any increase.
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
    /// A source supplied by the embedder via [`CdcRuntime::register_source`].
    Custom(Box<dyn crate::source::Source>),
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
            Self::Custom(source) => source.connect().await,
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
            Self::Custom(source) => source.close().await,
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
            Self::Custom(source) => source.start_snapshot(&refs).await,
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
            Self::Custom(source) => {
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
            Self::Custom(source) => source.start_stream(resume_from).await,
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
            Self::Custom(_) => Err(Error::ConfigError(
                "incremental snapshot startup is not available for a custom source. \
                 The DBLog watermark algorithm needs connector-native watermark queries \
                 that the Source trait does not expose; use snapshot_tables for a \
                 blocking initial snapshot instead."
                    .into(),
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
            Self::Custom(source) => source.perform_handoff(snapshot, stream).await,
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
    /// Events dropped by [`TransformErrorPolicy::Skip`].
    ///
    /// The docs promised a `transform_error_skipped_count` metric that did not exist
    /// anywhere in the crate, so an operator following them built an alert on a metric
    /// that was never emitted. Skipped events are unrecoverable (the checkpoint advances
    /// past them), which makes this the one counter that must not be missing.
    total_events_skipped: u64,
    last_checkpoint_saved_at_ms: Option<u64>,
    transform_pipeline: TransformPipeline,
    idempotency_guard: Option<EventIdempotencyGuard>,
    /// Registered sink that is closed (with the configured timeout) during
    /// [`stop`](CdcRuntime::stop), [`force_stop`](CdcRuntime::force_stop), and
    /// [`drain_and_stop`](CdcRuntime::drain_and_stop).
    ///
    /// Wrapped in `Mutex` so that `CdcRuntime` remains `Sync` (required by
    /// `BoxStream::boxed` used in the poll path).
    registered_sink: Option<std::sync::Mutex<BoxedSink>>,
    /// LSN that was durably checkpointed but which the source refused to confirm.
    ///
    /// A failed `confirm_lsn` leaves the source replaying events the runtime has
    /// already committed. The idempotency guard then suppresses every one of them —
    /// they were fingerprinted on the first pass — so the poll loop returns empty
    /// forever while liveness and readiness both report healthy. That is a silent,
    /// unbounded stall, and on PostgreSQL it accumulates WAL on the primary.
    ///
    /// Retaining the LSN lets the next poll retry the confirmation before dedup runs,
    /// which is what breaks the loop. If it cannot be confirmed, the runtime reports
    /// not-ready and fails loud rather than idling.
    pending_confirmation_lsn: Option<u64>,
    /// Consecutive polls that produced events but had every one suppressed by the
    /// idempotency guard while `pending_confirmation_lsn` was set.
    unconfirmed_stall_polls: u32,
}

/// Consecutive fully-suppressed polls tolerated before a stalled, unconfirmed
/// source position is escalated from a warning to a hard error.
pub(crate) const UNCONFIRMED_STALL_POLL_LIMIT: u32 = 3;

/// Multiple of `max_poll_wait_ms` after which a non-completing poll is a stall.
///
/// Generous on purpose: a slow-but-working poll must never be reported as stalled, or
/// the verdict becomes noise and operators stop trusting it.
const HEALTH_POLL_STALL_MULTIPLIER: u64 = 6;

/// Floor for the stall threshold, so a very small `max_poll_wait_ms` cannot produce a
/// threshold short enough to fire on normal scheduling jitter.
const HEALTH_MIN_POLL_STALL_MS: u64 = 30_000;

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

        // Record slot lag bytes if available from the last idle-advance cycle.
        if let Some(lag_bytes) = self
            .stream
            .as_ref()
            .and_then(|s| s.replication_slot_lag_bytes())
        {
            self.observability()
                .metrics
                .record_replication_slot_lag_bytes(lag_bytes);
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

        // `Skip` without somewhere to put the skipped events is silent data loss.
        //
        // A skipped event never reaches the commit barrier, so it never gets an offset
        // — but the events *after* it do, and `commit` persists the last accepted
        // offset, which is past the skipped one. The event is therefore never replayed
        // on restart: it is gone permanently. Out of the box the only trace was a
        // `warn!` log line.
        //
        // Requiring a dead-letter handler makes that a deliberate, captured routing
        // decision rather than an invisible one.
        if matches!(
            config.options.transform_error_policy,
            TransformErrorPolicy::Skip
        ) && config.options.dead_letter_handler.is_none()
        {
            return Err(Error::ConfigError(
                "TransformErrorPolicy::Skip requires a dead-letter handler. A skipped event \
                 is dropped *and* the checkpoint advances past it, so it is never replayed — \
                 without a handler the event is lost permanently with only a log line. \
                 Configure RuntimeOptions::with_dead_letter_handler(...) to capture skipped \
                 events, or use TransformErrorPolicy::Halt to stop on transform errors."
                    .into(),
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
            total_events_skipped: 0,
            last_checkpoint_saved_at_ms: None,
            pending_confirmation_lsn: None,
            unconfirmed_stall_polls: 0,
            transform_pipeline: TransformPipeline::default(),
            idempotency_guard,
            registered_sink: None,
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

    /// Add a **synchronous** transform stage applied to polled events.
    ///
    /// Prefer this. Every transform this crate ships is synchronous, and the sync path
    /// avoids a boxed future per event on the hottest path in the library.
    pub fn add_transform(&mut self, transform: Box<dyn crate::transform::Transform>) {
        self.transform_pipeline.add_transform(transform);
    }

    /// Add an **async** transform stage applied to polled events.
    ///
    /// For a stage that genuinely must `await` — a WASM sandbox, a network enrichment
    /// lookup. See [`crate::transform::AsyncTransform`].
    pub fn add_async_transform(&mut self, transform: Box<dyn crate::transform::AsyncTransform>) {
        self.transform_pipeline.add_async_transform(transform);
    }

    /// Register a sink to be closed (with the configured timeout) during
    /// [`stop`](CdcRuntime::stop), [`force_stop`](CdcRuntime::force_stop), and
    /// [`drain_and_stop`](CdcRuntime::drain_and_stop).
    ///
    /// The timeout is read from [`RuntimeOptions::sink_close_timeout_ms`] at
    /// shutdown time. If no timeout is configured, [`SinkAdapter::close`] is
    /// called without a deadline.
    ///
    /// Replaces any previously registered sink. The replaced sink is **dropped**
    /// without being closed — call `close` on it first if graceful close matters.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut sink = MyKafkaSink::new(config);
    /// runtime.register_sink(sink);
    /// runtime.start().await?;
    /// // …poll loop…
    /// runtime.stop().await?; // closes the registered sink with configured timeout
    /// ```
    pub fn register_sink<S: crate::sink::SinkAdapter + 'static>(&mut self, sink: S) {
        self.registered_sink = Some(std::sync::Mutex::new(BoxedSink::new(sink)));
    }

    /// Drive the runtime from a source this crate does not ship.
    ///
    /// Replaces whatever source the [`RuntimeConfig`] selected. Everything the runtime
    /// provides — the commit barrier, checkpointing, transforms, the idempotency
    /// guard, health verdicts, metrics — applies unchanged to a third-party
    /// [`Source`](crate::source::Source).
    ///
    /// Call this **before** [`CdcRuntime::start`]; the source is connected during
    /// `start()`.
    ///
    /// # Checkpoint offsets
    ///
    /// The runtime derives the checkpoint offset from the delivered event for the
    /// connectors it knows. For a custom source it falls back to persisting
    /// `Event::source.offset` verbatim, so that field must be a complete, resumable
    /// position — the same string your `start_stream(resume_from)` is able to resume
    /// from. Implement [`StreamHandle::position_offset`](crate::source::StreamHandle::position_offset)
    /// if you need richer state.
    pub fn register_source(&mut self, source: Box<dyn crate::source::Source>) {
        self.source = RuntimeSource::Custom(source);
    }

    /// Replace the runtime source with a mock for testing.
    #[cfg(test)]
    pub(crate) fn inject_mock_source(&mut self, source: Box<dyn crate::source::Source>) {
        self.register_source(source);
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
            Event, EventTracer, HealthVerdict, MetricsCollector, NoOpEventTracer,
            NoOpMetricsCollector, Operation, SnapshotMetadata, SourceMetadata,
            EVENT_ENVELOPE_VERSION,
        },
        ddl_capture::DdlDialect,
        schema_history::{InMemorySchemaHistory, SchemaHistoryRetention},
        transform::Transform,
    };

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
    use crate::checkpoint::FileCheckpoint;

    use super::{
        AckMode, CdcRuntime, ConnectionRetryPolicy, EventBatch, IdempotencyOptions, RuntimeConfig,
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
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
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

        fn record_replication_slot_lag_bytes(&self, _lag_bytes: u64) {}

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
        assert!(prometheus.contains("rustcdc_runtime_readiness"));
        assert!(prometheus.contains("rustcdc_runtime_events_polled_total"));
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

    // ─── Transaction-boundary policy ─────────────────────────────────────────

    use super::{RuntimeOptions, TransactionBoundaryPolicy};
    use crate::core::TransactionMetadata;

    fn tx_event(tx_id: u64, event_index: u32, offset: &str) -> Event {
        let mut event = event();
        event.op = Operation::Insert;
        event.source.offset = offset.to_string();
        event.transaction = Some(TransactionMetadata {
            tx_id,
            total_events: None,
            event_index,
        });
        event
    }

    fn transaction_boundary_runtime(
        policy: TransactionBoundaryPolicy,
        max_buffer_size: usize,
    ) -> CdcRuntime {
        let config = RuntimeConfig::new(
            RuntimeSourceConfig::Disabled,
            InMemoryCheckpoint::default(),
            crate::schema_history::InMemorySchemaHistory::default(),
        )
        .with_idempotency_disabled()
        .with_options(
            RuntimeOptions::new()
                .with_max_buffer_size(max_buffer_size)
                .with_transaction_boundary(policy),
        );
        CdcRuntime::new(config).unwrap()
    }

    /// Simulate the cut `flush_pending_source_events` makes: take `cut` events off
    /// the queue, leave the rest, then apply the boundary policy.
    fn cut_and_trim(
        runtime: &mut CdcRuntime,
        queued: Vec<Event>,
        cut: usize,
    ) -> (Vec<String>, Vec<String>) {
        runtime.pending_source_events.extend(queued);
        let mut chunk: Vec<Event> = (0..cut)
            .filter_map(|_| runtime.pending_source_events.pop_front())
            .collect();
        runtime.trim_to_transaction_boundary(&mut chunk);

        let delivered = chunk
            .iter()
            .map(|event| event.source.offset.clone())
            .collect();
        let requeued = runtime
            .pending_source_events
            .iter()
            .map(|event| event.source.offset.clone())
            .collect();
        (delivered, requeued)
    }

    #[test]
    fn split_is_the_default_policy() {
        assert_eq!(
            RuntimeOptions::default().transaction_boundary,
            TransactionBoundaryPolicy::Split,
            "changing the default would silently alter batch shapes for every embedder"
        );
    }

    #[test]
    fn split_policy_cuts_batches_mid_transaction() {
        // Baseline: the default deliberately splits, so the guarantee added by
        // `PreserveTransactions` is measured against a real difference, not a no-op.
        let mut runtime = transaction_boundary_runtime(TransactionBoundaryPolicy::Split, 16);
        let queued = vec![
            tx_event(7, 0, "o0"),
            tx_event(7, 1, "o1"),
            tx_event(8, 0, "o2"),
            tx_event(8, 1, "o3"),
        ];

        let (delivered, requeued) = cut_and_trim(&mut runtime, queued, 3);
        assert_eq!(delivered, vec!["o0", "o1", "o2"], "the cut is not adjusted");
        assert_eq!(requeued, vec!["o3"]);
    }

    #[test]
    fn preserve_transactions_trims_a_trailing_partial_transaction() {
        // The cut at 3 lands inside tx 8, so the batch must be trimmed back to the
        // tx 7 / tx 8 boundary and the partial transaction returned to the queue.
        let mut runtime =
            transaction_boundary_runtime(TransactionBoundaryPolicy::PreserveTransactions, 16);
        let queued = vec![
            tx_event(7, 0, "o0"),
            tx_event(7, 1, "o1"),
            tx_event(8, 0, "o2"),
            tx_event(8, 1, "o3"),
        ];

        let (delivered, requeued) = cut_and_trim(&mut runtime, queued, 3);
        assert_eq!(
            delivered,
            vec!["o0", "o1"],
            "batch must end on the boundary"
        );
        assert_eq!(
            requeued,
            vec!["o2", "o3"],
            "trimmed events must be requeued in their original order, none dropped"
        );
    }

    #[test]
    fn preserve_transactions_leaves_a_batch_that_already_ends_on_a_boundary() {
        let mut runtime =
            transaction_boundary_runtime(TransactionBoundaryPolicy::PreserveTransactions, 16);
        let queued = vec![
            tx_event(7, 0, "o0"),
            tx_event(7, 1, "o1"),
            tx_event(8, 0, "o2"),
        ];

        let (delivered, requeued) = cut_and_trim(&mut runtime, queued, 2);
        assert_eq!(delivered, vec!["o0", "o1"]);
        assert_eq!(requeued, vec!["o2"]);
    }

    #[test]
    fn a_transaction_larger_than_the_buffer_is_delivered_split_rather_than_stalling() {
        // Holding this back would produce an empty batch forever — a silent permanent
        // stall, strictly worse than the split the policy exists to avoid. The escape
        // hatch is `max_buffer_size`: once one unfinished transaction fills the batch,
        // it ships split with a WARN.
        let mut runtime =
            transaction_boundary_runtime(TransactionBoundaryPolicy::PreserveTransactions, 2);
        let queued = (0..4)
            .map(|index| tx_event(9, index, &format!("o{index}")))
            .collect();

        let (delivered, _) = cut_and_trim(&mut runtime, queued, 2);
        assert_eq!(
            delivered,
            vec!["o0", "o1"],
            "a transaction that cannot fit in one batch must still make progress"
        );
    }

    #[test]
    fn a_transaction_whose_rest_has_not_arrived_yet_is_held_back() {
        // The load-bearing case, and the one the previous implementation got wrong: the
        // queue behind the cut is *empty*, which means "I have not seen the rest yet" —
        // not "there is no rest". Treating the two as the same delivered a partial
        // transaction whenever one spanned two polls, which for a streaming source is
        // the common case rather than the exception.
        let mut runtime =
            transaction_boundary_runtime(TransactionBoundaryPolicy::PreserveTransactions, 16);
        let queued = vec![tx_event(11, 0, "o0"), tx_event(11, 1, "o1")];

        let (delivered, requeued) = cut_and_trim(&mut runtime, queued, 2);
        assert!(
            delivered.is_empty(),
            "a transaction with no observed end must not be delivered, got {delivered:?}"
        );
        assert_eq!(
            requeued,
            vec!["o0", "o1"],
            "the withheld events must stay queued, in order, for the next batch"
        );
    }

    #[test]
    fn a_transaction_that_declares_its_size_is_delivered_once_complete() {
        // `total_events` is the only end-of-transaction signal available when nothing is
        // queued behind the cut. A source that fills it in must not be made to wait.
        let mut runtime =
            transaction_boundary_runtime(TransactionBoundaryPolicy::PreserveTransactions, 16);
        let mut first = tx_event(12, 0, "o0");
        let mut second = tx_event(12, 1, "o1");
        if let Some(tx) = first.transaction.as_mut() {
            tx.total_events = Some(2);
        }
        if let Some(tx) = second.transaction.as_mut() {
            tx.total_events = Some(2);
        }

        let (delivered, requeued) = cut_and_trim(&mut runtime, vec![first, second], 2);
        assert_eq!(
            delivered,
            vec!["o0", "o1"],
            "a declared-complete transaction must ship immediately"
        );
        assert!(requeued.is_empty());
    }

    #[test]
    fn an_event_without_transaction_metadata_is_its_own_boundary() {
        // Snapshot rows and connectors that do not report transactions must never be
        // withheld — waiting for an end that will never be signalled is a wedge.
        let mut runtime =
            transaction_boundary_runtime(TransactionBoundaryPolicy::PreserveTransactions, 16);
        let mut plain = tx_event(13, 0, "o0");
        plain.transaction = None;

        let (delivered, _) = cut_and_trim(&mut runtime, vec![plain], 1);
        assert_eq!(delivered, vec!["o0"]);
    }

    #[test]
    fn preserve_transactions_leaves_events_without_transaction_metadata_alone() {
        // Snapshot rows and connectors that report no transaction boundaries must not
        // be trimmed — otherwise every event looks like "the same (absent) transaction"
        // and the batch would be trimmed to nothing.
        let mut runtime =
            transaction_boundary_runtime(TransactionBoundaryPolicy::PreserveTransactions, 16);
        let queued = (0..4)
            .map(|index| {
                let mut e = event();
                e.source.offset = format!("o{index}");
                e
            })
            .collect();

        let (delivered, requeued) = cut_and_trim(&mut runtime, queued, 2);
        assert_eq!(delivered, vec!["o0", "o1"]);
        assert_eq!(requeued, vec!["o2", "o3"]);
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

    impl Transform for FailTransform {
        fn apply(&self, _event: &mut Event) -> crate::core::Result<bool> {
            Err(crate::core::Error::TransformError("boom".into()))
        }

        fn name(&self) -> &str {
            "fail_transform"
        }
    }

    impl Transform for NonDeterministicTransform {
        fn apply(&self, event: &mut Event) -> crate::core::Result<bool> {
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
        assert!(
            matches!(error.root_cause(), crate::core::Error::TransformError(_)),
            "the transform failure must reach the caller with its cause intact; got: {error:?}"
        );
        assert!(error.to_string().contains("fail_transform"));
    }

    /// `Skip` without a dead-letter handler is silent data loss, and is rejected.
    ///
    /// A skipped event never reaches the commit barrier, so it gets no offset — but the
    /// events after it do, and `commit` persists the last accepted offset, which is past
    /// the skipped one. The event is therefore never replayed on restart. Out of the box
    /// the only trace was a `warn!` line.
    #[tokio::test]
    async fn transform_error_policy_skip_requires_a_dead_letter_handler() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_transform_error_policy(TransformErrorPolicy::Skip);

        let error = match CdcRuntime::new(config) {
            Err(error) => error,
            Ok(_) => panic!("Skip without a dead-letter handler must be rejected"),
        };
        let message = error.to_string();
        assert!(message.contains("dead-letter handler"), "{message}");
        assert!(message.contains("never replayed"), "{message}");
    }

    #[tokio::test]
    async fn transform_error_policy_skip_routes_to_dead_letter_and_counts() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let captured = Arc::new(AtomicUsize::new(0));
        let sink = Arc::clone(&captured);

        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let mut config =
            RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
                .with_transform_error_policy(TransformErrorPolicy::Skip);
        config.options = config
            .options
            .with_dead_letter_handler(move |_event, _error| {
                sink.fetch_add(1, Ordering::Relaxed);
            });

        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.add_transform(Box::new(FailTransform));

        let events = runtime.apply_transforms(vec![event()]).await.unwrap();
        assert!(
            events.is_empty(),
            "the failing event is dropped from the batch"
        );
        assert_eq!(
            captured.load(Ordering::Relaxed),
            1,
            "the skipped event must reach the dead-letter handler"
        );
        assert_eq!(
            runtime.admin_snapshot().total_events_skipped,
            1,
            "skipped events must be counted — any non-zero value means data was lost"
        );
    }

    // ─── Mock source infrastructure ─────────────────────────────────────────

    use std::collections::VecDeque as TestDeque;

    struct MockStreamHandle {
        batches: TestDeque<Vec<Event>>,
        confirmed_lsns: Arc<Mutex<Vec<u64>>>,
        /// Shared so a test can clear it mid-run, simulating the source recovering.
        confirm_lsn_error: Arc<Mutex<Option<String>>>,
        /// When `Some`, replay this batch on every `next_events` call until
        /// `confirm_lsn` succeeds — simulates a Postgres replication slot that
        /// is stuck at a fixed position because `pg_replication_slot_advance`
        /// was never called (BUG-5 scenario).
        replay_batch: Option<Vec<Event>>,
    }

    impl MockStreamHandle {
        fn new(
            batches: Vec<Vec<Event>>,
            confirmed_lsns: Arc<Mutex<Vec<u64>>>,
            confirm_lsn_error: Arc<Mutex<Option<String>>>,
        ) -> Self {
            Self {
                batches: batches.into_iter().collect(),
                confirmed_lsns,
                confirm_lsn_error,
                replay_batch: None,
            }
        }

        fn with_replay_batch(mut self, batch: Vec<Event>) -> Self {
            self.replay_batch = Some(batch);
            self
        }
    }

    #[async_trait::async_trait]
    impl crate::source::StreamHandle for MockStreamHandle {
        async fn next_events(&mut self, _timeout_ms: u64) -> crate::core::Result<Vec<Event>> {
            // If replay mode is active, return the fixed batch on every call
            // regardless of whether the queue has been drained.  This mirrors
            // `pg_logical_slot_peek_binary_changes` semantics: the slot does not
            // advance until `pg_replication_slot_advance` is called (confirm_lsn).
            if let Some(batch) = &self.replay_batch {
                return Ok(batch.clone());
            }
            Ok(self.batches.pop_front().unwrap_or_default())
        }

        async fn save_position(
            &self,
            _checkpoint: &mut dyn crate::checkpoint::Checkpoint,
        ) -> crate::core::Result<()> {
            Ok(())
        }

        async fn confirm_lsn(&mut self, lsn: u64) -> crate::core::Result<()> {
            let configured_error = self
                .confirm_lsn_error
                .lock()
                .map(|guard| guard.clone())
                .unwrap_or_default();
            if let Some(message) = configured_error {
                return Err(crate::core::Error::SourceError(message));
            }
            // Successful confirmation clears replay mode — the slot has advanced.
            self.replay_batch = None;
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
        confirm_lsn_error: Arc<Mutex<Option<String>>>,
        snapshot_checkpoint_error: Option<String>,
        snapshot_checkpoint_payload: Option<Vec<u8>>,
        snapshot_checkpoint_source_type: String,
        /// When set, `start_stream` produces a handle that replays this batch on
        /// every `next_events` call until `confirm_lsn` succeeds.  Used to
        /// simulate a stuck Postgres replication slot (BUG-5).
        replay_batch: Option<Vec<Event>>,
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
                confirm_lsn_error: Arc::new(Mutex::new(None)),
                snapshot_checkpoint_error: None,
                snapshot_checkpoint_payload: None,
                snapshot_checkpoint_source_type: "mock_snapshot".to_string(),
                replay_batch: None,
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
                confirm_lsn_error: Arc::new(Mutex::new(None)),
                snapshot_checkpoint_error: None,
                snapshot_checkpoint_payload: None,
                snapshot_checkpoint_source_type: "mock_snapshot".to_string(),
                replay_batch: None,
            }
        }

        #[cfg(feature = "postgres")]
        fn with_confirm_lsn_error(self, message: impl Into<String>) -> Self {
            *self.confirm_lsn_error.lock().unwrap() = Some(message.into());
            self
        }

        /// Handle for clearing the simulated failure mid-run.
        #[cfg(feature = "postgres")]
        fn confirm_lsn_error_handle(&self) -> Arc<Mutex<Option<String>>> {
            Arc::clone(&self.confirm_lsn_error)
        }

        /// Configure the mock stream to replay a fixed batch on every `next_events`
        /// call until `confirm_lsn` succeeds.  Simulates a Postgres replication slot
        /// that is stuck at a fixed WAL position because the advance query failed
        /// (BUG-5 scenario).
        #[cfg(feature = "postgres")]
        fn with_replay_stream(mut self, batch: Vec<Event>) -> Self {
            // Set the replay_batch field; start_stream wires this into
            // MockStreamHandle::with_replay_batch so that every next_events
            // call returns the same batch until confirm_lsn succeeds.
            self.replay_batch = Some(batch);
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

            let mut handle = MockStreamHandle::new(
                self.stream_batches.clone(),
                Arc::clone(&self.confirmed_lsns),
                Arc::clone(&self.confirm_lsn_error),
            );
            if let Some(batch) = &self.replay_batch {
                handle = handle.with_replay_batch(batch.clone());
            }
            Ok(Box::new(handle))
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
        runtime.add_transform(Box::new(
            MaskHashTransform::new(MaskHashConfig {
                mask_rules: rules,
                default_rule: MaskRule::UnsaltedSha256,
            })
            .unwrap(),
        ));

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
        // Release the inspection handle before the "restarted" runtime opens its own:
        // one instance per checkpoint directory is the enforced contract.
        drop(checkpoint);

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

        assert!(matches!(
            error,
            crate::core::Error::PostCommitConfirmFailed {
                checkpoint_safe: true,
                ..
            }
        ));

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
        runtime.source = RuntimeSource::Custom(Box::new(
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

    /// Regression test for BUG-5 (cdc-server report): when `confirm_lsn` fails after a durable
    /// commit under the default `FailFast` policy, the slot is never advanced.  On the next poll,
    /// the source replays the same events.  With the runtime idempotency guard active, all replayed
    /// events are deduplicated → `EventBatch::empty()` → the caller's `commit_ack` is a no-op →
    /// the slot stays unadvanced forever.
    ///
    /// This test verifies that the runtime correctly surfaces `PostCommitConfirmFailed` on the
    /// first attempt so the caller has a chance to handle it (e.g. retry or reconnect), and that
    /// a second call to `poll_event_batch` after the error returns the same events again (replay)
    /// when the idempotency guard is disabled — proving the source is still live and not silently
    /// stuck.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn bug5_confirm_lsn_failure_surfaces_error_for_caller_to_handle() {
        let mut evt = event();
        evt.source.source_name = "postgres".into();
        evt.source.offset = "16/001A0000".into();

        // Use replay mode: same batch returned on every poll until confirm_lsn succeeds.
        // Idempotency guard disabled so the test can inspect raw replay behaviour.
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history)
            .with_idempotency_disabled();
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.source = RuntimeSource::Custom(Box::new(
            MockSource::stream_only(vec![])
                .with_replay_stream(vec![evt.clone()])
                .with_confirm_lsn_error("simulated slot advance failure"),
        ));

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

        // First poll: events delivered.
        let batch1 = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch1.len(), 1, "first poll should deliver the event");

        // Commit fails because confirm_lsn fails; slot not advanced.
        let err = runtime
            .commit_ack(batch1.ack_mode())
            .await
            .expect_err("FailFast policy must surface PostCommitConfirmFailed");
        assert!(
            matches!(
                err,
                crate::core::Error::PostCommitConfirmFailed {
                    checkpoint_safe: true,
                    ..
                }
            ),
            "expected PostCommitConfirmFailed, got {err:?}"
        );

        // The checkpoint WAS durably committed (checkpoint_safe = true).
        assert_eq!(
            runtime
                .config
                .checkpoint
                .get_committed_count()
                .await
                .unwrap(),
            1,
            "checkpoint must be durable even though confirm_lsn failed"
        );

        // Second poll: slot still at old position → same event replayed.
        // Without idempotency guard this is visible as a non-empty batch.
        let batch2 = runtime.poll_event_batch().await.unwrap();
        assert_eq!(
            batch2.len(),
            1,
            "replayed batch must be visible when idempotency guard is disabled"
        );
    }

    /// A `confirm_lsn` failure must never become a *silent* stall.
    ///
    /// Previously: the source kept replaying already-committed events, the idempotency
    /// guard suppressed all of them (they were fingerprinted on the first pass), and
    /// every poll returned `EventBatch::empty()` while liveness and readiness both
    /// reported healthy — an unbounded no-progress loop that accumulated WAL on the
    /// PostgreSQL primary with no error, no metric, and no log above `debug`.
    ///
    /// Now the runtime retains the unconfirmed LSN, retries it before dedup runs on
    /// each poll, reports `readiness: false` while it is outstanding, and escalates to
    /// a hard `Unrecoverable` error once the no-progress loop is demonstrated.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn confirm_lsn_failure_escalates_instead_of_stalling_silently() {
        let mut evt = event();
        evt.source.source_name = "postgres".into();
        evt.source.offset = "16/001A0000".into();

        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        // Default options: FailFast + idempotency guard enabled.
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.source = RuntimeSource::Custom(Box::new(
            MockSource::stream_only(vec![])
                .with_replay_stream(vec![evt.clone()])
                .with_confirm_lsn_error("simulated slot advance failure"),
        ));

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

        // First poll: events delivered (not yet seen by idempotency guard).
        let batch1 = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch1.len(), 1);

        // Commit fails: confirm_lsn failure → PostCommitConfirmFailed, slot not advanced.
        let err = runtime.commit_ack(batch1.ack_mode()).await.unwrap_err();
        assert!(matches!(
            err,
            crate::core::Error::PostCommitConfirmFailed { .. }
        ));

        // The unconfirmed position must be retained and surfaced as not-ready. This is
        // the signal that was entirely missing before: an operator polling health saw
        // `readiness: true` throughout the stall.
        assert!(
            !runtime.admin_snapshot().readiness,
            "a durably committed but unconfirmed source position must report not-ready"
        );

        // Subsequent polls replay the same events and the guard suppresses them. That
        // is tolerated briefly (the confirmation is retried each poll), but it must not
        // be tolerated indefinitely.
        let mut escalated = None;
        for _ in 0..super::UNCONFIRMED_STALL_POLL_LIMIT + 1 {
            match runtime.poll_event_batch().await {
                Ok(batch) => assert!(
                    batch.is_empty(),
                    "replayed events are suppressed while the position is unconfirmed"
                ),
                Err(error) => {
                    escalated = Some(error);
                    break;
                }
            }
        }

        let error = escalated.expect(
            "a persistent no-progress loop must escalate to a hard error, not idle forever",
        );
        assert!(
            matches!(error, crate::core::Error::Unrecoverable(_)),
            "expected Unrecoverable, got {error:?}"
        );
        let message = error.to_string();
        assert!(
            message.contains("not making progress"),
            "the error must name the failure mode: {message}"
        );
        assert!(
            message.contains("Operator action required"),
            "the error must tell the operator what to do: {message}"
        );
    }

    /// Connector-emitted schema-change events must land in the durable schema history.
    ///
    /// Previously `record_ddl` had exactly one caller (`capture_ddl_statement`), which
    /// itself had no non-test callers — so the schema history was never populated in
    /// any production path, while `start()` nonetheless refused to run without a
    /// retention policy for it. The connectors synthesize `Operation::SchemaChange`
    /// events directly, so the runtime now records them where every connector's events
    /// converge.
    #[tokio::test]
    async fn connector_schema_change_events_populate_the_schema_history() {
        use crate::core::Operation;

        let mut ddl_event = event();
        ddl_event.op = Operation::SchemaChange;
        ddl_event.table = "users".into();
        ddl_event.before = None;
        // Shape matches what the connectors actually emit: the synthesized
        // schema-change payload carries `result_schema` alongside the statement.
        ddl_event.after = Some(serde_json::json!({
            "ddl_type": "CREATE_TABLE",
            "schema": "public",
            "table": "users",
            "statement": "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)",
            "result_schema": {
                "schema": "public",
                "table": "users",
                "columns": [
                    {"name": "id", "data_type": "BIGINT", "nullable": false,
                     "constraints": ["PRIMARY KEY"]},
                    {"name": "email", "data_type": "TEXT", "nullable": true,
                     "constraints": []},
                ],
                "primary_keys": ["id"],
                "version": 0,
            },
        }));

        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.start().await.unwrap();

        assert!(
            runtime
                .config
                .schema_history
                .latest_schema("public.users")
                .await
                .unwrap()
                .is_none(),
            "history must start empty"
        );

        runtime.enqueue_event(ddl_event).unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        assert_eq!(
            batch.len(),
            1,
            "the schema-change event still reaches the consumer"
        );

        let recorded = runtime
            .config
            .schema_history
            .latest_schema("public.users")
            .await
            .unwrap();
        assert!(
            recorded.is_some(),
            "a connector-emitted schema change must be recorded in the schema history"
        );
    }

    /// `max_event_bytes` must actually bound the batch.
    ///
    /// It was previously declared, defaulted, settable and documented as a flush limit
    /// — and never read anywhere. An operator setting it to protect a downstream with
    /// a hard message-size limit got no protection and no warning.
    #[tokio::test]
    async fn max_event_bytes_bounds_the_delivered_batch() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let mut config =
            RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        // Small enough that only a couple of events fit per batch.
        config.options.max_event_bytes = Some(600);
        config.options.max_buffer_size = 100;

        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.start().await.unwrap();

        for index in 0..20 {
            let mut event = event();
            event.source.offset = format!("offset-{index}");
            event.after = Some(serde_json::json!({ "id": index, "blob": "x".repeat(200) }));
            runtime.enqueue_event(event).unwrap();
        }

        let batch = runtime.poll_event_batch().await.unwrap();
        assert!(
            !batch.is_empty(),
            "the byte budget must not stall delivery entirely"
        );
        assert!(
            batch.len() < 20,
            "the byte budget must cut the batch below the event-count limit, got {}",
            batch.len()
        );
    }

    /// A single event larger than the whole budget must still be delivered.
    ///
    /// Refusing would stall the pipeline permanently on one oversized row, with no way
    /// for the caller to make progress.
    #[tokio::test]
    async fn max_event_bytes_still_delivers_a_single_oversized_event() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let mut config =
            RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        config.options.max_event_bytes = Some(16);

        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.start().await.unwrap();

        let mut oversized = event();
        oversized.after = Some(serde_json::json!({ "blob": "x".repeat(10_000) }));
        runtime.enqueue_event(oversized).unwrap();

        let batch = runtime.poll_event_batch().await.unwrap();
        assert_eq!(
            batch.len(),
            1,
            "an event larger than the entire budget must still be delivered"
        );
    }

    /// Backpressure must be its own `ErrorKind`, not `Terminal`.
    ///
    /// It previously surfaced as `StateError` → `ErrorKind::Terminal`, documented as
    /// "a permanent problem that retrying will not resolve" — so an embedder following
    /// the crate's own retry guidance would shut the pipeline down on routine flow
    /// control.
    #[tokio::test]
    async fn buffer_full_reports_backpressure_not_terminal() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let mut config =
            RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        config.options.max_buffer_size = 2;

        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.start().await.unwrap();

        // Fill the commit barrier without acknowledging, then keep enqueuing until the
        // runtime pushes back. Both the enqueue guard and the commit-barrier guard are
        // flow control and must report the same kind.
        let error = loop {
            if let Err(error) = runtime.enqueue_event(event()) {
                break error;
            }
            match runtime.poll_event_batch().await {
                Ok(batch) if batch.is_empty() => {
                    panic!("runtime yielded nothing and never pushed back")
                }
                Ok(_) => continue,
                Err(error) => break error,
            }
        };
        assert_eq!(
            error.kind(),
            crate::core::ErrorKind::Backpressure,
            "backpressure must not be classified Terminal: {error:?}"
        );
        assert!(
            !error.is_recoverable(),
            "backpressure is not a transient source condition either"
        );
        let message = error.to_string();
        assert!(
            message.contains("acknowledge"),
            "the error must tell the caller how to clear it: {message}"
        );
    }

    /// Redelivery after a partial ack must expose exactly the uncommitted suffix.
    ///
    /// `EventBatch` now shares the delivery's buffer and carries an offset instead of
    /// deep-cloning the suffix on every re-poll. That makes redelivery allocation-free,
    /// but it also means `events()`, `len()` and `into_events()` must all respect the
    /// offset — getting any of them wrong would re-deliver already-committed events.
    #[tokio::test]
    async fn redelivered_batch_view_excludes_committed_prefix() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.start().await.unwrap();

        for index in 0..5 {
            let mut event = event();
            event.source.offset = format!("offset-{index}");
            runtime.enqueue_event(event).unwrap();
        }

        let batch = runtime.poll_event_batch().await.unwrap();
        assert_eq!(batch.len(), 5);

        // Commit only the first two.
        let AckMode::Required(token) = batch.ack_mode() else {
            panic!("expected an ack token")
        };
        let (first_two, _rest) = token.split_at(2).unwrap();
        runtime.commit_ack(first_two).await.unwrap();

        let redelivered = runtime.poll_event_batch().await.unwrap();
        assert_eq!(
            redelivered.len(),
            3,
            "only the uncommitted suffix redelivers"
        );
        assert_eq!(redelivered.events().len(), 3, "events() honours the offset");
        assert_eq!(
            redelivered.events()[0].source.offset,
            "offset-2",
            "redelivery must resume after the committed prefix"
        );

        let owned = redelivered.into_events();
        assert_eq!(owned.len(), 3, "into_events() honours the offset");
        assert_eq!(owned[0].source.offset, "offset-2");
        assert_eq!(owned[2].source.offset, "offset-4");
    }

    /// The health verdict must separate "quiet source" from "stuck pipeline".
    ///
    /// `RuntimeState` cannot: a connector streaming from an idle database and one hung
    /// on a dead socket both report `state = running` with flat counters. An operator
    /// seeing "no events for 10 minutes" could not tell which, so the signal was
    /// unusable for alerting.
    #[tokio::test]
    async fn health_verdict_distinguishes_idle_from_stalled() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();

        // Not started.
        assert_eq!(runtime.admin_snapshot().health, HealthVerdict::NotRunning);
        assert!(!runtime.admin_snapshot().health.is_alertable());

        runtime.start().await.unwrap();

        // Running, nothing to do: idle, and deliberately NOT alertable. Paging on a
        // quiet database is what trains operators to ignore the alert.
        let idle = runtime.admin_snapshot().health;
        assert_eq!(idle, HealthVerdict::Idle);
        assert!(
            !idle.is_alertable(),
            "a quiet source must not page anyone: {idle:?}"
        );

        // Events flowing and committed: healthy.
        runtime.enqueue_event(event()).unwrap();
        let batch = runtime.poll_event_batch().await.unwrap();
        runtime.commit_ack(batch.ack_mode()).await.unwrap();
        assert_eq!(runtime.admin_snapshot().health, HealthVerdict::Healthy);

        // Stopped.
        runtime.stop().await.unwrap();
        assert_eq!(runtime.admin_snapshot().health, HealthVerdict::NotRunning);
    }

    /// A consumer that stops acknowledging is a stall, and must say so.
    ///
    /// This is the case that looks identical to source idleness in the raw counters:
    /// the source is fine, the poll loop is fine, and nothing is committing.
    #[tokio::test]
    async fn health_verdict_reports_a_consumer_that_stopped_acknowledging() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let mut config =
            RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        config.options.max_poll_wait_ms = 1;

        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.start().await.unwrap();

        runtime.enqueue_event(event()).unwrap();
        let _delivered = runtime.poll_event_batch().await.unwrap();
        // Deliberately no commit_ack.

        // Backdate the last commit past the stall threshold to simulate elapsed time
        // without sleeping.
        runtime.last_commit_at_ms = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or_default()
                .saturating_sub(10 * 60 * 1000),
        );

        let health = runtime.admin_snapshot().health;
        match &health {
            HealthVerdict::Stalled { reason } => {
                assert!(
                    reason.contains("commit_ack"),
                    "the reason must name the remedy: {reason}"
                );
                assert!(
                    reason.contains("not committed"),
                    "the reason must name the condition: {reason}"
                );
            }
            other => panic!("expected a stall verdict, got {other:?}"),
        }
        assert!(health.is_alertable());
    }

    /// An unconfirmed committed position must dominate the verdict.
    ///
    /// It is the most severe condition — the source retains its log (WAL on a
    /// PostgreSQL primary) until it clears — so it must be reported even when other
    /// signals look fine.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn health_verdict_reports_an_unconfirmed_source_position() {
        let mut evt = event();
        evt.source.source_name = "postgres".into();
        evt.source.offset = "16/001A0000".into();

        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.source = RuntimeSource::Custom(Box::new(
            MockSource::stream_only(vec![])
                .with_replay_stream(vec![evt])
                .with_confirm_lsn_error("simulated slot advance failure"),
        ));
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
        let _ = runtime.commit_ack(batch.ack_mode()).await;

        let health = runtime.admin_snapshot().health;
        match &health {
            HealthVerdict::Stalled { reason } => assert!(
                reason.contains("could not be confirmed"),
                "the reason must name the unconfirmed position: {reason}"
            ),
            other => panic!("expected a stall verdict, got {other:?}"),
        }
    }

    /// The Prometheus surface must expose exactly one active health verdict.
    #[tokio::test]
    async fn prometheus_output_exposes_the_health_verdict() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.start().await.unwrap();

        let text = runtime.admin_metrics_prometheus();
        let active = text
            .lines()
            .filter(|line| line.starts_with("rustcdc_runtime_health{") && line.ends_with(" 1"))
            .count();
        assert_eq!(
            active, 1,
            "exactly one verdict must be active so an alert rule is unambiguous:\n{text}"
        );
        assert!(
            text.contains("rustcdc_runtime_events_skipped_total"),
            "the skipped-event counter must be exposed: any non-zero value means data loss"
        );
    }

    /// Startup must not refuse to run merely because retention is unconfigured.
    #[tokio::test]
    async fn start_succeeds_without_a_schema_history_retention_policy() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut config = config;
        config.options.schema_history_retention = None;
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime
            .start()
            .await
            .expect("missing retention must warn, not block startup");
    }

    /// The happy path of the same mechanism: once the source accepts the confirmation,
    /// the runtime clears the pending position and returns to ready.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn recovered_confirm_lsn_clears_the_pending_position_and_restores_readiness() {
        let mut evt = event();
        evt.source.source_name = "postgres".into();
        evt.source.offset = "16/001A0000".into();

        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
        let mut runtime = CdcRuntime::new(config).unwrap();
        let source = MockSource::stream_only(vec![])
            .with_replay_stream(vec![evt.clone()])
            .with_confirm_lsn_error("simulated slot advance failure");
        let failure_handle = source.confirm_lsn_error_handle();
        runtime.source = RuntimeSource::Custom(Box::new(source));

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
        let _ = runtime.commit_ack(batch.ack_mode()).await;
        assert!(runtime.pending_confirmation_lsn.is_some());
        assert!(!runtime.admin_snapshot().readiness);

        // Source recovers: the next poll's retry succeeds.
        *failure_handle.lock().unwrap() = None;
        let _replayed = runtime.poll_event_batch().await.unwrap();

        assert!(
            runtime.pending_confirmation_lsn.is_none(),
            "a successful retry must clear the pending position"
        );
        assert!(
            runtime.admin_snapshot().readiness,
            "readiness must recover once the position is confirmed"
        );
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
        assert!(prometheus.contains("rustcdc_runtime_checkpoint_age_ms"));
        assert!(prometheus.contains("rustcdc_runtime_replication_lag_ms"));
    }

    /// A masking rule that never fires must reach the metrics endpoint, not just a log.
    ///
    /// A rule with zero hits means a column is shipping in clear text. Before this, the
    /// only way to see it was to call an accessor at shutdown — something an operator has
    /// to go looking for rather than something that pages them.
    #[tokio::test]
    async fn unmatched_transform_rules_reach_the_admin_surface() {
        use crate::transform::{MaskHashConfig, MaskHashTransform, MaskRule};

        let mut rules: ahash::AHashMap<String, MaskRule> = ahash::AHashMap::new();
        rules.insert("payload".into(), MaskRule::Redact("***".into()));
        rules.insert("paylaod".into(), MaskRule::Redact("***".into())); // typo

        let config = RuntimeConfig::new(
            RuntimeSourceConfig::Disabled,
            InMemoryCheckpoint::default(),
            InMemorySchemaHistory::default(),
        );
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.add_transform(Box::new(
            MaskHashTransform::new(MaskHashConfig {
                mask_rules: rules,
                default_rule: MaskRule::Passthrough,
            })
            .unwrap(),
        ));
        runtime.start().await.unwrap();

        // Before traffic every rule is unmatched, which says nothing — but it must not be
        // reported as if it did, or the metric fires on every cold start.
        let mut event = event();
        event.after = Some(serde_json::json!({"payload": "secret"}));
        runtime.enqueue_event(event).unwrap();
        let _ = runtime.poll_event_batch().await.unwrap();

        let admin = runtime.admin_snapshot();
        let reported: Vec<&str> = admin
            .unmatched_transform_rules
            .iter()
            .map(|rule| rule.rule.as_str())
            .collect();
        assert_eq!(
            reported,
            vec!["paylaod"],
            "only the rule that never matched must be reported"
        );

        let prometheus = runtime.admin_metrics_prometheus();
        assert!(
            prometheus.contains(
                "rustcdc_transform_rules_unmatched{source_type=\"unknown\",\
                 transform=\"mask_hash\",kind=\"mask\",rule=\"paylaod\"} 1"
            ),
            "the unmatched rule must be a labelled series an alert can match on:\n{prometheus}"
        );
    }

    /// A rule identifier containing a quote must not break the whole scrape.
    #[tokio::test]
    async fn prometheus_label_values_are_escaped() {
        use crate::transform::{
            FilterField, FilterOperator, FilterProjectionConfig, FilterProjectionTransform,
            FilterRule,
        };

        // `FilterRule::describe` renders the value with `{:?}`, so the label value
        // contains double quotes. Unescaped, they terminate the label early and the
        // exposition body becomes unparseable — taking every metric on the endpoint down,
        // not just this one.
        let config = RuntimeConfig::new(
            RuntimeSourceConfig::Disabled,
            InMemoryCheckpoint::default(),
            InMemorySchemaHistory::default(),
        );
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.add_transform(Box::new(
            FilterProjectionTransform::new(FilterProjectionConfig {
                filters: vec![FilterRule::new(
                    FilterField::Table,
                    FilterOperator::Eq,
                    "never\\matches",
                )],
                ..FilterProjectionConfig::default()
            })
            .unwrap(),
        ));
        runtime.start().await.unwrap();
        runtime.enqueue_event(event()).unwrap();
        let _ = runtime.poll_event_batch().await.unwrap();

        let prometheus = runtime.admin_metrics_prometheus();
        let line = prometheus
            .lines()
            .find(|line| line.starts_with("rustcdc_transform_rules_unmatched{"))
            .expect("the unmatched filter rule must be reported");
        assert!(
            line.ends_with("} 1"),
            "the series must be well-formed: {line}"
        );
        assert_eq!(
            line,
            concat!(
                r#"rustcdc_transform_rules_unmatched{source_type="unknown","#,
                r#"transform="filter_projection",kind="filter","#,
                // `describe` renders the value with `{:?}`, so the rule text is
                // `table eq "never\\matches"`. Prometheus escaping then doubles each
                // backslash and backslash-escapes each quote.
                r#"rule="table eq \"never\\\\matches\""} 1"#
            ),
            "quotes and backslashes inside a rule must be escaped so the exposition body \
             stays parseable"
        );
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

    // ── EventBatch accessor tests ─────────────────────────────────────────────

    fn make_batch_events() -> Arc<Vec<Event>> {
        use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
        use serde_json::json;
        Arc::new(vec![
            Event {
                table: "orders".into(),
                schema: Some("public".into()),
                op: Operation::Insert,
                after: Some(json!({"id": 1})),
                ts: 1,
                source: SourceMetadata {
                    source_name: "pg".into(),
                    offset: "1".into(),
                    timestamp: 1,
                },
                envelope_version: EVENT_ENVELOPE_VERSION,
                ..Event::default()
            },
            Event {
                table: "orders".into(),
                schema: Some("public".into()),
                op: Operation::Update,
                before: Some(json!({"id": 2})),
                after: Some(json!({"id": 2, "name": "bob"})),
                ts: 2,
                source: SourceMetadata {
                    source_name: "pg".into(),
                    offset: "2".into(),
                    timestamp: 2,
                },
                envelope_version: EVENT_ENVELOPE_VERSION,
                ..Event::default()
            },
            Event {
                table: "users".into(),
                schema: Some("auth".into()),
                op: Operation::Insert,
                after: Some(json!({"id": 10})),
                ts: 3,
                source: SourceMetadata {
                    source_name: "pg".into(),
                    offset: "3".into(),
                    timestamp: 3,
                },
                envelope_version: EVENT_ENVELOPE_VERSION,
                ..Event::default()
            },
        ])
    }

    #[test]
    fn event_batch_tables_returns_sorted_deduplicated_names() {
        let batch = EventBatch {
            events: make_batch_events(),
            offset: 0,
            ack_token: None,
        };
        let tables = batch.tables();
        assert_eq!(tables, vec!["orders", "users"]);
    }

    #[test]
    fn event_batch_qualified_tables_includes_schema() {
        let batch = EventBatch {
            events: make_batch_events(),
            offset: 0,
            ack_token: None,
        };
        let tables = batch.qualified_tables();
        assert_eq!(tables, vec!["auth.users", "public.orders"]);
    }

    #[test]
    fn event_batch_event_count_for_table() {
        let batch = EventBatch {
            events: make_batch_events(),
            offset: 0,
            ack_token: None,
        };
        assert_eq!(batch.event_count_for_table("orders"), 2);
        assert_eq!(batch.event_count_for_table("users"), 1);
        assert_eq!(batch.event_count_for_table("nonexistent"), 0);
    }

    #[test]
    fn event_batch_iter_and_into_iter_yield_same_events() {
        let events = make_batch_events();
        let batch = EventBatch {
            events: events.clone(),
            offset: 0,
            ack_token: None,
        };
        let via_iter: Vec<&Event> = batch.iter().collect();
        let via_into_iter: Vec<Event> = EventBatch {
            events,
            offset: 0,
            ack_token: None,
        }
        .into_iter()
        .collect();
        assert_eq!(via_iter.len(), via_into_iter.len());
        for (borrowed, owned) in via_iter.iter().zip(via_into_iter.iter()) {
            assert_eq!(*borrowed, owned);
        }
    }
}
