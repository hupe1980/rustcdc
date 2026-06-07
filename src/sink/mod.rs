//! Sink adapter trait and built-in implementations.
//!
//! The [`SinkAdapter`] trait is the primary integration point for embedders that
//! want to connect the CDC runtime output to a downstream system (Kafka, database,
//! HTTP endpoint, etc.).  Implement [`SinkAdapter`] on your own type and pass it to
//! the runtime's event processing loop.
//!
//! # Built-in adapters
//!
//! | Adapter | Notes |
//! |---|---|
//! | [`MemorySinkAdapter`] | In-memory; for tests and rapid prototyping |
//! | [`StdoutSink`] | Writes NDJSON to stdout; for local debugging and Docker deployments |
//! | [`FileJsonlSink`] | Appends NDJSON to a file with async I/O, batching, and rotation |
//! | [`FanOutSinkAdapter`] | Delivers each event to every child sink **concurrently** |
//! | [`BoxedSink`] | Type-erased heap-allocated adapter for dynamic dispatch |
//!
//! # Delivery contract
//!
//! Every `SinkAdapter` implementation advertises its delivery guarantee via
//! [`SinkAdapter::delivery_guarantee`].  Adapters that support transactional
//! checkpoint barriers should also override
//! [`transactional_checkpoint_barrier_capable`](SinkAdapter::transactional_checkpoint_barrier_capable)
//! and the three barrier methods.
//!
//! # Dynamic dispatch
//!
//! `SinkAdapter` is not object-safe (it uses RPITIT).  Use [`BoxedSink`] for
//! `Box<dyn …>` style dispatch and [`FanOutSinkAdapter`] to fan out to a
//! heterogeneous set of sinks.
//!
//! # Conformance testing
//!
//! [`AdapterConformanceSuite`] verifies that a custom [`SinkAdapter`] implementation
//! honours the contract (ordering, flush semantics, post-close error behaviour).

pub mod fan_out;
pub mod file_jsonl;
pub mod stdout;

pub use fan_out::FanOutSinkAdapter;
pub use file_jsonl::{FileJsonlSink, FileJsonlSinkConfig};
pub use stdout::StdoutSink;

use crate::core::{Error, Event, Result};

// ─── SinkDeliveryGuarantee ────────────────────────────────────────────────────

/// The delivery guarantee an adapter can offer to the pipeline.
///
/// When multiple sinks are composed (e.g. [`FanOutSinkAdapter`], a router),
/// the effective guarantee of the composition is the **weakest** of all members.
///
/// # Ordering
///
/// `AtLeastOnce` < `AtLeastOnceIdempotent` < `EffectivelyOnce`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SinkDeliveryGuarantee {
    /// Events are delivered at least once; duplicate delivery is possible.
    ///
    /// This is the default for most adapters.  Downstream consumers must
    /// deduplicate if idempotency is required.
    #[default]
    AtLeastOnce,

    /// Events are delivered at least once but the adapter guarantees that
    /// retrying the same event is idempotent (e.g. upsert-by-key semantics).
    ///
    /// Suitable for sinks that de-duplicate by event key or version.
    AtLeastOnceIdempotent,

    /// Events are delivered exactly once when paired with transactional
    /// checkpoint barriers (see [`SinkAdapter::transactional_checkpoint_barrier_capable`]).
    ///
    /// Requires the adapter to implement the full barrier protocol.
    EffectivelyOnce,
}

impl SinkDeliveryGuarantee {
    /// Numeric strength: higher is stronger.
    fn strength(self) -> u8 {
        match self {
            Self::AtLeastOnce => 0,
            Self::AtLeastOnceIdempotent => 1,
            Self::EffectivelyOnce => 2,
        }
    }

    /// Return the weaker of `self` and `other`.
    ///
    /// Used when composing sinks — the composed delivery contract is only as
    /// strong as the weakest participant.
    pub fn weakest(self, other: Self) -> Self {
        if self.strength() <= other.strength() {
            self
        } else {
            other
        }
    }

    /// Human-readable label for use in metrics and logs.
    pub fn as_label(self) -> &'static str {
        match self {
            Self::AtLeastOnce => "at_least_once",
            Self::AtLeastOnceIdempotent => "at_least_once_idempotent",
            Self::EffectivelyOnce => "effectively_once",
        }
    }
}

// ─── SinkDeliveryMetrics ──────────────────────────────────────────────────────

/// A snapshot of delivery counters exposed by a [`SinkAdapter`] for observability.
///
/// Returned by [`SinkAdapter::delivery_metrics`].  All counters accumulate
/// monotonically — they are never reset during the adapter's lifetime.
///
/// For composed adapters ([`FanOutSinkAdapter`], [`TableRouter`](crate::pipeline::TableRouter))
/// the values are the sum of all child counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SinkDeliveryMetrics {
    /// Total events successfully delivered since the adapter was created.
    pub events_sent: u64,
    /// Total events that resulted in an unrecovered error.
    pub events_errored: u64,
    /// Total retry attempts (across all events, including retries that ultimately succeeded).
    pub events_retried: u64,
    /// The offset string of the last successfully delivered event, if tracked by this adapter.
    pub last_delivered_offset: Option<String>,
}

impl SinkDeliveryMetrics {
    /// Merge `other` into `self` by summing counters.
    ///
    /// `last_delivered_offset` is not merged — it is only meaningful per leaf sink.
    pub fn merge(&mut self, other: &Self) {
        self.events_sent = self.events_sent.saturating_add(other.events_sent);
        self.events_errored = self.events_errored.saturating_add(other.events_errored);
        self.events_retried = self.events_retried.saturating_add(other.events_retried);
    }
}

// ─── SinkAdapter ─────────────────────────────────────────────────────────────

/// Trait for sending CDC events to a downstream system.
///
/// Implementations must be `Send` so they can be used across async task boundaries.
/// All methods take `&mut self` so the adapter can maintain internal state (e.g. a
/// connection handle or an in-flight buffer) without an inner `Mutex`.
///
/// # Required methods
///
/// Implement at minimum: [`send`], [`flush`], [`close`], [`name`].
///
/// # Optional capability declarations
///
/// Override [`delivery_guarantee`], [`idempotent_delivery_capable`],
/// [`transactional_checkpoint_barrier_capable`], [`queue_depth`],
/// [`flush_tick_interval`], and [`preflight_check`] to advertise capabilities
/// and hints to the runtime.
///
/// # Checkpoint barrier protocol
///
/// Adapters that support effectively-once delivery must override
/// [`transactional_checkpoint_barrier_capable`] → `true` and implement all
/// three barrier methods.  The protocol is:
///
/// 1. [`begin_checkpoint_barrier`] — prepare for a checkpoint commit.
/// 2. [`commit_checkpoint_barrier`] — durable commit.
/// 3. [`abort_checkpoint_barrier`] — roll back on failure.
///
/// # Object-safe wrapper
///
/// This trait is **not** object-safe (uses RPITIT).  Use [`BoxedSink`] when you
/// need `Box<dyn …>` dispatch.
///
/// # Implementing SinkAdapter
///
/// ```rust,no_run
/// use rustcdc::{core::{Event, Result}, sink::SinkAdapter};
///
/// struct MyKafkaSink { /* ... */ }
///
/// impl SinkAdapter for MyKafkaSink {
///     async fn send(&mut self, event: &Event) -> Result<()> {
///         // Deliver event to Kafka
///         Ok(())
///     }
///     async fn flush(&mut self) -> Result<()> { Ok(()) }
///     async fn close(&mut self) -> Result<()> { Ok(()) }
///     fn name(&self) -> &str { "kafka" }
/// }
/// ```
///
/// [`send`]: SinkAdapter::send
/// [`flush`]: SinkAdapter::flush
/// [`close`]: SinkAdapter::close
/// [`name`]: SinkAdapter::name
/// [`delivery_guarantee`]: SinkAdapter::delivery_guarantee
/// [`idempotent_delivery_capable`]: SinkAdapter::idempotent_delivery_capable
/// [`transactional_checkpoint_barrier_capable`]: SinkAdapter::transactional_checkpoint_barrier_capable
/// [`queue_depth`]: SinkAdapter::queue_depth
/// [`flush_tick_interval`]: SinkAdapter::flush_tick_interval
/// [`preflight_check`]: SinkAdapter::preflight_check
/// [`begin_checkpoint_barrier`]: SinkAdapter::begin_checkpoint_barrier
/// [`commit_checkpoint_barrier`]: SinkAdapter::commit_checkpoint_barrier
/// [`abort_checkpoint_barrier`]: SinkAdapter::abort_checkpoint_barrier
pub trait SinkAdapter: Send {
    // ── Required ─────────────────────────────────────────────────────────────

    /// Deliver a single CDC event to the sink.
    fn send(&mut self, event: &Event) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Flush any internal write buffer, making all previously `send`-ed events
    /// durable (or at least submitted to the downstream system).
    fn flush(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Perform an orderly close of the adapter.  Subsequent calls to [`send`] or
    /// [`flush`](SinkAdapter::flush) should return an error once the adapter is closed.
    ///
    /// [`send`]: SinkAdapter::send
    fn close(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Human-readable name used in logs and conformance reports.
    fn name(&self) -> &str;

    // ── Delivery contract ─────────────────────────────────────────────────────

    /// The delivery guarantee this adapter offers.
    ///
    /// Default: [`SinkDeliveryGuarantee::AtLeastOnce`].
    fn delivery_guarantee(&self) -> SinkDeliveryGuarantee {
        SinkDeliveryGuarantee::AtLeastOnce
    }

    /// Whether the adapter can deliver an event idempotently (e.g. upsert by key).
    ///
    /// Default: `false`.
    fn idempotent_delivery_capable(&self) -> bool {
        false
    }

    /// Whether the adapter supports the transactional checkpoint barrier protocol.
    ///
    /// Adapters that return `true` here **must** implement
    /// [`begin_checkpoint_barrier`](SinkAdapter::begin_checkpoint_barrier),
    /// [`commit_checkpoint_barrier`](SinkAdapter::commit_checkpoint_barrier), and
    /// [`abort_checkpoint_barrier`](SinkAdapter::abort_checkpoint_barrier).
    ///
    /// Default: `false`.
    fn transactional_checkpoint_barrier_capable(&self) -> bool {
        false
    }

    // ── Runtime hints ─────────────────────────────────────────────────────────

    /// Current number of events buffered in-memory and not yet flushed.
    ///
    /// Used by the runtime to observe back-pressure.  Return `None` when the
    /// adapter does not maintain an internal buffer.
    ///
    /// Default: `None`.
    fn queue_depth(&self) -> Option<usize> {
        None
    }

    /// Suggest how often the runtime should call [`flush`](SinkAdapter::flush)
    /// on a time basis (e.g. when the sink batches events by time window).
    ///
    /// The runtime will arrange for periodic flushing no less often than this
    /// interval.  Return `None` to leave flushing entirely to the runtime's
    /// checkpoint / batch schedule.
    ///
    /// Default: `None`.
    fn flush_tick_interval(&self) -> Option<std::time::Duration> {
        None
    }

    // ── Checkpoint barrier ────────────────────────────────────────────────────

    /// Begin a transactional checkpoint barrier.
    ///
    /// Called before the pipeline commits a checkpoint offset to durable
    /// storage.  The adapter should hold new sends buffered until
    /// [`commit_checkpoint_barrier`](SinkAdapter::commit_checkpoint_barrier) or
    /// [`abort_checkpoint_barrier`](SinkAdapter::abort_checkpoint_barrier) is called.
    ///
    /// Default: no-op `Ok(())`.
    fn begin_checkpoint_barrier(&mut self) -> impl std::future::Future<Output = Result<()>> + Send {
        std::future::ready(Ok(()))
    }

    /// Commit the in-flight checkpoint barrier.
    ///
    /// The adapter should atomically make buffered events durable and release
    /// the barrier.
    ///
    /// Default: no-op `Ok(())`.
    fn commit_checkpoint_barrier(
        &mut self,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        std::future::ready(Ok(()))
    }

    /// Abort the in-flight checkpoint barrier and discard buffered events.
    ///
    /// Called when a downstream failure makes the current checkpoint
    /// uncompletable.
    ///
    /// Default: no-op `Ok(())`.
    fn abort_checkpoint_barrier(&mut self) -> impl std::future::Future<Output = Result<()>> + Send {
        std::future::ready(Ok(()))
    }

    // ── Connectivity validation ────────────────────────────────────────────────

    /// Validate that the sink can accept events before the pipeline starts.
    ///
    /// The runtime calls this once during startup.  Use it to fail-fast on
    /// misconfigured endpoints, missing credentials, or unreachable services.
    ///
    /// Default: no-op `Ok(())`.
    fn preflight_check(&mut self) -> impl std::future::Future<Output = Result<()>> + Send {
        std::future::ready(Ok(()))
    }

    // ── Convenience wrappers ──────────────────────────────────────────────────

    /// Close the adapter, returning [`Error::TimeoutError`] if the close takes
    /// longer than `timeout_ms` milliseconds.
    ///
    /// Use this in shutdown paths where a hung sink must not prevent the process
    /// from exiting.  The adapter state is indeterminate after a timeout.
    fn close_with_timeout(
        &mut self,
        timeout_ms: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), self.close())
                .await
                .map_err(|_| {
                    Error::TimeoutError(format!(
                        "sink '{}' close exceeded timeout ({} ms)",
                        self.name(),
                        timeout_ms,
                    ))
                })?
        }
    }

    // ── Inspection hooks ──────────────────────────────────────────────────────

    /// Optional inspection hook for deterministic conformance assertions.
    ///
    /// Adapters that can safely expose a read-only in-memory view of all received
    /// events should return `Some`.  Opaque adapters (writing to an external system)
    /// may return `None`.
    fn exported_events(&self) -> Option<&[Event]> {
        None
    }

    /// Snapshot of delivery counters for observability.
    ///
    /// Override to expose per-sink metrics to admin endpoints or Prometheus scrapers.
    /// Returns `None` when the adapter does not track delivery counters.
    ///
    /// Default: `None`.
    fn delivery_metrics(&self) -> Option<SinkDeliveryMetrics> {
        None
    }

    /// Whether the adapter has been closed.
    ///
    /// Returns `true` after [`close`] has been called.  The default implementation
    /// always returns `false` — adapters that track closed state should override this.
    ///
    /// [`close`]: SinkAdapter::close
    fn is_closed(&self) -> bool {
        false
    }

    // ── Convenience ───────────────────────────────────────────────────────────

    /// Wrap this adapter in a type-erased [`BoxedSink`].
    ///
    /// Shorthand for [`BoxedSink::new(self)`](BoxedSink::new).
    fn boxed(self) -> BoxedSink
    where
        Self: Sized + 'static,
    {
        BoxedSink::new(self)
    }
}

// ─── ErasedSink (private) ─────────────────────────────────────────────────────
//
// Object-safe version of SinkAdapter used internally by BoxedSink.
// Not part of the public API.

type BoxFut<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>;

trait ErasedSink: Send {
    fn erased_send<'a>(&'a mut self, event: &'a Event) -> BoxFut<'a>;
    fn erased_flush<'a>(&'a mut self) -> BoxFut<'a>;
    fn erased_close<'a>(&'a mut self) -> BoxFut<'a>;
    fn erased_name(&self) -> &str;
    fn erased_delivery_guarantee(&self) -> SinkDeliveryGuarantee;
    fn erased_idempotent_delivery_capable(&self) -> bool;
    fn erased_transactional_checkpoint_barrier_capable(&self) -> bool;
    fn erased_queue_depth(&self) -> Option<usize>;
    fn erased_flush_tick_interval(&self) -> Option<std::time::Duration>;
    fn erased_delivery_metrics(&self) -> Option<SinkDeliveryMetrics>;
    fn erased_is_closed(&self) -> bool;
    fn erased_exported_events(&self) -> Option<&[Event]>;
    fn erased_begin_checkpoint_barrier<'a>(&'a mut self) -> BoxFut<'a>;
    fn erased_commit_checkpoint_barrier<'a>(&'a mut self) -> BoxFut<'a>;
    fn erased_abort_checkpoint_barrier<'a>(&'a mut self) -> BoxFut<'a>;
    fn erased_preflight_check<'a>(&'a mut self) -> BoxFut<'a>;
}

impl<T: SinkAdapter> ErasedSink for T {
    fn erased_send<'a>(&'a mut self, event: &'a Event) -> BoxFut<'a> {
        Box::pin(self.send(event))
    }
    fn erased_flush<'a>(&'a mut self) -> BoxFut<'a> {
        Box::pin(self.flush())
    }
    fn erased_close<'a>(&'a mut self) -> BoxFut<'a> {
        Box::pin(self.close())
    }
    fn erased_name(&self) -> &str {
        self.name()
    }
    fn erased_delivery_guarantee(&self) -> SinkDeliveryGuarantee {
        self.delivery_guarantee()
    }
    fn erased_idempotent_delivery_capable(&self) -> bool {
        self.idempotent_delivery_capable()
    }
    fn erased_transactional_checkpoint_barrier_capable(&self) -> bool {
        self.transactional_checkpoint_barrier_capable()
    }
    fn erased_queue_depth(&self) -> Option<usize> {
        self.queue_depth()
    }
    fn erased_flush_tick_interval(&self) -> Option<std::time::Duration> {
        self.flush_tick_interval()
    }
    fn erased_delivery_metrics(&self) -> Option<SinkDeliveryMetrics> {
        self.delivery_metrics()
    }
    fn erased_is_closed(&self) -> bool {
        self.is_closed()
    }
    fn erased_exported_events(&self) -> Option<&[Event]> {
        self.exported_events()
    }
    fn erased_begin_checkpoint_barrier<'a>(&'a mut self) -> BoxFut<'a> {
        Box::pin(self.begin_checkpoint_barrier())
    }
    fn erased_commit_checkpoint_barrier<'a>(&'a mut self) -> BoxFut<'a> {
        Box::pin(self.commit_checkpoint_barrier())
    }
    fn erased_abort_checkpoint_barrier<'a>(&'a mut self) -> BoxFut<'a> {
        Box::pin(self.abort_checkpoint_barrier())
    }
    fn erased_preflight_check<'a>(&'a mut self) -> BoxFut<'a> {
        Box::pin(self.preflight_check())
    }
}

// ─── BoxedSink ────────────────────────────────────────────────────────────────

/// A heap-allocated, type-erased [`SinkAdapter`] for dynamic dispatch.
///
/// Use this when the concrete adapter type is not known at compile time, or
/// when you need to store a heterogeneous collection of sinks (e.g. in a
/// [`FanOutSinkAdapter`] or a router).
///
/// Created with [`BoxedSink::new`].
///
/// ```rust,no_run
/// use rustcdc::sink::{BoxedSink, MemorySinkAdapter, SinkAdapter};
///
/// let inner = MemorySinkAdapter::new("mem");
/// let mut sink: BoxedSink = BoxedSink::new(inner);
/// // sink can now be stored in a Vec<BoxedSink>
/// ```
pub struct BoxedSink(Box<dyn ErasedSink>);

impl BoxedSink {
    /// Wrap `sink` in a type-erased [`BoxedSink`].
    ///
    /// The concrete type `S` must be `'static` so it can outlive any particular
    /// call site.
    pub fn new<S: SinkAdapter + 'static>(sink: S) -> Self {
        Self(Box::new(sink))
    }
}

impl std::fmt::Debug for BoxedSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxedSink")
            .field("name", &self.0.erased_name())
            .field("closed", &self.0.erased_is_closed())
            .finish_non_exhaustive()
    }
}

impl SinkAdapter for BoxedSink {
    async fn send(&mut self, event: &Event) -> Result<()> {
        self.0.erased_send(event).await
    }

    async fn flush(&mut self) -> Result<()> {
        self.0.erased_flush().await
    }

    async fn close(&mut self) -> Result<()> {
        self.0.erased_close().await
    }

    fn name(&self) -> &str {
        self.0.erased_name()
    }

    fn delivery_guarantee(&self) -> SinkDeliveryGuarantee {
        self.0.erased_delivery_guarantee()
    }

    fn idempotent_delivery_capable(&self) -> bool {
        self.0.erased_idempotent_delivery_capable()
    }

    fn transactional_checkpoint_barrier_capable(&self) -> bool {
        self.0.erased_transactional_checkpoint_barrier_capable()
    }

    fn queue_depth(&self) -> Option<usize> {
        self.0.erased_queue_depth()
    }

    fn flush_tick_interval(&self) -> Option<std::time::Duration> {
        self.0.erased_flush_tick_interval()
    }

    fn delivery_metrics(&self) -> Option<SinkDeliveryMetrics> {
        self.0.erased_delivery_metrics()
    }

    fn is_closed(&self) -> bool {
        self.0.erased_is_closed()
    }

    fn exported_events(&self) -> Option<&[Event]> {
        self.0.erased_exported_events()
    }

    async fn begin_checkpoint_barrier(&mut self) -> Result<()> {
        self.0.erased_begin_checkpoint_barrier().await
    }

    async fn commit_checkpoint_barrier(&mut self) -> Result<()> {
        self.0.erased_commit_checkpoint_barrier().await
    }

    async fn abort_checkpoint_barrier(&mut self) -> Result<()> {
        self.0.erased_abort_checkpoint_barrier().await
    }

    async fn preflight_check(&mut self) -> Result<()> {
        self.0.erased_preflight_check().await
    }
}

// ─── MemorySinkAdapter ────────────────────────────────────────────────────────

/// In-memory sink adapter for testing and rapid prototyping.
///
/// # Warning
///
/// **Not suitable for production use.** All events are kept in heap memory and lost
/// on process exit. For durable sinks, implement [`SinkAdapter`] against your
/// downstream system.
#[derive(Debug, Clone)]
pub struct MemorySinkAdapter {
    name: String,
    events: Vec<Event>,
    closed: bool,
}

impl Default for MemorySinkAdapter {
    fn default() -> Self {
        Self::new("memory")
    }
}

impl MemorySinkAdapter {
    /// Create a new adapter with the given logical name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            events: Vec::new(),
            closed: false,
        }
    }

    /// All events received so far, in arrival order.
    pub fn events(&self) -> &[Event] {
        &self.events
    }
}

impl SinkAdapter for MemorySinkAdapter {
    async fn send(&mut self, event: &Event) -> Result<()> {
        if self.closed {
            return Err(Error::StateError("adapter is closed".into()));
        }
        self.events.push(event.clone());
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::StateError("adapter is closed".into()));
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn exported_events(&self) -> Option<&[Event]> {
        Some(&self.events)
    }

    fn is_closed(&self) -> bool {
        self.closed
    }

    // delivery_guarantee, idempotent_delivery_capable, etc. use defaults.
}

// ─── Conformance testing ─────────────────────────────────────────────────────

/// A set of events used as input for a single conformance scenario.
#[derive(Debug, Clone)]
pub struct AdapterGoldenFixture {
    pub name: String,
    pub events: Vec<Event>,
}

impl AdapterGoldenFixture {
    pub fn new(name: impl Into<String>, events: Vec<Event>) -> Self {
        Self {
            name: name.into(),
            events,
        }
    }

    pub fn single_event(event: Event) -> Self {
        Self::new("single_event", vec![event])
    }

    pub fn batch(events: Vec<Event>) -> Self {
        Self::new("batch", events)
    }

    pub fn ordering(events: Vec<Event>) -> Self {
        Self::new("ordering", events)
    }

    pub fn crash_recovery(events: Vec<Event>) -> Self {
        Self::new("crash_recovery", events)
    }
}

/// Result of a single conformance scenario.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestResult {
    pub passed: bool,
    pub errors: Vec<String>,
    pub duration_ms: u64,
}

/// Default conformance validator for [`SinkAdapter`] implementations.
///
/// All methods are generic over `S: SinkAdapter`, avoiding dynamic dispatch and
/// `Box<dyn Future>` overhead. Pass a concrete adapter instance directly.
#[derive(Debug, Clone, Default)]
pub struct BasicAdapterConformance;

impl BasicAdapterConformance {
    fn pass() -> TestResult {
        TestResult {
            passed: true,
            errors: Vec::new(),
            duration_ms: 0,
        }
    }

    fn exported_len<S: SinkAdapter>(adapter: &S) -> Option<usize> {
        adapter.exported_events().map(|events| events.len())
    }

    /// Verify that a single event is delivered and flushed correctly.
    pub async fn single_event<S: SinkAdapter>(
        &self,
        adapter: &mut S,
        fixture: &AdapterGoldenFixture,
    ) -> Result<TestResult> {
        let Some(first) = fixture.events.first() else {
            return Err(Error::ConfigError(
                "single_event fixture requires at least one event".into(),
            ));
        };
        let before_len = Self::exported_len(adapter);
        adapter.send(first).await?;
        adapter.flush().await?;

        if let Some(before) = before_len {
            let after_events = adapter.exported_events().ok_or_else(|| {
                Error::StateError("adapter exported_events became unavailable mid-test".into())
            })?;
            let after = after_events.len();
            if after != before + 1 {
                return Err(Error::StateError(format!(
                    "single_event conformance expected +1 event, observed delta {}",
                    after.saturating_sub(before)
                )));
            }
            if after_events.last() != Some(first) {
                return Err(Error::StateError(
                    "single_event conformance expected last emitted event to match fixture".into(),
                ));
            }
        }

        Ok(Self::pass())
    }

    /// Verify that a batch of events is delivered in order.
    pub async fn batch_send<S: SinkAdapter>(
        &self,
        adapter: &mut S,
        fixture: &AdapterGoldenFixture,
    ) -> Result<TestResult> {
        let before_len = Self::exported_len(adapter);
        for event in &fixture.events {
            adapter.send(event).await?;
        }
        adapter.flush().await?;

        if let Some(before) = before_len {
            let after_events = adapter.exported_events().ok_or_else(|| {
                Error::StateError("adapter exported_events became unavailable mid-test".into())
            })?;
            let expected = fixture.events.len();
            let after = after_events.len();
            let observed = after.saturating_sub(before);
            if observed != expected {
                return Err(Error::StateError(format!(
                    "batch_send conformance expected {expected} new events, observed {observed}"
                )));
            }
            if after_events[before..after] != fixture.events[..] {
                return Err(Error::StateError(
                    "batch_send conformance expected emitted tail to match fixture order".into(),
                ));
            }
        }

        Ok(Self::pass())
    }

    /// Verify that multiple events are delivered in arrival order.
    pub async fn ordering<S: SinkAdapter>(
        &self,
        adapter: &mut S,
        fixture: &AdapterGoldenFixture,
    ) -> Result<TestResult> {
        let before_len = Self::exported_len(adapter);
        for event in &fixture.events {
            adapter.send(event).await?;
        }
        adapter.flush().await?;

        if let Some(before) = before_len {
            let after_events = adapter.exported_events().ok_or_else(|| {
                Error::StateError("adapter exported_events became unavailable mid-test".into())
            })?;
            let after = after_events.len();
            if after < before || after - before != fixture.events.len() {
                return Err(Error::StateError(
                    "ordering conformance observed unexpected emitted event count delta".into(),
                ));
            }
            if after_events[before..after] != fixture.events[..] {
                return Err(Error::StateError(
                    "ordering conformance expected emitted sequence to preserve fixture order"
                        .into(),
                ));
            }
        }

        Ok(Self::pass())
    }

    /// Verify that `close()` prevents further delivery and that pre-close events are durable.
    pub async fn crash_recovery<S: SinkAdapter>(
        &self,
        adapter: &mut S,
        fixture: &AdapterGoldenFixture,
    ) -> Result<TestResult> {
        let Some(first_event) = fixture.events.first() else {
            return Err(Error::ConfigError(
                "crash_recovery fixture requires at least one event".into(),
            ));
        };

        let before_len = Self::exported_len(adapter);
        for event in &fixture.events {
            adapter.send(event).await?;
        }
        adapter.flush().await?;
        adapter.close().await?;

        if !adapter.is_closed() {
            return Err(Error::StateError(
                "crash_recovery conformance expected adapter to report closed state".into(),
            ));
        }

        if let Some(before) = before_len {
            let after_events = adapter.exported_events().ok_or_else(|| {
                Error::StateError("adapter exported_events became unavailable mid-test".into())
            })?;
            let after = after_events.len();
            let observed = after.saturating_sub(before);
            if observed != fixture.events.len() {
                return Err(Error::StateError(format!(
                    "crash_recovery conformance expected {} new events before close, observed {observed}",
                    fixture.events.len()
                )));
            }
        }

        if adapter.send(first_event).await.is_ok() {
            return Err(Error::StateError(
                "crash_recovery conformance expected send to fail after adapter close".into(),
            ));
        }

        Ok(Self::pass())
    }
}

/// Convenience harness that runs all base adapter conformance scenarios against a
/// single [`SinkAdapter`] + [`AdapterGoldenFixture`] pair.
///
/// All methods are generic over `S: SinkAdapter` — no heap allocation, no
/// `Box<dyn Future>`, no `async-trait` dependency required.
#[derive(Debug, Clone, Default)]
pub struct AdapterConformanceSuite {
    harness: BasicAdapterConformance,
}

impl AdapterConformanceSuite {
    pub fn new() -> Self {
        Self::default()
    }

    /// Run all four base conformance scenarios.
    pub async fn run_all<S: SinkAdapter>(
        &self,
        adapter: &mut S,
        fixture: &AdapterGoldenFixture,
    ) -> Result<Vec<TestResult>> {
        let mut results = Vec::with_capacity(4);
        results.push(self.harness.single_event(adapter, fixture).await?);
        results.push(self.harness.batch_send(adapter, fixture).await?);
        results.push(self.harness.ordering(adapter, fixture).await?);
        results.push(self.harness.crash_recovery(adapter, fixture).await?);
        Ok(results)
    }
}
