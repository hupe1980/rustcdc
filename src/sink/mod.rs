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
//! | [`FileJsonlSink`] | Appends NDJSON to a file with fsync-on-close; for audit trails |
//!
//! # Conformance testing
//!
//! [`AdapterConformanceSuite`] verifies that a custom [`SinkAdapter`] implementation
//! honours the contract (ordering, flush semantics, post-close error behaviour).

pub mod file_jsonl;
pub mod stdout;

pub use file_jsonl::FileJsonlSink;
pub use stdout::StdoutSink;

use crate::core::{Error, Event, Result};

// ─── SinkAdapter ─────────────────────────────────────────────────────────────

/// Trait for sending CDC events to a downstream system.
///
/// Implementations must be `Send` so they can be used across async task boundaries.
/// All methods take `&mut self` so the adapter can maintain internal state (e.g. a
/// connection handle or an in-flight buffer) without an inner `Mutex`.
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
pub trait SinkAdapter: Send {
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

    /// Close the adapter, returning an error if the close takes longer than `timeout_ms`.
    ///
    /// This is a default convenience wrapper around [`close`] that applies a
    /// `tokio::time::timeout`. Use this in shutdown paths where a hung sink
    /// (e.g. a Kafka producer waiting for broker acknowledgement) must not
    /// prevent the process from exiting.
    ///
    /// Returns [`Error::TimeoutError`] if the deadline is exceeded. The adapter
    /// state is indeterminate after a timeout — treat it as permanently closed
    /// and do not send further events.
    ///
    /// [`close`]: SinkAdapter::close
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

    /// Human-readable name used in logs and conformance reports.
    fn name(&self) -> &str;

    /// Optional inspection hook for deterministic conformance assertions.
    ///
    /// Adapters that can safely expose a read-only in-memory view of all received
    /// events should return `Some`.  Opaque adapters (writing to an external system)
    /// may return `None`.
    fn exported_events(&self) -> Option<&[Event]> {
        None
    }

    /// Optional closed-state hook for conformance assertions.
    ///
    /// Return `Some(true)` after [`close`] has been called, `Some(false)` before,
    /// or `None` if the adapter cannot track closed state.
    ///
    /// [`close`]: SinkAdapter::close
    fn is_closed(&self) -> Option<bool> {
        None
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

    fn is_closed(&self) -> Option<bool> {
        Some(self.closed)
    }
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

        if let Some(is_closed) = adapter.is_closed() {
            if !is_closed {
                return Err(Error::StateError(
                    "crash_recovery conformance expected adapter to report closed state".into(),
                ));
            }
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
