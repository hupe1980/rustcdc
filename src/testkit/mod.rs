//! Test fixtures, golden-file helpers, and conformance harnesses.
//!
//! # Sink integration
//!
//! The [`SinkAdapter`] trait and built-in adapters have moved to [`crate::sink`].
//! They are re-exported here for convenience so existing test code continues to
//! compile with `use rustcdc::testkit::SinkAdapter`.
//!
//! # File-based fixtures
//!
//! [`JsonFixture`] loads newline-delimited JSON event files from disk (e.g. the
//! fixtures in `fixtures/`).  [`ReplayRunner`] feeds them through a
//! [`CdcRuntime`] and collects the output.
//!
//! # Conformance suites
//!
//! [`ConformanceSuite`] aggregates [`ConformanceTest`] implementations and reports
//! pass/fail per test.  [`NotImplementedConformanceTest`] marks tests that exist
//! in the contract but have not yet been implemented.

// Re-export the public sink API so callers importing via `testkit` continue to work.
pub use crate::sink::{
    AdapterConformanceSuite, AdapterGoldenFixture, BasicAdapterConformance, MemorySinkAdapter,
    SinkAdapter, TestResult,
};

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use crate::core::{CdcRuntime, Error, Event, Result};

// ─── File-based fixtures ─────────────────────────────────────────────────────

/// Trait for fixture types that expose a named, ordered event sequence.
pub trait Fixture {
    /// Fixture name, used in failure messages.
    fn name(&self) -> &str;
    /// The events this fixture replays.
    fn events(&self) -> &[Event];
    /// Mutable access, for tests that perturb a fixture before replaying it.
    fn events_mut(&mut self) -> &mut [Event];
}

/// Fixture loaded from a newline-delimited JSON file on disk.
#[derive(Debug, Clone)]
pub struct JsonFixture {
    name: String,
    events: Vec<Event>,
}

impl JsonFixture {
    /// Load a fixture from a JSON file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or does not parse as a fixture.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut events = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            events.push(Event::from_json(&line)?);
        }

        Ok(Self {
            name: path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("fixture")
                .to_string(),
            events,
        })
    }
}

impl Fixture for JsonFixture {
    fn name(&self) -> &str {
        &self.name
    }

    fn events(&self) -> &[Event] {
        &self.events
    }

    fn events_mut(&mut self) -> &mut [Event] {
        &mut self.events
    }
}

/// Diff result produced by [`ReplayRunner::verify_output`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureDiff {
    /// Events the fixture declared.
    pub expected_count: usize,
    /// Events the run actually produced.
    pub actual_count: usize,
    /// Human-readable description of each difference.
    pub mismatches: Vec<String>,
}

/// Feeds a fixture through a [`CdcRuntime`] and collects the emitted events.
pub struct ReplayRunner<'a> {
    fixture: Box<dyn Fixture>,
    runtime: &'a mut CdcRuntime,
}

impl<'a> ReplayRunner<'a> {
    /// Bind a fixture to the runtime that will replay it.
    pub fn new(fixture: Box<dyn Fixture>, runtime: &'a mut CdcRuntime) -> Self {
        Self { fixture, runtime }
    }

    /// Replay the fixture through the runtime and collect what came out.
    ///
    /// # Errors
    ///
    /// Propagates runtime errors from enqueue, poll, or commit.
    pub async fn run(&mut self) -> Result<Vec<Event>> {
        let expected = self.fixture.events().len();
        let mut output = Vec::with_capacity(expected);

        for event in self.fixture.events() {
            loop {
                match self.runtime.enqueue_event(event.clone()) {
                    Ok(()) => break,
                    // Match the kind, not the message text: the wording is not a
                    // contract, and matching on it silently stops working when it changes.
                    Err(error) if error.kind() == crate::core::ErrorKind::Backpressure => {
                        let batch = self.runtime.poll_event_batch().await?;
                        if batch.is_empty() {
                            return Err(Error::StateError(
                                "runtime buffer remained full without yielding events".into(),
                            ));
                        }
                        let mode = batch.ack_mode();
                        output.extend(batch.into_events());
                        self.runtime.commit_ack(mode).await?;
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        while output.len() < expected {
            let batch = self.runtime.poll_event_batch().await?;
            if batch.is_empty() {
                break;
            }
            let mode = batch.ack_mode();
            output.extend(batch.into_events());
            self.runtime.commit_ack(mode).await?;
        }

        Ok(output)
    }

    /// Compare produced events against expectations.
    ///
    /// Returns a [`FixtureDiff`] describing every difference rather than failing on the
    /// first — a single assertion tells you nothing about the shape of a regression.
    ///
    /// # Errors
    ///
    /// Propagates serialization errors while rendering mismatches.
    pub fn verify_output(&self, expected: &[Event], actual: &[Event]) -> Result<FixtureDiff> {
        let mut mismatches = Vec::new();
        for (index, (left, right)) in expected.iter().zip(actual.iter()).enumerate() {
            if left != right {
                mismatches.push(format!("event {index} differs"));
            }
        }
        if expected.len() != actual.len() {
            mismatches.push(format!(
                "expected {} events, got {}",
                expected.len(),
                actual.len()
            ));
        }
        Ok(FixtureDiff {
            expected_count: expected.len(),
            actual_count: actual.len(),
            mismatches,
        })
    }
}

// ─── Runtime conformance suites ──────────────────────────────────────────────

/// A single runtime-level conformance test scenario.
pub trait ConformanceTest {
    /// Test name, reported in the suite result.
    fn name(&self) -> &str;
    /// Execute the test against `runtime`.
    fn run(&self, runtime: &mut CdcRuntime) -> Result<TestResult>;
}

/// Aggregate result for a full [`ConformanceSuite`] run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuiteResult {
    /// Tests that passed.
    pub passed: usize,
    /// Tests that failed.
    pub failed: usize,
    /// Tests executed.
    pub total: usize,
    /// Per-test outcomes.
    pub tests: Vec<TestResult>,
}

/// Runs a collection of [`ConformanceTest`] instances and aggregates results.
pub struct ConformanceSuite {
    tests: Vec<Box<dyn ConformanceTest>>,
}

impl Default for ConformanceSuite {
    fn default() -> Self {
        Self::new()
    }
}

impl ConformanceSuite {
    /// Build an empty suite.
    pub fn new() -> Self {
        Self { tests: Vec::new() }
    }

    /// Append a test to the suite.
    pub fn add_test(&mut self, test: Box<dyn ConformanceTest>) {
        self.tests.push(test);
    }

    /// Run every test, continuing past failures, and aggregate the results.
    pub fn run_all(&mut self, runtime: &mut CdcRuntime) -> SuiteResult {
        let mut results = Vec::new();
        for test in &self.tests {
            let result = test.run(runtime).unwrap_or_else(|error| TestResult {
                passed: false,
                errors: vec![error.to_string()],
                duration_ms: 0,
            });
            results.push(result);
        }

        let passed = results.iter().filter(|result| result.passed).count();
        let failed = results.len().saturating_sub(passed);

        SuiteResult {
            passed,
            failed,
            total: results.len(),
            tests: results,
        }
    }
}

/// Placeholder conformance test for contract scenarios not yet implemented.
pub struct NotImplementedConformanceTest {
    name: &'static str,
}

impl NotImplementedConformanceTest {
    /// The checkpoint never advances past what the consumer acknowledged.
    pub fn checkpoint_barrier_enforced() -> Self {
        Self {
            name: "checkpoint_barrier_enforced",
        }
    }

    /// A crash replays from the last durable checkpoint without losing events.
    pub fn no_event_loss_on_crash() -> Self {
        Self {
            name: "no_event_loss_on_crash",
        }
    }
}

impl ConformanceTest for NotImplementedConformanceTest {
    fn name(&self) -> &str {
        self.name
    }

    fn run(&self, _runtime: &mut CdcRuntime) -> Result<TestResult> {
        Err(Error::NotImplemented(self.name.into()))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use serde_json::json;
    use tempfile::NamedTempFile;

    use crate::{
        checkpoint::InMemoryCheckpoint,
        core::{Event, Operation, RuntimeConfig, SourceMetadata, EVENT_ENVELOPE_VERSION},
        schema_history::InMemorySchemaHistory,
        testkit::{
            AdapterConformanceSuite, AdapterGoldenFixture, BasicAdapterConformance,
            ConformanceSuite, Fixture, JsonFixture, MemorySinkAdapter,
            NotImplementedConformanceTest, ReplayRunner, SinkAdapter,
        },
    };

    fn event() -> Event {
        Event {
            before: None,
            after: Some(json!({"id": 1})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "mock".into(),
                offset: "1".into(),
                timestamp: 1,
            },
            ts: 1,
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

    #[test]
    fn json_fixture_loads_events() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "{}", event().to_json().unwrap()).unwrap();

        let fixture = JsonFixture::load(file.path()).unwrap();
        assert_eq!(fixture.events().len(), 1);
    }

    #[tokio::test]
    async fn replay_runner_replays_events() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "{}", event().to_json().unwrap()).unwrap();
        let fixture = JsonFixture::load(file.path()).unwrap();

        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(
            crate::core::RuntimeSourceConfig::Disabled,
            checkpoint,
            schema_history,
        );
        let mut runtime = crate::core::CdcRuntime::new(config).unwrap();
        runtime.start().await.unwrap();

        let mut runner = ReplayRunner::new(Box::new(fixture.clone()), &mut runtime);
        let actual = runner.run().await.unwrap();
        let diff = runner.verify_output(fixture.events(), &actual).unwrap();
        assert!(diff.mismatches.is_empty());
    }

    #[tokio::test]
    async fn replay_runner_handles_fixtures_larger_than_poll_buffer() {
        let mut file = NamedTempFile::new().unwrap();
        let first = event();
        let mut second = event();
        second.ts = 2;
        second.source.offset = "2".into();

        writeln!(file, "{}", first.to_json().unwrap()).unwrap();
        writeln!(file, "{}", second.to_json().unwrap()).unwrap();

        let fixture = JsonFixture::load(file.path()).unwrap();
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(
            crate::core::RuntimeSourceConfig::Disabled,
            checkpoint,
            schema_history,
        )
        .with_max_buffer_size(1);
        let mut runtime = crate::core::CdcRuntime::new(config).unwrap();
        runtime.start().await.unwrap();

        let mut runner = ReplayRunner::new(Box::new(fixture.clone()), &mut runtime);
        let actual = runner.run().await.unwrap();

        assert_eq!(actual.len(), 2);
        let diff = runner.verify_output(fixture.events(), &actual).unwrap();
        assert!(diff.mismatches.is_empty());
    }

    #[test]
    fn conformance_suite_reports_not_implemented_tests() {
        let checkpoint = InMemoryCheckpoint::default();
        let schema_history = InMemorySchemaHistory::default();
        let config = RuntimeConfig::new(
            crate::core::RuntimeSourceConfig::Disabled,
            checkpoint,
            schema_history,
        );
        let mut runtime = crate::core::CdcRuntime::new(config).unwrap();

        let mut suite = ConformanceSuite::new();
        suite.add_test(Box::new(
            NotImplementedConformanceTest::checkpoint_barrier_enforced(),
        ));
        let result = suite.run_all(&mut runtime);
        assert_eq!(result.failed, 1);
        assert!(!result.tests[0].passed);
    }

    #[derive(Debug, Default)]
    struct MockSinkAdapter {
        events: Vec<Event>,
        closed: bool,
    }

    impl SinkAdapter for MockSinkAdapter {
        async fn send(&mut self, event: &Event) -> crate::core::Result<()> {
            if self.closed {
                return Err(crate::core::Error::StateError("adapter is closed".into()));
            }
            self.events.push(event.clone());
            Ok(())
        }

        async fn flush(&mut self) -> crate::core::Result<()> {
            if self.closed {
                return Err(crate::core::Error::StateError("adapter is closed".into()));
            }
            Ok(())
        }

        async fn close(&mut self) -> crate::core::Result<()> {
            self.closed = true;
            Ok(())
        }

        fn name(&self) -> &str {
            "mock"
        }

        fn exported_events(&self) -> Option<&[Event]> {
            Some(&self.events)
        }

        fn is_closed(&self) -> bool {
            self.closed
        }
    }

    #[test]
    fn adapter_golden_fixture_builders_work() {
        let single = AdapterGoldenFixture::single_event(event());
        assert_eq!(single.events.len(), 1);

        let batch = AdapterGoldenFixture::batch(vec![event(), event()]);
        assert_eq!(batch.events.len(), 2);

        let ordering = AdapterGoldenFixture::ordering(vec![event()]);
        assert_eq!(ordering.name, "ordering");

        let crash = AdapterGoldenFixture::crash_recovery(vec![event()]);
        assert_eq!(crash.name, "crash_recovery");
    }

    #[tokio::test]
    async fn basic_adapter_conformance_runs_all_scenarios() {
        let harness = BasicAdapterConformance;
        let fixture = AdapterGoldenFixture::batch(vec![event(), event()]);
        let mut adapter = MockSinkAdapter::default();

        let single = harness.single_event(&mut adapter, &fixture).await.unwrap();
        assert!(single.passed);
        let batch = harness.batch_send(&mut adapter, &fixture).await.unwrap();
        assert!(batch.passed);
        let ordering = harness.ordering(&mut adapter, &fixture).await.unwrap();
        assert!(ordering.passed);
        let crash = harness
            .crash_recovery(&mut adapter, &fixture)
            .await
            .unwrap();
        assert!(crash.passed);

        assert!(adapter.closed);
        assert!(adapter.events.len() >= fixture.events.len());
    }

    #[tokio::test]
    async fn adapter_conformance_suite_runs_all_harness_paths() {
        let fixture = AdapterGoldenFixture::batch(vec![event(), event()]);
        let suite = AdapterConformanceSuite::new();
        let mut adapter = MemorySinkAdapter::default();

        let results = suite.run_all(&mut adapter, &fixture).await.unwrap();

        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|result| result.passed));
        assert!(adapter.events().len() >= fixture.events.len());
    }
}
