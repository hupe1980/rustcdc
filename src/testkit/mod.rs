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
    AdapterConformanceSuite, AdapterConformanceTest, AdapterGoldenFixture, BasicAdapterConformance,
    MemorySinkAdapter, SinkAdapter, TestResult,
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
    fn name(&self) -> &str;
    fn events(&self) -> &[Event];
    fn events_mut(&mut self) -> &mut [Event];
}

/// Fixture loaded from a newline-delimited JSON file on disk.
#[derive(Debug, Clone)]
pub struct JsonFixture {
    name: String,
    events: Vec<Event>,
}

impl JsonFixture {
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
    pub expected_count: usize,
    pub actual_count: usize,
    pub mismatches: Vec<String>,
}

/// Feeds a fixture through a [`CdcRuntime`] and collects the emitted events.
pub struct ReplayRunner<'a, C, H> {
    fixture: Box<dyn Fixture>,
    runtime: &'a mut CdcRuntime<C, H>,
}

impl<'a, C, H> ReplayRunner<'a, C, H>
where
    C: crate::checkpoint::Checkpoint + Send + Sync + 'static,
    H: crate::schema_history::SchemaHistory + Send + Sync + 'static,
{
    pub fn new(fixture: Box<dyn Fixture>, runtime: &'a mut CdcRuntime<C, H>) -> Self {
        Self { fixture, runtime }
    }

    pub async fn run(&mut self) -> Result<Vec<Event>> {
        let expected = self.fixture.events().len();
        let mut output = Vec::with_capacity(expected);

        for event in self.fixture.events() {
            loop {
                match self.runtime.enqueue_event(event.clone()) {
                    Ok(()) => break,
                    Err(Error::StateError(message)) if message == "runtime buffer is full" => {
                        let batch = self.runtime.poll_event_batch().await?;
                        if batch.is_empty() {
                            return Err(Error::StateError(
                                "runtime buffer remained full without yielding events".into(),
                            ));
                        }
                        let token = batch.ack_token().ok_or_else(|| {
                            Error::StateError(
                                "runtime yielded a non-empty batch without ack token".into(),
                            )
                        })?;
                        output.extend(batch.into_events());
                        self.runtime.commit_ack(token).await?;
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
            let token = batch.ack_token().ok_or_else(|| {
                Error::StateError("runtime yielded a non-empty batch without ack token".into())
            })?;
            output.extend(batch.into_events());
            self.runtime.commit_ack(token).await?;
        }

        Ok(output)
    }

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
pub trait ConformanceTest<C, H> {
    fn name(&self) -> &str;
    fn run(&self, runtime: &mut CdcRuntime<C, H>) -> Result<TestResult>;
}

/// Aggregate result for a full [`ConformanceSuite`] run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuiteResult {
    pub passed: usize,
    pub failed: usize,
    pub total: usize,
    pub tests: Vec<TestResult>,
}

/// Runs a collection of [`ConformanceTest`] instances and aggregates results.
pub struct ConformanceSuite<C, H> {
    tests: Vec<Box<dyn ConformanceTest<C, H>>>,
}

impl<C, H> Default for ConformanceSuite<C, H> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C, H> ConformanceSuite<C, H> {
    pub fn new() -> Self {
        Self { tests: Vec::new() }
    }

    pub fn add_test(&mut self, test: Box<dyn ConformanceTest<C, H>>) {
        self.tests.push(test);
    }

    pub fn run_all(&mut self, runtime: &mut CdcRuntime<C, H>) -> SuiteResult {
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
    pub fn checkpoint_barrier_enforced() -> Self {
        Self {
            name: "checkpoint_barrier_enforced",
        }
    }

    pub fn no_event_loss_on_crash() -> Self {
        Self {
            name: "no_event_loss_on_crash",
        }
    }
}

impl<C, H> ConformanceTest<C, H> for NotImplementedConformanceTest {
    fn name(&self) -> &str {
        self.name
    }

    fn run(&self, _runtime: &mut CdcRuntime<C, H>) -> Result<TestResult> {
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
            AdapterConformanceSuite, AdapterConformanceTest, AdapterGoldenFixture,
            BasicAdapterConformance, ConformanceSuite, Fixture, JsonFixture, MemorySinkAdapter,
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
        let mut runtime = crate::core::CdcRuntime::<_, _>::new(config).unwrap();
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
        let mut runtime = crate::core::CdcRuntime::<_, _>::new(config).unwrap();
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
        let mut runtime = crate::core::CdcRuntime::<_, _>::new(config).unwrap();

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

    #[async_trait::async_trait]
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

        fn is_closed(&self) -> Option<bool> {
            Some(self.closed)
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
