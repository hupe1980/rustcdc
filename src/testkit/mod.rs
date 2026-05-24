//! Test fixtures and conformance helpers.

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use async_trait::async_trait;

use crate::core::{CdcRuntime, Error, Event, Result};

#[async_trait]
pub trait SinkAdapter: Send {
    async fn send(&mut self, event: &Event) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    fn name(&self) -> &str;

    /// Optional inspection hook for deterministic conformance assertions.
    ///
    /// Adapters that can safely expose a read-only in-memory view should return
    /// `Some`, while opaque adapters may return `None`.
    fn exported_events(&self) -> Option<&[Event]> {
        None
    }

    /// Optional closed-state hook for crash/close conformance assertions.
    fn is_closed(&self) -> Option<bool> {
        None
    }
}

/// In-memory sink adapter for conformance and integration testing.
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
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            events: Vec::new(),
            closed: false,
        }
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }
}

#[async_trait]
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

#[async_trait]
pub trait AdapterConformanceTest: Send + Sync {
    async fn single_event(
        &self,
        adapter: &mut dyn SinkAdapter,
        fixture: &AdapterGoldenFixture,
    ) -> Result<TestResult>;
    async fn batch_send(
        &self,
        adapter: &mut dyn SinkAdapter,
        fixture: &AdapterGoldenFixture,
    ) -> Result<TestResult>;
    async fn ordering(
        &self,
        adapter: &mut dyn SinkAdapter,
        fixture: &AdapterGoldenFixture,
    ) -> Result<TestResult>;
    async fn crash_recovery(
        &self,
        adapter: &mut dyn SinkAdapter,
        fixture: &AdapterGoldenFixture,
    ) -> Result<TestResult>;
}

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

    fn exported_len(adapter: &dyn SinkAdapter) -> Option<usize> {
        adapter.exported_events().map(|events| events.len())
    }
}

#[async_trait]
impl AdapterConformanceTest for BasicAdapterConformance {
    async fn single_event(
        &self,
        adapter: &mut dyn SinkAdapter,
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

    async fn batch_send(
        &self,
        adapter: &mut dyn SinkAdapter,
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

    async fn ordering(
        &self,
        adapter: &mut dyn SinkAdapter,
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

    async fn crash_recovery(
        &self,
        adapter: &mut dyn SinkAdapter,
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

/// Convenience harness that runs all base adapter conformance scenarios.
#[derive(Debug, Clone, Default)]
pub struct AdapterConformanceSuite {
    harness: BasicAdapterConformance,
}

impl AdapterConformanceSuite {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn run_all(
        &self,
        adapter: &mut dyn SinkAdapter,
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

pub trait Fixture {
    fn name(&self) -> &str;
    fn events(&self) -> &[Event];
    fn events_mut(&mut self) -> &mut [Event];
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureDiff {
    pub expected_count: usize,
    pub actual_count: usize,
    pub mismatches: Vec<String>,
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestResult {
    pub passed: bool,
    pub errors: Vec<String>,
    pub duration_ms: u64,
}

pub trait ConformanceTest<C, H> {
    fn name(&self) -> &str;
    fn run(&self, runtime: &mut CdcRuntime<C, H>) -> Result<TestResult>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuiteResult {
    pub passed: usize,
    pub failed: usize,
    pub total: usize,
    pub tests: Vec<TestResult>,
}

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
