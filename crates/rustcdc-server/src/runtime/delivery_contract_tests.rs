//! End-to-end evidence for the delivery contracts.
//!
//! Every other test in this repository checks one layer: the sink's transactional
//! barrier against a `FakeBroker`, the checkpoint store's atomic write, the router's
//! dispatch. None of them answered the only question an operator actually asks —
//! *if this process dies right now, what happens to my data?* The contract was
//! documented, labelled, exported as a metric and validated at startup without a
//! single test driving a real crash through the real batch handler.
//!
//! These tests drive [`super::batch_commit::handle_polled_batch`] — the production
//! code path, not a re-implementation — against a real `CdcRuntime`, a real local-fs
//! checkpoint store and a real `file_jsonl` sink in a `tempfile` directory. The crash
//! is injected with rustcdc's [`CheckpointFault::FailSave`], which models the
//! exact failure the contracts are defined around: the sink accepted the data and the
//! durable position never landed.
//!
//! The `effectively_once` case is covered by the transactional-barrier tests in
//! [`crate::sink::kafka`], which drive a `FakeBroker` and assert on committed records
//! and abort markers directly. What is asserted *here* is the property those tests
//! cannot see: that the crash window documented at
//! <https://hupe1980.github.io/rustcdc/docs/concepts/#3-delivery-contracts>
//! is real, and that the reconciliation marker detects it.

use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rustcdc::checkpoint::Checkpoint;
use rustcdc::core::{
    CdcRuntime, Event, Offset, Operation, RuntimeConfig, RuntimeSourceConfig, SourceMetadata,
};
use rustcdc::fault_injection::{CheckpointFault, FaultInjectingCheckpoint};
use rustcdc::source::{
    ConnectorCapabilities, HandoffResult, SnapshotEnd, SnapshotHandle, Source, StreamHandle,
};

use crate::admin::AdminState;
use crate::config::schema::{AppConfig, DeliveryContract, StateBackend};
use crate::config::sink::{FileJsonlSinkConfig, SinkConfig};
use crate::pipeline::transform::TransformPipeline;
use crate::runtime::batch_commit::handle_polled_batch;
use crate::runtime::event_loop::RuntimeLoopConfig;
use crate::runtime::metrics::RuntimeLoopMetricsAccumulator;
use crate::runtime::reconciliation::CheckpointTxnReconciler;
use crate::runtime::recovery::{RecoverableErrorState, RecoveryPolicyConfig};
use crate::state::CheckpointAgeSource;

const SOURCE_TYPE: &str = "scripted";

// ─────────────────────────────────────────────────────────────────────────────
// A scripted source
// ─────────────────────────────────────────────────────────────────────────────

/// Decode whatever the checkpoint handed back into a sequence number.
///
/// The runtime persists `Event::source.offset` verbatim for a custom source, so the
/// offset that comes back is the decimal string the scripted event carried — possibly
/// JSON-quoted, depending on how the backend serialised it.
///
/// A malformed offset resolves to 0 — "replay everything" — rather than to the head of
/// the log. Guessing forward is how a resume bug turns into silent data loss.
fn resume_sequence(resume_from: Option<&dyn Offset>) -> u64 {
    resume_from
        .and_then(|offset| offset.encode().ok())
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .and_then(|text| text.trim_matches('"').parse::<u64>().ok())
        .unwrap_or(0)
}

/// A stream that replays a fixed script from wherever the checkpoint left off.
///
/// This is what a source with a perfectly reliable, infinitely retained log looks
/// like: every event from `next_seq` onward is always still available. That is
/// deliberate — it isolates the property under test. Any event missing from the
/// output is the pipeline's loss, never the source's.
struct ScriptedStream {
    total: u64,
    next_seq: u64,
    batch_size: usize,
    /// Fail every read as a source whose replication slot was invalidated.
    slot_lost: bool,
}

/// Where a scripted source reports its replication slot as invalidated, if anywhere.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LostSlot {
    Never,
    OnPoll,
    OnStart,
}

fn slot_invalidated() -> rustcdc::core::Error {
    rustcdc::core::Error::source_error(
        rustcdc::core::SourceErrorKind::SlotInvalidated,
        "postgres invalidated replication slot 'cdc' (invalidation_reason = wal_removed)",
    )
}

#[async_trait]
impl StreamHandle for ScriptedStream {
    async fn next_events(&mut self, _timeout_ms: u64) -> rustcdc::core::Result<Vec<Event>> {
        if self.slot_lost {
            return Err(slot_invalidated());
        }
        let mut events = Vec::new();
        while events.len() < self.batch_size && self.next_seq < self.total {
            self.next_seq += 1;
            events.push(scripted_event(self.next_seq));
        }
        Ok(events)
    }

    async fn save_position(&self, _checkpoint: &mut dyn Checkpoint) -> rustcdc::core::Result<()> {
        Ok(())
    }

    async fn confirm_lsn(&mut self, _lsn: u64) -> rustcdc::core::Result<()> {
        Ok(())
    }
}

/// One INSERT carrying its sequence number as both the row id and the durable offset.
fn scripted_event(seq: u64) -> Event {
    Event::builder("ledger", Operation::Insert)
        .after(serde_json::json!({ "id": seq }))
        .source(SourceMetadata::new(SOURCE_TYPE, seq.to_string(), seq))
        .ts(seq)
        .schema("public")
        .primary_key(["id"])
        .build()
}

struct EmptySnapshot;

#[async_trait]
impl SnapshotHandle for EmptySnapshot {
    async fn next_chunk(&mut self, _chunk_size: usize) -> rustcdc::core::Result<Vec<Event>> {
        Ok(Vec::new())
    }

    async fn checkpoint(
        &self,
        _checkpoint: &mut dyn Checkpoint,
        _committed_event_count: u64,
    ) -> rustcdc::core::Result<()> {
        Ok(())
    }

    async fn finish(&mut self) -> rustcdc::core::Result<SnapshotEnd> {
        Ok(SnapshotEnd { snapshot_end_ts: 0 })
    }
}

struct ScriptedSource {
    total: u64,
    batch_size: usize,
    /// Where `start_stream` was asked to resume from, recorded for assertion.
    resumed_from: Arc<Mutex<Option<u64>>>,
    lost_slot: LostSlot,
}

#[async_trait]
impl Source for ScriptedSource {
    async fn start_snapshot(
        &mut self,
        _tables: &[&str],
    ) -> rustcdc::core::Result<Box<dyn SnapshotHandle>> {
        Ok(Box::new(EmptySnapshot))
    }

    async fn start_stream(
        &mut self,
        resume_from: Option<&dyn Offset>,
    ) -> rustcdc::core::Result<Box<dyn StreamHandle>> {
        if self.lost_slot == LostSlot::OnStart {
            return Err(slot_invalidated());
        }
        let next_seq = resume_sequence(resume_from);
        *self.resumed_from.lock().expect("resume lock") = Some(next_seq);
        Ok(Box::new(ScriptedStream {
            total: self.total,
            next_seq,
            batch_size: self.batch_size,
            slot_lost: self.lost_slot == LostSlot::OnPoll,
        }))
    }

    async fn perform_handoff(
        &mut self,
        _snapshot: &mut dyn SnapshotHandle,
        _stream: &mut dyn StreamHandle,
    ) -> rustcdc::core::Result<HandoffResult> {
        Ok(HandoffResult::default())
    }

    fn source_type(&self) -> &str {
        SOURCE_TYPE
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::none()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Harness
// ─────────────────────────────────────────────────────────────────────────────

/// One process lifetime: a runtime, its router, and everything the batch handler needs.
struct Pipeline {
    runtime: CdcRuntime,
    router: crate::pipeline::router::TableRouter,
    sink_metrics: crate::sink::SinkMetricsRegistry,
    transform_pipeline: TransformPipeline,
    admin_state: AdminState,
    checkpoint_age_source: CheckpointAgeSource,
    reconciler: CheckpointTxnReconciler,
    loop_config: RuntimeLoopConfig,
    recovery_policy: RecoveryPolicyConfig,
    recoverable: RecoverableErrorState,
    metrics: RuntimeLoopMetricsAccumulator,
    resumed_from: Arc<Mutex<Option<u64>>>,
}

impl Pipeline {
    /// Start a process against `state_dir` (persistent across restarts) writing to
    /// `output_path` (appended across restarts, so the test sees every delivery any
    /// incarnation made).
    async fn start(
        state_dir: &Path,
        output_path: &Path,
        total: u64,
        batch_size: usize,
        faults: &[CheckpointFault],
    ) -> Self {
        Self::start_with_max_event_bytes(state_dir, output_path, total, batch_size, faults, 1 << 20)
            .await
    }

    /// A pipeline whose sink is an HTTP endpoint nothing is listening on.
    async fn start_with_unreachable_sink(
        state_dir: &Path,
        output_path: &Path,
        total: u64,
        batch_size: usize,
    ) -> Self {
        Self::start_with_unreachable_sink_and_threshold(
            state_dir,
            output_path,
            total,
            batch_size,
            None,
        )
        .await
    }

    async fn start_with_unreachable_sink_and_threshold(
        state_dir: &Path,
        output_path: &Path,
        total: u64,
        batch_size: usize,
        health_stall_threshold_ms: Option<u64>,
    ) -> Self {
        let mut pipeline = Self::start_with_options(
            state_dir,
            output_path,
            total,
            batch_size,
            &[],
            1 << 20,
            health_stall_threshold_ms,
            LostSlot::Never,
        )
        .await;

        // Port 1 on loopback: reserved, never listening, and refused immediately.
        let unreachable = serde_json::from_value(serde_json::json!({
            "type": "http",
            "url": "http://127.0.0.1:1/events",
            // Flush on every event so the failure surfaces from `send`, where the
            // sink's own error classification survives. The router flattens *flush*
            // errors from all sinks into one `StateError` string, which upstream
            // classifies Terminal — see the note on `AppError::is_recoverable`.
            "batch_max_events": 1,
            "max_retries": 0,
            "backoff_initial_ms": 1,
            "backoff_max_ms": 1,
        }))
        .expect("unreachable http sink config");
        let binding =
            crate::sink::build_binding(&unreachable, &crate::sink::SinkBuildContext::new(1 << 20))
                .await
                .expect("http binding");
        let mut sink_metrics = crate::sink::SinkMetricsRegistry::default();
        sink_metrics.register(binding.metrics_handle());
        pipeline.router = crate::pipeline::router::single(binding);
        pipeline.sink_metrics = sink_metrics;
        pipeline
    }

    async fn start_with_max_event_bytes(
        state_dir: &Path,
        output_path: &Path,
        total: u64,
        batch_size: usize,
        faults: &[CheckpointFault],
        max_event_bytes: usize,
    ) -> Self {
        Self::start_with_options(
            state_dir,
            output_path,
            total,
            batch_size,
            faults,
            max_event_bytes,
            None,
            LostSlot::Never,
        )
        .await
    }

    /// A pipeline whose source reports its replication slot invalidated. Under
    /// [`LostSlot::OnStart`] the runtime is left unstarted, for the caller to start.
    async fn start_with_lost_slot(state_dir: &Path, output_path: &Path, lost: LostSlot) -> Self {
        Self::start_with_options(state_dir, output_path, 3, 3, &[], 1 << 20, None, lost).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_with_options(
        state_dir: &Path,
        output_path: &Path,
        total: u64,
        batch_size: usize,
        faults: &[CheckpointFault],
        max_event_bytes: usize,
        health_stall_threshold_ms: Option<u64>,
        lost_slot: LostSlot,
    ) -> Self {
        let mut app_config = config_for(state_dir, output_path);
        app_config.runtime.max_event_bytes = max_event_bytes;
        app_config.runtime.sink_flush_interval_events = 1;

        let built = crate::pipeline::binding::build_router(&app_config)
            .await
            .expect("router");
        let router = built.router;
        let sink_metrics = built.sink_metrics;
        let admin_state = AdminState::new(&app_config).await.expect("admin state");
        // The harness uses a `file_jsonl` sink, which has no transaction to share.
        let state = crate::state::build(&app_config.state, built.transaction_handle)
            .await
            .expect("state");

        let mut checkpoint = FaultInjectingCheckpoint::new(state.checkpoint);
        for fault in faults {
            checkpoint.inject(fault.clone());
        }

        let resumed_from = Arc::new(Mutex::new(None));
        let mut runtime_config = RuntimeConfig::new(
            RuntimeSourceConfig::disabled(),
            checkpoint,
            state.schema_history,
        );
        if let Some(threshold_ms) = health_stall_threshold_ms {
            runtime_config.options.max_poll_wait_ms = 10;
            runtime_config.options.health_stall_threshold_ms = Some(threshold_ms);
        }
        let mut runtime = CdcRuntime::new(runtime_config).expect("runtime");
        runtime.register_source(Box::new(ScriptedSource {
            total,
            batch_size,
            resumed_from: Arc::clone(&resumed_from),
            lost_slot,
        }));
        if lost_slot != LostSlot::OnStart {
            runtime.start().await.expect("runtime start");
        }

        let recovery_policy = RecoveryPolicyConfig {
            initial_backoff_ms: 1,
            max_backoff_ms: 2,
            backoff_multiplier: 1.0,
            jitter_ratio: 0.0,
            breaker_consecutive_threshold: 3,
            breaker_max_open_cycles: 1,
            breaker_cooldown_ms: 1,
            breaker_clean_window_successes: 10,
        };

        Self {
            router,
            transform_pipeline: TransformPipeline::from_config(Default::default(), Vec::new())
                .expect("an empty transform pipeline"),
            admin_state,
            checkpoint_age_source: state.checkpoint_age_source,
            reconciler: CheckpointTxnReconciler::new(
                state_dir.to_path_buf(),
                false,
                "file_jsonl".to_string(),
                DeliveryContract::AtLeastOnce.as_label().to_string(),
            ),
            loop_config: loop_config_for(&app_config),
            recoverable: RecoverableErrorState::new(recovery_policy.initial_backoff_ms),
            recovery_policy,
            sink_metrics,
            metrics: RuntimeLoopMetricsAccumulator::new(
                "file_jsonl",
                DeliveryContract::AtLeastOnce.as_label(),
                true,
                "at_least_once",
                false,
                false,
                16,
                1024,
            ),
            runtime,
            resumed_from,
        }
    }

    /// Poll and hand off exactly `batches` batches through the production handler.
    ///
    /// Returns early if the handler asks the loop to stop, so a test that expects a
    /// clean run can assert on the batch count it actually got.
    async fn pump(&mut self, batches: usize) -> usize {
        let mut handled = 0;
        for _ in 0..batches {
            let batch = self.runtime.poll_event_batch().await.expect("poll");
            if batch.is_empty() {
                break;
            }
            let outcome = handle_polled_batch(
                &mut self.runtime,
                &mut self.router,
                &self.transform_pipeline,
                &self.admin_state,
                &self.checkpoint_age_source,
                &mut self.reconciler,
                &self.loop_config,
                &self.recovery_policy,
                &mut self.recoverable,
                &mut self.metrics,
                &self.sink_metrics,
                None,
                None,
                batch,
            )
            .await;
            handled += 1;
            if outcome.is_some() {
                break;
            }
        }
        handled
    }

    /// The sequence number `start_stream` resumed from, i.e. the durable position the
    /// checkpoint store actually handed back.
    fn resumed_from(&self) -> u64 {
        self.resumed_from
            .lock()
            .expect("resume lock")
            .expect("start_stream must have been called by runtime.start()")
    }

    /// Simulate the process dying: no clean shutdown, no final flush, no `stop()`.
    ///
    /// Dropping the router closes the sink's file handle, which is what an OS does to
    /// a dying process's descriptors — it is not a flush the pipeline performed.
    fn crash(self) {
        drop(self);
    }
}

fn config_for(state_dir: &Path, output_path: &Path) -> AppConfig {
    let mut config =
        crate::commands::run::tests::minimal_config(state_dir.to_path_buf(), StateBackend::LocalFs);
    config.sink = SinkConfig::FileJsonl(FileJsonlSinkConfig {
        path: output_path.to_path_buf(),
        rotate_size_bytes: 0,
        fsync_every: 1,
    });
    config
}

fn loop_config_for(app_config: &AppConfig) -> RuntimeLoopConfig {
    RuntimeLoopConfig {
        prepare_parallelism: app_config.runtime.prepare_parallelism,
        sink_flush_interval_events: 1,
        sink_delivery_queue_capacity: app_config.runtime.sink_delivery_queue_capacity,
        sink_send_timeout_ms: 5_000,
        sink_flush_timeout_ms: 5_000,
        recoverable_error_backoff_initial_ms: 1,
        recoverable_error_backoff_max_ms: 2,
        recoverable_error_backoff_multiplier: 1.0,
        recoverable_error_backoff_jitter_ratio: 0.0,
        recoverable_error_breaker_consecutive_threshold: 3,
        recoverable_error_breaker_max_open_cycles: 1,
        recoverable_error_breaker_cooldown_ms: 1,
        sink_name: "file_jsonl".to_string(),
        requested_delivery_contract: DeliveryContract::AtLeastOnce.as_label().to_string(),
        delivery_contract_satisfied: true,
        sink_delivery_guarantee: "at_least_once".to_string(),
        sink_idempotent_delivery_capable: false,
        sink_transactional_checkpoint_barrier_capable: false,
        queue_depth_p95_window_samples: 16,
        correctness_dedup_window_size: 1024,
    }
}

/// Every row id the sink wrote, in delivery order, across every incarnation.
fn delivered_ids(output_path: &Path) -> Vec<u64> {
    let contents = std::fs::read_to_string(output_path).unwrap_or_default();
    contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|value| {
            value
                .pointer("/after/id")
                .or_else(|| value.pointer("/payload/after/id"))
                .and_then(serde_json::Value::as_u64)
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// The contracts
// ─────────────────────────────────────────────────────────────────────────────

/// **A transient sink failure must be retried, not fatal.**
///
/// A sink send timeout used to terminate the process: the batch path returned a
/// terminal outcome without ever consulting the recovery policy that the *source* path
/// has always used. A few-second broker leader election therefore became a process
/// exit, a restart and a full replay from the last checkpoint.
///
/// The sink is an HTTP endpoint on a closed loopback port, so every send fails with a
/// connection error — recoverable, and immediate, so the test neither sleeps nor
/// depends on timing. The breaker escalates after a bounded number of attempts, which
/// is what stops this test from hanging and is what an operator gets when a sink is
/// genuinely down rather than briefly unavailable.
#[tokio::test]
async fn a_transient_sink_failure_is_retried_under_the_recovery_policy_then_escalates() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.jsonl");

    let mut pipeline = Pipeline::start_with_unreachable_sink(dir.path(), &output, 3, 3).await;

    let started = std::time::Instant::now();
    let batch = pipeline.runtime.poll_event_batch().await.expect("poll");
    let outcome = handle_polled_batch(
        &mut pipeline.runtime,
        &mut pipeline.router,
        &pipeline.transform_pipeline,
        &pipeline.admin_state,
        &pipeline.checkpoint_age_source,
        &mut pipeline.reconciler,
        &pipeline.loop_config,
        &pipeline.recovery_policy,
        &mut pipeline.recoverable,
        &mut pipeline.metrics,
        &pipeline.sink_metrics,
        None,
        None,
        batch,
    )
    .await;

    assert!(
        outcome.is_some(),
        "a permanently failing sink must eventually escalate rather than retry forever"
    );
    let snapshot = pipeline.recoverable.snapshot();
    assert!(
        snapshot.total > 1,
        "the batch must have been retried under the policy before escalating — one \
         attempt means the sink path is still bypassing it (total attempts: {})",
        snapshot.total
    );
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(1),
        "the retries must actually back off rather than spin"
    );
}

/// **While a batch is retried, the published verdict says delivery is failing.**
///
/// The verdict was latched only after a committed batch, so a sink that refused writes for
/// five minutes was reported `idle` for five minutes. And once the runtime's own verdict
/// was consulted, a held batch looked like a poll loop that stopped turning — the cause
/// `/livez` restarts on, which cannot fix a destination that refuses writes.
///
/// A 1 s stall threshold and 200 ms backoffs: the retries outlast the threshold, and the
/// last publish before escalation must name the delivery.
#[tokio::test]
async fn a_failing_delivery_is_published_as_its_own_stall_while_the_batch_is_retried() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.jsonl");
    let mut pipeline =
        Pipeline::start_with_unreachable_sink_and_threshold(dir.path(), &output, 3, 3, Some(1_000))
            .await;
    pipeline.recovery_policy.initial_backoff_ms = 200;
    pipeline.recovery_policy.max_backoff_ms = 200;
    pipeline.recovery_policy.breaker_consecutive_threshold = 8;
    pipeline.recoverable = RecoverableErrorState::new(200);

    let batch = pipeline.runtime.poll_event_batch().await.expect("poll");
    let outcome = handle_polled_batch(
        &mut pipeline.runtime,
        &mut pipeline.router,
        &pipeline.transform_pipeline,
        &pipeline.admin_state,
        &pipeline.checkpoint_age_source,
        &mut pipeline.reconciler,
        &pipeline.loop_config,
        &pipeline.recovery_policy,
        &mut pipeline.recoverable,
        &mut pipeline.metrics,
        &pipeline.sink_metrics,
        None,
        None,
        batch,
    )
    .await;
    assert!(
        outcome.is_some(),
        "the unreachable sink must eventually escalate"
    );

    let health = pipeline.admin_state.health_snapshot_for_tests().await;
    assert_eq!(
        health,
        (
            Some("stalled".to_string()),
            Some("delivery_failing".to_string())
        ),
        "the verdict published during the retries must name the failing delivery"
    );
    assert_eq!(
        pipeline.admin_state.batches_processed_for_tests().await,
        0,
        "publishing health during a retry must not count a batch that never delivered"
    );
}

/// **A shutdown request is honoured during a retry backoff.**
///
/// The event loop's signal arm cannot run while a batch is held, so SIGTERM during a
/// backoff was ignored until the batch delivered or escalated — an orchestrator then
/// SIGKILLs. The batch stays uncommitted and redelivers from the checkpoint.
#[tokio::test]
async fn a_shutdown_request_interrupts_a_retry_backoff() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.jsonl");
    let mut pipeline = Pipeline::start_with_unreachable_sink(dir.path(), &output, 3, 3).await;
    // A minute between attempts: without the signal the test would hang here.
    pipeline.recovery_policy.initial_backoff_ms = 60_000;
    pipeline.recovery_policy.max_backoff_ms = 60_000;
    pipeline.recoverable = RecoverableErrorState::new(60_000);

    let (trigger, mut signals) = super::event_loop::ShutdownSignals::for_tests();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = trigger.send(());
    });

    let started = std::time::Instant::now();
    let batch = pipeline.runtime.poll_event_batch().await.expect("poll");
    let outcome = handle_polled_batch(
        &mut pipeline.runtime,
        &mut pipeline.router,
        &pipeline.transform_pipeline,
        &pipeline.admin_state,
        &pipeline.checkpoint_age_source,
        &mut pipeline.reconciler,
        &pipeline.loop_config,
        &pipeline.recovery_policy,
        &mut pipeline.recoverable,
        &mut pipeline.metrics,
        &pipeline.sink_metrics,
        None,
        Some(&mut signals),
        batch,
    )
    .await;

    assert!(
        matches!(
            outcome,
            Some(crate::runtime::lifecycle::RuntimeLoopOutcome::Clean(
                crate::runtime::lifecycle::RuntimeTerminalReasonCode::ShutdownSignal
            ))
        ),
        "a shutdown during the backoff must stop cleanly"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "the backoff must not run to completion first"
    );
    assert!(
        pipeline.runtime.stop().await.is_err(),
        "the held batch stays uncommitted, so it redelivers from the checkpoint"
    );
}

/// **A poison event must not be retried.**
///
/// An event that exceeds `runtime.max_event_bytes` is the same size on every attempt.
/// Retrying it is an infinite loop that makes no forward progress and never reaches the
/// events behind it — a crash-loop that looks like a hang. `AppError::EventTooLarge`
/// exists as its own variant precisely so this decision is not made by matching on
/// message text.
#[tokio::test]
async fn an_oversized_event_fails_immediately_without_retrying() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.jsonl");

    // 1 byte: every event is over the limit.
    let mut pipeline =
        Pipeline::start_with_max_event_bytes(dir.path(), &output, 3, 3, &[], 1).await;

    let batch = pipeline.runtime.poll_event_batch().await.expect("poll");
    let outcome = handle_polled_batch(
        &mut pipeline.runtime,
        &mut pipeline.router,
        &pipeline.transform_pipeline,
        &pipeline.admin_state,
        &pipeline.checkpoint_age_source,
        &mut pipeline.reconciler,
        &pipeline.loop_config,
        &pipeline.recovery_policy,
        &mut pipeline.recoverable,
        &mut pipeline.metrics,
        &pipeline.sink_metrics,
        None,
        None,
        batch,
    )
    .await;

    assert!(outcome.is_some(), "an oversized event must fail the batch");
    assert_eq!(
        pipeline.recoverable.snapshot().total,
        0,
        "a deterministic failure must not consume a single retry"
    );
}

/// **A poison event is quarantined and the pipeline keeps going.**
///
/// Without a dead-letter target an undeliverable event is terminal — the pipeline
/// halts, restarts, replays, and fails on the same event again: a crash-loop that never
/// reaches the events behind it. That was the only behaviour available for every sink
/// except HTTP.
///
/// The oversized event here is permanently undeliverable by construction (it is the
/// same size on every attempt), so it is exactly what quarantine is for. The
/// assertions that matter are that the batch *succeeds* and that the record lands with
/// enough context to act on.
#[tokio::test]
async fn a_poison_event_is_quarantined_and_the_batch_completes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.jsonl");
    let dlq_path = dir.path().join("dlq.jsonl");

    // 1 byte: every event is over the limit and therefore permanently undeliverable.
    let mut pipeline =
        Pipeline::start_with_max_event_bytes(dir.path(), &output, 3, 3, &[], 1).await;

    let dlq = tokio::sync::Mutex::new(
        crate::dlq::DeadLetterQueue::build(&crate::config::dlq::DlqConfig {
            enabled: true,
            target: crate::config::dlq::DlqTarget::File(crate::config::dlq::FileDlqConfig {
                path: dlq_path.display().to_string(),
                max_bytes: 1 << 20,
            }),
        })
        .await
        .expect("dlq builds")
        .expect("enabled dlq is present"),
    );

    let batch = pipeline.runtime.poll_event_batch().await.expect("poll");
    let outcome = handle_polled_batch(
        &mut pipeline.runtime,
        &mut pipeline.router,
        &pipeline.transform_pipeline,
        &pipeline.admin_state,
        &pipeline.checkpoint_age_source,
        &mut pipeline.reconciler,
        &pipeline.loop_config,
        &pipeline.recovery_policy,
        &mut pipeline.recoverable,
        &mut pipeline.metrics,
        &pipeline.sink_metrics,
        Some(&dlq),
        None,
        batch,
    )
    .await;

    assert!(
        outcome.is_none(),
        "with quarantine configured the batch must complete rather than terminate the \
         pipeline — otherwise the poison event still blocks everything behind it"
    );

    let quarantined = std::fs::read_to_string(&dlq_path).expect("dlq file written");
    let lines: Vec<&str> = quarantined.lines().collect();
    assert_eq!(lines.len(), 3, "every undeliverable event must be recorded");

    let first: serde_json::Value = serde_json::from_str(lines[0]).expect("valid JSONL");
    assert_eq!(first["table"], "public.ledger");
    assert_eq!(
        first["source_offset"], "1",
        "the source offset is what tells an operator which position was skipped"
    );
    assert!(
        first["error"]
            .as_str()
            .expect("error is a string")
            .contains("max_event_bytes"),
        "the record must say why the event could not be delivered: {first}"
    );
}

/// **Without a dead-letter target, a poison event still halts the pipeline.**
///
/// This is the safe default and it must stay that way. Quarantining advances the
/// checkpoint past an event that was never delivered — data loss, recorded rather than
/// silent, but loss. An operator has to choose that; the alternative (stop and page
/// someone) is correct for a pipeline whose contents matter more than its uptime.
#[tokio::test]
async fn without_a_dead_letter_target_a_poison_event_still_halts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.jsonl");

    let mut pipeline =
        Pipeline::start_with_max_event_bytes(dir.path(), &output, 3, 3, &[], 1).await;

    let batch = pipeline.runtime.poll_event_batch().await.expect("poll");
    let outcome = handle_polled_batch(
        &mut pipeline.runtime,
        &mut pipeline.router,
        &pipeline.transform_pipeline,
        &pipeline.admin_state,
        &pipeline.checkpoint_age_source,
        &mut pipeline.reconciler,
        &pipeline.loop_config,
        &pipeline.recovery_policy,
        &mut pipeline.recoverable,
        &mut pipeline.metrics,
        &pipeline.sink_metrics,
        None,
        None,
        batch,
    )
    .await;

    assert!(
        outcome.is_some(),
        "silently dropping data must never be the default"
    );
}

/// The happy path: with no faults, every event is delivered exactly once.
///
/// This is the baseline the crash tests are read against. Without it, "no loss after a
/// crash" could be satisfied by a pipeline that duplicates everything unconditionally.
#[tokio::test]
async fn at_least_once_delivers_every_event_once_when_nothing_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.jsonl");

    let mut pipeline = Pipeline::start(dir.path(), &output, 6, 3, &[]).await;
    pipeline.pump(4).await;
    pipeline.crash();

    assert_eq!(
        delivered_ids(&output),
        vec![1, 2, 3, 4, 5, 6],
        "a run with no faults must deliver each event once, in order"
    );
}

/// **The at_least_once contract.** A crash between delivery and the durable checkpoint
/// must replay, never skip.
///
/// The second batch reaches the sink and its checkpoint write is then lost — the exact
/// window the contract is defined around. The restart must resume from the *first*
/// batch's position and re-deliver, so the union of both incarnations covers every
/// event. A pipeline that checkpointed before delivering would lose events 4–6 here,
/// and the offset assertion is what distinguishes the two: it fails if the restart
/// resumed from anywhere but the last durable position.
#[tokio::test]
async fn at_least_once_loses_nothing_when_a_checkpoint_write_is_lost() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.jsonl");

    // Batch 1 (events 1–3) commits normally; batch 2 (events 4–6) reaches the sink and
    // its checkpoint write then fails.
    let mut first = Pipeline::start(dir.path(), &output, 6, 3, &[]).await;
    first.pump(1).await;
    assert_eq!(
        delivered_ids(&output),
        vec![1, 2, 3],
        "the first batch must be delivered before the fault is injected"
    );
    // The schema-history store refuses two live instances against one directory, so
    // each incarnation must be gone before the next starts — which is also what makes
    // this a restart rather than two concurrent writers.
    first.crash();

    let mut crashing =
        Pipeline::start(dir.path(), &output, 6, 3, &[CheckpointFault::FailSave]).await;
    assert_eq!(
        crashing.resumed_from(),
        3,
        "the second incarnation must resume from the first batch's durable position"
    );
    crashing.pump(1).await;
    crashing.crash();

    // Restart with a healthy checkpoint store.
    let restarted = Pipeline::start(dir.path(), &output, 6, 3, &[]).await;
    assert_eq!(
        restarted.resumed_from(),
        3,
        "the failed checkpoint write must not have advanced the durable position — \
         resuming past it is silent data loss, which is what this contract forbids"
    );

    let mut restarted = restarted;
    restarted.pump(2).await;

    let delivered = delivered_ids(&output);
    let mut covered: Vec<u64> = delivered.clone();
    covered.sort_unstable();
    covered.dedup();
    assert_eq!(
        covered,
        vec![1, 2, 3, 4, 5, 6],
        "no event may be missing after a crash: at_least_once permits duplicates, \
         never gaps (delivered sequence was {delivered:?})"
    );
    assert!(
        delivered.len() > covered.len(),
        "the replay must actually have re-delivered the uncheckpointed batch — if this \
         fails the fault was not injected and the test proves nothing (got {delivered:?})"
    );
}

/// A crash *between* batches replays nothing: the durable position is exact, not
/// approximate.
///
/// Over-replay is permitted by the contract but it is not free — it is duplicate load
/// on every downstream consumer. This pins the checkpoint to the batch boundary so a
/// regression that rounds the position backwards shows up as a test failure rather
/// than as a support ticket about duplicate rows.
#[tokio::test]
async fn a_clean_batch_boundary_replays_nothing_on_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.jsonl");

    let mut first = Pipeline::start(dir.path(), &output, 6, 3, &[]).await;
    first.pump(1).await;
    first.crash();

    let mut second = Pipeline::start(dir.path(), &output, 6, 3, &[]).await;
    assert_eq!(second.resumed_from(), 3);
    second.pump(1).await;

    assert_eq!(
        delivered_ids(&output),
        vec![1, 2, 3, 4, 5, 6],
        "a crash on a committed batch boundary must not re-deliver anything"
    );
}

/// **The deliver-then-checkpoint window is real**, for a sink with no transaction.
///
/// A crash between the sink accepting a batch and the checkpoint landing replays that
/// batch on restart. For `at_least_once` that is the contract working as specified, not a
/// defect, and this reproduces it end to end through the production batch handler.
///
/// # What this does *not* pin
///
/// This test previously carried a docstring claiming it was the tripwire for the
/// `effectively_once` crash window — that closing the window "**must fail** this test".
/// It could not. The harness uses a `file_jsonl` sink, which has no transactional barrier
/// and no `effectively_once` guarantee of any kind, so the window it reproduces is the
/// inherent `at_least_once` one, which will never close.
///
/// The genuine `effectively_once` window — the sink transaction commits, then the
/// checkpoint is written outside it — therefore has **no** regression tripwire. Closing it
/// requires writing the checkpoint record inside the sink's transaction, and needs a
/// Kafka-based crash harness this file cannot express: the checkpoint would have to be
/// written inside the sink's transaction, which requires the checkpoint to live in Kafka.
#[tokio::test]
async fn the_at_least_once_crash_window_replays_the_uncheckpointed_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.jsonl");

    let mut crashing =
        Pipeline::start(dir.path(), &output, 3, 3, &[CheckpointFault::FailSave]).await;
    crashing.pump(1).await;
    crashing.crash();

    let delivered_before = delivered_ids(&output);
    assert_eq!(
        delivered_before,
        vec![1, 2, 3],
        "the batch must have reached the sink before the checkpoint was lost"
    );

    let mut restarted = Pipeline::start(dir.path(), &output, 3, 3, &[]).await;
    assert_eq!(
        restarted.resumed_from(),
        0,
        "no checkpoint survived, so the restart replays from the beginning"
    );
    restarted.pump(1).await;

    assert_eq!(
        delivered_ids(&output),
        vec![1, 2, 3, 1, 2, 3],
        "at_least_once replays an uncheckpointed batch, which is the contract. If \
         duplicates stop appearing here, something has changed the checkpoint ordering \
         for a non-transactional sink — that is a behaviour change to investigate, not \
         an improvement to accept silently."
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// A lost replication slot
// ─────────────────────────────────────────────────────────────────────────────

/// Wait until the published verdict names the lost slot, then return the `/metrics` body.
async fn await_parked_verdict(admin_state: &AdminState) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let health = admin_state.health_snapshot_for_tests().await;
        if health
            == (
                Some("stalled".to_string()),
                Some("source_position_lost".to_string()),
            )
        {
            return admin_state.metrics_exposition().await;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the verdict was never published as stalled/source_position_lost: {health:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Drive `parked` to completion while `observe` runs, and report whether `parked` returned
/// only after `observe` released it.
async fn returns_only_when_released<F, O>(
    parked: F,
    observe: impl FnOnce(Arc<std::sync::atomic::AtomicBool>) -> O,
) -> (F::Output, bool)
where
    F: std::future::Future,
    O: std::future::Future<Output = ()>,
{
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watched = {
        let released = Arc::clone(&released);
        async move {
            let outcome = parked.await;
            let was_released = released.load(std::sync::atomic::Ordering::SeqCst);
            (outcome, was_released)
        }
    };
    let (result, ()) = tokio::join!(watched, observe(released));
    result
}

/// **A slot the source invalidated mid-run parks the process instead of ending it.**
///
/// Exiting hands the orchestrator a restart that replays into the same refusal, and a
/// restart cannot recreate the discarded WAL. The loop must publish the verdict — on the
/// `/metrics` body an alert reads, not only on the latch — stay up, and stop cleanly when
/// asked.
#[tokio::test]
async fn the_server_parks_rather_than_exits_on_a_lost_slot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.jsonl");
    let mut pipeline = Pipeline::start_with_lost_slot(dir.path(), &output, LostSlot::OnPoll).await;
    let (trigger, signals) = super::event_loop::ShutdownSignals::for_tests();
    let admin_state = pipeline.admin_state.clone();
    let mut admin_exit_rx = None;

    let parked = super::event_loop::execute_event_loop(
        &mut pipeline.runtime,
        &mut pipeline.router,
        &pipeline.transform_pipeline,
        &pipeline.admin_state,
        &mut admin_exit_rx,
        &pipeline.checkpoint_age_source,
        &mut pipeline.reconciler,
        &pipeline.sink_metrics,
        None,
        loop_config_for(&config_for(dir.path(), &output)),
        signals,
    );
    let (outcome, released) = returns_only_when_released(parked, |released| async move {
        let metrics = await_parked_verdict(&admin_state).await;
        assert!(
            metrics.contains("cause=\"source_position_lost\"} 1"),
            "the /metrics body must carry the cause an alert routes on:\n{metrics}"
        );
        // Long enough for an exit, or a retry loop, to have happened.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        released.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = trigger.send(());
    })
    .await;

    assert!(released, "the loop returned before it was told to stop");
    assert!(
        matches!(
            outcome,
            crate::runtime::lifecycle::RuntimeLoopOutcome::Clean(
                crate::runtime::lifecycle::RuntimeTerminalReasonCode::ShutdownSignal
            )
        ),
        "a parked process must stop cleanly on a signal"
    );
}

/// **A slot found invalidated at startup parks exactly as one lost mid-run does.**
#[tokio::test]
async fn a_lost_slot_at_startup_parks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.jsonl");
    let mut pipeline = Pipeline::start_with_lost_slot(dir.path(), &output, LostSlot::OnStart).await;
    pipeline
        .admin_state
        .set_state(crate::admin::InstanceState::Running)
        .await;
    let (trigger, mut signals) = super::event_loop::ShutdownSignals::for_tests();
    let admin_state = pipeline.admin_state.clone();
    let mut admin_exit_rx = None;
    let metrics = RuntimeLoopMetricsAccumulator::new(
        "file_jsonl",
        DeliveryContract::AtLeastOnce.as_label(),
        true,
        "at_least_once",
        false,
        false,
        16,
        1024,
    );

    let parked = crate::commands::run::start_or_park(
        &mut pipeline.runtime,
        &pipeline.admin_state,
        &mut admin_exit_rx,
        metrics,
        &mut signals,
    );
    let (outcome, released) = returns_only_when_released(parked, |released| async move {
        await_parked_verdict(&admin_state).await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        released.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = trigger.send(());
    })
    .await;

    assert!(
        released,
        "start_or_park returned before it was told to stop"
    );
    assert!(
        matches!(
            outcome,
            Ok(Some(crate::runtime::lifecycle::RuntimeLoopOutcome::Clean(
                crate::runtime::lifecycle::RuntimeTerminalReasonCode::ShutdownSignal
            )))
        ),
        "a lost slot at startup must park and then stop cleanly, not fail the start"
    );
}
