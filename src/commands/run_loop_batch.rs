use rustcdc::core::CdcRuntime;
use rustcdc::sink::SinkAdapter;

use crate::{admin::AdminState, pipeline::transform, state::CheckpointAgeSource};

use super::run_batch;
use super::run_lifecycle::{RuntimeLoopOutcome, RuntimeTerminalReasonCode};
use super::run_loop::RuntimeLoopConfig;
use super::run_metrics::RuntimeLoopMetricsAccumulator;
use super::run_reconciliation::CheckpointTxnReconciler;
use super::run_recovery::{RecoverableErrorState, RecoveryPolicyConfig};

#[allow(clippy::too_many_arguments)] // internal wiring of long-lived pipeline components
pub(super) async fn handle_polled_batch(
    runtime: &mut CdcRuntime,
    sink: &mut crate::pipeline::router::TableRouter,
    transform_pipeline: &transform::TransformPipeline,
    admin_state: &AdminState,
    checkpoint_age_source: &CheckpointAgeSource,
    checkpoint_txn_reconciler: &mut CheckpointTxnReconciler,
    config: &RuntimeLoopConfig,
    recovery_policy: &RecoveryPolicyConfig,
    recoverable: &mut RecoverableErrorState,
    metrics_accumulator: &mut RuntimeLoopMetricsAccumulator,
    batch: rustcdc::core::EventBatch,
) -> Option<RuntimeLoopOutcome> {
    tracing::trace!(batch_len = batch.len(), "handle_polled_batch: enter");
    let batch_delivery_started = std::time::Instant::now();
    recoverable.mark_success(recovery_policy.initial_backoff_ms, recovery_policy);
    let event_count = batch.len() as u64;
    let delivery_before = crate::pipeline::router::delivery_metrics(sink);
    let ack_mode = batch.ack_mode();
    let token_present = ack_mode.is_required();
    let transactional_barrier_active = match begin_transactional_barrier_if_needed(sink).await {
        Ok(active) => active,
        Err(outcome) => return Some(outcome),
    };

    if transactional_barrier_active && token_present {
        if let Err(err) = checkpoint_txn_reconciler.arm(event_count) {
            return Some(RuntimeLoopOutcome::error(
                RuntimeTerminalReasonCode::CheckpointReconciliationError,
                err,
            ));
        }
    }

    let batch_processing_stats = match process_batch_events_with_barrier_abort(
        sink,
        transform_pipeline,
        config,
        batch,
        transactional_barrier_active,
    )
    .await
    {
        Ok(stats) => stats,
        Err(outcome) => {
            if transactional_barrier_active && token_present {
                if let Err(err) = checkpoint_txn_reconciler.clear() {
                    tracing::warn!(
                        error = %err,
                        "failed to clear checkpoint-transaction reconciliation marker after batch delivery error"
                    );
                }
            }
            return Some(outcome);
        }
    };

    metrics_accumulator.merge_batch_processing_stats(&batch_processing_stats);

    if let Err(outcome) =
        commit_transactional_barrier_if_needed(sink, transactional_barrier_active).await
    {
        return Some(outcome);
    }

    if transactional_barrier_active && token_present {
        if let Err(err) = checkpoint_txn_reconciler.validate_armed_event_count(event_count) {
            return Some(RuntimeLoopOutcome::error(
                RuntimeTerminalReasonCode::CheckpointReconciliationError,
                err,
            ));
        }
    }

    if transactional_barrier_active && !token_present {
        if let Err(err) = checkpoint_txn_reconciler.clear() {
            return Some(RuntimeLoopOutcome::error(
                RuntimeTerminalReasonCode::CheckpointReconciliationError,
                err,
            ));
        }
    }

    let checkpoint_commit_latency_ms =
        match commit_checkpoint_ack_if_present(runtime, ack_mode).await {
            Ok(latency) => latency,
            Err(outcome) => return Some(outcome),
        };

    if transactional_barrier_active && token_present {
        if let Err(err) = checkpoint_txn_reconciler.clear() {
            return Some(RuntimeLoopOutcome::error(
                RuntimeTerminalReasonCode::CheckpointReconciliationError,
                err,
            ));
        }
    }

    // Correctness KPIs are now post-durable: only record after the batch has
    // successfully reached sink durability and checkpoint commit.
    for sample in &batch_processing_stats.committed_correctness_samples {
        metrics_accumulator.record_correctness_sample(sample);
    }

    tracing::trace!("handle_polled_batch: recording metrics");
    record_batch_metrics_and_admin(
        runtime,
        sink,
        transform_pipeline,
        admin_state,
        checkpoint_age_source,
        recoverable,
        metrics_accumulator,
        event_count,
        delivery_before,
        batch_delivery_started,
        checkpoint_commit_latency_ms,
    )
    .await;
    tracing::trace!("handle_polled_batch: done");

    None
}

async fn begin_transactional_barrier_if_needed(
    sink: &mut crate::pipeline::router::TableRouter,
) -> Result<bool, RuntimeLoopOutcome> {
    let transactional_barrier_active = sink.transactional_checkpoint_barrier_capable();
    if transactional_barrier_active {
        sink.begin_checkpoint_barrier().await.map_err(|e| {
            RuntimeLoopOutcome::runtime_error(
                RuntimeTerminalReasonCode::CheckpointBarrierBeginError,
                e,
            )
        })?;
    }
    Ok(transactional_barrier_active)
}

async fn process_batch_events_with_barrier_abort(
    sink: &mut crate::pipeline::router::TableRouter,
    transform_pipeline: &transform::TransformPipeline,
    config: &RuntimeLoopConfig,
    batch: rustcdc::core::EventBatch,
    transactional_barrier_active: bool,
) -> Result<run_batch::BatchProcessingStats, RuntimeLoopOutcome> {
    match run_batch::process_batch_events(
        sink,
        batch.events().iter().cloned(),
        transform_pipeline,
        config.max_event_bytes,
        config.prepare_parallelism,
        config.sink_flush_interval_events,
        config.sink_delivery_queue_capacity,
        config.sink_send_timeout_ms,
        config.sink_flush_timeout_ms,
    )
    .await
    {
        Ok(stats) => Ok(stats),
        Err(err) => {
            if transactional_barrier_active {
                if let Err(abort_err) = sink.abort_checkpoint_barrier().await {
                    tracing::warn!(
                        error = %abort_err,
                        "transactional checkpoint barrier abort failed after delivery error"
                    );
                }
            }
            Err(RuntimeLoopOutcome::error(
                RuntimeTerminalReasonCode::BatchDeliveryError,
                err,
            ))
        }
    }
}

async fn commit_transactional_barrier_if_needed(
    sink: &mut crate::pipeline::router::TableRouter,
    transactional_barrier_active: bool,
) -> Result<(), RuntimeLoopOutcome> {
    if !transactional_barrier_active {
        return Ok(());
    }

    match sink.commit_checkpoint_barrier().await {
        Ok(_) => Ok(()),
        Err(commit_err) => {
            if let Err(abort_err) = sink.abort_checkpoint_barrier().await {
                tracing::warn!(
                    error = %abort_err,
                    "transactional checkpoint barrier abort failed after commit failure"
                );
            }
            Err(RuntimeLoopOutcome::runtime_error(
                RuntimeTerminalReasonCode::CheckpointBarrierCommitError,
                commit_err,
            ))
        }
    }
}

async fn commit_checkpoint_ack_if_present(
    runtime: &mut CdcRuntime,
    ack_mode: rustcdc::core::AckMode,
) -> Result<Option<u64>, RuntimeLoopOutcome> {
    if !ack_mode.is_required() {
        return Ok(None);
    }

    let checkpoint_commit_started = std::time::Instant::now();
    runtime.commit_ack(ack_mode).await.map_err(|err| {
        RuntimeLoopOutcome::runtime_error(RuntimeTerminalReasonCode::CheckpointCommitError, err)
    })?;

    Ok(Some(checkpoint_commit_started.elapsed().as_millis() as u64))
}

#[allow(clippy::too_many_arguments)] // internal wiring of long-lived pipeline components
async fn record_batch_metrics_and_admin(
    runtime: &CdcRuntime,
    sink: &crate::pipeline::router::TableRouter,
    transform_pipeline: &transform::TransformPipeline,
    admin_state: &AdminState,
    checkpoint_age_source: &CheckpointAgeSource,
    recoverable: &RecoverableErrorState,
    metrics_accumulator: &mut RuntimeLoopMetricsAccumulator,
    event_count: u64,
    delivery_before: crate::sink::SinkDeliveryMetrics,
    batch_delivery_started: std::time::Instant,
    checkpoint_commit_latency_ms: Option<u64>,
) {
    if let Some(latency) = checkpoint_commit_latency_ms {
        metrics_accumulator.record_checkpoint_commit_latency(latency);
    }

    metrics_accumulator
        .update_wasm_metrics(transform_pipeline)
        .await;

    let batch_delivery_latency_ms = batch_delivery_started.elapsed().as_millis() as u64;
    metrics_accumulator.record_batch_delivery_latency(batch_delivery_latency_ms);

    let sink_queue_depth_last = sink.queue_depth().unwrap_or(0) as u64;
    metrics_accumulator.record_sink_queue_depth(sink_queue_depth_last);

    let delivery_after = crate::pipeline::router::delivery_metrics(sink);
    metrics_accumulator.record_sink_delivery_delta(delivery_before, delivery_after);

    let recovery_snapshot = recoverable.snapshot();
    let admin_snapshot = runtime.admin_snapshot();
    metrics_accumulator.observe_health_transition(&admin_snapshot);
    let runtime_metrics =
        metrics_accumulator.build_runtime_metrics_snapshot(admin_snapshot, recovery_snapshot);
    let checkpoint_age_seconds = checkpoint_age_source.checkpoint_age_seconds().await;

    admin_state
        .record_batch(event_count, runtime_metrics, checkpoint_age_seconds)
        .await;
}
