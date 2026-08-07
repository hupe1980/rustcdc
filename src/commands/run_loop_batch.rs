use rustcdc::core::CdcRuntime;
use rustcdc::sink::SinkAdapter;

use crate::{admin::AdminState, pipeline::transform, state::CheckpointAgeSource};

use super::run_batch;
use super::run_lifecycle::{RuntimeLoopOutcome, RuntimeTerminalReasonCode};
use super::run_loop::RuntimeLoopConfig;
use super::run_metrics::RuntimeLoopMetricsAccumulator;
use super::run_reconciliation::CheckpointTxnReconciler;
use super::run_recovery::{RecoverableErrorState, RecoveryAction, RecoveryPolicyConfig};

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
    dlq: Option<&tokio::sync::Mutex<crate::dlq::DeadLetterQueue>>,
    batch: rustcdc::core::EventBatch,
) -> Option<RuntimeLoopOutcome> {
    tracing::trace!(batch_len = batch.len(), "handle_polled_batch: enter");
    let batch_delivery_started = std::time::Instant::now();
    let event_count = batch.len() as u64;
    let delivery_before = crate::pipeline::router::delivery_metrics(sink);
    let ack_mode = batch.ack_mode();
    let token_present = ack_mode.is_required();
    // Deliver the batch, retrying the **same events** under the recovery policy.
    //
    // Sink failures used to be unconditionally terminal while source failures got
    // backoff and a circuit breaker, so a few-second broker leader election became a
    // process exit and a full replay from the last checkpoint. The retry happens here,
    // holding the batch, rather than by returning to the poll loop: the events have
    // already been taken from the runtime, and returning without committing would let
    // the next poll advance past them.
    //
    // Re-sending events that a partial failure already delivered is duplication, which
    // `at_least_once` permits by definition. Under `effectively_once` the aborted
    // barrier discards them, so a `read_committed` consumer sees only the retry.
    let (transactional_barrier_active, batch_processing_stats) = loop {
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

        match process_batch_events_with_barrier_abort(
            sink,
            transform_pipeline,
            config,
            batch.events().iter().cloned(),
            transactional_barrier_active,
            dlq,
        )
        .await
        {
            Ok(stats) => break (transactional_barrier_active, stats),
            Err(err) => {
                if transactional_barrier_active && token_present {
                    if let Err(clear_err) = checkpoint_txn_reconciler.clear() {
                        tracing::warn!(
                            error = %clear_err,
                            "failed to clear checkpoint-transaction reconciliation marker after batch delivery error"
                        );
                    }
                }

                if !err.is_recoverable() {
                    tracing::error!(error = %err, "unrecoverable sink delivery error");
                    return Some(RuntimeLoopOutcome::error(
                        RuntimeTerminalReasonCode::BatchDeliveryError,
                        err,
                    ));
                }

                match recoverable
                    .on_recoverable_error(recovery_policy, super::run_recovery::jitter_seed())
                {
                    RecoveryAction::Retry {
                        consecutive,
                        backoff_ms,
                        ..
                    } => {
                        tracing::warn!(
                            error = %err,
                            consecutive,
                            backoff_ms,
                            "recoverable sink delivery error; backing off before retrying the batch"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    }
                    RecoveryAction::BreakerCooldown {
                        consecutive,
                        breaker_open_consecutive,
                        cooldown_ms,
                    } => {
                        tracing::error!(
                            error = %err,
                            consecutive,
                            breaker_open_consecutive,
                            cooldown_ms,
                            "sink delivery circuit-breaker opened; cooling down before retrying the batch"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(cooldown_ms)).await;
                    }
                    RecoveryAction::Escalate { message } => {
                        return Some(RuntimeLoopOutcome::error(
                            RuntimeTerminalReasonCode::BatchDeliveryError,
                            crate::error::AppError::Other(format!("{message}: {err}")),
                        ));
                    }
                }
            }
        }
    };

    metrics_accumulator.merge_batch_processing_stats(&batch_processing_stats);

    // ── Checkpoint *before* the barrier commit ───────────────────────────────
    //
    // This ordering is what makes `effectively_once` exactly-once end to end rather than
    // only atomic at the sink.
    //
    // `commit_ack` writes the durable position through the checkpoint store. With a Kafka
    // state backend sharing the sink's producer, that write lands **inside the open
    // transaction**, so the single `commit_transaction` below makes the batch's data and
    // the position that describes it durable together. There is no window between them:
    // a crash before the commit discards both and the batch replays cleanly; a crash after
    // it keeps both and the batch is not replayed.
    //
    // The previous order — commit the barrier, then checkpoint — left exactly that window
    // open, and a crash inside it re-delivered the batch to a `read_committed` consumer.
    //
    // For any other state backend the checkpoint is a separate durability domain and the
    // window remains; the configuration loader refuses that combination under
    // `effectively_once` rather than letting it look closed.
    let checkpoint_commit_latency_us =
        match commit_checkpoint_ack_if_present(runtime, ack_mode).await {
            Ok(latency) => latency,
            Err(outcome) => {
                // The checkpoint is part of the transaction now, so a failure here must
                // discard the data too. Without the abort the records would stay in an
                // open transaction that nothing will ever commit or roll back, blocking
                // the partition's last stable offset until the transaction times out.
                if transactional_barrier_active {
                    if let Err(abort_err) = sink.abort_checkpoint_barrier().await {
                        tracing::warn!(
                            error = %abort_err,
                            "transactional checkpoint barrier abort failed after a checkpoint error"
                        );
                    }
                }
                return Some(outcome);
            }
        };

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

    if transactional_barrier_active {
        if let Err(err) = checkpoint_txn_reconciler.clear() {
            return Some(RuntimeLoopOutcome::error(
                RuntimeTerminalReasonCode::CheckpointReconciliationError,
                err,
            ));
        }
    }

    // `mark_success` belongs *here*, not at batch entry. Called on entry it meant
    // "a batch was attempted", so a failing batch reset the very counters the circuit
    // breaker uses to decide whether failures are persistent.
    recoverable.mark_success(recovery_policy.initial_backoff_ms, recovery_policy);

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
        checkpoint_commit_latency_us,
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
    events: impl IntoIterator<Item = rustcdc::core::Event>,
    transactional_barrier_active: bool,
    dlq: Option<&tokio::sync::Mutex<crate::dlq::DeadLetterQueue>>,
) -> Result<run_batch::BatchProcessingStats, crate::error::AppError> {
    match run_batch::process_batch_events(
        sink,
        events,
        transform_pipeline,
        config.prepare_parallelism,
        config.sink_flush_interval_events,
        config.sink_delivery_queue_capacity,
        config.sink_send_timeout_ms,
        config.sink_flush_timeout_ms,
        dlq,
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
            Err(err)
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

    Ok(Some(checkpoint_commit_started.elapsed().as_micros() as u64))
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
    checkpoint_commit_latency_us: Option<u64>,
) {
    if let Some(latency) = checkpoint_commit_latency_us {
        metrics_accumulator.record_checkpoint_commit_latency(latency);
    }

    metrics_accumulator
        .update_transform_metrics(transform_pipeline)
        .await;

    let batch_delivery_latency_us = batch_delivery_started.elapsed().as_micros() as u64;
    metrics_accumulator.record_batch_delivery_latency(batch_delivery_latency_us);

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
