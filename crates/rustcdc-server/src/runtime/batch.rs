use futures::{StreamExt, TryStreamExt};

use crate::cli::CheckpointParityMode;
use crate::config::schema::DeliveryContract;
use crate::error::{AppError, ConfigError};
use crate::pipeline::transform;
use rustcdc::sink::SinkAdapter;

use super::metrics::{
    CorrectnessSample, LATENCY_HISTOGRAM_BUCKETS_US, observe_latency_histogram_bucket,
};

#[derive(Debug, Default)]
pub struct BatchPrepareStats {
    pub(crate) transform_ops_total: u64,
    pub(crate) transform_latency_us_total: u64,
    pub(crate) transform_latency_us_last: u64,
    pub(crate) transform_latency_us_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_US.len()],
    pub(crate) prepare_ops_total: u64,
    pub(crate) prepare_latency_us_total: u64,
    pub(crate) prepare_latency_us_last: u64,
    pub(crate) prepare_latency_us_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_US.len()],
}

#[derive(Debug, Default)]
pub struct SinkDeliveryStats {
    /// Events quarantined to the dead-letter queue during this batch.
    pub(crate) dlq_events_total: u64,
    pub(crate) sink_send_ops_total: u64,
    pub(crate) sink_send_latency_us_total: u64,
    pub(crate) sink_send_latency_us_last: u64,
    pub(crate) sink_send_latency_us_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_US.len()],
    pub(crate) sink_flush_ops_total: u64,
    pub(crate) sink_flush_latency_us_total: u64,
    pub(crate) sink_flush_latency_us_last: u64,
    pub(crate) sink_flush_latency_us_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_US.len()],
}

#[derive(Debug, Default)]
pub struct BatchProcessingStats {
    pub(crate) prepare: BatchPrepareStats,
    pub(crate) delivery: SinkDeliveryStats,
    pub(crate) committed_correctness_samples: Vec<CorrectnessSample>,
    pub(crate) checkpoint_parity_requested: bool,
    pub(crate) checkpoint_parity_effective: bool,
    pub(crate) runtime_ack_commit_executed: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CheckpointParityPlan {
    pub(crate) requested: bool,
    pub(crate) effective: bool,
}

pub(crate) fn checkpoint_parity_plan(
    mode: CheckpointParityMode,
    sink: &crate::pipeline::router::TableRouter,
) -> CheckpointParityPlan {
    let sink_supports_barrier = sink.transactional_checkpoint_barrier_capable();
    match mode {
        CheckpointParityMode::Auto => CheckpointParityPlan {
            requested: sink_supports_barrier,
            effective: sink_supports_barrier,
        },
        CheckpointParityMode::Enabled => CheckpointParityPlan {
            requested: true,
            effective: sink_supports_barrier,
        },
        CheckpointParityMode::Disabled => CheckpointParityPlan {
            requested: false,
            effective: false,
        },
    }
}

/// Validate that `checkpoint_parity_mode` is compatible with `delivery_contract`
/// and the sink's capabilities.  Must be called once at startup, before the
/// batch loop begins.
///
/// # Errors
///
/// Returns `Err` when `delivery_contract = effectively_once`,
/// `checkpoint_parity_mode = enabled`, and the sink does not support
/// transactional checkpoint barriers.  Silently degrading in this scenario
/// would violate the operator's expressed delivery guarantee.
pub(crate) fn validate_parity_contract(
    mode: CheckpointParityMode,
    sink: &crate::pipeline::router::TableRouter,
    delivery_contract: DeliveryContract,
) -> Result<(), AppError> {
    if mode == CheckpointParityMode::Enabled
        && !sink.transactional_checkpoint_barrier_capable()
        && delivery_contract == DeliveryContract::EffectivelyOnce
    {
        return Err(AppError::Config(Box::new(ConfigError::Invalid(
            "checkpoint_parity_mode = enabled requires a sink that supports \
             transactional checkpoint barriers for effectively_once delivery, \
             but the configured sink does not.  Either use a Kafka sink, \
             set checkpoint_parity_mode = auto, or downgrade the delivery \
             contract."
                .to_string(),
        ))));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // internal wiring of long-lived pipeline components
pub async fn process_batch_events(
    sink: &mut crate::pipeline::router::TableRouter,
    events: impl IntoIterator<Item = rustcdc::core::Event>,
    transform_pipeline: &transform::TransformPipeline,
    prepare_parallelism: usize,
    flush_interval: usize,
    sink_delivery_queue_capacity: usize,
    sink_send_timeout_ms: u64,
    sink_flush_timeout_ms: u64,
    dlq: Option<&tokio::sync::Mutex<crate::dlq::DeadLetterQueue>>,
) -> Result<BatchProcessingStats, AppError> {
    struct EventPrepareResult {
        /// The transformed event and its correctness fingerprint, or `None` when a
        /// transform dropped the event.
        prepared: Option<(rustcdc::core::Event, CorrectnessSample)>,
        transform_latency_us: u64,
        prepare_latency_us: u64,
    }

    // The sample travels *with* the event rather than being collected here.
    //
    // The producer used to push every sample into its own vector, and the caller recorded
    // the lot once the batch was durable — under a comment saying correctness KPIs are
    // post-durable. They were not: an event the consumer dead-lettered had already had its
    // sample banked, so `rustcdc_data_events_total` and the duplicate/reorder rates counted
    // events that were quarantined and never delivered. Carrying the sample and collecting
    // it on the far side of a successful send makes the comment true.
    type Delivery = (rustcdc::core::Event, CorrectnessSample);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Delivery>(sink_delivery_queue_capacity.max(1));

    // `move` + the explicit `drop(tx)` below are load-bearing. The consumer's exit
    // condition is `rx.recv() == None`, which only happens once every sender is
    // gone. Capturing `tx` by reference (the pre-`move` behaviour) left the
    // original sender alive in this function's scope for as long as
    // `try_join!` ran — and `try_join!` was waiting on the consumer, which was
    // waiting on `recv()`. The result was a deadlock on every batch: all events
    // of the first batch were delivered, then the pipeline hung forever with no
    // checkpoint written and no error logged.
    let producer = async move {
        let mut per_event_results = futures::stream::iter(events.into_iter().map(|event| async {
            let prepare_started = std::time::Instant::now();

            let transform_started = std::time::Instant::now();
            let transformed = transform_pipeline.apply(event).await?;
            let transform_latency_us = transform_started.elapsed().as_micros() as u64;
            let prepared = transformed.map(|event| {
                let sample = CorrectnessSample::from_event(&event);
                (event, sample)
            });

            Ok::<EventPrepareResult, AppError>(EventPrepareResult {
                prepared,
                transform_latency_us,
                prepare_latency_us: prepare_started.elapsed().as_micros() as u64,
            })
        }))
        .buffered(prepare_parallelism.max(1));

        let mut stats = BatchPrepareStats::default();
        while let Some(event_result) = per_event_results.try_next().await? {
            stats.transform_ops_total = stats.transform_ops_total.saturating_add(1);
            stats.transform_latency_us_total = stats
                .transform_latency_us_total
                .saturating_add(event_result.transform_latency_us);
            stats.transform_latency_us_last = event_result.transform_latency_us;
            observe_latency_histogram_bucket(
                &mut stats.transform_latency_us_buckets,
                event_result.transform_latency_us,
            );

            stats.prepare_ops_total = stats.prepare_ops_total.saturating_add(1);
            stats.prepare_latency_us_total = stats
                .prepare_latency_us_total
                .saturating_add(event_result.prepare_latency_us);
            stats.prepare_latency_us_last = event_result.prepare_latency_us;
            observe_latency_histogram_bucket(
                &mut stats.prepare_latency_us_buckets,
                event_result.prepare_latency_us,
            );

            if let Some(delivery) = event_result.prepared {
                tx.send(delivery).await.map_err(|_| {
                    AppError::Other("sink delivery queue closed unexpectedly".to_string())
                })?;
            }
        }

        // Close the delivery channel so the consumer's `recv()` observes
        // end-of-batch and exits. A completed future inside `try_join!` is not
        // dropped until the join resolves, so relying on the future's drop to
        // release the sender would deadlock — the drop must be explicit.
        drop(tx);

        Ok::<BatchPrepareStats, AppError>(stats)
    };

    let consumer = async {
        let mut stats = SinkDeliveryStats::default();
        let mut committed_correctness_samples = Vec::new();
        let mut buffered_since_flush = 0usize;
        let flush_interval = flush_interval.max(1);
        let mut flush_ticker = sink.flush_tick_interval().map(tokio::time::interval);
        if let Some(ticker) = flush_ticker.as_mut() {
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        }

        loop {
            let maybe_event = if let Some(ticker) = flush_ticker.as_mut() {
                tokio::select! {
                    event = rx.recv() => event,
                    _ = ticker.tick() => {
                        if buffered_since_flush > 0 {
                            let sink_flush_started = std::time::Instant::now();
                            flush_sink_with_timeout(sink, sink_flush_timeout_ms).await?;
                            let sink_flush_latency_us = sink_flush_started.elapsed().as_micros() as u64;
                            record_sink_flush(&mut stats, sink_flush_latency_us);
                            buffered_since_flush = 0;
                        }
                        continue;
                    }
                }
            } else {
                rx.recv().await
            };

            let Some((event, correctness_sample)) = maybe_event else {
                break;
            };

            let sink_send_started = std::time::Instant::now();
            // `event` is borrowed for the send and still owned here for the dead-letter
            // path, so neither needs a copy. This used to clone the whole `Event` —
            // including its `serde_json::Value` payload, so a deep heap walk — for *every*
            // event, to serve a quarantine branch that fires only for permanently
            // undeliverable records.
            match send_event_with_timeout(sink, &event, sink_send_timeout_ms).await {
                Ok(()) => {}
                // A *recoverable* failure is the batch's problem, not the event's — the
                // caller retries the whole batch under the recovery policy. Only a
                // permanently undeliverable event is a candidate for quarantine;
                // dead-lettering a transient failure would discard good data because a
                // broker was briefly unavailable.
                Err(err) if err.is_recoverable() => return Err(err),
                // Permanent, but *not* the record's fault — bad credentials, a missing
                // topic, a revoked ACL. Quarantining these would drain the entire change
                // stream into the DLQ one event at a time while reporting success. Halt
                // instead, and let the operator see the real cause.
                Err(err) if !err.is_dead_letterable() => {
                    tracing::error!(
                        error = %err,
                        "sink failure is permanent but not attributable to this event; \
                         halting rather than quarantining the stream"
                    );
                    return Err(err);
                }
                Err(err) => {
                    let Some(dlq) = dlq else {
                        // No dead-letter target configured: halt, which is the safe
                        // default. Advancing past an undelivered event is data loss and
                        // must be something the operator asked for.
                        return Err(err);
                    };

                    let record = crate::dlq::DeadLetterRecord::new(sink.name(), &event, &err);
                    tracing::warn!(
                        target: "rustcdc_audit",
                        action = "dead_letter",
                        table = %record.table,
                        source_offset = %record.source_offset,
                        error = %err,
                        "event is permanently undeliverable; quarantining and advancing"
                    );
                    // A DLQ write failure is fatal by design. Continuing would advance
                    // the checkpoint past an event that was neither delivered nor
                    // recorded — the silent loss this whole mechanism exists to prevent.
                    dlq.lock().await.write(&record).await?;
                    stats.dlq_events_total = stats.dlq_events_total.saturating_add(1);
                    continue;
                }
            }

            // Reached only on a successful send, so a quarantined event contributes no
            // correctness sample — it never became part of the stream the KPI describes.
            committed_correctness_samples.push(correctness_sample);

            let sink_send_latency_us = sink_send_started.elapsed().as_micros() as u64;
            stats.sink_send_ops_total = stats.sink_send_ops_total.saturating_add(1);
            stats.sink_send_latency_us_total = stats
                .sink_send_latency_us_total
                .saturating_add(sink_send_latency_us);
            stats.sink_send_latency_us_last = sink_send_latency_us;
            observe_latency_histogram_bucket(
                &mut stats.sink_send_latency_us_buckets,
                sink_send_latency_us,
            );

            buffered_since_flush += 1;
            if buffered_since_flush >= flush_interval {
                let sink_flush_started = std::time::Instant::now();
                flush_sink_with_timeout(sink, sink_flush_timeout_ms).await?;

                let sink_flush_latency_us = sink_flush_started.elapsed().as_micros() as u64;
                record_sink_flush(&mut stats, sink_flush_latency_us);
                buffered_since_flush = 0;
            }
        }

        if buffered_since_flush > 0 {
            let sink_flush_started = std::time::Instant::now();
            flush_sink_with_timeout(sink, sink_flush_timeout_ms).await?;

            let sink_flush_latency_us = sink_flush_started.elapsed().as_micros() as u64;
            record_sink_flush(&mut stats, sink_flush_latency_us);
        }

        Ok::<(SinkDeliveryStats, Vec<CorrectnessSample>), AppError>((
            stats,
            committed_correctness_samples,
        ))
    };

    let (prepare_stats, (delivery_stats, committed_correctness_samples)) =
        tokio::try_join!(producer, consumer)?;

    Ok(BatchProcessingStats {
        prepare: prepare_stats,
        delivery: delivery_stats,
        committed_correctness_samples,
        checkpoint_parity_requested: false,
        checkpoint_parity_effective: false,
        runtime_ack_commit_executed: false,
    })
}

#[allow(clippy::too_many_arguments)] // internal wiring of long-lived pipeline components
pub(crate) async fn process_batch_events_with_optional_checkpoint_barrier(
    sink: &mut crate::pipeline::router::TableRouter,
    events: impl IntoIterator<Item = rustcdc::core::Event>,
    transform_pipeline: &transform::TransformPipeline,
    prepare_parallelism: usize,
    flush_interval: usize,
    sink_delivery_queue_capacity: usize,
    sink_send_timeout_ms: u64,
    sink_flush_timeout_ms: u64,
    checkpoint_parity_mode: CheckpointParityMode,
    dlq: Option<&tokio::sync::Mutex<crate::dlq::DeadLetterQueue>>,
) -> Result<BatchProcessingStats, AppError> {
    let plan = checkpoint_parity_plan(checkpoint_parity_mode, sink);

    if !plan.requested {
        let mut stats = process_batch_events(
            sink,
            events,
            transform_pipeline,
            prepare_parallelism,
            flush_interval,
            sink_delivery_queue_capacity,
            sink_send_timeout_ms,
            sink_flush_timeout_ms,
            dlq,
        )
        .await?;
        stats.checkpoint_parity_requested = false;
        stats.checkpoint_parity_effective = false;
        stats.runtime_ack_commit_executed = false;
        return Ok(stats);
    }

    if !plan.effective {
        tracing::warn!(
            sink = sink.name(),
            "checkpoint parity requested, but sink does not support transactional checkpoint barriers"
        );
        let mut stats = process_batch_events(
            sink,
            events,
            transform_pipeline,
            prepare_parallelism,
            flush_interval,
            sink_delivery_queue_capacity,
            sink_send_timeout_ms,
            sink_flush_timeout_ms,
            dlq,
        )
        .await?;
        stats.checkpoint_parity_requested = true;
        stats.checkpoint_parity_effective = false;
        stats.runtime_ack_commit_executed = false;
        return Ok(stats);
    }

    sink.begin_checkpoint_barrier()
        .await
        .map_err(AppError::from)?;

    match process_batch_events(
        sink,
        events,
        transform_pipeline,
        prepare_parallelism,
        flush_interval,
        sink_delivery_queue_capacity,
        sink_send_timeout_ms,
        sink_flush_timeout_ms,
        dlq,
    )
    .await
    {
        Ok(mut stats) => match sink.commit_checkpoint_barrier().await {
            Ok(()) => {
                stats.checkpoint_parity_requested = true;
                stats.checkpoint_parity_effective = true;
                stats.runtime_ack_commit_executed = false;
                Ok(stats)
            }
            Err(commit_err) => {
                if let Err(abort_err) = sink.abort_checkpoint_barrier().await {
                    tracing::warn!(
                        error = %abort_err,
                        "transactional checkpoint barrier abort failed after commit failure"
                    );
                }
                Err(AppError::from(commit_err))
            }
        },
        Err(err) => {
            if let Err(abort_err) = sink.abort_checkpoint_barrier().await {
                tracing::warn!(
                    error = %abort_err,
                    "transactional checkpoint barrier abort failed after batch delivery error"
                );
            }
            Err(err)
        }
    }
}

fn record_sink_flush(stats: &mut SinkDeliveryStats, sink_flush_latency_us: u64) {
    stats.sink_flush_ops_total = stats.sink_flush_ops_total.saturating_add(1);
    stats.sink_flush_latency_us_total = stats
        .sink_flush_latency_us_total
        .saturating_add(sink_flush_latency_us);
    stats.sink_flush_latency_us_last = sink_flush_latency_us;
    observe_latency_histogram_bucket(
        &mut stats.sink_flush_latency_us_buckets,
        sink_flush_latency_us,
    );
}

async fn send_event_with_timeout(
    sink: &mut crate::pipeline::router::TableRouter,
    event: &rustcdc::core::Event,
    sink_send_timeout_ms: u64,
) -> Result<(), AppError> {
    // The size limit is enforced by `SinkBinding` against the *encoded* payload — see
    // `crate::sink::SinkBinding::max_event_bytes`. It used to be enforced here by
    // serialising the event to JSON and discarding the result, which cost a measured
    // 13.9 us per event and, for any non-JSON codec, measured a payload that was never
    // transmitted.
    match tokio::time::timeout(
        std::time::Duration::from_millis(sink_send_timeout_ms),
        sink.send(event),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(AppError::Runtime(err)),
        Err(_) => Err(AppError::SinkTimeout(format!(
            "sink send operation timed out after {sink_send_timeout_ms} ms"
        ))),
    }
}

pub(crate) async fn flush_sink_with_timeout(
    sink: &mut crate::pipeline::router::TableRouter,
    sink_flush_timeout_ms: u64,
) -> Result<(), AppError> {
    match tokio::time::timeout(
        std::time::Duration::from_millis(sink_flush_timeout_ms),
        sink.flush(),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(AppError::from(err)),
        Err(_) => Err(AppError::SinkTimeout(format!(
            "sink flush operation timed out after {sink_flush_timeout_ms} ms"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::process_batch_events;
    use crate::config::schema::{SinkConfig, StdoutSinkConfig, TransformRuntimeConfig};
    use crate::pipeline::transform::TransformPipeline;
    use rustcdc::core::{Event, Operation, SourceMetadata};
    use serde_json::json;

    fn sample_event(id: u64) -> Event {
        Event::builder("users", Operation::Insert)
            .after(json!({"id": id, "name": "batch"}))
            .source(SourceMetadata::new(
                "postgres",
                format!("0/AA{id:04X}"),
                id + 1,
            ))
            .ts(id + 1)
            .schema("public")
            .primary_key(["id"])
            .build()
    }

    /// Deadlock regression: the consumer half of the
    /// prepare/deliver pipeline exits only when the delivery channel closes. If the
    /// producer does not drop its sender on completion, `try_join!` waits on a
    /// consumer that waits on `recv()` — forever, for every batch, empty or not.
    #[tokio::test]
    async fn process_batch_events_completes_for_empty_and_nonempty_batches() {
        let binding =
            crate::sink::build_binding(&SinkConfig::Stdout(StdoutSinkConfig::default()), 1 << 20)
                .await
                .expect("stdout binding");
        let mut router = crate::pipeline::router::single(binding);
        let pipeline = TransformPipeline::from_config(TransformRuntimeConfig::default(), vec![])
            .expect("pipeline");

        for events in [Vec::new(), vec![sample_event(1), sample_event(2)]] {
            let batch_len = events.len();
            let stats = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                process_batch_events(
                    &mut router,
                    events,
                    &pipeline,
                    4,     // prepare_parallelism
                    1,     // flush_interval (demo posture: flush every event)
                    1024,  // sink_delivery_queue_capacity
                    1_000, // sink_send_timeout_ms
                    1_000, // sink_flush_timeout_ms
                    None,  // no dead-letter quarantine in this test
                ),
            )
            .await
            .unwrap_or_else(|_| panic!("process_batch_events deadlocked (batch_len={batch_len})"))
            .expect("batch must succeed");
            assert_eq!(stats.delivery.sink_send_ops_total, batch_len as u64);
        }
    }
}
