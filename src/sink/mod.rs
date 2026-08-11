mod http;
mod iceberg;
mod kafka;
mod snowflake;
mod zerobus;

use std::time::Duration;

use futures::future::BoxFuture;
use rustcdc::sink::BoxedSink;

pub use http::HttpSink;
pub use iceberg::IcebergSink;
pub(crate) use kafka::enforce_durable_confirmation;
pub(crate) use kafka::kafka_connect_timeout;
pub use kafka::{KafkaSink, KafkaTransactionHandle};
pub use rustcdc::sink::{FanOutSinkAdapter as FanOutSink, FileJsonlSink, StdoutSink};
pub use snowflake::SnowflakeSink;
pub use zerobus::ZerobusSink;

use crate::codec::{BuiltCodec, build as build_codec};
use crate::{
    config::schema::{SinkConfig, StdoutSinkConfig},
    error::AppError,
};
use bytes::Bytes;
use rustcdc::{codec::AsyncCodec as _, core::Event, sink::SinkAdapter};

pub const HTTP_BATCH_SIZE_BUCKETS: [u64; 11] = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];
pub const HTTP_RETRY_DELAY_MS_BUCKETS: [u64; 11] =
    [1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_000, 5_000];
pub const HTTP_BATCH_RETRY_DURATION_MS_BUCKETS: [u64; 9] =
    [100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000];

// ─────────────────────────────────────────────────────────────────────────────
// SinkDeliveryMetrics / SinkDeliveryGuarantee
// ─────────────────────────────────────────────────────────────────────────────
//
// SinkDeliveryMetrics is a cdc-server-local struct carrying HTTP- and
// Iceberg-specific extended counters for the Prometheus scrape path.
// rustcdc::SinkDeliveryMetrics covers the generic events_sent/errored/retried
// aggregate and is used in the SinkAdapter trait.

#[derive(Debug, Clone, Copy, Default)]
pub struct SinkDeliveryMetrics {
    pub retries_total: u64,
    pub dlq_total: u64,
    pub http_requests_total: u64,
    pub http_batch_size_samples_total: u64,
    pub http_batch_size_bucket_counts: [u64; HTTP_BATCH_SIZE_BUCKETS.len()],
    pub http_batch_oldest_event_age_ms_last: u64,
    pub http_pending_events: u64,
    pub http_pending_bytes: u64,
    pub http_pending_bytes_high_watermark: u64,
    pub http_retry_delay_samples_total: u64,
    pub http_retry_delay_bucket_counts: [u64; HTTP_RETRY_DELAY_MS_BUCKETS.len()],
    pub http_batch_retry_duration_samples_total: u64,
    pub http_batch_retry_duration_bucket_counts: [u64; HTTP_BATCH_RETRY_DURATION_MS_BUCKETS.len()],
    pub http_batch_retry_duration_ms_total: u64,
    pub http_batch_retry_duration_ms_last: u64,
    pub retryable_status_429_total: u64,
    pub retryable_status_5xx_total: u64,
    pub retryable_error_timeout_total: u64,
    pub retryable_error_other_total: u64,
    pub terminal_status_4xx_total: u64,
    pub terminal_status_other_total: u64,
    pub terminal_error_timeout_total: u64,
    pub terminal_error_other_total: u64,
    /// Data files written to object storage that no snapshot ever referenced, because
    /// the commit failed terminally.
    ///
    /// The Iceberg sink has always counted these; nothing exported them, so the
    /// `RUSTCDCIcebergOrphanedDataFiles` alert referenced a metric that did not exist
    /// and could never fire. Orphaned files cost storage silently and forever.
    pub iceberg_orphaned_data_files_total: u64,
    pub iceberg_flush_lock_contention_events_total: u64,
    pub iceberg_flush_lock_contention_ms_total: u64,
    pub iceberg_flush_lock_contention_ms_max: u64,
    /// SASL/OAUTHBEARER token fetches performed by the Kafka producer.
    ///
    /// Counted only when an `[.oidc]` provider is configured — a static token is
    /// never fetched.
    pub kafka_oauth_token_fetches_total: u64,
    /// OAUTHBEARER token fetches that returned an error.
    ///
    /// This is the signal a misconfigured `token_endpoint` produces. Without it an
    /// OAuth round-trip failing on every connection is indistinguishable from the
    /// broker being unreachable — both surface as connection failures and neither
    /// names the identity provider.
    pub kafka_oauth_token_fetch_failures_total: u64,
    /// Expiry of the currently cached OAUTHBEARER token, in milliseconds since the
    /// Unix epoch. `0` means none has been fetched yet, or the provider returned no
    /// `expires_in`.
    pub kafka_oauth_token_expiry_epoch_ms: u64,
    /// Rows appended to a Snowpipe Streaming channel.
    pub snowflake_rows_appended_total: u64,
    /// Rows dropped on resume because the channel's committed offset token already
    /// covered them.
    ///
    /// Non-zero after a crash between a committed flush and the checkpoint write, which is
    /// the window this sink's exactly-once handling exists for. Persistently non-zero means
    /// the checkpoint is not advancing.
    pub snowflake_rows_skipped_on_resume_total: u64,
    /// Channel opens, including reopens after a stale continuation token.
    ///
    /// A steadily climbing count means another writer is using the same channel name — the
    /// two fence each other in a loop and neither makes progress.
    pub snowflake_channel_reopens_total: u64,
    /// Total time `flush` spent waiting for Snowflake to commit.
    ///
    /// This is the sink's dominant latency and it is *deliberate*: returning before the
    /// commit would let the checkpoint advance past rows a channel reopen discards.
    pub snowflake_commit_wait_ms_total: u64,
    /// Times the bounded resume scan gave up without finding the committed offset token.
    ///
    /// Each one is a window delivered at-least-once. Alert on any increase.
    pub snowflake_resume_scan_exhausted_total: u64,
    /// Records queued to a Zerobus ingest stream.
    pub zerobus_records_ingested_total: u64,
    /// Total time `flush` spent waiting for Databricks to acknowledge durability.
    ///
    /// The sink's dominant latency, and deliberate: returning earlier would let the
    /// checkpoint advance past records a process exit would lose.
    pub zerobus_ack_wait_ms_total: u64,
    /// Zerobus stream opens, including reconnections by the SDK's recovery path.
    ///
    /// A steadily climbing count means the stream keeps dropping; each reopen re-sends
    /// unacknowledged records, so it is also a duplicate source.
    pub zerobus_stream_opens_total: u64,
}

impl SinkDeliveryMetrics {
    /// Combine two sinks' counters into one pipeline-level figure.
    ///
    /// Cumulative counters add; high-water marks and `*_max` take the larger; `*_last`
    /// gauges take the other side's value when it has one, because a zero there means
    /// "this sink has never observed the quantity", not "the quantity is zero".
    ///
    /// Used both for fan-out children and for a router with several routes, so a
    /// `[[pipeline.routes]]` deployment reports one number per family rather than
    /// whichever sink happened to be sampled.
    pub fn merge(&mut self, other: &Self) {
        fn add(target: &mut u64, value: u64) {
            *target = target.saturating_add(value);
        }
        fn add_all<const N: usize>(target: &mut [u64; N], value: &[u64; N]) {
            for (slot, sample) in target.iter_mut().zip(value.iter()) {
                *slot = slot.saturating_add(*sample);
            }
        }
        fn latest(target: &mut u64, value: u64) {
            if value != 0 {
                *target = value;
            }
        }

        add(&mut self.retries_total, other.retries_total);
        add(&mut self.dlq_total, other.dlq_total);
        add(&mut self.http_requests_total, other.http_requests_total);
        add(
            &mut self.http_batch_size_samples_total,
            other.http_batch_size_samples_total,
        );
        add_all(
            &mut self.http_batch_size_bucket_counts,
            &other.http_batch_size_bucket_counts,
        );
        latest(
            &mut self.http_batch_oldest_event_age_ms_last,
            other.http_batch_oldest_event_age_ms_last,
        );
        add(&mut self.http_pending_events, other.http_pending_events);
        add(&mut self.http_pending_bytes, other.http_pending_bytes);
        self.http_pending_bytes_high_watermark = self
            .http_pending_bytes_high_watermark
            .max(other.http_pending_bytes_high_watermark);
        add(
            &mut self.http_retry_delay_samples_total,
            other.http_retry_delay_samples_total,
        );
        add_all(
            &mut self.http_retry_delay_bucket_counts,
            &other.http_retry_delay_bucket_counts,
        );
        add(
            &mut self.http_batch_retry_duration_samples_total,
            other.http_batch_retry_duration_samples_total,
        );
        add_all(
            &mut self.http_batch_retry_duration_bucket_counts,
            &other.http_batch_retry_duration_bucket_counts,
        );
        add(
            &mut self.http_batch_retry_duration_ms_total,
            other.http_batch_retry_duration_ms_total,
        );
        latest(
            &mut self.http_batch_retry_duration_ms_last,
            other.http_batch_retry_duration_ms_last,
        );
        add(
            &mut self.retryable_status_429_total,
            other.retryable_status_429_total,
        );
        add(
            &mut self.retryable_status_5xx_total,
            other.retryable_status_5xx_total,
        );
        add(
            &mut self.retryable_error_timeout_total,
            other.retryable_error_timeout_total,
        );
        add(
            &mut self.retryable_error_other_total,
            other.retryable_error_other_total,
        );
        add(
            &mut self.terminal_status_4xx_total,
            other.terminal_status_4xx_total,
        );
        add(
            &mut self.terminal_status_other_total,
            other.terminal_status_other_total,
        );
        add(
            &mut self.terminal_error_timeout_total,
            other.terminal_error_timeout_total,
        );
        add(
            &mut self.terminal_error_other_total,
            other.terminal_error_other_total,
        );
        add(
            &mut self.iceberg_orphaned_data_files_total,
            other.iceberg_orphaned_data_files_total,
        );
        add(
            &mut self.iceberg_flush_lock_contention_events_total,
            other.iceberg_flush_lock_contention_events_total,
        );
        add(
            &mut self.iceberg_flush_lock_contention_ms_total,
            other.iceberg_flush_lock_contention_ms_total,
        );
        self.iceberg_flush_lock_contention_ms_max = self
            .iceberg_flush_lock_contention_ms_max
            .max(other.iceberg_flush_lock_contention_ms_max);
        add(
            &mut self.kafka_oauth_token_fetches_total,
            other.kafka_oauth_token_fetches_total,
        );
        add(
            &mut self.kafka_oauth_token_fetch_failures_total,
            other.kafka_oauth_token_fetch_failures_total,
        );
        latest(
            &mut self.kafka_oauth_token_expiry_epoch_ms,
            other.kafka_oauth_token_expiry_epoch_ms,
        );
        add(
            &mut self.snowflake_rows_appended_total,
            other.snowflake_rows_appended_total,
        );
        add(
            &mut self.snowflake_rows_skipped_on_resume_total,
            other.snowflake_rows_skipped_on_resume_total,
        );
        add(
            &mut self.snowflake_channel_reopens_total,
            other.snowflake_channel_reopens_total,
        );
        add(
            &mut self.snowflake_commit_wait_ms_total,
            other.snowflake_commit_wait_ms_total,
        );
        add(
            &mut self.snowflake_resume_scan_exhausted_total,
            other.snowflake_resume_scan_exhausted_total,
        );
        add(
            &mut self.zerobus_records_ingested_total,
            other.zerobus_records_ingested_total,
        );
        add(
            &mut self.zerobus_ack_wait_ms_total,
            other.zerobus_ack_wait_ms_total,
        );
        add(
            &mut self.zerobus_stream_opens_total,
            other.zerobus_stream_opens_total,
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SinkMetricsHandle / SinkMetricsRegistry
// ─────────────────────────────────────────────────────────────────────────────
//
// **Why a side channel exists at all.**
//
// The pipeline delivers through `TableRouter<BoxedSink>`, and `BoxedSink` exposes only
// `SinkAdapter` — whose `delivery_metrics()` carries four generic fields
// (`events_sent`/`events_errored`/`events_retried`/`last_delivered_offset`). Everything
// else this server exports about a sink — HTTP status classes, batch-size and retry-delay
// histograms, pending bytes, Iceberg orphaned files and lock contention, Kafka OAUTHBEARER
// token health — has no room in that struct and cannot be reached once a binding is inside
// the router.
//
// The scrape path used to read the router and fill a local `SinkDeliveryMetrics` from the
// one field that survived, leaving **thirty** metric families pinned at zero for the life
// of the process. Four shipped alert rules watched four of them, so they could never fire.
// Every test that asserted otherwise built the struct by hand and never crossed the
// erasure boundary — the defect was invisible from inside the suite.
//
// A handle is taken on the way *in*, exactly like `KafkaTransactionHandle`, and the
// binding publishes into it at each flush. Flush is the right cadence rather than an
// arbitrary one: the run loop samples before and after each batch, and a batch ends with a
// flush, so both samples land on published values.

/// A published snapshot of one sink binding's extended counters.
#[derive(Debug, Clone, Default)]
pub struct SinkMetricsHandle(std::sync::Arc<std::sync::Mutex<SinkDeliveryMetrics>>);

impl SinkMetricsHandle {
    fn publish(&self, metrics: SinkDeliveryMetrics) {
        // A poisoned lock means some other thread panicked mid-publish. Metrics are not
        // worth propagating a panic for; the previous snapshot stays visible.
        if let Ok(mut slot) = self.0.lock() {
            *slot = metrics;
        }
    }

    /// The most recently published snapshot.
    pub fn snapshot(&self) -> SinkDeliveryMetrics {
        self.0.lock().map(|slot| *slot).unwrap_or_default()
    }
}

/// Every binding in one pipeline, so the scrape path can total them.
#[derive(Debug, Clone, Default)]
pub struct SinkMetricsRegistry {
    handles: Vec<SinkMetricsHandle>,
}

impl SinkMetricsRegistry {
    pub fn register(&mut self, handle: SinkMetricsHandle) {
        self.handles.push(handle);
    }

    /// The pipeline-wide total across every registered binding.
    pub fn snapshot(&self) -> SinkDeliveryMetrics {
        let mut total = SinkDeliveryMetrics::default();
        for handle in &self.handles {
            total.merge(&handle.snapshot());
        }
        total
    }
}

// Re-export rustcdc's SinkDeliveryGuarantee so callers don't need two imports.
pub use rustcdc::sink::SinkDeliveryGuarantee;

pub enum BuiltSink {
    Stdout(Box<StdoutSink>),
    FileJsonl(Box<FileJsonlSink>),
    Http(Box<HttpSink>),
    Kafka(Box<KafkaSink>),
    Iceberg(Box<IcebergSink>),
    Snowflake(Box<SnowflakeSink>),
    Zerobus(Box<ZerobusSink>),
    /// Fan-out, plus a handle onto each child's published counters.
    ///
    /// `FanOutSinkAdapter` erases its children to `BoxedSink`, so the parent cannot ask
    /// them for extended metrics. The handles are collected while the children are still
    /// concrete; the parent's `delivery_metrics()` merges them.
    Fan(Box<FanOutSink>, Vec<SinkMetricsHandle>),
}

/// Build the concrete sink from the application configuration.
///
/// `max_event_bytes` is threaded in for transports that produce the transmitted bytes
/// themselves. Snowflake is the first: its NDJSON row *is* the JSON, so it enforces the
/// limit on the bytes it is about to send rather than on a second rendering made purely to
/// measure and thrown away.
fn build(
    config: &SinkConfig,
    max_event_bytes: usize,
) -> BoxFuture<'_, Result<BuiltSink, AppError>> {
    Box::pin(async move {
        match config {
            SinkConfig::Stdout(StdoutSinkConfig {}) => {
                Ok(BuiltSink::Stdout(Box::new(StdoutSink::new())))
            }
            SinkConfig::FileJsonl(cfg) => {
                let sink = FileJsonlSink::open_with(
                    &cfg.path,
                    rustcdc::sink::FileJsonlSinkConfig {
                        rotate_size_bytes: cfg.rotate_size_bytes,
                        fsync_every: cfg.fsync_every,
                    },
                )
                .map_err(|e| {
                    AppError::Other(format!(
                        "failed to open JSONL sink at {}: {e}",
                        cfg.path.display()
                    ))
                })?;
                Ok(BuiltSink::FileJsonl(Box::new(sink)))
            }
            SinkConfig::Http(cfg) => {
                let sink = HttpSink::new(cfg)
                    .map_err(|e| AppError::Other(format!("failed to build HTTP sink: {e}")))?;
                Ok(BuiltSink::Http(Box::new(sink)))
            }
            SinkConfig::Snowflake(cfg) => {
                let sink = SnowflakeSink::new(cfg, max_event_bytes).await?;
                Ok(BuiltSink::Snowflake(Box::new(sink)))
            }
            SinkConfig::Zerobus(cfg) => {
                let sink = ZerobusSink::new(cfg, max_event_bytes).await?;
                Ok(BuiltSink::Zerobus(Box::new(sink)))
            }
            SinkConfig::Kafka(cfg) => {
                let sink = KafkaSink::new(cfg)
                    .await
                    .map_err(|e| AppError::Other(format!("failed to build Kafka sink: {e}")))?;
                Ok(BuiltSink::Kafka(Box::new(sink)))
            }
            SinkConfig::Iceberg(cfg) => {
                let sink = IcebergSink::open(cfg)
                    .await
                    .map_err(|e| AppError::Other(format!("failed to build Iceberg sink: {e}")))?;
                Ok(BuiltSink::Iceberg(Box::new(sink)))
            }
            SinkConfig::Fan(cfg) => {
                if cfg.sinks.is_empty() {
                    return Err(AppError::Other(
                        "fan-out sink requires at least one child sink in `sinks`".to_string(),
                    ));
                }
                let mut children: Vec<BoxedSink> = Vec::with_capacity(cfg.sinks.len());
                let mut child_metrics: Vec<SinkMetricsHandle> = Vec::with_capacity(cfg.sinks.len());
                for (i, child_cfg) in cfg.sinks.iter().enumerate() {
                    // `usize::MAX`: a fan-out child is only ever reached through the
                    // parent binding's direct path, which has already enforced the
                    // limit. Enforcing it again per child would reject the same event
                    // twice and report the child's name for the parent's decision.
                    let child = build_binding(child_cfg, max_event_bytes)
                        .await
                        .map_err(|e| {
                            AppError::Other(format!(
                                "failed to build fan-out child sink [{i}]: {e}"
                            ))
                        })?;
                    child_metrics.push(child.metrics_handle());
                    children.push(BoxedSink::new(child));
                }
                Ok(BuiltSink::Fan(
                    Box::new(FanOutSink::new(children)),
                    child_metrics,
                ))
            }
        }
    })
}

/// A configured sink binding: codec + transport.
///
/// The codec encodes events into key + value bytes; the transport delivers them.
pub struct SinkBinding {
    pub codec: BuiltCodec,
    pub transport: BuiltSink,
    /// `runtime.max_event_bytes`, enforced against the payload the transport will
    /// actually send.
    ///
    /// This used to live in `send_prepared_event_with_timeout`, which serialised every
    /// event to JSON solely to measure it and then threw the buffer away — 13.9 µs per
    /// event of pure waste, measured, before the codec then encoded the same event
    /// again. Worse, for Avro or Protobuf the limit was measuring a JSON rendering that
    /// is never transmitted, so it rejected events that would have fitted and could not
    /// be calibrated against a broker's `max.message.bytes`.
    pub max_event_bytes: usize,
    /// Where this binding publishes its extended counters so the scrape path can read
    /// them after the binding disappears into a `BoxedSink`.
    ///
    /// See the module-level note on [`SinkMetricsHandle`] for why the router cannot ask
    /// for them directly.
    metrics: SinkMetricsHandle,
}

impl SinkBinding {
    /// A share in this binding's Kafka transaction, when the transport has one.
    pub fn transaction_handle(&self) -> Option<KafkaTransactionHandle> {
        self.transport.transaction_handle()
    }

    /// A read handle onto this binding's extended counters.
    ///
    /// Must be taken before the binding is boxed into the router; afterwards the concrete
    /// transport is unreachable.
    pub fn metrics_handle(&self) -> SinkMetricsHandle {
        self.metrics.clone()
    }

    /// Publish the transport's current counters to the handle.
    ///
    /// Called on flush, close and preflight rather than per event: the run loop samples
    /// the registry at batch boundaries, and a batch ends with a flush, so every sample
    /// it takes lands on a freshly published value. Publishing per event would put a
    /// mutex and a ~450-byte copy on the hot path to gain accuracy nothing reads.
    fn publish_metrics(&self) {
        self.metrics.publish(self.transport.delivery_metrics());
    }

    /// Encode and deliver a single event.
    ///
    /// Three cases, and they are now asked about by *capability* rather than derived from a
    /// list of variant names — which is what the previous version's own comment said the
    /// real fix was:
    ///
    /// * the transport takes pre-encoded bytes (Kafka, HTTP) — the codec runs here, and the
    ///   limit measures exactly what goes on the wire;
    /// * the transport produces the transmitted bytes itself *and* enforces the limit on
    ///   them (Snowflake, whose NDJSON row is the JSON) — nothing is encoded here at all;
    /// * the transport owns its own serialisation and cannot report a size (stdout, JSONL,
    ///   Iceberg, fan-out) — the event's JSON is the honest approximation, and it costs an
    ///   encode that is thrown away.
    pub async fn send_event(&mut self, event: &Event) -> Result<(), AppError> {
        if self.transport.takes_encoded_bytes() {
            let output = self
                .codec
                .encode_async(event)
                .await
                .map_err(AppError::Runtime)?;
            let key = match output.key {
                Some(key) => Bytes::from(key),
                // Keyless events (table without a primary key): partition by the
                // qualified table name so per-table ordering survives. The previous
                // `unwrap_or_default()` produced an *empty* key, which Kafka
                // partitioners hash like any other key (Java semantics) — pinning
                // every keyless event across all tables to one murmur2("") partition.
                None => Bytes::from(event.qualified_table_name().into_bytes()),
            };
            let value = Bytes::from(output.value);
            // Exact: these are the bytes going on the wire, and measuring them costs
            // nothing because they already exist.
            enforce_size_limit(key.len() + value.len(), self.max_event_bytes)?;
            return self
                .transport
                .send_encoded(key, value)
                .await
                .map_err(AppError::Runtime);
        }

        if !self.transport.enforces_own_size_limit() {
            enforce_size_limit(
                serde_json::to_vec(event)
                    .map_err(|e| {
                        AppError::Other(format!("failed to serialize event for size check: {e}"))
                    })?
                    .len(),
                self.max_event_bytes,
            )?;
        }
        self.transport
            .send_event_direct(event)
            .await
            .map_err(AppError::Runtime)
    }
}

/// `SinkAdapter` implementation for [`SinkBinding`].
///
/// This allows `SinkBinding` to be wrapped in a `BoxedSink` and used as a
/// child of `FanOutSinkAdapter` or `TableRouter<BoxedSink>`.
impl SinkAdapter for SinkBinding {
    fn name(&self) -> &str {
        self.transport.name()
    }

    async fn send(&mut self, event: &Event) -> rustcdc::core::Result<()> {
        self.send_event(event)
            .await
            .map_err(rustcdc::core::Error::from)
    }

    async fn flush(&mut self) -> rustcdc::core::Result<()> {
        let result = self.transport.flush().await;
        // Published even when the flush failed: a failed flush is precisely when the
        // status-class counters this exports have just moved.
        self.publish_metrics();
        result
    }

    async fn close(&mut self) -> rustcdc::core::Result<()> {
        let result = self.transport.close().await;
        self.publish_metrics();
        result
    }

    fn delivery_guarantee(&self) -> rustcdc::sink::SinkDeliveryGuarantee {
        self.transport.delivery_guarantee()
    }

    fn idempotent_delivery_capable(&self) -> bool {
        self.transport.idempotent_delivery_capable()
    }

    fn queue_depth(&self) -> Option<usize> {
        self.transport.queue_depth()
    }

    fn flush_tick_interval(&self) -> Option<std::time::Duration> {
        self.transport.flush_tick_interval()
    }

    fn transactional_checkpoint_barrier_capable(&self) -> bool {
        self.transport.transactional_checkpoint_barrier_capable()
    }

    async fn begin_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        self.transport.begin_checkpoint_barrier().await
    }

    async fn commit_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        self.transport.commit_checkpoint_barrier().await
    }

    async fn abort_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        self.transport.abort_checkpoint_barrier().await
    }

    async fn preflight_check(&mut self) -> rustcdc::core::Result<()> {
        let result = self
            .transport
            .preflight_check()
            .await
            .map_err(rustcdc::core::Error::from);
        // Seeds the handle before the first batch, so the run loop's first
        // `delivery_before` sample is a real reading rather than a default.
        self.publish_metrics();
        result
    }

    fn is_closed(&self) -> bool {
        self.transport.is_closed()
    }

    fn delivery_metrics(&self) -> Option<rustcdc::sink::SinkDeliveryMetrics> {
        let local = self.transport.delivery_metrics();
        Some(rustcdc::sink::SinkDeliveryMetrics {
            events_sent: local.http_requests_total,
            events_errored: local
                .terminal_status_4xx_total
                .saturating_add(local.terminal_status_other_total)
                .saturating_add(local.terminal_error_timeout_total)
                .saturating_add(local.terminal_error_other_total),
            events_retried: local.retries_total,
            last_delivered_offset: None,
        })
    }
}

/// Reject an event whose encoded payload exceeds `runtime.max_event_bytes`.
fn enforce_size_limit(size: usize, max_event_bytes: usize) -> Result<(), AppError> {
    if size > max_event_bytes {
        return Err(AppError::EventTooLarge(format!(
            "encoded event payload size {size} exceeds runtime.max_event_bytes \
             {max_event_bytes}"
        )));
    }
    Ok(())
}

/// Build a [`SinkBinding`] (codec + transport) from configuration.
pub fn build_binding(
    config: &SinkConfig,
    max_event_bytes: usize,
) -> BoxFuture<'_, Result<SinkBinding, AppError>> {
    Box::pin(async move {
        let codec = build_codec_from_sink_config(config)
            .await
            .map_err(|e| AppError::Other(format!("failed to build codec: {e}")))?;
        let transport = build(config, max_event_bytes).await?;
        let binding = SinkBinding {
            codec,
            transport,
            max_event_bytes,
            metrics: SinkMetricsHandle::default(),
        };
        binding.publish_metrics();
        Ok(binding)
    })
}

async fn build_codec_from_sink_config(config: &SinkConfig) -> Result<BuiltCodec, String> {
    match config {
        SinkConfig::Kafka(cfg) => build_codec(cfg.codec.as_ref(), &cfg.topic).await,
        SinkConfig::Http(cfg) => build_codec(cfg.codec.as_ref(), "").await,
        _ => build_codec(None, "").await,
    }
}

impl BuiltSink {
    /// Validate sink connectivity before the pipeline is marked ready.
    ///
    /// Kafka: verifies the target topic exists via a temporary AdminClient.
    /// HTTP:  sends a HEAD request to the configured endpoint URL.
    /// Others: no-op (always `Ok`).
    pub async fn preflight_check(&mut self) -> Result<(), AppError> {
        match self {
            BuiltSink::Kafka(sink) => sink.preflight_check().await,
            BuiltSink::Http(sink) => Box::pin(sink.preflight_check()).await,
            BuiltSink::Snowflake(sink) => Box::pin(sink.preflight()).await,
            BuiltSink::Zerobus(sink) => Box::pin(sink.preflight()).await,
            BuiltSink::Fan(sink, _) => Box::pin(sink.preflight_check())
                .await
                .map_err(AppError::Runtime),
            _ => Ok(()),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            BuiltSink::Stdout(_) => "stdout",
            BuiltSink::FileJsonl(_) => "file_jsonl",
            BuiltSink::Http(_) => "http",
            BuiltSink::Kafka(_) => "kafka",
            BuiltSink::Iceberg(_) => "iceberg",
            BuiltSink::Snowflake(_) => "snowflake",
            BuiltSink::Zerobus(_) => "zerobus",
            BuiltSink::Fan(..) => "fan_out",
        }
    }

    /// Does this transport want the codec's output rather than the event?
    pub fn takes_encoded_bytes(&self) -> bool {
        matches!(self, BuiltSink::Kafka(_) | BuiltSink::Http(_))
    }

    /// Does this transport enforce `runtime.max_event_bytes` on the bytes it sends?
    ///
    /// True only where the transport *is* the encoder, so it can measure the real payload.
    /// Everything else is measured here against the event's JSON, which for Avro or
    /// Protobuf is a rendering that never leaves the process.
    pub fn enforces_own_size_limit(&self) -> bool {
        matches!(self, BuiltSink::Snowflake(_) | BuiltSink::Zerobus(_))
    }

    /// Deliver pre-encoded bytes to transport sinks (Kafka, HTTP).
    pub async fn send_encoded(&mut self, key: Bytes, value: Bytes) -> rustcdc::core::Result<()> {
        match self {
            BuiltSink::Kafka(sink) => sink.send_encoded(key, value).await,
            BuiltSink::Http(sink) => sink.send_json_vec(value.to_vec()).await,
            _ => Err(rustcdc::core::Error::StateError(
                "this sink type requires event dispatch, not encoded bytes".to_string(),
            )),
        }
    }

    /// Deliver a decoded event to schema-owning sinks (Stdout, FileJsonl, Iceberg, Fan).
    pub async fn send_event_direct(&mut self, event: &Event) -> rustcdc::core::Result<()> {
        match self {
            BuiltSink::Stdout(sink) => sink.send(event).await,
            BuiltSink::FileJsonl(sink) => sink.send(event).await,
            BuiltSink::Iceberg(sink) => sink.send(event).await,
            // Snowflake takes the structured event: the pipe owns the column mapping, and
            // the row goes on the wire as one NDJSON line.
            BuiltSink::Snowflake(sink) => sink.append_event(event).await,
            BuiltSink::Zerobus(sink) => sink.append_event(event).await,
            BuiltSink::Fan(sink, _) => Box::pin(sink.send(event)).await,
            _ => Err(rustcdc::core::Error::StateError(
                "this sink type requires pre-encoded bytes, not a raw event".to_string(),
            )),
        }
    }

    pub async fn flush(&mut self) -> rustcdc::core::Result<()> {
        match self {
            BuiltSink::Stdout(sink) => sink.flush().await,
            BuiltSink::FileJsonl(sink) => sink.flush().await,
            BuiltSink::Http(sink) => sink.flush().await,
            BuiltSink::Kafka(sink) => sink.flush().await,
            BuiltSink::Iceberg(sink) => sink.flush().await,
            BuiltSink::Snowflake(sink) => sink.flush_pending().await,
            BuiltSink::Zerobus(sink) => sink.flush_pending().await,
            BuiltSink::Fan(sink, _) => Box::pin(sink.flush()).await,
        }
    }

    pub async fn close(&mut self) -> rustcdc::core::Result<()> {
        match self {
            BuiltSink::Stdout(sink) => sink.close().await,
            BuiltSink::FileJsonl(sink) => sink.close().await,
            BuiltSink::Http(sink) => sink.close().await,
            BuiltSink::Kafka(sink) => sink.close().await,
            BuiltSink::Iceberg(sink) => sink.close().await,
            BuiltSink::Snowflake(sink) => sink.close().await,
            BuiltSink::Zerobus(sink) => sink.close().await,
            BuiltSink::Fan(sink, _) => Box::pin(sink.close()).await,
        }
    }

    pub fn queue_depth(&self) -> Option<usize> {
        match self {
            BuiltSink::FileJsonl(sink) => Some(sink.queue_depth()),
            BuiltSink::Http(sink) => Some(sink.pending_events()),
            BuiltSink::Snowflake(sink) => Some(sink.pending_rows()),
            BuiltSink::Zerobus(sink) => Some(sink.pending_records()),
            BuiltSink::Fan(sink, _) => sink.queue_depth(),
            _ => None,
        }
    }

    pub fn flush_tick_interval(&self) -> Option<Duration> {
        match self {
            BuiltSink::Http(sink) => Some(sink.flush_tick_interval()),
            BuiltSink::Snowflake(sink) => Some(sink.flush_tick_interval()),
            BuiltSink::Zerobus(sink) => Some(sink.flush_tick_interval()),
            BuiltSink::Fan(sink, _) => sink.flush_tick_interval(),
            _ => None,
        }
    }

    pub fn delivery_metrics(&self) -> SinkDeliveryMetrics {
        match self {
            BuiltSink::Http(sink) => {
                let accounting = sink.delivery_metrics();
                SinkDeliveryMetrics {
                    retries_total: accounting.retried,
                    dlq_total: accounting.dlq_written,
                    http_requests_total: accounting.http_requests_total,
                    http_batch_size_samples_total: accounting.http_batch_size_samples_total,
                    http_batch_size_bucket_counts: accounting.http_batch_size_bucket_counts,
                    http_batch_oldest_event_age_ms_last: accounting
                        .http_batch_oldest_event_age_ms_last,
                    http_pending_events: sink.pending_events() as u64,
                    http_pending_bytes: sink.pending_bytes(),
                    http_pending_bytes_high_watermark: accounting.http_pending_bytes_high_watermark,
                    http_retry_delay_samples_total: accounting.http_retry_delay_samples_total,
                    http_retry_delay_bucket_counts: accounting.http_retry_delay_bucket_counts,
                    http_batch_retry_duration_samples_total: accounting
                        .http_batch_retry_duration_samples_total,
                    http_batch_retry_duration_bucket_counts: accounting
                        .http_batch_retry_duration_bucket_counts,
                    http_batch_retry_duration_ms_total: accounting
                        .http_batch_retry_duration_ms_total,
                    http_batch_retry_duration_ms_last: accounting.http_batch_retry_duration_ms_last,
                    retryable_status_429_total: accounting.retryable_status_429,
                    retryable_status_5xx_total: accounting.retryable_status_5xx,
                    retryable_error_timeout_total: accounting.retryable_error_timeout,
                    retryable_error_other_total: accounting.retryable_error_other,
                    terminal_status_4xx_total: accounting.terminal_status_4xx,
                    terminal_status_other_total: accounting.terminal_status_other,
                    terminal_error_timeout_total: accounting.terminal_error_timeout,
                    terminal_error_other_total: accounting.terminal_error_other,
                    iceberg_orphaned_data_files_total: 0,
                    iceberg_flush_lock_contention_events_total: 0,
                    iceberg_flush_lock_contention_ms_total: 0,
                    iceberg_flush_lock_contention_ms_max: 0,
                    kafka_oauth_token_fetches_total: 0,
                    kafka_oauth_token_fetch_failures_total: 0,
                    kafka_oauth_token_expiry_epoch_ms: 0,
                    snowflake_rows_appended_total: 0,
                    snowflake_rows_skipped_on_resume_total: 0,
                    snowflake_channel_reopens_total: 0,
                    snowflake_commit_wait_ms_total: 0,
                    snowflake_resume_scan_exhausted_total: 0,
                    zerobus_records_ingested_total: 0,
                    zerobus_ack_wait_ms_total: 0,
                    zerobus_stream_opens_total: 0,
                }
            }
            BuiltSink::Iceberg(sink) => SinkDeliveryMetrics {
                iceberg_orphaned_data_files_total: sink.orphaned_data_files_total(),
                iceberg_flush_lock_contention_events_total: sink
                    .flush_lock_contention_events_total(),
                iceberg_flush_lock_contention_ms_total: sink.flush_lock_contention_ms_total(),
                iceberg_flush_lock_contention_ms_max: sink.flush_lock_contention_ms_max(),
                ..SinkDeliveryMetrics::default()
            },
            BuiltSink::Snowflake(sink) => {
                let accounting = sink.accounting();
                SinkDeliveryMetrics {
                    snowflake_rows_appended_total: accounting.rows_appended,
                    snowflake_rows_skipped_on_resume_total: accounting.rows_skipped_on_resume,
                    snowflake_channel_reopens_total: accounting.channel_reopens,
                    snowflake_commit_wait_ms_total: accounting.commit_wait_ms_total,
                    snowflake_resume_scan_exhausted_total: accounting.resume_scan_exhausted,
                    retries_total: accounting.retries,
                    ..SinkDeliveryMetrics::default()
                }
            }
            BuiltSink::Zerobus(sink) => {
                let accounting = sink.accounting();
                SinkDeliveryMetrics {
                    zerobus_records_ingested_total: accounting.records_ingested,
                    zerobus_ack_wait_ms_total: accounting.ack_wait_ms_total,
                    zerobus_stream_opens_total: accounting.stream_opens,
                    ..SinkDeliveryMetrics::default()
                }
            }
            BuiltSink::Kafka(sink) => {
                let connection = sink.connection_metrics();
                SinkDeliveryMetrics {
                    kafka_oauth_token_fetches_total: connection.oauth_token_fetches,
                    kafka_oauth_token_fetch_failures_total: connection.oauth_token_fetch_failures,
                    kafka_oauth_token_expiry_epoch_ms: connection.oauth_token_expiry_epoch_ms,
                    ..SinkDeliveryMetrics::default()
                }
            }
            // Each child publishes on its own flush — `FanOutSinkAdapter` forwards
            // `flush()` to every child, and each child is a `SinkBinding` — so merging
            // the handles here reports the fan-out's real totals rather than zeros.
            BuiltSink::Fan(_, child_metrics) => {
                let mut total = SinkDeliveryMetrics::default();
                for handle in child_metrics {
                    total.merge(&handle.snapshot());
                }
                total
            }
            _ => SinkDeliveryMetrics::default(),
        }
    }

    pub fn delivery_guarantee(&self) -> SinkDeliveryGuarantee {
        match self {
            BuiltSink::Stdout(_) => SinkDeliveryGuarantee::AtLeastOnce,
            BuiltSink::FileJsonl(_) => SinkDeliveryGuarantee::AtLeastOnce,
            BuiltSink::Http(_) => SinkDeliveryGuarantee::AtLeastOnce,
            BuiltSink::Kafka(sink) => sink.delivery_guarantee(),
            BuiltSink::Iceberg(_) => SinkDeliveryGuarantee::AtLeastOnce,
            // The channel's committed offset token, plus a `flush` that waits for it, plus
            // resume filtering — see `sink/snowflake.rs`.
            BuiltSink::Snowflake(_) => SinkDeliveryGuarantee::EffectivelyOnce,
            // Not `EffectivelyOnce`: Zerobus streams are ephemeral, so there is no durable
            // offset to resume from. The ack rules out loss, not duplicates.
            BuiltSink::Zerobus(_) => SinkDeliveryGuarantee::AtLeastOnce,
            BuiltSink::Fan(sink, _) => sink.delivery_guarantee(),
        }
    }

    pub fn idempotent_delivery_capable(&self) -> bool {
        match self {
            BuiltSink::Kafka(_) | BuiltSink::Snowflake(_) => true,
            BuiltSink::Fan(sink, _) => sink.idempotent_delivery_capable(),
            _ => false,
        }
    }

    pub fn transactional_checkpoint_barrier_capable(&self) -> bool {
        match self {
            BuiltSink::Kafka(sink) => sink.transactional_checkpoint_barrier_capable(),
            BuiltSink::Fan(sink, _) => sink.transactional_checkpoint_barrier_capable(),
            _ => false,
        }
    }

    /// A share in this sink's Kafka transaction, when it has one.
    ///
    /// Fan-out is deliberately excluded: its children are erased to `BoxedSink`, so there
    /// is no way to reach a child's producer, and a transaction spanning several sinks is
    /// not something this design offers. `effectively_once` over fan-out is rejected at
    /// load rather than silently degraded.
    pub fn transaction_handle(&self) -> Option<KafkaTransactionHandle> {
        match self {
            BuiltSink::Kafka(sink) => sink.transaction_handle(),
            _ => None,
        }
    }

    pub async fn begin_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        match self {
            BuiltSink::Kafka(sink) => sink.begin_checkpoint_barrier().await,
            BuiltSink::Fan(sink, _) => Box::pin(sink.begin_checkpoint_barrier()).await,
            _ => Ok(()),
        }
    }

    pub async fn commit_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        match self {
            BuiltSink::Kafka(sink) => sink.commit_checkpoint_barrier().await,
            BuiltSink::Fan(sink, _) => Box::pin(sink.commit_checkpoint_barrier()).await,
            _ => Ok(()),
        }
    }

    pub async fn abort_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        match self {
            BuiltSink::Kafka(sink) => sink.abort_checkpoint_barrier().await,
            BuiltSink::Fan(sink, _) => Box::pin(sink.abort_checkpoint_barrier()).await,
            _ => Ok(()),
        }
    }

    pub fn is_closed(&self) -> bool {
        match self {
            BuiltSink::Stdout(sink) => sink.is_closed(),
            BuiltSink::FileJsonl(sink) => sink.is_closed(),
            BuiltSink::Http(sink) => sink.is_closed(),
            BuiltSink::Kafka(sink) => sink.is_closed(),
            BuiltSink::Iceberg(sink) => sink.is_closed(),
            BuiltSink::Snowflake(sink) => sink.is_closed(),
            BuiltSink::Zerobus(sink) => sink.is_closed(),
            BuiltSink::Fan(sink, _) => sink.is_closed(),
        }
    }
}

#[cfg(test)]
mod size_limit_tests {
    use crate::config::codec::CodecConfig;
    use crate::config::sink::{HttpSinkConfig, SinkConfig};
    use rustcdc::codec::AsyncCodec;
    use rustcdc::core::{Event, Operation, SourceMetadata};

    fn wide_event() -> Event {
        Event::builder("orders", Operation::Insert)
            .after(serde_json::json!({
                "id": 1,
                "status": "shipped",
                "customer": "a customer name of unremarkable length",
                "notes": "free text that JSON spends key names on and a binary codec does not",
            }))
            .source(SourceMetadata::new("postgres", "0/16B6A70", 1))
            .ts(1)
            .schema("public")
            .primary_key(["id"])
            .build()
    }

    fn http_sink(codec: Option<CodecConfig>) -> SinkConfig {
        let mut cfg: HttpSinkConfig = serde_json::from_value(serde_json::json!({
            "url": "https://api.example.com/events",
        }))
        .expect("http sink config");
        cfg.codec = codec;
        SinkConfig::Http(cfg)
    }

    /// The whole point of moving the limit: it now measures the bytes the transport
    /// sends, so the *same* event can pass under one codec and fail under another.
    ///
    /// Previously both cases measured the identical JSON rendering, which meant an
    /// operator calibrating `max_event_bytes` against a broker's `max.message.bytes`
    /// was calibrating against a payload the broker never sees.
    #[tokio::test]
    async fn the_limit_measures_the_encoded_payload_not_a_json_rendering() {
        let event = wide_event();

        let json_binding = super::build_binding(&http_sink(None), usize::MAX)
            .await
            .expect("json binding");
        let json_len = json_binding
            .codec
            .encode_async(&event)
            .await
            .expect("json encode")
            .value
            .len();

        // A limit set just under the JSON size must reject under the JSON codec.
        let mut tight = super::build_binding(&http_sink(None), json_len - 1)
            .await
            .expect("json binding");
        let err = tight
            .send_event(&event)
            .await
            .expect_err("JSON payload exceeds the limit");
        assert!(
            err.to_string().contains("encoded event payload size"),
            "the error must describe the encoded payload: {err}"
        );

        // And accept it when the limit clears the encoded size. The key is included in
        // the measurement, so allow for it.
        let mut loose = super::build_binding(&http_sink(None), json_len * 2)
            .await
            .expect("json binding");
        loose
            .send_event(&event)
            .await
            .expect("payload within the limit must be accepted");
    }
}
