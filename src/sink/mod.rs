mod http;
mod iceberg;
mod kafka;

use std::time::Duration;

use futures::future::BoxFuture;
use rustcdc::sink::BoxedSink;

pub use http::HttpSink;
pub use iceberg::IcebergSink;
pub use kafka::KafkaSink;
pub use rustcdc::sink::{FanOutSinkAdapter as FanOutSink, FileJsonlSink, StdoutSink};

use crate::codec::{build as build_codec, BuiltCodec};
use crate::{
    config::schema::{SinkConfig, StdoutSinkConfig},
    error::AppError,
};
use bytes::Bytes;
use rustcdc::{codec::Codec, core::Event, sink::SinkAdapter};

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
    pub iceberg_flush_lock_contention_events_total: u64,
    pub iceberg_flush_lock_contention_ms_total: u64,
    pub iceberg_flush_lock_contention_ms_max: u64,
}

// Re-export rustcdc's SinkDeliveryGuarantee so callers don't need two imports.
pub use rustcdc::sink::SinkDeliveryGuarantee;

pub enum BuiltSink {
    Stdout(Box<StdoutSink>),
    FileJsonl(Box<FileJsonlSink>),
    Http(Box<HttpSink>),
    Kafka(Box<KafkaSink>),
    Iceberg(Box<IcebergSink>),
    Fan(Box<FanOutSink>),
}

/// Build the concrete sink from the application configuration.
fn build(config: &SinkConfig) -> BoxFuture<'_, Result<BuiltSink, AppError>> {
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
                for (i, child_cfg) in cfg.sinks.iter().enumerate() {
                    let child = build_binding(child_cfg).await.map_err(|e| {
                        AppError::Other(format!("failed to build fan-out child sink [{i}]: {e}"))
                    })?;
                    children.push(BoxedSink::new(child));
                }
                Ok(BuiltSink::Fan(Box::new(FanOutSink::new(children))))
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
}

impl SinkBinding {
    /// Encode and deliver a single event.
    pub async fn send_event(&mut self, event: &Event) -> Result<(), AppError> {
        if matches!(
            self.transport,
            BuiltSink::Stdout(_)
                | BuiltSink::FileJsonl(_)
                | BuiltSink::Iceberg(_)
                | BuiltSink::Fan(_)
        ) {
            self.transport
                .send_event_direct(event)
                .await
                .map_err(AppError::Runtime)
        } else {
            let output = self.codec.encode(event).map_err(AppError::Runtime)?;
            let key = Bytes::from(output.key.unwrap_or_default());
            let value = Bytes::from(output.value);
            self.transport
                .send_encoded(key, value)
                .await
                .map_err(AppError::Runtime)
        }
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
            .map_err(|e| rustcdc::core::Error::SourceError(e.to_string()))
    }

    async fn flush(&mut self) -> rustcdc::core::Result<()> {
        self.transport.flush().await
    }

    async fn close(&mut self) -> rustcdc::core::Result<()> {
        self.transport.close().await
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
        self.transport
            .preflight_check()
            .await
            .map_err(|e| rustcdc::core::Error::SourceError(e.to_string()))
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

/// Build a [`SinkBinding`] (codec + transport) from configuration.
pub fn build_binding(config: &SinkConfig) -> BoxFuture<'_, Result<SinkBinding, AppError>> {
    Box::pin(async move {
        let codec = build_codec_from_sink_config(config)
            .await
            .map_err(|e| AppError::Other(format!("failed to build codec: {e}")))?;
        let transport = build(config).await?;
        Ok(SinkBinding { codec, transport })
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
            BuiltSink::Fan(sink) => Box::pin(sink.preflight_check())
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
            BuiltSink::Fan(_) => "fan_out",
        }
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
            BuiltSink::Fan(sink) => Box::pin(sink.send(event)).await,
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
            BuiltSink::Fan(sink) => Box::pin(sink.flush()).await,
        }
    }

    pub async fn close(&mut self) -> rustcdc::core::Result<()> {
        match self {
            BuiltSink::Stdout(sink) => sink.close().await,
            BuiltSink::FileJsonl(sink) => sink.close().await,
            BuiltSink::Http(sink) => sink.close().await,
            BuiltSink::Kafka(sink) => sink.close().await,
            BuiltSink::Iceberg(sink) => sink.close().await,
            BuiltSink::Fan(sink) => Box::pin(sink.close()).await,
        }
    }

    pub fn queue_depth(&self) -> Option<usize> {
        match self {
            BuiltSink::FileJsonl(sink) => Some(sink.queue_depth()),
            BuiltSink::Http(sink) => Some(sink.pending_events()),
            BuiltSink::Fan(sink) => sink.queue_depth(),
            _ => None,
        }
    }

    pub fn flush_tick_interval(&self) -> Option<Duration> {
        match self {
            BuiltSink::Http(sink) => Some(sink.flush_tick_interval()),
            BuiltSink::Fan(sink) => sink.flush_tick_interval(),
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
                    iceberg_flush_lock_contention_events_total: 0,
                    iceberg_flush_lock_contention_ms_total: 0,
                    iceberg_flush_lock_contention_ms_max: 0,
                }
            }
            BuiltSink::Iceberg(sink) => SinkDeliveryMetrics {
                iceberg_flush_lock_contention_events_total: sink
                    .flush_lock_contention_events_total(),
                iceberg_flush_lock_contention_ms_total: sink.flush_lock_contention_ms_total(),
                iceberg_flush_lock_contention_ms_max: sink.flush_lock_contention_ms_max(),
                ..SinkDeliveryMetrics::default()
            },
            BuiltSink::Fan(_) => SinkDeliveryMetrics::default(),
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
            BuiltSink::Fan(sink) => sink.delivery_guarantee(),
        }
    }

    pub fn idempotent_delivery_capable(&self) -> bool {
        match self {
            BuiltSink::Kafka(_) => true,
            BuiltSink::Fan(sink) => sink.idempotent_delivery_capable(),
            _ => false,
        }
    }

    pub fn transactional_checkpoint_barrier_capable(&self) -> bool {
        match self {
            BuiltSink::Kafka(sink) => sink.transactional_checkpoint_barrier_capable(),
            BuiltSink::Fan(sink) => sink.transactional_checkpoint_barrier_capable(),
            _ => false,
        }
    }

    pub async fn begin_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        match self {
            BuiltSink::Kafka(sink) => sink.begin_checkpoint_barrier().await,
            BuiltSink::Fan(sink) => Box::pin(sink.begin_checkpoint_barrier()).await,
            _ => Ok(()),
        }
    }

    pub async fn commit_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        match self {
            BuiltSink::Kafka(sink) => sink.commit_checkpoint_barrier().await,
            BuiltSink::Fan(sink) => Box::pin(sink.commit_checkpoint_barrier()).await,
            _ => Ok(()),
        }
    }

    pub async fn abort_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        match self {
            BuiltSink::Kafka(sink) => sink.abort_checkpoint_barrier().await,
            BuiltSink::Fan(sink) => Box::pin(sink.abort_checkpoint_barrier()).await,
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
            BuiltSink::Fan(sink) => sink.is_closed(),
        }
    }
}
