use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::time::Instant;

use rustcdc::core::RuntimeAdminSnapshot;
use rustcdc::{fingerprint_event_stable, Event};

use super::run_batch;
use super::run_recovery::RecoverableErrorSnapshot;
use crate::pipeline::transform;
use crate::sink::{
    HTTP_BATCH_RETRY_DURATION_MS_BUCKETS, HTTP_BATCH_SIZE_BUCKETS, HTTP_RETRY_DELAY_MS_BUCKETS,
};
use crate::state;

pub(super) const LATENCY_HISTOGRAM_BUCKETS_MS: [u64; 8] = [1, 5, 10, 25, 50, 100, 250, 500];

pub(super) fn observe_latency_histogram_bucket(
    buckets: &mut [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    latency_ms: u64,
) {
    for (index, upper_bound_ms) in LATENCY_HISTOGRAM_BUCKETS_MS.iter().enumerate() {
        if latency_ms <= *upper_bound_ms {
            buckets[index] = buckets[index].saturating_add(1);
            return;
        }
    }
}

fn average_latency_ms(total_latency_ms: u64, sample_count: u64) -> f64 {
    if sample_count == 0 {
        0.0
    } else {
        total_latency_ms as f64 / sample_count as f64
    }
}

fn quantile_u64_histogram_upper_bound(
    buckets: &[u64],
    upper_bounds: &[u64],
    total_samples: u64,
    quantile: f64,
) -> u64 {
    if total_samples == 0 {
        return 0;
    }

    let rank = (total_samples as f64 * quantile).ceil() as u64;
    let mut cumulative = 0u64;
    for (count, upper_bound) in buckets.iter().zip(upper_bounds.iter()) {
        cumulative = cumulative.saturating_add(*count);
        if cumulative >= rank {
            return *upper_bound;
        }
    }

    *upper_bounds.last().unwrap_or(&0)
}

const MAX_CORRECTNESS_STREAMS: usize = 128;
const MILLIS_THRESHOLD: u64 = 1_000_000_000_000;

#[derive(Debug, Clone)]
pub(super) struct CorrectnessSample {
    pub(super) schema: Option<String>,
    pub(super) table: String,
    pub(super) source_name: String,
    pub(super) source_offset: String,
    pub(super) source_timestamp: u64,
    pub(super) event_timestamp: u64,
    pub(super) fingerprint: Option<String>,
}

impl CorrectnessSample {
    pub(super) fn from_event(event: &Event) -> Self {
        Self {
            schema: event.schema.clone(),
            table: event.table.clone(),
            source_name: event.source.source_name.clone(),
            source_offset: event.source.offset.clone(),
            source_timestamp: event.source.timestamp,
            event_timestamp: event.ts,
            fingerprint: fingerprint_event_stable(event).ok(),
        }
    }
}

fn normalize_stream_name_sample(sample: &CorrectnessSample) -> String {
    match sample.schema.as_deref() {
        Some(schema) if !schema.is_empty() => format!("{schema}.{}", sample.table),
        _ => format!("unknown.{}", sample.table),
    }
}

fn normalize_source_sequence_sample(sample: &CorrectnessSample) -> Option<u64> {
    let source_name = sample.source_name.trim().to_ascii_lowercase();
    let offset = sample.source_offset.trim();
    if offset.is_empty() {
        return None;
    }

    if source_name == "postgres" {
        return parse_postgres_lsn(offset);
    }

    parse_numeric_offset_component(offset)
}

fn normalize_source_timestamp_ms_sample(sample: &CorrectnessSample) -> Option<u64> {
    let mut ts = sample.source_timestamp.max(sample.event_timestamp);
    if ts == 0 {
        return None;
    }

    if ts < MILLIS_THRESHOLD {
        ts = ts.saturating_mul(1000);
    }

    Some(ts)
}

fn parse_postgres_lsn(offset: &str) -> Option<u64> {
    let (high, low) = offset.split_once('/')?;
    let high = u64::from_str_radix(high, 16).ok()?;
    let low = u64::from_str_radix(low, 16).ok()?;
    Some((high << 32) | low)
}

fn parse_numeric_offset_component(offset: &str) -> Option<u64> {
    if let Ok(value) = offset.parse::<u64>() {
        return Some(value);
    }

    offset
        .split(|ch: char| !ch.is_ascii_digit())
        .rev()
        .find(|part| !part.is_empty())
        .and_then(|part| part.parse::<u64>().ok())
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Clone)]
struct MetricLabel<'a> {
    key: &'a str,
    value: Cow<'a, str>,
}

#[derive(Default)]
struct PrometheusTextEncoder {
    output: String,
    /// Tracks metric family names for which `# HELP` / `# TYPE` headers have
    /// already been emitted.  Prometheus and OpenMetrics both require exactly
    /// one header block per metric family; duplicate headers are a spec
    /// violation that confuses some parsers (e.g. `prometheus_parse`).
    emitted_families: HashSet<String>,
}

impl PrometheusTextEncoder {
    fn finish(self) -> String {
        self.output
    }

    fn counter(
        &mut self,
        name: &str,
        help: &str,
        labels: &[MetricLabel<'_>],
        value: impl std::fmt::Display,
    ) {
        self.scalar(name, help, "counter", labels, value);
    }

    fn gauge(
        &mut self,
        name: &str,
        help: &str,
        labels: &[MetricLabel<'_>],
        value: impl std::fmt::Display,
    ) {
        self.scalar(name, help, "gauge", labels, value);
    }

    fn scalar(
        &mut self,
        name: &str,
        help: &str,
        metric_type: &str,
        labels: &[MetricLabel<'_>],
        value: impl std::fmt::Display,
    ) {
        if self.emitted_families.insert(name.to_owned()) {
            let _ = writeln!(self.output, "# HELP {name} {help}");
            let _ = writeln!(self.output, "# TYPE {name} {metric_type}");
        }
        self.write_metric_value_line(name, labels, value);
    }

    fn histogram(
        &mut self,
        name: &str,
        help: &str,
        labels: &[MetricLabel<'_>],
        buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
        sum: u64,
        count: u64,
    ) {
        if self.emitted_families.insert(name.to_owned()) {
            let _ = writeln!(self.output, "# HELP {name} {help}");
            let _ = writeln!(self.output, "# TYPE {name} histogram");
        }

        let mut cumulative_count = 0u64;
        for (index, upper_bound_ms) in LATENCY_HISTOGRAM_BUCKETS_MS.iter().enumerate() {
            cumulative_count = cumulative_count.saturating_add(buckets[index]);

            let mut bucket_labels = Vec::with_capacity(labels.len() + 1);
            bucket_labels.extend_from_slice(labels);
            bucket_labels.push(MetricLabel {
                key: "le",
                value: Cow::Owned(upper_bound_ms.to_string()),
            });
            self.write_metric_value_line(
                &format!("{name}_bucket"),
                &bucket_labels,
                cumulative_count,
            );
        }

        let mut inf_labels = Vec::with_capacity(labels.len() + 1);
        inf_labels.extend_from_slice(labels);
        inf_labels.push(MetricLabel {
            key: "le",
            value: Cow::Borrowed("+Inf"),
        });
        self.write_metric_value_line(&format!("{name}_bucket"), &inf_labels, count);
        self.write_metric_value_line(&format!("{name}_sum"), labels, sum);
        self.write_metric_value_line(&format!("{name}_count"), labels, count);
    }

    fn write_metric_value_line(
        &mut self,
        name: &str,
        labels: &[MetricLabel<'_>],
        value: impl std::fmt::Display,
    ) {
        if labels.is_empty() {
            let _ = writeln!(self.output, "{name} {value}");
            return;
        }

        self.output.push_str(name);
        self.output.push('{');
        for (idx, label) in labels.iter().enumerate() {
            if idx > 0 {
                self.output.push(',');
            }
            // Escape backslashes, double-quotes, and newlines so label values
            // containing these characters cannot break the Prometheus text
            // exposition format (per the OpenMetrics specification).
            let escaped = label
                .value
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");
            let _ = write!(self.output, "{}=\"{}\"", label.key, escaped);
        }
        let _ = writeln!(self.output, "}} {value}");
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RecoverableErrorMetrics {
    total: u64,
    consecutive: u64,
    backoff_ms: u64,
    backoff_last_ms: u64,
    breaker_open_total: u64,
    breaker_open_consecutive: u64,
    kafka_topic_state_corruption_detected_total: u64,
    kafka_topic_state_bootstrap_seeded_total: u64,
    opendal_state_write_failures_total: u64,
    /// Monotonic count of circuit-breaker open events since process start.
    lifetime_breaker_open_total: u64,
}

impl RecoverableErrorMetrics {
    pub(crate) fn render_prometheus(&self) -> String {
        let mut encoder = PrometheusTextEncoder::default();
        encoder.counter(
            "rustcdc_runtime_recoverable_poll_errors_total",
            "Total recoverable runtime poll errors",
            &[],
            self.total,
        );
        encoder.gauge(
            "rustcdc_runtime_recoverable_poll_errors_consecutive",
            "Current consecutive recoverable poll errors",
            &[],
            self.consecutive,
        );
        encoder.gauge(
            "rustcdc_runtime_recoverable_poll_backoff_ms",
            "Next recoverable poll error backoff in milliseconds",
            &[],
            self.backoff_ms,
        );
        encoder.gauge(
            "rustcdc_runtime_recoverable_poll_backoff_last_ms",
            "Most recent applied recoverable poll error backoff in milliseconds",
            &[],
            self.backoff_last_ms,
        );
        encoder.counter(
            "rustcdc_runtime_recoverable_breaker_open_total",
            "Total number of recoverable poll error breaker openings",
            &[],
            self.breaker_open_total,
        );
        encoder.gauge(
            "rustcdc_runtime_recoverable_breaker_open_consecutive",
            "Current consecutive recoverable poll error breaker openings",
            &[],
            self.breaker_open_consecutive,
        );
        encoder.counter(
            "rustcdc_runtime_state_kafka_corruption_detected_total",
            "Total kafka topic state corruption detections (fail-closed startup guards)",
            &[],
            self.kafka_topic_state_corruption_detected_total,
        );
        encoder.counter(
            "rustcdc_runtime_state_kafka_bootstrap_seeded_total",
            "Total kafka topic state bootstrap initializations seeded via init-state",
            &[],
            self.kafka_topic_state_bootstrap_seeded_total,
        );
        encoder.counter(
            "rustcdc_runtime_state_opendal_write_failures_total",
            "Total OpenDAL remote state-backend write failures (checkpoint + schema history)",
            &[],
            self.opendal_state_write_failures_total,
        );
        encoder.counter(
            "rustcdc_runtime_recoverable_breaker_lifetime_open_total",
            "Monotonic count of circuit-breaker open events since process start",
            &[],
            self.lifetime_breaker_open_total,
        );
        encoder.finish()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SinkMetricsSnapshot {
    sink_name: String,
    requested_delivery_contract: String,
    delivery_contract_satisfied: bool,
    sink_delivery_guarantee: String,
    sink_idempotent_delivery_capable: bool,
    sink_transactional_checkpoint_barrier_capable: bool,
    sink_send_ops_total: u64,
    sink_send_latency_ms_total: u64,
    sink_send_latency_ms_last: u64,
    sink_send_latency_ms_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    sink_flush_ops_total: u64,
    sink_flush_latency_ms_total: u64,
    sink_flush_latency_ms_last: u64,
    sink_flush_latency_ms_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    transform_ops_total: u64,
    transform_latency_ms_total: u64,
    transform_latency_ms_last: u64,
    transform_latency_ms_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    transform_wasm_instance_pool_size: u64,
    transform_wasm_invocations_total: u64,
    transform_wasm_errors_total: u64,
    transform_wasm_filtered_total: u64,
    transform_wasm_timeout_total: u64,
    prepare_ops_total: u64,
    prepare_latency_ms_total: u64,
    prepare_latency_ms_last: u64,
    prepare_latency_ms_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    batch_delivery_ops_total: u64,
    batch_delivery_latency_ms_total: u64,
    batch_delivery_latency_ms_last: u64,
    batch_delivery_latency_ms_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    checkpoint_commit_ops_total: u64,
    checkpoint_commit_latency_ms_total: u64,
    checkpoint_commit_latency_ms_last: u64,
    checkpoint_commit_latency_ms_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    sink_queue_depth_last: u64,
    sink_queue_depth_p95: u64,
    runtime_soak_duration_seconds: u64,
    sink_retry_rate: f64,
    sink_retries_total: u64,
    sink_dlq_total: u64,
    sink_http_requests_total: u64,
    sink_http_request_amplification: f64,
    sink_http_batch_size_p50: u64,
    sink_http_batch_size_p95: u64,
    sink_http_batch_oldest_event_age_ms_last: u64,
    sink_http_pending_events: u64,
    sink_http_pending_bytes: u64,
    sink_http_pending_bytes_high_watermark: u64,
    sink_http_retry_delay_ms_p50: u64,
    sink_http_retry_delay_ms_p95: u64,
    sink_http_batch_retry_duration_ms_total: u64,
    sink_http_batch_retry_duration_ms_avg: f64,
    sink_http_batch_retry_duration_ms_last: u64,
    sink_http_batch_retry_duration_ms_p50: u64,
    sink_http_batch_retry_duration_ms_p95: u64,
    sink_retryable_status_429_total: u64,
    sink_retryable_status_5xx_total: u64,
    sink_retryable_error_timeout_total: u64,
    sink_retryable_error_other_total: u64,
    sink_terminal_status_4xx_total: u64,
    sink_terminal_status_other_total: u64,
    sink_terminal_error_timeout_total: u64,
    sink_terminal_error_other_total: u64,
    sink_iceberg_flush_lock_contention_events_total: u64,
    sink_iceberg_flush_lock_contention_ms_total: u64,
    sink_iceberg_flush_lock_contention_ms_max: u64,
    data_events_total: u64,
    data_duplicates_total: u64,
    data_reorders_total: u64,
    end_to_end_ack_lag_samples_total: u64,
    end_to_end_ack_lag_ms_total: u64,
    end_to_end_ack_lag_ms_last: u64,
    stream_correctness: Vec<StreamCorrectnessMetricsSnapshot>,
}

#[derive(Debug, Clone, Default)]
struct StreamCorrectnessMetricsSnapshot {
    stream: String,
    events_total: u64,
    duplicates_total: u64,
    reorders_total: u64,
    ack_lag_samples_total: u64,
    ack_lag_ms_total: u64,
    ack_lag_ms_last: u64,
}

#[derive(Debug, Clone, Default)]
struct StreamCorrectnessMetricsAccumulator {
    events_total: u64,
    duplicates_total: u64,
    reorders_total: u64,
    ack_lag_samples_total: u64,
    ack_lag_ms_total: u64,
    ack_lag_ms_last: u64,
}

impl SinkMetricsSnapshot {
    pub(crate) fn render_prometheus(&self) -> String {
        let sink_labels = [MetricLabel {
            key: "sink",
            value: Cow::Borrowed(self.sink_name.as_str()),
        }];
        let sink_contract_labels = [
            MetricLabel {
                key: "sink",
                value: Cow::Borrowed(self.sink_name.as_str()),
            },
            MetricLabel {
                key: "contract",
                value: Cow::Borrowed(self.requested_delivery_contract.as_str()),
            },
        ];
        let sink_mode_labels = [
            MetricLabel {
                key: "sink",
                value: Cow::Borrowed(self.sink_name.as_str()),
            },
            MetricLabel {
                key: "mode",
                value: Cow::Borrowed(self.sink_delivery_guarantee.as_str()),
            },
        ];

        let mut encoder = PrometheusTextEncoder::default();

        encoder.counter(
            "rustcdc_sink_send_ops_total",
            "Total sink send operations",
            &sink_labels,
            self.sink_send_ops_total,
        );
        encoder.gauge(
            "rustcdc_sink_delivery_contract_requested",
            "Requested delivery contract for active sink",
            &sink_contract_labels,
            1,
        );
        encoder.gauge(
            "rustcdc_sink_delivery_contract_satisfied",
            "Whether active sink capabilities satisfy requested delivery contract",
            &sink_contract_labels,
            if self.delivery_contract_satisfied {
                1
            } else {
                0
            },
        );
        encoder.gauge(
            "rustcdc_sink_delivery_guarantee",
            "Delivery guarantee classification for active sink",
            &sink_mode_labels,
            1,
        );
        encoder.gauge(
            "rustcdc_sink_idempotent_delivery_capable",
            "Whether sink supports idempotent producer delivery semantics",
            &sink_labels,
            if self.sink_idempotent_delivery_capable {
                1
            } else {
                0
            },
        );
        encoder.gauge(
            "rustcdc_sink_transactional_checkpoint_barrier_capable",
            "Whether sink supports transactional checkpoint barriers",
            &sink_labels,
            if self.sink_transactional_checkpoint_barrier_capable {
                1
            } else {
                0
            },
        );
        encoder.gauge(
            "rustcdc_sink_send_latency_ms_avg",
            "Average sink send latency in milliseconds",
            &sink_labels,
            average_latency_ms(self.sink_send_latency_ms_total, self.sink_send_ops_total),
        );
        encoder.gauge(
            "rustcdc_sink_send_latency_ms_last",
            "Last sink send latency in milliseconds",
            &sink_labels,
            self.sink_send_latency_ms_last,
        );
        encoder.counter(
            "rustcdc_sink_flush_ops_total",
            "Total sink flush operations",
            &sink_labels,
            self.sink_flush_ops_total,
        );
        encoder.gauge(
            "rustcdc_sink_flush_latency_ms_avg",
            "Average sink flush latency in milliseconds",
            &sink_labels,
            average_latency_ms(self.sink_flush_latency_ms_total, self.sink_flush_ops_total),
        );
        encoder.gauge(
            "rustcdc_sink_flush_latency_ms_last",
            "Last sink flush latency in milliseconds",
            &sink_labels,
            self.sink_flush_latency_ms_last,
        );
        encoder.counter(
            "rustcdc_runtime_transform_ops_total",
            "Total event transform operations",
            &sink_labels,
            self.transform_ops_total,
        );
        encoder.gauge(
            "rustcdc_runtime_transform_latency_ms_avg",
            "Average event transform latency in milliseconds",
            &sink_labels,
            average_latency_ms(self.transform_latency_ms_total, self.transform_ops_total),
        );
        encoder.gauge(
            "rustcdc_runtime_transform_latency_ms_last",
            "Last event transform latency in milliseconds",
            &sink_labels,
            self.transform_latency_ms_last,
        );
        encoder.gauge(
            "rustcdc_runtime_transform_wasm_instance_pool_size",
            "Configured WASM instance pool size",
            &sink_labels,
            self.transform_wasm_instance_pool_size,
        );
        encoder.counter(
            "rustcdc_runtime_transform_wasm_invocations_total",
            "Total WASM transform invocations attempted since runtime init",
            &sink_labels,
            self.transform_wasm_invocations_total,
        );
        encoder.counter(
            "rustcdc_runtime_transform_wasm_errors_total",
            "Total WASM transform invocations that returned an error",
            &sink_labels,
            self.transform_wasm_errors_total,
        );
        encoder.counter(
            "rustcdc_runtime_transform_wasm_filtered_total",
            "Total events filtered (dropped) by the WASM module",
            &sink_labels,
            self.transform_wasm_filtered_total,
        );
        encoder.counter(
            "rustcdc_runtime_transform_wasm_timeout_total",
            "Total WASM transforms that exceeded the configured timeout",
            &sink_labels,
            self.transform_wasm_timeout_total,
        );
        encoder.counter(
            "rustcdc_runtime_prepare_ops_total",
            "Total event prepare operations (transform + encode)",
            &sink_labels,
            self.prepare_ops_total,
        );
        encoder.gauge(
            "rustcdc_runtime_prepare_latency_ms_avg",
            "Average event prepare latency in milliseconds",
            &sink_labels,
            average_latency_ms(self.prepare_latency_ms_total, self.prepare_ops_total),
        );
        encoder.gauge(
            "rustcdc_runtime_prepare_latency_ms_last",
            "Last event prepare latency in milliseconds",
            &sink_labels,
            self.prepare_latency_ms_last,
        );
        encoder.counter(
            "rustcdc_runtime_batch_delivery_ops_total",
            "Total delivered runtime batches",
            &sink_labels,
            self.batch_delivery_ops_total,
        );
        encoder.gauge(
            "rustcdc_runtime_batch_delivery_latency_ms_avg",
            "Average runtime batch delivery latency in milliseconds",
            &sink_labels,
            average_latency_ms(
                self.batch_delivery_latency_ms_total,
                self.batch_delivery_ops_total,
            ),
        );
        encoder.gauge(
            "rustcdc_runtime_batch_delivery_latency_ms_last",
            "Last runtime batch delivery latency in milliseconds",
            &sink_labels,
            self.batch_delivery_latency_ms_last,
        );
        encoder.counter(
            "rustcdc_runtime_checkpoint_commit_ops_total",
            "Total checkpoint commit operations",
            &sink_labels,
            self.checkpoint_commit_ops_total,
        );
        encoder.gauge(
            "rustcdc_runtime_checkpoint_commit_latency_ms_avg",
            "Average checkpoint commit latency in milliseconds",
            &sink_labels,
            average_latency_ms(
                self.checkpoint_commit_latency_ms_total,
                self.checkpoint_commit_ops_total,
            ),
        );
        encoder.gauge(
            "rustcdc_runtime_checkpoint_commit_latency_ms_last",
            "Last checkpoint commit latency in milliseconds",
            &sink_labels,
            self.checkpoint_commit_latency_ms_last,
        );
        encoder.gauge(
            "rustcdc_sink_queue_depth",
            "Last observed sink queue depth",
            &sink_labels,
            self.sink_queue_depth_last,
        );
        encoder.gauge(
            "rustcdc_sink_queue_depth_p95",
            "p95 sink queue depth over recent runtime window",
            &sink_labels,
            self.sink_queue_depth_p95,
        );
        encoder.gauge(
            "rustcdc_runtime_soak_duration_seconds",
            "Continuous runtime duration since pipeline start",
            &sink_labels,
            self.runtime_soak_duration_seconds,
        );
        encoder.counter(
            "rustcdc_sink_retries_total",
            "Total sink retry attempts",
            &sink_labels,
            self.sink_retries_total,
        );
        encoder.gauge(
            "rustcdc_sink_retry_rate",
            "Sink retry ratio retries_total/send_ops_total",
            &sink_labels,
            self.sink_retry_rate,
        );
        encoder.counter(
            "rustcdc_sink_dlq_total",
            "Total sink events written to DLQ",
            &sink_labels,
            self.sink_dlq_total,
        );
        encoder.counter(
            "rustcdc_sink_http_requests_total",
            "Total HTTP requests issued by sink delivery attempts (includes retries)",
            &sink_labels,
            self.sink_http_requests_total,
        );
        encoder.gauge(
            "rustcdc_sink_http_request_amplification",
            "HTTP request amplification ratio http_requests_total/sink_send_ops_total",
            &sink_labels,
            self.sink_http_request_amplification,
        );
        encoder.gauge(
            "rustcdc_sink_http_batch_size_p50",
            "Approximate p50 HTTP sink batch size (events/request)",
            &sink_labels,
            self.sink_http_batch_size_p50,
        );
        encoder.gauge(
            "rustcdc_sink_http_batch_size_p95",
            "Approximate p95 HTTP sink batch size (events/request)",
            &sink_labels,
            self.sink_http_batch_size_p95,
        );
        encoder.gauge(
            "rustcdc_sink_http_batch_oldest_event_age_ms",
            "Oldest event age in milliseconds at the last HTTP batch flush",
            &sink_labels,
            self.sink_http_batch_oldest_event_age_ms_last,
        );
        encoder.gauge(
            "rustcdc_sink_http_pending_events",
            "Current number of buffered events waiting for HTTP sink flush",
            &sink_labels,
            self.sink_http_pending_events,
        );
        encoder.gauge(
            "rustcdc_sink_http_pending_bytes",
            "Current aggregate bytes buffered waiting for HTTP sink flush",
            &sink_labels,
            self.sink_http_pending_bytes,
        );
        encoder.gauge(
            "rustcdc_sink_http_pending_bytes_high_watermark",
            "High-watermark aggregate buffered bytes observed for HTTP sink",
            &sink_labels,
            self.sink_http_pending_bytes_high_watermark,
        );
        encoder.gauge(
            "rustcdc_sink_http_retry_delay_ms_p50",
            "Approximate p50 HTTP sink retry backoff delay in milliseconds",
            &sink_labels,
            self.sink_http_retry_delay_ms_p50,
        );
        encoder.gauge(
            "rustcdc_sink_http_retry_delay_ms_p95",
            "Approximate p95 HTTP sink retry backoff delay in milliseconds",
            &sink_labels,
            self.sink_http_retry_delay_ms_p95,
        );
        encoder.counter(
            "rustcdc_sink_http_batch_retry_duration_ms_total",
            "Total elapsed retry/salvage time in milliseconds across flushed HTTP batches",
            &sink_labels,
            self.sink_http_batch_retry_duration_ms_total,
        );
        encoder.gauge(
            "rustcdc_sink_http_batch_retry_duration_ms_avg",
            "Average elapsed retry/salvage time in milliseconds per flushed HTTP batch",
            &sink_labels,
            self.sink_http_batch_retry_duration_ms_avg,
        );
        encoder.gauge(
            "rustcdc_sink_http_batch_retry_duration_ms_last",
            "Elapsed retry/salvage time in milliseconds for the last flushed HTTP batch",
            &sink_labels,
            self.sink_http_batch_retry_duration_ms_last,
        );
        encoder.gauge(
            "rustcdc_sink_http_batch_retry_duration_ms_p50",
            "Approximate p50 elapsed retry/salvage time in milliseconds per flushed HTTP batch",
            &sink_labels,
            self.sink_http_batch_retry_duration_ms_p50,
        );
        encoder.gauge(
            "rustcdc_sink_http_batch_retry_duration_ms_p95",
            "Approximate p95 elapsed retry/salvage time in milliseconds per flushed HTTP batch",
            &sink_labels,
            self.sink_http_batch_retry_duration_ms_p95,
        );
        encoder.counter(
            "rustcdc_sink_retryable_status_429_total",
            "Total sink retries caused by HTTP 429 responses",
            &sink_labels,
            self.sink_retryable_status_429_total,
        );
        encoder.counter(
            "rustcdc_sink_retryable_status_5xx_total",
            "Total sink retries caused by HTTP 5xx responses",
            &sink_labels,
            self.sink_retryable_status_5xx_total,
        );
        encoder.counter(
            "rustcdc_sink_retryable_error_timeout_total",
            "Total sink retries caused by timeout errors",
            &sink_labels,
            self.sink_retryable_error_timeout_total,
        );
        encoder.counter(
            "rustcdc_sink_retryable_error_other_total",
            "Total sink retries caused by non-timeout recoverable errors",
            &sink_labels,
            self.sink_retryable_error_other_total,
        );
        encoder.counter(
            "rustcdc_sink_terminal_status_4xx_total",
            "Total sink terminal failures caused by HTTP 4xx responses",
            &sink_labels,
            self.sink_terminal_status_4xx_total,
        );
        encoder.counter(
            "rustcdc_sink_terminal_status_other_total",
            "Total sink terminal failures caused by non-4xx HTTP statuses",
            &sink_labels,
            self.sink_terminal_status_other_total,
        );
        encoder.counter(
            "rustcdc_sink_terminal_error_timeout_total",
            "Total sink terminal failures caused by timeout errors",
            &sink_labels,
            self.sink_terminal_error_timeout_total,
        );
        encoder.counter(
            "rustcdc_sink_terminal_error_other_total",
            "Total sink terminal failures caused by non-timeout errors",
            &sink_labels,
            self.sink_terminal_error_other_total,
        );
        encoder.counter(
            "rustcdc_sink_iceberg_flush_lock_contention_events_total",
            "Total iceberg flush calls that observed lock contention",
            &sink_labels,
            self.sink_iceberg_flush_lock_contention_events_total,
        );
        encoder.counter(
            "rustcdc_sink_iceberg_flush_lock_contention_ms_total",
            "Total iceberg flush lock wait time in milliseconds",
            &sink_labels,
            self.sink_iceberg_flush_lock_contention_ms_total,
        );
        encoder.gauge(
            "rustcdc_sink_iceberg_flush_lock_contention_ms_max",
            "Maximum observed iceberg flush lock wait time in milliseconds",
            &sink_labels,
            self.sink_iceberg_flush_lock_contention_ms_max,
        );
        encoder.counter(
            "rustcdc_data_events_total",
            "Total source events observed by runtime correctness tracker",
            &sink_labels,
            self.data_events_total,
        );
        encoder.counter(
            "rustcdc_data_duplicates_total",
            "Total duplicate source events observed by runtime correctness tracker",
            &sink_labels,
            self.data_duplicates_total,
        );
        encoder.gauge(
            "rustcdc_data_duplicate_rate",
            "Duplicate event ratio duplicates_total/events_total",
            &sink_labels,
            average_latency_ms(self.data_duplicates_total, self.data_events_total),
        );
        encoder.counter(
            "rustcdc_data_reorders_total",
            "Total source-order regressions observed by runtime correctness tracker",
            &sink_labels,
            self.data_reorders_total,
        );
        encoder.gauge(
            "rustcdc_data_reorder_rate",
            "Reordered event ratio reorders_total/events_total",
            &sink_labels,
            average_latency_ms(self.data_reorders_total, self.data_events_total),
        );
        encoder.gauge(
            "rustcdc_end_to_end_ack_lag_ms_avg",
            "Average source-to-delivery acknowledgement lag in milliseconds",
            &sink_labels,
            average_latency_ms(
                self.end_to_end_ack_lag_ms_total,
                self.end_to_end_ack_lag_samples_total,
            ),
        );
        encoder.gauge(
            "rustcdc_end_to_end_ack_lag_ms_last",
            "Last observed source-to-delivery acknowledgement lag in milliseconds",
            &sink_labels,
            self.end_to_end_ack_lag_ms_last,
        );

        for stream in &self.stream_correctness {
            let labels = [
                MetricLabel {
                    key: "sink",
                    value: Cow::Borrowed(self.sink_name.as_str()),
                },
                MetricLabel {
                    key: "stream",
                    value: Cow::Borrowed(stream.stream.as_str()),
                },
            ];

            encoder.write_metric_value_line(
                "rustcdc_data_events_total",
                &labels,
                stream.events_total,
            );
            encoder.write_metric_value_line(
                "rustcdc_data_duplicates_total",
                &labels,
                stream.duplicates_total,
            );
            encoder.write_metric_value_line(
                "rustcdc_data_duplicate_rate",
                &labels,
                average_latency_ms(stream.duplicates_total, stream.events_total),
            );
            encoder.write_metric_value_line(
                "rustcdc_data_reorders_total",
                &labels,
                stream.reorders_total,
            );
            encoder.write_metric_value_line(
                "rustcdc_data_reorder_rate",
                &labels,
                average_latency_ms(stream.reorders_total, stream.events_total),
            );
            encoder.write_metric_value_line(
                "rustcdc_end_to_end_ack_lag_ms_avg",
                &labels,
                average_latency_ms(stream.ack_lag_ms_total, stream.ack_lag_samples_total),
            );
            encoder.write_metric_value_line(
                "rustcdc_end_to_end_ack_lag_ms_last",
                &labels,
                stream.ack_lag_ms_last,
            );
        }

        for (metric_name, help, buckets, total, count) in [
            (
                "rustcdc_sink_send_latency_ms",
                "Histogram of sink send latency in milliseconds",
                self.sink_send_latency_ms_buckets,
                self.sink_send_latency_ms_total,
                self.sink_send_ops_total,
            ),
            (
                "rustcdc_sink_flush_latency_ms",
                "Histogram of sink flush latency in milliseconds",
                self.sink_flush_latency_ms_buckets,
                self.sink_flush_latency_ms_total,
                self.sink_flush_ops_total,
            ),
            (
                "rustcdc_runtime_transform_latency_ms",
                "Histogram of event transform latency in milliseconds",
                self.transform_latency_ms_buckets,
                self.transform_latency_ms_total,
                self.transform_ops_total,
            ),
            (
                "rustcdc_runtime_prepare_latency_ms",
                "Histogram of event prepare latency in milliseconds",
                self.prepare_latency_ms_buckets,
                self.prepare_latency_ms_total,
                self.prepare_ops_total,
            ),
            (
                "rustcdc_runtime_batch_delivery_latency_ms",
                "Histogram of runtime batch delivery latency in milliseconds",
                self.batch_delivery_latency_ms_buckets,
                self.batch_delivery_latency_ms_total,
                self.batch_delivery_ops_total,
            ),
            (
                "rustcdc_runtime_checkpoint_commit_latency_ms",
                "Histogram of checkpoint commit latency in milliseconds",
                self.checkpoint_commit_latency_ms_buckets,
                self.checkpoint_commit_latency_ms_total,
                self.checkpoint_commit_ops_total,
            ),
        ] {
            encoder.histogram(metric_name, help, &sink_labels, &buckets, total, count);
        }

        encoder.finish()
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)] // test shim mirroring sink_metrics_snapshot's field list
pub(super) fn sink_metrics_prometheus(
    sink_name: &str,
    requested_delivery_contract: &str,
    delivery_contract_satisfied: bool,
    sink_delivery_guarantee: &str,
    sink_idempotent_delivery_capable: bool,
    sink_transactional_checkpoint_barrier_capable: bool,
    sink_send_ops_total: u64,
    sink_send_latency_ms_total: u64,
    sink_send_latency_ms_last: u64,
    sink_send_latency_ms_buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    sink_flush_ops_total: u64,
    sink_flush_latency_ms_total: u64,
    sink_flush_latency_ms_last: u64,
    sink_flush_latency_ms_buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    transform_ops_total: u64,
    transform_latency_ms_total: u64,
    transform_latency_ms_last: u64,
    transform_latency_ms_buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    prepare_ops_total: u64,
    prepare_latency_ms_total: u64,
    prepare_latency_ms_last: u64,
    prepare_latency_ms_buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    batch_delivery_ops_total: u64,
    batch_delivery_latency_ms_total: u64,
    batch_delivery_latency_ms_last: u64,
    batch_delivery_latency_ms_buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    checkpoint_commit_ops_total: u64,
    checkpoint_commit_latency_ms_total: u64,
    checkpoint_commit_latency_ms_last: u64,
    checkpoint_commit_latency_ms_buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    sink_queue_depth_last: u64,
    sink_queue_depth_p95: u64,
    runtime_soak_duration_seconds: u64,
    sink_retry_rate: f64,
    sink_retries_total: u64,
    sink_dlq_total: u64,
    sink_retryable_status_429_total: u64,
    sink_retryable_status_5xx_total: u64,
    sink_retryable_error_timeout_total: u64,
    sink_retryable_error_other_total: u64,
    sink_terminal_status_4xx_total: u64,
    sink_terminal_status_other_total: u64,
    sink_terminal_error_timeout_total: u64,
    sink_terminal_error_other_total: u64,
    sink_iceberg_flush_lock_contention_events_total: u64,
    sink_iceberg_flush_lock_contention_ms_total: u64,
    sink_iceberg_flush_lock_contention_ms_max: u64,
) -> String {
    sink_metrics_snapshot(
        sink_name,
        requested_delivery_contract,
        delivery_contract_satisfied,
        sink_delivery_guarantee,
        sink_idempotent_delivery_capable,
        sink_transactional_checkpoint_barrier_capable,
        sink_send_ops_total,
        sink_send_latency_ms_total,
        sink_send_latency_ms_last,
        sink_send_latency_ms_buckets,
        sink_flush_ops_total,
        sink_flush_latency_ms_total,
        sink_flush_latency_ms_last,
        sink_flush_latency_ms_buckets,
        transform_ops_total,
        transform_latency_ms_total,
        transform_latency_ms_last,
        transform_latency_ms_buckets,
        &transform::WasmRuntimeMetricsSnapshot::default(),
        prepare_ops_total,
        prepare_latency_ms_total,
        prepare_latency_ms_last,
        prepare_latency_ms_buckets,
        batch_delivery_ops_total,
        batch_delivery_latency_ms_total,
        batch_delivery_latency_ms_last,
        batch_delivery_latency_ms_buckets,
        checkpoint_commit_ops_total,
        checkpoint_commit_latency_ms_total,
        checkpoint_commit_latency_ms_last,
        checkpoint_commit_latency_ms_buckets,
        sink_queue_depth_last,
        sink_queue_depth_p95,
        runtime_soak_duration_seconds,
        sink_retry_rate,
        sink_retries_total,
        sink_dlq_total,
        sink_retryable_status_429_total,
        sink_retryable_status_5xx_total,
        sink_retryable_error_timeout_total,
        sink_retryable_error_other_total,
        sink_terminal_status_4xx_total,
        sink_terminal_status_other_total,
        sink_terminal_error_timeout_total,
        sink_terminal_error_other_total,
        sink_iceberg_flush_lock_contention_events_total,
        sink_iceberg_flush_lock_contention_ms_total,
        sink_iceberg_flush_lock_contention_ms_max,
    )
    .render_prometheus()
}

#[allow(clippy::too_many_arguments)] // flat metric field list mirrors the accumulator layout
pub(super) fn sink_metrics_snapshot(
    sink_name: &str,
    requested_delivery_contract: &str,
    delivery_contract_satisfied: bool,
    sink_delivery_guarantee: &str,
    sink_idempotent_delivery_capable: bool,
    sink_transactional_checkpoint_barrier_capable: bool,
    sink_send_ops_total: u64,
    sink_send_latency_ms_total: u64,
    sink_send_latency_ms_last: u64,
    sink_send_latency_ms_buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    sink_flush_ops_total: u64,
    sink_flush_latency_ms_total: u64,
    sink_flush_latency_ms_last: u64,
    sink_flush_latency_ms_buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    transform_ops_total: u64,
    transform_latency_ms_total: u64,
    transform_latency_ms_last: u64,
    transform_latency_ms_buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    wasm_metrics: &transform::WasmRuntimeMetricsSnapshot,
    prepare_ops_total: u64,
    prepare_latency_ms_total: u64,
    prepare_latency_ms_last: u64,
    prepare_latency_ms_buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    batch_delivery_ops_total: u64,
    batch_delivery_latency_ms_total: u64,
    batch_delivery_latency_ms_last: u64,
    batch_delivery_latency_ms_buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    checkpoint_commit_ops_total: u64,
    checkpoint_commit_latency_ms_total: u64,
    checkpoint_commit_latency_ms_last: u64,
    checkpoint_commit_latency_ms_buckets: &[u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    sink_queue_depth_last: u64,
    sink_queue_depth_p95: u64,
    runtime_soak_duration_seconds: u64,
    sink_retry_rate: f64,
    sink_retries_total: u64,
    sink_dlq_total: u64,
    sink_retryable_status_429_total: u64,
    sink_retryable_status_5xx_total: u64,
    sink_retryable_error_timeout_total: u64,
    sink_retryable_error_other_total: u64,
    sink_terminal_status_4xx_total: u64,
    sink_terminal_status_other_total: u64,
    sink_terminal_error_timeout_total: u64,
    sink_terminal_error_other_total: u64,
    sink_iceberg_flush_lock_contention_events_total: u64,
    sink_iceberg_flush_lock_contention_ms_total: u64,
    sink_iceberg_flush_lock_contention_ms_max: u64,
) -> SinkMetricsSnapshot {
    SinkMetricsSnapshot {
        sink_name: sink_name.to_string(),
        requested_delivery_contract: requested_delivery_contract.to_string(),
        delivery_contract_satisfied,
        sink_delivery_guarantee: sink_delivery_guarantee.to_string(),
        sink_idempotent_delivery_capable,
        sink_transactional_checkpoint_barrier_capable,
        sink_send_ops_total,
        sink_send_latency_ms_total,
        sink_send_latency_ms_last,
        sink_send_latency_ms_buckets: *sink_send_latency_ms_buckets,
        sink_flush_ops_total,
        sink_flush_latency_ms_total,
        sink_flush_latency_ms_last,
        sink_flush_latency_ms_buckets: *sink_flush_latency_ms_buckets,
        transform_ops_total,
        transform_latency_ms_total,
        transform_latency_ms_last,
        transform_latency_ms_buckets: *transform_latency_ms_buckets,
        transform_wasm_instance_pool_size: wasm_metrics.instance_pool_size,
        transform_wasm_invocations_total: wasm_metrics.transform_total,
        transform_wasm_errors_total: wasm_metrics.transform_error_total,
        transform_wasm_filtered_total: wasm_metrics.filtered_total,
        transform_wasm_timeout_total: wasm_metrics.timeout_total,
        prepare_ops_total,
        prepare_latency_ms_total,
        prepare_latency_ms_last,
        prepare_latency_ms_buckets: *prepare_latency_ms_buckets,
        batch_delivery_ops_total,
        batch_delivery_latency_ms_total,
        batch_delivery_latency_ms_last,
        batch_delivery_latency_ms_buckets: *batch_delivery_latency_ms_buckets,
        checkpoint_commit_ops_total,
        checkpoint_commit_latency_ms_total,
        checkpoint_commit_latency_ms_last,
        checkpoint_commit_latency_ms_buckets: *checkpoint_commit_latency_ms_buckets,
        sink_queue_depth_last,
        sink_queue_depth_p95,
        runtime_soak_duration_seconds,
        sink_retry_rate,
        sink_retries_total,
        sink_dlq_total,
        sink_http_requests_total: 0,
        sink_http_request_amplification: 0.0,
        sink_http_batch_size_p50: 0,
        sink_http_batch_size_p95: 0,
        sink_http_batch_oldest_event_age_ms_last: 0,
        sink_http_pending_events: 0,
        sink_http_pending_bytes: 0,
        sink_http_pending_bytes_high_watermark: 0,
        sink_http_retry_delay_ms_p50: 0,
        sink_http_retry_delay_ms_p95: 0,
        sink_http_batch_retry_duration_ms_total: 0,
        sink_http_batch_retry_duration_ms_avg: 0.0,
        sink_http_batch_retry_duration_ms_last: 0,
        sink_http_batch_retry_duration_ms_p50: 0,
        sink_http_batch_retry_duration_ms_p95: 0,
        sink_retryable_status_429_total,
        sink_retryable_status_5xx_total,
        sink_retryable_error_timeout_total,
        sink_retryable_error_other_total,
        sink_terminal_status_4xx_total,
        sink_terminal_status_other_total,
        sink_terminal_error_timeout_total,
        sink_terminal_error_other_total,
        sink_iceberg_flush_lock_contention_events_total,
        sink_iceberg_flush_lock_contention_ms_total,
        sink_iceberg_flush_lock_contention_ms_max,
        data_events_total: 0,
        data_duplicates_total: 0,
        data_reorders_total: 0,
        end_to_end_ack_lag_samples_total: 0,
        end_to_end_ack_lag_ms_total: 0,
        end_to_end_ack_lag_ms_last: 0,
        stream_correctness: Vec::new(),
    }
}

#[cfg(test)]
pub(super) fn recoverable_error_metrics_prometheus(
    total: u64,
    consecutive: u64,
    backoff_ms: u64,
    backoff_last_ms: u64,
    breaker_open_total: u64,
    breaker_open_consecutive: u64,
) -> String {
    recoverable_error_metrics_snapshot(
        total,
        consecutive,
        backoff_ms,
        backoff_last_ms,
        breaker_open_total,
        breaker_open_consecutive,
        0, // lifetime_breaker_open_total not exercised in unit tests
    )
    .render_prometheus()
}

pub(super) fn recoverable_error_metrics_snapshot(
    total: u64,
    consecutive: u64,
    backoff_ms: u64,
    backoff_last_ms: u64,
    breaker_open_total: u64,
    breaker_open_consecutive: u64,
    lifetime_breaker_open_total: u64,
) -> RecoverableErrorMetrics {
    RecoverableErrorMetrics {
        total,
        consecutive,
        backoff_ms,
        backoff_last_ms,
        breaker_open_total,
        breaker_open_consecutive,
        kafka_topic_state_corruption_detected_total:
            state::kafka_topic_state_corruption_detected_total(),
        kafka_topic_state_bootstrap_seeded_total: state::kafka_topic_state_bootstrap_seeded_total(),
        opendal_state_write_failures_total: state::opendal_state_write_failures_total(),
        lifetime_breaker_open_total,
    }
}

pub(super) struct RuntimeLoopMetricsAccumulator {
    sink_name: String,
    requested_delivery_contract: String,
    delivery_contract_satisfied: bool,
    sink_delivery_guarantee: String,
    sink_idempotent_delivery_capable: bool,
    sink_transactional_checkpoint_barrier_capable: bool,
    sink_send_ops_total: u64,
    sink_send_latency_ms_total: u64,
    sink_send_latency_ms_last: u64,
    sink_send_latency_ms_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    sink_flush_ops_total: u64,
    sink_flush_latency_ms_total: u64,
    sink_flush_latency_ms_last: u64,
    sink_flush_latency_ms_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    transform_ops_total: u64,
    transform_latency_ms_total: u64,
    transform_latency_ms_last: u64,
    transform_latency_ms_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    prepare_ops_total: u64,
    prepare_latency_ms_total: u64,
    prepare_latency_ms_last: u64,
    prepare_latency_ms_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    batch_delivery_ops_total: u64,
    batch_delivery_latency_ms_total: u64,
    batch_delivery_latency_ms_last: u64,
    batch_delivery_latency_ms_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    checkpoint_commit_ops_total: u64,
    checkpoint_commit_latency_ms_total: u64,
    checkpoint_commit_latency_ms_last: u64,
    checkpoint_commit_latency_ms_buckets: [u64; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
    sink_retries_total: u64,
    sink_dlq_total: u64,
    sink_http_requests_total: u64,
    sink_http_batch_size_samples_total: u64,
    sink_http_batch_size_bucket_counts: [u64; HTTP_BATCH_SIZE_BUCKETS.len()],
    sink_http_batch_oldest_event_age_ms_last: u64,
    sink_http_pending_events: u64,
    sink_http_pending_bytes: u64,
    sink_http_pending_bytes_high_watermark: u64,
    sink_http_retry_delay_samples_total: u64,
    sink_http_retry_delay_bucket_counts: [u64; HTTP_RETRY_DELAY_MS_BUCKETS.len()],
    sink_http_batch_retry_duration_samples_total: u64,
    sink_http_batch_retry_duration_bucket_counts: [u64; HTTP_BATCH_RETRY_DURATION_MS_BUCKETS.len()],
    sink_http_batch_retry_duration_ms_total: u64,
    sink_http_batch_retry_duration_ms_last: u64,
    sink_retryable_status_429_total: u64,
    sink_retryable_status_5xx_total: u64,
    sink_retryable_error_timeout_total: u64,
    sink_retryable_error_other_total: u64,
    sink_terminal_status_4xx_total: u64,
    sink_terminal_status_other_total: u64,
    sink_terminal_error_timeout_total: u64,
    sink_terminal_error_other_total: u64,
    sink_iceberg_flush_lock_contention_events_total: u64,
    sink_iceberg_flush_lock_contention_ms_total: u64,
    sink_iceberg_flush_lock_contention_ms_max: u64,
    data_events_total: u64,
    data_duplicates_total: u64,
    data_reorders_total: u64,
    end_to_end_ack_lag_samples_total: u64,
    end_to_end_ack_lag_ms_total: u64,
    end_to_end_ack_lag_ms_last: u64,
    stream_correctness: BTreeMap<String, StreamCorrectnessMetricsAccumulator>,
    stream_last_source_ts_ms: HashMap<String, u64>,
    stream_last_source_sequence: HashMap<String, u64>,
    recent_fingerprints: HashSet<(String, String)>,
    recent_fingerprint_window: VecDeque<(String, String)>,
    dedup_window_size: usize,
    sink_queue_depth_last: u64,
    sink_queue_depth_window: VecDeque<u64>,
    queue_depth_p95_window_samples: usize,
    pipeline_started: Instant,
    wasm_metrics: transform::WasmRuntimeMetricsSnapshot,
    last_health_verdict: Option<rustcdc::core::HealthVerdict>,
}

impl RuntimeLoopMetricsAccumulator {
    #[allow(clippy::too_many_arguments)] // constructor mirrors sink capability flags
    pub(super) fn new(
        sink_name: &str,
        requested_delivery_contract: &str,
        delivery_contract_satisfied: bool,
        sink_delivery_guarantee: &str,
        sink_idempotent_delivery_capable: bool,
        sink_transactional_checkpoint_barrier_capable: bool,
        queue_depth_p95_window_samples: usize,
        dedup_window_size: usize,
    ) -> Self {
        Self {
            sink_name: sink_name.to_string(),
            requested_delivery_contract: requested_delivery_contract.to_string(),
            delivery_contract_satisfied,
            sink_delivery_guarantee: sink_delivery_guarantee.to_string(),
            sink_idempotent_delivery_capable,
            sink_transactional_checkpoint_barrier_capable,
            sink_send_ops_total: 0,
            sink_send_latency_ms_total: 0,
            sink_send_latency_ms_last: 0,
            sink_send_latency_ms_buckets: [0; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
            sink_flush_ops_total: 0,
            sink_flush_latency_ms_total: 0,
            sink_flush_latency_ms_last: 0,
            sink_flush_latency_ms_buckets: [0; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
            transform_ops_total: 0,
            transform_latency_ms_total: 0,
            transform_latency_ms_last: 0,
            transform_latency_ms_buckets: [0; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
            prepare_ops_total: 0,
            prepare_latency_ms_total: 0,
            prepare_latency_ms_last: 0,
            prepare_latency_ms_buckets: [0; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
            batch_delivery_ops_total: 0,
            batch_delivery_latency_ms_total: 0,
            batch_delivery_latency_ms_last: 0,
            batch_delivery_latency_ms_buckets: [0; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
            checkpoint_commit_ops_total: 0,
            checkpoint_commit_latency_ms_total: 0,
            checkpoint_commit_latency_ms_last: 0,
            checkpoint_commit_latency_ms_buckets: [0; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
            sink_retries_total: 0,
            sink_dlq_total: 0,
            sink_http_requests_total: 0,
            sink_http_batch_size_samples_total: 0,
            sink_http_batch_size_bucket_counts: [0; HTTP_BATCH_SIZE_BUCKETS.len()],
            sink_http_batch_oldest_event_age_ms_last: 0,
            sink_http_pending_events: 0,
            sink_http_pending_bytes: 0,
            sink_http_pending_bytes_high_watermark: 0,
            sink_http_retry_delay_samples_total: 0,
            sink_http_retry_delay_bucket_counts: [0; HTTP_RETRY_DELAY_MS_BUCKETS.len()],
            sink_http_batch_retry_duration_samples_total: 0,
            sink_http_batch_retry_duration_bucket_counts: [0; HTTP_BATCH_RETRY_DURATION_MS_BUCKETS
                .len()],
            sink_http_batch_retry_duration_ms_total: 0,
            sink_http_batch_retry_duration_ms_last: 0,
            sink_retryable_status_429_total: 0,
            sink_retryable_status_5xx_total: 0,
            sink_retryable_error_timeout_total: 0,
            sink_retryable_error_other_total: 0,
            sink_terminal_status_4xx_total: 0,
            sink_terminal_status_other_total: 0,
            sink_terminal_error_timeout_total: 0,
            sink_terminal_error_other_total: 0,
            sink_iceberg_flush_lock_contention_events_total: 0,
            sink_iceberg_flush_lock_contention_ms_total: 0,
            sink_iceberg_flush_lock_contention_ms_max: 0,
            data_events_total: 0,
            data_duplicates_total: 0,
            data_reorders_total: 0,
            end_to_end_ack_lag_samples_total: 0,
            end_to_end_ack_lag_ms_total: 0,
            end_to_end_ack_lag_ms_last: 0,
            stream_correctness: BTreeMap::new(),
            stream_last_source_ts_ms: HashMap::new(),
            stream_last_source_sequence: HashMap::new(),
            recent_fingerprints: HashSet::new(),
            recent_fingerprint_window: VecDeque::with_capacity(dedup_window_size),
            dedup_window_size,
            sink_queue_depth_last: 0,
            sink_queue_depth_window: VecDeque::with_capacity(queue_depth_p95_window_samples),
            queue_depth_p95_window_samples,
            pipeline_started: Instant::now(),
            wasm_metrics: transform::WasmRuntimeMetricsSnapshot::default(),
            last_health_verdict: None,
        }
    }

    /// Surface runtime health-verdict transitions in the logs.
    ///
    /// The one-hot `rustcdc_runtime_health` gauge is the alerting source of truth;
    /// this makes the *moment* of a transition (and the stall reason) greppable next
    /// to whatever else the pipeline logged at that time.
    pub(super) fn observe_health_transition(&mut self, admin: &RuntimeAdminSnapshot) {
        use rustcdc::core::HealthVerdict;

        if self.last_health_verdict.as_ref() == Some(&admin.health) {
            return;
        }
        let previous = self.last_health_verdict.replace(admin.health.clone());

        match &admin.health {
            HealthVerdict::Stalled { reason } => {
                tracing::warn!(
                    verdict = admin.health.as_str(),
                    previous = previous.as_ref().map(HealthVerdict::as_str),
                    reason = %reason,
                    "runtime health degraded to stalled"
                );
            }
            verdict => {
                if matches!(previous, Some(HealthVerdict::Stalled { .. })) {
                    tracing::info!(
                        verdict = verdict.as_str(),
                        "runtime health recovered from stalled"
                    );
                } else {
                    tracing::debug!(
                        verdict = verdict.as_str(),
                        previous = previous.as_ref().map(HealthVerdict::as_str),
                        "runtime health verdict changed"
                    );
                }
            }
        }
    }

    /// Refresh the cached WASM metrics snapshot from the live runtime.
    /// Called once per batch by `record_batch_metrics_and_admin`.
    pub(super) async fn update_wasm_metrics(&mut self, pipeline: &transform::TransformPipeline) {
        self.wasm_metrics = pipeline.wasm_metrics().await;
    }

    pub(super) fn merge_batch_processing_stats(&mut self, stats: &run_batch::BatchProcessingStats) {
        self.transform_ops_total = self
            .transform_ops_total
            .saturating_add(stats.prepare.transform_ops_total);
        self.transform_latency_ms_total = self
            .transform_latency_ms_total
            .saturating_add(stats.prepare.transform_latency_ms_total);
        self.transform_latency_ms_last = stats.prepare.transform_latency_ms_last;
        for (index, count) in stats
            .prepare
            .transform_latency_ms_buckets
            .iter()
            .enumerate()
        {
            self.transform_latency_ms_buckets[index] =
                self.transform_latency_ms_buckets[index].saturating_add(*count);
        }

        self.prepare_ops_total = self
            .prepare_ops_total
            .saturating_add(stats.prepare.prepare_ops_total);
        self.prepare_latency_ms_total = self
            .prepare_latency_ms_total
            .saturating_add(stats.prepare.prepare_latency_ms_total);
        self.prepare_latency_ms_last = stats.prepare.prepare_latency_ms_last;
        for (index, count) in stats.prepare.prepare_latency_ms_buckets.iter().enumerate() {
            self.prepare_latency_ms_buckets[index] =
                self.prepare_latency_ms_buckets[index].saturating_add(*count);
        }

        self.sink_send_ops_total = self
            .sink_send_ops_total
            .saturating_add(stats.delivery.sink_send_ops_total);
        self.sink_send_latency_ms_total = self
            .sink_send_latency_ms_total
            .saturating_add(stats.delivery.sink_send_latency_ms_total);
        self.sink_send_latency_ms_last = stats.delivery.sink_send_latency_ms_last;
        for (index, count) in stats
            .delivery
            .sink_send_latency_ms_buckets
            .iter()
            .enumerate()
        {
            self.sink_send_latency_ms_buckets[index] =
                self.sink_send_latency_ms_buckets[index].saturating_add(*count);
        }

        self.sink_flush_ops_total = self
            .sink_flush_ops_total
            .saturating_add(stats.delivery.sink_flush_ops_total);
        self.sink_flush_latency_ms_total = self
            .sink_flush_latency_ms_total
            .saturating_add(stats.delivery.sink_flush_latency_ms_total);
        self.sink_flush_latency_ms_last = stats.delivery.sink_flush_latency_ms_last;
        for (index, count) in stats
            .delivery
            .sink_flush_latency_ms_buckets
            .iter()
            .enumerate()
        {
            self.sink_flush_latency_ms_buckets[index] =
                self.sink_flush_latency_ms_buckets[index].saturating_add(*count);
        }
    }

    pub(super) fn record_checkpoint_commit_latency(&mut self, latency_ms: u64) {
        self.checkpoint_commit_ops_total = self.checkpoint_commit_ops_total.saturating_add(1);
        self.checkpoint_commit_latency_ms_total = self
            .checkpoint_commit_latency_ms_total
            .saturating_add(latency_ms);
        self.checkpoint_commit_latency_ms_last = latency_ms;
        observe_latency_histogram_bucket(
            &mut self.checkpoint_commit_latency_ms_buckets,
            latency_ms,
        );
    }

    pub(super) fn record_batch_delivery_latency(&mut self, latency_ms: u64) {
        self.batch_delivery_ops_total = self.batch_delivery_ops_total.saturating_add(1);
        self.batch_delivery_latency_ms_total = self
            .batch_delivery_latency_ms_total
            .saturating_add(latency_ms);
        self.batch_delivery_latency_ms_last = latency_ms;
        observe_latency_histogram_bucket(&mut self.batch_delivery_latency_ms_buckets, latency_ms);
    }

    pub(super) fn record_sink_queue_depth(&mut self, queue_depth: u64) {
        self.sink_queue_depth_last = queue_depth;
        self.sink_queue_depth_window.push_back(queue_depth);
        if self.sink_queue_depth_window.len() > self.queue_depth_p95_window_samples {
            self.sink_queue_depth_window.pop_front();
        }
    }

    pub(super) fn record_sink_delivery_delta(
        &mut self,
        before: crate::sink::SinkDeliveryMetrics,
        after: crate::sink::SinkDeliveryMetrics,
    ) {
        self.sink_retries_total = self
            .sink_retries_total
            .saturating_add(after.retries_total.saturating_sub(before.retries_total));
        self.sink_dlq_total = self
            .sink_dlq_total
            .saturating_add(after.dlq_total.saturating_sub(before.dlq_total));
        self.sink_http_requests_total = self.sink_http_requests_total.saturating_add(
            after
                .http_requests_total
                .saturating_sub(before.http_requests_total),
        );
        self.sink_http_batch_size_samples_total =
            self.sink_http_batch_size_samples_total.saturating_add(
                after
                    .http_batch_size_samples_total
                    .saturating_sub(before.http_batch_size_samples_total),
            );
        self.sink_http_batch_oldest_event_age_ms_last = after.http_batch_oldest_event_age_ms_last;
        self.sink_http_pending_events = after.http_pending_events;
        self.sink_http_pending_bytes = after.http_pending_bytes;
        self.sink_http_pending_bytes_high_watermark = self
            .sink_http_pending_bytes_high_watermark
            .max(after.http_pending_bytes_high_watermark);
        for (index, bucket) in self
            .sink_http_batch_size_bucket_counts
            .iter_mut()
            .enumerate()
        {
            *bucket = bucket.saturating_add(
                after.http_batch_size_bucket_counts[index]
                    .saturating_sub(before.http_batch_size_bucket_counts[index]),
            );
        }
        self.sink_http_retry_delay_samples_total =
            self.sink_http_retry_delay_samples_total.saturating_add(
                after
                    .http_retry_delay_samples_total
                    .saturating_sub(before.http_retry_delay_samples_total),
            );
        for (index, bucket) in self
            .sink_http_retry_delay_bucket_counts
            .iter_mut()
            .enumerate()
        {
            *bucket = bucket.saturating_add(
                after.http_retry_delay_bucket_counts[index]
                    .saturating_sub(before.http_retry_delay_bucket_counts[index]),
            );
        }
        self.sink_http_batch_retry_duration_samples_total = self
            .sink_http_batch_retry_duration_samples_total
            .saturating_add(
                after
                    .http_batch_retry_duration_samples_total
                    .saturating_sub(before.http_batch_retry_duration_samples_total),
            );
        self.sink_http_batch_retry_duration_ms_total =
            self.sink_http_batch_retry_duration_ms_total.saturating_add(
                after
                    .http_batch_retry_duration_ms_total
                    .saturating_sub(before.http_batch_retry_duration_ms_total),
            );
        self.sink_http_batch_retry_duration_ms_last = after.http_batch_retry_duration_ms_last;
        for (index, bucket) in self
            .sink_http_batch_retry_duration_bucket_counts
            .iter_mut()
            .enumerate()
        {
            *bucket = bucket.saturating_add(
                after.http_batch_retry_duration_bucket_counts[index]
                    .saturating_sub(before.http_batch_retry_duration_bucket_counts[index]),
            );
        }
        self.sink_retryable_status_429_total = self.sink_retryable_status_429_total.saturating_add(
            after
                .retryable_status_429_total
                .saturating_sub(before.retryable_status_429_total),
        );
        self.sink_retryable_status_5xx_total = self.sink_retryable_status_5xx_total.saturating_add(
            after
                .retryable_status_5xx_total
                .saturating_sub(before.retryable_status_5xx_total),
        );
        self.sink_retryable_error_timeout_total =
            self.sink_retryable_error_timeout_total.saturating_add(
                after
                    .retryable_error_timeout_total
                    .saturating_sub(before.retryable_error_timeout_total),
            );
        self.sink_retryable_error_other_total =
            self.sink_retryable_error_other_total.saturating_add(
                after
                    .retryable_error_other_total
                    .saturating_sub(before.retryable_error_other_total),
            );
        self.sink_terminal_status_4xx_total = self.sink_terminal_status_4xx_total.saturating_add(
            after
                .terminal_status_4xx_total
                .saturating_sub(before.terminal_status_4xx_total),
        );
        self.sink_terminal_status_other_total =
            self.sink_terminal_status_other_total.saturating_add(
                after
                    .terminal_status_other_total
                    .saturating_sub(before.terminal_status_other_total),
            );
        self.sink_terminal_error_timeout_total =
            self.sink_terminal_error_timeout_total.saturating_add(
                after
                    .terminal_error_timeout_total
                    .saturating_sub(before.terminal_error_timeout_total),
            );
        self.sink_terminal_error_other_total = self.sink_terminal_error_other_total.saturating_add(
            after
                .terminal_error_other_total
                .saturating_sub(before.terminal_error_other_total),
        );
        self.sink_iceberg_flush_lock_contention_events_total = self
            .sink_iceberg_flush_lock_contention_events_total
            .saturating_add(
                after
                    .iceberg_flush_lock_contention_events_total
                    .saturating_sub(before.iceberg_flush_lock_contention_events_total),
            );
        self.sink_iceberg_flush_lock_contention_ms_total = self
            .sink_iceberg_flush_lock_contention_ms_total
            .saturating_add(
                after
                    .iceberg_flush_lock_contention_ms_total
                    .saturating_sub(before.iceberg_flush_lock_contention_ms_total),
            );
        self.sink_iceberg_flush_lock_contention_ms_max = self
            .sink_iceberg_flush_lock_contention_ms_max
            .max(after.iceberg_flush_lock_contention_ms_max);
    }

    pub(super) fn record_correctness_sample(&mut self, sample: &CorrectnessSample) {
        let stream_name = normalize_stream_name_sample(sample);
        let stream_slot = if self.stream_correctness.contains_key(&stream_name)
            || self.stream_correctness.len() < MAX_CORRECTNESS_STREAMS
        {
            stream_name
        } else {
            "__other__".to_string()
        };
        self.data_events_total = self.data_events_total.saturating_add(1);

        let mut duplicate = false;
        let mut reordered = false;
        let mut ack_lag_ms: Option<u64> = None;

        if let Some(fingerprint) = &sample.fingerprint {
            let fp_key = (stream_slot.clone(), fingerprint.clone());
            if self.recent_fingerprints.contains(&fp_key) {
                duplicate = true;
            } else {
                self.recent_fingerprints.insert(fp_key.clone());
                self.recent_fingerprint_window.push_back(fp_key);
                if self.recent_fingerprint_window.len() > self.dedup_window_size {
                    if let Some(old) = self.recent_fingerprint_window.pop_front() {
                        self.recent_fingerprints.remove(&old);
                    }
                }
            }
        }

        let source_sequence = normalize_source_sequence_sample(sample);
        if let Some(source_sequence) = source_sequence {
            if let Some(previous) = self.stream_last_source_sequence.get(&stream_slot) {
                if source_sequence < *previous {
                    reordered = true;
                }
            }
            self.stream_last_source_sequence
                .insert(stream_slot.clone(), source_sequence);
        } else if let Some(source_ts_ms) = normalize_source_timestamp_ms_sample(sample) {
            if let Some(previous) = self.stream_last_source_ts_ms.get(&stream_slot) {
                if source_ts_ms < *previous {
                    reordered = true;
                }
            }
            self.stream_last_source_ts_ms
                .insert(stream_slot.clone(), source_ts_ms);
        }

        if let Some(source_ts_ms) = normalize_source_timestamp_ms_sample(sample) {
            let now_ms = now_unix_ms();
            ack_lag_ms = Some(now_ms.saturating_sub(source_ts_ms));
        }

        if duplicate {
            self.data_duplicates_total = self.data_duplicates_total.saturating_add(1);
        }
        if reordered {
            self.data_reorders_total = self.data_reorders_total.saturating_add(1);
        }
        if let Some(lag_ms) = ack_lag_ms {
            self.end_to_end_ack_lag_samples_total =
                self.end_to_end_ack_lag_samples_total.saturating_add(1);
            self.end_to_end_ack_lag_ms_total =
                self.end_to_end_ack_lag_ms_total.saturating_add(lag_ms);
            self.end_to_end_ack_lag_ms_last = lag_ms;
        }

        let stream = self.stream_correctness.entry(stream_slot).or_default();
        stream.events_total = stream.events_total.saturating_add(1);
        if duplicate {
            stream.duplicates_total = stream.duplicates_total.saturating_add(1);
        }
        if reordered {
            stream.reorders_total = stream.reorders_total.saturating_add(1);
        }
        if let Some(lag_ms) = ack_lag_ms {
            stream.ack_lag_samples_total = stream.ack_lag_samples_total.saturating_add(1);
            stream.ack_lag_ms_total = stream.ack_lag_ms_total.saturating_add(lag_ms);
            stream.ack_lag_ms_last = lag_ms;
        }
    }

    pub(super) fn build_runtime_metrics_snapshot(
        &self,
        runtime_admin: RuntimeAdminSnapshot,
        recoverable: RecoverableErrorSnapshot,
    ) -> RuntimeMetricsSnapshot {
        let sink_retry_rate = if self.sink_send_ops_total == 0 {
            0.0
        } else {
            self.sink_retries_total as f64 / self.sink_send_ops_total as f64
        };

        let mut sink_metrics = sink_metrics_snapshot(
            &self.sink_name,
            &self.requested_delivery_contract,
            self.delivery_contract_satisfied,
            &self.sink_delivery_guarantee,
            self.sink_idempotent_delivery_capable,
            self.sink_transactional_checkpoint_barrier_capable,
            self.sink_send_ops_total,
            self.sink_send_latency_ms_total,
            self.sink_send_latency_ms_last,
            &self.sink_send_latency_ms_buckets,
            self.sink_flush_ops_total,
            self.sink_flush_latency_ms_total,
            self.sink_flush_latency_ms_last,
            &self.sink_flush_latency_ms_buckets,
            self.transform_ops_total,
            self.transform_latency_ms_total,
            self.transform_latency_ms_last,
            &self.transform_latency_ms_buckets,
            &self.wasm_metrics,
            self.prepare_ops_total,
            self.prepare_latency_ms_total,
            self.prepare_latency_ms_last,
            &self.prepare_latency_ms_buckets,
            self.batch_delivery_ops_total,
            self.batch_delivery_latency_ms_total,
            self.batch_delivery_latency_ms_last,
            &self.batch_delivery_latency_ms_buckets,
            self.checkpoint_commit_ops_total,
            self.checkpoint_commit_latency_ms_total,
            self.checkpoint_commit_latency_ms_last,
            &self.checkpoint_commit_latency_ms_buckets,
            self.sink_queue_depth_last,
            p95_u64_window(&self.sink_queue_depth_window),
            self.pipeline_started.elapsed().as_secs(),
            sink_retry_rate,
            self.sink_retries_total,
            self.sink_dlq_total,
            self.sink_retryable_status_429_total,
            self.sink_retryable_status_5xx_total,
            self.sink_retryable_error_timeout_total,
            self.sink_retryable_error_other_total,
            self.sink_terminal_status_4xx_total,
            self.sink_terminal_status_other_total,
            self.sink_terminal_error_timeout_total,
            self.sink_terminal_error_other_total,
            self.sink_iceberg_flush_lock_contention_events_total,
            self.sink_iceberg_flush_lock_contention_ms_total,
            self.sink_iceberg_flush_lock_contention_ms_max,
        );

        sink_metrics.data_events_total = self.data_events_total;
        sink_metrics.data_duplicates_total = self.data_duplicates_total;
        sink_metrics.data_reorders_total = self.data_reorders_total;
        sink_metrics.end_to_end_ack_lag_samples_total = self.end_to_end_ack_lag_samples_total;
        sink_metrics.end_to_end_ack_lag_ms_total = self.end_to_end_ack_lag_ms_total;
        sink_metrics.end_to_end_ack_lag_ms_last = self.end_to_end_ack_lag_ms_last;
        sink_metrics.sink_http_requests_total = self.sink_http_requests_total;
        sink_metrics.sink_http_request_amplification = if self.sink_send_ops_total == 0 {
            0.0
        } else {
            self.sink_http_requests_total as f64 / self.sink_send_ops_total as f64
        };
        sink_metrics.sink_http_batch_size_p50 = quantile_u64_histogram_upper_bound(
            &self.sink_http_batch_size_bucket_counts,
            &HTTP_BATCH_SIZE_BUCKETS,
            self.sink_http_batch_size_samples_total,
            0.50,
        );
        sink_metrics.sink_http_batch_size_p95 = quantile_u64_histogram_upper_bound(
            &self.sink_http_batch_size_bucket_counts,
            &HTTP_BATCH_SIZE_BUCKETS,
            self.sink_http_batch_size_samples_total,
            0.95,
        );
        sink_metrics.sink_http_batch_oldest_event_age_ms_last =
            self.sink_http_batch_oldest_event_age_ms_last;
        sink_metrics.sink_http_pending_events = self.sink_http_pending_events;
        sink_metrics.sink_http_pending_bytes = self.sink_http_pending_bytes;
        sink_metrics.sink_http_pending_bytes_high_watermark =
            self.sink_http_pending_bytes_high_watermark;
        sink_metrics.sink_http_retry_delay_ms_p50 = quantile_u64_histogram_upper_bound(
            &self.sink_http_retry_delay_bucket_counts,
            &HTTP_RETRY_DELAY_MS_BUCKETS,
            self.sink_http_retry_delay_samples_total,
            0.50,
        );
        sink_metrics.sink_http_retry_delay_ms_p95 = quantile_u64_histogram_upper_bound(
            &self.sink_http_retry_delay_bucket_counts,
            &HTTP_RETRY_DELAY_MS_BUCKETS,
            self.sink_http_retry_delay_samples_total,
            0.95,
        );
        sink_metrics.sink_http_batch_retry_duration_ms_total =
            self.sink_http_batch_retry_duration_ms_total;
        sink_metrics.sink_http_batch_retry_duration_ms_avg = average_latency_ms(
            self.sink_http_batch_retry_duration_ms_total,
            self.sink_http_batch_retry_duration_samples_total,
        );
        sink_metrics.sink_http_batch_retry_duration_ms_last =
            self.sink_http_batch_retry_duration_ms_last;
        sink_metrics.sink_http_batch_retry_duration_ms_p50 = quantile_u64_histogram_upper_bound(
            &self.sink_http_batch_retry_duration_bucket_counts,
            &HTTP_BATCH_RETRY_DURATION_MS_BUCKETS,
            self.sink_http_batch_retry_duration_samples_total,
            0.50,
        );
        sink_metrics.sink_http_batch_retry_duration_ms_p95 = quantile_u64_histogram_upper_bound(
            &self.sink_http_batch_retry_duration_bucket_counts,
            &HTTP_BATCH_RETRY_DURATION_MS_BUCKETS,
            self.sink_http_batch_retry_duration_samples_total,
            0.95,
        );
        sink_metrics.stream_correctness = self
            .stream_correctness
            .iter()
            .map(|(stream, stats)| StreamCorrectnessMetricsSnapshot {
                stream: stream.clone(),
                events_total: stats.events_total,
                duplicates_total: stats.duplicates_total,
                reorders_total: stats.reorders_total,
                ack_lag_samples_total: stats.ack_lag_samples_total,
                ack_lag_ms_total: stats.ack_lag_ms_total,
                ack_lag_ms_last: stats.ack_lag_ms_last,
            })
            .collect();

        RuntimeMetricsSnapshot {
            runtime_admin,
            recoverable: recoverable_error_metrics_snapshot(
                recoverable.total,
                recoverable.consecutive,
                recoverable.backoff_ms,
                recoverable.backoff_last_ms,
                recoverable.breaker_open_total,
                recoverable.breaker_open_consecutive,
                recoverable.lifetime_breaker_open_total,
            ),
            sink: sink_metrics,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeMetricsSnapshot {
    pub(crate) runtime_admin: RuntimeAdminSnapshot,
    pub(crate) recoverable: RecoverableErrorMetrics,
    pub(crate) sink: SinkMetricsSnapshot,
}

impl RuntimeMetricsSnapshot {
    pub(crate) fn render_prometheus(&self) -> String {
        format!(
            "{}{}{}",
            runtime_admin_metrics_prometheus(&self.runtime_admin),
            self.recoverable.render_prometheus(),
            self.sink.render_prometheus(),
        )
    }
}

fn runtime_admin_metrics_prometheus(admin: &RuntimeAdminSnapshot) -> String {
    let mut out = String::new();

    out.push_str("# HELP rustcdc_runtime_readiness Runtime readiness (1=ready, 0=not ready).\n");
    out.push_str("# TYPE rustcdc_runtime_readiness gauge\n");
    let _ = writeln!(
        out,
        "rustcdc_runtime_readiness{{state=\"{}\"}} {}",
        admin.state,
        if admin.readiness { 1 } else { 0 }
    );

    out.push_str("# HELP rustcdc_runtime_liveness Runtime liveness (1=alive, 0=stopped).\n");
    out.push_str("# TYPE rustcdc_runtime_liveness gauge\n");
    let _ = writeln!(
        out,
        "rustcdc_runtime_liveness{{state=\"{}\"}} {}",
        admin.state,
        if admin.liveness { 1 } else { 0 }
    );

    out.push_str(
        "# HELP rustcdc_runtime_buffer_depth Number of buffered events waiting for delivery.\n",
    );
    out.push_str("# TYPE rustcdc_runtime_buffer_depth gauge\n");
    let _ = writeln!(out, "rustcdc_runtime_buffer_depth {}", admin.buffer_depth);

    out.push_str(
        "# HELP rustcdc_runtime_in_flight_events Number of delivered but uncommitted events.\n",
    );
    out.push_str("# TYPE rustcdc_runtime_in_flight_events gauge\n");
    let _ = writeln!(
        out,
        "rustcdc_runtime_in_flight_events {}",
        admin.in_flight_events
    );

    out.push_str(
        "# HELP rustcdc_runtime_events_polled_total Total events delivered by runtime batches.\n",
    );
    out.push_str("# TYPE rustcdc_runtime_events_polled_total counter\n");
    let _ = writeln!(
        out,
        "rustcdc_runtime_events_polled_total {}",
        admin.total_events_polled
    );

    out.push_str(
        "# HELP rustcdc_runtime_events_committed_total Total events acknowledged and checkpointed.\n",
    );
    out.push_str("# TYPE rustcdc_runtime_events_committed_total counter\n");
    let _ = writeln!(
        out,
        "rustcdc_runtime_events_committed_total {}",
        admin.total_events_committed
    );

    out.push_str("# HELP rustcdc_runtime_events_deduplicated_total Total events suppressed by runtime idempotency guard.\n");
    out.push_str("# TYPE rustcdc_runtime_events_deduplicated_total counter\n");
    let _ = writeln!(
        out,
        "rustcdc_runtime_events_deduplicated_total {}",
        admin.total_events_deduplicated
    );

    // Data-loss tripwire: a skipped event is dropped AND the checkpoint advances
    // past it, so it is never replayed. Any non-zero value means data was lost.
    out.push_str("# HELP rustcdc_runtime_events_skipped_total Events permanently dropped by TransformErrorPolicy::Skip. Any increase means data loss.\n");
    out.push_str("# TYPE rustcdc_runtime_events_skipped_total counter\n");
    let _ = writeln!(
        out,
        "rustcdc_runtime_events_skipped_total {}",
        admin.total_events_skipped
    );

    // One-hot health verdict: exactly one series is 1 at any time, so an alert
    // rule (`rustcdc_runtime_health{verdict="stalled"} == 1`) is unambiguous and
    // dashboards never see two verdicts at once or a vanished series.
    out.push_str("# HELP rustcdc_runtime_health Derived runtime health verdict (one-hot; exactly one verdict label is 1).\n");
    out.push_str("# TYPE rustcdc_runtime_health gauge\n");
    let active_verdict = admin.health.as_str();
    for verdict in ["healthy", "idle", "stalled", "not_running"] {
        let _ = writeln!(
            out,
            "rustcdc_runtime_health{{verdict=\"{}\"}} {}",
            verdict,
            u8::from(verdict == active_verdict)
        );
    }

    if let Some(checkpoint_age_ms) = admin.checkpoint_age_ms {
        out.push_str(
            "# HELP rustcdc_runtime_checkpoint_age_ms Age of last durable checkpoint in milliseconds.\n",
        );
        out.push_str("# TYPE rustcdc_runtime_checkpoint_age_ms gauge\n");
        let _ = writeln!(
            out,
            "rustcdc_runtime_checkpoint_age_ms {}",
            checkpoint_age_ms
        );
    }

    if let Some(lag_ms) = admin.replication_lag_ms {
        out.push_str("# HELP rustcdc_runtime_replication_lag_ms Estimated replication lag in milliseconds (source event timestamp preferred; poll recency fallback).\n");
        out.push_str("# TYPE rustcdc_runtime_replication_lag_ms gauge\n");
        let _ = writeln!(out, "rustcdc_runtime_replication_lag_ms {}", lag_ms);
    }

    if let Some(slot_lag_bytes) = admin.replication_slot_lag_bytes {
        out.push_str("# HELP rustcdc_runtime_replication_slot_lag_bytes Replication slot WAL lag in bytes (pg_current_wal_lsn - confirmed_flush_lsn). Unbounded growth risks WAL-retention exhaustion.\n");
        out.push_str("# TYPE rustcdc_runtime_replication_slot_lag_bytes gauge\n");
        let _ = writeln!(
            out,
            "rustcdc_runtime_replication_slot_lag_bytes {}",
            slot_lag_bytes
        );
    }

    out.push_str("# HELP rustcdc_runtime_source_capability Connector capability flags.\n");
    out.push_str("# TYPE rustcdc_runtime_source_capability gauge\n");
    out.push_str(&format_capability_metric(
        "snapshot",
        admin.capabilities.snapshot,
    ));
    out.push_str(&format_capability_metric(
        "handoff",
        admin.capabilities.handoff,
    ));
    out.push_str(&format_capability_metric(
        "ddl_capture",
        admin.capabilities.ddl_capture,
    ));
    out.push_str(&format_capability_metric(
        "heartbeat",
        admin.capabilities.heartbeat,
    ));
    out.push_str(&format_capability_metric("tls", admin.capabilities.tls));
    out.push_str(&format_capability_metric(
        "schema_introspection",
        admin.capabilities.schema_introspection,
    ));
    out.push_str(&format_capability_metric(
        "snapshot_checkpoint_resume",
        admin.capabilities.snapshot_checkpoint_resume,
    ));
    out.push_str(&format_capability_metric(
        "truncate",
        admin.capabilities.truncate,
    ));
    out.push_str(&format_capability_metric(
        "incremental_snapshot",
        admin.capabilities.incremental_snapshot,
    ));

    out
}

fn format_capability_metric(capability: &str, enabled: bool) -> String {
    format!(
        "rustcdc_runtime_source_capability{{capability=\"{}\"}} {}\n",
        capability,
        if enabled { 1 } else { 0 }
    )
}

pub(super) fn p95_u64_window(window: &VecDeque<u64>) -> u64 {
    if window.is_empty() {
        return 0;
    }

    let mut values = window.iter().copied().collect::<Vec<_>>();
    let rank = ((values.len() * 95).div_ceil(100)).saturating_sub(1);
    let (_, p95, _) = values.select_nth_unstable(rank);
    *p95
}

#[cfg(test)]
mod tests {
    use rustcdc::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use serde_json::json;

    use super::RuntimeLoopMetricsAccumulator;
    use super::{parse_numeric_offset_component, parse_postgres_lsn, CorrectnessSample};
    use super::{MetricLabel, PrometheusTextEncoder};
    use std::borrow::Cow;

    fn sample_event(source_name: &str, offset: &str, source_timestamp: u64, ts: u64) -> Event {
        Event {
            before: None,
            after: Some(json!({"id": offset, "value": source_timestamp})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: source_name.to_string(),
                offset: offset.to_string(),
                timestamp: source_timestamp,
            },
            ts,
            schema: Some("public".to_string()),
            table: "orders".to_string(),
            primary_key: Some(vec!["id".to_string()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    #[test]
    fn parses_postgres_lsn_into_monotonic_sequence() {
        assert_eq!(parse_postgres_lsn("0/16B6A70"), Some(23816816));
        assert_eq!(parse_postgres_lsn("1/0"), Some(1u64 << 32));
        assert_eq!(parse_postgres_lsn("invalid"), None);
    }

    #[test]
    fn parses_generic_numeric_offset_component() {
        assert_eq!(parse_numeric_offset_component("12345"), Some(12345));
        assert_eq!(
            parse_numeric_offset_component("mysql-bin.000001:42"),
            Some(42)
        );
        assert_eq!(parse_numeric_offset_component("offset=abc"), None);
    }

    #[test]
    fn reorder_detection_prefers_source_sequence_over_timestamp() {
        let mut metrics = RuntimeLoopMetricsAccumulator::new(
            "stdout",
            "at_least_once",
            true,
            "at_least_once",
            false,
            false,
            128,
            50_000,
        );

        let first = sample_event("postgres", "0/10", 2_000, 2_000);
        let second = sample_event("postgres", "0/11", 1_000, 1_000);

        metrics.record_correctness_sample(&CorrectnessSample::from_event(&first));
        metrics.record_correctness_sample(&CorrectnessSample::from_event(&second));

        assert_eq!(parse_postgres_lsn("0/10"), Some(16));
        assert_eq!(parse_postgres_lsn("0/11"), Some(17));
        assert_eq!(metrics.data_reorders_total, 0);
    }

    #[test]
    fn reorder_detection_flags_sequence_regression_even_if_timestamp_increases() {
        let mut metrics = RuntimeLoopMetricsAccumulator::new(
            "stdout",
            "at_least_once",
            true,
            "at_least_once",
            false,
            false,
            128,
            50_000,
        );

        let first = sample_event("postgres", "0/20", 1_000, 1_000);
        let second = sample_event("postgres", "0/1F", 2_000, 2_000);

        metrics.record_correctness_sample(&CorrectnessSample::from_event(&first));
        metrics.record_correctness_sample(&CorrectnessSample::from_event(&second));

        assert_eq!(metrics.data_reorders_total, 1);
    }

    #[test]
    fn prometheus_encoder_escapes_special_chars_in_label_values() {
        let mut encoder = PrometheusTextEncoder::default();
        let labels = [MetricLabel {
            key: "source",
            value: Cow::Borrowed("path\\with\\backslash"),
        }];
        encoder.counter("test_metric", "help text", &labels, 1u64);
        let out = encoder.finish();
        assert!(
            out.contains(r#"source="path\\with\\backslash""#),
            "backslash not escaped: {out}"
        );

        let mut encoder = PrometheusTextEncoder::default();
        let labels = [MetricLabel {
            key: "source",
            value: Cow::Borrowed("value with \"quotes\""),
        }];
        encoder.counter("test_metric2", "help", &labels, 2u64);
        let out = encoder.finish();
        assert!(
            out.contains(r#"source="value with \"quotes\"""#),
            "double-quote not escaped: {out}"
        );

        let mut encoder = PrometheusTextEncoder::default();
        let labels = [MetricLabel {
            key: "msg",
            value: Cow::Borrowed("line1\nline2"),
        }];
        encoder.counter("test_metric3", "help", &labels, 3u64);
        let out = encoder.finish();
        assert!(
            out.contains(r#"msg="line1\nline2""#),
            "newline not escaped: {out}"
        );
    }

    /// The health verdict must render one-hot (exactly one series = 1) so the
    /// alert rule `rustcdc_runtime_health{verdict="stalled"} == 1` is unambiguous
    /// and dashboards never see a vanished series on a verdict change.
    #[test]
    fn runtime_admin_metrics_render_one_hot_health_and_data_loss_counter() {
        // RuntimeAdminSnapshot is #[non_exhaustive]; build it the way the admin API
        // would receive it — through serde.
        let snapshot: rustcdc::core::RuntimeAdminSnapshot = serde_json::from_value(json!({
            "source_type": "postgres",
            "state": "running",
            "readiness": true,
            "liveness": true,
            "capabilities": {
                "snapshot": true,
                "snapshot_checkpoint_resume": true,
                "handoff": true,
                "ddl_capture": true,
                "heartbeat": true,
                "tls": false,
                "schema_introspection": true,
                "truncate": true,
                "incremental_snapshot": true
            },
            "buffer_depth": 0,
            "in_flight_events": 0,
            "snapshot_active": false,
            "stream_active": true,
            "handoff_complete": true,
            "total_events_polled": 10,
            "total_events_committed": 7,
            "total_events_deduplicated": 0,
            "total_events_skipped": 3,
            "health": {"status": "stalled", "reason": "no successful poll in 120s"},
            "started_at_ms": 1,
            "last_poll_at_ms": 2,
            "last_commit_at_ms": 3,
            "checkpoint_age_ms": 5,
            "replication_lag_ms": 7,
            "replication_slot_lag_bytes": 42
        }))
        .expect("snapshot deserializes");

        let out = super::runtime_admin_metrics_prometheus(&snapshot);

        assert!(
            out.contains("rustcdc_runtime_health{verdict=\"stalled\"} 1"),
            "{out}"
        );
        for quiet in ["healthy", "idle", "not_running"] {
            assert!(
                out.contains(&format!("rustcdc_runtime_health{{verdict=\"{quiet}\"}} 0")),
                "verdict {quiet} must render 0:\n{out}"
            );
        }
        assert!(
            out.contains("rustcdc_runtime_events_skipped_total 3"),
            "{out}"
        );
        assert!(
            out.contains("rustcdc_runtime_replication_slot_lag_bytes 42"),
            "{out}"
        );
        // Every HELP/TYPE header must name the metric it precedes.
        for line in out.lines().filter(|l| l.starts_with("# ")) {
            let name = line.split_whitespace().nth(2).unwrap_or("");
            assert!(
                out.lines()
                    .any(|l| !l.starts_with('#') && l.starts_with(name)),
                "header names a metric with no samples: {line}"
            );
        }
    }

    /// `# HELP` and `# TYPE` must appear exactly once per metric family in a
    /// valid Prometheus text-format document.  Verify the encoder tracks
    /// emitted families and skips duplicate headers when the same metric name
    /// is written with multiple different label sets (e.g. per-sink counters in
    /// a fan-out topology).
    #[test]
    fn prometheus_encoder_emits_headers_once_per_metric_family() {
        let mut encoder = PrometheusTextEncoder::default();

        let labels_kafka = [MetricLabel {
            key: "sink",
            value: Cow::Borrowed("kafka"),
        }];
        let labels_http = [MetricLabel {
            key: "sink",
            value: Cow::Borrowed("http"),
        }];

        encoder.counter(
            "rustcdc_sink_send_ops_total",
            "Total sink send operations",
            &labels_kafka,
            10u64,
        );
        encoder.counter(
            "rustcdc_sink_send_ops_total",
            "Total sink send operations",
            &labels_http,
            7u64,
        );

        let out = encoder.finish();

        // Headers must appear exactly once each.
        assert_eq!(
            out.matches("# HELP rustcdc_sink_send_ops_total").count(),
            1,
            "# HELP duplicated:\n{out}"
        );
        assert_eq!(
            out.matches("# TYPE rustcdc_sink_send_ops_total").count(),
            1,
            "# TYPE duplicated:\n{out}"
        );

        // Both time-series values must still be present.
        assert!(
            out.contains(r#"sink="kafka""#),
            "kafka value missing:\n{out}"
        );
        assert!(out.contains(r#"sink="http""#), "http value missing:\n{out}");
    }
}
