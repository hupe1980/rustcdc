//! OpenTelemetry metrics and tracing integrations.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use opentelemetry::{
    global,
    metrics::{Counter, Gauge, Histogram, MeterProvider as _},
    trace::{Span as _, Status, TraceContextExt, Tracer as _},
    Context, KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{metrics::SdkMeterProvider, runtime, trace as sdktrace, Resource};

use crate::core::{Error, Event, EventTracer, MetricsCollector, Operation, Result};

#[derive(Debug, Clone)]
pub struct OTelConfig {
    pub endpoint: String,
    pub service_name: String,
    pub service_version: String,
    pub environment: String,
    pub export_interval_ms: u64,
    pub export_timeout_ms: u64,
}

impl OTelConfig {
    pub fn new(
        endpoint: impl Into<String>,
        service_name: impl Into<String>,
        service_version: impl Into<String>,
        environment: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            service_name: service_name.into(),
            service_version: service_version.into(),
            environment: environment.into(),
            export_interval_ms: 1_000,
            export_timeout_ms: 5_000,
        }
    }
}

#[derive(Clone)]
pub struct OTelMetricsCollector {
    state: Arc<Mutex<MetricsState>>,
    sdk: Option<Arc<MetricsSdk>>,
}

#[derive(Debug, Clone, Default)]
struct MetricsState {
    counters: HashMap<String, (u64, HashMap<String, String>)>,
    gauges: HashMap<String, f64>,
    histograms: HashMap<String, Vec<u64>>,
    service_name: String,
    service_version: String,
    environment: String,
}

#[derive(Clone)]
struct MetricsSdk {
    provider: SdkMeterProvider,
    instruments: MetricsInstruments,
}

#[derive(Clone)]
struct MetricsInstruments {
    events_processed: Counter<u64>,
    events_filtered: Counter<u64>,
    errors: Counter<u64>,
    checkpoint_committed: Counter<u64>,
    replication_lag_ms: Gauge<u64>,
    replication_lag_events: Gauge<u64>,
    checkpoint_offset: Gauge<u64>,
    buffer_size: Gauge<u64>,
    snapshot_progress: Gauge<u64>,
    event_processing_duration: Histogram<u64>,
    checkpoint_commit_duration: Histogram<u64>,
}

impl OTelMetricsCollector {
    pub fn new(service_name: &str, service_version: &str, environment: &str) -> Self {
        let state = MetricsState {
            service_name: service_name.to_string(),
            service_version: service_version.to_string(),
            environment: environment.to_string(),
            ..Default::default()
        };

        Self {
            state: Arc::new(Mutex::new(state)),
            sdk: None,
        }
    }

    pub fn with_otlp_exporter(config: OTelConfig) -> Result<Self> {
        let meter_provider = opentelemetry_otlp::new_pipeline()
            .metrics(runtime::Tokio)
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(config.endpoint.clone()),
            )
            .with_resource(Resource::new(vec![
                KeyValue::new("service.name", config.service_name.clone()),
                KeyValue::new("service.version", config.service_version.clone()),
                KeyValue::new("deployment.environment", config.environment.clone()),
            ]))
            .with_period(Duration::from_millis(config.export_interval_ms))
            .with_timeout(Duration::from_millis(config.export_timeout_ms))
            .build()
            .map_err(|error| {
                Error::ConfigError(format!("failed to build OTLP metrics pipeline: {error}"))
            })?;

        let meter = meter_provider.meter("cdc-rs");
        let instruments = MetricsInstruments {
            events_processed: meter
                .u64_counter("cdc.events.processed")
                .with_description("Processed CDC events")
                .init(),
            events_filtered: meter
                .u64_counter("cdc.events.filtered")
                .with_description("Filtered CDC events")
                .init(),
            errors: meter
                .u64_counter("cdc.errors")
                .with_description("CDC processing errors")
                .init(),
            checkpoint_committed: meter
                .u64_counter("cdc.checkpoint.committed_count")
                .with_description("Committed checkpoint event count")
                .init(),
            replication_lag_ms: meter
                .u64_gauge("cdc.replication_lag_ms")
                .with_description("Replication lag in milliseconds")
                .init(),
            replication_lag_events: meter
                .u64_gauge("cdc.replication_lag_events")
                .with_description("Replication lag in events")
                .init(),
            checkpoint_offset: meter
                .u64_gauge("cdc.checkpoint_offset")
                .with_description("Checkpoint offset surrogate value")
                .init(),
            buffer_size: meter
                .u64_gauge("cdc.buffer_size")
                .with_description("In-flight event buffer size")
                .init(),
            snapshot_progress: meter
                .u64_gauge("cdc.snapshot_progress_percent")
                .with_description("Snapshot progress percentage")
                .init(),
            event_processing_duration: meter
                .u64_histogram("cdc.event_processing_duration_ms")
                .with_description("End-to-end event processing duration")
                .init(),
            checkpoint_commit_duration: meter
                .u64_histogram("cdc.checkpoint_commit_duration_ms")
                .with_description("Checkpoint commit duration")
                .init(),
        };

        let collector = Self::new(
            &config.service_name,
            &config.service_version,
            &config.environment,
        );

        Ok(Self {
            sdk: Some(Arc::new(MetricsSdk {
                provider: meter_provider,
                instruments,
            })),
            ..collector
        })
    }

    pub fn shutdown(&self) -> Result<()> {
        if let Some(sdk) = &self.sdk {
            sdk.provider.force_flush().map_err(|error| {
                Error::StateError(format!("metrics force flush failed: {error}"))
            })?;
            sdk.provider
                .shutdown()
                .map_err(|error| Error::StateError(format!("metrics shutdown failed: {error}")))?;
        }
        Ok(())
    }

    pub fn record_events_processed(&self, op: Operation, count: u64) {
        if let Ok(mut state) = self.state.lock() {
            let op_name = op.to_string();
            let metric_key = format!("cdc.events.processed[op={op_name}]");
            let entry = state
                .counters
                .entry(metric_key)
                .or_insert((0, HashMap::new()));
            entry.0 = entry.0.saturating_add(count);
            entry.1.insert("operation".to_string(), op_name.clone());

            if let Some(sdk) = &self.sdk {
                sdk.instruments
                    .events_processed
                    .add(count, &[KeyValue::new("operation", op_name)]);
            }
        }
    }

    pub fn record_events_filtered(&self, count: u64) {
        if let Ok(mut state) = self.state.lock() {
            let entry = state
                .counters
                .entry("cdc.events.filtered".to_string())
                .or_insert((0, HashMap::new()));
            entry.0 = entry.0.saturating_add(count);

            if let Some(sdk) = &self.sdk {
                sdk.instruments.events_filtered.add(count, &[]);
            }
        }
    }

    pub fn record_replication_lag_gauge_ms(&self, lag_ms: u64) {
        if let Ok(mut state) = self.state.lock() {
            state
                .gauges
                .insert("cdc.replication_lag_ms".to_string(), lag_ms as f64);

            if let Some(sdk) = &self.sdk {
                sdk.instruments.replication_lag_ms.record(lag_ms, &[]);
            }
        }
    }

    pub fn record_checkpoint_offset(&self, offset: &str) {
        if let Ok(mut state) = self.state.lock() {
            let surrogate = offset.len() as u64;
            state
                .gauges
                .insert("cdc.checkpoint_offset".to_string(), surrogate as f64);

            if let Some(sdk) = &self.sdk {
                sdk.instruments.checkpoint_offset.record(surrogate, &[]);
            }
        }
    }

    pub fn record_event_processing_duration(&self, duration_ms: u64) {
        if let Ok(mut state) = self.state.lock() {
            state
                .histograms
                .entry("cdc.event_processing_duration_ms".to_string())
                .or_insert_with(Vec::new)
                .push(duration_ms);

            if let Some(sdk) = &self.sdk {
                sdk.instruments
                    .event_processing_duration
                    .record(duration_ms, &[]);
            }
        }
    }

    pub fn record_checkpoint_commit_duration(&self, duration_ms: u64) {
        if let Ok(mut state) = self.state.lock() {
            state
                .histograms
                .entry("cdc.checkpoint_commit_duration_ms".to_string())
                .or_insert_with(Vec::new)
                .push(duration_ms);

            if let Some(sdk) = &self.sdk {
                sdk.instruments
                    .checkpoint_commit_duration
                    .record(duration_ms, &[]);
            }
        }
    }

    pub fn record_buffer_size(&self, size: u64) {
        if let Ok(mut state) = self.state.lock() {
            state
                .gauges
                .insert("cdc.buffer_size".to_string(), size as f64);

            if let Some(sdk) = &self.sdk {
                sdk.instruments.buffer_size.record(size, &[]);
            }
        }
    }

    pub fn record_snapshot_progress(&self, percent: u64) {
        if let Ok(mut state) = self.state.lock() {
            state
                .gauges
                .insert("cdc.snapshot_progress_percent".to_string(), percent as f64);

            if let Some(sdk) = &self.sdk {
                sdk.instruments.snapshot_progress.record(percent, &[]);
            }
        }
    }

    pub fn export_metrics(&self) -> std::result::Result<MetricsReport, String> {
        let state = self.state.lock().map_err(|error| error.to_string())?;
        Ok(MetricsReport {
            service_name: state.service_name.clone(),
            service_version: state.service_version.clone(),
            environment: state.environment.clone(),
            counters: state.counters.clone(),
            gauges: state.gauges.clone(),
            histograms: state.histograms.clone(),
        })
    }

    pub fn reset(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.counters.clear();
            state.gauges.clear();
            state.histograms.clear();
        }
    }
}

impl MetricsCollector for OTelMetricsCollector {
    fn record_event_processed(&self, op: Operation, latency_ms: u64) {
        self.record_events_processed(op, 1);
        self.record_event_processing_duration(latency_ms);
    }

    fn record_checkpoint_committed(&self, event_count: u64, latency_ms: u64) {
        if let Ok(mut state) = self.state.lock() {
            let entry = state
                .counters
                .entry("cdc.checkpoint.committed_count".to_string())
                .or_insert((0, HashMap::new()));
            entry.0 = entry.0.saturating_add(event_count);

            if let Some(sdk) = &self.sdk {
                sdk.instruments.checkpoint_committed.add(event_count, &[]);
            }
        }
        self.record_checkpoint_commit_duration(latency_ms);
    }

    fn record_replication_lag_ms(&self, lag_ms: u64, lag_events: u64) {
        self.record_replication_lag_gauge_ms(lag_ms);
        if let Ok(mut state) = self.state.lock() {
            state
                .gauges
                .insert("cdc.replication_lag_events".to_string(), lag_events as f64);

            if let Some(sdk) = &self.sdk {
                sdk.instruments
                    .replication_lag_events
                    .record(lag_events, &[]);
            }
        }
    }

    fn record_error(&self, error: &Error, context: &str) {
        let error_class = error_metric_class(error);
        if let Ok(mut state) = self.state.lock() {
            let metric_key = format!("cdc.errors[context={context}]");
            let entry = state
                .counters
                .entry(metric_key)
                .or_insert((0, HashMap::new()));
            entry.0 = entry.0.saturating_add(1);
            entry
                .1
                .insert("error_class".to_string(), error_class.to_string());
            entry.1.insert("context".to_string(), context.to_string());

            if let Some(sdk) = &self.sdk {
                sdk.instruments.errors.add(
                    1,
                    &[
                        KeyValue::new("context", context.to_string()),
                        KeyValue::new("error.class", error_class),
                    ],
                );
            }
        }
    }
}

fn error_metric_class(error: &Error) -> &'static str {
    match error {
        Error::SourceError(_) => "source",
        Error::CheckpointError(_) => "checkpoint",
        Error::SchemaError(_) => "schema",
        Error::ValidationError(_) => "validation",
        Error::ConfigError(_) => "config",
        Error::IoError(_) => "io",
        Error::SerializationError(_) => "serialization",
        Error::TimeoutError(_) => "timeout",
        Error::Unrecoverable(_) => "unrecoverable",
        Error::StateError(_) => "state",
        Error::TransformError(_) => "transform",
        Error::NotImplemented(_) => "not_implemented",
    }
}

#[derive(Debug, Clone)]
pub struct MetricsReport {
    pub service_name: String,
    pub service_version: String,
    pub environment: String,
    pub counters: HashMap<String, (u64, HashMap<String, String>)>,
    pub gauges: HashMap<String, f64>,
    pub histograms: HashMap<String, Vec<u64>>,
}

impl MetricsReport {
    pub fn get_counter(&self, name: &str) -> Option<u64> {
        self.counters.get(name).map(|(value, _)| *value)
    }

    pub fn get_gauge(&self, name: &str) -> Option<f64> {
        self.gauges.get(name).copied()
    }

    pub fn get_histogram_percentile(&self, name: &str, percentile: f64) -> Option<u64> {
        self.histograms.get(name).and_then(|values| {
            let mut sorted = values.clone();
            sorted.sort_unstable();
            let index = ((sorted.len() as f64) * (percentile / 100.0)) as usize;
            sorted
                .get(index.min(sorted.len().saturating_sub(1)))
                .copied()
        })
    }

    pub fn total_events_processed(&self) -> u64 {
        self.counters
            .iter()
            .filter(|(name, _)| name.starts_with("cdc.events.processed"))
            .map(|(_, (count, _))| count)
            .sum()
    }

    pub fn avg_event_processing_latency(&self) -> Option<f64> {
        self.histograms
            .get("cdc.event_processing_duration_ms")
            .and_then(|values| {
                if values.is_empty() {
                    None
                } else {
                    let total: u64 = values.iter().sum();
                    Some(total as f64 / values.len() as f64)
                }
            })
    }
}

#[derive(Clone)]
pub struct OTelEventTracer {
    state: Arc<Mutex<TracingState>>,
    tracer: Arc<opentelemetry::global::BoxedTracer>,
    source_type: String,
}

#[derive(Default)]
struct TracingState {
    active_spans: HashMap<String, ActiveSpan>,
    completed_spans: Vec<SpanRecord>,
    event_correlation: HashMap<String, CorrelationContext>,
}

struct ActiveSpan {
    name: String,
    start_time_ms: u64,
    attributes: HashMap<String, String>,
    parent_span_id: Option<String>,
    span: opentelemetry::global::BoxedSpan,
}

#[derive(Debug, Clone)]
struct CorrelationContext {
    trace_id: String,
    span_id: String,
}

#[derive(Debug, Clone)]
pub struct SpanRecord {
    pub span_id: String,
    pub name: String,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    pub attributes: HashMap<String, String>,
}

impl OTelEventTracer {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(TracingState::default())),
            tracer: Arc::new(global::tracer("cdc-rs")),
            source_type: "unknown".to_string(),
        }
    }

    pub fn with_otlp_exporter(config: OTelConfig) -> Result<Self> {
        opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(config.endpoint),
            )
            .with_trace_config(sdktrace::config().with_resource(Resource::new(vec![
                KeyValue::new("service.name", config.service_name),
                KeyValue::new("service.version", config.service_version),
                KeyValue::new("deployment.environment", config.environment),
            ])))
            .install_batch(runtime::Tokio)
            .map_err(|error| {
                Error::ConfigError(format!("failed to build OTLP tracer pipeline: {error}"))
            })?;

        Ok(Self {
            state: Arc::new(Mutex::new(TracingState::default())),
            tracer: Arc::new(global::tracer("cdc-rs")),
            source_type: "unknown".to_string(),
        })
    }

    pub fn with_source_type(mut self, source_type: impl Into<String>) -> Self {
        self.source_type = source_type.into();
        self
    }

    pub fn shutdown(&self) {
        global::shutdown_tracer_provider();
    }

    pub fn start_span(&self, span_id: &str, span_name: &str, attributes: HashMap<String, String>) {
        self.start_span_with_parent(span_id, span_name, attributes, None);
    }

    pub fn start_span_with_parent(
        &self,
        span_id: &str,
        span_name: &str,
        mut attributes: HashMap<String, String>,
        parent_span_id: Option<&str>,
    ) {
        attributes
            .entry("source.type".to_string())
            .or_insert_with(|| self.source_type.clone());

        let parent_context = parent_span_id
            .and_then(|id| self.parent_context(id))
            .unwrap_or_default();

        let mut span = self
            .tracer
            .start_with_context(span_name.to_string(), &parent_context);
        for (key, value) in &attributes {
            span.set_attribute(KeyValue::new(key.clone(), value.clone()));
        }

        let span_context = span.span_context().clone();
        let correlation = CorrelationContext {
            trace_id: span_context.trace_id().to_string(),
            span_id: span_context.span_id().to_string(),
        };

        if let Ok(mut state) = self.state.lock() {
            state.active_spans.insert(
                span_id.to_string(),
                ActiveSpan {
                    name: span_name.to_string(),
                    start_time_ms: now_millis(),
                    attributes,
                    parent_span_id: parent_span_id.map(ToOwned::to_owned),
                    span,
                },
            );
            state
                .event_correlation
                .insert(span_id.to_string(), correlation);
        }
    }

    pub fn end_span_with_status(&self, span_id: &str, status: &str, error_type: Option<&str>) {
        if let Ok(mut state) = self.state.lock() {
            if let Some(mut active) = state.active_spans.remove(span_id) {
                if status != "ok" {
                    active.span.set_status(Status::error(status.to_string()));
                    let kind = error_type.unwrap_or(status).to_string();
                    active
                        .span
                        .set_attribute(KeyValue::new("error.type", kind.clone()));
                    active.attributes.insert("error.type".to_string(), kind);
                }

                if let Some(parent_span_id) = &active.parent_span_id {
                    active
                        .attributes
                        .insert("parent.span_id".to_string(), parent_span_id.clone());
                }

                active.span.end();
                state.completed_spans.push(SpanRecord {
                    span_id: span_id.to_string(),
                    name: active.name,
                    start_time_ms: active.start_time_ms,
                    end_time_ms: now_millis(),
                    attributes: active.attributes,
                });
            }
        }
    }

    pub fn end_span(&self, span_id: &str) {
        self.end_span_with_status(span_id, "ok", None);
    }

    pub fn start_snapshot_span(&self, span_id: &str, table: &str, row_count: u64) {
        let mut attrs = HashMap::new();
        attrs.insert("source.table".to_string(), table.to_string());
        attrs.insert("snapshot.row_count".to_string(), row_count.to_string());
        self.start_span(span_id, "cdc.snapshot", attrs);
    }

    pub fn start_snapshot_chunk_span(
        &self,
        span_id: &str,
        snapshot_span_id: &str,
        table: &str,
        chunk_index: u64,
        chunk_size: u64,
    ) {
        let mut attrs = HashMap::new();
        attrs.insert("source.table".to_string(), table.to_string());
        attrs.insert("snapshot.chunk_index".to_string(), chunk_index.to_string());
        attrs.insert("snapshot.chunk_size".to_string(), chunk_size.to_string());
        self.start_span_with_parent(span_id, "cdc.snapshot.chunk", attrs, Some(snapshot_span_id));
    }

    pub fn start_stream_span(&self, span_id: &str, table: Option<&str>, events_count: u64) {
        let mut attrs = HashMap::new();
        attrs.insert("stream.events_count".to_string(), events_count.to_string());
        attrs.insert(
            "source.table".to_string(),
            table.unwrap_or("n/a").to_string(),
        );
        self.start_span(span_id, "cdc.stream", attrs);
    }

    pub fn start_transform_span(
        &self,
        span_id: &str,
        transform_name: &str,
        table: Option<&str>,
        parent_span_id: Option<&str>,
    ) {
        let mut attrs = HashMap::new();
        attrs.insert("transform.name".to_string(), transform_name.to_string());
        attrs.insert(
            "source.table".to_string(),
            table.unwrap_or("n/a").to_string(),
        );
        self.start_span_with_parent(span_id, "cdc.event.transform", attrs, parent_span_id);
    }

    pub fn start_checkpoint_commit_span(&self, span_id: &str, events_count: u64) {
        let mut attrs = HashMap::new();
        attrs.insert(
            "checkpoint.events_count".to_string(),
            events_count.to_string(),
        );
        attrs.insert("source.table".to_string(), "n/a".to_string());
        self.start_span(span_id, "cdc.checkpoint.commit", attrs);
    }

    pub fn start_handoff_span(
        &self,
        span_id: &str,
        overlap_events_dropped: u64,
        stream_watermark_gap: Option<u64>,
    ) {
        let mut attrs = HashMap::new();
        attrs.insert(
            "handoff.overlap_events_dropped".to_string(),
            overlap_events_dropped.to_string(),
        );
        if let Some(gap) = stream_watermark_gap {
            attrs.insert("handoff.stream_watermark_gap".to_string(), gap.to_string());
        }
        attrs.insert("source.table".to_string(), "n/a".to_string());
        self.start_span(span_id, "cdc.handoff", attrs);
    }

    pub fn export_spans(&self) -> std::result::Result<Vec<SpanRecord>, String> {
        self.state
            .lock()
            .map_err(|error| error.to_string())
            .map(|state| state.completed_spans.clone())
    }

    pub fn reset(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.active_spans.clear();
            state.completed_spans.clear();
            state.event_correlation.clear();
        }
    }

    pub fn propagate_baggage_to_event(&self, event_id: &str, event: &mut Event) -> bool {
        let correlation = if let Ok(state) = self.state.lock() {
            state.event_correlation.get(event_id).cloned()
        } else {
            None
        };

        let Some(correlation) = correlation else {
            return false;
        };

        if let Some(after) = event.after.as_mut().and_then(|value| value.as_object_mut()) {
            after.insert(
                "_otel_trace_id".to_string(),
                serde_json::Value::String(correlation.trace_id),
            );
            after.insert(
                "_otel_span_id".to_string(),
                serde_json::Value::String(correlation.span_id),
            );
            return true;
        }

        if let Some(before) = event
            .before
            .as_mut()
            .and_then(|value| value.as_object_mut())
        {
            before.insert(
                "_otel_trace_id".to_string(),
                serde_json::Value::String(correlation.trace_id),
            );
            before.insert(
                "_otel_span_id".to_string(),
                serde_json::Value::String(correlation.span_id),
            );
            return true;
        }

        false
    }

    fn parent_context(&self, parent_span_id: &str) -> Option<Context> {
        if let Ok(state) = self.state.lock() {
            if let Some(parent) = state.active_spans.get(parent_span_id) {
                let parent_span_context = parent.span.span_context().clone();
                return Some(Context::new().with_remote_span_context(parent_span_context));
            }
        }
        None
    }
}

impl Default for OTelEventTracer {
    fn default() -> Self {
        Self::new()
    }
}

impl EventTracer for OTelEventTracer {
    fn trace_event_start(&self, event_id: &str) {
        let mut attributes = HashMap::new();
        attributes.insert("event.id".to_string(), event_id.to_string());
        attributes.insert("source.table".to_string(), "n/a".to_string());
        self.start_span(event_id, "cdc.event.transform", attributes);
    }

    fn trace_event_end(&self, event_id: &str, status: &str) {
        self.end_span_with_status(event_id, status, Some(status));
    }

    fn trace_checkpoint_barrier(&self, state: &str) {
        let span_id = format!("barrier-{state}");
        let mut attributes = HashMap::new();
        attributes.insert("checkpoint.state".to_string(), state.to_string());
        attributes.insert("source.table".to_string(), "n/a".to_string());
        self.start_span(&span_id, "cdc.checkpoint.commit", attributes);
        self.end_span(&span_id);
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otel_metrics_collector_creation() {
        let collector = OTelMetricsCollector::new("cdc-service", "1.0.0", "test");
        let report = collector.export_metrics().unwrap();
        assert_eq!(report.service_name, "cdc-service");
        assert_eq!(report.service_version, "1.0.0");
        assert_eq!(report.environment, "test");
    }

    #[test]
    fn test_otel_metrics_events_processed() {
        let collector = OTelMetricsCollector::new("cdc-service", "1.0.0", "test");
        collector.record_events_processed(Operation::Insert, 10);
        collector.record_events_processed(Operation::Update, 5);
        collector.record_events_filtered(2);
        let report = collector.export_metrics().unwrap();
        assert_eq!(report.total_events_processed(), 15);
        assert_eq!(report.get_counter("cdc.events.filtered"), Some(2));
    }

    #[test]
    fn test_otel_metrics_processing_duration() {
        let collector = OTelMetricsCollector::new("cdc-service", "1.0.0", "test");
        collector.record_event_processing_duration(100);
        collector.record_event_processing_duration(200);
        collector.record_event_processing_duration(150);
        let report = collector.export_metrics().unwrap();
        let avg = report.avg_event_processing_latency();
        assert!(avg.is_some());
        assert!((avg.unwrap() - 150.0).abs() < 1.0);
    }

    #[test]
    fn test_otel_metrics_gauges() {
        let collector = OTelMetricsCollector::new("cdc-service", "1.0.0", "test");
        collector.record_replication_lag_gauge_ms(1_000);
        collector.record_buffer_size(500);
        collector.record_snapshot_progress(75);
        let report = collector.export_metrics().unwrap();
        assert_eq!(report.get_gauge("cdc.replication_lag_ms"), Some(1_000.0));
        assert_eq!(report.get_gauge("cdc.buffer_size"), Some(500.0));
        assert_eq!(
            report.get_gauge("cdc.snapshot_progress_percent"),
            Some(75.0)
        );
    }

    #[test]
    fn test_otel_metrics_reset() {
        let collector = OTelMetricsCollector::new("cdc-service", "1.0.0", "test");
        collector.record_events_processed(Operation::Delete, 42);
        collector.reset();
        let report = collector.export_metrics().unwrap();
        assert_eq!(report.total_events_processed(), 0);
    }

    #[test]
    fn test_otel_event_tracer_spans() {
        let tracer = OTelEventTracer::new().with_source_type("postgres");
        let mut attrs = HashMap::new();
        attrs.insert("source.table".to_string(), "users".to_string());
        tracer.start_span("event-1", "cdc.snapshot.chunk", attrs);
        tracer.end_span("event-1");
        let spans = tracer.export_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "cdc.snapshot.chunk");
        assert_eq!(
            spans[0].attributes.get("source.type"),
            Some(&"postgres".to_string())
        );
    }

    #[test]
    fn test_otel_event_tracer_checkpoint_barrier() {
        let tracer = OTelEventTracer::new();
        tracer.trace_checkpoint_barrier("commit_started");
        let spans = tracer.export_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "cdc.checkpoint.commit");
    }

    #[test]
    fn test_span_hierarchy_snapshot_to_chunk() {
        let tracer = OTelEventTracer::new().with_source_type("sqlserver");
        tracer.start_snapshot_span("snapshot-root", "dbo.users", 1000);
        tracer.start_snapshot_chunk_span("chunk-1", "snapshot-root", "dbo.users", 0, 500);
        tracer.end_span("chunk-1");
        tracer.end_span("snapshot-root");

        let spans = tracer.export_spans().unwrap();
        assert_eq!(spans.len(), 2);
        let chunk = spans.iter().find(|span| span.span_id == "chunk-1").unwrap();
        assert_eq!(chunk.name, "cdc.snapshot.chunk");
        assert_eq!(
            chunk.attributes.get("parent.span_id"),
            Some(&"snapshot-root".to_string())
        );
    }

    #[test]
    fn test_baggage_propagation_to_event_payload() {
        let tracer = OTelEventTracer::new();
        tracer.trace_event_start("event-123");

        let mut event = Event {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            op: Operation::Insert,
            source: crate::core::SourceMetadata {
                source_name: "postgres".to_string(),
                offset: "0/16B6A70".to_string(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".to_string()),
            table: "users".to_string(),
            primary_key: Some(vec!["id".to_string()]),
            snapshot: None,
            transaction: None,
            envelope_version: crate::EVENT_ENVELOPE_VERSION,
        };

        let propagated = tracer.propagate_baggage_to_event("event-123", &mut event);
        assert!(propagated);

        let payload = event.after.as_ref().unwrap();
        assert!(payload.get("_otel_trace_id").is_some());
        assert!(payload.get("_otel_span_id").is_some());

        tracer.trace_event_end("event-123", "ok");
    }

    #[test]
    fn test_metrics_trait_paths_and_percentiles() {
        let collector = OTelMetricsCollector::new("cdc-service", "1.0.0", "test");
        collector.record_event_processed(Operation::Insert, 11);
        collector.record_event_processed(Operation::Delete, 29);
        collector.record_checkpoint_committed(7, 5);
        MetricsCollector::record_replication_lag_ms(&collector, 128, 3);
        collector.record_error(&Error::StateError("boom".to_string()), "runtime.poll");

        let report = collector.export_metrics().unwrap();
        assert_eq!(
            report.get_counter("cdc.checkpoint.committed_count"),
            Some(7)
        );
        assert_eq!(report.get_gauge("cdc.replication_lag_events"), Some(3.0));
        assert_eq!(
            report.get_histogram_percentile("cdc.event_processing_duration_ms", 50.0),
            Some(29)
        );
        assert!(report
            .counters
            .contains_key("cdc.errors[context=runtime.poll]"));
    }

    #[test]
    fn test_metrics_report_helpers_handle_empty_histograms() {
        let report = MetricsReport {
            service_name: "svc".to_string(),
            service_version: "1".to_string(),
            environment: "test".to_string(),
            counters: HashMap::new(),
            gauges: HashMap::new(),
            histograms: HashMap::from([(
                "cdc.event_processing_duration_ms".to_string(),
                Vec::new(),
            )]),
        };

        assert_eq!(
            report.get_histogram_percentile("cdc.event_processing_duration_ms", 95.0),
            None
        );
        assert_eq!(report.avg_event_processing_latency(), None);
    }

    #[test]
    fn test_transform_and_handoff_span_helpers() {
        let tracer = OTelEventTracer::new().with_source_type("mysql");
        tracer.start_stream_span("stream-1", None, 3);
        tracer.start_transform_span("transform-1", "mask_hash", None, Some("stream-1"));
        tracer.end_span_with_status("transform-1", "transform_crash", Some("panic"));
        tracer.end_span("stream-1");

        tracer.start_checkpoint_commit_span("checkpoint-1", 10);
        tracer.end_span("checkpoint-1");
        tracer.start_handoff_span("handoff-1", 2, Some(8));
        tracer.end_span("handoff-1");

        let spans = tracer.export_spans().unwrap();
        assert!(spans.iter().any(|span| span.name == "cdc.event.transform"));
        assert!(spans
            .iter()
            .any(|span| span.name == "cdc.checkpoint.commit"));
        assert!(spans.iter().any(|span| span.name == "cdc.handoff"));

        let transform = spans
            .iter()
            .find(|span| span.span_id == "transform-1")
            .expect("transform span present");
        assert_eq!(
            transform.attributes.get("parent.span_id"),
            Some(&"stream-1".to_string())
        );
        assert_eq!(
            transform.attributes.get("error.type"),
            Some(&"panic".to_string())
        );
        assert_eq!(
            transform.attributes.get("source.type"),
            Some(&"mysql".to_string())
        );

        let handoff = spans
            .iter()
            .find(|span| span.span_id == "handoff-1")
            .expect("handoff span present");
        assert_eq!(
            handoff.attributes.get("handoff.overlap_events_dropped"),
            Some(&"2".to_string())
        );
        assert_eq!(
            handoff.attributes.get("handoff.stream_watermark_gap"),
            Some(&"8".to_string())
        );
    }

    #[test]
    fn test_baggage_propagates_to_before_when_after_is_absent() {
        let tracer = OTelEventTracer::new();
        tracer.trace_event_start("event-before");

        let mut event = Event {
            before: Some(serde_json::json!({"id": 7})),
            after: None,
            op: Operation::Delete,
            source: crate::core::SourceMetadata {
                source_name: "postgres".to_string(),
                offset: "0/16B6A70".to_string(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".to_string()),
            table: "users".to_string(),
            primary_key: Some(vec!["id".to_string()]),
            snapshot: None,
            transaction: None,
            envelope_version: crate::EVENT_ENVELOPE_VERSION,
        };

        assert!(tracer.propagate_baggage_to_event("event-before", &mut event));
        let payload = event.before.as_ref().expect("before payload present");
        assert!(payload.get("_otel_trace_id").is_some());
        assert!(payload.get("_otel_span_id").is_some());
    }

    #[test]
    fn test_baggage_propagation_returns_false_for_unknown_event() {
        let tracer = OTelEventTracer::new();
        let mut event = Event {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            op: Operation::Insert,
            source: crate::core::SourceMetadata {
                source_name: "postgres".to_string(),
                offset: "0/16B6A70".to_string(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".to_string()),
            table: "users".to_string(),
            primary_key: Some(vec!["id".to_string()]),
            snapshot: None,
            transaction: None,
            envelope_version: crate::EVENT_ENVELOPE_VERSION,
        };

        assert!(!tracer.propagate_baggage_to_event("missing-event", &mut event));
    }
}
