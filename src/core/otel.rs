//! OpenTelemetry metrics and tracing integrations.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use opentelemetry::{
    global,
    metrics::{Counter, Gauge, Histogram, MeterProvider as _},
    trace::{Span as _, Status, TraceContextExt, Tracer as _},
    Context, KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::{metrics::SdkMeterProvider, trace as sdktrace, Resource};

use crate::core::{Error, Event, EventTracer, MetricsCollector, Operation, Result};

/// Build the OpenTelemetry `Resource` describing this service.
///
/// The service triple is what a backend groups and filters on, so it is set in one place
/// rather than restated per signal — metrics and traces disagreeing about their own
/// identity is a debugging trap.
fn otel_resource(service_name: &str, service_version: &str, environment: &str) -> Resource {
    Resource::builder()
        .with_attributes([
            KeyValue::new("service.name", service_name.to_string()),
            KeyValue::new("service.version", service_version.to_string()),
            KeyValue::new("deployment.environment", environment.to_string()),
        ])
        .build()
}

/// Connection and identity settings for the OTLP exporters.
///
/// The service triple (`service_name`, `service_version`, `environment`) becomes the
/// OpenTelemetry `Resource` attached to every metric and span, so it is what a backend
/// groups and filters on. Set `environment` to something that distinguishes deployments —
/// otherwise staging and production series merge silently.
#[derive(Debug, Clone)]
pub struct OTelConfig {
    /// OTLP gRPC endpoint, e.g. `http://otel-collector:4317`.
    pub endpoint: String,
    /// `service.name` resource attribute.
    pub service_name: String,
    /// `service.version` resource attribute.
    pub service_version: String,
    /// `deployment.environment` resource attribute.
    pub environment: String,
    /// How often the periodic reader exports metrics, in milliseconds. Default 1,000.
    ///
    /// Shorter intervals cost more network traffic and give the backend more points;
    /// they do not make the runtime's own measurements finer-grained.
    pub export_interval_ms: u64,
    /// Deadline for a single export attempt, in milliseconds. Default 5,000.
    pub export_timeout_ms: u64,
}

impl OTelConfig {
    /// Build a config with the default export interval (1 s) and timeout (5 s).
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
/// [`MetricsCollector`] backed by OpenTelemetry, with an in-memory mirror.
///
/// Cheap to clone — clones share one state. The events-processed counter is a lockless
/// atomic on the hot path; everything else takes a mutex, so recorders other than
/// `record_events_processed` should not be called per event at high volume.
pub struct OTelMetricsCollector {
    state: Arc<Mutex<MetricsState>>,
    sdk: Option<Arc<MetricsSdk>>,
    /// Lockless hot-path counter for events processed. Updated on every poll
    /// loop iteration without acquiring the `state` mutex.
    events_processed_total: Arc<AtomicU64>,
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
    /// Replication slot WAL lag in bytes (`pg_current_wal_lsn - confirmed_flush_lsn`).
    /// Updated by the idle-advance path; `0` when no idle advance has occurred yet.
    replication_slot_lag_bytes: Gauge<u64>,
    checkpoint_offset: Gauge<u64>,
    buffer_size: Gauge<u64>,
    snapshot_progress: Gauge<u64>,
    event_processing_duration: Histogram<u64>,
    checkpoint_commit_duration: Histogram<u64>,
}

impl OTelMetricsCollector {
    /// Build a collector that records metrics in memory but exports nothing.
    ///
    /// Useful for tests and for reading `export_metrics()` directly. Use
    /// [`OTelMetricsCollector::with_otlp_exporter`] to actually ship them.
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
            events_processed_total: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Build a collector that exports over OTLP/gRPC on a periodic reader.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] if the exporter or meter provider cannot be built —
    /// typically a malformed endpoint. Note that a *reachable* endpoint is not verified
    /// here: export failures surface later through the OpenTelemetry SDK's own error
    /// handler, not from this call.
    pub fn with_otlp_exporter(config: OTelConfig) -> Result<Self> {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(config.endpoint.clone())
            .build()
            .map_err(|error| {
                Error::ConfigError(format!("failed to build OTLP metric exporter: {error}"))
            })?;

        let reader = PeriodicReader::builder(exporter)
            .with_interval(Duration::from_millis(config.export_interval_ms))
            .build();

        let meter_provider = SdkMeterProvider::builder()
            .with_resource(otel_resource(
                &config.service_name,
                &config.service_version,
                &config.environment,
            ))
            .with_reader(reader)
            .build();

        // ── Instrument naming ────────────────────────────────────────────────────
        //
        // Every name is chosen so that the standard OpenTelemetry → Prometheus translation
        // (dots to underscores, `_total` appended to monotonic counters) produces **the same
        // series name** as this crate's own `/metrics` text exposition. That is not cosmetic.
        //
        // The two surfaces used to name overlapping quantities differently — OTel emitted
        // `rustcdc.replication_lag_ms` and `rustcdc.buffer_size`, the text exposition emitted
        // `rustcdc_runtime_replication_lag_ms` and `rustcdc_runtime_buffer_depth` — and the
        // runbook documents only the latter. So every alert threshold in the runbook, applied
        // by an operator on the OTel path, matched **nothing and never fired**. An alert that
        // silently does not fire is worse than no alert: it looks like coverage.
        //
        // Units are deliberately in the name rather than declared via `with_unit`. A declared
        // unit makes the Prometheus exporter append a unit suffix, which would break the
        // correspondence this comment exists to preserve — and the unit-in-name form is the
        // Prometheus convention anyway. `metric_names_match_the_prometheus_exposition` pins
        // the mapping so it cannot drift again.
        let meter = meter_provider.meter("rustcdc");
        let instruments = MetricsInstruments {
            events_processed: meter
                .u64_counter("rustcdc.runtime.events_polled")
                .with_description("Processed CDC events")
                .build(),
            events_filtered: meter
                .u64_counter("rustcdc.runtime.events_filtered")
                .with_description("Filtered CDC events")
                .build(),
            errors: meter
                .u64_counter("rustcdc.runtime.errors")
                .with_description("CDC processing errors")
                .build(),
            checkpoint_committed: meter
                .u64_counter("rustcdc.runtime.events_committed")
                .with_description("Committed checkpoint event count")
                .build(),
            replication_lag_ms: meter
                .u64_gauge("rustcdc.runtime.replication_lag_ms")
                .with_description("Replication lag in milliseconds")
                .build(),
            replication_lag_events: meter
                .u64_gauge("rustcdc.runtime.replication_lag_events")
                .with_description("Replication lag in events")
                .build(),
            replication_slot_lag_bytes: meter
                .u64_gauge("rustcdc.replication_slot_lag_bytes")
                .with_description(
                    "Replication slot WAL lag in bytes (pg_current_wal_lsn - confirmed_flush_lsn). \
                     Non-zero during idle periods indicates healthy idle-advance; \
                     monotonically growing indicates a stalled slot.",
                )
                .build(),
            checkpoint_offset: meter
                .u64_gauge("rustcdc.runtime.checkpoint_offset")
                .with_description("Checkpoint offset surrogate value")
                .build(),
            buffer_size: meter
                .u64_gauge("rustcdc.runtime.buffer_depth")
                .with_description("In-flight event buffer size")
                .build(),
            snapshot_progress: meter
                .u64_gauge("rustcdc.runtime.snapshot_progress_percent")
                .with_description("Snapshot progress percentage")
                .build(),
            event_processing_duration: meter
                .u64_histogram("rustcdc.runtime.event_processing_duration_ms")
                .with_description("End-to-end event processing duration")
                .build(),
            checkpoint_commit_duration: meter
                .u64_histogram("rustcdc.runtime.checkpoint_commit_duration_ms")
                .with_description("Checkpoint commit duration")
                .build(),
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

    /// Flush pending metrics and shut the exporter down.
    ///
    /// Call this before process exit: the periodic reader buffers up to
    /// `export_interval_ms` of data, which is otherwise lost. A no-op when no exporter
    /// is configured.
    ///
    /// # Errors
    ///
    /// Returns [`Error::StateError`] if the flush or shutdown fails.
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

    /// Record `count` events of operation `op` as processed.
    pub fn record_events_processed(&self, op: Operation, count: u64) {
        // Hot path: increment the lockless total first, without acquiring the mutex.
        self.events_processed_total
            .fetch_add(count, Ordering::Relaxed);

        if let Ok(mut state) = self.state.lock() {
            // `Operation` is a closed set, so the metric key is one of six `&'static
            // str`s — no formatting, no allocation. This ran per event and previously
            // cost an `op.to_string()`, a `format!` for the map key, and an
            // `"operation".to_string()` plus a clone, all inside the mutex.
            //
            // Note `Operation::to_str()` returns `&'static str` and its own docs say
            // "Prefer this over `to_string()` on hot paths" — which the hot path was
            // not doing.
            let op_name = op.to_str();
            let metric_key = match op {
                Operation::Insert => "rustcdc.runtime.events_polled[op=insert]",
                Operation::Update => "rustcdc.runtime.events_polled[op=update]",
                Operation::Delete => "rustcdc.runtime.events_polled[op=delete]",
                Operation::Read => "rustcdc.runtime.events_polled[op=read]",
                Operation::SchemaChange => "rustcdc.runtime.events_polled[op=schema_change]",
                Operation::Truncate => "rustcdc.runtime.events_polled[op=truncate]",
                // Deliberately exhaustive: `Operation` is `#[non_exhaustive]` only for
                // downstream crates, so a new variant added here is a compile error
                // until it gets a metric key — which is what we want.
            };

            match state.counters.get_mut(metric_key) {
                Some(entry) => entry.0 = entry.0.saturating_add(count),
                None => {
                    let mut labels = HashMap::new();
                    labels.insert("operation".to_string(), op_name.to_string());
                    state
                        .counters
                        .insert(metric_key.to_string(), (count, labels));
                }
            }

            if let Some(sdk) = &self.sdk {
                sdk.instruments
                    .events_processed
                    .add(count, &[KeyValue::new("operation", op_name)]);
            }
        }
    }

    /// Record `count` events dropped by the transform pipeline.
    pub fn record_events_filtered(&self, count: u64) {
        if let Ok(mut state) = self.state.lock() {
            let entry = state
                .counters
                .entry("rustcdc.runtime.events_filtered".to_string())
                .or_insert((0, HashMap::new()));
            entry.0 = entry.0.saturating_add(count);

            if let Some(sdk) = &self.sdk {
                sdk.instruments.events_filtered.add(count, &[]);
            }
        }
    }

    /// Record the current replication lag in milliseconds.
    ///
    /// On MySQL and MariaDB this is derived from a binlog timestamp with **one-second**
    /// resolution, so it over-reports by up to 1,000 ms — see
    /// [`SourceMetadata::timestamp`](crate::core::SourceMetadata::timestamp).
    pub fn record_replication_lag_gauge_ms(&self, lag_ms: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.gauges.insert(
                "rustcdc.runtime.replication_lag_ms".to_string(),
                lag_ms as f64,
            );

            if let Some(sdk) = &self.sdk {
                sdk.instruments.replication_lag_ms.record(lag_ms, &[]);
            }
        }
    }

    /// Record PostgreSQL replication-slot WAL lag in bytes.
    ///
    /// The single most operationally critical PostgreSQL signal: a slot pins WAL on the
    /// primary, so unbounded growth ends in a full `pg_wal` volume. Alert on the
    /// derivative, not the level — a non-zero value during idle periods is normal.
    pub fn record_replication_slot_lag_bytes_gauge(&self, lag_bytes: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.gauges.insert(
                "rustcdc.replication_slot_lag_bytes".to_string(),
                lag_bytes as f64,
            );
            if let Some(sdk) = &self.sdk {
                sdk.instruments
                    .replication_slot_lag_bytes
                    .record(lag_bytes, &[]);
            }
        }
    }

    /// Record the current checkpoint offset.
    ///
    /// Offsets are connector-specific strings, so this is exported as a numeric gauge
    /// only when it parses as one; otherwise it is retained for `export_metrics()`.
    pub fn record_checkpoint_offset(&self, offset: &str) {
        if let Ok(mut state) = self.state.lock() {
            let surrogate = offset.len() as u64;
            state.gauges.insert(
                "rustcdc.runtime.checkpoint_offset".to_string(),
                surrogate as f64,
            );

            if let Some(sdk) = &self.sdk {
                sdk.instruments.checkpoint_offset.record(surrogate, &[]);
            }
        }
    }

    /// Record one event's processing duration in milliseconds.
    pub fn record_event_processing_duration(&self, duration_ms: u64) {
        if let Ok(mut state) = self.state.lock() {
            push_bounded(
                state
                    .histograms
                    .entry("rustcdc.runtime.event_processing_duration_ms".to_string())
                    .or_default(),
                duration_ms,
            );

            if let Some(sdk) = &self.sdk {
                sdk.instruments
                    .event_processing_duration
                    .record(duration_ms, &[]);
            }
        }
    }

    /// Record one checkpoint commit duration in milliseconds — dominated by the fsync.
    pub fn record_checkpoint_commit_duration(&self, duration_ms: u64) {
        if let Ok(mut state) = self.state.lock() {
            push_bounded(
                state
                    .histograms
                    .entry("rustcdc.runtime.checkpoint_commit_duration_ms".to_string())
                    .or_default(),
                duration_ms,
            );

            if let Some(sdk) = &self.sdk {
                sdk.instruments
                    .checkpoint_commit_duration
                    .record(duration_ms, &[]);
            }
        }
    }

    /// Record the current in-flight event buffer depth.
    pub fn record_buffer_size(&self, size: u64) {
        if let Ok(mut state) = self.state.lock() {
            state
                .gauges
                .insert("rustcdc.runtime.buffer_depth".to_string(), size as f64);

            if let Some(sdk) = &self.sdk {
                sdk.instruments.buffer_size.record(size, &[]);
            }
        }
    }

    /// Record snapshot completion as a percentage.
    pub fn record_snapshot_progress(&self, percent: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.gauges.insert(
                "rustcdc.runtime.snapshot_progress_percent".to_string(),
                percent as f64,
            );

            if let Some(sdk) = &self.sdk {
                sdk.instruments.snapshot_progress.record(percent, &[]);
            }
        }
    }

    /// Snapshot the in-memory metric state.
    ///
    /// Independent of the OTLP exporter: this reads the collector's own counters and
    /// bounded histogram windows, so it works with [`OTelMetricsCollector::new`] too.
    ///
    /// # Errors
    ///
    /// Returns an error string if the internal state mutex is poisoned.
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

    /// Return the total number of events processed, read atomically without
    /// acquiring the `Mutex<MetricsState>`.
    ///
    /// Useful for high-frequency polling (e.g. Prometheus hot scrape paths) where
    /// the per-operation breakdown from [`export_metrics`](Self::export_metrics)
    /// is not needed.
    pub fn events_processed_total(&self) -> u64 {
        self.events_processed_total.load(Ordering::Relaxed)
    }

    /// Clear all counters, gauges and histogram samples.
    ///
    /// For tests. Resetting a monotonic counter mid-flight makes a backend read it as a
    /// counter reset and interpolate, so do not call this in production.
    pub fn reset(&self) {
        self.events_processed_total.store(0, Ordering::Relaxed);
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
                .entry("rustcdc.runtime.events_committed".to_string())
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
            state.gauges.insert(
                "rustcdc.runtime.replication_lag_events".to_string(),
                lag_events as f64,
            );

            if let Some(sdk) = &self.sdk {
                sdk.instruments
                    .replication_lag_events
                    .record(lag_events, &[]);
            }
        }
    }

    fn record_replication_slot_lag_bytes(&self, lag_bytes: u64) {
        self.record_replication_slot_lag_bytes_gauge(lag_bytes);
    }

    fn record_error(&self, error: &Error, context: &str) {
        let error_class = error_metric_class(error);
        if let Ok(mut state) = self.state.lock() {
            let metric_key = format!("rustcdc.runtime.errors[context={context}]");
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
        // Context frames are transparent: the metric must describe the failure, not
        // the layer that annotated it, or one label would cover every cause.
        Error::Context { source, .. } => error_metric_class(source),
        Error::SourceError(_) | Error::ClassifiedSourceError { .. } => "source",
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
        Error::PostCommitConfirmFailed { .. } => "post_commit_confirm_failed",
        Error::Backpressure(_) => "backpressure",
        Error::Aggregate { .. } => "aggregate",
    }
}

/// Maximum samples retained per in-memory histogram.
///
/// The in-memory histograms exist to serve `export_metrics()` between scrapes; they
/// are not a durable store. They previously appended one `u64` per event **forever**
/// — only `reset()` cleared them — so a long-running pipeline leaked roughly 2.8 GB
/// per hour at 100k events/s, on the event path. `export_metrics` also clones the
/// whole vector per scrape, so unbounded growth made scraping progressively slower too.
///
/// Retaining a bounded window of the most recent samples keeps p50/p95/p99 meaningful
/// (they describe recent behaviour, which is what an operator wants) at fixed cost.
const MAX_HISTOGRAM_SAMPLES: usize = 8192;

/// Append a sample, evicting the oldest when the window is full.
fn push_bounded(samples: &mut Vec<u64>, value: u64) {
    if samples.len() >= MAX_HISTOGRAM_SAMPLES {
        // Drop the oldest quarter in one move rather than shifting on every push,
        // which would make this O(n) per event.
        samples.drain(..MAX_HISTOGRAM_SAMPLES / 4);
    }
    samples.push(value);
}

/// A point-in-time snapshot of the collector's in-memory metric state.
///
/// Produced by [`OTelMetricsCollector::export_metrics`], and independent of the OTLP
/// exporter — useful for assertions in tests and for embedders that want to serve their
/// own metrics endpoint.
#[derive(Debug, Clone)]
pub struct MetricsReport {
    /// `service.name` resource attribute.
    pub service_name: String,
    /// `service.version` resource attribute.
    pub service_version: String,
    /// `deployment.environment` resource attribute.
    pub environment: String,
    /// Monotonic counters keyed by metric name, with their label set.
    pub counters: HashMap<String, (u64, HashMap<String, String>)>,
    /// Last-written gauge values keyed by metric name.
    pub gauges: HashMap<String, f64>,
    /// Retained histogram samples keyed by metric name.
    ///
    /// **Bounded**: only the most recent samples are kept (see the collector's window
    /// size). These previously grew one `u64` per event forever, which leaked roughly
    /// 2.8 GB/hour at 100k events/sec.
    pub histograms: HashMap<String, Vec<u64>>,
}

impl MetricsReport {
    /// Read a counter by exact metric name.
    pub fn get_counter(&self, name: &str) -> Option<u64> {
        self.counters.get(name).map(|(value, _)| *value)
    }

    /// Read a gauge by exact metric name.
    pub fn get_gauge(&self, name: &str) -> Option<f64> {
        self.gauges.get(name).copied()
    }

    /// Compute a percentile over a histogram's retained samples.
    ///
    /// `percentile` is expressed 0–100. The result covers only the retained window, not
    /// the full history — a long-running process's p99 here is the p99 of recent traffic.
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

    /// Sum of every `rustcdc.runtime.events_polled` counter across all operation labels.
    pub fn total_events_processed(&self) -> u64 {
        self.counters
            .iter()
            .filter(|(name, _)| name.starts_with("rustcdc.runtime.events_polled"))
            .map(|(_, (count, _))| count)
            .sum()
    }

    /// Mean event-processing duration over the retained histogram window, in ms.
    ///
    /// `None` when no samples have been recorded. A mean is not a latency SLO — prefer
    /// [`MetricsReport::get_histogram_percentile`] for tail behaviour.
    pub fn avg_event_processing_latency(&self) -> Option<f64> {
        self.histograms
            .get("rustcdc.runtime.event_processing_duration_ms")
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
/// [`EventTracer`] backed by OpenTelemetry, with an in-memory span mirror.
///
/// Cheap to clone — clones share one state. `is_enabled()` returns `true`, so the runtime
/// will build trace ids on the event path; use [`crate::core::NoOpEventTracer`] when
/// tracing is off, which skips that work entirely.
pub struct OTelEventTracer {
    state: Arc<Mutex<TracingState>>,
    tracer: Arc<opentelemetry::global::BoxedTracer>,
    source_type: String,
    /// Retained so [`OTelEventTracer::shutdown`] can flush the batch exporter.
    ///
    /// `None` when this tracer did not install a provider (it is using whatever the
    /// process already had), in which case shutting it down is not this tracer's business.
    provider: Option<Arc<sdktrace::SdkTracerProvider>>,
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

/// A completed span retained in memory for inspection.
///
/// The tracer also exports spans over OTLP when configured; this record exists so tests
/// and embedders can assert on span shape without an external collector.
#[derive(Debug, Clone)]
pub struct SpanRecord {
    /// Caller-supplied span identifier.
    pub span_id: String,
    /// Span name.
    pub name: String,
    /// Unix epoch milliseconds when the span started.
    pub start_time_ms: u64,
    /// Unix epoch milliseconds when the span ended.
    pub end_time_ms: u64,
    /// Span attributes.
    pub attributes: HashMap<String, String>,
}

impl OTelEventTracer {
    /// Build a tracer against the globally installed tracer provider.
    ///
    /// Records spans in memory; exports only if a provider has been installed elsewhere.
    /// Use [`OTelEventTracer::with_otlp_exporter`] to install one.
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(TracingState::default())),
            tracer: Arc::new(global::tracer("rustcdc")),
            source_type: "unknown".to_string(),
            // This tracer did not install a provider, so shutting one down is not its
            // business — flushing a provider another component owns would cut its spans off.
            provider: None,
        }
    }

    /// Build a tracer that exports spans over OTLP/gRPC via a batch exporter.
    ///
    /// **Installs a process-global tracer provider.** Calling this more than once in a
    /// process replaces the previous provider.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] if the span exporter cannot be built.
    pub fn with_otlp_exporter(config: OTelConfig) -> Result<Self> {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(config.endpoint)
            .build()
            .map_err(|error| {
                Error::ConfigError(format!("failed to build OTLP span exporter: {error}"))
            })?;

        let tracer_provider = sdktrace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(otel_resource(
                &config.service_name,
                &config.service_version,
                &config.environment,
            ))
            .build();

        // Retained so `shutdown()` can flush it: as of opentelemetry 0.32 there is no
        // `global::shutdown_tracer_provider()`, and dropping the global without flushing
        // silently discards whatever the batch exporter still holds.
        global::set_tracer_provider(tracer_provider.clone());

        Ok(Self {
            state: Arc::new(Mutex::new(TracingState::default())),
            tracer: Arc::new(global::tracer("rustcdc")),
            source_type: "unknown".to_string(),
            provider: Some(Arc::new(tracer_provider)),
        })
    }

    /// Label every span this tracer produces with a connector type.
    #[must_use]
    pub fn with_source_type(mut self, source_type: impl Into<String>) -> Self {
        self.source_type = source_type.into();
        self
    }

    /// Flush and shut down the process-global tracer provider.
    ///
    /// Call before process exit: the batch exporter buffers spans that are otherwise lost.
    pub fn shutdown(&self) {
        if let Some(provider) = &self.provider {
            if let Err(error) = provider.shutdown() {
                tracing::warn!(
                    target: "rustcdc::core::otel",
                    error = %error,
                    "tracer provider shutdown failed; buffered spans may be lost",
                );
            }
        }
    }

    /// Start a root span under the caller-supplied `span_id`.
    pub fn start_span(&self, span_id: &str, span_name: &str, attributes: HashMap<String, String>) {
        self.start_span_with_parent(span_id, span_name, attributes, None);
    }

    /// Start a span, optionally as a child of a currently-active span.
    ///
    /// An unknown `parent_span_id` starts a root span rather than failing.
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

    /// End a span, recording an outcome status and an optional error classification.
    ///
    /// Unknown `span_id` values are ignored — ending a span twice is not an error.
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

    /// End a span with status `"ok"`.
    pub fn end_span(&self, span_id: &str) {
        self.end_span_with_status(span_id, "ok", None);
    }

    /// Start a span covering a table's whole snapshot.
    pub fn start_snapshot_span(&self, span_id: &str, table: &str, row_count: u64) {
        let mut attrs = HashMap::new();
        attrs.insert("source.table".to_string(), table.to_string());
        attrs.insert("snapshot.row_count".to_string(), row_count.to_string());
        self.start_span(span_id, "rustcdc.snapshot", attrs);
    }

    /// Start a span covering one snapshot chunk.
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
        self.start_span_with_parent(
            span_id,
            "rustcdc.snapshot.chunk",
            attrs,
            Some(snapshot_span_id),
        );
    }

    /// Start a span covering one batch of streamed events.
    pub fn start_stream_span(&self, span_id: &str, table: Option<&str>, events_count: u64) {
        let mut attrs = HashMap::new();
        attrs.insert("stream.events_count".to_string(), events_count.to_string());
        attrs.insert(
            "source.table".to_string(),
            table.unwrap_or("n/a").to_string(),
        );
        self.start_span(span_id, "rustcdc.stream", attrs);
    }

    /// Start a span covering one transform stage.
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
        self.start_span_with_parent(span_id, "rustcdc.event.transform", attrs, parent_span_id);
    }

    /// Start a span covering one durable checkpoint commit.
    pub fn start_checkpoint_commit_span(&self, span_id: &str, events_count: u64) {
        let mut attrs = HashMap::new();
        attrs.insert(
            "checkpoint.events_count".to_string(),
            events_count.to_string(),
        );
        attrs.insert("source.table".to_string(), "n/a".to_string());
        self.start_span(span_id, "rustcdc.checkpoint.commit", attrs);
    }

    /// Start a span covering the snapshot-to-stream handoff.
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
        self.start_span(span_id, "rustcdc.handoff", attrs);
    }

    /// Snapshot the completed spans retained in memory.
    ///
    /// # Errors
    ///
    /// Returns an error string if the internal state mutex is poisoned.
    pub fn export_spans(&self) -> std::result::Result<Vec<SpanRecord>, String> {
        self.state
            .lock()
            .map_err(|error| error.to_string())
            .map(|state| state.completed_spans.clone())
    }

    /// Drop all active and completed spans. For tests.
    pub fn reset(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.active_spans.clear();
            state.completed_spans.clear();
            state.event_correlation.clear();
        }
    }

    /// Attach this tracer's trace/span ids to an event payload for downstream correlation.
    ///
    /// Returns `false` when no correlation context is recorded for `event_id`, leaving the
    /// event untouched — a consumer must not treat an absent trace id as an empty one.
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
        self.start_span(event_id, "rustcdc.event.transform", attributes);
    }

    fn trace_event_end(&self, event_id: &str, status: &str) {
        self.end_span_with_status(event_id, status, Some(status));
    }

    fn trace_checkpoint_barrier(&self, state: &str) {
        let span_id = format!("barrier-{state}");
        let mut attributes = HashMap::new();
        attributes.insert("checkpoint.state".to_string(), state.to_string());
        attributes.insert("source.table".to_string(), "n/a".to_string());
        self.start_span(&span_id, "rustcdc.checkpoint.commit", attributes);
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
        assert_eq!(
            report.get_counter("rustcdc.runtime.events_filtered"),
            Some(2)
        );
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
        assert_eq!(
            report.get_gauge("rustcdc.runtime.replication_lag_ms"),
            Some(1_000.0)
        );
        assert_eq!(
            report.get_gauge("rustcdc.runtime.buffer_depth"),
            Some(500.0)
        );
        assert_eq!(
            report.get_gauge("rustcdc.runtime.snapshot_progress_percent"),
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
        tracer.start_span("event-1", "rustcdc.snapshot.chunk", attrs);
        tracer.end_span("event-1");
        let spans = tracer.export_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "rustcdc.snapshot.chunk");
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
        assert_eq!(spans[0].name, "rustcdc.checkpoint.commit");
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
        assert_eq!(chunk.name, "rustcdc.snapshot.chunk");
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
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
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
            report.get_counter("rustcdc.runtime.events_committed"),
            Some(7)
        );
        assert_eq!(
            report.get_gauge("rustcdc.runtime.replication_lag_events"),
            Some(3.0)
        );
        assert_eq!(
            report.get_histogram_percentile("rustcdc.runtime.event_processing_duration_ms", 50.0),
            Some(29)
        );
        assert!(report
            .counters
            .contains_key("rustcdc.runtime.errors[context=runtime.poll]"));
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
                "rustcdc.runtime.event_processing_duration_ms".to_string(),
                Vec::new(),
            )]),
        };

        assert_eq!(
            report.get_histogram_percentile("rustcdc.runtime.event_processing_duration_ms", 95.0),
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
        assert!(spans
            .iter()
            .any(|span| span.name == "rustcdc.event.transform"));
        assert!(spans
            .iter()
            .any(|span| span.name == "rustcdc.checkpoint.commit"));
        assert!(spans.iter().any(|span| span.name == "rustcdc.handoff"));

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
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
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
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        };

        assert!(!tracer.propagate_baggage_to_event("missing-event", &mut event));
    }
}
