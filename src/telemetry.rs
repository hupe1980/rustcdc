use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::{metrics::SdkMeterProvider, trace::SdkTracerProvider};
use std::time::Duration;
use tracing_subscriber::{filter::EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::error::AppError;

// ─────────────────────────────────────────────────────────────────────────────
// RAII guard
// ─────────────────────────────────────────────────────────────────────────────

/// Returned by [`init`].  Hold it alive for the duration of `main`; it
/// flushes and shuts down the OTel trace and metrics exporters on drop.
pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            if let Err(e) = provider.shutdown() {
                eprintln!("warn: OTel tracer shutdown error: {e}");
            }
        }
        if let Some(provider) = self.meter_provider.take() {
            if let Err(e) = provider.shutdown() {
                eprintln!("warn: OTel meter shutdown error: {e}");
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Initialise the global tracing subscriber and, optionally, the OTel tracer
/// and metrics provider.
///
/// Must be called exactly once, before any `tracing::*` macros are used.
pub fn init(
    format: Option<&str>,
    level: Option<&str>,
    otlp_endpoint: Option<&str>,
    service_name: &str,
) -> Result<TelemetryGuard, AppError> {
    init_with_metrics(format, level, otlp_endpoint, None, 30, service_name)
}

/// Full initialisation with separate trace and metrics endpoints.
///
/// `otlp_metrics_endpoint` falls back to `otlp_endpoint` when `None`.
/// `metrics_interval_secs` controls how often the `PeriodicReader` exports.
pub fn init_with_metrics(
    format: Option<&str>,
    level: Option<&str>,
    otlp_endpoint: Option<&str>,
    otlp_metrics_endpoint: Option<&str>,
    metrics_interval_secs: u64,
    service_name: &str,
) -> Result<TelemetryGuard, AppError> {
    let filter =
        EnvFilter::try_new(level.unwrap_or("info")).unwrap_or_else(|_| EnvFilter::new("info"));

    let (maybe_tracer_provider, otel_tracer) = build_optional_tracer(otlp_endpoint, service_name)?;

    // Resolve the effective metrics endpoint: explicit override → trace endpoint fallback.
    let effective_metrics_endpoint = otlp_metrics_endpoint.or(otlp_endpoint);
    let maybe_meter_provider = build_optional_meter(
        effective_metrics_endpoint,
        metrics_interval_secs,
        service_name,
    )?;

    // Register the global meter provider so instrument macros work without a handle.
    if let Some(ref mp) = maybe_meter_provider {
        opentelemetry::global::set_meter_provider(mp.clone());
    }

    // Four branches: json×{otel,no-otel}  and  text×{otel,no-otel}.
    match (format, otel_tracer) {
        (Some("json"), Some(tracer)) => {
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    fmt::layer()
                        .json()
                        .with_current_span(true)
                        .with_span_list(true),
                )
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .try_init()
                .map_err(|e| AppError::Other(e.to_string()))?;
        }
        (Some("json"), None) => {
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    fmt::layer()
                        .json()
                        .with_current_span(true)
                        .with_span_list(true),
                )
                .try_init()
                .map_err(|e| AppError::Other(e.to_string()))?;
        }
        (_, Some(tracer)) => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().with_target(true))
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .try_init()
                .map_err(|e| AppError::Other(e.to_string()))?;
        }
        _ => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().with_target(true))
                .try_init()
                .map_err(|e| AppError::Other(e.to_string()))?;
        }
    }

    Ok(TelemetryGuard {
        tracer_provider: maybe_tracer_provider,
        meter_provider: maybe_meter_provider,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Internals
// ─────────────────────────────────────────────────────────────────────────────

/// Build a span exporter and return `(provider, tracer)`.
/// Returns `(None, None)` when no endpoint is configured.
fn build_optional_tracer(
    endpoint: Option<&str>,
    service_name: &str,
) -> Result<
    (
        Option<SdkTracerProvider>,
        Option<opentelemetry_sdk::trace::Tracer>,
    ),
    AppError,
> {
    let Some(endpoint) = endpoint else {
        return Ok((None, None));
    };

    validate_otlp_endpoint(endpoint)?;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(OTLP_EXPORT_TIMEOUT)
        .build()
        .map_err(|e| AppError::Other(format!("OTLP exporter init error: {e}")))?;

    let provider = SdkTracerProvider::builder()
        .with_resource(otel_resource(service_name))
        .with_batch_exporter(exporter)
        .build();

    // `tracer()` borrows `&self`, so it's safe to call before the move below.
    let tracer = provider.tracer(service_name.to_string());

    Ok((Some(provider), Some(tracer)))
}

/// Build an OTLP metrics `SdkMeterProvider` with a `PeriodicReader`.
/// Returns `None` when no endpoint is configured.
fn build_optional_meter(
    endpoint: Option<&str>,
    interval_secs: u64,
    service_name: &str,
) -> Result<Option<SdkMeterProvider>, AppError> {
    let Some(endpoint) = endpoint else {
        return Ok(None);
    };

    validate_otlp_endpoint(endpoint)?;

    // The export timeout moved onto the exporter in opentelemetry 0.32 — the
    // `PeriodicReader` builder now only owns the interval.
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(OTLP_EXPORT_TIMEOUT)
        .build()
        .map_err(|e| AppError::Other(format!("OTLP metrics exporter init error: {e}")))?;

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(interval_secs.max(1)))
        .build();

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(otel_resource(service_name))
        .build();

    Ok(Some(provider))
}

/// Deadline for a single OTLP export attempt.
const OTLP_EXPORT_TIMEOUT: Duration = Duration::from_secs(30);

/// Resource identifying this process to the collector.
///
/// `Resource::builder()` seeds the SDK-detected attributes (telemetry SDK name,
/// version, language) and honours `OTEL_RESOURCE_ATTRIBUTES`, so an operator can
/// add `deployment.environment` or `service.instance.id` without a config field.
fn otel_resource(service_name: &str) -> opentelemetry_sdk::Resource {
    opentelemetry_sdk::Resource::builder()
        .with_service_name(service_name.to_string())
        .build()
}

/// Reject plaintext gRPC (`http://`) to non-loopback hosts unless the
/// operator has explicitly opted in with `OTLP_ALLOW_INSECURE=1`.
///
/// This prevents silent credential/trace exfiltration when a misconfigured
/// endpoint routes OTLP spans to an attacker-controlled host over the wire.
fn validate_otlp_endpoint(endpoint: &str) -> Result<(), AppError> {
    // Allow the escape hatch for development / test environments, but emit a
    // prominent warning so this setting is never silently carried to production
    // (a security information-disclosure risk).
    if std::env::var("OTLP_ALLOW_INSECURE").as_deref() == Ok("1") {
        // Emit a structured warning via tracing so the override is
        // visible in log aggregators and monitoring dashboards, not only on
        // stderr.  The warning fires at startup and on every re-validation so
        // operators cannot silently carry this setting to production.
        tracing::warn!(
            otlp_endpoint = %endpoint,
            "OTLP_ALLOW_INSECURE=1 is active: traces are being exported over \
             plaintext gRPC. CDC pipeline metadata (table names, operation \
             types, source offsets) will be transmitted unencrypted. \
             DO NOT use this setting in production."
        );
        return Ok(());
    }

    // Only inspect URLs we can parse; pass-through on parse failure (the OTLP
    // SDK will surface a clearer error at build time).
    let Ok(url) = endpoint.parse::<url::Url>() else {
        return Ok(());
    };

    if url.scheme() == "http" {
        let host = url.host_str().unwrap_or("");
        let is_loopback = matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]");
        if !is_loopback {
            return Err(AppError::Other(format!(
                "observability.otlp_endpoint '{}' uses plaintext gRPC (http://) to a \
                 non-loopback host. Use https:// or a local sidecar, or set \
                 OTLP_ALLOW_INSECURE=1 to override (development only).",
                endpoint
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_otlp_endpoint;
    use std::sync::Mutex;

    // Serialize tests that mutate OTLP_ALLOW_INSECURE to prevent cross-test races.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn rejects_plaintext_remote_otlp() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let err = validate_otlp_endpoint("http://otel-collector.prod.example.com:4317")
            .expect_err("must reject non-loopback http");
        assert!(err.to_string().contains("plaintext gRPC"));
    }

    #[test]
    fn allows_loopback_http() {
        let _guard = ENV_MUTEX.lock().unwrap();
        validate_otlp_endpoint("http://localhost:4317").expect("loopback http must be allowed");
        validate_otlp_endpoint("http://127.0.0.1:4317").expect("127.0.0.1 must be allowed");
    }

    #[test]
    fn allows_tls_remote() {
        let _guard = ENV_MUTEX.lock().unwrap();
        validate_otlp_endpoint("https://otel-collector.prod.example.com:4317")
            .expect("https must be allowed");
    }

    #[test]
    fn allows_insecure_override() {
        let _guard = ENV_MUTEX.lock().unwrap();
        // Safety: test-only, serialized via ENV_MUTEX; no concurrent threads mutate this var.
        unsafe { std::env::set_var("OTLP_ALLOW_INSECURE", "1") };
        let result = validate_otlp_endpoint("http://remote-host:4317");
        unsafe { std::env::remove_var("OTLP_ALLOW_INSECURE") };
        result.expect("OTLP_ALLOW_INSECURE=1 must bypass the check");
    }
}
