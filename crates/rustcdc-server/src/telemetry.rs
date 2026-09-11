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
        if let Some(provider) = self.tracer_provider.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("warn: OTel tracer shutdown error: {e}");
        }
        if let Some(provider) = self.meter_provider.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("warn: OTel meter shutdown error: {e}");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Everything the OTLP exporters need, in one value.
///
/// Previously seven positional arguments that `main` unpacked field by field from
/// `ObservabilityConfig`, with the eighth — the insecure-transport override — smuggled in
/// through a bare `std::env::var` inside the validator. One struct built by
/// `From<&ObservabilityConfig>` means adding a setting is a field, not a signature change,
/// and there is exactly one place that decides what telemetry is configured with.
#[derive(Debug, Clone)]
pub struct OtlpOptions {
    /// Traces endpoint. `None` disables trace export.
    pub endpoint: Option<String>,
    /// Metrics endpoint. Falls back to [`Self::endpoint`] when `None`.
    pub metrics_endpoint: Option<String>,
    /// `PeriodicReader` export interval.
    pub metrics_interval_secs: u64,
    pub protocol: OtlpProtocol,
    pub service_name: String,
    /// Permit plaintext OTLP to a non-loopback host. See [`validate_otlp_endpoint`].
    ///
    /// Named differently from its configuration field (`observability.otlp_allow_insecure`)
    /// on purpose: `registries.*.allow_insecure` is a distinct setting, and the
    /// inert-settings scanner matches consumers by field name, so a second
    /// `.allow_insecure` in the corpus would mark the registry field as externally read
    /// when it is not.
    pub insecure_transport: bool,
}

impl Default for OtlpOptions {
    fn default() -> Self {
        Self {
            endpoint: None,
            metrics_endpoint: None,
            metrics_interval_secs: 30,
            protocol: OtlpProtocol::Grpc,
            service_name: "rustcdc-server".to_string(),
            insecure_transport: false,
        }
    }
}

impl From<&crate::config::schema::ObservabilityConfig> for OtlpOptions {
    fn from(config: &crate::config::schema::ObservabilityConfig) -> Self {
        Self {
            endpoint: config.otlp_endpoint.clone(),
            metrics_endpoint: config.otlp_metrics_endpoint.clone(),
            metrics_interval_secs: config.otlp_metrics_interval_secs,
            protocol: OtlpProtocol::from_config(&config.otlp_protocol),
            service_name: config.service_name.clone(),
            insecure_transport: config.otlp_allow_insecure,
        }
    }
}

/// Which OTLP wire protocol the exporters speak.
///
/// `observability.otlp_protocol` has been in the configuration schema, the reference docs
/// and the `[observability]` example since the beginning, and **nothing read it** — both
/// exporters were built with a hardcoded `.with_tonic()`. Pointing the server at an
/// OTLP/HTTP collector on `:4318`, as the documented `otlp_protocol = "http"` invites,
/// produced a gRPC exporter talking to an HTTP endpoint: no telemetry, no error, and a
/// setting that reads as applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpProtocol {
    /// OTLP over gRPC, the default. Collector port 4317.
    Grpc,
    /// OTLP over HTTP with protobuf encoding (`http/protobuf`). Collector port 4318.
    Http,
}

impl OtlpProtocol {
    /// Parse the configured value, defaulting to gRPC for anything unrecognised.
    ///
    /// The loader validates this field, so an unknown value should not reach here; the
    /// fallback exists so a future schema change cannot turn a typo into a startup crash
    /// in the telemetry layer, which is the one subsystem that must never stop the server.
    pub fn from_config(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "http" | "http/protobuf" | "http-proto" => Self::Http,
            _ => Self::Grpc,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::Http => "http/protobuf",
        }
    }
}

/// Initialise the global tracing subscriber and, optionally, the OTel tracer and metrics
/// providers.
///
/// Must be called exactly once, before any `tracing::*` macro is used.
pub fn init_with_metrics(
    format: Option<&str>,
    level: Option<&str>,
    otlp: &OtlpOptions,
) -> Result<TelemetryGuard, AppError> {
    let filter =
        EnvFilter::try_new(level.unwrap_or("info")).unwrap_or_else(|_| EnvFilter::new("info"));

    let otlp_endpoint = otlp.endpoint.as_deref();
    let (maybe_tracer_provider, otel_tracer) = build_optional_tracer(otlp_endpoint, otlp)?;

    // Resolve the effective metrics endpoint: explicit override → trace endpoint fallback.
    let effective_metrics_endpoint = otlp.metrics_endpoint.as_deref().or(otlp_endpoint);
    let maybe_meter_provider = build_optional_meter(effective_metrics_endpoint, otlp)?;

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
    otlp: &OtlpOptions,
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
    let (protocol, service_name) = (otlp.protocol, otlp.service_name.as_str());

    validate_otlp_endpoint(endpoint, otlp.insecure_transport)?;

    let exporter = match protocol {
        OtlpProtocol::Grpc => opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .with_timeout(OTLP_EXPORT_TIMEOUT)
            .build(),
        OtlpProtocol::Http => opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(http_signal_endpoint(endpoint, "/v1/traces"))
            .with_timeout(OTLP_EXPORT_TIMEOUT)
            .build(),
    }
    .map_err(|e| {
        AppError::Other(format!(
            "OTLP span exporter init error ({}): {e}",
            protocol.as_str()
        ))
    })?;

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
    otlp: &OtlpOptions,
) -> Result<Option<SdkMeterProvider>, AppError> {
    let Some(endpoint) = endpoint else {
        return Ok(None);
    };
    let (interval_secs, protocol, service_name) = (
        otlp.metrics_interval_secs,
        otlp.protocol,
        otlp.service_name.as_str(),
    );

    validate_otlp_endpoint(endpoint, otlp.insecure_transport)?;

    // The export timeout moved onto the exporter in opentelemetry 0.32 — the
    // `PeriodicReader` builder now only owns the interval.
    let exporter = match protocol {
        OtlpProtocol::Grpc => opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .with_timeout(OTLP_EXPORT_TIMEOUT)
            .build(),
        OtlpProtocol::Http => opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(http_signal_endpoint(endpoint, "/v1/metrics"))
            .with_timeout(OTLP_EXPORT_TIMEOUT)
            .build(),
    }
    .map_err(|e| {
        AppError::Other(format!(
            "OTLP metrics exporter init error ({}): {e}",
            protocol.as_str()
        ))
    })?;

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

/// Resolve the endpoint an OTLP/HTTP exporter should POST to.
///
/// `opentelemetry-otlp`'s HTTP exporter uses `with_endpoint` **verbatim**: given
/// `http://collector:4318` it posts to `/`, which every collector answers with 404 — and
/// the SDK reports export failures on its internal log, so the symptom is "no telemetry"
/// with nothing obvious to point at.
///
/// One `otlp_endpoint` setting has to serve both protocols, and for gRPC it is a bare
/// authority with no path. So this follows the OTLP specification's rule for
/// `OTEL_EXPORTER_OTLP_ENDPOINT`: a base URL gets the signal path appended. An endpoint
/// that already carries a path is left exactly as written, which is the escape hatch for
/// a collector behind a prefix (`https://gw.example.com/otlp/v1/traces`).
fn http_signal_endpoint(endpoint: &str, signal_path: &str) -> String {
    let Ok(mut url) = endpoint.parse::<url::Url>() else {
        // Unparseable endpoints are passed through here for the same reason
        // `validate_otlp_endpoint` passes them through: the SDK's own error names the
        // problem more precisely than a guess from here would.
        return endpoint.to_string();
    };

    if !matches!(url.path(), "" | "/") {
        return endpoint.to_string();
    }

    url.set_path(signal_path);
    url.to_string()
}

/// Reject plaintext (`http://`) OTLP to non-loopback hosts unless the operator has
/// explicitly opted in with `observability.otlp_allow_insecure`.
///
/// This prevents silent trace exfiltration when a misconfigured endpoint routes OTLP
/// spans — which carry table names, column names and source offsets — to a
/// attacker-controlled host in the clear.
///
/// The opt-in used to be the bare environment variable `OTLP_ALLOW_INSECURE=1`, read
/// here and declared nowhere. That made a security-relevant override invisible to
/// `validate-config`, absent from `GET /config`, unreachable by the inert-settings
/// scanner, and impossible to see in the configuration an operator reviews. It is a
/// configuration field now, for the same reason every other switch in this server is.
fn validate_otlp_endpoint(endpoint: &str, allow_insecure: bool) -> Result<(), AppError> {
    // Allow the escape hatch for development / test environments, but emit a
    // prominent warning so this setting is never silently carried to production
    // (a security information-disclosure risk).
    if allow_insecure {
        // Emit a structured warning via tracing so the override is
        // visible in log aggregators and monitoring dashboards, not only on
        // stderr.  The warning fires at startup and on every re-validation so
        // operators cannot silently carry this setting to production.
        tracing::warn!(
            otlp_endpoint = %endpoint,
            "observability.otlp_allow_insecure is active: traces are being exported over \
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
                "observability.otlp_endpoint '{}' is plaintext (http://) to a \
                 non-loopback host, so traces and metrics would cross the network \
                 unencrypted. Use https:// or a local collector sidecar, or set \
                 `observability.otlp_allow_insecure = true` to override (development \
                 only). Note this is \
                 about the transport, not the OTLP protocol: `otlp_protocol = \"http\"` \
                 against an https:// endpoint is fine.",
                endpoint
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        OtlpProtocol, build_optional_tracer, http_signal_endpoint, validate_otlp_endpoint,
    };

    /// `otlp_protocol = "http"` must put an OTLP/HTTP request on the wire.
    ///
    /// Asserting that the config string maps to `OtlpProtocol::Http` proves nothing about
    /// the exporter — that mapping existed in spirit for the whole time both exporters
    /// were hardcoded to `.with_tonic()`, and a revert to that would leave every
    /// enum-level test green. So this drives a real provider at a real socket and reads
    /// the request line: `POST /v1/traces` over HTTP/1.1 is something a gRPC exporter
    /// cannot produce.
    ///
    /// `build_optional_tracer` is used rather than `init*` because the latter installs a
    /// process-global subscriber, which no test can do twice.
    #[tokio::test]
    async fn the_http_protocol_actually_speaks_otlp_over_http() {
        use opentelemetry::trace::Tracer as _;
        use tokio::io::AsyncReadExt as _;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a collector stand-in");
        let addr = listener.local_addr().expect("local addr");

        let received = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.ok()?;
            let mut buf = vec![0u8; 512];
            let read = socket.read(&mut buf).await.ok()?;
            Some(String::from_utf8_lossy(&buf[..read]).into_owned())
        });

        let (provider, tracer) = build_optional_tracer(
            Some(&format!("http://{addr}")),
            &super::OtlpOptions {
                protocol: OtlpProtocol::Http,
                service_name: "otlp-protocol-test".to_string(),
                ..Default::default()
            },
        )
        .expect("the http exporter must build");

        let provider = provider.expect("a configured endpoint yields a provider");
        let tracer = tracer.expect("a configured endpoint yields a tracer");
        tracer.in_span("probe", |_| {});
        let _ = provider.force_flush();

        let request = tokio::time::timeout(std::time::Duration::from_secs(10), received)
            .await
            .expect("the exporter must reach the collector within the timeout")
            .expect("the accept task must not panic")
            .expect("the collector stand-in must receive a request");

        assert!(
            request.starts_with("POST /v1/traces"),
            "expected an OTLP/HTTP trace export; a gRPC exporter cannot produce this \
             request line. Got:\n{request}"
        );
        // 127.0.0.1 is loopback, so the plaintext guard admits it — asserted here so a
        // change to that guard cannot silently make this test unreachable.
        assert!(request.contains("HTTP/1.1"));

        let _ = provider.shutdown();
    }

    /// A base endpoint gains the signal path; an explicit one is left alone.
    ///
    /// One `otlp_endpoint` serves both protocols, and for gRPC it is a bare authority.
    /// Passing that straight to the HTTP exporter posts to `/`, which collectors 404 —
    /// silently, because the SDK reports export failures on its own internal log.
    #[test]
    fn a_base_endpoint_gains_the_signal_path_and_an_explicit_one_does_not() {
        assert_eq!(
            http_signal_endpoint("http://collector:4318", "/v1/traces"),
            "http://collector:4318/v1/traces"
        );
        assert_eq!(
            http_signal_endpoint("http://collector:4318/", "/v1/metrics"),
            "http://collector:4318/v1/metrics"
        );
        assert_eq!(
            http_signal_endpoint("https://gw.example.com/otlp/v1/traces", "/v1/traces"),
            "https://gw.example.com/otlp/v1/traces",
            "an explicit path is the escape hatch for a collector behind a prefix"
        );
        assert_eq!(
            http_signal_endpoint("not a url", "/v1/traces"),
            "not a url",
            "an unparseable endpoint is left for the SDK to complain about precisely"
        );
    }

    #[test]
    fn the_configured_protocol_string_selects_the_exporter() {
        assert_eq!(OtlpProtocol::from_config("http"), OtlpProtocol::Http);
        assert_eq!(OtlpProtocol::from_config("HTTP"), OtlpProtocol::Http);
        assert_eq!(OtlpProtocol::from_config("grpc"), OtlpProtocol::Grpc);
        // The loader rejects anything else, so this fallback is a backstop rather than a
        // behaviour anyone should reach.
        assert_eq!(OtlpProtocol::from_config("nonsense"), OtlpProtocol::Grpc);
    }

    // These no longer need a mutex. The override used to be a process-global environment
    // variable, so every test that touched it had to be serialised against every other —
    // and `std::env::set_var` is a data race against any concurrent `getenv` anywhere in
    // the binary, which no mutex in this module could prevent. It is a parameter now.
    #[test]
    fn rejects_plaintext_remote_otlp() {
        let err = validate_otlp_endpoint("http://otel-collector.prod.example.com:4317", false)
            .expect_err("must reject non-loopback http");
        assert!(err.to_string().contains("plaintext (http://)"));
    }

    #[test]
    fn allows_loopback_http() {
        validate_otlp_endpoint("http://localhost:4317", false)
            .expect("loopback http must be allowed");
        validate_otlp_endpoint("http://127.0.0.1:4317", false).expect("127.0.0.1 must be allowed");
    }

    #[test]
    fn allows_tls_remote() {
        validate_otlp_endpoint("https://otel-collector.prod.example.com:4317", false)
            .expect("https must be allowed");
    }

    #[test]
    fn allows_insecure_override() {
        validate_otlp_endpoint("http://remote-host:4317", true)
            .expect("observability.otlp_allow_insecure must bypass the check");
    }

    /// The override must arrive from configuration, not from the ambient environment.
    #[test]
    fn the_insecure_override_comes_from_configuration() {
        use crate::config::schema::ObservabilityConfig;

        let mut config = ObservabilityConfig {
            otlp_endpoint: Some("http://remote-host:4317".to_string()),
            ..Default::default()
        };
        let options = super::OtlpOptions::from(&config);
        assert!(!options.insecure_transport, "the default must be refuse");
        assert!(
            validate_otlp_endpoint("http://remote-host:4317", options.insecure_transport).is_err(),
            "a default configuration must still refuse plaintext to a remote host"
        );

        config.otlp_allow_insecure = true;
        let options = super::OtlpOptions::from(&config);
        assert!(options.insecure_transport);
    }
}
