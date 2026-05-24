#![cfg(feature = "metrics")]

use std::time::Duration;

use cdc_rs::{MetricsCollector, OTelConfig, OTelMetricsCollector, Operation};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

const OTEL_COLLECTOR_CONFIG: &str = r#"
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  prometheus:
    endpoint: 0.0.0.0:8889

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
"#;

#[tokio::test]
async fn otel_metrics_exports_over_otlp_to_queryable_prometheus_backend() -> cdc_rs::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping otel metrics integration test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let collector_container = GenericImage::new("otel/opentelemetry-collector-contrib", "0.104.0")
        .with_exposed_port(4317.tcp())
        .with_exposed_port(8889.tcp())
        .with_wait_for(WaitFor::seconds(2))
        .with_cmd(["--config=/etc/otelcol-contrib/config.yaml"])
        .with_copy_to(
            "/etc/otelcol-contrib/config.yaml",
            OTEL_COLLECTOR_CONFIG.as_bytes().to_vec(),
        )
        .start()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let host = collector_container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let otlp_port = collector_container
        .get_host_port_ipv4(4317.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
    let prometheus_port = collector_container
        .get_host_port_ipv4(8889.tcp())
        .await
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let service_name = format!("cdc-rs-otel-metrics-{}", std::process::id());
    let collector = OTelMetricsCollector::with_otlp_exporter(OTelConfig::new(
        format!("http://{host}:{otlp_port}"),
        service_name,
        "test",
        "integration",
    ))?;

    for _ in 0..500 {
        collector.record_event_processed(Operation::Insert, 4);
        collector.record_event_processed(Operation::Update, 7);
        collector.record_events_filtered(1);
    }

    collector.record_replication_lag_ms(9_999, 42);
    collector.record_checkpoint_offset("0/16B6A70");
    collector.record_checkpoint_committed(1000, 88);
    collector.record_buffer_size(128);
    collector.record_snapshot_progress(100);
    collector.record_error(
        &cdc_rs::Error::StateError("boom".to_string()),
        "integration",
    );

    tokio::time::timeout(
        Duration::from_secs(3),
        tokio::task::spawn_blocking(move || collector.shutdown()),
    )
    .await
    .map_err(|_| cdc_rs::Error::TimeoutError("otel metrics shutdown timed out".to_string()))?
    .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))??;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

    let mut metrics_text = String::new();
    for _ in 0..12 {
        let response = client
            .get(format!("http://{host}:{prometheus_port}/metrics"))
            .send()
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;
        if !response.status().is_success() {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }

        let body = response
            .text()
            .await
            .map_err(|error| cdc_rs::Error::SourceError(error.to_string()))?;

        if body.contains("cdc_events_processed_total") && body.contains("cdc_events_filtered_total")
        {
            metrics_text = body;
            break;
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    assert!(
        !metrics_text.is_empty(),
        "collector /metrics endpoint did not expose CDC metric series"
    );

    let insert_total = metric_value(
        &metrics_text,
        "cdc_events_processed_total",
        &["operation=\"insert\""],
    )
    .unwrap_or(0.0);
    let update_total = metric_value(
        &metrics_text,
        "cdc_events_processed_total",
        &["operation=\"update\""],
    )
    .unwrap_or(0.0);
    let filtered_total =
        metric_value(&metrics_text, "cdc_events_filtered_total", &[]).unwrap_or(0.0);

    assert!(
        insert_total >= 500.0,
        "expected insert counter >= 500, got {insert_total}"
    );
    assert!(
        update_total >= 500.0,
        "expected update counter >= 500, got {update_total}"
    );
    assert!(
        filtered_total >= 500.0,
        "expected filtered counter >= 500, got {filtered_total}"
    );
    assert!(
        metric_value(&metrics_text, "cdc_replication_lag_ms", &[]).is_some(),
        "expected cdc_replication_lag_ms gauge in exported metrics"
    );

    Ok(())
}

fn metric_value(metrics_text: &str, metric_name: &str, required_labels: &[&str]) -> Option<f64> {
    for line in metrics_text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if !trimmed.starts_with(metric_name) {
            continue;
        }
        if required_labels.iter().any(|label| !trimmed.contains(label)) {
            continue;
        }

        let value = trimmed.split_whitespace().last()?;
        if let Ok(parsed) = value.parse::<f64>() {
            return Some(parsed);
        }
    }

    None
}
