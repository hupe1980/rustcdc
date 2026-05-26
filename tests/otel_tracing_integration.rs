#![cfg(feature = "metrics")]

use std::{collections::HashMap, time::Duration};

use rustcdc::{OTelConfig, OTelEventTracer};
use serde_json::Value;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

#[tokio::test]
async fn otel_tracing_exports_hierarchy_and_crash_retry_to_jaeger() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping otel tracing integration test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("jaegertracing/all-in-one", "1.57")
        .with_exposed_port(4317.tcp())
        .with_exposed_port(16686.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "Starting jaeger-collector gRPC server",
        ))
        .with_env_var("COLLECTOR_OTLP_ENABLED", "true")
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let otlp_port = container
        .get_host_port_ipv4(4317.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let ui_port = container
        .get_host_port_ipv4(16686.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let service_name = format!("rustcdc-otel-tracing-{}", std::process::id());
    let tracer = OTelEventTracer::with_otlp_exporter(OTelConfig::new(
        format!("http://{host}:{otlp_port}"),
        service_name.clone(),
        "test",
        "integration",
    ))?
    .with_source_type("postgres");

    tracer.start_snapshot_span("snapshot-root", "public.users", 1000);
    tracer.start_snapshot_chunk_span("chunk-1", "snapshot-root", "public.users", 0, 500);
    tracer.end_span("chunk-1");
    tracer.end_span("snapshot-root");

    tracer.start_handoff_span("handoff-1", 17, None);

    let mut stream_attrs = HashMap::new();
    stream_attrs.insert("source.table".to_string(), "public.users".to_string());
    stream_attrs.insert("stream.events_count".to_string(), "42".to_string());
    tracer.start_span_with_parent("stream-1", "cdc.stream", stream_attrs, Some("handoff-1"));

    tracer.start_transform_span(
        "transform-crash",
        "mask_hash",
        Some("public.users"),
        Some("stream-1"),
    );
    tracer.end_span_with_status("transform-crash", "panic", Some("transform_crash"));

    tracer.start_transform_span(
        "transform-retry",
        "mask_hash",
        Some("public.users"),
        Some("stream-1"),
    );
    tracer.end_span("transform-retry");

    tracer.end_span("stream-1");
    tracer.end_span("handoff-1");

    tokio::time::timeout(
        Duration::from_secs(3),
        tokio::task::spawn_blocking(move || tracer.shutdown()),
    )
    .await
    .map_err(|_| rustcdc::Error::TimeoutError("otel tracer shutdown timed out".to_string()))?
    .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let mut validated = false;
    for _ in 0..30 {
        let response = client
            .get(format!(
                "http://{host}:{ui_port}/api/traces?service={service_name}&limit=20"
            ))
            .send()
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

        if !response.status().is_success() {
            tokio::time::sleep(Duration::from_millis(300)).await;
            continue;
        }

        let payload: Value = response
            .json()
            .await
            .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

        if payload_contains_required_hierarchy_and_retry(&payload) {
            validated = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    assert!(
        validated,
        "expected snapshot->chunk and handoff->stream hierarchy with crash/retry transform traces"
    );

    Ok(())
}

fn payload_contains_required_hierarchy_and_retry(payload: &Value) -> bool {
    let Some(traces) = payload.get("data").and_then(Value::as_array) else {
        return false;
    };

    let mut snapshot_parent_ok = false;
    let mut handoff_stream_ok = false;
    let mut saw_crash = false;
    let mut saw_retry = false;

    for trace in traces {
        let Some(spans) = trace.get("spans").and_then(Value::as_array) else {
            continue;
        };

        snapshot_parent_ok |= child_relation_exists(spans, "cdc.snapshot", "cdc.snapshot.chunk");
        handoff_stream_ok |= child_relation_exists(spans, "cdc.handoff", "cdc.stream");

        for span in spans {
            if span
                .get("operationName")
                .and_then(Value::as_str)
                .is_none_or(|name| name != "cdc.event.transform")
            {
                continue;
            }

            let has_crash_tag = span_has_tag(span, "error.type", "transform_crash")
                || span_has_tag(span, "error.type", "panic");

            if has_crash_tag {
                saw_crash = true;
            } else {
                saw_retry = true;
            }
        }
    }

    snapshot_parent_ok && handoff_stream_ok && saw_crash && saw_retry
}

fn child_relation_exists(spans: &[Value], parent_operation: &str, child_operation: &str) -> bool {
    let mut parent_span_id = None;
    for span in spans {
        if span
            .get("operationName")
            .and_then(Value::as_str)
            .is_some_and(|name| name == parent_operation)
        {
            parent_span_id = span
                .get("spanID")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            break;
        }
    }

    let Some(parent_span_id) = parent_span_id else {
        return false;
    };

    for span in spans {
        if span
            .get("operationName")
            .and_then(Value::as_str)
            .is_none_or(|name| name != child_operation)
        {
            continue;
        }

        let references = span
            .get("references")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        for reference in references {
            let is_child = reference
                .get("refType")
                .and_then(Value::as_str)
                .is_some_and(|value| value == "CHILD_OF");
            let parent_id_matches = reference
                .get("spanID")
                .and_then(Value::as_str)
                .is_some_and(|value| value == parent_span_id);
            if is_child && parent_id_matches {
                return true;
            }
        }
    }

    false
}

fn span_has_tag(span: &Value, key: &str, expected_value: &str) -> bool {
    let Some(tags) = span.get("tags").and_then(Value::as_array) else {
        return false;
    };

    tags.iter().any(|tag| {
        tag.get("key")
            .and_then(Value::as_str)
            .is_some_and(|tag_key| tag_key == key)
            && tag
                .get("value")
                .and_then(Value::as_str)
                .is_some_and(|value| value == expected_value)
    })
}
