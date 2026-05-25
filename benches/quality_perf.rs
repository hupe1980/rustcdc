use async_trait::async_trait;
use rustcdc::transform::{Transform, TransformPipeline};
use rustcdc::{Event, Operation, SnapshotValidator, SourceMetadata, EVENT_ENVELOPE_VERSION};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::json;
use std::collections::HashSet;
use std::hint::black_box;
use tokio::runtime::Builder;

fn build_transform_pipeline() -> TransformPipeline {
    let mut pipeline = TransformPipeline::default();
    pipeline.add_transform(Box::new(AddTagTransform));
    pipeline.add_transform(Box::new(NormalizeNameTransform));
    pipeline
}

fn build_event(id: u64) -> Event {
    Event {
        before: None,
        after: Some(
            json!({"id": id, "name": format!("user-{id}"), "email": format!("user-{id}@example.com")}),
        ),
        op: Operation::Read,
        source: SourceMetadata {
            source_name: "bench".to_string(),
            offset: id.to_string(),
            timestamp: id + 1,
        },
        ts: id + 1,
        schema: Some("public".to_string()),
        table: "users".to_string(),
        primary_key: Some(vec!["id".to_string()]),
        snapshot: None,
        transaction: None,
        envelope_version: EVENT_ENVELOPE_VERSION,
    }
}

fn run_pipeline_batch(
    runtime: &tokio::runtime::Runtime,
    pipeline: &mut TransformPipeline,
    size: u64,
) {
    for idx in 1..=size {
        let event = build_event(idx);
        let _ = runtime
            .block_on(pipeline.apply(event))
            .expect("apply transform pipeline")
            .expect("event should not be filtered");
    }
}

fn dedup_by_id(mut events: Vec<Event>) -> Vec<Event> {
    let mut seen = HashSet::with_capacity(events.len());
    events.retain(|event| {
        let id = event
            .after
            .as_ref()
            .and_then(|value| value.get("id"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        seen.insert(id)
    });
    events
}

struct AddTagTransform;

#[async_trait]
impl Transform for AddTagTransform {
    async fn apply(&self, event: &mut Event) -> rustcdc::Result<bool> {
        if let Some(after) = event.after.as_mut().and_then(|value| value.as_object_mut()) {
            after.insert("bench_tag".to_string(), json!("quality"));
        }
        Ok(true)
    }

    fn name(&self) -> &str {
        "add_tag"
    }
}

struct NormalizeNameTransform;

#[async_trait]
impl Transform for NormalizeNameTransform {
    async fn apply(&self, event: &mut Event) -> rustcdc::Result<bool> {
        if let Some(after) = event.after.as_mut().and_then(|value| value.as_object_mut()) {
            if let Some(serde_json::Value::String(name)) = after.get_mut("name") {
                name.make_ascii_uppercase();
            }
        }
        Ok(true)
    }

    fn name(&self) -> &str {
        "normalize_name"
    }
}

fn bench_event_json_roundtrip(c: &mut Criterion) {
    let payload = build_event(1);
    c.bench_function("event_json_roundtrip", |b| {
        b.iter(|| {
            let encoded = black_box(&payload).to_json().expect("serialize event");
            Event::from_json(&encoded).expect("deserialize event")
        })
    });
}

fn bench_transform_pipeline(c: &mut Criterion) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let pipeline = build_transform_pipeline();

    c.bench_function("transform_pipeline_two_stages", |b| {
        b.iter(|| {
            let event = build_event(black_box(100));
            runtime
                .block_on(pipeline.apply(event))
                .expect("apply transform pipeline")
        })
    });
}

fn bench_snapshot_10k_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_validator");
    let size = 10_000_u64;
    let events: Vec<Event> = (1..=size).map(build_event).collect();
    group.throughput(Throughput::Elements(size));
    group.bench_with_input(BenchmarkId::from_parameter(size), &events, |b, input| {
        b.iter(|| {
            let mut validator = SnapshotValidator::new();
            validator.set_expected_count("users", size);
            for event in input {
                validator.track_event(event).expect("track snapshot event");
            }
            validator.finalize().expect("finalize snapshot validator")
        })
    });
    group.finish();
}

fn bench_stream_1k_events(c: &mut Criterion) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let mut pipeline = build_transform_pipeline();

    let mut group = c.benchmark_group("stream_events");
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("stream_1k_events", |b| {
        b.iter(|| run_pipeline_batch(&runtime, &mut pipeline, black_box(1_000)))
    });
    group.finish();
}

fn bench_full_cycle_snapshot_stream_handoff(c: &mut Criterion) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let mut pipeline = build_transform_pipeline();

    let snapshot_events: Vec<Event> = (1..=10_000).map(build_event).collect();
    let overlap_prefetch: Vec<Event> = (9_500..=10_500).map(build_event).collect();

    c.bench_function("full_cycle_snapshot_stream_handoff", |b| {
        b.iter(|| {
            let mut validator = SnapshotValidator::new();
            validator.set_expected_count("users", 10_000);
            for event in &snapshot_events {
                validator.track_event(event).expect("track snapshot event");
            }
            let _ = validator.finalize().expect("validate snapshot consistency");

            run_pipeline_batch(&runtime, &mut pipeline, 1_000);

            let _forward = dedup_by_id(overlap_prefetch.clone());
        })
    });
}

fn bench_parallel_snapshot_4x100k(c: &mut Criterion) {
    let table_events: Vec<(String, Vec<Event>)> = (0..4_u64)
        .map(|table_idx| {
            let table_name = format!("users_{table_idx}");
            let offset_base = table_idx * 100_000;
            let events = (1..=100_000_u64)
                .map(|row| {
                    let mut event = build_event(offset_base + row);
                    event.table = table_name.clone();
                    event
                })
                .collect::<Vec<_>>();
            (table_name, events)
        })
        .collect();

    c.bench_function("parallel_snapshot_4_tables_100k", |b| {
        b.iter(|| {
            for (table_name, events) in &table_events {
                let mut validator = SnapshotValidator::new();
                validator.set_expected_count(table_name, 100_000);
                for event in events {
                    validator
                        .track_event(event)
                        .expect("track parallel snapshot row");
                }
                let _ = validator
                    .finalize()
                    .expect("finalize parallel snapshot table");
            }
        })
    });
}

fn bench_quality_gate_targets(c: &mut Criterion) {
    let mut group = c.benchmark_group("quality_gates");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(3));

    group.bench_function("snapshot_10k_rows", |b| {
        let events: Vec<Event> = (1..=10_000).map(build_event).collect();
        b.iter(|| {
            let mut validator = SnapshotValidator::new();
            validator.set_expected_count("users", 10_000);
            for event in &events {
                validator.track_event(event).expect("track snapshot event");
            }
            validator.finalize().expect("finalize snapshot")
        })
    });

    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let mut pipeline = build_transform_pipeline();
    group.bench_function("stream_1k_events_target", |b| {
        b.iter(|| run_pipeline_batch(&runtime, &mut pipeline, 1_000))
    });

    group.finish();
}

fn bench_full_quality_suite(c: &mut Criterion) {
    bench_snapshot_10k_rows(c);
    bench_stream_1k_events(c);
    bench_full_cycle_snapshot_stream_handoff(c);
    bench_parallel_snapshot_4x100k(c);
    bench_quality_gate_targets(c);
}

fn bench_utility(c: &mut Criterion) {
    bench_event_json_roundtrip(c);
    bench_transform_pipeline(c);
}

criterion_group!(quality_perf, bench_full_quality_suite, bench_utility);
criterion_main!(quality_perf);
