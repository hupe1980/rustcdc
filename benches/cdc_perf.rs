use async_trait::async_trait;
use cdc_rs::transform::{Transform, TransformPipeline};
use cdc_rs::{Event, Operation, SnapshotValidator, SourceMetadata, WasmConfig, WasmRuntime, EVENT_ENVELOPE_VERSION};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::json;
use std::hint::black_box;
use std::sync::Arc;
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
        let transformed = runtime
            .block_on(pipeline.apply(event))
            .expect("apply transform pipeline");
        assert!(transformed.is_some(), "event should not be filtered");
    }
}

fn dedup_by_id(mut events: Vec<Event>) -> Vec<Event> {
    events.sort_by_key(|event| {
        event
            .after
            .as_ref()
            .and_then(|value| value.get("id"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0)
    });
    events.dedup_by(|a, b| {
        let a_id = a
            .after
            .as_ref()
            .and_then(|value| value.get("id"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        let b_id = b
            .after
            .as_ref()
            .and_then(|value| value.get("id"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        a_id == b_id
    });
    events
}

struct AddTagTransform;

#[async_trait]
impl Transform for AddTagTransform {
    async fn apply(&self, event: &mut Event) -> cdc_rs::Result<bool> {
        if let Some(after) = event.after.as_mut().and_then(|value| value.as_object_mut()) {
            after.insert("bench_tag".to_string(), json!("benchmark"));
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
    async fn apply(&self, event: &mut Event) -> cdc_rs::Result<bool> {
        if let Some(name) = event
            .after
            .as_mut()
            .and_then(|value| value.as_object_mut())
            .and_then(|row| row.get_mut("name"))
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
        {
            if let Some(after) = event.after.as_mut().and_then(|value| value.as_object_mut()) {
                after.insert("name".to_string(), json!(name.to_ascii_uppercase()));
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
            let transformed = runtime
                .block_on(pipeline.apply(event))
                .expect("apply transform pipeline");
            black_box(transformed)
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

    c.bench_function("full_cycle_snapshot_stream_handoff", |b| {
        b.iter(|| {
            let snapshot_events: Vec<Event> = (1..=10_000).map(build_event).collect();
            let mut validator = SnapshotValidator::new();
            validator.set_expected_count("users", 10_000);
            for event in &snapshot_events {
                validator.track_event(event).expect("track snapshot event");
            }
            let _ = validator.finalize().expect("validate snapshot consistency");

            run_pipeline_batch(&runtime, &mut pipeline, 1_000);

            let overlap_prefetch: Vec<Event> = (9_500..=10_500).map(build_event).collect();
            let _forward = dedup_by_id(overlap_prefetch);
        })
    });
}

fn bench_parallel_snapshot_4x100k(c: &mut Criterion) {
    c.bench_function("parallel_snapshot_4_tables_100k", |b| {
        b.iter(|| {
            for table_idx in 0..4_u64 {
                let table_name = format!("users_{table_idx}");
                let mut validator = SnapshotValidator::new();
                validator.set_expected_count(&table_name, 100_000);
                let offset_base = table_idx * 100_000;
                for row in 1..=100_000_u64 {
                    let mut event = build_event(offset_base + row);
                    event.table = table_name.clone();
                    validator
                        .track_event(&event)
                        .expect("track parallel snapshot row");
                }
                let _ = validator
                    .finalize()
                    .expect("finalize parallel snapshot table");
            }
        })
    });
}

fn bench_event_buffering(c: &mut Criterion) {
    use std::collections::VecDeque;

    c.bench_function("event_buffer_push_pop_1k", |b| {
        b.iter(|| {
            let mut buffered = VecDeque::with_capacity(1_000);
            for idx in 1..=1_000_u64 {
                buffered.push_back(build_event(idx));
            }

            let mut delivered = Vec::with_capacity(buffered.len());
            while let Some(event) = buffered.pop_front() {
                delivered.push(event);
            }

            black_box(delivered)
        })
    });
}

fn bench_partial_redelivery_clone_guardrails(c: &mut Criterion) {
    let events: Arc<[Event]> = Arc::from((1..=5_000_u64).map(build_event).collect::<Vec<_>>());

    let mut group = c.benchmark_group("partial_redelivery_guardrails");
    group.throughput(Throughput::Elements(5_000));

    group.bench_function("shared_backing_slice_view", |b| {
        b.iter(|| {
            let mut total_rows = 0usize;
            for prefix in 0..1_000_usize {
                let view = &events[prefix..];
                total_rows = total_rows.saturating_add(view.len());
            }
            black_box(total_rows)
        })
    });

    group.bench_function("clone_slice_baseline", |b| {
        b.iter(|| {
            let mut total_rows = 0usize;
            for prefix in 0..1_000_usize {
                let cloned = events[prefix..].to_vec();
                total_rows = total_rows.saturating_add(cloned.len());
            }
            black_box(total_rows)
        })
    });

    group.finish();
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
    bench_event_buffering(c);
    bench_partial_redelivery_clone_guardrails(c);
    bench_quality_gate_targets(c);
}

fn bench_utility(c: &mut Criterion) {
    bench_event_json_roundtrip(c);
    bench_transform_pipeline(c);
}

/// Compile a WAT text fixture to an in-memory WASM module bytes.
fn compile_wat(name: &str) -> Vec<u8> {
    let wat_path = std::path::Path::new("fixtures/wasm").join(name);
    let wat_src = std::fs::read_to_string(&wat_path)
        .unwrap_or_else(|_| panic!("read wat fixture: {}", wat_path.display()));
    wat::parse_str(&wat_src).expect("compile wat fixture")
}

/// Write WASM bytes to a tempfile and return the path.
/// The file must outlive the benchmark; callers keep it alive via a named binding.
fn wasm_tempfile(wasm: &[u8]) -> tempfile::NamedTempFile {
    let file = tempfile::Builder::new()
        .suffix(".wasm")
        .tempfile()
        .expect("create temp wasm file");
    std::fs::write(file.path(), wasm).expect("write wasm fixture");
    file
}

/// Benchmark the WASM pass-through transform (event in → same event out).
/// Documents per-invocation overhead against the <1ms target in docs/wasm_transform_sdk.md.
fn bench_wasm_transform_pass_through(c: &mut Criterion) {
    let wasm_bytes = compile_wat("pass_through.wat");
    let wasm_file = wasm_tempfile(&wasm_bytes);

    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let mut runtime = rt
        .block_on(async {
            let mut r = WasmRuntime::new_with_config(WasmConfig {
                module_path: wasm_file.path().to_path_buf(),
                timeout_ms: 50,
                memory_limit_mb: 16,
            })
            .expect("create wasm runtime");
            r.init().await.expect("init wasm runtime");
            r
        });

    let event = build_event(1);

    let mut group = c.benchmark_group("wasm_transform");
    group.throughput(Throughput::Elements(1));
    group.bench_function("pass_through_single_event", |b| {
        b.iter(|| {
            rt.block_on(runtime.transform(black_box(&event)))
                .expect("wasm transform")
        })
    });

    // 100-event batch: measures per-batch overhead and wasmtime epoch reset amortisation.
    let events: Vec<Event> = (1..=100).map(build_event).collect();
    group.throughput(Throughput::Elements(100));
    group.bench_function("pass_through_100_events", |b| {
        b.iter(|| {
            for e in black_box(&events) {
                rt.block_on(runtime.transform(e)).expect("wasm transform");
            }
        })
    });

    group.finish();
    // Keep the tempfile alive until after the benchmark.
    let _ = wasm_file;
}

/// Benchmark the WASM filter-all transform (every event is dropped).
/// Measures the fast-path overhead of a WASM transform that returns None.
fn bench_wasm_transform_filter_all(c: &mut Criterion) {
    let wasm_bytes = compile_wat("filter_out_all.wat");
    let wasm_file = wasm_tempfile(&wasm_bytes);

    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let mut runtime = rt.block_on(async {
        let mut r = WasmRuntime::new_with_config(WasmConfig {
            module_path: wasm_file.path().to_path_buf(),
            timeout_ms: 50,
            memory_limit_mb: 16,
        })
        .expect("create wasm runtime");
        r.init().await.expect("init wasm runtime");
        r
    });

    let event = build_event(1);

    c.bench_function("wasm_filter_all_single_event", |b| {
        b.iter(|| {
            rt.block_on(runtime.transform(black_box(&event)))
                .expect("wasm transform filter")
        })
    });

    let _ = wasm_file;
}

fn bench_wasm_suite(c: &mut Criterion) {
    bench_wasm_transform_pass_through(c);
    bench_wasm_transform_filter_all(c);
}

criterion_group!(cdc_perf, bench_full_quality_suite, bench_utility, bench_wasm_suite);
criterion_main!(cdc_perf);
