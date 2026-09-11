//! Pipeline throughput — the number this project's premise rests on.
//!
//! # Why this exists
//!
//! `benches/pipeline.rs` benchmarks encoding, event construction and size checks. It does
//! not benchmark the pipeline. So the claim that a Rust CDC server beats a JVM one was
//! unfalsifiable, and the one performance regression this project has actually suffered —
//! the Kafka sink awaiting each acknowledgement inside `send_encoded`, 35× worse at
//! `linger_ms = 5` — was found by reading code rather than by measuring.
//!
//! This drives the **real** `process_batch_events` (hence its `pub`), so what is measured
//! is what runs.
//!
//! # What it is designed to answer
//!
//! A review hypothesised that the transform stage dominates a no-op pipeline, because
//! `TransformPipeline::apply` clones several owned `String`s and builds a metadata map per
//! event *regardless of whether any rule matches*. At a measured JSON encode cost of
//! 0.56 µs/event, a handful of small allocations per event is plausibly the same order as
//! the encode itself. The `transform/*` group exists to settle that, and the
//! `pipeline/parallelism_*` group exists to show whether `prepare_parallelism` buys
//! anything once the sink is in the picture.
//!
//! # Reading the numbers
//!
//! Every group reports `Throughput::Elements`, so criterion prints events/second directly.
//! Take a baseline before changing anything:
//!
//! ```text
//! cargo bench --bench throughput -- --save-baseline main
//! # …make a change…
//! cargo bench --bench throughput -- --baseline main
//! ```
//!
//! Numbers are hardware-specific; see `benches/BASELINES.md`. The gate that matters is the
//! *ratio* to a baseline taken on the same machine, not the absolute value.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustcdc::core::{Event, Operation, SourceMetadata};
use rustcdc_server::config::schema::{SinkConfig, TransformRuleConfig};
use rustcdc_server::pipeline::transform::TransformPipeline;

/// A representative `Update`: before + after, five columns, ~520 B encoded.
///
/// Deliberately the same shape as `benches/pipeline.rs` uses, so the encode cost measured
/// there is subtractable from the figures here.
fn representative_event(id: u64) -> Event {
    Event::builder("orders", Operation::Update)
        .before(serde_json::json!({
            "id": id,
            "customer_id": id % 1000,
            "status": "pending",
            "total_cents": 1234 + id,
            "note": "a representative free-text column of moderate length",
        }))
        .after(serde_json::json!({
            "id": id,
            "customer_id": id % 1000,
            "status": "shipped",
            "total_cents": 1234 + id,
            "note": "a representative free-text column of moderate length",
        }))
        .source(SourceMetadata::new(
            "postgres",
            format!("0/{:08X}", 0x1000000 + id),
            id,
        ))
        .ts(1_700_000_000_000 + id)
        .schema("public")
        .primary_key(["id"])
        .build()
}

fn events(count: u64) -> Vec<Event> {
    (0..count).map(representative_event).collect()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("bench runtime")
}

/// A mask rule that actually matches, so the "with a rule" figure includes rule execution
/// rather than only the matcher's rejection path.
fn mask_rule() -> TransformRuleConfig {
    serde_json::from_value(serde_json::json!({
        "name": "bench_mask",
        "when": { "tables": ["orders"] },
        "actions": [{
            "type": "mask",
            "rules": { "note": { "type": "redact", "placeholder": "***" } }
        }]
    }))
    .expect("bench mask rule")
}

/// Isolates the transform stage: the hypothesis is that a pipeline with **no rules at all**
/// is not free, because the per-event work happens before any rule is consulted.
///
/// If `no_rules` is close to `one_mask_rule`, the fixed per-event cost dominates and the
/// allocation work is worth removing. If it is far cheaper, the matcher is doing its job
/// and optimisation effort belongs elsewhere.
fn bench_transform_stage(c: &mut Criterion) {
    const EVENTS: u64 = 1_000;

    let rt = runtime();
    let mut group = c.benchmark_group("transform");
    group.throughput(Throughput::Elements(EVENTS));

    for (label, rules) in [
        ("no_rules", Vec::new()),
        ("one_mask_rule", vec![mask_rule()]),
    ] {
        let pipeline = TransformPipeline::from_config(Default::default(), rules)
            .expect("transform pipeline builds");
        let batch = events(EVENTS);

        group.bench_function(label, |b| {
            // `iter_batched` so the `Vec<Event>` clone happens in **setup**, outside the
            // measured region. The first version of this benchmark cloned inside the
            // loop and reported 0.61 µs/event for a pipeline with zero rules — which was
            // `Event::clone`, not the transform. `benches/pipeline.rs` measures event
            // construction at 0.62 µs/event, so the two figures agreeing was the tell.
            b.to_async(&rt).iter_batched(
                || batch.clone(),
                |batch| async {
                    for event in batch {
                        let out = pipeline.apply(event).await.expect("transform");
                        black_box(out);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// The whole batch path: transform → prepare → deliver → flush, through the real
/// `process_batch_events`.
///
/// The sink is `file_jsonl` into a temp directory. That is a real sink with real
/// serialisation and real writes rather than a null adapter, which is the point: a
/// benchmark against a no-op sink measures a pipeline nobody deploys. It does mean the
/// figure includes filesystem cost, so compare ratios across `prepare_parallelism`, not the
/// absolute value against some other system.
fn bench_pipeline_batch(c: &mut Criterion) {
    const EVENTS: u64 = 1_000;

    let rt = runtime();
    let mut group = c.benchmark_group("pipeline");
    group.throughput(Throughput::Elements(EVENTS));
    group.sample_size(20);

    let transform =
        TransformPipeline::from_config(Default::default(), Vec::new()).expect("pipeline builds");

    for parallelism in [1usize, 4, 16] {
        group.bench_with_input(
            BenchmarkId::new("parallelism", parallelism),
            &parallelism,
            |b, &parallelism| {
                b.to_async(&rt).iter_batched(
                    || {
                        // A fresh sink per iteration: appending to one growing file would
                        // measure the file system's behaviour at increasing sizes rather
                        // than the pipeline's.
                        let dir = tempfile::tempdir().expect("tempdir");
                        let path = dir.path().join("events.jsonl");
                        let config: SinkConfig = serde_json::from_value(serde_json::json!({
                            "type": "file_jsonl",
                            "path": path,
                        }))
                        .expect("file_jsonl sink config");
                        (dir, config, events(EVENTS))
                    },
                    |(dir, config, batch)| {
                        let transform = &transform;
                        async move {
                            let binding = rustcdc_server::sink::build_binding(&config, 1 << 20)
                                .await
                                .expect("sink binding");
                            let mut router = rustcdc_server::pipeline::router::single(binding);

                            let stats = rustcdc_server::runtime::batch::process_batch_events(
                                &mut router,
                                batch,
                                transform,
                                parallelism,
                                100,   // sink_flush_interval_events
                                1_024, // sink_delivery_queue_capacity
                                30_000,
                                30_000,
                                None,
                            )
                            .await
                            .expect("batch delivered");

                            black_box(stats);
                            drop(dir);
                        }
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_transform_stage, bench_pipeline_batch);
criterion_main!(benches);
