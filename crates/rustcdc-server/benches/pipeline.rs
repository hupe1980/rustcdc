//! Throughput baselines for the hot path.
//!
//! Until this existed there was no performance evidence of any kind, which is how a
//! discarded JSON encode survived on the send path: nothing measured throughput, and
//! the latency histograms were quantised to whole milliseconds against a lowest bucket
//! of 1 ms, so every per-event operation reported zero.
//!
//! Measure in the profile you ship. The first estimate of that discarded encode was
//! taken in a debug build and came to 13.9 µs/event; the release figure is 0.56 µs —
//! about 25× cheaper. See `BASELINES.md`.
//!
//! These are deliberately *micro*-benchmarks of the stages an event actually passes
//! through, not an end-to-end pipeline run. An end-to-end number is dominated by the
//! sink's network behaviour and is not reproducible in CI; these are.
//!
//! Run with `cargo xtask bench`. To compare against a saved baseline:
//!
//! ```text
//! cargo xtask bench -p rustcdc-server --bench pipeline -- --save-baseline main
//! # ...make a change...
//! cargo xtask bench -p rustcdc-server --bench pipeline -- --baseline main
//! ```
//!
//! Criterion reports a regression when the median moves outside its noise threshold.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustcdc::core::{Event, Operation, SourceMetadata};
use serde_json::json;
use std::hint::black_box;

/// An event shaped like a real one: before and after images, a mix of types, a
/// free-text column. Benchmarking a two-field event flatters every serialiser.
fn representative_event(id: u64) -> Event {
    Event::builder("orders", Operation::Update)
        .before(json!({
            "id": id,
            "status": "pending",
            "total_cents": 129_900,
            "customer": "customer-name-of-average-length",
            "notes": "a typical free-text column with a sentence in it",
        }))
        .after(json!({
            "id": id,
            "status": "shipped",
            "total_cents": 129_900,
            "customer": "customer-name-of-average-length",
            "notes": "a typical free-text column with a sentence in it",
        }))
        .source(SourceMetadata::new("postgres", format!("0/{id:X}"), id))
        .ts(id)
        .schema("public")
        .primary_key(["id"])
        .build()
}

fn events(count: u64) -> Vec<Event> {
    (0..count).map(representative_event).collect()
}

/// JSON encoding of a single event.
///
/// The send path used to do this **twice** per
/// event — once to measure the payload against `runtime.max_event_bytes` and then throw
/// the buffer away, once for real in the codec. Whatever this benchmark reports was,
/// until that fix, the per-event tax for doing nothing.
///
/// It was never the main point of the finding, though: the discarded encode also meant
/// the limit was enforced against a JSON rendering that an Avro or Protobuf sink never
/// transmits.
fn bench_event_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");
    let events = events(1_000);
    group.throughput(Throughput::Elements(events.len() as u64));
    group.bench_function("json_serialize", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for event in &events {
                total += serde_json::to_vec(black_box(event)).expect("encode").len();
            }
            black_box(total)
        })
    });
    group.finish();
}

/// Event construction, so the encode numbers can be read net of fixture cost.
///
/// A benchmark whose setup dominates its measurement reports the setup. This is the
/// subtrahend.
fn bench_event_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("build");
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("event_builder", |b| {
        b.iter(|| black_box(events(black_box(1_000))))
    });
    group.finish();
}

/// Size-limit enforcement across payload sizes.
///
/// The limit now measures the **encoded** payload rather than a JSON rendering the
/// transport never sends, so this tracks the cost of the check itself — which should be
/// a length comparison and nothing more.
fn bench_size_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("size_check");
    for width in [1_usize, 16, 256] {
        let events: Vec<Event> = (0..500)
            .map(|id| {
                Event::builder("wide", Operation::Insert)
                    .after(json!({
                        "id": id,
                        "payload": "x".repeat(width * 64),
                    }))
                    .source(SourceMetadata::new("postgres", id.to_string(), id))
                    .ts(id)
                    .primary_key(["id"])
                    .build()
            })
            .collect();

        group.throughput(Throughput::Elements(events.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}B", width * 64)),
            &events,
            |b, events| {
                b.iter(|| {
                    let mut over = 0usize;
                    for event in events {
                        let encoded = serde_json::to_vec(black_box(event)).expect("encode");
                        if encoded.len() > 4_096 {
                            over += 1;
                        }
                    }
                    black_box(over)
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_event_encode,
    bench_event_build,
    bench_size_check
);
criterion_main!(benches);
