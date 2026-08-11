//! End-to-end runtime throughput: poll → transform → sink → acknowledge → checkpoint.
//!
//! # Why this exists separately from `cdc_perf`
//!
//! Every other benchmark in this repository measures a *component* — the transform
//! pipeline, the JSON round trip, the snapshot validator. Each is useful and none of them
//! answers the question an operator actually asks: **how many events per second does the
//! runtime sustain?** Adding up component timings does not answer it either, because the
//! commit path is not a component: it is an `fsync` per acknowledged batch, and its cost
//! per event depends entirely on how many events share a batch.
//!
//! That gap was the last open evidence condition on a 1.0 release. This closes it with a
//! number that is reproducible on any machine with `cargo bench --bench throughput`.
//!
//! # What the number does and does not include
//!
//! **Included:** the whole library path — source poll, the idempotency guard, the
//! transform pipeline, delivery buffering, the sink, the ack token, the commit barrier
//! and the durable checkpoint write.
//!
//! **Excluded:** database I/O. The source is synthetic, so the figure is the runtime's own
//! ceiling — what the library costs on top of whatever the database and the sink cost. A
//! connector-inclusive number is a property of the server, the network and the schema, and
//! reporting one measured against a container on a laptop would be evidence of nothing.
//!
//! # Reading the results
//!
//! The `in_memory_checkpoint` line is the runtime's CPU ceiling. The `file_checkpoint`
//! lines are the honest production shape: `FileCheckpoint` fsyncs the record and then
//! fsyncs the directory, twice per acknowledged batch, so the batch size — not the event
//! rate — is what moves that number. The two batch sizes are there to make the trade
//! visible rather than to be quoted in isolation.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use rustcdc::checkpoint::{Checkpoint, FileCheckpoint, InMemoryCheckpoint};
use rustcdc::schema_history::InMemorySchemaHistory;
use rustcdc::sink::{SinkAdapter, SinkDeliveryGuarantee};
use rustcdc::source::{
    ConnectorCapabilities, HandoffResult, SnapshotEnd, SnapshotHandle, Source, StreamHandle,
};
use rustcdc::{
    CdcRuntime, Event, Offset, Operation, Result, RuntimeConfig, RuntimeOptions,
    RuntimeSourceConfig, SourceMetadata,
};

// ─── A source that costs as little as possible ────────────────────────────────
//
// The point is to measure the runtime, so the source must not dominate. It hands out
// pre-built batches cloned from a shared template: one `Vec` clone plus the per-event
// `serde_json::Value` clones, which is the same shape a real connector produces after
// decoding and therefore not an unfair head start.

#[derive(Debug, Clone)]
struct SeqOffset(u64);

impl Offset for SeqOffset {
    fn source_type(&self) -> &str {
        "bench"
    }
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.0.to_string().into_bytes())
    }
}

struct BenchStream {
    template: Arc<Vec<Event>>,
    next_seq: u64,
    position: Arc<Mutex<u64>>,
}

#[async_trait]
impl StreamHandle for BenchStream {
    async fn next_events(&mut self, _timeout_ms: u64) -> Result<Vec<Event>> {
        let base = self.next_seq;
        self.next_seq += self.template.len() as u64;
        let batch: Vec<Event> = self
            .template
            .iter()
            .enumerate()
            .map(|(index, event)| {
                let mut event = event.clone();
                event.source.offset = (base + index as u64).to_string();
                event
            })
            .collect();
        *self.position.lock().expect("position") = self.next_seq;
        Ok(batch)
    }

    async fn save_position(&self, checkpoint: &mut dyn Checkpoint) -> Result<()> {
        let position = *self.position.lock().expect("position");
        checkpoint.save(&SeqOffset(position), 0).await
    }

    fn position_offset(&self) -> Option<Box<dyn Offset>> {
        Some(Box::new(SeqOffset(
            *self.position.lock().expect("position"),
        )))
    }

    async fn confirm_lsn(&mut self, _lsn: u64) -> Result<()> {
        Ok(())
    }
}

struct NoSnapshot;

#[async_trait]
impl SnapshotHandle for NoSnapshot {
    async fn next_chunk(&mut self, _chunk_size: usize) -> Result<Vec<Event>> {
        Ok(Vec::new())
    }
    async fn checkpoint(&self, _checkpoint: &mut dyn Checkpoint, _committed: u64) -> Result<()> {
        Ok(())
    }
    async fn finish(&mut self) -> Result<SnapshotEnd> {
        Ok(SnapshotEnd { snapshot_end_ts: 0 })
    }
}

struct BenchSource {
    template: Arc<Vec<Event>>,
}

#[async_trait]
impl Source for BenchSource {
    async fn start_snapshot(&mut self, _tables: &[&str]) -> Result<Box<dyn SnapshotHandle>> {
        Ok(Box::new(NoSnapshot))
    }

    async fn start_stream(
        &mut self,
        _resume: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>> {
        Ok(Box::new(BenchStream {
            template: Arc::clone(&self.template),
            next_seq: 0,
            position: Arc::new(Mutex::new(0)),
        }))
    }

    async fn perform_handoff(
        &mut self,
        _snapshot: &mut dyn SnapshotHandle,
        _stream: &mut dyn StreamHandle,
    ) -> Result<HandoffResult> {
        Ok(HandoffResult::default())
    }

    fn source_type(&self) -> &str {
        "bench"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::none()
    }

    async fn connect(&self) -> Result<()> {
        Ok(())
    }

    async fn close(&self) {}
}

/// A sink that accepts and discards, so the measurement is not a memory benchmark.
#[derive(Debug, Default)]
struct CountingSink {
    received: u64,
}

impl SinkAdapter for CountingSink {
    fn name(&self) -> &str {
        "counting"
    }

    fn delivery_guarantee(&self) -> SinkDeliveryGuarantee {
        SinkDeliveryGuarantee::AtLeastOnce
    }

    async fn send(&mut self, _event: &Event) -> Result<()> {
        self.received += 1;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

// ─── Harness ──────────────────────────────────────────────────────────────────

/// One event shaped like a real row: a handful of text columns and a primary key.
fn template_batch(batch_size: usize) -> Arc<Vec<Event>> {
    Arc::new(
        (0..batch_size)
            .map(|index| {
                Event::builder("orders", Operation::Insert)
                    .schema("public")
                    .source(SourceMetadata::new(
                        "bench",
                        index.to_string(),
                        1_700_000_000,
                    ))
                    .after(serde_json::json!({
                        "id": index.to_string(),
                        "customer": "acme-corporation",
                        "amount": "1234.56",
                        "currency": "EUR",
                        "status": "confirmed",
                        "created_at": "2026-08-11T09:15:00Z",
                    }))
                    .primary_key(["id"])
                    .ts(1_700_000_000)
                    .build()
            })
            .collect(),
    )
}

enum Store {
    Memory,
    File,
}

/// Drive `batches` polls end to end, acknowledging each one, and return events committed.
async fn run_pipeline(template: Arc<Vec<Event>>, batches: usize, store: &Store) -> u64 {
    let temp = matches!(store, Store::File).then(|| tempfile::tempdir().expect("tempdir"));

    // Large enough that a batch is delivered whole, so the benchmark measures one
    // commit per poll rather than the buffer's re-cutting behaviour.
    let options = RuntimeOptions::new().with_max_buffer_size(template.len().max(1));

    let config = match &temp {
        Some(dir) => RuntimeConfig::new(
            RuntimeSourceConfig::Disabled,
            FileCheckpoint::new(dir.path()),
            InMemorySchemaHistory::default(),
        ),
        None => RuntimeConfig::new(
            RuntimeSourceConfig::Disabled,
            InMemoryCheckpoint::default(),
            InMemorySchemaHistory::default(),
        ),
    }
    .with_options(options);

    let mut runtime = CdcRuntime::new(config).expect("runtime builds");
    runtime.register_source(Box::new(BenchSource { template }));
    runtime.register_sink(CountingSink::default());
    runtime.start().await.expect("runtime starts");

    let mut committed = 0u64;
    for _ in 0..batches {
        let batch = runtime.poll_event_batch().await.expect("poll");
        committed += batch.events().len() as u64;
        runtime.commit_ack(batch.ack_mode()).await.expect("ack");
    }
    runtime.stop().await.expect("runtime stops");
    committed
}

fn bench_runtime_throughput(c: &mut Criterion) {
    let tokio = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("throughput/runtime_end_to_end");
    // The commit path dominates, so wall clock per iteration is high relative to the
    // component benchmarks; a shorter measurement window keeps the suite usable in CI.
    group.measurement_time(Duration::from_secs(10));

    for batch_size in [64usize, 1024] {
        let batches = 32;
        let events = (batch_size * batches) as u64;
        let template = template_batch(batch_size);

        group.throughput(Throughput::Elements(events));
        group.bench_function(format!("in_memory_checkpoint/batch_{batch_size}"), |b| {
            b.iter(|| {
                let template = Arc::clone(&template);
                let committed = tokio.block_on(run_pipeline(template, batches, &Store::Memory));
                assert_eq!(committed, events, "every event must be committed");
            });
        });

        group.bench_function(format!("file_checkpoint/batch_{batch_size}"), |b| {
            b.iter(|| {
                let template = Arc::clone(&template);
                let committed = tokio.block_on(run_pipeline(template, batches, &Store::File));
                assert_eq!(committed, events, "every event must be committed");
            });
        });
    }

    group.finish();
}

criterion_group!(throughput, bench_runtime_throughput);
criterion_main!(throughput);
