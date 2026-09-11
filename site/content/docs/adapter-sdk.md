+++
title = "Sink adapter SDK"
description = "Build a custom sink adapter for rustcdc, and verify it against the conformance suite."
weight = 60
+++

## Contract

`SinkAdapter` is an async trait for adapter development. It uses native `async fn in traits`
(RPITIT, stabilised in Rust 1.75). **No `#[async_trait]` macro is required** in implementations —
just write plain `async fn` methods.

Required methods:
- `send(&mut self, event: &Event) -> Result<()>`: accept one canonical event.
- `flush(&mut self) -> Result<()>`: durably flush buffered sends.
- `close(&mut self) -> Result<()>`: graceful shutdown.
- `name(&self) -> &str`: stable adapter identifier.

Optional capability methods (all default to no-op / `false`):
- `delivery_guarantee() -> SinkDeliveryGuarantee`: advertise `AtLeastOnce`, `AtLeastOnceIdempotent`, or `EffectivelyOnce`.
- `idempotent_delivery_capable() -> bool`: whether retrying the same event is idempotent.
- `transactional_checkpoint_barrier_capable() -> bool`: whether the adapter implements the full barrier protocol.
- `queue_depth() -> Option<usize>`: current in-memory buffer depth for back-pressure observation.
- `flush_tick_interval() -> Option<Duration>`: hint for periodic time-based flushing.
- `begin_checkpoint_barrier / commit_checkpoint_barrier / abort_checkpoint_barrier`: two-phase-commit-style barrier protocol for effectively-once delivery.
- `preflight_check()`: connectivity validation called once at startup.
- `close_with_timeout(ms)`: pre-built graceful close with timeout (no override needed).
- `exported_events() -> Option<&[Event]>`: for in-memory/test adapters; enables conformance assertions.
- `delivery_metrics() -> Option<SinkDeliveryMetrics>`: snapshot of delivery counters (`events_sent`, `events_errored`, `events_retried`, `last_delivered_offset`) for admin endpoints and Prometheus scrapers. `SinkDeliveryMetrics::merge()` is provided for composed sinks.
- `is_closed() -> bool`: closed-state hook used by conformance suite; defaults to `false`.

Behavioral expectations:
- Preserve event ordering per `send` call sequence.
- Return structured errors instead of panicking.
- Treat `flush` as a durability boundary.
- Reject sends after `close` with a state error.

## Implementation Guide

1. Implement a concrete sink type that stores connection/client state.
2. Implement `SinkAdapter` on that type (only `send`, `flush`, `close`, `name` are required).
3. Ensure idempotent `close` behavior.
4. Redact credentials from logs and debug output.
5. Surface recoverable versus unrecoverable errors using `rustcdc::Error` variants.
6. Override `delivery_guarantee` if your sink can offer stronger-than-default guarantees.
7. Override `delivery_metrics` to expose per-sink counters to admin/observability endpoints.
8. Use `your_sink.boxed()` (or `BoxedSink::new(your_sink)`) when you need type-erased storage or `FanOutSinkAdapter` / `TableRouter<BoxedSink>` composition.

Recommended pattern:
- `send`: enqueue and optionally batch.
- `flush`: commit batch to external sink.
- `close`: call `flush`, then release resources.

## Conformance Instructions

Use `AdapterGoldenFixture`, `BasicAdapterConformance`, and `AdapterConformanceSuite` from `testkit`:

- `single_event`: validates basic acceptance path.
- `batch_send`: validates batch handling and flush behavior.
- `ordering`: validates stable send order.
- `crash_recovery`: validates close semantics after sends.

Minimum validation loop:
1. Build fixture sequences with `AdapterGoldenFixture::{single_event,batch,ordering,crash_recovery}`.
2. Run `AdapterConformanceSuite::run_all()` against your adapter and fixture.
3. Assert all returned `TestResult` values are `passed = true`.
4. Add adapter-specific fault tests (network loss, sink timeout, partial flush failure).

Quick smoke harness:

```rust
use rustcdc::testkit::{AdapterConformanceSuite, AdapterGoldenFixture};
use rustcdc::{Event, MemorySinkAdapter, Operation, SourceMetadata};
use serde_json::json;

async fn validate_adapter() -> rustcdc::Result<()> {
    // Build from `Event::default()` and override what matters. A full struct literal
    // breaks every time the envelope gains a field, which is exactly how this sample
    // rotted before the Markdown was wired into the doctest run.
    let fixture = AdapterGoldenFixture::single_event(
        Event::builder("items", Operation::Insert)
            .after(json!({"id": 1}))
            .source(SourceMetadata::new("test", "1", 1))
            .ts(1)
            .schema("public")
            .primary_key(["id"])
            .build(),
    );
    let suite = AdapterConformanceSuite::new();
    let mut adapter = MemorySinkAdapter::default();

    let results = suite.run_all(&mut adapter, &fixture).await?;
    assert!(results.iter().all(|result| result.passed));
    Ok(())
}
```

## Dynamic Dispatch, Fan-Out, and Routing

To store sinks heterogeneously or compose multiple sinks:

```rust,no_run
use rustcdc::sink::{BoxedSink, FanOutSinkAdapter, MemorySinkAdapter, SinkAdapter};
use rustcdc::pipeline::HeterogeneousTableRouter;

// Type-erase any SinkAdapter (two equivalent spellings):
let boxed: BoxedSink = MemorySinkAdapter::new("mem").boxed();
let boxed2: BoxedSink = BoxedSink::new(MemorySinkAdapter::new("mem2"));

// Fan out to multiple sinks concurrently:
// An empty child list is refused: a fan-out with no children accepts every event,
// delivers none, and reports success.
let fan = FanOutSinkAdapter::new(vec![
    MemorySinkAdapter::new("a").boxed(),
    MemorySinkAdapter::new("b").boxed(),
])
.expect("at least one child");
// fan.send(event) drives all children concurrently; latency = max(children)

// Route events by table name to heterogeneous sinks:
let router = HeterogeneousTableRouter::builder("demo")
    .route("public.orders", MemorySinkAdapter::new("orders").boxed())
    .route("public.audit", MemorySinkAdapter::new("audit").boxed())
    .default(MemorySinkAdapter::new("fallback").boxed())
    .build()
    .expect("valid patterns");
```

## Notes

- The four required methods (`send`, `flush`, `close`, `name`) are the minimum viable implementation.
- All optional capability methods have no-op defaults; override only what your sink actually supports.
- Retry policies and delivery semantics are the responsibility of the embedding application.
- `MemorySinkAdapter` is exported from the crate root for convenience in tests.

## EffectivelyOnce Delivery: Barrier Protocol

`SinkDeliveryGuarantee::EffectivelyOnce` is the strongest guarantee available. It requires
the sink to participate in a **two-phase checkpoint barrier** so that the runtime and the sink
advance their durable state atomically — no event is lost on crash, and no event is emitted
twice to the downstream system (assuming the downstream is a transactional target such as a
database or a Kafka topic with transactions enabled).

### Responsibility split

> **Important:** The runtime does **not** call `begin_checkpoint_barrier`, `commit_checkpoint_barrier`,
> or `abort_checkpoint_barrier` automatically. No runtime code drives the barrier.
> The **embedder** must call these methods explicitly around the commit path.
> The barrier methods are pure hooks on the `SinkAdapter` trait — they exist so the embedder
> can drive them in a coordinated sequence with `commit_ack`.

The **embedder** is responsible for:
- Calling `sink.begin_checkpoint_barrier()` before the commit window opens.
- Calling `sink.commit_checkpoint_barrier()` after `runtime.commit_ack(...)` succeeds.
- Calling `sink.abort_checkpoint_barrier()` on any error in the commit window.

The **sink** is responsible for:
- Opening a transaction (or writing to a transactional staging area) on `begin_checkpoint_barrier`.
- Making all `send()` calls part of that transaction during the window.
- Committing the transaction atomically with the checkpoint on `commit_checkpoint_barrier`.
- Rolling back on `abort_checkpoint_barrier`.

### Minimal implementation skeleton

```rust,no_run
use rustcdc::sink::{SinkAdapter, SinkDeliveryGuarantee};
use rustcdc::{Event, Result};

/// A sink that writes to a transactional target and participates in the
/// checkpoint barrier protocol to provide EffectivelyOnce delivery.
pub struct TransactionalSink {
    name: String,
    pending: Vec<Event>,
    in_barrier: bool,
    closed: bool,
}

impl TransactionalSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            pending: Vec::new(),
            in_barrier: false,
            closed: false,
        }
    }

    async fn begin_transaction(&mut self) -> Result<()> {
        // Open a database transaction, Kafka transaction, etc.
        self.in_barrier = true;
        Ok(())
    }

    async fn commit_transaction(&mut self) -> Result<()> {
        // Commit all pending events atomically with the checkpoint.
        self.pending.clear();
        self.in_barrier = false;
        Ok(())
    }

    async fn rollback_transaction(&mut self) -> Result<()> {
        self.pending.clear();
        self.in_barrier = false;
        Ok(())
    }
}

impl SinkAdapter for TransactionalSink {
    async fn send(&mut self, event: &Event) -> Result<()> {
        // Buffer into the open transaction.
        self.pending.push(event.clone());
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        // Flush pending events within the current transaction (no commit yet).
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.in_barrier {
            self.rollback_transaction().await?;
        }
        self.closed = true;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn delivery_guarantee(&self) -> SinkDeliveryGuarantee {
        SinkDeliveryGuarantee::EffectivelyOnce
    }

    fn transactional_checkpoint_barrier_capable(&self) -> bool {
        true
    }

    fn is_closed(&self) -> bool {
        self.closed
    }

    async fn begin_checkpoint_barrier(&mut self) -> Result<()> {
        self.begin_transaction().await
    }

    async fn commit_checkpoint_barrier(&mut self) -> Result<()> {
        self.commit_transaction().await
    }

    async fn abort_checkpoint_barrier(&mut self) -> Result<()> {
        self.rollback_transaction().await
    }
}
```

### Embedder-side barrier orchestration

```rust,no_run
# use rustcdc::{CdcRuntime, sink::SinkAdapter};
async fn run(mut runtime: CdcRuntime, mut sink: impl SinkAdapter) -> rustcdc::Result<()> {
    loop {
        let batch = runtime.poll_event_batch().await?;
        if batch.is_empty() {
            break;
        }
        // Send all events in the batch to the sink.
        for event in batch.events() {
            sink.send(event).await?;
        }
        // Open the barrier — sink begins its transaction.
        sink.begin_checkpoint_barrier().await?;
        // Commit the checkpoint durably.
        match runtime.commit_ack(batch.ack_mode()).await {
            Ok(_) => {
                // Checkpoint is durable — commit the sink transaction.
                sink.commit_checkpoint_barrier().await?;
            }
            Err(e) => {
                // Checkpoint failed — roll back the sink transaction.
                sink.abort_checkpoint_barrier().await?;
                return Err(e);
            }
        }
    }
    sink.close().await?;
    Ok(())
}
```
