# Adapter SDK Guide

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
- `is_closed() -> Option<bool>`: closed-state hook used by conformance suite.

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
7. Wrap in `BoxedSink::new(your_sink)` when you need type-erased storage or `FanOutSinkAdapter` composition.

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
use rustcdc::{MemorySinkAdapter, Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
use serde_json::json;

async fn validate_adapter() -> rustcdc::Result<()> {
    let fixture = AdapterGoldenFixture::single_event(Event {
        before: None,
        after: Some(json!({"id": 1})),
        op: Operation::Insert,
        source: SourceMetadata {
            source_name: "test".into(),
            offset: "1".into(),
            timestamp: 1,
        },
        ts: 1,
        schema: Some("public".into()),
        table: "items".into(),
        primary_key: Some(vec!["id".into()]),
        snapshot: None,
        transaction: None,
        envelope_version: EVENT_ENVELOPE_VERSION,
        before_is_key_only: false,
    });
    let suite = AdapterConformanceSuite::new();
    let mut adapter = MemorySinkAdapter::default();

    let results = suite.run_all(&mut adapter, &fixture).await?;
    assert!(results.iter().all(|result| result.passed));
    Ok(())
}
```

## Dynamic Dispatch and Fan-Out

To store sinks heterogeneously or compose multiple sinks:

```rust,no_run
use rustcdc::sink::{BoxedSink, FanOutSinkAdapter, MemorySinkAdapter, SinkAdapter};

// Type-erase any SinkAdapter:
let boxed: BoxedSink = BoxedSink::new(MemorySinkAdapter::new("mem"));

// Fan out to multiple sinks concurrently:
let fan = FanOutSinkAdapter::new(vec![
    BoxedSink::new(MemorySinkAdapter::new("a")),
    BoxedSink::new(MemorySinkAdapter::new("b")),
]);
// fan.send(event) drives all children concurrently; latency = max(children)
```

## Notes

- The four required methods (`send`, `flush`, `close`, `name`) are the minimum viable implementation.
- All optional capability methods have no-op defaults; override only what your sink actually supports.
- Retry policies and delivery semantics are the responsibility of the embedding application.
- `MemorySinkAdapter` is exported from the crate root for convenience in tests.
