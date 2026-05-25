# Adapter SDK Guide

## Contract

`SinkAdapter` is an async trait exposed from `testkit` for adapter development.

Required methods:
- `send(&mut self, event: &Event) -> Result<()>`: accept one canonical event.
- `flush(&mut self) -> Result<()>`: durably flush buffered sends.
- `close(&mut self) -> Result<()>`: graceful shutdown.
- `name(&self) -> &str`: stable adapter identifier.

Behavioral expectations:
- Preserve event ordering per `send` call sequence.
- Return structured errors instead of panicking.
- Treat `flush` as a durability boundary.
- Reject sends after `close` with a state error.

## Implementation Guide

1. Implement a concrete sink type that stores connection/client state.
2. Implement `SinkAdapter` on that type.
3. Ensure idempotent `close` behavior.
4. Redact credentials from logs and debug output.
5. Surface recoverable versus unrecoverable errors using `rustcdc::Error` variants.

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
use rustcdc::testkit::{AdapterConformanceSuite, AdapterGoldenFixture, MemorySinkAdapter};
use rustcdc::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
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
	});
	let suite = AdapterConformanceSuite::new();
	let mut adapter = MemorySinkAdapter::default();

	let results = suite.run_all(&mut adapter, &fixture).await?;
	assert!(results.iter().all(|result| result.passed));
	Ok(())
}
```

## Notes

- The adapter surface is intentionally narrow to keep the contract stable and testable.
- Retry policies and delivery semantics are the responsibility of the embedding application.
