//! End-to-end coverage of the `register_source` extension point.
//!
//! # Why this file exists
//!
//! `CdcRuntime::register_source` is the crate's headline claim for third-party connectors:
//! *"everything the runtime provides — commit barrier, checkpointing, transforms, the
//! idempotency guard, health verdicts, metrics — applies unchanged."* That claim was never
//! tested end to end. The audit listed it as an open evidence gap, and a claim about an
//! extension point that nobody drives is exactly the kind that rots quietly: it holds until
//! the runtime grows a branch that assumes a built-in connector, and nothing notices.
//!
//! These tests drive a purpose-built `impl Source` through the *real* runtime — start,
//! poll, transform, acknowledge, checkpoint, restart — with no connector feature enabled
//! and no database involved. Everything asserted here is a promise the docs make to
//! somebody writing their own connector.
//!
//! `TransactionBoundaryPolicy` is exercised here too, for the same reason: its trimming
//! logic was unit-tested against the delivery queue directly, never against a source
//! actually emitting transactions through the runtime.

use async_trait::async_trait;
use rustcdc::checkpoint::{Checkpoint, FileCheckpoint, InMemoryCheckpoint};
use rustcdc::schema_history::InMemorySchemaHistory;
use rustcdc::source::{
    ConnectorCapabilities, HandoffResult, SnapshotEnd, SnapshotHandle, Source, StreamHandle,
};
use rustcdc::{
    CdcRuntime, Event, Offset, Operation, Result, RuntimeConfig, RuntimeOptions,
    RuntimeSourceConfig, SourceMetadata, TransactionBoundaryPolicy, TransactionMetadata,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

// ─── A minimal but honest custom source ───────────────────────────────────────

/// Offset for the toy source: a monotonically increasing sequence number.
#[derive(Debug, Clone)]
struct SeqOffset(u64);

impl Offset for SeqOffset {
    fn source_type(&self) -> &str {
        "toy"
    }
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.0.to_string().into_bytes())
    }
}

/// Stream handle that hands out pre-programmed batches and records its position.
struct ToyStream {
    batches: Mutex<std::collections::VecDeque<Vec<Event>>>,
    /// Highest sequence number handed to the caller — the durable position.
    position: Arc<Mutex<u64>>,
    /// Counts `next_events` calls, so a test can prove the runtime actually polled.
    polls: Arc<AtomicUsize>,
}

#[async_trait]
impl StreamHandle for ToyStream {
    async fn next_events(&mut self, _timeout_ms: u64) -> Result<Vec<Event>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        let batch = self
            .batches
            .lock()
            .expect("batches lock")
            .pop_front()
            .unwrap_or_default();
        if let Some(last) = batch.last() {
            let seq: u64 = last.source.offset.parse().unwrap_or_default();
            *self.position.lock().expect("position lock") = seq;
        }
        Ok(batch)
    }

    async fn save_position(&self, checkpoint: &mut dyn Checkpoint) -> Result<()> {
        let Some(offset) = self.position_offset() else {
            return Ok(());
        };
        checkpoint.save(offset.as_ref(), 0).await
    }

    fn position_offset(&self) -> Option<Box<dyn Offset>> {
        Some(Box::new(SeqOffset(
            *self.position.lock().expect("position lock"),
        )))
    }

    async fn confirm_lsn(&mut self, _lsn: u64) -> Result<()> {
        Ok(())
    }
}

/// Snapshot handle that yields nothing — this source streams only.
struct EmptySnapshot;

#[async_trait]
impl SnapshotHandle for EmptySnapshot {
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

/// A source the crate does not ship, driven through `register_source`.
struct ToySource {
    batches: Vec<Vec<Event>>,
    position: Arc<Mutex<u64>>,
    polls: Arc<AtomicUsize>,
    connected: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    /// Sequence the runtime asked to resume from, recorded so a restart can be asserted.
    resumed_from: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl Source for ToySource {
    async fn start_snapshot(&mut self, _tables: &[&str]) -> Result<Box<dyn SnapshotHandle>> {
        Ok(Box::new(EmptySnapshot))
    }

    async fn start_stream(
        &mut self,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>> {
        *self.resumed_from.lock().expect("resume lock") = resume_from
            .and_then(|offset| offset.encode().ok())
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned());
        Ok(Box::new(ToyStream {
            batches: Mutex::new(self.batches.clone().into()),
            position: Arc::clone(&self.position),
            polls: Arc::clone(&self.polls),
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
        "toy"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        // `#[non_exhaustive]` means a third-party connector cannot write a struct literal
        // here; the `with_*` builders are the only way to express a capability set from
        // outside this crate. Before they existed, `none()` was the only reachable value.
        ConnectorCapabilities::none()
            .with_tls(true)
            .with_schema_introspection(true)
    }

    async fn connect(&self) -> Result<()> {
        self.connected.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn close(&self) {
        self.closed.fetch_add(1, Ordering::Relaxed);
    }
}

fn event(seq: u64, tx: Option<(u64, u32, Option<u32>)>) -> Event {
    let mut builder = Event::builder("widgets", Operation::Insert)
        .source(SourceMetadata::new(
            "toy",
            seq.to_string(),
            1_700_000_000 + seq,
        ))
        .schema("public")
        .after(serde_json::json!({ "id": seq }))
        .primary_key(["id"])
        .ts(1_700_000_000 + seq);
    if let Some((tx_id, index, total)) = tx {
        builder = builder.transaction(TransactionMetadata::new(tx_id, index, total));
    }
    builder.build()
}

struct Harness {
    polls: Arc<AtomicUsize>,
    connected: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    resumed_from: Arc<Mutex<Option<String>>>,
}

fn toy_source(batches: Vec<Vec<Event>>) -> (ToySource, Harness) {
    let harness = Harness {
        polls: Arc::new(AtomicUsize::new(0)),
        connected: Arc::new(AtomicUsize::new(0)),
        closed: Arc::new(AtomicUsize::new(0)),
        resumed_from: Arc::new(Mutex::new(None)),
    };
    let source = ToySource {
        batches,
        position: Arc::new(Mutex::new(0)),
        polls: Arc::clone(&harness.polls),
        connected: Arc::clone(&harness.connected),
        closed: Arc::clone(&harness.closed),
        resumed_from: Arc::clone(&harness.resumed_from),
    };
    (source, harness)
}

/// Drain the runtime, acknowledging every batch, until it goes quiet.
async fn drain(runtime: &mut CdcRuntime) -> Result<Vec<Event>> {
    let mut collected = Vec::new();
    let mut quiet = 0;
    for _ in 0..40 {
        let batch = runtime.poll_event_batch().await?;
        if batch.is_empty() {
            quiet += 1;
            if quiet == 3 {
                break;
            }
        } else {
            quiet = 0;
            collected.extend(batch.events().to_vec());
        }
        runtime.commit_ack(batch.ack_mode()).await?;
    }
    Ok(collected)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// The runtime drives a source it has never heard of: connect, poll, ack, stop.
#[tokio::test]
async fn the_runtime_drives_a_registered_custom_source() -> Result<()> {
    let (source, harness) = toy_source(vec![
        vec![event(1, None), event(2, None)],
        vec![event(3, None)],
    ]);

    let config = RuntimeConfig::new(
        RuntimeSourceConfig::Disabled,
        InMemoryCheckpoint::default(),
        InMemorySchemaHistory::default(),
    );
    let mut runtime = CdcRuntime::new(config)?;
    runtime.register_source(Box::new(source));
    runtime.start().await?;

    let events = drain(&mut runtime).await?;
    runtime.stop().await?;

    assert_eq!(events.len(), 3, "every emitted event must reach the caller");
    assert_eq!(
        events
            .iter()
            .map(|event| event.source.offset.as_str())
            .collect::<Vec<_>>(),
        ["1", "2", "3"],
        "order must be preserved across batch boundaries"
    );
    assert!(
        harness.polls.load(Ordering::Relaxed) > 0,
        "the runtime must actually have polled the custom source"
    );
    assert_eq!(
        harness.connected.load(Ordering::Relaxed),
        1,
        "`Source::connect` must be driven by `start()` — a custom source cannot open its \
         own connection any other way"
    );
    assert_eq!(
        harness.closed.load(Ordering::Relaxed),
        1,
        "`Source::close` must be driven by `stop()`, or a custom source leaks its resources"
    );
    Ok(())
}

/// A restart resumes from the checkpoint the custom source's offset produced.
///
/// The runtime persists `position_offset()` for a source it does not know. If it did not,
/// a restart would re-read from the beginning — the uncontrolled-duplication failure mode.
#[tokio::test]
async fn a_restart_resumes_a_custom_source_from_its_persisted_offset() -> Result<()> {
    let directory = tempfile::tempdir().expect("tempdir");

    // First run: consume two events and acknowledge them.
    {
        let (source, _) = toy_source(vec![vec![event(1, None), event(2, None)]]);
        let config = RuntimeConfig::new(
            RuntimeSourceConfig::Disabled,
            FileCheckpoint::new(directory.path()),
            InMemorySchemaHistory::default(),
        );
        let mut runtime = CdcRuntime::new(config)?;
        runtime.register_source(Box::new(source));
        runtime.start().await?;
        let events = drain(&mut runtime).await?;
        assert_eq!(events.len(), 2);
        runtime.stop().await?;
    }

    // Second run: the runtime must hand the source the position it left off at.
    let (source, harness) = toy_source(vec![vec![event(3, None)]]);
    let config = RuntimeConfig::new(
        RuntimeSourceConfig::Disabled,
        FileCheckpoint::new(directory.path()),
        InMemorySchemaHistory::default(),
    );
    let mut runtime = CdcRuntime::new(config)?;
    runtime.register_source(Box::new(source));
    runtime.start().await?;
    let _ = drain(&mut runtime).await?;
    runtime.stop().await?;

    assert_eq!(
        harness.resumed_from.lock().expect("resume lock").as_deref(),
        Some("2"),
        "the runtime must resume a custom source from its own persisted offset; without \
         this every restart re-reads from the beginning"
    );
    Ok(())
}

/// Under `PreserveTransactions`, no delivered batch ends mid-transaction.
///
/// The trimming logic was unit-tested against the delivery queue directly. This drives it
/// through the runtime from a source that actually emits transaction metadata, which is
/// the only way to catch a runtime path that drops the metadata before the trim sees it.
#[tokio::test]
async fn preserve_transactions_never_delivers_a_partial_transaction() -> Result<()> {
    // Two transactions of three events each, delivered in batches that deliberately cut
    // across the boundary: 2 + 2 + 2.
    let batches = vec![
        vec![
            event(1, Some((100, 0, Some(3)))),
            event(2, Some((100, 1, Some(3)))),
        ],
        vec![
            event(3, Some((100, 2, Some(3)))),
            event(4, Some((200, 0, Some(3)))),
        ],
        vec![
            event(5, Some((200, 1, Some(3)))),
            event(6, Some((200, 2, Some(3)))),
        ],
    ];
    let (source, _) = toy_source(batches);

    let config = RuntimeConfig::new(
        RuntimeSourceConfig::Disabled,
        InMemoryCheckpoint::default(),
        InMemorySchemaHistory::default(),
    )
    .with_options(
        RuntimeOptions::new()
            .with_transaction_boundary(TransactionBoundaryPolicy::PreserveTransactions)
            .with_max_buffer_size(64),
    );
    let mut runtime = CdcRuntime::new(config)?;
    runtime.register_source(Box::new(source));
    runtime.start().await?;

    let mut delivered_total = 0usize;
    for _ in 0..40 {
        let batch = runtime.poll_event_batch().await?;
        let events = batch.events().to_vec();
        if !events.is_empty() {
            delivered_total += events.len();
            // Every non-empty batch must end on a transaction boundary: the last event of
            // a batch must be the last event of its transaction.
            let last = events.last().expect("non-empty");
            if let Some(transaction) = last.transaction.as_ref() {
                if let Some(total) = transaction.total_events {
                    assert_eq!(
                        transaction.event_index + 1,
                        total,
                        "a batch ended mid-transaction (tx {} at index {} of {}): a sink \
                         applying this batch would commit a state that never existed \
                         upstream",
                        transaction.tx_id,
                        transaction.event_index,
                        total
                    );
                }
            }
        }
        runtime.commit_ack(batch.ack_mode()).await?;
        if delivered_total >= 6 {
            break;
        }
    }
    runtime.stop().await?;

    assert_eq!(
        delivered_total, 6,
        "withholding must defer events to the next batch, never drop them"
    );
    // This test is also the wedge guard. The runtime drains its queued events before
    // polling the source, so withholding an entire batch used to mean the rest of the
    // transaction could never arrive: the same events were re-cut and re-withheld
    // forever. If that regresses, this test stops making progress and fails on the
    // delivered count rather than hanging.
    Ok(())
}

/// The default policy is allowed to split, and must still deliver everything.
///
/// The contrast matters: if `Split` also happened to preserve boundaries, the test above
/// would pass for the wrong reason.
#[tokio::test]
async fn the_default_policy_delivers_every_event_even_when_it_splits() -> Result<()> {
    let batches = vec![
        vec![
            event(1, Some((100, 0, Some(3)))),
            event(2, Some((100, 1, Some(3)))),
        ],
        vec![event(3, Some((100, 2, Some(3))))],
    ];
    let (source, _) = toy_source(batches);

    let config = RuntimeConfig::new(
        RuntimeSourceConfig::Disabled,
        InMemoryCheckpoint::default(),
        InMemorySchemaHistory::default(),
    );
    let mut runtime = CdcRuntime::new(config)?;
    runtime.register_source(Box::new(source));
    runtime.start().await?;
    let events = drain(&mut runtime).await?;
    runtime.stop().await?;

    assert_eq!(
        events.len(),
        3,
        "no event may be lost under the default policy"
    );
    Ok(())
}

/// Health verdicts apply to a custom source.
///
/// `RuntimeState` reports `Running` for both a quiet source and a dead one, so the verdict
/// is the only signal an operator can alert on — and it must be produced for a connector
/// the runtime has never heard of.
#[tokio::test]
async fn health_verdicts_are_produced_for_a_custom_source() -> Result<()> {
    let (source, _) = toy_source(vec![vec![event(1, None)]]);
    let config = RuntimeConfig::new(
        RuntimeSourceConfig::Disabled,
        InMemoryCheckpoint::default(),
        InMemorySchemaHistory::default(),
    );
    let mut runtime = CdcRuntime::new(config)?;
    runtime.register_source(Box::new(source));
    runtime.start().await?;
    let _ = drain(&mut runtime).await?;

    let snapshot = runtime.admin_snapshot();
    assert!(
        !snapshot.health.is_alertable(),
        "a source that just delivered an event must not read as stalled, got {:?}",
        snapshot.health
    );
    runtime.stop().await?;
    Ok(())
}
