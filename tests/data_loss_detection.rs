use std::collections::VecDeque;

use async_trait::async_trait;
use cdc_rs::{
    fault_injection::{CrashSimulationState, DataLossValidator, FaultInjectingSource, SourceFault},
    source::{HandoffResult, SnapshotEnd, SnapshotHandle, Source, StreamHandle},
    Event, Offset, Operation, SnapshotMetadata, SourceMetadata, TransactionMetadata,
    EVENT_ENVELOPE_VERSION,
};

#[derive(Debug, Clone)]
struct TestOffset;

impl Offset for TestOffset {
    fn source_type(&self) -> &str {
        "mock"
    }

    fn encode(&self) -> cdc_rs::Result<Vec<u8>> {
        Ok(Vec::new())
    }
}

fn event(id: u64) -> Event {
    Event {
        before: None,
        after: Some(serde_json::json!({"id": id, "payload": format!("v-{id}")})),
        op: Operation::Insert,
        source: SourceMetadata {
            source_name: "mock".into(),
            offset: id.to_string(),
            timestamp: id + 1,
        },
        ts: id + 1,
        schema: Some("public".into()),
        table: "events".into(),
        primary_key: Some(vec!["id".into()]),
        snapshot: Some(SnapshotMetadata {
            snapshot_id: "integration".into(),
            chunk_index: 0,
            is_last_chunk: false,
        }),
        transaction: Some(TransactionMetadata {
            tx_id: id / 10,
            total_events: 1,
            event_index: 0,
        }),
        envelope_version: EVENT_ENVELOPE_VERSION,
    }
}

struct NoopSnapshotHandle;

#[async_trait]
impl SnapshotHandle for NoopSnapshotHandle {
    async fn next_chunk(&mut self, _chunk_size: usize) -> cdc_rs::Result<Vec<Event>> {
        Ok(Vec::new())
    }

    async fn checkpoint(
        &self,
        _checkpoint: &mut dyn cdc_rs::checkpoint::Checkpoint,
        _committed_event_count: u64,
    ) -> cdc_rs::Result<()> {
        Ok(())
    }

    async fn finish(&mut self) -> cdc_rs::Result<SnapshotEnd> {
        Ok(SnapshotEnd { snapshot_end_ts: 0 })
    }
}

struct MockStreamHandle {
    batches: VecDeque<Vec<Event>>,
}

#[async_trait]
impl StreamHandle for MockStreamHandle {
    async fn next_events(&mut self, _timeout_ms: u64) -> cdc_rs::Result<Vec<Event>> {
        Ok(self.batches.pop_front().unwrap_or_default())
    }

    async fn save_position(
        &self,
        _checkpoint: &mut dyn cdc_rs::checkpoint::Checkpoint,
    ) -> cdc_rs::Result<()> {
        Ok(())
    }

    async fn confirm_lsn(&mut self, _lsn: u64) -> cdc_rs::Result<()> {
        Ok(())
    }
}

struct MockSource {
    stream_batches: VecDeque<Vec<Event>>,
}

#[async_trait]
impl Source for MockSource {
    async fn start_snapshot(
        &mut self,
        _tables: &[&str],
    ) -> cdc_rs::Result<Box<dyn SnapshotHandle>> {
        Ok(Box::new(NoopSnapshotHandle))
    }

    async fn start_stream(
        &mut self,
        _resume_from: Option<&dyn Offset>,
    ) -> cdc_rs::Result<Box<dyn StreamHandle>> {
        Ok(Box::new(MockStreamHandle {
            batches: self.stream_batches.clone(),
        }))
    }

    async fn perform_handoff(
        &mut self,
        _snapshot: &mut dyn SnapshotHandle,
        _stream: &mut dyn StreamHandle,
    ) -> cdc_rs::Result<HandoffResult> {
        Ok(HandoffResult {
            snapshot_end_ts: Some(0),
            stream_start_ts: Some(0),
            overlap_events_dropped: 0,
            stream_watermark_gap: None,
        })
    }

    fn source_type(&self) -> &str {
        "mock"
    }
}

fn build_source(total_events: u64, batch_size: usize) -> MockSource {
    let mut batches = VecDeque::new();
    let mut current = Vec::with_capacity(batch_size);
    for id in 0..total_events {
        current.push(event(id));
        if current.len() == batch_size {
            batches.push_back(std::mem::take(&mut current));
            current = Vec::with_capacity(batch_size);
        }
    }

    if !current.is_empty() {
        batches.push_back(current);
    }

    MockSource {
        stream_batches: batches,
    }
}

#[tokio::test]
async fn data_loss_detection_with_fault_injected_stream_100k() -> cdc_rs::Result<()> {
    let total = 100_000_u64;
    let source = build_source(total, 1000);
    let mut faulted = FaultInjectingSource::new(source);

    // Inject realistic non-loss faults: delay, duplicates, and low-rate corruption disabled.
    faulted.inject(
        SourceFault::DelayFault(std::time::Duration::from_millis(1)),
        10_000,
    );
    faulted.inject(SourceFault::DuplicateEventsFault(50), 20_000);

    let mut stream = faulted.start_stream(Some(&TestOffset)).await?;
    let mut received = Vec::new();

    loop {
        let batch = stream.next_events(100).await?;
        if batch.is_empty() {
            break;
        }
        received.extend(batch);
    }

    let report = DataLossValidator::validate(total, received)?;
    assert_eq!(report.missing_events, 0);
    assert!(report.duplicate_events >= 50);
    assert_eq!(report.corrupt_events, 0);
    Ok(())
}

#[tokio::test]
async fn data_loss_detection_catches_corruption_fault() -> cdc_rs::Result<()> {
    let source = build_source(10_000, 500);
    let mut faulted = FaultInjectingSource::new(source);
    faulted.inject(SourceFault::CorruptionFault(100), 0);

    let mut stream = faulted.start_stream(None).await?;
    let mut received = Vec::new();
    loop {
        let batch = stream.next_events(100).await?;
        if batch.is_empty() {
            break;
        }
        received.extend(batch);
    }

    let error = DataLossValidator::validate(10_000, received).unwrap_err();
    assert!(format!("{error}").contains("corrupt_events"));
    Ok(())
}

#[tokio::test]
async fn data_loss_matrix_short_10k_connection_fault_is_observable() -> cdc_rs::Result<()> {
    let source = build_source(10_000, 500);
    let mut faulted = FaultInjectingSource::new(source);
    faulted.inject(
        SourceFault::ConnectionFault(cdc_rs::Error::SourceError(
            "simulated connection drop".into(),
        )),
        2_500,
    );

    let mut stream = faulted.start_stream(None).await?;
    loop {
        match stream.next_events(100).await {
            Ok(batch) if batch.is_empty() => break,
            Ok(_) => continue,
            Err(err) => {
                assert!(format!("{err}").contains("connection fault"));
                return Ok(());
            }
        }
    }

    Err(cdc_rs::Error::SourceError(
        "expected connection fault was not triggered".into(),
    ))
}

#[tokio::test]
async fn data_loss_matrix_short_10k_timeout_fault_is_observable() -> cdc_rs::Result<()> {
    let source = build_source(10_000, 500);
    let mut faulted = FaultInjectingSource::new(source);
    faulted.inject(
        SourceFault::TimeoutFault(std::time::Duration::from_millis(1)),
        1_000,
    );

    let mut stream = faulted.start_stream(None).await?;
    loop {
        match stream.next_events(100).await {
            Ok(batch) if batch.is_empty() => break,
            Ok(_) => continue,
            Err(err) => {
                assert!(format!("{err}").contains("timed out"));
                return Ok(());
            }
        }
    }

    Err(cdc_rs::Error::TimeoutError(
        "expected timeout fault was not triggered".into(),
    ))
}

#[test]
fn data_loss_matrix_short_10k_crash_recovery_has_no_missing_events() {
    let total = 10_000_u64;
    let crash_state = CrashSimulationState::new(vec![2_000, 4_000, 6_000, 8_000, 9_000]);

    let mut start = 0_u64;
    for crash in [2_000_u64, 4_000, 6_000, 8_000, 9_000] {
        let batch = (start..crash).map(event).collect();
        crash_state
            .record_cycle(batch)
            .expect("cycle should record");
        start = crash;
    }
    crash_state
        .record_cycle((start..total).map(event).collect())
        .expect("final cycle should record");

    let collected = crash_state
        .get_collected_events()
        .expect("events should be readable");
    let report = DataLossValidator::validate(total, collected).expect("validation should pass");
    assert_eq!(report.missing_events, 0);
    assert_eq!(report.corrupt_events, 0);
}

#[test]
fn data_loss_matrix_long_1m_iterator_validation_no_loss() {
    let total = 1_000_000_u64;
    let report = DataLossValidator::validate_iter(total, (0..total).map(event))
        .expect("iterator validation should succeed for 1M events");

    assert_eq!(report.expected_events, total);
    assert_eq!(report.received_unique, total);
    assert_eq!(report.missing_events, 0);
    assert_eq!(report.corrupt_events, 0);
}
