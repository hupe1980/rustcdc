use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use async_trait::async_trait;

use rustcdc::{
    checkpoint::{Checkpoint, GenericOffset, InMemoryCheckpoint},
    fault_injection::{
        CheckpointFault, DataLossValidator, FaultInjectingCheckpoint, FaultInjectingSource,
        SourceFault,
    },
    source::{HandoffResult, SnapshotEnd, SnapshotHandle, Source, StreamHandle},
    Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION,
};

const SOAK_TOTAL_EVENTS: usize = 5_000;
const SOAK_BATCH_SIZE: usize = 100;

#[derive(Debug, Clone)]
struct TestOffset;

impl rustcdc::Offset for TestOffset {
    fn source_type(&self) -> &str {
        "mock"
    }

    fn encode(&self) -> rustcdc::Result<Vec<u8>> {
        Ok(Vec::new())
    }
}

fn make_event(source_name: &str, id: usize) -> Event {
    let ts = id as u64 + 1;
    Event {
        before: None,
        after: Some(serde_json::json!({"id": id, "payload": format!("evt-{id}")})),
        op: Operation::Insert,
        source: SourceMetadata {
            source_name: source_name.to_string(),
            offset: id.to_string(),
            timestamp: ts,
        },
        ts,
        schema: Some("public".to_string()),
        table: "soak_table".to_string(),
        primary_key: Some(vec!["id".to_string()]),
        snapshot: None,
        transaction: None,
        envelope_version: EVENT_ENVELOPE_VERSION,
    }
}

fn make_batches(source_name: &str, total: usize, batch_size: usize) -> VecDeque<Vec<Event>> {
    let mut batches = VecDeque::new();
    let mut current = Vec::with_capacity(batch_size);

    for i in 0..total {
        current.push(make_event(source_name, i));
        if current.len() == batch_size {
            batches.push_back(std::mem::take(&mut current));
            current = Vec::with_capacity(batch_size);
        }
    }

    if !current.is_empty() {
        batches.push_back(current);
    }

    batches
}

struct MockSnapshotHandle;

#[async_trait]
impl SnapshotHandle for MockSnapshotHandle {
    async fn next_chunk(&mut self, _chunk_size: usize) -> rustcdc::Result<Vec<Event>> {
        Ok(Vec::new())
    }

    async fn checkpoint(
        &self,
        _checkpoint: &mut dyn rustcdc::checkpoint::Checkpoint,
        _committed_event_count: u64,
    ) -> rustcdc::Result<()> {
        Ok(())
    }

    async fn finish(&mut self) -> rustcdc::Result<SnapshotEnd> {
        Ok(SnapshotEnd { snapshot_end_ts: 0 })
    }
}

struct MockStreamHandle {
    batches: VecDeque<Vec<Event>>,
}

#[async_trait]
impl StreamHandle for MockStreamHandle {
    async fn next_events(&mut self, _timeout_ms: u64) -> rustcdc::Result<Vec<Event>> {
        Ok(self.batches.pop_front().unwrap_or_default())
    }

    async fn save_position(
        &self,
        _checkpoint: &mut dyn rustcdc::checkpoint::Checkpoint,
    ) -> rustcdc::Result<()> {
        Ok(())
    }

    async fn confirm_lsn(&mut self, _lsn: u64) -> rustcdc::Result<()> {
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
    ) -> rustcdc::Result<Box<dyn SnapshotHandle>> {
        Ok(Box::new(MockSnapshotHandle))
    }

    async fn start_stream(
        &mut self,
        _resume_from: Option<&dyn rustcdc::Offset>,
    ) -> rustcdc::Result<Box<dyn StreamHandle>> {
        Ok(Box::new(MockStreamHandle {
            batches: self.stream_batches.clone(),
        }))
    }

    async fn perform_handoff(
        &mut self,
        _snapshot: &mut dyn SnapshotHandle,
        _stream: &mut dyn StreamHandle,
    ) -> rustcdc::Result<HandoffResult> {
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

#[tokio::test]
#[ignore = "nightly soak matrix"]
async fn postgres_soak_network_interruptions_recover_with_bounded_resume_latency() {
    let source = MockSource {
        stream_batches: make_batches("postgres", SOAK_TOTAL_EVENTS, SOAK_BATCH_SIZE),
    };
    let mut wrapped = FaultInjectingSource::new(source);
    wrapped.inject(SourceFault::TimeoutFault(Duration::from_millis(50)), 2_000);

    let mut stream = wrapped
        .start_stream(Some(&TestOffset))
        .await
        .expect("stream");
    let mut validator = DataLossValidator::new(SOAK_TOTAL_EVENTS as u64);

    let start = Instant::now();
    let mut first_fault_at = None;
    let mut recovered_after_fault_at = None;

    loop {
        match stream.next_events(50).await {
            Ok(events) => {
                if events.is_empty() {
                    break;
                }
                if first_fault_at.is_some() && recovered_after_fault_at.is_none() {
                    recovered_after_fault_at = Some(Instant::now());
                }
                for event in events {
                    validator.track_event(&event);
                }
            }
            Err(_) => {
                if first_fault_at.is_none() {
                    first_fault_at = Some(Instant::now());
                    wrapped.reset();
                    continue;
                }
                panic!("unexpected repeated stream error after reset");
            }
        }
    }

    let report = validator.finalize().expect("no data loss/corruption");
    assert_eq!(report.missing_events, 0);

    let fault_time = first_fault_at.expect("fault should trigger");
    let recovered_time = recovered_after_fault_at.expect("should recover after fault");
    let resume_latency = recovered_time.saturating_duration_since(fault_time);
    assert!(resume_latency < Duration::from_millis(250));
    assert!(start.elapsed() < Duration::from_secs(10));
}

#[tokio::test]
#[ignore = "nightly soak matrix"]
async fn mysql_soak_checkpoint_slowness_and_duplicates_stay_within_bounds() {
    let source = MockSource {
        stream_batches: make_batches("mysql", SOAK_TOTAL_EVENTS, SOAK_BATCH_SIZE),
    };
    let mut wrapped = FaultInjectingSource::new(source);
    wrapped.inject(SourceFault::DuplicateEventsFault(150), 1_000);

    let mut checkpoint = FaultInjectingCheckpoint::new(InMemoryCheckpoint::default());
    checkpoint.inject(CheckpointFault::SlowSave(Duration::from_millis(2)));

    let mut stream = wrapped
        .start_stream(Some(&TestOffset))
        .await
        .expect("stream");
    let mut validator = DataLossValidator::new(SOAK_TOTAL_EVENTS as u64);

    let started = Instant::now();
    let mut checkpoint_writes = 0_u64;

    loop {
        let events = stream.next_events(50).await.expect("stream next_events");
        if events.is_empty() {
            break;
        }

        for event in events {
            let offset = GenericOffset::new(
                event.source.source_name.clone(),
                event.source.offset.as_bytes().to_vec(),
            );
            checkpoint
                .save(&offset, checkpoint_writes)
                .await
                .expect("checkpoint save");
            checkpoint_writes = checkpoint_writes.saturating_add(1);
            validator.track_event(&event);
        }
    }

    let report = validator.finalize().expect("data should remain complete");
    let duplicate_rate = if report.expected_events == 0 {
        0.0
    } else {
        report.duplicate_events as f64 / report.expected_events as f64
    };

    assert!(
        duplicate_rate <= 0.05,
        "duplicate rate too high: {duplicate_rate}"
    );
    assert!(started.elapsed() < Duration::from_secs(30));
}

#[tokio::test]
#[ignore = "nightly soak matrix"]
async fn sqlserver_soak_transient_auth_failure_recovers_without_loss() {
    let source = MockSource {
        stream_batches: make_batches("sqlserver", SOAK_TOTAL_EVENTS, SOAK_BATCH_SIZE),
    };
    let mut wrapped = FaultInjectingSource::new(source);
    wrapped.inject(
        SourceFault::ConnectionFault(rustcdc::Error::SourceError("transient auth failure".into())),
        0,
    );

    let mut stream = wrapped
        .start_stream(Some(&TestOffset))
        .await
        .expect("stream");
    let mut validator = DataLossValidator::new(SOAK_TOTAL_EVENTS as u64);

    let mut saw_transient_auth_error = false;
    let mut attempts = 0_u64;

    loop {
        attempts = attempts.saturating_add(1);
        let batch = match stream.next_events(50).await {
            Ok(batch) => batch,
            Err(error) => {
                if !saw_transient_auth_error {
                    saw_transient_auth_error = true;
                    assert!(error.to_string().contains("transient auth failure"));
                    wrapped.reset();
                    continue;
                }
                panic!("unexpected repeated auth/connection error: {error}");
            }
        };

        if batch.is_empty() {
            break;
        }

        for event in batch {
            validator.track_event(&event);
        }
    }

    let report = validator.finalize().expect("validator should pass");
    assert!(saw_transient_auth_error);
    assert_eq!(report.missing_events, 0);
    assert!(
        attempts < 500,
        "too many attempts indicates poor recovery behavior"
    );
}
