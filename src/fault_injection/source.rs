use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;

use crate::{
    core::{Error, Event, Result},
    source::{HandoffResult, SnapshotEnd, SnapshotHandle, Source, StreamHandle},
};

/// Faults that can be injected into source stream/snapshot operations.
#[derive(Debug)]
pub enum SourceFault {
    ConnectionFault(Error),
    TimeoutFault(Duration),
    CorruptionFault(u8),
    DelayFault(Duration),
    DropEventsFault(u64),
    DuplicateEventsFault(u64),
}

#[derive(Debug)]
struct ScheduledSourceFault {
    after_event_count: u64,
    fault: SourceFault,
}

#[derive(Debug, Default)]
struct SourceFaultState {
    consumed_events: u64,
    scheduled: Vec<ScheduledSourceFault>,
    corruption_seed: u64,
}

#[derive(Debug, Clone)]
struct FaultController {
    state: Arc<Mutex<SourceFaultState>>,
}

impl FaultController {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(SourceFaultState::default())),
        }
    }

    fn inject(&self, fault: SourceFault, after_event_count: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.scheduled.push(ScheduledSourceFault {
                after_event_count,
                fault,
            });
        }
    }

    fn reset(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.scheduled.clear();
            state.consumed_events = 0;
            state.corruption_seed = 0;
        }
    }

    async fn before_operation(&self) -> Result<()> {
        let mut delays = Vec::new();
        let mut timeout = None;
        let mut connection = None;

        if let Ok(state) = self.state.lock() {
            for scheduled in &state.scheduled {
                if state.consumed_events < scheduled.after_event_count {
                    continue;
                }
                match &scheduled.fault {
                    SourceFault::DelayFault(duration) => delays.push(*duration),
                    SourceFault::TimeoutFault(duration) => timeout = Some(*duration),
                    SourceFault::ConnectionFault(error) => connection = Some(error.to_string()),
                    SourceFault::CorruptionFault(_)
                    | SourceFault::DropEventsFault(_)
                    | SourceFault::DuplicateEventsFault(_) => {}
                }
            }
        }

        for duration in delays {
            tokio::time::sleep(duration).await;
        }

        if let Some(duration) = timeout {
            return Err(Error::TimeoutError(format!(
                "fault injection: operation timed out after {}ms",
                duration.as_millis()
            )));
        }

        if let Some(message) = connection {
            return Err(Error::SourceError(format!(
                "fault injection: connection fault triggered: {message}"
            )));
        }

        Ok(())
    }

    fn should_corrupt(&self, event_index: u64, rate_percent: u8, seed: u64) -> bool {
        if rate_percent == 0 {
            return false;
        }
        if rate_percent >= 100 {
            return true;
        }

        let mut hasher = DefaultHasher::new();
        event_index.hash(&mut hasher);
        seed.hash(&mut hasher);
        (hasher.finish() % 100) < u64::from(rate_percent)
    }

    fn after_operation(&self, mut events: Vec<Event>) -> Vec<Event> {
        if let Ok(mut state) = self.state.lock() {
            let consumed_events = state.consumed_events;
            let mut dropped = 0_u64;
            let mut duplicate_budget = 0_u64;
            let mut corruption_rate = 0_u8;

            for scheduled in &mut state.scheduled {
                if consumed_events < scheduled.after_event_count {
                    continue;
                }
                match &mut scheduled.fault {
                    SourceFault::DropEventsFault(remaining) => {
                        dropped = dropped.saturating_add(*remaining);
                        *remaining = 0;
                    }
                    SourceFault::DuplicateEventsFault(remaining) => {
                        duplicate_budget = duplicate_budget.saturating_add(*remaining);
                        *remaining = 0;
                    }
                    SourceFault::CorruptionFault(rate_percent) => {
                        corruption_rate = corruption_rate.max(*rate_percent);
                    }
                    SourceFault::ConnectionFault(_)
                    | SourceFault::TimeoutFault(_)
                    | SourceFault::DelayFault(_) => {}
                }
            }

            if dropped > 0 {
                let to_drop = usize::try_from(dropped).unwrap_or(usize::MAX);
                if to_drop >= events.len() {
                    events.clear();
                } else {
                    events.drain(0..to_drop);
                }
            }

            if duplicate_budget > 0 && !events.is_empty() {
                let count = usize::try_from(duplicate_budget)
                    .unwrap_or(usize::MAX)
                    .min(events.len());
                let mut duplicates = events.iter().take(count).cloned().collect::<Vec<_>>();
                events.append(&mut duplicates);
            }

            if corruption_rate > 0 {
                state.corruption_seed = state.corruption_seed.wrapping_add(1);
                let seed = state.corruption_seed;
                for (index, event) in events.iter_mut().enumerate() {
                    if !self.should_corrupt(index as u64 + consumed_events, corruption_rate, seed) {
                        continue;
                    }

                    if let Some(after) = event.after.as_mut() {
                        if let Some(obj) = after.as_object_mut() {
                            obj.insert("__fault_corrupted".into(), serde_json::json!(true));
                        }
                    } else if let Some(before) = event.before.as_mut() {
                        if let Some(obj) = before.as_object_mut() {
                            obj.insert("__fault_corrupted".into(), serde_json::json!(true));
                        }
                    } else {
                        event.after = Some(serde_json::json!({"__fault_corrupted": true}));
                    }
                }
            }

            state.consumed_events = state
                .consumed_events
                .saturating_add(u64::try_from(events.len()).unwrap_or(u64::MAX));
        }

        events
    }
}

/// Wraps a source and injects configured faults into stream/snapshot operations.
pub struct FaultInjectingSource<S> {
    inner: S,
    controller: FaultController,
}

impl<S> FaultInjectingSource<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            controller: FaultController::new(),
        }
    }

    /// Inject `fault` once the source has delivered at least `after_event_count` events.
    pub fn inject(&mut self, fault: SourceFault, after_event_count: u64) {
        self.controller.inject(fault, after_event_count);
    }

    pub fn reset(&mut self) {
        self.controller.reset();
    }
}

struct FaultInjectingSnapshotHandle {
    inner: Box<dyn SnapshotHandle>,
    controller: FaultController,
}

#[async_trait]
impl SnapshotHandle for FaultInjectingSnapshotHandle {
    async fn next_chunk(&mut self, chunk_size: usize) -> Result<Vec<Event>> {
        self.controller.before_operation().await?;
        let chunk = self.inner.next_chunk(chunk_size).await?;
        Ok(self.controller.after_operation(chunk))
    }

    async fn checkpoint(
        &self,
        checkpoint: &mut dyn crate::checkpoint::Checkpoint,
        committed_event_count: u64,
    ) -> Result<()> {
        self.inner
            .checkpoint(checkpoint, committed_event_count)
            .await
    }

    async fn finish(&mut self) -> Result<SnapshotEnd> {
        self.inner.finish().await
    }
}

struct FaultInjectingStreamHandle {
    inner: Box<dyn StreamHandle>,
    controller: FaultController,
}

#[async_trait]
impl StreamHandle for FaultInjectingStreamHandle {
    async fn next_events(&mut self, timeout_ms: u64) -> Result<Vec<Event>> {
        self.controller.before_operation().await?;
        let events = self.inner.next_events(timeout_ms).await?;
        Ok(self.controller.after_operation(events))
    }

    async fn save_position(
        &self,
        checkpoint: &mut dyn crate::checkpoint::Checkpoint,
    ) -> Result<()> {
        self.inner.save_position(checkpoint).await
    }

    async fn requeue_events(&mut self, events: Vec<Event>) -> Result<()> {
        self.inner.requeue_events(events).await
    }

    async fn confirm_lsn(&mut self, lsn: u64) -> Result<()> {
        self.inner.confirm_lsn(lsn).await
    }
}

#[async_trait]
impl<S> Source for FaultInjectingSource<S>
where
    S: Source + Send + Sync,
{
    async fn start_snapshot(&mut self, tables: &[&str]) -> Result<Box<dyn SnapshotHandle>> {
        let handle = self.inner.start_snapshot(tables).await?;
        Ok(Box::new(FaultInjectingSnapshotHandle {
            inner: handle,
            controller: self.controller.clone(),
        }))
    }

    async fn start_stream(
        &mut self,
        resume_from: Option<&dyn crate::core::Offset>,
    ) -> Result<Box<dyn StreamHandle>> {
        let handle = self.inner.start_stream(resume_from).await?;
        Ok(Box::new(FaultInjectingStreamHandle {
            inner: handle,
            controller: self.controller.clone(),
        }))
    }

    async fn perform_handoff(
        &mut self,
        snapshot: &mut dyn SnapshotHandle,
        stream: &mut dyn StreamHandle,
    ) -> Result<HandoffResult> {
        self.inner.perform_handoff(snapshot, stream).await
    }

    fn source_type(&self) -> &str {
        self.inner.source_type()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use async_trait::async_trait;

    use crate::{
        core::SnapshotMetadata,
        core::{Offset, SourceMetadata, TransactionMetadata, EVENT_ENVELOPE_VERSION},
    };

    use super::*;

    #[derive(Debug, Clone)]
    struct TestOffset;

    impl Offset for TestOffset {
        fn source_type(&self) -> &str {
            "mock"
        }

        fn encode(&self) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
    }

    fn event(id: i64) -> Event {
        Event {
            before: None,
            after: Some(serde_json::json!({"id": id})),
            op: crate::Operation::Insert,
            source: SourceMetadata {
                source_name: "mock".into(),
                offset: format!("{id}"),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".into()),
            table: "items".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: Some(SnapshotMetadata {
                snapshot_id: "s1".into(),
                chunk_index: 0,
                is_last_chunk: false,
            }),
            transaction: Some(TransactionMetadata {
                tx_id: 1,
                total_events: 1,
                event_index: 0,
            }),
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    struct MockSnapshotHandle {
        chunks: VecDeque<Vec<Event>>,
    }

    #[async_trait]
    impl SnapshotHandle for MockSnapshotHandle {
        async fn next_chunk(&mut self, _chunk_size: usize) -> Result<Vec<Event>> {
            Ok(self.chunks.pop_front().unwrap_or_default())
        }

        async fn checkpoint(
            &self,
            _checkpoint: &mut dyn crate::checkpoint::Checkpoint,
            _committed_event_count: u64,
        ) -> Result<()> {
            Ok(())
        }

        async fn finish(&mut self) -> Result<SnapshotEnd> {
            Ok(SnapshotEnd { snapshot_end_ts: 1 })
        }
    }

    struct MockStreamHandle {
        batches: VecDeque<Vec<Event>>,
    }

    #[async_trait]
    impl StreamHandle for MockStreamHandle {
        async fn next_events(&mut self, _timeout_ms: u64) -> Result<Vec<Event>> {
            Ok(self.batches.pop_front().unwrap_or_default())
        }

        async fn save_position(
            &self,
            _checkpoint: &mut dyn crate::checkpoint::Checkpoint,
        ) -> Result<()> {
            Ok(())
        }

        async fn confirm_lsn(&mut self, _lsn: u64) -> Result<()> {
            Ok(())
        }
    }

    struct MockSource {
        snapshot_chunks: VecDeque<Vec<Event>>,
        stream_batches: VecDeque<Vec<Event>>,
    }

    #[async_trait]
    impl Source for MockSource {
        async fn start_snapshot(&mut self, _tables: &[&str]) -> Result<Box<dyn SnapshotHandle>> {
            Ok(Box::new(MockSnapshotHandle {
                chunks: self.snapshot_chunks.clone(),
            }))
        }

        async fn start_stream(
            &mut self,
            _resume_from: Option<&dyn Offset>,
        ) -> Result<Box<dyn StreamHandle>> {
            Ok(Box::new(MockStreamHandle {
                batches: self.stream_batches.clone(),
            }))
        }

        async fn perform_handoff(
            &mut self,
            _snapshot: &mut dyn SnapshotHandle,
            _stream: &mut dyn StreamHandle,
        ) -> Result<HandoffResult> {
            Ok(HandoffResult {
                snapshot_end_ts: Some(1),
                stream_start_ts: Some(2),
                overlap_events_dropped: 0,
                stream_watermark_gap: None,
            })
        }

        fn source_type(&self) -> &str {
            "mock"
        }
    }

    #[tokio::test]
    async fn fault_injected_at_correct_point() {
        let source = MockSource {
            snapshot_chunks: VecDeque::new(),
            stream_batches: VecDeque::from(vec![
                vec![event(1), event(2)],
                vec![event(3), event(4)],
            ]),
        };
        let mut wrapped = FaultInjectingSource::new(source);
        wrapped.inject(SourceFault::DropEventsFault(1), 2);

        let mut stream = wrapped.start_stream(Some(&TestOffset)).await.unwrap();
        let first = stream.next_events(10).await.unwrap();
        assert_eq!(first.len(), 2);
        let second = stream.next_events(10).await.unwrap();
        assert_eq!(second.len(), 1);
    }

    #[tokio::test]
    async fn multiple_faults_are_stackable() {
        let source = MockSource {
            snapshot_chunks: VecDeque::new(),
            stream_batches: VecDeque::from(vec![vec![event(1), event(2)]]),
        };
        let mut wrapped = FaultInjectingSource::new(source);
        wrapped.inject(SourceFault::DuplicateEventsFault(1), 0);
        wrapped.inject(SourceFault::CorruptionFault(100), 0);

        let mut stream = wrapped.start_stream(None).await.unwrap();
        let out = stream.next_events(10).await.unwrap();
        assert_eq!(out.len(), 3);
        assert!(out.iter().any(|e| {
            e.after
                .as_ref()
                .and_then(|row| row.get("__fault_corrupted"))
                .is_some()
        }));
    }

    #[tokio::test]
    async fn connection_fault_returns_error() {
        let source = MockSource {
            snapshot_chunks: VecDeque::new(),
            stream_batches: VecDeque::from(vec![vec![event(1)]]),
        };
        let mut wrapped = FaultInjectingSource::new(source);
        wrapped.inject(
            SourceFault::ConnectionFault(Error::SourceError("boom".into())),
            0,
        );

        let mut stream = wrapped.start_stream(None).await.unwrap();
        let err = stream.next_events(10).await.unwrap_err();
        assert!(format!("{err}").contains("connection fault"));
    }

    #[tokio::test]
    async fn corruption_fault_is_detectable() {
        let source = MockSource {
            snapshot_chunks: VecDeque::new(),
            stream_batches: VecDeque::from(vec![vec![event(1), event(2), event(3)]]),
        };
        let mut wrapped = FaultInjectingSource::new(source);
        wrapped.inject(SourceFault::CorruptionFault(100), 0);

        let mut stream = wrapped.start_stream(None).await.unwrap();
        let out = stream.next_events(10).await.unwrap();
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|e| {
            e.after
                .as_ref()
                .and_then(|row| row.get("__fault_corrupted"))
                .is_some()
        }));
    }
}
