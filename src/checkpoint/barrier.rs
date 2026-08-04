//! Commit barrier enforcing checkpoint safety.

use std::collections::VecDeque;

use crate::core::{Error, Offset, Result};

use super::Checkpoint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Lifecycle position of the commit barrier.
pub enum BarrierState {
    /// Accepting new events. The steady state while a pipeline is running.
    Open,
    /// A durable write is in progress; `add_event` is refused until it settles.
    ///
    /// This state is transient by construction: a failed write restores the previous
    /// state rather than leaving the barrier here, because a barrier stuck in `Flushing`
    /// rejects every subsequent event and turns one transient checkpoint-store failure
    /// into a permanently dead runtime.
    Flushing,
    /// Every pending record has been durably committed and drained.
    Committed,
}

#[derive(Clone)]
struct PendingRecord {
    offset: Option<Box<dyn Offset>>,
    accepted: bool,
}

/// Enforces that the durable checkpoint never advances past what the consumer accepted.
///
/// The barrier holds one record per delivered event. A record becomes *accepted* when the
/// consumer acknowledges it, and only the **contiguous accepted prefix** is ever committed
/// — so a partial acknowledgement advances the checkpoint exactly as far as the consumer
/// actually got, and no further.
///
/// Records with no offset (`add_non_persistent_event`) still advance the committed count
/// but do not move the durable position; snapshot rows use this, because their progress is
/// persisted through their own connector-native state.
pub struct CommitBarrier {
    pending_records: VecDeque<PendingRecord>,
    barrier_state: BarrierState,
    max_buffer_size: usize,
    committed_event_count: u64,
}

impl CommitBarrier {
    /// Create a barrier holding at most `max_buffer_size` pending records.
    ///
    /// Reaching the cap surfaces as backpressure, not as a failure.
    pub fn new(max_buffer_size: usize) -> Self {
        Self {
            pending_records: VecDeque::new(),
            barrier_state: BarrierState::Open,
            max_buffer_size,
            committed_event_count: 0,
        }
    }

    /// Current lifecycle position.
    pub fn state(&self) -> BarrierState {
        self.barrier_state
    }

    /// Total events durably committed since the counter was last hydrated.
    pub fn committed_event_count(&self) -> u64 {
        self.committed_event_count
    }

    /// Seed the committed counter from a loaded checkpoint at startup.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CheckpointError`] if records are already pending: hydrating then
    /// would silently discard progress the barrier is still tracking.
    pub fn hydrate_committed_event_count(&mut self, committed_event_count: u64) -> Result<()> {
        if !self.pending_records.is_empty() {
            return Err(Error::CheckpointError(
                "cannot hydrate committed count while pending records exist".into(),
            ));
        }

        self.committed_event_count = committed_event_count;
        Ok(())
    }

    /// Add an event whose durable position is `offset`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CheckpointError`] if the barrier is flushing or full. A full
    /// barrier is flow control: acknowledge the outstanding batch and retry.
    pub fn add_event<T>(&mut self, offset: T) -> Result<()>
    where
        T: Offset + Clone + 'static,
    {
        self.push_record(Some(Box::new(offset)))
    }

    /// Add an event whose offset is already boxed.
    ///
    /// Used for incremental-snapshot rows, whose offset is produced by the stream
    /// handle ([`crate::source::StreamHandle::position_offset`]) rather than derived
    /// from the event itself.
    pub fn add_boxed_event(&mut self, offset: Box<dyn Offset>) -> Result<()> {
        self.push_record(Some(offset))
    }

    /// Add an event that advances the committed count without moving the durable position.
    ///
    /// Used for snapshot rows, whose progress is persisted by
    /// [`crate::source::SnapshotHandle::checkpoint`] using connector-native state — a
    /// per-event offset here would clobber it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CheckpointError`] if the barrier is flushing or full.
    pub fn add_non_persistent_event(&mut self) -> Result<()> {
        self.push_record(None)
    }

    fn push_record(&mut self, offset: Option<Box<dyn Offset>>) -> Result<()> {
        if self.barrier_state == BarrierState::Flushing {
            return Err(Error::CheckpointError(
                "cannot add events while barrier is flushing".into(),
            ));
        }
        if self.pending_records.len() >= self.max_buffer_size {
            return Err(Error::CheckpointError(
                "commit barrier buffer is full".into(),
            ));
        }
        self.barrier_state = BarrierState::Open;
        self.pending_records.push_back(PendingRecord {
            offset,
            accepted: false,
        });
        Ok(())
    }

    /// Accept `event_count` further events and durably commit the accepted prefix.
    ///
    /// Acceptance and commit are one operation because they were previously two, and
    /// a failure between them wedged the barrier permanently: the acceptance marks
    /// had already been written, so the natural retry saw zero unaccepted records and
    /// failed with *"acceptance notification exceeds pending records"* forever, while
    /// `stop()` refused to run with events pending. The only exit was `force_stop()`,
    /// which discards them.
    ///
    /// On any failure this restores the exact pre-call state — acceptance marks,
    /// barrier state and committed count — so the caller can retry the identical
    /// call once the checkpoint store recovers.
    pub async fn accept_and_commit(
        &mut self,
        event_count: u64,
        checkpoint: &mut dyn Checkpoint,
    ) -> Result<()> {
        let previously_accepted = self
            .pending_records
            .iter()
            .filter(|record| record.accepted)
            .count();
        let restore_state = self.barrier_state;

        self.notify_consumer_accepted(event_count)?;

        match self.commit(checkpoint).await {
            Ok(()) => Ok(()),
            Err(error) => {
                // Undo only the marks this call set; records accepted by an earlier
                // partial acknowledgement keep their state.
                for record in self
                    .pending_records
                    .iter_mut()
                    .skip(previously_accepted)
                    .take(event_count as usize)
                {
                    record.accepted = false;
                }
                self.barrier_state = restore_state;
                Err(error)
            }
        }
    }

    /// Mark the next `event_count` unaccepted records as accepted.
    ///
    /// Prefer [`CommitBarrier::accept_and_commit`], which pairs this with the durable
    /// write transactionally. Calling this alone and then failing to commit leaves the
    /// acceptance marks applied, which is the shape of the wedge `accept_and_commit`
    /// exists to prevent.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CheckpointError`] if `event_count` exceeds the unaccepted records.
    pub fn notify_consumer_accepted(&mut self, event_count: u64) -> Result<()> {
        let available = self
            .pending_records
            .iter()
            .filter(|record| !record.accepted)
            .count() as u64;
        if event_count > available {
            return Err(Error::CheckpointError(
                "acceptance notification exceeds pending records".into(),
            ));
        }

        let mut remaining = event_count;
        for record in &mut self.pending_records {
            if !record.accepted {
                record.accepted = true;
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Durably persist the contiguous accepted prefix and drain it.
    ///
    /// Writes the **last persistable offset** in the prefix, so a prefix mixing snapshot
    /// and stream events commits at the stream position rather than regressing.
    ///
    /// On failure the barrier state is restored, so a transient checkpoint-store error is
    /// retryable rather than terminal.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CheckpointError`] if no accepted prefix exists, or propagates the
    /// checkpoint store's own error.
    pub async fn commit(&mut self, checkpoint: &mut dyn Checkpoint) -> Result<()> {
        if self.pending_records.is_empty() {
            self.barrier_state = BarrierState::Committed;
            return Ok(());
        }

        let commit_len = self
            .pending_records
            .iter()
            .take_while(|record| record.accepted)
            .count();

        if commit_len == 0 {
            return Err(Error::CheckpointError(
                "cannot commit with no accepted prefix".into(),
            ));
        }

        let restore_state = self.barrier_state;
        self.barrier_state = BarrierState::Flushing;
        let new_committed_count = self.committed_event_count + commit_len as u64;
        let last_persistable = self
            .pending_records
            .iter()
            .take(commit_len)
            .rev()
            .find_map(|record| record.offset.as_ref());

        if let Some(last_committable) = last_persistable {
            if let Err(error) = checkpoint
                .save(last_committable.as_ref(), new_committed_count)
                .await
            {
                // Leaving the barrier in `Flushing` would reject every subsequent
                // `add_event`, turning one transient checkpoint-store failure into a
                // permanently dead runtime.
                self.barrier_state = restore_state;
                return Err(error);
            }
        }
        self.committed_event_count = new_committed_count;
        let _ = self.pending_records.drain(..commit_len);
        self.barrier_state = if self.pending_records.is_empty() {
            BarrierState::Committed
        } else {
            BarrierState::Open
        };
        Ok(())
    }

    /// Drop every pending record without committing.
    ///
    /// **Discards uncommitted progress.** Only for a forced shutdown path that has already
    /// surfaced the dropped events to the caller.
    pub fn clear_pending(&mut self) {
        self.pending_records.clear();
        self.barrier_state = BarrierState::Open;
    }

    /// Number of records held but not yet committed.
    pub fn pending_count(&self) -> usize {
        self.pending_records.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::checkpoint::{Checkpoint, GenericOffset, InMemoryCheckpoint};

    use super::{BarrierState, CommitBarrier};

    #[tokio::test]
    async fn commit_requires_accepted_events() {
        let mut barrier = CommitBarrier::new(10);
        let mut checkpoint = InMemoryCheckpoint::default();
        barrier
            .add_event(GenericOffset::new("test", vec![1]))
            .unwrap();

        let error = barrier.commit(&mut checkpoint).await.unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));
    }

    #[tokio::test]
    async fn partial_acceptance_commits_prefix_only() {
        let mut barrier = CommitBarrier::new(10);
        let mut checkpoint = InMemoryCheckpoint::default();

        barrier
            .add_event(GenericOffset::new("test", vec![1]))
            .unwrap();
        barrier
            .add_event(GenericOffset::new("test", vec![2]))
            .unwrap();

        barrier.notify_consumer_accepted(1).unwrap();
        barrier.commit(&mut checkpoint).await.unwrap();
        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 1);
        assert_eq!(barrier.state(), BarrierState::Open);

        barrier.notify_consumer_accepted(1).unwrap();
        barrier.commit(&mut checkpoint).await.unwrap();
        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 2);
        assert_eq!(barrier.state(), BarrierState::Committed);
    }

    #[tokio::test]
    async fn accepted_events_commit_and_update_count() {
        let mut barrier = CommitBarrier::new(10);
        let mut checkpoint = InMemoryCheckpoint::default();

        barrier
            .add_event(GenericOffset::new("test", vec![1]))
            .unwrap();
        barrier
            .add_event(GenericOffset::new("test", vec![2]))
            .unwrap();
        barrier.notify_consumer_accepted(2).unwrap();
        barrier.commit(&mut checkpoint).await.unwrap();

        assert_eq!(barrier.state(), BarrierState::Committed);
        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn non_persistent_events_advance_committed_count_without_checkpoint_write() {
        let mut barrier = CommitBarrier::new(10);
        let mut checkpoint = InMemoryCheckpoint::default();

        barrier.add_non_persistent_event().unwrap();
        barrier.notify_consumer_accepted(1).unwrap();
        barrier.commit(&mut checkpoint).await.unwrap();

        assert_eq!(barrier.committed_event_count(), 1);
        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 0);
        assert_eq!(barrier.state(), BarrierState::Committed);
    }

    #[tokio::test]
    async fn mixed_prefix_commits_using_last_persistable_offset() {
        let mut barrier = CommitBarrier::new(10);
        let mut checkpoint = InMemoryCheckpoint::default();

        barrier
            .add_event(GenericOffset::new("test", vec![1]))
            .unwrap();
        barrier.add_non_persistent_event().unwrap();

        barrier.notify_consumer_accepted(2).unwrap();
        barrier.commit(&mut checkpoint).await.unwrap();

        assert_eq!(barrier.committed_event_count(), 2);
        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 2);
    }

    #[test]
    fn clear_pending_resets_state_and_empties_buffer() {
        let mut barrier = CommitBarrier::new(10);
        barrier
            .add_event(GenericOffset::new("test", vec![1]))
            .unwrap();
        barrier.clear_pending();
        assert_eq!(barrier.pending_count(), 0);
        assert_eq!(barrier.state(), BarrierState::Open);
    }

    #[test]
    fn hydrate_committed_event_count_updates_counter() {
        let mut barrier = CommitBarrier::new(10);
        barrier.hydrate_committed_event_count(42).unwrap();
        assert_eq!(barrier.committed_event_count(), 42);
    }

    #[test]
    fn hydrate_committed_event_count_rejects_pending_records() {
        let mut barrier = CommitBarrier::new(10);
        barrier
            .add_event(GenericOffset::new("test", vec![1]))
            .unwrap();

        let error = barrier.hydrate_committed_event_count(7).unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));
    }

    /// A checkpoint store that fails a configurable number of times, then succeeds.
    struct FlakyCheckpoint {
        failures_remaining: std::sync::atomic::AtomicUsize,
        inner: InMemoryCheckpoint,
    }

    #[async_trait::async_trait]
    impl Checkpoint for FlakyCheckpoint {
        async fn save(
            &mut self,
            offset: &dyn crate::core::Offset,
            committed_event_count: u64,
        ) -> crate::core::Result<()> {
            use std::sync::atomic::Ordering;
            if self
                .failures_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    n.checked_sub(1).or(Some(0))
                })
                .unwrap_or(0)
                > 0
            {
                return Err(crate::core::Error::CheckpointError("disk full".into()));
            }
            self.inner.save(offset, committed_event_count).await
        }

        async fn load(&self) -> crate::core::Result<Option<Box<dyn crate::core::Offset>>> {
            self.inner.load().await
        }

        async fn get_committed_count(&self) -> crate::core::Result<u64> {
            self.inner.get_committed_count().await
        }
    }

    #[tokio::test]
    async fn a_failed_durable_write_does_not_wedge_the_barrier() {
        // Regression: acceptance used to be applied before the durable write and was
        // not rolled back when the write failed. The retry then saw zero unaccepted
        // records and failed with "acceptance notification exceeds pending records"
        // forever; `add_event` also refused because the barrier stayed `Flushing`.
        // The only exit was `force_stop()`, which discards the events.
        let mut barrier = CommitBarrier::new(10);
        let mut checkpoint = FlakyCheckpoint {
            failures_remaining: std::sync::atomic::AtomicUsize::new(1),
            inner: InMemoryCheckpoint::default(),
        };

        barrier
            .add_event(GenericOffset::new("test", vec![1]))
            .unwrap();
        barrier
            .add_event(GenericOffset::new("test", vec![2]))
            .unwrap();

        let error = barrier
            .accept_and_commit(2, &mut checkpoint)
            .await
            .unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));

        assert_eq!(
            barrier.state(),
            BarrierState::Open,
            "a failed write must not leave the barrier flushing"
        );
        assert_eq!(barrier.pending_count(), 2, "no record may be dropped");
        assert_eq!(barrier.committed_event_count(), 0);

        // Both recovery paths must work: accepting more events, and retrying the
        // identical acknowledgement.
        barrier
            .add_event(GenericOffset::new("test", vec![3]))
            .unwrap();
        barrier.accept_and_commit(3, &mut checkpoint).await.unwrap();
        assert_eq!(barrier.committed_event_count(), 3);
        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn rollback_preserves_acceptance_from_an_earlier_partial_ack() {
        let mut barrier = CommitBarrier::new(10);
        let mut checkpoint = FlakyCheckpoint {
            failures_remaining: std::sync::atomic::AtomicUsize::new(0),
            inner: InMemoryCheckpoint::default(),
        };

        for byte in 1..=3_u8 {
            barrier
                .add_event(GenericOffset::new("test", vec![byte]))
                .unwrap();
        }
        // Accept one event without committing it, mirroring a partial ack.
        barrier.notify_consumer_accepted(1).unwrap();

        checkpoint
            .failures_remaining
            .store(1, std::sync::atomic::Ordering::SeqCst);
        assert!(barrier.accept_and_commit(2, &mut checkpoint).await.is_err());

        // The earlier acceptance must survive: committing it alone still works.
        checkpoint
            .failures_remaining
            .store(0, std::sync::atomic::Ordering::SeqCst);
        barrier.commit(&mut checkpoint).await.unwrap();
        assert_eq!(
            barrier.committed_event_count(),
            1,
            "only the pre-existing acceptance should have been committed"
        );
    }

    #[tokio::test]
    async fn boxed_events_participate_in_the_committed_prefix() {
        let mut barrier = CommitBarrier::new(10);
        let mut checkpoint = InMemoryCheckpoint::default();

        barrier
            .add_boxed_event(Box::new(GenericOffset::new("test", vec![9])))
            .unwrap();
        barrier.accept_and_commit(1, &mut checkpoint).await.unwrap();

        assert_eq!(checkpoint.get_committed_count().await.unwrap(), 1);
        let stored = checkpoint.load().await.unwrap().unwrap();
        assert_eq!(stored.encode().unwrap(), vec![9]);
    }

    #[tokio::test]
    async fn randomized_notify_commit_stress_has_no_phantom_commits() {
        let mut barrier = CommitBarrier::new(200);
        let mut checkpoint = InMemoryCheckpoint::default();

        for ts in 1..=100_u64 {
            barrier
                .add_event(GenericOffset::new("test", ts.to_be_bytes().to_vec()))
                .unwrap();
        }

        let mut seed = 0x5A17_F00D_u64;
        let mut accepted = 0_u64;
        let mut committed = 0_u64;
        let mut iterations = 0_u32;

        while committed < 100 {
            iterations += 1;
            assert!(iterations <= 10_000, "stress loop should converge");

            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let action = ((seed >> 33) % 3) as u8;

            if action != 0 && accepted < 100 {
                let remaining = 100 - accepted;
                let chunk = (((seed >> 40) % 5) + 1).min(remaining);
                barrier.notify_consumer_accepted(chunk).unwrap();
                accepted += chunk;
            } else {
                let _ = barrier.commit(&mut checkpoint).await;
            }

            let latest = checkpoint.get_committed_count().await.unwrap();
            assert!(
                latest <= accepted,
                "checkpoint advanced beyond accepted events"
            );
            assert!(latest >= committed, "checkpoint count regressed");
            committed = latest;
        }

        assert_eq!(accepted, 100);
        assert_eq!(committed, 100);
        assert_eq!(barrier.state(), BarrierState::Committed);
        assert!(checkpoint.history_len() > 0);

        let loaded = checkpoint.load().await.unwrap().unwrap();
        let encoded = loaded.encode().unwrap();
        assert_eq!(encoded, 100_u64.to_be_bytes().to_vec());
    }
}
