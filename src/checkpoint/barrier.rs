//! Commit barrier enforcing checkpoint safety.

use std::collections::VecDeque;

use crate::core::{Error, Offset, Result};

use super::Checkpoint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierState {
    Open,
    Flushing,
    Committed,
}

#[derive(Clone)]
struct PendingRecord {
    offset: Option<Box<dyn Offset>>,
    accepted: bool,
}

pub struct CommitBarrier {
    pending_records: VecDeque<PendingRecord>,
    barrier_state: BarrierState,
    max_buffer_size: usize,
    committed_event_count: u64,
}

impl CommitBarrier {
    pub fn new(max_buffer_size: usize) -> Self {
        Self {
            pending_records: VecDeque::new(),
            barrier_state: BarrierState::Open,
            max_buffer_size,
            committed_event_count: 0,
        }
    }

    pub fn state(&self) -> BarrierState {
        self.barrier_state
    }

    pub fn committed_event_count(&self) -> u64 {
        self.committed_event_count
    }

    pub fn hydrate_committed_event_count(&mut self, committed_event_count: u64) -> Result<()> {
        if !self.pending_records.is_empty() {
            return Err(Error::CheckpointError(
                "cannot hydrate committed count while pending records exist".into(),
            ));
        }

        self.committed_event_count = committed_event_count;
        Ok(())
    }

    pub fn add_event<T>(&mut self, offset: T) -> Result<()>
    where
        T: Offset + Clone + 'static,
    {
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
            offset: Some(Box::new(offset)),
            accepted: false,
        });
        Ok(())
    }

    pub fn add_non_persistent_event(&mut self) -> Result<()> {
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
            offset: None,
            accepted: false,
        });
        Ok(())
    }

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

        self.barrier_state = BarrierState::Flushing;
        let new_committed_count = self.committed_event_count + commit_len as u64;
        let last_persistable = self
            .pending_records
            .iter()
            .take(commit_len)
            .rev()
            .find_map(|record| record.offset.as_ref());

        if let Some(last_committable) = last_persistable {
            checkpoint
                .save(last_committable.as_ref(), new_committed_count)
                .await?;
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

    pub fn clear_pending(&mut self) {
        self.pending_records.clear();
        self.barrier_state = BarrierState::Open;
    }

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
