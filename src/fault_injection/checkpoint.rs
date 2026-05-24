use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;

use crate::{
    checkpoint::{Checkpoint, GenericOffset},
    core::{Error, Offset, Result},
};

/// Faults that can be injected into checkpoint save/load operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointFault {
    CorruptCheckpoint,
    FailLoad,
    FailSave,
    SlowSave(Duration),
    LoseCheckpoint,
}

/// Checkpoint wrapper that injects deterministic failures for tests.
#[derive(Debug, Clone)]
pub struct FaultInjectingCheckpoint<C> {
    inner: C,
    faults: Arc<Mutex<Vec<CheckpointFault>>>,
}

impl<C> FaultInjectingCheckpoint<C> {
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            faults: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn inject(&mut self, fault: CheckpointFault) {
        if let Ok(mut faults) = self.faults.lock() {
            faults.push(fault);
        }
    }

    pub fn reset(&mut self) {
        if let Ok(mut faults) = self.faults.lock() {
            faults.clear();
        }
    }

    fn active_faults(&self) -> Vec<CheckpointFault> {
        self.faults
            .lock()
            .map(|faults| faults.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl<C> Checkpoint for FaultInjectingCheckpoint<C>
where
    C: Checkpoint + Send + Sync,
{
    async fn save(&mut self, offset: &dyn Offset, committed_event_count: u64) -> Result<()> {
        let faults = self.active_faults();
        for fault in &faults {
            match fault {
                CheckpointFault::SlowSave(duration) => {
                    tokio::time::sleep(*duration).await;
                }
                CheckpointFault::FailSave => {
                    return Err(Error::CheckpointError(
                        "fault injection: checkpoint save failed".into(),
                    ));
                }
                CheckpointFault::LoseCheckpoint => {
                    return Ok(());
                }
                CheckpointFault::CorruptCheckpoint => {
                    let corrupt = GenericOffset::new(offset.source_type(), b"CORRUPTED".to_vec());
                    return self.inner.save(&corrupt, committed_event_count).await;
                }
                CheckpointFault::FailLoad => {}
            }
        }

        self.inner.save(offset, committed_event_count).await
    }

    async fn load(&self) -> Result<Option<Box<dyn Offset>>> {
        let faults = self.active_faults();
        for fault in &faults {
            match fault {
                CheckpointFault::FailLoad => {
                    return Err(Error::CheckpointError(
                        "fault injection: checkpoint load failed".into(),
                    ));
                }
                CheckpointFault::LoseCheckpoint => return Ok(None),
                CheckpointFault::CorruptCheckpoint => {
                    let loaded = self.inner.load().await?;
                    if let Some(offset) = loaded {
                        let corrupted =
                            GenericOffset::new(offset.source_type(), b"CORRUPTED".to_vec());
                        return Ok(Some(Box::new(corrupted)));
                    }
                    return Ok(None);
                }
                CheckpointFault::FailSave | CheckpointFault::SlowSave(_) => {}
            }
        }

        self.inner.load().await
    }

    async fn get_committed_count(&self) -> Result<u64> {
        self.inner.get_committed_count().await
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::checkpoint::{Checkpoint, GenericOffset, InMemoryCheckpoint};

    use super::{CheckpointFault, FaultInjectingCheckpoint};

    #[tokio::test]
    async fn fail_save_fault_returns_error() {
        let mut checkpoint = FaultInjectingCheckpoint::new(InMemoryCheckpoint::default());
        checkpoint.inject(CheckpointFault::FailSave);

        let offset = GenericOffset::new("postgres", b"ok".to_vec());
        let error = checkpoint.save(&offset, 1).await.unwrap_err();
        assert!(format!("{error}").contains("save failed"));
    }

    #[tokio::test]
    async fn fail_load_fault_returns_error() {
        let mut checkpoint = FaultInjectingCheckpoint::new(InMemoryCheckpoint::default());
        let offset = GenericOffset::new("postgres", b"ok".to_vec());
        checkpoint.save(&offset, 1).await.unwrap();

        checkpoint.inject(CheckpointFault::FailLoad);
        let error = checkpoint.load().await.unwrap_err();
        assert!(format!("{error}").contains("load failed"));
    }

    #[tokio::test]
    async fn lose_checkpoint_fault_hides_saved_state() {
        let mut checkpoint = FaultInjectingCheckpoint::new(InMemoryCheckpoint::default());
        let offset = GenericOffset::new("mysql", b"state".to_vec());
        checkpoint.save(&offset, 7).await.unwrap();

        checkpoint.inject(CheckpointFault::LoseCheckpoint);
        assert!(checkpoint.load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn slow_save_fault_adds_latency() {
        let mut checkpoint = FaultInjectingCheckpoint::new(InMemoryCheckpoint::default());
        checkpoint.inject(CheckpointFault::SlowSave(Duration::from_millis(30)));

        let offset = GenericOffset::new("sqlserver", b"state".to_vec());
        let started = Instant::now();
        checkpoint.save(&offset, 3).await.unwrap();

        assert!(started.elapsed() >= Duration::from_millis(25));
    }

    #[tokio::test]
    async fn corrupt_checkpoint_fault_rewrites_payload() {
        let mut checkpoint = FaultInjectingCheckpoint::new(InMemoryCheckpoint::default());
        let offset = GenericOffset::new("postgres", b"state".to_vec());
        checkpoint.save(&offset, 2).await.unwrap();

        checkpoint.inject(CheckpointFault::CorruptCheckpoint);
        let loaded = checkpoint.load().await.unwrap().unwrap();
        assert_eq!(loaded.source_type(), "postgres");
        assert_eq!(loaded.encode().unwrap(), b"CORRUPTED".to_vec());
    }

    #[tokio::test]
    async fn reset_restores_normal_behavior() {
        let mut checkpoint = FaultInjectingCheckpoint::new(InMemoryCheckpoint::default());
        checkpoint.inject(CheckpointFault::FailSave);
        let offset = GenericOffset::new("mysql", b"state".to_vec());
        assert!(checkpoint.save(&offset, 1).await.is_err());

        checkpoint.reset();
        checkpoint.save(&offset, 1).await.unwrap();
        let loaded = checkpoint.load().await.unwrap().unwrap();
        assert_eq!(loaded.source_type(), "mysql");
        assert_eq!(loaded.encode().unwrap(), b"state".to_vec());
    }
}
