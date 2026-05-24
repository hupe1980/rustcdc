//! Parallel snapshot chunking for efficient multi-table snapshots.

use std::sync::{Arc, Mutex};

use crate::core::{Error, Event, Result};
use crate::source::SnapshotProgress;

/// Configuration for parallel snapshot execution.
#[derive(Debug, Clone)]
pub struct ParallelSnapshotConfig {
    /// Maximum number of tables to snapshot in parallel
    pub max_parallel_tables: usize,
    /// Chunk size for each table
    pub chunk_size: usize,
}

impl Default for ParallelSnapshotConfig {
    fn default() -> Self {
        Self {
            max_parallel_tables: 4,
            chunk_size: 5000,
        }
    }
}

/// State for tracking parallel snapshot execution.
#[derive(Debug)]
pub struct ParallelSnapshotState {
    /// Snapshot progress across all tables
    progress: Arc<Mutex<SnapshotProgress>>,
    /// Buffered events from parallel chunk fetches
    pending_events: Arc<Mutex<Vec<Event>>>,
    /// Table processing order
    table_order: Vec<String>,
}

impl ParallelSnapshotState {
    /// Create a new parallel snapshot state.
    pub fn new(
        snapshot_id: String,
        created_at: u64,
        tables: Vec<String>,
        _config: ParallelSnapshotConfig,
    ) -> Self {
        let mut progress = SnapshotProgress::new(snapshot_id, created_at);
        for table in &tables {
            progress.add_table(table.clone());
        }

        Self {
            progress: Arc::new(Mutex::new(progress)),
            pending_events: Arc::new(Mutex::new(Vec::new())),
            table_order: tables,
        }
    }

    /// Get the number of tables to process.
    pub fn table_count(&self) -> usize {
        self.table_order.len()
    }

    /// Get current snapshot progress.
    pub fn get_progress(&self) -> Result<SnapshotProgress> {
        self.progress
            .lock()
            .map(|p| p.clone())
            .map_err(|e| Error::StateError(format!("Failed to lock progress: {}", e)))
    }

    /// Record events from a chunk.
    pub fn record_chunk_events(
        &self,
        table: &str,
        events: Vec<Event>,
        cursor_token: Option<Vec<u8>>,
    ) -> Result<()> {
        let event_count = events.len();

        // Add events to pending buffer
        if let Ok(mut pending) = self.pending_events.lock() {
            pending.extend(events);
        }

        self.record_chunk_progress(table, event_count, cursor_token)
    }

    /// Record chunk progress without buffering events.
    ///
    /// This is useful for stress tests and resumability simulations where
    /// only progress accounting is needed.
    pub fn record_chunk_progress(
        &self,
        table: &str,
        row_count: usize,
        cursor_token: Option<Vec<u8>>,
    ) -> Result<()> {
        if let Ok(mut progress) = self.progress.lock() {
            progress.record_table_chunk(table, row_count, cursor_token)?;
            Ok(())
        } else {
            Err(Error::StateError("Failed to lock progress".into()))
        }
    }

    /// Get buffered events (up to max_chunk_size).
    pub fn get_buffered_events(&self, max_size: usize) -> Result<Vec<Event>> {
        if let Ok(mut pending) = self.pending_events.lock() {
            let drain_count = std::cmp::min(pending.len(), max_size);
            let events: Vec<Event> = pending.drain(..drain_count).collect();
            Ok(events)
        } else {
            Err(Error::StateError("Failed to lock pending events".into()))
        }
    }

    /// Check if all tables have been processed.
    pub fn all_complete(&self) -> Result<bool> {
        if let Ok(progress) = self.progress.lock() {
            Ok(progress.is_all_complete())
        } else {
            Err(Error::StateError("Failed to lock progress".into()))
        }
    }

    /// Get pending tables (not yet complete).
    pub fn get_pending_tables(&self) -> Result<Vec<String>> {
        if let Ok(progress) = self.progress.lock() {
            Ok(progress.get_pending_tables())
        } else {
            Err(Error::StateError("Failed to lock progress".into()))
        }
    }

    /// Mark a table as complete.
    pub fn mark_table_complete(&self, table: &str) -> Result<()> {
        if let Ok(mut progress) = self.progress.lock() {
            progress.mark_table_complete(table)?;
        }
        Ok(())
    }

    /// Get total rows processed so far.
    pub fn total_rows_processed(&self) -> Result<u64> {
        if let Ok(progress) = self.progress.lock() {
            Ok(progress.total_rows_processed())
        } else {
            Err(Error::StateError("Failed to lock progress".into()))
        }
    }

    /// Get count of completed tables.
    pub fn completed_tables(&self) -> Result<usize> {
        if let Ok(progress) = self.progress.lock() {
            Ok(progress.completed_tables())
        } else {
            Err(Error::StateError("Failed to lock progress".into()))
        }
    }

    /// Get snapshot progress percentage (0-100).
    pub fn progress_percent(&self) -> Result<u8> {
        let completed = self.completed_tables()?;
        let total = self.table_count();
        if total == 0 {
            return Ok(0);
        }
        Ok(((completed * 100) / total) as u8)
    }
}

/// Report on parallel snapshot execution.
#[derive(Debug, Clone)]
pub struct ParallelSnapshotReport {
    pub snapshot_id: String,
    pub total_tables: usize,
    pub completed_tables: usize,
    pub total_rows_processed: u64,
    pub pending_tables: Vec<String>,
    pub progress_percent: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_snapshot_state_creation() {
        let tables = vec!["users".into(), "orders".into(), "products".into()];
        let state = ParallelSnapshotState::new("snap1".into(), 1000, tables, Default::default());

        assert_eq!(state.table_count(), 3);
        assert!(!state.all_complete().unwrap());
    }

    #[test]
    fn test_parallel_snapshot_state_progress() {
        let tables = vec!["users".into(), "orders".into()];
        let state = ParallelSnapshotState::new("snap1".into(), 1000, tables, Default::default());

        state.mark_table_complete("users").unwrap();
        assert_eq!(state.completed_tables().unwrap(), 1);
        assert_eq!(state.progress_percent().unwrap(), 50);

        state.mark_table_complete("orders").unwrap();
        assert_eq!(state.completed_tables().unwrap(), 2);
        assert_eq!(state.progress_percent().unwrap(), 100);
        assert!(state.all_complete().unwrap());
    }

    #[test]
    fn test_parallel_snapshot_pending_tables() {
        let tables = vec!["users".into(), "orders".into(), "products".into()];
        let state = ParallelSnapshotState::new("snap1".into(), 1000, tables, Default::default());

        let pending = state.get_pending_tables().unwrap();
        assert_eq!(pending.len(), 3);

        state.mark_table_complete("users").unwrap();
        let pending = state.get_pending_tables().unwrap();
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"orders".to_string()));
        assert!(pending.contains(&"products".to_string()));
    }

    #[test]
    fn test_parallel_snapshot_total_rows() {
        let tables = vec!["users".into(), "orders".into()];
        let state = ParallelSnapshotState::new("snap1".into(), 1000, tables, Default::default());

        assert_eq!(state.total_rows_processed().unwrap(), 0);

        // Create mock events
        let events = vec![Event {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            op: crate::core::Operation::Read,
            source: crate::core::SourceMetadata {
                source_name: "test".into(),
                offset: "0".into(),
                timestamp: 1000,
            },
            ts: 1000,
            schema: None,
            table: "users".into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: crate::EVENT_ENVELOPE_VERSION,
        }];

        state
            .record_chunk_events("users", events.clone(), None)
            .unwrap();
        assert_eq!(state.total_rows_processed().unwrap(), 1); // Tracks actual rows/events
    }

    #[test]
    fn test_parallel_snapshot_buffered_events() {
        let tables = vec!["users".into()];
        let state = ParallelSnapshotState::new("snap1".into(), 1000, tables, Default::default());

        let events = vec![Event {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            op: crate::core::Operation::Read,
            source: crate::core::SourceMetadata {
                source_name: "test".into(),
                offset: "0".into(),
                timestamp: 1000,
            },
            ts: 1000,
            schema: None,
            table: "users".into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: crate::EVENT_ENVELOPE_VERSION,
        }];

        state.record_chunk_events("users", events, None).unwrap();

        let buffered = state.get_buffered_events(100).unwrap();
        assert_eq!(buffered.len(), 1);

        let buffered = state.get_buffered_events(100).unwrap();
        assert_eq!(buffered.len(), 0); // Already drained
    }

    #[test]
    fn test_parallel_snapshot_default_config() {
        let config = ParallelSnapshotConfig::default();
        assert_eq!(config.max_parallel_tables, 4);
        assert_eq!(config.chunk_size, 5000);
    }
}
