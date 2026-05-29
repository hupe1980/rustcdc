//! Per-table snapshot progress tracking for multi-table snapshot coordination.
//!
//! [`SnapshotProgressTracker`] is a chunk-progress state machine intended to track
//! per-table snapshot progress when an embedder coordinates its own multi-table
//! snapshot workers. It does **not** execute snapshot queries itself; connector
//! snapshots are sequential by default. Embedders that need true parallel snapshot
//! execution must manage their own connection pool and call
//! [`SnapshotProgressTracker::record_chunk_progress`] /
//! [`SnapshotProgressTracker::record_chunk_events`] as
//! each worker completes a chunk.
//!
//! For the state-of-the-art approach to non-blocking snapshot, see the roadmap item
//! for watermark-based incremental snapshotting (DBLog pattern, G-05 in FINDINGS.md).

use std::sync::{Arc, Mutex};

use crate::core::{Error, Event, Result};
use crate::source::SnapshotProgress;

/// Configuration for the [`SnapshotProgressTracker`] coordinator.
#[derive(Debug, Clone)]
pub struct SnapshotTrackerConfig {
    /// Default chunk size used when fetching rows for each table.
    pub chunk_size: usize,
}

impl Default for SnapshotTrackerConfig {
    fn default() -> Self {
        Self { chunk_size: 5000 }
    }
}

/// Per-table snapshot progress tracker for multi-table snapshot coordination.
///
/// This type tracks chunk-level progress (row counts, PK cursor tokens, completion
/// state) across multiple tables. It is designed to be the persistence/coordination
/// layer for embedder-managed parallel snapshot workers. Connector-level snapshot
/// execution is **not** included; callers drive workers and report progress here.
#[derive(Debug)]
pub struct SnapshotProgressTracker {
    /// Snapshot progress across all tables
    progress: Arc<Mutex<SnapshotProgress>>,
    /// Buffered events from chunk fetches
    pending_events: Arc<Mutex<Vec<Event>>>,
    /// Default chunk size from configuration
    chunk_size: usize,
    /// Table processing order
    table_order: Vec<String>,
}

impl SnapshotProgressTracker {
    /// Create a new snapshot progress tracker.
    pub fn new(
        snapshot_id: String,
        created_at: u64,
        tables: Vec<String>,
        config: SnapshotTrackerConfig,
    ) -> Self {
        let mut progress = SnapshotProgress::new(snapshot_id, created_at);
        for table in &tables {
            progress.add_table(table.clone());
        }

        Self {
            progress: Arc::new(Mutex::new(progress)),
            pending_events: Arc::new(Mutex::new(Vec::new())),
            chunk_size: config.chunk_size,
            table_order: tables,
        }
    }

    /// Default chunk size from the tracker configuration.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Number of tables registered with this tracker.
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
pub struct SnapshotTrackerReport {
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
        let state = SnapshotProgressTracker::new("snap1".into(), 1000, tables, Default::default());

        assert_eq!(state.table_count(), 3);
        assert!(!state.all_complete().unwrap());
    }

    #[test]
    fn test_parallel_snapshot_state_progress() {
        let tables = vec!["users".into(), "orders".into()];
        let state = SnapshotProgressTracker::new("snap1".into(), 1000, tables, Default::default());

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
        let state = SnapshotProgressTracker::new("snap1".into(), 1000, tables, Default::default());

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
        let state = SnapshotProgressTracker::new("snap1".into(), 1000, tables, Default::default());

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
        let state = SnapshotProgressTracker::new("snap1".into(), 1000, tables, Default::default());

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
        let config = SnapshotTrackerConfig::default();
        assert_eq!(config.chunk_size, 5000);
    }

    // ---- C-05: interleaving, partial failure, checkpoint resume ----

    /// Two simulated workers interleave chunk progress on different tables.
    /// Neither should interfere with the other's row counts.
    #[test]
    fn test_interleaved_chunk_progress() {
        let tables = vec!["users".into(), "orders".into()];
        let tracker = SnapshotProgressTracker::new("snap-x".into(), 0, tables, Default::default());

        // Alternate chunk reports between the two tables (simulates concurrent workers).
        tracker
            .record_chunk_progress("users", 100, Some(b"cursor-users-1".to_vec()))
            .unwrap();
        tracker
            .record_chunk_progress("orders", 200, Some(b"cursor-orders-1".to_vec()))
            .unwrap();
        tracker
            .record_chunk_progress("users", 50, Some(b"cursor-users-2".to_vec()))
            .unwrap();
        tracker.record_chunk_progress("orders", 150, None).unwrap();

        assert_eq!(tracker.total_rows_processed().unwrap(), 500);
        assert!(!tracker.all_complete().unwrap());

        tracker.mark_table_complete("users").unwrap();
        tracker.mark_table_complete("orders").unwrap();

        assert!(tracker.all_complete().unwrap());
        assert_eq!(tracker.progress_percent().unwrap(), 100);
    }

    /// Recording progress for an unknown table returns an error rather than
    /// silently swallowing the problem.
    #[test]
    fn test_unknown_table_returns_error() {
        let tables = vec!["users".into()];
        let tracker = SnapshotProgressTracker::new("snap-y".into(), 0, tables, Default::default());

        let err = tracker.record_chunk_progress("nonexistent", 10, None);
        assert!(
            err.is_err(),
            "recording progress for unknown table must fail"
        );

        // The existing table is unaffected.
        assert_eq!(tracker.total_rows_processed().unwrap(), 0);
    }

    /// Checkpoint resume: encode current progress, decode it, and verify that
    /// completed tables are reflected correctly in the restored progress.
    #[test]
    fn test_checkpoint_resume_via_encode_decode() {
        use crate::source::snapshot_progress::SnapshotCheckpointHelper;

        let tables = vec!["a".into(), "b".into(), "c".into()];
        let tracker = SnapshotProgressTracker::new("snap-z".into(), 42, tables, Default::default());

        // Complete two of three tables, with a cursor token on the partial one.
        tracker
            .record_chunk_progress("a", 300, Some(b"pk-300".to_vec()))
            .unwrap();
        tracker.mark_table_complete("a").unwrap();
        tracker
            .record_chunk_progress("b", 150, Some(b"pk-150".to_vec()))
            .unwrap();
        tracker.mark_table_complete("b").unwrap();
        tracker
            .record_chunk_progress("c", 50, Some(b"pk-50".to_vec()))
            .unwrap();
        // "c" intentionally left incomplete — simulates a mid-run checkpoint.

        let snapshot = tracker.get_progress().unwrap();
        let encoded = SnapshotCheckpointHelper::serialize_progress(&snapshot).unwrap();
        let restored = SnapshotCheckpointHelper::deserialize_progress(&encoded).unwrap();

        assert_eq!(restored.completed_tables(), 2);
        assert_eq!(restored.total_rows_processed(), 500);
        assert!(!restored.is_all_complete(), "c is still pending");

        let pending = restored.get_pending_tables();
        assert_eq!(pending, vec!["c".to_string()]);

        // Verify the cursor token for "c" survived round-trip.
        let c_progress = restored.get_table_progress("c").unwrap();
        assert_eq!(c_progress.cursor_token, Some(b"pk-50".to_vec()));
    }

    /// Behavioral identity: continuing from a restored checkpoint should produce
    /// the same final progress state as running uninterrupted.
    #[test]
    fn test_checkpoint_restore_matches_continuous_progression() {
        use crate::source::snapshot_progress::SnapshotCheckpointHelper;

        let tables = vec!["users".into(), "orders".into()];

        // Path A: uninterrupted progression.
        let continuous = SnapshotProgressTracker::new(
            "snap-identical".into(),
            100,
            tables.clone(),
            Default::default(),
        );
        continuous
            .record_chunk_progress("users", 100, Some(b"u-1".to_vec()))
            .unwrap();
        continuous
            .record_chunk_progress("users", 80, Some(b"u-2".to_vec()))
            .unwrap();
        continuous
            .record_chunk_progress("orders", 200, Some(b"o-1".to_vec()))
            .unwrap();
        continuous.mark_table_complete("users").unwrap();
        continuous
            .record_chunk_progress("orders", 50, Some(b"o-2".to_vec()))
            .unwrap();
        continuous.mark_table_complete("orders").unwrap();

        // Path B: checkpoint mid-run, restore, then continue.
        let before_restore = SnapshotProgressTracker::new(
            "snap-identical".into(),
            100,
            tables,
            Default::default(),
        );
        before_restore
            .record_chunk_progress("users", 100, Some(b"u-1".to_vec()))
            .unwrap();
        before_restore
            .record_chunk_progress("users", 80, Some(b"u-2".to_vec()))
            .unwrap();
        before_restore
            .record_chunk_progress("orders", 200, Some(b"o-1".to_vec()))
            .unwrap();
        before_restore.mark_table_complete("users").unwrap();

        let encoded = SnapshotCheckpointHelper::serialize_progress(
            &before_restore.get_progress().unwrap(),
        )
        .unwrap();
        let restored = SnapshotCheckpointHelper::deserialize_progress(&encoded).unwrap();

        let resumed = SnapshotProgressTracker::new(
            "snap-identical".into(),
            100,
            vec!["users".into(), "orders".into()],
            Default::default(),
        );
        *resumed.progress.lock().unwrap() = restored;

        resumed
            .record_chunk_progress("orders", 50, Some(b"o-2".to_vec()))
            .unwrap();
        resumed.mark_table_complete("orders").unwrap();

        let final_a = continuous.get_progress().unwrap();
        let final_b = resumed.get_progress().unwrap();

        assert_eq!(final_a.snapshot_id, final_b.snapshot_id);
        assert_eq!(final_a.created_at, final_b.created_at);
        assert_eq!(final_a.total_rows_processed(), final_b.total_rows_processed());
        assert_eq!(final_a.completed_tables(), final_b.completed_tables());
        assert_eq!(final_a.get_pending_tables(), final_b.get_pending_tables());

        for table in ["users", "orders"] {
            let a = final_a.get_table_progress(table).unwrap();
            let b = final_b.get_table_progress(table).unwrap();
            assert_eq!(a.row_count, b.row_count, "row_count mismatch for {table}");
            assert_eq!(
                a.chunk_index, b.chunk_index,
                "chunk_index mismatch for {table}"
            );
            assert_eq!(
                a.cursor_token, b.cursor_token,
                "cursor mismatch for {table}"
            );
            assert_eq!(
                a.is_complete, b.is_complete,
                "completion mismatch for {table}"
            );
        }
    }
}
