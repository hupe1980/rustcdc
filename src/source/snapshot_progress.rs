//! Resumable snapshot progress tracking and checkpoint persistence.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::core::{Error, Result};

/// Per-table snapshot progress tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableProgress {
    pub table_name: String,
    pub row_count: u64,
    pub chunk_index: u64,
    pub cursor_token: Option<Vec<u8>>, // Opaque keyset pagination token for resumption
    pub is_complete: bool,
}

impl TableProgress {
    /// Create a new table progress tracker.
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            row_count: 0,
            chunk_index: 0,
            cursor_token: None,
            is_complete: false,
        }
    }

    /// Record progress after a chunk is processed.
    pub fn record_chunk(&mut self, chunk_size: usize, cursor_token: Option<Vec<u8>>) {
        self.row_count += chunk_size as u64;
        self.chunk_index += 1;
        self.cursor_token = cursor_token;
    }

    /// Mark this table as complete.
    pub fn mark_complete(&mut self) {
        self.is_complete = true;
    }
}

/// Snapshot progress tracking across multiple tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotProgress {
    pub snapshot_id: String,
    pub created_at: u64,
    pub table_progress: HashMap<String, TableProgress>,
}

impl SnapshotProgress {
    /// Create a new snapshot progress tracker.
    pub fn new(snapshot_id: String, created_at: u64) -> Self {
        Self {
            snapshot_id,
            created_at,
            table_progress: HashMap::new(),
        }
    }

    /// Add a table to track (for multi-table snapshots).
    pub fn add_table(&mut self, table_name: String) {
        self.table_progress
            .insert(table_name.clone(), TableProgress::new(table_name));
    }

    /// Get progress for a specific table.
    pub fn get_table_progress(&self, table: &str) -> Option<&TableProgress> {
        self.table_progress.get(table)
    }

    /// Mutable access to table progress.
    pub fn get_table_progress_mut(&mut self, table: &str) -> Option<&mut TableProgress> {
        self.table_progress.get_mut(table)
    }

    /// Record that a chunk was processed for a table.
    pub fn record_table_chunk(
        &mut self,
        table: &str,
        chunk_size: usize,
        cursor_token: Option<Vec<u8>>,
    ) -> Result<()> {
        if let Some(progress) = self.table_progress.get_mut(table) {
            progress.record_chunk(chunk_size, cursor_token);
            Ok(())
        } else {
            Err(Error::StateError(format!(
                "Table {} not found in snapshot progress",
                table
            )))
        }
    }

    /// Mark a table as complete.
    pub fn mark_table_complete(&mut self, table: &str) -> Result<()> {
        if let Some(progress) = self.table_progress.get_mut(table) {
            progress.mark_complete();
            Ok(())
        } else {
            Err(Error::StateError(format!(
                "Table {} not found in snapshot progress",
                table
            )))
        }
    }

    /// Check if all tables have completed.
    pub fn is_all_complete(&self) -> bool {
        if self.table_progress.is_empty() {
            return false;
        }
        self.table_progress.values().all(|p| p.is_complete)
    }

    /// Encode progress to bytes for checkpoint persistence.
    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| {
            Error::SerializationError(format!("Failed to encode snapshot progress: {}", e))
        })
    }

    /// Decode progress from bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data).map_err(|e| {
            Error::SerializationError(format!("Failed to decode snapshot progress: {}", e))
        })
    }

    /// Get tables that still need to be processed (not complete).
    pub fn get_pending_tables(&self) -> Vec<String> {
        self.table_progress
            .values()
            .filter(|p| !p.is_complete)
            .map(|p| p.table_name.clone())
            .collect()
    }

    /// Get total rows processed across all tables.
    pub fn total_rows_processed(&self) -> u64 {
        self.table_progress.values().map(|p| p.row_count).sum()
    }

    /// Get total tables.
    pub fn total_tables(&self) -> usize {
        self.table_progress.len()
    }

    /// Get completed tables count.
    pub fn completed_tables(&self) -> usize {
        self.table_progress
            .values()
            .filter(|p| p.is_complete)
            .count()
    }
}

/// Helper to persist snapshot progress to file or external storage.
/// Note: For integration with checkpoint, the snapshot progress should be encoded
/// into the Offset structure or stored separately in application state.
#[derive(Debug)]
pub struct SnapshotCheckpointHelper;

impl SnapshotCheckpointHelper {
    /// Encode snapshot progress for storage.
    /// This can be called from application code to serialize progress
    /// for storage in a checkpoint store (as part of application state).
    pub fn serialize_progress(progress: &SnapshotProgress) -> Result<Vec<u8>> {
        progress.encode()
    }

    /// Decode snapshot progress from storage.
    pub fn deserialize_progress(data: &[u8]) -> Result<SnapshotProgress> {
        SnapshotProgress::decode(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_progress_creation() {
        let progress = TableProgress::new("users".into());
        assert_eq!(progress.table_name, "users");
        assert_eq!(progress.row_count, 0);
        assert_eq!(progress.chunk_index, 0);
        assert!(!progress.is_complete);
    }

    #[test]
    fn test_table_progress_record_chunk() {
        let mut progress = TableProgress::new("users".into());
        progress.record_chunk(100, Some(vec![1, 2, 3]));

        assert_eq!(progress.row_count, 100);
        assert_eq!(progress.chunk_index, 1);
        assert_eq!(progress.cursor_token, Some(vec![1, 2, 3]));

        progress.record_chunk(100, None);
        assert_eq!(progress.row_count, 200);
        assert_eq!(progress.chunk_index, 2);
    }

    #[test]
    fn test_snapshot_progress_creation() {
        let progress = SnapshotProgress::new("snap1".into(), 1000);
        assert_eq!(progress.snapshot_id, "snap1");
        assert_eq!(progress.created_at, 1000);
        assert!(progress.table_progress.is_empty());
    }

    #[test]
    fn test_snapshot_progress_add_table() {
        let mut progress = SnapshotProgress::new("snap1".into(), 1000);
        progress.add_table("users".into());
        progress.add_table("orders".into());

        assert_eq!(progress.total_tables(), 2);
        assert!(progress.get_table_progress("users").is_some());
        assert!(progress.get_table_progress("orders").is_some());
    }

    #[test]
    fn test_snapshot_progress_record_chunk() {
        let mut progress = SnapshotProgress::new("snap1".into(), 1000);
        progress.add_table("users".into());

        progress
            .record_table_chunk("users", 50, Some(vec![1, 2]))
            .unwrap();

        let table_prog = progress.get_table_progress("users").unwrap();
        assert_eq!(table_prog.row_count, 50);
        assert_eq!(table_prog.chunk_index, 1);
    }

    #[test]
    fn test_snapshot_progress_completion_tracking() {
        let mut progress = SnapshotProgress::new("snap1".into(), 1000);
        progress.add_table("users".into());
        progress.add_table("orders".into());

        assert!(!progress.is_all_complete());

        progress.mark_table_complete("users").unwrap();
        assert!(!progress.is_all_complete());

        progress.mark_table_complete("orders").unwrap();
        assert!(progress.is_all_complete());
    }

    #[test]
    fn test_snapshot_progress_pending_tables() {
        let mut progress = SnapshotProgress::new("snap1".into(), 1000);
        progress.add_table("users".into());
        progress.add_table("orders".into());
        progress.add_table("products".into());

        progress.mark_table_complete("users").unwrap();

        let pending = progress.get_pending_tables();
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"orders".to_string()));
        assert!(pending.contains(&"products".to_string()));
    }

    #[test]
    fn test_snapshot_progress_total_rows() {
        let mut progress = SnapshotProgress::new("snap1".into(), 1000);
        progress.add_table("users".into());
        progress.add_table("orders".into());

        progress.record_table_chunk("users", 100, None).unwrap();
        progress.record_table_chunk("orders", 50, None).unwrap();

        assert_eq!(progress.total_rows_processed(), 150);
    }

    #[test]
    fn test_snapshot_progress_encode_decode() {
        let mut progress = SnapshotProgress::new("snap1".into(), 1000);
        progress.add_table("users".into());
        progress
            .record_table_chunk("users", 100, Some(vec![1, 2, 3]))
            .unwrap();
        progress.mark_table_complete("users").unwrap();

        let encoded = progress.encode().unwrap();
        let decoded = SnapshotProgress::decode(&encoded).unwrap();

        assert_eq!(decoded.snapshot_id, "snap1");
        assert_eq!(decoded.created_at, 1000);
        assert_eq!(decoded.total_tables(), 1);
        assert!(decoded.is_all_complete());

        let table_prog = decoded.get_table_progress("users").unwrap();
        assert_eq!(table_prog.row_count, 100);
        assert_eq!(table_prog.cursor_token, Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_snapshot_progress_completed_tables_count() {
        let mut progress = SnapshotProgress::new("snap1".into(), 1000);
        progress.add_table("users".into());
        progress.add_table("orders".into());
        progress.add_table("products".into());

        assert_eq!(progress.completed_tables(), 0);

        progress.mark_table_complete("users").unwrap();
        assert_eq!(progress.completed_tables(), 1);

        progress.mark_table_complete("orders").unwrap();
        assert_eq!(progress.completed_tables(), 2);
    }
}
