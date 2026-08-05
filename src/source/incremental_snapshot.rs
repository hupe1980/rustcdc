//! Durable, connector-agnostic progress state for DBLog incremental snapshots.
//!
//! # Why this exists
//!
//! The incremental (non-blocking) snapshot reads a table in keyset-paginated chunks
//! while the replication stream keeps running. Each connector tracks a per-table
//! keyset cursor in memory. Without persisting that cursor, a restart re-reads every
//! configured table **from row zero** — an uncontrolled duplicate flood proportional
//! to the whole dataset, not to the crash window, and one that repeats on every
//! restart until the snapshot happens to finish inside a single process lifetime.
//!
//! The state travels inside the connector's checkpoint offset (see
//! [`crate::checkpoint::PostgresOffset::incremental_snapshot`] and its MySQL and
//! SQL Server counterparts) so it is written by the same atomic, fsynced,
//! checksummed record as the stream position. That coupling is deliberate: a chunk
//! cursor is only meaningful relative to the stream position it was captured
//! against, and two separately-written files could disagree after a crash between
//! them.
//!
//! # Resume semantics
//!
//! Resuming re-reads the chunk that was in flight when the process stopped, because
//! the cursor advances only once a chunk has been fully emitted. That is
//! at-least-once — the same guarantee the rest of the pipeline provides — and is
//! bounded by `chunk_size` rather than by table size.

mod driver;

pub use driver::{ChunkRow, IncrementalSnapshotBackend, IncrementalSnapshotDriver, SnapshotTable};

use serde::{Deserialize, Serialize};

/// Durable progress of an in-flight incremental snapshot.
///
/// Persisted inside the connector checkpoint offset and handed back to
/// `start_incremental_snapshot` on restart.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementalSnapshotState {
    /// Stable identifier for this snapshot run, carried on every emitted
    /// [`crate::core::SnapshotMetadata::snapshot_id`].
    ///
    /// Preserved across restarts so a consumer correlating rows by snapshot id sees
    /// one snapshot, not one per process lifetime.
    pub snapshot_id: String,
    /// Per-table progress, one entry per configured table.
    pub tables: Vec<IncrementalSnapshotTableState>,
}

/// Per-table progress within an [`IncrementalSnapshotState`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementalSnapshotTableState {
    /// Table in `"schema.table"` form, matching the configured entry.
    pub table: String,
    /// Keyset cursor: the primary-key values of the last row of the last **fully
    /// emitted** chunk. `None` means the table has not produced a chunk yet.
    ///
    /// Scalar JSON values only — the same constraint the connectors' chunk SELECTs
    /// impose when binding the cursor.
    pub pk_cursor: Option<Vec<serde_json::Value>>,
    /// Whether this table has been read to exhaustion.
    pub is_complete: bool,
    /// Number of chunks emitted so far, used to continue
    /// [`crate::core::SnapshotMetadata::chunk_index`] rather than restart it at 0.
    pub chunks_emitted: u32,
    /// Number of rows emitted so far, for progress reporting.
    pub rows_emitted: u64,
}

impl IncrementalSnapshotState {
    /// Look up the persisted progress for `table` (`"schema.table"`, case-insensitive).
    pub fn table(&self, table: &str) -> Option<&IncrementalSnapshotTableState> {
        self.tables
            .iter()
            .find(|entry| entry.table.eq_ignore_ascii_case(table))
    }

    /// Whether every table in the state is complete.
    ///
    /// A resumed handle whose tables are all complete emits no snapshot rows and
    /// degenerates to a pass-through of the underlying stream.
    pub fn is_complete(&self) -> bool {
        !self.tables.is_empty() && self.tables.iter().all(|table| table.is_complete)
    }
}

/// Recover the persisted incremental-snapshot state from a checkpoint offset.
///
/// Returns `None` when the offset carries no state — a fresh start, or a resume
/// from a checkpoint written before the snapshot began.
///
/// The offset payload is the connector's own JSON encoding; every connector that
/// supports incremental snapshot stores the state under the same
/// `incremental_snapshot` key, so one reader serves all of them. An offset whose
/// payload is not an object (SQL Server's pre-0.8 bare-string form, for example)
/// yields `None` rather than an error: a missing cursor is a correctness-neutral
/// restart from the beginning, and refusing to start would be a worse outcome than
/// re-reading.
pub fn state_from_offset(
    offset: Option<&dyn crate::core::Offset>,
) -> Option<IncrementalSnapshotState> {
    let offset = offset?;
    let encoded = offset.encode().ok()?;
    let value: serde_json::Value = serde_json::from_slice(&encoded).ok()?;
    let state = value.get("incremental_snapshot")?;
    serde_json::from_value(state.clone()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::PostgresOffset;

    fn state() -> IncrementalSnapshotState {
        IncrementalSnapshotState {
            snapshot_id: "incremental-42".into(),
            tables: vec![
                IncrementalSnapshotTableState {
                    table: "public.users".into(),
                    pk_cursor: Some(vec![serde_json::json!("500")]),
                    is_complete: false,
                    chunks_emitted: 3,
                    rows_emitted: 1500,
                },
                IncrementalSnapshotTableState {
                    table: "public.orders".into(),
                    pk_cursor: None,
                    is_complete: true,
                    chunks_emitted: 9,
                    rows_emitted: 40_000,
                },
            ],
        }
    }

    #[test]
    fn state_round_trips_through_a_postgres_offset() {
        let offset = PostgresOffset {
            lsn: 9_001,
            slot_name: "slot".into(),
            incremental_snapshot: Some(state()),
        };

        let recovered = state_from_offset(Some(&offset)).expect("state should survive the offset");
        assert_eq!(recovered, state());
        assert_eq!(
            recovered.table("PUBLIC.USERS").map(|t| t.chunks_emitted),
            Some(3),
            "table lookup should be case-insensitive"
        );
    }

    #[test]
    fn an_offset_without_state_yields_none_rather_than_an_error() {
        let offset = PostgresOffset {
            lsn: 9_001,
            slot_name: "slot".into(),
            incremental_snapshot: None,
        };
        assert!(state_from_offset(Some(&offset)).is_none());
        assert!(state_from_offset(None).is_none());
    }

    #[test]
    fn is_complete_requires_every_table_and_rejects_the_empty_set() {
        let mut s = state();
        assert!(!s.is_complete());
        s.tables[0].is_complete = true;
        assert!(s.is_complete());
        assert!(
            !IncrementalSnapshotState::default().is_complete(),
            "an empty state is not a completed snapshot"
        );
    }
}
