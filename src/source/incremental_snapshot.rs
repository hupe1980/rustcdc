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

pub use driver::{
    BracketPosition, ChunkRow, IncrementalSnapshotBackend, IncrementalSnapshotDriver,
    SnapshotTable,
};

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
    /// Whether chunk reading is suspended.
    ///
    /// Set by [`CdcRuntime::pause_incremental_snapshot`](crate::CdcRuntime::pause_incremental_snapshot).
    /// The live stream is unaffected — only the next chunk read is withheld — and the flag
    /// travels in the checkpoint alongside the cursors, so a pause survives a restart.
    /// Without that it would silently un-pause on the next deploy, which for a backfill
    /// paused to protect a production primary is the opposite of what was asked for.
    ///
    /// `#[serde(default)]`, so a checkpoint written before this field existed loads as
    /// "not paused".
    #[serde(default)]
    pub paused: bool,
    /// Whether the snapshot was **abandoned** by
    /// [`CdcRuntime::stop_incremental_snapshot`](crate::CdcRuntime::stop_incremental_snapshot).
    ///
    /// This has to be recorded explicitly rather than inferred from an empty `tables`,
    /// and getting that wrong made `stop` silently ineffective across a restart. A stop
    /// clears the per-table entries, and the driver seeds one entry per **configured**
    /// table on startup: a table absent from the persisted state looks like a table that
    /// has not started, so every statically configured table restarted from row zero on
    /// the next deploy — re-running the whole backfill an operator had just stopped,
    /// typically to take load off a production primary.
    ///
    /// With the flag, absence and abandonment are distinguishable. A stopped snapshot
    /// stays stopped until tables are re-requested through
    /// [`CdcRuntime::request_incremental_snapshot`](crate::CdcRuntime::request_incremental_snapshot),
    /// which clears it.
    ///
    /// `#[serde(default)]`, so a checkpoint written before this field existed loads as
    /// "not stopped" — the previous behaviour, which is the right default for a state
    /// written by a build that had no way to express a stop.
    #[serde(default)]
    pub stopped: bool,
    /// How many times snapshot work has been (re)requested on this driver.
    ///
    /// Included in every snapshot `Read` event's synthetic
    /// [`SourceMetadata::offset`](crate::core::SourceMetadata::offset), which is what makes a
    /// **deliberate re-snapshot distinguishable from a replay**.
    ///
    /// Without it the two are identical. A snapshot read's offset identifies the row rather
    /// than a log position, so re-reading an unchanged row produces a byte-identical event —
    /// and the runtime's idempotency guard, which is on by default, correctly classified it as
    /// a duplicate and dropped it. An operator who re-requested a table got `enqueued: 1` and
    /// no rows: the guard whose job is to protect delivery silently discarded the delivery
    /// that was asked for.
    ///
    /// Bumping it per request keeps both behaviours: a chunk re-read after a mid-snapshot
    /// reconnect stays in the same generation and is still deduplicated, while a new request
    /// starts a generation whose rows cannot collide with the previous one's.
    ///
    /// Persisted, so the offsets stay stable across a restart as their documentation promises.
    ///
    /// `#[serde(default)]`, so a checkpoint written before this field existed loads as
    /// generation 0.
    #[serde(default)]
    pub generation: u32,
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
    /// The row filter actually in effect for this table, if any.
    ///
    /// Reported so the answer to "did my filter take effect?" is **observable rather than
    /// inferable**. Without it, an operator looking at `orders: 3,000,000 rows emitted` has
    /// no way to tell a filter that applied from one that was silently ignored — and one that
    /// was silently ignored is exactly the defect this crate shipped for on-demand snapshots
    /// before 0.12.0. A gauge or a status field built on this makes the same class of defect
    /// visible the next time rather than discoverable only by volume.
    ///
    /// Reflects the merged result: a per-request condition from
    /// [`SnapshotRequest`](crate::source::SnapshotRequest) where one was given, otherwise the
    /// configured [`IncrementalSnapshotConfig::table_conditions`] entry, otherwise `None`.
    ///
    /// `#[serde(default)]`, so a checkpoint written before this field existed loads as "no
    /// filter" — which is what it means for a state that could not record one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
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

    /// Rows emitted across every table, for a single progress number.
    pub fn rows_emitted(&self) -> u64 {
        self.tables.iter().map(|table| table.rows_emitted).sum()
    }

    /// Tables still to finish, for a single progress number.
    pub fn tables_remaining(&self) -> usize {
        self.tables
            .iter()
            .filter(|table| !table.is_complete)
            .count()
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
            paused: false,
            stopped: false,
            generation: 0,
            snapshot_id: "incremental-42".into(),
            tables: vec![
                IncrementalSnapshotTableState {
                    table: "public.users".into(),
                    pk_cursor: Some(vec![serde_json::json!("500")]),
                    is_complete: false,
                    chunks_emitted: 3,
                    rows_emitted: 1500,
                    condition: None,
                },
                IncrementalSnapshotTableState {
                    table: "public.orders".into(),
                    pk_cursor: None,
                    is_complete: true,
                    chunks_emitted: 9,
                    rows_emitted: 40_000,
                    condition: None,
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
