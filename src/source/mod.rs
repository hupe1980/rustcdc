//! Source traits, connector configuration, and feature-gated connector modules.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    checkpoint::Checkpoint,
    core::{Event, Offset, Result},
};

pub(crate) mod helpers;
pub mod snapshot_progress;
pub mod snapshot_tracker;
pub mod snapshot_validator;

pub use snapshot_progress::{SnapshotCheckpointHelper, SnapshotProgress, TableProgress};
pub use snapshot_tracker::{SnapshotProgressTracker, SnapshotTrackerConfig, SnapshotTrackerReport};
pub use snapshot_validator::{SnapshotValidationResult, SnapshotValidator};

// ─── Table filtering ─────────────────────────────────────────────────────────

/// Returns `true` if an event for `schema.table` should be forwarded to the caller.
///
/// * When `include_list` is non-empty, only listed tables pass through.
/// * When `include_list` is empty and `exclude_list` is non-empty, listed tables are dropped.
/// * When both lists are empty, all events pass through.
///
/// Table names are matched case-insensitively against `"schema.table"` tokens.
pub(crate) fn table_is_allowed(
    schema: Option<&str>,
    table: &str,
    include_list: &[String],
    exclude_list: &[String],
) -> bool {
    // Fast path: no filtering configured — avoids all allocations on the hot path.
    if include_list.is_empty() && exclude_list.is_empty() {
        return true;
    }

    let table_lower = table.to_lowercase();
    let qualified_lower = match schema {
        Some(s) => format!("{}.{table_lower}", s.to_lowercase()),
        None => table_lower.clone(),
    };

    let matches = |list: &[String]| {
        list.iter().any(|entry| {
            let e = entry.to_lowercase();
            e == qualified_lower || e == table_lower
        })
    };

    if !include_list.is_empty() {
        return matches(include_list);
    }
    !matches(exclude_list)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEnd {
    pub snapshot_end_ts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffResult {
    pub snapshot_end_ts: Option<u64>,
    pub stream_start_ts: Option<u64>,
    /// Number of overlap events dropped during handoff deduplication.
    pub overlap_events_dropped: u64,
    /// Optional source-specific watermark distance observed at handoff.
    ///
    /// For PostgreSQL this is an LSN delta in bytes. Connectors that cannot
    /// provide a reliable watermark distance should leave this as `None`.
    pub stream_watermark_gap: Option<u64>,
}

/// Configuration for incremental (non-blocking) snapshot using the DBLog watermark pattern.
///
/// Used by `PostgresConnection::start_incremental_snapshot`,
/// `MysqlConnection::start_incremental_snapshot`, and
/// `SqlServerConnection::start_incremental_snapshot`.
///
/// Unlike the blocking bulk snapshot, incremental snapshotting interleaves chunk reads
/// with the live replication stream. The stream never pauses, no long-held
/// `REPEATABLE READ` transaction accumulates transaction IDs, and each chunk is
/// independently resumable after a crash.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct IncrementalSnapshotConfig {
    /// Tables to snapshot in `"schema.table"` format. Tables are processed in order.
    pub tables: Vec<String>,
    /// Number of rows to read per chunk. Defaults to `5_000`.
    pub chunk_size: usize,
}

impl IncrementalSnapshotConfig {
    /// Create a new config with the given tables and the default chunk size (5,000).
    pub fn new(tables: impl Into<Vec<String>>) -> Self {
        Self {
            tables: tables.into(),
            chunk_size: 5_000,
        }
    }

    /// Override the per-chunk row limit.
    #[must_use]
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }
}

/// Declares connector feature support for runtime and embedder introspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ConnectorCapabilities {
    pub snapshot: bool,
    pub snapshot_checkpoint_resume: bool,
    pub handoff: bool,
    pub ddl_capture: bool,
    pub heartbeat: bool,
    pub tls: bool,
    pub schema_introspection: bool,
    /// Whether the connector surfaces `TRUNCATE` operations as
    /// [`crate::core::Operation::Truncate`] events.
    pub truncate: bool,
    /// Whether the connector supports non-blocking incremental snapshot via the
    /// DBLog watermark pattern (`PostgresConnection::start_incremental_snapshot`).
    pub incremental_snapshot: bool,
}

impl ConnectorCapabilities {
    /// Capability set for disabled or unknown sources.
    pub const fn none() -> Self {
        Self {
            snapshot: false,
            snapshot_checkpoint_resume: false,
            handoff: false,
            ddl_capture: false,
            heartbeat: false,
            tls: false,
            schema_introspection: false,
            truncate: false,
            incremental_snapshot: false,
        }
    }
}

#[async_trait]
pub trait SnapshotHandle: Send + Sync {
    async fn next_chunk(&mut self, chunk_size: usize) -> Result<Vec<Event>>;
    async fn checkpoint(
        &self,
        checkpoint: &mut dyn Checkpoint,
        committed_event_count: u64,
    ) -> Result<()>;
    async fn finish(&mut self) -> Result<SnapshotEnd>;
}

#[async_trait]
pub trait StreamHandle: Send + Sync {
    async fn next_events(&mut self, timeout_ms: u64) -> Result<Vec<Event>>;
    async fn save_position(&self, checkpoint: &mut dyn Checkpoint) -> Result<()>;
    /// Requeue events so they are returned by a subsequent `next_events` call.
    ///
    /// This is used by snapshot-to-stream handoff to prefetch overlap events,
    /// apply deduplication, and preserve forward delivery order.
    async fn requeue_events(&mut self, _events: Vec<Event>) -> Result<()> {
        Ok(())
    }
    /// Confirm that all messages up to `lsn` have been durably consumed.
    /// Prevents WAL retention bloat on replication slots.
    async fn confirm_lsn(&mut self, lsn: u64) -> Result<()>;
}

#[async_trait]
pub trait Source: Send + Sync {
    async fn start_snapshot(&mut self, tables: &[&str]) -> Result<Box<dyn SnapshotHandle>>;
    /// Start snapshot capture from a previously persisted snapshot checkpoint.
    ///
    /// Default implementation falls back to `start_snapshot`, which preserves
    /// backwards behavior for source implementations that do not need explicit
    /// resume handling.
    async fn start_snapshot_from_checkpoint(
        &mut self,
        tables: &[&str],
        _resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn SnapshotHandle>> {
        self.start_snapshot(tables).await
    }
    async fn start_stream(
        &mut self,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>>;
    async fn perform_handoff(
        &mut self,
        snapshot: &mut dyn SnapshotHandle,
        stream: &mut dyn StreamHandle,
    ) -> Result<HandoffResult>;
    fn source_type(&self) -> &str;
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::none()
    }
}

#[cfg(feature = "mariadb")]
pub mod mariadb;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlserver")]
pub mod sqlserver;
#[cfg(feature = "sqlserver")]
pub use sqlserver::SqlServerConnection;
#[cfg(feature = "sqlserver")]
pub use sqlserver::SqlServerSourceConfig;

#[cfg(feature = "mariadb")]
pub use mariadb::{
    MariaDbConnection, MariaDbIncrementalSnapshotHandle, MariaDbSnapshotHandle,
    MariaDbSourceConfig, MariaDbStreamHandle,
};
#[cfg(feature = "mysql")]
pub use mysql::incremental_snapshot::MysqlIncrementalSnapshotHandle;
#[cfg(feature = "mysql")]
pub use mysql::MysqlConnection;
#[cfg(feature = "mysql")]
pub use mysql::{MysqlSourceConfig, ServerFlavor};
#[cfg(feature = "postgres")]
pub use postgres::incremental_snapshot::IncrementalSnapshotHandle;
#[cfg(feature = "postgres")]
pub use postgres::PostgresConnection;
#[cfg(feature = "postgres")]
pub use postgres::PostgresSourceConfig;
#[cfg(feature = "sqlserver")]
pub use sqlserver::incremental_snapshot::SqlServerIncrementalSnapshotHandle;

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::json;

    use crate::{
        checkpoint::{Checkpoint, InMemoryCheckpoint},
        core::{Event, Offset, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION},
    };

    use super::{
        ConnectorCapabilities, HandoffResult, SnapshotEnd, SnapshotHandle, Source, StreamHandle,
    };

    fn sample_event() -> Event {
        Event {
            before: None,
            after: Some(json!({"id": 1})),
            op: Operation::Read,
            source: SourceMetadata {
                source_name: "mock".to_string(),
                offset: "1".to_string(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".to_string()),
            table: "users".to_string(),
            primary_key: Some(vec!["id".to_string()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    struct MockSnapshot;

    #[async_trait]
    impl SnapshotHandle for MockSnapshot {
        async fn next_chunk(&mut self, _chunk_size: usize) -> crate::core::Result<Vec<Event>> {
            Ok(vec![sample_event()])
        }

        async fn checkpoint(
            &self,
            _checkpoint: &mut dyn Checkpoint,
            _committed_event_count: u64,
        ) -> crate::core::Result<()> {
            Ok(())
        }

        async fn finish(&mut self) -> crate::core::Result<SnapshotEnd> {
            Ok(SnapshotEnd {
                snapshot_end_ts: 42,
            })
        }
    }

    struct MockStream;

    #[async_trait]
    impl StreamHandle for MockStream {
        async fn next_events(&mut self, _timeout_ms: u64) -> crate::core::Result<Vec<Event>> {
            Ok(vec![sample_event()])
        }

        async fn save_position(&self, _checkpoint: &mut dyn Checkpoint) -> crate::core::Result<()> {
            Ok(())
        }

        async fn confirm_lsn(&mut self, _lsn: u64) -> crate::core::Result<()> {
            Ok(())
        }
    }

    struct MockSource;

    #[async_trait]
    impl Source for MockSource {
        async fn start_snapshot(
            &mut self,
            _tables: &[&str],
        ) -> crate::core::Result<Box<dyn SnapshotHandle>> {
            Ok(Box::new(MockSnapshot))
        }

        async fn start_stream(
            &mut self,
            _resume_from: Option<&dyn Offset>,
        ) -> crate::core::Result<Box<dyn StreamHandle>> {
            Ok(Box::new(MockStream))
        }

        async fn perform_handoff(
            &mut self,
            _snapshot: &mut dyn SnapshotHandle,
            _stream: &mut dyn StreamHandle,
        ) -> crate::core::Result<HandoffResult> {
            Ok(HandoffResult {
                snapshot_end_ts: Some(42),
                stream_start_ts: Some(43),
                overlap_events_dropped: 0,
                stream_watermark_gap: None,
            })
        }

        fn source_type(&self) -> &str {
            "mock"
        }

        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities {
                snapshot: true,
                snapshot_checkpoint_resume: true,
                handoff: true,
                ddl_capture: false,
                heartbeat: false,
                tls: false,
                schema_introspection: true,
                truncate: false,
                incremental_snapshot: false,
            }
        }
    }

    #[tokio::test]
    async fn stream_default_requeue_is_noop_success() {
        let mut stream = MockStream;
        stream.requeue_events(vec![sample_event()]).await.unwrap();
    }

    #[tokio::test]
    async fn source_trait_round_trip_mock_handles() {
        let mut source = MockSource;
        let mut snapshot = source.start_snapshot(&["users"]).await.unwrap();
        let mut stream = source.start_stream(None).await.unwrap();

        let snapshot_chunk = snapshot.next_chunk(10).await.unwrap();
        let stream_chunk = stream.next_events(10).await.unwrap();
        let handoff = source
            .perform_handoff(snapshot.as_mut(), stream.as_mut())
            .await
            .unwrap();

        assert_eq!(source.source_type(), "mock");
        assert_eq!(snapshot_chunk.len(), 1);
        assert_eq!(stream_chunk.len(), 1);
        assert_eq!(handoff.snapshot_end_ts, Some(42));
        assert_eq!(handoff.stream_start_ts, Some(43));
        assert_eq!(handoff.overlap_events_dropped, 0);
        assert_eq!(handoff.stream_watermark_gap, None);
        assert!(source.capabilities().snapshot);
    }

    #[tokio::test]
    async fn snapshot_checkpoint_and_finish_paths_are_callable() {
        let mut snapshot = MockSnapshot;
        let mut checkpoint = InMemoryCheckpoint::default();

        snapshot.checkpoint(&mut checkpoint, 1).await.unwrap();
        let end = snapshot.finish().await.unwrap();
        assert_eq!(end.snapshot_end_ts, 42);
    }
}
