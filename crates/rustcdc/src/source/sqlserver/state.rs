use tokio::task::JoinHandle;

use super::TableSnapshot;

#[derive(Default)]
pub(super) struct ConnectionState {
    pub(super) connected: bool,
    pub(super) heartbeat_task: Option<JoinHandle<()>>,
    pub(super) snapshot_lsn_start: Option<[u8; 10]>,
    pub(super) stream_lsn_start: Option<[u8; 10]>,
}

#[derive(Debug, Clone)]
pub(super) struct TableSnapshotState {
    pub(super) snapshot: TableSnapshot,
    pub(super) schema: String,
    pub(super) table: String,
    pub(super) primary_key_columns: Vec<String>,
    pub(super) column_names: Vec<String>,
    /// The table's declared columns, read alongside the names at snapshot start.
    ///
    /// The projection wants names; the rows' shape wants types. One read serves both, so the
    /// two cannot disagree about which columns the table has.
    pub(super) catalog_columns: Vec<crate::source::schema_catalog::CatalogColumn>,
    /// The shape the snapshot's rows carry, built from `catalog_columns` before the table's
    /// first chunk. `None` when the catalogue read nothing.
    pub(super) shape: Option<std::sync::Arc<crate::RowShape>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct SqlServerSnapshotCheckpointState {
    pub(super) snapshot_id: String,
    pub(super) lsn_start: [u8; 10],
    pub(super) current_table: usize,
    pub(super) next_chunk_index: u32,
    pub(super) tables: Vec<TableSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SqlServerHandoff {
    pub(super) snapshot_lsn_start: [u8; 10],
    pub(super) stream_lsn_start: [u8; 10],
}
