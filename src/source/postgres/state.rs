use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_postgres::Client;

use super::TableSnapshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamState {
    Starting,
    Streaming,
    Stopped,
}

#[derive(Debug, Clone)]
pub(super) struct PostgresStream {
    pub(super) slot_name: String,
    pub(super) publication_name: String,
    pub(super) lsn_position: u64,
    pub(super) replication_status: StreamState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PostgresHandoff {
    pub(super) snapshot_watermark: u64,
    pub(super) stream_watermark: u64,
    pub(super) handoff_complete: bool,
}

#[derive(Default)]
pub(super) struct ConnectionState {
    pub(super) client: Option<Arc<Client>>,
    pub(super) connection_task: Option<JoinHandle<()>>,
    pub(super) heartbeat_task: Option<JoinHandle<()>>,
    pub(super) snapshot_watermark: Option<u64>,
    pub(super) stream_start_watermark: Option<u64>,
}

#[derive(Debug, Clone)]
pub(super) struct TableSnapshotState {
    pub(super) snapshot: TableSnapshot,
    pub(super) rows: Vec<serde_json::Value>,
    pub(super) next_row: usize,
    pub(super) live_query: bool,
    pub(super) primary_key_columns: Vec<String>,
    pub(super) primary_key_types: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct SnapshotCheckpointState {
    pub(super) snapshot_id: String,
    pub(super) snapshot_start_ts: u64,
    pub(super) snapshot_end_ts: u64,
    pub(super) snapshot_watermark: u64,
    pub(super) current_table: usize,
    pub(super) next_chunk_index: u32,
    pub(super) tables: Vec<TableSnapshot>,
}
