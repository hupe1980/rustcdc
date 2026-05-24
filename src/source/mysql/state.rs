use mysql_async::Pool as MySqlPool;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::ddl_capture::CapturedDdl;

#[derive(Default)]
pub(super) struct ConnectionState {
    pub(super) pool: Option<MySqlPool>,
    pub(super) heartbeat_task: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone)]
pub(super) struct TableSnapshotState {
    pub(super) snapshot: super::TableSnapshot,
    pub(super) primary_key_columns: Vec<String>,
    pub(super) rows: Vec<serde_json::Value>,
    pub(super) next_row: usize,
    pub(super) live_query: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SnapshotCheckpointState {
    pub(super) snapshot_id: String,
    pub(super) snapshot_start_ts: u64,
    pub(super) binlog_file: String,
    pub(super) binlog_pos: u32,
    pub(super) gtid: String,
    pub(super) current_table: usize,
    pub(super) next_chunk_index: u32,
    pub(super) tables: Vec<super::TableSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamState {
    Starting,
    Streaming,
    Stopped,
}

/// Watermark pair describing the binlog positions at snapshot start and stream start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MysqlHandoff {
    pub(super) snapshot_binlog_file: String,
    pub(super) snapshot_binlog_pos: u32,
    pub(super) snapshot_gtid: String,
    pub(super) stream_start_binlog_file: String,
    pub(super) stream_start_binlog_pos: u32,
    pub(super) stream_start_gtid: String,
}

impl MysqlHandoff {
    /// Returns true when the stream start position is at or before the snapshot watermark,
    /// meaning no gap exists between snapshot and stream.
    pub(super) fn has_no_gap(&self) -> bool {
        compare_binlog_position(
            &self.stream_start_binlog_file,
            self.stream_start_binlog_pos,
            &self.snapshot_binlog_file,
            self.snapshot_binlog_pos,
        )
        .is_le()
    }
}

/// Compare two binlog positions (file + pos) and return their ordering.
/// Binlog files are ordered lexicographically (mysql-bin.000001 < mysql-bin.000002).
pub(super) fn compare_binlog_position(
    file_a: &str,
    pos_a: u32,
    file_b: &str,
    pos_b: u32,
) -> std::cmp::Ordering {
    match file_a.cmp(file_b) {
        std::cmp::Ordering::Equal => pos_a.cmp(&pos_b),
        other => other,
    }
}

#[derive(Debug, Clone)]
pub(super) struct MysqlStream {
    pub(super) binlog_file: String,
    pub(super) binlog_pos: u32,
    pub(super) gtid: String,
    pub(super) stream_state: StreamState,
}

#[derive(Debug, Clone)]
pub(super) struct MysqlRowChange {
    pub(super) schema: Option<String>,
    pub(super) table: String,
    pub(super) primary_key: Option<Vec<String>>,
    pub(super) before: Option<serde_json::Value>,
    pub(super) after: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub(super) enum MysqlBinlogMessage {
    Begin {
        tx_id: u64,
        timestamp_ms: u64,
    },
    WriteRows(MysqlRowChange),
    UpdateRows(MysqlRowChange),
    DeleteRows(MysqlRowChange),
    Xid {
        tx_id: u64,
        timestamp_ms: u64,
        binlog_file: String,
        binlog_pos: u32,
        gtid: Option<String>,
    },
    Rotate {
        binlog_file: String,
        binlog_pos: u32,
    },
    Gtid {
        gtid: String,
    },
    Ddl {
        captured: CapturedDdl,
        timestamp_ms: u64,
        binlog_file: String,
        binlog_pos: u32,
    },
    Heartbeat,
}
