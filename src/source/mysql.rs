//! MySQL source configuration, connection lifecycle, and validation helpers.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures_util::StreamExt;
use mysql_async::{prelude::Queryable, Pool as MySqlPool};
use mysql_async::{BinlogStream, BinlogStreamRequest, Conn as MySqlBinlogConn};
use mysql_common::{
    binlog::{
        events::{EventData, RowsEventData, TableMapEvent},
        row::BinlogRow,
    },
    value::Value as MysqlValue,
};
use tokio::{sync::Mutex, task::JoinHandle};

use crate::{
    checkpoint::{GenericOffset, MysqlOffset},
    core::{Error, Event, Offset, Result, SecretString, StructuredLogger, TransportConfig},
    ddl_capture::{extract_captured_ddl, DdlDialect},
    source::{
        ConnectorCapabilities, HandoffResult, IncrementalSnapshotConfig, SnapshotEnd,
        SnapshotHandle, Source, StreamHandle,
    },
};
use serde::{Deserialize, Serialize};

mod config;
mod handoff;
pub mod incremental_snapshot;
mod parser;
mod query;
mod snapshot_chunk;
mod snapshot_start;
mod state;
mod stream_messages;
mod stream_start;

use self::{
    parser::{mysql_qualified_table_name_from_reference, quoted_mysql_identifier},
    query::{
        binlog_row_to_mysql_row, format_gtid, mysql_json_value_to_param, mysql_row_to_json,
        mysql_value_to_json, primary_key_columns_from_row,
    },
    state::{
        ConnectionState, MysqlBinlogMessage, MysqlRowChange, MysqlStream, SnapshotCheckpointState,
        StreamState, TableSnapshotState,
    },
};
use crate::source::helpers::now_millis;
use handoff::mysql_handoff_result;
use snapshot_chunk::next_snapshot_chunk;
use snapshot_start::begin_snapshot_and_collect_table_states;
use stream_start::resolve_stream_start_position;

const HEARTBEAT_SECS: u64 = 60;
const DEFAULT_SNAPSHOT_CHUNK_SIZE: usize = 5_000;
const STREAM_POLL_INTERVAL_MS: u64 = 50;
const MAX_EVENTS_PER_POLL: usize = 1_000;

/// Identifies the server dialect for a MySQL-protocol connection.
///
/// Both MySQL and MariaDB use the same binlog wire protocol and `mysql_async`
/// driver. The flavor affects `source_type()` (and therefore checkpoint file
/// names) and structured log labels, but not connection or decoding logic.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ServerFlavor {
    #[default]
    Mysql,
    MariaDb,
}

impl ServerFlavor {
    /// The short name used as `source_type()` and in log labels.
    pub const fn source_name(self) -> &'static str {
        match self {
            Self::Mysql => "mysql",
            Self::MariaDb => "mariadb",
        }
    }

    /// The source type used for snapshot checkpoint offsets.
    pub const fn snapshot_source_name(self) -> &'static str {
        match self {
            Self::Mysql => "mysql_snapshot",
            Self::MariaDb => "mariadb_snapshot",
        }
    }
}

/// Configuration for a MySQL CDC connection.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MysqlSourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: SecretString,
    pub database: String,
    pub server_id: u32,
    pub gtid_mode_enabled: bool,
    pub binlog_format_check: bool,
    pub transport: TransportConfig,
    pub conn_timeout_secs: u64,
    /// Stream poll interval in milliseconds.
    pub stream_poll_interval_ms: u64,
    /// Maximum events yielded by a single stream poll cycle.
    pub max_events_per_poll: usize,
    /// Identifies the server dialect (MySQL or MariaDB).
    ///
    /// Defaults to `ServerFlavor::Mysql`. Set to `ServerFlavor::MariaDb` when
    /// connecting to a MariaDB server so that `source_type()` returns `"mariadb"`
    /// and checkpoints use a separate `checkpoint_mariadb.json` file.
    #[serde(default)]
    pub server_flavor: ServerFlavor,
    /// Allowlist of tables to stream, in `"schema.table"` format.
    ///
    /// When non-empty, only tables in this list are forwarded to the caller.
    /// Takes precedence over [`table_exclude_list`](MysqlSourceConfig::table_exclude_list).
    /// An empty list means *all* tables are included.
    pub table_include_list: Vec<String>,
    /// Blocklist of tables to suppress, in `"schema.table"` format.
    ///
    /// Ignored when [`table_include_list`](MysqlSourceConfig::table_include_list) is non-empty.
    /// An empty list means no tables are excluded.
    pub table_exclude_list: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSnapshot {
    pub table: String,
    pub total_rows: u64,
    pub rows_processed: u64,
    pub cursor_position: Option<String>,
    pub is_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MysqlSnapshot {
    pub tables: Vec<TableSnapshot>,
    pub snapshot_id: String,
    pub snapshot_start_ts: u64,
    pub binlog_file: String,
    pub binlog_pos: u32,
    pub gtid: String,
}

pub struct MysqlSnapshotHandle {
    source_name: String,
    snapshot: MysqlSnapshot,
    tables: Vec<TableSnapshotState>,
    connection: Option<mysql_async::Conn>,
    transaction_open: bool,
    current_table: usize,
    next_chunk_index: u32,
    emitted_rows: u64,
}

#[async_trait]
trait MysqlBinlogProvider: Send + Sync {
    async fn poll_events(&mut self, max_events: usize) -> Result<Vec<MysqlBinlogMessage>>;
}

struct LiveMysqlBinlogProvider {
    stream: BinlogStream,
    binlog_file: String,
    next_pos: u32,
    active_gtid: Option<String>,
    active_tx_id: Option<u64>,
    next_tx_id: u64,
    poll_interval_ms: u64,
}

impl LiveMysqlBinlogProvider {
    async fn new(
        config: &MysqlSourceConfig,
        binlog_file: String,
        next_pos: u32,
        gtid_mode_enabled: bool,
        poll_interval_ms: u64,
    ) -> Result<Self> {
        let connection = MySqlBinlogConn::new(config.build_pool_opts()?)
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "failed to establish mysql replication connection: {error}"
                ))
            })?;

        let mut request = BinlogStreamRequest::new(config.server_id)
            .with_filename(binlog_file.as_bytes())
            .with_pos(u64::from(next_pos));
        if gtid_mode_enabled {
            request = request.with_gtid();
        }

        let stream = connection
            .get_binlog_stream(request)
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "failed to start mysql replication stream at {}:{}: {error}",
                    binlog_file, next_pos
                ))
            })?;

        Ok(Self {
            stream,
            binlog_file,
            next_pos,
            active_gtid: None,
            active_tx_id: None,
            next_tx_id: 1,
            poll_interval_ms: poll_interval_ms.max(1),
        })
    }

    fn decode_row_change(
        &self,
        table_map: &TableMapEvent<'_>,
        before: Option<BinlogRow>,
        after: Option<BinlogRow>,
    ) -> Result<MysqlRowChange> {
        let before = before.map(binlog_row_to_mysql_row).transpose()?;
        let after = after.map(binlog_row_to_mysql_row).transpose()?;
        let primary_key = before
            .as_ref()
            .and_then(primary_key_columns_from_row)
            .or_else(|| after.as_ref().and_then(primary_key_columns_from_row));

        Ok(MysqlRowChange {
            schema: Some(table_map.database_name().into_owned()),
            table: table_map.table_name().into_owned(),
            primary_key,
            before: before.as_ref().map(mysql_row_to_json),
            after: after.as_ref().map(mysql_row_to_json),
        })
    }

    fn ensure_active_tx(&mut self) -> (u64, bool) {
        if let Some(tx_id) = self.active_tx_id {
            (tx_id, false)
        } else {
            let tx_id = self.next_tx_id;
            self.next_tx_id = self.next_tx_id.saturating_add(1);
            self.active_tx_id = Some(tx_id);
            (tx_id, true)
        }
    }

    fn decode_event_data(
        &mut self,
        data: EventData<'_>,
        timestamp_ms: u64,
    ) -> Result<Vec<MysqlBinlogMessage>> {
        let mut out = Vec::new();
        match data {
            EventData::QueryEvent(query) => {
                let statement = query.query();
                if statement.trim().eq_ignore_ascii_case("BEGIN") {
                    let (tx_id, _) = self.ensure_active_tx();
                    out.push(MysqlBinlogMessage::Begin {
                        tx_id,
                        timestamp_ms,
                    });
                } else if let Some(mut captured) =
                    extract_captured_ddl(DdlDialect::Mysql, &statement)
                {
                    captured.ts = timestamp_ms;
                    out.push(MysqlBinlogMessage::Ddl {
                        captured,
                        timestamp_ms,
                        binlog_file: self.binlog_file.clone(),
                        binlog_pos: self.next_pos,
                    });
                }
            }
            EventData::RowsEvent(rows) => {
                let (tx_id, opened_here) = self.ensure_active_tx();
                if opened_here {
                    out.push(MysqlBinlogMessage::Begin {
                        tx_id,
                        timestamp_ms,
                    });
                }

                let table_map = self.stream.get_tme(rows.table_id()).ok_or_else(|| {
                    Error::SourceError(format!(
                        "mysql rows event missing table map metadata for table_id {}",
                        rows.table_id()
                    ))
                })?;

                for row in rows.rows(table_map) {
                    let (before, after) = row.map_err(|error| {
                        Error::SourceError(format!(
                            "failed decoding mysql rows event row pair: {error}"
                        ))
                    })?;
                    let change = self.decode_row_change(table_map, before, after)?;
                    match &rows {
                        RowsEventData::WriteRowsEventV1(_) | RowsEventData::WriteRowsEvent(_) => {
                            out.push(MysqlBinlogMessage::WriteRows(change));
                        }
                        RowsEventData::UpdateRowsEventV1(_)
                        | RowsEventData::UpdateRowsEvent(_)
                        | RowsEventData::PartialUpdateRowsEvent(_) => {
                            out.push(MysqlBinlogMessage::UpdateRows(change));
                        }
                        RowsEventData::DeleteRowsEventV1(_) | RowsEventData::DeleteRowsEvent(_) => {
                            out.push(MysqlBinlogMessage::DeleteRows(change));
                        }
                    }
                }
            }
            EventData::XidEvent(xid) => {
                let tx_id = self.active_tx_id.take().unwrap_or_else(|| {
                    if xid.xid == 0 {
                        let tx_id = self.next_tx_id;
                        self.next_tx_id = self.next_tx_id.saturating_add(1);
                        tx_id
                    } else {
                        xid.xid
                    }
                });
                out.push(MysqlBinlogMessage::Xid {
                    tx_id,
                    timestamp_ms,
                    binlog_file: self.binlog_file.clone(),
                    binlog_pos: self.next_pos,
                    gtid: self.active_gtid.clone(),
                });
                self.active_gtid = None;
            }
            EventData::RotateEvent(rotate) => {
                self.binlog_file = rotate.name().into_owned();
                self.next_pos = u32::try_from(rotate.position()).map_err(|_| {
                    Error::SourceError(format!(
                        "mysql rotate position exceeds u32: {}",
                        rotate.position()
                    ))
                })?;
                out.push(MysqlBinlogMessage::Rotate {
                    binlog_file: self.binlog_file.clone(),
                    binlog_pos: self.next_pos,
                });
            }
            EventData::GtidEvent(gtid) => {
                let value = format_gtid(gtid.sid(), gtid.gno());
                self.active_gtid = Some(value.clone());
                out.push(MysqlBinlogMessage::Gtid { gtid: value });
            }
            EventData::HeartbeatEvent => out.push(MysqlBinlogMessage::Heartbeat),
            _ => {}
        }

        Ok(out)
    }
}

#[async_trait]
impl MysqlBinlogProvider for LiveMysqlBinlogProvider {
    async fn poll_events(&mut self, max_events: usize) -> Result<Vec<MysqlBinlogMessage>> {
        if max_events == 0 {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();

        while out.len() < max_events {
            let next_event = tokio::time::timeout(
                Duration::from_millis(self.poll_interval_ms),
                self.stream.next(),
            )
            .await;

            let Some(event) = (match next_event {
                Ok(value) => value,
                Err(_) => break,
            }) else {
                return Err(Error::SourceError(
                    "mysql replication stream closed unexpectedly".into(),
                ));
            };

            let event = event.map_err(|error| {
                Error::SourceError(format!("mysql replication stream yielded error: {error}"))
            })?;

            let header = event.header();
            self.next_pos = header.log_pos();
            let timestamp_ms = u64::from(header.timestamp()) * 1_000;

            if let Some(data) = event.read_data().map_err(|error| {
                Error::SourceError(format!(
                    "failed decoding mysql binlog event payload: {error}"
                ))
            })? {
                out.extend(self.decode_event_data(data, timestamp_ms)?);
            }
        }

        Ok(out)
    }
}

pub struct MysqlStreamHandle {
    source_name: String,
    stream: MysqlStream,
    provider: Box<dyn MysqlBinlogProvider>,
    current_tx_id: Option<u64>,
    current_commit_ts: u64,
    partial_tx_events: Vec<Event>,
    requeued_events: VecDeque<Event>,
    events_polled: u64,
    max_events_per_poll: usize,
    stream_poll_interval_ms: u64,
}

impl MysqlStreamHandle {
    fn new(
        source_name: String,
        stream: MysqlStream,
        provider: Box<dyn MysqlBinlogProvider>,
        max_events_per_poll: usize,
        stream_poll_interval_ms: u64,
    ) -> Self {
        Self {
            source_name,
            stream,
            provider,
            current_tx_id: None,
            current_commit_ts: 0,
            partial_tx_events: Vec::new(),
            requeued_events: VecDeque::new(),
            events_polled: 0,
            max_events_per_poll: max_events_per_poll.max(1),
            stream_poll_interval_ms: stream_poll_interval_ms.max(1),
        }
    }
}

impl MysqlSnapshotHandle {
    fn new(
        source_name: String,
        snapshot: MysqlSnapshot,
        tables: Vec<TableSnapshotState>,
        connection: Option<mysql_async::Conn>,
        transaction_open: bool,
    ) -> Self {
        Self {
            source_name,
            snapshot,
            tables,
            connection,
            transaction_open,
            current_table: 0,
            next_chunk_index: 0,
            emitted_rows: 0,
        }
    }

    fn resume_from_checkpoint_payload(mut self, payload: &[u8]) -> Result<Self> {
        let state: SnapshotCheckpointState = serde_json::from_slice(payload)?;
        if state.tables.len() != self.tables.len() {
            return Err(Error::CheckpointError(
                "mysql snapshot checkpoint table count does not match snapshot handle".into(),
            ));
        }

        self.snapshot.snapshot_id = state.snapshot_id;
        self.snapshot.snapshot_start_ts = state.snapshot_start_ts;
        self.snapshot.binlog_file = state.binlog_file;
        self.snapshot.binlog_pos = state.binlog_pos;
        self.snapshot.gtid = state.gtid;
        self.current_table = state.current_table;
        self.next_chunk_index = state.next_chunk_index;
        self.emitted_rows = 0;

        for (index, table_state) in self.tables.iter_mut().enumerate() {
            let saved = &state.tables[index];
            table_state.snapshot = saved.clone();
            if table_state.live_query {
                table_state.next_row = 0;
            } else {
                table_state.next_row = usize::try_from(saved.rows_processed).map_err(|_| {
                    Error::CheckpointError(format!(
                        "rows_processed does not fit into usize for table {}",
                        saved.table
                    ))
                })?;
                if table_state.next_row > table_state.rows.len() {
                    return Err(Error::CheckpointError(format!(
                        "rows_processed exceeds available rows for table {}",
                        saved.table
                    )));
                }
            }
            self.emitted_rows += saved.rows_processed;
        }

        self.sync_snapshot_tables();
        Ok(self)
    }

    fn is_complete(&self) -> bool {
        self.tables.iter().all(|table| table.snapshot.is_complete)
    }

    fn sync_snapshot_tables(&mut self) {
        self.snapshot.tables = self
            .tables
            .iter()
            .map(|table| table.snapshot.clone())
            .collect();
    }

    fn total_expected_rows(&self) -> u64 {
        self.tables
            .iter()
            .map(|table| table.snapshot.total_rows)
            .sum()
    }

    async fn fetch_live_rows(
        &mut self,
        table_index: usize,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(serde_json::Value, serde_json::Value)>> {
        let connection = self.connection.as_mut().ok_or_else(|| {
            Error::StateError("mysql snapshot live query requires an active connection".into())
        })?;

        let table = &self.tables[table_index];
        if table.primary_key_columns.is_empty() {
            return Err(Error::SourceError(format!(
                "mysql snapshot requires a PRIMARY KEY for keyset pagination: {}",
                table.snapshot.table
            )));
        }

        let table_ref = mysql_qualified_table_name_from_reference(&table.snapshot.table)?;

        let cursor_projection = table
            .primary_key_columns
            .iter()
            .map(|column| quoted_mysql_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");

        let cursor_values = if let Some(raw_cursor) = cursor {
            let parsed_cursor: Vec<serde_json::Value> =
                serde_json::from_str(raw_cursor).map_err(|error| {
                    Error::CheckpointError(format!(
                        "mysql snapshot cursor decode failed for table '{}': {error}",
                        table.snapshot.table
                    ))
                })?;

            if parsed_cursor.len() != table.primary_key_columns.len() {
                return Err(Error::CheckpointError(format!(
                    "mysql snapshot cursor width mismatch for table '{}'",
                    table.snapshot.table
                )));
            }

            let mut params = Vec::with_capacity(parsed_cursor.len());
            for value in &parsed_cursor {
                params.push(mysql_json_value_to_param(value)?);
            }

            Some(params)
        } else {
            None
        };

        let where_clause = cursor_values
            .as_ref()
            .map(|values| {
                let placeholders = vec!["?"; values.len()].join(", ");
                format!("WHERE ({cursor_projection}) > ({placeholders})")
            })
            .unwrap_or_default();

        let query = format!(
            "SELECT * FROM {} {where_clause} ORDER BY {cursor_projection} LIMIT ?",
            table_ref
        );

        let mut query_params = cursor_values.unwrap_or_default();
        query_params.push(MysqlValue::UInt(limit as u64));

        let rows: Vec<mysql_async::Row> = connection
            .exec(query, mysql_async::Params::Positional(query_params))
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "failed fetching mysql snapshot rows for table '{}': {error}",
                    table.snapshot.table
                ))
            })?;

        let mut decoded = Vec::with_capacity(rows.len());
        for row in rows {
            let row_json = mysql_row_to_json(&row);
            let mut cursor_values = Vec::with_capacity(table.primary_key_columns.len());
            for column in &table.primary_key_columns {
                let index = row
                    .columns_ref()
                    .iter()
                    .position(|row_column| row_column.name_str() == column.as_str())
                    .ok_or_else(|| {
                        Error::SerializationError(format!(
                            "mysql snapshot primary key column '{}' missing from row for table '{}'",
                            column, table.snapshot.table
                        ))
                    })?;
                let value = row
                    .as_ref(index)
                    .map(mysql_value_to_json)
                    .unwrap_or(serde_json::Value::Null);
                cursor_values.push(value);
            }
            let cursor_json = serde_json::Value::Array(cursor_values);
            decoded.push((cursor_json, row_json));
        }

        Ok(decoded)
    }
}

/// MySQL connector lifecycle manager.
pub struct MysqlConnection {
    config: MysqlSourceConfig,
    logger: StructuredLogger,
    state: Arc<Mutex<ConnectionState>>,
    /// Binlog position captured at snapshot start — set by `start_snapshot`.
    snapshot_watermark: Option<MysqlOffset>,
    /// Binlog position the stream was initialised from — set by `start_stream`.
    stream_start: Option<MysqlOffset>,
    stream_poll_interval_ms: u64,
    max_events_per_poll: usize,
}

impl MysqlConnection {
    pub fn new(config: MysqlSourceConfig) -> Self {
        let stream_poll_interval_ms = config.stream_poll_interval_ms.max(1);
        let max_events_per_poll = config.max_events_per_poll.max(1);
        let flavor_name = config.server_flavor.source_name();
        Self {
            config,
            logger: StructuredLogger::new(flavor_name),
            state: Arc::new(Mutex::new(ConnectionState::default())),
            snapshot_watermark: None,
            stream_start: None,
            stream_poll_interval_ms,
            max_events_per_poll,
        }
    }

    pub async fn connect(&self) -> Result<()> {
        self.config.validate()?;
        {
            let state = self.state.lock().await;
            if state.pool.is_some() {
                return Err(Error::StateError(
                    "mysql connection already established".into(),
                ));
            }
        }

        #[cfg(feature = "tls")]
        {
            let opts = self.config.build_pool_opts()?;
            let pool = MySqlPool::new(opts);

            // Verify the connection works before storing.
            tokio::time::timeout(
                Duration::from_secs(self.config.conn_timeout_secs),
                pool.get_conn(),
            )
            .await
            .map_err(|_| Error::SourceError("mysql connection timed out".into()))?
            .map_err(|error| Error::SourceError(format!("mysql connection failed: {error}")))?;

            let backend = LiveValidationBackend { pool: &pool };
            Self::validate_with_backend(&self.config, &backend).await?;

            let heartbeat_task = self.start_heartbeat(pool.clone());

            let mut state = self.state.lock().await;
            state.pool = Some(pool);
            state.heartbeat_task = Some(heartbeat_task);
            self.logger.source_connected();
            Ok(())
        }
    }

    pub async fn close(&self) {
        let mut state = self.state.lock().await;
        if let Some(handle) = state.heartbeat_task.take() {
            handle.abort();
        }
        if let Some(pool) = state.pool.take() {
            let _ = pool.disconnect().await;
        }
        self.logger.source_disconnected();
    }

    pub async fn is_connected(&self) -> bool {
        self.state.lock().await.pool.is_some()
    }

    async fn start_snapshot_internal(
        &mut self,
        tables: &[&str],
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn SnapshotHandle>> {
        if tables.is_empty() {
            return Err(Error::ConfigError(
                "mysql snapshot requires at least one table".into(),
            ));
        }

        let pool = {
            let state = self.state.lock().await;
            state.pool.clone().ok_or_else(|| {
                Error::StateError("mysql connection must be established before snapshot".into())
            })?
        };

        let mut connection = pool.get_conn().await.map_err(|error| {
            Error::SourceError(format!("failed to acquire mysql connection: {error}"))
        })?;

        let setup_result =
            begin_snapshot_and_collect_table_states(&mut connection, tables, &self.config.database)
                .await;

        let (snapshot, states) = match setup_result {
            Ok(value) => value,
            Err(error) => {
                let _ = connection.query_drop("ROLLBACK").await;
                return Err(error);
            }
        };

        let mut handle = MysqlSnapshotHandle::new(
            self.source_type().to_string(),
            snapshot,
            states,
            Some(connection),
            true,
        );

        if let Some(offset) = resume_from {
            let expected = self.config.server_flavor.snapshot_source_name();
            if offset.source_type() != expected {
                return Err(Error::CheckpointError(format!(
                    "cannot resume {} snapshot from source type '{}'",
                    self.config.source_type(),
                    offset.source_type()
                )));
            }
            handle = handle.resume_from_checkpoint_payload(&offset.encode()?)?;
        }

        self.snapshot_watermark = Some(MysqlOffset {
            binlog_file: handle.snapshot.binlog_file.clone(),
            binlog_pos: handle.snapshot.binlog_pos,
            gtid: handle.snapshot.gtid.clone(),
        });

        Ok(Box::new(handle))
    }

    pub async fn start_snapshot_from_checkpoint(
        &mut self,
        tables: &[&str],
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn SnapshotHandle>> {
        self.start_snapshot_internal(tables, resume_from).await
    }

    async fn validate_with_backend(
        config: &MysqlSourceConfig,
        backend: &dyn ValidationBackend,
    ) -> Result<()> {
        if config.gtid_mode_enabled && !backend.gtid_mode_enabled().await? {
            return Err(Error::SourceError(
                "mysql GTID mode is required but not enabled".into(),
            ));
        }

        if config.binlog_format_check && !backend.binlog_format_row().await? {
            return Err(Error::SourceError(
                "mysql binlog_format must be ROW for CDC".into(),
            ));
        }

        if !backend.has_replication_privilege().await? {
            return Err(Error::SourceError(
                "mysql user lacks REPLICATION privilege".into(),
            ));
        }

        if !backend.binlog_enabled().await? {
            return Err(Error::SourceError(
                "mysql binary logging is disabled (log_bin=OFF)".into(),
            ));
        }

        let _ = backend.master_position().await?;
        Ok(())
    }

    fn start_heartbeat(&self, pool: MySqlPool) -> JoinHandle<()> {
        let logger = self.logger.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(HEARTBEAT_SECS));
            loop {
                interval.tick().await;
                match pool.get_conn().await {
                    Ok(mut conn) => {
                        if let Err(error) = conn.query_drop("SELECT 1").await {
                            logger.connection_error(&format!("heartbeat query failed: {error}"));
                            break;
                        }
                    }
                    Err(error) => {
                        logger.connection_error(&format!(
                            "heartbeat failed to acquire connection: {error}"
                        ));
                        break;
                    }
                }
            }
        })
    }
}

impl Drop for MysqlConnection {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.try_lock() {
            if let Some(handle) = state.heartbeat_task.take() {
                handle.abort();
            }
        }
    }
}

impl MysqlConnection {
    /// Start a non-blocking incremental snapshot using the DBLog watermark pattern.
    pub async fn start_incremental_snapshot(
        &mut self,
        config: IncrementalSnapshotConfig,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>> {
        use crate::source::mysql::incremental_snapshot::MysqlIncrementalSnapshotHandle;
        let pool = {
            let state = self.state.lock().await;
            state.pool.clone().ok_or_else(|| {
                Error::StateError(
                    "mysql connection must be established before incremental snapshot".into(),
                )
            })?
        };
        let inner = self.start_stream(resume_from).await?;
        let source_name = self.source_type().to_string();
        let default_database = self.config.database.clone();
        let handle =
            MysqlIncrementalSnapshotHandle::new(inner, pool, config, source_name, default_database)
                .await?;
        Ok(Box::new(handle))
    }
}

#[async_trait]
impl Source for MysqlConnection {
    async fn start_snapshot(&mut self, tables: &[&str]) -> Result<Box<dyn SnapshotHandle>> {
        self.start_snapshot_internal(tables, None).await
    }

    async fn start_stream(
        &mut self,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>> {
        let pool = {
            let state = self.state.lock().await;
            state.pool.clone().ok_or_else(|| {
                Error::StateError("mysql connection must be established before stream".into())
            })?
        };

        let start = resolve_stream_start_position(&pool, self.source_type(), resume_from).await?;

        let mut stream = MysqlStream {
            binlog_file: start.binlog_file.clone(),
            binlog_pos: start.binlog_pos,
            gtid: start.gtid.clone(),
            stream_state: StreamState::Starting,
        };
        stream.stream_state = StreamState::Streaming;

        // Store stream start watermark so perform_handoff can validate the gap-free invariant.
        self.stream_start = Some(MysqlOffset {
            binlog_file: start.binlog_file.clone(),
            binlog_pos: start.binlog_pos,
            gtid: start.gtid.clone(),
        });

        Ok(Box::new(MysqlStreamHandle::new(
            self.source_type().to_string(),
            stream,
            Box::new(
                LiveMysqlBinlogProvider::new(
                    &self.config,
                    start.binlog_file,
                    start.binlog_pos,
                    self.config.gtid_mode_enabled,
                    self.stream_poll_interval_ms,
                )
                .await?,
            ),
            self.max_events_per_poll,
            self.stream_poll_interval_ms,
        )))
    }

    async fn perform_handoff(
        &mut self,
        snapshot: &mut dyn SnapshotHandle,
        stream: &mut dyn StreamHandle,
    ) -> Result<HandoffResult> {
        // Retrieve watermarks recorded during start_snapshot / start_stream.
        let snapshot_wm = self.snapshot_watermark.clone().ok_or_else(|| {
            Error::StateError(
                "mysql perform_handoff requires start_snapshot to have been called first".into(),
            )
        })?;
        let stream_wm = self.stream_start.clone().ok_or_else(|| {
            Error::StateError(
                "mysql perform_handoff requires start_stream to have been called first".into(),
            )
        })?;

        mysql_handoff_result(snapshot, stream, snapshot_wm, stream_wm).await
    }

    fn source_type(&self) -> &str {
        self.config.source_type()
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            snapshot: true,
            snapshot_checkpoint_resume: true,
            handoff: true,
            ddl_capture: true,
            heartbeat: true,
            tls: cfg!(feature = "tls"),
            schema_introspection: true,
            truncate: false,
            incremental_snapshot: true,
        }
    }
}

#[async_trait]
impl SnapshotHandle for MysqlSnapshotHandle {
    async fn next_chunk(&mut self, chunk_size: usize) -> Result<Vec<Event>> {
        next_snapshot_chunk(self, chunk_size).await
    }

    async fn checkpoint(
        &self,
        checkpoint: &mut dyn crate::checkpoint::Checkpoint,
        committed_event_count: u64,
    ) -> Result<()> {
        let payload = SnapshotCheckpointState {
            snapshot_id: self.snapshot.snapshot_id.clone(),
            snapshot_start_ts: self.snapshot.snapshot_start_ts,
            binlog_file: self.snapshot.binlog_file.clone(),
            binlog_pos: self.snapshot.binlog_pos,
            gtid: self.snapshot.gtid.clone(),
            current_table: self.current_table,
            next_chunk_index: self.next_chunk_index,
            tables: self.snapshot.tables.clone(),
        };

        let encoded = serde_json::to_vec(&payload)?;
        let snapshot_source = format!("{}_snapshot", self.source_name);
        let offset = GenericOffset::new(&snapshot_source, encoded);
        checkpoint.save(&offset, committed_event_count).await
    }

    async fn finish(&mut self) -> Result<SnapshotEnd> {
        self.sync_snapshot_tables();
        let total_processed: u64 = self
            .snapshot
            .tables
            .iter()
            .map(|table| table.rows_processed)
            .sum();
        if total_processed != self.emitted_rows {
            return Err(Error::SourceError(format!(
                "mysql snapshot consistency check failed: emitted_rows={} rows_processed={total_processed}",
                self.emitted_rows
            )));
        }
        if self.total_expected_rows() != total_processed {
            return Err(Error::SourceError(
                "mysql snapshot consistency check failed: not all rows were emitted".into(),
            ));
        }

        if self.transaction_open {
            let connection = self.connection.as_mut().ok_or_else(|| {
                Error::StateError(
                    "mysql snapshot transaction is open but connection is unavailable".into(),
                )
            })?;
            connection.query_drop("COMMIT").await.map_err(|error| {
                Error::SourceError(format!(
                    "failed to commit mysql snapshot transaction: {error}"
                ))
            })?;
            self.transaction_open = false;
            self.connection.take();
        }

        Ok(SnapshotEnd {
            snapshot_end_ts: now_millis(),
        })
    }
}

#[async_trait]
impl StreamHandle for MysqlStreamHandle {
    async fn next_events(&mut self, timeout_ms: u64) -> Result<Vec<Event>> {
        if self.stream.stream_state != StreamState::Streaming {
            return Err(Error::StateError(
                "mysql stream polling requested while stream is not running".into(),
            ));
        }

        if !self.requeued_events.is_empty() {
            let drained = self.requeued_events.drain(..).collect::<Vec<_>>();
            return Ok(drained);
        }

        let started = std::time::Instant::now();
        let timeout = Duration::from_millis(timeout_ms);

        loop {
            let messages = self.provider.poll_events(self.max_events_per_poll).await?;
            if !messages.is_empty() {
                let events = self.process_messages(messages);
                if !events.is_empty() {
                    tracing::debug!(
                        target: "rustcdc::source::mysql",
                        count = events.len(),
                        file = %self.stream.binlog_file,
                        pos = self.stream.binlog_pos,
                        "mysql stream events received",
                    );
                    return Ok(events);
                }
            }

            if timeout_ms == 0 || started.elapsed() >= timeout {
                return Ok(Vec::new());
            }

            let remaining = timeout.saturating_sub(started.elapsed());
            tokio::time::sleep(Duration::from_millis(
                self.stream_poll_interval_ms
                    .min(remaining.as_millis() as u64),
            ))
            .await;
        }
    }

    async fn save_position(
        &self,
        checkpoint: &mut dyn crate::checkpoint::Checkpoint,
    ) -> Result<()> {
        let offset = MysqlOffset {
            gtid: self.stream.gtid.clone(),
            binlog_file: self.stream.binlog_file.clone(),
            binlog_pos: self.stream.binlog_pos,
        };
        checkpoint.save(&offset, self.events_polled).await
    }

    async fn requeue_events(&mut self, events: Vec<Event>) -> Result<()> {
        self.requeued_events.extend(events);
        Ok(())
    }

    async fn confirm_lsn(&mut self, _lsn: u64) -> Result<()> {
        Ok(())
    }
}

impl Drop for MysqlStreamHandle {
    fn drop(&mut self) {
        self.stream.stream_state = StreamState::Stopped;
    }
}

#[async_trait]
trait ValidationBackend: Send + Sync {
    async fn gtid_mode_enabled(&self) -> Result<bool>;
    async fn binlog_format_row(&self) -> Result<bool>;
    async fn has_replication_privilege(&self) -> Result<bool>;
    async fn binlog_enabled(&self) -> Result<bool>;
    async fn master_position(&self) -> Result<(String, u64)>;
}

struct LiveValidationBackend<'a> {
    pool: &'a MySqlPool,
}

#[async_trait]
impl ValidationBackend for LiveValidationBackend<'_> {
    async fn gtid_mode_enabled(&self) -> Result<bool> {
        let mut conn =
            self.pool.get_conn().await.map_err(|error| {
                Error::SourceError(format!("failed to query GTID mode: {error}"))
            })?;
        let mode: Option<String> = conn
            .query_first("SELECT @@GLOBAL.GTID_MODE")
            .await
            .map_err(|error| Error::SourceError(format!("failed to query GTID mode: {error}")))?;
        Ok(mode
            .map(|value| value.eq_ignore_ascii_case("ON"))
            .unwrap_or(false))
    }

    async fn binlog_format_row(&self) -> Result<bool> {
        let mut conn = self.pool.get_conn().await.map_err(|error| {
            Error::SourceError(format!("failed to query binlog format: {error}"))
        })?;
        let value: Option<String> = conn
            .query_first("SELECT @@GLOBAL.BINLOG_FORMAT")
            .await
            .map_err(|error| {
                Error::SourceError(format!("failed to query binlog format: {error}"))
            })?;
        Ok(value
            .map(|item| item.eq_ignore_ascii_case("ROW"))
            .unwrap_or(false))
    }

    async fn has_replication_privilege(&self) -> Result<bool> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|error| Error::SourceError(format!("failed to query grants: {error}")))?;
        let grants: Vec<String> = conn
            .query("SHOW GRANTS FOR CURRENT_USER()")
            .await
            .map_err(|error| Error::SourceError(format!("failed to query grants: {error}")))?;
        Ok(grants.into_iter().any(|grant| {
            let upper = grant.to_ascii_uppercase();
            upper.contains("REPLICATION") || upper.contains("ALL PRIVILEGES")
        }))
    }

    async fn binlog_enabled(&self) -> Result<bool> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|error| Error::SourceError(format!("failed to query log_bin: {error}")))?;
        let value: Option<u8> = conn
            .query_first("SELECT @@GLOBAL.LOG_BIN")
            .await
            .map_err(|error| Error::SourceError(format!("failed to query log_bin: {error}")))?;
        Ok(value.unwrap_or_default() != 0)
    }

    async fn master_position(&self) -> Result<(String, u64)> {
        let mut conn = self.pool.get_conn().await.map_err(|error| {
            Error::SourceError(format!("failed to query master status: {error}"))
        })?;
        let mut row: mysql_async::Row = match conn.query_first("SHOW MASTER STATUS").await {
            Ok(Some(row)) => row,
            Ok(None) => {
                return Err(Error::SourceError("mysql master status unavailable".into()));
            }
            Err(primary_error) => conn
                .query_first("SHOW BINARY LOG STATUS")
                .await
                .map_err(|fallback_error| {
                    Error::SourceError(format!(
                        "failed to query mysql binary log status (SHOW MASTER STATUS error: {primary_error}; SHOW BINARY LOG STATUS error: {fallback_error})"
                    ))
                })?
                .ok_or_else(|| Error::SourceError("mysql binary log status unavailable".into()))?,
        };
        let file: String = row.take(0).unwrap_or_default();
        let pos: u64 = row.take(1).unwrap_or(4);
        Ok((file, pos))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    };

    use async_trait::async_trait;
    use serde_json::json;

    use tokio::sync::Mutex;

    use crate::{
        checkpoint::{Checkpoint, InMemoryCheckpoint, MysqlOffset},
        core::{Event, StructuredLogger, TransportConfig},
        source::{SnapshotHandle, Source, StreamHandle},
    };

    use super::MysqlSourceConfig;
    use super::{
        ConnectionState, MysqlBinlogMessage, MysqlBinlogProvider, MysqlConnection, MysqlRowChange,
        MysqlSnapshot, MysqlSnapshotHandle, MysqlStream, MysqlStreamHandle, StreamState,
        TableSnapshot, TableSnapshotState, ValidationBackend, MAX_EVENTS_PER_POLL,
        STREAM_POLL_INTERVAL_MS,
    };
    use crate::ddl_capture::{extract_captured_ddl, DdlDialect};
    use crate::SecretString;

    #[derive(Default)]
    struct MockValidationBackend {
        gtid_mode_enabled: bool,
        binlog_format_row: bool,
        has_replication_privilege: bool,
        binlog_enabled: bool,
        master_position_called: Arc<AtomicBool>,
    }

    #[async_trait]
    impl ValidationBackend for MockValidationBackend {
        async fn gtid_mode_enabled(&self) -> crate::core::Result<bool> {
            Ok(self.gtid_mode_enabled)
        }

        async fn binlog_format_row(&self) -> crate::core::Result<bool> {
            Ok(self.binlog_format_row)
        }

        async fn has_replication_privilege(&self) -> crate::core::Result<bool> {
            Ok(self.has_replication_privilege)
        }

        async fn binlog_enabled(&self) -> crate::core::Result<bool> {
            Ok(self.binlog_enabled)
        }

        async fn master_position(&self) -> crate::core::Result<(String, u64)> {
            self.master_position_called.store(true, Ordering::Relaxed);
            Ok(("mysql-bin.000001".into(), 4))
        }
    }

    #[test]
    fn config_validation_rejects_empty_fields() {
        let config = MysqlSourceConfig::default();
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_validation_rejects_zero_stream_tuning() {
        let mut config = MysqlSourceConfig {
            host: "localhost".into(),
            port: 3306,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            server_id: 1,
            gtid_mode_enabled: false,
            binlog_format_check: true,
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: 1,
            max_events_per_poll: 1,
            ..Default::default()
        };

        config.stream_poll_interval_ms = 0;
        assert!(config.validate().is_err());

        config.stream_poll_interval_ms = 1;
        config.max_events_per_poll = 0;
        assert!(config.validate().is_err());

        config.max_events_per_poll = 1;
        config.conn_timeout_secs = 301;
        assert!(config.validate().is_err());

        config.conn_timeout_secs = 30;
        config.stream_poll_interval_ms = 60_001;
        assert!(config.validate().is_err());

        config.stream_poll_interval_ms = 1;
        config.max_events_per_poll = 100_001;
        assert!(config.validate().is_err());
    }

    #[test]
    fn default_config_prefers_tls_when_available() {
        let config = MysqlSourceConfig::default();
        assert!(config.transport.is_tls());
    }

    #[test]
    fn debug_redacts_password() {
        let config = MysqlSourceConfig {
            host: "localhost".into(),
            port: 3306,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            server_id: 7,
            gtid_mode_enabled: true,
            binlog_format_check: true,
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        };

        let debug = format!("{config:?}");
        assert!(debug.contains("***redacted***"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn validation_accepts_callback_backed_passwords() {
        let config = MysqlSourceConfig {
            host: "localhost".into(),
            port: 3306,
            user: "cdc".into(),
            password: SecretString::from_callback("mysql-test", || Ok("secret".to_string())),
            database: "app".into(),
            server_id: 7,
            gtid_mode_enabled: false,
            binlog_format_check: true,
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        };

        assert!(config.validate().is_ok());
        assert!(config.build_pool_opts().is_ok());
    }

    #[test]
    fn callback_backed_password_is_re_resolved_for_rotation() {
        let counter = Arc::new(AtomicUsize::new(0));
        let config = MysqlSourceConfig {
            host: "localhost".into(),
            port: 3306,
            user: "cdc".into(),
            password: {
                let counter = counter.clone();
                SecretString::from_callback("mysql-rotation", move || {
                    let next = counter.fetch_add(1, Ordering::Relaxed) + 1;
                    Ok(format!("secret-{next}"))
                })
            },
            database: "app".into(),
            server_id: 7,
            gtid_mode_enabled: false,
            binlog_format_check: true,
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        };

        let _ = config.build_pool_opts().unwrap();
        let _ = config.build_pool_opts().unwrap();

        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn source_type_is_mysql() {
        let connection = MysqlConnection::new(MysqlSourceConfig {
            host: "localhost".into(),
            port: 3306,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            server_id: 10,
            gtid_mode_enabled: false,
            binlog_format_check: true,
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        });

        assert_eq!(connection.source_type(), "mysql");
        let capabilities = connection.capabilities();
        assert!(capabilities.snapshot);
        assert!(capabilities.handoff);
        assert!(capabilities.heartbeat);
        assert!(capabilities.ddl_capture);
    }

    #[tokio::test]
    async fn validation_passes_when_prerequisites_are_satisfied() {
        let config = MysqlSourceConfig {
            host: "localhost".into(),
            port: 3306,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            server_id: 10,
            gtid_mode_enabled: true,
            binlog_format_check: true,
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        };
        let backend = MockValidationBackend {
            gtid_mode_enabled: true,
            binlog_format_row: true,
            has_replication_privilege: true,
            binlog_enabled: true,
            master_position_called: Arc::new(AtomicBool::new(false)),
        };

        MysqlConnection::validate_with_backend(&config, &backend)
            .await
            .unwrap();
        assert!(backend.master_position_called.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn validation_rejects_missing_gtid_mode_when_required() {
        let config = MysqlSourceConfig {
            host: "localhost".into(),
            port: 3306,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            server_id: 10,
            gtid_mode_enabled: true,
            binlog_format_check: true,
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        };
        let backend = MockValidationBackend {
            gtid_mode_enabled: false,
            binlog_format_row: true,
            has_replication_privilege: true,
            binlog_enabled: true,
            ..Default::default()
        };

        let error = MysqlConnection::validate_with_backend(&config, &backend)
            .await
            .unwrap_err();
        assert!(matches!(error, crate::core::Error::SourceError(_)));
    }

    #[tokio::test]
    async fn validation_rejects_missing_binlog_or_privilege() {
        let config = MysqlSourceConfig {
            host: "localhost".into(),
            port: 3306,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            server_id: 10,
            gtid_mode_enabled: false,
            binlog_format_check: true,
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        };

        let missing_priv = MockValidationBackend {
            gtid_mode_enabled: true,
            binlog_format_row: true,
            has_replication_privilege: false,
            binlog_enabled: true,
            ..Default::default()
        };
        let error = MysqlConnection::validate_with_backend(&config, &missing_priv)
            .await
            .unwrap_err();
        assert!(matches!(error, crate::core::Error::SourceError(_)));

        let missing_binlog = MockValidationBackend {
            gtid_mode_enabled: true,
            binlog_format_row: true,
            has_replication_privilege: true,
            binlog_enabled: false,
            ..Default::default()
        };
        let error = MysqlConnection::validate_with_backend(&config, &missing_binlog)
            .await
            .unwrap_err();
        assert!(matches!(error, crate::core::Error::SourceError(_)));
    }

    struct MockBinlogProvider {
        batches: VecDeque<Vec<MysqlBinlogMessage>>,
    }

    impl MockBinlogProvider {
        fn new(batches: Vec<Vec<MysqlBinlogMessage>>) -> Self {
            Self {
                batches: batches.into_iter().collect(),
            }
        }
    }

    #[async_trait]
    impl MysqlBinlogProvider for MockBinlogProvider {
        async fn poll_events(
            &mut self,
            _max_events: usize,
        ) -> crate::core::Result<Vec<MysqlBinlogMessage>> {
            Ok(self.batches.pop_front().unwrap_or_default())
        }
    }

    fn row_change(
        table: &str,
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
    ) -> MysqlRowChange {
        MysqlRowChange {
            schema: Some("app".into()),
            table: table.into(),
            primary_key: Some(vec!["id".into()]),
            before,
            after,
        }
    }

    fn make_stream_handle(
        file: &str,
        pos: u32,
        gtid: &str,
        provider: MockBinlogProvider,
    ) -> MysqlStreamHandle {
        MysqlStreamHandle::new(
            "mysql".into(),
            MysqlStream {
                binlog_file: file.into(),
                binlog_pos: pos,
                gtid: gtid.into(),
                stream_state: StreamState::Streaming,
            },
            Box::new(provider),
            super::MAX_EVENTS_PER_POLL,
            super::STREAM_POLL_INTERVAL_MS,
        )
    }

    #[tokio::test]
    async fn stream_maps_insert_update_delete_with_xid_boundaries() {
        let mut handle = make_stream_handle(
            "mysql-bin.000001",
            4,
            "",
            MockBinlogProvider::new(vec![vec![
                MysqlBinlogMessage::Begin {
                    tx_id: 77,
                    timestamp_ms: 1,
                },
                MysqlBinlogMessage::WriteRows(row_change(
                    "users",
                    None,
                    Some(json!({"id": 1, "name": "alice"})),
                )),
                MysqlBinlogMessage::UpdateRows(row_change(
                    "users",
                    Some(json!({"id": 1, "name": "alice"})),
                    Some(json!({"id": 1, "name": "bob"})),
                )),
                MysqlBinlogMessage::DeleteRows(row_change(
                    "users",
                    Some(json!({"id": 1, "name": "bob"})),
                    None,
                )),
                MysqlBinlogMessage::Xid {
                    tx_id: 77,
                    timestamp_ms: 1234,
                    binlog_file: "mysql-bin.000002".into(),
                    binlog_pos: 900,
                    gtid: Some("uuid:1-10".into()),
                },
            ]]),
        );

        let events = handle.next_events(50).await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].op, crate::core::Operation::Insert);
        assert_eq!(events[1].op, crate::core::Operation::Update);
        assert_eq!(events[2].op, crate::core::Operation::Delete);
        assert_eq!(
            events[0].source.offset,
            "mysql-bin.000002:900#gtid=uuid:1-10"
        );
        assert_eq!(events[0].source.timestamp, 1234);

        let tx0 = events[0].transaction.as_ref().expect("tx metadata");
        let tx2 = events[2].transaction.as_ref().expect("tx metadata");
        assert_eq!(tx0.tx_id, 77);
        assert_eq!(tx0.total_events, 3);
        assert_eq!(tx0.event_index, 0);
        assert_eq!(tx2.event_index, 2);

        assert_eq!(handle.stream.binlog_file, "mysql-bin.000002");
        assert_eq!(handle.stream.binlog_pos, 900);
        assert_eq!(handle.stream.gtid, "uuid:1-10");
    }

    #[tokio::test]
    async fn stream_metadata_messages_update_position_without_events() {
        let mut handle = make_stream_handle(
            "mysql-bin.000001",
            4,
            "",
            MockBinlogProvider::new(vec![vec![
                MysqlBinlogMessage::Rotate {
                    binlog_file: "mysql-bin.000010".into(),
                    binlog_pos: 120,
                },
                MysqlBinlogMessage::Gtid {
                    gtid: "uuid:100-120".into(),
                },
                MysqlBinlogMessage::Heartbeat,
            ]]),
        );

        let events = handle.next_events(20).await.unwrap();
        assert!(events.is_empty());
        assert_eq!(handle.stream.binlog_file, "mysql-bin.000010");
        assert_eq!(handle.stream.binlog_pos, 120);
        assert_eq!(handle.stream.gtid, "uuid:100-120");
    }

    #[tokio::test]
    async fn stream_emits_schema_change_for_ddl_query_message() {
        let mut captured =
            extract_captured_ddl(DdlDialect::Mysql, "CREATE TABLE products (id INT)")
                .expect("expected mysql DDL extraction");
        captured.ts = 2500;

        let mut handle = make_stream_handle(
            "mysql-bin.000001",
            4,
            "",
            MockBinlogProvider::new(vec![vec![MysqlBinlogMessage::Ddl {
                captured,
                timestamp_ms: 2500,
                binlog_file: "mysql-bin.000010".into(),
                binlog_pos: 321,
            }]]),
        );

        let events = handle.next_events(20).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op, crate::core::Operation::SchemaChange);
        assert_eq!(events[0].source.offset, "mysql-bin.000010:321");
        assert_eq!(events[0].source.timestamp, 2500);
        assert_eq!(events[0].schema.as_deref(), Some("default"));
        assert_eq!(events[0].table, "products__ddl_events");
    }

    #[tokio::test]
    async fn stream_save_position_persists_mysql_offset() {
        let mut handle = make_stream_handle(
            "mysql-bin.000001",
            4,
            "",
            MockBinlogProvider::new(vec![vec![
                MysqlBinlogMessage::Begin {
                    tx_id: 88,
                    timestamp_ms: 10,
                },
                MysqlBinlogMessage::WriteRows(row_change("users", None, Some(json!({"id": 5})))),
                MysqlBinlogMessage::Xid {
                    tx_id: 88,
                    timestamp_ms: 20,
                    binlog_file: "mysql-bin.000003".into(),
                    binlog_pos: 12345,
                    gtid: Some("uuid:20-21".into()),
                },
            ]]),
        );

        let events = handle.next_events(50).await.unwrap();
        assert_eq!(events.len(), 1);

        let mut checkpoint = InMemoryCheckpoint::default();
        handle.save_position(&mut checkpoint).await.unwrap();

        let offset = checkpoint.load().await.unwrap().expect("offset saved");
        let restored = MysqlOffset::from_bytes(&offset.encode().unwrap()).unwrap();
        assert_eq!(restored.binlog_file, "mysql-bin.000003");
        assert_eq!(restored.binlog_pos, 12345);
        assert_eq!(restored.gtid, "uuid:20-21");
    }

    #[test]
    fn decode_stream_resume_position_uses_mysql_checkpoint_offset() {
        let offset = MysqlOffset {
            gtid: "uuid:3-5".into(),
            binlog_file: "mysql-bin.000010".into(),
            binlog_pos: 777,
        };
        let restored = super::parser::decode_stream_resume_position("mysql", &offset).unwrap();
        assert_eq!(restored.binlog_file, "mysql-bin.000010");
        assert_eq!(restored.binlog_pos, 777);
        assert_eq!(restored.gtid, "uuid:3-5");
    }

    #[test]
    fn decode_stream_resume_position_rejects_source_type_mismatch() {
        let offset = crate::checkpoint::GenericOffset::new("postgres", vec![1, 2, 3]);
        let result = super::parser::decode_stream_resume_position("mysql", &offset);
        assert!(matches!(
            result,
            Err(crate::core::Error::CheckpointError(_))
        ));
    }

    #[tokio::test]
    async fn stream_timeout_returns_empty() {
        let mut handle =
            make_stream_handle("mysql-bin.000001", 4, "", MockBinlogProvider::new(vec![]));
        let events = handle.next_events(5).await.unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn split_table_reference_accepts_valid_inputs() {
        let (schema, table) = super::parser::split_table_reference("users").unwrap();
        assert_eq!(schema, None);
        assert_eq!(table, "users");

        let (schema, table) = super::parser::split_table_reference("app.users").unwrap();
        assert_eq!(schema.as_deref(), Some("app"));
        assert_eq!(table, "users");
    }

    #[test]
    fn split_table_reference_rejects_invalid_inputs() {
        assert!(super::parser::split_table_reference("app.users.extra").is_err());
        assert!(super::parser::split_table_reference(" app.users ").is_ok());
        assert!(super::parser::split_table_reference("app.-users").is_err());
        assert!(super::parser::split_table_reference("users;DROP TABLE audit").is_err());
        assert!(super::parser::split_table_reference("app.users --comment").is_err());
        let (schema, table) = super::parser::split_table_reference("`users.with.dot`").unwrap();
        assert_eq!(schema, None);
        assert_eq!(table, "users.with.dot");

        let (schema, table) =
            super::parser::split_table_reference("`analytics-team`.`users`").unwrap();
        assert_eq!(schema.as_deref(), Some("analytics-team"));
        assert_eq!(table, "users");

        assert!(super::parser::split_table_reference(".users").is_err());
        assert!(super::parser::split_table_reference("users.").is_err());
        assert!(super::parser::split_table_reference("").is_err());
        assert!(super::parser::split_table_reference("`unterminated").is_err());
    }

    #[test]
    fn mysql_qualified_table_name_quotes_identifiers() {
        let unqualified = super::parser::mysql_qualified_table_name(None, "users");
        assert_eq!(unqualified, "`users`");

        let qualified = super::parser::mysql_qualified_table_name(Some("app"), "users");
        assert_eq!(qualified, "`app`.`users`");
    }

    fn test_snapshot_handle(rows: Vec<serde_json::Value>) -> MysqlSnapshotHandle {
        let table = TableSnapshot {
            table: "users".into(),
            total_rows: rows.len() as u64,
            rows_processed: 0,
            cursor_position: None,
            is_complete: rows.is_empty(),
        };
        let snapshot = MysqlSnapshot {
            tables: vec![table.clone()],
            snapshot_id: "mysql-snapshot-test".into(),
            snapshot_start_ts: 1,
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 4,
            gtid: "".into(),
        };

        MysqlSnapshotHandle::new(
            "mysql".into(),
            snapshot,
            vec![TableSnapshotState {
                snapshot: table,
                primary_key_columns: vec!["id".into()],
                rows,
                next_row: 0,
                live_query: false,
            }],
            None,
            false,
        )
    }

    #[tokio::test]
    async fn snapshot_chunking_and_last_chunk_marker() {
        let mut handle = test_snapshot_handle(vec![
            json!({"id": 1, "name": "alice"}),
            json!({"id": 2, "name": "bob"}),
            json!({"id": 3, "name": "carol"}),
        ]);

        let chunk1 = handle.next_chunk(2).await.unwrap();
        assert_eq!(chunk1.len(), 2);
        assert_eq!(chunk1[0].op, crate::core::Operation::Read);
        assert_eq!(
            chunk1[0]
                .snapshot
                .as_ref()
                .expect("snapshot metadata")
                .chunk_index,
            0
        );
        assert!(
            !chunk1[1]
                .snapshot
                .as_ref()
                .expect("snapshot metadata")
                .is_last_chunk
        );

        let chunk2 = handle.next_chunk(2).await.unwrap();
        assert_eq!(chunk2.len(), 1);
        assert!(
            chunk2[0]
                .snapshot
                .as_ref()
                .expect("snapshot metadata")
                .is_last_chunk
        );
        assert_eq!(
            chunk2[0]
                .snapshot
                .as_ref()
                .expect("snapshot metadata")
                .chunk_index,
            1
        );

        let chunk3 = handle.next_chunk(2).await.unwrap();
        assert!(chunk3.is_empty());

        let seen_ids = chunk1
            .iter()
            .chain(chunk2.iter())
            .map(|event| event.after.as_ref().expect("after payload")["id"].clone())
            .collect::<Vec<_>>();
        assert_eq!(seen_ids, vec![json!(1), json!(2), json!(3)]);
    }

    #[tokio::test]
    async fn snapshot_checkpoint_resume_roundtrip() {
        let mut handle = test_snapshot_handle(vec![
            json!({"id": 10, "name": "nora"}),
            json!({"id": 11, "name": "otto"}),
            json!({"id": 12, "name": "pia"}),
        ]);

        let first = handle.next_chunk(1).await.unwrap();
        assert_eq!(first.len(), 1);

        let mut checkpoint = InMemoryCheckpoint::default();
        handle.checkpoint(&mut checkpoint, 9).await.unwrap();

        let saved = checkpoint.load().await.unwrap().expect("saved offset");
        assert_eq!(saved.source_type(), "mysql_snapshot");
        let payload = saved.encode().unwrap();

        let mut resumed = test_snapshot_handle(vec![
            json!({"id": 10, "name": "nora"}),
            json!({"id": 11, "name": "otto"}),
            json!({"id": 12, "name": "pia"}),
        ])
        .resume_from_checkpoint_payload(&payload)
        .unwrap();

        let next = resumed.next_chunk(10).await.unwrap();
        assert_eq!(next.len(), 2);
        assert_eq!(next[0].after.as_ref().unwrap()["id"], json!(11));
        assert_eq!(next[1].after.as_ref().unwrap()["id"], json!(12));
    }

    #[tokio::test]
    async fn snapshot_finish_returns_end_timestamp() {
        let mut handle = test_snapshot_handle(vec![json!({"id": 1, "name": "alice"})]);
        let _ = handle.next_chunk(10).await.unwrap();
        let end = handle.finish().await.unwrap();
        assert!(end.snapshot_end_ts > 0);
    }

    #[tokio::test]
    async fn snapshot_empty_tables_return_no_events() {
        let mut handle = test_snapshot_handle(Vec::new());
        let chunk = handle.next_chunk(10).await.unwrap();
        assert!(chunk.is_empty());
        let end = handle.finish().await.unwrap();
        assert!(end.snapshot_end_ts > 0);
    }

    #[tokio::test]
    async fn snapshot_start_rejects_empty_table_list() {
        let mut connection = MysqlConnection::new(MysqlSourceConfig {
            host: "localhost".into(),
            port: 3306,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            server_id: 10,
            gtid_mode_enabled: false,
            binlog_format_check: true,
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        });

        let result = connection.start_snapshot(&[]).await;
        assert!(matches!(result, Err(crate::core::Error::ConfigError(_))));
    }

    // ── Handoff unit tests ───────────────────────────────────────────────────

    fn make_connection_with_watermarks(
        snapshot_wm: MysqlOffset,
        stream_wm: MysqlOffset,
    ) -> MysqlConnection {
        MysqlConnection {
            config: MysqlSourceConfig {
                host: "localhost".into(),
                port: 3306,
                user: "cdc".into(),
                password: "secret".into(),
                database: "app".into(),
                server_id: 1,
                gtid_mode_enabled: false,
                binlog_format_check: true,
                transport: TransportConfig::tls(),
                conn_timeout_secs: 30,
                stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
                max_events_per_poll: MAX_EVENTS_PER_POLL,
                ..Default::default()
            },
            logger: StructuredLogger::new("mysql"),
            state: Arc::new(Mutex::new(ConnectionState::default())),
            snapshot_watermark: Some(snapshot_wm),
            stream_start: Some(stream_wm),
            stream_poll_interval_ms: super::STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: super::MAX_EVENTS_PER_POLL,
        }
    }

    struct AlreadyDoneSnapshotHandle;

    #[async_trait]
    impl SnapshotHandle for AlreadyDoneSnapshotHandle {
        async fn next_chunk(&mut self, _: usize) -> crate::core::Result<Vec<Event>> {
            Ok(Vec::new())
        }
        async fn checkpoint(
            &self,
            _: &mut dyn crate::checkpoint::Checkpoint,
            _: u64,
        ) -> crate::core::Result<()> {
            Ok(())
        }
        async fn finish(&mut self) -> crate::core::Result<crate::source::SnapshotEnd> {
            Ok(crate::source::SnapshotEnd {
                snapshot_end_ts: 1_700_000_000_000,
            })
        }
    }

    struct NoOpStreamHandle;

    #[async_trait]
    impl StreamHandle for NoOpStreamHandle {
        async fn next_events(&mut self, _: u64) -> crate::core::Result<Vec<Event>> {
            Ok(Vec::new())
        }
        async fn save_position(
            &self,
            _: &mut dyn crate::checkpoint::Checkpoint,
        ) -> crate::core::Result<()> {
            Ok(())
        }
        async fn confirm_lsn(&mut self, _: u64) -> crate::core::Result<()> {
            Ok(())
        }
    }

    struct HandoffStreamHandle {
        batches: VecDeque<Vec<Event>>,
        requeued: Vec<Event>,
    }

    impl HandoffStreamHandle {
        fn new(batches: Vec<Vec<Event>>) -> Self {
            Self {
                batches: batches.into_iter().collect(),
                requeued: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl StreamHandle for HandoffStreamHandle {
        async fn next_events(&mut self, _: u64) -> crate::core::Result<Vec<Event>> {
            Ok(self.batches.pop_front().unwrap_or_default())
        }

        async fn save_position(
            &self,
            _: &mut dyn crate::checkpoint::Checkpoint,
        ) -> crate::core::Result<()> {
            Ok(())
        }

        async fn requeue_events(&mut self, events: Vec<Event>) -> crate::core::Result<()> {
            self.requeued.extend(events);
            Ok(())
        }

        async fn confirm_lsn(&mut self, _: u64) -> crate::core::Result<()> {
            Ok(())
        }
    }

    fn handoff_event(offset: &str, id: i64) -> Event {
        Event {
            before: None,
            after: Some(json!({"id": id, "v": format!("value-{id}")})),
            op: crate::core::Operation::Update,
            source: crate::core::SourceMetadata {
                source_name: "mysql".into(),
                offset: offset.into(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("app".into()),
            table: "users".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: Some(crate::core::TransactionMetadata {
                tx_id: 1,
                total_events: 1,
                event_index: 0,
            }),
            envelope_version: crate::core::EVENT_ENVELOPE_VERSION,
        }
    }

    #[tokio::test]
    async fn handoff_succeeds_when_stream_starts_at_snapshot_watermark() {
        let wm = MysqlOffset {
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 1024,
            gtid: String::new(),
        };
        let mut conn = make_connection_with_watermarks(wm.clone(), wm.clone());
        let result = conn
            .perform_handoff(&mut AlreadyDoneSnapshotHandle, &mut NoOpStreamHandle)
            .await
            .unwrap();
        assert_eq!(result.snapshot_end_ts, Some(1_700_000_000_000));
        assert!(result.stream_start_ts.is_some());
        assert_eq!(result.overlap_events_dropped, 0);
    }

    #[tokio::test]
    async fn handoff_succeeds_when_stream_starts_before_snapshot_watermark() {
        let snapshot_wm = MysqlOffset {
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 2048,
            gtid: String::new(),
        };
        let stream_wm = MysqlOffset {
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 512,
            gtid: String::new(),
        };
        let mut conn = make_connection_with_watermarks(snapshot_wm, stream_wm);
        let result = conn
            .perform_handoff(&mut AlreadyDoneSnapshotHandle, &mut NoOpStreamHandle)
            .await
            .unwrap();
        // No overlap events were prefetched from stream in this test setup.
        assert_eq!(result.overlap_events_dropped, 0);
    }

    #[tokio::test]
    async fn handoff_fails_when_stream_starts_after_snapshot_watermark() {
        let snapshot_wm = MysqlOffset {
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 512,
            gtid: String::new(),
        };
        let stream_wm = MysqlOffset {
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 1024,
            gtid: String::new(),
        };
        let mut conn = make_connection_with_watermarks(snapshot_wm, stream_wm);
        let err = conn
            .perform_handoff(&mut AlreadyDoneSnapshotHandle, &mut NoOpStreamHandle)
            .await
            .unwrap_err();
        assert!(matches!(err, crate::core::Error::SourceError(_)));
    }

    #[tokio::test]
    async fn handoff_fails_when_stream_starts_in_later_binlog_file() {
        // Even if pos is small, a later file means gap.
        let snapshot_wm = MysqlOffset {
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 9999,
            gtid: String::new(),
        };
        let stream_wm = MysqlOffset {
            binlog_file: "mysql-bin.000002".into(),
            binlog_pos: 4,
            gtid: String::new(),
        };
        let mut conn = make_connection_with_watermarks(snapshot_wm, stream_wm);
        let err = conn
            .perform_handoff(&mut AlreadyDoneSnapshotHandle, &mut NoOpStreamHandle)
            .await
            .unwrap_err();
        assert!(matches!(err, crate::core::Error::SourceError(_)));
    }

    #[tokio::test]
    async fn handoff_fails_when_snapshot_watermark_not_set() {
        let mut conn = MysqlConnection::new(MysqlSourceConfig {
            host: "localhost".into(),
            port: 3306,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            server_id: 1,
            gtid_mode_enabled: false,
            binlog_format_check: true,
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        });
        // Only set stream_start, not snapshot_watermark
        conn.stream_start = Some(MysqlOffset {
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 4,
            gtid: String::new(),
        });
        let err = conn
            .perform_handoff(&mut AlreadyDoneSnapshotHandle, &mut NoOpStreamHandle)
            .await
            .unwrap_err();
        assert!(matches!(err, crate::core::Error::StateError(_)));
    }

    #[tokio::test]
    async fn handoff_succeeds_when_stream_starts_in_earlier_binlog_file() {
        let snapshot_wm = MysqlOffset {
            binlog_file: "mysql-bin.000002".into(),
            binlog_pos: 100,
            gtid: String::new(),
        };
        let stream_wm = MysqlOffset {
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 9999,
            gtid: String::new(),
        };
        let mut conn = make_connection_with_watermarks(snapshot_wm, stream_wm);
        let result = conn
            .perform_handoff(&mut AlreadyDoneSnapshotHandle, &mut NoOpStreamHandle)
            .await
            .unwrap();
        // Different files: no byte-overlap count.
        assert_eq!(result.overlap_events_dropped, 0);
    }

    #[tokio::test]
    async fn handoff_overlap_is_deduplicated_by_primary_key_and_requeued() {
        let snapshot_wm = MysqlOffset {
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 100,
            gtid: String::new(),
        };
        let stream_wm = MysqlOffset {
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 10,
            gtid: String::new(),
        };
        let mut conn = make_connection_with_watermarks(snapshot_wm, stream_wm);

        let mut stream = HandoffStreamHandle::new(vec![vec![
            handoff_event("mysql-bin.000001:90", 1),
            handoff_event("mysql-bin.000001:95", 1),
            handoff_event("mysql-bin.000001:99", 2),
            handoff_event("mysql-bin.000001:120", 3),
        ]]);

        let result = conn
            .perform_handoff(&mut AlreadyDoneSnapshotHandle, &mut stream)
            .await
            .unwrap();

        // id=1 appears twice in the overlap window and must be compacted.
        assert_eq!(result.overlap_events_dropped, 1);
        assert_eq!(stream.requeued.len(), 3);
        assert_eq!(
            stream.requeued[0].after.as_ref().unwrap()["id"],
            serde_json::json!(1)
        );
        assert_eq!(
            stream.requeued[1].after.as_ref().unwrap()["id"],
            serde_json::json!(2)
        );
        assert_eq!(
            stream.requeued[2].after.as_ref().unwrap()["id"],
            serde_json::json!(3)
        );
    }

    #[test]
    fn dedup_overlap_events_by_pk_keeps_last_writer_wins() {
        let events = vec![
            handoff_event("mysql-bin.000001:10", 10),
            handoff_event("mysql-bin.000001:11", 10),
            handoff_event("mysql-bin.000001:12", 11),
        ];

        let (deduped, duplicates) = super::query::dedup_overlap_events_by_pk(events);
        assert_eq!(duplicates, 1);
        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].after.as_ref().unwrap()["id"], json!(10));
        assert_eq!(deduped[1].after.as_ref().unwrap()["id"], json!(11));
    }

    #[test]
    fn format_gtid_renders_standard_sid_gno_notation() {
        let sid = [
            0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42,
            0x95, 0x62,
        ];
        let gtid = super::format_gtid(sid, 23);
        assert_eq!(gtid, "3e11fa47-71ca-11e1-9e33-c80aa9429562:23");
    }

    #[test]
    fn mysql_value_to_json_encodes_binary_bytes_as_hex() {
        let value = super::mysql_value_to_json(&super::MysqlValue::Bytes(vec![0xff, 0x00, 0x1a]));
        assert_eq!(value, json!("ff001a"));
    }

    // ── Large transaction test ───────────────────────────────────────────────

    #[tokio::test]
    async fn stream_large_transaction_handles_1k_plus_events() {
        // Build a transaction that spans two provider batches (600 + 600 events).
        // The Xid commits only in the second batch, so partial_tx_events must accumulate
        // across poll boundaries without losing or duplicating events.
        const TX_EVENTS: usize = 1_200;
        const BATCH: usize = 600;

        let mut first_batch: Vec<MysqlBinlogMessage> = vec![MysqlBinlogMessage::Begin {
            tx_id: 42,
            timestamp_ms: 1,
        }];
        for i in 0..BATCH {
            first_batch.push(MysqlBinlogMessage::WriteRows(row_change(
                "orders",
                None,
                Some(json!({"id": i, "v": "a"})),
            )));
        }

        let mut second_batch: Vec<MysqlBinlogMessage> = Vec::new();
        for i in BATCH..TX_EVENTS {
            second_batch.push(MysqlBinlogMessage::WriteRows(row_change(
                "orders",
                None,
                Some(json!({"id": i, "v": "b"})),
            )));
        }
        second_batch.push(MysqlBinlogMessage::Xid {
            tx_id: 42,
            timestamp_ms: 2,
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 9999,
            gtid: None,
        });
        // Third batch is empty — triggers timeout path.
        let provider = MockBinlogProvider::new(vec![first_batch, second_batch, vec![]]);

        let mut handle = make_stream_handle("mysql-bin.000001", 4, "", provider);

        // First poll collects the first batch but no Xid yet → empty committed events.
        // timeout_ms=0 keeps the call to a single provider poll, making batching deterministic.
        let events1 = handle.next_events(0).await.unwrap();
        assert!(
            events1.is_empty(),
            "no Xid in first batch so nothing committed yet"
        );

        // Second poll processes the rest + Xid → all TX_EVENTS committed together.
        let events2 = handle.next_events(0).await.unwrap();
        assert_eq!(
            events2.len(),
            TX_EVENTS,
            "all {TX_EVENTS} events committed on Xid"
        );

        // All events in same transaction.
        for event in &events2 {
            let tx = event.transaction.as_ref().expect("transaction metadata");
            assert_eq!(tx.tx_id, 42);
        }
        assert_eq!(handle.events_polled, TX_EVENTS as u64);
    }
}
