//! SQL Server source configuration and connection lifecycle.

use std::{
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, sync::Mutex};

use crate::{
    checkpoint::GenericOffset,
    core::{
        Error, Offset, Result, SecretString, StructuredLogger, TransportConfig,
    },
    core::Event,
    source::{ConnectorCapabilities, HandoffResult, SnapshotHandle, Source, StreamHandle},
};
#[cfg(test)]
use crate::core::{Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
use crate::source::helpers::now_millis;

mod parser;
mod query;
mod state;
mod config;
mod prereq;
mod snapshot_fetch;
mod stream_schema;
mod stream_window;
mod snapshot_chunk;
mod snapshot_finalize;
mod stream_start;
mod snapshot_start;
mod connection_lifecycle;

use self::snapshot_chunk::next_sqlserver_snapshot_chunk;
use self::snapshot_finalize::{checkpoint_sqlserver_snapshot, finish_sqlserver_snapshot};
use self::snapshot_start::{
    start_sqlserver_snapshot_from_checkpoint, start_sqlserver_snapshot_internal,
};
use self::connection_lifecycle::connect_sqlserver_with_probe;
use self::stream_start::start_sqlserver_stream;

use self::prereq::{
    LiveSqlServerPrereqProbe, SqlServerPrereqProbe, SqlServerPrereqSnapshot,
};
use self::snapshot_fetch::{
    DisconnectedSqlServerSnapshotRowFetcher, LiveSqlServerSnapshotRowFetcher,
    SqlServerSnapshotRowFetcher,
};
use self::state::{
    ConnectionState, SqlServerHandoff, SqlServerSnapshotCheckpointState, TableSnapshotState,
};

const HEARTBEAT_SECS: u64 = 60;
const DEFAULT_POOL_SIZE: usize = 4;
const DEFAULT_STREAM_POLL_INTERVAL_MS: u64 = 5000;
// Keep this high enough to avoid dropping or starving busy CDC windows when
// a poll covers a large LSN span (for example, bursty insert workloads).
const MAX_EVENTS_PER_POLL: usize = 10_000;
const ZERO_LSN_HEX: &str = "0x00000000000000000000";

type SqlClient = tiberius::Client<tokio_util::compat::Compat<TcpStream>>;

/// Configuration for a SQL Server CDC connection.
#[derive(Clone, PartialEq, Eq)]
pub struct SqlServerSourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: SecretString,
    pub database: String,
    pub instance_name: Option<String>,
    pub transport: TransportConfig,
    pub conn_timeout_secs: u64,
    pub cdc_enabled: bool,
    pub cdc_schema: String,
    /// Maximum concurrent SQL Server connections used by prerequisite checks.
    ///
    /// This does not change stream snapshot semantics directly, but it bounds
    /// probe/heartbeat fanout pressure under multi-runtime deployments.
    pub prereq_pool_size: usize,
    /// Stream poll interval in milliseconds.
    pub stream_poll_interval_ms: u64,
    /// Maximum events yielded by a single stream poll cycle.
    pub max_events_per_poll: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CaptureInstanceMeta {
    capture_instance: String,
    schema: String,
    table: String,
    primary_key: Vec<String>,
    captured_columns: Vec<String>,
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
pub struct SqlServerSnapshot {
    pub lsn_start: [u8; 10],
    pub snapshot_id: String,
    pub tables: Vec<TableSnapshot>,
}

/// SQL Server CDC stream state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlServerStream {
    pub lsn_start: [u8; 10],
    pub lsn_end: [u8; 10],
    pub change_tables: Vec<String>,
    pub poll_interval_ms: u64,
}

#[derive(Debug, Clone)]
struct SqlServerRawChange {
    start_lsn_hex: String,
    seqval_hex: String,
    operation: i32,
    ts_ms: u64,
    row: serde_json::Value,
}

impl SqlServerHandoff {
    fn has_no_gap(&self) -> bool {
        compare_lsn(&self.stream_lsn_start, &self.snapshot_lsn_start).is_le()
    }
}

pub struct SqlServerStreamHandle {
    config: SqlServerSourceConfig,
    stream: SqlServerStream,
    metas: Vec<CaptureInstanceMeta>,
    events_polled: u64,
    requeued_events: Vec<Event>,
    max_events_per_poll: usize,
}

pub struct SqlServerSnapshotHandle {
    snapshot: SqlServerSnapshot,
    tables: Vec<TableSnapshotState>,
    client: Option<Arc<Mutex<SqlClient>>>,
    row_fetcher: Arc<dyn SqlServerSnapshotRowFetcher>,
    transaction_open: bool,
    current_table: usize,
    next_chunk_index: u32,
    emitted_rows: u64,
}

impl SqlServerSnapshotHandle {
    fn new(
        snapshot: SqlServerSnapshot,
        tables: Vec<TableSnapshotState>,
        client: Option<SqlClient>,
        transaction_open: bool,
    ) -> Self {
        let client = client.map(|value| Arc::new(Mutex::new(value)));
        let row_fetcher: Arc<dyn SqlServerSnapshotRowFetcher> = if let Some(client_ref) = &client {
            Arc::new(LiveSqlServerSnapshotRowFetcher {
                client: client_ref.clone(),
            })
        } else {
            Arc::new(DisconnectedSqlServerSnapshotRowFetcher)
        };

        Self {
            snapshot,
            tables,
            client,
            row_fetcher,
            transaction_open,
            current_table: 0,
            next_chunk_index: 0,
            emitted_rows: 0,
        }
    }

    #[cfg(test)]
    fn new_with_fetcher(
        snapshot: SqlServerSnapshot,
        tables: Vec<TableSnapshotState>,
        row_fetcher: Arc<dyn SqlServerSnapshotRowFetcher>,
    ) -> Self {
        Self {
            snapshot,
            tables,
            client: None,
            row_fetcher,
            transaction_open: false,
            current_table: 0,
            next_chunk_index: 0,
            emitted_rows: 0,
        }
    }

    fn resume_from_checkpoint_payload(mut self, payload: &[u8]) -> Result<Self> {
        let state: SqlServerSnapshotCheckpointState = serde_json::from_slice(payload)?;
        if state.tables.len() != self.tables.len() {
            return Err(Error::CheckpointError(
                "sqlserver snapshot checkpoint table count does not match snapshot handle".into(),
            ));
        }

        self.snapshot.snapshot_id = state.snapshot_id;
        self.snapshot.lsn_start = state.lsn_start;
        self.current_table = state.current_table;
        self.next_chunk_index = state.next_chunk_index;
        self.emitted_rows = 0;

        for (index, table_state) in self.tables.iter_mut().enumerate() {
            let saved = &state.tables[index];
            table_state.snapshot = saved.clone();
            self.emitted_rows = self.emitted_rows.saturating_add(saved.rows_processed);
        }

        self.sync_snapshot_tables();
        Ok(self)
    }

    fn sync_snapshot_tables(&mut self) {
        self.snapshot.tables = self
            .tables
            .iter()
            .map(|table| table.snapshot.clone())
            .collect();
    }

    fn is_complete(&self) -> bool {
        self.tables.iter().all(|table| table.snapshot.is_complete)
    }

    fn total_expected_rows(&self) -> u64 {
        self.tables
            .iter()
            .map(|table| table.snapshot.total_rows)
            .sum()
    }
}

fn lsn_hex_to_bytes(lsn_hex: &str) -> Result<[u8; 10]> {
    parser::lsn_hex_to_bytes(lsn_hex)
}

fn lsn_bytes_to_hex(lsn: &[u8; 10]) -> String {
    parser::lsn_bytes_to_hex(lsn)
}

fn compare_lsn(left: &[u8; 10], right: &[u8; 10]) -> std::cmp::Ordering {
    parser::compare_lsn(left, right)
}

fn tx_id_from_seqval(seqval_hex: &str) -> u64 {
    parser::tx_id_from_seqval(seqval_hex)
}

fn lsn_from_source_offset(offset: &str) -> Option<[u8; 10]> {
    parser::lsn_from_source_offset(offset)
}

fn sqlserver_resume_lsn_from_offset_bytes(encoded: &[u8]) -> Result<[u8; 10]> {
    parser::sqlserver_resume_lsn_from_offset_bytes(encoded)
}

fn dedup_overlap_events_by_pk(events: Vec<Event>) -> (Vec<Event>, u64) {
    parser::dedup_overlap_events_by_pk(events)
}

fn validate_capture_instance_name(name: &str) -> Result<()> {
    parser::validate_capture_instance_name(name)
}

fn parse_schema_table(name: &str) -> Result<(String, String)> {
    parser::parse_schema_table(name)
}

fn qualified_table_name(schema: &str, table: &str) -> String {
    parser::qualified_table_name(schema, table)
}

fn build_snapshot_fetch_sql(
    table_ref: &str,
    primary_key_columns: &[String],
    column_names: &[String],
    limit_param_index: usize,
    include_seek_where_clause: bool,
) -> String {
    parser::build_snapshot_fetch_sql(
        table_ref,
        primary_key_columns,
        column_names,
        limit_param_index,
        include_seek_where_clause,
    )
}

fn build_cdc_poll_sql(
    capture_instance: &str,
    columns: &[String],
    max_events_per_poll: usize,
    start_lsn_hex: &str,
    end_lsn_hex: &str,
) -> String {
    parser::build_cdc_poll_sql(
        capture_instance,
        columns,
        max_events_per_poll,
        start_lsn_hex,
        end_lsn_hex,
    )
}

fn build_snapshot_row_count_sql(schema: &str, table: &str) -> String {
    parser::build_snapshot_row_count_sql(schema, table)
}

#[derive(Debug, Clone)]
enum SqlServerCursorParam {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl SqlServerCursorParam {
    fn bind(&self, query: &mut tiberius::Query) {
        match self {
            Self::Bool(value) => {
                query.bind(*value);
            }
            Self::Int(value) => {
                query.bind(*value);
            }
            Self::Float(value) => {
                query.bind(*value);
            }
            Self::Text(value) => {
                query.bind(value.clone());
            }
        }
    }
}

fn sqlserver_json_value_to_param(value: &serde_json::Value) -> Result<SqlServerCursorParam> {
    match value {
        serde_json::Value::Null => Err(Error::CheckpointError(
            "sqlserver snapshot cursor does not support NULL primary key values".into(),
        )),
        serde_json::Value::Bool(boolean) => Ok(SqlServerCursorParam::Bool(*boolean)),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Ok(SqlServerCursorParam::Int(value))
            } else if let Some(value) = number.as_u64() {
                let value = i64::try_from(value).map_err(|_| {
                    Error::CheckpointError("sqlserver snapshot cursor integer exceeds i64".into())
                })?;
                Ok(SqlServerCursorParam::Int(value))
            } else if let Some(value) = number.as_f64() {
                Ok(SqlServerCursorParam::Float(value))
            } else {
                Err(Error::CheckpointError(
                    "sqlserver snapshot cursor contains unsupported numeric value".into(),
                ))
            }
        }
        serde_json::Value::String(text) => Ok(SqlServerCursorParam::Text(text.clone())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Err(Error::CheckpointError(
            "sqlserver snapshot cursor only supports scalar PK values".into(),
        )),
    }
}

fn is_sqlserver_cdc_window_error(message: &str) -> bool {
    parser::is_sqlserver_cdc_window_error(message)
}

async fn load_capture_metas_for_config(
    config: &SqlServerSourceConfig,
    error_prefix: &str,
    require_non_empty_metas: bool,
    require_non_empty_columns: bool,
) -> Result<Vec<CaptureInstanceMeta>> {
    let mut client = query::connect_client(config).await?;
    let rows = client
        .query(
            "SELECT ct.capture_instance, sc.name AS source_schema, tb.name AS source_name \
             FROM cdc.change_tables ct \
             JOIN sys.tables tb ON ct.source_object_id = tb.object_id \
             JOIN sys.schemas sc ON tb.schema_id = sc.schema_id \
             ORDER BY ct.capture_instance",
            &[],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!("{error_prefix} metadata query failed: {error}"))
        })?
        .into_first_result()
        .await
        .map_err(|error| {
            Error::SourceError(format!("{error_prefix} metadata decode failed: {error}"))
        })?;

    let mut metas = Vec::new();
    for row in rows {
        let capture_instance = row.get::<&str, _>(0).ok_or_else(|| {
            Error::SourceError(format!("{error_prefix} metadata missing capture_instance"))
        })?;
        validate_capture_instance_name(capture_instance)?;
        let schema = row
            .get::<&str, _>(1)
            .ok_or_else(|| {
                Error::SourceError(format!("{error_prefix} metadata missing source_schema"))
            })?
            .to_string();
        let table = row
            .get::<&str, _>(2)
            .ok_or_else(|| {
                Error::SourceError(format!("{error_prefix} metadata missing source_name"))
            })?
            .to_string();

        let captured_columns =
            load_captured_columns_for_instance(&mut client, capture_instance, error_prefix).await?;
        if require_non_empty_columns && captured_columns.is_empty() {
            return Err(Error::SourceError(format!(
                "sqlserver capture instance '{capture_instance}' has no captured columns"
            )));
        }
        let primary_key =
            load_primary_key_columns_for_instance(&mut client, capture_instance, error_prefix)
                .await?;

        metas.push(CaptureInstanceMeta {
            capture_instance: capture_instance.to_string(),
            schema,
            table,
            primary_key,
            captured_columns,
        });
    }

    if require_non_empty_metas && metas.is_empty() {
        return Err(Error::SourceError(
            "sqlserver CDC has no capture instances; enable CDC on at least one table".into(),
        ));
    }

    Ok(metas)
}

async fn load_captured_columns_for_instance(
    client: &mut SqlClient,
    capture_instance: &str,
    error_prefix: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT cc.column_name \
             FROM cdc.captured_columns cc \
             JOIN cdc.change_tables ct ON cc.object_id = ct.object_id \
             WHERE ct.capture_instance = @P1 \
             ORDER BY cc.column_id",
            &[&capture_instance],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "{error_prefix} captured columns query failed for '{capture_instance}': {error}"
            ))
        })?
        .into_first_result()
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "{error_prefix} captured columns decode failed for '{capture_instance}': {error}"
            ))
        })?;

    Ok(rows
        .into_iter()
        .filter_map(|row| row.get::<&str, _>(0).map(|value| value.to_string()))
        .collect())
}

async fn load_primary_key_columns_for_instance(
    client: &mut SqlClient,
    capture_instance: &str,
    error_prefix: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT ic.column_name \
             FROM cdc.index_columns ic \
             JOIN cdc.change_tables ct ON ic.object_id = ct.object_id \
             WHERE ct.capture_instance = @P1 \
             ORDER BY ic.index_ordinal",
            &[&capture_instance],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "{error_prefix} primary key metadata query failed for '{capture_instance}': {error}"
            ))
        })?
        .into_first_result()
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "{error_prefix} primary key metadata decode failed for '{capture_instance}': {error}"
            ))
        })?;

    Ok(rows
        .into_iter()
        .filter_map(|row| row.get::<&str, _>(0).map(|value| value.to_string()))
        .collect())
}

impl SqlServerStreamHandle {
}

fn decode_sqlserver_cell_to_json(row: &tiberius::Row, index: usize) -> serde_json::Value {
    if let Ok(Some(text)) = row.try_get::<&str, _>(index) {
        return serde_json::Value::String(text.to_string());
    }
    if let Ok(Some(number)) = row.try_get::<i64, _>(index) {
        return serde_json::Value::Number(number.into());
    }
    if let Ok(Some(number)) = row.try_get::<i32, _>(index) {
        return serde_json::Value::Number((number as i64).into());
    }
    if let Ok(Some(number)) = row.try_get::<f64, _>(index) {
        if let Some(value) = serde_json::Number::from_f64(number) {
            return serde_json::Value::Number(value);
        }
    }
    if let Ok(Some(boolean)) = row.try_get::<bool, _>(index) {
        return serde_json::Value::Bool(boolean);
    }

    serde_json::Value::Null
}

#[async_trait]
impl StreamHandle for SqlServerStreamHandle {
    async fn next_events(&mut self, timeout_ms: u64) -> Result<Vec<Event>> {
        if !self.requeued_events.is_empty() {
            let drained = self.requeued_events.drain(..).collect::<Vec<_>>();
            return Ok(drained);
        }

        let mut schema_events = self.refresh_metas_and_collect_schema_events().await?;
        if !schema_events.is_empty() {
            self.events_polled = self
                .events_polled
                .saturating_add(schema_events.len() as u64);
            if schema_events.len() > self.max_events_per_poll {
                schema_events.truncate(self.max_events_per_poll);
            }
            return Ok(schema_events);
        }

        let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
        let mut out = Vec::new();

        while out.is_empty() && std::time::Instant::now() <= deadline {
            for meta in &self.metas {
                let changes = self
                    .fetch_changes_for_capture_instance(
                        &meta.capture_instance,
                        &meta.captured_columns,
                        self.max_events_per_poll,
                    )
                    .await?;
                if changes.is_empty() {
                    continue;
                }

                let mut events = self.map_changes_to_events(meta, changes)?;
                self.events_polled += events.len() as u64;
                out.append(&mut events);
                if out.len() >= self.max_events_per_poll {
                    out.truncate(self.max_events_per_poll);
                    break;
                }
            }

            if out.is_empty() {
                let sleep_for = self
                    .stream
                    .poll_interval_ms
                    .min(timeout_ms.max(1))
                    .min(DEFAULT_STREAM_POLL_INTERVAL_MS);
                tokio::time::sleep(Duration::from_millis(sleep_for)).await;
            }

            self.advance_window().await?;
        }

        Ok(out)
    }

    async fn save_position(
        &self,
        checkpoint: &mut dyn crate::checkpoint::Checkpoint,
    ) -> Result<()> {
        let offset = GenericOffset::new(
            "sqlserver",
            serde_json::to_vec(&lsn_bytes_to_hex(&self.stream.lsn_start))
                .map_err(|error| Error::SerializationError(error.to_string()))?,
        );
        checkpoint.save(&offset, self.events_polled).await
    }

    async fn confirm_lsn(&mut self, _lsn: u64) -> Result<()> {
        Ok(())
    }

    async fn requeue_events(&mut self, mut events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        events.append(&mut self.requeued_events);
        self.requeued_events = events;
        Ok(())
    }
}

#[async_trait]
impl SnapshotHandle for SqlServerSnapshotHandle {
    async fn next_chunk(&mut self, chunk_size: usize) -> Result<Vec<Event>> {
        next_sqlserver_snapshot_chunk(self, chunk_size).await
    }

    async fn checkpoint(
        &self,
        checkpoint: &mut dyn crate::checkpoint::Checkpoint,
        committed_event_count: u64,
    ) -> Result<()> {
        checkpoint_sqlserver_snapshot(self, checkpoint, committed_event_count).await
    }

    async fn finish(&mut self) -> Result<crate::source::SnapshotEnd> {
        finish_sqlserver_snapshot(self).await
    }
}

/// SQL Server connector lifecycle manager.
pub struct SqlServerConnection {
    config: SqlServerSourceConfig,
    logger: StructuredLogger,
    state: Arc<Mutex<ConnectionState>>,
    prereq_probe: Arc<dyn SqlServerPrereqProbe>,
    stream_poll_interval_ms: u64,
    max_events_per_poll: usize,
}

impl SqlServerConnection {
    pub fn new(config: SqlServerSourceConfig) -> Self {
        let prereq_pool_size = config.prereq_pool_size.max(1);
        let stream_poll_interval_ms = config.stream_poll_interval_ms.max(1);
        let max_events_per_poll = config.max_events_per_poll.max(1);
        Self {
            config,
            logger: StructuredLogger::new("sqlserver"),
            state: Arc::new(Mutex::new(ConnectionState::default())),
            prereq_probe: Arc::new(LiveSqlServerPrereqProbe::new(prereq_pool_size)),
            stream_poll_interval_ms,
            max_events_per_poll,
        }
    }

    #[cfg(test)]
    fn with_probe(config: SqlServerSourceConfig, probe: Arc<dyn SqlServerPrereqProbe>) -> Self {
        let stream_poll_interval_ms = config.stream_poll_interval_ms.max(1);
        let max_events_per_poll = config.max_events_per_poll.max(1);
        Self {
            config,
            logger: StructuredLogger::new("sqlserver"),
            state: Arc::new(Mutex::new(ConnectionState::default())),
            prereq_probe: probe,
            stream_poll_interval_ms,
            max_events_per_poll,
        }
    }

    pub async fn connect(&self) -> Result<()> {
        connect_sqlserver_with_probe(self).await
    }

    pub async fn close(&self) {
        let mut state = self.state.lock().await;
        if let Some(task) = state.heartbeat_task.take() {
            task.abort();
        }
        if state.connected {
            self.logger.source_disconnected();
        }
        state.connected = false;
        state.snapshot_lsn_start = None;
        state.stream_lsn_start = None;
    }

    pub async fn is_connected(&self) -> bool {
        self.state.lock().await.connected
    }

    async fn ensure_connected(&self) -> Result<()> {
        if self.is_connected().await {
            Ok(())
        } else {
            Err(Error::StateError(
                "sqlserver connection must be established before starting stream".into(),
            ))
        }
    }

    async fn load_capture_metas(&self) -> Result<Vec<CaptureInstanceMeta>> {
        load_capture_metas_for_config(&self.config, "sqlserver change table", true, true).await
    }

    async fn query_max_lsn_hex(&self) -> Result<String> {
        let mut client = query::connect_client(&self.config).await?;
        let rows = client
            .query(
                "SELECT sys.fn_varbintohexstr(sys.fn_cdc_get_max_lsn())",
                &[],
            )
            .await
            .map_err(|error| {
                Error::SourceError(format!("sqlserver max LSN query failed: {error}"))
            })?
            .into_first_result()
            .await
            .map_err(|error| {
                Error::SourceError(format!("sqlserver max LSN decode failed: {error}"))
            })?;

        let value = rows
            .into_iter()
            .next()
            .and_then(|row| row.get::<&str, _>(0).map(ToOwned::to_owned))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| ZERO_LSN_HEX.to_string());

        Ok(value)
    }

    async fn query_min_lsn_hex(&self, capture_instance: &str) -> Result<String> {
        let mut client = query::connect_client(&self.config).await?;
        let rows = client
            .query(
                "SELECT sys.fn_varbintohexstr(sys.fn_cdc_get_min_lsn(@P1))",
                &[&capture_instance],
            )
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "sqlserver min LSN query failed for '{capture_instance}': {error}"
                ))
            })?
            .into_first_result()
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "sqlserver min LSN decode failed for '{capture_instance}': {error}"
                ))
            })?;

        let value = rows
            .into_iter()
            .next()
            .and_then(|row| row.get::<&str, _>(0).map(ToOwned::to_owned))
            .unwrap_or_else(|| ZERO_LSN_HEX.to_string());
        if value.is_empty() {
            Ok(ZERO_LSN_HEX.to_string())
        } else {
            Ok(value)
        }
    }

    async fn load_snapshot_tables(
        &self,
        client: &mut SqlClient,
        tables: &[&str],
    ) -> Result<Vec<TableSnapshotState>> {
        if tables.is_empty() {
            return Err(Error::ConfigError(
                "sqlserver snapshot requires at least one table".into(),
            ));
        }

        let mut states = Vec::with_capacity(tables.len());

        for entry in tables {
            let (schema, table) = parse_schema_table(entry)?;
            let column_names = self
                .load_table_columns(client, schema.as_str(), table.as_str())
                .await?;
            if column_names.is_empty() {
                return Err(Error::SourceError(format!(
                    "sqlserver snapshot table '{}.{}' has no columns",
                    schema, table
                )));
            }

            let primary_key_columns = self
                .load_table_primary_key_columns(client, schema.as_str(), table.as_str())
                .await?;
            if primary_key_columns.is_empty() {
                return Err(Error::SourceError(format!(
                    "sqlserver snapshot requires a PRIMARY KEY: {}.{}",
                    schema, table
                )));
            }

            let total_rows = self
                .query_table_row_count(client, schema.as_str(), table.as_str())
                .await?;

            states.push(TableSnapshotState {
                snapshot: TableSnapshot {
                    table: format!("{schema}.{table}"),
                    total_rows,
                    rows_processed: 0,
                    cursor_position: None,
                    is_complete: total_rows == 0,
                },
                schema,
                table,
                primary_key_columns,
                column_names,
            });
        }

        Ok(states)
    }

    async fn begin_snapshot_transaction(client: &mut SqlClient) -> Result<bool> {
        // Prefer SNAPSHOT isolation for non-blocking consistent reads when enabled.
        let snapshot_isolation_ok = client
            .execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT", &[])
            .await
            .is_ok();

        // Fallback to SERIALIZABLE for deterministic consistency when SNAPSHOT is unavailable.
        if !snapshot_isolation_ok {
            client
                .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
                .await
                .map_err(|error| {
                    Error::SourceError(format!(
                        "sqlserver failed to configure snapshot isolation level: {error}"
                    ))
                })?;
        }

        match client.execute("BEGIN TRANSACTION", &[]).await {
            Ok(_) => Ok(true),
            Err(error) => {
                let text = error.to_string();
                if text.contains("code: 266") {
                    // Some SQL Server/TDS paths reject explicit BEGIN in this execution mode.
                    // Degrade gracefully: continue snapshot without an explicit transaction.
                    return Ok(false);
                }

                Err(Error::SourceError(format!(
                    "sqlserver failed to start consistent snapshot transaction: {error}"
                )))
            }
        }
    }

    async fn start_snapshot_internal(
        &mut self,
        tables: &[&str],
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn SnapshotHandle>> {
        start_sqlserver_snapshot_internal(self, tables, resume_from).await
    }

    pub async fn start_snapshot_from_checkpoint(
        &mut self,
        tables: &[&str],
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn SnapshotHandle>> {
        start_sqlserver_snapshot_from_checkpoint(self, tables, resume_from).await
    }

    async fn load_table_columns(
        &self,
        client: &mut SqlClient,
        schema: &str,
        table: &str,
    ) -> Result<Vec<String>> {
        let rows = client
			.query(
				"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2 ORDER BY ORDINAL_POSITION",
				&[&schema, &table],
			)
			.await
			.map_err(|error| {
				Error::SourceError(format!(
					"sqlserver snapshot columns query failed for '{}.{}': {error}",
					schema, table
				))
			})?
			.into_first_result()
			.await
			.map_err(|error| {
				Error::SourceError(format!(
					"sqlserver snapshot columns decode failed for '{}.{}': {error}",
					schema, table
				))
			})?;

        Ok(rows
            .into_iter()
            .filter_map(|row| row.get::<&str, _>(0).map(ToOwned::to_owned))
            .collect())
    }

    async fn load_table_primary_key_columns(
        &self,
        client: &mut SqlClient,
        schema: &str,
        table: &str,
    ) -> Result<Vec<String>> {
        let rows = client
            .query(
                "SELECT k.COLUMN_NAME \
				 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
				 JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k \
				   ON tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME \
				  AND tc.TABLE_SCHEMA = k.TABLE_SCHEMA \
				 WHERE tc.TABLE_SCHEMA = @P1 \
				   AND tc.TABLE_NAME = @P2 \
				   AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
				 ORDER BY k.ORDINAL_POSITION",
                &[&schema, &table],
            )
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "sqlserver snapshot PK query failed for '{}.{}': {error}",
                    schema, table
                ))
            })?
            .into_first_result()
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "sqlserver snapshot PK decode failed for '{}.{}': {error}",
                    schema, table
                ))
            })?;

        Ok(rows
            .into_iter()
            .filter_map(|row| row.get::<&str, _>(0).map(ToOwned::to_owned))
            .collect())
    }

    async fn query_table_row_count(
        &self,
        client: &mut SqlClient,
        schema: &str,
        table: &str,
    ) -> Result<u64> {
        let sql = build_snapshot_row_count_sql(schema, table);
        let rows = client
            .query(&sql, &[])
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "sqlserver snapshot row count query failed for '{}.{}': {error}",
                    schema, table
                ))
            })?
            .into_first_result()
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "sqlserver snapshot row count decode failed for '{}.{}': {error}",
                    schema, table
                ))
            })?;

        let count = rows
            .into_iter()
            .next()
            .and_then(|row| row.get::<i64, _>(0))
            .ok_or_else(|| {
                Error::SourceError(format!(
                    "sqlserver snapshot row count returned no value for '{}.{}'",
                    schema, table
                ))
            })?;
        u64::try_from(count).map_err(|_| {
            Error::SourceError(format!(
                "sqlserver snapshot row count was negative for '{}.{}'",
                schema, table
            ))
        })
    }

}

impl Drop for SqlServerConnection {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.try_lock() {
            if let Some(task) = state.heartbeat_task.take() {
                task.abort();
            }
            state.connected = false;
        }
    }
}

#[async_trait]
impl Source for SqlServerConnection {
    async fn start_snapshot(&mut self, tables: &[&str]) -> Result<Box<dyn SnapshotHandle>> {
        self.start_snapshot_internal(tables, None).await
    }

    async fn start_stream(
        &mut self,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>> {
        start_sqlserver_stream(self, resume_from).await
    }

    async fn perform_handoff(
        &mut self,
        snapshot: &mut dyn SnapshotHandle,
        stream: &mut dyn StreamHandle,
    ) -> Result<HandoffResult> {
        let (mut snapshot_lsn_start, stream_lsn_start) = {
            let state = self.state.lock().await;
            let snapshot_lsn_start = state.snapshot_lsn_start.ok_or_else(|| {
                Error::StateError(
                    "sqlserver perform_handoff requires start_snapshot to have been called first"
                        .into(),
                )
            })?;
            let stream_lsn_start = state.stream_lsn_start.ok_or_else(|| {
                Error::StateError(
                    "sqlserver perform_handoff requires start_stream to have been called first"
                        .into(),
                )
            })?;
            (snapshot_lsn_start, stream_lsn_start)
        };

        if snapshot_lsn_start == [0_u8; 10] {
            snapshot_lsn_start = stream_lsn_start;
        }

        let handoff = SqlServerHandoff {
            snapshot_lsn_start,
            stream_lsn_start,
        };

        if !handoff.has_no_gap() {
            return Err(Error::StateError(format!(
				"sqlserver handoff detected a gap: stream start LSN {} is after snapshot start LSN {}",
				lsn_bytes_to_hex(&handoff.stream_lsn_start),
				lsn_bytes_to_hex(&handoff.snapshot_lsn_start)
			)));
        }

        let snapshot_end = snapshot.finish().await?.snapshot_end_ts;
        let mut overlap_events_dropped = 0_u64;
        let mut reached_post_snapshot_lsn = false;

        for _ in 0..256 {
            let batch = stream.next_events(25).await?;
            if batch.is_empty() {
                break;
            }

            let mut forward = Vec::with_capacity(batch.len());
            for event in batch {
                match lsn_from_source_offset(&event.source.offset) {
                    Some(lsn) if compare_lsn(&lsn, &handoff.snapshot_lsn_start).is_le() => {
                        overlap_events_dropped = overlap_events_dropped.saturating_add(1);
                    }
                    Some(_) | None => {
                        reached_post_snapshot_lsn = true;
                        forward.push(event);
                    }
                }
            }

            if !forward.is_empty() {
                let (deduped, duplicates) = dedup_overlap_events_by_pk(forward);
                overlap_events_dropped = overlap_events_dropped.saturating_add(duplicates);
                stream.requeue_events(deduped).await?;
                break;
            }
        }

        if !reached_post_snapshot_lsn {
            stream.requeue_events(Vec::new()).await?;
        }

        stream.confirm_lsn(0).await?;

        Ok(HandoffResult {
            snapshot_end_ts: Some(snapshot_end),
            stream_start_ts: Some(now_millis()),
            overlap_events_dropped,
            stream_watermark_gap: None,
        })
    }

    fn source_type(&self) -> &str {
        SqlServerSourceConfig::source_type()
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
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use crate::checkpoint::{Checkpoint, InMemoryCheckpoint};
    use crate::{SecretProvider, SecretString};

    use super::*;

    type MockSnapshotRow = (String, serde_json::Value);
    type MockSnapshotPages = HashMap<String, VecDeque<Vec<MockSnapshotRow>>>;

    struct MockProbe {
        snapshot: Option<SqlServerPrereqSnapshot>,
        error_message: Option<String>,
        heartbeat_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SqlServerPrereqProbe for MockProbe {
        async fn probe(&self, _config: &SqlServerSourceConfig) -> Result<SqlServerPrereqSnapshot> {
            if let Some(message) = &self.error_message {
                return Err(Error::SourceError(message.clone()));
            }
            self.snapshot.clone().ok_or_else(|| {
                Error::SourceError("mock probe missing prerequisite snapshot".into())
            })
        }

        async fn heartbeat(&self, _config: &SqlServerSourceConfig) -> Result<()> {
            self.heartbeat_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[derive(Default)]
    struct MockSnapshotRowFetcher {
        pages: std::sync::Mutex<MockSnapshotPages>,
    }

    impl MockSnapshotRowFetcher {
        fn with_table_pages(table: &str, pages: Vec<Vec<MockSnapshotRow>>) -> Self {
            let mut all = HashMap::new();
            all.insert(table.to_string(), pages.into_iter().collect());
            Self {
                pages: std::sync::Mutex::new(all),
            }
        }
    }

    #[async_trait]
    impl SqlServerSnapshotRowFetcher for MockSnapshotRowFetcher {
        async fn fetch_keyset_rows(
            &self,
            table: &TableSnapshotState,
            _cursor: Option<&str>,
            limit: usize,
        ) -> Result<Vec<MockSnapshotRow>> {
            let mut lock = self
                .pages
                .lock()
                .map_err(|_| Error::StateError("mock snapshot fetcher mutex poisoned".into()))?;
            let queue = lock
                .get_mut(&table.snapshot.table)
                .ok_or_else(|| Error::StateError("mock snapshot fetcher table not found".into()))?;
            let mut next = queue.pop_front().unwrap_or_default();
            if next.len() > limit {
                let remainder = next.split_off(limit);
                queue.push_front(remainder);
            }
            Ok(next)
        }
    }

    fn config() -> SqlServerSourceConfig {
        SqlServerSourceConfig {
            host: "localhost".into(),
            port: 1433,
            user: "sa".into(),
            password: "StrongPass!123".into(),
            database: "master".into(),
            instance_name: None,
            #[cfg(feature = "tls")]
            transport: TransportConfig::tls(),
            #[cfg(not(feature = "tls"))]
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            cdc_enabled: true,
            cdc_schema: "cdc".into(),
            prereq_pool_size: DEFAULT_POOL_SIZE,
            stream_poll_interval_ms: DEFAULT_STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
        }
    }

    #[test]
    fn config_validation_rejects_missing_values() {
        let mut cfg = config();
        cfg.host = String::new();
        assert!(cfg.validate().is_err());

        cfg = config();
        cfg.user = String::new();
        assert!(cfg.validate().is_err());

        cfg = config();
        cfg.password = SecretString::default();
        assert!(cfg.validate().is_err());

        cfg = config();
        cfg.cdc_schema = String::new();
        assert!(cfg.validate().is_err());

        cfg = config();
        cfg.prereq_pool_size = 0;
        assert!(cfg.validate().is_err());

        cfg = config();
        cfg.stream_poll_interval_ms = 0;
        assert!(cfg.validate().is_err());

        cfg = config();
        cfg.max_events_per_poll = 0;
        assert!(cfg.validate().is_err());

        cfg = config();
        cfg.conn_timeout_secs = 301;
        assert!(cfg.validate().is_err());

        cfg = config();
        cfg.prereq_pool_size = 65;
        assert!(cfg.validate().is_err());

        cfg = config();
        cfg.stream_poll_interval_ms = 60_001;
        assert!(cfg.validate().is_err());

        cfg = config();
        cfg.max_events_per_poll = 100_001;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn default_config_prefers_tls_when_available() {
        let cfg = SqlServerSourceConfig::default();
        #[cfg(feature = "tls")]
        assert!(cfg.transport.is_tls());
        #[cfg(not(feature = "tls"))]
        assert!(!cfg.transport.is_tls());
    }

    #[test]
    fn debug_redacts_password() {
        let cfg = config();
        let debug = format!("{cfg:?}");
        assert!(debug.contains("***redacted***"));
        assert!(!debug.contains("StrongPass!123"));
    }

    #[test]
    fn validation_accepts_provider_backed_passwords() {
        struct TestProvider;

        impl SecretProvider for TestProvider {
            fn resolve_secret(&self, reference: &str) -> Result<String> {
                Ok(format!("resolved-{reference}"))
            }
        }

        let mut cfg = config();
        cfg.password = SecretString::from_provider(
            "test-provider",
            "sqlserver/password",
            Arc::new(TestProvider),
        );

        assert!(cfg.validate().is_ok());
        assert!(cfg.to_tiberius_config().is_ok());
    }

    #[tokio::test]
    async fn source_capabilities_are_reported() {
        let connection = SqlServerConnection::with_probe(
            config(),
            Arc::new(MockProbe {
                snapshot: Some(SqlServerPrereqSnapshot {
                    cdc_enabled: true,
                    has_cdc_admin_role: true,
                    major_version: 16,
                }),
                error_message: None,
                heartbeat_calls: Arc::new(AtomicUsize::new(0)),
            }),
        );

        assert_eq!(connection.source_type(), "sqlserver");
        let capabilities = connection.capabilities();
        assert!(capabilities.snapshot);
        assert!(capabilities.handoff);
        assert!(capabilities.heartbeat);
        assert!(capabilities.ddl_capture);
    }

    #[tokio::test]
    async fn connect_succeeds_when_prerequisites_pass() {
        let probe = Arc::new(MockProbe {
            snapshot: Some(SqlServerPrereqSnapshot {
                cdc_enabled: true,
                has_cdc_admin_role: true,
                major_version: 16,
            }),
            error_message: None,
            heartbeat_calls: Arc::new(AtomicUsize::new(0)),
        });
        let connection = SqlServerConnection::with_probe(config(), probe);
        connection.connect().await.unwrap();
        assert!(connection.is_connected().await);
        connection.close().await;
        assert!(!connection.is_connected().await);
    }

    #[tokio::test]
    async fn connect_fails_when_authentication_fails() {
        let probe = Arc::new(MockProbe {
            snapshot: None,
            error_message: Some("authentication failed".into()),
            heartbeat_calls: Arc::new(AtomicUsize::new(0)),
        });
        let connection = SqlServerConnection::with_probe(config(), probe);
        let error = connection.connect().await.unwrap_err();
        assert!(matches!(error, Error::SourceError(_)));
    }

    #[tokio::test]
    async fn connect_fails_when_cdc_is_disabled() {
        let probe = Arc::new(MockProbe {
            snapshot: Some(SqlServerPrereqSnapshot {
                cdc_enabled: false,
                has_cdc_admin_role: true,
                major_version: 16,
            }),
            error_message: None,
            heartbeat_calls: Arc::new(AtomicUsize::new(0)),
        });
        let connection = SqlServerConnection::with_probe(config(), probe);
        let error = connection.connect().await.unwrap_err();
        assert!(matches!(error, Error::SourceError(_)));
    }

    #[tokio::test]
    async fn connect_fails_when_role_is_missing() {
        let probe = Arc::new(MockProbe {
            snapshot: Some(SqlServerPrereqSnapshot {
                cdc_enabled: true,
                has_cdc_admin_role: false,
                major_version: 16,
            }),
            error_message: None,
            heartbeat_calls: Arc::new(AtomicUsize::new(0)),
        });
        let connection = SqlServerConnection::with_probe(config(), probe);
        let error = connection.connect().await.unwrap_err();
        assert!(matches!(error, Error::SourceError(_)));
    }

    #[tokio::test]
    async fn connect_fails_for_unsupported_version() {
        let probe = Arc::new(MockProbe {
            snapshot: Some(SqlServerPrereqSnapshot {
                cdc_enabled: true,
                has_cdc_admin_role: true,
                major_version: 12,
            }),
            error_message: None,
            heartbeat_calls: Arc::new(AtomicUsize::new(0)),
        });
        let connection = SqlServerConnection::with_probe(config(), probe);
        let error = connection.connect().await.unwrap_err();
        assert!(matches!(error, Error::SourceError(_)));
    }

    #[test]
    fn lsn_hex_round_trip() {
        let value = "0x000000230000015A0004";
        let bytes = lsn_hex_to_bytes(value).unwrap();
        assert_eq!(lsn_bytes_to_hex(&bytes), value);
    }

    #[test]
    fn operation_mapping_produces_expected_events() {
        let handle = SqlServerStreamHandle {
            config: config(),
            stream: SqlServerStream {
                lsn_start: [0; 10],
                lsn_end: [0; 10],
                change_tables: vec!["dbo_users".into()],
                poll_interval_ms: 5000,
            },
            metas: vec![],
            events_polled: 0,
            requeued_events: Vec::new(),
            max_events_per_poll: MAX_EVENTS_PER_POLL,
        };

        let meta = CaptureInstanceMeta {
            capture_instance: "dbo_users".into(),
            schema: "dbo".into(),
            table: "users".into(),
            primary_key: vec!["id".into()],
            captured_columns: vec!["id".into(), "name".into()],
        };

        let changes = vec![
            SqlServerRawChange {
                start_lsn_hex: "0x000000230000015A0004".into(),
                seqval_hex: "0x000000230000015A0005".into(),
                operation: 2,
                ts_ms: 1,
                row: serde_json::json!({"id": "1", "name": "alice"}),
            },
            SqlServerRawChange {
                start_lsn_hex: "0x000000230000015A0006".into(),
                seqval_hex: "0x000000230000015A0007".into(),
                operation: 4,
                ts_ms: 2,
                row: serde_json::json!({"id": "1", "name": "alice-v2"}),
            },
            SqlServerRawChange {
                start_lsn_hex: "0x000000230000015A0008".into(),
                seqval_hex: "0x000000230000015A0009".into(),
                operation: 1,
                ts_ms: 3,
                row: serde_json::json!({"id": "1", "name": "alice-v2"}),
            },
        ];

        let events = handle.map_changes_to_events(&meta, changes).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].op, Operation::Insert);
        assert_eq!(events[1].op, Operation::Update);
        assert_eq!(events[2].op, Operation::Delete);
        assert!(events[0].after.is_some());
        assert!(events[2].before.is_some());
        assert!(events[0].transaction.as_ref().unwrap().tx_id > 0);
    }

    #[test]
    fn metadata_refresh_emits_schema_change_events() {
        let mut handle = SqlServerStreamHandle {
            config: config(),
            stream: SqlServerStream {
                lsn_start: [0; 10],
                lsn_end: [1; 10],
                change_tables: vec!["dbo_users".into()],
                poll_interval_ms: 5000,
            },
            metas: vec![CaptureInstanceMeta {
                capture_instance: "dbo_users".into(),
                schema: "dbo".into(),
                table: "users".into(),
                primary_key: vec!["id".into()],
                captured_columns: vec!["id".into(), "name".into()],
            }],
            events_polled: 0,
            requeued_events: Vec::new(),
            max_events_per_poll: MAX_EVENTS_PER_POLL,
        };

        let refreshed = vec![
            CaptureInstanceMeta {
                capture_instance: "dbo_users".into(),
                schema: "dbo".into(),
                table: "users".into(),
                primary_key: vec!["id".into()],
                captured_columns: vec!["id".into(), "name".into(), "email".into()],
            },
            CaptureInstanceMeta {
                capture_instance: "sales_orders".into(),
                schema: "sales".into(),
                table: "orders".into(),
                primary_key: vec!["order_id".into()],
                captured_columns: vec!["order_id".into(), "total".into()],
            },
        ];

        let events = handle.compute_schema_events_for_meta_refresh(&refreshed);
        assert_eq!(events.len(), 2);
        assert!(events
            .iter()
            .any(|event| event.op == Operation::SchemaChange));
        assert!(events.iter().any(|event| {
            event
                .after
                .as_ref()
                .and_then(|value| value.get("ddl_type"))
                .and_then(|value| value.as_str())
                == Some("ALTER_TABLE")
        }));
        assert!(events.iter().any(|event| {
            event
                .after
                .as_ref()
                .and_then(|value| value.get("ddl_type"))
                .and_then(|value| value.as_str())
                == Some("CREATE_TABLE")
        }));

        handle.metas = refreshed;
        let second = handle.compute_schema_events_for_meta_refresh(&handle.metas);
        assert!(second.is_empty());
    }

    #[test]
    fn metadata_refresh_emits_drop_event_for_removed_capture_instance() {
        let handle = SqlServerStreamHandle {
            config: config(),
            stream: SqlServerStream {
                lsn_start: [0; 10],
                lsn_end: [2; 10],
                change_tables: vec!["dbo_users".into()],
                poll_interval_ms: 5000,
            },
            metas: vec![CaptureInstanceMeta {
                capture_instance: "dbo_users".into(),
                schema: "dbo".into(),
                table: "users".into(),
                primary_key: vec!["id".into()],
                captured_columns: vec!["id".into(), "name".into()],
            }],
            events_polled: 0,
            requeued_events: Vec::new(),
            max_events_per_poll: MAX_EVENTS_PER_POLL,
        };

        let events = handle.compute_schema_events_for_meta_refresh(&[]);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op, Operation::SchemaChange);
        let ddl_type = events[0]
            .after
            .as_ref()
            .and_then(|value| value.get("ddl_type"))
            .and_then(|value| value.as_str());
        assert_eq!(ddl_type, Some("DROP_TABLE"));
    }

    #[test]
    fn resume_lsn_older_than_minimum_is_rejected() {
        let min = lsn_hex_to_bytes("0x000000230000015A0008").unwrap();
        let resume = lsn_hex_to_bytes("0x000000230000015A0004").unwrap();
        assert!(compare_lsn(&resume, &min).is_lt());
    }

    #[test]
    fn parse_schema_table_defaults_schema_and_validates_identifiers() {
        let (schema, table) = parse_schema_table("users").unwrap();
        assert_eq!(schema, "dbo");
        assert_eq!(table, "users");

        let (schema, table) = parse_schema_table("sales.orders").unwrap();
        assert_eq!(schema, "sales");
        assert_eq!(table, "orders");

        let (schema, table) = parse_schema_table("[sales-team].[orders.v2]").unwrap();
        assert_eq!(schema, "sales-team");
        assert_eq!(table, "orders.v2");

        assert!(parse_schema_table("sales.order-items").is_err());
        assert!(parse_schema_table("dbo.users;DROP TABLE audit").is_err());
        assert!(parse_schema_table("dbo.users --comment").is_err());
        assert!(parse_schema_table("[dbo].[users").is_err());
    }

    #[test]
    fn snapshot_fetch_sql_builder_includes_seek_clause_when_cursor_present() {
        let sql = build_snapshot_fetch_sql(
            "[dbo].[users]",
            &["id".to_string(), "tenant_id".to_string()],
            &[
                "id".to_string(),
                "tenant_id".to_string(),
                "name".to_string(),
            ],
            3,
            true,
        );

        assert!(sql.contains("SELECT TOP (@P3)"));
        assert!(sql.contains("WHERE (t.[id] > @P1) OR (t.[id] = @P1 AND t.[tenant_id] > @P2)"));
        assert!(sql.contains("ORDER BY [id], [tenant_id]"));
    }

    #[test]
    fn cdc_poll_sql_builder_quotes_columns_and_orders_consistently() {
        let sql = build_cdc_poll_sql(
            "dbo_users",
            &["id".to_string(), "name".to_string()],
            128,
            "0x01",
            "0x02",
        );

        assert!(sql.contains("SELECT TOP (128)"));
        assert!(sql.contains("[id], [name]"));
        assert!(sql.contains("fn_cdc_get_all_changes_dbo_users"));
        assert!(sql.contains("ORDER BY __$start_lsn, __$seqval, __$operation"));
    }

    #[test]
    fn sqlserver_json_value_to_param_handles_scalars() {
        assert!(matches!(
            sqlserver_json_value_to_param(&serde_json::json!(true)).unwrap(),
            SqlServerCursorParam::Bool(true)
        ));
        assert!(matches!(
            sqlserver_json_value_to_param(&serde_json::json!(42)).unwrap(),
            SqlServerCursorParam::Int(42)
        ));
        assert!(matches!(
            sqlserver_json_value_to_param(&serde_json::json!("O'Hara")).unwrap(),
            SqlServerCursorParam::Text(value) if value == "O'Hara"
        ));
        assert!(sqlserver_json_value_to_param(&serde_json::json!({"id": 1})).is_err());
    }

    #[tokio::test]
    async fn snapshot_checkpoint_can_resume_handle_state() {
        let snapshot = SqlServerSnapshot {
            lsn_start: [1; 10],
            snapshot_id: "snap-1".into(),
            tables: vec![],
        };
        let table_state = TableSnapshotState {
            snapshot: TableSnapshot {
                table: "dbo.users".into(),
                total_rows: 10,
                rows_processed: 5,
                cursor_position: Some("[5]".into()),
                is_complete: false,
            },
            schema: "dbo".into(),
            table: "users".into(),
            primary_key_columns: vec!["id".into()],
            column_names: vec!["id".into(), "name".into()],
        };

        let mut handle = SqlServerSnapshotHandle::new(snapshot, vec![table_state], None, false);
        handle.sync_snapshot_tables();
        handle.current_table = 0;
        handle.next_chunk_index = 3;
        handle.emitted_rows = 5;

        let mut checkpoint = InMemoryCheckpoint::default();
        handle.checkpoint(&mut checkpoint, 11).await.unwrap();
        let payload = checkpoint.load().await.unwrap().unwrap().encode().unwrap();

        let resumed = SqlServerSnapshotHandle::new(
            SqlServerSnapshot {
                lsn_start: [0; 10],
                snapshot_id: "new".into(),
                tables: vec![],
            },
            vec![TableSnapshotState {
                snapshot: TableSnapshot {
                    table: "dbo.users".into(),
                    total_rows: 10,
                    rows_processed: 0,
                    cursor_position: None,
                    is_complete: false,
                },
                schema: "dbo".into(),
                table: "users".into(),
                primary_key_columns: vec!["id".into()],
                column_names: vec!["id".into(), "name".into()],
            }],
            None,
            false,
        )
        .resume_from_checkpoint_payload(&payload)
        .unwrap();

        assert_eq!(resumed.snapshot.snapshot_id, "snap-1");
        assert_eq!(resumed.snapshot.lsn_start, [1; 10]);
        assert_eq!(resumed.next_chunk_index, 3);
        assert_eq!(resumed.tables[0].snapshot.rows_processed, 5);
        assert_eq!(
            resumed.tables[0].snapshot.cursor_position.as_deref(),
            Some("[5]")
        );
    }

    #[tokio::test]
    async fn snapshot_large_table_is_chunked_in_order() {
        let snapshot = SqlServerSnapshot {
            lsn_start: [2; 10],
            snapshot_id: "snap-large".into(),
            tables: vec![],
        };
        let table_state = TableSnapshotState {
            snapshot: TableSnapshot {
                table: "dbo.users".into(),
                total_rows: 5,
                rows_processed: 0,
                cursor_position: None,
                is_complete: false,
            },
            schema: "dbo".into(),
            table: "users".into(),
            primary_key_columns: vec!["id".into()],
            column_names: vec!["id".into(), "name".into()],
        };

        let fetcher = Arc::new(MockSnapshotRowFetcher::with_table_pages(
            "dbo.users",
            vec![
                vec![
                    ("[1]".into(), serde_json::json!({"id": 1, "name": "u1"})),
                    ("[2]".into(), serde_json::json!({"id": 2, "name": "u2"})),
                ],
                vec![
                    ("[3]".into(), serde_json::json!({"id": 3, "name": "u3"})),
                    ("[4]".into(), serde_json::json!({"id": 4, "name": "u4"})),
                ],
                vec![("[5]".into(), serde_json::json!({"id": 5, "name": "u5"}))],
            ],
        ));

        let mut handle =
            SqlServerSnapshotHandle::new_with_fetcher(snapshot, vec![table_state], fetcher);

        let c1 = handle.next_chunk(2).await.unwrap();
        let c2 = handle.next_chunk(2).await.unwrap();
        let c3 = handle.next_chunk(2).await.unwrap();
        let c4 = handle.next_chunk(2).await.unwrap();

        assert_eq!(c1.len(), 2);
        assert_eq!(c2.len(), 2);
        assert_eq!(c3.len(), 1);
        assert!(c4.is_empty());

        assert_eq!(
            c1[0].snapshot.as_ref().map(|snapshot| snapshot.chunk_index),
            Some(0)
        );
        assert_eq!(
            c2[0].snapshot.as_ref().map(|snapshot| snapshot.chunk_index),
            Some(1)
        );
        assert_eq!(
            c3[0].snapshot.as_ref().map(|snapshot| snapshot.chunk_index),
            Some(2)
        );
        assert_eq!(
            c3[0]
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.is_last_chunk),
            Some(true)
        );
    }

    #[tokio::test]
    async fn snapshot_interrupt_resume_has_no_duplicate_rows() {
        let initial_snapshot = SqlServerSnapshot {
            lsn_start: [3; 10],
            snapshot_id: "snap-resume".into(),
            tables: vec![],
        };
        let table_state = TableSnapshotState {
            snapshot: TableSnapshot {
                table: "dbo.users".into(),
                total_rows: 5,
                rows_processed: 0,
                cursor_position: None,
                is_complete: false,
            },
            schema: "dbo".into(),
            table: "users".into(),
            primary_key_columns: vec!["id".into()],
            column_names: vec!["id".into(), "name".into()],
        };

        let first_fetcher = Arc::new(MockSnapshotRowFetcher::with_table_pages(
            "dbo.users",
            vec![vec![
                ("[1]".into(), serde_json::json!({"id": 1, "name": "u1"})),
                ("[2]".into(), serde_json::json!({"id": 2, "name": "u2"})),
            ]],
        ));

        let mut first = SqlServerSnapshotHandle::new_with_fetcher(
            initial_snapshot,
            vec![table_state.clone()],
            first_fetcher,
        );
        let first_chunk = first.next_chunk(2).await.unwrap();
        assert_eq!(first_chunk.len(), 2);

        let mut checkpoint = InMemoryCheckpoint::default();
        first.checkpoint(&mut checkpoint, 13).await.unwrap();
        let payload = checkpoint.load().await.unwrap().unwrap().encode().unwrap();

        let second_fetcher = Arc::new(MockSnapshotRowFetcher::with_table_pages(
            "dbo.users",
            vec![
                vec![
                    ("[3]".into(), serde_json::json!({"id": 3, "name": "u3"})),
                    ("[4]".into(), serde_json::json!({"id": 4, "name": "u4"})),
                ],
                vec![("[5]".into(), serde_json::json!({"id": 5, "name": "u5"}))],
            ],
        ));

        let mut resumed = SqlServerSnapshotHandle::new_with_fetcher(
            SqlServerSnapshot {
                lsn_start: [0; 10],
                snapshot_id: "new".into(),
                tables: vec![],
            },
            vec![table_state],
            second_fetcher,
        )
        .resume_from_checkpoint_payload(&payload)
        .unwrap();

        let mut resumed_events = Vec::new();
        loop {
            let batch = resumed.next_chunk(2).await.unwrap();
            if batch.is_empty() {
                break;
            }
            resumed_events.extend(batch);
        }

        let mut ids = Vec::new();
        for event in first_chunk.into_iter().chain(resumed_events.into_iter()) {
            let id = event
                .after
                .as_ref()
                .and_then(|row| row.get("id"))
                .and_then(|value| value.as_i64())
                .unwrap();
            ids.push(id);
        }

        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
        let unique = ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), 5);
    }

    #[test]
    fn handoff_no_gap_validation() {
        let handoff = SqlServerHandoff {
            snapshot_lsn_start: lsn_hex_to_bytes("0x000000230000015A0008").unwrap(),
            stream_lsn_start: lsn_hex_to_bytes("0x000000230000015A0008").unwrap(),
        };
        assert!(handoff.has_no_gap());

        let gap = SqlServerHandoff {
            snapshot_lsn_start: lsn_hex_to_bytes("0x000000230000015A0008").unwrap(),
            stream_lsn_start: lsn_hex_to_bytes("0x000000230000015A0010").unwrap(),
        };
        assert!(!gap.has_no_gap());
    }

    #[test]
    fn dedup_overlap_events_by_pk_keeps_last_event_per_pk() {
        let base = Event {
            before: None,
            after: Some(serde_json::json!({"id": 1, "v": 1})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "sqlserver".into(),
                offset: "0x000000230000015A0001".into(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("dbo".into()),
            table: "users".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        };

        let mut updated = base.clone();
        updated.op = Operation::Update;
        updated.before = Some(serde_json::json!({"id": 1, "v": 1}));
        updated.after = Some(serde_json::json!({"id": 1, "v": 2}));
        updated.source.offset = "0x000000230000015A0002".into();

        let mut second_pk = base.clone();
        second_pk.after = Some(serde_json::json!({"id": 2, "v": 1}));
        second_pk.source.offset = "0x000000230000015A0003".into();

        let (deduped, duplicates) =
            dedup_overlap_events_by_pk(vec![base, updated.clone(), second_pk]);
        assert_eq!(duplicates, 1);
        assert_eq!(deduped.len(), 2);
        assert!(deduped.iter().any(|event| {
            event
                .after
                .as_ref()
                .and_then(|row| row.get("id"))
                .and_then(|value| value.as_i64())
                == Some(1)
                && event.op == Operation::Update
        }));
    }
}
