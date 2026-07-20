//! SQL Server source configuration and connection lifecycle.

use std::{sync::Arc, time::Duration};

use ahash::AHashMap;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, sync::Mutex};

#[cfg(test)]
use crate::core::{Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
use crate::source::helpers::now_millis;
use crate::{
    checkpoint::GenericOffset,
    core::Event,
    core::{Error, Offset, Result, SecretString, StructuredLogger, TransportConfig},
    source::{
        ConnectorCapabilities, HandoffResult, IncrementalSnapshotConfig, SnapshotHandle, Source,
        StreamHandle,
    },
};

mod config;
mod connection_lifecycle;
pub mod incremental_snapshot;
mod parser;
mod prereq;
mod query;
mod snapshot_chunk;
mod snapshot_fetch;
mod snapshot_finalize;
mod snapshot_start;
mod state;
mod stream_schema;
mod stream_start;
mod stream_window;

use self::connection_lifecycle::connect_sqlserver_with_probe;
use self::parser::SqlServerCdcCursor;
use self::snapshot_chunk::next_sqlserver_snapshot_chunk;
use self::snapshot_finalize::{checkpoint_sqlserver_snapshot, finish_sqlserver_snapshot};
use self::snapshot_start::{
    start_sqlserver_snapshot_from_checkpoint, start_sqlserver_snapshot_internal,
};
use self::stream_start::start_sqlserver_stream;

use self::prereq::{LiveSqlServerPrereqProbe, SqlServerPrereqProbe, SqlServerPrereqSnapshot};
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
    ///
    /// # Latency characteristic
    ///
    /// SQL Server CDC uses **polling** against the `cdc.fn_cdc_get_all_changes_*`
    /// table-valued functions rather than a push-based protocol like PostgreSQL
    /// logical replication.  Event-to-consumer latency is therefore bounded by
    /// this interval: a committed transaction will not be visible to rustcdc until
    /// the next poll cycle.
    ///
    /// - **p50 latency** is typically a fraction of `stream_poll_interval_ms`
    ///   (events committed just before a poll boundary).
    /// - **p99 latency** approaches the full `stream_poll_interval_ms` (events
    ///   committed just after a poll boundary) plus CDC capture agent propagation
    ///   delay (usually < 5 seconds on an unloaded server).
    ///
    /// Lower values reduce tail latency at the cost of higher poll frequency and
    /// SQL Server query load.  The default (5 000 ms) is a reasonable production
    /// starting point; latency-sensitive workloads may set this to 500–1 000 ms.
    pub stream_poll_interval_ms: u64,
    /// Maximum events yielded by a single stream poll cycle.
    pub max_events_per_poll: usize,
    /// Allowlist of tables to stream, in `"schema.table"` format.
    ///
    /// When non-empty, only tables in this list are forwarded to the caller.
    /// Takes precedence over [`table_exclude_list`](SqlServerSourceConfig::table_exclude_list).
    /// An empty list means *all* tables are included.
    pub table_include_list: Vec<String>,
    /// Blocklist of tables to suppress, in `"schema.table"` format.
    ///
    /// Ignored when [`table_include_list`](SqlServerSourceConfig::table_include_list) is non-empty.
    /// An empty list means no tables are excluded.
    pub table_exclude_list: Vec<String>,
    /// Capture `TRUNCATE TABLE` operations via a database-level DDL trigger.
    ///
    /// ## How it works
    ///
    /// SQL Server CDC (`cdc.fn_cdc_get_all_changes_*`) cannot capture `TRUNCATE
    /// TABLE` because TRUNCATE bypasses row-level logging.  When this option is
    /// `true`, rustcdc creates a shadow table (`[<cdc_schema>].[rustcdc_truncate_events]`)
    /// and a database-level DDL trigger (`rustcdc_truncate_capture`) on first connect.
    /// The trigger fires synchronously during each `TRUNCATE TABLE` statement and
    /// records the affected schema and table, together with the current CDC maximum
    /// LSN captured via `sys.fn_cdc_get_max_lsn()`.  rustcdc polls the shadow table
    /// alongside the normal CDC change tables and emits [`crate::Operation::Truncate`] events
    /// positioned after all DML changes at or before the captured LSN.
    ///
    /// ## Setup requirements
    ///
    /// The connected user must hold `db_owner`, `db_ddladmin`, or `sysadmin` to
    /// create the shadow table and DDL trigger (already required for CDC admin
    /// operations).  The objects are created idempotently and survive restarts.
    ///
    /// ## Ordering guarantee
    ///
    /// The ordering is *best-effort*: the truncate event is placed after all DML
    /// changes whose commit LSN ≤ the LSN captured at DDL trigger execution time.
    /// This is as precise as SQL Server allows for DDL operations that bypass the
    /// transaction log at the row level.
    pub capture_truncate_events: bool,
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
    /// Resume point *within* the current window, set when a poll could not read the
    /// whole window because `max_events_per_poll` truncated the result set.
    ///
    /// While this is `Some`, the window is re-queried from the cursor rather than
    /// advanced — advancing would skip the unread remainder permanently.
    pub(crate) cursor: Option<parser::SqlServerCdcCursor>,
}

/// Total order over a CDC row position, matching the server-side
/// `ORDER BY __$start_lsn, __$seqval, __$operation`.
fn cursor_ordering(left: &SqlServerCdcCursor, right: &SqlServerCdcCursor) -> std::cmp::Ordering {
    match (
        parser::lsn_hex_to_bytes_opt(&left.lsn_hex),
        parser::lsn_hex_to_bytes_opt(&right.lsn_hex),
    ) {
        (Some(a), Some(b)) => compare_lsn(&a, &b),
        _ => left.lsn_hex.cmp(&right.lsn_hex),
    }
    .then_with(|| left.seqval_hex.cmp(&right.seqval_hex))
    .then_with(|| left.operation.cmp(&right.operation))
}

#[derive(Debug, Clone)]
struct SqlServerRawChange {
    start_lsn_hex: String,
    seqval_hex: String,
    operation: i32,
    ts_ms: u64,
    row: serde_json::Value,
}

/// A TRUNCATE TABLE event captured by the `rustcdc_truncate_capture` DDL trigger.
#[derive(Debug, Clone)]
struct SqlServerRawTruncate {
    /// Row ID in the shadow table (used for marking consumed).
    id: i64,
    schema_name: String,
    table_name: String,
    /// LSN hex captured inside the DDL trigger via `sys.fn_cdc_get_max_lsn()`.
    /// TRUNCATE events sort after all DML changes at the same LSN.
    lsn_hex: String,
    ts_ms: u64,
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
    /// Buffered UPDATE after-images (op=3) awaiting their op=4 before-image partner.
    ///
    /// SQL Server CDC with `'all update old'` emits op=3 (after) before op=4 (before)
    /// for the same `(start_lsn, seqval)` key.  We buffer the after-image until the
    /// matching before-image arrives, then merge them into a single `Event` with both
    /// `before` and `after` populated.  This buffer persists across poll boundaries so
    /// a pair split by `max_events_per_poll` is handled correctly.
    pending_update_befores: AHashMap<(String, String), (serde_json::Value, u64)>,
    /// Events collected from all capture instances in the current LSN window, sorted by
    /// LSN and waiting to be delivered in pages.
    ///
    /// SQL Server CDC requires polling each capture instance (table) independently.
    /// Without a merge-sort step the relative order of events across tables is
    /// determined by the poll loop iteration order — not by the commit LSN.
    ///
    /// This buffer fills once per LSN window (all capture instances are queried before
    /// any events are returned), then is drained in `max_events_per_poll`-sized pages.
    /// The LSN window is **not** advanced until the buffer is fully drained, preserving
    /// at-least-once restart safety: `save_position` always records the window-start LSN
    /// of the events that have been delivered, so a crash mid-buffer causes at most one
    /// window of duplicate delivery (handled by the idempotency guard).
    window_buffer: Vec<Event>,
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

/// Returns a monotonic u64 proxy for LSN ordering / distance calculations.
///
/// SQL Server LSN layout: `[vlfSeqNo:4][blockOffset:4][recordNo:2]`.
/// Taking the first 8 bytes as a big-endian u64 gives a value that increases
/// monotonically as the log advances and can be used for gap estimation.
fn lsn_bytes_to_u64_distance(lsn: &[u8; 10]) -> u64 {
    u64::from_be_bytes(lsn[..8].try_into().expect("slice is exactly 8 bytes"))
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
    cursor: Option<&SqlServerCdcCursor>,
) -> String {
    parser::build_cdc_poll_sql(
        capture_instance,
        columns,
        max_events_per_poll,
        start_lsn_hex,
        end_lsn_hex,
        cursor,
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

impl SqlServerStreamHandle {}

#[async_trait]
impl StreamHandle for SqlServerStreamHandle {
    async fn next_events(&mut self, timeout_ms: u64) -> Result<Vec<Event>> {
        // ── Priority 1: flush snapshot-handoff requeued events ─────────────────
        if !self.requeued_events.is_empty() {
            return Ok(std::mem::take(&mut self.requeued_events));
        }

        // ── Priority 2: flush schema-change events ──────────────────────────────
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

        // ── Priority 3: drain the window buffer if it already has events ────────
        //
        // We advance the LSN window only after the buffer is fully drained so
        // that `save_position` always records a window-start LSN that is ≤ all
        // events delivered since the last checkpoint.  A crash mid-buffer causes
        // at most one window of duplicate delivery (at-least-once guarantee).
        if !self.window_buffer.is_empty() {
            let count = self.max_events_per_poll.min(self.window_buffer.len());
            let batch: Vec<Event> = self.window_buffer.drain(..count).collect();
            // Advance the window once the buffer is empty so the next fill
            // queries a fresh LSN range.
            if self.window_buffer.is_empty() {
                self.advance_window().await?;
            }
            return Ok(batch);
        }

        // ── Priority 4: fill the window buffer ─────────────────────────────────
        //
        // Collect changes from *all* capture instances for the current LSN
        // window, sort them by commit LSN for global cross-table ordering, then
        // store in `window_buffer`.  When all events fit in a single page the
        // window is advanced immediately after the drain (see below).  For
        // multi-page windows the advance is deferred to priority 3 once the
        // buffer fully drains, keeping the at-least-once guarantee intact.
        let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);

        loop {
            // Clone meta list to avoid holding an immutable borrow while the
            // mutable `map_changes_to_events` methods run.
            let metas_snapshot = self.metas.clone();
            let mut all_changes: Vec<(CaptureInstanceMeta, Vec<SqlServerRawChange>)> = Vec::new();

            // Resume point for a window that could not be read in one poll.
            //
            // Each capture instance is queried with its own `TOP (max_events_per_poll)`,
            // so instances truncate at different positions. The only globally safe
            // resume point is the **minimum** last-row position across all instances
            // that returned a full page: past that point, some truncated instance has
            // rows we have not read. Rows beyond the cursor are dropped from this batch
            // and re-read next poll, so the cut costs neither a gap nor a duplicate.
            let mut truncation_cursor: Option<SqlServerCdcCursor> = None;

            for meta in &metas_snapshot {
                let changes = self
                    .fetch_changes_for_capture_instance(
                        &meta.capture_instance,
                        &meta.captured_columns,
                        self.max_events_per_poll,
                    )
                    .await?;

                // A full page means `TOP` cut the result set — unread rows remain in
                // this window. Advancing past them would lose them permanently, and
                // silently: `events_polled` would report a plausible count.
                if changes.len() >= self.max_events_per_poll {
                    if let Some(last) = changes.last() {
                        let candidate = SqlServerCdcCursor {
                            lsn_hex: last.start_lsn_hex.clone(),
                            seqval_hex: last.seqval_hex.clone(),
                            operation: last.operation,
                        };
                        truncation_cursor = Some(match truncation_cursor {
                            Some(existing) if cursor_ordering(&existing, &candidate).is_le() => {
                                existing
                            }
                            _ => candidate,
                        });
                    }
                }
                if !changes.is_empty() {
                    all_changes.push((meta.clone(), changes));
                }
            }

            if !all_changes.is_empty() {
                // Flatten all (meta, changes) pairs with their LSN for sorting.
                // Each SqlServerRawChange already carries `start_lsn_hex` and
                // `seqval_hex`; we use those to establish a total global order.
                let mut flat: Vec<(CaptureInstanceMeta, SqlServerRawChange)> = all_changes
                    .into_iter()
                    .flat_map(|(meta, changes)| changes.into_iter().map(move |c| (meta.clone(), c)))
                    .collect();

                flat.sort_by(|(_, a), (_, b)| {
                    // Parse hex LSN bytes for comparison.  Fall back to equal on
                    // parse error (keeps stable relative order within each table).
                    let ord = match (
                        parser::lsn_hex_to_bytes_opt(&a.start_lsn_hex),
                        parser::lsn_hex_to_bytes_opt(&b.start_lsn_hex),
                    ) {
                        (Some(la), Some(lb)) => compare_lsn(&la, &lb),
                        _ => std::cmp::Ordering::Equal,
                    };
                    ord.then_with(|| a.seqval_hex.cmp(&b.seqval_hex))
                        .then_with(|| a.operation.cmp(&b.operation))
                });

                // Drop everything past the truncation cursor. Those rows belong to a
                // later poll; keeping them would deliver rows that sort *after* unread
                // rows from a truncated instance, so a crash between the two would
                // leave a permanent hole.
                if let Some(cursor) = truncation_cursor.as_ref() {
                    flat.retain(|(_, change)| {
                        let position = SqlServerCdcCursor {
                            lsn_hex: change.start_lsn_hex.clone(),
                            seqval_hex: change.seqval_hex.clone(),
                            operation: change.operation,
                        };
                        cursor_ordering(&position, cursor).is_le()
                    });
                }

                // Map sorted raw changes to Events.  UPDATE op=3/op=4 pairs share
                // the same (start_lsn, seqval) key, so the after-image (op=3)
                // will always precede the before-image (op=4) in our sort order
                // (since 3 < 4), preserving the expected merge behaviour.
                for (meta, change) in flat {
                    let mut events = self.map_changes_to_events(&meta, vec![change])?;
                    self.window_buffer.append(&mut events);
                }

                // Merge in any truncate events that fall within the current LSN
                // window.  Truncate events are positioned after all DML changes
                // at the same LSN (TRUNCATE bypasses row-level logging, so the
                // captured LSN is a ceiling bound, not an exact match).
                let mut truncate_events = self.fetch_and_emit_truncate_events().await?;
                if !truncate_events.is_empty() {
                    // Insert each truncate event after all DML events at or
                    // before its captured LSN.
                    self.window_buffer.append(&mut truncate_events);
                    // Re-sort the combined buffer by offset (LSN hex string sort
                    // is byte-lexicographic, which matches numeric order for the
                    // 0x-prefixed fixed-width strings SQL Server produces).
                    self.window_buffer
                        .sort_by(|a, b| a.source.offset.cmp(&b.source.offset));
                }

                self.events_polled = self
                    .events_polled
                    .saturating_add(self.window_buffer.len() as u64);

                // Drain the first page and return.  If the buffer is now empty
                // (all events fit in a single page) advance the window here so
                // the next call queries a fresh LSN range instead of
                // re-delivering this window indefinitely.  Multi-page batches
                // defer the advance to priority 3 once the buffer fully drains,
                // keeping the at-least-once guarantee intact.
                let count = self.max_events_per_poll.min(self.window_buffer.len());
                let batch: Vec<Event> = self.window_buffer.drain(..count).collect();
                if self.window_buffer.is_empty() {
                    if let Some(cursor) = truncation_cursor {
                        // Unread rows remain inside this window. Record where we stopped
                        // and re-query the SAME window from there — do not advance past it.
                        tracing::debug!(
                            target: "rustcdc::source::sqlserver",
                            lsn = %cursor.lsn_hex,
                            seqval = %cursor.seqval_hex,
                            operation = cursor.operation,
                            "sqlserver CDC window truncated by max_events_per_poll; \
                             resuming mid-window at the recorded cursor",
                        );
                        self.stream.cursor = Some(cursor);
                    } else {
                        self.advance_window().await?;
                    }
                }
                return Ok(batch);
            }

            // No DML changes in this window — still check for truncate events
            // which may have arrived while the DML window was empty.
            let truncate_events = self.fetch_and_emit_truncate_events().await?;
            if !truncate_events.is_empty() {
                self.events_polled = self
                    .events_polled
                    .saturating_add(truncate_events.len() as u64);
                self.advance_window().await?;
                return Ok(truncate_events);
            }

            // No changes in this window — advance past it and wait.
            self.advance_window().await?;

            if std::time::Instant::now() >= deadline {
                return Ok(vec![]);
            }

            let sleep_for = self
                .stream
                .poll_interval_ms
                .min(timeout_ms.max(1))
                .min(DEFAULT_STREAM_POLL_INTERVAL_MS);
            tokio::time::sleep(Duration::from_millis(sleep_for)).await;
        }
    }

    async fn save_position(
        &self,
        checkpoint: &mut dyn crate::checkpoint::Checkpoint,
    ) -> Result<()> {
        // When a window was only partially read, the checkpoint must carry the
        // within-window cursor as well. Persisting only the LSN would make a
        // mid-window position unrepresentable, so a restart would either re-read the
        // whole window (duplicates) or, with the window advanced, skip the remainder.
        let encoded = match self.stream.cursor.as_ref() {
            Some(cursor) => cursor.encode(),
            None => lsn_bytes_to_hex(&self.stream.lsn_start),
        };
        let offset = GenericOffset::new(
            "sqlserver",
            serde_json::to_vec(&encoded)
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

    /// Start a non-blocking incremental snapshot using the DBLog watermark pattern.
    pub async fn start_incremental_snapshot(
        &mut self,
        config: IncrementalSnapshotConfig,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>> {
        use crate::source::sqlserver::incremental_snapshot::SqlServerIncrementalSnapshotHandle;
        self.ensure_connected().await?;
        let inner = self.start_stream(resume_from).await?;
        let source_name = self.source_type().to_string();
        let handle = SqlServerIncrementalSnapshotHandle::new(
            inner,
            self.config.clone(),
            config,
            source_name,
        )
        .await?;
        Ok(Box::new(handle))
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

        // Compute the LSN distance between the CDC maximum log position and the
        // stream start position as a proxy for capture-job backlog at handoff time.
        let stream_watermark_gap = match self.query_max_lsn_hex().await {
            Ok(max_lsn_hex) => lsn_hex_to_bytes(&max_lsn_hex).ok().map(|max_lsn| {
                lsn_bytes_to_u64_distance(&max_lsn)
                    .saturating_sub(lsn_bytes_to_u64_distance(&handoff.stream_lsn_start))
            }),
            Err(_) => None,
        };

        Ok(HandoffResult {
            snapshot_end_ts: Some(snapshot_end),
            stream_start_ts: Some(now_millis()),
            overlap_events_dropped: Some(overlap_events_dropped),
            stream_watermark_gap,
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
            truncate: self.config.capture_truncate_events,
            incremental_snapshot: true,
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
            ..Default::default()
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

    #[test]
    fn plaintext_transport_is_explicitly_supported() {
        let mut cfg = config();
        cfg.transport = TransportConfig::plaintext();

        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn transport_helper_methods_set_expected_mode() {
        let plaintext = SqlServerSourceConfig::default().with_plaintext_transport();
        assert!(!plaintext.transport.is_tls());

        let tls = plaintext.with_tls_transport();
        assert!(tls.transport.is_tls());
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
        // Parsing accepts either case; rendering normalizes to lowercase so it matches
        // `sys.fn_varbintohexstr`, which is what every LSN read back from the server
        // looks like.
        let bytes = lsn_hex_to_bytes("0x000000230000015A0004").unwrap();
        assert_eq!(lsn_bytes_to_hex(&bytes), "0x000000230000015a0004");
        assert_eq!(
            lsn_hex_to_bytes("0x000000230000015a0004").unwrap(),
            bytes,
            "lowercase must parse to the same bytes as uppercase"
        );
    }

    #[test]
    fn operation_mapping_produces_expected_events() {
        let mut handle = SqlServerStreamHandle {
            config: config(),
            stream: SqlServerStream {
                lsn_start: [0; 10],
                lsn_end: [0; 10],
                change_tables: vec!["dbo_users".into()],
                poll_interval_ms: 5000,
                cursor: None,
            },
            metas: vec![],
            events_polled: 0,
            requeued_events: Vec::new(),
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            pending_update_befores: AHashMap::new(),
            window_buffer: Vec::new(),
        };

        let meta = CaptureInstanceMeta {
            capture_instance: "dbo_users".into(),
            schema: "dbo".into(),
            table: "users".into(),
            primary_key: vec!["id".into()],
            captured_columns: vec!["id".into(), "name".into()],
        };

        // A realistic CDC window: INSERT, UPDATE (op=3 before-image then op=4 after-image), DELETE.
        //
        // Per `cdc.fn_cdc_get_all_changes_<capture_instance>`: op=3 carries the captured
        // column values BEFORE the update, op=4 carries the values AFTER the update.
        let changes = vec![
            // INSERT — op=2: full row is the after-image.
            SqlServerRawChange {
                start_lsn_hex: "0x000000230000015A0002".into(),
                seqval_hex: "0x000000230000015A0003".into(),
                operation: 2,
                ts_ms: 1,
                row: serde_json::json!({"id": "1", "name": "alice"}),
            },
            // UPDATE before-image (op=3) — OLD values, arrives first in ASC ORDER BY.
            SqlServerRawChange {
                start_lsn_hex: "0x000000230000015A0004".into(),
                seqval_hex: "0x000000230000015A0005".into(),
                operation: 3,
                ts_ms: 2,
                row: serde_json::json!({"id": "1", "name": "alice"}),
            },
            // UPDATE after-image (op=4) — NEW values, arrives second; same (lsn, seqval).
            SqlServerRawChange {
                start_lsn_hex: "0x000000230000015A0004".into(),
                seqval_hex: "0x000000230000015A0005".into(),
                operation: 4,
                ts_ms: 2,
                row: serde_json::json!({"id": "1", "name": "alice-v2"}),
            },
            // DELETE — op=1: full row is the before-image.
            SqlServerRawChange {
                start_lsn_hex: "0x000000230000015A0008".into(),
                seqval_hex: "0x000000230000015A0009".into(),
                operation: 1,
                ts_ms: 3,
                row: serde_json::json!({"id": "1", "name": "alice-v2"}),
            },
        ];

        // op=3+op=4 merge into a single Event → total 3 events (not 4).
        let events = handle.map_changes_to_events(&meta, changes).unwrap();
        assert_eq!(events.len(), 3);

        // INSERT
        assert_eq!(events[0].op, Operation::Insert);
        assert!(events[0].before.is_none(), "INSERT before should be None");
        assert_eq!(
            events[0].after,
            Some(serde_json::json!({"id": "1", "name": "alice"}))
        );

        // UPDATE — before=old values (op=3), after=new values (op=4)
        assert_eq!(events[1].op, Operation::Update);
        assert_eq!(
            events[1].before,
            Some(serde_json::json!({"id": "1", "name": "alice"})),
            "UPDATE before should hold the OLD row (op=3)"
        );
        assert_eq!(
            events[1].after,
            Some(serde_json::json!({"id": "1", "name": "alice-v2"})),
            "UPDATE after should hold the NEW row (op=4)"
        );

        // DELETE
        assert_eq!(events[2].op, Operation::Delete);
        assert_eq!(
            events[2].before,
            Some(serde_json::json!({"id": "1", "name": "alice-v2"}))
        );
        assert!(events[2].after.is_none(), "DELETE after should be None");

        // tx_id is derived from seqval and must be non-zero for non-trivial seqval.
        assert!(events[0].transaction.as_ref().unwrap().tx_id > 0);
    }

    #[test]
    fn update_pair_split_across_polls_merges_correctly() {
        // Verifies that an op=3 before-image buffered at the end of one poll window is
        // correctly merged when the matching op=4 after-image arrives in the next poll.
        let mut handle = SqlServerStreamHandle {
            config: config(),
            stream: SqlServerStream {
                lsn_start: [0; 10],
                lsn_end: [0; 10],
                change_tables: vec!["dbo_users".into()],
                poll_interval_ms: 5000,
                cursor: None,
            },
            metas: vec![],
            events_polled: 0,
            requeued_events: Vec::new(),
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            pending_update_befores: AHashMap::new(),
            window_buffer: Vec::new(),
        };

        let meta = CaptureInstanceMeta {
            capture_instance: "dbo_users".into(),
            schema: "dbo".into(),
            table: "users".into(),
            primary_key: vec!["id".into()],
            captured_columns: vec!["id".into(), "name".into()],
        };

        // Poll 1: only the op=3 before-image (OLD values) arrives.
        let poll1 = vec![SqlServerRawChange {
            start_lsn_hex: "0x000000230000015A0004".into(),
            seqval_hex: "0x000000230000015A0005".into(),
            operation: 3,
            ts_ms: 10,
            row: serde_json::json!({"id": "1", "name": "alice"}),
        }];
        let events1 = handle.map_changes_to_events(&meta, poll1).unwrap();
        assert!(
            events1.is_empty(),
            "op=3 alone should be buffered, not emitted"
        );
        assert_eq!(handle.pending_update_befores.len(), 1);

        // Poll 2: the op=4 after-image (NEW values) arrives; merges with the buffered op=3.
        let poll2 = vec![SqlServerRawChange {
            start_lsn_hex: "0x000000230000015A0004".into(),
            seqval_hex: "0x000000230000015A0005".into(),
            operation: 4,
            ts_ms: 10,
            row: serde_json::json!({"id": "1", "name": "alice-v2"}),
        }];
        let events2 = handle.map_changes_to_events(&meta, poll2).unwrap();
        assert_eq!(events2.len(), 1);
        assert_eq!(events2[0].op, Operation::Update);
        assert_eq!(
            events2[0].before,
            Some(serde_json::json!({"id": "1", "name": "alice"}))
        );
        assert_eq!(
            events2[0].after,
            Some(serde_json::json!({"id": "1", "name": "alice-v2"}))
        );
        assert!(
            handle.pending_update_befores.is_empty(),
            "buffer should be drained after merge"
        );
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
                cursor: None,
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
            pending_update_befores: AHashMap::new(),
            window_buffer: Vec::new(),
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
                cursor: None,
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
            pending_update_befores: AHashMap::new(),
            window_buffer: Vec::new(),
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
            None,
        );

        assert!(sql.contains("SELECT TOP (128)"));
        assert!(sql.contains("c.[id] AS [id]"), "{sql}");
        assert!(sql.contains("c.[name] AS [name]"), "{sql}");
        assert!(sql.contains("fn_cdc_get_all_changes_dbo_users"));
        assert!(
            sql.contains("ORDER BY c.__$start_lsn, c.__$seqval, c.__$operation"),
            "{sql}"
        );
        // Values must be serialized server-side, not decoded through a client-side
        // type ladder that silently nulls anything it does not recognise.
        assert!(
            sql.contains("FOR JSON PATH, WITHOUT_ARRAY_WRAPPER"),
            "{sql}"
        );
        // Commit time must come from the LSN→time mapping, not the poll wall-clock.
        assert!(sql.contains("fn_cdc_map_lsn_to_time"), "{sql}");
        // With no cursor there must be no WHERE clause narrowing the window.
        assert!(!sql.contains("WHERE"), "{sql}");
    }

    #[test]
    fn cdc_poll_sql_builder_resumes_from_a_within_window_cursor() {
        let cursor = SqlServerCdcCursor {
            lsn_hex: "0x0000002a".into(),
            seqval_hex: "0x0000002b".into(),
            operation: 3,
        };
        let sql = build_cdc_poll_sql(
            "dbo_users",
            &["id".to_string()],
            128,
            "0x01",
            "0x02",
            Some(&cursor),
        );

        // Strict lexicographic (lsn, seqval, operation) — `operation` must be part of
        // the key, because op=3/op=4 share one (lsn, seqval) and a two-part cursor
        // would skip the op=4 after-image when a batch boundary falls between them.
        assert!(sql.contains("c.__$start_lsn > CONVERT"), "{sql}");
        assert!(sql.contains("c.__$seqval > CONVERT"), "{sql}");
        assert!(sql.contains("c.__$operation > 3"), "{sql}");
    }

    #[test]
    fn cdc_cursor_round_trips_through_the_checkpoint_offset() {
        let cursor = SqlServerCdcCursor {
            lsn_hex: "0x0000002a0000015a0002".into(),
            seqval_hex: "0x0000002a0000015a0003".into(),
            operation: 4,
        };
        let encoded = cursor.encode();
        assert_eq!(
            SqlServerCdcCursor::decode(&encoded),
            Some(cursor),
            "cursor must survive a checkpoint round trip"
        );

        // A bare LSN is the window-boundary form and carries no cursor.
        assert_eq!(SqlServerCdcCursor::decode("0x0000002a0000015a0002"), None);
    }

    #[test]
    fn lsn_hex_is_lowercase_to_match_server_rendering() {
        // `sys.fn_varbintohexstr` emits lowercase. Mixing cases breaks the truncate
        // comparison under a binary collation and the window-buffer sort by offset.
        let hex = lsn_bytes_to_hex(&[0x0A, 0xBC, 0xDE, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(hex, hex.to_lowercase(), "LSN hex must be lowercase");
        assert!(hex.starts_with("0x0abcde"), "{hex}");
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
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
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

    /// Verifies that `map_changes_to_events` followed by the LSN sort used in
    /// `next_events` produces strictly ordered output across two capture
    /// instances (tables) whose events arrive out-of-LSN-order from the DB.
    ///
    /// Before the window-buffer fix, changes were appended per capture-instance
    /// in poll-loop order — `table_a` events always preceded `table_b` events
    /// regardless of commit LSN.  After the fix, the combined batch is sorted by
    /// `(start_lsn, seqval, operation)` before delivery.
    #[test]
    fn cross_table_events_are_sorted_by_lsn() {
        // table_a has a single INSERT at LSN 0003 (later).
        let meta_a = CaptureInstanceMeta {
            capture_instance: "dbo_table_a".into(),
            schema: "dbo".into(),
            table: "table_a".into(),
            primary_key: vec!["id".into()],
            captured_columns: vec!["id".into()],
        };
        let changes_a = vec![SqlServerRawChange {
            start_lsn_hex: "0x000000230000000A0003".into(), // later LSN
            seqval_hex: "0x00000000000000000001".into(),
            operation: 2, // INSERT
            ts_ms: 10,
            row: serde_json::json!({"id": "10"}),
        }];

        // table_b has a single INSERT at LSN 0001 (earlier).
        let meta_b = CaptureInstanceMeta {
            capture_instance: "dbo_table_b".into(),
            schema: "dbo".into(),
            table: "table_b".into(),
            primary_key: vec!["id".into()],
            captured_columns: vec!["id".into()],
        };
        let changes_b = vec![SqlServerRawChange {
            start_lsn_hex: "0x000000230000000A0001".into(), // earlier LSN
            seqval_hex: "0x00000000000000000001".into(),
            operation: 2, // INSERT
            ts_ms: 5,
            row: serde_json::json!({"id": "20"}),
        }];

        // Simulate the per-capture-instance raw changes exactly as `next_events` collects them:
        // table_a polled first (returns LSN 0003), then table_b (LSN 0001).
        let mut handle = SqlServerStreamHandle {
            config: config(),
            stream: SqlServerStream {
                lsn_start: [0; 10],
                lsn_end: [0xff; 10],
                change_tables: vec!["dbo_table_a".into(), "dbo_table_b".into()],
                poll_interval_ms: 5000,
                cursor: None,
            },
            metas: vec![meta_a.clone(), meta_b.clone()],
            events_polled: 0,
            requeued_events: Vec::new(),
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            pending_update_befores: AHashMap::new(),
            window_buffer: Vec::new(),
        };

        // Collect all changes (as next_events does) and flatten with meta.
        let all_changes: Vec<(CaptureInstanceMeta, Vec<SqlServerRawChange>)> =
            vec![(meta_a, changes_a), (meta_b, changes_b)];
        let mut flat: Vec<(CaptureInstanceMeta, SqlServerRawChange)> = all_changes
            .into_iter()
            .flat_map(|(meta, changes)| changes.into_iter().map(move |c| (meta.clone(), c)))
            .collect();

        // Apply the same sort as next_events.
        flat.sort_by(|(_, a), (_, b)| {
            let ord = match (
                parser::lsn_hex_to_bytes_opt(&a.start_lsn_hex),
                parser::lsn_hex_to_bytes_opt(&b.start_lsn_hex),
            ) {
                (Some(la), Some(lb)) => compare_lsn(&la, &lb),
                _ => std::cmp::Ordering::Equal,
            };
            ord.then_with(|| a.seqval_hex.cmp(&b.seqval_hex))
                .then_with(|| a.operation.cmp(&b.operation))
        });

        // Map to events.
        let mut events = Vec::new();
        for (meta, change) in flat {
            let mut batch = handle.map_changes_to_events(&meta, vec![change]).unwrap();
            events.append(&mut batch);
        }

        // table_b's event (LSN 0001) must come before table_a's event (LSN 0003)
        // regardless of poll order.
        assert_eq!(events.len(), 2, "expected 2 events");
        let offsets: Vec<&str> = events.iter().map(|e| e.source.offset.as_str()).collect();
        assert!(
            offsets[0] < offsets[1],
            "events must be in ascending LSN order; got {offsets:?}"
        );
        // Confirm which table came first.
        assert_eq!(
            events[0].table, "table_b",
            "table_b (earlier LSN) must be first"
        );
        assert_eq!(
            events[1].table, "table_a",
            "table_a (later LSN) must be second"
        );
    }
}
