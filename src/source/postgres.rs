//! PostgreSQL source configuration, connection lifecycle, and validation helpers.

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_postgres::{Client, Connection, Socket};

use serde::{Deserialize, Serialize};

use crate::{
    checkpoint::PostgresOffset,
    core::{
        Error, Event, Offset, Result, SecretString, StructuredLogger, TransportConfig,
    },
    source::{
        helpers::now_millis, ConnectorCapabilities, HandoffResult, SnapshotHandle, Source,
        StreamHandle, IncrementalSnapshotConfig,
    },
};

mod decoder;
mod parser;
mod query;
mod state;
mod config;
mod handoff;
mod stream_messages;
mod snapshot_chunk;
mod snapshot_finalize;
mod stream_start;
mod snapshot_start;
mod validation;
pub mod incremental_snapshot;

// Import decoder types used directly in this module.
use self::decoder::{PgOutputMessageProvider, PgRelation};

use self::handoff::postgres_handoff_result;
#[cfg(test)]
use self::handoff::postgres_handoff_stream_watermark_gap;
use self::snapshot_chunk::next_postgres_snapshot_chunk;
use self::snapshot_finalize::{checkpoint_postgres_snapshot, finish_postgres_snapshot};
use self::snapshot_start::{
    start_postgres_snapshot_from_checkpoint, start_postgres_snapshot_internal,
};
use self::stream_start::start_postgres_stream;
use self::validation::validate_connected_postgres_client;
use self::state::{
    ConnectionState, PostgresHandoff, PostgresStream, SnapshotCheckpointState, StreamState,
    TableSnapshotState,
};
pub use self::incremental_snapshot::IncrementalSnapshotHandle;

const HEARTBEAT_SECS: u64 = 60;
const DEFAULT_SNAPSHOT_CHUNK_SIZE: usize = 5_000;
const STREAM_POLL_INTERVAL_MS: u64 = 50;
const MAX_EVENTS_PER_POLL: usize = 1_000;

// ─── PostgresStreamHandle ───────────────────────────────────────────────────
pub struct PostgresStreamHandle {
    source_name: String,
    stream: PostgresStream,
    provider: Box<dyn PgOutputMessageProvider>,
    relation_map: HashMap<u32, PgRelation>,
    current_xid: Option<u32>,
    current_commit_ts: u64,
    partial_tx_events: Vec<Event>,
    events_polled: u64,
    max_events_per_poll: usize,
    stream_poll_interval_ms: u64,
}

impl PostgresStreamHandle {
    fn new(
        source_name: String,
        stream: PostgresStream,
        provider: Box<dyn PgOutputMessageProvider>,
        max_events_per_poll: usize,
        stream_poll_interval_ms: u64,
    ) -> Self {
        Self {
            source_name,
            stream,
            provider,
            relation_map: HashMap::new(),
            current_xid: None,
            current_commit_ts: 0,
            partial_tx_events: Vec::new(),
            events_polled: 0,
            max_events_per_poll: max_events_per_poll.max(1),
            stream_poll_interval_ms: stream_poll_interval_ms.max(1),
        }
    }
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
pub struct PostgresSnapshot {
    pub tables: Vec<TableSnapshot>,
    pub snapshot_id: String,
    pub snapshot_start_ts: u64,
    pub snapshot_end_ts: u64,
}

pub struct PostgresSnapshotHandle {
    source_name: String,
    snapshot: PostgresSnapshot,
    tables: Vec<TableSnapshotState>,
    client: Option<Arc<Client>>,
    transaction_open: bool,
    snapshot_watermark: u64,
    current_table: usize,
    next_chunk_index: u32,
    emitted_rows: u64,
    emitted_in_run: u64,
}

impl PostgresSnapshotHandle {
    fn new(
        source_name: String,
        snapshot: PostgresSnapshot,
        tables: Vec<TableSnapshotState>,
        client: Option<Arc<Client>>,
        transaction_open: bool,
        snapshot_watermark: u64,
    ) -> Self {
        Self {
            source_name,
            snapshot,
            tables,
            client,
            transaction_open,
            snapshot_watermark,
            current_table: 0,
            next_chunk_index: 0,
            emitted_rows: 0,
            emitted_in_run: 0,
        }
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

    fn has_live_query_tables(&self) -> bool {
        self.tables.iter().any(|table| table.live_query)
    }

    fn decode_pk_cursor(cursor: &str, expected_columns: usize) -> Result<Vec<String>> {
        let values: Vec<String> = serde_json::from_str(cursor).map_err(|error| {
            Error::CheckpointError(format!(
                "invalid postgres snapshot cursor: expected JSON array of primary key values: {error}"
            ))
        })?;

        if values.len() != expected_columns {
            return Err(Error::CheckpointError(format!(
                "invalid postgres snapshot cursor: expected {expected_columns} key values, got {}",
                values.len()
            )));
        }

        Ok(values)
    }

    fn derive_current_table_from_progress(tables: &[TableSnapshotState]) -> usize {
        tables
            .iter()
            .position(|table| !table.snapshot.is_complete)
            .unwrap_or(tables.len())
    }

    fn resume_from_checkpoint_payload(mut self, payload: &[u8]) -> Result<Self> {
        let state: SnapshotCheckpointState = serde_json::from_slice(payload)?;
        if state.tables.len() != self.tables.len() {
            return Err(Error::CheckpointError(
                "postgres snapshot checkpoint table count does not match snapshot handle".into(),
            ));
        }

        self.snapshot.snapshot_id = state.snapshot_id;
        self.snapshot.snapshot_start_ts = state.snapshot_start_ts;
        self.snapshot.snapshot_end_ts = state.snapshot_end_ts;
        self.snapshot_watermark = state.snapshot_watermark;
        self.next_chunk_index = state.next_chunk_index;
        self.emitted_rows = 0;
        self.emitted_in_run = 0;

        for (index, table_state) in self.tables.iter_mut().enumerate() {
            let saved = &state.tables[index];
            if table_state.snapshot.table != saved.table {
                return Err(Error::CheckpointError(format!(
                    "postgres snapshot checkpoint table mismatch at index {index}: expected '{}' got '{}'",
                    table_state.snapshot.table, saved.table
                )));
            }

            table_state.snapshot = saved.clone();
            if table_state.live_query {
                if let Some(cursor) = table_state.snapshot.cursor_position.as_deref() {
                    Self::decode_pk_cursor(cursor, table_state.primary_key_columns.len()).map_err(
                        |error| {
                            Error::CheckpointError(format!(
                                "invalid postgres snapshot cursor for table '{}': {error}",
                                saved.table
                            ))
                        },
                    )?;
                }
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

        self.current_table = Self::derive_current_table_from_progress(&self.tables);

        if state.current_table != self.current_table {
            return Err(Error::CheckpointError(format!(
                "postgres snapshot checkpoint current_table mismatch: saved={} derived={} from table completion state",
                state.current_table, self.current_table
            )));
        }

        if self.current_table > self.tables.len() {
            return Err(Error::CheckpointError(format!(
                "postgres snapshot checkpoint current_table {} exceeds table count {}",
                self.current_table,
                self.tables.len()
            )));
        }

        self.sync_snapshot_tables();
        Ok(self)
    }

    async fn fetch_live_rows(
        &self,
        table: &str,
        key_columns: &[String],
        key_types: &[String],
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(Vec<String>, serde_json::Value)>> {
        let client = self.client.as_ref().ok_or_else(|| {
            Error::StateError(
                "postgres snapshot live query requires an active client connection".into(),
            )
        })?;
        let (schema, table_name) = parse_table_reference(table)?;
        let table_ref = qualified_table_name(&schema, &table_name);
        let limit = i64::try_from(limit).map_err(|_| {
            Error::SourceError(format!("snapshot chunk size exceeds i64 limit: {limit}"))
        })?;

        if key_columns.is_empty() || key_types.is_empty() || key_columns.len() != key_types.len() {
            return Err(Error::SourceError(format!(
                "missing or invalid primary key metadata for snapshot table '{schema}.{table_name}'"
            )));
        }

        let order_expr = key_columns
            .iter()
            .map(|column| format!("t.{}", quote_pg_identifier(column)))
            .collect::<Vec<_>>()
            .join(", ");
        let key_value_expr = key_columns
            .iter()
            .map(|column| format!("t.{}::text", quote_pg_identifier(column)))
            .collect::<Vec<_>>()
            .join(", ");

        let rows = if let Some(last_pk_cursor) = cursor {
            let key_values =
                Self::decode_pk_cursor(last_pk_cursor, key_columns.len()).map_err(|error| {
                    Error::SourceError(format!(
                        "invalid snapshot cursor for table '{table}': {error}"
                    ))
                })?;

            // Bind snapshot keyset cursor values as text and cast inside SQL.
            // This keeps checkpoint cursor encoding stable across restarts while
            // avoiding driver-side serialization mismatches for typed PK columns.
            let predicate_expr = key_types
                .iter()
                .enumerate()
                .map(|(index, pg_type)| format!("${}::text::{pg_type}", index + 1))
                .collect::<Vec<_>>()
                .join(", ");

            let query = format!(
                "SELECT ARRAY[{key_value_expr}], row_to_json(t)::text \
                 FROM {table_ref} t \
                 WHERE ({order_expr}) > ({predicate_expr}) \
                 ORDER BY {order_expr} \
                 LIMIT ${}",
                key_columns.len() + 1
            );

            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                Vec::with_capacity(key_values.len() + 1);
            for value in &key_values {
                params.push(value as &(dyn tokio_postgres::types::ToSql + Sync));
            }
            params.push(&limit as &(dyn tokio_postgres::types::ToSql + Sync));

            client
                .query(&query, &params)
                .await
                .map_err(|error| {
                    Error::SourceError(format!(
                        "failed fetching snapshot rows for table '{schema}.{table_name}' after cursor {last_pk_cursor}: {error}"
                    ))
                })?
        } else {
            let query = format!(
                "SELECT ARRAY[{key_value_expr}], row_to_json(t)::text \
                 FROM {table_ref} t \
                 ORDER BY {order_expr} \
                 LIMIT $1"
            );
            client.query(&query, &[&limit]).await.map_err(|error| {
                Error::SourceError(format!(
                    "failed fetching snapshot rows for table '{schema}.{table_name}': {error}"
                ))
            })?
        };

        let mut decoded = Vec::with_capacity(rows.len());
        for row in rows {
            let key_values: Vec<Option<String>> = row.get(0);
            let key_values = key_values
                .into_iter()
                .map(|value| {
                    value.ok_or_else(|| {
                        Error::SourceError(format!(
                            "primary key column returned null value for table '{schema}.{table_name}'"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let payload: String = row.get(1);
            let json = serde_json::from_str(&payload).map_err(|error| {
                Error::SerializationError(format!(
                    "failed decoding live snapshot JSON row for table '{schema}.{table_name}': {error}"
                ))
            })?;
            decoded.push((key_values, json));
        }

        Ok(decoded)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresSourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: SecretString,
    pub database: String,
    pub replication_slot_name: String,
    pub publication_name: String,
    pub transport: TransportConfig,
    pub conn_timeout_secs: u64,
    /// Stream poll interval in milliseconds.
    pub stream_poll_interval_ms: u64,
    /// Maximum events yielded by a single stream poll cycle.
    pub max_events_per_poll: usize,
    /// Allowlist of tables to stream, in `"schema.table"` format.
    ///
    /// When non-empty, only tables in this list are forwarded to the caller.
    /// Takes precedence over [`table_exclude_list`](PostgresSourceConfig::table_exclude_list).
    /// An empty list means *all* tables are included (subject to the publication).
    pub table_include_list: Vec<String>,
    /// Blocklist of tables to suppress, in `"schema.table"` format.
    ///
    /// Ignored when [`table_include_list`](PostgresSourceConfig::table_include_list) is non-empty.
    /// An empty list means no tables are excluded.
    pub table_exclude_list: Vec<String>,
}

/// PostgreSQL connector lifecycle manager.
pub struct PostgresConnection {
    config: PostgresSourceConfig,
    logger: StructuredLogger,
    state: Arc<Mutex<ConnectionState>>,
    stream_poll_interval_ms: u64,
    max_events_per_poll: usize,
}

impl PostgresConnection {
    pub fn new(config: PostgresSourceConfig) -> Self {
        let stream_poll_interval_ms = config.stream_poll_interval_ms.max(1);
        let max_events_per_poll = config.max_events_per_poll.max(1);
        Self {
            config,
            logger: StructuredLogger::new("postgres"),
            state: Arc::new(Mutex::new(ConnectionState::default())),
            stream_poll_interval_ms,
            max_events_per_poll,
        }
    }

    pub fn with_logger(config: PostgresSourceConfig, logger: StructuredLogger) -> Self {
        let stream_poll_interval_ms = config.stream_poll_interval_ms.max(1);
        let max_events_per_poll = config.max_events_per_poll.max(1);
        Self {
            config,
            logger,
            state: Arc::new(Mutex::new(ConnectionState::default())),
            stream_poll_interval_ms,
            max_events_per_poll,
        }
    }

    pub async fn connect(&self) -> Result<()> {
        self.config.validate()?;
        {
            let state = self.state.lock().await;
            if state.client.is_some() {
                return Err(Error::StateError(
                    "postgres connection already established".into(),
                ));
            }
        }

        let connect_result: Result<()> = match &self.config.transport {
            TransportConfig::Plaintext => {
                let connect_config = self.config.build_connect_config()?;
                let (client, connection) = connect_config
                    .connect(tokio_postgres::NoTls)
                    .await
                    .map_err(|error| Error::SourceError(format!("postgres plaintext connection failed: {error}")))?;
                let connection_task = tokio::spawn(run_connection_task(connection));
                self.validate_connected_client(&client).await?;
                let client = Arc::new(client);
                let heartbeat_task = self.start_heartbeat(client.clone());
                let mut state = self.state.lock().await;
                state.client = Some(client);
                state.connection_task = Some(connection_task);
                state.heartbeat_task = Some(heartbeat_task);
                Ok(())
            }
            TransportConfig::Tls { ca_cert_path, client_cert_path, client_key_path } => {
                #[cfg(not(feature = "tls"))]
                {
                    let _ = (ca_cert_path, client_cert_path, client_key_path);
                    return Err(Error::ConfigError(
                        "postgres connector requires crate feature 'tls' for TLS transport"
                            .into(),
                    ));
                }

                #[cfg(feature = "tls")]
                {
                    use tokio_postgres_rustls::MakeRustlsConnect;

                    let tls_config = build_tls_client_config(
                        ca_cert_path.as_deref(),
                        client_cert_path.as_deref(),
                        client_key_path.as_deref(),
                    )?;

                    let tls_connector = MakeRustlsConnect::new(tls_config);
                    let connect_config = self.config.build_connect_config()?;
                    let (client, connection) = connect_config
                        .connect(tls_connector)
                        .await
                        .map_err(|error| Error::SourceError(format!("postgres tls connection failed: {error}")))?;

                    let connection_task = tokio::spawn(run_connection_task(connection));
                    self.validate_connected_client(&client).await?;
                    let client = Arc::new(client);
                    let heartbeat_task = self.start_heartbeat(client.clone());

                    let mut state = self.state.lock().await;
                    state.client = Some(client);
                    state.connection_task = Some(connection_task);
                    state.heartbeat_task = Some(heartbeat_task);
                    Ok(())
                }
            }
        };
        connect_result.inspect(|_| self.logger.source_connected())?;
        Ok(())
    }

    pub async fn close(&self) {
        let mut state = self.state.lock().await;
        if let Some(handle) = state.heartbeat_task.take() {
            handle.abort();
        }
        if let Some(handle) = state.connection_task.take() {
            handle.abort();
        }
        state.client = None;
        self.logger.source_disconnected();
    }

    pub async fn is_connected(&self) -> bool {
        self.state.lock().await.client.is_some()
    }

    async fn start_snapshot_internal(&mut self, tables: &[&str]) -> Result<PostgresSnapshotHandle> {
        start_postgres_snapshot_internal(self, tables).await
    }

    pub async fn start_snapshot_from_checkpoint(
        &mut self,
        tables: &[&str],
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn SnapshotHandle>> {
        start_postgres_snapshot_from_checkpoint(self, tables, resume_from).await
    }

    /// Start an incremental (non-blocking) snapshot using the DBLog watermark pattern.
    ///
    /// Unlike `start_snapshot`, this method:
    /// - Does **not** pause the replication stream.
    /// - Does **not** hold a `REPEATABLE READ` transaction.
    /// - Reads the table in small chunks (keyset-paginated, `READ COMMITTED`).
    /// - For each chunk, captures a low/high watermark LSN and uses the replication
    ///   stream to detect concurrent writes, suppressing stale chunk rows.
    ///
    /// The returned `StreamHandle` interleaves snapshot `Read` events with live
    /// replication events.  Once all tables are exhausted it acts as a pure
    /// stream delegate.
    ///
    /// `resume_from` is forwarded to `start_stream` to resume from a saved
    /// checkpoint offset.
    pub async fn start_incremental_snapshot(
        &mut self,
        config: IncrementalSnapshotConfig,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>> {
        let client = {
            let state = self.state.lock().await;
            state.client.clone().ok_or_else(|| {
                Error::StateError(
                    "postgres connection must be established before starting an incremental snapshot".into(),
                )
            })?
        };
        let inner = start_postgres_stream(self, resume_from).await?;
        let source_name = self.source_type().to_string();
        let handle =
            IncrementalSnapshotHandle::new(inner, client, config, source_name).await?;
        Ok(Box::new(handle))
    }

    async fn validate_connected_client(&self, client: &Client) -> Result<()> {
        validate_connected_postgres_client(&self.config, client).await
    }

    fn start_heartbeat(&self, client: Arc<Client>) -> JoinHandle<()> {
        let logger = self.logger.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(HEARTBEAT_SECS));
            loop {
                interval.tick().await;
                if let Err(error) = client.simple_query("SELECT 1").await {
                    logger.connection_error(&format!("heartbeat query failed: {error}"));
                    break;
                }
            }
        })
    }
}

impl Drop for PostgresConnection {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.try_lock() {
            if let Some(handle) = state.heartbeat_task.take() {
                handle.abort();
            }
            if let Some(handle) = state.connection_task.take() {
                handle.abort();
            }
        }
    }
}

#[async_trait]
impl Source for PostgresConnection {
    async fn start_snapshot(&mut self, tables: &[&str]) -> Result<Box<dyn SnapshotHandle>> {
        Ok(Box::new(self.start_snapshot_internal(tables).await?))
    }

    async fn start_stream(
        &mut self,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>> {
        start_postgres_stream(self, resume_from).await
    }

    async fn perform_handoff(
        &mut self,
        snapshot: &mut dyn SnapshotHandle,
        stream: &mut dyn StreamHandle,
    ) -> Result<HandoffResult> {
        let (snapshot_watermark, stream_watermark) = {
            let state = self.state.lock().await;
            let snapshot_watermark = state.snapshot_watermark.ok_or_else(|| {
                Error::StateError(
                    "postgres perform_handoff requires start_snapshot to have been called first"
                        .into(),
                )
            })?;
            let stream_watermark = state.stream_start_watermark.ok_or_else(|| {
                Error::StateError(
                    "postgres perform_handoff requires start_stream to have been called first"
                        .into(),
                )
            })?;
            (snapshot_watermark, stream_watermark)
        };

        let snapshot_end = snapshot.finish().await?.snapshot_end_ts;
        stream.confirm_lsn(snapshot_watermark).await?;
        let handoff = PostgresHandoff {
            snapshot_watermark,
            stream_watermark,
            handoff_complete: true,
        };

        tracing::info!(
            target: "cdc_rs::source::postgres",
            snapshot_watermark = handoff.snapshot_watermark,
            stream_watermark = handoff.stream_watermark,
            stream_watermark_gap = handoff.stream_watermark_gap(),
            "postgres snapshot-to-stream handoff completed",
        );

        postgres_handoff_result(
            Some(snapshot_end),
            Some(handoff.snapshot_watermark),
            Some(handoff.stream_watermark),
        )
    }

    fn source_type(&self) -> &str {
        PostgresSourceConfig::source_type()
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
            truncate: true,
            incremental_snapshot: true,
        }
    }
}

#[async_trait]
impl SnapshotHandle for PostgresSnapshotHandle {
    async fn next_chunk(&mut self, chunk_size: usize) -> Result<Vec<Event>> {
        next_postgres_snapshot_chunk(self, chunk_size).await
    }

    async fn checkpoint(
        &self,
        checkpoint: &mut dyn crate::checkpoint::Checkpoint,
        committed_event_count: u64,
    ) -> Result<()> {
        checkpoint_postgres_snapshot(self, checkpoint, committed_event_count).await
    }

    async fn finish(&mut self) -> Result<crate::source::SnapshotEnd> {
        finish_postgres_snapshot(self).await
    }
}

#[async_trait]
impl StreamHandle for PostgresStreamHandle {
    async fn next_events(&mut self, timeout_ms: u64) -> Result<Vec<Event>> {
        if self.stream.replication_status != StreamState::Streaming {
            return Err(Error::StateError(
                "postgres stream polling requested while stream is not running".into(),
            ));
        }

        let started = std::time::Instant::now();
        let timeout = Duration::from_millis(timeout_ms);

        loop {
            let xlog_data = self
                .provider
                .poll_xlog_data(self.max_events_per_poll)
                .await?;
            if !xlog_data.is_empty() {
                let events = self.process_messages(xlog_data).await?;
                if !events.is_empty() {
                    tracing::debug!(
                        target: "cdc_rs::source::postgres",
                        count = events.len(),
                        lsn = self.stream.lsn_position,
                        "postgres stream events received",
                    );
                    return Ok(events);
                }
                // Got only metadata messages (RELATION, etc.) — continue polling.
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
        let offset = PostgresOffset {
            lsn: self.stream.lsn_position,
            slot_name: self.stream.slot_name.clone(),
        };
        checkpoint.save(&offset, self.events_polled).await
    }

    async fn confirm_lsn(&mut self, lsn: u64) -> Result<()> {
        self.provider.confirm_lsn(lsn).await
    }
}

impl Drop for PostgresStreamHandle {
    fn drop(&mut self) {
        self.stream.replication_status = StreamState::Stopped;
    }
}

fn parse_table_reference(table: &str) -> Result<(String, String)> {
    parser::parse_table_reference(table)
}

fn quote_pg_identifier(identifier: &str) -> String {
    parser::quote_pg_identifier(identifier)
}

fn qualified_table_name(schema: &str, table: &str) -> String {
    parser::qualified_table_name(schema, table)
}

async fn query_primary_key_columns_and_types(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<(Vec<String>, Vec<String>)> {
    query::query_primary_key_columns_and_types(client, schema, table).await
}

fn parse_pg_lsn(value: &str) -> Result<u64> {
    parser::parse_pg_lsn(value)
}

/// Format a u64 LSN as the PostgreSQL "HIGH/LOW" hex string expected by SQL queries.
fn format_pg_lsn(lsn: u64) -> String {
    parser::format_pg_lsn(lsn)
}

/// Convert a PostgreSQL microsecond timestamp (since 2000-01-01 UTC) to Unix milliseconds.
fn pg_timestamp_to_millis(pg_us: i64) -> u64 {
    parser::pg_timestamp_to_millis(pg_us)
}

fn decode_stream_resume_lsn(
    source_type: &str,
    configured_slot_name: &str,
    resume_from: &dyn Offset,
) -> Result<u64> {
    parser::decode_stream_resume_lsn(source_type, configured_slot_name, resume_from)
}

#[cfg(test)]
fn reconcile_stream_resume_lsn(
    checkpoint_lsn: u64,
    slot_confirmed_lsn: u64,
    slot_name: &str,
) -> Result<u64> {
    parser::reconcile_stream_resume_lsn(checkpoint_lsn, slot_confirmed_lsn, slot_name)
}

async fn reconcile_stream_resume_lsn_with_retry(
    client: &Client,
    checkpoint_lsn: u64,
    slot_name: &str,
    attempts: usize,
    retry_delay: Duration,
) -> Result<u64> {
    query::reconcile_stream_resume_lsn_with_retry(
        client,
        checkpoint_lsn,
        slot_name,
        attempts,
        retry_delay,
    )
    .await
}

async fn query_current_wal_lsn(client: &Client) -> Result<u64> {
    query::query_current_wal_lsn(client).await
}

/// Build a rustls `RootCertStore` from a PEM file path, or use system roots if `None`.
///
/// When mTLS paths are provided (`client_cert_path` + `client_key_path`), mutual TLS
/// authentication is configured. When only `ca_cert_path` is provided, server-auth-only
/// TLS is used. Falls back to system trust roots when `ca_cert_path` is `None`.
#[cfg(feature = "tls")]
fn build_tls_client_config(
    ca_cert_path: Option<&str>,
    client_cert_path: Option<&str>,
    client_key_path: Option<&str>,
) -> Result<rustls::ClientConfig> {
    query::build_tls_client_config(ca_cert_path, client_cert_path, client_key_path)
}

async fn run_connection_task<S>(connection: Connection<Socket, S>)
where
    S: tokio_postgres::tls::TlsStream + Send + Unpin + 'static,
{
    if let Err(error) = connection.await {
        tracing::warn!(target: "cdc_rs::source::postgres", %error, "postgres connection task ended with error");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use crate::checkpoint::{Checkpoint, InMemoryCheckpoint, PostgresOffset};
    use crate::source::{SnapshotHandle, Source, StreamHandle};

    use super::PostgresSourceConfig;
    use super::validation::{validate_with_backend, ValidationBackend};
    use super::{
        PgOutputMessageProvider, PostgresConnection, PostgresSnapshotHandle, PostgresStream,
        PostgresStreamHandle, StreamState, TableSnapshot, TableSnapshotState, MAX_EVENTS_PER_POLL,
        STREAM_POLL_INTERVAL_MS,
    };
    use super::decoder::{
        decode_pgoutput_message, PgOutputMessage, PgOutputXLogData, PgValue,
    };
    use super::parser::map_pgoutput_poll_error;
    use crate::{core::TransportConfig, SecretString};

    // ─── Validation backend mock ─────────────────────────────────────────────

    #[derive(Default)]
    struct MockValidationBackend {
        slot_exists: bool,
        create_slot_result: Option<crate::core::Error>,
        publication_exists: bool,
        has_replication_privilege: bool,
        create_called: Arc<AtomicBool>,
    }

    #[async_trait]
    impl ValidationBackend for MockValidationBackend {
        async fn replication_slot_exists(&self, _slot_name: &str) -> crate::core::Result<bool> {
            Ok(self.slot_exists)
        }

        async fn create_replication_slot(&self, _slot_name: &str) -> crate::core::Result<()> {
            self.create_called.store(true, Ordering::Relaxed);
            if let Some(error) = &self.create_slot_result {
                return Err(crate::core::Error::SourceError(error.to_string()));
            }
            Ok(())
        }

        async fn publication_exists(&self, _publication_name: &str) -> crate::core::Result<bool> {
            Ok(self.publication_exists)
        }

        async fn has_replication_privilege(&self) -> crate::core::Result<bool> {
            Ok(self.has_replication_privilege)
        }
    }

    // ─── Pgoutput message provider mock ──────────────────────────────────────

    struct MockPgOutputProvider {
        batches: VecDeque<Vec<PgOutputXLogData>>,
        confirmed_lsn: Arc<Mutex<u64>>,
    }

    impl MockPgOutputProvider {
        fn new(batches: Vec<Vec<PgOutputXLogData>>) -> Self {
            Self {
                batches: batches.into_iter().collect(),
                confirmed_lsn: Arc::new(Mutex::new(0)),
            }
        }
    }

    #[async_trait]
    impl PgOutputMessageProvider for MockPgOutputProvider {
        async fn poll_xlog_data(
            &mut self,
            _max: usize,
        ) -> crate::core::Result<Vec<PgOutputXLogData>> {
            Ok(self.batches.pop_front().unwrap_or_default())
        }

        async fn confirm_lsn(&mut self, lsn: u64) -> crate::core::Result<()> {
            *self.confirmed_lsn.lock().await = lsn;
            Ok(())
        }
    }

    #[test]
    fn default_config_prefers_tls_when_available() {
        let config = PostgresSourceConfig::default();
        assert!(config.transport.is_tls());
    }

    // ─── Binary message builders ──────────────────────────────────────────────

    fn build_begin(final_lsn: u64, timestamp_us: i64, xid: u32) -> Vec<u8> {
        let mut buf = vec![b'B'];
        buf.extend_from_slice(&final_lsn.to_be_bytes());
        buf.extend_from_slice(&timestamp_us.to_be_bytes());
        buf.extend_from_slice(&xid.to_be_bytes());
        buf
    }

    fn build_commit(commit_lsn: u64, end_lsn: u64, timestamp_us: i64) -> Vec<u8> {
        let mut buf = vec![b'C', 0u8]; // flags = 0
        buf.extend_from_slice(&commit_lsn.to_be_bytes());
        buf.extend_from_slice(&end_lsn.to_be_bytes());
        buf.extend_from_slice(&timestamp_us.to_be_bytes());
        buf
    }

    fn build_relation(oid: u32, ns: &str, name: &str, cols: &[(&str, bool)]) -> Vec<u8> {
        let mut buf = vec![b'R'];
        buf.extend_from_slice(&oid.to_be_bytes());
        buf.extend_from_slice(ns.as_bytes());
        buf.push(0);
        buf.extend_from_slice(name.as_bytes());
        buf.push(0);
        buf.push(b'd'); // replica identity = default
        let num: u16 = cols.len() as u16;
        buf.extend_from_slice(&num.to_be_bytes());
        for (col, is_key) in cols {
            buf.push(u8::from(*is_key));
            buf.extend_from_slice(col.as_bytes());
            buf.push(0);
            buf.extend_from_slice(&23u32.to_be_bytes()); // int4 OID
            buf.extend_from_slice(&(-1i32).to_be_bytes()); // atttypmod = -1
        }
        buf
    }

    fn append_tuple_data(buf: &mut Vec<u8>, values: &[Option<&str>]) {
        buf.extend_from_slice(&(values.len() as u16).to_be_bytes());
        for val in values {
            match val {
                None => buf.push(b'n'),
                Some(s) => {
                    buf.push(b't');
                    buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
            }
        }
    }

    fn build_insert(oid: u32, values: &[Option<&str>]) -> Vec<u8> {
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&oid.to_be_bytes());
        buf.push(b'N');
        append_tuple_data(&mut buf, values);
        buf
    }

    fn build_update(oid: u32, old: Option<&[Option<&str>]>, new: &[Option<&str>]) -> Vec<u8> {
        let mut buf = vec![b'U'];
        buf.extend_from_slice(&oid.to_be_bytes());
        if let Some(old_vals) = old {
            buf.push(b'O');
            append_tuple_data(&mut buf, old_vals);
        }
        buf.push(b'N');
        append_tuple_data(&mut buf, new);
        buf
    }

    fn build_delete(oid: u32, key: &[Option<&str>]) -> Vec<u8> {
        let mut buf = vec![b'D'];
        buf.extend_from_slice(&oid.to_be_bytes());
        buf.push(b'K');
        append_tuple_data(&mut buf, key);
        buf
    }

    fn xlog(lsn: u64, data: Vec<u8>) -> PgOutputXLogData {
        PgOutputXLogData { lsn, data }
    }

    fn make_stream_handle(
        initial_lsn: u64,
        provider: MockPgOutputProvider,
    ) -> PostgresStreamHandle {
        PostgresStreamHandle::new(
            "postgres".into(),
            PostgresStream {
                slot_name: "slot".into(),
                publication_name: "pub".into(),
                lsn_position: initial_lsn,
                replication_status: StreamState::Streaming,
            },
            Box::new(provider),
            super::MAX_EVENTS_PER_POLL,
            super::STREAM_POLL_INTERVAL_MS,
        )
    }

    // ─── Pgoutput decoder tests ───────────────────────────────────────────────

    #[test]
    fn decode_pgoutput_begin_message() {
        let data = build_begin(1000, 946_684_800_000_000, 42);
        match decode_pgoutput_message(&data).unwrap() {
            PgOutputMessage::Begin(b) => {
                assert_eq!(b.final_lsn, 1000);
                assert_eq!(b.xid, 42);
                assert_eq!(b.commit_timestamp_us, 946_684_800_000_000);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_pgoutput_commit_message() {
        let data = build_commit(900, 1000, 0);
        match decode_pgoutput_message(&data).unwrap() {
            PgOutputMessage::Commit(c) => {
                assert_eq!(c.commit_lsn, 900);
                assert_eq!(c.end_lsn, 1000);
                assert_eq!(c.flags, 0);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_pgoutput_relation_message() {
        let data = build_relation(1001, "public", "users", &[("id", true), ("name", false)]);
        match decode_pgoutput_message(&data).unwrap() {
            PgOutputMessage::Relation(r) => {
                assert_eq!(r.oid, 1001);
                assert_eq!(r.namespace, "public");
                assert_eq!(r.name, "users");
                assert_eq!(r.columns.len(), 2);
                assert_eq!(r.columns[0].name, "id");
                assert!(r.columns[0].is_key());
                assert_eq!(r.columns[1].name, "name");
                assert!(!r.columns[1].is_key());
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_pgoutput_insert_message() {
        let data = build_insert(1001, &[Some("1"), Some("alice")]);
        match decode_pgoutput_message(&data).unwrap() {
            PgOutputMessage::Insert(i) => {
                assert_eq!(i.relation_oid, 1001);
                assert_eq!(i.new_tuple.len(), 2);
                assert_eq!(i.new_tuple[0], PgValue::Text("1".into()));
                assert_eq!(i.new_tuple[1], PgValue::Text("alice".into()));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_pgoutput_insert_with_null_column() {
        let data = build_insert(1001, &[Some("1"), None]);
        match decode_pgoutput_message(&data).unwrap() {
            PgOutputMessage::Insert(i) => {
                assert_eq!(i.new_tuple[1], PgValue::Null);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_pgoutput_update_message_with_old_tuple() {
        let data = build_update(
            1001,
            Some(&[Some("1"), Some("alice")]),
            &[Some("1"), Some("bob")],
        );
        match decode_pgoutput_message(&data).unwrap() {
            PgOutputMessage::Update(u) => {
                assert_eq!(u.relation_oid, 1001);
                assert!(u.old_tuple.is_some());
                let old = u.old_tuple.as_ref().unwrap();
                assert_eq!(old[1], PgValue::Text("alice".into()));
                assert_eq!(u.new_tuple[1], PgValue::Text("bob".into()));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_pgoutput_update_message_without_old_tuple() {
        let data = build_update(1001, None, &[Some("1"), Some("bob")]);
        match decode_pgoutput_message(&data).unwrap() {
            PgOutputMessage::Update(u) => {
                assert!(u.old_tuple.is_none());
                assert!(u.key_tuple.is_none());
                assert_eq!(u.new_tuple[0], PgValue::Text("1".into()));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_pgoutput_delete_message_with_key() {
        let data = build_delete(1001, &[Some("1"), None]);
        match decode_pgoutput_message(&data).unwrap() {
            PgOutputMessage::Delete(d) => {
                assert_eq!(d.relation_oid, 1001);
                assert!(d.key_tuple.is_some());
                let key = d.key_tuple.as_ref().unwrap();
                assert_eq!(key[0], PgValue::Text("1".into()));
                assert_eq!(key[1], PgValue::Null);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_pgoutput_unknown_message_type() {
        let data = vec![b'X'];
        match decode_pgoutput_message(&data).unwrap() {
            PgOutputMessage::Unknown(t) => assert_eq!(t, b'X'),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_pgoutput_rejects_empty_message() {
        let result = decode_pgoutput_message(&[]);
        assert!(matches!(result, Err(crate::core::Error::SourceError(_))));
    }

    #[test]
    fn decode_pgoutput_rejects_truncated_begin() {
        let truncated = &build_begin(1000, 0, 1)[..5]; // cut short
        let result = decode_pgoutput_message(truncated);
        assert!(result.is_err());
    }

    // ─── Pgoutput timestamp conversion ───────────────────────────────────────

    #[test]
    fn pg_timestamp_to_millis_at_pg_epoch() {
        // PG epoch = 2000-01-01 → Unix ms = 946_684_800_000
        let ms = super::pg_timestamp_to_millis(0);
        assert_eq!(ms, 946_684_800_000);
    }

    #[test]
    fn pg_timestamp_to_millis_handles_negative() {
        // Before PG epoch is clamped to 0
        let ms = super::pg_timestamp_to_millis(i64::MIN);
        assert_eq!(ms, 0);
    }

    // ─── format_pg_lsn round-trip ─────────────────────────────────────────────

    #[test]
    fn format_pg_lsn_round_trips_with_parse() {
        let original: u64 = (0x16_u64 << 32) | 0xB374D848;
        let formatted = super::format_pg_lsn(original);
        let parsed = super::parse_pg_lsn(&formatted).unwrap();
        assert_eq!(parsed, original);
    }

    // ─── Stream handle — pgoutput integration tests ───────────────────────────

    #[tokio::test]
    async fn stream_next_events_returns_insert_event() {
        const OID: u32 = 999;
        let provider = MockPgOutputProvider::new(vec![vec![
            xlog(
                800,
                build_relation(OID, "public", "users", &[("id", true), ("name", false)]),
            ),
            xlog(800, build_begin(1000, 0, 1)),
            xlog(900, build_insert(OID, &[Some("1"), Some("alice")])),
            xlog(1000, build_commit(900, 1100, 0)),
        ]]);
        let mut handle = make_stream_handle(0, provider);

        let events = handle.next_events(100).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op, crate::core::Operation::Insert);
        assert_eq!(events[0].table, "users");
        assert_eq!(
            events[0].after,
            Some(serde_json::json!({"id": "1", "name": "alice"}))
        );
        assert_eq!(events[0].primary_key, Some(vec!["id".to_string()]));
        // LSN position updated to the end LSN from COMMIT
        assert_eq!(handle.stream.lsn_position, 1100);
    }

    #[tokio::test]
    async fn stream_next_events_returns_update_event_with_before_after() {
        const OID: u32 = 999;
        let provider = MockPgOutputProvider::new(vec![vec![
            xlog(
                800,
                build_relation(OID, "public", "users", &[("id", true), ("name", false)]),
            ),
            xlog(800, build_begin(1000, 0, 2)),
            xlog(
                900,
                build_update(
                    OID,
                    Some(&[Some("1"), Some("alice")]),
                    &[Some("1"), Some("bob")],
                ),
            ),
            xlog(1000, build_commit(900, 1100, 0)),
        ]]);
        let mut handle = make_stream_handle(0, provider);

        let events = handle.next_events(100).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op, crate::core::Operation::Update);
        assert_eq!(
            events[0].before,
            Some(serde_json::json!({"id": "1", "name": "alice"}))
        );
        assert_eq!(
            events[0].after,
            Some(serde_json::json!({"id": "1", "name": "bob"}))
        );
    }

    #[tokio::test]
    async fn stream_next_events_returns_delete_event_with_before() {
        const OID: u32 = 999;
        let provider = MockPgOutputProvider::new(vec![vec![
            xlog(
                800,
                build_relation(OID, "public", "users", &[("id", true), ("name", false)]),
            ),
            xlog(800, build_begin(1000, 0, 3)),
            xlog(900, build_delete(OID, &[Some("1"), None])),
            xlog(1000, build_commit(900, 1100, 0)),
        ]]);
        let mut handle = make_stream_handle(0, provider);

        let events = handle.next_events(100).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op, crate::core::Operation::Delete);
        assert!(events[0].before.is_some());
        assert!(events[0].after.is_none());
    }

    #[tokio::test]
    async fn stream_next_events_times_out_when_provider_returns_empty() {
        let provider = MockPgOutputProvider::new(vec![]); // always empty
        let mut handle = make_stream_handle(100, provider);
        let events = handle.next_events(5).await.unwrap();
        assert!(events.is_empty());
        assert_eq!(handle.stream.lsn_position, 100);
    }

    #[tokio::test]
    async fn stream_next_events_returns_empty_on_zero_timeout() {
        let provider = MockPgOutputProvider::new(vec![]);
        let mut handle = make_stream_handle(100, provider);
        let events = handle.next_events(0).await.unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn stream_next_events_rejects_non_streaming_state() {
        let provider = MockPgOutputProvider::new(vec![]);
        let mut handle = PostgresStreamHandle::new(
            "postgres".into(),
            PostgresStream {
                slot_name: "slot".into(),
                publication_name: "pub".into(),
                lsn_position: 0,
                replication_status: StreamState::Starting,
            },
            Box::new(provider),
            super::MAX_EVENTS_PER_POLL,
            super::STREAM_POLL_INTERVAL_MS,
        );
        let result = handle.next_events(100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn stream_save_position_persists_commit_lsn() {
        const OID: u32 = 1;
        let provider = MockPgOutputProvider::new(vec![vec![
            xlog(100, build_relation(OID, "public", "t", &[("id", true)])),
            xlog(100, build_begin(200, 0, 5)),
            xlog(150, build_insert(OID, &[Some("1")])),
            xlog(200, build_commit(200, 250, 0)),
        ]]);
        let mut handle = make_stream_handle(0, provider);
        handle.next_events(50).await.unwrap();

        let mut checkpoint = InMemoryCheckpoint::default();
        handle.save_position(&mut checkpoint).await.unwrap();
        let offset = checkpoint.load().await.unwrap().unwrap();
        let restored = PostgresOffset::from_bytes(&offset.encode().unwrap()).unwrap();
        assert_eq!(restored.lsn, 250);
        assert_eq!(restored.slot_name, "slot");
    }

    #[tokio::test]
    async fn stream_transaction_metadata_populated_correctly() {
        const OID: u32 = 1;
        // PG epoch timestamp → Unix ms = 946_684_800_000
        let provider = MockPgOutputProvider::new(vec![vec![
            xlog(100, build_relation(OID, "public", "t", &[("id", true)])),
            xlog(100, build_begin(200, 0, 77)),
            xlog(150, build_insert(OID, &[Some("1")])),
            xlog(160, build_insert(OID, &[Some("2")])),
            xlog(200, build_commit(200, 300, 0)),
        ]]);
        let mut handle = make_stream_handle(0, provider);
        let events = handle.next_events(100).await.unwrap();

        assert_eq!(events.len(), 2);
        let tx0 = events[0].transaction.as_ref().unwrap();
        let tx1 = events[1].transaction.as_ref().unwrap();
        assert_eq!(tx0.tx_id, 77);
        assert_eq!(tx0.total_events, 2);
        assert_eq!(tx0.event_index, 0);
        assert_eq!(tx1.total_events, 2);
        assert_eq!(tx1.event_index, 1);
    }

    #[tokio::test]
    async fn stream_confirm_lsn_delegates_to_provider() {
        let provider = MockPgOutputProvider::new(vec![]);
        let lsn_store = provider.confirmed_lsn.clone();
        let mut handle = make_stream_handle(0, provider);
        handle.confirm_lsn(999).await.unwrap();
        assert_eq!(*lsn_store.lock().await, 999);
    }

    #[tokio::test]
    async fn stream_relation_map_persists_across_polls() {
        const OID: u32 = 5;
        // First batch: RELATION + first transaction.
        // Second batch: second transaction — no RELATION (schema already cached).
        let provider = MockPgOutputProvider::new(vec![
            vec![
                xlog(100, build_relation(OID, "public", "items", &[("id", true)])),
                xlog(100, build_begin(200, 0, 10)),
                xlog(150, build_insert(OID, &[Some("42")])),
                xlog(200, build_commit(200, 250, 0)),
            ],
            vec![
                // No RELATION — relation_map must still contain OID from first poll.
                xlog(250, build_begin(300, 0, 11)),
                xlog(280, build_insert(OID, &[Some("43")])),
                xlog(300, build_commit(300, 350, 0)),
            ],
        ]);
        let mut handle = make_stream_handle(0, provider);

        let first = handle.next_events(50).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].table, "items");

        // relation_map preserved: second poll decodes correctly without a new RELATION.
        let second = handle.next_events(50).await.unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].table, "items");
    }

    #[tokio::test]
    async fn stream_schema_qualified_table_name() {
        const OID: u32 = 7;
        let provider = MockPgOutputProvider::new(vec![vec![
            xlog(
                100,
                build_relation(OID, "myschema", "orders", &[("id", true)]),
            ),
            xlog(100, build_begin(200, 0, 20)),
            xlog(150, build_insert(OID, &[Some("1")])),
            xlog(200, build_commit(200, 300, 0)),
        ]]);
        let mut handle = make_stream_handle(0, provider);
        let events = handle.next_events(100).await.unwrap();
        assert_eq!(events[0].table, "myschema.orders");
        assert_eq!(events[0].schema, Some("myschema".to_string()));
    }

    #[tokio::test]
    async fn stream_emits_schema_change_on_relation_update() {
        const OID: u32 = 21;
        let provider = MockPgOutputProvider::new(vec![vec![
            xlog(
                100,
                build_relation(OID, "public", "users", &[("id", true), ("name", false)]),
            ),
            xlog(
                400,
                build_relation(
                    OID,
                    "public",
                    "users",
                    &[("id", true), ("name", false), ("email", false)],
                ),
            ),
        ]]);
        let mut handle = make_stream_handle(0, provider);

        let events = handle.next_events(100).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op, crate::core::Operation::SchemaChange);
        assert_eq!(events[0].source.offset, "0/00000190");
        assert_eq!(events[0].schema.as_deref(), Some("public"));
        assert_eq!(events[0].table, "users__ddl_events");

        let after = events[0].after.as_ref().expect("schema event payload");
        assert_eq!(after["ddl_type"], "ALTER_TABLE");
        assert_eq!(after["schema"], "public");
        assert_eq!(after["table"], "users");
    }

    #[tokio::test]
    async fn stream_large_transaction_handles_10k_events() {
        const OID: u32 = 42;
        let mut batch = vec![
            xlog(
                100,
                build_relation(OID, "public", "big_table", &[("id", true)]),
            ),
            xlog(100, build_begin(1_000, 0, 555)),
        ];
        for i in 0..10_000_u32 {
            batch.push(xlog(
                200 + u64::from(i),
                build_insert(OID, &[Some(&i.to_string())]),
            ));
        }
        batch.push(xlog(20_500, build_commit(20_000, 21_000, 0)));

        let provider = MockPgOutputProvider::new(vec![batch]);
        let mut handle = make_stream_handle(0, provider);
        let events = handle.next_events(100).await.unwrap();

        assert_eq!(events.len(), 10_000);
        assert_eq!(events[0].table, "big_table");
        assert_eq!(events[0].transaction.as_ref().map(|t| t.tx_id), Some(555));
        assert_eq!(
            events[0].transaction.as_ref().map(|t| t.total_events),
            Some(10_000)
        );
        assert_eq!(
            events
                .last()
                .and_then(|e| e.transaction.as_ref())
                .map(|t| t.event_index),
            Some(9_999)
        );
    }

    #[test]
    fn pgoutput_poll_error_maps_dead_slot_guidance() {
        let err =
            map_pgoutput_poll_error("slot1", "ERROR: required WAL segment has been removed");
        let msg = err.to_string();
        assert!(msg.contains("stale or dead"));
        assert!(msg.contains("slot1"));
    }

    // ─── Existing tests kept ──────────────────────────────────────────────────

    #[test]
    fn parse_pg_lsn_supports_valid_hex_format() {
        let parsed = super::parse_pg_lsn("16/B374D848").unwrap();
        assert_eq!(parsed, (0x16_u64 << 32) | 0xB374D848);
    }

    #[test]
    fn parse_pg_lsn_rejects_invalid_format() {
        let error = super::parse_pg_lsn("invalid").unwrap_err();
        assert!(matches!(error, crate::core::Error::SourceError(_)));
    }

    #[test]
    fn parse_table_reference_supports_quoted_identifiers_and_rejects_injection_like_inputs() {
        assert!(super::parse_table_reference("public.users").is_ok());
        let quoted = super::parse_table_reference("public.\"users.with.dot\"").unwrap();
        assert_eq!(quoted.0, "public");
        assert_eq!(quoted.1, "users.with.dot");

        let quoted_schema = super::parse_table_reference("\"sales-team\".users").unwrap();
        assert_eq!(quoted_schema.0, "sales-team");
        assert_eq!(quoted_schema.1, "users");

        assert!(super::parse_table_reference("users;DROP TABLE audit").is_err());
        assert!(super::parse_table_reference("public.users --comment").is_err());
        assert!(super::parse_table_reference("public.\"unterminated").is_err());
    }

    #[test]
    fn decode_stream_resume_lsn_uses_checkpoint_value() {
        let offset = PostgresOffset {
            lsn: 4242,
            slot_name: "slot".into(),
        };
        let lsn = super::decode_stream_resume_lsn("postgres", "slot", &offset).unwrap();
        assert_eq!(lsn, 4242);
    }

    #[test]
    fn stream_resume_alignment_accepts_exact_match() {
        assert_eq!(
            super::reconcile_stream_resume_lsn(42, 42, "slot").unwrap(),
            42
        );
    }

    #[test]
    fn stream_resume_alignment_accepts_checkpoint_behind_slot() {
        assert_eq!(
            super::reconcile_stream_resume_lsn(41, 42, "slot").unwrap(),
            41
        );
    }

    #[test]
    fn stream_resume_alignment_rejects_checkpoint_ahead_of_slot() {
        let error = super::reconcile_stream_resume_lsn(43, 42, "slot").unwrap_err();
        assert!(matches!(error, crate::core::Error::CheckpointError(_)));
    }

    #[test]
    fn config_validation_rejects_empty_fields() {
        let config = PostgresSourceConfig::default();
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_validation_rejects_zero_stream_tuning() {
        let mut config = PostgresSourceConfig {
            host: "localhost".into(),
            port: 5432,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            replication_slot_name: "slot".into(),
            publication_name: "pub".into(),
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
    fn debug_redacts_password() {
        let config = PostgresSourceConfig {
            host: "localhost".into(),
            port: 5432,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            replication_slot_name: "slot".into(),
            publication_name: "pub".into(),
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
    fn validation_accepts_env_backed_passwords() {
        let config = PostgresSourceConfig {
            host: "localhost".into(),
            port: 5432,
            user: "cdc".into(),
            password: SecretString::from_env("HOME"),
            database: "app".into(),
            replication_slot_name: "slot".into(),
            publication_name: "pub".into(),
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        };

        assert!(config.validate().is_ok());
        assert!(config.build_connect_config().is_ok());
    }

    #[tokio::test]
    async fn source_type_is_postgres() {
        let connection = PostgresConnection::new(PostgresSourceConfig {
            host: "localhost".into(),
            port: 5432,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            replication_slot_name: "slot".into(),
            publication_name: "pub".into(),
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        });

        assert_eq!(connection.source_type(), "postgres");
        let capabilities = connection.capabilities();
        assert!(capabilities.snapshot);
        assert!(capabilities.handoff);
        assert!(capabilities.heartbeat);
        assert!(capabilities.ddl_capture);
    }

    #[tokio::test]
    async fn validation_creates_replication_slot_when_missing() {
        let config = PostgresSourceConfig {
            host: "localhost".into(),
            port: 5432,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            replication_slot_name: "slot".into(),
            publication_name: "pub".into(),
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        };
        let backend = MockValidationBackend {
            slot_exists: false,
            publication_exists: true,
            has_replication_privilege: true,
            create_called: Arc::new(AtomicBool::new(false)),
            ..Default::default()
        };

        validate_with_backend(&config, &backend)
            .await
            .unwrap();
        assert!(backend.create_called.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn validation_rejects_missing_publication() {
        let config = PostgresSourceConfig {
            host: "localhost".into(),
            port: 5432,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            replication_slot_name: "slot".into(),
            publication_name: "pub".into(),
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        };
        let backend = MockValidationBackend {
            slot_exists: true,
            publication_exists: false,
            has_replication_privilege: true,
            ..Default::default()
        };

        let error = validate_with_backend(&config, &backend)
            .await
            .unwrap_err();
        assert!(matches!(error, crate::core::Error::SourceError(_)));
    }

    #[tokio::test]
    async fn validation_rejects_missing_replication_privilege() {
        let config = PostgresSourceConfig {
            host: "localhost".into(),
            port: 5432,
            user: "cdc".into(),
            password: "secret".into(),
            database: "app".into(),
            replication_slot_name: "slot".into(),
            publication_name: "pub".into(),
            transport: TransportConfig::tls(),
            conn_timeout_secs: 30,
            stream_poll_interval_ms: STREAM_POLL_INTERVAL_MS,
            max_events_per_poll: MAX_EVENTS_PER_POLL,
            ..Default::default()
        };
        let backend = MockValidationBackend {
            slot_exists: true,
            publication_exists: true,
            has_replication_privilege: false,
            ..Default::default()
        };

        let error = validate_with_backend(&config, &backend)
            .await
            .unwrap_err();
        assert!(matches!(error, crate::core::Error::SourceError(_)));
    }

    #[tokio::test]
    async fn snapshot_handle_chunks_rows_and_finishes_consistently() {
        let snapshot = super::PostgresSnapshot {
            tables: vec![TableSnapshot {
                table: "users".into(),
                total_rows: 3,
                rows_processed: 0,
                cursor_position: None,
                is_complete: false,
            }],
            snapshot_id: "snap-1".into(),
            snapshot_start_ts: 1,
            snapshot_end_ts: 0,
        };
        let mut handle = PostgresSnapshotHandle::new(
            "postgres".into(),
            snapshot,
            vec![TableSnapshotState {
                snapshot: TableSnapshot {
                    table: "users".into(),
                    total_rows: 3,
                    rows_processed: 0,
                    cursor_position: None,
                    is_complete: false,
                },
                rows: vec![
                    serde_json::json!({"id": 1}),
                    serde_json::json!({"id": 2}),
                    serde_json::json!({"id": 3}),
                ],
                next_row: 0,
                live_query: false,
                primary_key_columns: vec![],
                primary_key_types: vec![],
            }],
            None,
            false,
            0,
        );

        let first = handle.next_chunk(2).await.unwrap();
        assert_eq!(first.len(), 2);
        let second = handle.next_chunk(2).await.unwrap();
        assert_eq!(second.len(), 1);
        let none = handle.next_chunk(2).await.unwrap();
        assert!(none.is_empty());

        let end = handle.finish().await.unwrap();
        assert!(end.snapshot_end_ts > 0);
    }

    #[tokio::test]
    async fn snapshot_checkpoint_persists_cursor_state() {
        let snapshot = super::PostgresSnapshot {
            tables: vec![TableSnapshot {
                table: "users".into(),
                total_rows: 1,
                rows_processed: 0,
                cursor_position: None,
                is_complete: false,
            }],
            snapshot_id: "snap-1".into(),
            snapshot_start_ts: 1,
            snapshot_end_ts: 0,
        };
        let mut handle = PostgresSnapshotHandle::new(
            "postgres".into(),
            snapshot,
            vec![TableSnapshotState {
                snapshot: TableSnapshot {
                    table: "users".into(),
                    total_rows: 1,
                    rows_processed: 0,
                    cursor_position: None,
                    is_complete: false,
                },
                rows: vec![serde_json::json!({"id": 1})],
                next_row: 0,
                live_query: false,
                primary_key_columns: vec![],
                primary_key_types: vec![],
            }],
            None,
            false,
            0,
        );

        handle.next_chunk(1).await.unwrap();
        let mut checkpoint = InMemoryCheckpoint::default();
        handle.checkpoint(&mut checkpoint, 7).await.unwrap();
        assert!(checkpoint.load().await.unwrap().is_some());
    }

    #[test]
    fn snapshot_live_query_cursor_validation_accepts_json_pk_values() {
        assert!(PostgresSnapshotHandle::decode_pk_cursor("[\"1\"]", 1).is_ok());
        assert!(PostgresSnapshotHandle::decode_pk_cursor("[\"42\",\"9\"]", 2).is_ok());
        assert!(PostgresSnapshotHandle::decode_pk_cursor("12", 1).is_err());
        assert!(PostgresSnapshotHandle::decode_pk_cursor("[\"1\"]", 2).is_err());
        assert!(PostgresSnapshotHandle::decode_pk_cursor("[]", 1).is_err());
    }

    #[test]
    fn snapshot_resume_rejects_malformed_pk_keyset_cursor() {
        let snapshot = super::PostgresSnapshot {
            tables: vec![TableSnapshot {
                table: "users".into(),
                total_rows: 10,
                rows_processed: 0,
                cursor_position: None,
                is_complete: false,
            }],
            snapshot_id: "snap-1".into(),
            snapshot_start_ts: 1,
            snapshot_end_ts: 0,
        };
        let handle = PostgresSnapshotHandle::new(
            "postgres".into(),
            snapshot,
            vec![TableSnapshotState {
                snapshot: TableSnapshot {
                    table: "users".into(),
                    total_rows: 10,
                    rows_processed: 0,
                    cursor_position: None,
                    is_complete: false,
                },
                rows: vec![],
                next_row: 0,
                live_query: true,
                primary_key_columns: vec!["id".into()],
                primary_key_types: vec!["bigint".into()],
            }],
            None,
            false,
            0,
        );

        let state = super::SnapshotCheckpointState {
            snapshot_id: "snap-1".into(),
            snapshot_start_ts: 1,
            snapshot_end_ts: 0,
            snapshot_watermark: 10,
            current_table: 0,
            next_chunk_index: 2,
            tables: vec![TableSnapshot {
                table: "users".into(),
                total_rows: 10,
                rows_processed: 5,
                cursor_position: Some("5".into()),
                is_complete: false,
            }],
        };

        let payload = serde_json::to_vec(&state).unwrap();
        let error = match handle.resume_from_checkpoint_payload(&payload) {
            Ok(_) => {
                panic!("resume should reject malformed keyset cursor for live query snapshots")
            }
            Err(error) => error,
        };
        match error {
            crate::core::Error::CheckpointError(message) => {
                assert!(message.contains("expected JSON array of primary key values"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn snapshot_empty_table_emits_no_rows() {
        let snapshot = super::PostgresSnapshot {
            tables: vec![TableSnapshot {
                table: "users".into(),
                total_rows: 0,
                rows_processed: 0,
                cursor_position: None,
                is_complete: true,
            }],
            snapshot_id: "snap-1".into(),
            snapshot_start_ts: 1,
            snapshot_end_ts: 1,
        };
        let mut handle = PostgresSnapshotHandle::new(
            "postgres".into(),
            snapshot,
            vec![TableSnapshotState {
                snapshot: TableSnapshot {
                    table: "users".into(),
                    total_rows: 0,
                    rows_processed: 0,
                    cursor_position: None,
                    is_complete: true,
                },
                rows: vec![],
                next_row: 0,
                live_query: false,
                primary_key_columns: vec![],
                primary_key_types: vec![],
            }],
            None,
            false,
            0,
        );

        assert!(handle.next_chunk(10).await.unwrap().is_empty());
        assert!(handle.finish().await.unwrap().snapshot_end_ts > 0);
    }

    #[tokio::test]
    async fn snapshot_offsets_do_not_repeat_across_chunks() {
        let snapshot = super::PostgresSnapshot {
            tables: vec![TableSnapshot {
                table: "users".into(),
                total_rows: 4,
                rows_processed: 0,
                cursor_position: None,
                is_complete: false,
            }],
            snapshot_id: "snap-1".into(),
            snapshot_start_ts: 1,
            snapshot_end_ts: 0,
        };
        let mut handle = PostgresSnapshotHandle::new(
            "postgres".into(),
            snapshot,
            vec![TableSnapshotState {
                snapshot: TableSnapshot {
                    table: "users".into(),
                    total_rows: 4,
                    rows_processed: 0,
                    cursor_position: None,
                    is_complete: false,
                },
                rows: vec![
                    serde_json::json!({"id": 1}),
                    serde_json::json!({"id": 2}),
                    serde_json::json!({"id": 3}),
                    serde_json::json!({"id": 4}),
                ],
                next_row: 0,
                live_query: false,
                primary_key_columns: vec![],
                primary_key_types: vec![],
            }],
            None,
            false,
            0,
        );

        let mut seen = std::collections::HashSet::new();
        for chunk in [2_usize, 1_usize, 10_usize] {
            for event in handle.next_chunk(chunk).await.unwrap() {
                assert!(seen.insert(event.source.offset));
            }
        }
        assert_eq!(seen.len(), 4);
        assert!(handle.finish().await.is_ok());
    }

    #[tokio::test]
    async fn snapshot_finish_allows_row_count_drift_for_live_query_tables() {
        let snapshot = super::PostgresSnapshot {
            tables: vec![TableSnapshot {
                table: "users".into(),
                total_rows: 10,
                rows_processed: 0,
                cursor_position: None,
                is_complete: false,
            }],
            snapshot_id: "snap-1".into(),
            snapshot_start_ts: 1,
            snapshot_end_ts: 0,
        };
        let mut handle = PostgresSnapshotHandle::new(
            "postgres".into(),
            snapshot,
            vec![TableSnapshotState {
                snapshot: TableSnapshot {
                    table: "users".into(),
                    total_rows: 10,
                    rows_processed: 0,
                    cursor_position: None,
                    is_complete: false,
                },
                rows: vec![
                    serde_json::json!({"id": 1}),
                    serde_json::json!({"id": 2}),
                    serde_json::json!({"id": 3}),
                ],
                next_row: 0,
                live_query: true,
                primary_key_columns: vec!["id".into()],
                primary_key_types: vec!["bigint".into()],
            }],
            None,
            false,
            0,
        );

        let events = handle.next_chunk(10).await.unwrap();
        assert_eq!(events.len(), 3);
        assert!(handle.finish().await.is_ok());
    }

    #[test]
    fn handoff_watermarks_accept_equal_or_forward_progress() {
        let equal = super::postgres_handoff_stream_watermark_gap(100, 100).unwrap();
        assert_eq!(equal, 0);

        let overlap = super::postgres_handoff_stream_watermark_gap(100, 160).unwrap();
        assert_eq!(overlap, 60);
    }

    #[test]
    fn handoff_watermarks_reject_stream_behind_snapshot() {
        let err = super::postgres_handoff_stream_watermark_gap(200, 199).unwrap_err();
        assert!(matches!(err, crate::core::Error::SourceError(_)));
    }

    #[test]
    fn handoff_snapshot_only_returns_no_stream_start() {
        let result = super::postgres_handoff_result(Some(11), Some(10), None).unwrap();
        assert_eq!(result.snapshot_end_ts, Some(11));
        assert_eq!(result.stream_start_ts, None);
        assert_eq!(result.overlap_events_dropped, 0);
        assert_eq!(result.stream_watermark_gap, None);
    }

    #[test]
    fn handoff_stream_only_returns_no_snapshot_end() {
        let result = super::postgres_handoff_result(None, None, Some(10)).unwrap();
        assert_eq!(result.snapshot_end_ts, None);
        assert!(result.stream_start_ts.is_some());
        assert_eq!(result.overlap_events_dropped, 0);
        assert_eq!(result.stream_watermark_gap, None);
    }

    #[test]
    fn handoff_overlap_reports_watermark_gap_not_event_count() {
        let result = super::postgres_handoff_result(Some(25), Some(100), Some(160)).unwrap();
        assert_eq!(result.snapshot_end_ts, Some(25));
        assert_eq!(result.overlap_events_dropped, 0);
        assert_eq!(result.stream_watermark_gap, Some(60));
        assert!(result.stream_start_ts.is_some());
    }
}
