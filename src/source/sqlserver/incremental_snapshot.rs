//! Incremental (non-blocking) snapshot for the SQL Server connector.
//!
//! # DBLog watermark pattern (LSN-based)
//!
//! For each chunk the coordinator:
//! 1. Captures a **low-watermark** LSN (`fn_cdc_get_max_lsn`) before the `SELECT`.
//! 2. Reads `chunk_size` rows (keyset pagination with OR-of-AND seek, `READ COMMITTED`).
//! 3. Captures a **high-watermark** LSN after the `SELECT`.
//! 4. Polls the CDC stream; records override PKs for events on the snapshotted table
//!    with LSN in `(low_wm, high_wm]`.
//! 5. Once the stream advances past `high_wm`, emits snapshot `Read` events for
//!    chunk rows whose PK was **not** overridden.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{
    checkpoint::Checkpoint,
    core::{Error, Event, Operation, Result, SnapshotMetadata, SourceMetadata,
           EVENT_ENVELOPE_VERSION},
    source::{IncrementalSnapshotConfig, StreamHandle},
    source::helpers::now_millis,
};

use super::{
    SqlServerSourceConfig, SqlClient,
    parser::{
        build_snapshot_fetch_sql, compare_lsn, lsn_bytes_to_hex, lsn_from_source_offset,
        parse_schema_table, qualified_table_name,
    },
    query::connect_client,
};

// ─── Watermark helpers ────────────────────────────────────────────────────────

/// Query `sys.fn_cdc_get_max_lsn()` and return it as a 10-byte array.
async fn query_max_lsn(client: &mut SqlClient) -> Result<[u8; 10]> {
    let rows = client
        .query(
            "SELECT sys.fn_varbintohexstr(sys.fn_cdc_get_max_lsn())",
            &[],
        )
        .await
        .map_err(|e| {
            Error::SourceError(format!(
                "incremental snapshot: max LSN query failed: {e}"
            ))
        })?
        .into_first_result()
        .await
        .map_err(|e| {
            Error::SourceError(format!(
                "incremental snapshot: max LSN decode failed: {e}"
            ))
        })?;

    let hex: &str = rows
        .first()
        .and_then(|row| row.get(0))
        .ok_or_else(|| {
            Error::SourceError("incremental snapshot: max LSN returned no row".into())
        })?;

    super::parser::lsn_hex_to_bytes(hex)
}

fn lsn_from_event(event: &Event) -> Option<[u8; 10]> {
    lsn_from_source_offset(&event.source.offset)
}

// ─── Cursor param helpers ─────────────────────────────────────────────────────

enum CursorParam {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl CursorParam {
    fn bind(&self, query: &mut tiberius::Query) {
        match self {
            Self::Bool(v) => query.bind(*v),
            Self::Int(v) => query.bind(*v),
            Self::Float(v) => query.bind(*v),
            Self::Text(v) => query.bind(v.clone()),
        }
    }
}

fn json_value_to_cursor_param(value: &serde_json::Value) -> Result<CursorParam> {
    match value {
        serde_json::Value::Null => Err(Error::CheckpointError(
            "sqlserver incremental snapshot cursor does not support NULL pk values".into(),
        )),
        serde_json::Value::Bool(b) => Ok(CursorParam::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(v) = n.as_i64() {
                Ok(CursorParam::Int(v))
            } else if let Some(v) = n.as_f64() {
                Ok(CursorParam::Float(v))
            } else {
                Err(Error::CheckpointError(
                    "sqlserver incremental snapshot: unsupported numeric pk value".into(),
                ))
            }
        }
        serde_json::Value::String(s) => Ok(CursorParam::Text(s.clone())),
        _ => Err(Error::CheckpointError(
            "sqlserver incremental snapshot: only scalar pk values are supported".into(),
        )),
    }
}

// ─── Per-table metadata ───────────────────────────────────────────────────────

struct TableSpec {
    schema: String,
    name: String,
    /// `[schema].[table]`
    qualified: String,
    pk_columns: Vec<String>,
    column_names: Vec<String>,
    /// Keyset cursor: last PK values seen in the previous chunk.
    pk_cursor: Option<Vec<serde_json::Value>>,
    is_complete: bool,
    chunks_emitted: u32,
    rows_emitted: u64,
}

// ─── Phase state machine ──────────────────────────────────────────────────────

enum Phase {
    ChunkPrepare { table_idx: usize },
    ChunkCollect {
        table_idx: usize,
        low_wm: [u8; 10],
        high_wm: [u8; 10],
        chunk_rows: Vec<(String, Event)>,
        override_pks: HashSet<String>,
    },
    ChunkEmit {
        table_idx: usize,
        events: VecDeque<Event>,
    },
    Done,
}

// ─── Public handle ────────────────────────────────────────────────────────────

/// Non-blocking incremental snapshot handle for SQL Server.
///
/// Returned by [`SqlServerConnection::start_incremental_snapshot`].
pub struct SqlServerIncrementalSnapshotHandle {
    inner: Box<dyn StreamHandle>,
    /// Dedicated READ COMMITTED connection used for watermark queries and chunk SELECTs.
    client: Arc<Mutex<SqlClient>>,
    tables: Vec<TableSpec>,
    phase: Phase,
    chunk_size: usize,
    source_name: String,
    snapshot_id: String,
}

impl SqlServerIncrementalSnapshotHandle {
    /// Construct a new handle, eagerly loading metadata for every table.
    pub(super) async fn new(
        inner: Box<dyn StreamHandle>,
        config: SqlServerSourceConfig,
        cfg: IncrementalSnapshotConfig,
        source_name: String,
    ) -> Result<Self> {
        let mut client = connect_client(&config).await?;

        let mut tables = Vec::with_capacity(cfg.tables.len());
        for table_ref in &cfg.tables {
            let (schema, name) = parse_schema_table(table_ref)?;

            let pk_columns = load_pk_columns(&mut client, &schema, &name).await?;
            if pk_columns.is_empty() {
                return Err(Error::ConfigError(format!(
                    "incremental snapshot: table '{schema}.{name}' must have a primary key"
                )));
            }
            let column_names = load_all_columns(&mut client, &schema, &name).await?;
            let qualified = qualified_table_name(&schema, &name);
            tables.push(TableSpec {
                schema,
                name,
                qualified,
                pk_columns,
                column_names,
                pk_cursor: None,
                is_complete: false,
                chunks_emitted: 0,
                rows_emitted: 0,
            });
        }

        let phase = if tables.is_empty() {
            Phase::Done
        } else {
            Phase::ChunkPrepare { table_idx: 0 }
        };

        let snapshot_id = format!("incremental-sqlserver-{}", now_millis());

        Ok(Self {
            inner,
            client: Arc::new(Mutex::new(client)),
            tables,
            phase,
            chunk_size: cfg.chunk_size.max(1),
            source_name,
            snapshot_id,
        })
    }

    // ─── Metadata helpers ─────────────────────────────────────────────────────

    // ─── Chunk fetch ──────────────────────────────────────────────────────────

    /// Execute a keyset-paginated chunk SELECT.
    /// Returns `(pk_json_values, row_json)` pairs.
    async fn fetch_chunk(
        &self,
        table_idx: usize,
    ) -> Result<Vec<(Vec<serde_json::Value>, serde_json::Value)>> {
        let table = &self.tables[table_idx];
        let has_cursor = table.pk_cursor.is_some();

        // limit param comes after cursor params (if any)
        let cursor_param_count = if has_cursor { table.pk_columns.len() } else { 0 };
        let limit_param_idx = cursor_param_count + 1;

        let sql = build_snapshot_fetch_sql(
            &table.qualified,
            &table.pk_columns,
            &table.column_names,
            limit_param_idx,
            has_cursor,
        );

        let limit_i32 = i32::try_from(self.chunk_size.min(i32::MAX as usize)).unwrap_or(i32::MAX);

        let mut client = self.client.lock().await;

        let mut query = tiberius::Query::new(&sql);
        if let Some(cursor) = &table.pk_cursor {
            for value in cursor {
                let param = json_value_to_cursor_param(value)?;
                param.bind(&mut query);
            }
        }
        query.bind(limit_i32);

        let result_stream = query.query(&mut *client).await.map_err(|e| {
            Error::SourceError(format!(
                "incremental snapshot: chunk SELECT failed for '{}': {e}",
                table.qualified
            ))
        })?;

        let rows = result_stream
            .into_first_result()
            .await
            .map_err(|e| {
                Error::SourceError(format!(
                    "incremental snapshot: chunk SELECT decode failed for '{}': {e}",
                    table.qualified
                ))
            })?;

        let table = &self.tables[table_idx];
        let mut decoded = Vec::with_capacity(rows.len());
        for row in &rows {
            // cursor_json: FOR JSON PATH string of PK columns
            let cursor_json_str: &str = row.get(0).ok_or_else(|| {
                Error::SourceError(format!(
                    "incremental snapshot: missing cursor_json for '{}'",
                    table.qualified
                ))
            })?;
            // row_json: FOR JSON PATH string of all columns
            let row_json_str: &str = row.get(1).ok_or_else(|| {
                Error::SourceError(format!(
                    "incremental snapshot: missing row_json for '{}'",
                    table.qualified
                ))
            })?;

            // FOR JSON PATH with WITHOUT_ARRAY_WRAPPER returns a single JSON object
            let cursor_obj: serde_json::Value =
                serde_json::from_str(cursor_json_str).map_err(|e| {
                    Error::SerializationError(format!(
                        "incremental snapshot: cursor_json parse failed for '{}': {e}",
                        table.qualified
                    ))
                })?;
            let row_obj: serde_json::Value =
                serde_json::from_str(row_json_str).map_err(|e| {
                    Error::SerializationError(format!(
                        "incremental snapshot: row_json parse failed for '{}': {e}",
                        table.qualified
                    ))
                })?;

            // Extract PK values in column order from the cursor object
            let pk_json: Vec<serde_json::Value> = table
                .pk_columns
                .iter()
                .map(|col| {
                    cursor_obj
                        .get(col)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect();

            if pk_json.iter().any(|v| v.is_null()) {
                return Err(Error::SourceError(format!(
                    "incremental snapshot: NULL primary-key column for '{}'",
                    table.qualified
                )));
            }

            decoded.push((pk_json, row_obj));
        }
        Ok(decoded)
    }

    // ─── PK fingerprint helpers ───────────────────────────────────────────────

    fn pk_fingerprint(table: &str, pk_values: &[serde_json::Value]) -> String {
        format!(
            "{}|{}",
            table,
            serde_json::to_string(pk_values).unwrap_or_else(|_| pk_values
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(","))
        )
    }

    fn extract_event_pk_fingerprint(event: &Event) -> Option<String> {
        let pk_cols = event.primary_key.as_ref()?;
        if pk_cols.is_empty() {
            return None;
        }
        let payload = event.after.as_ref().or(event.before.as_ref())?;
        let values: Vec<serde_json::Value> = pk_cols
            .iter()
            .map(|col| payload.get(col).cloned().unwrap_or(serde_json::Value::Null))
            .collect();
        Some(Self::pk_fingerprint(&event.table, &values))
    }

    // ─── Snapshot event builder ───────────────────────────────────────────────

    fn build_snapshot_event(
        &self,
        table_idx: usize,
        pk_values: &[serde_json::Value],
        json: serde_json::Value,
        chunk_index: u32,
    ) -> Event {
        let table = &self.tables[table_idx];
        let now = now_millis();
        Event {
            before: None,
            after: Some(json),
            op: Operation::Read,
            source: SourceMetadata {
                source_name: self.source_name.clone(),
                offset: format!(
                    "incremental:{}:{}",
                    table.qualified,
                    Self::pk_fingerprint(&table.name, pk_values)
                ),
                timestamp: now,
            },
            ts: now,
            schema: Some(table.schema.clone()),
            table: table.name.clone(),
            primary_key: Some(table.pk_columns.clone()),
            snapshot: Some(SnapshotMetadata {
                snapshot_id: self.snapshot_id.clone(),
                chunk_index,
                is_last_chunk: false,
            }),
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    // ─── Phase: ChunkPrepare ──────────────────────────────────────────────────

    async fn drive_chunk_prepare(&mut self) -> Result<()> {
        let table_idx = match self.phase {
            Phase::ChunkPrepare { table_idx } => table_idx,
            _ => return Ok(()),
        };

        let effective_idx = (table_idx..self.tables.len())
            .find(|&i| !self.tables[i].is_complete);
        let table_idx = match effective_idx {
            Some(idx) => idx,
            None => {
                self.phase = Phase::Done;
                return Ok(());
            }
        };

        // Low watermark
        let low_wm = {
            let mut client = self.client.lock().await;
            query_max_lsn(&mut client).await?
        };

        // Read the chunk
        let rows = self.fetch_chunk(table_idx).await?;

        // High watermark
        let high_wm = {
            let mut client = self.client.lock().await;
            query_max_lsn(&mut client).await?
        };

        if rows.is_empty() {
            self.tables[table_idx].is_complete = true;
            tracing::debug!(
                target: "cdc_rs::source::sqlserver::incremental_snapshot",
                table = %self.tables[table_idx].qualified,
                chunks = self.tables[table_idx].chunks_emitted,
                rows = self.tables[table_idx].rows_emitted,
                "incremental snapshot: sqlserver table complete",
            );
            let next = (table_idx + 1..self.tables.len())
                .find(|&i| !self.tables[i].is_complete);
            self.phase = match next {
                Some(idx) => Phase::ChunkPrepare { table_idx: idx },
                None => Phase::Done,
            };
            return Ok(());
        }

        let last_pk = rows.last().map(|(pk, _)| pk.clone()).unwrap_or_default();
        self.tables[table_idx].pk_cursor = Some(last_pk);

        let chunk_index = self.tables[table_idx].chunks_emitted;
        let chunk_rows: Vec<(String, Event)> = rows
            .into_iter()
            .map(|(pk_values, json)| {
                let fp = Self::pk_fingerprint(&self.tables[table_idx].name, &pk_values);
                let event = self.build_snapshot_event(table_idx, &pk_values, json, chunk_index);
                (fp, event)
            })
            .collect();

        tracing::debug!(
            target: "cdc_rs::source::sqlserver::incremental_snapshot",
            table = %self.tables[table_idx].qualified,
            chunk = chunk_index,
            rows = chunk_rows.len(),
            low_wm = lsn_bytes_to_hex(&low_wm),
            high_wm = lsn_bytes_to_hex(&high_wm),
            "incremental snapshot: sqlserver chunk read, entering collect phase",
        );

        self.phase = Phase::ChunkCollect {
            table_idx,
            low_wm,
            high_wm,
            chunk_rows,
            override_pks: HashSet::new(),
        };
        Ok(())
    }

    // ─── Phase: ChunkCollect ──────────────────────────────────────────────────

    async fn stream_passed_high_wm(
        &self,
        stream_events: &[Event],
        max_batch_lsn: Option<&[u8; 10]>,
        high_wm: &[u8; 10],
    ) -> Result<bool> {
        if let Some(lsn) = max_batch_lsn {
            if compare_lsn(lsn, high_wm).is_gt() {
                return Ok(true);
            }
        }
        if stream_events.is_empty() {
            let current = {
                let mut client = self.client.lock().await;
                query_max_lsn(&mut client).await?
            };
            return Ok(compare_lsn(&current, high_wm).is_gt());
        }
        Ok(false)
    }

    fn finalize_collect(&mut self) {
        let (table_idx, chunk_rows, override_pks) = match &self.phase {
            Phase::ChunkCollect {
                table_idx,
                chunk_rows,
                override_pks,
                ..
            } => (*table_idx, chunk_rows.clone(), override_pks.clone()),
            _ => return,
        };

        let merged: VecDeque<Event> = chunk_rows
            .into_iter()
            .filter(|(fp, _)| !override_pks.contains(fp))
            .map(|(_, event)| event)
            .collect();

        let emitted = merged.len() as u64;
        self.tables[table_idx].chunks_emitted += 1;
        self.tables[table_idx].rows_emitted += emitted;

        tracing::debug!(
            target: "cdc_rs::source::sqlserver::incremental_snapshot",
            table = %self.tables[table_idx].qualified,
            chunk = self.tables[table_idx].chunks_emitted,
            emitted,
            suppressed = override_pks.len(),
            "incremental snapshot: sqlserver chunk merged, entering emit phase",
        );

        self.phase = Phase::ChunkEmit {
            table_idx,
            events: merged,
        };
    }
}

// ─── Table metadata queries ───────────────────────────────────────────────────

async fn load_pk_columns(
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
        .map_err(|e| {
            Error::SourceError(format!(
                "incremental snapshot: PK query failed for '{schema}.{table}': {e}"
            ))
        })?
        .into_first_result()
        .await
        .map_err(|e| {
            Error::SourceError(format!(
                "incremental snapshot: PK decode failed for '{schema}.{table}': {e}"
            ))
        })?;

    Ok(rows
        .into_iter()
        .filter_map(|row| row.get::<&str, _>(0).map(ToOwned::to_owned))
        .collect())
}

async fn load_all_columns(
    client: &mut SqlClient,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT COLUMN_NAME \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2 \
             ORDER BY ORDINAL_POSITION",
            &[&schema, &table],
        )
        .await
        .map_err(|e| {
            Error::SourceError(format!(
                "incremental snapshot: columns query failed for '{schema}.{table}': {e}"
            ))
        })?
        .into_first_result()
        .await
        .map_err(|e| {
            Error::SourceError(format!(
                "incremental snapshot: columns decode failed for '{schema}.{table}': {e}"
            ))
        })?;

    Ok(rows
        .into_iter()
        .filter_map(|row| row.get::<&str, _>(0).map(ToOwned::to_owned))
        .collect())
}

// ─── StreamHandle impl ────────────────────────────────────────────────────────

#[async_trait]
impl StreamHandle for SqlServerIncrementalSnapshotHandle {
    async fn next_events(&mut self, timeout_ms: u64) -> Result<Vec<Event>> {
        loop {
            match &self.phase {
                Phase::Done => {
                    return self.inner.next_events(timeout_ms).await;
                }

                Phase::ChunkEmit { .. } => {
                    let (table_idx, done) = match &mut self.phase {
                        Phase::ChunkEmit { table_idx, events } => {
                            let batch_size = events.len().min(1_000);
                            if batch_size > 0 {
                                let batch: Vec<Event> = events.drain(..batch_size).collect();
                                return Ok(batch);
                            }
                            (*table_idx, true)
                        }
                        _ => unreachable!(),
                    };
                    if done {
                        self.phase = Phase::ChunkPrepare { table_idx };
                    }
                }

                Phase::ChunkPrepare { .. } => {
                    self.drive_chunk_prepare().await?;
                }

                Phase::ChunkCollect { .. } => {
                    let stream_events = self.inner.next_events(timeout_ms.min(100)).await?;

                    let (table_idx, low_wm, high_wm) = match &self.phase {
                        Phase::ChunkCollect {
                            table_idx,
                            low_wm,
                            high_wm,
                            ..
                        } => (*table_idx, *low_wm, *high_wm),
                        _ => unreachable!(),
                    };
                    let target_table = self.tables[table_idx].name.clone();
                    let target_schema = self.tables[table_idx].schema.clone();

                    let mut max_batch_lsn: Option<[u8; 10]> = None;
                    if let Phase::ChunkCollect { override_pks, .. } = &mut self.phase {
                        for event in &stream_events {
                            if let Some(lsn) = lsn_from_event(event) {
                                let is_greater = max_batch_lsn
                                    .as_ref()
                                    .map(|m| compare_lsn(&lsn, m).is_gt())
                                    .unwrap_or(true);
                                if is_greater {
                                    max_batch_lsn = Some(lsn);
                                }
                                let event_table = event.table.as_str();
                                let event_schema =
                                    event.schema.as_deref().unwrap_or(&target_schema);
                                if event_table == target_table
                                    && event_schema == target_schema
                                    && compare_lsn(&lsn, &low_wm).is_gt()
                                    && compare_lsn(&lsn, &high_wm).is_le()
                                {
                                    if let Some(fp) = Self::extract_event_pk_fingerprint(event) {
                                        override_pks.insert(fp);
                                    }
                                }
                            }
                        }
                    }

                    let wm_passed = self
                        .stream_passed_high_wm(
                            &stream_events,
                            max_batch_lsn.as_ref(),
                            &high_wm,
                        )
                        .await?;

                    if wm_passed {
                        self.finalize_collect();
                    }

                    if !stream_events.is_empty() {
                        return Ok(stream_events);
                    }

                    if wm_passed {
                        continue;
                    }

                    return Ok(Vec::new());
                }
            }
        }
    }

    async fn save_position(&self, checkpoint: &mut dyn Checkpoint) -> Result<()> {
        self.inner.save_position(checkpoint).await
    }

    async fn requeue_events(&mut self, events: Vec<Event>) -> Result<()> {
        self.inner.requeue_events(events).await
    }

    async fn confirm_lsn(&mut self, lsn: u64) -> Result<()> {
        self.inner.confirm_lsn(lsn).await
    }
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use super::{SqlServerIncrementalSnapshotHandle, compare_lsn, lsn_from_event};

    fn lsn_bytes(pos: u64) -> [u8; 10] {
        let mut lsn = [0u8; 10];
        let bytes = pos.to_be_bytes();
        lsn[2..10].copy_from_slice(&bytes);
        lsn
    }

    fn stream_event_with_lsn(table: &str, schema: &str, pk: u64, lsn_pos: u64) -> Event {
        use super::super::parser::lsn_bytes_to_hex;
        let lsn = lsn_bytes(lsn_pos);
        let hex = lsn_bytes_to_hex(&lsn);
        Event {
            before: None,
            after: Some(serde_json::json!({"id": pk})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "sqlserver".into(),
                offset: hex,
                timestamp: 1,
            },
            ts: 1,
            schema: Some(schema.into()),
            table: table.into(),
            primary_key: Some(vec!["id".to_string()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    #[test]
    fn pk_fingerprint_is_stable() {
        let vals = vec![serde_json::json!(42), serde_json::json!("hello")];
        let fp = SqlServerIncrementalSnapshotHandle::pk_fingerprint("orders", &vals);
        assert!(fp.contains("orders"));
        assert!(fp.contains("42"));
        assert!(fp.contains("hello"));
    }

    #[test]
    fn lsn_ordering_is_correct() {
        let low = lsn_bytes(100);
        let mid = lsn_bytes(150);
        let high = lsn_bytes(200);
        assert!(compare_lsn(&mid, &low).is_gt());
        assert!(compare_lsn(&mid, &high).is_lt());
        assert!(compare_lsn(&low, &low).is_eq());
    }

    #[test]
    fn override_window_detects_in_range_lsn() {
        let low_wm = lsn_bytes(100);
        let high_wm = lsn_bytes(200);

        // LSN = 150: in (low_wm, high_wm]
        let event = stream_event_with_lsn("users", "dbo", 1, 150);
        let lsn = lsn_from_event(&event).unwrap();
        assert!(compare_lsn(&lsn, &low_wm).is_gt());
        assert!(compare_lsn(&lsn, &high_wm).is_le());

        // LSN = 100 (= low_wm): NOT in range (exclusive lower bound)
        let event2 = stream_event_with_lsn("users", "dbo", 2, 100);
        let lsn2 = lsn_from_event(&event2).unwrap();
        assert!(!compare_lsn(&lsn2, &low_wm).is_gt());

        // LSN = 201: past high_wm
        let event3 = stream_event_with_lsn("users", "dbo", 3, 201);
        let lsn3 = lsn_from_event(&event3).unwrap();
        assert!(compare_lsn(&lsn3, &high_wm).is_gt());
    }

    #[test]
    fn merge_suppresses_overridden_rows() {
        use std::collections::HashSet;

        let row1_pk = vec![serde_json::json!(1_u64)];
        let row2_pk = vec![serde_json::json!(2_u64)];
        let fp1 =
            SqlServerIncrementalSnapshotHandle::pk_fingerprint("users", &row1_pk);
        let fp2 =
            SqlServerIncrementalSnapshotHandle::pk_fingerprint("users", &row2_pk);

        let event1 = stream_event_with_lsn("users", "dbo", 1, 50);
        let event2 = stream_event_with_lsn("users", "dbo", 2, 50);

        let mut override_pks = HashSet::new();
        override_pks.insert(fp1.clone());

        let chunk_rows = vec![(fp1, event1), (fp2.clone(), event2)];
        let merged: Vec<_> = chunk_rows
            .into_iter()
            .filter(|(fp, _)| !override_pks.contains(fp))
            .collect();

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].0, fp2);
    }
}
