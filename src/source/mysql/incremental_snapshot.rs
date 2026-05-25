//! Incremental (non-blocking) snapshot for the MySQL connector.
//!
//! # DBLog watermark pattern
//!
//! The algorithm interleaves small chunk reads with the live binlog stream so
//! the stream never pauses. For each chunk the coordinator:
//! 1. Captures a **low-watermark** binlog position before the `SELECT`.
//! 2. Reads `chunk_size` rows (keyset pagination, `READ COMMITTED`).
//! 3. Captures a **high-watermark** binlog position after the `SELECT`.
//! 4. Continues polling the binlog stream, recording the primary key of every
//!    event for the snapshotted table whose binlog position falls in `(low_wm, high_wm]`.
//! 5. Once the stream advances past `high_wm`, emits snapshot `Read` events only for
//!    chunk rows whose primary key was **not** in the override set.

use std::collections::{HashSet, VecDeque};

use async_trait::async_trait;
use mysql_async::{prelude::Queryable, Pool as MySqlPool};

use crate::{
    checkpoint::Checkpoint,
    core::{Error, Event, Operation, Result, SnapshotMetadata, SourceMetadata,
           EVENT_ENVELOPE_VERSION},
    source::{IncrementalSnapshotConfig, StreamHandle},
    source::helpers::now_millis,
};

use super::{
    parser::{parse_mysql_source_offset, quoted_mysql_identifier, split_table_reference},
    query::mysql_json_value_to_param,
    state::compare_binlog_position,
};

// ─── Types ────────────────────────────────────────────────────────────────────

type BinlogPos = (String, u32);

fn cmp_binlog(a: &BinlogPos, b: &BinlogPos) -> std::cmp::Ordering {
    compare_binlog_position(&a.0, a.1, &b.0, b.1)
}

async fn query_master_status(pool: &MySqlPool) -> Result<BinlogPos> {
    let mut conn = pool.get_conn().await.map_err(|e| {
        Error::SourceError(format!("incremental snapshot: failed to get mysql conn: {e}"))
    })?;
    let mut row: mysql_async::Row = conn
        .query_first("SHOW MASTER STATUS")
        .await
        .map_err(|e| {
            Error::SourceError(format!("incremental snapshot: SHOW MASTER STATUS failed: {e}"))
        })?
        .ok_or_else(|| {
            Error::SourceError("incremental snapshot: SHOW MASTER STATUS returned no row".into())
        })?;
    let file: String = row.take(0).unwrap_or_default();
    let pos_u64: u64 = row.take(1).unwrap_or(4);
    let pos = u32::try_from(pos_u64).map_err(|_| {
        Error::SourceError(format!(
            "incremental snapshot: mysql binlog position exceeds u32: {pos_u64}"
        ))
    })?;
    Ok((file, pos))
}

fn binlog_pos_from_event(event: &Event) -> Option<BinlogPos> {
    let (file, pos) = parse_mysql_source_offset(&event.source.offset)?;
    Some((file.to_string(), pos))
}

// ─── Per-table metadata ───────────────────────────────────────────────────────

struct TableSpec {
    schema: String,
    name: String,
    /// `` `schema`.`table` ``
    qualified: String,
    pk_columns: Vec<String>,
    /// JSON-encoded cursor: last PK values seen in the previous chunk.
    /// `None` means the first chunk (start from beginning).
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
        low_wm: BinlogPos,
        high_wm: BinlogPos,
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

/// Non-blocking incremental snapshot handle for MySQL.
///
/// Returned by [`MysqlConnection::start_incremental_snapshot`].  Wraps the live
/// binlog stream and interleaves DBLog watermark-based chunk reads.
pub struct MysqlIncrementalSnapshotHandle {
    inner: Box<dyn StreamHandle>,
    pool: MySqlPool,
    tables: Vec<TableSpec>,
    phase: Phase,
    chunk_size: usize,
    source_name: String,
    snapshot_id: String,
}

impl MysqlIncrementalSnapshotHandle {
    /// Construct a new handle, eagerly loading PK metadata for every table.
    pub(super) async fn new(
        inner: Box<dyn StreamHandle>,
        pool: MySqlPool,
        config: IncrementalSnapshotConfig,
        source_name: String,
        default_database: String,
    ) -> Result<Self> {
        let mut conn = pool.get_conn().await.map_err(|e| {
            Error::SourceError(format!(
                "incremental snapshot: failed to acquire mysql connection: {e}"
            ))
        })?;

        let mut tables = Vec::with_capacity(config.tables.len());
        for table_ref in &config.tables {
            let (schema_opt, name) = split_table_reference(table_ref)?;
            let schema = schema_opt.unwrap_or_else(|| default_database.clone());
            let pk_columns: Vec<String> = conn
                .exec(
                    "SELECT COLUMN_NAME \
                     FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
                     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' \
                     ORDER BY ORDINAL_POSITION",
                    (&schema, &name),
                )
                .await
                .map_err(|e| {
                    Error::ConfigError(format!(
                        "incremental snapshot: PK query failed for '{schema}.{name}': {e}"
                    ))
                })?;
            if pk_columns.is_empty() {
                return Err(Error::ConfigError(format!(
                    "incremental snapshot: table '{schema}.{name}' must have a primary key"
                )));
            }
            let qualified = format!(
                "{}.{}",
                quoted_mysql_identifier(&schema),
                quoted_mysql_identifier(&name)
            );
            tables.push(TableSpec {
                schema,
                name,
                qualified,
                pk_columns,
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

        let snapshot_id = format!("incremental-mysql-{}", now_millis());

        Ok(Self {
            inner,
            pool,
            tables,
            phase,
            chunk_size: config.chunk_size.max(1),
            source_name,
            snapshot_id,
        })
    }

    // ─── Chunk fetch ──────────────────────────────────────────────────────────

    /// Execute a keyset-paginated chunk SELECT.
    /// Returns `(pk_json_values, row_json)` pairs.
    async fn fetch_chunk(
        &self,
        table_idx: usize,
    ) -> Result<Vec<(Vec<serde_json::Value>, serde_json::Value)>> {
        let table = &self.tables[table_idx];
        let pk_cols = &table.pk_columns;
        let table_ref = &table.qualified;
        let limit = self.chunk_size;

        let order_expr = pk_cols
            .iter()
            .map(|c| quoted_mysql_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");

        let mut conn = self.pool.get_conn().await.map_err(|e| {
            Error::SourceError(format!(
                "incremental snapshot: chunk fetch failed to get conn for '{}': {e}",
                table.qualified
            ))
        })?;

        let rows: Vec<mysql_async::Row> = if let Some(cursor) = &table.pk_cursor {
            // Row value constructor: MySQL supports `(pk1, pk2) > (?, ?)`
            let placeholders = cursor.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let pk_list = pk_cols
                .iter()
                .map(|c| quoted_mysql_identifier(c))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT * FROM {table_ref} WHERE ({pk_list}) > ({placeholders}) \
                 ORDER BY {order_expr} LIMIT {limit}"
            );
            let params: Vec<mysql_async::Value> = cursor
                .iter()
                .map(mysql_json_value_to_param)
                .collect::<Result<Vec<_>>>()?;
            conn.exec(sql, params).await.map_err(|e| {
                Error::SourceError(format!(
                    "incremental snapshot: chunk SELECT failed for '{}': {e}",
                    table.qualified
                ))
            })?
        } else {
            let sql = format!(
                "SELECT * FROM {table_ref} ORDER BY {order_expr} LIMIT {limit}"
            );
            conn.exec(sql, ()).await.map_err(|e| {
                Error::SourceError(format!(
                    "incremental snapshot: chunk SELECT failed for '{}': {e}",
                    table.qualified
                ))
            })?
        };

        let table = &self.tables[table_idx]; // re-borrow
        let mut decoded = Vec::with_capacity(rows.len());
        for row in rows {
            let json = mysql_row_to_json(&row);
            // Extract PK values in column order
            let pk_json: Vec<serde_json::Value> = pk_cols
                .iter()
                .map(|col| json.get(col).cloned().unwrap_or(serde_json::Value::Null))
                .collect();
            if pk_json.iter().any(|v| v.is_null()) {
                return Err(Error::SourceError(format!(
                    "incremental snapshot: NULL primary-key column for '{}'",
                    table.qualified
                )));
            }
            decoded.push((pk_json, json));
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

        // Capture low watermark before the SELECT.
        let low_wm = query_master_status(&self.pool).await?;

        // Read the chunk (keyset-paginated, READ COMMITTED — no transaction).
        let rows = self.fetch_chunk(table_idx).await?;

        // Capture high watermark after the SELECT.
        let high_wm = query_master_status(&self.pool).await?;

        if rows.is_empty() {
            self.tables[table_idx].is_complete = true;
            tracing::debug!(
                target: "cdc_rs::source::mysql::incremental_snapshot",
                table = %self.tables[table_idx].qualified,
                chunks = self.tables[table_idx].chunks_emitted,
                rows = self.tables[table_idx].rows_emitted,
                "incremental snapshot: mysql table complete",
            );
            let next = (table_idx + 1..self.tables.len())
                .find(|&i| !self.tables[i].is_complete);
            self.phase = match next {
                Some(idx) => Phase::ChunkPrepare { table_idx: idx },
                None => Phase::Done,
            };
            return Ok(());
        }

        // Update keyset cursor.
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
            target: "cdc_rs::source::mysql::incremental_snapshot",
            table = %self.tables[table_idx].qualified,
            chunk = chunk_index,
            rows = chunk_rows.len(),
            low_wm = format!("{}:{}", low_wm.0, low_wm.1),
            high_wm = format!("{}:{}", high_wm.0, high_wm.1),
            "incremental snapshot: mysql chunk read, entering collect phase",
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
        max_batch_pos: Option<&BinlogPos>,
        high_wm: &BinlogPos,
    ) -> Result<bool> {
        if let Some(pos) = max_batch_pos {
            if cmp_binlog(pos, high_wm).is_gt() {
                return Ok(true);
            }
        }
        if stream_events.is_empty() {
            // Quiet database: re-query SHOW MASTER STATUS to detect progress.
            let current = query_master_status(&self.pool).await?;
            return Ok(cmp_binlog(&current, high_wm).is_gt());
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
            target: "cdc_rs::source::mysql::incremental_snapshot",
            table = %self.tables[table_idx].qualified,
            chunk = self.tables[table_idx].chunks_emitted,
            emitted,
            suppressed = override_pks.len(),
            "incremental snapshot: mysql chunk merged, entering emit phase",
        );

        self.phase = Phase::ChunkEmit {
            table_idx,
            events: merged,
        };
    }
}

// ─── StreamHandle impl ────────────────────────────────────────────────────────

#[async_trait]
impl StreamHandle for MysqlIncrementalSnapshotHandle {
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
                        } => (*table_idx, low_wm.clone(), high_wm.clone()),
                        _ => unreachable!(),
                    };
                    let target_table = self.tables[table_idx].name.clone();
                    let target_schema = self.tables[table_idx].schema.clone();

                    let mut max_batch_pos: Option<BinlogPos> = None;
                    if let Phase::ChunkCollect { override_pks, .. } = &mut self.phase {
                        for event in &stream_events {
                            if let Some(pos) = binlog_pos_from_event(event) {
                                let is_after = max_batch_pos
                                    .as_ref()
                                    .map(|m| cmp_binlog(&pos, m).is_gt())
                                    .unwrap_or(true);
                                if is_after {
                                    max_batch_pos = Some(pos.clone());
                                }
                                // Record override: event is for the snapshotted
                                // table and its position falls in (low_wm, high_wm].
                                let event_table = event.table.as_str();
                                let event_schema =
                                    event.schema.as_deref().unwrap_or(&target_schema);
                                if event_table == target_table
                                    && event_schema == target_schema
                                    && cmp_binlog(&pos, &low_wm).is_gt()
                                    && cmp_binlog(&pos, &high_wm).is_le()
                                {
                                    if let Some(fp) = Self::extract_event_pk_fingerprint(event) {
                                        override_pks.insert(fp);
                                    }
                                }
                            }
                        }
                    }

                    let wm_passed = self
                        .stream_passed_high_wm(&stream_events, max_batch_pos.as_ref(), &high_wm)
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

// ─── helpers used above (inline) ─────────────────────────────────────────────

fn mysql_row_to_json(row: &mysql_async::Row) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (idx, col) in row.columns_ref().iter().enumerate() {
        let name = col.name_str().to_string();
        let value = match row.as_ref(idx) {
            Some(v) => mysql_value_to_json(v),
            None => serde_json::Value::Null,
        };
        map.insert(name, value);
    }
    serde_json::Value::Object(map)
}

fn mysql_value_to_json(value: &mysql_common::value::Value) -> serde_json::Value {
    use mysql_common::value::Value as MysqlValue;
    match value {
        MysqlValue::NULL => serde_json::Value::Null,
        MysqlValue::Bytes(bytes) => String::from_utf8(bytes.clone())
            .map(serde_json::Value::String)
            .unwrap_or_else(|_| {
                let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
                serde_json::Value::String(hex)
            }),
        MysqlValue::Int(v) => serde_json::Value::Number((*v).into()),
        MysqlValue::UInt(v) => serde_json::Value::Number((*v).into()),
        MysqlValue::Float(v) => serde_json::Number::from_f64(f64::from(*v))
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(v.to_string())),
        MysqlValue::Double(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(v.to_string())),
        MysqlValue::Date(year, month, day, hour, minute, second, micros) => {
            serde_json::Value::String(format!(
                "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}"
            ))
        }
        MysqlValue::Time(neg, days, hours, minutes, seconds, micros) => {
            let sign = if *neg { "-" } else { "" };
            serde_json::Value::String(format!(
                "{sign}{days}:{hours:02}:{minutes:02}:{seconds:02}.{micros:06}"
            ))
        }
    }
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use super::MysqlIncrementalSnapshotHandle;

    fn stream_event(table: &str, schema: &str, pk: u64, binlog_pos: u32, op: Operation) -> Event {
        let pk_str = pk.to_string();
        let (after, before) = match op {
            Operation::Delete => (None, Some(serde_json::json!({"id": pk_str}))),
            _ => (Some(serde_json::json!({"id": pk_str})), None),
        };
        Event {
            before,
            after,
            op,
            source: SourceMetadata {
                source_name: "mysql".into(),
                offset: format!("mysql-bin.000001:{binlog_pos}"),
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
        let vals = vec![serde_json::json!(42), serde_json::json!("foo")];
        let fp = MysqlIncrementalSnapshotHandle::pk_fingerprint("users", &vals);
        assert!(fp.contains("users"));
        assert!(fp.contains("42"));
        assert!(fp.contains("foo"));
    }

    #[test]
    fn extract_event_pk_fingerprint_uses_table_name() {
        let event = stream_event("orders", "shop", 7, 100, Operation::Insert);
        let fp = MysqlIncrementalSnapshotHandle::extract_event_pk_fingerprint(&event).unwrap();
        assert!(fp.contains("orders"));
        assert!(fp.contains("7"));
    }

    #[test]
    fn override_window_detects_in_range_event() {
        use super::{BinlogPos, cmp_binlog, binlog_pos_from_event};

        let low_wm: BinlogPos = ("mysql-bin.000001".into(), 100);
        let high_wm: BinlogPos = ("mysql-bin.000001".into(), 200);

        // Event at position 150 — in (low_wm, high_wm]
        let event = stream_event("users", "db", 1, 150, Operation::Update);
        let pos = binlog_pos_from_event(&event).unwrap();
        assert!(cmp_binlog(&pos, &low_wm).is_gt());
        assert!(cmp_binlog(&pos, &high_wm).is_le());

        // Event at position 100 (= low_wm) — not in range (exclusive lower bound)
        let event2 = stream_event("users", "db", 2, 100, Operation::Update);
        let pos2 = binlog_pos_from_event(&event2).unwrap();
        assert!(!cmp_binlog(&pos2, &low_wm).is_gt());

        // Event at position 201 — past high_wm
        let event3 = stream_event("users", "db", 3, 201, Operation::Update);
        let pos3 = binlog_pos_from_event(&event3).unwrap();
        assert!(cmp_binlog(&pos3, &high_wm).is_gt());
    }

    #[test]
    fn merge_suppresses_overridden_rows() {
        use std::collections::HashSet;

        // Build two fake chunk rows
        let row1_pk = vec![serde_json::json!(1_u64)];
        let row2_pk = vec![serde_json::json!(2_u64)];
        let fp1 = MysqlIncrementalSnapshotHandle::pk_fingerprint("users", &row1_pk);
        let fp2 = MysqlIncrementalSnapshotHandle::pk_fingerprint("users", &row2_pk);

        let event1 = stream_event("users", "db", 1, 50, Operation::Read);
        let event2 = stream_event("users", "db", 2, 50, Operation::Read);

        let mut override_pks = HashSet::new();
        override_pks.insert(fp1.clone());

        let chunk_rows = vec![(fp1, event1), (fp2.clone(), event2)];
        // Simulate finalize: filter overridden rows
        let merged: Vec<_> = chunk_rows
            .into_iter()
            .filter(|(fp, _)| !override_pks.contains(fp))
            .collect();

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].0, fp2);
    }
}
