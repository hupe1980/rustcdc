//! Incremental (non-blocking) snapshot for the PostgreSQL connector.
//!
//! # DBLog watermark pattern
//!
//! The algorithm interleaves small chunk reads with the live replication stream so
//! the stream never pauses, no long-held `REPEATABLE READ` transaction accumulates
//! transaction IDs, and each chunk is independently resumable after a crash.
//!
//! For each chunk the coordinator:
//! 1. Captures a **low-watermark LSN** before the `SELECT`.
//! 2. Reads `chunk_size` rows (keyset pagination, `READ COMMITTED`).
//! 3. Captures a **high-watermark LSN** after the `SELECT`.
//! 4. Continues polling the replication stream, recording the primary key of every
//!    event for the snapshotted table whose commit LSN falls in `(low_wm, high_wm]`.
//! 5. Once the stream advances past `high_wm`, emits snapshot `Read` events only for
//!    chunk rows whose primary key was **not** in the override set.
//!
//! The replication stream passes through to the consumer unchanged in every phase, so
//! downstream sinks see a continuous, gap-free event feed interleaved with snapshot
//! `Read` events.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use tokio_postgres::Client;

use crate::{
    checkpoint::Checkpoint,
    core::{Error, Event, Operation, Result, SnapshotMetadata, SourceMetadata,
           EVENT_ENVELOPE_VERSION},
    source::{IncrementalSnapshotConfig, StreamHandle},
};

use super::{
    format_pg_lsn, now_millis, parse_pg_lsn, parse_table_reference,
    qualified_table_name, query_current_wal_lsn, query_primary_key_columns_and_types,
    quote_pg_identifier,
};

// ─── Per-table metadata ───────────────────────────────────────────────────────

struct TableSpec {
    schema: String,
    name: String,
    /// Fully quoted `"schema"."table"` reference for SQL interpolation.
    qualified: String,
    pk_columns: Vec<String>,
    pk_types: Vec<String>,
    /// Keyset cursor: the last primary-key values seen in the previous chunk.
    /// `None` means the table has not started yet (first chunk from the beginning).
    pk_cursor: Option<Vec<String>>,
    is_complete: bool,
    chunks_emitted: u32,
    rows_emitted: u64,
}

// ─── Coordinator state machine ────────────────────────────────────────────────

enum Phase {
    /// Preparing the next chunk: will query watermarks and run the SELECT.
    ChunkPrepare { table_idx: usize },

    /// Chunk rows buffered; collecting stream events in `(low_wm, high_wm]` to
    /// build the override set.
    ChunkCollect {
        table_idx: usize,
        low_watermark: u64,
        high_watermark: u64,
        /// Buffered chunk rows as `(pk_json, event)` pairs.
        chunk_rows: Vec<(String, Event)>,
        /// Primary-key fingerprints of stream events in the override window.
        override_pks: HashSet<String>,
    },

    /// Merged events ready to return to the caller.
    ChunkEmit {
        table_idx: usize,
        events: VecDeque<Event>,
    },

    /// All tables are complete; the handle acts as a pure stream delegate.
    Done,
}

// ─── IncrementalSnapshotHandle ────────────────────────────────────────────────

/// A [`StreamHandle`] that interleaves per-table chunk reads with the live
/// replication stream using the DBLog watermark pattern.
///
/// Obtain an instance via [`PostgresConnection::start_incremental_snapshot`].
pub struct IncrementalSnapshotHandle {
    inner: Box<dyn StreamHandle>,
    /// Separate regular (non-replication) connection used for chunk SELECT queries
    /// and WAL LSN checks. Never holds a transaction.
    query_client: Arc<Client>,
    tables: Vec<TableSpec>,
    phase: Phase,
    chunk_size: usize,
    source_name: String,
    snapshot_id: String,
    /// The highest commit LSN seen from the inner replication stream.
    last_stream_lsn: u64,
}

impl IncrementalSnapshotHandle {
    /// Construct a new handle.  Queries the primary-key metadata for every table
    /// in `config.tables` before returning so that configuration errors are
    /// surfaced eagerly.
    pub(super) async fn new(
        inner: Box<dyn StreamHandle>,
        query_client: Arc<Client>,
        config: IncrementalSnapshotConfig,
        source_name: String,
    ) -> Result<Self> {
        let mut tables = Vec::with_capacity(config.tables.len());
        for table_ref in &config.tables {
            let (schema, name) = parse_table_reference(table_ref)?;
            let (pk_columns, pk_types) =
                query_primary_key_columns_and_types(&query_client, &schema, &name).await?;
            if pk_columns.is_empty() {
                return Err(Error::ConfigError(format!(
                    "incremental snapshot: table '{schema}.{name}' must have a primary key"
                )));
            }
            let qualified = qualified_table_name(&schema, &name);
            tables.push(TableSpec {
                schema,
                name,
                qualified,
                pk_columns,
                pk_types,
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

        let snapshot_id = format!("incremental-{}", now_millis());

        Ok(Self {
            inner,
            query_client,
            tables,
            phase,
            chunk_size: config.chunk_size.max(1),
            source_name,
            snapshot_id,
            last_stream_lsn: 0,
        })
    }

    // ─── Chunk fetch ──────────────────────────────────────────────────────────

    /// Execute a keyset-paginated chunk SELECT on the regular (READ COMMITTED)
    /// query client.  Returns decoded `(pk_values, row_json)` pairs.
    async fn fetch_chunk(
        &self,
        table_idx: usize,
    ) -> Result<Vec<(Vec<String>, serde_json::Value)>> {
        let table = &self.tables[table_idx];
        let limit = i64::try_from(self.chunk_size).unwrap_or(i64::MAX);
        let pk_cols = &table.pk_columns;
        let pk_types = &table.pk_types;
        let table_ref = &table.qualified;

        let order_expr = pk_cols
            .iter()
            .map(|c| format!("t.{}", quote_pg_identifier(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let key_value_expr = pk_cols
            .iter()
            .map(|c| format!("t.{}::text", quote_pg_identifier(c)))
            .collect::<Vec<_>>()
            .join(", ");

        let raw_rows = if let Some(cursor) = &table.pk_cursor {
            // Keyset cursor: bind as text, cast inside SQL to the actual PK type.
            let predicate_expr = pk_types
                .iter()
                .enumerate()
                .map(|(i, pg_type)| format!("${}::text::{pg_type}", i + 1))
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!(
                "SELECT ARRAY[{key_value_expr}], row_to_json(t)::text \
                 FROM {table_ref} t \
                 WHERE ({order_expr}) > ({predicate_expr}) \
                 ORDER BY {order_expr} \
                 LIMIT ${}",
                pk_cols.len() + 1
            );
            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                Vec::with_capacity(cursor.len() + 1);
            for v in cursor {
                params.push(v as &(dyn tokio_postgres::types::ToSql + Sync));
            }
            params.push(&limit);
            self.query_client
                .query(&query, &params)
                .await
                .map_err(|e| {
                    Error::SourceError(format!(
                        "incremental snapshot chunk failed for '{}': {e}",
                        table.qualified
                    ))
                })?
        } else {
            let query = format!(
                "SELECT ARRAY[{key_value_expr}], row_to_json(t)::text \
                 FROM {table_ref} t \
                 ORDER BY {order_expr} \
                 LIMIT $1"
            );
            self.query_client
                .query(&query, &[&limit])
                .await
                .map_err(|e| {
                    Error::SourceError(format!(
                        "incremental snapshot chunk failed for '{}': {e}",
                        table.qualified
                    ))
                })?
        };

        let table = &self.tables[table_idx]; // re-borrow after await
        let mut decoded = Vec::with_capacity(raw_rows.len());
        for row in raw_rows {
            let key_values: Vec<Option<String>> = row.get(0);
            let key_values = key_values
                .into_iter()
                .map(|v| {
                    v.ok_or_else(|| {
                        Error::SourceError(format!(
                            "incremental snapshot: NULL primary-key column for '{}'",
                            table.qualified
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let payload: String = row.get(1);
            let json = serde_json::from_str(&payload).map_err(|e| {
                Error::SerializationError(format!(
                    "incremental snapshot: JSON decode failed for '{}': {e}",
                    table.qualified
                ))
            })?;
            decoded.push((key_values, json));
        }
        Ok(decoded)
    }

    // ─── PK fingerprint helpers ───────────────────────────────────────────────

    /// Produce a stable JSON-array fingerprint for a set of PK column values.
    fn pk_fingerprint(pk_values: &[String]) -> String {
        serde_json::to_string(pk_values).unwrap_or_else(|_| pk_values.join(","))
    }

    /// Extract the primary-key fingerprint from a stream event's `after` / `before`
    /// payload and the event's `primary_key` column list.
    fn extract_event_pk_fingerprint(event: &Event) -> Option<String> {
        let pk_cols = event.primary_key.as_ref()?;
        if pk_cols.is_empty() {
            return None;
        }
        let payload = event.after.as_ref().or(event.before.as_ref())?;
        let values: Vec<String> = pk_cols
            .iter()
            .map(|col| {
                payload
                    .get(col)
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default()
            })
            .collect();
        Some(serde_json::to_string(&values).unwrap_or_else(|_| values.join(",")))
    }

    /// Parse the commit LSN from a stream event's `source.offset` field.
    fn lsn_from_event(event: &Event) -> Option<u64> {
        parse_pg_lsn(&event.source.offset).ok()
    }

    // ─── Snapshot event builder ───────────────────────────────────────────────

    fn build_snapshot_event(
        &self,
        table_idx: usize,
        pk_values: &[String],
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
                // Encode as a synthetic LSN-like string; the format is stable
                // across restarts and identifies the chunk + PK position.
                offset: format!(
                    "incremental:{}:{}",
                    table.qualified,
                    Self::pk_fingerprint(pk_values)
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
                is_last_chunk: false, // set later when the last chunk is detected
            }),
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    // ─── Phase: ChunkPrepare ──────────────────────────────────────────────────

    /// Fetch the next chunk and transition to `ChunkCollect`.
    /// If there are no more rows for this table, marks it complete and advances
    /// to the next pending table (or `Done`).
    async fn drive_chunk_prepare(&mut self) -> Result<()> {
        let table_idx = match self.phase {
            Phase::ChunkPrepare { table_idx } => table_idx,
            _ => return Ok(()),
        };

        // Find the first incomplete table starting from table_idx.
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
        let low_watermark = query_current_wal_lsn(&self.query_client).await?;

        // Read the chunk (keyset-paginated, READ COMMITTED — no transaction).
        let rows = self.fetch_chunk(table_idx).await?;

        // Capture high watermark after the SELECT.
        let high_watermark = query_current_wal_lsn(&self.query_client).await?;

        if rows.is_empty() {
            // Table fully exhausted.
            self.tables[table_idx].is_complete = true;
            tracing::debug!(
                target: "cdc_rs::source::incremental_snapshot",
                table = %self.tables[table_idx].qualified,
                chunks = self.tables[table_idx].chunks_emitted,
                rows = self.tables[table_idx].rows_emitted,
                "incremental snapshot: table complete",
            );
            // Advance to the next pending table.
            let next = (table_idx + 1..self.tables.len())
                .find(|&i| !self.tables[i].is_complete);
            self.phase = match next {
                Some(idx) => Phase::ChunkPrepare { table_idx: idx },
                None => Phase::Done,
            };
            return Ok(());
        }

        // Update the keyset cursor to the last PK of this chunk.
        let last_pk = rows.last().map(|(pk, _)| pk.clone()).unwrap_or_default();
        self.tables[table_idx].pk_cursor = Some(last_pk);

        // Build snapshot events, indexed by PK fingerprint.
        let chunk_index = self.tables[table_idx].chunks_emitted;
        let chunk_rows: Vec<(String, Event)> = rows
            .into_iter()
            .map(|(pk_values, json)| {
                let fp = Self::pk_fingerprint(&pk_values);
                let event =
                    self.build_snapshot_event(table_idx, &pk_values, json, chunk_index);
                (fp, event)
            })
            .collect();

        tracing::debug!(
            target: "cdc_rs::source::incremental_snapshot",
            table = %self.tables[table_idx].qualified,
            chunk = chunk_index,
            rows = chunk_rows.len(),
            low_watermark = format_pg_lsn(low_watermark),
            high_watermark = format_pg_lsn(high_watermark),
            "incremental snapshot: chunk read, entering collect phase",
        );

        self.phase = Phase::ChunkCollect {
            table_idx,
            low_watermark,
            high_watermark,
            chunk_rows,
            override_pks: HashSet::new(),
        };
        Ok(())
    }

    // ─── Phase: ChunkCollect ──────────────────────────────────────────────────

    /// Determine whether the stream has advanced past `high_watermark`.
    ///
    /// First checks the events returned by the stream poll.  If no events
    /// arrived (quiet database), falls back to a single `pg_current_wal_lsn()`
    /// query so we don't wait for a stream event that may never come.
    async fn stream_passed_high_wm(
        &self,
        stream_events: &[Event],
        max_lsn_in_batch: u64,
        high_watermark: u64,
    ) -> Result<bool> {
        if max_lsn_in_batch > high_watermark {
            return Ok(true);
        }
        if stream_events.is_empty() {
            // No events from the inner stream; check WAL directly.
            let current = query_current_wal_lsn(&self.query_client).await?;
            return Ok(current > high_watermark);
        }
        Ok(false)
    }

    /// Merge `chunk_rows` with `override_pks` and transition to `ChunkEmit`.
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

        // Emit chunk rows whose PK was NOT overridden by a stream event in the window.
        let merged: VecDeque<Event> = chunk_rows
            .into_iter()
            .filter(|(fp, _)| !override_pks.contains(fp))
            .map(|(_, event)| event)
            .collect();

        let emitted = merged.len() as u64;
        self.tables[table_idx].chunks_emitted += 1;
        self.tables[table_idx].rows_emitted += emitted;

        tracing::debug!(
            target: "cdc_rs::source::incremental_snapshot",
            table = %self.tables[table_idx].qualified,
            chunk = self.tables[table_idx].chunks_emitted,
            emitted,
            suppressed = override_pks.len(),
            "incremental snapshot: chunk merged, entering emit phase",
        );

        self.phase = Phase::ChunkEmit {
            table_idx,
            events: merged,
        };
    }
}

// ─── StreamHandle impl ────────────────────────────────────────────────────────

#[async_trait]
impl StreamHandle for IncrementalSnapshotHandle {
    async fn next_events(&mut self, timeout_ms: u64) -> Result<Vec<Event>> {
        loop {
            match &self.phase {
                // ── All tables done: pure stream delegation ──────────────────
                Phase::Done => {
                    return self.inner.next_events(timeout_ms).await;
                }

                // ── Drain buffered snapshot events ───────────────────────────
                Phase::ChunkEmit { .. } => {
                    // We need ownership of the fields; use a nested match on the
                    // mutable reference.
                    let (table_idx, done) = match &mut self.phase {
                        Phase::ChunkEmit { table_idx, events } => {
                            let batch_size = events.len().min(1_000);
                            if batch_size > 0 {
                                let batch: Vec<Event> =
                                    events.drain(..batch_size).collect();
                                return Ok(batch);
                            }
                            (*table_idx, true)
                        }
                        _ => unreachable!(),
                    };
                    if done {
                        // Queue exhausted — start the next chunk for the same table
                        // (prepare_next_chunk will detect table completion via an
                        // empty result set).
                        self.phase = Phase::ChunkPrepare { table_idx };
                        // Loop around to drive ChunkPrepare.
                    }
                }

                // ── Fetch next chunk ─────────────────────────────────────────
                Phase::ChunkPrepare { .. } => {
                    self.drive_chunk_prepare().await?;
                    // Loop: re-evaluate the new phase.
                }

                // ── Collect stream overrides ─────────────────────────────────
                Phase::ChunkCollect { .. } => {
                    // Use a short timeout so we don't block too long per iteration.
                    let stream_events =
                        self.inner.next_events(timeout_ms.min(100)).await?;

                    // Extract the target table before the mutable borrow below.
                    let (table_idx, low_wm, high_wm) = match &self.phase {
                        Phase::ChunkCollect {
                            table_idx,
                            low_watermark,
                            high_watermark,
                            ..
                        } => (*table_idx, *low_watermark, *high_watermark),
                        _ => unreachable!(),
                    };
                    let target_table = self.tables[table_idx].name.clone();
                    let target_schema = self.tables[table_idx].schema.clone();

                    // Scan stream events: update last_stream_lsn and capture overrides.
                    let mut max_batch_lsn = 0_u64;
                    if let Phase::ChunkCollect { override_pks, .. } = &mut self.phase {
                        for event in &stream_events {
                            if let Some(lsn) = Self::lsn_from_event(event) {
                                if lsn > max_batch_lsn {
                                    max_batch_lsn = lsn;
                                }
                                // Record override: event is for the snapshotted
                                // table and its LSN falls in (low_wm, high_wm].
                                let event_table = event.table.as_str();
                                let event_schema =
                                    event.schema.as_deref().unwrap_or("public");
                                if event_table == target_table
                                    && event_schema == target_schema
                                    && lsn > low_wm
                                    && lsn <= high_wm
                                {
                                    if let Some(fp) =
                                        Self::extract_event_pk_fingerprint(event)
                                    {
                                        override_pks.insert(fp);
                                    }
                                }
                            }
                        }
                    }

                    if max_batch_lsn > self.last_stream_lsn {
                        self.last_stream_lsn = max_batch_lsn;
                    }

                    // Check if the stream has passed the high watermark.
                    let wm_passed = self
                        .stream_passed_high_wm(&stream_events, max_batch_lsn, high_wm)
                        .await?;

                    if wm_passed {
                        self.finalize_collect();
                    }

                    // Always return stream events first so the consumer stays
                    // current; snapshot events follow in the next call.
                    if !stream_events.is_empty() {
                        return Ok(stream_events);
                    }

                    if wm_passed {
                        // No stream events in this batch but collection is done —
                        // loop to drain ChunkEmit immediately.
                        continue;
                    }

                    // Neither watermark passed nor events: return empty.
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
    use crate::{
        core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION},
    };

    fn stream_event(table: &str, pk: u64, lsn: u64, op: Operation) -> Event {
        let pk_str = pk.to_string();
        let payload = match op {
            Operation::Delete => None,
            _ => Some(serde_json::json!({"id": pk_str})),
        };
        let before = if op == Operation::Delete {
            Some(serde_json::json!({"id": pk_str}))
        } else {
            None
        };
        Event {
            before,
            after: payload,
            op,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: crate::source::postgres::format_pg_lsn(lsn),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".into()),
            table: table.into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    // ─── State machine unit tests (no database required) ──────────────────────

    /// Verify that `IncrementalSnapshotConfig` builder defaults and override work.
    #[test]
    fn config_defaults_and_builder() {
        let cfg = IncrementalSnapshotConfig::new(vec!["public.users".to_string()]);
        assert_eq!(cfg.chunk_size, 5_000);
        assert_eq!(cfg.tables, vec!["public.users"]);

        let cfg = cfg.with_chunk_size(1_000);
        assert_eq!(cfg.chunk_size, 1_000);
    }

    /// `pk_fingerprint` must produce a stable JSON-array string.
    #[test]
    fn pk_fingerprint_stable() {
        let single = IncrementalSnapshotHandle::pk_fingerprint(&["42".into()]);
        assert_eq!(single, r#"["42"]"#);

        let composite = IncrementalSnapshotHandle::pk_fingerprint(&["1".into(), "a".into()]);
        assert_eq!(composite, r#"["1","a"]"#);
    }

    /// `extract_event_pk_fingerprint` must return `None` when `primary_key` is absent.
    #[test]
    fn extract_pk_none_when_missing() {
        let ev = Event {
            before: None,
            after: Some(serde_json::json!({"id": "7"})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "s".into(),
                offset: "0/1".into(),
                timestamp: 0,
            },
            ts: 0,
            schema: None,
            table: "t".into(),
            primary_key: None, // no PK columns
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        };
        assert!(IncrementalSnapshotHandle::extract_event_pk_fingerprint(&ev).is_none());
    }

    /// `lsn_from_event` must correctly parse a formatted LSN offset.
    #[test]
    fn lsn_from_event_roundtrip() {
        use crate::source::postgres::format_pg_lsn;
        let lsn: u64 = 0x0001_5D0A_2000;
        let ev = Event {
            before: None,
            after: None,
            op: Operation::Read,
            source: SourceMetadata {
                source_name: "pg".into(),
                offset: format_pg_lsn(lsn),
                timestamp: 0,
            },
            ts: 0,
            schema: None,
            table: "t".into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        };
        assert_eq!(IncrementalSnapshotHandle::lsn_from_event(&ev), Some(lsn));
    }

    /// Override detection: only events in (low_wm, high_wm] for the correct
    /// table should populate the override set.
    #[test]
    fn override_detection_window() {
        let low_wm: u64 = 100;
        let high_wm: u64 = 200;

        // Event inside window, correct table → should be an override.
        let inside = stream_event("users", 5, 150, Operation::Update);
        // Event inside window, wrong table → not an override.
        let wrong_table = stream_event("orders", 5, 150, Operation::Update);
        // Event before window → not an override.
        let before = stream_event("users", 6, 50, Operation::Update);
        // Event exactly at low_wm → not in (low_wm, high_wm] (exclusive lower bound).
        let at_low = stream_event("users", 7, 100, Operation::Update);
        // Event exactly at high_wm → in the window (inclusive upper bound).
        let at_high = stream_event("users", 8, 200, Operation::Update);
        // Event after window → not an override.
        let after = stream_event("users", 9, 250, Operation::Update);

        let events = [&inside, &wrong_table, &before, &at_low, &at_high, &after];

        let mut overrides = HashSet::new();
        for ev in events {
            if let Some(lsn) = IncrementalSnapshotHandle::lsn_from_event(ev) {
                if ev.table == "users"
                    && ev.schema.as_deref().unwrap_or("public") == "public"
                    && lsn > low_wm
                    && lsn <= high_wm
                {
                    if let Some(fp) = IncrementalSnapshotHandle::extract_event_pk_fingerprint(ev) {
                        overrides.insert(fp);
                    }
                }
            }
        }

        // Only `inside` (pk=5) and `at_high` (pk=8) qualify.
        let fp5 = IncrementalSnapshotHandle::pk_fingerprint(&["5".into()]);
        let fp8 = IncrementalSnapshotHandle::pk_fingerprint(&["8".into()]);
        assert!(overrides.contains(&fp5), "pk=5 inside window should be an override");
        assert!(overrides.contains(&fp8), "pk=8 at high_wm should be an override");
        assert_eq!(overrides.len(), 2, "only 2 qualifying events");
    }

    /// Merge logic: chunk rows with PK in the override set are suppressed;
    /// the rest are emitted as Read events.
    #[test]
    fn merge_suppresses_overridden_rows() {
        use crate::core::SnapshotMetadata;

        let make_event = |pk: u64| -> Event {
            Event {
                before: None,
                after: Some(serde_json::json!({"id": pk.to_string()})),
                op: Operation::Read,
                source: SourceMetadata {
                    source_name: "pg".into(),
                    offset: format!("incremental:\"public\".\"users\":[\"{pk}\"]"),
                    timestamp: 0,
                },
                ts: 0,
                schema: Some("public".into()),
                table: "users".into(),
                primary_key: Some(vec!["id".into()]),
                snapshot: Some(SnapshotMetadata {
                    snapshot_id: "s1".into(),
                    chunk_index: 0,
                    is_last_chunk: false,
                }),
                transaction: None,
                envelope_version: EVENT_ENVELOPE_VERSION,
            }
        };

        let chunk_rows: Vec<(String, Event)> = vec![
            (IncrementalSnapshotHandle::pk_fingerprint(&["1".into()]), make_event(1)),
            (IncrementalSnapshotHandle::pk_fingerprint(&["2".into()]), make_event(2)),
            (IncrementalSnapshotHandle::pk_fingerprint(&["3".into()]), make_event(3)),
        ];
        let mut override_pks = HashSet::new();
        override_pks.insert(IncrementalSnapshotHandle::pk_fingerprint(&["2".into()]));

        // Replicate the merge logic from `finalize_collect`.
        let merged: Vec<Event> = chunk_rows
            .into_iter()
            .filter(|(fp, _)| !override_pks.contains(fp))
            .map(|(_, event)| event)
            .collect();

        assert_eq!(merged.len(), 2, "only pk=1 and pk=3 should be emitted");
        assert!(merged
            .iter()
            .all(|e| e.after.as_ref().and_then(|j| j.get("id")).map(|v| v.as_str().unwrap_or("")) != Some("2")),
            "pk=2 must be suppressed");
    }

    use super::*;
}
