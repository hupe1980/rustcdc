//! Source-agnostic DBLog incremental snapshot.
//!
//! # Why this is one implementation and not three
//!
//! The watermark algorithm is identical for every log-based source; only the
//! *position type* and the *SQL dialect* differ. It used to be copied once per
//! connector, and the copies drifted: the resume-from-cursor fix (C1 in the audit)
//! had to be applied three times because the same missing feature existed three
//! times. A connector now supplies [`IncrementalSnapshotBackend`] — six methods of
//! genuinely database-specific work — and inherits the state machine, the override
//! window, cursor persistence and the [`StreamHandle`] contract unchanged.
//!
//! This is also the extension point for third-party connectors: an `impl Source`
//! that implements the backend gets non-blocking snapshots without reimplementing
//! the correctness-critical part.
//!
//! # The algorithm
//!
//! For each chunk the driver:
//! 1. Captures a **low watermark** position before the `SELECT`.
//! 2. Reads `chunk_size` rows using keyset pagination, outside any transaction.
//! 3. Captures a **high watermark** position after the `SELECT`.
//! 4. Keeps polling the live stream, recording the primary key of every event for
//!    the snapshotted table whose position falls in `(low, high]`.
//! 5. Once the stream advances past the high watermark, emits snapshot `Read`
//!    events only for chunk rows whose primary key was **not** in that override set.
//!
//! The live stream passes through to the consumer unchanged in every phase, so the
//! consumer sees one continuous, gap-free feed with snapshot rows interleaved. No
//! long-held transaction is ever opened, so the source accumulates no transaction-ID
//! backlog and the stream never pauses.
//!
//! # Why the override window is required
//!
//! A chunk read is not atomic with respect to the stream. A row modified between the
//! two watermarks appears both in the chunk (at its pre-modification value) and in
//! the stream (at its post-modification value). Emitting both in chunk-then-stream
//! order would be harmless; emitting them in the order they are *produced* would
//! resurrect the stale value. Suppressing the chunk copy makes the outcome
//! independent of interleaving.

use std::collections::{HashSet, VecDeque};

use async_trait::async_trait;

use crate::{
    checkpoint::Checkpoint,
    core::{
        Error, Event, Offset, Operation, Result, SnapshotMetadata, SourceMetadata,
        EVENT_ENVELOPE_VERSION,
    },
    source::{
        IncrementalSnapshotConfig, IncrementalSnapshotState, IncrementalSnapshotTableState,
        StreamHandle,
    },
};

/// Emitted-event batch size when draining a merged chunk.
///
/// Bounds the memory a single `next_events` return can hold; the remainder of the
/// chunk is drained by subsequent calls.
const EMIT_BATCH_SIZE: usize = 1_000;

/// Upper bound on how long a single collect iteration waits on the inner stream.
///
/// The caller's timeout governs the call as a whole, but a collect iteration must
/// come back promptly enough to re-check the watermark against a quiet database.
const COLLECT_POLL_CEILING_MS: u64 = 100;

// ─── Backend contract ─────────────────────────────────────────────────────────

/// A table selected for snapshotting, resolved against the source's catalog.
#[derive(Debug, Clone)]
pub struct SnapshotTable {
    /// Schema (PostgreSQL/SQL Server) or database (MySQL) the table lives in.
    pub schema: String,
    /// Bare table name, as it appears in [`Event::table`].
    pub name: String,
    /// Quoted, fully qualified reference for interpolation into the backend's SQL.
    pub qualified: String,
    /// Primary-key column names, in key order.
    ///
    /// A table with no primary key cannot be chunked deterministically, so
    /// [`IncrementalSnapshotBackend::describe_table`] must reject it.
    pub pk_columns: Vec<String>,
    /// Backend-defined type names for `pk_columns`, or empty if the backend does
    /// not need them. PostgreSQL uses these to cast text-bound cursor values back
    /// to the column's real type.
    pub pk_types: Vec<String>,
    /// All column names in ordinal order, or empty if the backend does not need
    /// them. SQL Server uses these to build an explicit `FOR JSON PATH` projection,
    /// because `SELECT *` there yields no column names to key the JSON object by.
    pub columns: Vec<String>,
}

/// One row returned by a chunk read.
#[derive(Debug, Clone)]
pub struct ChunkRow {
    /// Primary-key values in `pk_columns` order, in whatever JSON form the backend
    /// wants persisted as the keyset cursor and handed back to `fetch_chunk`.
    pub cursor: Vec<serde_json::Value>,
    /// The full row payload, which becomes [`Event::after`].
    pub row: serde_json::Value,
}

/// The database-specific half of an incremental snapshot.
///
/// Implement this to give a connector non-blocking snapshots. The driver owns the
/// state machine, the watermark comparison, the override set, cursor persistence and
/// the [`StreamHandle`] contract; this trait supplies only what genuinely varies.
///
/// # Contract
///
/// - [`current_position`](Self::current_position) must be **monotonic** and comparable
///   against [`position_of_event`](Self::position_of_event) for the same source. If the
///   two use different scales the override window silently never matches, and stale
///   chunk rows are emitted over newer stream values.
/// - [`fetch_chunk`](Self::fetch_chunk) must read **outside a transaction** and order by
///   the primary key, returning at most `limit` rows strictly greater than `cursor`.
///   Holding a transaction open across chunks is the behaviour this whole design exists
///   to avoid.
/// - Rows must be returned in ascending primary-key order; the driver takes the last
///   row's `cursor` as the next starting point.
#[async_trait]
pub trait IncrementalSnapshotBackend: Send + Sync {
    /// Totally ordered stream position — an LSN, a binlog coordinate, a change
    /// sequence number.
    type Position: Ord + Clone + Send + Sync + std::fmt::Debug;

    /// Resolve a `"schema.table"` reference against the catalog.
    ///
    /// Must fail rather than return an empty `pk_columns`: chunking without a
    /// primary key cannot resume, and a snapshot that cannot resume re-reads the
    /// table from row zero on every restart.
    async fn describe_table(&mut self, table_ref: &str) -> Result<SnapshotTable>;

    /// Read the source's current stream position.
    ///
    /// Called twice per chunk for the watermarks, and again when the stream is quiet,
    /// so it must be cheap.
    async fn current_position(&mut self) -> Result<Self::Position>;

    /// Read up to `limit` rows of `table` beyond `cursor`, ordered by primary key.
    ///
    /// `cursor` is `None` for the first chunk of a table, and otherwise the `cursor`
    /// of the last row of the previous chunk — possibly one restored from a
    /// checkpoint written by an earlier process.
    async fn fetch_chunk(
        &mut self,
        table: &SnapshotTable,
        cursor: Option<&[serde_json::Value]>,
        limit: usize,
    ) -> Result<Vec<ChunkRow>>;

    /// Recover the stream position of a live event.
    ///
    /// `None` for an event that carries no usable position; such an event is passed
    /// through but cannot participate in the override window.
    fn position_of_event(&self, event: &Event) -> Option<Self::Position>;

    /// Render a position for logs. Defaults to the `Debug` form.
    fn render_position(&self, position: &Self::Position) -> String {
        format!("{position:?}")
    }

    /// Attach `state` to the inner stream's offset, producing the offset the driver
    /// checkpoints.
    ///
    /// Returning `None` falls back to the inner stream's own `save_position`, which
    /// **discards every chunk cursor** — correct only for a connector with no typed
    /// offset to carry the state in.
    fn offset_with_snapshot_state(
        &self,
        inner: &dyn Offset,
        state: IncrementalSnapshotState,
    ) -> Option<Box<dyn Offset>>;
}

// ─── Per-table progress ───────────────────────────────────────────────────────

struct TableProgress {
    spec: SnapshotTable,
    /// Keyset cursor: primary-key values of the last row of the last **fully
    /// delivered** chunk. `None` means the table has not started.
    ///
    /// This is the durable cursor: it appears in every checkpoint written while the
    /// snapshot is in flight (see [`super::super::StreamHandle::position_offset`]),
    /// so it must never run ahead of what the consumer has actually been handed.
    /// Advancing it at chunk *read* time silently lost the chunk on any restart
    /// before the chunk was emitted — see [`Phase::ChunkEmit::next_cursor`].
    pk_cursor: Option<Vec<serde_json::Value>>,
    is_complete: bool,
    chunks_emitted: u32,
    rows_emitted: u64,
}

impl TableProgress {
    /// `"schema.table"` — the key used in the persisted state, and the form the
    /// resume lookup matches on.
    fn key(&self) -> String {
        format!("{}.{}", self.spec.schema, self.spec.name)
    }
}

// ─── State machine ────────────────────────────────────────────────────────────

enum Phase<P> {
    /// Fetch the next chunk and capture its watermarks.
    ChunkPrepare { table_idx: usize },
    /// Chunk buffered; collecting stream events in `(low, high]`.
    ChunkCollect {
        table_idx: usize,
        low_watermark: P,
        high_watermark: P,
        /// Buffered chunk rows as `(pk_fingerprint, event)`.
        chunk_rows: Vec<(String, Event)>,
        override_pks: HashSet<String>,
        /// Cursor this chunk ends at, held back until the chunk is delivered.
        next_cursor: Vec<serde_json::Value>,
    },
    /// Merged snapshot events awaiting delivery.
    ChunkEmit {
        table_idx: usize,
        events: VecDeque<Event>,
        /// Cursor to promote into [`TableProgress::pk_cursor`] once `events` is empty.
        ///
        /// The whole reason this travels with the queue instead of being written at
        /// chunk-read time: the durable checkpoint embeds
        /// [`TableProgress::pk_cursor`] on **every** commit, including commits of the
        /// live stream events that flow past during `ChunkCollect`. A cursor written
        /// before its rows were handed to the consumer therefore became durable
        /// before those rows existed anywhere, and a restart resumed *after* them —
        /// silently dropping up to `chunk_size` rows from the snapshot, with no error
        /// and no counter to notice it by. Promoting it only once the queue drains
        /// costs at most one re-read of one chunk after a crash, which is the
        /// at-least-once behaviour the rest of the pipeline already documents.
        next_cursor: Vec<serde_json::Value>,
        /// Rows in this chunk, added to [`TableProgress::rows_emitted`] on promotion
        /// so the persisted counters stay consistent with the persisted cursor.
        row_count: u64,
    },
    /// Every table complete; the driver is a pure stream delegate.
    Done,
}

// ─── Fingerprints ─────────────────────────────────────────────────────────────

/// Stable identity for a row within a table, used to match chunk rows against
/// stream events in the override window.
///
/// Chunk rows and stream events both derive this from the *row payload* via
/// [`fingerprint_from_payload`], so the two sides agree by construction. Deriving
/// the chunk side from the keyset cursor instead would let a backend that binds
/// cursor values as text disagree with a stream event carrying the same key as a
/// JSON number — the override would silently never match.
fn pk_fingerprint(table: &str, values: &[serde_json::Value]) -> String {
    let rendered = serde_json::to_string(values).unwrap_or_else(|_| {
        values
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    });
    format!("{table}|{rendered}")
}

/// Fingerprint a row payload by reading `pk_columns` out of it.
fn fingerprint_from_payload(
    table: &str,
    pk_columns: &[String],
    payload: &serde_json::Value,
) -> String {
    let values: Vec<serde_json::Value> = pk_columns
        .iter()
        .map(|column| {
            payload
                .get(column)
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        })
        .collect();
    pk_fingerprint(table, &values)
}

/// Fingerprint a live stream event, or `None` if it carries no usable key.
fn event_fingerprint(event: &Event) -> Option<String> {
    let pk_columns = event.primary_key.as_ref()?;
    if pk_columns.is_empty() {
        return None;
    }
    let payload = event.after.as_ref().or(event.before.as_ref())?;
    Some(fingerprint_from_payload(&event.table, pk_columns, payload))
}

// ─── Driver ───────────────────────────────────────────────────────────────────

/// A [`StreamHandle`] that interleaves chunk reads with the live stream using the
/// DBLog watermark pattern, driven by a connector-supplied
/// [`IncrementalSnapshotBackend`].
pub struct IncrementalSnapshotDriver<B: IncrementalSnapshotBackend> {
    backend: B,
    inner: Box<dyn StreamHandle>,
    tables: Vec<TableProgress>,
    phase: Phase<B::Position>,
    chunk_size: usize,
    source_name: String,
    snapshot_id: String,
    /// Events handed to the caller, for `save_position` accounting.
    events_emitted: u64,
}

impl<B: IncrementalSnapshotBackend> IncrementalSnapshotDriver<B> {
    /// Build a driver, resolving every configured table eagerly so a bad table
    /// reference fails at startup rather than midway through a snapshot.
    ///
    /// `resume` restores per-table cursors from a checkpoint. Without it every
    /// restart re-reads each table from row zero — a duplicate flood proportional to
    /// the dataset, repeating until a snapshot completes inside one process lifetime.
    pub async fn new(
        mut backend: B,
        inner: Box<dyn StreamHandle>,
        config: IncrementalSnapshotConfig,
        source_name: String,
        resume: Option<IncrementalSnapshotState>,
    ) -> Result<Self> {
        let mut tables = Vec::with_capacity(config.tables.len());
        for table_ref in &config.tables {
            let spec = backend.describe_table(table_ref).await?;
            if spec.pk_columns.is_empty() {
                return Err(Error::ConfigError(format!(
                    "incremental snapshot: table '{}.{}' must have a primary key",
                    spec.schema, spec.name
                )));
            }
            let key = format!("{}.{}", spec.schema, spec.name);
            let persisted = resume.as_ref().and_then(|state| state.table(&key));
            // A cursor whose arity no longer matches the primary key cannot be
            // resumed from: continuing would skip every row between the truncated
            // position and the real one, permanently and without an error. Checked
            // here so every backend gets it, rather than in each backend's chunk read
            // where two of the three used to forget.
            if let Some(cursor) = persisted.and_then(|entry| entry.pk_cursor.as_ref()) {
                if cursor.len() != spec.pk_columns.len() {
                    return Err(Error::CheckpointError(format!(
                        "incremental snapshot: persisted keyset cursor for '{key}' has {} \
                         value(s) but the table's primary key has {} column(s). The primary key \
                         changed since the checkpoint was written; restart the snapshot with a \
                         fresh checkpoint directory rather than resuming from an incompatible \
                         cursor",
                        cursor.len(),
                        spec.pk_columns.len()
                    )));
                }
            }
            tables.push(TableProgress {
                pk_cursor: persisted.and_then(|entry| entry.pk_cursor.clone()),
                is_complete: persisted.is_some_and(|entry| entry.is_complete),
                chunks_emitted: persisted.map_or(0, |entry| entry.chunks_emitted),
                rows_emitted: persisted.map_or(0, |entry| entry.rows_emitted),
                spec,
            });
        }

        let phase = match tables.iter().position(|table| !table.is_complete) {
            Some(table_idx) => Phase::ChunkPrepare { table_idx },
            None => Phase::Done,
        };

        // Keep the snapshot id stable across restarts so a consumer correlating rows
        // by `snapshot_id` sees one snapshot, not one per process lifetime.
        let snapshot_id = resume
            .as_ref()
            .map(|state| state.snapshot_id.clone())
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| format!("incremental-{}", crate::source::helpers::now_millis()));

        if let Some(state) = resume.as_ref() {
            tracing::info!(
                target: "rustcdc::source::incremental_snapshot",
                snapshot_id = %snapshot_id,
                tables_total = tables.len(),
                tables_complete = state.tables.iter().filter(|table| table.is_complete).count(),
                rows_already_emitted = state.tables.iter().map(|table| table.rows_emitted).sum::<u64>(),
                "incremental snapshot resumed from checkpoint",
            );
        }

        Ok(Self {
            backend,
            inner,
            tables,
            phase,
            chunk_size: config.chunk_size.max(1),
            source_name,
            snapshot_id,
            events_emitted: 0,
        })
    }

    /// Durable per-table progress for the checkpoint record.
    fn snapshot_state(&self) -> IncrementalSnapshotState {
        IncrementalSnapshotState {
            snapshot_id: self.snapshot_id.clone(),
            tables: self
                .tables
                .iter()
                .map(|table| IncrementalSnapshotTableState {
                    table: table.key(),
                    pk_cursor: table.pk_cursor.clone(),
                    is_complete: table.is_complete,
                    chunks_emitted: table.chunks_emitted,
                    rows_emitted: table.rows_emitted,
                })
                .collect(),
        }
    }

    fn build_snapshot_event(
        &self,
        table_idx: usize,
        fingerprint: &str,
        row: serde_json::Value,
        chunk_index: u32,
    ) -> Event {
        let table = &self.tables[table_idx].spec;
        let now = crate::source::helpers::now_millis();
        Event {
            before: None,
            after: Some(row),
            op: Operation::Read,
            source: SourceMetadata {
                source_name: self.source_name.clone(),
                // Synthetic, stable across restarts, and identifies the row rather
                // than a log position — a snapshot read has no log position.
                offset: format!("incremental:{}:{}", table.qualified, fingerprint),
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
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    /// Fetch the next chunk and enter `ChunkCollect`, or complete the table.
    async fn drive_chunk_prepare(&mut self) -> Result<()> {
        let Phase::ChunkPrepare { table_idx } = self.phase else {
            return Ok(());
        };

        let Some(table_idx) = (table_idx..self.tables.len()).find(|&i| !self.tables[i].is_complete)
        else {
            self.phase = Phase::Done;
            return Ok(());
        };

        // Watermarks bracket the read: any event between them may have superseded a
        // row the read returned.
        let low_watermark = self.backend.current_position().await?;
        let rows = {
            let spec = self.tables[table_idx].spec.clone();
            let cursor = self.tables[table_idx].pk_cursor.clone();
            self.backend
                .fetch_chunk(&spec, cursor.as_deref(), self.chunk_size)
                .await?
        };
        let high_watermark = self.backend.current_position().await?;

        if rows.is_empty() {
            self.tables[table_idx].is_complete = true;
            tracing::debug!(
                target: "rustcdc::source::incremental_snapshot",
                table = %self.tables[table_idx].spec.qualified,
                chunks = self.tables[table_idx].chunks_emitted,
                rows = self.tables[table_idx].rows_emitted,
                "incremental snapshot: table complete",
            );
            let next = (table_idx + 1..self.tables.len()).find(|&i| !self.tables[i].is_complete);
            self.phase = match next {
                Some(idx) => Phase::ChunkPrepare { table_idx: idx },
                None => Phase::Done,
            };
            return Ok(());
        }

        // Held back, not applied: see `Phase::ChunkEmit::next_cursor`. The fetch above
        // still starts from `pk_cursor`, which stays pinned to the last fully delivered
        // chunk until this one has been handed to the consumer.
        let next_cursor = rows
            .last()
            .map(|row| row.cursor.clone())
            .unwrap_or_default();

        let chunk_index = self.tables[table_idx].chunks_emitted;
        let table_name = self.tables[table_idx].spec.name.clone();
        let pk_columns = self.tables[table_idx].spec.pk_columns.clone();
        let chunk_rows: Vec<(String, Event)> = rows
            .into_iter()
            .map(|row| {
                let fingerprint = fingerprint_from_payload(&table_name, &pk_columns, &row.row);
                let event =
                    self.build_snapshot_event(table_idx, &fingerprint, row.row, chunk_index);
                (fingerprint, event)
            })
            .collect();

        tracing::debug!(
            target: "rustcdc::source::incremental_snapshot",
            table = %self.tables[table_idx].spec.qualified,
            chunk = chunk_index,
            rows = chunk_rows.len(),
            low_watermark = %self.backend.render_position(&low_watermark),
            high_watermark = %self.backend.render_position(&high_watermark),
            "incremental snapshot: chunk read, entering collect phase",
        );

        self.phase = Phase::ChunkCollect {
            table_idx,
            low_watermark,
            high_watermark,
            chunk_rows,
            override_pks: HashSet::new(),
            next_cursor,
        };
        Ok(())
    }

    /// Merge the chunk against the override set and enter `ChunkEmit`.
    fn finalize_collect(&mut self) {
        let Phase::ChunkCollect {
            table_idx,
            ref chunk_rows,
            ref override_pks,
            ref next_cursor,
            ..
        } = self.phase
        else {
            return;
        };

        let merged: VecDeque<Event> = chunk_rows
            .iter()
            .filter(|(fingerprint, _)| !override_pks.contains(fingerprint))
            .map(|(_, event)| event.clone())
            .collect();
        let suppressed = override_pks.len();
        let next_cursor = next_cursor.clone();

        let emitted = merged.len() as u64;

        tracing::debug!(
            target: "rustcdc::source::incremental_snapshot",
            table = %self.tables[table_idx].spec.qualified,
            chunk = self.tables[table_idx].chunks_emitted,
            emitted,
            suppressed,
            "incremental snapshot: chunk merged, entering emit phase",
        );

        self.phase = Phase::ChunkEmit {
            table_idx,
            events: merged,
            next_cursor,
            row_count: emitted,
        };
    }

    /// Promote a fully delivered chunk's cursor and counters into durable progress.
    ///
    /// Called only once the emit queue is empty, so the cursor that becomes durable
    /// always describes rows the consumer has already been handed.
    fn commit_chunk_progress(
        &mut self,
        table_idx: usize,
        next_cursor: Vec<serde_json::Value>,
        row_count: u64,
    ) {
        let table = &mut self.tables[table_idx];
        if !next_cursor.is_empty() {
            table.pk_cursor = Some(next_cursor);
        }
        table.chunks_emitted = table.chunks_emitted.saturating_add(1);
        table.rows_emitted = table.rows_emitted.saturating_add(row_count);
    }

    /// Advance the state machine by one step.
    async fn drive(&mut self, timeout_ms: u64) -> Result<Vec<Event>> {
        loop {
            match self.phase {
                Phase::Done => return self.inner.next_events(timeout_ms).await,

                Phase::ChunkEmit {
                    table_idx,
                    ref mut events,
                    ref mut next_cursor,
                    row_count,
                } => {
                    let batch_size = events.len().min(EMIT_BATCH_SIZE);
                    if batch_size > 0 {
                        return Ok(events.drain(..batch_size).collect());
                    }
                    // Queue drained: the whole chunk is now in the consumer's hands, so
                    // its cursor may finally become durable.
                    let next_cursor = std::mem::take(next_cursor);
                    self.commit_chunk_progress(table_idx, next_cursor, row_count);
                    // Try the next chunk of the same table. `drive_chunk_prepare`
                    // detects completion via an empty read.
                    self.phase = Phase::ChunkPrepare { table_idx };
                }

                Phase::ChunkPrepare { .. } => self.drive_chunk_prepare().await?,

                Phase::ChunkCollect { .. } => {
                    // Bounded so a quiet database still lets us re-check the
                    // watermark rather than blocking for the caller's full timeout.
                    let stream_events = self
                        .inner
                        .next_events(timeout_ms.min(COLLECT_POLL_CEILING_MS))
                        .await?;

                    let Phase::ChunkCollect {
                        table_idx,
                        ref low_watermark,
                        ref high_watermark,
                        ..
                    } = self.phase
                    else {
                        unreachable!("phase cannot change while borrowed")
                    };
                    let low_watermark = low_watermark.clone();
                    let high_watermark = high_watermark.clone();
                    let target_table = self.tables[table_idx].spec.name.clone();
                    let target_schema = self.tables[table_idx].spec.schema.clone();

                    let mut max_batch_position: Option<B::Position> = None;
                    for event in &stream_events {
                        let Some(position) = self.backend.position_of_event(event) else {
                            continue;
                        };
                        if max_batch_position
                            .as_ref()
                            .is_none_or(|current| position > *current)
                        {
                            max_batch_position = Some(position.clone());
                        }

                        // An event supersedes a chunk row only if it targets the same
                        // table and landed strictly inside the bracket.
                        let same_table = event.table == target_table
                            && event.schema.as_deref().unwrap_or(&target_schema) == target_schema;
                        if same_table && position > low_watermark && position <= high_watermark {
                            if let Some(fingerprint) = event_fingerprint(event) {
                                if let Phase::ChunkCollect {
                                    ref mut override_pks,
                                    ..
                                } = self.phase
                                {
                                    override_pks.insert(fingerprint);
                                }
                            }
                        }
                    }

                    let watermark_passed = match max_batch_position {
                        Some(ref position) if *position >= high_watermark => true,
                        // No positioned events this round: ask the source directly
                        // rather than waiting for an event that may never come.
                        _ if stream_events.is_empty() => {
                            self.backend.current_position().await? >= high_watermark
                        }
                        _ => false,
                    };

                    if watermark_passed {
                        self.finalize_collect();
                    }

                    // Stream events go out first so the consumer stays current;
                    // snapshot rows follow on the next call.
                    if !stream_events.is_empty() {
                        return Ok(stream_events);
                    }
                    if watermark_passed {
                        continue;
                    }
                    return Ok(Vec::new());
                }
            }
        }
    }
}

#[async_trait]
impl<B: IncrementalSnapshotBackend + 'static> StreamHandle for IncrementalSnapshotDriver<B> {
    async fn next_events(&mut self, timeout_ms: u64) -> Result<Vec<Event>> {
        let events = self.drive(timeout_ms).await?;
        self.events_emitted = self.events_emitted.saturating_add(events.len() as u64);
        Ok(events)
    }

    async fn save_position(&self, checkpoint: &mut dyn Checkpoint) -> Result<()> {
        // Delegating to the inner stream would persist the log position while
        // dropping every chunk cursor, so an orderly shutdown would forfeit exactly
        // the progress this method exists to preserve.
        let Some(offset) = self.position_offset() else {
            return self.inner.save_position(checkpoint).await;
        };
        checkpoint.save(offset.as_ref(), self.events_emitted).await
    }

    fn position_offset(&self) -> Option<Box<dyn Offset>> {
        let inner = self.inner.position_offset()?;
        self.backend
            .offset_with_snapshot_state(inner.as_ref(), self.snapshot_state())
    }

    fn incremental_snapshot_state(&self) -> Option<IncrementalSnapshotState> {
        Some(self.snapshot_state())
    }

    async fn requeue_events(&mut self, events: Vec<Event>) -> Result<()> {
        self.inner.requeue_events(events).await
    }

    async fn confirm_lsn(&mut self, lsn: u64) -> Result<()> {
        self.inner.confirm_lsn(lsn).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event_with_key(table: &str, id: i64) -> Event {
        Event {
            before: None,
            after: Some(json!({ "id": id, "name": "x" })),
            op: Operation::Update,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "0".into(),
                timestamp: 0,
            },
            ts: 0,
            schema: Some("public".into()),
            table: table.into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    #[test]
    fn a_chunk_row_and_a_stream_event_for_the_same_key_fingerprint_identically() {
        // This is the load-bearing invariant of the override window. If the two sides
        // disagree the suppression silently never fires and stale chunk rows are
        // emitted over newer stream values — with no error anywhere.
        let chunk = fingerprint_from_payload(
            "users",
            &["id".to_string()],
            &json!({ "id": 7, "name": "x" }),
        );
        let stream = event_fingerprint(&event_with_key("users", 7)).expect("event has a key");
        assert_eq!(chunk, stream);
    }

    #[test]
    fn fingerprints_are_scoped_to_the_table() {
        let users = fingerprint_from_payload("users", &["id".to_string()], &json!({ "id": 1 }));
        let orders = fingerprint_from_payload("orders", &["id".to_string()], &json!({ "id": 1 }));
        assert_ne!(
            users, orders,
            "the same key in two tables must not collide in one override set"
        );
    }

    #[test]
    fn a_composite_key_fingerprints_in_column_order() {
        let columns = vec!["tenant".to_string(), "id".to_string()];
        let payload = json!({ "id": 2, "tenant": "acme", "other": 9 });
        assert_eq!(
            fingerprint_from_payload("t", &columns, &payload),
            pk_fingerprint("t", &[json!("acme"), json!(2)]),
            "column order must follow the primary key, not the payload's key order"
        );
    }

    #[test]
    fn a_missing_key_column_fingerprints_as_null_rather_than_being_skipped() {
        // Silently shortening the vector would make a two-column key collide with a
        // one-column key holding the same first value.
        assert_eq!(
            fingerprint_from_payload("t", &["a".into(), "b".into()], &json!({ "a": 1 })),
            pk_fingerprint("t", &[json!(1), serde_json::Value::Null]),
        );
    }

    #[test]
    fn an_event_without_a_primary_key_has_no_fingerprint() {
        let mut event = event_with_key("users", 1);
        event.primary_key = None;
        assert!(event_fingerprint(&event).is_none());

        let mut empty = event_with_key("users", 1);
        empty.primary_key = Some(Vec::new());
        assert!(event_fingerprint(&empty).is_none());
    }

    #[test]
    fn a_delete_event_fingerprints_from_its_before_image() {
        let mut event = event_with_key("users", 5);
        event.before = event.after.take();
        event.op = Operation::Delete;
        assert_eq!(
            event_fingerprint(&event).expect("delete carries a before image"),
            fingerprint_from_payload("users", &["id".to_string()], &json!({ "id": 5 })),
        );
    }
    // ─── Driver state-machine tests ───────────────────────────────────────────
    //
    // These drive the real state machine through a fake backend and a fake inner
    // stream. Each connector used to carry its own partial re-implementation of
    // these assertions, which tested the test rather than the driver.

    use crate::source::IncrementalSnapshotConfig;
    use std::sync::{Arc, Mutex};

    /// Inner stream that yields pre-programmed batches, then nothing.
    struct FakeStream {
        batches: VecDeque<Vec<Event>>,
    }

    #[async_trait]
    impl StreamHandle for FakeStream {
        async fn next_events(&mut self, _timeout_ms: u64) -> Result<Vec<Event>> {
            Ok(self.batches.pop_front().unwrap_or_default())
        }
        async fn save_position(&self, _checkpoint: &mut dyn Checkpoint) -> Result<()> {
            Ok(())
        }
        fn position_offset(&self) -> Option<Box<dyn Offset>> {
            None
        }
        async fn confirm_lsn(&mut self, _lsn: u64) -> Result<()> {
            Ok(())
        }
    }

    /// Backend with an in-memory table and a caller-controlled clock.
    struct FakeBackend {
        rows: Vec<serde_json::Value>,
        /// Position returned by `current_position`.
        clock: Arc<Mutex<u64>>,
        /// How far the clock advances during a chunk read.
        ///
        /// This is what opens the watermark bracket: the low watermark is taken
        /// before the read and the high watermark after it, so a zero advance makes
        /// the window empty and nothing can ever be superseded.
        advance_on_read: u64,
        /// Every chunk read, recorded so a test can assert on pagination.
        reads: Arc<Mutex<Vec<Option<Vec<serde_json::Value>>>>>,
    }

    #[async_trait]
    impl IncrementalSnapshotBackend for FakeBackend {
        type Position = u64;

        async fn describe_table(&mut self, _table_ref: &str) -> Result<SnapshotTable> {
            Ok(SnapshotTable {
                schema: "public".into(),
                name: "users".into(),
                qualified: "\"public\".\"users\"".into(),
                pk_columns: vec!["id".into()],
                pk_types: vec!["bigint".into()],
                columns: vec!["id".into()],
            })
        }

        async fn current_position(&mut self) -> Result<u64> {
            Ok(*self.clock.lock().expect("clock lock"))
        }

        async fn fetch_chunk(
            &mut self,
            _table: &SnapshotTable,
            cursor: Option<&[serde_json::Value]>,
            limit: usize,
        ) -> Result<Vec<ChunkRow>> {
            self.reads
                .lock()
                .expect("reads lock")
                .push(cursor.map(<[serde_json::Value]>::to_vec));
            *self.clock.lock().expect("clock lock") += self.advance_on_read;
            let after = cursor
                .and_then(|values| values.first().cloned())
                .and_then(|value| value.as_i64())
                .unwrap_or(i64::MIN);
            Ok(self
                .rows
                .iter()
                .filter(|row| row["id"].as_i64().unwrap_or_default() > after)
                .take(limit)
                .map(|row| ChunkRow {
                    cursor: vec![row["id"].clone()],
                    row: row.clone(),
                })
                .collect())
        }

        fn position_of_event(&self, event: &Event) -> Option<u64> {
            event.source.offset.parse().ok()
        }

        fn offset_with_snapshot_state(
            &self,
            _inner: &dyn Offset,
            _state: IncrementalSnapshotState,
        ) -> Option<Box<dyn Offset>> {
            None
        }
    }

    fn stream_event_at(table: &str, id: i64, position: u64) -> Event {
        let mut event = event_with_key(table, id);
        event.source.offset = position.to_string();
        event
    }

    async fn driver_with(
        rows: Vec<serde_json::Value>,
        batches: Vec<Vec<Event>>,
        advance_on_read: u64,
        chunk_size: usize,
    ) -> (
        IncrementalSnapshotDriver<FakeBackend>,
        Arc<Mutex<Vec<Option<Vec<serde_json::Value>>>>>,
    ) {
        let reads = Arc::new(Mutex::new(Vec::new()));
        let backend = FakeBackend {
            rows,
            clock: Arc::new(Mutex::new(100)),
            advance_on_read,
            reads: Arc::clone(&reads),
        };
        let inner = Box::new(FakeStream {
            batches: batches.into_iter().collect(),
        });
        let mut config = IncrementalSnapshotConfig::new(vec!["public.users".to_string()]);
        config.chunk_size = chunk_size;
        let driver =
            IncrementalSnapshotDriver::new(backend, inner, config, "test".to_string(), None)
                .await
                .expect("driver builds");
        (driver, reads)
    }

    /// Drain the driver until it has produced no events for two consecutive calls.
    async fn drain(driver: &mut IncrementalSnapshotDriver<FakeBackend>) -> Vec<Event> {
        let mut all = Vec::new();
        let mut quiet = 0;
        for _ in 0..200 {
            let batch = driver.next_events(10).await.expect("drive");
            if batch.is_empty() {
                quiet += 1;
                if quiet == 2 {
                    break;
                }
            } else {
                quiet = 0;
                all.extend(batch);
            }
        }
        all
    }

    #[tokio::test]
    async fn a_quiet_database_still_completes_the_snapshot() {
        // With no stream events at all, the watermark can only be cleared by asking
        // the source directly. Without that fallback the driver waits forever for an
        // event that is never coming.
        let rows = vec![json!({ "id": 1 }), json!({ "id": 2 }), json!({ "id": 3 })];
        let (mut driver, _) = driver_with(rows, vec![], 0, 10).await;

        let emitted = drain(&mut driver).await;
        assert_eq!(emitted.len(), 3, "every row must be emitted");
        assert!(emitted.iter().all(|event| event.op == Operation::Read));
    }

    #[tokio::test]
    async fn a_stream_event_inside_the_watermark_window_suppresses_its_chunk_row() {
        // The row was modified between the two watermarks, so the chunk copy is stale.
        // Emitting it would resurrect the pre-modification value.
        // The read advances the clock 100 -> 200, so the bracket is (100, 200] and
        // the event at 150 lands strictly inside it.
        let rows = vec![json!({ "id": 1 }), json!({ "id": 2 })];
        let batches = vec![vec![stream_event_at("users", 2, 150)]];
        let (mut driver, _) = driver_with(rows, batches, 100, 10).await;

        let emitted = drain(&mut driver).await;
        let snapshot_ids: Vec<i64> = emitted
            .iter()
            .filter(|event| event.op == Operation::Read)
            .map(|event| {
                event.after.as_ref().expect("row")["id"]
                    .as_i64()
                    .expect("id")
            })
            .collect();
        assert_eq!(
            snapshot_ids,
            vec![1],
            "row 2 was superseded inside the window and must not be emitted as a snapshot read"
        );
        assert!(
            emitted.iter().any(|event| event.op == Operation::Update),
            "the live event itself must still pass through to the consumer"
        );
    }

    #[tokio::test]
    async fn a_stream_event_outside_the_window_does_not_suppress_anything() {
        // An event at or before the low watermark is already reflected in the chunk
        // read, so suppressing the chunk row would drop the row entirely.
        let rows = vec![json!({ "id": 1 }), json!({ "id": 2 })];
        let batches = vec![vec![stream_event_at("users", 2, 100)]];
        let (mut driver, _) = driver_with(rows, batches, 100, 10).await;

        let emitted = drain(&mut driver).await;
        let snapshot_ids: Vec<i64> = emitted
            .iter()
            .filter(|event| event.op == Operation::Read)
            .map(|event| {
                event.after.as_ref().expect("row")["id"]
                    .as_i64()
                    .expect("id")
            })
            .collect();
        assert_eq!(snapshot_ids, vec![1, 2], "no row may be dropped");
    }

    #[tokio::test]
    async fn an_event_for_a_different_table_never_suppresses_a_chunk_row() {
        let rows = vec![json!({ "id": 1 })];
        let batches = vec![vec![stream_event_at("orders", 1, 150)]];
        let (mut driver, _) = driver_with(rows, batches, 100, 10).await;

        let emitted = drain(&mut driver).await;
        assert_eq!(
            emitted.iter().filter(|e| e.op == Operation::Read).count(),
            1,
            "an unrelated table's event must not suppress this table's row"
        );
    }

    #[tokio::test]
    async fn chunks_paginate_forward_and_never_re_read_the_same_cursor() {
        let rows = (1..=5).map(|id| json!({ "id": id })).collect();
        let (mut driver, reads) = driver_with(rows, vec![], 0, 2).await;

        let emitted = drain(&mut driver).await;
        assert_eq!(emitted.len(), 5, "every row exactly once");

        let cursors = reads.lock().expect("reads").clone();
        assert_eq!(cursors[0], None, "the first read starts from the beginning");
        let advanced: Vec<i64> = cursors
            .iter()
            .skip(1)
            .filter_map(|cursor| cursor.as_ref()?.first()?.as_i64())
            .collect();
        assert_eq!(
            advanced,
            vec![2, 4, 5],
            "each read must resume strictly after the previous chunk's last key"
        );
    }

    #[tokio::test]
    async fn a_checkpoint_taken_mid_chunk_does_not_skip_the_undelivered_chunk() {
        // The durable checkpoint embeds `incremental_snapshot_state()` on *every*
        // commit, including commits of the live stream events that flow past while a
        // chunk sits in the collect phase. So the cursor must not move when the chunk
        // is *read* — only when it has been handed to the consumer. It used to move at
        // read time, which made a restart resume after rows that were never emitted:
        // up to `chunk_size` rows silently missing from the snapshot, with no error and
        // no counter to notice it by.
        let rows: Vec<serde_json::Value> = (1..=6).map(|id| json!({ "id": id })).collect();

        // `advance_on_read = 10` opens a watermark bracket of (100, 110]; the stream
        // event at 105 lands inside it, so the watermark is not passed and the driver
        // stays in the collect phase holding the unemitted chunk.
        let (mut driver, _) = driver_with(
            rows.clone(),
            vec![vec![stream_event_at("other", 99, 105)]],
            10,
            3,
        )
        .await;

        let first = driver.next_events(10).await.expect("drive");
        assert!(
            first.iter().all(|event| event.snapshot.is_none()),
            "the chunk must still be undelivered at this point, so only the live \
             stream event may have been returned"
        );

        // This is the state a commit of that stream event makes durable.
        let mid_chunk = driver
            .incremental_snapshot_state()
            .expect("driver reports snapshot state");
        let table = mid_chunk.table("public.users").expect("table state");
        assert_eq!(
            table.pk_cursor, None,
            "a chunk that has not been delivered must not have advanced the durable \
             cursor: {:?}",
            table.pk_cursor
        );

        // Now prove the end-to-end property: restarting from that checkpoint still
        // yields every row.
        let reads = Arc::new(Mutex::new(Vec::new()));
        let mut resumed = IncrementalSnapshotDriver::new(
            FakeBackend {
                rows,
                clock: Arc::new(Mutex::new(100)),
                advance_on_read: 0,
                reads,
            },
            Box::new(FakeStream {
                batches: VecDeque::new(),
            }),
            IncrementalSnapshotConfig::new(vec!["public.users".to_string()])
                .with_chunk_size(3),
            "test".to_string(),
            Some(mid_chunk),
        )
        .await
        .expect("driver builds");

        let mut ids: Vec<i64> = drain(&mut resumed)
            .await
            .iter()
            .filter(|event| event.snapshot.is_some())
            .map(|event| {
                event.after.as_ref().expect("row")["id"]
                    .as_i64()
                    .expect("id")
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![1, 2, 3, 4, 5, 6],
            "a restart from a mid-chunk checkpoint must re-read the undelivered chunk \
             rather than skip it"
        );
    }

    #[tokio::test]
    async fn a_delivered_chunk_advances_the_durable_cursor_and_its_counters() {
        // The other half of the contract: holding the cursor back must not turn into
        // never advancing it, which would re-read chunk one forever.
        let rows: Vec<serde_json::Value> = (1..=4).map(|id| json!({ "id": id })).collect();
        let (mut driver, _) = driver_with(rows, vec![], 0, 2).await;

        // Drive until the first chunk has been fully handed over.
        let first = driver.next_events(10).await.expect("drive");
        assert_eq!(first.len(), 2, "the first chunk is delivered in one batch");

        // The cursor is promoted when the emit queue drains, which happens on the next
        // call as the driver moves on to chunk two.
        let _ = driver.next_events(10).await.expect("drive");
        let state = driver
            .incremental_snapshot_state()
            .expect("driver reports snapshot state");
        let table = state.table("public.users").expect("table state");
        assert_eq!(
            table.pk_cursor,
            Some(vec![json!(2)]),
            "a delivered chunk must advance the durable cursor to its last key"
        );
        assert_eq!(table.chunks_emitted, 1);
        assert_eq!(table.rows_emitted, 2);
    }

    #[tokio::test]
    async fn resuming_from_a_persisted_cursor_skips_the_rows_already_emitted() {
        // This is the C1 regression, now covered once for every connector.
        let rows: Vec<serde_json::Value> = (1..=5).map(|id| json!({ "id": id })).collect();
        let reads = Arc::new(Mutex::new(Vec::new()));
        let backend = FakeBackend {
            rows,
            clock: Arc::new(Mutex::new(100)),
            advance_on_read: 0,
            reads: Arc::clone(&reads),
        };
        let resume = IncrementalSnapshotState {
            snapshot_id: "incremental-earlier-run".to_string(),
            tables: vec![IncrementalSnapshotTableState {
                table: "public.users".to_string(),
                pk_cursor: Some(vec![json!(3)]),
                is_complete: false,
                chunks_emitted: 2,
                rows_emitted: 3,
            }],
        };
        let mut driver = IncrementalSnapshotDriver::new(
            backend,
            Box::new(FakeStream {
                batches: VecDeque::new(),
            }),
            IncrementalSnapshotConfig::new(vec!["public.users".to_string()]),
            "test".to_string(),
            Some(resume),
        )
        .await
        .expect("driver builds");

        let emitted = drain(&mut driver).await;
        let ids: Vec<i64> = emitted
            .iter()
            .map(|event| {
                event.after.as_ref().expect("row")["id"]
                    .as_i64()
                    .expect("id")
            })
            .collect();
        assert_eq!(
            ids,
            vec![4, 5],
            "a resumed snapshot must continue from the cursor, not restart the table"
        );
        assert_eq!(
            driver.snapshot_id, "incremental-earlier-run",
            "the snapshot id must survive the restart so a consumer sees one snapshot"
        );
    }

    #[tokio::test]
    async fn a_table_marked_complete_in_the_checkpoint_is_not_read_again() {
        let reads = Arc::new(Mutex::new(Vec::new()));
        let backend = FakeBackend {
            rows: vec![json!({ "id": 1 })],
            clock: Arc::new(Mutex::new(100)),
            advance_on_read: 0,
            reads: Arc::clone(&reads),
        };
        let resume = IncrementalSnapshotState {
            snapshot_id: "done".to_string(),
            tables: vec![IncrementalSnapshotTableState {
                table: "public.users".to_string(),
                pk_cursor: Some(vec![json!(1)]),
                is_complete: true,
                chunks_emitted: 1,
                rows_emitted: 1,
            }],
        };
        let mut driver = IncrementalSnapshotDriver::new(
            backend,
            Box::new(FakeStream {
                batches: VecDeque::new(),
            }),
            IncrementalSnapshotConfig::new(vec!["public.users".to_string()]),
            "test".to_string(),
            Some(resume),
        )
        .await
        .expect("driver builds");

        assert!(drain(&mut driver).await.is_empty());
        assert!(
            reads.lock().expect("reads").is_empty(),
            "a completed table must not be read at all"
        );
    }

    #[tokio::test]
    async fn a_table_without_a_primary_key_is_rejected_at_construction() {
        struct KeylessBackend;

        #[async_trait]
        impl IncrementalSnapshotBackend for KeylessBackend {
            type Position = u64;
            async fn describe_table(&mut self, _table_ref: &str) -> Result<SnapshotTable> {
                Ok(SnapshotTable {
                    schema: "public".into(),
                    name: "logs".into(),
                    qualified: "public.logs".into(),
                    pk_columns: Vec::new(),
                    pk_types: Vec::new(),
                    columns: Vec::new(),
                })
            }
            async fn current_position(&mut self) -> Result<u64> {
                Ok(0)
            }
            async fn fetch_chunk(
                &mut self,
                _table: &SnapshotTable,
                _cursor: Option<&[serde_json::Value]>,
                _limit: usize,
            ) -> Result<Vec<ChunkRow>> {
                Ok(Vec::new())
            }
            fn position_of_event(&self, _event: &Event) -> Option<u64> {
                None
            }
            fn offset_with_snapshot_state(
                &self,
                _inner: &dyn Offset,
                _state: IncrementalSnapshotState,
            ) -> Option<Box<dyn Offset>> {
                None
            }
        }

        let Err(error) = IncrementalSnapshotDriver::new(
            KeylessBackend,
            Box::new(FakeStream {
                batches: VecDeque::new(),
            }),
            IncrementalSnapshotConfig::new(vec!["public.logs".to_string()]),
            "test".to_string(),
            None,
        )
        .await
        else {
            panic!("a keyless table cannot be chunked");
        };
        assert!(
            error.to_string().contains("must have a primary key"),
            "got: {error}"
        );
    }
    #[tokio::test]
    async fn a_persisted_cursor_that_no_longer_matches_the_primary_key_is_rejected() {
        // Hoisted out of the connectors: two of the three used to skip this check, so
        // a primary-key change silently resumed from a truncated cursor and skipped
        // every row in between.
        let resume = IncrementalSnapshotState {
            snapshot_id: "x".to_string(),
            tables: vec![IncrementalSnapshotTableState {
                table: "public.users".to_string(),
                pk_cursor: Some(vec![json!(1), json!(2)]),
                is_complete: false,
                chunks_emitted: 1,
                rows_emitted: 1,
            }],
        };
        let Err(error) = IncrementalSnapshotDriver::new(
            FakeBackend {
                rows: vec![json!({ "id": 1 })],
                clock: Arc::new(Mutex::new(100)),
                advance_on_read: 0,
                reads: Arc::new(Mutex::new(Vec::new())),
            },
            Box::new(FakeStream {
                batches: VecDeque::new(),
            }),
            IncrementalSnapshotConfig::new(vec!["public.users".to_string()]),
            "test".to_string(),
            Some(resume),
        )
        .await
        else {
            panic!("an incompatible cursor must be rejected");
        };
        assert!(
            error.to_string().contains("primary key changed"),
            "the error must name the cause and the remedy, got: {error}"
        );
    }
}
