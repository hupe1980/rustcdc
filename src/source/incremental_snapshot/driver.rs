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
//! 2. Captures the set of **transactions still in flight**, which closes the
//!    commit-visibility race described below.
//! 3. Reads `chunk_size` rows using keyset pagination, outside any transaction.
//! 4. Captures a **high watermark** position after the `SELECT`.
//! 5. Keeps polling the live stream, recording the primary key of every event for
//!    the snapshotted table that the `SELECT` could not have seen — either its position
//!    falls in `(low, high]`, or it belongs to one of the in-flight transactions.
//! 6. Once the stream advances past the high watermark, emits snapshot `Read`
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
//!
//! # Why the low watermark alone is not the bracket
//!
//! The obvious bracket — "log position in `(low, high]`" — is unsound on every database
//! this crate supports, because reaching the log and becoming visible are two steps with
//! a gap between them. PostgreSQL writes a transaction's commit record to WAL (advancing
//! `pg_current_wal_lsn()`), flushes it, and only then clears the transaction from the proc
//! array; a chunk `SELECT` starting inside that gap still reads the *old* row while the
//! transaction's log position already sits below the low watermark. The position test
//! misses it, the chunk row is not suppressed, and the pre-image overwrites the newer
//! stream value — the very corruption the override window exists to prevent, reachable
//! whenever a commit's fsync overlaps a chunk read.
//!
//! So membership in the window is a question about **visibility**, not about ordering, and only
//! the backend can answer it — the evidence differs per engine and none of it is a log position.
//! [`IncrementalSnapshotBackend::event_in_bracket`] is that question. Its default is the ordinal
//! test, which is correct only where the watermark lags visibility rather than leading it
//! (SQL Server, whose capture job harvests after commit); the backends that need more override it:
//!
//! * **PostgreSQL** captures `pg_current_snapshot()` alongside the LSN and asks whether the
//!   event's `xid` was invisible to the low watermark's snapshot — `xid >= xmax || xip.contains`.
//! * **MySQL** captures `Executed_Gtid_Set`, which the server updates *after* the engine commit,
//!   and takes the set difference between the two bounds.
//!
//! # Why events past the high watermark are held back
//!
//! The override window closes at the high watermark, so an event *past* it is not
//! suppressed — correctly, because such an event committed after the `SELECT`
//! finished and therefore describes a row state the chunk cannot contain. What the
//! algorithm additionally requires is that the chunk is emitted **at** the high
//! watermark, before any later log event. DBLog gets this for free by emitting the
//! buffered chunk the moment the high-watermark marker is read from the log.
//!
//! Here the log is read in batches, and one batch routinely straddles the high
//! watermark: it can carry events at LSN 900 (inside the window) and 1200 (past it)
//! together. Returning the whole batch and *then* the chunk hands the consumer the
//! newer value first and the chunk's older value second — the exact stale-value
//! resurrection the override window exists to prevent, just moved one step later.
//!
//! So a straddling batch is split at the first event past the high watermark: the
//! head is delivered, the chunk follows, and the tail is delivered after it. Order
//! within the log is preserved, and the chunk lands exactly where DBLog puts it.

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
        SnapshotRequest, StreamHandle,
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

/// Where a live event sits relative to a chunk's watermark bracket.
///
/// The driver asks the backend rather than comparing positions itself, because only the backend
/// knows whether its watermark is an ordered coordinate or a set. MySQL's is a GTID set, which is
/// **partially** ordered — `>` cannot express membership in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BracketPosition {
    /// The chunk read could have seen this event, so its chunk row is not superseded.
    Before,
    /// The chunk read could **not** have seen it, and it committed no later than the high
    /// watermark. Its chunk row must be suppressed.
    Inside,
    /// It committed after the chunk read finished. The chunk is emitted **before** it, so
    /// suppressing would discard the newer value.
    After,
}

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
    /// Operator-supplied SQL boolean expression restricting which rows this snapshot
    /// reads, from [`IncrementalSnapshotConfig::table_conditions`].
    ///
    /// A backend must `AND` it into its chunk `SELECT`'s `WHERE` clause. It is raw SQL and
    /// trusted input — see the config field for the trust boundary. The table is aliased
    /// `t` in every backend's chunk read, so a qualified column reference works.
    pub condition: Option<String>,
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

    /// Classify a live event against the bracket captured for the current chunk.
    ///
    /// # Why the backend decides
    ///
    /// The default is the ordinal test the driver used to inline: inside when the event's
    /// position is past `low` (or its transaction was in flight) and at or below `high`. That is
    /// correct for a watermark which is a single ordered coordinate, which PostgreSQL's LSN and
    /// SQL Server's capture LSN both are.
    ///
    /// It is **not** correct for every source, and the reason is not a detail of one connector.
    /// A binlog file-and-position advances at the binlog flush stage, before the engine commit
    /// that makes rows visible — so an event can sit below the low watermark and still have been
    /// invisible to the chunk read. MySQL's answer is to bracket by executed-GTID **set
    /// membership** instead, which no `Ord` comparison can express. Overriding this is how a
    /// backend says so.
    ///
    /// # Contract
    ///
    /// Both bounds must come from the same notion of order. Mixing them — a set-based lower
    /// bound with an ordinal upper bound — is unsound in a way that is easy to reach: an event
    /// inside the ordinal high bound but absent from the high watermark's set committed *after*
    /// that read, and suppressing it discards the newer value.
    ///
    /// An implementation that cannot classify a particular event should fall back to the
    /// default rather than guess.
    fn event_in_bracket(
        &self,
        event: &Event,
        position: &Self::Position,
        low: &Self::Position,
        high: &Self::Position,
    ) -> BracketPosition {
        if *position > *high {
            return BracketPosition::After;
        }
        let _ = event;
        if *position > *low {
            BracketPosition::Inside
        } else {
            BracketPosition::Before
        }
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

/// Resolve a table's row filter, matching on either the reference the operator wrote or the
/// canonical `"schema.table"` the catalog resolved it to.
///
/// Both, because they legitimately differ: an operator may configure `orders` against a
/// default schema and the catalog resolves it to `public.orders`. Matching only one form
/// would silently drop the filter — and a silently dropped filter snapshots the whole table,
/// which is exactly the load the filter was written to avoid.
fn lookup_condition(
    conditions: &ahash::AHashMap<String, String>,
    table_ref: &str,
    spec: &SnapshotTable,
) -> Option<String> {
    let canonical = format!("{}.{}", spec.schema, spec.name);
    conditions
        .iter()
        .find(|(key, _)| {
            key.eq_ignore_ascii_case(table_ref) || key.eq_ignore_ascii_case(&canonical)
        })
        .map(|(_, condition)| condition.clone())
}

/// Resolve `table_ref` against the catalog **and** attach its row filter.
///
/// The two steps live together because separating them is what broke them. The condition
/// used to be applied at two of the three resolution sites — the startup tables and the
/// tables adopted from a checkpoint — and not at
/// [`IncrementalSnapshotDriver::enqueue_tables`], which services every on-demand request.
/// A table requested at runtime therefore snapshotted **in full**, ignoring the operator's
/// filter, with nothing to report it: the only symptom is volume, indistinguishable from a
/// big table.
///
/// Worse than simply ignoring it, the two paths disagreed. A runtime-requested table ran
/// unfiltered, and then a restart adopted it from the checkpoint *with* the filter applied —
/// so the delivered rows corresponded to no single predicate, and the split depended on when
/// the process happened to restart.
///
/// One function, called from all three sites, is the fix. `describe_table` deliberately
/// leaves `condition` unset so a backend cannot get this wrong either.
async fn describe_with_condition<B: IncrementalSnapshotBackend>(
    backend: &mut B,
    table_ref: &str,
    conditions: &ahash::AHashMap<String, String>,
) -> Result<SnapshotTable> {
    let mut spec = backend.describe_table(table_ref).await?;
    spec.condition = lookup_condition(conditions, table_ref, &spec);
    Ok(spec)
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
        ///
        /// Doubles as the **shadow image** used to repair partial live payloads: each row
        /// starts as the complete image the chunk read returned, and is updated by every
        /// in-bracket event for that key. See
        /// [`IncrementalSnapshotDriver::repair_partial_payload`].
        chunk_rows: Vec<(String, Event)>,
        /// `pk_fingerprint` → index into `chunk_rows`, so the repair is a lookup rather
        /// than a scan of the whole chunk per event.
        chunk_index_by_key: ahash::AHashMap<String, usize>,
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
    /// Configured per-table row filters, retained for the lifetime of the driver.
    ///
    /// `config` used to be a by-value parameter dropped once the startup tables were
    /// resolved, which made it *structurally impossible* for `enqueue_tables` to honour a
    /// filter — see [`describe_with_condition`].
    table_conditions: ahash::AHashMap<String, String>,
    source_name: String,
    snapshot_id: String,
    /// Events handed to the caller, for `save_position` accounting.
    events_emitted: u64,
    /// Chunk reading is suspended; the live stream continues untouched.
    ///
    /// Checked only where the next chunk would be *started*, so a chunk already read stays
    /// consistent: an in-flight collect finishes and its rows are emitted before the driver
    /// parks. Pausing mid-chunk instead would either discard a read the source has already
    /// paid for, or leave a merged-but-undelivered chunk whose cursor can never be promoted.
    paused: bool,
    /// How many times work has been (re)requested; see
    /// [`IncrementalSnapshotState::generation`]. Included in every snapshot `Read` offset so a
    /// deliberate re-snapshot is not mistaken for a replay and dropped by the idempotency
    /// guard.
    generation: u32,
    /// The snapshot was abandoned by `stop_snapshot`, and that must survive a restart.
    ///
    /// Recorded explicitly because absence of per-table entries is not the same thing: the
    /// driver seeds one entry per *configured* table on startup, so an abandoned snapshot
    /// that left no entries behind looked exactly like one that had not started, and every
    /// configured table restarted from row zero on the next deploy.
    stopped: bool,
    /// Log events read past the current chunk's high watermark, held back until the
    /// chunk has been delivered. See the module header.
    ///
    /// While this is non-empty the driver reports **no** durable position
    /// ([`Self::position_offset`] returns `None`), because the inner stream has
    /// already advanced past events the consumer has not been given. Persisting that
    /// position would skip them on restart.
    deferred: VecDeque<Event>,
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
        // A snapshot the operator abandoned stays abandoned. Seeding the configured tables
        // here is what made `stop_incremental_snapshot` silently ineffective: a stop clears
        // the per-table entries, and a configured table with no entry looks like one that
        // has not started, so the next deploy re-ran the whole backfill that had just been
        // stopped to take load off a production primary.
        let stopped = resume.as_ref().is_some_and(|state| state.stopped);
        let configured: &[String] = if stopped { &[] } else { &config.tables };
        if stopped {
            tracing::info!(
                target: "rustcdc::source::incremental_snapshot",
                configured_tables = config.tables.len(),
                "the persisted incremental-snapshot state records a stop, so the configured \
                 tables are not seeded. Call request_incremental_snapshot to start over.",
            );
        }

        let mut tables = Vec::with_capacity(configured.len());
        for table_ref in configured {
            let spec =
                describe_with_condition(&mut backend, table_ref, &config.table_conditions).await?;
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

        // Adopt in-flight tables the checkpoint knows about but the static config does not.
        //
        // Without this, a table added at runtime through
        // [`StreamHandle::request_snapshot_tables`](crate::source::StreamHandle::request_snapshot_tables)
        // would vanish on the next restart: `config.tables` is the *initial* set, while the
        // checkpoint is the record of work actually in flight. Only **incomplete** entries are
        // adopted — a finished table has nothing left to do, so re-adding it would restart a
        // snapshot the operator never asked to repeat.
        if let Some(state) = resume.as_ref().filter(|_| !stopped) {
            for persisted in &state.tables {
                if persisted.is_complete {
                    continue;
                }
                if tables.iter().any(|table| table.key() == persisted.table) {
                    continue;
                }
                // A table that has since been dropped must not wedge startup: the snapshot it
                // was running is moot, and failing here would make the pipeline unstartable
                // until someone hand-edited a checkpoint.
                match describe_with_condition(
                    &mut backend,
                    &persisted.table,
                    &config.table_conditions,
                )
                .await
                {
                    Ok(spec) if !spec.pk_columns.is_empty() => {
                        tracing::info!(
                            target: "rustcdc::source::incremental_snapshot",
                            table = %persisted.table,
                            "adopting an in-flight incremental snapshot table from the \
                             checkpoint; it was requested at runtime rather than configured",
                        );
                        tables.push(TableProgress {
                            pk_cursor: persisted.pk_cursor.clone(),
                            is_complete: false,
                            chunks_emitted: persisted.chunks_emitted,
                            rows_emitted: persisted.rows_emitted,
                            spec,
                        });
                    }
                    Ok(_) | Err(_) => {
                        tracing::warn!(
                            target: "rustcdc::source::incremental_snapshot",
                            table = %persisted.table,
                            "checkpoint records an unfinished incremental snapshot for a table \
                             that can no longer be described (dropped, renamed, or its primary \
                             key removed); dropping it from this run",
                        );
                    }
                }
            }
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
            table_conditions: config.table_conditions,
            source_name,
            snapshot_id,
            events_emitted: 0,
            paused: resume.as_ref().is_some_and(|state| state.paused),
            generation: resume.as_ref().map_or(0, |state| state.generation),
            stopped,
            deferred: VecDeque::new(),
        })
    }

    /// Enqueue tables for snapshotting on a running driver.
    ///
    /// See [`StreamHandle::request_snapshot_tables`](crate::source::StreamHandle::request_snapshot_tables)
    /// for the semantics this implements.
    async fn enqueue_tables(&mut self, request: SnapshotRequest) -> Result<usize> {
        // Request conditions override configured ones for the same table; a table with no
        // override keeps its configured filter, so static configuration stays meaningful.
        // Merging here rather than at each lookup keeps precedence in one visible place.
        let mut conditions = self.table_conditions.clone();
        conditions.extend(
            request
                .conditions
                .iter()
                .map(|(table, condition)| (table.clone(), condition.clone())),
        );

        // Resolve everything before mutating, so a typo in the third of four names does not
        // leave the first two half-applied with no way to tell.
        let mut resolved = Vec::with_capacity(request.tables.len());
        for table_ref in &request.tables {
            let spec = describe_with_condition(&mut self.backend, table_ref, &conditions).await?;
            if spec.pk_columns.is_empty() {
                return Err(Error::ConfigError(format!(
                    "incremental snapshot: table '{}.{}' must have a primary key",
                    spec.schema, spec.name
                )));
            }
            resolved.push(spec);
        }

        let mut enqueued = 0usize;
        for spec in resolved {
            let key = format!("{}.{}", spec.schema, spec.name);
            match self.tables.iter_mut().find(|table| table.key() == key) {
                // Already running: leave it alone. Resetting a table mid-flight would
                // re-deliver every row already handed to the consumer.
                Some(existing) if !existing.is_complete => {
                    tracing::debug!(
                        target: "rustcdc::source::incremental_snapshot",
                        table = %key,
                        "incremental snapshot already in progress for this table; request \
                         ignored as a no-op",
                    );
                }
                // Finished: this is a deliberate re-snapshot, so rewind it — and adopt the
                // freshly resolved spec. Keeping the old one would silently run the new
                // request under the *previous* request's filter, which is the same class of
                // defect as ignoring the filter outright.
                Some(existing) => {
                    existing.spec = spec;
                    existing.pk_cursor = None;
                    existing.is_complete = false;
                    existing.chunks_emitted = 0;
                    existing.rows_emitted = 0;
                    enqueued += 1;
                }
                None => {
                    self.tables.push(TableProgress {
                        spec,
                        pk_cursor: None,
                        is_complete: false,
                        chunks_emitted: 0,
                        rows_emitted: 0,
                    });
                    enqueued += 1;
                }
            }
        }

        // A driver that had finished everything is parked in `Done` and delegates straight to
        // the inner stream. Newly enqueued work has to move it back onto the state machine, or
        // the tables would sit in the list untouched.
        if enqueued > 0 {
            // A new generation, so these rows cannot be mistaken for the previous
            // request's and dropped as duplicates.
            self.generation = self.generation.saturating_add(1);
            // A new request is the operator un-stopping the snapshot; leaving the flag set
            // would make this request vanish on the next restart for the same reason a stop
            // used to.
            self.stopped = false;
            if let Some(table_idx) = self.next_incomplete_table() {
                if matches!(self.phase, Phase::Done) {
                    self.phase = Phase::ChunkPrepare { table_idx };
                }
            }
        }

        Ok(enqueued)
    }

    /// Index of the next table with snapshot work outstanding, scanning from the start.
    ///
    /// Scanning from the *current* index instead would strand a table that
    /// [`Self::enqueue_tables`] rewound behind the cursor: a re-snapshot requested for
    /// an already-finished table while a later table is mid-flight was silently never
    /// read, and the driver parked in [`Phase::Done`] reporting the snapshot complete.
    fn next_incomplete_table(&self) -> Option<usize> {
        self.tables.iter().position(|table| !table.is_complete)
    }

    /// Suspend or resume chunk reading.
    ///
    /// Returns the previous state, so a caller can tell a no-op from a real change.
    fn set_paused(&mut self, paused: bool) -> bool {
        let previous = self.paused;
        self.paused = paused;
        if previous != paused {
            tracing::info!(
                target: "rustcdc::source::incremental_snapshot",
                snapshot_id = %self.snapshot_id,
                paused,
                tables_remaining = self.tables.iter().filter(|t| !t.is_complete).count(),
                "incremental snapshot chunk reading {}",
                if paused { "paused" } else { "resumed" },
            );
        }
        // Resuming from `Done` has to put the state machine back on a table, exactly as
        // `enqueue_tables` does — the driver parks in `Done` whenever it has no work, and
        // a paused driver reaches `ChunkPrepare` and stops there rather than parking, so
        // this is only needed when everything genuinely finished while paused.
        if !paused && matches!(self.phase, Phase::Done) {
            if let Some(table_idx) = self.next_incomplete_table() {
                self.phase = Phase::ChunkPrepare { table_idx };
            }
        }
        previous
    }

    /// Abandon the snapshot: drop every table, cursor and undelivered chunk row.
    ///
    /// Returns the number of tables that still had work outstanding.
    ///
    /// The undelivered rows of an in-flight chunk go with it. They are snapshot reads the
    /// operator has just asked to stop producing, and keeping them would deliver part of a
    /// chunk whose cursor is then discarded — a partial chunk with no record that it
    /// happened.
    ///
    /// Held-back log events are **not** dropped: they belong to the live stream, not to the
    /// snapshot, and discarding them would lose change data. They drain before the driver
    /// becomes a pass-through.
    fn stop_snapshot(&mut self) -> usize {
        let abandoned = self.tables.iter().filter(|t| !t.is_complete).count();
        self.tables.clear();
        self.phase = Phase::Done;
        self.paused = false;
        self.stopped = true;
        // A stop discards the table list, so a later re-request would otherwise start from
        // generation 0 and produce offsets identical to the run just abandoned.
        self.generation = self.generation.saturating_add(1);
        tracing::warn!(
            target: "rustcdc::source::incremental_snapshot",
            snapshot_id = %self.snapshot_id,
            abandoned_tables = abandoned,
            "incremental snapshot stopped; chunk cursors discarded. The next checkpoint \
             records the stop, so a restart will not resume it — not even for tables in the \
             static config. Re-request the tables to start over.",
        );
        abandoned
    }

    /// Durable per-table progress for the checkpoint record.
    fn snapshot_state(&self) -> IncrementalSnapshotState {
        IncrementalSnapshotState {
            snapshot_id: self.snapshot_id.clone(),
            paused: self.paused,
            stopped: self.stopped,
            generation: self.generation,
            tables: self
                .tables
                .iter()
                .map(|table| IncrementalSnapshotTableState {
                    table: table.key(),
                    pk_cursor: table.pk_cursor.clone(),
                    is_complete: table.is_complete,
                    chunks_emitted: table.chunks_emitted,
                    rows_emitted: table.rows_emitted,
                    condition: table.spec.condition.clone(),
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
                //
                // The generation is part of the identity because without it a re-snapshot of
                // an unchanged row is byte-identical to the first read, and the idempotency
                // guard drops it as a duplicate. See
                // [`IncrementalSnapshotState::generation`].
                offset: format!(
                    "incremental:{}:{}:{}",
                    self.generation, table.qualified, fingerprint
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
                // Never true here, and not an oversight — see `SnapshotMetadata::is_last_chunk`.
                // An incremental snapshot can be paused, stopped, or have a table added to
                // it while running, so "the last chunk" is a claim the next request would
                // falsify. `incremental_snapshot_state()` is the completion signal for this
                // path, and it distinguishes finished from paused and stopped.
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
    ///
    /// A no-op while paused: the phase stays on `ChunkPrepare` so `drive` falls through to
    /// the live stream, and resuming picks up exactly here.
    async fn drive_chunk_prepare(&mut self) -> Result<()> {
        let Phase::ChunkPrepare { table_idx } = self.phase else {
            return Ok(());
        };
        if self.paused {
            return Ok(());
        }

        // Prefer the table the phase names — chunking one table to completion keeps its
        // keyset scan sequential — but fall back to a global scan so nothing is stranded.
        let table_idx = match self.tables.get(table_idx) {
            Some(table) if !table.is_complete => table_idx,
            _ => match self.next_incomplete_table() {
                Some(idx) => idx,
                None => {
                    self.phase = Phase::Done;
                    return Ok(());
                }
            },
        };

        // Watermarks bracket the read: any event between them may have superseded a
        // row the read returned.
        //
        // The three calls below are ordered, and the order is what makes the bracket sound
        // rather than merely plausible. The low watermark must be observed *before* the chunk
        // read and the high one *after* it, because each carries the visibility evidence
        // `event_in_bracket` tests against — PostgreSQL's transaction snapshot, MySQL's
        // executed-GTID set — and evidence captured on the wrong side of the read answers a
        // question about the wrong moment. See `IncrementalSnapshotBackend::event_in_bracket`
        // for the commit-visibility race this closes.
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
            self.phase = match self.next_incomplete_table() {
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

        let chunk_index_by_key = chunk_rows
            .iter()
            .enumerate()
            .map(|(index, (fingerprint, _))| (fingerprint.clone(), index))
            .collect();

        self.phase = Phase::ChunkCollect {
            table_idx,
            low_watermark,
            high_watermark,
            chunk_rows,
            chunk_index_by_key,
            override_pks: HashSet::new(),
            next_cursor,
        };
        Ok(())
    }

    /// Fill a live event's unavailable columns from the chunk's own image of that row, and
    /// fold the event's values back into that image.
    ///
    /// Returns the columns that could **not** be filled, which should be empty in practice.
    ///
    /// # Why this is sound, where reading the column back out of the table is not
    ///
    /// [`Event::unavailable_columns`](crate::core::Event::unavailable_columns) documents that
    /// a missing unchanged-TOAST value is unrecoverable, because reading it back out of band
    /// races concurrent writes and yields a value from an unknown point in time. That
    /// objection does not apply here, and the difference is the whole reason this is safe:
    ///
    /// - The value does not come from an out-of-band read. It comes from **this chunk's**
    ///   `SELECT`, at a snapshot the driver knows the position of.
    /// - `unavailable_columns` means the `UPDATE` **did not modify** those columns. So their
    ///   post-event value equals their value at the start of the event.
    /// - The only thing that could have changed a column between the chunk snapshot and this
    ///   event is another transaction — and any such transaction is *also* inside the bracket
    ///   (it commits after the chunk snapshot), so its event has already passed through here
    ///   and folded its values into the shadow image. If it modified the column, it carried
    ///   it; if it did not, the chunk value still stands.
    ///
    /// The driver therefore knows every write between the read and the event, which is
    /// exactly what an out-of-band read does not.
    ///
    /// # What this fixes
    ///
    /// Without it, suppressing a complete chunk row in favour of an incomplete event traded
    /// one gap for another. A PostgreSQL unchanged-TOAST `UPDATE` omits the large column, so
    /// its event is a `RowWrite::Merge` — and a merge into a row the consumer does not have
    /// yet, the normal case during a first snapshot, applies nothing. The chunk row carrying
    /// the column had just been dropped, so no delivery contained it. Repairing the event
    /// makes it a complete `RowWrite::Replace`, so suppression costs nothing.
    ///
    /// Only columns the chunk actually read are filled, and only for the key's own row.
    fn repair_partial_payload(&mut self, fingerprint: &str, event: &mut Event) -> Vec<String> {
        let Phase::ChunkCollect {
            ref mut chunk_rows,
            ref chunk_index_by_key,
            ..
        } = self.phase
        else {
            return event.unavailable_columns.clone();
        };
        let Some(&index) = chunk_index_by_key.get(fingerprint) else {
            // Not a row this chunk read, so no chunk row is being suppressed for it and
            // there is nothing to repair from — or to lose.
            return Vec::new();
        };
        let Some(shadow) = chunk_rows
            .get_mut(index)
            .and_then(|(_, row)| row.after.as_mut())
            .and_then(serde_json::Value::as_object_mut)
        else {
            return event.unavailable_columns.clone();
        };

        // Read the shadow *before* folding this event in: the before-image's holes describe
        // the state prior to the event, which is what the shadow currently holds.
        let mut unfilled = Vec::new();
        let mut fill = |columns: &mut Vec<String>, image: &mut Option<serde_json::Value>| {
            if columns.is_empty() {
                return;
            }
            let Some(object) = image.as_mut().and_then(serde_json::Value::as_object_mut) else {
                return;
            };
            columns.retain(|column| match shadow.get(column) {
                // Filling means inserting the value *and* dropping the column from the
                // list. A column that is both listed and present fails envelope validation,
                // and rightly so — a sink would write whatever placeholder it found.
                Some(value) => {
                    object.insert(column.clone(), value.clone());
                    false
                }
                None => {
                    unfilled.push(column.clone());
                    true
                }
            });
        };
        fill(&mut event.before_unavailable_columns, &mut event.before);
        fill(&mut event.unavailable_columns, &mut event.after);

        // Fold this event's values into the shadow so a later in-bracket event that omits a
        // column this one wrote is repaired to the new value rather than the chunk's.
        if let Some(after) = event.after.as_ref().and_then(serde_json::Value::as_object) {
            for (column, value) in after {
                shadow.insert(column.clone(), value.clone());
            }
        }

        unfilled
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

    /// Drain up to one batch of held-back post-high-watermark log events.
    fn drain_deferred(&mut self) -> Vec<Event> {
        let batch_size = self.deferred.len().min(EMIT_BATCH_SIZE);
        self.deferred.drain(..batch_size).collect()
    }

    /// Advance the state machine by one step.
    async fn drive(&mut self, timeout_ms: u64) -> Result<Vec<Event>> {
        loop {
            match self.phase {
                Phase::Done => {
                    // A final chunk can straddle its high watermark and leave held-back
                    // events behind; they must go out before the stream is delegated to,
                    // or they would be delivered out of log order.
                    if !self.deferred.is_empty() {
                        return Ok(self.drain_deferred());
                    }
                    return self.inner.next_events(timeout_ms).await;
                }

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
                    // The chunk is out; anything held back behind it goes next, before
                    // the driver reads another chunk.
                    if !self.deferred.is_empty() {
                        self.phase = Phase::ChunkPrepare { table_idx };
                        return Ok(self.drain_deferred());
                    }
                    // Try the next chunk of the same table. `drive_chunk_prepare`
                    // detects completion via an empty read.
                    self.phase = Phase::ChunkPrepare { table_idx };
                }

                Phase::ChunkPrepare { .. } => {
                    // Paused: hold the phase and behave as a pass-through, so the live
                    // stream keeps flowing and resuming picks up at exactly this chunk.
                    // Falling through to `drive_chunk_prepare` would spin, because it
                    // returns without changing phase.
                    if self.paused {
                        if !self.deferred.is_empty() {
                            return Ok(self.drain_deferred());
                        }
                        return self.inner.next_events(timeout_ms).await;
                    }
                    self.drive_chunk_prepare().await?
                }

                Phase::ChunkCollect { .. } => {
                    // Bounded so a quiet database still lets us re-check the
                    // watermark rather than blocking for the caller's full timeout.
                    let mut stream_events = self
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
                    // Index of the first event past the high watermark. Everything from
                    // there on is held back until the chunk has been delivered — see the
                    // module header.
                    let mut split_at: Option<usize> = None;
                    // Indices of same-table events the chunk read could not have seen.
                    // Collected here and processed below, because that processing *mutates*
                    // the events (repairing partial payloads) and cannot run inside a loop
                    // holding the batch immutably.
                    let mut in_bracket: Vec<usize> = Vec::new();
                    for (index, event) in stream_events.iter().enumerate() {
                        let Some(position) = self.backend.position_of_event(event) else {
                            continue;
                        };
                        if max_batch_position
                            .as_ref()
                            .is_none_or(|current| position > *current)
                        {
                            max_batch_position = Some(position.clone());
                        }

                        // Asking the backend, not comparing positions here: only it knows
                        // whether its watermark is an ordered coordinate or a set. See
                        // `IncrementalSnapshotBackend::event_in_bracket`.
                        let membership = self.backend.event_in_bracket(
                            event,
                            &position,
                            &low_watermark,
                            &high_watermark,
                        );

                        if membership == BracketPosition::After && split_at.is_none() {
                            split_at = Some(index);
                        }

                        // An event supersedes a chunk row when it targets the same table
                        // and the chunk read could not have seen it.
                        //
                        // "Could not have seen it" is a visibility question, and the backend
                        // answers it via `event_in_bracket`. A log position alone is not
                        // enough: a transaction can have reached the log *below* the low
                        // watermark and still have been invisible to the chunk's snapshot,
                        // because committing to the log and becoming visible are separate
                        // steps. Testing only the position leaves that transaction
                        // unsuppressed and lets the chunk's pre-image overwrite the newer
                        // stream value.
                        //
                        // The upper bound stays absolute: an event past the high watermark
                        // committed after the `SELECT` finished, and the chunk is emitted
                        // before it, so suppressing it would discard a newer value.
                        let same_table = event.table == target_table
                            && event.schema.as_deref().unwrap_or(&target_schema) == target_schema;
                        if same_table && membership == BracketPosition::Inside {
                            in_bracket.push(index);
                        }
                    }

                    // Suppressing a *complete* chunk row in favour of an *incomplete* stream
                    // event would trade one gap for another: a PostgreSQL unchanged-TOAST
                    // `UPDATE` omits the large column, and a merge into a row the consumer
                    // does not have yet applies nothing, so the column would be in neither
                    // delivery. Repairing the event from the chunk's own image of that row —
                    // which is sound here in a way an out-of-band read is not, see
                    // `repair_partial_payload` — makes it a complete write, so the
                    // suppression costs nothing.
                    for index in in_bracket {
                        let Some(fingerprint) = event_fingerprint(&stream_events[index]) else {
                            continue;
                        };
                        let unfilled =
                            self.repair_partial_payload(&fingerprint, &mut stream_events[index]);
                        if !unfilled.is_empty() {
                            // The chunk read every column of the table, so this means the
                            // event named a column the table does not have — a schema change
                            // inside the chunk window. Nothing can fill it, and staying
                            // silent would hide a genuine gap.
                            tracing::warn!(
                                target: "rustcdc::source::incremental_snapshot",
                                snapshot_id = %self.snapshot_id,
                                table = %target_table,
                                key = %fingerprint,
                                unfilled_columns = ?unfilled,
                                "a live event omitted columns that the chunk's own image of the \
                                 row could not supply, so they are in neither delivery. The usual \
                                 cause is a schema change inside the chunk window. Re-snapshot \
                                 this table with request_incremental_snapshot to recover them.",
                            );
                        }
                        if let Phase::ChunkCollect {
                            ref mut override_pks,
                            ..
                        } = self.phase
                        {
                            override_pks.insert(fingerprint);
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

                    if let Some(index) = split_at {
                        // Preserves log order: head now, chunk next, tail after it.
                        self.deferred.extend(stream_events.drain(index..));
                    }

                    if watermark_passed {
                        self.finalize_collect();
                    }

                    // Stream events up to the high watermark go out first so the consumer
                    // stays current; snapshot rows follow on the next call.
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
        // Events read past the high watermark but not yet handed over are already
        // behind the inner stream's position. Writing that position would mark them
        // consumed and skip them on the next start, so the previous durable record is
        // left standing instead: the restart replays from there, which is the
        // at-least-once behaviour the rest of the pipeline documents.
        if !self.deferred.is_empty() {
            tracing::info!(
                target: "rustcdc::source::incremental_snapshot",
                held_back = self.deferred.len(),
                "skipping the position write: log events read past the chunk's high \
                 watermark have not been delivered yet, so the last durable checkpoint \
                 stands and they will be replayed",
            );
            return Ok(());
        }
        // Delegating to the inner stream would persist the log position while
        // dropping every chunk cursor, so an orderly shutdown would forfeit exactly
        // the progress this method exists to preserve.
        let Some(offset) = self.position_offset() else {
            return self.inner.save_position(checkpoint).await;
        };
        checkpoint.save(offset.as_ref(), self.events_emitted).await
    }

    fn position_offset(&self) -> Option<Box<dyn Offset>> {
        // While events are held back the inner stream's position covers events the
        // consumer has not been given. The runtime uses this offset to checkpoint
        // snapshot rows — which carry no position of their own — so reporting the
        // inner position here would make the held-back events durable-as-consumed and
        // lose them. `None` makes those rows non-persistent barrier entries instead:
        // the committed count advances, the durable source position does not, and the
        // held-back events carry it forward with their own offsets a moment later.
        if !self.deferred.is_empty() {
            return None;
        }
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

    async fn request_snapshot_tables(&mut self, request: SnapshotRequest) -> Result<usize> {
        self.enqueue_tables(request).await
    }

    async fn set_snapshot_paused(&mut self, paused: bool) -> Result<bool> {
        Ok(self.set_paused(paused))
    }

    async fn stop_snapshot(&mut self) -> Result<usize> {
        Ok(IncrementalSnapshotDriver::stop_snapshot(self))
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
    pub(super) struct FakeStream {
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
            Some(Box::new(crate::checkpoint::GenericOffset::new(
                "fake",
                b"pos".to_vec(),
            )))
        }
        async fn confirm_lsn(&mut self, _lsn: u64) -> Result<()> {
            Ok(())
        }
    }

    /// Backend with an in-memory table and a caller-controlled clock.
    pub(super) struct FakeBackend {
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
        /// Transactions reported as still in flight when the chunk is read.
        in_flight: HashSet<u64>,
    }

    #[async_trait]
    impl IncrementalSnapshotBackend for FakeBackend {
        type Position = u64;

        async fn describe_table(&mut self, _table_ref: &str) -> Result<SnapshotTable> {
            Ok(SnapshotTable {
                condition: None,
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

        /// Mirrors what a real backend does: a transaction the chunk read could not see is
        /// inside the bracket even when its position is at or below the low watermark.
        fn event_in_bracket(
            &self,
            event: &Event,
            position: &u64,
            low: &u64,
            high: &u64,
        ) -> crate::source::BracketPosition {
            use crate::source::BracketPosition;
            if position > high {
                return BracketPosition::After;
            }
            let invisible = position > low
                || event
                    .transaction
                    .as_ref()
                    .is_some_and(|tx| self.in_flight.contains(&tx.tx_id));
            if invisible {
                BracketPosition::Inside
            } else {
                BracketPosition::Before
            }
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
            inner: &dyn Offset,
            _state: IncrementalSnapshotState,
        ) -> Option<Box<dyn Offset>> {
            Some(inner.clone_box())
        }
    }

    pub(super) fn stream_event_at(table: &str, id: i64, position: u64) -> Event {
        let mut event = event_with_key(table, id);
        event.source.offset = position.to_string();
        event
    }

    /// Build a driver that resumes from `resume`, with the same static config as
    /// [`driver_with`] — the shape a restart takes.
    pub(super) async fn driver_resuming_from(
        rows: Vec<serde_json::Value>,
        resume: Option<IncrementalSnapshotState>,
        chunk_size: usize,
    ) -> IncrementalSnapshotDriver<FakeBackend> {
        let mut config = IncrementalSnapshotConfig::new(vec!["public.users".to_string()]);
        config.chunk_size = chunk_size;
        IncrementalSnapshotDriver::new(
            FakeBackend {
                rows,
                clock: Arc::new(Mutex::new(100)),
                advance_on_read: 0,
                reads: Arc::new(Mutex::new(Vec::new())),
                in_flight: HashSet::new(),
            },
            Box::new(FakeStream {
                batches: VecDeque::new(),
            }),
            config,
            "test".to_string(),
            resume,
        )
        .await
        .expect("driver builds")
    }

    /// Build a driver from an explicit config, so a test can exercise `table_conditions`.
    pub(super) async fn driver_with_config(
        rows: Vec<serde_json::Value>,
        config: IncrementalSnapshotConfig,
    ) -> IncrementalSnapshotDriver<FakeBackend> {
        driver_with_config_resuming(rows, config, None).await
    }

    pub(super) async fn driver_with_config_resuming(
        rows: Vec<serde_json::Value>,
        config: IncrementalSnapshotConfig,
        resume: Option<IncrementalSnapshotState>,
    ) -> IncrementalSnapshotDriver<FakeBackend> {
        IncrementalSnapshotDriver::new(
            FakeBackend {
                rows,
                clock: Arc::new(Mutex::new(100)),
                advance_on_read: 0,
                reads: Arc::new(Mutex::new(Vec::new())),
                in_flight: HashSet::new(),
            },
            Box::new(FakeStream {
                batches: VecDeque::new(),
            }),
            config,
            "test".to_string(),
            resume,
        )
        .await
        .expect("driver builds")
    }

    pub(super) async fn driver_with(
        rows: Vec<serde_json::Value>,
        batches: Vec<Vec<Event>>,
        advance_on_read: u64,
        chunk_size: usize,
    ) -> (
        IncrementalSnapshotDriver<FakeBackend>,
        Arc<Mutex<Vec<Option<Vec<serde_json::Value>>>>>,
    ) {
        driver_with_in_flight(rows, batches, advance_on_read, chunk_size, HashSet::new()).await
    }

    pub(super) async fn driver_with_in_flight(
        rows: Vec<serde_json::Value>,
        batches: Vec<Vec<Event>>,
        advance_on_read: u64,
        chunk_size: usize,
        in_flight: HashSet<u64>,
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
            in_flight,
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
    pub(super) async fn drain(driver: &mut IncrementalSnapshotDriver<FakeBackend>) -> Vec<Event> {
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
    async fn the_incremental_driver_never_claims_a_last_chunk() {
        // All three bulk snapshot paths set `is_last_chunk` on the final chunk, so the
        // absence here reads like an oversight. It is not, and this pins the decision.
        //
        // An incremental snapshot interleaves with the live stream, can be paused, resumed
        // and stopped, and can have a table added while it runs. Flagging the chunk that
        // drains the currently-known set would be a claim the next `request_incremental_
        // snapshot` falsifies — and a consumer that swapped in a staging table on it would
        // swap early and then receive more rows. `incremental_snapshot_state()` is the
        // completion signal for this path; it survives a restart and distinguishes
        // finished from paused and stopped.
        let rows = vec![json!({ "id": 1 }), json!({ "id": 2 })];
        let (mut driver, _) = driver_with(rows, vec![], 0, 10).await;

        let emitted = drain(&mut driver).await;
        assert_eq!(emitted.len(), 2, "the snapshot ran to completion");
        assert!(
            emitted
                .iter()
                .filter_map(|event| event.snapshot.as_ref())
                .all(|snapshot| !snapshot.is_last_chunk),
            "changing this is a contract change, not a bug fix — see \
             SnapshotMetadata::is_last_chunk"
        );
    }

    #[tokio::test]
    async fn a_batch_straddling_the_high_watermark_delivers_the_chunk_before_the_tail() {
        // The load-bearing ordering property of the algorithm, and the one a batched log
        // reader breaks by default.
        //
        // The read advances the clock 100 -> 200, so the bracket is (100, 200]. One
        // batch carries an event at 150 (inside the window, suppresses its chunk row)
        // and one at 250 (past the high watermark, so it is *not* suppressed — it
        // describes a row state committed after the SELECT finished).
        //
        // Returning both and only then the chunk hands the consumer the 250 value first
        // and the chunk's older value second, which resurrects the stale row. The event
        // past the watermark has to wait behind the chunk.
        let rows = vec![json!({ "id": 1 }), json!({ "id": 2 })];
        let batches = vec![vec![
            stream_event_at("users", 2, 150),
            stream_event_at("users", 1, 250),
        ]];
        let (mut driver, _) = driver_with(rows, batches, 100, 10).await;

        let emitted = drain(&mut driver).await;
        let shape: Vec<String> = emitted
            .iter()
            .map(|event| {
                let id = event
                    .after
                    .as_ref()
                    .or(event.before.as_ref())
                    .and_then(|row| row["id"].as_i64())
                    .unwrap_or_default();
                format!("{}:{id}", event.op)
            })
            .collect();

        assert_eq!(
            shape,
            vec!["update:2", "read:1", "update:1"],
            "expected head event, then the chunk, then the held-back event past the \
             high watermark; got {shape:?}",
        );
    }

    #[tokio::test]
    async fn a_chunk_row_is_non_persistent_while_later_log_events_are_held_back() {
        // The runtime checkpoints a snapshot row using `position_offset()`, because the
        // row has no log position of its own. While events read past the high watermark
        // are still queued, the inner stream's position already covers them — persisting
        // it would mark them consumed and skip them after a crash.
        let rows = vec![json!({ "id": 1 })];
        let batches = vec![vec![stream_event_at("users", 9, 250)]];
        let (mut driver, _) = driver_with(rows, batches, 100, 10).await;

        // The batch's only event is past the high watermark, so it is held back and the
        // chunk comes out first.
        let first = driver.next_events(10).await.expect("drive");
        assert!(
            !first.is_empty() && first.iter().all(|event| event.snapshot.is_some()),
            "the chunk goes out before the held-back event, got {first:?}",
        );
        assert!(
            driver.position_offset().is_none(),
            "no durable position may be reported while log events are held back — the \
             inner stream has already consumed them",
        );

        let second = driver.next_events(10).await.expect("drive");
        assert_eq!(second.len(), 1, "the held-back event follows the chunk");
        assert!(second[0].snapshot.is_none());
        assert!(
            driver.position_offset().is_some(),
            "the position becomes reportable again once nothing is held back",
        );
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
                in_flight: HashSet::new(),
            },
            Box::new(FakeStream {
                batches: VecDeque::new(),
            }),
            IncrementalSnapshotConfig::new(vec!["public.users".to_string()]).with_chunk_size(3),
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
    async fn a_table_requested_at_runtime_is_snapshotted() {
        // The on-demand case: the driver has finished everything and is parked in `Done`,
        // delegating straight to the inner stream. A request must move it back onto the state
        // machine, or the table sits in the list untouched.
        let rows: Vec<serde_json::Value> = (1..=4).map(|id| json!({ "id": id })).collect();
        let (mut driver, _) = driver_with(rows, vec![], 0, 10).await;

        let first_pass = drain(&mut driver).await;
        assert_eq!(first_pass.len(), 4, "the configured table snapshots first");
        assert!(
            matches!(driver.phase, Phase::Done),
            "the driver must be parked once every table is complete"
        );

        // `FakeBackend::describe_table` always resolves to `public.users`, which is already
        // tracked and complete — so this is the deliberate re-snapshot path.
        let enqueued = driver
            .enqueue_tables(vec!["public.users".to_string()].into())
            .await
            .expect("request accepted");
        assert_eq!(enqueued, 1);
        assert!(
            !matches!(driver.phase, Phase::Done),
            "an enqueued table must take the driver out of Done"
        );

        let second_pass = drain(&mut driver).await;
        assert_eq!(
            second_pass.len(),
            4,
            "a completed table requested again must be read from the start"
        );
    }

    #[test]
    fn a_row_filter_matches_either_the_configured_reference_or_the_resolved_name() {
        // Both forms, because they legitimately differ: an operator writes `orders` against
        // a default schema and the catalog resolves it to `public.orders`. Matching only one
        // would silently drop the filter — and a dropped filter snapshots the whole table,
        // which is precisely the load it was written to avoid.
        let spec = SnapshotTable {
            schema: "public".into(),
            name: "orders".into(),
            qualified: "\"public\".\"orders\"".into(),
            pk_columns: vec!["id".into()],
            pk_types: Vec::new(),
            columns: Vec::new(),
            condition: None,
        };

        let mut by_short = ahash::AHashMap::new();
        by_short.insert("orders".to_string(), "tenant = 7".to_string());
        assert_eq!(
            lookup_condition(&by_short, "orders", &spec).as_deref(),
            Some("tenant = 7"),
        );

        let mut by_qualified = ahash::AHashMap::new();
        by_qualified.insert("PUBLIC.Orders".to_string(), "tenant = 7".to_string());
        assert_eq!(
            lookup_condition(&by_qualified, "orders", &spec).as_deref(),
            Some("tenant = 7"),
            "the match is case-insensitive, like every other table reference here",
        );

        let mut unrelated = ahash::AHashMap::new();
        unrelated.insert("public.customers".to_string(), "tenant = 7".to_string());
        assert!(lookup_condition(&unrelated, "orders", &spec).is_none());
    }

    #[tokio::test]
    async fn pausing_stops_chunk_reads_while_the_live_stream_keeps_flowing() {
        // The whole point of pause: take snapshot load off a production primary without
        // stopping capture. Chunk reads must stop; the stream must not.
        let rows: Vec<serde_json::Value> = (1..=6).map(|id| json!({ "id": id })).collect();
        let (mut driver, reads) =
            driver_with(rows, vec![vec![stream_event_at("other", 99, 50)]; 6], 0, 2).await;

        driver.next_events(10).await.expect("drive");
        assert!(!driver.set_paused(true), "was not paused before");
        let reads_at_pause = reads.lock().expect("reads").len();

        // The chunk already read is merged and delivered first — pausing takes effect at a
        // chunk *boundary*, so no read is wasted and no cursor is stranded. What must not
        // happen is a further read.
        let mut live = 0;
        for _ in 0..10 {
            for event in driver.next_events(10).await.expect("drive") {
                if event.snapshot.is_none() {
                    live += 1;
                }
            }
        }
        assert!(live > 0, "the live stream must keep flowing while paused");
        assert_eq!(
            reads.lock().expect("reads").len(),
            reads_at_pause,
            "no further chunk may be read while paused"
        );

        // Resuming finishes the table.
        assert!(driver.set_paused(false), "was paused before");
        let after = drain(&mut driver).await;
        assert!(
            after.iter().any(|event| event.snapshot.is_some()),
            "resuming must continue the snapshot"
        );
    }

    #[tokio::test]
    async fn the_paused_flag_is_durable_across_a_restart() {
        // Without this a pause silently lifts on the next deploy — the opposite of what an
        // operator asked for when they paused a backfill to protect a primary.
        let rows: Vec<serde_json::Value> = (1..=6).map(|id| json!({ "id": id })).collect();
        let (mut driver, _) = driver_with(rows.clone(), vec![], 0, 2).await;
        driver.next_events(10).await.expect("drive");
        driver.set_paused(true);

        let state = driver
            .incremental_snapshot_state()
            .expect("driver reports state");
        assert!(state.paused, "the checkpoint must record the pause");

        let reads = Arc::new(Mutex::new(Vec::new()));
        let mut resumed = IncrementalSnapshotDriver::new(
            FakeBackend {
                rows,
                clock: Arc::new(Mutex::new(100)),
                advance_on_read: 0,
                reads: Arc::clone(&reads),
                in_flight: HashSet::new(),
            },
            Box::new(FakeStream {
                batches: VecDeque::new(),
            }),
            IncrementalSnapshotConfig::new(vec!["public.users".to_string()]).with_chunk_size(2),
            "test".to_string(),
            Some(state),
        )
        .await
        .expect("driver builds");

        let emitted = drain(&mut resumed).await;
        assert!(
            emitted.is_empty(),
            "a snapshot paused before the restart must stay paused after it, got {emitted:?}"
        );
        assert!(reads.lock().expect("reads").is_empty(), "no chunk was read");
    }

    #[tokio::test]
    async fn stopping_abandons_the_snapshot_but_keeps_the_stream() {
        let rows: Vec<serde_json::Value> = (1..=6).map(|id| json!({ "id": id })).collect();
        let (mut driver, _) =
            driver_with(rows, vec![vec![stream_event_at("other", 99, 50)]; 4], 0, 2).await;
        driver.next_events(10).await.expect("drive");

        let abandoned = IncrementalSnapshotDriver::stop_snapshot(&mut driver);
        assert_eq!(abandoned, 1, "one table still had work outstanding");

        let state = driver
            .incremental_snapshot_state()
            .expect("driver still reports state");
        assert!(
            state.tables.is_empty(),
            "the next checkpoint must clear the persisted snapshot, got {:?}",
            state.tables
        );

        // Capture continues: live events still arrive, no snapshot rows do.
        let mut live = 0;
        for _ in 0..6 {
            for event in driver.next_events(10).await.expect("drive") {
                assert!(event.snapshot.is_none(), "the snapshot was abandoned");
                live += 1;
            }
        }
        assert!(live > 0, "the live stream must survive a stopped snapshot");
    }

    #[tokio::test]
    async fn a_table_behind_the_current_index_is_still_picked_up() {
        // `enqueue_tables` can rewind a table that sits *before* the one being read —
        // the operator asks to re-snapshot the first table while the second is still in
        // flight. Scanning forward from the current index skipped it: the driver parked
        // in `Done` reporting the snapshot finished, with a table that was never read.
        let rows: Vec<serde_json::Value> = (1..=3).map(|id| json!({ "id": id })).collect();
        let (mut driver, _) = driver_with(rows, vec![], 0, 10).await;

        // Second entry, already complete, with the phase pointing at it.
        let stranded = TableProgress {
            spec: driver.tables[0].spec.clone(),
            pk_cursor: None,
            is_complete: true,
            chunks_emitted: 0,
            rows_emitted: 0,
        };
        driver.tables.push(stranded);
        driver.phase = Phase::ChunkPrepare { table_idx: 1 };

        let emitted = drain(&mut driver).await;
        assert_eq!(
            emitted.len(),
            3,
            "the incomplete table before the phase index must still be read",
        );
    }

    #[tokio::test]
    async fn requesting_a_table_already_in_progress_is_a_no_op() {
        // Idempotence matters because a request is an operator action that may be retried.
        // Rewinding a table mid-flight would re-deliver rows the consumer already has.
        let rows: Vec<serde_json::Value> = (1..=6).map(|id| json!({ "id": id })).collect();
        let (mut driver, _) = driver_with(rows, vec![], 0, 2).await;

        let first = driver.next_events(10).await.expect("drive");
        assert_eq!(
            first.len(),
            2,
            "one chunk is delivered, so the table is mid-flight"
        );

        let enqueued = driver
            .enqueue_tables(vec!["public.users".to_string()].into())
            .await
            .expect("request accepted");
        assert_eq!(
            enqueued,
            0,
            "an in-progress table must not be re-enqueued: {:?}",
            driver.tables.len()
        );

        // The remaining rows still arrive exactly once — no rewind, no duplicates.
        let mut ids: Vec<i64> = first
            .iter()
            .chain(drain(&mut driver).await.iter())
            .filter_map(|event| event.after.as_ref()?.get("id")?.as_i64())
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn a_bad_table_reference_fails_the_whole_request() {
        // Atomicity: resolving every name before mutating means a typo in one entry cannot
        // leave the others half-applied with no way to tell which took effect.
        struct RejectingBackend;

        #[async_trait]
        impl IncrementalSnapshotBackend for RejectingBackend {
            type Position = u64;
            async fn describe_table(&mut self, table_ref: &str) -> Result<SnapshotTable> {
                Err(Error::ConfigError(format!("no such table '{table_ref}'")))
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

        let mut driver = IncrementalSnapshotDriver::new(
            RejectingBackend,
            Box::new(FakeStream {
                batches: VecDeque::new(),
            }),
            IncrementalSnapshotConfig::new(Vec::<String>::new()),
            "test".to_string(),
            None,
        )
        .await
        .expect("an empty table set builds");

        let error = driver
            .enqueue_tables(vec!["public.missing".to_string()].into())
            .await
            .expect_err("must reject");
        assert!(error.to_string().contains("no such table"));
        assert!(
            driver.tables.is_empty(),
            "a rejected request must leave no partial state"
        );
    }

    #[tokio::test]
    async fn an_unfinished_table_in_the_checkpoint_is_adopted_even_if_unconfigured() {
        // A table requested at runtime is not in `config.tables`, so without adopting it from
        // the checkpoint its snapshot would vanish on the next restart — the request would look
        // honoured and then silently stop.
        let rows: Vec<serde_json::Value> = (1..=4).map(|id| json!({ "id": id })).collect();
        let resume = IncrementalSnapshotState {
            paused: false,
            stopped: false,
            generation: 0,
            snapshot_id: "incremental-earlier-run".to_string(),
            tables: vec![IncrementalSnapshotTableState {
                table: "public.users".to_string(),
                pk_cursor: Some(vec![json!(2)]),
                is_complete: false,
                chunks_emitted: 1,
                rows_emitted: 2,
                condition: None,
            }],
        };

        let mut driver = IncrementalSnapshotDriver::new(
            FakeBackend {
                rows,
                clock: Arc::new(Mutex::new(100)),
                advance_on_read: 0,
                reads: Arc::new(Mutex::new(Vec::new())),
                in_flight: HashSet::new(),
            },
            Box::new(FakeStream {
                batches: VecDeque::new(),
            }),
            // Deliberately empty: the table exists only in the checkpoint.
            IncrementalSnapshotConfig::new(Vec::<String>::new()),
            "test".to_string(),
            Some(resume),
        )
        .await
        .expect("driver builds");

        let ids: Vec<i64> = drain(&mut driver)
            .await
            .iter()
            .filter_map(|event| event.after.as_ref()?.get("id")?.as_i64())
            .collect();
        assert_eq!(
            ids,
            vec![3, 4],
            "the adopted table must resume from its persisted cursor, not restart"
        );
    }

    #[tokio::test]
    async fn a_completed_table_in_the_checkpoint_is_not_adopted() {
        // Adopting a finished table would restart a snapshot nobody asked to repeat.
        let resume = IncrementalSnapshotState {
            paused: false,
            stopped: false,
            generation: 0,
            snapshot_id: "incremental-earlier-run".to_string(),
            tables: vec![IncrementalSnapshotTableState {
                table: "public.users".to_string(),
                pk_cursor: Some(vec![json!(4)]),
                is_complete: true,
                chunks_emitted: 2,
                rows_emitted: 4,
                condition: None,
            }],
        };

        let mut driver = IncrementalSnapshotDriver::new(
            FakeBackend {
                rows: (1..=4).map(|id| json!({ "id": id })).collect(),
                clock: Arc::new(Mutex::new(100)),
                advance_on_read: 0,
                reads: Arc::new(Mutex::new(Vec::new())),
                in_flight: HashSet::new(),
            },
            Box::new(FakeStream {
                batches: VecDeque::new(),
            }),
            IncrementalSnapshotConfig::new(Vec::<String>::new()),
            "test".to_string(),
            Some(resume),
        )
        .await
        .expect("driver builds");

        assert!(
            drain(&mut driver).await.is_empty(),
            "a completed table must not be re-read on restart"
        );
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
            in_flight: HashSet::new(),
        };
        let resume = IncrementalSnapshotState {
            paused: false,
            stopped: false,
            generation: 0,
            snapshot_id: "incremental-earlier-run".to_string(),
            tables: vec![IncrementalSnapshotTableState {
                table: "public.users".to_string(),
                pk_cursor: Some(vec![json!(3)]),
                is_complete: false,
                chunks_emitted: 2,
                rows_emitted: 3,
                condition: None,
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
            in_flight: HashSet::new(),
        };
        let resume = IncrementalSnapshotState {
            paused: false,
            stopped: false,
            generation: 0,
            snapshot_id: "done".to_string(),
            tables: vec![IncrementalSnapshotTableState {
                table: "public.users".to_string(),
                pk_cursor: Some(vec![json!(1)]),
                is_complete: true,
                chunks_emitted: 1,
                rows_emitted: 1,
                condition: None,
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
                    condition: None,
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
            paused: false,
            stopped: false,
            generation: 0,
            snapshot_id: "x".to_string(),
            tables: vec![IncrementalSnapshotTableState {
                table: "public.users".to_string(),
                pk_cursor: Some(vec![json!(1), json!(2)]),
                is_complete: false,
                chunks_emitted: 1,
                rows_emitted: 1,
                condition: None,
            }],
        };
        let Err(error) = IncrementalSnapshotDriver::new(
            FakeBackend {
                rows: vec![json!({ "id": 1 })],
                clock: Arc::new(Mutex::new(100)),
                advance_on_read: 0,
                reads: Arc::new(Mutex::new(Vec::new())),
                in_flight: HashSet::new(),
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

#[cfg(test)]
mod stop_durability_tests {
    use super::tests::*;
    use super::*;
    use serde_json::json;

    /// A stop that does not survive a restart is not a stop.
    ///
    /// `stop_snapshot` clears the per-table entries, and the driver seeds one entry per
    /// **configured** table on startup — so a configured table with no entry looked exactly
    /// like a table that had not started. The next deploy re-ran the whole backfill the
    /// operator had just stopped, typically to take load off a production primary, and the
    /// driver's own log line claimed the opposite would happen.
    #[tokio::test]
    async fn a_stopped_snapshot_stays_stopped_across_a_restart() {
        let rows = vec![json!({ "id": 1 }), json!({ "id": 2 })];
        let (mut driver, _reads) = driver_with(rows.clone(), Vec::new(), 0, 1).await;

        let abandoned = IncrementalSnapshotDriver::stop_snapshot(&mut driver);
        assert_eq!(abandoned, 1, "one table still had work outstanding");

        let state = driver
            .incremental_snapshot_state()
            .expect("the driver reports its state");
        assert!(
            state.stopped,
            "the stop must be recorded explicitly; an empty table list is indistinguishable \
             from a snapshot that has not started"
        );
        assert!(state.tables.is_empty());

        // Restart with the *same static config* — the case that used to silently restart.
        let mut resumed = driver_resuming_from(rows, Some(state), 1).await;

        let emitted = drain(&mut resumed).await;
        assert!(
            emitted.is_empty(),
            "a stopped snapshot must not re-read the configured tables: {} rows emitted",
            emitted.len()
        );
        assert!(
            resumed
                .incremental_snapshot_state()
                .is_some_and(|state| state.stopped),
            "the flag must persist until tables are re-requested"
        );
    }

    /// The flag must not become a one-way latch: re-requesting a table is the operator
    /// un-stopping the snapshot, and leaving it set would make *that* request vanish on the
    /// next restart for exactly the same reason.
    #[tokio::test]
    async fn requesting_a_table_clears_the_stopped_flag() {
        let (mut driver, _reads) = driver_with(vec![json!({ "id": 1 })], Vec::new(), 0, 10).await;
        IncrementalSnapshotDriver::stop_snapshot(&mut driver);
        assert!(driver.incremental_snapshot_state().unwrap().stopped);

        let enqueued = driver
            .request_snapshot_tables(vec!["public.users".to_string()].into())
            .await
            .expect("re-request succeeds");
        assert_eq!(enqueued, 1);
        assert!(
            !driver.incremental_snapshot_state().unwrap().stopped,
            "a re-request must clear the stop"
        );

        let emitted = drain(&mut driver).await;
        assert!(
            emitted.iter().any(|event| event.op == Operation::Read),
            "the re-requested table must actually be snapshotted"
        );
    }

    /// A state written by a build that had no way to express a stop must keep the old
    /// behaviour rather than being read as "stopped".
    #[tokio::test]
    async fn a_state_without_the_flag_is_not_read_as_stopped() {
        let json = r#"{"snapshot_id":"incremental-1","tables":[]}"#;
        let state: IncrementalSnapshotState =
            serde_json::from_str(json).expect("older state still deserialises");
        assert!(!state.stopped);
        assert!(!state.paused);
    }
}

#[cfg(test)]
mod commit_visibility_tests {
    use super::tests::*;
    use super::*;
    use serde_json::json;
    use std::collections::HashSet;

    /// The commit-visibility race, reproduced end to end.
    ///
    /// A transaction reaches the log at position 90 — **below** the low watermark of 100 —
    /// but has not yet become visible to new snapshots, so the chunk `SELECT` still reads
    /// the pre-image. The position test alone cannot see this: 90 is not in `(100, 110]`,
    /// so the chunk row is not suppressed and its stale value is emitted after the
    /// stream's newer one.
    ///
    /// The in-flight transaction set is what catches it.
    #[tokio::test]
    async fn a_transaction_below_the_low_watermark_but_still_invisible_is_suppressed() {
        let mut event = stream_event_at("users", 1, 90);
        event.transaction = Some(crate::core::TransactionMetadata::new(4242, 0, None));

        let (mut driver, _reads) = driver_with_in_flight(
            vec![json!({ "id": 1 }), json!({ "id": 2 })],
            vec![vec![event]],
            10,
            10,
            HashSet::from([4242]),
        )
        .await;

        let emitted = drain(&mut driver).await;
        let snapshot_ids: Vec<i64> = emitted
            .iter()
            .filter(|event| event.op == Operation::Read)
            .filter_map(|event| event.after.as_ref()?.get("id")?.as_i64())
            .collect();

        assert!(
            !snapshot_ids.contains(&1),
            "row 1 was modified by a transaction the chunk read could not see; emitting \
             the chunk's pre-image after the stream event resurrects the stale value. \
             Emitted snapshot ids: {snapshot_ids:?}"
        );
        assert!(
            snapshot_ids.contains(&2),
            "row 2 was untouched and must still be snapshotted: {snapshot_ids:?}"
        );
    }

    /// The set must not swallow the whole chunk. Only the transaction actually named is
    /// inside the bracket; an unrelated one below the low watermark stays outside it.
    #[tokio::test]
    async fn an_unrelated_transaction_below_the_low_watermark_suppresses_nothing() {
        let mut event = stream_event_at("users", 1, 90);
        event.transaction = Some(crate::core::TransactionMetadata::new(11, 0, None));

        let (mut driver, _reads) = driver_with_in_flight(
            vec![json!({ "id": 1 }), json!({ "id": 2 })],
            vec![vec![event]],
            10,
            10,
            // A different transaction is in flight; this event's is already committed.
            HashSet::from([9999]),
        )
        .await;

        let emitted = drain(&mut driver).await;
        let snapshot_ids: Vec<i64> = emitted
            .iter()
            .filter(|event| event.op == Operation::Read)
            .filter_map(|event| event.after.as_ref()?.get("id")?.as_i64())
            .collect();
        assert_eq!(
            snapshot_ids,
            vec![1, 2],
            "an event committed before the low watermark describes a state the chunk \
             already contains, so nothing may be suppressed"
        );
    }

    /// The upper bound stays absolute. An in-flight transaction whose event lands *past*
    /// the high watermark committed after the `SELECT` finished, and the chunk is emitted
    /// before it — suppressing it would discard the newer value instead of the older one.
    #[tokio::test]
    async fn the_high_watermark_still_bounds_the_in_flight_set() {
        let mut event = stream_event_at("users", 1, 500);
        event.transaction = Some(crate::core::TransactionMetadata::new(4242, 0, None));

        let (mut driver, _reads) = driver_with_in_flight(
            vec![json!({ "id": 1 })],
            vec![vec![event]],
            10,
            10,
            HashSet::from([4242]),
        )
        .await;

        let emitted = drain(&mut driver).await;
        let snapshot_ids: Vec<i64> = emitted
            .iter()
            .filter(|event| event.op == Operation::Read)
            .filter_map(|event| event.after.as_ref()?.get("id")?.as_i64())
            .collect();
        assert_eq!(
            snapshot_ids,
            vec![1],
            "an event past the high watermark must not suppress the chunk row, whatever \
             its transaction id"
        );
    }

    /// A backend that cannot supply the set (the default) must keep working exactly as
    /// before: the position bracket alone, with the race documented rather than silently
    /// mis-suppressing.
    #[tokio::test]
    async fn an_empty_in_flight_set_leaves_the_position_bracket_untouched() {
        let (mut driver, _reads) = driver_with_in_flight(
            vec![json!({ "id": 1 }), json!({ "id": 2 })],
            vec![vec![stream_event_at("users", 1, 105)]],
            10,
            10,
            HashSet::new(),
        )
        .await;

        let emitted = drain(&mut driver).await;
        let snapshot_ids: Vec<i64> = emitted
            .iter()
            .filter(|event| event.op == Operation::Read)
            .filter_map(|event| event.after.as_ref()?.get("id")?.as_i64())
            .collect();
        assert_eq!(
            snapshot_ids,
            vec![2],
            "position 105 is inside (100, 110], so row 1 must still be suppressed"
        );
    }
}

#[cfg(test)]
mod partial_payload_repair_tests {
    use super::tests::*;
    use super::*;
    use serde_json::json;
    use std::collections::HashSet;

    /// A stream event with a TOASTed column omitted, inside the bracket.
    fn toast_update_at(id: i64, position: u64, title: &str) -> Event {
        let mut event = stream_event_at("users", id, position);
        event.op = Operation::Update;
        // The shared fixture leaves `ts` at 0, which envelope validation rejects; set it so
        // `a_repaired_event_still_validates` tests the repair rather than the fixture.
        event.ts = 1;
        event.source.timestamp = 1;
        event.before = Some(json!({ "id": id }));
        event.before_is_key_only = true;
        // `body` is the unchanged-TOAST column: absent, not null.
        event.after = Some(json!({ "id": id, "title": title }));
        event.unavailable_columns = vec!["body".to_string()];
        event
    }

    fn delivered_update(events: &[Event], id: i64) -> &Event {
        events
            .iter()
            .find(|event| {
                event.op == Operation::Update
                    && event.after.as_ref().and_then(|a| a.get("id")?.as_i64()) == Some(id)
            })
            .expect("the update was delivered")
    }

    /// Without the repair the TOASTed column is in **neither** delivery: the event omits it
    /// and the chunk row carrying it is suppressed to keep ordering correct. A merge into a
    /// row the consumer does not have yet — the normal case during a first snapshot — applies
    /// nothing at all.
    #[tokio::test]
    async fn an_omitted_toast_column_is_filled_from_the_chunks_own_image() {
        let (mut driver, _reads) = driver_with(
            vec![json!({ "id": 1, "title": "old", "body": "a very large value" })],
            vec![vec![toast_update_at(1, 105, "new")]],
            10,
            10,
        )
        .await;

        let emitted = drain(&mut driver).await;

        // The chunk row is still suppressed: the stream event is newer.
        assert!(
            !emitted.iter().any(|event| event.op == Operation::Read),
            "the superseded chunk row must not be emitted after the newer event"
        );

        let update = delivered_update(&emitted, 1);
        assert!(
            update.unavailable_columns.is_empty(),
            "the repaired event must declare no unavailable columns, or a sink still \
             treats it as partial: {:?}",
            update.unavailable_columns
        );
        assert_eq!(
            update.after.as_ref().unwrap()["body"],
            json!("a very large value"),
            "the column the UPDATE did not modify must carry its real value"
        );
        assert_eq!(
            update.after.as_ref().unwrap()["title"],
            json!("new"),
            "the column the UPDATE did modify must keep the event's value, not the chunk's"
        );
        // The whole point: it is now a complete write rather than a merge into nothing.
        assert!(
            !update.row_write().is_partial(),
            "a repaired event must yield a full-row write"
        );
    }

    /// The shadow image has to track earlier in-bracket events, not just the chunk read.
    /// Event A writes `body`; event B omits it. Filling B from the chunk would resurrect the
    /// pre-snapshot value and silently undo A.
    #[tokio::test]
    async fn a_later_event_is_filled_from_an_earlier_events_value_not_the_chunks() {
        let mut wrote_body = stream_event_at("users", 1, 104);
        wrote_body.op = Operation::Update;
        wrote_body.before = Some(json!({ "id": 1 }));
        wrote_body.before_is_key_only = true;
        wrote_body.after = Some(json!({ "id": 1, "title": "mid", "body": "rewritten" }));

        let (mut driver, _reads) = driver_with(
            vec![json!({ "id": 1, "title": "old", "body": "stale" })],
            vec![vec![wrote_body, toast_update_at(1, 105, "new")]],
            10,
            10,
        )
        .await;

        let emitted = drain(&mut driver).await;
        let repaired = emitted
            .iter()
            .rfind(|event| event.op == Operation::Update)
            .expect("both updates were delivered");
        assert_eq!(
            repaired.after.as_ref().unwrap()["body"],
            json!("rewritten"),
            "filling from the chunk image would resurrect the pre-snapshot value and undo \
             the earlier event"
        );
    }

    /// The before-image's holes describe the state *prior* to the event, which is what the
    /// shadow holds when the event is processed.
    #[tokio::test]
    async fn a_partial_before_image_is_filled_from_the_pre_event_state() {
        let mut event = stream_event_at("users", 1, 105);
        event.op = Operation::Update;
        event.before = Some(json!({ "id": 1, "title": "old" }));
        event.after = Some(json!({ "id": 1, "title": "new", "body": "fresh" }));
        event.before_unavailable_columns = vec!["body".to_string()];

        let (mut driver, _reads) = driver_with(
            vec![json!({ "id": 1, "title": "old", "body": "prior" })],
            vec![vec![event]],
            10,
            10,
        )
        .await;

        let emitted = drain(&mut driver).await;
        let update = delivered_update(&emitted, 1);
        assert!(update.before_unavailable_columns.is_empty());
        assert_eq!(
            update.before.as_ref().unwrap()["body"],
            json!("prior"),
            "the before-image must be filled from the state before the event, not after it"
        );
    }

    /// A repaired event must still satisfy the envelope contract — in particular, a column
    /// may not be both listed as unavailable and present in the payload.
    #[tokio::test]
    async fn a_repaired_event_still_validates() {
        let (mut driver, _reads) = driver_with(
            vec![json!({ "id": 1, "title": "old", "body": "large" })],
            vec![vec![toast_update_at(1, 105, "new")]],
            10,
            10,
        )
        .await;

        let emitted = drain(&mut driver).await;
        for event in &emitted {
            event
                .validate()
                .expect("every delivered event must satisfy the envelope contract");
        }
    }

    /// An event for a key this chunk did not read has no chunk row being suppressed for it,
    /// so there is nothing to repair from — and nothing lost. It must pass through untouched
    /// rather than being filled from another row's image.
    #[tokio::test]
    async fn an_event_for_a_key_outside_the_chunk_is_left_alone() {
        let (mut driver, _reads) = driver_with(
            vec![json!({ "id": 1, "title": "old", "body": "large" })],
            // id 99 is not in the chunk.
            vec![vec![toast_update_at(99, 105, "new")]],
            10,
            10,
        )
        .await;

        let emitted = drain(&mut driver).await;
        let update = delivered_update(&emitted, 99);
        assert_eq!(
            update.unavailable_columns,
            vec!["body".to_string()],
            "an event outside the chunk must keep its own partiality rather than borrow \
             another row's value"
        );
        assert!(
            update.after.as_ref().unwrap().get("body").is_none(),
            "no value may be invented for a row the chunk never read"
        );
        // Row 1 was untouched by the stream, so its chunk row is still emitted.
        assert!(
            emitted.iter().any(|event| event.op == Operation::Read),
            "the untouched chunk row must still be snapshotted"
        );
    }

    /// An event *past* the high watermark is outside the bracket: the chunk is emitted before
    /// it, so the consumer does have the row and a merge is the correct thing to deliver.
    #[tokio::test]
    async fn an_event_past_the_high_watermark_is_not_repaired() {
        let (mut driver, _reads) = driver_with_in_flight(
            vec![json!({ "id": 1, "title": "old", "body": "large" })],
            vec![vec![toast_update_at(1, 500, "new")]],
            10,
            10,
            HashSet::new(),
        )
        .await;

        let emitted = drain(&mut driver).await;
        let update = delivered_update(&emitted, 1);
        assert_eq!(
            update.unavailable_columns,
            vec!["body".to_string()],
            "past the high watermark the chunk row is delivered first, so the merge applies \
             to a row the consumer already has"
        );
        assert!(
            emitted.iter().any(|event| event.op == Operation::Read),
            "the chunk row must be emitted, since nothing superseded it inside the bracket"
        );
    }
}

#[cfg(test)]
mod row_filter_tests {
    use super::tests::*;
    use super::*;
    use serde_json::json;

    fn config_with_condition(
        tables: Vec<&str>,
        condition: Option<&str>,
    ) -> IncrementalSnapshotConfig {
        let mut config = IncrementalSnapshotConfig::new(
            tables.into_iter().map(String::from).collect::<Vec<_>>(),
        );
        config.chunk_size = 10;
        if let Some(condition) = condition {
            config
                .table_conditions
                .insert("public.users".to_string(), condition.to_string());
        }
        config
    }

    /// The configured filter must reach a table requested at **runtime**.
    ///
    /// It used to be applied at two of the three resolution sites — startup tables and tables
    /// adopted from a checkpoint — and not at `enqueue_tables`, which services every
    /// on-demand request. The driver did not even retain the config, so honouring it there was
    /// structurally impossible. An operator scoping a backfill to one tenant, then firing the
    /// request, got the whole table, and the only symptom was volume.
    #[tokio::test]
    async fn a_runtime_requested_table_gets_the_configured_condition() {
        // `tables = []` plus a condition is the natural way to pre-declare a filter for an
        // on-demand snapshot, and is exactly the shape that silently lost it.
        let mut driver = driver_with_config(
            vec![json!({ "id": 1 })],
            config_with_condition(Vec::new(), Some("tenant_id = 42")),
        )
        .await;

        driver
            .request_snapshot_tables(SnapshotRequest::new(["public.users"]))
            .await
            .expect("request succeeds");

        let condition = driver.incremental_snapshot_state().and_then(|state| {
            state
                .tables
                .first()
                .and_then(|table| table.condition.clone())
        });
        assert_eq!(
            condition.as_deref(),
            Some("tenant_id = 42"),
            "an on-demand request must inherit the configured filter, or it reads more data \
             than the operator asked for"
        );
    }

    /// A request may carry its own filter, and it overrides the configured one.
    #[tokio::test]
    async fn a_request_condition_overrides_the_configured_one() {
        let mut driver = driver_with_config(
            vec![json!({ "id": 1 })],
            config_with_condition(Vec::new(), Some("tenant_id = 42")),
        )
        .await;

        driver
            .request_snapshot_tables(
                SnapshotRequest::new(["public.users"]).with_condition("public.users", "id > 100"),
            )
            .await
            .expect("request succeeds");

        let condition = driver.incremental_snapshot_state().and_then(|state| {
            state
                .tables
                .first()
                .and_then(|table| table.condition.clone())
        });
        assert_eq!(condition.as_deref(), Some("id > 100"));
    }

    /// Re-requesting a finished table must adopt the new request's filter rather than keeping
    /// the previous one — running a new request under an old filter is the same class of
    /// defect as ignoring the filter outright.
    #[tokio::test]
    async fn re_requesting_a_finished_table_adopts_the_new_condition() {
        let mut driver = driver_with_config(
            vec![json!({ "id": 1 })],
            config_with_condition(vec!["public.users"], Some("tenant_id = 42")),
        )
        .await;

        // Drain so the table reaches `is_complete`, which is the re-snapshot path.
        let _ = drain(&mut driver).await;

        driver
            .request_snapshot_tables(
                SnapshotRequest::new(["public.users"]).with_condition("public.users", "id > 100"),
            )
            .await
            .expect("re-request succeeds");

        let condition = driver.incremental_snapshot_state().and_then(|state| {
            state
                .tables
                .first()
                .and_then(|table| table.condition.clone())
        });
        assert_eq!(
            condition.as_deref(),
            Some("id > 100"),
            "a rewound table must take the new request's filter"
        );
    }

    /// The consequence the original report missed: the adoption path *did* apply the filter,
    /// so a runtime-requested table ran unfiltered and then a restart adopted it **with** the
    /// filter. The emitted rows then corresponded to no single predicate, and where the split
    /// fell depended on when the process happened to restart. Both paths must agree.
    #[tokio::test]
    async fn the_runtime_and_restart_paths_resolve_the_same_condition() {
        let rows = vec![json!({ "id": 1 }), json!({ "id": 2 })];
        let mut driver = driver_with_config(
            rows.clone(),
            config_with_condition(Vec::new(), Some("tenant_id = 42")),
        )
        .await;
        driver
            .request_snapshot_tables(SnapshotRequest::new(["public.users"]))
            .await
            .expect("request succeeds");
        let before_restart = driver
            .incremental_snapshot_state()
            .expect("state is reported");

        let resumed = driver_with_config_resuming(
            rows,
            config_with_condition(Vec::new(), Some("tenant_id = 42")),
            Some(before_restart.clone()),
        )
        .await;
        let after_restart = resumed.incremental_snapshot_state().expect("state");

        let condition_of = |state: &IncrementalSnapshotState| {
            state
                .tables
                .iter()
                .find(|table| table.table == "public.users")
                .and_then(|table| table.condition.clone())
        };
        assert_eq!(
            condition_of(&before_restart),
            condition_of(&after_restart),
            "a table must not change which rows it snapshots because the process restarted"
        );
        assert_eq!(
            condition_of(&after_restart).as_deref(),
            Some("tenant_id = 42")
        );
    }

    #[tokio::test]
    async fn a_table_with_no_filter_reports_none() {
        let mut driver = driver_with_config(
            vec![json!({ "id": 1 })],
            config_with_condition(Vec::new(), None),
        )
        .await;
        driver
            .request_snapshot_tables(SnapshotRequest::new(["public.users"]))
            .await
            .expect("request succeeds");
        assert!(driver
            .incremental_snapshot_state()
            .and_then(|state| state
                .tables
                .first()
                .and_then(|table| table.condition.clone()))
            .is_none());
    }
}

#[cfg(test)]
mod resnapshot_identity_tests {
    use super::tests::*;
    use super::*;
    use serde_json::json;

    /// A deliberate re-snapshot must not be mistaken for a replay and dropped.
    ///
    /// A snapshot read's offset identifies the *row*, not a log position, so re-reading an
    /// unchanged row used to produce a byte-identical event — and the runtime's idempotency
    /// guard, which is **on by default**, correctly classified it as a duplicate and dropped
    /// it. The operator got `enqueued: 1` and no rows: the component whose job is to protect
    /// delivery silently discarded the delivery that was asked for.
    ///
    /// This drives the real driver twice and puts the emitted events through the real guard,
    /// so it fails if either half of the chain regresses.
    #[tokio::test]
    async fn a_re_snapshotted_row_survives_the_idempotency_guard() {
        let rows = vec![json!({ "id": 1, "name": "a" })];
        let (mut driver, _reads) = driver_with(rows, Vec::new(), 0, 10).await;
        let mut guard = crate::core::EventIdempotencyGuard::new(1024).expect("guard");

        let first = drain(&mut driver).await;
        let delivered_first = first
            .iter()
            .filter(|event| event.op == Operation::Read)
            .filter(|event| guard.should_process(event).expect("fingerprintable"))
            .count();
        assert_eq!(
            delivered_first, 1,
            "the first snapshot must deliver the row"
        );

        // The table is now complete; this is the deliberate re-snapshot path.
        let enqueued = driver
            .request_snapshot_tables(SnapshotRequest::new(["public.users"]))
            .await
            .expect("re-request succeeds");
        assert_eq!(enqueued, 1);

        let second = drain(&mut driver).await;
        let read_again: Vec<&Event> = second
            .iter()
            .filter(|event| event.op == Operation::Read)
            .collect();
        assert_eq!(read_again.len(), 1, "the driver must re-read the row");

        let delivered_second = read_again
            .iter()
            .filter(|event| guard.should_process(event).expect("fingerprintable"))
            .count();
        assert_eq!(
            delivered_second, 1,
            "a re-snapshotted row must reach the consumer. It was suppressed as a duplicate \
             because its identity did not record which snapshot generation produced it, so an \
             operator's re-snapshot request completed successfully and delivered nothing."
        );
    }

    /// The counterpart: within one generation, a re-read of the same row is still a duplicate.
    /// A mid-snapshot reconnect re-reads at most one chunk, and suppressing that is the guard's
    /// job — the generation must not turn every replay into a fresh delivery.
    #[tokio::test]
    async fn a_replay_within_one_generation_is_still_suppressed() {
        let rows = vec![json!({ "id": 1, "name": "a" })];
        let (mut driver, _reads) = driver_with(rows.clone(), Vec::new(), 0, 10).await;
        let mut guard = crate::core::EventIdempotencyGuard::new(1024).expect("guard");

        let emitted = drain(&mut driver).await;
        let row = emitted
            .iter()
            .find(|event| event.op == Operation::Read)
            .expect("a snapshot row");
        assert!(guard.should_process(row).expect("fingerprintable"));
        assert!(
            !guard.should_process(row).expect("fingerprintable"),
            "the same event delivered twice inside one generation is a replay and must still \
             be suppressed"
        );
    }

    /// A stop discards the table list, so a later re-request would otherwise restart at
    /// generation 0 and collide with the run it abandoned.
    #[tokio::test]
    async fn a_request_after_a_stop_starts_a_fresh_generation() {
        let rows = vec![json!({ "id": 1, "name": "a" })];
        let (mut driver, _reads) = driver_with(rows, Vec::new(), 0, 10).await;
        let before = driver
            .incremental_snapshot_state()
            .expect("state")
            .generation;

        IncrementalSnapshotDriver::stop_snapshot(&mut driver);
        driver
            .request_snapshot_tables(SnapshotRequest::new(["public.users"]))
            .await
            .expect("re-request succeeds");

        assert!(
            driver
                .incremental_snapshot_state()
                .expect("state")
                .generation
                > before,
            "the generation must advance across a stop, or the re-request's rows collide with \
             the abandoned run's"
        );
    }

    #[tokio::test]
    async fn the_generation_survives_a_restart() {
        let rows = vec![json!({ "id": 1, "name": "a" })];
        let (mut driver, _reads) = driver_with(rows.clone(), Vec::new(), 0, 10).await;
        let _ = drain(&mut driver).await;
        driver
            .request_snapshot_tables(SnapshotRequest::new(["public.users"]))
            .await
            .expect("re-request succeeds");
        let state = driver.incremental_snapshot_state().expect("state");
        assert!(state.generation > 0);

        let resumed = driver_resuming_from(rows, Some(state.clone()), 10).await;
        assert_eq!(
            resumed
                .incremental_snapshot_state()
                .expect("state")
                .generation,
            state.generation,
            "the offsets are documented as stable across a restart, so the generation they \
             embed has to be persisted"
        );
    }
}
