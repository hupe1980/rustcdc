//! Source traits, connector configuration, and feature-gated connector modules.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    checkpoint::Checkpoint,
    core::{Event, Offset, Result},
};

pub(crate) mod helpers;
pub mod incremental_snapshot;
pub mod snapshot_progress;
pub mod snapshot_tracker;
pub mod snapshot_validator;

pub use incremental_snapshot::{
    state_from_offset as incremental_snapshot_state_from_offset, IncrementalSnapshotState,
    IncrementalSnapshotTableState,
};
pub use incremental_snapshot::{
    BracketPosition, ChunkRow, IncrementalSnapshotBackend, IncrementalSnapshotDriver, SnapshotTable,
};
pub use snapshot_progress::{SnapshotCheckpointHelper, SnapshotProgress, TableProgress};
pub use snapshot_tracker::{SnapshotProgressTracker, SnapshotTrackerConfig, SnapshotTrackerReport};
pub use snapshot_validator::{SnapshotValidationResult, SnapshotValidator};

/// Authentication mode for database source connectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DatabaseAuthMode {
    /// Traditional static password authentication.
    #[default]
    Password,
    /// AWS IAM database authentication using short-lived auth tokens.
    ///
    /// Token generation/rotation is provided by the embedder via `SecretString`
    /// deferred resolution patterns.
    AwsIamToken,
}

/// One on-demand incremental-snapshot request: which tables, and which rows of them.
///
/// # Why the filter belongs to the request
///
/// [`IncrementalSnapshotConfig::table_conditions`] scopes a snapshot the *deployment*
/// declares. An on-demand backfill is not that shape: "backfill tenant 42's orders" is a
/// one-off, and expressing it through static configuration means editing a config file and
/// restarting the process to run something that was supposed to be a signal. Debezium's
/// `execute-snapshot` carries `data-collections` and `additional-conditions` together for the
/// same reason.
///
/// Conditions set here **override** the configured ones for the same table; a table with no
/// override falls back to its configured condition, so static configuration keeps working.
///
/// # This is raw SQL and it is trusted input
///
/// Same trust level as [`IncrementalSnapshotConfig::table_conditions`] — the expression is
/// interpolated into the chunk `SELECT`, because a filter restricted to bound parameters
/// could not express the predicates a filter exists for. **Never build one from untrusted
/// input**, and do not treat it as a tenancy boundary.
///
/// ```
/// use rustcdc::source::SnapshotRequest;
///
/// let request = SnapshotRequest::new(["public.orders", "public.order_items"])
///     .with_condition("public.orders", "tenant_id = 42")
///     .with_condition("public.order_items", "tenant_id = 42");
/// assert_eq!(request.tables.len(), 2);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SnapshotRequest {
    /// Tables to snapshot, in `"schema.table"` form. Processed in order.
    pub tables: Vec<String>,
    /// Per-table row filter for **this request**, keyed the same way as
    /// [`IncrementalSnapshotConfig::table_conditions`], which it overrides.
    pub conditions: ahash::AHashMap<String, String>,
}

impl SnapshotRequest {
    /// A request for `tables`, with no row filter.
    pub fn new<I, S>(tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            tables: tables.into_iter().map(Into::into).collect(),
            conditions: ahash::AHashMap::new(),
        }
    }

    /// Restrict `table` to rows matching `condition` for this request only.
    ///
    /// `table` is matched against both the reference as written and the catalog's canonical
    /// `"schema.table"`, so `orders` and `public.orders` both work — a filter that silently
    /// failed to match its table would snapshot the whole table, which is the load the filter
    /// was written to avoid.
    #[must_use]
    pub fn with_condition(
        mut self,
        table: impl Into<String>,
        condition: impl Into<String>,
    ) -> Self {
        self.conditions.insert(table.into(), condition.into());
        self
    }
}

impl<S: Into<String>> From<Vec<S>> for SnapshotRequest {
    fn from(tables: Vec<S>) -> Self {
        Self::new(tables)
    }
}

// ─── Table filtering ─────────────────────────────────────────────────────────

/// Returns `true` if an event for `schema.table` should be forwarded to the caller.
///
/// * When `include_list` is non-empty, only matching tables pass through.
/// * When `include_list` is empty and `exclude_list` is non-empty, matching tables are dropped.
/// * When both lists are empty, all events pass through.
///
/// # Entries are case-insensitive glob patterns
///
/// An entry is matched against `"schema.table"` (or the bare table name when the source
/// reports no schema) using the same glob semantics as
/// [`table_matches`](crate::pipeline::table_matches): `*` and `?` inside a segment,
/// `"schema.*"` for a whole schema, `"*.name"` for a name in any schema.
///
/// These lists used to match **exact strings only**, while the sink router matched globs
/// and nothing documented the difference. `table_exclude_list = ["public.tmp_*"]`
/// therefore excluded nothing, which looks identical to a set of tables that never
/// changed — and on the include side an allowlist matching nothing looks identical to an
/// idle database. One semantics now, in one implementation.
///
/// An **unqualified** entry such as `"orders"` matches that table in *every* schema; see
/// [`crate::core::glob::table_matches`] for why, and
/// [`warn_on_schema_agnostic_include_entries`] for the startup warning that says so.
#[cfg_attr(
    not(any(feature = "postgres", feature = "mysql", feature = "sqlserver")),
    allow(dead_code)
)]
pub(crate) fn table_is_allowed(
    schema: Option<&str>,
    table: &str,
    include_list: &[String],
    exclude_list: &[String],
) -> bool {
    // Fast path: no filtering configured — avoids all allocations on the hot path.
    if include_list.is_empty() && exclude_list.is_empty() {
        return true;
    }

    // Glob matching is case-sensitive, and every supported server treats at least some
    // identifiers case-insensitively, so both sides are lowered once here.
    let key = match schema {
        Some(schema) if !schema.is_empty() => {
            format!(
                "{}.{}",
                schema.to_ascii_lowercase(),
                table.to_ascii_lowercase()
            )
        }
        _ => table.to_ascii_lowercase(),
    };

    let matches = |list: &[String]| {
        list.iter().any(|entry| {
            let pattern = entry.trim();
            !pattern.is_empty()
                && crate::core::glob::table_matches(&pattern.to_ascii_lowercase(), &key)
        })
    };

    if !include_list.is_empty() {
        return matches(include_list);
    }
    !matches(exclude_list)
}

/// Log a warning for every allowlist entry that names no schema.
///
/// An unqualified entry is schema-agnostic, so `table_include_list = ["users"]` captures
/// `public.users` *and* `tenant_private.users`. That is a widening of an allowlist, which
/// is the direction that matters: the operator wrote the list to bound what leaves the
/// database. Rejecting it outright would break MySQL, where tables are habitually named
/// bare, so this states the consequence once at startup instead.
#[cfg_attr(
    not(any(feature = "postgres", feature = "mysql", feature = "sqlserver")),
    allow(dead_code)
)]
pub(crate) fn warn_on_schema_agnostic_include_entries(connector: &str, include_list: &[String]) {
    for entry in include_list {
        let pattern = entry.trim();
        if crate::core::glob::is_schema_agnostic(pattern) {
            tracing::warn!(
                target: "rustcdc::source",
                connector,
                entry = %pattern,
                "table_include_list entry names no schema, so it matches this table in \
                 every schema the connector can see — including ones the allowlist was \
                 written to keep out. Qualify it as \"schema.{pattern}\" to bound it.",
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Outcome of finishing a snapshot phase.
pub struct SnapshotEnd {
    /// Unix epoch milliseconds when the snapshot completed.
    ///
    /// The handoff uses this to reason about the overlap window between the snapshot's
    /// consistent view and the point the stream resumes from.
    pub snapshot_end_ts: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// Outcome of the snapshot-to-stream handoff.
///
/// The handoff is the point where a data-loss window would open if the stream resumed
/// after the snapshot's view rather than at or before it, so these fields exist to make
/// the relationship observable rather than assumed.
pub struct HandoffResult {
    /// Unix epoch milliseconds when the snapshot completed, if the connector reports it.
    pub snapshot_end_ts: Option<u64>,
    /// Unix epoch milliseconds the stream resumed from, if the connector reports it.
    pub stream_start_ts: Option<u64>,
    /// Number of overlap events dropped during handoff deduplication.
    ///
    /// `None` means the connector does not measure overlap at handoff (e.g. PostgreSQL).
    /// `Some(0)` means the handoff was clean — no duplicate events were observed.
    pub overlap_events_dropped: Option<u64>,
    /// Optional source-specific watermark distance observed at handoff.
    ///
    /// For PostgreSQL this is an LSN delta in bytes. Connectors that cannot
    /// provide a reliable watermark distance should leave this as `None`.
    pub stream_watermark_gap: Option<u64>,
}

/// Configuration for incremental (non-blocking) snapshot using the DBLog watermark pattern.
///
/// Used by `PostgresConnection::start_incremental_snapshot`,
/// `MysqlConnection::start_incremental_snapshot`, and
/// `SqlServerConnection::start_incremental_snapshot`.
///
/// Unlike the blocking bulk snapshot, incremental snapshotting interleaves chunk reads
/// with the live replication stream. The stream never pauses, no long-held
/// `REPEATABLE READ` transaction accumulates transaction IDs, and each chunk is
/// independently resumable after a crash.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct IncrementalSnapshotConfig {
    /// Tables to snapshot in `"schema.table"` format. Tables are processed in order.
    pub tables: Vec<String>,
    /// Number of rows to read per chunk. Defaults to `5_000`.
    pub chunk_size: usize,
    /// Optional per-table row filter, keyed by `"schema.table"`.
    ///
    /// The value is a SQL boolean expression appended to that table's chunk `SELECT`,
    /// which is Debezium's `additional-condition` in the equivalent signal. It restricts
    /// *which rows are snapshotted* — backfill one tenant, or only rows newer than a
    /// cutoff — without restricting the live stream, which continues to carry every change
    /// to the table.
    ///
    /// Referenced columns must exist on the table and be usable in a `WHERE` clause. Alias
    /// the table as `t` if you need to qualify a column; that is the alias every connector's
    /// chunk read uses.
    ///
    /// # This is raw SQL and it is trusted input
    ///
    /// The expression is interpolated into the chunk `SELECT`, because a filter that could
    /// only be a bound parameter would not be a filter — it has to express predicates the
    /// connector cannot anticipate. **Never build one from untrusted input.** It carries the
    /// same trust level as the connection string, and the same is true of Debezium's.
    ///
    /// A filter that fails to parse surfaces as a chunk-read error naming the table, at the
    /// first chunk rather than silently returning nothing.
    pub table_conditions: ahash::AHashMap<String, String>,
}

impl IncrementalSnapshotConfig {
    /// Create a new config with the given tables and the default chunk size (5,000).
    pub fn new(tables: impl Into<Vec<String>>) -> Self {
        Self {
            tables: tables.into(),
            chunk_size: 5_000,
            table_conditions: ahash::AHashMap::new(),
        }
    }

    /// Override the per-chunk row limit.
    #[must_use]
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Restrict which rows of `table` the snapshot reads.
    ///
    /// See [`table_conditions`](Self::table_conditions) for the semantics and the trust
    /// boundary — `condition` is raw SQL and must never come from untrusted input.
    ///
    /// ```
    /// use rustcdc::IncrementalSnapshotConfig;
    ///
    /// let config = IncrementalSnapshotConfig::new(vec!["public.orders".to_string()])
    ///     .with_table_condition("public.orders", "created_at >= '2026-01-01'");
    /// # let _ = config;
    /// ```
    #[must_use]
    pub fn with_table_condition(
        mut self,
        table: impl Into<String>,
        condition: impl Into<String>,
    ) -> Self {
        self.table_conditions.insert(table.into(), condition.into());
        self
    }
}

/// Declares connector feature support for runtime and embedder introspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ConnectorCapabilities {
    /// Connector can perform a blocking bulk snapshot.
    pub snapshot: bool,
    /// A snapshot interrupted mid-way can be resumed from its checkpoint rather than
    /// restarted. When `false`, the runtime warns and restarts the snapshot from scratch.
    pub snapshot_checkpoint_resume: bool,
    /// Connector implements the snapshot-to-stream handoff.
    pub handoff: bool,
    /// Connector surfaces DDL as `Operation::SchemaChange` events.
    pub ddl_capture: bool,
    /// Connector maintains a keepalive against the source.
    pub heartbeat: bool,
    /// Connector supports an encrypted transport.
    pub tls: bool,
    /// Connector can read column and key metadata from the source catalog.
    pub schema_introspection: bool,
    /// Whether the connector surfaces `TRUNCATE` operations as
    /// [`crate::core::Operation::Truncate`] events.
    ///
    /// **PostgreSQL, MySQL, and MariaDB** emit `Truncate` events.
    ///
    /// - PostgreSQL: captured via the `pgoutput` logical replication protocol.
    /// - MySQL/MariaDB: captured from the `TRUNCATE TABLE` `QueryEvent` in the
    ///   binlog (logged as a DDL statement, not a rows event). Respects
    ///   `table_include_list` / `table_exclude_list` filters.
    ///
    /// **SQL Server CDC** (`cdc.fn_cdc_get_all_changes_*`) does **not** record
    /// `TRUNCATE TABLE` in the change tables because TRUNCATE bypasses row-level
    /// logging.  When `SqlServerSourceConfig::capture_truncate_events` is
    /// `true`, rustcdc installs a database-level DDL trigger that records each
    /// `TRUNCATE TABLE` in a shadow table and emits an `Operation::Truncate`
    /// event positioned after all DML changes at the LSN captured by the trigger.
    /// The SQL Server connector reports `truncate: true` only when this option is
    /// enabled.
    pub truncate: bool,
    /// Whether the connector supports non-blocking incremental snapshot via the
    /// DBLog watermark pattern (`PostgresConnection::start_incremental_snapshot`).
    pub incremental_snapshot: bool,
}

impl ConnectorCapabilities {
    /// Capability set for disabled or unknown sources.
    pub const fn none() -> Self {
        Self {
            snapshot: false,
            snapshot_checkpoint_resume: false,
            handoff: false,
            ddl_capture: false,
            heartbeat: false,
            tls: false,
            schema_introspection: false,
            truncate: false,
            incremental_snapshot: false,
        }
    }
}

/// Builder methods for [`ConnectorCapabilities`].
///
/// The struct is `#[non_exhaustive]` so that a new capability is not a breaking change for
/// downstream crates — but that also means a third-party connector cannot write a struct
/// literal, and cannot use `..none()` either. Without these methods the only capability set
/// reachable from outside this crate is [`ConnectorCapabilities::none`], which makes
/// `Source::capabilities` impossible to override honestly.
///
/// Start from `none()` and enable what the connector actually implements:
///
/// ```
/// use rustcdc::source::ConnectorCapabilities;
///
/// let capabilities = ConnectorCapabilities::none()
///     .with_snapshot(true)
///     .with_handoff(true)
///     .with_tls(true);
/// assert!(capabilities.snapshot);
/// assert!(!capabilities.ddl_capture);
/// ```
impl ConnectorCapabilities {
    /// Declare support for a blocking bulk snapshot.
    #[must_use]
    pub const fn with_snapshot(mut self, supported: bool) -> Self {
        self.snapshot = supported;
        self
    }

    /// Declare that an interrupted snapshot resumes from its checkpoint rather than
    /// restarting. Claiming this falsely makes the runtime skip a warning it would
    /// otherwise emit before re-reading a table from row zero.
    #[must_use]
    pub const fn with_snapshot_checkpoint_resume(mut self, supported: bool) -> Self {
        self.snapshot_checkpoint_resume = supported;
        self
    }

    /// Declare support for the snapshot-to-stream handoff.
    #[must_use]
    pub const fn with_handoff(mut self, supported: bool) -> Self {
        self.handoff = supported;
        self
    }

    /// Declare that DDL is surfaced as [`crate::core::Operation::SchemaChange`].
    #[must_use]
    pub const fn with_ddl_capture(mut self, supported: bool) -> Self {
        self.ddl_capture = supported;
        self
    }

    /// Declare that the connector keeps a keepalive against the source.
    #[must_use]
    pub const fn with_heartbeat(mut self, supported: bool) -> Self {
        self.heartbeat = supported;
        self
    }

    /// Declare support for an encrypted transport.
    #[must_use]
    pub const fn with_tls(mut self, supported: bool) -> Self {
        self.tls = supported;
        self
    }

    /// Declare that column and key metadata is read from the source catalog.
    #[must_use]
    pub const fn with_schema_introspection(mut self, supported: bool) -> Self {
        self.schema_introspection = supported;
        self
    }

    /// Declare that `TRUNCATE` is surfaced as [`crate::core::Operation::Truncate`].
    #[must_use]
    pub const fn with_truncate(mut self, supported: bool) -> Self {
        self.truncate = supported;
        self
    }

    /// Declare support for the non-blocking DBLog incremental snapshot.
    #[must_use]
    pub const fn with_incremental_snapshot(mut self, supported: bool) -> Self {
        self.incremental_snapshot = supported;
        self
    }
}

impl Default for ConnectorCapabilities {
    /// No capabilities — the honest starting point for a connector that has not declared
    /// any. Equivalent to [`ConnectorCapabilities::none`].
    fn default() -> Self {
        Self::none()
    }
}

#[async_trait]
/// A blocking bulk snapshot in progress.
///
/// Driven by the runtime until `next_chunk` returns empty, at which point the handoff to
/// streaming runs.
pub trait SnapshotHandle: Send + Sync {
    /// Read up to `chunk_size` rows as `Operation::Read` events.
    ///
    /// An empty result means the snapshot is exhausted — it is the completion signal, not
    /// an error, and not "nothing right now".
    async fn next_chunk(&mut self, chunk_size: usize) -> Result<Vec<Event>>;

    /// Persist snapshot progress using connector-native structured state.
    ///
    /// Called before the runtime commits a batch containing snapshot rows, so a crash
    /// mid-snapshot resumes at the row boundary rather than restarting the table. The
    /// state must include whatever the connector needs to derive the stream start
    /// position later — losing it opens a data-loss window at the handoff.
    async fn checkpoint(
        &self,
        checkpoint: &mut dyn Checkpoint,
        committed_event_count: u64,
    ) -> Result<()>;

    /// Release snapshot resources and report when the snapshot's view ended.
    async fn finish(&mut self) -> Result<SnapshotEnd>;
}

#[async_trait]
/// A live change stream in progress.
pub trait StreamHandle: Send + Sync {
    /// Read the next batch of change events, returning within `timeout_ms`.
    ///
    /// An empty result means "nothing available within the budget" — **not** end of
    /// stream. Implementations must treat `timeout_ms` as a wall-clock bound on batch
    /// assembly, not merely on waiting for the first event: accumulating until a size cap
    /// is reached makes every event in the batch wait for the last one.
    async fn next_events(&mut self, timeout_ms: u64) -> Result<Vec<Event>>;

    /// Persist this handle's current position.
    ///
    /// Used by direct `StreamHandle` consumers; the runtime commits through the commit
    /// barrier instead. An implementation carrying state beyond the log position (an
    /// incremental-snapshot cursor, say) must persist that here too, or an explicit
    /// shutdown forfeits it.
    async fn save_position(&self, checkpoint: &mut dyn Checkpoint) -> Result<()>;
    /// Requeue events so they are returned by a subsequent `next_events` call.
    ///
    /// This is used by snapshot-to-stream handoff to prefetch overlap events,
    /// apply deduplication, and preserve forward delivery order.
    async fn requeue_events(&mut self, _events: Vec<Event>) -> Result<()> {
        Ok(())
    }
    /// Add tables to an in-flight incremental snapshot.
    ///
    /// This is the library equivalent of Debezium's `execute-snapshot` signal: it backfills a
    /// table on a **running** pipeline, without a restart and without a signal table in the
    /// source. Reach for it when a table is added to the publication, when a downstream store
    /// needs rebuilding, or when a bad transform has to be re-run over history.
    ///
    /// Returns the number of tables actually enqueued.
    ///
    /// # Semantics
    ///
    /// * A table **not currently tracked** is added and read from the start.
    /// * A table **already in progress** is left alone, so the call is idempotent. Restarting
    ///   it mid-flight would re-deliver rows the consumer already has.
    /// * A table **already complete** is reset and read again — the re-snapshot case above.
    ///
    /// The call is atomic: every reference is resolved against the catalog first, so a bad
    /// table name fails without half-applying the request.
    ///
    /// # Errors
    ///
    /// The default implementation returns [`Error::NotImplemented`](crate::core::Error::NotImplemented): a handle that is not
    /// driving an incremental snapshot has nothing to add tables to, and silently accepting
    /// the request would report a backfill that never happens.
    async fn request_snapshot_tables(&mut self, _request: SnapshotRequest) -> Result<usize> {
        Err(crate::core::Error::NotImplemented(
            "this stream is not running an incremental snapshot, so tables cannot be added to \
             one. Configure RuntimeConfig::with_incremental_snapshot to enable on-demand \
             snapshots."
                .into(),
        ))
    }

    /// Suspend or resume chunk reading on an in-flight incremental snapshot.
    ///
    /// Returns the **previous** state, so a caller can distinguish a real change from a
    /// repeated request. The live change stream is unaffected in both directions — only the
    /// next chunk read is withheld — which is what makes this usable to take snapshot load
    /// off a production primary during business hours without stopping capture.
    ///
    /// Pausing takes effect at a chunk boundary: a chunk already read is merged and
    /// delivered first. Stopping mid-chunk would either discard a read the source has
    /// already paid for or strand a merged chunk whose cursor can never be promoted.
    ///
    /// The paused flag rides in [`IncrementalSnapshotState`], so it is durable and a pause
    /// survives a restart rather than silently lifting on the next deploy.
    ///
    /// # Errors
    ///
    /// The default returns [`Error::NotImplemented`](crate::core::Error::NotImplemented):
    /// a handle not driving an incremental snapshot has nothing to pause, and quietly
    /// accepting would report a pause that never happened.
    async fn set_snapshot_paused(&mut self, _paused: bool) -> Result<bool> {
        Err(crate::core::Error::NotImplemented(
            "this stream is not running an incremental snapshot, so there is nothing to              pause or resume. Configure RuntimeConfig::with_incremental_snapshot to enable              on-demand snapshots."
                .into(),
        ))
    }

    /// Abandon the in-flight incremental snapshot, discarding its chunk cursors.
    ///
    /// Returns how many tables still had work outstanding.
    ///
    /// Undelivered rows of the in-flight chunk go with it — they are snapshot reads the
    /// operator has just asked to stop producing. Held-back **log** events are kept and
    /// drained: they belong to the live stream, and discarding them would lose change data.
    ///
    /// The persisted state is cleared by the next checkpoint write, so a restart does not
    /// resume the snapshot. A crash in the window before that write resumes it — abandoning
    /// is not itself a durable operation, and the alternative (an extra synchronous
    /// checkpoint write from a control path) would let an operator action rewrite the
    /// stream position.
    ///
    /// # Errors
    ///
    /// The default returns [`Error::NotImplemented`](crate::core::Error::NotImplemented).
    async fn stop_snapshot(&mut self) -> Result<usize> {
        Err(crate::core::Error::NotImplemented(
            "this stream is not running an incremental snapshot, so there is nothing to              stop. Configure RuntimeConfig::with_incremental_snapshot to enable on-demand              snapshots."
                .into(),
        ))
    }

    /// Where a restart must resume from once `event` is durably committed.
    ///
    /// Returns an offset string in this connector's own format, or `None` (the default) to
    /// mean "the event's own [`SourceMetadata::offset`](crate::core::SourceMetadata) is
    /// already a resumable boundary".
    ///
    /// # Why an event's own position is often *not* resumable
    ///
    /// The two are different things, and conflating them costs a guaranteed duplicate on
    /// every restart. An event's offset identifies **the change**; the resume position is
    /// the first position **not** consumed.
    ///
    /// PostgreSQL is the clearest case. Each change keeps its own WAL position, but logical
    /// decoding filters at **transaction** granularity: `START_REPLICATION ... X` re-sends
    /// every transaction whose *commit record* sits at or after `X`. A change LSN always
    /// precedes its own transaction's commit record, so resuming from one replays that
    /// entire transaction — deterministically, on every restart, with no new writes on the
    /// source at all.
    ///
    /// Nudging the LSN forward does not help: the commit record is still ahead of `X + 1`.
    /// The only position that skips the transaction is the one **after** its commit record,
    /// which pgoutput reports in the COMMIT message as `end_lsn`. That is what the
    /// PostgreSQL handle returns here.
    ///
    /// The runtime uses this for both the durable checkpoint and the source-side
    /// confirmation, so a connector implementing it fixes replication-slot retention and
    /// restart duplicates together.
    fn resume_offset_for(&self, _event: &Event) -> Option<String> {
        None
    }

    /// Confirm that all messages up to `lsn` have been durably consumed.
    /// Prevents WAL retention bloat on replication slots.
    async fn confirm_lsn(&mut self, lsn: u64) -> Result<()>;
    /// Return the most recently observed replication slot WAL lag in bytes.
    ///
    /// Populated by the idle-advance path (`pg_current_wal_lsn - confirmed_flush_lsn`)
    /// for PostgreSQL sources. Returns `None` for all other connectors and before
    /// the first idle-advance call completes.
    ///
    /// Use this value for the `rustcdc_replication_slot_lag_bytes` metric and
    /// `RuntimeAdminSnapshot::replication_slot_lag_bytes`.
    fn replication_slot_lag_bytes(&self) -> Option<u64> {
        None
    }

    /// A checkpoint offset describing this handle's own durable position.
    ///
    /// The runtime normally derives the checkpoint offset from the delivered event.
    /// Some events carry no position of their own — an incremental-snapshot `Read`
    /// row is identified by a chunk cursor, not a log position — and for those the
    /// runtime asks the handle instead.
    ///
    /// Returning `None` (the default) means "this event is not persistable"; the
    /// runtime then records a non-persistent barrier entry, which advances the
    /// committed count without moving the durable source position.
    ///
    /// Implementations that run an incremental snapshot **must** return an offset
    /// here, and that offset must carry
    /// [`IncrementalSnapshotState`] — otherwise a restart mid-snapshot re-reads every
    /// table from row zero.
    fn position_offset(&self) -> Option<Box<dyn Offset>> {
        None
    }

    /// Durable incremental-snapshot progress owned by this handle, if any.
    ///
    /// The runtime attaches this to every checkpoint offset it builds, so the chunk
    /// cursors become durable in the same atomic write as the stream position.
    fn incremental_snapshot_state(&self) -> Option<IncrementalSnapshotState> {
        None
    }
}

#[async_trait]
/// A CDC source connector.
///
/// Implement this to drive [`crate::CdcRuntime`] from a system this crate does not ship;
/// register it with [`crate::CdcRuntime::register_source`]. Everything the runtime
/// provides — commit barrier, checkpointing, transforms, the idempotency guard, health
/// verdicts, metrics — applies unchanged.
pub trait Source: Send + Sync {
    /// Begin a blocking bulk snapshot of `tables`.
    async fn start_snapshot(&mut self, tables: &[&str]) -> Result<Box<dyn SnapshotHandle>>;
    /// Start snapshot capture from a previously persisted snapshot checkpoint.
    ///
    /// Default implementation falls back to `start_snapshot`, which preserves
    /// backwards behavior for source implementations that do not need explicit
    /// resume handling.
    async fn start_snapshot_from_checkpoint(
        &mut self,
        tables: &[&str],
        _resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn SnapshotHandle>> {
        self.start_snapshot(tables).await
    }
    /// Begin streaming changes, resuming from `resume_from` when supplied.
    ///
    /// `None` means "start from the current head of the log". **Never substitute `None`
    /// for a checkpoint that failed to load**: resuming from the head silently skips
    /// everything written since the last durable position.
    async fn start_stream(
        &mut self,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>>;

    /// Transition from snapshot to streaming without opening a gap or a duplicate flood.
    ///
    /// The stream must resume at or before the snapshot's consistent view: resuming after
    /// it loses every change in between. Overlap is the safe direction — it produces
    /// duplicates, which the delivery contract already permits.
    async fn perform_handoff(
        &mut self,
        snapshot: &mut dyn SnapshotHandle,
        stream: &mut dyn StreamHandle,
    ) -> Result<HandoffResult>;

    /// Stable connector identifier, e.g. `"postgres"`.
    ///
    /// This selects the checkpoint file name, so it must be stable across restarts and
    /// distinct per source *flavor* — a MariaDB stream reporting `"mysql"` writes
    /// `checkpoint_mysql.json`, finds nothing on restart, and resumes from the live head.
    fn source_type(&self) -> &str;

    /// Capabilities this connector actually implements.
    ///
    /// The runtime and embedders branch on these; over-reporting a capability produces a
    /// runtime failure at the point it is exercised, which is usually mid-pipeline.
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::none()
    }

    /// Establish the connector's connection(s) to the source system.
    ///
    /// Called by the runtime once at `start()` and again before every reconnect
    /// attempt. Implementations must be idempotent: a `connect()` on an already-
    /// connected source is a no-op success, not an error.
    ///
    /// This is on the trait — not only an inherent method on the built-in connectors
    /// — because without it the runtime could not drive a third-party `impl Source`
    /// at all: connection setup was dispatched through a closed enum of the connectors
    /// this crate ships. A library whose premise is embeddability has to let its users
    /// bring their own source.
    ///
    /// The default is a no-op success, for sources that need no explicit setup.
    async fn connect(&self) -> Result<()> {
        Ok(())
    }

    /// Release the connector's connection(s).
    ///
    /// Called before each reconnect and during runtime shutdown. Must be safe to call
    /// on an already-closed source, and must not block indefinitely — the runtime
    /// applies its own timeout but a hung `close` still delays shutdown.
    async fn close(&self) {}
}

#[cfg(feature = "mariadb")]
pub mod mariadb;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlserver")]
pub mod sqlserver;
#[cfg(feature = "sqlserver")]
pub use sqlserver::SqlServerConnection;
#[cfg(feature = "sqlserver")]
pub use sqlserver::SqlServerSourceConfig;

#[cfg(feature = "mariadb")]
pub use mariadb::{
    MariaDbConnection, MariaDbIncrementalSnapshotHandle, MariaDbSnapshotHandle,
    MariaDbSourceConfig, MariaDbStreamHandle,
};
#[cfg(feature = "mysql")]
pub use mysql::incremental_snapshot::MysqlIncrementalSnapshotHandle;
#[cfg(feature = "mysql")]
pub use mysql::MysqlConnection;
#[cfg(feature = "mysql")]
pub use mysql::{MysqlSourceConfig, ServerFlavor};
#[cfg(feature = "postgres")]
pub use postgres::incremental_snapshot::IncrementalSnapshotHandle;
#[cfg(feature = "postgres")]
pub use postgres::PostgresConnection;
#[cfg(feature = "postgres")]
pub use postgres::{PostgresSourceConfig, WalTransport};
#[cfg(feature = "sqlserver")]
pub use sqlserver::incremental_snapshot::SqlServerIncrementalSnapshotHandle;

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::json;

    use crate::{
        checkpoint::{Checkpoint, InMemoryCheckpoint},
        core::{Event, Offset, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION},
    };

    use super::{
        table_is_allowed, ConnectorCapabilities, HandoffResult, SnapshotEnd, SnapshotHandle,
        Source, StreamHandle,
    };

    fn sample_event() -> Event {
        Event {
            before: None,
            after: Some(json!({"id": 1})),
            op: Operation::Read,
            source: SourceMetadata {
                source_name: "mock".to_string(),
                offset: "1".to_string(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".to_string()),
            table: "users".to_string(),
            primary_key: Some(vec!["id".to_string()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    struct MockSnapshot;

    #[async_trait]
    impl SnapshotHandle for MockSnapshot {
        async fn next_chunk(&mut self, _chunk_size: usize) -> crate::core::Result<Vec<Event>> {
            Ok(vec![sample_event()])
        }

        async fn checkpoint(
            &self,
            _checkpoint: &mut dyn Checkpoint,
            _committed_event_count: u64,
        ) -> crate::core::Result<()> {
            Ok(())
        }

        async fn finish(&mut self) -> crate::core::Result<SnapshotEnd> {
            Ok(SnapshotEnd {
                snapshot_end_ts: 42,
            })
        }
    }

    struct MockStream;

    #[async_trait]
    impl StreamHandle for MockStream {
        async fn next_events(&mut self, _timeout_ms: u64) -> crate::core::Result<Vec<Event>> {
            Ok(vec![sample_event()])
        }

        async fn save_position(&self, _checkpoint: &mut dyn Checkpoint) -> crate::core::Result<()> {
            Ok(())
        }

        async fn confirm_lsn(&mut self, _lsn: u64) -> crate::core::Result<()> {
            Ok(())
        }
    }

    struct MockSource;

    #[async_trait]
    impl Source for MockSource {
        async fn start_snapshot(
            &mut self,
            _tables: &[&str],
        ) -> crate::core::Result<Box<dyn SnapshotHandle>> {
            Ok(Box::new(MockSnapshot))
        }

        async fn start_stream(
            &mut self,
            _resume_from: Option<&dyn Offset>,
        ) -> crate::core::Result<Box<dyn StreamHandle>> {
            Ok(Box::new(MockStream))
        }

        async fn perform_handoff(
            &mut self,
            _snapshot: &mut dyn SnapshotHandle,
            _stream: &mut dyn StreamHandle,
        ) -> crate::core::Result<HandoffResult> {
            Ok(HandoffResult {
                snapshot_end_ts: Some(42),
                stream_start_ts: Some(43),
                overlap_events_dropped: None,
                stream_watermark_gap: None,
            })
        }

        fn source_type(&self) -> &str {
            "mock"
        }

        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities {
                snapshot: true,
                snapshot_checkpoint_resume: true,
                handoff: true,
                ddl_capture: false,
                heartbeat: false,
                tls: false,
                schema_introspection: true,
                truncate: false,
                incremental_snapshot: false,
            }
        }
    }

    #[tokio::test]
    async fn stream_default_requeue_is_noop_success() {
        let mut stream = MockStream;
        stream.requeue_events(vec![sample_event()]).await.unwrap();
    }

    #[tokio::test]
    async fn source_trait_round_trip_mock_handles() {
        let mut source = MockSource;
        let mut snapshot = source.start_snapshot(&["users"]).await.unwrap();
        let mut stream = source.start_stream(None).await.unwrap();

        let snapshot_chunk = snapshot.next_chunk(10).await.unwrap();
        let stream_chunk = stream.next_events(10).await.unwrap();
        let handoff = source
            .perform_handoff(snapshot.as_mut(), stream.as_mut())
            .await
            .unwrap();

        assert_eq!(source.source_type(), "mock");
        assert_eq!(snapshot_chunk.len(), 1);
        assert_eq!(stream_chunk.len(), 1);
        assert_eq!(handoff.snapshot_end_ts, Some(42));
        assert_eq!(handoff.stream_start_ts, Some(43));
        assert_eq!(handoff.overlap_events_dropped, None);
        assert_eq!(handoff.stream_watermark_gap, None);
        assert!(source.capabilities().snapshot);
    }

    #[tokio::test]
    async fn snapshot_checkpoint_and_finish_paths_are_callable() {
        let mut snapshot = MockSnapshot;
        let mut checkpoint = InMemoryCheckpoint::default();

        snapshot.checkpoint(&mut checkpoint, 1).await.unwrap();
        let end = snapshot.finish().await.unwrap();
        assert_eq!(end.snapshot_end_ts, 42);
    }

    #[test]
    fn table_filter_include_takes_precedence() {
        let include = vec!["public.users".to_string()];
        let exclude = vec!["users".to_string()];

        assert!(table_is_allowed(
            Some("public"),
            "users",
            &include,
            &exclude
        ));
        assert!(!table_is_allowed(
            Some("public"),
            "orders",
            &include,
            &exclude
        ));
    }

    #[test]
    fn table_filter_exclude_applies_when_include_empty() {
        let include = Vec::new();
        let exclude = vec!["users".to_string()];

        assert!(!table_is_allowed(
            Some("public"),
            "users",
            &include,
            &exclude
        ));
        assert!(table_is_allowed(
            Some("public"),
            "orders",
            &include,
            &exclude
        ));
    }

    #[test]
    fn table_filter_matches_schema_table_case_insensitively() {
        let include = vec!["Public.Users".to_string()];
        let exclude = Vec::new();

        assert!(table_is_allowed(
            Some("public"),
            "users",
            &include,
            &exclude
        ));
        assert!(table_is_allowed(
            Some("PUBLIC"),
            "USERS",
            &include,
            &exclude
        ));
    }

    /// These lists used to match exact strings only, so a pattern excluded nothing —
    /// indistinguishable from a set of tables that never changed.
    #[test]
    fn table_filter_entries_are_glob_patterns() {
        let none = Vec::new();

        let exclude = vec!["public.tmp_*".to_string()];
        assert!(!table_is_allowed(
            Some("public"),
            "tmp_import",
            &none,
            &exclude
        ));
        assert!(table_is_allowed(Some("public"), "orders", &none, &exclude));
        assert!(
            table_is_allowed(Some("staging"), "tmp_import", &none, &exclude),
            "the pattern names a schema, so it must not reach another one"
        );

        let include = vec!["public.*".to_string()];
        assert!(table_is_allowed(Some("public"), "orders", &include, &none));
        assert!(!table_is_allowed(
            Some("staging"),
            "orders",
            &include,
            &none
        ));

        let include = vec!["*.audit_?".to_string()];
        assert!(table_is_allowed(Some("any"), "audit_1", &include, &none));
        assert!(!table_is_allowed(Some("any"), "audit_10", &include, &none));
    }

    /// The documented widening: an unqualified entry is schema-agnostic. `connect()`
    /// warns about it on the include side, where it loosens an allowlist.
    #[test]
    fn an_unqualified_filter_entry_matches_every_schema() {
        let none = Vec::new();
        let include = vec!["users".to_string()];

        assert!(table_is_allowed(Some("public"), "users", &include, &none));
        assert!(table_is_allowed(
            Some("tenant_private"),
            "users",
            &include,
            &none
        ));
        assert!(table_is_allowed(None, "users", &include, &none));
        assert!(!table_is_allowed(Some("public"), "orders", &include, &none));
    }

    /// A qualified entry must not match an event the source reports without a schema:
    /// there is nothing to match the schema half against.
    #[test]
    fn a_qualified_filter_entry_does_not_match_a_schemaless_event() {
        let none = Vec::new();
        let include = vec!["public.users".to_string()];
        assert!(!table_is_allowed(None, "users", &include, &none));
        assert!(!table_is_allowed(Some(""), "users", &include, &none));
    }

    #[test]
    fn a_blank_filter_entry_is_ignored_rather_than_matching_everything() {
        let none = Vec::new();
        let exclude = vec!["   ".to_string(), "public.tmp".to_string()];
        assert!(table_is_allowed(Some("public"), "orders", &none, &exclude));
        assert!(!table_is_allowed(Some("public"), "tmp", &none, &exclude));
    }
}
