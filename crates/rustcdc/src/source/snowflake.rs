//! Snowflake source over the `CHANGES` clause.
//!
//! # What this connector is, and what it deliberately is not
//!
//! Snowflake exposes two change-tracking mechanisms. This connector uses the one an
//! external consumer can read **safely**, and it is not the one named "stream".
//!
//! A Snowflake *stream* advances its offset only when it is consumed inside a **DML
//! transaction**. An external reader would therefore have to write to the source account to
//! make progress, and — worse — that write commits *before* rustcdc's checkpoint is
//! durable. A crash in between loses the changes permanently: they are gone from the stream
//! and were never in the checkpoint. That is at-most-once, silently, which is the opposite
//! of what this crate guarantees.
//!
//! The `CHANGES` clause has no server-side cursor at all. The caller supplies both ends of
//! the interval, so the durable position lives in the [`Checkpoint`](crate::checkpoint) with
//! every other connector's, the source is never written to, and a crash replays the window —
//! at-least-once, the crate's normal contract.
//!
//! # You supply the transport
//!
//! Snowflake speaks neither the PostgreSQL nor the MySQL wire protocol. Reaching it means
//! HTTPS plus RSA key-pair JWT signing (or OAuth), which is a dependency tree this crate
//! does not carry and — with no self-hostable Snowflake — could never test in CI. So the
//! transport is a trait you implement, in the same shape every other connector here uses
//! internally: [`SnowflakeQueryExecutor`] runs a statement and hands back text.
//!
//! Everything above that line is this crate's job and is unit-tested: statement
//! construction and identifier quoting, the window arithmetic, collapsing Snowflake's
//! delete/insert update pairs back into one `Operation::Update`, the text-valued event
//! contract, retention-failure classification, and a time-travel-consistent initial load.
//!
//! # What the event stream can and cannot carry
//!
//! `CHANGES` reports the **net effect** of an interval, not a log:
//!
//! * A row inserted and then deleted inside one window does not appear at all, and a row
//!   updated three times yields one event. Intermediate versions are unrecoverable.
//! * There is no transaction id and no commit grouping, so `Event::transaction` is always
//!   `None`.
//! * Rows within a window have no source order. Events are emitted sorted by
//!   `METADATA$ROW_ID` so two reads of one window are byte-identical, but that order is
//!   this connector's, not Snowflake's.
//!
//! Shrinking the poll interval shrinks the window and therefore how much collapsing
//! happens — at the cost of warehouse credits, which is the real tuning trade here.
//!
//! ```
//! use rustcdc::source::snowflake::{SnowflakeSourceConfig, SnowflakeSource};
//!
//! let config = SnowflakeSourceConfig::new("ANALYTICS", "PUBLIC")
//!     .with_tables(["ORDERS"])
//!     .with_primary_key("ORDERS", ["ID"]);
//! assert_eq!(config.database, "ANALYTICS");
//! # let _ = |executor| SnowflakeSource::new(config, executor);
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    checkpoint::SnowflakeOffset,
    core::{Error, Offset, Result},
    source::{ConnectorCapabilities, HandoffResult, SnapshotHandle, Source, StreamHandle},
};

mod mapping;
mod snapshot;
mod sql;
mod stream;

pub use mapping::SnowflakeResultSet;
pub use snapshot::SnowflakeSnapshotHandle;
pub use stream::SnowflakeStreamHandle;

/// Runs a statement against Snowflake and returns its result set.
///
/// # Why this is a trait rather than a built-in HTTP client
///
/// Snowflake has no wire protocol this crate could speak and no self-hostable
/// implementation to test against. Shipping an HTTPS + JWT client would add a dependency
/// tree to a crate whose default build has no HTTP client at all, and every line of it
/// would be unverifiable in CI — in a repository where every other connector's correctness
/// claims are pinned against a real server in a container. The transport is the part you
/// can test against your own account; the CDC semantics are the part this crate tests.
///
/// # Contract
///
/// * Return the statement's result set **unchanged** — same columns, same order, one entry
///   per row. Do not reorder, filter or re-type.
/// * Return every value as **text**, exactly as Snowflake rendered it, and `None` for SQL
///   `NULL`. The REST API already does this. Parsing a `NUMBER(38,4)` into a float on the
///   way through would lose precision the rest of this crate goes to some length to keep.
/// * Statements are read-only `SELECT`s. A correct implementation needs no write grant, and
///   giving it one removes the property that makes this connector safe.
/// * **Refresh your own credentials.** Every authentication method Snowflake offers a service
///   user is short-lived — a key-pair JWT lasts at most an hour, WIF attestations and OAuth
///   tokens less — and nothing will ever hand this object a fresh one: unlike the database
///   connectors, which re-resolve a `SecretString` on each reconnect, an executor is
///   constructed once and lives for the process. `query` takes `&self`, so cache the token
///   behind a lock and mint a new one before expiry. Signing once at construction works in
///   testing and starts returning `401` an hour into production.
///
/// Because the executor owns the session, every method Snowflake supports works here —
/// key-pair (with or without an encrypted private key), workload identity federation on AWS,
/// Entra ID, GCP, OIDC/Kubernetes or SPIFFE, OAuth, and programmatic access tokens. None of
/// them appears in [`SnowflakeSourceConfig`], which is deliberate: Snowflake is retiring
/// single-factor passwords, and a connector holding no credential type cannot fall behind
/// that roadmap.
///
/// The Snowflake SQL REST API (`POST /api/v2/statements`) returns exactly this shape:
/// `resultSetMetaData.rowType[].name` are the columns and `data` is an array of arrays of
/// nullable strings.
#[async_trait]
pub trait SnowflakeQueryExecutor: Send + Sync + std::fmt::Debug {
    /// Execute `statement` and return its result set.
    ///
    /// # Errors
    ///
    /// Any transport or server failure. Return the server's message intact: this crate
    /// inspects it to tell a time-travel retention failure — which is data loss, and
    /// terminal — from an ordinary transient error.
    async fn query(&self, statement: &str) -> Result<SnowflakeResultSet>;
}

/// Configuration for the Snowflake source.
///
/// Identifiers are used **exactly as written**. Snowflake folds an unquoted identifier to
/// upper case, so a table created as `orders` is `ORDERS` and this crate quotes whatever
/// you put here — `"ORDERS"` finds it, `"orders"` does not. Result-set column names come
/// back folded the same way, which is why the declared key columns must match the folded
/// spelling too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnowflakeSourceConfig {
    /// Database holding the tracked tables.
    pub database: String,
    /// Schema holding the tracked tables.
    pub schema: String,
    /// Tables to capture, named within `schema`.
    ///
    /// Each needs `CHANGE_TRACKING = TRUE` (`ALTER TABLE … SET CHANGE_TRACKING = TRUE`, or
    /// implicitly the first time a stream is created on it). Without it the `CHANGES` query
    /// fails, and this crate turns that into an error naming the remedy.
    pub tables: Vec<String>,
    /// Key columns per table, keyed by the same name used in [`tables`](Self::tables).
    ///
    /// `CHANGES` reports `METADATA$ROW_ID`, which is Snowflake's internal row identity: it
    /// is not one of your columns and cannot be ordered by. So the key has to be declared.
    /// Without one a table's events carry `primary_key: None` — usable for an append-only
    /// sink, useless for an upsert or a compacted log — and it cannot be snapshotted at
    /// all, because the keyset pagination has no order to walk.
    #[serde(default)]
    pub primary_keys: HashMap<String, Vec<String>>,
    /// Emit only inserts, using `INFORMATION => APPEND_ONLY`.
    ///
    /// Cheaper on the warehouse: Snowflake skips the join that computes deletes and
    /// updates. Correct only for a table that is genuinely append-only — on any other, the
    /// deletes and updates are silently absent.
    #[serde(default)]
    pub append_only: bool,
    /// Milliseconds between windows.
    ///
    /// This is a **cost** dial as much as a latency one: each poll runs queries on a
    /// warehouse that bills by the second it is awake. It also sets how much collapsing
    /// happens — a shorter window reports more of the intermediate versions that a longer
    /// one nets away.
    #[serde(default = "SnowflakeSourceConfig::default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Soft cap on events per poll, used to warn rather than to truncate. See
    /// [`SnowflakeStreamHandle::next_events`](stream::SnowflakeStreamHandle).
    #[serde(default = "SnowflakeSourceConfig::default_max_events_per_poll")]
    pub max_events_per_poll: usize,
    /// Rows per snapshot chunk.
    #[serde(default = "SnowflakeSourceConfig::default_snapshot_chunk_size")]
    pub snapshot_chunk_size: usize,
    /// Allowlist of `"schema.table"` glob patterns; takes precedence over the exclude list.
    #[serde(default)]
    pub table_include_list: Vec<String>,
    /// Blocklist of `"schema.table"` glob patterns; ignored when the include list is set.
    #[serde(default)]
    pub table_exclude_list: Vec<String>,
    /// `Event::source.source_name` for events from this connector.
    #[serde(default = "SnowflakeSourceConfig::default_source_name")]
    pub source_name: String,
}

impl SnowflakeSourceConfig {
    fn default_poll_interval_ms() -> u64 {
        30_000
    }
    fn default_max_events_per_poll() -> usize {
        10_000
    }
    fn default_snapshot_chunk_size() -> usize {
        10_000
    }
    fn default_source_name() -> String {
        "snowflake".to_string()
    }

    /// A configuration for one database and schema, with defaults elsewhere.
    pub fn new(database: impl Into<String>, schema: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            schema: schema.into(),
            tables: Vec::new(),
            primary_keys: HashMap::new(),
            append_only: false,
            poll_interval_ms: Self::default_poll_interval_ms(),
            max_events_per_poll: Self::default_max_events_per_poll(),
            snapshot_chunk_size: Self::default_snapshot_chunk_size(),
            table_include_list: Vec::new(),
            table_exclude_list: Vec::new(),
            source_name: Self::default_source_name(),
        }
    }

    /// Set the captured tables.
    #[must_use]
    pub fn with_tables<I, S>(mut self, tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tables = tables.into_iter().map(Into::into).collect();
        self
    }

    /// Declare a table's key columns.
    #[must_use]
    pub fn with_primary_key<I, S>(mut self, table: impl Into<String>, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.primary_keys
            .insert(table.into(), columns.into_iter().map(Into::into).collect());
        self
    }

    /// Read only inserts, with `INFORMATION => APPEND_ONLY`.
    #[must_use]
    pub fn append_only(mut self, append_only: bool) -> Self {
        self.append_only = append_only;
        self
    }

    /// Set the window interval in milliseconds.
    #[must_use]
    pub fn with_poll_interval_ms(mut self, poll_interval_ms: u64) -> Self {
        self.poll_interval_ms = poll_interval_ms;
        self
    }

    /// Check the configuration is usable before anything reaches the warehouse.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] for an empty or unquotable identifier, an empty table
    /// list, or a declared key naming no columns.
    pub fn validate(&self) -> Result<()> {
        sql::validate_identifier(&self.database, "database")?;
        sql::validate_identifier(&self.schema, "schema")?;
        if self.tables.is_empty() {
            return Err(Error::ConfigError(
                "snowflake source needs at least one table in `tables`".into(),
            ));
        }
        for table in &self.tables {
            sql::validate_identifier(table, "table")?;
        }
        for (table, key) in &self.primary_keys {
            if key.is_empty() {
                return Err(Error::ConfigError(format!(
                    "snowflake primary key for '{table}' names no columns; remove the entry \
                     rather than declaring an empty key, which reads as 'this table has a \
                     key' everywhere it is consulted"
                )));
            }
            for column in key {
                sql::validate_identifier(column, "primary key column")?;
            }
        }
        if self.poll_interval_ms == 0 {
            return Err(Error::ConfigError(
                "snowflake poll_interval_ms must be greater than zero; a zero interval spins \
                 the warehouse continuously and bills for it"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// Turn a query failure into an error that says whether data was lost.
///
/// A `CHANGES` or time-travel read whose start point has fallen outside the table's
/// retention **fails the query** rather than returning partial data — the safe direction,
/// and the same condition SQL Server's capture-window classifier models. It is also
/// terminal: retrying cannot bring the data back, and silently restarting from now would
/// skip everything in between. Saying so is the difference between an operator restoring
/// from a backfill and one watching a pipeline retry forever.
fn classify_window_error(table: &str, from_nanos: u64, error: Error) -> Error {
    let text = error.to_string().to_ascii_lowercase();

    let out_of_retention = text.contains("time travel")
        || text.contains("out of retention")
        || text.contains("data is not available")
        || text.contains("beyond the retention");
    if out_of_retention {
        return Error::SourceError(format!(
            "snowflake refused a change window on '{table}' starting at {from_nanos}ns \
             because that point is outside the table's time-travel retention: {error}. The \
             changes between the checkpoint and now are no longer readable — this is data \
             loss, not a transient failure, and restarting from the current time would hide \
             it. Re-snapshot the table, and raise DATA_RETENTION_TIME_IN_DAYS above the \
             longest outage the pipeline must survive."
        ));
    }

    if text.contains("change_tracking") || text.contains("change tracking") {
        return Error::SourceError(format!(
            "snowflake refused a change window on '{table}': {error}. Change tracking must \
             be enabled before a table can be read this way: ALTER TABLE {table} SET \
             CHANGE_TRACKING = TRUE. Note that it only records changes made *after* it is \
             enabled."
        ));
    }

    error
}

/// The Snowflake source.
#[derive(Debug)]
pub struct SnowflakeSource {
    config: SnowflakeSourceConfig,
    executor: Arc<dyn SnowflakeQueryExecutor>,
    /// Instant the snapshot pinned itself to, so the stream can start from exactly there.
    snapshot_pin_nanos: Option<u64>,
}

impl SnowflakeSource {
    /// Build a source from a configuration and a transport.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] when [`SnowflakeSourceConfig::validate`] rejects the
    /// configuration.
    pub fn new(
        config: SnowflakeSourceConfig,
        executor: Arc<dyn SnowflakeQueryExecutor>,
    ) -> Result<Self> {
        config.validate()?;
        crate::source::warn_on_schema_agnostic_include_entries(
            "snowflake",
            &config.table_include_list,
        );
        Ok(Self {
            config,
            executor,
            snapshot_pin_nanos: None,
        })
    }
}

#[async_trait]
impl Source for SnowflakeSource {
    async fn start_snapshot(&mut self, tables: &[&str]) -> Result<Box<dyn SnapshotHandle>> {
        let at_nanos = SnowflakeStreamHandle::server_now_nanos(self.executor.as_ref()).await?;

        // An explicit table list still passes through the include/exclude lists: the
        // filters bound what may leave the database, and a snapshot request is not an
        // exemption from that.
        //
        // But a requested table that survives no filter is refused rather than dropped.
        // Silently returning a snapshot of nothing is the failure two earlier audits of
        // this crate named repeatedly — an operator action that reports success and does
        // nothing — and here it is worse than usual, because "snapshot completed" is the
        // signal a backfill is finished.
        let selected = self.config.selected_tables();
        let requested: Vec<String> = if tables.is_empty() {
            selected
        } else {
            let (kept, dropped): (Vec<String>, Vec<String>) = tables
                .iter()
                .map(|table| (*table).to_string())
                .partition(|table| selected.contains(table));
            if !dropped.is_empty() {
                return Err(Error::ConfigError(format!(
                    "snowflake snapshot requested for {dropped:?}, which {} not available: a                      table must be listed in SnowflakeSourceConfig::tables and survive the                      include/exclude lists. Configured and selectable: {selected:?}. Names                      are compared as written — Snowflake folds an unquoted identifier to                      upper case, so 'orders' and 'ORDERS' are different entries here.",
                    if dropped.len() == 1 { "is" } else { "are" }
                )));
            }
            kept
        };

        let handle = SnowflakeSnapshotHandle::new(
            self.config.clone(),
            Arc::clone(&self.executor),
            at_nanos,
            requested,
        )?;
        // Remember the pin so `start_stream` opens exactly where the snapshot's consistent
        // view ends. This is what removes the overlap window the other connectors need a
        // watermark bracket to close.
        self.snapshot_pin_nanos = Some(handle.at_nanos());
        Ok(Box::new(handle))
    }

    async fn start_stream(
        &mut self,
        resume_from: Option<&dyn Offset>,
    ) -> Result<Box<dyn StreamHandle>> {
        let start_nanos = match resume_from {
            Some(offset) => {
                if offset.source_type() != "snowflake" {
                    return Err(Error::CheckpointError(format!(
                        "cannot resume a snowflake stream from a '{}' offset",
                        offset.source_type()
                    )));
                }
                let saved = SnowflakeOffset::from_bytes(&offset.encode()?)?;
                if saved.database != self.config.database || saved.schema != self.config.schema {
                    return Err(Error::CheckpointError(format!(
                        "checkpoint is for snowflake {}.{} but this source is configured for \
                         {}.{}. Resuming would read a window from the wrong table version \
                         history; use a separate checkpoint directory per source.",
                        saved.database, saved.schema, self.config.database, self.config.schema
                    )));
                }
                saved.window_end_nanos
            }
            // No checkpoint: start where the snapshot's consistent view ended if there was
            // one, otherwise from now. Starting from now with no snapshot is a deliberate
            // "tail from here", and it is the only case where history is skipped.
            None => match self.snapshot_pin_nanos {
                Some(pinned) => pinned,
                None => SnowflakeStreamHandle::server_now_nanos(self.executor.as_ref()).await?,
            },
        };

        Ok(Box::new(SnowflakeStreamHandle::new(
            self.config.clone(),
            Arc::clone(&self.executor),
            start_nanos,
        )?))
    }

    async fn perform_handoff(
        &mut self,
        _snapshot: &mut dyn SnapshotHandle,
        _stream: &mut dyn StreamHandle,
    ) -> Result<HandoffResult> {
        // Nothing to reconcile. The snapshot read the table version at instant `T` and the
        // stream opened at `T`, so the two phases meet exactly: no overlap to deduplicate
        // and no gap to lose changes in. Every other connector in this crate needs a
        // watermark bracket here because a chunk `SELECT` and a log position refer to
        // different moments; Snowflake's time travel makes them the same moment.
        Ok(HandoffResult::default())
    }

    fn source_type(&self) -> &str {
        "snowflake"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::none()
            .with_tls(true)
            .with_schema_introspection(false)
    }

    async fn connect(&self) -> Result<()> {
        // One statement, which proves the transport works, the warehouse is reachable and
        // the session can read — before a pipeline reports itself healthy and delivers
        // nothing. It is also the value the whole window scheme is built on.
        SnowflakeStreamHandle::server_now_nanos(self.executor.as_ref())
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "snowflake connectivity check failed: {error}. The executor could not run \
                     `SELECT CURRENT_TIMESTAMP()`; check the account URL, the key-pair or \
                     OAuth credentials, and that the role has a usable warehouse."
                ))
            })?;
        Ok(())
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests;
