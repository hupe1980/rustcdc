//! Time-travel-consistent initial load.
//!
//! # Why this connector needs no watermark bracket
//!
//! Every other connector in this crate interleaves chunk reads with the live stream and
//! brackets each chunk between watermarks, because a chunk `SELECT` sees a *different*
//! moment than the log position around it. Snowflake removes the problem rather than
//! solving it: `SELECT … AT(TIMESTAMP => T)` reads the table version at exactly `T`, so
//! every chunk — however long the snapshot takes — sees one consistent instant. Start the
//! change stream at the same `T` and the two phases join with no overlap, no duplicate
//! window and no suppression rule.
//!
//! The cost is that `T` has to stay inside the table's time-travel retention for the whole
//! snapshot. A snapshot that outruns retention fails loudly; see `classify_window_error`.

use async_trait::async_trait;

use crate::{
    checkpoint::{Checkpoint, SnowflakeOffset},
    core::{Error, Result},
    source::{helpers::now_millis, SnapshotEnd, SnapshotHandle},
};

use super::{
    mapping::{events_from_snapshot_rows, SnapshotRowContext},
    sql, SnowflakeQueryExecutor, SnowflakeSourceConfig,
};

/// Keyset-paginated read of every selected table, pinned to one instant.
#[derive(Debug)]
pub struct SnowflakeSnapshotHandle {
    config: SnowflakeSourceConfig,
    executor: std::sync::Arc<dyn SnowflakeQueryExecutor>,
    /// The instant every chunk is read at, and the instant the stream will start from.
    at_nanos: u64,
    snapshot_id: String,
    /// Remaining tables, most recent last so `pop` walks them in configured order.
    remaining: Vec<String>,
    /// Keyset cursor within the table currently being read.
    cursor: Option<Vec<String>>,
    chunk_index: u32,
}

impl SnowflakeSnapshotHandle {
    pub(super) fn new(
        config: SnowflakeSourceConfig,
        executor: std::sync::Arc<dyn SnowflakeQueryExecutor>,
        at_nanos: u64,
        tables: Vec<String>,
    ) -> Result<Self> {
        // A keyset snapshot needs an order, and only the operator knows the table's key:
        // `CHANGES` reports `METADATA$ROW_ID`, which is Snowflake's internal row identity
        // and not a column you can `ORDER BY`. Refusing here is far better than paginating
        // with `OFFSET`, which on a warehouse re-scans from the start for every chunk and
        // — without a total order — can skip or repeat rows between them.
        for table in &tables {
            let key = config.primary_keys.get(table);
            if key.is_none_or(Vec::is_empty) {
                return Err(Error::ConfigError(format!(
                    "snowflake snapshot of '{table}' needs its key columns declared in \
                     SnowflakeSourceConfig::primary_keys. The snapshot is keyset-paginated \
                     and a keyset needs a total order; Snowflake's CHANGES metadata reports \
                     METADATA$ROW_ID, which is an internal identity and cannot be ordered \
                     by. Declare the key, or run the stream without an initial load."
                )));
            }
        }

        let mut remaining = tables;
        remaining.reverse();
        Ok(Self {
            config,
            executor,
            at_nanos,
            snapshot_id: format!("snowflake-{at_nanos}"),
            remaining,
            cursor: None,
            chunk_index: 0,
        })
    }

    /// The instant the snapshot is pinned to — where the stream must start.
    pub(super) fn at_nanos(&self) -> u64 {
        self.at_nanos
    }

    fn key_of(&self, table: &str) -> &[String] {
        self.config
            .primary_keys
            .get(table)
            .map_or(&[][..], Vec::as_slice)
    }
}

#[async_trait]
impl SnapshotHandle for SnowflakeSnapshotHandle {
    async fn next_chunk(&mut self, chunk_size: usize) -> Result<Vec<crate::core::Event>> {
        let chunk_size = chunk_size.max(1);

        while let Some(table) = self.remaining.last().cloned() {
            let key = self.key_of(&table).to_vec();
            let statement = sql::snapshot_chunk_statement(
                &self.config.database,
                &self.config.schema,
                &table,
                self.at_nanos,
                &key,
                self.cursor.as_deref(),
                chunk_size,
            );
            let result = self
                .executor
                .query(&statement)
                .await
                .map_err(|error| super::classify_window_error(&table, self.at_nanos, error))?;

            if result.rows.is_empty() {
                // Table exhausted; move on with a fresh cursor.
                self.remaining.pop();
                self.cursor = None;
                continue;
            }

            // A short chunk ends the table; if it was also the last table, this is the last
            // chunk of the snapshot. Decided *before* the events are built, because the flag
            // rides on every event in the chunk.
            let table_exhausted = result.rows.len() < chunk_size;
            let is_last_chunk = table_exhausted && self.remaining.len() == 1;

            let events = events_from_snapshot_rows(
                &result,
                &SnapshotRowContext {
                    source_name: &self.config.source_name,
                    schema: &self.config.schema,
                    table: &table,
                    primary_key: &key,
                    at_nanos: self.at_nanos,
                    snapshot_id: &self.snapshot_id,
                    chunk_index: self.chunk_index,
                    is_last_chunk,
                },
            )?;
            self.chunk_index = self.chunk_index.saturating_add(1);

            // Advance the cursor from the **last row of this chunk**, read out of the event
            // payload rather than tracked separately, so the cursor cannot disagree with
            // what was actually emitted. A key column missing from the payload would make
            // the next chunk restart the table, so it is an error rather than a reset.
            let last = events.last().ok_or_else(|| {
                Error::SourceError(format!(
                    "snowflake snapshot chunk for '{table}' produced no events from \
                     {} rows",
                    result.rows.len()
                ))
            })?;
            let after = last.after.as_ref().and_then(|value| value.as_object());
            let mut next_cursor = Vec::with_capacity(key.len());
            for column in &key {
                let value = after.and_then(|object| object.get(column)).ok_or_else(|| {
                    Error::SourceError(format!(
                        "snowflake snapshot of '{table}' declared key column '{column}', but \
                         the row read back does not contain it. Check the declared key \
                         against the table — Snowflake folds an unquoted identifier to \
                         upper case, so `id` in a CREATE TABLE is `ID` in a result set."
                    ))
                })?;
                next_cursor.push(match value {
                    serde_json::Value::String(text) => text.clone(),
                    // A NULL key column cannot be a keyset cursor: `(a) > (NULL)` is
                    // unknown, so the next chunk would return nothing and the table would
                    // silently end early.
                    serde_json::Value::Null => {
                        return Err(Error::SourceError(format!(
                            "snowflake snapshot of '{table}' found NULL in key column \
                             '{column}'. A keyset cursor cannot advance past NULL, so the \
                             snapshot would stop there and report success. Declare a key \
                             whose columns are NOT NULL."
                        )));
                    }
                    other => other.to_string(),
                });
            }
            self.cursor = Some(next_cursor);

            if table_exhausted {
                // Skip the extra round trip that would return zero rows.
                self.remaining.pop();
                self.cursor = None;
            }

            return Ok(events);
        }

        Ok(Vec::new())
    }

    async fn checkpoint(&self, checkpoint: &mut dyn Checkpoint, committed: u64) -> Result<()> {
        // The snapshot's durable position is the instant it is pinned to. Recording it
        // means a restart mid-snapshot resumes the *stream* from the same instant, so no
        // change between the pin and the crash is lost — the snapshot itself restarts,
        // which costs a re-read and produces duplicates the sink's idempotency handles.
        checkpoint
            .save(
                &SnowflakeOffset::new(
                    self.at_nanos,
                    self.config.database.clone(),
                    self.config.schema.clone(),
                ),
                committed,
            )
            .await
    }

    async fn finish(&mut self) -> Result<SnapshotEnd> {
        Ok(SnapshotEnd {
            snapshot_end_ts: now_millis(),
        })
    }
}
