//! The `CHANGES`-window stream handle.

use async_trait::async_trait;

use crate::{
    checkpoint::{Checkpoint, SnowflakeOffset},
    core::{Error, Event, Offset, Result},
    source::{table_is_allowed, StreamHandle},
};

use super::{mapping::events_from_changes, sql, SnowflakeQueryExecutor, SnowflakeSourceConfig};

/// Reads consecutive `CHANGES` windows and emits canonical events.
///
/// The window is the whole design. Each poll fixes an upper bound from the **server's**
/// clock, reads `(committed_end, upper]` for every selected table, and leaves the offset
/// where it was until the runtime's commit barrier says the sink has the events. A crash
/// re-reads the window: at-least-once, the same contract as every other connector here.
#[derive(Debug)]
pub struct SnowflakeStreamHandle {
    config: SnowflakeSourceConfig,
    executor: std::sync::Arc<dyn SnowflakeQueryExecutor>,
    /// Exclusive lower bound of the next window — the last window end handed to the caller.
    position_nanos: u64,
    tables: Vec<String>,
    /// Events handed to the caller since this handle opened.
    ///
    /// Not decoration: `FileCheckpoint` refuses a write whose committed-event count moves
    /// backwards, so a `save_position` that reported zero would fail on shutdown for any
    /// stream that had already committed anything — turning an orderly stop into a
    /// checkpoint error. Every other connector here carries the same counter.
    events_polled: u64,
}

impl SnowflakeStreamHandle {
    pub(super) fn new(
        config: SnowflakeSourceConfig,
        executor: std::sync::Arc<dyn SnowflakeQueryExecutor>,
        start_nanos: u64,
    ) -> Result<Self> {
        let tables = config.selected_tables();
        if tables.is_empty() {
            return Err(Error::ConfigError(
                "snowflake source has no tables to read: `tables` is empty, or the \
                 include/exclude lists exclude every entry. A stream over no tables polls \
                 the warehouse forever and delivers nothing."
                    .into(),
            ));
        }
        Ok(Self {
            config,
            executor,
            position_nanos: start_nanos,
            tables,
            events_polled: 0,
        })
    }

    /// The server's current instant, in epoch nanoseconds.
    ///
    /// Taken from Snowflake rather than the local clock: a client running fast would ask
    /// for a window ending in the future and skip whatever lands in the gap, with no error.
    pub(super) async fn server_now_nanos(executor: &dyn SnowflakeQueryExecutor) -> Result<u64> {
        let result = executor
            .query(&sql::current_epoch_nanos_statement())
            .await?;
        let raw = result.scalar().ok_or_else(|| {
            Error::SourceError(
                "snowflake did not return a single value for CURRENT_TIMESTAMP(); the \
                 executor must hand back the statement's result set unchanged"
                    .into(),
            )
        })?;
        // The REST API renders NUMBER as text, and epoch nanoseconds exceed f64's exact
        // range by 2033 — parsing through a float would quantise the window boundary and
        // is exactly the mistake the offset is an integer to avoid.
        raw.trim().parse::<u64>().map_err(|error| {
            Error::SourceError(format!(
                "snowflake returned '{raw}' for the current epoch nanosecond, which is not \
                 an unsigned integer: {error}"
            ))
        })
    }

    /// The current window's position, for checkpointing.
    fn offset(&self) -> SnowflakeOffset {
        SnowflakeOffset::new(
            self.position_nanos,
            self.config.database.clone(),
            self.config.schema.clone(),
        )
    }
}

#[async_trait]
impl StreamHandle for SnowflakeStreamHandle {
    async fn next_events(&mut self, _timeout_ms: u64) -> Result<Vec<Event>> {
        let upper = Self::server_now_nanos(self.executor.as_ref()).await?;

        // A clock that has not advanced past the last window is not an error — two polls
        // inside the same nanosecond are possible on a fast loop, and an empty window is
        // the correct answer. Going *backwards* is: the offset would rewind and the same
        // changes would be re-read forever.
        if upper <= self.position_nanos {
            return Ok(Vec::new());
        }

        let mut events = Vec::new();
        for table in &self.tables {
            let statement = sql::changes_statement(
                &self.config.database,
                &self.config.schema,
                table,
                self.position_nanos,
                upper,
                self.config.append_only,
            );
            let result =
                self.executor.query(&statement).await.map_err(|error| {
                    super::classify_window_error(table, self.position_nanos, error)
                })?;

            events.extend(events_from_changes(
                &result,
                &self.config.source_name,
                &self.config.schema,
                table,
                self.config.primary_keys.get(table),
                upper,
            )?);

            if events.len() >= self.config.max_events_per_poll {
                // The window is *not* closed early. Every table in the window must be read
                // before the position advances, or the tables that were not read would be
                // skipped when the next poll starts from `upper`. Reading the remainder is
                // the only correct response to an over-large window; the cap bounds a
                // single response, and shrinking the poll interval bounds the window.
                tracing::warn!(
                    target: "rustcdc::source::snowflake",
                    table = %table,
                    events = events.len(),
                    max_events_per_poll = self.config.max_events_per_poll,
                    "snowflake change window exceeded max_events_per_poll; the whole window \
                     is still read, because advancing the position with tables unread would \
                     skip them. Poll more often to make windows smaller.",
                );
            }
        }

        self.position_nanos = upper;
        self.events_polled = self.events_polled.saturating_add(events.len() as u64);
        Ok(events)
    }

    async fn save_position(&self, checkpoint: &mut dyn Checkpoint) -> Result<()> {
        checkpoint.save(&self.offset(), self.events_polled).await
    }

    fn position_offset(&self) -> Option<Box<dyn Offset>> {
        Some(Box::new(self.offset()))
    }

    async fn confirm_lsn(&mut self, _lsn: u64) -> Result<()> {
        // Nothing to confirm: Snowflake holds no server-side cursor for a `CHANGES` read.
        // That absence is the whole reason this connector is safe to build — see the
        // Streams comparison in the Snowflake documentation page.
        Ok(())
    }
}

/// Which of the configured tables this stream reads, after the include/exclude lists.
impl SnowflakeSourceConfig {
    pub(super) fn selected_tables(&self) -> Vec<String> {
        self.tables
            .iter()
            .filter(|table| {
                table_is_allowed(
                    Some(&self.schema),
                    table,
                    &self.table_include_list,
                    &self.table_exclude_list,
                )
            })
            .cloned()
            .collect()
    }
}
