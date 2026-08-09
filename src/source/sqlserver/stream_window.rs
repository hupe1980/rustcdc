use crate::{
    core::{
        Error, Event, Operation, Result, SourceMetadata, TransactionMetadata,
        EVENT_ENVELOPE_VERSION,
    },
    source::table_is_allowed,
};

use super::{
    build_cdc_poll_sql, compare_lsn, is_sqlserver_cdc_window_error, lsn_bytes_to_hex,
    lsn_hex_to_bytes, now_millis, query, tx_id_from_seqval, validate_capture_instance_name,
    CaptureInstanceMeta, SqlServerRawChange, SqlServerRawTruncate, SqlServerStreamHandle,
    ZERO_LSN_HEX,
};

impl SqlServerStreamHandle {
    /// Decide what to do with the LSN window now that `window_buffer` is empty.
    ///
    /// Two outcomes, and picking the wrong one loses data in one direction or
    /// re-reads the whole window in the other:
    ///
    /// * A parked truncation cursor means some capture instance had rows inside this
    ///   window that `max_events_per_poll` cut off. The window must stay put and be
    ///   re-queried from the cursor; advancing past it drops those rows.
    /// * No parked cursor means the window was read to the end, so advance to a fresh
    ///   one. Not advancing would re-deliver the same window forever.
    ///
    /// Deferring both to the drain point is what keeps `save_position` honest: while
    /// events from this window are still buffered, neither the cursor nor the window
    /// start may move ahead of what the consumer has been handed.
    pub(super) async fn settle_drained_window(&mut self) -> Result<()> {
        match self.stream.pending_cursor.take() {
            Some(cursor) => {
                tracing::debug!(
                    target: "rustcdc::source::sqlserver",
                    lsn = %cursor.lsn_hex,
                    seqval = %cursor.seqval_hex,
                    operation = cursor.operation,
                    "sqlserver CDC window truncated by max_events_per_poll; \
                     resuming mid-window at the recorded cursor",
                );
                self.stream.cursor = Some(cursor);
                Ok(())
            }
            None => self.advance_window().await,
        }
    }

    pub(super) async fn advance_window(&mut self) -> Result<()> {
        let mut client = query::connect_client(&self.config).await?;
        let rows = client
            .query(
                "SELECT sys.fn_varbintohexstr(sys.fn_cdc_increment_lsn(@P1)), sys.fn_varbintohexstr(sys.fn_cdc_get_max_lsn())",
                &[&self.stream.lsn_end.to_vec().as_slice()],
            )
            .await
            .map_err(|error| {
                Error::SourceError(format!("sqlserver CDC window advance query failed: {error}"))
            })?
            .into_first_result()
            .await
            .map_err(|error| {
                Error::SourceError(format!("sqlserver CDC window advance decode failed: {error}"))
            })?;

        let row = rows.into_iter().next().ok_or_else(|| {
            Error::SourceError("sqlserver CDC window advance returned no row".into())
        })?;
        let start_hex = row
            .get::<&str, _>(0)
            .map(ToOwned::to_owned)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| ZERO_LSN_HEX.to_string());
        let end_hex = row
            .get::<&str, _>(1)
            .map(ToOwned::to_owned)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| ZERO_LSN_HEX.to_string());

        let incremented = lsn_hex_to_bytes(&start_hex)?;
        let harvested_max = lsn_hex_to_bytes(&end_hex)?;

        let (next_start, next_end) = Self::next_window(
            self.stream.lsn_start,
            self.stream.lsn_end,
            incremented,
            harvested_max,
        );
        self.stream.lsn_start = next_start;
        self.stream.lsn_end = next_end;
        // Advancing to a fresh window invalidates any within-window resume point,
        // parked or applied.
        self.stream.cursor = None;
        self.stream.pending_cursor = None;
        Ok(())
    }

    /// Where the next LSN window sits, given the window just finished, `fn_cdc_increment_lsn(lsn_end)`
    /// and `fn_cdc_get_max_lsn()`.
    ///
    /// Extracted as a pure function so the decision is testable — the SQL that feeds it needs a server.
    ///
    /// # `lsn_end` never exceeds what the capture job has harvested
    ///
    /// That is the whole rule, and it is what stops the read point walking forward on a quiet
    /// database. `fn_cdc_get_max_lsn()` reports what the capture job has harvested into `cdc.*`, so
    /// while nothing changes it does not move. Clamping the end *up* to the incremented start — to keep
    /// the window non-inverted — made the next advance increment from that clamped end, pushing the
    /// read point one minimal LSN step above the harvested maximum on every empty poll, indefinitely.
    ///
    /// So an **empty window is represented, not avoided**: `lsn_start > lsn_end` means "nothing to
    /// read yet", and the poll skips querying rather than issuing a round trip per capture instance
    /// for a range that cannot contain anything.
    ///
    /// # Why the lower bound only moves when something was consumed
    ///
    /// The naive repair — park one step past the maximum and stop — **skips an LSN**, and it is worth
    /// being precise about which. Parked at `[M+1, M+1]` with nothing harvested at `M+1`, a later
    /// advance sees the maximum has moved and increments from the parked *end* to `M+2`. But `M+1` was
    /// never readable when that window was set, so it is never read at all.
    ///
    /// The fix is to distinguish the two cases by whether the window was empty:
    ///
    /// - **consumed** (`start <= end`): everything through `end` was read, so the next lower bound is
    ///   `increment(end)`;
    /// - **empty** (`start > end`): nothing was read, so the lower bound **stays** and only the upper
    ///   bound is refreshed from the harvested maximum.
    ///
    /// An empty window whose maximum later jumps therefore reopens as `[start, new_max]` — with the
    /// original `start` intact, so nothing between it and the new maximum is skipped.
    /// # End-to-end validation
    ///
    /// The transitions are pinned by `window_advance_tests`, including the skip that made the naive
    /// repair worse than the creep. The rule is validated against a live SQL Server 2022 by
    /// `sqlserver_idle_window_integration`, which polls repeatedly against a **standing** harvested
    /// maximum and then requires a subsequent write to arrive — plus
    /// `sqlserver_stream_integration`, `sqlserver_window_truncation_integration` and
    /// `sqlserver_snapshot_integration`, which advance real windows throughout.
    ///
    /// The idle test deliberately does **not** call `sys.sp_cdc_scan` during its idle phase, unlike the
    /// other SQL Server suites. A scan harvests whatever is in the log, so `fn_cdc_get_max_lsn()` would
    /// keep advancing and the read point would never be left behind a standing maximum — masking the
    /// exact condition the creep needed. Getting that wrong is what made a first version of the test
    /// report zero events and look like a connector fault.
    fn next_window(
        current_start: [u8; 10],
        current_end: [u8; 10],
        incremented_end: [u8; 10],
        harvested_max: [u8; 10],
    ) -> ([u8; 10], [u8; 10]) {
        if compare_lsn(&current_start, &current_end).is_gt() {
            // The window was empty: nothing was consumed, so the lower bound must not move.
            (current_start, harvested_max)
        } else {
            // The window was read through `current_end`.
            (incremented_end, harvested_max)
        }
    }

    /// Read one page of changes for a single capture instance in the current window.
    ///
    /// The instance's own [`CaptureInstanceMeta::capture_floor`] raises the lower bound:
    /// capture instances do not all start at the same LSN, and asking one for changes
    /// below its floor makes SQL Server raise the same error 313 it raises for purged
    /// retention. Clamping keeps the ordinary case — a table added to CDC after the
    /// stream started, or simply enabled second — out of the error path entirely, so the
    /// error that does reach [`Self::classify_cdc_window_error`] is the one that
    /// genuinely means changes were cleaned up.
    pub(super) async fn fetch_changes_for_capture_instance(
        &self,
        meta: &CaptureInstanceMeta,
        max_events_per_poll: usize,
    ) -> Result<Vec<SqlServerRawChange>> {
        // An empty window — `lsn_start > lsn_end` — is how "the capture job has not harvested
        // anything at or above the read point yet" is represented, now that the end is never
        // clamped up past the harvested maximum. It cannot contain a change, so querying it would
        // be one wasted round trip per capture instance per poll.
        if compare_lsn(&self.stream.lsn_start, &self.stream.lsn_end).is_gt() {
            return Ok(Vec::new());
        }

        let capture_instance = meta.capture_instance.as_str();
        let columns = meta.captured_columns.as_slice();
        validate_capture_instance_name(capture_instance)?;

        let window_start = if compare_lsn(&meta.capture_floor, &self.stream.lsn_start).is_gt() {
            // The instance begins after the window opens, so there is nothing for it
            // before its floor. If the floor is also past the window's end it has no
            // rows in this window at all.
            if compare_lsn(&meta.capture_floor, &self.stream.lsn_end).is_gt() {
                tracing::debug!(
                    target: "rustcdc::source::sqlserver",
                    capture_instance,
                    capture_floor = %lsn_bytes_to_hex(&meta.capture_floor),
                    window_end = %lsn_bytes_to_hex(&self.stream.lsn_end),
                    "sqlserver capture instance starts after this window; skipping it",
                );
                return Ok(Vec::new());
            }
            meta.capture_floor
        } else {
            self.stream.lsn_start
        };

        let mut client = query::connect_client(&self.config).await?;
        let start_lsn_hex = lsn_bytes_to_hex(&window_start);
        let end_lsn_hex = lsn_bytes_to_hex(&self.stream.lsn_end);

        let sql = build_cdc_poll_sql(
            capture_instance,
            columns,
            max_events_per_poll,
            &start_lsn_hex,
            &end_lsn_hex,
            self.stream.cursor.as_ref(),
        );

        let query_result = match client.query(&sql, &[]).await {
            Ok(value) => value,
            Err(error) => {
                let text = error.to_string();
                if is_sqlserver_cdc_window_error(&text) {
                    return self
                        .classify_cdc_window_error(
                            capture_instance,
                            &start_lsn_hex,
                            &end_lsn_hex,
                            &text,
                        )
                        .await;
                }
                return Err(Error::SourceError(format!(
                    "sqlserver CDC poll failed for capture instance '{capture_instance}': {error}"
                )));
            }
        };

        let rows = match query_result.into_first_result().await {
            Ok(value) => value,
            Err(error) => {
                let text = error.to_string();
                if is_sqlserver_cdc_window_error(&text) {
                    return self
                        .classify_cdc_window_error(
                            capture_instance,
                            &start_lsn_hex,
                            &end_lsn_hex,
                            &text,
                        )
                        .await;
                }
                return Err(Error::SourceError(format!(
                    "sqlserver CDC poll decode failed for capture instance '{capture_instance}': {error}"
                )));
            }
        };

        let mut out = Vec::new();
        for row in rows {
            let start_lsn_hex = row
                .get::<&str, _>(0)
                .map(ToOwned::to_owned)
                .ok_or_else(|| {
                    Error::SourceError(format!(
                        "sqlserver CDC row missing __$start_lsn for capture instance '{capture_instance}'"
                    ))
                })?;
            let seqval_hex = row
                .get::<&str, _>(1)
                .map(ToOwned::to_owned)
                .ok_or_else(|| {
                    Error::SourceError(format!(
                        "sqlserver CDC row missing __$seqval for capture instance '{capture_instance}'"
                    ))
                })?;
            let operation = row.get::<i32, _>(2).ok_or_else(|| {
                Error::SourceError(format!(
                    "sqlserver CDC row missing __$operation for capture instance '{capture_instance}'"
                ))
            })?;

            let ts_ms = row.get::<i64, _>(3).unwrap_or(0);

            // Column 4 is the server-side `FOR JSON PATH` rendering of the captured
            // columns. SQL Server serializes every type correctly, so there is no
            // client-side type ladder to fall through.
            //
            // `FOR JSON PATH` omits keys whose value is NULL, so re-materialize the
            // full column set with explicit nulls. Without this, a NULL column would be
            // *absent* rather than `null`, which downstream cannot distinguish from a
            // column that was never captured.
            let row_json = row.get::<&str, _>(4).unwrap_or("{}");
            let parsed = super::parser::decode_row_json_as_text(row_json).map_err(|error| {
                Error::SourceError(format!(
                    "sqlserver CDC row_json is not valid JSON for capture instance \
                     '{capture_instance}': {error}"
                ))
            })?;
            let mut object = serde_json::Map::new();
            for column in columns {
                let value = parsed
                    .get(column.as_str())
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                object.insert(column.clone(), value);
            }
            out.push(SqlServerRawChange {
                start_lsn_hex,
                seqval_hex,
                operation,
                ts_ms: u64::try_from(ts_ms).unwrap_or_default(),
                row: serde_json::Value::Object(object),
            });
        }

        Ok(out)
    }

    /// Decide whether a SQL Server error 313 means data loss or "not yet captured".
    ///
    /// `cdc.fn_cdc_get_all_changes_*` raises 313 whenever the requested LSN range is
    /// "not appropriate", and that covers **two opposite situations**:
    ///
    /// * `from_lsn` is **below** the capture instance's `min_lsn` — the CDC cleanup job
    ///   purged rows we had not read. Unrecoverable: advancing past them loses data.
    /// * `from_lsn` is **above** the highest captured LSN — the capture job simply has
    ///   not populated this range yet, which is routine on a freshly enabled capture
    ///   instance or an idle table. Entirely transient.
    ///
    /// The two are indistinguishable from the error text alone, so we ask the server for
    /// `min_lsn` and compare. Getting this wrong is costly in both directions: treating
    /// the purge as transient silently skips data (the original defect), while treating
    /// the not-yet-captured case as fatal takes down a healthy pipeline on startup.
    async fn classify_cdc_window_error(
        &self,
        capture_instance: &str,
        start_lsn_hex: &str,
        end_lsn_hex: &str,
        server_message: &str,
    ) -> Result<Vec<SqlServerRawChange>> {
        let mut client = query::connect_client(&self.config).await?;
        let min_lsn_hex: String = match client
            .query(
                "SELECT sys.fn_varbintohexstr(sys.fn_cdc_get_min_lsn(@P1))",
                &[&capture_instance],
            )
            .await
        {
            Ok(result) => match result.into_first_result().await {
                Ok(rows) => rows
                    .into_iter()
                    .next()
                    .and_then(|row| row.get::<&str, _>(0).map(ToOwned::to_owned))
                    .unwrap_or_default(),
                Err(_) => String::new(),
            },
            Err(_) => String::new(),
        };

        // If min_lsn is unreadable we cannot prove the data is still there, and the
        // safe assumption for a correctness-first connector is that it is not.
        let (Ok(start), Ok(min)) = (
            lsn_hex_to_bytes(start_lsn_hex),
            lsn_hex_to_bytes(&min_lsn_hex),
        ) else {
            return Err(out_of_retention_error(
                capture_instance,
                start_lsn_hex,
                end_lsn_hex,
                server_message,
            ));
        };

        if compare_lsn(&start, &min).is_lt() {
            // Genuinely purged: our position predates the oldest retained change.
            return Err(out_of_retention_error(
                capture_instance,
                start_lsn_hex,
                end_lsn_hex,
                server_message,
            ));
        }

        // Ahead of what has been captured so far. Return no rows and let the window
        // logic retry; the window is never advanced past unread data, so this is safe.
        tracing::debug!(
            target: "rustcdc::source::sqlserver",
            capture_instance,
            start_lsn = %start_lsn_hex,
            min_lsn = %min_lsn_hex,
            "sqlserver CDC window is ahead of captured data (capture job has not reached \
             it yet); treating as empty and retrying",
        );
        Ok(Vec::new())
    }

    pub(super) fn map_changes_to_events(
        &mut self,
        meta: &CaptureInstanceMeta,
        changes: Vec<SqlServerRawChange>,
    ) -> Result<Vec<Event>> {
        // SQL Server CDC with `'all update old'` emits two rows per UPDATE.  Per the
        // `cdc.fn_cdc_get_all_changes_<capture_instance>` contract:
        //   op=3  UPDATE before-image (captured column values BEFORE the update)
        //         — ORDER BY emits this first (3 < 4)
        //   op=4  UPDATE after-image  (captured column values AFTER the update)
        //         — emitted second
        //
        // Both rows share the same (__$start_lsn, __$seqval).  We buffer the op=3
        // before-image in `self.pending_update_befores` and emit a single merged Event
        // when the op=4 after-image arrives.  The buffer persists across poll boundaries
        // so pairs split by `max_events_per_poll` are handled correctly.
        let mut out = Vec::with_capacity(changes.len());

        for change in changes {
            match change.operation {
                // DELETE: full row is the before-image.
                1 => {
                    if table_is_allowed(
                        Some(meta.schema.as_str()),
                        &meta.table,
                        &self.config.table_include_list,
                        &self.config.table_exclude_list,
                    ) {
                        out.push(build_sqlserver_event(
                            meta,
                            &change.start_lsn_hex,
                            &change.seqval_hex,
                            change.ts_ms,
                            Operation::Delete,
                            Some(change.row),
                            None,
                        ));
                    }
                }
                // INSERT: full row is the after-image.
                2 => {
                    if table_is_allowed(
                        Some(meta.schema.as_str()),
                        &meta.table,
                        &self.config.table_include_list,
                        &self.config.table_exclude_list,
                    ) {
                        out.push(build_sqlserver_event(
                            meta,
                            &change.start_lsn_hex,
                            &change.seqval_hex,
                            change.ts_ms,
                            Operation::Insert,
                            None,
                            Some(change.row),
                        ));
                    }
                }
                // UPDATE before-image: buffer until the op=4 after-image arrives.
                3 => {
                    let key = (change.start_lsn_hex, change.seqval_hex);
                    self.pending_update_befores
                        .insert(key, (change.row, change.ts_ms));
                }
                // UPDATE after-image: merge with the buffered op=3 before-image.
                4 => {
                    let key = (change.start_lsn_hex.clone(), change.seqval_hex.clone());
                    let (before_row, ts_ms) = self
                        .pending_update_befores
                        .remove(&key)
                        .map(|(row, ts)| (Some(row), ts))
                        .unwrap_or_else(|| (None, change.ts_ms));
                    if table_is_allowed(
                        Some(meta.schema.as_str()),
                        &meta.table,
                        &self.config.table_include_list,
                        &self.config.table_exclude_list,
                    ) {
                        out.push(build_sqlserver_event(
                            meta,
                            &change.start_lsn_hex,
                            &change.seqval_hex,
                            ts_ms,
                            Operation::Update,
                            before_row,
                            Some(change.row),
                        ));
                    }
                }
                other => {
                    return Err(Error::SourceError(format!(
                        "unsupported sqlserver CDC __$operation value: {other}"
                    )));
                }
            }
        }

        Ok(out)
    }
}

/// Build the hard error raised when SQL Server rejects the requested LSN window.
///
/// SQL Server raises error 313 ("An insufficient number of arguments were supplied
/// for the procedure or function") from `cdc.fn_cdc_get_all_changes_<capture_instance>`
/// when `from_lsn` falls outside the capture instance's currently retained range —
/// i.e. the CDC cleanup job has purged change rows we have not yet read.
///
/// This condition **must** fail loud.  Treating it as an empty result set would let
/// the poll loop advance the window past the purged range, permanently and silently
/// discarding every change the cleanup job removed.
fn out_of_retention_error(
    capture_instance: &str,
    start_lsn_hex: &str,
    end_lsn_hex: &str,
    server_message: &str,
) -> Error {
    Error::Unrecoverable(format!(
        "sqlserver CDC change data for capture instance '{capture_instance}' is no longer \
         retained: the requested window [{start_lsn_hex}, {end_lsn_hex}] is outside the range \
         currently available in the change tables, which means the CDC cleanup job has purged \
         changes this connector had not yet read. Resuming would silently skip those changes, \
         so the stream is stopped instead. Operator action required: re-snapshot the affected \
         tables, then restart from a fresh checkpoint. To prevent recurrence, increase the CDC \
         retention window (`sys.sp_cdc_change_job @job_type = 'cleanup', @retention = ...`) so \
         it comfortably exceeds the maximum expected connector downtime. \
         (server message: {server_message})"
    ))
}

fn build_sqlserver_event(
    meta: &CaptureInstanceMeta,
    start_lsn_hex: &str,
    seqval_hex: &str,
    ts_ms: u64,
    op: Operation,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
) -> Event {
    Event {
        before,
        after,
        op,
        source: SourceMetadata {
            source_name: "sqlserver".into(),
            offset: start_lsn_hex.to_owned(),
            timestamp: ts_ms,
        },
        ts: ts_ms,
        schema: Some(meta.schema.clone()),
        table: meta.table.clone(),
        primary_key: if meta.primary_key.is_empty() {
            None
        } else {
            Some(meta.primary_key.clone())
        },
        snapshot: None,
        transaction: Some(TransactionMetadata {
            tx_id: tx_id_from_seqval(seqval_hex),
            total_events: Some(1),
            event_index: 0,
        }),
        envelope_version: EVENT_ENVELOPE_VERSION,
        before_is_key_only: false,
        unavailable_columns: Vec::new(),
        before_unavailable_columns: Vec::new(),
    }
}

impl SqlServerStreamHandle {
    /// Fetch pending truncate events from the shadow table whose captured LSN
    /// is ≤ the current window end, filter by the table allow/deny lists, and
    /// return them as `Operation::Truncate` events.  Consumed IDs are marked
    /// after the batch is assembled (at-least-once: on crash-before-mark,
    /// the same truncate events will be re-emitted on replay).
    pub(super) async fn fetch_and_emit_truncate_events(&mut self) -> Result<Vec<Event>> {
        if !self.config.capture_truncate_events {
            return Ok(Vec::new());
        }

        let lsn_end_hex = lsn_bytes_to_hex(&self.stream.lsn_end);
        let mut client = query::connect_client(&self.config).await?;

        let rows = query::fetch_pending_truncate_events(
            &mut client,
            &self.config.cdc_schema,
            &lsn_end_hex,
        )
        .await?;

        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let ts_now = now_millis();
        let mut consumed_ids: Vec<i64> = Vec::with_capacity(rows.len());
        let mut events: Vec<Event> = Vec::with_capacity(rows.len());

        for row in rows {
            if !table_is_allowed(
                Some(row.schema_name.as_str()),
                &row.table_name,
                &self.config.table_include_list,
                &self.config.table_exclude_list,
            ) {
                // Mark as consumed even if filtered to avoid re-emitting on
                // subsequent polls.
                consumed_ids.push(row.id);
                continue;
            }

            let lsn_hex = row
                .max_lsn_bytes
                .map(|b| lsn_bytes_to_hex(&b))
                .unwrap_or_else(|| lsn_end_hex.clone());

            let raw = SqlServerRawTruncate {
                id: row.id,
                schema_name: row.schema_name.clone(),
                table_name: row.table_name.clone(),
                lsn_hex: lsn_hex.clone(),
                ts_ms: if row.ts_ms > 0 { row.ts_ms } else { ts_now },
            };

            events.push(build_truncate_event(&raw));
            consumed_ids.push(raw.id);
        }

        if !consumed_ids.is_empty() {
            query::mark_truncate_events_consumed(
                &mut client,
                &self.config.cdc_schema,
                &consumed_ids,
            )
            .await?;

            // Best-effort periodic cleanup; ignore errors to avoid interrupting
            // the stream on non-critical housekeeping failures.
            let _ =
                query::cleanup_consumed_truncate_events(&mut client, &self.config.cdc_schema).await;
        }

        Ok(events)
    }
}

fn build_truncate_event(raw: &SqlServerRawTruncate) -> Event {
    Event {
        before: None,
        after: None,
        op: Operation::Truncate,
        source: SourceMetadata {
            source_name: "sqlserver".into(),
            offset: raw.lsn_hex.clone(),
            timestamp: raw.ts_ms,
        },
        ts: raw.ts_ms,
        schema: Some(raw.schema_name.clone()),
        table: raw.table_name.clone(),
        primary_key: None,
        snapshot: None,
        transaction: None,
        envelope_version: EVENT_ENVELOPE_VERSION,
        before_is_key_only: false,
        unavailable_columns: Vec::new(),
        before_unavailable_columns: Vec::new(),
    }
}

#[cfg(test)]
mod window_advance_tests {
    use super::{compare_lsn, SqlServerStreamHandle};

    /// LSNs are 10 bytes, compared big-endian. Only the tail matters for these cases.
    fn lsn(tail: u8) -> [u8; 10] {
        let mut bytes = [0u8; 10];
        bytes[9] = tail;
        bytes
    }

    /// `(next_start, next_end)` for a window `[start, end]`, given the harvested maximum.
    fn advance(start: u8, end: u8, max: u8) -> (u8, u8) {
        let (next_start, next_end) = SqlServerStreamHandle::next_window(
            lsn(start),
            lsn(end),
            lsn(end.saturating_add(1)), // fn_cdc_increment_lsn(end)
            lsn(max),
        );
        (next_start[9], next_end[9])
    }

    fn is_empty(window: (u8, u8)) -> bool {
        compare_lsn(&lsn(window.0), &lsn(window.1)).is_gt()
    }

    /// The ordinary case: the capture job harvested past the window just read.
    #[test]
    fn a_consumed_window_advances_to_the_new_harvested_maximum() {
        assert_eq!(advance(5, 10, 20), (11, 20));
        assert!(!is_empty((11, 20)));
    }

    /// A consumed window whose end is the maximum becomes **empty**, not clamped. Clamping the end
    /// up to the start is what made the next advance step again, walking the read point forward on
    /// every empty poll.
    #[test]
    fn a_consumed_window_at_the_maximum_becomes_empty_rather_than_clamped() {
        let window = advance(5, 10, 10);
        assert_eq!(window, (11, 10));
        assert!(
            is_empty(window),
            "an empty window is represented, not avoided — that is what stops the creep"
        );
    }

    /// The creep this closes. The maximum stands still; the read point must not move.
    #[test]
    fn a_quiet_database_does_not_walk_the_read_point_forward() {
        let mut window = advance(5, 10, 10); // (11, 10), empty
        for _ in 0..100 {
            let next = advance(window.0, window.1, 10);
            assert_eq!(
                next,
                (11, 10),
                "while the harvested maximum stands still the read point must stay at 11"
            );
            window = next;
        }
    }

    /// The trap the earlier attempt fell into, now asserted as fixed: an empty window whose
    /// maximum later jumps must reopen **from its original start**, so the LSN it was parked at is
    /// still read. Advancing from the parked end instead would skip it.
    #[test]
    fn an_empty_window_reopens_without_skipping_the_lsn_it_was_parked_at() {
        let empty = advance(5, 10, 10);
        assert_eq!(empty, (11, 10));

        let reopened = advance(empty.0, empty.1, 40);
        assert_eq!(
            reopened,
            (11, 40),
            "LSN 11 was never readable while the maximum was 10, so it must be inside the \
             reopened window — advancing to 12 here is the skip that made the naive repair worse \
             than the creep"
        );
        assert!(!is_empty(reopened));
    }

    /// And once that reopened window is consumed, the lower bound moves again.
    #[test]
    fn a_reopened_window_advances_normally_once_consumed() {
        assert_eq!(advance(11, 40, 40), (41, 40));
        assert!(is_empty((41, 40)));
    }

    /// A first window that is already empty (nothing harvested at startup) stays put.
    #[test]
    fn an_initially_empty_window_holds_its_lower_bound() {
        assert_eq!(advance(1, 0, 0), (1, 0));
        assert_eq!(
            advance(1, 0, 7),
            (1, 7),
            "and opens from 1 when the maximum arrives"
        );
    }
}
