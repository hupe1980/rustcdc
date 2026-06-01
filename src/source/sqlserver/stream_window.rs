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

        let next_start = lsn_hex_to_bytes(&start_hex)?;
        let mut next_end = lsn_hex_to_bytes(&end_hex)?;
        if compare_lsn(&next_end, &next_start).is_lt() {
            next_end = next_start;
        }

        self.stream.lsn_start = next_start;
        self.stream.lsn_end = next_end;
        Ok(())
    }

    pub(super) async fn fetch_changes_for_capture_instance(
        &self,
        capture_instance: &str,
        columns: &[String],
        max_events_per_poll: usize,
    ) -> Result<Vec<SqlServerRawChange>> {
        validate_capture_instance_name(capture_instance)?;
        let mut client = query::connect_client(&self.config).await?;
        let start_lsn_hex = lsn_bytes_to_hex(&self.stream.lsn_start);
        let end_lsn_hex = lsn_bytes_to_hex(&self.stream.lsn_end);

        let sql = build_cdc_poll_sql(
            capture_instance,
            columns,
            max_events_per_poll,
            &start_lsn_hex,
            &end_lsn_hex,
        );

        let query_result = match client.query(&sql, &[]).await {
            Ok(value) => value,
            Err(error) => {
                let text = error.to_string();
                if is_sqlserver_cdc_window_error(&text) {
                    return Ok(Vec::new());
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
                    return Ok(Vec::new());
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

            let mut object = serde_json::Map::new();
            for (index, column) in columns.iter().enumerate() {
                object.insert(
                    column.clone(),
                    super::decode_sqlserver_cell_to_json(&row, 3 + index),
                );
            }

            let ts_ms = row.get::<i64, _>(3 + columns.len()).unwrap_or(0);
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

    pub(super) fn map_changes_to_events(
        &mut self,
        meta: &CaptureInstanceMeta,
        changes: Vec<SqlServerRawChange>,
    ) -> Result<Vec<Event>> {
        // SQL Server CDC with `'all update old'` emits two rows per UPDATE:
        //   op=3  UPDATE after-image  (new column values)  — ORDER BY emits this first (3 < 4)
        //   op=4  UPDATE before-image (old column values)  — emitted second
        //
        // Both rows share the same (__$start_lsn, __$seqval).  We buffer the op=3 row
        // in `self.pending_update_afters` and emit a single merged Event when op=4 arrives.
        // The buffer persists across poll boundaries so pairs split by `max_events_per_poll`
        // are handled correctly.
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
                // UPDATE after-image: buffer until op=4 (before-image) arrives.
                3 => {
                    let key = (change.start_lsn_hex, change.seqval_hex);
                    self.pending_update_afters
                        .insert(key, (change.row, change.ts_ms));
                }
                // UPDATE before-image: merge with buffered op=3 after-image.
                4 => {
                    let key = (change.start_lsn_hex.clone(), change.seqval_hex.clone());
                    let (after_row, ts_ms) = self
                        .pending_update_afters
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
                            Some(change.row),
                            after_row,
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
            total_events: 1,
            event_index: 0,
        }),
        envelope_version: EVENT_ENVELOPE_VERSION,
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
    }
}
