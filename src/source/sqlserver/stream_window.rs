use crate::core::{
    Error, Event, Operation, Result, SourceMetadata, TransactionMetadata, EVENT_ENVELOPE_VERSION,
};

use super::{
    build_cdc_poll_sql, compare_lsn, is_sqlserver_cdc_window_error, lsn_bytes_to_hex,
    lsn_hex_to_bytes, query, tx_id_from_seqval, validate_capture_instance_name,
    CaptureInstanceMeta, SqlServerRawChange, SqlServerStreamHandle, ZERO_LSN_HEX,
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
        &self,
        meta: &CaptureInstanceMeta,
        changes: Vec<SqlServerRawChange>,
    ) -> Result<Vec<Event>> {
        let mut out = Vec::with_capacity(changes.len());

        for change in changes {
            let (op, before, after) = match change.operation {
                1 => (Operation::Delete, Some(change.row.clone()), None),
                2 => (Operation::Insert, None, Some(change.row.clone())),
                3 => (Operation::Update, Some(change.row.clone()), None),
                4 => (Operation::Update, None, Some(change.row.clone())),
                other => {
                    return Err(Error::SourceError(format!(
                        "unsupported sqlserver CDC __$operation value: {other}"
                    )))
                }
            };

            out.push(Event {
                before,
                after,
                op,
                source: SourceMetadata {
                    source_name: "sqlserver".into(),
                    offset: change.start_lsn_hex.clone(),
                    timestamp: change.ts_ms,
                },
                ts: change.ts_ms,
                schema: Some(meta.schema.clone()),
                table: meta.table.clone(),
                primary_key: if meta.primary_key.is_empty() {
                    None
                } else {
                    Some(meta.primary_key.clone())
                },
                snapshot: None,
                transaction: Some(TransactionMetadata {
                    tx_id: tx_id_from_seqval(&change.seqval_hex),
                    total_events: 1,
                    event_index: 0,
                }),
                envelope_version: EVENT_ENVELOPE_VERSION,
            });
        }

        Ok(out)
    }
}
