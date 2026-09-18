use crate::core::{
    BeforeImage, EVENT_ENVELOPE_VERSION, Event, Operation, Result, SnapshotMetadata, SourceMetadata,
};
use crate::ddl_capture::{CapturedDdl, DDL_TYPE_READ_SCHEMA};
use crate::source::helpers::now_millis;
use crate::source::schema_catalog::{observed_statement, table_schema_from_catalog};

use super::{SqlServerSnapshotHandle, lsn_bytes_to_hex};

pub(super) async fn next_sqlserver_snapshot_chunk(
    handle: &mut SqlServerSnapshotHandle,
    chunk_size: usize,
) -> Result<Vec<Event>> {
    if handle.is_complete() {
        return Ok(Vec::new());
    }

    let requested = if chunk_size == 0 { 1000 } else { chunk_size };
    let mut events = Vec::with_capacity(requested);

    while events.len() < requested && handle.current_table < handle.tables.len() {
        let table_index = handle.current_table;
        let (schema_name, cursor, primary_key_columns, table_name_only, is_complete) = {
            let state = &handle.tables[table_index];
            (
                state.schema.clone(),
                state.snapshot.cursor_position.clone(),
                state.primary_key_columns.clone(),
                state.table.clone(),
                state.snapshot.is_complete,
            )
        };

        if is_complete {
            handle.current_table += 1;
            continue;
        }

        // Announce the table's schema before its first row. SQL Server change rows carry
        // text values, and until this the snapshot path emitted no schema event at all.
        if !handle.tables[table_index].schema_announced {
            handle.tables[table_index].schema_announced = true;
            let catalog = handle.tables[table_index].catalog_columns.clone();
            if !catalog.is_empty() {
                let ts = now_millis();
                let captured = CapturedDdl {
                    ddl_type: DDL_TYPE_READ_SCHEMA.to_string(),
                    schema: schema_name.clone(),
                    table: table_name_only.clone(),
                    statement: observed_statement(
                        &schema_name,
                        &table_name_only,
                        "INFORMATION_SCHEMA.COLUMNS",
                    ),
                    result_schema: Some(table_schema_from_catalog(
                        &schema_name,
                        &table_name_only,
                        &catalog,
                        &primary_key_columns,
                    )),
                    schema_diff: None,
                    ts,
                };
                // The same offset shape the rows around it carry — the snapshot LSN plus
                // a cursor. An event's offset is a source position the runtime parses
                // when it builds a checkpoint, so a synthetic label is not merely
                // uninformative: it fails the parse and takes the handoff with it.
                events.push(captured.to_event(
                    "sqlserver",
                    format!(
                        "{}:schema:{schema_name}.{table_name_only}",
                        lsn_bytes_to_hex(&handle.snapshot.lsn_start)
                    ),
                    ts,
                ));
            }
        }

        let remaining = requested - events.len();
        let rows = handle
            .row_fetcher
            .fetch_keyset_rows(&handle.tables[table_index], cursor.as_deref(), remaining)
            .await?;

        if rows.is_empty() {
            let state = &mut handle.tables[table_index];
            state.snapshot.is_complete = true;
            handle.current_table += 1;
            continue;
        }

        for (cursor_json, row_json) in rows {
            {
                let state = &mut handle.tables[table_index];
                state.snapshot.rows_processed = state.snapshot.rows_processed.saturating_add(1);
                state.snapshot.cursor_position = Some(cursor_json.clone());
            }
            handle.emitted_rows = handle.emitted_rows.saturating_add(1);
            let ts = now_millis();

            events.push(Event {
                before: BeforeImage::Unavailable,
                after: Some(row_json),
                op: Operation::Read,
                source: SourceMetadata {
                    source_name: "sqlserver".into(),
                    offset: format!(
                        "{}:{}",
                        lsn_bytes_to_hex(&handle.snapshot.lsn_start),
                        cursor_json
                    ),
                    timestamp: ts,
                },
                ts,
                schema: Some(schema_name.clone()),
                table: table_name_only.clone(),
                primary_key: if primary_key_columns.is_empty() {
                    None
                } else {
                    Some(primary_key_columns.clone())
                },
                snapshot: Some(SnapshotMetadata {
                    snapshot_id: handle.snapshot.snapshot_id.clone(),
                    chunk_index: handle.next_chunk_index,
                    is_last_chunk: false,
                }),
                transaction: None,
                envelope_version: EVENT_ENVELOPE_VERSION,
                unavailable_columns: Vec::new(),
            });
        }

        let state = &mut handle.tables[table_index];
        if state.snapshot.rows_processed >= state.snapshot.total_rows {
            state.snapshot.is_complete = true;
            handle.current_table += 1;
        }
    }

    if !events.is_empty() {
        if handle.is_complete()
            && let Some(last) = events.last_mut()
            && let Some(snapshot) = last.snapshot.as_mut()
        {
            snapshot.is_last_chunk = true;
        }
        handle.next_chunk_index = handle.next_chunk_index.saturating_add(1);
    }

    handle.sync_snapshot_tables();
    Ok(events)
}
