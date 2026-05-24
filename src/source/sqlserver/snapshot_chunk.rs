use crate::core::{Event, Operation, Result, SnapshotMetadata, SourceMetadata, EVENT_ENVELOPE_VERSION};
use crate::source::helpers::now_millis;

use super::{lsn_bytes_to_hex, SqlServerSnapshotHandle};

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
                before: None,
                after: Some(row_json),
                op: Operation::Read,
                source: SourceMetadata {
                    source_name: "sqlserver".into(),
                    offset: format!("{}:{}", lsn_bytes_to_hex(&handle.snapshot.lsn_start), cursor_json),
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
            });
        }

        let state = &mut handle.tables[table_index];
        if state.snapshot.rows_processed >= state.snapshot.total_rows {
            state.snapshot.is_complete = true;
            handle.current_table += 1;
        }
    }

    if !events.is_empty() {
        if handle.is_complete() {
            if let Some(last) = events.last_mut() {
                if let Some(snapshot) = last.snapshot.as_mut() {
                    snapshot.is_last_chunk = true;
                }
            }
        }
        handle.next_chunk_index = handle.next_chunk_index.saturating_add(1);
    }

    handle.sync_snapshot_tables();
    Ok(events)
}
