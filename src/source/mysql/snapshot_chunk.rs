use crate::core::{Event, Operation, Result, SnapshotMetadata, SourceMetadata, EVENT_ENVELOPE_VERSION};
use crate::source::helpers::now_millis;

use super::{MysqlSnapshotHandle, DEFAULT_SNAPSHOT_CHUNK_SIZE};

pub(super) async fn next_snapshot_chunk(
    handle: &mut MysqlSnapshotHandle,
    chunk_size: usize,
) -> Result<Vec<Event>> {
    if handle.is_complete() {
        return Ok(Vec::new());
    }

    let mut events = Vec::new();
    let requested = if chunk_size == 0 {
        DEFAULT_SNAPSHOT_CHUNK_SIZE
    } else {
        chunk_size
    };

    while events.len() < requested && handle.current_table < handle.tables.len() {
        let table_index = handle.current_table;
        let (table_name, live_query, cursor_position, primary_key_columns) = {
            let table = &handle.tables[table_index];
            (
                table.snapshot.table.clone(),
                table.live_query,
                table.snapshot.cursor_position.clone(),
                table.primary_key_columns.clone(),
            )
        };
        let remaining = requested - events.len();

        if live_query {
            let rows = handle
                .fetch_live_rows(table_index, cursor_position.as_deref(), remaining)
                .await?;
            if rows.is_empty() {
                let table = &mut handle.tables[table_index];
                table.snapshot.is_complete = true;
                handle.current_table += 1;
                continue;
            }

            for (cursor_json, row_json) in rows {
                let cursor_text = serde_json::to_string(&cursor_json)?;
                {
                    let table = &mut handle.tables[table_index];
                    table.snapshot.rows_processed += 1;
                    table.snapshot.cursor_position = Some(cursor_text.clone());
                }
                handle.emitted_rows += 1;

                let ts = now_millis();
                events.push(Event {
                    before: None,
                    after: Some(row_json),
                    op: Operation::Read,
                    source: SourceMetadata {
                        source_name: handle.source_name.clone(),
                        offset: format!(
                            "{}:{}:{}",
                            handle.snapshot.binlog_file,
                            handle.snapshot.binlog_pos,
                            cursor_text
                        ),
                        timestamp: ts,
                    },
                    ts,
                    schema: None,
                    table: table_name.clone(),
                    primary_key: Some(primary_key_columns.clone()),
                    snapshot: Some(SnapshotMetadata {
                        snapshot_id: handle.snapshot.snapshot_id.clone(),
                        chunk_index: handle.next_chunk_index,
                        is_last_chunk: false,
                    }),
                    transaction: None,
                    envelope_version: EVENT_ENVELOPE_VERSION,
                });
            }
        } else {
            let table = &mut handle.tables[table_index];
            while events.len() < requested && table.next_row < table.rows.len() {
                let cursor = serde_json::to_string(&serde_json::json!([table.next_row]))?;
                table.snapshot.rows_processed += 1;
                table.snapshot.cursor_position = Some(cursor.clone());

                let row = table.rows[table.next_row].clone();
                table.next_row += 1;
                handle.emitted_rows += 1;
                let ts = now_millis();

                events.push(Event {
                    before: None,
                    after: Some(row),
                    op: Operation::Read,
                    source: SourceMetadata {
                        source_name: handle.source_name.clone(),
                        offset: format!(
                            "{}:{}:{}",
                            handle.snapshot.binlog_file,
                            handle.snapshot.binlog_pos,
                            cursor
                        ),
                        timestamp: ts,
                    },
                    ts,
                    schema: None,
                    table: table.snapshot.table.clone(),
                    primary_key: Some(table.primary_key_columns.clone()),
                    snapshot: Some(SnapshotMetadata {
                        snapshot_id: handle.snapshot.snapshot_id.clone(),
                        chunk_index: handle.next_chunk_index,
                        is_last_chunk: false,
                    }),
                    transaction: None,
                    envelope_version: EVENT_ENVELOPE_VERSION,
                });
            }

            if table.next_row >= table.rows.len() {
                table.snapshot.is_complete = true;
                handle.current_table += 1;
            }
        }
    }

    if !events.is_empty() {
        let final_chunk = handle.is_complete();
        if final_chunk {
            if let Some(last) = events.last_mut() {
                if let Some(snapshot) = last.snapshot.as_mut() {
                    snapshot.is_last_chunk = true;
                }
            }
        }
        handle.next_chunk_index += 1;
    }

    handle.sync_snapshot_tables();
    Ok(events)
}
