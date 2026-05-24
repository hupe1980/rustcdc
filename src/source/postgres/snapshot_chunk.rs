use crate::{
    core::{Error, Event, Operation, Result, SnapshotMetadata, SourceMetadata, EVENT_ENVELOPE_VERSION},
    source::helpers::now_millis,
};

use super::{PostgresSnapshotHandle, DEFAULT_SNAPSHOT_CHUNK_SIZE};

pub(super) async fn next_postgres_snapshot_chunk(
    handle: &mut PostgresSnapshotHandle,
    chunk_size: usize,
) -> Result<Vec<Event>> {
    if handle.is_complete() {
        if handle.snapshot.snapshot_end_ts == 0 {
            handle.snapshot.snapshot_end_ts = now_millis();
        }
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
        let (table_name, live_query, cursor_position, key_columns, key_types) = {
            let table = &handle.tables[table_index];
            (
                table.snapshot.table.clone(),
                table.live_query,
                table.snapshot.cursor_position.clone(),
                table.primary_key_columns.clone(),
                table.primary_key_types.clone(),
            )
        };
        let remaining = requested - events.len();

        if live_query {
            if handle.client.is_none() {
                let table = &mut handle.tables[table_index];
                while events.len() < requested && table.next_row < table.rows.len() {
                    let row = table.rows[table.next_row].clone();
                    let offset = format!("{table_name}:offline:{}", table.next_row);
                    table.next_row += 1;
                    table.snapshot.rows_processed += 1;
                    handle.emitted_rows += 1;
                    handle.emitted_in_run += 1;

                    events.push(Event {
                        before: None,
                        after: Some(row),
                        op: Operation::Read,
                        source: SourceMetadata {
                            source_name: handle.source_name.clone(),
                            offset,
                            timestamp: now_millis(),
                        },
                        ts: now_millis(),
                        schema: None,
                        table: table_name.clone(),
                        primary_key: None,
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
                continue;
            }

            let rows = handle
                .fetch_live_rows(
                    &table_name,
                    &key_columns,
                    &key_types,
                    cursor_position.as_deref(),
                    remaining,
                )
                .await?;
            if rows.is_empty() {
                let table = &mut handle.tables[table_index];
                table.snapshot.is_complete = true;
                handle.current_table += 1;
                continue;
            }

            for (key_values, row) in rows {
                let key_cursor = serde_json::to_string(&key_values).map_err(|error| {
                    Error::SerializationError(format!(
                        "failed encoding snapshot keyset cursor for table '{table_name}': {error}"
                    ))
                })?;
                {
                    let table = &mut handle.tables[table_index];
                    table.snapshot.rows_processed += 1;
                    table.snapshot.cursor_position = Some(key_cursor.clone());
                }
                handle.emitted_rows += 1;
                handle.emitted_in_run += 1;

                events.push(Event {
                    before: None,
                    after: Some(row),
                    op: Operation::Read,
                    source: SourceMetadata {
                        source_name: handle.source_name.clone(),
                        offset: format!("{table_name}:{key_cursor}"),
                        timestamp: now_millis(),
                    },
                    ts: now_millis(),
                    schema: None,
                    table: table_name.clone(),
                    primary_key: None,
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
                let cursor = format!("{}:{}", table_index, table.next_row);
                table.snapshot.rows_processed += 1;
                table.snapshot.cursor_position = Some(cursor.clone());

                let row = table.rows[table.next_row].clone();
                table.next_row += 1;
                handle.emitted_rows += 1;
                handle.emitted_in_run += 1;

                events.push(Event {
                    before: None,
                    after: Some(row),
                    op: Operation::Read,
                    source: SourceMetadata {
                        source_name: handle.source_name.clone(),
                        offset: cursor,
                        timestamp: now_millis(),
                    },
                    ts: now_millis(),
                    schema: None,
                    table: table.snapshot.table.clone(),
                    primary_key: None,
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
    if handle.is_complete() {
        handle.snapshot.snapshot_end_ts = now_millis();
    }
    Ok(events)
}
