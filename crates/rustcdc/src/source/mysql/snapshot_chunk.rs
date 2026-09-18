use crate::core::{
    BeforeImage, EVENT_ENVELOPE_VERSION, Event, Operation, Result, SnapshotMetadata, SourceMetadata,
};
use crate::ddl_capture::{CapturedDdl, DDL_TYPE_READ_SCHEMA};
use crate::source::helpers::now_millis;
use crate::source::schema_catalog::{
    mark_as_snapshot_event, observed_statement, table_schema_from_catalog,
};

use super::{DEFAULT_SNAPSHOT_CHUNK_SIZE, MysqlSnapshotHandle};

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
        let (live_query, cursor_position, primary_key_columns, schema_name, bare_table) = {
            let table = &handle.tables[table_index];
            (
                table.live_query,
                table.snapshot.cursor_position.clone(),
                table.primary_key_columns.clone(),
                table.schema_name.clone(),
                table.bare_table.clone(),
            )
        };

        // Announce the table's schema before its first row. MySQL's schema events
        // otherwise come only from DDL parsed out of the binlog, so a table whose
        // `CREATE TABLE` predates capture had none at all — and snapshot rows carry text
        // values like every other event.
        if !handle.tables[table_index].schema_announced {
            handle.tables[table_index].schema_announced = true;
            let catalog = handle.tables[table_index].catalog_columns.clone();
            if !catalog.is_empty() {
                let ts = now_millis();
                let captured = CapturedDdl {
                    ddl_type: DDL_TYPE_READ_SCHEMA.to_string(),
                    schema: schema_name.clone(),
                    table: bare_table.clone(),
                    statement: observed_statement(&schema_name, &bare_table, "information_schema"),
                    result_schema: Some(table_schema_from_catalog(
                        &schema_name,
                        &bare_table,
                        &catalog,
                        &primary_key_columns,
                    )),
                    schema_diff: None,
                    ts,
                };
                let mut event = captured.to_event(
                    &handle.source_name,
                    format!(
                        "{}:{}",
                        handle.snapshot.binlog_file, handle.snapshot.binlog_pos
                    ),
                    ts,
                );
                mark_as_snapshot_event(
                    &mut event,
                    &handle.snapshot.snapshot_id,
                    handle.next_chunk_index,
                );
                events.push(event);
            }
        }

        // Computed **after** the announcement, not before: the announcement occupies a
        // slot in this chunk, and a `remaining` taken ahead of it makes the chunk return
        // `requested + 1` events. That overflow costs a row — the runtime delivers a
        // buffer's worth and the extra one is dropped, while the snapshot cursor has
        // already advanced past it.
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
                    before: BeforeImage::Unavailable,
                    after: Some(row_json),
                    op: Operation::Read,
                    source: SourceMetadata {
                        source_name: handle.source_name.clone(),
                        offset: format!(
                            "{}:{}:{}",
                            handle.snapshot.binlog_file, handle.snapshot.binlog_pos, cursor_text
                        ),
                        timestamp: ts,
                    },
                    ts,
                    schema: Some(schema_name.clone()),
                    table: bare_table.clone(),
                    primary_key: Some(primary_key_columns.clone()),
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
                    before: BeforeImage::Unavailable,
                    after: Some(row),
                    op: Operation::Read,
                    source: SourceMetadata {
                        source_name: handle.source_name.clone(),
                        offset: format!(
                            "{}:{}:{}",
                            handle.snapshot.binlog_file, handle.snapshot.binlog_pos, cursor
                        ),
                        timestamp: ts,
                    },
                    ts,
                    // Same identity as the live-query branch and the stream: schema and
                    // bare table carried separately, never a joined string.
                    schema: Some(table.schema_name.clone()),
                    table: table.bare_table.clone(),
                    primary_key: Some(table.primary_key_columns.clone()),
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

            if table.next_row >= table.rows.len() {
                table.snapshot.is_complete = true;
                handle.current_table += 1;
            }
        }
    }

    if !events.is_empty() {
        let final_chunk = handle.is_complete();
        if final_chunk
            && let Some(last) = events.last_mut()
            && let Some(snapshot) = last.snapshot.as_mut()
        {
            snapshot.is_last_chunk = true;
        }
        handle.next_chunk_index += 1;
    }

    handle.sync_snapshot_tables();
    Ok(events)
}
