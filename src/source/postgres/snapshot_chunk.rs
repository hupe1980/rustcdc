use crate::{
    core::{
        Error, Event, Operation, Result, SnapshotMetadata, SourceMetadata, EVENT_ENVELOPE_VERSION,
    },
    source::helpers::now_millis,
};

use super::{PostgresSnapshotHandle, DEFAULT_SNAPSHOT_CHUNK_SIZE};

/// Split a configured table name into `(schema, bare_table)`.
///
/// Snapshot tables are configured as either `"users"` or `"public.users"`. The event
/// envelope carries the two separately — `Event::qualified_table_name()` joins them —
/// so the schema must be stripped from `table` or the name double-qualifies.
///
/// Unqualified names default to `public`, matching PostgreSQL's own default
/// `search_path` and the identity the streaming path derives from pgoutput RELATION
/// messages. Getting this consistent is what keeps a router pattern matching the same
/// table across the snapshot→stream transition.
fn split_qualified_table_name(configured: &str) -> (Option<String>, String) {
    match configured.split_once('.') {
        Some((schema, table)) if !schema.is_empty() && !table.is_empty() => {
            (Some(schema.to_string()), table.to_string())
        }
        _ => (Some("public".to_string()), configured.to_string()),
    }
}

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

        // Emit the same identity the streaming path emits.
        //
        // Snapshot events previously carried `schema: None` and `primary_key: None`
        // while streaming populated both, so for one physical table the two phases
        // disagreed on both the event key and the routing name:
        //
        //   * `Event::primary_key_values()` returned `None`, so `encode_key` produced
        //     an unkeyed record for **every row of the initial load**. Log compaction
        //     never collapses those rows, upsert consumers cannot correlate them with
        //     later updates, and the pre-snapshot value resurfaces after compaction.
        //   * `qualified_table_name()` yielded `"users"` during snapshot but
        //     `"public.users"` during streaming, so a router configured for
        //     `public.users` silently received zero snapshot rows.
        //
        // MySQL and SQL Server already set the primary key on snapshot rows;
        // PostgreSQL was the outlier, and `postgres` is the default feature.
        let (schema_name, bare_table) = split_qualified_table_name(&table_name);
        let primary_key = if key_columns.is_empty() {
            None
        } else {
            Some(key_columns.clone())
        };

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
                        schema: schema_name.clone(),
                        table: bare_table.clone(),
                        primary_key: primary_key.clone(),
                        snapshot: Some(SnapshotMetadata {
                            snapshot_id: handle.snapshot.snapshot_id.clone(),
                            chunk_index: handle.next_chunk_index,
                            is_last_chunk: false,
                        }),
                        transaction: None,
                        envelope_version: EVENT_ENVELOPE_VERSION,
                        before_is_key_only: false,
                        unavailable_columns: Vec::new(),
                        before_unavailable_columns: Vec::new(),
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
                    schema: schema_name.clone(),
                    table: bare_table.clone(),
                    primary_key: primary_key.clone(),
                    snapshot: Some(SnapshotMetadata {
                        snapshot_id: handle.snapshot.snapshot_id.clone(),
                        chunk_index: handle.next_chunk_index,
                        is_last_chunk: false,
                    }),
                    transaction: None,
                    envelope_version: EVENT_ENVELOPE_VERSION,
                    before_is_key_only: false,
                    unavailable_columns: Vec::new(),
                    before_unavailable_columns: Vec::new(),
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
                    schema: schema_name.clone(),
                    table: table.snapshot.table.clone(),
                    primary_key: primary_key.clone(),
                    snapshot: Some(SnapshotMetadata {
                        snapshot_id: handle.snapshot.snapshot_id.clone(),
                        chunk_index: handle.next_chunk_index,
                        is_last_chunk: false,
                    }),
                    transaction: None,
                    envelope_version: EVENT_ENVELOPE_VERSION,
                    before_is_key_only: false,
                    unavailable_columns: Vec::new(),
                    before_unavailable_columns: Vec::new(),
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
