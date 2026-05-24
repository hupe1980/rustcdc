use mysql_async::prelude::Queryable;

use crate::{
    core::{Error, Result},
    source::helpers::now_millis,
};

use super::{
    parser::{mysql_qualified_table_name_from_reference, split_table_reference},
    state::TableSnapshotState,
    MysqlSnapshot, TableSnapshot,
};

pub(super) async fn begin_snapshot_and_collect_table_states(
    connection: &mut mysql_async::Conn,
    tables: &[&str],
    default_database: &str,
) -> Result<(MysqlSnapshot, Vec<TableSnapshotState>)> {
    let start_ts = now_millis();
    connection
        .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "failed to start mysql snapshot transaction: {error}"
            ))
        })?;

    let mut master_row: mysql_async::Row = connection
        .query_first("SHOW MASTER STATUS")
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "failed to read mysql master status for snapshot: {error}"
            ))
        })?
        .ok_or_else(|| Error::SourceError("mysql master status unavailable for snapshot".into()))?;
    let binlog_file: String = master_row.take(0).unwrap_or_default();
    let binlog_pos_u64: u64 = master_row.take(1).unwrap_or(4);
    let binlog_pos = u32::try_from(binlog_pos_u64)
        .map_err(|_| Error::SourceError(format!("mysql binlog position exceeds u32: {binlog_pos_u64}")))?;
    let gtid: String = connection
        .query_first("SELECT @@GLOBAL.GTID_EXECUTED")
        .await
        .ok()
        .flatten()
        .unwrap_or_default();

    let mut states = Vec::with_capacity(tables.len());
    for table in tables {
        let (schema_name, table_name) = split_table_reference(table)?;
        let table_schema = schema_name.unwrap_or_else(|| default_database.to_string());

        let primary_key_columns: Vec<String> = connection
            .exec(
                "SELECT COLUMN_NAME \
                 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' \
                 ORDER BY ORDINAL_POSITION",
                (&table_schema, &table_name),
            )
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "failed to query mysql primary key metadata for '{}': {error}",
                    table
                ))
            })?;

        if primary_key_columns.is_empty() {
            return Err(Error::SourceError(format!(
                "mysql snapshot requires PRIMARY KEY columns for table '{table}'"
            )));
        }

        let all_columns: Vec<String> = connection
            .exec(
                "SELECT COLUMN_NAME \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
                 ORDER BY ORDINAL_POSITION",
                (&table_schema, &table_name),
            )
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "failed to query mysql column metadata for '{}': {error}",
                    table
                ))
            })?;

        if all_columns.is_empty() {
            return Err(Error::SourceError(format!(
                "mysql snapshot table '{table}' has no columns"
            )));
        }

        let table_ref = mysql_qualified_table_name_from_reference(table)?;
        let total_rows_query = format!("SELECT COUNT(*) FROM {table_ref}");
        let total_rows: i64 = connection
            .query_first(&total_rows_query)
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "failed counting rows for mysql table '{}': {error}",
                    table
                ))
            })?
            .unwrap_or(0);

        let total_rows = u64::try_from(total_rows).map_err(|_| {
            Error::SourceError(format!(
                "negative row count returned for mysql table '{}'",
                table
            ))
        })?;

        states.push(TableSnapshotState {
            snapshot: TableSnapshot {
                table: (*table).to_string(),
                total_rows,
                rows_processed: 0,
                cursor_position: None,
                is_complete: total_rows == 0,
            },
            primary_key_columns,
            rows: Vec::new(),
            next_row: 0,
            live_query: true,
        });
    }

    let snapshot = MysqlSnapshot {
        tables: states.iter().map(|state| state.snapshot.clone()).collect(),
        snapshot_id: format!("mysql-snapshot-{start_ts}-{}", tables.len()),
        snapshot_start_ts: start_ts,
        binlog_file,
        binlog_pos,
        gtid,
    };

    Ok((snapshot, states))
}
