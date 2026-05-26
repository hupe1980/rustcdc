use std::sync::Arc;

use tokio_postgres::Client;

use crate::{
    core::{Error, Offset, Result},
    source::{SnapshotHandle, Source},
};

use super::{
    now_millis, parse_table_reference, qualified_table_name, query_current_wal_lsn,
    query_primary_key_columns_and_types, PostgresConnection, PostgresSnapshot,
    PostgresSnapshotHandle, TableSnapshot, TableSnapshotState,
};

pub(super) async fn start_postgres_snapshot_internal(
    connection: &mut PostgresConnection,
    tables: &[&str],
) -> Result<PostgresSnapshotHandle> {
    if tables.is_empty() {
        return Err(Error::ConfigError(
            "postgres snapshot requires at least one table".into(),
        ));
    }

    let client = {
        let state = connection.state.lock().await;
        state.client.clone().ok_or_else(|| {
            Error::StateError("postgres connection must be established before snapshot".into())
        })?
    };

    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "failed to begin postgres snapshot transaction: {error}"
            ))
        })?;

    let start_ts = now_millis();
    let setup_result = build_snapshot_setup(&client, tables, start_ts).await;

    let (snapshot, states, snapshot_watermark) = match setup_result {
        Ok(value) => value,
        Err(error) => {
            let _ = client.batch_execute("ROLLBACK").await;
            return Err(error);
        }
    };

    let handle = PostgresSnapshotHandle::new(
        connection.source_type().to_string(),
        snapshot,
        states,
        Some(client),
        true,
        snapshot_watermark,
    );

    {
        let mut state = connection.state.lock().await;
        state.snapshot_watermark = Some(handle.snapshot_watermark);
    }

    Ok(handle)
}

pub(super) async fn start_postgres_snapshot_from_checkpoint(
    connection: &mut PostgresConnection,
    tables: &[&str],
    resume_from: Option<&dyn Offset>,
) -> Result<Box<dyn SnapshotHandle>> {
    if let Some(offset) = resume_from {
        if offset.source_type() != "postgres_snapshot" {
            return Err(Error::CheckpointError(format!(
                "cannot resume postgres snapshot from source type '{}'",
                offset.source_type()
            )));
        }
    }

    let mut handle = start_postgres_snapshot_internal(connection, tables).await?;

    if let Some(offset) = resume_from {
        handle = handle.resume_from_checkpoint_payload(&offset.encode()?)?;
        let mut state = connection.state.lock().await;
        state.snapshot_watermark = Some(handle.snapshot_watermark);
    }

    Ok(Box::new(handle))
}

async fn build_snapshot_setup(
    client: &Arc<Client>,
    tables: &[&str],
    start_ts: u64,
) -> Result<(PostgresSnapshot, Vec<TableSnapshotState>, u64)> {
    let snapshot_watermark = query_current_wal_lsn(client).await?;
    let snapshot_id: String = client
        .query_one("SELECT txid_current_snapshot()::text", &[])
        .await
        .map(|row| row.get(0))
        .unwrap_or_else(|_| format!("pg-snapshot-{start_ts}-{}", tables.len()));
    let mut states = Vec::with_capacity(tables.len());

    for table in tables {
        let (schema, name) = parse_table_reference(table)?;
        let (pk_columns, pk_types) =
            query_primary_key_columns_and_types(client, &schema, &name).await?;
        if pk_columns.is_empty() {
            return Err(Error::ConfigError(format!(
                "postgres snapshot requires a primary key for resumable table '{schema}.{name}'"
            )));
        }
        let total_rows_query = format!(
            "SELECT COUNT(*)::BIGINT FROM {}",
            qualified_table_name(&schema, &name)
        );
        let total_rows = client
            .query_one(&total_rows_query, &[])
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "failed counting rows for table '{schema}.{name}': {error}"
                ))
            })?
            .get::<usize, i64>(0);

        states.push(TableSnapshotState {
            snapshot: TableSnapshot {
                table: if schema == "public" {
                    name.clone()
                } else {
                    format!("{schema}.{name}")
                },
                total_rows: u64::try_from(total_rows).map_err(|_| {
                    Error::SourceError(format!(
                        "negative row count returned for table '{schema}.{name}'"
                    ))
                })?,
                rows_processed: 0,
                cursor_position: None,
                is_complete: total_rows == 0,
            },
            rows: Vec::new(),
            next_row: 0,
            live_query: true,
            primary_key_columns: pk_columns,
            primary_key_types: pk_types,
        });
    }

    let mut snapshot = PostgresSnapshot {
        tables: states.iter().map(|state| state.snapshot.clone()).collect(),
        snapshot_id,
        snapshot_start_ts: start_ts,
        snapshot_end_ts: 0,
    };
    if states.iter().all(|state| state.snapshot.is_complete) {
        snapshot.snapshot_end_ts = start_ts;
    }

    Ok((snapshot, states, snapshot_watermark))
}
