use super::pg_error::PgError;
use std::sync::Arc;

use tokio_postgres::Client;

use crate::{
    core::{Error, Offset, Result},
    source::{SnapshotHandle, Source},
};

use super::{
    PostgresConnection, PostgresSnapshot, PostgresSnapshotHandle, TableSnapshot,
    TableSnapshotState, now_millis, parse_table_reference, qualified_table_name,
    query_current_wal_lsn, query_primary_key_columns_and_types,
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

    // The anchor exports the snapshot and reads the watermark in one statement; every reader
    // transaction, the first and any reopened after a failed chunk, imports that snapshot.
    let mut anchor = match SnapshotAnchor::open(connection).await {
        Ok(anchor) => Some(anchor),
        Err(error) => {
            tracing::warn!(
                target: "rustcdc::source::postgres",
                error = %error,
                "postgres snapshot: no anchor connection; the snapshot reads from one point in \
                 time only until a chunk fails, after which a row written since it began may \
                 arrive both as a snapshot read and as a stream change",
            );
            None
        }
    };
    begin_snapshot_transaction(&client, &mut anchor).await?;

    let start_ts = now_millis();
    let watermark = anchor.as_ref().map(|anchor| anchor.watermark);
    let setup_result = build_snapshot_setup(&client, tables, start_ts, watermark).await;

    let (snapshot, states, snapshot_watermark) = match setup_result {
        Ok(value) => value,
        Err(error) => {
            let _ = client.batch_execute("ROLLBACK").await;
            return Err(error);
        }
    };

    let mut handle = PostgresSnapshotHandle::new(
        connection.source_type().to_string(),
        snapshot,
        states,
        Some(client),
        true,
        snapshot_watermark,
    );
    handle.anchor = anchor;

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
    if let Some(offset) = resume_from
        && offset.source_type() != "postgres_snapshot"
    {
        return Err(Error::CheckpointError(format!(
            "cannot resume postgres snapshot from source type '{}'",
            offset.source_type()
        )));
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
    anchor_watermark: Option<u64>,
) -> Result<(PostgresSnapshot, Vec<TableSnapshotState>, u64)> {
    let snapshot_watermark = match anchor_watermark {
        Some(watermark) => watermark,
        None => query_current_wal_lsn(client).await?,
    };
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
        // The same projection the stream uses, so one table is described identically in
        // both phases. A snapshot has no publication to enumerate — a snapshot-only
        // deployment need not have one — so this reads per table rather than per
        // publication.
        let catalog_columns =
            super::query::query_table_column_types(client, &schema, &name).await?;
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
            .map_err(PgError::from)
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
            catalog_columns,
            shape: None,
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

/// The connection holding a snapshot open for the snapshot's readers to import.
///
/// Its transaction exports the snapshot and does nothing else, so it cannot fail part-way the way
/// a reader can. Dropping it closes the connection, which ends the transaction.
pub(super) struct SnapshotAnchor {
    _client: Client,
    task: tokio::task::JoinHandle<()>,
    snapshot_id: String,
    pub(super) watermark: u64,
}

impl SnapshotAnchor {
    async fn open(connection: &PostgresConnection) -> Result<Self> {
        let (client, task) = connection.open_client().await?;
        let anchor = async {
            client
                .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .await
                .map_err(PgError::from)?;
            // One statement: the watermark and the snapshot are taken together.
            let row = client
                .query_one(
                    "SELECT pg_current_wal_lsn()::text, pg_export_snapshot()",
                    &[],
                )
                .await
                .map_err(PgError::from)?;
            Ok::<_, PgError>((row.get::<_, String>(0), row.get::<_, String>(1)))
        }
        .await;
        let (lsn, snapshot_id) = match anchor {
            Ok(values) => values,
            Err(error) => {
                task.abort();
                return Err(Error::SourceError(format!(
                    "failed to export a postgres snapshot: {error}"
                )));
            }
        };
        // Interpolated into `SET TRANSACTION SNAPSHOT`, which takes no parameter.
        if snapshot_id.is_empty()
            || !snapshot_id
                .chars()
                .all(|c| c.is_ascii_hexdigit() || c == '-')
        {
            task.abort();
            return Err(Error::SourceError(format!(
                "postgres exported an unexpected snapshot id '{snapshot_id}'"
            )));
        }
        Ok(Self {
            _client: client,
            task,
            snapshot_id,
            watermark: super::parser::parse_pg_lsn(&lsn)?,
        })
    }
}

impl Drop for SnapshotAnchor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Begin a reader transaction on the anchor's snapshot, or on a fresh one when there is no anchor
/// or it is gone — in which case the anchor is dropped and the WARN says what that costs.
pub(super) async fn begin_snapshot_transaction(
    client: &Client,
    anchor: &mut Option<SnapshotAnchor>,
) -> Result<()> {
    let begin = |error: tokio_postgres::Error| {
        Error::SourceError(format!(
            "failed to begin postgres snapshot transaction: {}",
            PgError::from(error)
        ))
    };
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .map_err(begin)?;
    let Some(held) = anchor.as_ref() else {
        return Ok(());
    };
    let import = format!("SET TRANSACTION SNAPSHOT '{}'", held.snapshot_id);
    if let Err(error) = client.batch_execute(&import).await {
        tracing::warn!(
            target: "rustcdc::source::postgres",
            error = %PgError::from(error),
            "postgres snapshot: its anchor connection is gone; continuing in a new transaction, so \
             the rest of this snapshot reads a later point in time and a row written since it \
             began may arrive both as a snapshot read and as a stream change",
        );
        *anchor = None;
        let _ = client.batch_execute("ROLLBACK").await;
        client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(begin)?;
    }
    Ok(())
}
