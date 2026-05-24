use crate::{
    checkpoint::GenericOffset,
    core::{Error, Result},
};

use super::{SqlServerSnapshotCheckpointState, SqlServerSnapshotHandle};

pub(super) async fn checkpoint_sqlserver_snapshot(
    handle: &SqlServerSnapshotHandle,
    checkpoint: &mut dyn crate::checkpoint::Checkpoint,
    committed_event_count: u64,
) -> Result<()> {
    let payload = SqlServerSnapshotCheckpointState {
        snapshot_id: handle.snapshot.snapshot_id.clone(),
        lsn_start: handle.snapshot.lsn_start,
        current_table: handle.current_table,
        next_chunk_index: handle.next_chunk_index,
        tables: handle
            .tables
            .iter()
            .map(|table| table.snapshot.clone())
            .collect(),
    };
    let offset = GenericOffset::new("sqlserver_snapshot", serde_json::to_vec(&payload)?);
    checkpoint.save(&offset, committed_event_count).await
}

pub(super) async fn finish_sqlserver_snapshot(
    handle: &mut SqlServerSnapshotHandle,
) -> Result<crate::source::SnapshotEnd> {
    let processed: u64 = handle
        .tables
        .iter()
        .map(|table| table.snapshot.rows_processed)
        .sum();
    if processed != handle.emitted_rows {
        return Err(Error::SourceError(format!(
            "sqlserver snapshot consistency check failed: emitted_rows={} rows_processed={processed}",
            handle.emitted_rows
        )));
    }
    if processed != handle.total_expected_rows() {
        return Err(Error::SourceError(
            "sqlserver snapshot consistency check failed: not all rows were emitted".into(),
        ));
    }

    if handle.transaction_open {
        let client = handle.client.as_ref().ok_or_else(|| {
            Error::StateError("sqlserver snapshot handle is missing an active transaction client".into())
        })?;
        let mut client = client.lock().await;
        client
            .execute("COMMIT TRANSACTION", &[])
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "sqlserver snapshot transaction commit failed: {error}"
                ))
            })?;
        handle.transaction_open = false;
    }

    Ok(crate::source::SnapshotEnd {
        snapshot_end_ts: crate::source::helpers::now_millis(),
    })
}
