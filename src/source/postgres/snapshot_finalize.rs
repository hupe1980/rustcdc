use crate::{
    checkpoint::GenericOffset,
    core::{Error, Result},
};

use super::{PostgresSnapshotHandle, SnapshotCheckpointState};

pub(super) async fn checkpoint_postgres_snapshot(
    handle: &PostgresSnapshotHandle,
    checkpoint: &mut dyn crate::checkpoint::Checkpoint,
    committed_event_count: u64,
) -> Result<()> {
    let payload = SnapshotCheckpointState {
        snapshot_id: handle.snapshot.snapshot_id.clone(),
        snapshot_start_ts: handle.snapshot.snapshot_start_ts,
        snapshot_end_ts: handle.snapshot.snapshot_end_ts,
        snapshot_watermark: handle.snapshot_watermark,
        current_table: handle.current_table,
        next_chunk_index: handle.next_chunk_index,
        tables: handle.snapshot.tables.clone(),
    };

    let encoded = serde_json::to_vec(&payload)?;
    let offset = GenericOffset::new("postgres_snapshot", encoded);
    checkpoint.save(&offset, committed_event_count).await
}

pub(super) async fn finish_postgres_snapshot(
    handle: &mut PostgresSnapshotHandle,
) -> Result<crate::source::SnapshotEnd> {
    handle.sync_snapshot_tables();
    if handle.snapshot.snapshot_end_ts == 0 {
        handle.snapshot.snapshot_end_ts = crate::source::helpers::now_millis();
    }

    let total_processed: u64 = handle
        .snapshot
        .tables
        .iter()
        .map(|table| table.rows_processed)
        .sum();
    if total_processed != handle.emitted_rows {
        return Err(Error::SourceError(format!(
            "snapshot consistency check failed: emitted_rows={} rows_processed={total_processed}",
            handle.emitted_rows
        )));
    }
    if !handle.has_live_query_tables() && handle.total_expected_rows() != total_processed {
        return Err(Error::SourceError(
            "snapshot consistency check failed: not all rows were emitted".into(),
        ));
    }
    if handle.emitted_in_run > handle.emitted_rows {
        return Err(Error::StateError(
            "snapshot consistency check failed: emitted_in_run exceeds emitted_rows".into(),
        ));
    }

    if handle.transaction_open {
        let client = handle.client.as_ref().ok_or_else(|| {
            Error::StateError(
                "postgres snapshot transaction is open but snapshot client is unavailable".into(),
            )
        })?;
        client.batch_execute("COMMIT").await.map_err(|error| {
            Error::SourceError(format!(
                "failed to commit postgres snapshot transaction: {error}"
            ))
        })?;
        handle.transaction_open = false;
    }

    Ok(crate::source::SnapshotEnd {
        snapshot_end_ts: handle.snapshot.snapshot_end_ts,
    })
}
