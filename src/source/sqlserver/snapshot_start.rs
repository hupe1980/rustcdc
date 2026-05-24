use crate::{
    core::{Error, Offset, Result},
    source::SnapshotHandle,
};

use super::{
    lsn_hex_to_bytes, now_millis, query, SqlServerConnection, SqlServerSnapshot,
    SqlServerSnapshotHandle,
};

pub(super) async fn start_sqlserver_snapshot_internal(
    connection: &mut SqlServerConnection,
    tables: &[&str],
    resume_from: Option<&dyn Offset>,
) -> Result<Box<dyn SnapshotHandle>> {
    if let Some(offset) = resume_from {
        if offset.source_type() != "sqlserver_snapshot" {
            return Err(Error::CheckpointError(format!(
                "cannot resume sqlserver snapshot from source type '{}'",
                offset.source_type()
            )));
        }
    }

    connection.ensure_connected().await?;

    let lsn_start_hex = connection.query_max_lsn_hex().await?;
    let lsn_start = lsn_hex_to_bytes(&lsn_start_hex)?;
    let mut snapshot_client = query::connect_client(&connection.config).await?;
    let transaction_open =
        SqlServerConnection::begin_snapshot_transaction(&mut snapshot_client).await?;

    let table_states = match connection
        .load_snapshot_tables(&mut snapshot_client, tables)
        .await
    {
        Ok(states) => states,
        Err(error) => {
            if transaction_open {
                let _ = snapshot_client.execute("ROLLBACK TRANSACTION", &[]).await;
            }
            return Err(error);
        }
    };

    let snapshot = SqlServerSnapshot {
        lsn_start,
        snapshot_id: format!("sqlserver-snapshot-{}", now_millis()),
        tables: table_states
            .iter()
            .map(|state| state.snapshot.clone())
            .collect(),
    };

    let mut handle = SqlServerSnapshotHandle::new(
        snapshot,
        table_states,
        Some(snapshot_client),
        transaction_open,
    );
    let mut effective_lsn_start = lsn_start;

    if let Some(offset) = resume_from {
        handle = handle.resume_from_checkpoint_payload(&offset.encode()?)?;
        effective_lsn_start = handle.snapshot.lsn_start;
    }

    {
        let mut state = connection.state.lock().await;
        state.snapshot_lsn_start = Some(effective_lsn_start);
    }

    Ok(Box::new(handle))
}

pub(super) async fn start_sqlserver_snapshot_from_checkpoint(
    connection: &mut SqlServerConnection,
    tables: &[&str],
    resume_from: Option<&dyn Offset>,
) -> Result<Box<dyn SnapshotHandle>> {
    start_sqlserver_snapshot_internal(connection, tables, resume_from).await
}
