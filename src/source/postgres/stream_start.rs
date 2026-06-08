use std::time::Duration;

use crate::{
    core::{Error, Offset, Result},
    source::{Source, StreamHandle},
};

use super::decoder::LivePgOutputMessageProvider;
use super::{
    decode_stream_resume_lsn, query_current_wal_lsn, reconcile_stream_resume_lsn_with_retry,
    PostgresConnection, PostgresStream, PostgresStreamHandle, StreamState,
};

pub(super) async fn start_postgres_stream(
    connection: &mut PostgresConnection,
    resume_from: Option<&dyn Offset>,
) -> Result<Box<dyn StreamHandle>> {
    let client = {
        let state = connection.state.lock().await;
        state.client.clone().ok_or_else(|| {
            Error::StateError("postgres connection must be established before stream".into())
        })?
    };

    let mut stream = PostgresStream {
        slot_name: connection.config.replication_slot_name.clone(),
        publication_name: connection.config.publication_name.clone(),
        lsn_position: 0,
        replication_status: StreamState::Starting,
    };

    if let Some(offset) = resume_from {
        stream.lsn_position =
            decode_stream_resume_lsn(connection.source_type(), &stream.slot_name, offset)?;
        stream.lsn_position = reconcile_stream_resume_lsn_with_retry(
            &client,
            stream.lsn_position,
            &stream.slot_name,
            5,
            Duration::from_millis(250),
        )
        .await?;
    } else {
        stream.lsn_position = query_current_wal_lsn(&client).await?;
    }

    stream.replication_status = StreamState::Streaming;
    {
        let mut state = connection.state.lock().await;
        state.stream_start_watermark = Some(stream.lsn_position);
    }
    // When resuming from a checkpoint the checkpoint LSN is the last durably
    // confirmed WAL position, so it is the correct lower bound for skip-guard.
    // When starting fresh (no checkpoint) stream.lsn_position was set to
    // query_current_wal_lsn() — a value HIGHER than the slot's current
    // confirmed_flush_lsn.  If we pass that high value as confirmed_lsn, every
    // confirm_lsn call for historical events (which have lower LSNs) hits the
    // `lsn <= self.confirmed_lsn` guard and returns without advancing the slot,
    // causing an infinite replay loop.  Use 0 on fresh start so the slot is
    // advanced as historical batches are acknowledged.
    let initial_confirmed_lsn = if resume_from.is_some() {
        stream.lsn_position
    } else {
        0
    };
    let provider = Box::new(LivePgOutputMessageProvider {
        client,
        slot_name: stream.slot_name.clone(),
        publication_name: stream.publication_name.clone(),
        confirmed_lsn: initial_confirmed_lsn,
    });
    Ok(Box::new(PostgresStreamHandle::new(
        connection.source_type().to_string(),
        stream,
        provider,
        connection.max_events_per_poll,
        connection.stream_poll_interval_ms,
        connection.slot_idle_advance_interval_ms,
        connection.config.table_include_list.clone(),
        connection.config.table_exclude_list.clone(),
    )))
}
