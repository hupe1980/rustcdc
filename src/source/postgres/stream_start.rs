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
    let provider = Box::new(LivePgOutputMessageProvider {
        client,
        slot_name: stream.slot_name.clone(),
        publication_name: stream.publication_name.clone(),
        confirmed_lsn: stream.lsn_position,
    });
    Ok(Box::new(PostgresStreamHandle::new(
        connection.source_type().to_string(),
        stream,
        provider,
        connection.max_events_per_poll,
        connection.stream_poll_interval_ms,
    )))
}
