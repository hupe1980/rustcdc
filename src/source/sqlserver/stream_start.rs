use crate::{
    core::{Error, Offset, Result},
    source::{Source, StreamHandle},
};

use super::{
    compare_lsn, lsn_bytes_to_hex, lsn_hex_to_bytes, sqlserver_resume_lsn_from_offset_bytes,
    SqlServerConnection, SqlServerStream, SqlServerStreamHandle,
};

pub(super) async fn start_sqlserver_stream(
    connection: &mut SqlServerConnection,
    resume_from: Option<&dyn Offset>,
) -> Result<Box<dyn StreamHandle>> {
    connection.ensure_connected().await?;

    let metas = connection.load_capture_metas().await?;
    let mut min_lsn: Option<[u8; 10]> = None;
    for meta in &metas {
        let min_hex = connection.query_min_lsn_hex(&meta.capture_instance).await?;
        let min_bytes = lsn_hex_to_bytes(&min_hex)?;
        if min_lsn
            .as_ref()
            .map(|current| compare_lsn(&min_bytes, current).is_lt())
            .unwrap_or(true)
        {
            min_lsn = Some(min_bytes);
        }
    }
    let min_lsn = min_lsn.ok_or_else(|| {
        Error::SourceError("sqlserver could not determine CDC minimum LSN".into())
    })?;

    let max_lsn_hex = connection.query_max_lsn_hex().await?;
    let mut max_lsn = lsn_hex_to_bytes(&max_lsn_hex)?;

    let start_lsn = if let Some(offset) = resume_from {
        if offset.source_type() != connection.source_type() {
            return Err(Error::CheckpointError(format!(
                "cannot resume sqlserver stream from source type '{}'",
                offset.source_type()
            )));
        }

        let encoded = offset.encode()?;
        let resume = sqlserver_resume_lsn_from_offset_bytes(encoded.as_slice())?;
        if compare_lsn(&resume, &min_lsn).is_lt() {
            return Err(Error::CheckpointError(format!(
                "sqlserver checkpoint LSN {} is older than CDC minimum {}; CDC cleanup removed required rows",
                lsn_bytes_to_hex(&resume),
                lsn_bytes_to_hex(&min_lsn)
            )));
        }
        resume
    } else {
        min_lsn
    };
    if compare_lsn(&max_lsn, &start_lsn).is_lt() {
        max_lsn = start_lsn;
    }
    {
        let mut state = connection.state.lock().await;
        state.stream_lsn_start = Some(start_lsn);
    }

    let stream = SqlServerStream {
        lsn_start: start_lsn,
        lsn_end: max_lsn,
        change_tables: metas
            .iter()
            .map(|meta| meta.capture_instance.clone())
            .collect(),
        poll_interval_ms: connection.stream_poll_interval_ms,
    };

    Ok(Box::new(SqlServerStreamHandle {
        config: connection.config.clone(),
        stream,
        metas,
        events_polled: 0,
        requeued_events: Vec::new(),
        max_events_per_poll: connection.max_events_per_poll,
        pending_update_afters: ahash::AHashMap::new(),
    }))
}
