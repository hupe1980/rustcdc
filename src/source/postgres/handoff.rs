use crate::{
    core::{Error, Result},
    source::HandoffResult,
};

use super::{now_millis, PostgresHandoff};

pub(super) fn postgres_handoff_result(
    snapshot_end_ts: Option<u64>,
    snapshot_watermark: Option<u64>,
    stream_watermark: Option<u64>,
) -> Result<HandoffResult> {
    match (snapshot_end_ts, snapshot_watermark, stream_watermark) {
        (Some(snapshot_end_ts), Some(snapshot_watermark), Some(stream_watermark)) => {
            let stream_watermark_gap =
                postgres_handoff_stream_watermark_gap(snapshot_watermark, stream_watermark)?;
            Ok(HandoffResult {
                snapshot_end_ts: Some(snapshot_end_ts),
                stream_start_ts: Some(now_millis()),
                overlap_events_dropped: 0,
                stream_watermark_gap: Some(stream_watermark_gap),
            })
        }
        (Some(snapshot_end_ts), Some(_snapshot_watermark), None) => Ok(HandoffResult {
            snapshot_end_ts: Some(snapshot_end_ts),
            stream_start_ts: None,
            overlap_events_dropped: 0,
            stream_watermark_gap: None,
        }),
        (None, None, Some(_stream_watermark)) => Ok(HandoffResult {
            snapshot_end_ts: None,
            stream_start_ts: Some(now_millis()),
            overlap_events_dropped: 0,
            stream_watermark_gap: None,
        }),
        (Some(_), None, None) => Err(Error::StateError(
            "postgres handoff requires at least one watermark and a snapshot end timestamp when completing snapshot-to-stream handoff".into(),
        )),
        (Some(_), None, Some(_)) => Err(Error::StateError(
            "postgres handoff requires a snapshot watermark when a stream watermark is present"
                .into(),
        )),
        (None, Some(_), Some(_)) => Err(Error::StateError(
            "postgres handoff requires snapshot_end_ts when both watermarks are present".into(),
        )),
        (None, Some(_), None) | (None, None, None) => Ok(HandoffResult {
            snapshot_end_ts: None,
            stream_start_ts: None,
            overlap_events_dropped: 0,
            stream_watermark_gap: None,
        }),
    }
}

pub(super) fn postgres_handoff_stream_watermark_gap(
    snapshot_watermark: u64,
    stream_watermark: u64,
) -> Result<u64> {
    if stream_watermark < snapshot_watermark {
        return Err(Error::SourceError(format!(
            "postgres handoff invariant violated: stream watermark {stream_watermark} is behind snapshot watermark {snapshot_watermark}"
        )));
    }
    Ok(stream_watermark.saturating_sub(snapshot_watermark))
}

impl PostgresHandoff {
    pub(super) fn stream_watermark_gap(&self) -> u64 {
        self.stream_watermark
            .saturating_sub(self.snapshot_watermark)
    }
}
