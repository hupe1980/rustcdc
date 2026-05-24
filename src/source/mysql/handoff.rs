use crate::{
    checkpoint::MysqlOffset,
    core::{Error, Result},
    source::{HandoffResult, SnapshotHandle, StreamHandle},
};

use super::{
    parser::parse_mysql_source_offset,
    query::dedup_overlap_events_by_pk,
    state::{compare_binlog_position, MysqlHandoff},
};
use crate::source::helpers::now_millis;

pub(super) async fn mysql_handoff_result(
    snapshot: &mut dyn SnapshotHandle,
    stream: &mut dyn StreamHandle,
    snapshot_wm: MysqlOffset,
    stream_wm: MysqlOffset,
) -> Result<HandoffResult> {
    let handoff = MysqlHandoff {
        snapshot_binlog_file: snapshot_wm.binlog_file,
        snapshot_binlog_pos: snapshot_wm.binlog_pos,
        snapshot_gtid: snapshot_wm.gtid,
        stream_start_binlog_file: stream_wm.binlog_file,
        stream_start_binlog_pos: stream_wm.binlog_pos,
        stream_start_gtid: stream_wm.gtid,
    };

    // The stream must start at or before the snapshot's binlog position so that
    // every change made after the snapshot was opened is visible in the stream.
    if !handoff.has_no_gap() {
        return Err(Error::SourceError(format!(
            "mysql handoff invariant violated: stream starts at {}:{} which is after snapshot watermark {}:{} - events would be lost",
            handoff.stream_start_binlog_file,
            handoff.stream_start_binlog_pos,
            handoff.snapshot_binlog_file,
            handoff.snapshot_binlog_pos,
        )));
    }

    // Finish the snapshot (commits the consistent-read transaction).
    let snapshot_end = snapshot.finish().await?.snapshot_end_ts;

    // Read overlap events once and deduplicate by primary key (last writer wins).
    // Then requeue retained events so downstream consumption order is preserved.
    let mut overlap_events = Vec::new();
    let mut non_overlap_events = Vec::new();
    let mut overlap_phase_complete = false;
    let mut polls = 0_usize;

    while !overlap_phase_complete && polls < 8 {
        polls += 1;
        let batch = stream.next_events(0).await?;
        if batch.is_empty() {
            break;
        }

        for event in batch {
            let is_overlap = parse_mysql_source_offset(&event.source.offset)
                .map(|(file, pos)| {
                    compare_binlog_position(
                        file,
                        pos,
                        &handoff.snapshot_binlog_file,
                        handoff.snapshot_binlog_pos,
                    )
                    .is_le()
                })
                .unwrap_or(false);

            if !overlap_phase_complete && is_overlap {
                overlap_events.push(event);
            } else {
                overlap_phase_complete = true;
                non_overlap_events.push(event);
            }
        }
    }

    let (deduped_overlap, overlap_duplicates) = dedup_overlap_events_by_pk(overlap_events);
    let mut replay_events = deduped_overlap;
    replay_events.extend(non_overlap_events);
    if !replay_events.is_empty() {
        stream.requeue_events(replay_events).await?;
    }

    if overlap_duplicates > 0 {
        tracing::info!(
            target: "cdc_rs::source::mysql",
            overlap_duplicates,
            snapshot_binlog_file = %handoff.snapshot_binlog_file,
            snapshot_binlog_pos = handoff.snapshot_binlog_pos,
            "mysql handoff deduplicated overlap events by primary key"
        );
    }

    Ok(HandoffResult {
        snapshot_end_ts: Some(snapshot_end),
        stream_start_ts: Some(now_millis()),
        overlap_events_dropped: overlap_duplicates,
        stream_watermark_gap: None,
    })
}
