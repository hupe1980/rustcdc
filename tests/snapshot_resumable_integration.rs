#![cfg(any(feature = "postgres", feature = "mysql"))]

use std::collections::HashMap;

use cdc_rs::source::SnapshotProgress;

const TABLE_COUNT: usize = 5;
const ROWS_PER_TABLE: usize = 100_000;
const CHUNK_SIZE: usize = 5_000;
const CHUNKS_PER_TABLE: usize = ROWS_PER_TABLE / CHUNK_SIZE;

fn table_names() -> Vec<String> {
    (0..TABLE_COUNT)
        .map(|index| format!("public.table_{index}"))
        .collect()
}

#[test]
fn snapshot_progress_resumes_from_table2_chunk5_and_skips_completed_work() {
    let tables = table_names();

    // Phase 1: process until checkpoint at table 2, chunk 5 (1-based),
    // while tables 0 and 1 are already complete.
    let mut phase1 = SnapshotProgress::new("snapshot-resumable-5x100k".into(), 1_700_000_000);
    for table in &tables {
        phase1.add_table(table.clone());
    }

    // Tables 0 and 1 complete all 100K rows.
    for table in &tables[0..2] {
        for chunk in 0..CHUNKS_PER_TABLE {
            phase1
                .record_table_chunk(
                    table,
                    CHUNK_SIZE,
                    Some(format!("{table}:cursor:{chunk}").into_bytes()),
                )
                .expect("phase1 chunk record for completed tables should succeed");
        }
        phase1
            .mark_table_complete(table)
            .expect("phase1 completion marker should succeed");
    }

    // Table 2 has processed chunks 0..4; checkpoint should resume from chunk 5.
    let table2 = &tables[2];
    for chunk in 0..5 {
        phase1
            .record_table_chunk(
                table2,
                CHUNK_SIZE,
                Some(format!("{table2}:cursor:{chunk}").into_bytes()),
            )
            .expect("phase1 chunk record for table2 should succeed");
    }

    let checkpoint_bytes = phase1.encode().expect("checkpoint encoding should succeed");

    // Phase 2 restart: load progress and continue remaining work.
    let mut resumed =
        SnapshotProgress::decode(&checkpoint_bytes).expect("checkpoint decoding should succeed");

    // Validate restored state before resuming.
    for table in &tables[0..2] {
        let progress = resumed
            .get_table_progress(table)
            .expect("restored progress should include completed tables");
        assert!(progress.is_complete);
        assert_eq!(progress.chunk_index, CHUNKS_PER_TABLE as u64);
        assert_eq!(progress.row_count, ROWS_PER_TABLE as u64);
    }

    let restored_table2 = resumed
        .get_table_progress(table2)
        .expect("restored progress should include table2");
    assert!(!restored_table2.is_complete);
    assert_eq!(restored_table2.chunk_index, 5);
    assert_eq!(restored_table2.row_count, 25_000);

    for table in &tables[3..5] {
        let progress = resumed
            .get_table_progress(table)
            .expect("restored progress should include untouched tables");
        assert!(!progress.is_complete);
        assert_eq!(progress.chunk_index, 0);
        assert_eq!(progress.row_count, 0);
    }

    // Resume accounting: track only new work done after restart.
    let mut resumed_chunks: HashMap<String, usize> = HashMap::new();

    // Continue table2 from chunk 5 through chunk 19.
    for chunk in 5..CHUNKS_PER_TABLE {
        resumed
            .record_table_chunk(
                table2,
                CHUNK_SIZE,
                Some(format!("{table2}:cursor:{chunk}").into_bytes()),
            )
            .expect("resumed chunk record for table2 should succeed");
        *resumed_chunks.entry(table2.clone()).or_insert(0) += 1;
    }
    resumed
        .mark_table_complete(table2)
        .expect("resumed completion marker for table2 should succeed");

    // Process remaining tables (3 and 4) fully after restart.
    for table in &tables[3..5] {
        for chunk in 0..CHUNKS_PER_TABLE {
            resumed
                .record_table_chunk(
                    table,
                    CHUNK_SIZE,
                    Some(format!("{table}:cursor:{chunk}").into_bytes()),
                )
                .expect("resumed chunk record for remaining tables should succeed");
            *resumed_chunks.entry(table.clone()).or_insert(0) += 1;
        }
        resumed
            .mark_table_complete(table)
            .expect("resumed completion marker for remaining tables should succeed");
    }

    // Completed tables 0 and 1 must remain skipped after restart.
    assert_eq!(resumed_chunks.get(&tables[0]).copied().unwrap_or(0), 0);
    assert_eq!(resumed_chunks.get(&tables[1]).copied().unwrap_or(0), 0);

    // Table2 must continue from chunk5 (15 chunks remaining out of 20).
    assert_eq!(resumed_chunks.get(table2).copied().unwrap_or(0), 15);

    // Tables3-4 must run full 20 chunks each.
    assert_eq!(resumed_chunks.get(&tables[3]).copied().unwrap_or(0), 20);
    assert_eq!(resumed_chunks.get(&tables[4]).copied().unwrap_or(0), 20);

    // Final state validation.
    assert!(resumed.is_all_complete());
    assert_eq!(resumed.completed_tables(), TABLE_COUNT);
    assert!(resumed.get_pending_tables().is_empty());
    assert_eq!(
        resumed.total_rows_processed(),
        (TABLE_COUNT * ROWS_PER_TABLE) as u64
    );

    for table in &tables {
        let progress = resumed
            .get_table_progress(table)
            .expect("final progress should include every table");
        assert!(progress.is_complete);
        assert_eq!(progress.chunk_index, CHUNKS_PER_TABLE as u64);
        assert_eq!(progress.row_count, ROWS_PER_TABLE as u64);
    }
}
