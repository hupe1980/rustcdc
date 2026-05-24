use std::sync::Arc;

use cdc_rs::source::{ParallelSnapshotConfig, ParallelSnapshotState, SnapshotProgress};

const TABLE_COUNT: usize = 10;
const ROWS_PER_TABLE: usize = 100_000;
const CHUNK_SIZE: usize = 5_000;
const CRASH_AFTER_CHUNKS: usize = 5;
const WORKERS: usize = 8;

fn table_names() -> Vec<String> {
    (0..TABLE_COUNT)
        .map(|idx| format!("public.table_{idx}"))
        .collect()
}

fn worker_tables(tables: &[String], worker: usize) -> Vec<String> {
    tables
        .iter()
        .enumerate()
        .filter(|(idx, _)| idx % WORKERS == worker)
        .map(|(_, table)| table.clone())
        .collect()
}

#[test]
fn parallel_snapshot_stress_resume_after_chunk_5_for_10_tables() {
    let tables = table_names();
    let total_chunks = ROWS_PER_TABLE / CHUNK_SIZE;

    let state = Arc::new(ParallelSnapshotState::new(
        "parallel-stress".into(),
        1,
        tables.clone(),
        ParallelSnapshotConfig {
            max_parallel_tables: WORKERS,
            chunk_size: CHUNK_SIZE,
        },
    ));

    // Phase 1: process only first 5 chunks per table, then simulate a crash.
    let mut first_phase = Vec::new();
    for worker in 0..WORKERS {
        let state_ref = state.clone();
        let assigned = worker_tables(&tables, worker);
        first_phase.push(std::thread::spawn(move || {
            for table in assigned {
                for chunk in 0..CRASH_AFTER_CHUNKS {
                    let cursor = Some(format!("{table}:{chunk}").into_bytes());
                    state_ref
                        .record_chunk_progress(&table, CHUNK_SIZE, cursor)
                        .expect("phase 1 progress update should succeed");
                }
            }
        }));
    }
    for handle in first_phase {
        handle.join().expect("phase 1 thread should join");
    }

    let before_crash_progress = state
        .get_progress()
        .expect("progress should be readable before crash");
    let checkpoint_bytes = before_crash_progress
        .encode()
        .expect("progress checkpoint encode should succeed");

    let restored = SnapshotProgress::decode(&checkpoint_bytes)
        .expect("progress checkpoint decode should succeed");

    for table in &tables {
        let progress = restored
            .get_table_progress(table)
            .expect("table progress should exist after restore");
        assert_eq!(progress.chunk_index, CRASH_AFTER_CHUNKS as u64);
        assert_eq!(progress.row_count, (CRASH_AFTER_CHUNKS * CHUNK_SIZE) as u64);
        assert!(!progress.is_complete);
    }

    // Phase 2 (restart): hydrate a new state from checkpointed chunk progress,
    // then continue remaining chunks with 8 workers.
    let resumed = Arc::new(ParallelSnapshotState::new(
        restored.snapshot_id.clone(),
        restored.created_at,
        tables.clone(),
        ParallelSnapshotConfig {
            max_parallel_tables: WORKERS,
            chunk_size: CHUNK_SIZE,
        },
    ));

    for table in &tables {
        for _ in 0..CRASH_AFTER_CHUNKS {
            resumed
                .record_chunk_progress(table, CHUNK_SIZE, None)
                .expect("rehydration progress update should succeed");
        }
    }

    let mut second_phase = Vec::new();
    for worker in 0..WORKERS {
        let resumed_ref = resumed.clone();
        let assigned = worker_tables(&tables, worker);
        second_phase.push(std::thread::spawn(move || {
            for table in assigned {
                for chunk in CRASH_AFTER_CHUNKS..total_chunks {
                    let cursor = Some(format!("{table}:{chunk}").into_bytes());
                    resumed_ref
                        .record_chunk_progress(&table, CHUNK_SIZE, cursor)
                        .expect("phase 2 progress update should succeed");
                }
                resumed_ref
                    .mark_table_complete(&table)
                    .expect("table completion should succeed");
            }
        }));
    }

    for handle in second_phase {
        handle.join().expect("phase 2 thread should join");
    }

    assert!(resumed.all_complete().expect("all_complete should succeed"));
    assert_eq!(
        resumed.completed_tables().expect("completed_tables"),
        TABLE_COUNT
    );
    assert_eq!(resumed.progress_percent().expect("progress_percent"), 100);
    assert!(resumed
        .get_pending_tables()
        .expect("pending tables")
        .is_empty());
    assert_eq!(
        resumed.total_rows_processed().expect("total rows"),
        (TABLE_COUNT * ROWS_PER_TABLE) as u64
    );
}
