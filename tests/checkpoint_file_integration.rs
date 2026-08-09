use rustcdc::checkpoint::{Checkpoint, FileCheckpoint, PostgresOffset};

#[tokio::test]
async fn file_checkpoint_survives_checkpoint_store_restart() {
    let temp = tempfile::tempdir().expect("tempdir");
    let directory = temp.path().to_path_buf();

    let mut writer = FileCheckpoint::new(directory.clone());
    let offset = PostgresOffset {
        lsn: 9001,
        slot_name: "phase1_slot".to_string(),
        incremental_snapshot: None,
    };

    writer.save(&offset, 128).await.expect("save checkpoint");

    // Simulate a process restart by releasing the writer, then read the record back.
    // A read-only handle would work without the drop — it takes no owner lease — but
    // dropping first is what a restart actually looks like.
    drop(writer);
    let reader = FileCheckpoint::read_only(directory);
    let loaded = reader
        .load()
        .await
        .expect("load checkpoint")
        .expect("existing checkpoint");

    assert_eq!(loaded.source_type(), "postgres");
    assert_eq!(reader.get_committed_count().await.expect("load count"), 128);
}

/// An incremental snapshot must resume at its chunk boundary, not from row zero.
///
/// This is the end-to-end shape of the defect that made a restart re-read every
/// configured table in full: the chunk cursors lived only in memory, so the
/// checkpoint carried the stream position and nothing else. A restart then began
/// each table again at row zero — an uncontrolled duplicate flood proportional to
/// the dataset rather than to the crash window, repeating on every restart until a
/// snapshot happened to finish inside one process lifetime.
///
/// The assertion that matters is the *round trip through a real durable checkpoint*:
/// atomic write, fsync, checksum, reload, decode. A unit test on the state struct
/// would not have caught a cursor that never reached the file.
#[tokio::test]
async fn incremental_snapshot_cursors_survive_a_checkpoint_round_trip() {
    use rustcdc::source::{
        incremental_snapshot_state_from_offset, IncrementalSnapshotState,
        IncrementalSnapshotTableState,
    };

    let temp = tempfile::tempdir().expect("tempdir");
    let directory = temp.path().to_path_buf();

    let state = IncrementalSnapshotState {
        paused: false,
        stopped: false,
        generation: 0,
        snapshot_id: "incremental-1754300000000".to_string(),
        tables: vec![
            IncrementalSnapshotTableState {
                table: "public.users".to_string(),
                pk_cursor: Some(vec![serde_json::json!("50000")]),
                is_complete: false,
                chunks_emitted: 10,
                rows_emitted: 50_000,
                condition: None,
            },
            IncrementalSnapshotTableState {
                table: "public.orders".to_string(),
                pk_cursor: None,
                is_complete: true,
                chunks_emitted: 3,
                rows_emitted: 12_000,
                condition: None,
            },
        ],
    };

    let mut writer = FileCheckpoint::new(directory.clone());
    writer
        .save(
            &PostgresOffset::new(9_001, "cdc_slot").with_incremental_snapshot(Some(state.clone())),
            62_000,
        )
        .await
        .expect("save checkpoint carrying incremental-snapshot progress");
    drop(writer);

    let reader = FileCheckpoint::read_only(directory);
    let loaded = reader
        .load()
        .await
        .expect("load checkpoint")
        .expect("existing checkpoint");

    let recovered = incremental_snapshot_state_from_offset(Some(loaded.as_ref()))
        .expect("the chunk cursors must survive the checkpoint round trip");

    assert_eq!(
        recovered, state,
        "resume state must round-trip byte-for-byte; a lost cursor silently re-reads \
         the table from row zero"
    );

    let users = recovered
        .table("public.users")
        .expect("per-table progress must be addressable by name");
    assert_eq!(
        users.pk_cursor,
        Some(vec![serde_json::json!("50000")]),
        "the keyset cursor is what makes the resume bounded by chunk_size rather than \
         by table size"
    );
    assert!(
        !users.is_complete,
        "an in-flight table must resume, not be skipped"
    );
    assert!(
        recovered
            .table("public.orders")
            .expect("completed table must still be recorded")
            .is_complete,
        "a completed table must not be re-snapshotted"
    );

    // The stream position must still be readable and unchanged — the cursors ride
    // along with it, they do not displace it.
    let payload: serde_json::Value =
        serde_json::from_slice(&loaded.encode().expect("encode")).expect("offset json");
    assert_eq!(payload["lsn"], 9_001);
    assert_eq!(payload["slot_name"], "cdc_slot");
}

/// An offset written before the snapshot began carries no state, and that is not an error.
#[tokio::test]
async fn a_checkpoint_without_snapshot_progress_is_a_clean_fresh_start() {
    use rustcdc::source::incremental_snapshot_state_from_offset;

    let temp = tempfile::tempdir().expect("tempdir");
    let directory = temp.path().to_path_buf();

    let mut writer = FileCheckpoint::new(directory.clone());
    writer
        .save(&PostgresOffset::new(42, "cdc_slot"), 1)
        .await
        .expect("save");
    drop(writer);

    let reader = FileCheckpoint::read_only(directory);
    let loaded = reader.load().await.expect("load").expect("present");

    assert!(
        incremental_snapshot_state_from_offset(Some(loaded.as_ref())).is_none(),
        "a missing cursor must read as 'start from the beginning', not as an error — \
         refusing to start would be worse than re-reading"
    );
}

/// A read-only handle can inspect a directory that a live writer owns.
///
/// This is the ordinary operational case — a readiness endpoint reporting committed
/// progress, or an operator dumping the resume position — and it must not require
/// stopping the runtime. Concurrent *readers* cannot corrupt anything; only concurrent
/// writers can, which is why the exclusion is on writes alone.
#[tokio::test]
async fn a_read_only_handle_inspects_a_directory_a_writer_owns() {
    let temp = tempfile::tempdir().expect("tempdir");
    let directory = temp.path().to_path_buf();

    let mut owner = FileCheckpoint::new(directory.clone());
    owner
        .save(&PostgresOffset::new(4_242, "cdc_slot"), 7)
        .await
        .expect("owning instance writes");

    // The writer is still alive and still holds the lease.
    let inspector = FileCheckpoint::read_only(directory.clone());
    assert!(inspector.is_read_only());
    assert_eq!(
        inspector
            .get_committed_count()
            .await
            .expect("inspection must not require stopping the writer"),
        7
    );
    assert!(inspector.load().await.expect("load").is_some());

    // The owner keeps working, and its progress is visible to the inspector.
    owner
        .save(&PostgresOffset::new(5_000, "cdc_slot"), 9)
        .await
        .expect("owner still holds the lease");
    assert_eq!(inspector.get_committed_count().await.expect("count"), 9);
}

/// A read-only handle refuses to write, with a message naming the remedy.
#[tokio::test]
async fn a_read_only_handle_refuses_to_write() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut inspector = FileCheckpoint::read_only(temp.path());

    let error = inspector
        .save(&PostgresOffset::new(1, "cdc_slot"), 1)
        .await
        .expect_err("a read-only handle must refuse to write, not silently no-op");
    let message = error.to_string();
    assert!(message.contains("read-only"), "message was: {message}");
    assert!(
        message.contains("FileCheckpoint::new()"),
        "the error must name the remedy; message was: {message}"
    );
}

/// A second *writable* instance is still refused — that is the corruption case.
#[tokio::test]
async fn a_second_writable_handle_is_still_refused() {
    let temp = tempfile::tempdir().expect("tempdir");
    let directory = temp.path().to_path_buf();

    let mut owner = FileCheckpoint::new(directory.clone());
    owner
        .save(&PostgresOffset::new(1, "cdc_slot"), 1)
        .await
        .expect("owning instance writes");

    let second = FileCheckpoint::new(directory);
    let error = second
        .load()
        .await
        .expect_err("a second writable instance must be refused");
    assert!(error
        .to_string()
        .contains("already held by another instance in this process"));
}
