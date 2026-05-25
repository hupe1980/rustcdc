use rustcdc::checkpoint::{Checkpoint, FileCheckpoint, PostgresOffset};

#[tokio::test]
async fn file_checkpoint_survives_checkpoint_store_restart() {
    let temp = tempfile::tempdir().expect("tempdir");
    let directory = temp.path().to_path_buf();

    let mut writer = FileCheckpoint::new(directory.clone());
    let offset = PostgresOffset {
        lsn: 9001,
        slot_name: "phase1_slot".to_string(),
    };

    writer.save(&offset, 128).await.expect("save checkpoint");

    // Simulate process restart by constructing a new checkpoint store instance.
    let reader = FileCheckpoint::new(directory);
    let loaded = reader
        .load()
        .await
        .expect("load checkpoint")
        .expect("existing checkpoint");

    assert_eq!(loaded.source_type(), "postgres");
    assert_eq!(reader.get_committed_count().await.expect("load count"), 128);
}
