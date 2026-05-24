use cdc_rs::{
    Operation, SnapshotMetadata, SnapshotValidator, SourceMetadata, TransactionMetadata,
    EVENT_ENVELOPE_VERSION,
};

fn snapshot_read_event(table: &str, id: i64) -> cdc_rs::Event {
    cdc_rs::Event {
        before: None,
        after: Some(serde_json::json!({"id": id, "name": format!("user-{id}")})),
        op: Operation::Read,
        source: SourceMetadata {
            source_name: "snapshot-integration".into(),
            offset: id.to_string(),
            timestamp: 1_700_000_000 + id as u64,
        },
        ts: 1_700_000_000 + id as u64,
        schema: Some("dbo".into()),
        table: table.into(),
        primary_key: Some(vec!["id".into()]),
        snapshot: Some(SnapshotMetadata {
            snapshot_id: "validator-integration".into(),
            chunk_index: 0,
            is_last_chunk: false,
        }),
        transaction: Some(TransactionMetadata {
            tx_id: 0,
            total_events: 1,
            event_index: 0,
        }),
        envelope_version: EVENT_ENVELOPE_VERSION,
    }
}

#[test]
fn snapshot_validator_detects_missing_rows_for_10k_snapshot() {
    let table = "users";
    let expected = 10_000_u64;
    let skipped = [17_i64, 2_500, 4_999, 7_500, 9_999];

    let mut validator = SnapshotValidator::new();
    validator.set_expected_count(table, expected);

    for id in 0_i64..10_000_i64 {
        if skipped.contains(&id) {
            continue;
        }
        validator
            .track_event(&snapshot_read_event(table, id))
            .expect("tracking event should succeed");
    }

    let result = validator.finalize().expect("finalize should succeed");

    assert!(!result.is_valid);
    assert_eq!(result.rows_expected, expected);
    assert_eq!(result.rows_received, expected - skipped.len() as u64);
    assert_eq!(result.duplicate_count, 0);
    assert_eq!(result.missing_rows.len(), skipped.len());
}

#[test]
fn snapshot_validator_detects_duplicate_row_for_10k_snapshot() {
    let table = "users";
    let expected = 10_000_u64;

    let mut validator = SnapshotValidator::new();
    validator.set_expected_count(table, expected);

    for id in 0_i64..10_000_i64 {
        validator
            .track_event(&snapshot_read_event(table, id))
            .expect("tracking event should succeed");
    }

    validator
        .track_event(&snapshot_read_event(table, 4_242))
        .expect("tracking duplicate event should succeed");

    let result = validator.finalize().expect("finalize should succeed");

    assert!(!result.is_valid);
    assert_eq!(result.rows_expected, expected);
    assert_eq!(result.rows_received, expected + 1);
    assert_eq!(result.duplicate_count, 1);
    assert_eq!(result.extra_rows.len(), 1);
}
