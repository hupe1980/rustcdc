use rustcdc::{
    Operation, SnapshotMetadata, SnapshotValidator, SourceMetadata, TransactionMetadata,
};

fn snapshot_read_event(table: &str, id: i64) -> rustcdc::Event {
    rustcdc::Event::builder(table, Operation::Read)
        .after(serde_json::json!({"id": id, "name": format!("user-{id}")}))
        .source(SourceMetadata::new(
            "snapshot-integration",
            id.to_string(),
            1_700_000_000 + id as u64,
        ))
        .ts(1_700_000_000 + id as u64)
        .schema("dbo")
        .primary_key(["id"])
        .snapshot(SnapshotMetadata::new("validator-integration", 0, false))
        .transaction(TransactionMetadata::new(0, 0, Some(1)))
        .build()
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
    // The validator emits one diagnostic entry per table (not per missing row).
    // All 5 missing rows belong to the same "users" table, so there is exactly 1 entry.
    assert_eq!(result.missing_rows.len(), 1);
    let diag = &result.missing_rows[0];
    assert!(
        diag.contains("users"),
        "diagnostic must name the table; got: {diag}"
    );
    assert!(
        diag.contains(&format!("missing ~{}", skipped.len())),
        "diagnostic must report missing count; got: {diag}"
    );
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
