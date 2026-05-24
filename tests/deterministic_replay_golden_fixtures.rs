use std::{fs, path::PathBuf};

use cdc_rs::deterministic_replay::{semantic_diff, DiffLevel, Fixture, ReplaySession};
use cdc_rs::Event;

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/deterministic_replay")
}

fn load_golden(path: PathBuf) -> Vec<Event> {
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed reading golden file '{}': {error}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("failed parsing golden file '{}': {error}", path.display()))
}

fn assert_matches_golden_with_expected_error(
    fixture_name: &str,
    golden_name: &str,
    expected_error_substring: Option<&str>,
) {
    let fixture_path = fixture_root().join(fixture_name);
    let fixture = Fixture::from_path(&fixture_path).unwrap_or_else(|error| {
        panic!(
            "failed loading fixture '{}': {error}",
            fixture_path.display()
        )
    });

    let mut replay = ReplaySession::new(fixture).expect("replay session creation");
    let result = replay.replay();
    if let Some(expected) = expected_error_substring {
        assert!(
            result.errors.iter().any(|error| error.contains(expected)),
            "expected replay error containing '{expected}' for {} but saw: {:?}",
            fixture_name,
            result.errors
        );
    } else {
        assert!(
            result.success,
            "replay should succeed for {} but had errors: {:?}",
            fixture_name, result.errors
        );
    }

    let actual_events: Vec<Event> = replay
        .events()
        .iter()
        .map(|item| item.event.clone())
        .collect();

    let golden_path = fixture_root().join(golden_name);

    if std::env::var("UPDATE_GOLDENS").is_ok() {
        let json = serde_json::to_string_pretty(&actual_events)
            .expect("failed to serialize golden events");
        fs::write(&golden_path, json).unwrap_or_else(|error| {
            panic!(
                "failed writing golden file '{}': {error}",
                golden_path.display()
            )
        });
        return;
    }

    let golden_events = load_golden(golden_path);

    assert_eq!(
        actual_events.len(),
        golden_events.len(),
        "event count mismatch for {}",
        fixture_name
    );

    for (index, (actual, expected)) in actual_events.iter().zip(golden_events.iter()).enumerate() {
        let diffs = semantic_diff(expected, actual);
        let meaningful: Vec<_> = diffs
            .into_iter()
            .filter(|diff| diff.level != DiffLevel::Identical)
            .collect();

        assert!(
            meaningful.is_empty(),
            "semantic diff mismatch for {} event #{}: {:?}",
            fixture_name,
            index,
            meaningful
        );
        assert_eq!(
            actual, expected,
            "exact event mismatch for {} event #{}",
            fixture_name, index
        );
    }
}

fn assert_matches_golden(fixture_name: &str, golden_name: &str) {
    assert_matches_golden_with_expected_error(fixture_name, golden_name, None)
}

#[test]
fn postgres_fixture_matches_golden() {
    assert_matches_golden(
        "postgres_live_capture_v1.fixture.json",
        "postgres_live_capture_v1.golden.json",
    );
}

#[test]
fn postgres_long_transaction_schema_evolution_fixture_matches_golden() {
    assert_matches_golden(
        "postgres_long_transaction_schema_evolution_v1.fixture.json",
        "postgres_long_transaction_schema_evolution_v1.golden.json",
    );
}

#[test]
fn postgres_resumed_post_crash_fixture_matches_golden() {
    assert_matches_golden(
        "postgres_resumed_post_crash_v1.fixture.json",
        "postgres_resumed_post_crash_v1.golden.json",
    );
}

#[test]
fn postgres_crash_interrupted_transaction_fixture_matches_golden() {
    assert_matches_golden_with_expected_error(
        "postgres_crash_interrupted_transaction_v1.fixture.json",
        "postgres_crash_interrupted_transaction_v1.golden.json",
        Some("was not committed before end of fixture"),
    );
}

#[test]
fn postgres_ddl_unsupported_alter_fixture_matches_golden() {
    assert_matches_golden(
        "postgres_ddl_unsupported_alter_v1.fixture.json",
        "postgres_ddl_unsupported_alter_v1.golden.json",
    );
}

#[test]
fn postgres_ddl_unsupported_rename_fixture_matches_golden() {
    assert_matches_golden(
        "postgres_ddl_unsupported_rename_v1.fixture.json",
        "postgres_ddl_unsupported_rename_v1.golden.json",
    );
}

#[test]
fn postgres_ddl_unsupported_tablespace_fixture_matches_golden() {
    assert_matches_golden(
        "postgres_ddl_unsupported_tablespace_v1.fixture.json",
        "postgres_ddl_unsupported_tablespace_v1.golden.json",
    );
}

#[test]
fn postgres_ddl_unsupported_partition_fixture_matches_golden() {
    assert_matches_golden(
        "postgres_ddl_unsupported_partition_v1.fixture.json",
        "postgres_ddl_unsupported_partition_v1.golden.json",
    );
}

#[test]
fn postgres_ddl_mixed_quoted_identifiers_fixture_matches_golden() {
    assert_matches_golden(
        "postgres_ddl_mixed_quoted_identifiers_v1.fixture.json",
        "postgres_ddl_mixed_quoted_identifiers_v1.golden.json",
    );
}

#[test]
fn postgres_ddl_mixed_escaped_identifier_fixture_matches_golden() {
    assert_matches_golden(
        "postgres_ddl_mixed_escaped_identifier_v1.fixture.json",
        "postgres_ddl_mixed_escaped_identifier_v1.golden.json",
    );
}

#[test]
fn postgres_ddl_escaped_relation_name_fixture_matches_golden() {
    assert_matches_golden(
        "postgres_ddl_escaped_relation_name_v1.fixture.json",
        "postgres_ddl_escaped_relation_name_v1.golden.json",
    );
}

#[test]
fn postgres_ddl_dotted_identifier_fixture_matches_golden() {
    assert_matches_golden(
        "postgres_ddl_dotted_identifier_v1.fixture.json",
        "postgres_ddl_dotted_identifier_v1.golden.json",
    );
}

#[test]
fn mysql_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_live_capture_v1.fixture.json",
        "mysql_live_capture_v1.golden.json",
    );
}

#[test]
fn mysql_long_transaction_schema_evolution_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_long_transaction_schema_evolution_v1.fixture.json",
        "mysql_long_transaction_schema_evolution_v1.golden.json",
    );
}

#[test]
fn mysql_resumed_post_crash_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_resumed_post_crash_v1.fixture.json",
        "mysql_resumed_post_crash_v1.golden.json",
    );
}

#[test]
fn mysql_crash_interrupted_transaction_fixture_matches_golden() {
    assert_matches_golden_with_expected_error(
        "mysql_crash_interrupted_transaction_v1.fixture.json",
        "mysql_crash_interrupted_transaction_v1.golden.json",
        Some("was not committed before end of fixture"),
    );
}

#[test]
fn mysql_transaction_rollback_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_transaction_rollback_v1.fixture.json",
        "mysql_transaction_rollback_v1.golden.json",
    );
}

#[test]
fn mysql_ddl_mixed_alter_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_ddl_mixed_alter_v1.fixture.json",
        "mysql_ddl_mixed_alter_v1.golden.json",
    );
}

#[test]
fn mysql_ddl_unsupported_storage_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_ddl_unsupported_storage_v1.fixture.json",
        "mysql_ddl_unsupported_storage_v1.golden.json",
    );
}

#[test]
fn mysql_ddl_unsupported_storage_quoted_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_ddl_unsupported_storage_quoted_v1.fixture.json",
        "mysql_ddl_unsupported_storage_quoted_v1.golden.json",
    );
}

#[test]
fn mysql_ddl_unsupported_partition_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_ddl_unsupported_partition_v1.fixture.json",
        "mysql_ddl_unsupported_partition_v1.golden.json",
    );
}

#[test]
fn mysql_ddl_mixed_ordered_quoted_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_ddl_mixed_ordered_quoted_v1.fixture.json",
        "mysql_ddl_mixed_ordered_quoted_v1.golden.json",
    );
}

#[test]
fn mysql_ddl_mixed_quoted_identifiers_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_ddl_mixed_quoted_identifiers_v1.fixture.json",
        "mysql_ddl_mixed_quoted_identifiers_v1.golden.json",
    );
}

#[test]
fn mysql_ddl_mixed_escaped_identifier_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_ddl_mixed_escaped_identifier_v1.fixture.json",
        "mysql_ddl_mixed_escaped_identifier_v1.golden.json",
    );
}

#[test]
fn mysql_ddl_escaped_relation_name_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_ddl_escaped_relation_name_v1.fixture.json",
        "mysql_ddl_escaped_relation_name_v1.golden.json",
    );
}

#[test]
fn mysql_ddl_dotted_identifier_fixture_matches_golden() {
    assert_matches_golden(
        "mysql_ddl_dotted_identifier_v1.fixture.json",
        "mysql_ddl_dotted_identifier_v1.golden.json",
    );
}

#[test]
fn sqlserver_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_live_capture_v1.fixture.json",
        "sqlserver_live_capture_v1.golden.json",
    );
}

#[test]
fn sqlserver_long_transaction_schema_evolution_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_long_transaction_schema_evolution_v1.fixture.json",
        "sqlserver_long_transaction_schema_evolution_v1.golden.json",
    );
}

#[test]
fn sqlserver_resumed_post_crash_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_resumed_post_crash_v1.fixture.json",
        "sqlserver_resumed_post_crash_v1.golden.json",
    );
}

#[test]
fn sqlserver_crash_interrupted_transaction_fixture_matches_golden() {
    assert_matches_golden_with_expected_error(
        "sqlserver_crash_interrupted_transaction_v1.fixture.json",
        "sqlserver_crash_interrupted_transaction_v1.golden.json",
        Some("was not committed before end of fixture"),
    );
}

#[test]
fn sqlserver_transaction_boundaries_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_transaction_boundaries_v1.fixture.json",
        "sqlserver_transaction_boundaries_v1.golden.json",
    );
}

#[test]
fn sqlserver_ddl_mixed_alter_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_ddl_mixed_alter_v1.fixture.json",
        "sqlserver_ddl_mixed_alter_v1.golden.json",
    );
}

#[test]
fn sqlserver_ddl_unsupported_constraint_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_ddl_unsupported_constraint_v1.fixture.json",
        "sqlserver_ddl_unsupported_constraint_v1.golden.json",
    );
}

#[test]
fn sqlserver_ddl_unsupported_options_quoted_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_ddl_unsupported_options_quoted_v1.fixture.json",
        "sqlserver_ddl_unsupported_options_quoted_v1.golden.json",
    );
}

#[test]
fn sqlserver_ddl_mixed_ordered_quoted_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_ddl_mixed_ordered_quoted_v1.fixture.json",
        "sqlserver_ddl_mixed_ordered_quoted_v1.golden.json",
    );
}

#[test]
fn sqlserver_ddl_mixed_escaped_identifier_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_ddl_mixed_escaped_identifier_v1.fixture.json",
        "sqlserver_ddl_mixed_escaped_identifier_v1.golden.json",
    );
}

#[test]
fn sqlserver_ddl_mixed_escaped_literal_identifier_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_ddl_mixed_escaped_literal_identifier_v1.fixture.json",
        "sqlserver_ddl_mixed_escaped_literal_identifier_v1.golden.json",
    );
}

#[test]
fn sqlserver_ddl_escaped_relation_name_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_ddl_escaped_relation_name_v1.fixture.json",
        "sqlserver_ddl_escaped_relation_name_v1.golden.json",
    );
}

#[test]
fn sqlserver_ddl_dotted_identifier_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_ddl_dotted_identifier_v1.fixture.json",
        "sqlserver_ddl_dotted_identifier_v1.golden.json",
    );
}

#[test]
fn sqlserver_ddl_three_part_name_fixture_matches_golden() {
    assert_matches_golden(
        "sqlserver_ddl_three_part_name_v1.fixture.json",
        "sqlserver_ddl_three_part_name_v1.golden.json",
    );
}
