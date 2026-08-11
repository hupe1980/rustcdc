use std::{fs, path::PathBuf};

use rustcdc::deterministic_replay::{semantic_diff, DiffLevel, Fixture, ReplaySession};
use rustcdc::Event;

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

    // A replayed event must satisfy the envelope contract, not merely match a recorded golden.
    // Matching alone would let a fixture pin a *malformed* envelope in place: record the golden
    // once, and the suite then defends the malformation forever. The partial-payload rules are
    // the ones that matter here — a column may not be both listed as unavailable and present in
    // the payload, and a key-only before-image may not also carry unavailable columns.
    //
    // Checked **before** the `UPDATE_GOLDENS` branch, deliberately. It used to run only on the
    // comparison path, so re-blessing wrote the golden and returned without validating anything
    // — a run that reported `ok` having checked nothing, and the one moment a contributor is
    // most likely to be wrong about what the events should look like. CI caught it on the next
    // run, so this was never a hole in the suite; it was a hole in the feedback.
    for (index, event) in actual_events.iter().enumerate() {
        if let Err(errors) = event.validate() {
            panic!(
                "replayed event #{index} of {fixture_name} violates the envelope contract: \
                 {errors}\n  event: {event:?}"
            );
        }
    }

    // Regenerating goldens: `UPDATE_GOLDENS=1 cargo test --test deterministic_replay_golden_fixtures`.
    //
    // Legitimate when the fixture changed or the envelope changed on purpose. It is **not** the
    // way to make a failing test pass: a golden is the recorded answer, so re-blessing a
    // regression records the regression and the suite then defends it. Read the diff of the
    // golden files before committing — that diff is the entire review surface for this suite.
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

    let golden_raw = fs::read_to_string(&golden_path).unwrap_or_else(|error| {
        panic!(
            "failed reading golden file '{}': {error}",
            golden_path.display()
        )
    });
    let golden_events = load_golden(golden_path.clone());

    assert_eq!(
        actual_events.len(),
        golden_events.len(),
        "event count mismatch for {}",
        fixture_name
    );

    assert_golden_records_every_field(fixture_name, &golden_path, &golden_raw, &actual_events);

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

/// Fail when a golden file omits a field the current envelope serializes.
///
/// # The hole this closes
///
/// `Event` fields carry `#[serde(default)]`, so a golden recorded before a field existed
/// still deserializes — the missing field silently becomes `false`, `None` or empty. The
/// comparison then passes whenever the default happens to be the right answer, which means
/// the suite is not pinning that field at all: it is agreeing with itself.
///
/// This was live. Forty of the forty-one goldens predated `before_is_key_only` and did not
/// record it; each loaded as `false`, which was correct for those fixtures, so nothing
/// failed. Nothing would have failed either if a future field's default were *wrong* for
/// them — the recorded answer would simply have been silently incomplete, which is the
/// property a golden exists to not have.
///
/// Fields with `skip_serializing_if` are legitimately absent when empty, so the comparison
/// is against what *this* event actually serializes rather than against a fixed field list.
fn assert_golden_records_every_field(
    fixture_name: &str,
    golden_path: &std::path::Path,
    golden_raw: &str,
    actual_events: &[Event],
) {
    let recorded: Vec<serde_json::Value> =
        serde_json::from_str(golden_raw).unwrap_or_else(|error| {
            panic!(
                "golden '{}' is not a JSON array: {error}",
                golden_path.display()
            )
        });

    for (index, (actual, recorded)) in actual_events.iter().zip(recorded.iter()).enumerate() {
        let expected_value =
            serde_json::to_value(actual).expect("an Event always serializes to JSON");
        let (Some(expected_object), Some(recorded_object)) =
            (expected_value.as_object(), recorded.as_object())
        else {
            panic!(
                "golden '{}' event #{index} is not an object",
                golden_path.display()
            );
        };

        let missing: Vec<&String> = expected_object
            .keys()
            .filter(|key| !recorded_object.contains_key(*key))
            .collect();
        assert!(
            missing.is_empty(),
            "golden for {fixture_name} is stale: event #{index} does not record {missing:?}, \
             which the current envelope serializes. Those fields are `#[serde(default)]`, so \
             they load as defaults and the comparison silently stops pinning them. \
             Regenerate with `UPDATE_GOLDENS=1 cargo test --test \
             deterministic_replay_golden_fixtures` and review the diff."
        );
    }
}

fn assert_matches_golden(fixture_name: &str, golden_name: &str) {
    assert_matches_golden_with_expected_error(fixture_name, golden_name, None)
}

/// The partial-payload contract, which no fixture could express before 0.12.0: the replay
/// engine hardcoded `before_is_key_only` and both unavailable-column lists to their empty
/// defaults, so `semantic_diff` compared fields that were structurally always equal. A
/// regression that stopped reporting an unchanged-TOAST column — making a sink write `NULL`
/// over live data — was invisible to every golden.
#[test]
fn postgres_unchanged_toast_fixture_matches_golden() {
    assert_matches_golden(
        "postgres_unchanged_toast_v1.fixture.json",
        "postgres_unchanged_toast_v1.golden.json",
    );
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
