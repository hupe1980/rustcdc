/// Semantic diff tool for canonical event comparison.
///
/// Compares events at the semantic level (table, operation, key fields)
/// rather than raw JSON comparison, which reduces noise and highlights real regressions.
use crate::core::Event;
use serde::{Deserialize, Serialize};

/// Diff level: what kind of change was detected.
///
/// Variants are ordered from lowest to highest severity so that standard `>`
/// comparisons and `max()` calls work intuitively: `Critical > Semantic >
/// Inconsequential > Identical`.
///
/// [`semantic_diff`] returns results sorted **descending** (most severe first).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DiffLevel {
    /// No difference detected
    Identical,
    /// Inconsequential difference (e.g., JSON key reordering)
    Inconsequential,
    /// Semantic change that may affect correctness (e.g., table name, data field)
    Semantic,
    /// Critical structural difference (e.g., missing required field or wrong operation)
    Critical,
}

impl std::fmt::Display for DiffLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Critical => "critical",
            Self::Semantic => "semantic",
            Self::Inconsequential => "inconsequential",
            Self::Identical => "identical",
        })
    }
}

/// Semantic difference between two events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDiff {
    /// Severity of the difference
    pub level: DiffLevel,

    /// Human-readable summary of what changed
    pub summary: String,

    /// Detailed description of the difference
    pub details: Vec<String>,

    /// Path to the changed field in dot notation (e.g., "after.id", "source.timestamp")
    pub paths: Vec<String>,
}

impl std::fmt::Display for EventDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.level, self.summary)?;
        if !self.paths.is_empty() {
            write!(f, " ({})", self.paths.join(", "))?;
        }
        Ok(())
    }
}

impl EventDiff {
    /// Create a new diff entry.
    pub fn new(level: DiffLevel, summary: impl Into<String>, details: Vec<String>) -> Self {
        Self {
            level,
            summary: summary.into(),
            details,
            paths: Vec::new(),
        }
    }

    /// Add a field path to this diff.
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.paths.push(path.into());
        self
    }
}

/// Compare two events semantically.
///
/// Returns a list of differences, most severe first.
///
/// # What is compared, and what is deliberately not
///
/// This function is the **sole** comparison the golden-fixture suite performs, so a field it
/// ignores is invisible to every fixture. That is worth stating explicitly, because it was
/// once true of fields whose regressions are exactly what the fixtures exist to catch:
/// `primary_key`, `unavailable_columns`, `before_unavailable_columns`, `envelope_version` and
/// `source.offset` were all unchecked, so a change to any of them left 60-odd goldens green.
///
/// **Compared** — every field whose value is a deterministic function of the replayed input:
/// `op`, `table`, `schema`, `source.source_name`, `source.offset`, `before`, `after`,
/// `before_is_key_only`, `unavailable_columns`, `before_unavailable_columns`, `primary_key`,
/// `envelope_version`, `transaction`, and the deterministic half of `snapshot`.
///
/// **Not compared**, each for a reason rather than by omission:
///
/// | Field | Why |
/// |---|---|
/// | `ts`, `source.timestamp` | Wall-clock at capture. Differs every run by construction |
/// | `snapshot.snapshot_id` | Contains the millisecond the snapshot started (`incremental-<ms>`), so it differs per run while carrying no correctness meaning of its own |
///
/// Anything added to [`Event`] in future belongs in one of those two lists. A new field that
/// silently lands in neither is a field the fixtures cannot see.
pub fn semantic_diff(old: &Event, new: &Event) -> Vec<EventDiff> {
    let mut diffs = Vec::new();

    // A different envelope version is a different contract, not a changed value.
    if old.envelope_version != new.envelope_version {
        diffs.push(
            EventDiff::new(
                DiffLevel::Critical,
                format!(
                    "Envelope version changed from {} to {}",
                    old.envelope_version, new.envelope_version
                ),
                vec![],
            )
            .with_path("envelope_version"),
        );
    }

    // Critical structural diffs
    if old.op != new.op {
        diffs.push(
            EventDiff::new(
                DiffLevel::Critical,
                format!("Operation changed from {:?} to {:?}", old.op, new.op),
                vec![
                    format!("Old operation: {:?}", old.op),
                    format!("New operation: {:?}", new.op),
                ],
            )
            .with_path("op"),
        );
    }

    if old.table != new.table {
        diffs.push(
            EventDiff::new(
                DiffLevel::Semantic,
                format!("Table name changed from '{}' to '{}'", old.table, new.table),
                vec![
                    format!("Old table: {}", old.table),
                    format!("New table: {}", new.table),
                ],
            )
            .with_path("table"),
        );
    }

    // Source name diffs
    if old.source.source_name != new.source.source_name {
        diffs.push(
            EventDiff::new(
                DiffLevel::Semantic,
                format!(
                    "Source name changed from '{}' to '{}'",
                    old.source.source_name, new.source.source_name
                ),
                vec![],
            )
            .with_path("source.source_name"),
        );
    }

    // Schema diffs
    if old.schema != new.schema {
        diffs.push(
            EventDiff::new(
                DiffLevel::Semantic,
                format!("Schema changed from {:?} to {:?}", old.schema, new.schema),
                vec![],
            )
            .with_path("schema"),
        );
    }

    // Data field diffs (after, before)
    let after_diff = compare_json_fields(&old.after, &new.after, "after");
    diffs.extend(after_diff);

    let before_diff = compare_json_fields(&old.before, &new.before, "before");
    diffs.extend(before_diff);

    if old.before_is_key_only != new.before_is_key_only {
        diffs.push(
            EventDiff::new(
                DiffLevel::Semantic,
                format!(
                    "before_is_key_only changed from {} to {}",
                    old.before_is_key_only, new.before_is_key_only
                ),
                vec![],
            )
            .with_path("before_is_key_only"),
        );
    }

    // The declared key columns. A change here breaks message keys, log compaction and
    // upsert consumers — and `Event::primary_key_values` is all-or-nothing over this list,
    // so dropping one column stops the event resolving a key at all.
    if old.primary_key != new.primary_key {
        diffs.push(
            EventDiff::new(
                DiffLevel::Semantic,
                format!(
                    "primary_key changed from {:?} to {:?}",
                    old.primary_key, new.primary_key
                ),
                vec![],
            )
            .with_path("primary_key"),
        );
    }

    // The partial-payload contract. A regression that stopped reporting an unchanged-TOAST
    // column would make a sink write NULL over live data, which is the loudest failure this
    // crate is built to prevent and was the quietest one in this comparison.
    for (field, old_columns, new_columns) in [
        (
            "unavailable_columns",
            &old.unavailable_columns,
            &new.unavailable_columns,
        ),
        (
            "before_unavailable_columns",
            &old.before_unavailable_columns,
            &new.before_unavailable_columns,
        ),
    ] {
        if old_columns != new_columns {
            diffs.push(
                EventDiff::new(
                    DiffLevel::Semantic,
                    format!("{field} changed from {old_columns:?} to {new_columns:?}"),
                    vec![],
                )
                .with_path(field),
            );
        }
    }

    // The resume coordinate. A checkpoint is only as good as this string, and getting it
    // wrong costs a guaranteed duplicate — or a gap — on every restart.
    if old.source.offset != new.source.offset {
        diffs.push(
            EventDiff::new(
                DiffLevel::Semantic,
                format!(
                    "source.offset changed from '{}' to '{}'",
                    old.source.offset, new.source.offset
                ),
                vec![],
            )
            .with_path("source.offset"),
        );
    }

    if old.transaction != new.transaction {
        diffs.push(
            EventDiff::new(
                DiffLevel::Semantic,
                format!(
                    "transaction metadata changed from {:?} to {:?}",
                    old.transaction, new.transaction
                ),
                vec![],
            )
            .with_path("transaction"),
        );
    }

    // `snapshot_id` embeds the millisecond the snapshot began, so only the rest is
    // deterministic. Comparing presence as well catches a row that stopped being a snapshot
    // read, or started being one.
    let snapshot_shape = |event: &Event| {
        event
            .snapshot
            .as_ref()
            .map(|snapshot| (snapshot.chunk_index, snapshot.is_last_chunk))
    };
    if snapshot_shape(old) != snapshot_shape(new) {
        diffs.push(
            EventDiff::new(
                DiffLevel::Semantic,
                format!(
                    "snapshot chunk position changed from {:?} to {:?}",
                    snapshot_shape(old),
                    snapshot_shape(new)
                ),
                vec![],
            )
            .with_path("snapshot"),
        );
    }

    // Sort descending: highest severity (Critical) first.
    // With Critical as the highest discriminant, Reverse wrapping gives ascending sort key.
    diffs.sort_by_key(|d| std::cmp::Reverse(d.level));

    diffs
}

/// Compare two optional JSON values semantically.
fn compare_json_fields(
    old: &Option<serde_json::Value>,
    new: &Option<serde_json::Value>,
    field_name: &str,
) -> Vec<EventDiff> {
    let mut diffs = Vec::new();

    match (old, new) {
        (Some(old_val), Some(new_val)) if old_val != new_val => {
            // Check if it's just key reordering (inconsequential)
            if is_equivalent_json(old_val, new_val) {
                diffs.push(
                    EventDiff::new(
                        DiffLevel::Inconsequential,
                        format!("{} field reordered (semantically equivalent)", field_name),
                        vec![],
                    )
                    .with_path(format!("{} (keys reordered)", field_name)),
                );
            } else {
                diffs.push(
                    EventDiff::new(
                        DiffLevel::Semantic,
                        format!("{} field changed structurally", field_name),
                        vec![format!("Old: {}", old_val), format!("New: {}", new_val)],
                    )
                    .with_path(field_name),
                );
            }
        }
        (None, Some(_)) => {
            diffs.push(
                EventDiff::new(
                    DiffLevel::Semantic,
                    format!("{} field was added", field_name),
                    vec![],
                )
                .with_path(field_name),
            );
        }
        (Some(_), None) => {
            diffs.push(
                EventDiff::new(
                    DiffLevel::Semantic,
                    format!("{} field was removed", field_name),
                    vec![],
                )
                .with_path(field_name),
            );
        }
        _ => {}
    }

    diffs
}

/// Check if two JSON values are semantically equivalent (same data, possibly different order).
fn is_equivalent_json(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    // Normalize both to canonical form and compare
    match (a, b) {
        (serde_json::Value::Object(a_map), serde_json::Value::Object(b_map)) => {
            if a_map.len() != b_map.len() {
                return false;
            }
            a_map
                .iter()
                .all(|(k, v)| b_map.get(k).is_some_and(|bv| is_equivalent_json(v, bv)))
        }
        (serde_json::Value::Array(a_arr), serde_json::Value::Array(b_arr)) => {
            if a_arr.len() != b_arr.len() {
                return false;
            }
            a_arr
                .iter()
                .zip(b_arr.iter())
                .all(|(av, bv)| is_equivalent_json(av, bv))
        }
        _ => a == b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Operation, SourceMetadata};

    #[test]
    fn semantic_diff_detects_operation_changes() {
        let old = Event {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "postgres".to_string(),
                offset: "0".to_string(),
                timestamp: 0,
            },
            ts: 0,
            schema: None,
            table: "test".to_string(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: crate::core::EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        };

        let mut new = old.clone();
        new.op = Operation::Update;

        let diffs = semantic_diff(&old, &new);
        assert!(!diffs.is_empty());
        assert_eq!(diffs[0].level, DiffLevel::Critical);
    }

    #[test]
    fn semantic_diff_detects_table_changes() {
        let old = Event {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "postgres".to_string(),
                offset: "0".to_string(),
                timestamp: 0,
            },
            ts: 0,
            schema: None,
            table: "table_a".to_string(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: crate::core::EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        };

        let mut new = old.clone();
        new.table = "table_b".to_string();

        let diffs = semantic_diff(&old, &new);
        assert!(!diffs.is_empty());
        assert_eq!(diffs[0].level, DiffLevel::Semantic);
    }

    #[test]
    fn semantic_diff_ignores_equivalent_json_reordering() {
        let old_json = serde_json::json!({"a": 1, "b": 2});
        let new_json = serde_json::json!({"b": 2, "a": 1});

        assert!(is_equivalent_json(&old_json, &new_json));
    }
}

#[cfg(test)]
mod field_coverage_tests {
    use super::{semantic_diff, DiffLevel};
    use crate::core::{
        Event, Operation, SnapshotMetadata, SourceMetadata, TransactionMetadata,
        EVENT_ENVELOPE_VERSION,
    };

    fn baseline() -> Event {
        Event::builder("orders", Operation::Update)
            .schema("public")
            .source(SourceMetadata::new("postgres", "0/16B6A70", 1))
            .ts(1)
            .before(serde_json::json!({ "id": "1" }))
            .after(serde_json::json!({ "id": "1", "total": "10" }))
            .before_is_key_only(true)
            .primary_key(["id"])
            .transaction(TransactionMetadata::new(42, 0, None))
            .snapshot(SnapshotMetadata::new("incremental-1", 3, false))
            .build()
    }

    fn diff_paths(mutate: impl FnOnce(&mut Event)) -> Vec<String> {
        let expected = baseline();
        let mut actual = baseline();
        mutate(&mut actual);
        semantic_diff(&expected, &actual)
            .into_iter()
            .filter(|diff| diff.level != DiffLevel::Identical)
            .flat_map(|diff| diff.paths)
            .collect()
    }

    /// `semantic_diff` is the **only** comparison the golden-fixture suite performs, so a
    /// field it ignores is invisible to every fixture. Each of these was ignored, and each is
    /// a field whose regression this crate's own history shows matters.
    #[test]
    fn every_deterministic_field_is_actually_compared() {
        for (label, mutate) in [
            (
                "primary_key",
                Box::new(|event: &mut Event| event.primary_key = Some(vec!["tenant".into()]))
                    as Box<dyn FnOnce(&mut Event)>,
            ),
            (
                "unavailable_columns",
                Box::new(|event: &mut Event| event.unavailable_columns = vec!["body".into()]),
            ),
            (
                "before_unavailable_columns",
                Box::new(|event: &mut Event| {
                    event.before_is_key_only = true;
                    event.before_unavailable_columns = vec!["body".into()];
                }),
            ),
            (
                "envelope_version",
                Box::new(|event: &mut Event| {
                    event.envelope_version = EVENT_ENVELOPE_VERSION + 1
                }),
            ),
            (
                "source.offset",
                Box::new(|event: &mut Event| event.source.offset = "0/DEADBEEF".into()),
            ),
            (
                "transaction",
                Box::new(|event: &mut Event| {
                    event.transaction = Some(TransactionMetadata::new(42, 7, None))
                }),
            ),
            (
                "snapshot",
                Box::new(|event: &mut Event| {
                    event.snapshot = Some(SnapshotMetadata::new("incremental-1", 9, false))
                }),
            ),
        ] {
            let paths = diff_paths(mutate);
            assert!(
                paths.iter().any(|path| path.starts_with(label)),
                "a change to '{label}' produced no diff, so every golden fixture is blind to \
                 it. Paths reported: {paths:?}"
            );
        }
    }

    /// The other half: fields that legitimately differ every run must stay ignored, or every
    /// fixture fails on wall-clock noise and the suite gets disabled.
    #[test]
    fn per_run_varying_fields_stay_ignored() {
        assert!(
            diff_paths(|event| event.ts = 999_999).is_empty(),
            "`ts` is wall-clock at capture and differs every run"
        );
        assert!(
            diff_paths(|event| event.source.timestamp = 999_999).is_empty(),
            "`source.timestamp` is the source's commit clock and differs every run"
        );
        assert!(
            diff_paths(|event| {
                event.snapshot = Some(SnapshotMetadata::new("incremental-999", 3, false));
            })
            .is_empty(),
            "`snapshot_id` embeds the millisecond the snapshot began, so it differs per run \
             while carrying no correctness meaning of its own"
        );
    }

    #[test]
    fn an_identical_event_produces_no_diff() {
        assert!(semantic_diff(&baseline(), &baseline()).is_empty());
    }

    #[test]
    fn an_envelope_version_change_is_critical_not_merely_semantic() {
        let expected = baseline();
        let mut actual = baseline();
        actual.envelope_version = EVENT_ENVELOPE_VERSION + 1;
        let diffs = semantic_diff(&expected, &actual);
        assert_eq!(
            diffs.first().map(|diff| diff.level),
            Some(DiffLevel::Critical),
            "a different envelope version is a different contract, and results sort most \
             severe first"
        );
    }
}
