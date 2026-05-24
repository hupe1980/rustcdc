/// Semantic diff tool for canonical event comparison.
///
/// Compares events at the semantic level (table, operation, key fields)
/// rather than raw JSON comparison, which reduces noise and highlights real regressions.
use crate::core::Event;
use serde::{Deserialize, Serialize};

/// Diff level: what kind of change was detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiffLevel {
    /// No difference detected
    Identical,
    /// Inconsequential difference (e.g., timestamp, internal IDs)
    Inconsequential,
    /// Semantic change that may affect correctness (e.g., table name, operation type)
    Semantic,
    /// Critical structural difference (e.g., missing required field)
    Critical,
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
/// Returns a list of semantic differences, sorted by severity.
/// Ignores inconsequential fields like timestamps and internal IDs.
pub fn semantic_diff(old: &Event, new: &Event) -> Vec<EventDiff> {
    let mut diffs = Vec::new();

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

    // Sort by severity
    diffs.sort_by_key(|d| match d.level {
        DiffLevel::Identical => 0,
        DiffLevel::Inconsequential => 1,
        DiffLevel::Semantic => 2,
        DiffLevel::Critical => 3,
    });

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
