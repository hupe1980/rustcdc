//! Snapshot consistency validation to detect missing rows, duplicates, and corruption.

use std::hash::{Hash, Hasher};

use ahash::{AHashMap as HashMap, AHashSet as HashSet, AHasher};

use crate::core::{Error, Event, Operation, Result};
use serde_json::Value;

/// Result of snapshot validation.
#[derive(Debug, Clone)]
pub struct SnapshotValidationResult {
    pub rows_expected: u64,
    pub rows_received: u64,
    pub duplicate_count: u64,
    pub missing_rows: Vec<String>, // PK values that were expected but missing
    pub extra_rows: Vec<String>,   // PK values that were unexpected
    pub is_valid: bool,
}

/// Tracks expected vs. received snapshot rows for validation.
#[derive(Debug)]
pub struct SnapshotValidator {
    /// Table-local validation state keyed by table name.
    tables: HashMap<String, TableValidationState>,
}

#[derive(Debug, Default)]
struct TableValidationState {
    received_pks: HashSet<u64>,
    rows_received: u64,
    expected_rows: Option<u64>,
}

impl TableValidationState {
    fn set_expected_rows(&mut self, count: u64) {
        self.expected_rows = Some(count);
        if let Ok(capacity) = usize::try_from(count) {
            let current_capacity = self.received_pks.capacity();
            self.received_pks
                .reserve(capacity.saturating_sub(current_capacity));
        }
    }
}

impl SnapshotValidator {
    /// Create a new snapshot validator.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Track a snapshot event. Should be called for each Read operation.
    pub fn track_event(&mut self, event: &Event) -> Result<()> {
        match event.op {
            Operation::Read => {
                let pk = if let Some(pk) = &event.primary_key {
                    pk
                } else {
                    return Err(Error::ValidationError(vec![
                        "Read event missing primary_key metadata".into(),
                    ]));
                };

                let after = if let Some(after) = &event.after {
                    after
                } else {
                    return Err(Error::ValidationError(vec![
                        "Read event missing 'after' field for PK extraction".into(),
                    ]));
                };

                let pk_hash = hash_primary_key_values(pk, after)?;

                // Avoid cloning table name on the hot path: look up by &str first,
                // and only allocate an owned String for the first-seen table.
                if let Some(state) = self.tables.get_mut(event.table.as_str()) {
                    state.rows_received = state.rows_received.saturating_add(1);
                    state.received_pks.insert(pk_hash);
                } else {
                    let state = self
                        .tables
                        .entry(event.table.clone())
                        .or_default();
                    state.rows_received = state.rows_received.saturating_add(1);
                    state.received_pks.insert(pk_hash);
                }

                Ok(())
            }
            _ => Err(Error::ValidationError(vec![format!(
                "SnapshotValidator only tracks Read events, got {:?}",
                event.op
            )])),
        }
    }

    /// Record expected row count for a table (typically from SELECT COUNT).
    pub fn set_expected_count(&mut self, table: &str, count: u64) {
        self.tables
            .entry(table.into())
            .or_default()
            .set_expected_rows(count);
    }

    /// Finalize validation and check for data integrity issues.
    pub fn finalize(self) -> Result<SnapshotValidationResult> {
        let mut total_expected = 0u64;
        let mut total_received = 0u64;
        let mut duplicates = 0u64;
        let mut missing_tables = Vec::new();
        let mut extra_tables = Vec::new();

        // Check all tables with expected counts.
        for (table, state) in &self.tables {
            let Some(expected_count) = state.expected_rows else {
                continue;
            };

            total_expected += expected_count;
            let received_count = state.rows_received;
            let unique_count = state.received_pks.len() as u64;
            total_received += received_count;
            duplicates = duplicates.saturating_add(received_count.saturating_sub(unique_count));

            if received_count < expected_count {
                let missing = expected_count - received_count;
                for i in 0..missing {
                    missing_tables.push(format!("{}[row_{}]", table, i));
                }
            } else if received_count > expected_count {
                for i in 0..(received_count - expected_count) {
                    extra_tables.push(format!("{}[duplicate_{}]", table, i));
                }
            }
        }

        // Check for unexpected tables
        for (table, state) in &self.tables {
            if state.expected_rows.is_none() && state.rows_received > 0 {
                extra_tables.push(format!("{}[unexpected_table]", table));
            }
        }

        let is_valid = total_received == total_expected && duplicates == 0;

        Ok(SnapshotValidationResult {
            rows_expected: total_expected,
            rows_received: total_received,
            duplicate_count: duplicates,
            missing_rows: missing_tables,
            extra_rows: extra_tables,
            is_valid,
        })
    }
}

impl Default for SnapshotValidator {
    fn default() -> Self {
        Self::new()
    }
}

fn hash_primary_key_values(pk: &[String], after: &Value) -> Result<u64> {
    let object = after.as_object().ok_or_else(|| {
        Error::ValidationError(vec![
            "Read event 'after' field must be a JSON object for PK extraction".into(),
        ])
    })?;

    // Use AHasher (non-cryptographic, fast) instead of DefaultHasher (SipHash-1-3).
    let mut hasher = AHasher::default();
    for key in pk {
        key.hash(&mut hasher);
        let value = object.get(key).ok_or_else(|| {
            Error::ValidationError(vec![format!(
                "Read event missing primary key field '{key}' in 'after' payload"
            )])
        })?;
        hash_json_value(value, &mut hasher);
    }
    Ok(hasher.finish())
}

fn hash_json_value(value: &Value, hasher: &mut AHasher) {
    match value {
        Value::Null => 0_u8.hash(hasher),
        Value::Bool(v) => {
            1_u8.hash(hasher);
            v.hash(hasher);
        }
        Value::Number(number) => {
            2_u8.hash(hasher);
            if let Some(v) = number.as_i64() {
                v.hash(hasher);
            } else if let Some(v) = number.as_u64() {
                v.hash(hasher);
            } else if let Some(v) = number.as_f64() {
                v.to_bits().hash(hasher);
            } else {
                number.to_string().hash(hasher);
            }
        }
        Value::String(v) => {
            3_u8.hash(hasher);
            v.hash(hasher);
        }
        // Composite values are uncommon for PK columns; fallback to canonical JSON text.
        Value::Array(_) | Value::Object(_) => {
            4_u8.hash(hasher);
            value.to_string().hash(hasher);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SnapshotMetadata, SourceMetadata, TransactionMetadata, EVENT_ENVELOPE_VERSION};

    fn read_event(table: &str, pk_id: i64) -> Event {
        Event {
            before: None,
            after: Some(serde_json::json!({"id": pk_id, "data": format!("row_{pk_id}")})),
            op: Operation::Read,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: format!("{}", pk_id),
                timestamp: 1000 + pk_id as u64,
            },
            ts: 1000 + pk_id as u64,
            schema: Some("public".into()),
            table: table.into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: Some(SnapshotMetadata {
                snapshot_id: "test_snap".into(),
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
    fn test_snapshot_validator_perfect_match() {
        let mut validator = SnapshotValidator::new();
        validator.set_expected_count("users", 3);

        validator.track_event(&read_event("users", 1)).unwrap();
        validator.track_event(&read_event("users", 2)).unwrap();
        validator.track_event(&read_event("users", 3)).unwrap();

        let result = validator.finalize().unwrap();
        assert!(result.is_valid);
        assert_eq!(result.rows_expected, 3);
        assert_eq!(result.rows_received, 3);
        assert_eq!(result.duplicate_count, 0);
        assert_eq!(result.missing_rows.len(), 0);
    }

    #[test]
    fn test_snapshot_validator_missing_rows() {
        let mut validator = SnapshotValidator::new();
        validator.set_expected_count("users", 5);

        validator.track_event(&read_event("users", 1)).unwrap();
        validator.track_event(&read_event("users", 2)).unwrap();
        validator.track_event(&read_event("users", 3)).unwrap();

        let result = validator.finalize().unwrap();
        assert!(!result.is_valid);
        assert_eq!(result.rows_expected, 5);
        assert_eq!(result.rows_received, 3);
        assert_eq!(result.missing_rows.len(), 2);
    }

    #[test]
    fn test_snapshot_validator_duplicate_rows() {
        let mut validator = SnapshotValidator::new();
        validator.set_expected_count("users", 2);

        validator.track_event(&read_event("users", 1)).unwrap();
        validator.track_event(&read_event("users", 2)).unwrap();
        validator.track_event(&read_event("users", 1)).unwrap(); // Duplicate

        let result = validator.finalize().unwrap();
        assert!(!result.is_valid);
        assert_eq!(result.rows_expected, 2);
        assert_eq!(result.rows_received, 3);
        assert_eq!(result.duplicate_count, 1);
    }

    #[test]
    fn test_snapshot_validator_multi_table() {
        let mut validator = SnapshotValidator::new();
        validator.set_expected_count("users", 2);
        validator.set_expected_count("orders", 3);

        validator.track_event(&read_event("users", 1)).unwrap();
        validator.track_event(&read_event("users", 2)).unwrap();
        validator.track_event(&read_event("orders", 100)).unwrap();
        validator.track_event(&read_event("orders", 101)).unwrap();
        validator.track_event(&read_event("orders", 102)).unwrap();

        let result = validator.finalize().unwrap();
        assert!(result.is_valid);
        assert_eq!(result.rows_expected, 5);
        assert_eq!(result.rows_received, 5);
    }

    #[test]
    fn test_snapshot_validator_missing_after_field() {
        let mut validator = SnapshotValidator::new();
        validator.set_expected_count("users", 1);

        let mut event = read_event("users", 1);
        event.after = None;

        let result = validator.track_event(&event);
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_validator_missing_primary_key() {
        let mut validator = SnapshotValidator::new();
        validator.set_expected_count("users", 1);

        let mut event = read_event("users", 1);
        event.primary_key = None;

        let result = validator.track_event(&event);
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_validator_non_read_operation() {
        let mut validator = SnapshotValidator::new();
        validator.set_expected_count("users", 1);

        let mut event = read_event("users", 1);
        event.op = Operation::Insert;

        let result = validator.track_event(&event);
        assert!(result.is_err());
    }
}
