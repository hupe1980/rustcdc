use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::{Hash, Hasher},
};

use crate::{
    core::{Error, Event, Result},
    Operation,
};

/// Data-loss validation report for a captured event batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataLossReport {
    pub expected_events: u64,
    pub received_total: u64,
    pub received_unique: u64,
    pub missing_events: u64,
    pub duplicate_events: u64,
    pub corrupt_events: u64,
}

/// Tracks event integrity and loss metrics for fault-injection validation.
#[derive(Debug, Clone)]
pub struct DataLossValidator {
    expected_events: u64,
    received_total: u64,
    duplicate_events: u64,
    corrupt_events: u64,
    seen_fingerprints: HashSet<u64>,
}

impl DataLossValidator {
    pub fn new(expected_events: u64) -> Self {
        Self {
            expected_events,
            received_total: 0,
            duplicate_events: 0,
            corrupt_events: 0,
            seen_fingerprints: HashSet::new(),
        }
    }

    pub fn track_event(&mut self, event: &Event) {
        self.received_total = self.received_total.saturating_add(1);

        if event.validate().is_err() || event_has_corruption_marker(event) {
            self.corrupt_events = self.corrupt_events.saturating_add(1);
        }

        let key = event_fingerprint(event);
        if !self.seen_fingerprints.insert(key) {
            self.duplicate_events = self.duplicate_events.saturating_add(1);
        }
    }

    pub fn finalize(self) -> Result<DataLossReport> {
        let received_unique = u64::try_from(self.seen_fingerprints.len()).unwrap_or(u64::MAX);
        let missing_events = self.expected_events.saturating_sub(received_unique);

        let report = DataLossReport {
            expected_events: self.expected_events,
            received_total: self.received_total,
            received_unique,
            missing_events,
            duplicate_events: self.duplicate_events,
            corrupt_events: self.corrupt_events,
        };

        if report.missing_events > 0 {
            return Err(Error::ValidationError(vec![format!(
                "data loss detected: missing_events={} expected={} unique_received={}",
                report.missing_events, report.expected_events, report.received_unique
            )]));
        }

        if report.corrupt_events > 0 {
            return Err(Error::ValidationError(vec![format!(
                "data corruption detected: corrupt_events={}",
                report.corrupt_events
            )]));
        }

        Ok(report)
    }

    pub fn validate(expected: u64, received: Vec<Event>) -> Result<DataLossReport> {
        Self::validate_iter(expected, received)
    }

    pub fn validate_iter<I>(expected: u64, received: I) -> Result<DataLossReport>
    where
        I: IntoIterator<Item = Event>,
    {
        let mut validator = Self::new(expected);
        for event in received {
            validator.track_event(&event);
        }
        validator.finalize()
    }
}

fn event_fingerprint(event: &Event) -> u64 {
    let mut hasher = DefaultHasher::new();
    event.source.source_name.hash(&mut hasher);
    event.table.hash(&mut hasher);
    event.source.offset.hash(&mut hasher);

    let op = match event.op {
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
        Operation::Read => "read",
        Operation::SchemaChange => "schema_change",
        Operation::Truncate => "truncate",
    };
    op.hash(&mut hasher);

    if let Some(pk) = &event.primary_key {
        for key in pk {
            key.hash(&mut hasher);
        }
    }

    hasher.finish()
}

fn event_has_corruption_marker(event: &Event) -> bool {
    event
        .after
        .as_ref()
        .and_then(|row| row.get("__fault_corrupted"))
        .is_some()
        || event
            .before
            .as_ref()
            .and_then(|row| row.get("__fault_corrupted"))
            .is_some()
}

#[cfg(test)]
mod tests {
    use crate::{
        core::{SourceMetadata, TransactionMetadata, EVENT_ENVELOPE_VERSION},
        SnapshotMetadata,
    };

    use super::*;

    fn event(id: i64) -> Event {
        Event {
            before: None,
            after: Some(serde_json::json!({"id": id})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "mock".into(),
                offset: format!("{id}"),
                timestamp: u64::try_from(id).unwrap_or(0) + 1,
            },
            ts: u64::try_from(id).unwrap_or(0) + 1,
            schema: Some("public".into()),
            table: "items".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: Some(SnapshotMetadata {
                snapshot_id: "s1".into(),
                chunk_index: 0,
                is_last_chunk: false,
            }),
            transaction: Some(TransactionMetadata {
                tx_id: 1,
                total_events: 1,
                event_index: 0,
            }),
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    #[test]
    fn valid_batch_passes() {
        let events = vec![event(1), event(2), event(3)];
        let report = DataLossValidator::validate(3, events).unwrap();
        assert_eq!(report.missing_events, 0);
        assert_eq!(report.duplicate_events, 0);
        assert_eq!(report.corrupt_events, 0);
    }

    #[test]
    fn missing_events_are_detected() {
        let events = vec![event(1), event(2)];
        let error = DataLossValidator::validate(3, events).unwrap_err();
        assert!(format!("{error}").contains("missing_events"));
    }

    #[test]
    fn duplicate_events_are_reported() {
        let events = vec![event(1), event(1), event(2)];
        let report = DataLossValidator::validate(2, events).unwrap();
        assert_eq!(report.duplicate_events, 1);
        assert_eq!(report.missing_events, 0);
    }

    #[test]
    fn corruption_is_detected() {
        let mut corrupt = event(1);
        corrupt.after = None;
        let error = DataLossValidator::validate(1, vec![corrupt]).unwrap_err();
        assert!(format!("{error}").contains("corrupt_events"));
    }

    #[test]
    fn iterator_validation_is_supported() {
        let iter = (1..=5).map(event);
        let report = DataLossValidator::validate_iter(5, iter).unwrap();
        assert_eq!(report.received_unique, 5);
        assert_eq!(report.missing_events, 0);
    }
}
