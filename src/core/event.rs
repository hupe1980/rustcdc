//! Canonical event envelope definitions and validation helpers.

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::{Error, Result};

/// Current version of the canonical event envelope.
pub const EVENT_ENVELOPE_VERSION: u16 = 1;

/// CRUD-style operations emitted by a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Insert,
    Update,
    Delete,
    Read,
    SchemaChange,
    /// All rows were removed from the table by a `TRUNCATE` statement.
    ///
    /// `before` and `after` are always `None` for truncate events.
    /// Only connectors that advertise [`crate::source::ConnectorCapabilities::truncate`]
    /// emit this variant.
    Truncate,
}

impl Display for Operation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

impl Operation {
    /// Return a `&'static str` representation without heap allocation.
    ///
    /// Prefer this over `to_string()` on hot paths.
    pub fn to_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Read => "read",
            Self::SchemaChange => "schema_change",
            Self::Truncate => "truncate",
        }
    }
}

/// Source identity and position metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceMetadata {
    /// Logical name of the source connector.
    pub source_name: String,
    /// Source-specific durable position encoded as a string.
    pub offset: String,
    /// Source timestamp associated with the position.
    pub timestamp: u64,
}

/// Snapshot progress information when an event is emitted during snapshotting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Identifier for the snapshot session.
    pub snapshot_id: String,
    /// Zero-based snapshot chunk index.
    pub chunk_index: u32,
    /// Whether this chunk is the final one in the snapshot.
    pub is_last_chunk: bool,
}

/// Transaction metadata when an event belongs to a multi-event transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionMetadata {
    /// Transaction identifier assigned by the source.
    pub tx_id: u64,
    /// Total number of events expected in the transaction.
    pub total_events: u32,
    /// Zero-based position of this event within the transaction.
    pub event_index: u32,
}

/// Validation error describing a broken contract in an event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationError {
    /// Name of the field that failed validation.
    pub field: String,
    /// Human-readable explanation of the validation failure.
    pub message: String,
}

impl ValidationError {
    fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

/// Canonical event envelope used across all sources.
///
/// # Examples
///
/// ```
/// use cdc_rs::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
/// use serde_json::json;
///
/// let event = Event {
///     before: None,
///     after: Some(json!({"id": 1, "name": "alice"})),
///     op: Operation::Insert,
///     source: SourceMetadata {
///         source_name: "postgres".into(),
///         offset: "0/16B6A70".into(),
///         timestamp: 1,
///     },
///     ts: 1,
///     schema: Some("public".into()),
///     table: "users".into(),
///     primary_key: Some(vec!["id".into()]),
///     snapshot: None,
///     transaction: None,
///     envelope_version: EVENT_ENVELOPE_VERSION,
/// };
///
/// let encoded = event.to_json().unwrap();
/// let decoded = Event::from_json(&encoded).unwrap();
/// assert_eq!(decoded.table, "users");
/// assert!(decoded.validate().is_ok());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// Row state before the operation, when available.
    pub before: Option<Value>,
    /// Row state after the operation, when available.
    pub after: Option<Value>,
    /// CRUD operation represented by this event.
    pub op: Operation,
    /// Source identity and durable position metadata.
    pub source: SourceMetadata,
    /// Event timestamp in milliseconds since epoch.
    pub ts: u64,
    /// Schema name when the source provides one.
    pub schema: Option<String>,
    /// Table name that produced the event.
    pub table: String,
    /// Primary key column names, if available.
    pub primary_key: Option<Vec<String>>,
    /// Snapshot metadata when the event belongs to a snapshot phase.
    pub snapshot: Option<SnapshotMetadata>,
    /// Transaction metadata when the event belongs to a transaction.
    pub transaction: Option<TransactionMetadata>,
    /// Canonical envelope version for compatibility checks.
    pub envelope_version: u16,
}

impl Event {
    /// Serialize the event to compact JSON.
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    /// Deserialize an event from JSON.
    pub fn from_json(input: &str) -> Result<Self> {
        Ok(serde_json::from_str(input)?)
    }

    /// Validate the event against the canonical envelope contract.
    pub fn validate(&self) -> std::result::Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();

        if self.table.trim().is_empty() {
            errors.push(ValidationError::new(
                "table",
                "table name must not be empty",
            ));
        }

        if self.ts == 0 {
            errors.push(ValidationError::new("ts", "timestamp must be non-zero"));
        }

        if self.envelope_version != EVENT_ENVELOPE_VERSION {
            errors.push(ValidationError::new(
                "envelope_version",
                format!(
                    "expected envelope version {EVENT_ENVELOPE_VERSION}, got {}",
                    self.envelope_version
                ),
            ));
        }

        if self.source.source_name.trim().is_empty() {
            errors.push(ValidationError::new(
                "source.source_name",
                "source_name must not be empty",
            ));
        }

        match self.op {
            Operation::Insert => {
                if self.after.is_none() {
                    errors.push(ValidationError::new(
                        "after",
                        "insert events must include after",
                    ));
                }
                if self.before.is_some() {
                    errors.push(ValidationError::new(
                        "before",
                        "insert events must not include before",
                    ));
                }
            }
            Operation::Update => {
                if self.after.is_none() {
                    errors.push(ValidationError::new(
                        "after",
                        "update events must include after",
                    ));
                }
                if self.before.is_none() {
                    errors.push(ValidationError::new(
                        "before",
                        "update events must include before",
                    ));
                }
            }
            Operation::Delete => {
                if self.before.is_none() {
                    errors.push(ValidationError::new(
                        "before",
                        "delete events must include before",
                    ));
                }
                if self.after.is_some() {
                    errors.push(ValidationError::new(
                        "after",
                        "delete events must not include after",
                    ));
                }
            }
            Operation::Read => {
                if self.after.is_none() {
                    errors.push(ValidationError::new(
                        "after",
                        "read events must include after",
                    ));
                }
            }
            Operation::SchemaChange => {
                if self.after.is_none() {
                    errors.push(ValidationError::new(
                        "after",
                        "schema_change events must include after",
                    ));
                }
            }
            Operation::Truncate => {
                if self.before.is_some() {
                    errors.push(ValidationError::new(
                        "before",
                        "truncate events must not include before",
                    ));
                }
                if self.after.is_some() {
                    errors.push(ValidationError::new(
                        "after",
                        "truncate events must not include after",
                    ));
                }
            }
        }

        if let Some(transaction) = &self.transaction {
            if transaction.total_events == 0 {
                errors.push(ValidationError::new(
                    "transaction.total_events",
                    "total_events must be greater than zero",
                ));
            }
            if transaction.event_index >= transaction.total_events {
                errors.push(ValidationError::new(
                    "transaction.event_index",
                    "event_index must be lower than total_events",
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Convert validation failures into the crate's shared error type.
    pub fn validate_or_error(&self) -> Result<()> {
        self.validate().map_err(|errors| {
            Error::ValidationError(
                errors
                    .into_iter()
                    .map(|error| format!("{}: {}", error.field, error.message))
                    .collect(),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::core::Error;

    use super::{
        Event, Operation, SnapshotMetadata, SourceMetadata, TransactionMetadata,
        EVENT_ENVELOPE_VERSION,
    };

    fn valid_event() -> Event {
        Event {
            before: None,
            after: Some(json!({"id": 1, "name": "alice"})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "postgres".into(),
                offset: "0/16B6A70".into(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".into()),
            table: "users".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: Some(SnapshotMetadata {
                snapshot_id: "snap-1".into(),
                chunk_index: 0,
                is_last_chunk: false,
            }),
            transaction: Some(TransactionMetadata {
                tx_id: 42,
                total_events: 2,
                event_index: 0,
            }),
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    #[test]
    fn round_trip_json_preserves_event() {
        let event = valid_event();
        let encoded = event.to_json().unwrap();
        let decoded = Event::from_json(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn valid_event_passes_validation() {
        assert!(valid_event().validate().is_ok());
    }

    #[test]
    fn invalid_insert_reports_multiple_errors() {
        let mut event = valid_event();
        event.before = Some(json!({"id": 1}));
        event.after = None;
        event.table.clear();
        event.ts = 0;
        event.envelope_version = 99;

        let errors = event.validate().unwrap_err();
        assert!(errors.iter().any(|error| error.field == "before"));
        assert!(errors.iter().any(|error| error.field == "after"));
        assert!(errors.iter().any(|error| error.field == "table"));
        assert!(errors.iter().any(|error| error.field == "ts"));
        assert!(errors.iter().any(|error| error.field == "envelope_version"));
    }

    #[test]
    fn invalid_json_returns_error_not_panic() {
        let error = Event::from_json("{").unwrap_err();
        assert!(matches!(error, crate::core::Error::SerializationError(_)));
    }

    #[test]
    fn large_payload_round_trip_is_supported() {
        let mut event = valid_event();
        event.after = Some(json!({"blob": "x".repeat(1024 * 1024)}));
        let encoded = event.to_json().unwrap();
        let decoded = Event::from_json(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn operation_display_uses_stable_lowercase_labels() {
        assert_eq!(Operation::Insert.to_string(), "insert");
        assert_eq!(Operation::Update.to_string(), "update");
        assert_eq!(Operation::Delete.to_string(), "delete");
        assert_eq!(Operation::Read.to_string(), "read");
        assert_eq!(Operation::SchemaChange.to_string(), "schema_change");
    }

    #[test]
    fn update_delete_read_validation_paths_enforce_contract() {
        let mut update = valid_event();
        update.op = Operation::Update;
        update.before = None;
        let update_errors = update.validate().unwrap_err();
        assert!(update_errors.iter().any(|error| error.field == "before"));

        let mut delete = valid_event();
        delete.op = Operation::Delete;
        delete.before = None;
        delete.after = Some(json!({"id": 1}));
        let delete_errors = delete.validate().unwrap_err();
        assert!(delete_errors.iter().any(|error| error.field == "before"));
        assert!(delete_errors.iter().any(|error| error.field == "after"));

        let mut read = valid_event();
        read.op = Operation::Read;
        read.after = None;
        let read_errors = read.validate().unwrap_err();
        assert!(read_errors.iter().any(|error| error.field == "after"));

        let mut schema_change = valid_event();
        schema_change.op = Operation::SchemaChange;
        schema_change.after = None;
        let schema_change_errors = schema_change.validate().unwrap_err();
        assert!(schema_change_errors
            .iter()
            .any(|error| error.field == "after"));
    }

    #[test]
    fn transaction_validation_rejects_invalid_bounds() {
        let mut event = valid_event();
        event.transaction = Some(TransactionMetadata {
            tx_id: 9,
            total_events: 0,
            event_index: 0,
        });
        let errors = event.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|error| error.field == "transaction.total_events"));

        event.transaction = Some(TransactionMetadata {
            tx_id: 9,
            total_events: 2,
            event_index: 2,
        });
        let errors = event.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|error| error.field == "transaction.event_index"));
    }

    #[test]
    fn validate_or_error_maps_to_validation_error_type() {
        let mut event = valid_event();
        event.source.source_name = String::new();
        let error = event.validate_or_error().unwrap_err();
        match error {
            Error::ValidationError(messages) => {
                assert!(messages
                    .iter()
                    .any(|message| message.contains("source.source_name")));
            }
            other => panic!("expected ValidationError, got {other}"),
        }
    }
}
