//! Canonical event envelope definitions and validation helpers.

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::{Error, Result};

/// Current version of the canonical event envelope.
pub const EVENT_ENVELOPE_VERSION: u16 = 1;

/// CRUD-style operations emitted by a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Operation {
    #[default]
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

    /// Returns `true` for INSERT, UPDATE, and DELETE operations.
    ///
    /// Use this to filter out READ, SCHEMA_CHANGE, and TRUNCATE events when
    /// you only care about row-level data mutations.
    #[inline]
    pub const fn is_data_change(self) -> bool {
        matches!(self, Self::Insert | Self::Update | Self::Delete)
    }

    /// Returns `true` for INSERT events.
    #[inline]
    pub const fn is_insert(self) -> bool {
        matches!(self, Self::Insert)
    }

    /// Returns `true` for UPDATE events.
    #[inline]
    pub const fn is_update(self) -> bool {
        matches!(self, Self::Update)
    }

    /// Returns `true` for DELETE events.
    #[inline]
    pub const fn is_delete(self) -> bool {
        matches!(self, Self::Delete)
    }

    /// Returns `true` for READ events (emitted during snapshot).
    #[inline]
    pub const fn is_read(self) -> bool {
        matches!(self, Self::Read)
    }

    /// Returns `true` for SCHEMA_CHANGE events.
    #[inline]
    pub const fn is_schema_change(self) -> bool {
        matches!(self, Self::SchemaChange)
    }

    /// Returns `true` for TRUNCATE events.
    #[inline]
    pub const fn is_truncate(self) -> bool {
        matches!(self, Self::Truncate)
    }
}

impl std::str::FromStr for Operation {
    type Err = Error;

    /// Parse an `Operation` from its canonical string form.
    ///
    /// Accepts the same lowercase snake_case strings produced by [`Operation::to_str`] and
    /// [`Display`](std::fmt::Display). Parsing is case-sensitive.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ValidationError`] when the string does not match any known variant.
    ///
    /// # Example
    ///
    /// ```
    /// use std::str::FromStr;
    /// use rustcdc::Operation;
    ///
    /// assert_eq!(Operation::from_str("insert").unwrap(), Operation::Insert);
    /// assert_eq!(Operation::from_str("schema_change").unwrap(), Operation::SchemaChange);
    /// assert!(Operation::from_str("INSERT").is_err()); // case-sensitive
    /// ```
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "insert" => Ok(Self::Insert),
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            "read" => Ok(Self::Read),
            "schema_change" => Ok(Self::SchemaChange),
            "truncate" => Ok(Self::Truncate),
            other => Err(Error::ValidationError(vec![format!(
                "unknown operation '{}': expected one of insert, update, delete, read, schema_change, truncate",
                other
            )])),
        }
    }
}

/// Source identity and position metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
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
    /// Total number of events expected in the transaction, if reported by the source.
    ///
    /// `None` when the connector does not provide a total-event count for the transaction
    /// (most CDC protocols do not). Connectors that do know the count should set this.
    pub total_events: Option<u32>,
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

impl Display for ValidationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}

impl std::error::Error for ValidationError {}

impl ValidationError {
    fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

/// All validation failures from a single [`Event::validate`] call.
///
/// Returned as the `Err` variant of [`Event::validate`] so callers can access
/// each field-level error individually or format the whole list as a single string.
///
/// ```
/// use rustcdc::Event;
///
/// let event = Event::default(); // ts == 0 → invalid
/// let err = event.validate().unwrap_err();
/// // Display: semicolon-joined list of all violations
/// println!("{err}");
/// // Iterate individual errors
/// for e in err.errors() {
///     println!("  field={} msg={}", e.field, e.message);
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationErrors(Vec<ValidationError>);

impl ValidationErrors {
    fn new(errors: Vec<ValidationError>) -> Self {
        Self(errors)
    }

    /// Returns a slice of the individual field-level validation failures.
    pub fn errors(&self) -> &[ValidationError] {
        &self.0
    }

    /// Consume this wrapper and return the owned error list.
    pub fn into_errors(self) -> Vec<ValidationError> {
        self.0
    }

    /// Number of distinct validation failures.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` when there are no validation failures.
    ///
    /// This is always `false` in practice — `ValidationErrors` is only
    /// constructed when at least one error exists.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterate over individual field-level validation failures.
    pub fn iter(&self) -> std::slice::Iter<'_, ValidationError> {
        self.0.iter()
    }
}

impl<'a> IntoIterator for &'a ValidationErrors {
    type Item = &'a ValidationError;
    type IntoIter = std::slice::Iter<'a, ValidationError>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl IntoIterator for ValidationErrors {
    type Item = ValidationError;
    type IntoIter = std::vec::IntoIter<ValidationError>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl Display for ValidationErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let messages: Vec<String> = self.0.iter().map(|e| e.to_string()).collect();
        write!(f, "{}", messages.join("; "))
    }
}

impl std::error::Error for ValidationErrors {}

impl From<ValidationErrors> for Error {
    fn from(errs: ValidationErrors) -> Self {
        Self::ValidationError(errs.0.iter().map(|e| e.to_string()).collect())
    }
}

/// Canonical event envelope used across all sources.
///
/// # Examples
///
/// ```
/// use rustcdc::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
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
///     before_is_key_only: false,
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
    /// Advisory flag — set to `true` when the `before` field contains only
    /// primary-key columns rather than the full pre-image row.
    ///
    /// This occurs on PostgreSQL UPDATE and DELETE events when the table's
    /// `REPLICA IDENTITY` is `DEFAULT` (the factory default). In that mode,
    /// PostgreSQL only includes the old primary key values in the WAL record
    /// rather than the complete before-image. Applications that compute row diffs
    /// or need the full prior state must check this flag; when it is `true`,
    /// `before` cannot be used as a complete row snapshot.
    ///
    /// Always `false` for INSERT, READ, SCHEMA_CHANGE, and TRUNCATE events, and
    /// for all MySQL / MariaDB / SQL Server events.
    #[serde(default)]
    pub before_is_key_only: bool,
}

impl Default for Event {
    /// Returns a minimal INSERT event skeleton with correct `envelope_version`.
    ///
    /// All fields default to empty/zero; callers must set `table`, `ts`, and
    /// other required fields before passing the event to validation or encoding.
    fn default() -> Self {
        Self {
            before: None,
            after: None,
            op: Operation::default(),
            source: SourceMetadata::default(),
            ts: 0,
            schema: None,
            table: String::new(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
        }
    }
}

impl Event {
    /// Serialize the event to compact JSON.
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    /// Serialize the event to compact JSON bytes.
    ///
    /// Prefer this over `to_json()` when you need a `Vec<u8>` directly (e.g. for
    /// Kafka message values, HTTP request bodies). Avoids a UTF-8 round-trip.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Deserialize an event from JSON.
    pub fn from_json(input: &str) -> Result<Self> {
        Ok(serde_json::from_str(input)?)
    }

    /// Deserialize an event from JSON bytes.
    pub fn from_json_bytes(input: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(input)?)
    }

    /// Return the fully-qualified table name as `"schema.table"` when a schema
    /// is present, or just `"table"` when no schema was provided by the source.
    ///
    /// Useful for routing, logging, and constructing Kafka topic names.
    ///
    /// # Example
    ///
    /// ```
    /// use rustcdc::{Event, Operation, EVENT_ENVELOPE_VERSION};
    ///
    /// let mut event = Event { table: "orders".into(), ..Event::default() };
    /// assert_eq!(event.qualified_table_name(), "orders");
    ///
    /// event.schema = Some("public".into());
    /// assert_eq!(event.qualified_table_name(), "public.orders");
    /// ```
    pub fn qualified_table_name(&self) -> String {
        match &self.schema {
            Some(schema) if !schema.is_empty() => format!("{}.{}", schema, self.table),
            _ => self.table.clone(),
        }
    }

    /// Validate the event against the canonical envelope contract.
    ///
    /// Returns `Ok(())` when the event satisfies all envelope constraints.
    /// Returns `Err(ValidationErrors)` with every violated constraint when one
    /// or more fields fail. Use [`ValidationErrors::errors()`] to iterate the
    /// individual failures or `Display` to format them as a joined string.
    pub fn validate(&self) -> std::result::Result<(), ValidationErrors> {
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
            if let Some(total) = transaction.total_events {
                if total == 0 {
                    errors.push(ValidationError::new(
                        "transaction.total_events",
                        "total_events must be greater than zero when set",
                    ));
                }
                if transaction.event_index >= total {
                    errors.push(ValidationError::new(
                        "transaction.event_index",
                        "event_index must be lower than total_events",
                    ));
                }
            }
        }

        if self.before_is_key_only && self.op != Operation::Update && self.op != Operation::Delete {
            errors.push(ValidationError::new(
                "before_is_key_only",
                "before_is_key_only can only be true for UPDATE or DELETE events",
            ));
        }

        if self.before_is_key_only && self.before.is_none() {
            errors.push(ValidationError::new(
                "before_is_key_only",
                "before_is_key_only is true but before is None; \
                 key-only before-images must carry at least the primary-key columns in before",
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ValidationErrors::new(errors))
        }
    }

    /// Convert validation failures into the crate's shared error type.
    ///
    /// Equivalent to `event.validate().map_err(Error::from)`.
    pub fn validate_or_error(&self) -> Result<()> {
        self.validate().map_err(Error::from)
    }

    /// Returns `true` when a full pre-image row is available in `before`.
    ///
    /// This is `true` iff `before` is `Some` **and** `before_is_key_only` is `false`.
    ///
    /// Use this instead of checking `before.is_some()` alone when you need the
    /// complete prior row state — for example, when computing row diffs or emitting
    /// before-images to a downstream store. A `before` field that is `Some` but
    /// `before_is_key_only == true` contains only primary-key columns and cannot
    /// be used as a complete row snapshot.
    ///
    /// # Example
    ///
    /// ```
    /// use rustcdc::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    /// use serde_json::json;
    ///
    /// let mut event = Event {
    ///     before: Some(json!({"id": 1})),
    ///     after: Some(json!({"id": 1, "name": "bob"})),
    ///     op: Operation::Update,
    ///     before_is_key_only: true,
    ///     ..Event::default()
    /// };
    /// event.ts = 1;
    /// event.table = "users".into();
    /// event.source.source_name = "pg".into();
    ///
    /// // Key-only before: `before` is present but partial.
    /// assert!(!event.has_full_before());
    ///
    /// event.before_is_key_only = false;
    /// assert!(event.has_full_before());
    /// ```
    #[inline]
    pub fn has_full_before(&self) -> bool {
        self.before.is_some() && !self.before_is_key_only
    }

    /// Extracts the primary-key column values from the most appropriate row image.
    ///
    /// Returns a JSON object containing only the columns listed in `primary_key`,
    /// taken from `after` for INSERT / UPDATE / READ / SCHEMA_CHANGE, and from
    /// `before` for DELETE. Returns `None` when:
    ///
    /// - `primary_key` is `None` or empty.
    /// - The relevant row image (`after` or `before`) is absent or not a JSON object.
    ///
    /// This is the canonical source for Kafka message keys and idempotency
    /// fingerprints derived from primary-key values alone.
    ///
    /// # Example
    ///
    /// ```
    /// use rustcdc::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    /// use serde_json::json;
    ///
    /// let event = Event {
    ///     before: None,
    ///     after: Some(json!({"id": 42, "name": "alice", "age": 30})),
    ///     op: Operation::Insert,
    ///     primary_key: Some(vec!["id".into()]),
    ///     ..Event::default()
    /// };
    ///
    /// let key = event.primary_key_values().unwrap();
    /// assert_eq!(key["id"], json!(42));
    /// assert!(key.get("name").is_none());
    /// ```
    pub fn primary_key_values(&self) -> Option<serde_json::Value> {
        let keys = self.primary_key.as_deref()?;
        if keys.is_empty() {
            return None;
        }

        let row = match self.op {
            Operation::Delete => self.before.as_ref(),
            _ => self.after.as_ref().or(self.before.as_ref()),
        };

        let obj = row?.as_object()?;
        let mut result = serde_json::Map::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = obj.get(key) {
                result.insert(key.clone(), value.clone());
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(result))
        }
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
                total_events: Some(2),
                event_index: 0,
            }),
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
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
            total_events: Some(0),
            event_index: 0,
        });
        let errors = event.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|error| error.field == "transaction.total_events"));

        event.transaction = Some(TransactionMetadata {
            tx_id: 9,
            total_events: Some(2),
            event_index: 2,
        });
        let errors = event.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|error| error.field == "transaction.event_index"));
    }

    #[test]
    fn before_is_key_only_rejected_on_non_update_delete_events() {
        for op in [
            Operation::Insert,
            Operation::Read,
            Operation::SchemaChange,
            Operation::Truncate,
        ] {
            let mut event = valid_event();
            event.op = op;
            event.before_is_key_only = true;
            // Adjust before/after to satisfy per-op contract so only the flag fires.
            match op {
                Operation::Insert | Operation::Read | Operation::SchemaChange => {
                    event.before = None;
                    event.after = Some(json!({"id": 1}));
                }
                Operation::Truncate => {
                    event.before = None;
                    event.after = None;
                }
                _ => {}
            }
            let errors = event.validate().unwrap_err();
            assert!(
                errors.iter().any(|e| e.field == "before_is_key_only"),
                "expected before_is_key_only error for op={op:?}"
            );
        }
    }

    #[test]
    fn before_is_key_only_accepted_on_update_and_delete_events() {
        // UPDATE with key-only before
        let mut update = valid_event();
        update.op = Operation::Update;
        update.before = Some(json!({"id": 1}));
        update.after = Some(json!({"id": 1, "name": "bob"}));
        update.before_is_key_only = true;
        assert!(
            update.validate().is_ok(),
            "UPDATE should allow before_is_key_only=true"
        );

        // DELETE with key-only before
        let mut delete = valid_event();
        delete.op = Operation::Delete;
        delete.before = Some(json!({"id": 1}));
        delete.after = None;
        delete.before_is_key_only = true;
        assert!(
            delete.validate().is_ok(),
            "DELETE should allow before_is_key_only=true"
        );
    }

    #[test]
    fn before_is_key_only_true_requires_before_to_be_some() {
        // before_is_key_only = true with before = None is always invalid, regardless of op.
        for op in [Operation::Update, Operation::Delete] {
            let mut event = valid_event();
            event.op = op;
            event.before = None; // ← no before image at all
            event.before_is_key_only = true;
            if op == Operation::Update {
                event.after = Some(json!({"id": 1}));
            }
            let errors = event.validate().unwrap_err();
            assert!(
                errors.iter().any(|e| e.field == "before_is_key_only"),
                "expected before_is_key_only error when before=None for op={op:?}; got: {errors}"
            );
        }
    }

    #[test]
    fn event_default_has_correct_envelope_version() {
        let event = Event::default();
        assert_eq!(event.envelope_version, EVENT_ENVELOPE_VERSION);
        assert!(!event.before_is_key_only);
        assert_eq!(event.op, Operation::Insert);
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

    #[test]
    fn has_full_before_distinguishes_key_only_from_full() {
        let base = Event {
            before: Some(json!({"id": 1, "name": "alice"})),
            after: Some(json!({"id": 1, "name": "bob"})),
            op: Operation::Update,
            before_is_key_only: false,
            ..Event::default()
        };
        assert!(base.has_full_before(), "full before should return true");

        let key_only = Event {
            before_is_key_only: true,
            ..base.clone()
        };
        assert!(
            !key_only.has_full_before(),
            "key-only before should return false"
        );

        let no_before = Event {
            before: None,
            before_is_key_only: false,
            ..base
        };
        assert!(
            !no_before.has_full_before(),
            "absent before should return false"
        );
    }

    #[test]
    fn primary_key_values_extracts_from_after_on_insert() {
        let event = Event {
            after: Some(json!({"id": 42, "name": "alice", "age": 30})),
            op: Operation::Insert,
            primary_key: Some(vec!["id".into()]),
            ..Event::default()
        };
        let kv = event.primary_key_values().unwrap();
        assert_eq!(kv["id"], json!(42));
        assert!(kv.get("name").is_none());
    }

    #[test]
    fn primary_key_values_extracts_from_before_on_delete() {
        let event = Event {
            before: Some(json!({"id": 7, "name": "bob"})),
            after: None,
            op: Operation::Delete,
            primary_key: Some(vec!["id".into()]),
            ..Event::default()
        };
        let kv = event.primary_key_values().unwrap();
        assert_eq!(kv["id"], json!(7));
    }

    #[test]
    fn primary_key_values_returns_none_when_no_pk_defined() {
        let event = Event {
            after: Some(json!({"id": 1})),
            op: Operation::Insert,
            primary_key: None,
            ..Event::default()
        };
        assert!(event.primary_key_values().is_none());
    }

    #[test]
    fn primary_key_values_returns_none_when_pk_fields_absent_from_row() {
        let event = Event {
            after: Some(json!({"name": "only_name"})),
            op: Operation::Insert,
            primary_key: Some(vec!["id".into()]),
            ..Event::default()
        };
        // "id" not present in `after`, so result should be None
        assert!(event.primary_key_values().is_none());
    }

    #[test]
    fn primary_key_values_handles_composite_keys() {
        let event = Event {
            after: Some(json!({"tenant_id": 1, "user_id": 99, "name": "charlie"})),
            op: Operation::Insert,
            primary_key: Some(vec!["tenant_id".into(), "user_id".into()]),
            ..Event::default()
        };
        let kv = event.primary_key_values().unwrap();
        assert_eq!(kv["tenant_id"], json!(1));
        assert_eq!(kv["user_id"], json!(99));
        assert!(kv.get("name").is_none());
    }

    #[test]
    fn operation_from_str_parses_all_variants() {
        use std::str::FromStr;
        assert_eq!(Operation::from_str("insert").unwrap(), Operation::Insert);
        assert_eq!(Operation::from_str("update").unwrap(), Operation::Update);
        assert_eq!(Operation::from_str("delete").unwrap(), Operation::Delete);
        assert_eq!(Operation::from_str("read").unwrap(), Operation::Read);
        assert_eq!(
            Operation::from_str("schema_change").unwrap(),
            Operation::SchemaChange
        );
        assert_eq!(
            Operation::from_str("truncate").unwrap(),
            Operation::Truncate
        );
    }

    #[test]
    fn operation_from_str_rejects_unknown_and_wrong_case() {
        use std::str::FromStr;
        assert!(Operation::from_str("INSERT").is_err()); // case-sensitive
        assert!(Operation::from_str("unknown").is_err());
        assert!(Operation::from_str("").is_err());
    }

    #[test]
    fn operation_round_trips_through_str() {
        use std::str::FromStr;
        for op in [
            Operation::Insert,
            Operation::Update,
            Operation::Delete,
            Operation::Read,
            Operation::SchemaChange,
            Operation::Truncate,
        ] {
            assert_eq!(
                Operation::from_str(op.to_str()).unwrap(),
                op,
                "round-trip failed for {op}"
            );
        }
    }

    #[test]
    fn validation_errors_display_joins_all_failures() {
        let event = Event::default(); // ts == 0, table empty, source_name empty
        let errs = event.validate().unwrap_err();
        assert!(errs.len() >= 3); // at least ts, table, source_name
        let display = errs.to_string();
        assert!(display.contains("ts"));
        assert!(display.contains("table"));
    }

    #[test]
    fn validation_errors_iterates_individually() {
        let event = Event::default();
        let errs = event.validate().unwrap_err();
        let fields: Vec<&str> = errs.iter().map(|e| e.field.as_str()).collect();
        assert!(fields.contains(&"ts"));
        assert!(fields.contains(&"table"));
    }

    #[test]
    fn validation_errors_into_iter_consuming_works() {
        let event = Event::default();
        let errs = event.validate().unwrap_err();
        let count = errs.len();
        let collected: Vec<_> = errs.into_iter().collect();
        assert_eq!(collected.len(), count);
    }

    #[test]
    fn validation_error_implements_std_error() {
        use std::error::Error as StdError;
        let ve = super::ValidationError {
            field: "ts".into(),
            message: "must be non-zero".into(),
        };
        // std::error::Error is object-safe; can be used as dyn Error
        let _: &dyn StdError = &ve;
    }

    #[test]
    fn qualified_table_name_includes_schema_when_present() {
        let mut event = Event {
            table: "orders".into(),
            ..Event::default()
        };
        assert_eq!(event.qualified_table_name(), "orders");
        event.schema = Some("public".into());
        assert_eq!(event.qualified_table_name(), "public.orders");
    }

    #[test]
    fn qualified_table_name_ignores_empty_schema() {
        let event = Event {
            table: "users".into(),
            schema: Some(String::new()),
            ..Event::default()
        };
        assert_eq!(event.qualified_table_name(), "users");
    }
}
