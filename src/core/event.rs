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
    /// A row was created. `before` is `None`; `after` carries the new row.
    #[default]
    Insert,
    /// A row was modified. `after` carries the new image; `before` carries the old one,
    /// subject to the source's replica-identity / row-image configuration.
    Update,
    /// A row was removed. `after` is `None`; `before` identifies the deleted row.
    Delete,
    /// A row read during a snapshot, not a live change.
    ///
    /// `before` is `None` and `after` carries the row as of the snapshot's consistent
    /// view. Carries [`SnapshotMetadata`]; a live change never does.
    Read,
    /// A DDL statement changed a captured table's schema.
    ///
    /// Recorded in the durable schema history **before** this event is delivered, so a
    /// consumer can never observe a schema change the history lacks.
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
#[non_exhaustive]
pub struct SourceMetadata {
    /// Logical name of the source connector.
    pub source_name: String,
    /// Source-specific durable position encoded as a string.
    pub offset: String,
    /// Source commit timestamp in Unix epoch milliseconds.
    ///
    /// # Resolution differs by connector, and MySQL's is coarse
    ///
    /// | Connector | Source | Resolution |
    /// |---|---|---|
    /// | PostgreSQL | pgoutput `COMMIT` message | microseconds — exact |
    /// | SQL Server | `sys.fn_cdc_map_lsn_to_time` | milliseconds — exact |
    /// | MySQL / MariaDB | binlog common header | **whole seconds** |
    ///
    /// The MySQL binlog header stores the commit time in seconds, so this field is
    /// truncated *down* to the second: a row committed at `T+0.999s` reports `T+0.000s`.
    /// Lag computed as `now - timestamp` is therefore **over-reported by up to 1,000 ms**
    /// on MySQL and MariaDB — measured median over-report against MySQL 8 is ~480 ms,
    /// the expected half-second for uniformly distributed sub-second commits.
    ///
    /// This matters when setting alert thresholds: a MySQL pipeline reporting
    /// `replication_lag_ms` near 500 ms may be running at near-zero real lag. Do not set
    /// a MySQL lag alert below about 2 s, and prefer alerting on the *derivative* rather
    /// than the level. For an exact figure, timestamp the row in the application and
    /// compare against that instead.
    pub timestamp: u64,
}

/// Snapshot progress information when an event is emitted during snapshotting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
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
#[non_exhaustive]
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
/// let event = Event::builder("users", Operation::Insert)
///     .after(json!({"id": 1, "name": "alice"}))
///     .source(SourceMetadata::new("postgres", "0/16B6A70", 1))
///     .ts(1)
///     .schema("public")
///     .primary_key(["id"])
///     .build();
///
/// let encoded = event.to_json().unwrap();
/// let decoded = Event::from_json(&encoded).unwrap();
/// assert_eq!(decoded.table, "users");
/// assert!(decoded.validate().is_ok());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
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
    /// Columns that exist on the table but whose value the source could not supply.
    ///
    /// These columns are **absent** from `before`/`after` — not `null`. Without this
    /// list the two cases are indistinguishable, which is the classic CDC data-loss
    /// footgun: a consumer performing a full-row upsert from `after` writes `NULL`
    /// (or the column default) over a value that never actually changed.
    ///
    /// The concrete case today is **PostgreSQL unchanged-TOAST**. When a large value
    /// (roughly >8 KB: text, bytea, jsonb) is not modified by an UPDATE, PostgreSQL
    /// omits it from the WAL record entirely and pgoutput sends the `'u'` placeholder.
    /// The value cannot be recovered — reading it back from the table out-of-band
    /// would race concurrent writes and yield a value from a different point in time,
    /// so there is no safe way to fill it in.
    ///
    /// **One exception, and it is not an out-of-band read.** During an incremental
    /// snapshot, an event inside a chunk's watermark bracket is repaired from *that
    /// chunk's own image* of the row, and arrives with this list empty. That is sound
    /// where a fresh read is not: the value comes from a `SELECT` at a snapshot whose
    /// position the driver knows, `unavailable_columns` means the UPDATE did not modify
    /// those columns, and every write between the read and the event is itself inside the
    /// bracket and already folded in. See
    /// [`IncrementalSnapshotDriver`](crate::source::IncrementalSnapshotDriver). Outside
    /// that window the paragraph above stands.
    ///
    /// **A consumer that writes whole rows must exclude these columns from the write**
    /// (e.g. omit them from the `SET` clause of an upsert) rather than writing NULL.
    ///
    /// Empty for every event whose `after` image is complete.
    ///
    /// This list describes **`after` only**. The before-image has its own holes, tracked
    /// separately by [`Event::before_unavailable_columns`] — they are not the same set. A
    /// TOASTed column that *was* modified arrives present in `after` and `'u'` in
    /// `before`, so merging the two lists would mark a column that genuinely changed as
    /// unwritable and silently drop the update.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unavailable_columns: Vec<String>,
    /// Columns absent from `before` for the same reason as [`Event::unavailable_columns`].
    ///
    /// Only relevant to consumers that use the before-image — computing diffs, or
    /// building compensating writes. A column listed here had *some* prior value; the
    /// source simply could not report it. Do not read its absence as "was NULL".
    ///
    /// Always empty when [`Event::before_is_key_only`] is `true`: a key-only before-image
    /// is already known to be incomplete, and its non-key columns are absent by design
    /// rather than by TOAST.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub before_unavailable_columns: Vec<String>,
}

/// Why an [`Event`] yields no row write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NoRowWrite {
    /// A DDL event. It changes the table, not any row.
    SchemaChange,
    /// The event carries no row payload where one was expected.
    MissingPayload,
    /// The write needs a primary key to target a row, and the event has none.
    ///
    /// Either `primary_key` is unset, or the key columns are absent from the payload.
    /// A keyless partial payload cannot be applied safely at all: there is no way to
    /// address the row *and* no way to reconstruct the missing columns.
    MissingPrimaryKey,
}

/// The only row write that is correct for a given [`Event`].
///
/// Obtain one with [`Event::row_write`]. The point of this type is that the
/// data-corrupting write is not expressible: when a payload is incomplete you get
/// [`RowWrite::Merge`], which hands you *only* the columns that are actually present,
/// so there is nothing to accidentally write `NULL` from.
///
/// See the module documentation on [`Event::unavailable_columns`] for why incomplete
/// payloads occur.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RowWrite<'a> {
    /// The payload is a complete row. Write every column.
    ///
    /// `key` is `None` when the event declares no primary key — append-only sinks can
    /// still insert; keyed sinks should treat that as a configuration error.
    Replace {
        /// Primary-key columns and values, when the event declares a key.
        key: Option<serde_json::Value>,
        /// The complete row.
        row: &'a serde_json::Value,
    },
    /// The payload is **incomplete**. Write only `columns`, and leave every other column
    /// in the target row untouched.
    ///
    /// In SQL terms this is `UPDATE ... SET <columns> WHERE <key>` — never an upsert
    /// built from the full column list, and never `INSERT ... ON CONFLICT DO UPDATE SET`
    /// with a column list wider than `columns`.
    Merge {
        /// Primary-key columns and values identifying the row to update.
        key: serde_json::Value,
        /// The columns whose values the source did supply. Write exactly these.
        columns: &'a serde_json::Value,
        /// Columns the source could not supply. Their current values in the target row
        /// are still correct and must be preserved.
        unavailable_columns: &'a [String],
    },
    /// Delete the row identified by `key`.
    Delete {
        /// Primary-key columns and values identifying the row to delete.
        key: serde_json::Value,
    },
    /// Remove every row in the table.
    Truncate,
    /// No row write applies.
    None {
        /// Why.
        reason: NoRowWrite,
    },
}

impl RowWrite<'_> {
    /// `true` when applying this write requires preserving columns already in the target
    /// row — i.e. it is a [`RowWrite::Merge`].
    ///
    /// A sink that cannot express a partial update (an append-only file, a full-row
    /// document replace) must branch on this rather than flattening the payload.
    pub fn is_partial(&self) -> bool {
        matches!(self, Self::Merge { .. })
    }
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
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }
}

impl Event {
    /// The one row write that is correct for this event.
    ///
    /// Prefer this over reading [`Event::after`] directly in a sink. It folds
    /// [`Event::unavailable_columns`] and the primary key into a single decision, so the
    /// classic CDC corruption — upserting a full row from a payload that was missing
    /// columns, writing `NULL` over values that never changed — cannot be written by
    /// accident.
    ///
    /// ```
    /// use rustcdc::core::{Event, Operation, RowWrite};
    /// use serde_json::json;
    ///
    /// # fn sink(event: &Event) {
    /// match event.row_write() {
    ///     RowWrite::Replace { key, row } => { /* write every column */ }
    ///     RowWrite::Merge { key, columns, .. } => { /* UPDATE SET only `columns` */ }
    ///     RowWrite::Delete { key } => { /* delete by key */ }
    ///     RowWrite::Truncate => { /* clear the table */ }
    ///     RowWrite::None { reason } => { /* DDL, or no addressable row */ }
    ///     _ => {}
    /// }
    /// # }
    /// ```
    pub fn row_write(&self) -> RowWrite<'_> {
        match self.op {
            Operation::Truncate => RowWrite::Truncate,
            Operation::SchemaChange => RowWrite::None {
                reason: NoRowWrite::SchemaChange,
            },
            Operation::Delete => match self.primary_key_values() {
                Some(key) => RowWrite::Delete { key },
                // Without a key there is no way to name the row to remove. Deleting on a
                // guess is worse than surfacing the gap.
                None => RowWrite::None {
                    reason: NoRowWrite::MissingPrimaryKey,
                },
            },
            Operation::Insert | Operation::Update | Operation::Read => {
                let Some(row) = self.after.as_ref() else {
                    return RowWrite::None {
                        reason: NoRowWrite::MissingPayload,
                    };
                };
                if self.unavailable_columns.is_empty() {
                    return RowWrite::Replace {
                        key: self.primary_key_values(),
                        row,
                    };
                }
                match self.primary_key_values() {
                    Some(key) => RowWrite::Merge {
                        key,
                        columns: row,
                        unavailable_columns: &self.unavailable_columns,
                    },
                    None => RowWrite::None {
                        reason: NoRowWrite::MissingPrimaryKey,
                    },
                }
            }
        }
    }

    /// `true` when `after` holds every column of the row.
    ///
    /// `false` means some columns are absent and their prior values must be preserved —
    /// see [`Event::unavailable_columns`].
    pub fn has_complete_after_image(&self) -> bool {
        self.unavailable_columns.is_empty()
    }

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
    /// let mut event = Event::builder("orders", Operation::Insert).build();
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

        // "Unavailable" means *absent*. A column that is both listed and present is a
        // contradiction, and the dangerous reading wins: a sink that trusts the payload
        // writes whatever placeholder is sitting there. Each list is checked against its
        // own image — they describe different sets and must never be merged.
        let contradicts = |columns: &[String], row: &Option<serde_json::Value>| {
            let Some(object) = row.as_ref().and_then(serde_json::Value::as_object) else {
                return false;
            };
            columns.iter().any(|column| object.contains_key(column))
        };

        if contradicts(&self.unavailable_columns, &self.after) {
            errors.push(ValidationError::new(
                "unavailable_columns",
                "a column listed in unavailable_columns must be absent from after; \
                 emitting it with a placeholder value defeats the purpose of the list",
            ));
        }

        if contradicts(&self.before_unavailable_columns, &self.before) {
            errors.push(ValidationError::new(
                "before_unavailable_columns",
                "a column listed in before_unavailable_columns must be absent from before; \
                 emitting it with a placeholder value defeats the purpose of the list",
            ));
        }

        if self.before_is_key_only && !self.before_unavailable_columns.is_empty() {
            errors.push(ValidationError::new(
                "before_unavailable_columns",
                "before_unavailable_columns must be empty when before_is_key_only is true; \
                 a key-only before-image omits non-key columns by design, not by TOAST",
            ));
        }

        if matches!(self.op, Operation::Truncate | Operation::SchemaChange)
            && !(self.unavailable_columns.is_empty() && self.before_unavailable_columns.is_empty())
        {
            errors.push(ValidationError::new(
                "unavailable_columns",
                "unavailable_columns must be empty for TRUNCATE and SCHEMA_CHANGE events, \
                 which carry no row payload",
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
    /// let mut event = Event::builder("users", Operation::Update)
    ///     .before(json!({"id": 1}))
    ///     .after(json!({"id": 1, "name": "bob"}))
    ///     .before_is_key_only(true)
    ///     .source(SourceMetadata::new("pg", "0/1", 1))
    ///     .ts(1)
    ///     .build();
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
    /// - **Any** declared key column is missing from that row image.
    ///
    /// # A partial composite key is never returned
    ///
    /// The last condition is all-or-nothing on purpose, and it is the difference
    /// between a missing write and a catastrophic one. Given `primary_key =
    /// ["tenant_id", "id"]` and a payload carrying only `tenant_id`, returning
    /// `{"tenant_id": 7}` produces a key that *looks* valid and addresses **every row
    /// of that tenant**. A sink turning it into `DELETE FROM t WHERE tenant_id = 7`
    /// deletes the whole tenant; an upsert collapses the tenant onto one row. Both are
    /// silent, and neither is recoverable from the event stream.
    ///
    /// Returning `None` routes the event to
    /// [`RowWrite::None { reason: MissingPrimaryKey }`](RowWrite::None) instead, which a
    /// sink must handle explicitly. A visible gap beats an invisible over-write.
    ///
    /// This is also the canonical source for message keys and idempotency
    /// fingerprints derived from primary-key values alone, and both need the same
    /// guarantee: a truncated key silently merges distinct rows into one compaction
    /// group.
    ///
    /// # Example
    ///
    /// ```
    /// use rustcdc::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    /// use serde_json::json;
    ///
    /// let event = Event::builder("", Operation::Insert)
    ///     .after(json!({"id": 42, "name": "alice", "age": 30}))
    ///     .primary_key(["id"])
    ///     .build();
    ///
    /// let key = event.primary_key_values().unwrap();
    /// assert_eq!(key["id"], json!(42));
    /// assert!(key.get("name").is_none());
    ///
    /// // A composite key with one column missing from the payload yields no key at all,
    /// // rather than one that addresses every row sharing the column that is present.
    /// let partial = Event::builder("", Operation::Insert)
    ///     .after(json!({"tenant_id": 7}))
    ///     .primary_key(["tenant_id", "id"])
    ///     .build();
    /// assert!(partial.primary_key_values().is_none());
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
            // `?` on the lookup, not a skip: a key missing even one column is not a key.
            result.insert(key.clone(), obj.get(key)?.clone());
        }

        Some(serde_json::Value::Object(result))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::core::Error;

    use super::{
        Event, NoRowWrite, Operation, RowWrite, SnapshotMetadata, SourceMetadata,
        TransactionMetadata, EVENT_ENVELOPE_VERSION,
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
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
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
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
            ..Event::default()
        };
        assert!(base.has_full_before(), "full before should return true");

        let key_only = Event {
            before_is_key_only: true,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
            ..base.clone()
        };
        assert!(
            !key_only.has_full_before(),
            "key-only before should return false"
        );

        let no_before = Event {
            before: None,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
            ..base
        };
        assert!(
            !no_before.has_full_before(),
            "absent before should return false"
        );
    }

    /// The core guarantee: an incomplete payload never yields a whole-row write.
    #[test]
    fn an_incomplete_payload_yields_a_merge_not_a_replace() {
        let mut event = valid_event();
        event.op = Operation::Update;
        event.primary_key = Some(vec!["id".into()]);
        // `body` is a TOASTed column PostgreSQL did not ship. It is absent, not null.
        event.before = Some(json!({"id": 1}));
        event.before_is_key_only = true;
        event.after = Some(json!({"id": 1, "title": "new title"}));
        event.unavailable_columns = vec!["body".into()];
        event
            .validate()
            .expect("this is a well-formed partial payload");

        match event.row_write() {
            RowWrite::Merge {
                key,
                columns,
                unavailable_columns,
            } => {
                assert_eq!(key, json!({"id": 1}));
                assert_eq!(columns, &json!({"id": 1, "title": "new title"}));
                assert_eq!(unavailable_columns, ["body".to_string()]);
                assert!(
                    columns.get("body").is_none(),
                    "the unavailable column must not be reachable as a value to write"
                );
            }
            other => panic!("a partial payload must never produce a full-row write: {other:?}"),
        }
        assert!(event.row_write().is_partial());
        assert!(!event.has_complete_after_image());
    }

    #[test]
    fn a_complete_payload_yields_a_replace() {
        let mut event = valid_event();
        event.op = Operation::Update;
        event.primary_key = Some(vec!["id".into()]);
        event.after = Some(json!({"id": 1, "title": "t", "body": "b"}));

        match event.row_write() {
            RowWrite::Replace { key, row } => {
                assert_eq!(key, Some(json!({"id": 1})));
                assert_eq!(row, &json!({"id": 1, "title": "t", "body": "b"}));
            }
            other => panic!("expected a full-row write, got {other:?}"),
        }
        assert!(!event.row_write().is_partial());
        assert!(event.has_complete_after_image());
    }

    /// A partial payload with no key can be applied neither wholly nor partially.
    #[test]
    fn an_incomplete_payload_without_a_key_yields_no_write() {
        let mut event = valid_event();
        event.op = Operation::Update;
        event.primary_key = None;
        event.after = Some(json!({"title": "t"}));
        event.unavailable_columns = vec!["body".into()];

        assert_eq!(
            event.row_write(),
            RowWrite::None {
                reason: NoRowWrite::MissingPrimaryKey
            }
        );
    }

    #[test]
    fn delete_and_truncate_and_ddl_map_to_their_own_writes() {
        let mut delete = valid_event();
        delete.op = Operation::Delete;
        delete.primary_key = Some(vec!["id".into()]);
        delete.after = None;
        delete.before = Some(json!({"id": 7}));
        assert_eq!(
            delete.row_write(),
            RowWrite::Delete {
                key: json!({"id": 7})
            }
        );

        let mut truncate = valid_event();
        truncate.op = Operation::Truncate;
        truncate.after = None;
        assert_eq!(truncate.row_write(), RowWrite::Truncate);

        let mut ddl = valid_event();
        ddl.op = Operation::SchemaChange;
        ddl.after = None;
        assert_eq!(
            ddl.row_write(),
            RowWrite::None {
                reason: NoRowWrite::SchemaChange
            }
        );
    }

    /// Listing a column as unavailable while also emitting it is a contradiction, and the
    /// dangerous reading — trust the payload — is the one a sink would take.
    #[test]
    fn a_column_that_is_both_unavailable_and_present_fails_validation() {
        let mut event = valid_event();
        event.op = Operation::Update;
        event.after = Some(json!({"id": 1, "body": "__unavailable__"}));
        event.unavailable_columns = vec!["body".into()];

        let errors = event.validate().expect_err("this must not validate");
        assert!(errors
            .iter()
            .any(|error| error.field == "unavailable_columns"));
    }

    #[test]
    fn truncate_events_may_not_carry_unavailable_columns() {
        let mut event = valid_event();
        event.op = Operation::Truncate;
        event.before = None;
        event.after = None;
        event.unavailable_columns = vec!["body".into()];

        let errors = event.validate().expect_err("this must not validate");
        assert!(errors
            .iter()
            .any(|error| error.field == "unavailable_columns"));
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

    /// A composite key missing one column addresses every row sharing the rest of it.
    #[test]
    fn primary_key_values_refuses_a_partial_composite_key() {
        // `tenant_id` alone selects the whole tenant. Returning it as "the key" turns a
        // single-row delete into a tenant wipe, with nothing in the event stream to
        // show it happened.
        let event = Event {
            after: Some(json!({"tenant_id": 7, "name": "charlie"})),
            op: Operation::Insert,
            primary_key: Some(vec!["tenant_id".into(), "user_id".into()]),
            ..Event::default()
        };
        assert!(
            event.primary_key_values().is_none(),
            "a partial composite key must not be returned"
        );
    }

    #[test]
    fn a_partial_composite_key_yields_no_row_write_rather_than_a_wide_delete() {
        let delete = Event {
            before: Some(json!({"tenant_id": 7})),
            after: None,
            op: Operation::Delete,
            primary_key: Some(vec!["tenant_id".into(), "user_id".into()]),
            ..Event::default()
        };
        assert_eq!(
            delete.row_write(),
            RowWrite::None {
                reason: NoRowWrite::MissingPrimaryKey
            },
            "a delete with a truncated key must be refused, not widened"
        );

        // The same holds for the upsert direction: a partial key would collapse every
        // row of the tenant onto one.
        let insert = Event {
            after: Some(json!({"tenant_id": 7, "name": "charlie"})),
            op: Operation::Insert,
            primary_key: Some(vec!["tenant_id".into(), "user_id".into()]),
            ..Event::default()
        };
        match insert.row_write() {
            RowWrite::Replace { key, .. } => assert!(
                key.is_none(),
                "a truncated composite key must not be offered as a write key"
            ),
            other => panic!("expected Replace with no key, got {other:?}"),
        }
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

// ─── Builders ─────────────────────────────────────────────────────────────────

impl SourceMetadata {
    /// Build source metadata.
    ///
    /// `offset` must be a **complete, resumable position** for the source — the same
    /// string a later `start_stream(resume_from)` can restart from. The runtime
    /// persists it verbatim for sources it does not natively know, so a partial or
    /// display-only value there resumes capture at the wrong place after a restart.
    pub fn new(source_name: impl Into<String>, offset: impl Into<String>, timestamp: u64) -> Self {
        Self {
            source_name: source_name.into(),
            offset: offset.into(),
            timestamp,
        }
    }
}

impl SnapshotMetadata {
    /// Build snapshot metadata for a chunked read.
    ///
    /// `snapshot_id` must be **stable across restarts** so a consumer correlating rows
    /// by it sees one snapshot rather than one per process lifetime.
    pub fn new(snapshot_id: impl Into<String>, chunk_index: u32, is_last_chunk: bool) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
            chunk_index,
            is_last_chunk,
        }
    }
}

impl TransactionMetadata {
    /// Build transaction metadata.
    ///
    /// `total_events` is `None` when the source cannot report the transaction size
    /// before the transaction ends — which is the normal case for streaming decoders.
    /// A consumer must not treat `None` as zero.
    pub fn new(tx_id: u64, event_index: u32, total_events: Option<u32>) -> Self {
        Self {
            tx_id,
            total_events,
            event_index,
        }
    }
}

/// Fluent builder for [`Event`].
///
/// [`Event`] is `#[non_exhaustive]`, so downstream crates construct it through this
/// builder rather than a struct literal. That is deliberate: adding a field to the
/// envelope is then a non-breaking change, where previously every new field broke every
/// construction site — including the ones in this crate's own published examples.
///
/// The builder sets `envelope_version` for you. Getting that constant wrong by hand is
/// not a compile error but makes the event fail validation at the far end of the
/// pipeline, which is a poor place to learn about it.
///
/// ```
/// use rustcdc::core::{Event, Operation, SourceMetadata};
/// use serde_json::json;
///
/// let event = Event::builder("users", Operation::Insert)
///     .source(SourceMetadata::new("postgres", "0/16B2E48", 1_700_000_000_000))
///     .schema("public")
///     .after(json!({ "id": 1, "email": "a@example.com" }))
///     .primary_key(["id"])
///     .ts(1_700_000_000_000)
///     .build();
///
/// assert!(event.validate().is_ok());
/// ```
#[derive(Debug, Clone)]
pub struct EventBuilder {
    event: Event,
}

impl EventBuilder {
    /// Row state before the operation.
    #[must_use]
    pub fn before(mut self, before: Value) -> Self {
        self.event.before = Some(before);
        self
    }

    /// Row state after the operation.
    #[must_use]
    pub fn after(mut self, after: Value) -> Self {
        self.event.after = Some(after);
        self
    }

    /// Source identity and durable position.
    #[must_use]
    pub fn source(mut self, source: SourceMetadata) -> Self {
        self.event.source = source;
        self
    }

    /// Event timestamp in milliseconds since the Unix epoch.
    #[must_use]
    pub fn ts(mut self, ts: u64) -> Self {
        self.event.ts = ts;
        self
    }

    /// Schema (or database) the table lives in.
    #[must_use]
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.event.schema = Some(schema.into());
        self
    }

    /// Primary-key column names, in key order.
    ///
    /// Order matters: consumers and the incremental-snapshot override window both
    /// derive a row identity from these columns in the order given.
    #[must_use]
    pub fn primary_key<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.event.primary_key = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Snapshot metadata, for an event produced by a snapshot read.
    #[must_use]
    pub fn snapshot(mut self, snapshot: SnapshotMetadata) -> Self {
        self.event.snapshot = Some(snapshot);
        self
    }

    /// Transaction metadata, for a source that reports transaction boundaries.
    #[must_use]
    pub fn transaction(mut self, transaction: TransactionMetadata) -> Self {
        self.event.transaction = Some(transaction);
        self
    }

    /// Mark `before` as containing only primary-key columns rather than a full
    /// pre-image. See [`Event::before_is_key_only`].
    #[must_use]
    pub fn before_is_key_only(mut self, key_only: bool) -> Self {
        self.event.before_is_key_only = key_only;
        self
    }

    /// Columns absent from `after` because the source could not supply them.
    ///
    /// See [`Event::unavailable_columns`] — a consumer that writes whole rows must
    /// exclude these from the write rather than writing `NULL`.
    #[must_use]
    pub fn unavailable_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.event.unavailable_columns = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Columns absent from `before`. Tracked separately from
    /// [`unavailable_columns`](Self::unavailable_columns): the two sets are not the same,
    /// and merging them marks genuinely changed columns as unwritable.
    #[must_use]
    pub fn before_unavailable_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.event.before_unavailable_columns = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Finish without validating.
    ///
    /// Prefer [`build_validated`](Self::build_validated) at a source boundary; an
    /// envelope that violates the contract is otherwise discovered by a sink, far from
    /// the code that produced it.
    #[must_use]
    pub fn build(self) -> Event {
        self.event
    }

    /// Finish, returning an error if the envelope contract is violated.
    pub fn build_validated(self) -> Result<Event> {
        self.event.validate()?;
        Ok(self.event)
    }
}

impl Event {
    /// Start building an event for `table`.
    ///
    /// `envelope_version` is set for you; every other field starts empty.
    pub fn builder(table: impl Into<String>, op: Operation) -> EventBuilder {
        EventBuilder {
            event: Event {
                table: table.into(),
                op,
                ..Event::default()
            },
        }
    }
}

#[cfg(test)]
mod builder_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_builder_sets_the_envelope_version_so_a_caller_cannot_forget_it() {
        // A hand-written literal with the wrong constant is not a compile error; it
        // fails validation at the far end of the pipeline instead.
        let event = Event::builder("users", Operation::Insert).build();
        assert_eq!(event.envelope_version, EVENT_ENVELOPE_VERSION);
    }

    #[test]
    fn a_built_event_round_trips_through_json() {
        let event = Event::builder("users", Operation::Update)
            .source(SourceMetadata::new("postgres", "0/16B2E48", 42))
            .schema("public")
            .before(json!({ "id": 1, "email": "old@example.com" }))
            .after(json!({ "id": 1, "email": "new@example.com" }))
            .primary_key(["id"])
            .ts(42)
            .build();

        let decoded = Event::from_json(&event.to_json().expect("encode")).expect("decode");
        assert_eq!(decoded, event);
    }

    #[test]
    fn build_validated_rejects_an_envelope_that_violates_the_contract() {
        // An UPDATE with neither before nor after carries no row state at all.
        let error = Event::builder("users", Operation::Update)
            .source(SourceMetadata::new("postgres", "0/1", 1))
            .ts(1)
            .build_validated()
            .expect_err("an update with no payload must be rejected");
        assert!(!error.to_string().is_empty());
    }

    #[test]
    fn primary_key_preserves_the_order_it_was_given() {
        // The override window and every consumer derive row identity positionally.
        let event = Event::builder("t", Operation::Insert)
            .primary_key(["tenant", "id"])
            .build();
        assert_eq!(
            event.primary_key.as_deref(),
            Some(["tenant".to_string(), "id".to_string()].as_slice())
        );
    }

    #[test]
    fn the_two_unavailable_column_lists_stay_separate() {
        // Merging them marks a column that genuinely changed as unwritable, silently
        // dropping the update.
        let event = Event::builder("t", Operation::Update)
            .unavailable_columns(["blob_a"])
            .before_unavailable_columns(["blob_b"])
            .build();
        assert_eq!(event.unavailable_columns, vec!["blob_a".to_string()]);
        assert_eq!(event.before_unavailable_columns, vec!["blob_b".to_string()]);
    }
}
