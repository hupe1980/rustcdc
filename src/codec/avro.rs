//! Apache Avro encoding for CDC events.
//!
//! Uses [`apache_avro`](https://crates.io/crates/apache-avro) for schema-aware
//! Avro binary serialization.  The schema is embedded in this module as the
//! [`AVRO_SCHEMA`] constant; it is also available at `schemas/event.avsc` in
//! the repository root for use with schema registries or code generators.
//!
//! # Row payload encoding
//!
//! The `before` and `after` row-image fields are encoded as **Avro `bytes`**
//! containing UTF-8 JSON.  This preserves the schemaless nature of the CDC row
//! payload while keeping the Avro schema stable regardless of table structure.
//! Consumers decode the bytes as a JSON object and can re-validate against a
//! table-specific schema if desired.
//!
//! # Decoding
//!
//! [`AvroDecoder`] reverses this encoding. It is hand-written rather than derived because
//! `apache_avro::from_value::<Event>` cannot reverse the `bytes`-holding-JSON
//! representation of `before`/`after` above — it sees a byte array where `Event` declares
//! a `serde_json::Value`.
//!
//! # Confluent Schema Registry integration
//!
//! The `AvroEncoder` produces bare Avro binary (no framing).  To integrate with
//! the [Confluent Schema Registry wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format),
//! prepend the 5-byte magic framing (`0x00` + 4-byte big-endian schema ID) to
//! the bytes returned by [`encode`](AvroEncoder::encode) after registering
//! [`AVRO_SCHEMA`] with your registry.

use apache_avro::{schema::Schema, to_avro_datum, types::Value as AvroValue};

use crate::codec::{EncodedOutput, EventEncoder};
use crate::core::{Error, Event, Operation, Result};

const CONTENT_TYPE: &str = "avro/binary";

// ─── Avro schema ──────────────────────────────────────────────────────────────

/// Avro schema (JSON) for the canonical CDC event envelope.
///
/// Loaded from `schemas/event.avsc` at compile time, so the published schema file and
/// the encoder can never drift apart.
/// Register this schema with your schema registry to enable Confluent
/// Schema Registry framing (see module docs).
pub const AVRO_SCHEMA: &str = include_str!("../../schemas/event.avsc");

// ─── Operation index mapping ──────────────────────────────────────────────────
//
// The Avro enum `symbols` array defines 0-based indices.
// These must match the symbol order in AVRO_SCHEMA above.

fn op_avro_index(op: Operation) -> u32 {
    match op {
        Operation::Insert => 0,
        Operation::Update => 1,
        Operation::Delete => 2,
        Operation::Read => 3,
        Operation::SchemaChange => 4,
        Operation::Truncate => 5,
    }
}

fn op_avro_symbol(op: Operation) -> &'static str {
    match op {
        Operation::Insert => "INSERT",
        Operation::Update => "UPDATE",
        Operation::Delete => "DELETE",
        Operation::Read => "READ",
        Operation::SchemaChange => "SCHEMA_CHANGE",
        Operation::Truncate => "TRUNCATE",
    }
}

// ─── AvroEncoder ──────────────────────────────────────────────────────────────

/// Encodes CDC events as Apache Avro binary.
///
/// The schema embedded in this encoder matches `schemas/event.avsc` in the
/// repository.  The encoder is constructed once and reused; schema parsing
/// happens at construction time.
///
/// See the [module documentation](self) for notes on Confluent Schema Registry
/// integration.
///
/// # Example
///
/// ```rust
/// # use rustcdc::codec::{EventEncoder, AvroEncoder};
/// # use rustcdc::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
/// let encoder = AvroEncoder::new().unwrap();
/// let event = Event::builder("users", Operation::Insert)
///     .after(serde_json::json!({"id": 1}))
///     .source(SourceMetadata::new("postgres", "0/16B6A70", 1))
///     .ts(1)
///     .build();
/// let out = encoder.encode(&event).unwrap();
/// assert_eq!(out.content_type, "avro/binary");
/// ```
#[derive(Debug, Clone)]
pub struct AvroEncoder {
    schema: Schema,
}

impl AvroEncoder {
    /// Create a new `AvroEncoder` by parsing the built-in [`AVRO_SCHEMA`].
    ///
    /// Schema parsing is done once at construction; the result is reused for
    /// every [`encode`](Self::encode) call.
    pub fn new() -> Result<Self> {
        let schema = Schema::parse_str(AVRO_SCHEMA)
            .map_err(|e| Error::SerializationError(format!("Avro schema parse error: {e}")))?;
        Ok(Self { schema })
    }

    /// Access the compiled [`Schema`] (e.g. to register with a schema registry).
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl EventEncoder for AvroEncoder {
    fn encode(&self, event: &Event) -> Result<EncodedOutput> {
        let value = event_to_avro_value(event)?;
        let bytes = to_avro_datum(&self.schema, value)
            .map_err(|e| Error::SerializationError(format!("Avro encode error: {e}")))?;
        Ok(EncodedOutput::new(bytes, CONTENT_TYPE))
    }

    fn content_type(&self) -> &'static str {
        CONTENT_TYPE
    }
}

// ─── Event → AvroValue ────────────────────────────────────────────────────────

fn event_to_avro_value(event: &Event) -> Result<AvroValue> {
    // Helper: optional JSON → Avro ["null","bytes"] union.
    let json_opt_to_avro = |v: &Option<serde_json::Value>| -> Result<AvroValue> {
        match v {
            Some(json) => {
                let bytes = serde_json::to_vec(json)
                    .map_err(|e| Error::SerializationError(e.to_string()))?;
                Ok(AvroValue::Union(1, Box::new(AvroValue::Bytes(bytes))))
            }
            None => Ok(AvroValue::Union(0, Box::new(AvroValue::Null))),
        }
    };

    let op = AvroValue::Enum(op_avro_index(event.op), op_avro_symbol(event.op).into());

    let source = AvroValue::Record(vec![
        (
            "source_name".into(),
            AvroValue::String(event.source.source_name.clone()),
        ),
        (
            "offset".into(),
            AvroValue::String(event.source.offset.clone()),
        ),
        (
            "timestamp".into(),
            AvroValue::Long(event.source.timestamp as i64),
        ),
    ]);

    let schema_val = match &event.schema {
        Some(s) => AvroValue::Union(1, Box::new(AvroValue::String(s.clone()))),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    };

    let primary_key = AvroValue::Array(
        event
            .primary_key
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|k| AvroValue::String(k.clone()))
            .collect(),
    );

    let snapshot = match &event.snapshot {
        Some(s) => AvroValue::Union(
            1,
            Box::new(AvroValue::Record(vec![
                (
                    "snapshot_id".into(),
                    AvroValue::String(s.snapshot_id.clone()),
                ),
                ("chunk_index".into(), AvroValue::Int(s.chunk_index as i32)),
                ("is_last_chunk".into(), AvroValue::Boolean(s.is_last_chunk)),
            ])),
        ),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    };

    let transaction = match &event.transaction {
        Some(t) => AvroValue::Union(
            1,
            Box::new(AvroValue::Record(vec![
                ("tx_id".into(), AvroValue::Long(t.tx_id as i64)),
                (
                    "total_events".into(),
                    AvroValue::Int(t.total_events.unwrap_or(0) as i32),
                ),
                ("event_index".into(), AvroValue::Int(t.event_index as i32)),
            ])),
        ),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    };

    Ok(AvroValue::Record(vec![
        ("before".into(), json_opt_to_avro(&event.before)?),
        ("after".into(), json_opt_to_avro(&event.after)?),
        ("op".into(), op),
        ("source".into(), source),
        ("ts".into(), AvroValue::Long(event.ts as i64)),
        ("schema".into(), schema_val),
        ("table".into(), AvroValue::String(event.table.clone())),
        ("primary_key".into(), primary_key),
        ("snapshot".into(), snapshot),
        ("transaction".into(), transaction),
        (
            "envelope_version".into(),
            AvroValue::Int(event.envelope_version as i32),
        ),
        (
            "before_is_key_only".into(),
            AvroValue::Boolean(event.before_is_key_only),
        ),
        (
            "unavailable_columns".into(),
            AvroValue::Array(
                event
                    .unavailable_columns
                    .iter()
                    .map(|column| AvroValue::String(column.clone()))
                    .collect(),
            ),
        ),
        (
            "before_unavailable_columns".into(),
            AvroValue::Array(
                event
                    .before_unavailable_columns
                    .iter()
                    .map(|column| AvroValue::String(column.clone()))
                    .collect(),
            ),
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        Event, Operation, SnapshotMetadata, SourceMetadata, TransactionMetadata,
        EVENT_ENVELOPE_VERSION,
    };
    use apache_avro::from_avro_datum;

    fn update_event() -> Event {
        Event {
            before: Some(serde_json::json!({"id": 1, "name": "alice"})),
            after: Some(serde_json::json!({"id": 1, "name": "alice-v2"})),
            op: Operation::Update,
            source: SourceMetadata {
                source_name: "postgres".into(),
                offset: "0/1A0000".into(),
                timestamp: 1716595200000,
            },
            ts: 1716595200000,
            schema: Some("public".into()),
            table: "users".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: Some(TransactionMetadata {
                tx_id: 7,
                total_events: Some(2),
                event_index: 0,
            }),
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    fn insert_event() -> Event {
        Event {
            before: None,
            after: Some(serde_json::json!({"id": 2})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "mysql".into(),
                offset: "gtid:xyz".into(),
                timestamp: 1,
            },
            ts: 1,
            schema: None,
            table: "orders".into(),
            primary_key: None,
            snapshot: Some(SnapshotMetadata {
                snapshot_id: "s1".into(),
                chunk_index: 3,
                is_last_chunk: true,
            }),
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    #[test]
    fn schema_parses_without_error() {
        assert!(AvroEncoder::new().is_ok());
    }

    #[test]
    fn encode_produces_non_empty_avro_bytes() {
        let enc = AvroEncoder::new().unwrap();
        let out = enc.encode(&insert_event()).unwrap();
        assert!(!out.bytes.is_empty());
        assert_eq!(out.content_type, "avro/binary");
    }

    #[test]
    fn avro_roundtrip_update_event() {
        let enc = AvroEncoder::new().unwrap();
        let event = update_event();
        let out = enc.encode(&event).unwrap();

        // Decode back to AvroValue for field-level assertions.
        let mut reader = out.bytes.as_slice();
        let decoded = from_avro_datum(enc.schema(), &mut reader, None).unwrap();

        // Verify table and ts fields.
        if let AvroValue::Record(fields) = decoded {
            let field = |name: &str| -> AvroValue {
                fields
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
                    .unwrap_or(AvroValue::Null)
            };

            assert_eq!(field("table"), AvroValue::String("users".into()));
            assert_eq!(field("ts"), AvroValue::Long(1716595200000i64));
            assert_eq!(
                field("op"),
                AvroValue::Enum(op_avro_index(Operation::Update), "UPDATE".into())
            );

            // `before` and `after` are union bytes carrying JSON.
            if let AvroValue::Union(_, inner) = field("before") {
                if let AvroValue::Bytes(b) = *inner {
                    let json: serde_json::Value = serde_json::from_slice(&b).unwrap();
                    assert_eq!(json["name"], "alice");
                } else {
                    panic!("expected Bytes");
                }
            } else {
                panic!("expected Union for before");
            }
        } else {
            panic!("expected Record");
        }
    }

    #[test]
    fn avro_insert_no_before() {
        let enc = AvroEncoder::new().unwrap();
        let out = enc.encode(&insert_event()).unwrap();
        let mut reader = out.bytes.as_slice();
        let decoded = from_avro_datum(enc.schema(), &mut reader, None).unwrap();

        if let AvroValue::Record(fields) = decoded {
            let before = fields.iter().find(|(k, _)| k == "before").unwrap();
            // Union index 0 = null branch
            assert_eq!(
                before.1,
                AvroValue::Union(0, Box::new(AvroValue::Null)),
                "INSERT before must be null"
            );
        }
    }

    #[test]
    fn all_operations_encode_without_error() {
        let enc = AvroEncoder::new().unwrap();
        let ops = [
            Operation::Insert,
            Operation::Update,
            Operation::Delete,
            Operation::Read,
            Operation::SchemaChange,
            Operation::Truncate,
        ];
        for op in ops {
            let mut ev = insert_event();
            ev.op = op;
            if op == Operation::Delete || op == Operation::Update {
                ev.before = Some(serde_json::json!({"id": 2}));
            }
            if op == Operation::Delete {
                ev.after = None;
            }
            enc.encode(&ev)
                .unwrap_or_else(|e| panic!("encode failed for {op:?}: {e}"));
        }
    }

    #[test]
    fn schema_accessor_returns_valid_schema() {
        let enc = AvroEncoder::new().unwrap();
        // The schema name should be "Event"
        let json = enc.schema().canonical_form();
        assert!(json.contains("Event"), "schema should contain 'Event'");
    }

    #[test]
    /// A partial payload that survives encoding as if it were complete is silent
    /// corruption at the sink. Both availability lists must reach the wire, and they must
    /// stay distinct — a merged list marks genuinely-changed columns as unwritable.
    fn availability_lists_round_trip_separately() {
        let enc = AvroEncoder::new().unwrap();
        let mut event = update_event();
        event.unavailable_columns = vec!["big_kept".into()];
        event.before_unavailable_columns = vec!["big_changed".into()];
        let out = enc.encode(&event).unwrap();

        let mut reader = out.bytes.as_slice();
        let decoded = from_avro_datum(enc.schema(), &mut reader, None).unwrap();

        let AvroValue::Record(fields) = decoded else {
            panic!("expected Record");
        };
        let list = |name: &str| {
            fields
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .unwrap_or(AvroValue::Null)
        };
        assert_eq!(
            list("unavailable_columns"),
            AvroValue::Array(vec![AvroValue::String("big_kept".into())]),
            "the after-image holes must reach the wire, or an Avro sink cannot know the \
             payload is partial"
        );
        assert_eq!(
            list("before_unavailable_columns"),
            AvroValue::Array(vec![AvroValue::String("big_changed".into())]),
            "the before-image holes must stay separate from the after-image holes"
        );
    }

    #[test]
    fn before_is_key_only_flag_round_trips() {
        let enc = AvroEncoder::new().unwrap();
        let mut event = update_event();
        event.before_is_key_only = true;
        let out = enc.encode(&event).unwrap();

        let mut reader = out.bytes.as_slice();
        let decoded = from_avro_datum(enc.schema(), &mut reader, None).unwrap();

        if let AvroValue::Record(fields) = decoded {
            let flag = fields
                .iter()
                .find(|(k, _)| k == "before_is_key_only")
                .map(|(_, v)| v.clone())
                .unwrap_or(AvroValue::Null);
            assert_eq!(flag, AvroValue::Boolean(true));
        } else {
            panic!("expected Record");
        }
    }
}

// ─── AvroValue → Event ────────────────────────────────────────────────────────

/// Reconstruct an [`Event`] from the Avro record produced by [`AvroEncoder`].
///
/// # Why this is hand-written
///
/// `apache_avro::from_value::<Event>` cannot reverse this encoding. `before` and `after`
/// are deliberately Avro **`bytes` holding UTF-8 JSON** — that is what keeps the Avro
/// schema stable regardless of table structure — so a blanket serde mapping sees a byte
/// array where `Event` declares a `serde_json::Value` and fails with
/// *"invalid type: byte array, expected any valid JSON value"*.
///
/// Until this existed there was no Avro → `Event` path that worked at all: the encoder had
/// no counterpart, and the registry decoder used the blanket mapping. The unit tests did
/// not catch it because they decoded to a raw `AvroValue` and asserted on individual
/// fields rather than reconstructing an event. Only a live round trip through a real
/// registry surfaced it.
pub fn avro_value_to_event(value: &AvroValue) -> Result<Event> {
    let AvroValue::Record(fields) = value else {
        return Err(Error::SerializationError(
            "avro → Event: expected a record at the top level".into(),
        ));
    };

    let get = |name: &str| -> Option<&AvroValue> {
        fields
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value)
    };
    let required = |name: &str| -> Result<&AvroValue> {
        get(name).ok_or_else(|| {
            Error::SerializationError(format!("avro → Event: missing field '{name}'"))
        })
    };

    /// Unwrap a union to its payload, or `None` for the null branch.
    fn unwrap_union(value: &AvroValue) -> Option<&AvroValue> {
        match value {
            AvroValue::Union(_, inner) => match inner.as_ref() {
                AvroValue::Null => None,
                other => Some(other),
            },
            AvroValue::Null => None,
            other => Some(other),
        }
    }

    fn json_field(value: &AvroValue, name: &str) -> Result<Option<serde_json::Value>> {
        let Some(inner) = unwrap_union(value) else {
            return Ok(None);
        };
        let bytes = match inner {
            AvroValue::Bytes(bytes) => bytes.as_slice(),
            // Tolerated because some registries normalise `bytes` to `string` when a
            // schema is re-registered through a compatibility layer.
            AvroValue::String(text) => text.as_bytes(),
            other => {
                return Err(Error::SerializationError(format!(
                    "avro → Event: field '{name}' must be bytes holding JSON, got {other:?}"
                )))
            }
        };
        serde_json::from_slice(bytes).map(Some).map_err(|error| {
            Error::SerializationError(format!(
                "avro → Event: field '{name}' is not valid JSON: {error}"
            ))
        })
    }

    fn string_field(value: &AvroValue, name: &str) -> Result<String> {
        match unwrap_union(value) {
            Some(AvroValue::String(text)) => Ok(text.clone()),
            Some(AvroValue::Enum(_, symbol)) => Ok(symbol.clone()),
            other => Err(Error::SerializationError(format!(
                "avro → Event: field '{name}' must be a string, got {other:?}"
            ))),
        }
    }

    fn long_field(value: &AvroValue, name: &str) -> Result<i64> {
        match unwrap_union(value) {
            Some(AvroValue::Long(number)) => Ok(*number),
            Some(AvroValue::Int(number)) => Ok(i64::from(*number)),
            other => Err(Error::SerializationError(format!(
                "avro → Event: field '{name}' must be an integer, got {other:?}"
            ))),
        }
    }

    fn string_array(value: Option<&AvroValue>) -> Vec<String> {
        match value.and_then(unwrap_union) {
            Some(AvroValue::Array(items)) => items
                .iter()
                .filter_map(|item| match item {
                    AvroValue::String(text) => Some(text.clone()),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    fn record_fields(value: &AvroValue) -> Option<&Vec<(String, AvroValue)>> {
        match unwrap_union(value)? {
            AvroValue::Record(fields) => Some(fields),
            _ => None,
        }
    }

    let op = match string_field(required("op")?, "op")?.as_str() {
        "INSERT" => Operation::Insert,
        "UPDATE" => Operation::Update,
        "DELETE" => Operation::Delete,
        "READ" => Operation::Read,
        "SCHEMA_CHANGE" => Operation::SchemaChange,
        "TRUNCATE" => Operation::Truncate,
        other => {
            // Defaulting an unknown symbol would fabricate an operation — an unrecognised
            // op silently read as INSERT turns a foreign message into a row creation a
            // sink would apply.
            return Err(Error::SerializationError(format!(
                "avro → Event: unknown operation symbol '{other}'"
            )));
        }
    };

    let source_fields = record_fields(required("source")?).ok_or_else(|| {
        Error::SerializationError("avro → Event: field 'source' must be a record".into())
    })?;
    let source_get = |name: &str| -> Option<&AvroValue> {
        source_fields
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value)
    };
    let source = crate::core::SourceMetadata::new(
        source_get("source_name")
            .map(|value| string_field(value, "source.source_name"))
            .transpose()?
            .unwrap_or_default(),
        source_get("offset")
            .map(|value| string_field(value, "source.offset"))
            .transpose()?
            .unwrap_or_default(),
        source_get("timestamp")
            .map(|value| long_field(value, "source.timestamp"))
            .transpose()?
            .unwrap_or_default() as u64,
    );

    let snapshot = record_fields(required("snapshot")?).map(|snapshot_fields| {
        let field = |name: &str| {
            snapshot_fields
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value)
        };
        crate::core::SnapshotMetadata::new(
            field("snapshot_id")
                .and_then(|value| string_field(value, "snapshot.snapshot_id").ok())
                .unwrap_or_default(),
            field("chunk_index")
                .and_then(|value| long_field(value, "snapshot.chunk_index").ok())
                .unwrap_or_default() as u32,
            matches!(
                field("is_last_chunk").and_then(unwrap_union),
                Some(AvroValue::Boolean(true))
            ),
        )
    });

    let transaction = record_fields(required("transaction")?).map(|transaction_fields| {
        let field = |name: &str| {
            transaction_fields
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value)
        };
        let total = field("total_events")
            .and_then(|value| long_field(value, "transaction.total_events").ok())
            .unwrap_or_default() as u32;
        crate::core::TransactionMetadata::new(
            field("tx_id")
                .and_then(|value| long_field(value, "transaction.tx_id").ok())
                .unwrap_or_default() as u64,
            field("event_index")
                .and_then(|value| long_field(value, "transaction.event_index").ok())
                .unwrap_or_default() as u32,
            // The encoder writes 0 for an absent count because the Avro field is not
            // nullable. Zero is not a meaningful transaction size, so it maps back to
            // `None` rather than to a transaction claiming to hold no events.
            (total > 0).then_some(total),
        )
    });

    let mut builder = Event::builder(string_field(required("table")?, "table")?, op)
        .source(source)
        .ts(long_field(required("ts")?, "ts")? as u64)
        .primary_key(string_array(get("primary_key")))
        .unavailable_columns(string_array(get("unavailable_columns")))
        .before_unavailable_columns(string_array(get("before_unavailable_columns")))
        .before_is_key_only(matches!(
            get("before_is_key_only").and_then(unwrap_union),
            Some(AvroValue::Boolean(true))
        ));

    if let Some(before) = json_field(required("before")?, "before")? {
        builder = builder.before(before);
    }
    if let Some(after) = json_field(required("after")?, "after")? {
        builder = builder.after(after);
    }
    if let Some(schema) = unwrap_union(required("schema")?) {
        builder = builder.schema(string_field(schema, "schema")?);
    }
    if let Some(snapshot) = snapshot {
        builder = builder.snapshot(snapshot);
    }
    if let Some(transaction) = transaction {
        builder = builder.transaction(transaction);
    }

    let mut event = builder.build();
    // The envelope version is carried on the wire rather than re-stamped, so a consumer
    // can detect a producer running an older envelope.
    if let Some(version) = get("envelope_version") {
        event.envelope_version = long_field(version, "envelope_version")? as u16;
    }
    // An empty primary-key array on the wire means "no key", not "a key with no columns".
    if event
        .primary_key
        .as_ref()
        .is_some_and(|columns| columns.is_empty())
    {
        event.primary_key = None;
    }
    Ok(event)
}

/// Decodes CDC events from bare Avro binary produced by [`AvroEncoder`].
///
/// For Confluent-framed payloads use `ConfluentAvroDecoder`, which strips the 5-byte
/// header and resolves the writer schema from the registry before delegating here.
#[derive(Debug)]
pub struct AvroDecoder {
    schema: Schema,
}

impl AvroDecoder {
    /// Build a decoder against the canonical envelope schema.
    pub fn new() -> Result<Self> {
        let schema = Schema::parse_str(AVRO_SCHEMA)
            .map_err(|e| Error::SerializationError(format!("Avro schema parse error: {e}")))?;
        Ok(Self { schema })
    }

    /// Decode bare Avro binary into an [`Event`].
    pub fn decode(&self, bytes: &[u8]) -> Result<Event> {
        let value = apache_avro::from_avro_datum(&self.schema, &mut { bytes }, None)
            .map_err(|e| Error::SerializationError(format!("Avro decode error: {e}")))?;
        avro_value_to_event(&value)
    }
}

#[cfg(test)]
mod decoder_tests {
    use super::*;
    use crate::core::{SnapshotMetadata, SourceMetadata, TransactionMetadata};
    use serde_json::json;

    fn round_trip(event: &Event) -> Event {
        let encoded = AvroEncoder::new()
            .expect("encoder")
            .encode(event)
            .expect("encode");
        AvroDecoder::new()
            .expect("decoder")
            .decode(&encoded.bytes)
            .expect("decode")
    }

    #[test]
    fn a_full_event_round_trips_through_avro() {
        // Until `avro_value_to_event` existed there was no Avro → Event path that worked
        // at all. The encoder's tests decoded to a raw AvroValue and inspected fields,
        // which is why an unusable decoder went unnoticed.
        let event = Event::builder("users", Operation::Update)
            .source(SourceMetadata::new("postgres", "0/16B2E48", 1_700_000_000))
            .schema("public")
            .before(json!({ "id": 1, "email": "old@example.com" }))
            .after(json!({ "id": 1, "email": "new@example.com" }))
            .primary_key(["id"])
            .ts(1_700_000_000)
            .build();

        assert_eq!(round_trip(&event), event);
    }

    #[test]
    fn row_payloads_survive_as_json_not_as_opaque_bytes() {
        // `before`/`after` are Avro `bytes` holding JSON. A decoder that returned the raw
        // bytes, or stringified them, would hand a sink a string where it expects an
        // object — and the sink would write that string into the row.
        let event = Event::builder("t", Operation::Insert)
            .source(SourceMetadata::new("s", "1", 1))
            .after(json!({ "nested": { "array": [1, 2, 3] }, "flag": true }))
            .ts(1)
            .build();
        let decoded = round_trip(&event);
        assert_eq!(
            decoded.after.as_ref().and_then(|value| value.get("nested")),
            Some(&json!({ "array": [1, 2, 3] })),
        );
    }

    #[test]
    fn a_delete_with_no_after_image_round_trips_as_none() {
        // `Some(Value::Null)` and `None` are different: the first says "the row is null",
        // the second says "there is no after image".
        let event = Event::builder("t", Operation::Delete)
            .source(SourceMetadata::new("s", "1", 1))
            .before(json!({ "id": 9 }))
            .ts(1)
            .build();
        let decoded = round_trip(&event);
        assert!(decoded.after.is_none(), "absent must not become null");
        assert_eq!(decoded.before, Some(json!({ "id": 9 })));
        assert_eq!(decoded.op, Operation::Delete);
    }

    #[test]
    fn every_operation_symbol_round_trips() {
        for op in [
            Operation::Insert,
            Operation::Update,
            Operation::Delete,
            Operation::Read,
            Operation::SchemaChange,
            Operation::Truncate,
        ] {
            let event = Event::builder("t", op)
                .source(SourceMetadata::new("s", "1", 1))
                .after(json!({ "id": 1 }))
                .ts(1)
                .build();
            assert_eq!(
                round_trip(&event).op,
                op,
                "operation {op:?} did not survive"
            );
        }
    }

    #[test]
    fn an_unknown_operation_symbol_is_rejected_rather_than_defaulted() {
        // Defaulting to INSERT would turn a foreign or truncated message into a row
        // creation that a sink would apply.
        let record = AvroValue::Record(vec![
            (
                "before".into(),
                AvroValue::Union(0, Box::new(AvroValue::Null)),
            ),
            (
                "after".into(),
                AvroValue::Union(0, Box::new(AvroValue::Null)),
            ),
            ("op".into(), AvroValue::Enum(9, "FROM_THE_FUTURE".into())),
            (
                "source".into(),
                AvroValue::Record(vec![
                    ("source_name".into(), AvroValue::String("s".into())),
                    ("offset".into(), AvroValue::String("1".into())),
                    ("timestamp".into(), AvroValue::Long(1)),
                ]),
            ),
            ("ts".into(), AvroValue::Long(1)),
            (
                "schema".into(),
                AvroValue::Union(0, Box::new(AvroValue::Null)),
            ),
            ("table".into(), AvroValue::String("t".into())),
            ("primary_key".into(), AvroValue::Array(Vec::new())),
            (
                "snapshot".into(),
                AvroValue::Union(0, Box::new(AvroValue::Null)),
            ),
            (
                "transaction".into(),
                AvroValue::Union(0, Box::new(AvroValue::Null)),
            ),
            ("envelope_version".into(), AvroValue::Int(1)),
            ("before_is_key_only".into(), AvroValue::Boolean(false)),
            ("unavailable_columns".into(), AvroValue::Array(Vec::new())),
            (
                "before_unavailable_columns".into(),
                AvroValue::Array(Vec::new()),
            ),
        ]);
        let error = avro_value_to_event(&record).expect_err("unknown symbol must be rejected");
        assert!(
            error.to_string().contains("unknown operation symbol"),
            "got: {error}"
        );
    }

    #[test]
    fn snapshot_and_transaction_metadata_round_trip() {
        let event = Event::builder("t", Operation::Read)
            .source(SourceMetadata::new("s", "1", 1))
            .after(json!({ "id": 1 }))
            .ts(1)
            .snapshot(SnapshotMetadata::new("snap-1", 3, true))
            .transaction(TransactionMetadata::new(77, 2, Some(5)))
            .build();
        let decoded = round_trip(&event);
        assert_eq!(decoded.snapshot, event.snapshot);
        assert_eq!(decoded.transaction, event.transaction);
    }

    #[test]
    fn an_absent_transaction_size_stays_absent() {
        // The Avro field is not nullable, so the encoder writes 0. Reading 0 back as
        // `Some(0)` would claim a transaction holds no events.
        let event = Event::builder("t", Operation::Insert)
            .source(SourceMetadata::new("s", "1", 1))
            .after(json!({ "id": 1 }))
            .ts(1)
            .transaction(TransactionMetadata::new(5, 0, None))
            .build();
        let decoded = round_trip(&event);
        assert_eq!(
            decoded.transaction.as_ref().and_then(|tx| tx.total_events),
            None,
        );
    }

    #[test]
    fn the_two_availability_lists_stay_separate_across_the_round_trip() {
        // Merging them marks a genuinely changed column as unwritable and drops the update.
        let event = Event::builder("t", Operation::Update)
            .source(SourceMetadata::new("s", "1", 1))
            .before(json!({ "id": 1 }))
            .after(json!({ "id": 1 }))
            .unavailable_columns(["big_kept"])
            .before_unavailable_columns(["big_changed"])
            .ts(1)
            .build();
        let decoded = round_trip(&event);
        assert_eq!(decoded.unavailable_columns, vec!["big_kept".to_string()]);
        assert_eq!(
            decoded.before_unavailable_columns,
            vec!["big_changed".to_string()]
        );
    }

    #[test]
    fn a_keyless_table_round_trips_as_no_key_rather_than_an_empty_key() {
        // An empty column list is not the same as "this table has a key with no columns";
        // the idempotency guard treats the two differently.
        let event = Event::builder("logs", Operation::Insert)
            .source(SourceMetadata::new("s", "1", 1))
            .after(json!({ "line": "x" }))
            .ts(1)
            .build();
        assert!(round_trip(&event).primary_key.is_none());
    }
}
