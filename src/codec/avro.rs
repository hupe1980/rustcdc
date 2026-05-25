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
/// Also available as `schemas/event.avsc` in the repository.
/// Register this schema with your schema registry to enable Confluent
/// Schema Registry framing (see module docs).
pub const AVRO_SCHEMA: &str = r#"{
  "type": "record",
  "name": "Event",
  "namespace": "io.cdc_rs",
  "doc": "Canonical CDC event envelope — cdc-rs envelope_version=1",
  "fields": [
    {
      "name": "before",
      "type": ["null", "bytes"],
      "default": null,
      "doc": "JSON-encoded before-image. null for INSERT events."
    },
    {
      "name": "after",
      "type": ["null", "bytes"],
      "default": null,
      "doc": "JSON-encoded after-image. null for DELETE events."
    },
    {
      "name": "op",
      "type": {
        "type": "enum",
        "name": "Operation",
        "namespace": "io.cdc_rs",
        "symbols": ["INSERT", "UPDATE", "DELETE", "READ", "SCHEMA_CHANGE", "TRUNCATE"],
        "doc": "CRUD operation that produced this event."
      }
    },
    {
      "name": "source",
      "type": {
        "type": "record",
        "name": "SourceMetadata",
        "namespace": "io.cdc_rs",
        "fields": [
          {"name": "source_name", "type": "string", "doc": "Logical connector name"},
          {"name": "offset",      "type": "string", "doc": "Source-specific durable position"},
          {"name": "timestamp",   "type": "long",   "doc": "Source timestamp in ms since epoch"}
        ]
      }
    },
    {
      "name": "ts",
      "type": "long",
      "doc": "Event timestamp in milliseconds since Unix epoch."
    },
    {
      "name": "schema",
      "type": ["null", "string"],
      "default": null,
      "doc": "Database schema name. null when unknown."
    },
    {
      "name": "table",
      "type": "string",
      "doc": "Table name that produced the event."
    },
    {
      "name": "primary_key",
      "type": {"type": "array", "items": "string"},
      "default": [],
      "doc": "Ordered list of primary key column names."
    },
    {
      "name": "snapshot",
      "type": ["null", {
        "type": "record",
        "name": "SnapshotMetadata",
        "namespace": "io.cdc_rs",
        "fields": [
          {"name": "snapshot_id",   "type": "string"},
          {"name": "chunk_index",   "type": "int"},
          {"name": "is_last_chunk", "type": "boolean"}
        ]
      }],
      "default": null,
      "doc": "Snapshot phase metadata. null outside snapshot."
    },
    {
      "name": "transaction",
      "type": ["null", {
        "type": "record",
        "name": "TransactionMetadata",
        "namespace": "io.cdc_rs",
        "fields": [
          {"name": "tx_id",        "type": "long"},
          {"name": "total_events", "type": "int"},
          {"name": "event_index",  "type": "int"}
        ]
      }],
      "default": null,
      "doc": "Transaction metadata. null for single-event transactions."
    },
    {
      "name": "envelope_version",
      "type": "int",
      "default": 1,
      "doc": "Canonical envelope schema version. Currently always 1."
    }
  ]
}"#;

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
/// # use cdc_rs::codec::{EventEncoder, AvroEncoder};
/// # use cdc_rs::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
/// let encoder = AvroEncoder::new().unwrap();
/// let event = Event {
///     before: None,
///     after: Some(serde_json::json!({"id": 1})),
///     op: Operation::Insert,
///     source: SourceMetadata {
///         source_name: "postgres".into(),
///         offset: "0/16B6A70".into(),
///         timestamp: 1,
///     },
///     ts: 1,
///     schema: None,
///     table: "users".into(),
///     primary_key: None,
///     snapshot: None,
///     transaction: None,
///     envelope_version: EVENT_ENVELOPE_VERSION,
/// };
/// let out = encoder.encode(&event).unwrap();
/// assert_eq!(out.content_type, "avro/binary");
/// ```
#[derive(Debug)]
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
                    AvroValue::Int(t.total_events as i32),
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
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::from_avro_datum;
    use crate::core::{
        Event, Operation, SnapshotMetadata, SourceMetadata, TransactionMetadata,
        EVENT_ENVELOPE_VERSION,
    };

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
                total_events: 2,
                event_index: 0,
            }),
            envelope_version: EVENT_ENVELOPE_VERSION,
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
            enc.encode(&ev).unwrap_or_else(|e| panic!("encode failed for {op:?}: {e}"));
        }
    }

    #[test]
    fn schema_accessor_returns_valid_schema() {
        let enc = AvroEncoder::new().unwrap();
        // The schema name should be "Event"
        let json = enc.schema().canonical_form();
        assert!(json.contains("Event"), "schema should contain 'Event'");
    }
}
