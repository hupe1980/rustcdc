//! CloudEvents 1.0 structured-JSON encoding for CDC events.
//!
//! [CloudEvents](https://cloudevents.io/) is a CNCF specification (v1.0.2) for
//! describing event data in a common, interoperable way.  It is natively
//! supported by Knative, Azure Event Grid, Google Cloud Eventarc, and the
//! Apache Kafka CloudEvents binding (`kafka-clients` + CloudEvents spec).
//!
//! This encoder produces the **Structured Content Mode** CloudEvents JSON
//! binding (`application/cloudevents+json`).
//!
//! # Attribute mapping
//!
//! | CloudEvents attribute | Derived from |
//! |---|---|
//! | `specversion` | Always `"1.0"` |
//! | `type` | `"io.rustcdc.change.{op}"` (e.g. `io.rustcdc.change.insert`) |
//! | `source` | `"/{connector}/{schema_or_dash}/{table}"` |
//! | `id` | `"{source_name}/{offset}"` |
//! | `time` | RFC 3339 timestamp from `event.ts` (ms since epoch) |
//! | `datacontenttype` | `"application/json"` |
//! | `subject` | `"{schema}.{table}"` or `"{table}"` |
//!
//! # CDC extension attributes
//!
//! Extension attribute names are all-lowercase ASCII alphanumeric per the
//! CloudEvents spec.
//!
//! | Extension | Value |
//! |---|---|
//! | `cdcop` | Operation string (`"insert"`, `"update"`, `"delete"`, …) |
//! | `cdctable` | Table name |
//! | `cdcschema` | Schema name (omitted when unknown) |
//! | `cdcsource` | Source connector name |
//! | `cdcoffset` | Source offset / LSN |
//!
//! # `data` payload
//!
//! The `data` field holds the CDC-specific payload:
//! ```json
//! {
//!   "before": { "id": 1, "name": "alice" },
//!   "after":  { "id": 1, "name": "alice-v2" },
//!   "primary_key": ["id"],
//!   "snapshot":    null,
//!   "transaction": { "tx_id": 42, "total_events": 1, "event_index": 0 }
//! }
//! ```

use serde_json::{json, Map, Value};

use crate::codec::{EncodedOutput, EventEncoder};
use crate::core::{Event, Result};

const CONTENT_TYPE: &str = "application/cloudevents+json";
const CE_SPEC_VERSION: &str = "1.0";

// ─── CloudEventsEncoder ───────────────────────────────────────────────────────

/// Encodes CDC events as [CloudEvents 1.0](https://cloudevents.io/) structured JSON.
///
/// See the [module documentation](self) for the full attribute mapping.
///
/// # Example
///
/// ```rust
/// # use rustcdc::codec::{EventEncoder, CloudEventsEncoder};
/// # use rustcdc::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
/// let encoder = CloudEventsEncoder::default();
/// let event = Event {
///     before: None,
///     after: Some(serde_json::json!({"id": 1})),
///     op: Operation::Insert,
///     source: SourceMetadata {
///         source_name: "postgres".into(),
///         offset: "0/16B6A70".into(),
///         timestamp: 1716595200000,
///     },
///     ts: 1716595200000,
///     schema: Some("public".into()),
///     table: "users".into(),
///     primary_key: Some(vec!["id".into()]),
///     snapshot: None,
///     transaction: None,
///     envelope_version: rustcdc::EVENT_ENVELOPE_VERSION,
/// };
///
/// let out = encoder.encode(&event).unwrap();
/// let ce: serde_json::Value = serde_json::from_slice(&out.bytes).unwrap();
/// assert_eq!(ce["specversion"], "1.0");
/// assert_eq!(ce["type"], "io.rustcdc.change.insert");
/// ```
#[derive(Debug, Clone, Default)]
pub struct CloudEventsEncoder {
    /// Optional URI prefix used as the base of the CloudEvents `source`
    /// attribute.  When `None`, the source is derived as
    /// `"/{connector}/{schema}/{table}"`.
    ///
    /// Example: `Some("urn:cdc:myapp".to_string())` produces
    /// `"urn:cdc:myapp/public/users"`.
    pub source_uri_prefix: Option<String>,
}

impl CloudEventsEncoder {
    /// Create a new encoder with an optional `source` URI prefix.
    pub fn new(source_uri_prefix: Option<String>) -> Self {
        Self { source_uri_prefix }
    }
}

impl EventEncoder for CloudEventsEncoder {
    fn encode(&self, event: &Event) -> Result<EncodedOutput> {
        let schema_str = event.schema.as_deref().unwrap_or("-");

        // CloudEvents `source` — URI identifying the event producer.
        let source_uri = match &self.source_uri_prefix {
            Some(prefix) => format!("{}/{}/{}", prefix, schema_str, event.table),
            None => format!(
                "/{}/{}/{}",
                event.source.source_name, schema_str, event.table
            ),
        };

        // CloudEvents `type` — reverse-DNS prefixed event type.
        let ce_type = format!("io.rustcdc.change.{}", event.op.to_str());

        // CloudEvents `id` — unique per event; use source + offset as a stable key.
        let id = format!("{}/{}", event.source.source_name, event.source.offset);

        // CloudEvents `time` — RFC 3339 timestamp.
        let time = unix_ms_to_rfc3339(event.ts);

        // CloudEvents `subject` — logical entity name.
        let subject = match &event.schema {
            Some(schema) => format!("{}.{}", schema, event.table),
            None => event.table.clone(),
        };

        // Build the `data` payload (CDC-specific fields).
        let mut data = Map::new();
        data.insert("before".into(), event.before.clone().unwrap_or(Value::Null));
        data.insert("after".into(), event.after.clone().unwrap_or(Value::Null));
        if let Some(pk) = &event.primary_key {
            data.insert("primary_key".into(), json!(pk));
        }
        if let Some(snapshot) = &event.snapshot {
            data.insert("snapshot".into(), serde_json::to_value(snapshot)?);
        }
        if let Some(tx) = &event.transaction {
            data.insert("transaction".into(), serde_json::to_value(tx)?);
        }

        // Assemble the CloudEvents envelope.
        let mut ce = Map::new();
        ce.insert("specversion".into(), json!(CE_SPEC_VERSION));
        ce.insert("id".into(), json!(id));
        ce.insert("type".into(), json!(ce_type));
        ce.insert("source".into(), json!(source_uri));
        ce.insert("time".into(), json!(time));
        ce.insert("datacontenttype".into(), json!("application/json"));
        ce.insert("subject".into(), json!(subject));

        // CDC extension attributes (spec: lowercase alphanumeric, max 20 chars).
        ce.insert("cdcop".into(), json!(event.op.to_str()));
        ce.insert("cdctable".into(), json!(event.table));
        if let Some(schema) = &event.schema {
            ce.insert("cdcschema".into(), json!(schema));
        }
        ce.insert("cdcsource".into(), json!(event.source.source_name));
        ce.insert("cdcoffset".into(), json!(event.source.offset));

        ce.insert("data".into(), Value::Object(data));

        let bytes = serde_json::to_vec(&Value::Object(ce))?;
        Ok(EncodedOutput::new(bytes, CONTENT_TYPE))
    }

    fn content_type(&self) -> &'static str {
        CONTENT_TYPE
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Format Unix epoch milliseconds as an RFC 3339 / ISO 8601 UTC timestamp.
///
/// This is a dependency-free implementation to avoid pulling in `chrono` or
/// `time` solely for timestamp formatting.
///
/// Examples: `0` → `"1970-01-01T00:00:00.000Z"`,
///           `1716595200000` → `"2024-05-25T00:00:00.000Z"`
pub fn unix_ms_to_rfc3339(ts_ms: u64) -> String {
    let secs = ts_ms / 1000;
    let ms = ts_ms % 1000;
    let (year, month, day, hour, min, sec) = epoch_secs_to_datetime(secs);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year, month, day, hour, min, sec, ms
    )
}

/// Decompose Unix epoch seconds into (year, month, day, hour, min, sec) UTC.
fn epoch_secs_to_datetime(total_secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sec = (total_secs % 60) as u32;
    let total_mins = total_secs / 60;
    let min = (total_mins % 60) as u32;
    let total_hours = total_mins / 60;
    let hour = (total_hours % 24) as u32;
    let mut days = (total_hours / 24) as u32; // days since 1970-01-01

    let mut year = 1970u32;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }

    let month_lengths = if is_leap_year(year) {
        [31u32, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31u32, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1u32;
    for &len in &month_lengths {
        if days < len {
            break;
        }
        days -= len;
        month += 1;
    }

    (year, month, days + 1, hour, min, sec)
}

fn is_leap_year(year: u32) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        Event, Operation, SourceMetadata, TransactionMetadata, EVENT_ENVELOPE_VERSION,
    };

    fn insert_event() -> Event {
        Event {
            before: None,
            after: Some(serde_json::json!({"id": 1, "name": "alice"})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "postgres".into(),
                offset: "0/16B6A70".into(),
                timestamp: 1716595200000,
            },
            ts: 1716595200000,
            schema: Some("public".into()),
            table: "users".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: Some(TransactionMetadata {
                tx_id: 42,
                total_events: 1,
                event_index: 0,
            }),
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    // ── RFC 3339 formatter ───────────────────────────────────────────────────

    #[test]
    fn rfc3339_unix_epoch() {
        assert_eq!(unix_ms_to_rfc3339(0), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn rfc3339_known_date() {
        // 2024-05-25T00:00:00.000Z = 1716595200000 ms
        assert_eq!(
            unix_ms_to_rfc3339(1716595200000),
            "2024-05-25T00:00:00.000Z"
        );
    }

    #[test]
    fn rfc3339_sub_second_preserved() {
        // 1716595200123 ms → should end in .123Z
        let ts = 1716595200123u64;
        assert!(unix_ms_to_rfc3339(ts).ends_with(".123Z"));
    }

    #[test]
    fn rfc3339_leap_day() {
        // 2024-02-29T00:00:00.000Z = 1709164800000 ms
        assert_eq!(
            unix_ms_to_rfc3339(1709164800000),
            "2024-02-29T00:00:00.000Z"
        );
    }

    // ── CloudEvents encoder ──────────────────────────────────────────────────

    #[test]
    fn specversion_is_always_one_zero() {
        let enc = CloudEventsEncoder::default();
        let out = enc.encode(&insert_event()).unwrap();
        let ce: serde_json::Value = serde_json::from_slice(&out.bytes).unwrap();
        assert_eq!(ce["specversion"], "1.0");
    }

    #[test]
    fn type_encodes_operation_name() {
        let enc = CloudEventsEncoder::default();
        let out = enc.encode(&insert_event()).unwrap();
        let ce: serde_json::Value = serde_json::from_slice(&out.bytes).unwrap();
        assert_eq!(ce["type"], "io.rustcdc.change.insert");
    }

    #[test]
    fn source_uses_connector_schema_table() {
        let enc = CloudEventsEncoder::default();
        let out = enc.encode(&insert_event()).unwrap();
        let ce: serde_json::Value = serde_json::from_slice(&out.bytes).unwrap();
        assert_eq!(ce["source"], "/postgres/public/users");
    }

    #[test]
    fn source_prefix_is_respected() {
        let enc = CloudEventsEncoder::new(Some("urn:cdc:myapp".to_string()));
        let out = enc.encode(&insert_event()).unwrap();
        let ce: serde_json::Value = serde_json::from_slice(&out.bytes).unwrap();
        assert_eq!(ce["source"], "urn:cdc:myapp/public/users");
    }

    #[test]
    fn subject_is_schema_dot_table() {
        let enc = CloudEventsEncoder::default();
        let out = enc.encode(&insert_event()).unwrap();
        let ce: serde_json::Value = serde_json::from_slice(&out.bytes).unwrap();
        assert_eq!(ce["subject"], "public.users");
    }

    #[test]
    fn subject_falls_back_to_table_when_no_schema() {
        let enc = CloudEventsEncoder::default();
        let mut ev = insert_event();
        ev.schema = None;
        let out = enc.encode(&ev).unwrap();
        let ce: serde_json::Value = serde_json::from_slice(&out.bytes).unwrap();
        assert_eq!(ce["subject"], "users");
    }

    #[test]
    fn cdc_extension_attributes_present() {
        let enc = CloudEventsEncoder::default();
        let out = enc.encode(&insert_event()).unwrap();
        let ce: serde_json::Value = serde_json::from_slice(&out.bytes).unwrap();
        assert_eq!(ce["cdcop"], "insert");
        assert_eq!(ce["cdctable"], "users");
        assert_eq!(ce["cdcschema"], "public");
        assert_eq!(ce["cdcsource"], "postgres");
        assert_eq!(ce["cdcoffset"], "0/16B6A70");
    }

    #[test]
    fn cdcschema_absent_when_no_schema() {
        let enc = CloudEventsEncoder::default();
        let mut ev = insert_event();
        ev.schema = None;
        let out = enc.encode(&ev).unwrap();
        let ce: serde_json::Value = serde_json::from_slice(&out.bytes).unwrap();
        assert!(
            ce.get("cdcschema").is_none(),
            "cdcschema must be absent when schema is None"
        );
    }

    #[test]
    fn data_contains_after_and_transaction() {
        let enc = CloudEventsEncoder::default();
        let out = enc.encode(&insert_event()).unwrap();
        let ce: serde_json::Value = serde_json::from_slice(&out.bytes).unwrap();
        let data = &ce["data"];
        assert_eq!(data["after"]["name"], "alice");
        assert_eq!(data["before"], serde_json::Value::Null);
        assert_eq!(data["transaction"]["tx_id"], 42);
    }

    #[test]
    fn content_type_is_cloudevents_json() {
        let enc = CloudEventsEncoder::default();
        assert_eq!(enc.content_type(), "application/cloudevents+json");
        let out = enc.encode(&insert_event()).unwrap();
        assert_eq!(out.content_type, "application/cloudevents+json");
    }

    #[test]
    fn update_event_type_is_update() {
        let enc = CloudEventsEncoder::default();
        let mut ev = insert_event();
        ev.op = Operation::Update;
        ev.before = Some(serde_json::json!({"id": 1, "name": "alice"}));
        ev.after = Some(serde_json::json!({"id": 1, "name": "alice-v2"}));
        let out = enc.encode(&ev).unwrap();
        let ce: serde_json::Value = serde_json::from_slice(&out.bytes).unwrap();
        assert_eq!(ce["type"], "io.rustcdc.change.update");
        assert_eq!(ce["cdcop"], "update");
    }
}
