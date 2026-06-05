//! JSON event encoder.

use crate::codec::{EncodedOutput, EncoderCodec, EventEncoder};
use crate::core::{Event, Result};

// ─── JsonEncoder ──────────────────────────────────────────────────────────────

/// Encodes CDC events as compact (single-line) JSON.
///
/// This is the canonical in-process wire format used throughout rustcdc.
/// It is a zero-copy re-serialization of the [`Event`] struct using
/// `serde_json::to_vec`.
///
/// Content-Type: `application/json`
#[derive(Debug, Clone, Default)]
pub struct JsonEncoder;

impl EventEncoder for JsonEncoder {
    fn encode(&self, event: &Event) -> Result<EncodedOutput> {
        let bytes = serde_json::to_vec(event)?;
        Ok(EncodedOutput::new(bytes, self.content_type()))
    }

    fn content_type(&self) -> &'static str {
        "application/json"
    }
}

// ─── JsonPrettyEncoder ────────────────────────────────────────────────────────

/// Encodes CDC events as pretty-printed (human-readable) JSON.
///
/// Indentation uses the serde_json default (two spaces).  Useful for
/// development, debugging, and low-volume log sinks.  Not recommended for
/// high-throughput production pipelines due to increased byte overhead.
///
/// Content-Type: `application/json`
#[derive(Debug, Clone, Default)]
pub struct JsonPrettyEncoder;

impl EventEncoder for JsonPrettyEncoder {
    fn encode(&self, event: &Event) -> Result<EncodedOutput> {
        let bytes = serde_json::to_vec_pretty(event)?;
        Ok(EncodedOutput::new(bytes, self.content_type()))
    }

    fn content_type(&self) -> &'static str {
        "application/json"
    }
}

// ─── JsonCodec ───────────────────────────────────────────────────────────────

/// Convenience type alias for an [`EncoderCodec`] backed by [`JsonEncoder`].
///
/// Use this when you need a [`Codec`] that produces compact JSON key+value pairs.
/// The key is the compact-JSON serialisation of the event's primary key (via
/// `EventEncoder::encode_key`); the value is the full compact JSON event.
///
/// # Example
///
/// ```rust
/// use rustcdc::codec::{Codec, JsonCodec};
/// use rustcdc::{Event, Operation, EVENT_ENVELOPE_VERSION};
/// use serde_json::json;
///
/// let event = Event {
///     after: Some(json!({"id": 42, "name": "alice"})),
///     op: Operation::Insert,
///     primary_key: Some(vec!["id".into()]),
///     ..Event::default()
/// };
///
/// let codec = JsonCodec::default();
/// let out = codec.encode(&event).unwrap();
/// assert!(out.key.is_some(), "primary key should be encoded");
/// assert_eq!(out.content_type, "application/json");
/// ```
///
/// [`Codec`]: crate::codec::Codec
///
/// To construct: use `JsonCodec::default()` (derives [`Default`] from [`EncoderCodec`]) or
/// `EncoderCodec::new(JsonEncoder)`.
pub type JsonCodec = EncoderCodec<JsonEncoder>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};

    fn event() -> Event {
        Event {
            before: None,
            after: Some(serde_json::json!({"id": 1, "name": "alice"})),
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
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
        }
    }

    #[test]
    fn compact_json_is_single_line() {
        let out = JsonEncoder.encode(&event()).unwrap();
        let s = std::str::from_utf8(&out.bytes).unwrap();
        assert!(!s.contains('\n'), "compact JSON must not contain newlines");
    }

    #[test]
    fn pretty_json_is_multi_line() {
        let out = JsonPrettyEncoder.encode(&event()).unwrap();
        let s = std::str::from_utf8(&out.bytes).unwrap();
        assert!(s.contains('\n'), "pretty JSON must contain newlines");
    }

    #[test]
    fn json_roundtrip_preserves_event() {
        let original = event();
        let out = JsonEncoder.encode(&original).unwrap();
        let decoded: Event = serde_json::from_slice(&out.bytes).unwrap();
        assert_eq!(decoded, original);
    }
}
