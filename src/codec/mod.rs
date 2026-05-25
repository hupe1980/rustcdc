//! Wire-format encoders for CDC events.
//!
//! This module provides an [`EventEncoder`] trait and four built-in implementations
//! covering the most common wire formats used in event-streaming pipelines.
//!
//! | Encoder | Feature flag | Content-Type |
//! |---|---|---|
//! | [`JsonEncoder`] | *(always available)* | `application/json` |
//! | [`JsonPrettyEncoder`] | *(always available)* | `application/json` |
//! | [`CloudEventsEncoder`] | `cloudevents` | `application/cloudevents+json` |
//! | [`ProtobufEncoder`] | `protobuf` | `application/x-protobuf` |
//! | [`AvroEncoder`] | `avro` | `avro/binary` |
//!
//! # Usage
//!
//! ```rust
//! use cdc_rs::codec::{EventEncoder, JsonEncoder};
//! use cdc_rs::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
//!
//! let event = Event {
//!     before: None,
//!     after: Some(serde_json::json!({"id": 1, "name": "alice"})),
//!     op: Operation::Insert,
//!     source: SourceMetadata {
//!         source_name: "postgres".into(),
//!         offset: "0/16B6A70".into(),
//!         timestamp: 1,
//!     },
//!     ts: 1,
//!     schema: Some("public".into()),
//!     table: "users".into(),
//!     primary_key: Some(vec!["id".into()]),
//!     snapshot: None,
//!     transaction: None,
//!     envelope_version: EVENT_ENVELOPE_VERSION,
//! };
//!
//! let encoder = JsonEncoder;
//! let output = encoder.encode(&event).unwrap();
//! assert_eq!(output.content_type, "application/json");
//! assert!(!output.bytes.is_empty());
//! ```

pub mod json;
#[cfg(feature = "cloudevents")]
pub mod cloudevents;
#[cfg(feature = "protobuf")]
pub mod protobuf;
#[cfg(feature = "avro")]
pub mod avro;

pub use json::{JsonEncoder, JsonPrettyEncoder};
#[cfg(feature = "cloudevents")]
pub use cloudevents::CloudEventsEncoder;
#[cfg(feature = "protobuf")]
pub use protobuf::ProtobufEncoder;
#[cfg(feature = "avro")]
pub use avro::AvroEncoder;

use crate::core::{Event, Result};

// ─── EncodedOutput ────────────────────────────────────────────────────────────

/// Encoded event bytes with the associated MIME content type.
#[derive(Debug, Clone)]
pub struct EncodedOutput {
    /// The encoded bytes.
    pub bytes: Vec<u8>,
    /// MIME content type that describes the encoding.
    pub content_type: &'static str,
}

impl EncodedOutput {
    /// Create a new `EncodedOutput`.
    pub fn new(bytes: Vec<u8>, content_type: &'static str) -> Self {
        Self { bytes, content_type }
    }
}

// ─── EventEncoder ─────────────────────────────────────────────────────────────

/// Encodes a CDC [`Event`] into a specific wire format.
///
/// Implementations are `Send + Sync` so they can be shared across async tasks
/// (e.g. via `Arc<dyn EventEncoder>`).
///
/// # Implementing a custom encoder
///
/// ```rust
/// use cdc_rs::codec::{EncodedOutput, EventEncoder};
/// use cdc_rs::core::{Event, Result};
///
/// struct MyEncoder;
///
/// impl EventEncoder for MyEncoder {
///     fn encode(&self, event: &Event) -> Result<EncodedOutput> {
///         let bytes = format!("{}:{}", event.table, event.op).into_bytes();
///         Ok(EncodedOutput::new(bytes, "text/plain"))
///     }
///
///     fn content_type(&self) -> &'static str {
///         "text/plain"
///     }
/// }
/// ```
pub trait EventEncoder: Send + Sync {
    /// Encode a single CDC event into bytes.
    fn encode(&self, event: &Event) -> Result<EncodedOutput>;

    /// The MIME content type for every successful [`encode`](Self::encode) call.
    ///
    /// This is a constant associated with the encoder type, not with individual events.
    fn content_type(&self) -> &'static str;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use crate::codec::json::JsonEncoder;

    fn sample_event() -> Event {
        Event {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "0".into(),
                timestamp: 1,
            },
            ts: 1,
            schema: None,
            table: "t".into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    #[test]
    fn json_encoder_content_type_matches_output() {
        let enc = JsonEncoder;
        let out = enc.encode(&sample_event()).unwrap();
        assert_eq!(out.content_type, enc.content_type());
    }

    #[test]
    fn encoded_output_fields_accessible() {
        let out = EncodedOutput::new(b"hello".to_vec(), "text/plain");
        assert_eq!(out.content_type, "text/plain");
        assert_eq!(out.bytes, b"hello");
    }
}
