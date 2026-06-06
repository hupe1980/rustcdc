//! Wire-format encoders for CDC events.
//!
//! This module provides two complementary traits:
//!
//! - [`EventEncoder`] — low-level, encodes a single event into *value* bytes and a MIME
//!   content type.  Use this when your downstream system handles key routing separately.
//!
//! - [`Codec`] — higher-level, produces a `(`[`CodecOutput`]`)` containing both the optional
//!   *key* bytes (for Kafka/Pulsar message keys) and the *value* bytes in a single call.
//!   Built on top of any [`EventEncoder`] via [`EncoderCodec`].
//!
//! | Encoder / Codec | Feature flag | Content-Type |
//! |---|---|---|
//! | [`JsonEncoder`] / [`JsonCodec`] | *(always available)* | `application/json` |
//! | [`JsonPrettyEncoder`] | *(always available)* | `application/json` |
//! | `CloudEventsEncoder` | `cloudevents` | `application/cloudevents+json` |
//! | `ProtobufEncoder` | `protobuf` | `application/x-protobuf` |
//! | `AvroEncoder` | `avro` | `avro/binary` |
//!
//! # Usage — `EventEncoder`
//!
//! ```rust
//! use rustcdc::codec::{EventEncoder, JsonEncoder};
//! use rustcdc::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
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
//!     before_is_key_only: false,
//! };
//!
//! let encoder = JsonEncoder;
//! let output = encoder.encode(&event).unwrap();
//! assert_eq!(output.content_type, "application/json");
//! assert!(!output.bytes.is_empty());
//! ```
//!
//! # Usage — `Codec` (key + value)
//!
//! ```rust
//! use rustcdc::codec::{Codec, JsonCodec};
//! use rustcdc::{Event, Operation, EVENT_ENVELOPE_VERSION};
//! use serde_json::json;
//!
//! let event = Event {
//!     after: Some(json!({"id": 5, "name": "alice"})),
//!     op: Operation::Insert,
//!     primary_key: Some(vec!["id".into()]),
//!     ..Event::default()
//! };
//!
//! let codec = JsonCodec::default();
//! let output = codec.encode(&event).unwrap();
//! assert!(output.key.is_some()); // compact JSON of primary key
//! assert!(!output.value.is_empty()); // full event JSON
//! ```

#[cfg(feature = "avro")]
pub mod avro;
#[cfg(feature = "cloudevents")]
pub mod cloudevents;
pub mod json;
#[cfg(feature = "protobuf")]
pub mod protobuf;
#[cfg(feature = "schemreg")]
pub mod schema_registry;

#[cfg(feature = "avro")]
pub use avro::AvroEncoder;
#[cfg(feature = "cloudevents")]
pub use cloudevents::CloudEventsEncoder;
pub use json::{JsonCodec, JsonEncoder, JsonPrettyEncoder};
#[cfg(feature = "protobuf")]
pub use protobuf::ProtobufEncoder;
#[cfg(feature = "schemreg")]
pub use schema_registry::{
    decode_wire_format, encode_wire_format, CachedSchemaRegistry, CompatibilityLevel,
    ConfluentAvroCodec, ConfluentAvroDecoder, ConfluentAvroEncoder, ConfluentSchemaRegistry,
    EncodeTarget, SchemaId, SchemaRegistryAuth, SchemaRegistryClient, SchemaRegistryConfig,
    SchemaType, SubjectNameStrategy,
};

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
        Self {
            bytes,
            content_type,
        }
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
/// use rustcdc::codec::{EncodedOutput, EventEncoder};
/// use rustcdc::core::{Event, Result};
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

    /// Encode the primary-key columns of an event as a compact JSON key.
    ///
    /// The default implementation serialises the object returned by
    /// [`Event::primary_key_values`] as compact JSON. Returns `None` when the
    /// event has no `primary_key` defined, when none of the key columns appear in
    /// the row image, or when serialisation fails.
    ///
    /// Override this method to produce a different key format (e.g. Avro-encoded
    /// keys, string-formatted composite keys, or opaque binary keys).
    ///
    /// # Use case
    ///
    /// Kafka / Pulsar producers need a separate key payload for message routing and
    /// log-compaction. Passing the result of `encode_key()` as the Kafka message key
    /// ensures that all events for the same row land on the same partition.
    ///
    /// # Example
    ///
    /// ```
    /// use rustcdc::codec::{EventEncoder, JsonEncoder};
    /// use rustcdc::{Event, Operation, EVENT_ENVELOPE_VERSION};
    /// use serde_json::json;
    ///
    /// let event = Event {
    ///     after: Some(json!({"id": 5, "name": "alice"})),
    ///     op: Operation::Insert,
    ///     primary_key: Some(vec!["id".into()]),
    ///     ..Event::default()
    /// };
    ///
    /// let encoder = JsonEncoder;
    /// let key = encoder.encode_key(&event).unwrap();
    /// assert_eq!(key, br#"{"id":5}"#);
    /// ```
    fn encode_key(&self, event: &Event) -> Option<Vec<u8>> {
        let value = event.primary_key_values()?;
        serde_json::to_vec(&value).ok()
    }
}

// ─── Codec ────────────────────────────────────────────────────────────────────

/// Combined key + value encoding of a CDC event.
///
/// Returned by [`Codec::encode`].
#[derive(Debug, Clone)]
pub struct CodecOutput {
    /// Encoded message key, or `None` when the event has no primary key.
    ///
    /// For Kafka producers: pass this as the Kafka message key so all events
    /// for the same row land on the same partition and log-compaction works.
    pub key: Option<Vec<u8>>,
    /// Encoded event value bytes.
    pub value: Vec<u8>,
    /// MIME content type of the *value* bytes.
    pub content_type: &'static str,
}

impl CodecOutput {
    /// Create a new `CodecOutput`.
    pub fn new(key: Option<Vec<u8>>, value: Vec<u8>, content_type: &'static str) -> Self {
        Self {
            key,
            value,
            content_type,
        }
    }
}

/// Higher-level encoding abstraction that produces both a key and a value.
///
/// This is the right abstraction for Kafka / Pulsar producers and any pipeline
/// that must route or compact messages by primary key. Prefer [`Codec`] over
/// [`EventEncoder`] when building producer-side integrations.
///
/// Implementations must be `Send + Sync` so they can be shared across async tasks
/// (e.g. behind an `Arc<dyn Codec>`).
///
/// # Implementing a custom codec
///
/// ```rust
/// use rustcdc::codec::{Codec, CodecOutput};
/// use rustcdc::core::{Event, Result};
///
/// struct MyAvroCodec;
///
/// impl Codec for MyAvroCodec {
///     fn encode(&self, event: &Event) -> Result<CodecOutput> {
///         let key = event.primary_key_values()
///             .and_then(|v| serde_json::to_vec(&v).ok());
///         let value = serde_json::to_vec(event)?;
///         Ok(CodecOutput::new(key, value, "application/json"))
///     }
///
///     fn content_type(&self) -> &'static str {
///         "application/json"
///     }
/// }
/// ```
pub trait Codec: Send + Sync {
    /// Encode the event into a key + value pair.
    fn encode(&self, event: &Event) -> Result<CodecOutput>;

    /// The MIME content type for every successful `value` byte sequence.
    fn content_type(&self) -> &'static str;

    /// Wrap `self` in a [`BoxedCodec`], erasing the concrete type.
    ///
    /// Enables storing heterogeneous codecs without a shared enum or generic
    /// parameters on the containing struct.  Requires `Self: 'static`.
    fn boxed(self) -> BoxedCodec
    where
        Self: Sized + 'static,
    {
        BoxedCodec::new(self)
    }
}

/// A [`Codec`] adapter that wraps any [`EventEncoder`].
///
/// `EncoderCodec` calls `EventEncoder::encode_key` for the key and
/// `EventEncoder::encode` for the value.  It is the canonical bridge between the
/// lower-level [`EventEncoder`] trait and the higher-level [`Codec`] trait.
///
/// # Example
///
/// ```rust
/// use rustcdc::codec::{Codec, EncoderCodec, JsonEncoder};
/// use rustcdc::{Event, Operation, EVENT_ENVELOPE_VERSION};
/// use serde_json::json;
///
/// let codec = EncoderCodec::new(JsonEncoder);
/// let event = Event {
///     after: Some(json!({"id": 1})),
///     op: Operation::Insert,
///     primary_key: Some(vec!["id".into()]),
///     ..Event::default()
/// };
/// let out = codec.encode(&event).unwrap();
/// assert_eq!(out.content_type, "application/json");
/// assert!(out.key.is_some());
/// ```
#[derive(Debug, Clone, Default)]
pub struct EncoderCodec<E> {
    encoder: E,
}

impl<E: EventEncoder> EncoderCodec<E> {
    /// Wrap an `EventEncoder` as a `Codec`.
    pub fn new(encoder: E) -> Self {
        Self { encoder }
    }

    /// Borrow the inner encoder.
    pub fn encoder(&self) -> &E {
        &self.encoder
    }

    /// Unwrap the inner encoder.
    pub fn into_encoder(self) -> E {
        self.encoder
    }
}

impl<E: EventEncoder> Codec for EncoderCodec<E> {
    fn encode(&self, event: &Event) -> Result<CodecOutput> {
        let key = self.encoder.encode_key(event);
        let value_output = self.encoder.encode(event)?;
        Ok(CodecOutput::new(
            key,
            value_output.bytes,
            value_output.content_type,
        ))
    }

    fn content_type(&self) -> &'static str {
        self.encoder.content_type()
    }
}

// ─── BoxedCodec ───────────────────────────────────────────────────────────────

/// A type-erased [`Codec`] that wraps any concrete codec behind a single type.
///
/// `BoxedCodec` removes the need for generic parameters when storing or
/// passing codecs across pipeline stages that support multiple encodings
/// (JSON, CloudEvents, Avro, …) without a shared enum.
///
/// # Construction
///
/// ```rust
/// use rustcdc::codec::{BoxedCodec, Codec, JsonCodec};
///
/// // Via the .boxed() convenience method (preferred):
/// let codec: BoxedCodec = JsonCodec::default().boxed();
///
/// // Or explicitly:
/// let codec = BoxedCodec::new(JsonCodec::default());
/// ```
pub struct BoxedCodec(Box<dyn Codec>);

impl BoxedCodec {
    /// Wrap any [`Codec`] implementation in a type-erased `BoxedCodec`.
    ///
    /// Prefer [`Codec::boxed`] for ergonomic construction.
    pub fn new<C: Codec + 'static>(codec: C) -> Self {
        Self(Box::new(codec))
    }
}

impl Codec for BoxedCodec {
    fn encode(&self, event: &Event) -> Result<CodecOutput> {
        self.0.encode(event)
    }

    fn content_type(&self) -> &'static str {
        self.0.content_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::json::JsonEncoder;
    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};

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
            before_is_key_only: false,
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

    #[test]
    fn encoder_codec_no_primary_key_gives_no_key() {
        let codec = EncoderCodec::new(JsonEncoder);
        let event = sample_event(); // primary_key = None
        let out = codec.encode(&event).unwrap();
        assert!(out.key.is_none());
        assert!(!out.value.is_empty());
        assert_eq!(out.content_type, "application/json");
    }

    #[test]
    fn encoder_codec_with_primary_key_encodes_key() {
        let codec = EncoderCodec::new(JsonEncoder);
        let mut event = sample_event();
        event.primary_key = Some(vec!["id".into()]);
        event.after = Some(serde_json::json!({"id": 7, "name": "bob"}));
        let out = codec.encode(&event).unwrap();
        let key = out.key.expect("key should be present");
        let parsed: serde_json::Value = serde_json::from_slice(&key).unwrap();
        assert_eq!(parsed["id"], 7);
    }

    #[test]
    fn encoder_codec_content_type_matches_encoder() {
        let codec = EncoderCodec::new(JsonEncoder);
        let event = sample_event();
        let out = codec.encode(&event).unwrap();
        assert_eq!(out.content_type, codec.content_type());
    }

    #[test]
    fn codec_output_constructor() {
        let o = CodecOutput::new(Some(b"k".to_vec()), b"v".to_vec(), "text/plain");
        assert_eq!(o.key.unwrap(), b"k");
        assert_eq!(o.value, b"v");
        assert_eq!(o.content_type, "text/plain");
    }

    #[test]
    fn json_codec_default_works() {
        use crate::codec::json::JsonCodec;
        let codec = JsonCodec::default();
        let mut event = sample_event();
        event.primary_key = Some(vec!["id".into()]);
        event.after = Some(serde_json::json!({"id": 1}));
        let out = codec.encode(&event).unwrap();
        assert!(out.key.is_some());
        assert_eq!(out.content_type, "application/json");
    }

    #[test]
    fn boxed_codec_erases_type_and_encodes() {
        use crate::codec::json::JsonCodec;
        let codec: BoxedCodec = JsonCodec::default().boxed();
        let mut event = sample_event();
        event.primary_key = Some(vec!["id".into()]);
        event.after = Some(serde_json::json!({"id": 42}));
        let out = codec.encode(&event).unwrap();
        assert_eq!(out.content_type, "application/json");
        assert!(out.key.is_some());
    }

    #[test]
    fn boxed_codec_new_works() {
        use crate::codec::json::JsonCodec;
        let codec = BoxedCodec::new(JsonCodec::default());
        let event = sample_event();
        let out = codec.encode(&event).unwrap();
        assert!(!out.value.is_empty());
    }
}
