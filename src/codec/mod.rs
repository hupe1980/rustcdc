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
//! let event = Event::builder("users", Operation::Insert)
//!     .after(serde_json::json!({"id": 1, "name": "alice"}))
//!     .source(SourceMetadata::new("postgres", "0/16B6A70", 1))
//!     .ts(1)
//!     .schema("public")
//!     .primary_key(["id"])
//!     .build();
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
//! let event = Event::builder("", Operation::Insert)
//!     .after(json!({"id": 5, "name": "alice"}))
//!     .primary_key(["id"])
//!     .build();
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
pub use avro::{avro_value_to_event, AvroDecoder, AvroEncoder, AVRO_SCHEMA};
#[cfg(feature = "cloudevents")]
pub use cloudevents::CloudEventsEncoder;
pub use json::{JsonCodec, JsonEncoder, JsonPrettyEncoder};
#[cfg(feature = "protobuf")]
pub use protobuf::{ProtoEventKey, ProtobufEncoder};
#[cfg(feature = "glue")]
pub use schema_registry::glue;
#[cfg(feature = "glue")]
pub use schema_registry::glue::{GlueAvroConfig, GlueAvroDecoder, GlueAvroEncoder};
#[cfg(feature = "apicurio")]
pub use schema_registry::ApicurioRegistryConfig;
#[cfg(feature = "schemreg")]
pub use schema_registry::{
    decode_wire_format, detect_wire_format, encode_wire_format, preflight_schema_registry,
    warm_schema_cache, AnySchemaCache, CachedSchemaRegistry, CompatibilityLevel,
    ConfluentAvroCodec, ConfluentAvroDecoder, ConfluentAvroEncoder, ConfluentJsonSchemaCodec,
    ConfluentJsonSchemaDecoder, ConfluentJsonSchemaEncoder, ConfluentProtobufDecoder,
    ConfluentProtobufEncoder, ConfluentSchemaRegistry, DecodedMessage, DetectedWireFormat,
    DynSchemaRegistryClient, EncodeTarget, RetryPolicy, SchemaDecoder, SchemaEncoder, SchemaFormat,
    SchemaId, SchemaReference, SchemaRegError, SchemaRegistryAuth, SchemaRegistryClient,
    SchemaRegistryConfig, SchemaType, SchemaVersion, SubjectNameStrategy, WireFormatDecoder,
    DEFAULT_BASE_BACKOFF, DEFAULT_MAX_BACKOFF, DEFAULT_MAX_RETRIES, EVENT_JSON_SCHEMA,
    KEY_AVRO_SCHEMA, KEY_JSON_SCHEMA, KEY_PROTO_SCHEMA,
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
    /// [`Event::primary_key_values`] as compact JSON.
    ///
    /// Override this method to produce a different key format (e.g. Avro-encoded
    /// keys, string-formatted composite keys, or opaque binary keys).
    ///
    /// # `Ok(None)` and `Err` are different things
    ///
    /// This returns `Result<Option<Vec<u8>>>` because those two outcomes must not be
    /// confused, and a bare `Option` confused them:
    ///
    /// - **`Ok(None)`** — the event genuinely has no key. A `TRUNCATE`, a `SCHEMA_CHANGE`, a
    ///   table with no primary key, or a payload missing a key column (see
    ///   [`Event::primary_key_values`], which is all-or-nothing). Publishing it unkeyed is
    ///   correct: a Kafka producer round-robins it rather than collapsing every keyless event
    ///   onto one partition.
    /// - **`Err`** — encoding failed. The event *does* have a key and the encoder could not
    ///   render it.
    ///
    /// Collapsing the second into the first is a silent correctness failure, not a lost error
    /// message. A keyed sink treats `None` as "unkeyed", so the record is produced without a
    /// key: partition routing becomes round-robin, **ordering for that row is lost**, and log
    /// compaction stops collapsing it. The record still arrives, so nothing looks wrong. This
    /// is the same failure the transform pipeline refuses to allow a transform to cause — see
    /// [`TransformPipeline::apply`](crate::transform::TransformPipeline::apply), which errors
    /// rather than emit an event whose key it destroyed — and it would have been inconsistent
    /// to let an encoder cause it quietly.
    ///
    /// # Use case
    ///
    /// Kafka / Pulsar producers need a separate key payload for message routing and
    /// log-compaction. Passing the result of `encode_key()` as the Kafka message key
    /// ensures that all events for the same row land on the same partition.
    ///
    /// # Errors
    ///
    /// Returns [`Error::SerializationError`] when the key cannot be encoded.
    ///
    /// # Example
    ///
    /// ```
    /// use rustcdc::codec::{EventEncoder, JsonEncoder};
    /// use rustcdc::{Event, Operation, EVENT_ENVELOPE_VERSION};
    /// use serde_json::json;
    ///
    /// let event = Event::builder("", Operation::Insert)
    ///     .after(json!({"id": 5, "name": "alice"}))
    ///     .primary_key(["id"])
    ///     .build();
    ///
    /// let encoder = JsonEncoder;
    /// let key = encoder.encode_key(&event).unwrap().unwrap();
    /// assert_eq!(key, br#"{"id":5}"#);
    ///
    /// // A keyless event is `Ok(None)`, not an error: publishing it unkeyed is correct.
    /// let truncate = Event::builder("t", Operation::Truncate).build();
    /// assert!(encoder.encode_key(&truncate).unwrap().is_none());
    /// ```
    fn encode_key(&self, event: &Event) -> Result<Option<Vec<u8>>> {
        let Some(value) = event.primary_key_values() else {
            return Ok(None);
        };
        Ok(Some(serde_json::to_vec(&value)?))
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
/// let event = Event::builder("", Operation::Insert)
///     .after(json!({"id": 1}))
///     .primary_key(["id"])
///     .build();
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
        // `?` rather than swallowing: an encoder that could not render a key must not be
        // read as "this event has no key", which would publish the record unkeyed.
        let key = self.encoder.encode_key(event)?;
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

// ─── AsyncCodec ───────────────────────────────────────────────────────────────

/// The `async` counterpart of [`Codec`], for encoders that must await.
///
/// # Why this exists
///
/// Two of the three Confluent encoders resolve their subject **lazily**, on first
/// encode, because `SubjectNameStrategy::RecordName` and `TopicRecordName` exist precisely
/// to give each record type its own subject — resolving eagerly at construction would
/// defeat them. Their `encode` is therefore `async`, which fits neither [`Codec`] nor
/// [`EventEncoder`].
///
/// Before this trait, a sink that wanted to hold "some codec" could not hold all three
/// Confluent formats behind one type: `ConfluentAvroEncoder` reached [`BoxedCodec`] via
/// [`EncoderCodec`], while `ConfluentJsonSchemaEncoder` and `ConfluentProtobufEncoder`
/// sat outside the type system entirely, and every embedder hand-rolled the same
/// three-variant dispatch enum. `AsyncCodec` is that enum, once, in the library.
///
/// (The Confluent encoders are named as plain code rather than linked: they live behind
/// the `schemreg` feature, and an intra-doc link from this ungated trait to a gated item
/// is a broken link in every build that does not enable it.)
///
/// # Relationship to [`Codec`]
///
/// There is a blanket `impl<T: Codec> AsyncCodec for T`, so **every synchronous codec is
/// already an `AsyncCodec`** and a sink only ever needs to accept this one trait. Generic
/// code bounded on `AsyncCodec` therefore takes [`JsonCodec`] and
/// `ConfluentProtobufEncoder` alike.
///
/// Two consequences of the blanket impl are worth knowing:
///
/// - A type cannot implement both traits by hand. Implement [`Codec`] when encoding never
///   awaits, and `AsyncCodec` only when it must.
/// - The encode method is [`encode_async`](Self::encode_async), *not* `encode`. A trait
///   that is blanket-implemented over another must not reuse its method names: with both
///   traits in scope, `codec.encode(..)` would be an `E0034` ambiguity on every synchronous
///   codec — an error the library would be handing to its users on the hottest call in the
///   API. [`content_type`](Self::content_type) does share a name, because the blanket impl
///   forwards it and both traits return the same value, so the worst case there is a
///   compiler-suggested disambiguation that cannot change behaviour.
///
/// # Example
///
/// ```rust
/// use rustcdc::codec::{AsyncCodec, BoxedAsyncCodec, JsonCodec};
///
/// # async fn example() -> rustcdc::core::Result<()> {
/// // A synchronous codec crosses over for free via the blanket impl.
/// let codec: BoxedAsyncCodec = JsonCodec::default().boxed_async();
///
/// # let event = rustcdc::Event::builder("t", rustcdc::Operation::Insert).build();
/// let out = codec.encode_async(&event).await?;
/// assert_eq!(out.content_type, "application/json");
/// # Ok(()) }
/// ```
#[async_trait::async_trait]
pub trait AsyncCodec: Send + Sync {
    /// Encode the event into a key + value pair, awaiting whatever the encoder needs.
    ///
    /// Named `encode_async` rather than `encode` so that it never collides with
    /// [`Codec::encode`] on the types the blanket impl covers — see the trait docs.
    ///
    /// # Errors
    ///
    /// Whatever the underlying encoder reports — for the registry-backed codecs, a
    /// classified source error when the registry is unreachable (`Transient`) or the
    /// subject cannot be resolved (`Terminal`).
    async fn encode_async(&self, event: &Event) -> Result<CodecOutput>;

    /// The MIME content type for every successful `value` byte sequence.
    fn content_type(&self) -> &'static str;

    /// Wrap `self` in a [`BoxedAsyncCodec`], erasing the concrete type.
    fn boxed_async(self) -> BoxedAsyncCodec
    where
        Self: Sized + 'static,
    {
        BoxedAsyncCodec::new(self)
    }
}

#[async_trait::async_trait]
impl<T: Codec> AsyncCodec for T {
    async fn encode_async(&self, event: &Event) -> Result<CodecOutput> {
        Codec::encode(self, event)
    }

    fn content_type(&self) -> &'static str {
        Codec::content_type(self)
    }
}

/// A type-erased [`AsyncCodec`].
///
/// The async counterpart of [`BoxedCodec`]. Because of the blanket
/// `impl<T: Codec> AsyncCodec for T`, this holds synchronous and asynchronous codecs
/// alike — which is the point: a sink stores one type regardless of format.
///
/// ```rust
/// use rustcdc::codec::{AsyncCodec, BoxedAsyncCodec, JsonCodec};
///
/// let codec: BoxedAsyncCodec = JsonCodec::default().boxed_async();
/// assert_eq!(AsyncCodec::content_type(&codec), "application/json");
/// ```
pub struct BoxedAsyncCodec(Box<dyn AsyncCodec>);

impl std::fmt::Debug for BoxedAsyncCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxedAsyncCodec")
            .field("content_type", &self.0.content_type())
            .finish()
    }
}

impl BoxedAsyncCodec {
    /// Wrap any [`AsyncCodec`] implementation in a type-erased `BoxedAsyncCodec`.
    ///
    /// Prefer [`AsyncCodec::boxed_async`] for ergonomic construction.
    pub fn new<C: AsyncCodec + 'static>(codec: C) -> Self {
        Self(Box::new(codec))
    }
}

#[async_trait::async_trait]
impl AsyncCodec for BoxedAsyncCodec {
    async fn encode_async(&self, event: &Event) -> Result<CodecOutput> {
        self.0.encode_async(event).await
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
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
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
        assert_eq!(out.content_type, Codec::content_type(&codec));
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

    // ─── AsyncCodec ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_sync_codec_is_an_async_codec_via_the_blanket_impl() {
        use crate::codec::json::JsonCodec;
        let codec = JsonCodec::default();
        let mut event = sample_event();
        event.primary_key = Some(vec!["id".into()]);

        let out = codec.encode_async(&event).await.unwrap();
        assert_eq!(out.content_type, "application/json");
        assert!(out.key.is_some());
    }

    #[tokio::test]
    async fn boxed_async_codec_erases_the_concrete_type() {
        use crate::codec::json::JsonCodec;
        // The point of the trait: one type holds sync and async codecs alike, so a sink
        // does not need a hand-rolled dispatch enum per registry format.
        let codecs: Vec<BoxedAsyncCodec> = vec![
            JsonCodec::default().boxed_async(),
            EncoderCodec::new(JsonEncoder).boxed_async(),
        ];

        let mut event = sample_event();
        event.primary_key = Some(vec!["id".into()]);
        for codec in &codecs {
            let out = codec.encode_async(&event).await.unwrap();
            assert_eq!(AsyncCodec::content_type(codec), "application/json");
            assert!(out.key.is_some());
        }
    }

    #[test]
    fn boxed_async_codec_is_send_and_sync() {
        // A sink shares one across tasks; if this ever stops holding, every caller has to
        // reach for a mutex.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<BoxedAsyncCodec>();
    }
}

#[cfg(test)]
mod key_encoding_contract_tests {
    use super::{EventEncoder, JsonEncoder};
    use crate::core::{Event, Operation, SourceMetadata};

    /// `Ok(None)` and `Err` mean different things, and conflating them is a silent
    /// correctness failure rather than a lost error message.
    ///
    /// A keyed sink reads `None` as "unkeyed" and publishes the record without a key:
    /// partition routing becomes round-robin, ordering for that row is lost, and log
    /// compaction stops collapsing it. The record still arrives, so nothing looks wrong. The
    /// transform pipeline already refuses to let a *transform* cause that; an encoder must not
    /// be able to cause it quietly either.
    #[test]
    fn a_keyless_event_is_ok_none_and_never_an_error() {
        let encoder = JsonEncoder;

        // No `primary_key` declared at all.
        let keyless = Event::builder("t", Operation::Insert)
            .after(serde_json::json!({ "a": "1" }))
            .build();
        assert!(matches!(encoder.encode_key(&keyless), Ok(None)));

        // TRUNCATE and SCHEMA_CHANGE carry no row to take a key from.
        for op in [Operation::Truncate, Operation::SchemaChange] {
            let event = Event::builder("t", op).build();
            assert!(
                matches!(encoder.encode_key(&event), Ok(None)),
                "a {op} event has no key, which is not an error"
            );
        }

        // A composite key with a column missing is `None` too — `primary_key_values` is
        // all-or-nothing, because a truncated key addresses more rows than the event describes.
        let partial = Event::builder("t", Operation::Insert)
            .after(serde_json::json!({ "tenant_id": "7" }))
            .primary_key(["tenant_id", "id"])
            .build();
        assert!(matches!(encoder.encode_key(&partial), Ok(None)));
    }

    #[test]
    fn a_resolvable_key_is_encoded() {
        let event = Event::builder("t", Operation::Insert)
            .after(serde_json::json!({ "id": "5", "name": "alice" }))
            .primary_key(["id"])
            .source(SourceMetadata::new("pg", "0/1", 1))
            .ts(1)
            .build();
        let key = JsonEncoder
            .encode_key(&event)
            .expect("encoding succeeds")
            .expect("the event has a resolvable key");
        assert_eq!(key, br#"{"id":"5"}"#);
    }

    /// The combined codec must propagate a key-encoding failure rather than reporting the
    /// event as keyless, which is what `CodecOutput { key: None, .. }` means downstream.
    #[test]
    fn the_combined_codec_propagates_a_key_failure_rather_than_reporting_no_key() {
        use crate::codec::{Codec, EncodedOutput, EncoderCodec};

        struct BrokenKeyEncoder;
        impl EventEncoder for BrokenKeyEncoder {
            fn encode(&self, _event: &Event) -> crate::core::Result<EncodedOutput> {
                Ok(EncodedOutput::new(b"{}".to_vec(), "application/json"))
            }
            fn content_type(&self) -> &'static str {
                "application/json"
            }
            fn encode_key(&self, _event: &Event) -> crate::core::Result<Option<Vec<u8>>> {
                Err(crate::core::Error::SerializationError("key broke".into()))
            }
        }

        let event = Event::builder("t", Operation::Insert)
            .after(serde_json::json!({ "id": "1" }))
            .primary_key(["id"])
            .build();
        let error = EncoderCodec::new(BrokenKeyEncoder)
            .encode(&event)
            .expect_err("a key-encoding failure must surface");
        assert!(error.to_string().contains("key broke"), "{error}");
    }
}
