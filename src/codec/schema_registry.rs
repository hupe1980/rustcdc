//! Confluent Schema Registry integration for rustcdc, backed by the [`schemreg`] crate.
//!
//! # Overview
//!
//! This module provides two rustcdc-specific adapters on top of `schemreg`:
//!
//! | Type | Purpose |
//! |---|---|
//! | [`ConfluentAvroEncoder`] | Implements [`EventEncoder`] — serialises a CDC [`Event`] to Confluent-framed Avro |
//! | [`ConfluentAvroDecoder`] | Deserialises Confluent-framed Avro bytes back to a CDC [`Event`] |
//!
//! Everything else (HTTP client, caching, wire format, subject naming) is provided directly by
//! `schemreg` and re-exported here for convenience.
//!
//! # Debezium compatibility
//!
//! Debezium's Avro converter registers a **separate key schema** per topic
//! (`{topic}-key`) using a record with a single `key: ["null", "string"]` field.
//! [`ConfluentAvroEncoder`] mirrors this exactly:
//!
//! - The **value** subject is resolved via [`SubjectNameStrategy`].
//! - The **key** subject uses the same strategy with [`EncodeTarget::Key`], carrying
//!   the [`KEY_AVRO_SCHEMA`] constant.
//!
//! Both are registered (or looked up) at encoder construction time.

use std::sync::Arc;
use std::time::Duration;

use apache_avro::Schema;

// ─── schemreg re-exports ──────────────────────────────────────────────────────

pub use ::schemreg::confluent::ConfluentSchemaRegistry;
pub use ::schemreg::wire::{decode_wire_format, encode_wire_format};
pub use ::schemreg::{
    CachedSchemaRegistry, CompatibilityLevel, EncodeTarget, SchemaId, SchemaRegistryClient,
    SchemaType, SubjectNameStrategy,
};

use crate::codec::avro::AvroEncoder;
use crate::codec::{EncodedOutput, EventEncoder};
use crate::core::{Error, Event, Result, SecretString};

// ─── Wire format content type ─────────────────────────────────────────────────

const CONFLUENT_CONTENT_TYPE: &str = "application/vnd.kafka+avro";

// ─── Key schema ───────────────────────────────────────────────────────────────

/// Avro schema for the primary-key envelope produced by [`ConfluentAvroEncoder::encode_key`].
///
/// Mirrors Debezium\'s key schema: a record with a single `key` field that is
/// a nullable string carrying the JSON-encoded primary key.
pub const KEY_AVRO_SCHEMA: &str = r#"{
  "type": "record",
  "name": "EventKey",
  "namespace": "io.rustcdc",
  "fields": [
    {
      "name": "key",
      "type": ["null", "string"],
      "default": null
    }
  ]
}"#;

// ─── SchemaRegistryAuth ───────────────────────────────────────────────────────

/// Authentication credentials for the Confluent Schema Registry HTTP client.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum SchemaRegistryAuth {
    /// HTTP Basic authentication (username + password).
    Basic { username: String, password: String },
    /// OAuth / IAM bearer token stored as a [`SecretString`] to prevent accidental logging.
    BearerToken(SecretString),
}

// ─── SchemaRegistryConfig ─────────────────────────────────────────────────────

/// Configuration for the Confluent Schema Registry client and CDC Avro encoders.
///
/// Call [`build`](Self::build) to construct a
/// [`CachedSchemaRegistry<ConfluentSchemaRegistry>`] ready for use with
/// [`ConfluentAvroEncoder`] and [`ConfluentAvroDecoder`].
#[derive(Clone, Debug)]
pub struct SchemaRegistryConfig {
    /// Schema Registry base URL (trailing slash is trimmed automatically).
    pub url: String,
    /// Kafka topic name used to derive value and key subject names via
    /// [`SubjectNameStrategy`].
    pub topic: String,
    /// Subject name strategy. Defaults to [`SubjectNameStrategy::TopicName`].
    pub strategy: SubjectNameStrategy,
    /// Optional authentication credentials.
    pub auth: Option<SchemaRegistryAuth>,
    /// When `true` (default), register value and key schemas automatically on
    /// first use. Set to `false` to require the schemas to already exist.
    pub auto_register: bool,
    /// HTTP request timeout in milliseconds. `None` uses the `schemreg` default (30 s).
    pub request_timeout_ms: Option<u64>,
    /// Maximum number of schema entries to keep in the in-memory cache.
    /// `None` uses the `schemreg` default (1 000).
    pub max_cache_entries: Option<usize>,
    /// TCP connection establishment timeout in milliseconds. `None` uses the
    /// `schemreg` default (no explicit connect timeout).
    pub connect_timeout_ms: Option<u64>,
    /// When `true`, append `?normalize=true` to schema registration requests so
    /// the registry normalises the schema before storing it (default: `false`).
    ///
    /// Useful when Avro schemas are generated programmatically from table metadata
    /// and may differ in field ordering across producers, causing schema ID churn.
    pub normalize_schemas: bool,
    /// Maximum number of idle keep-alive connections per host in the underlying
    /// HTTP connection pool. `None` uses the `reqwest` default (unlimited).
    ///
    /// Tune this when all producers share a single Schema Registry host to cap
    /// idle connection overhead under bursty traffic.
    pub pool_max_idle_per_host: Option<usize>,
}

impl SchemaRegistryConfig {
    /// Create a config with the given Schema Registry URL and Kafka topic.
    pub fn new(url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            url: url.into().trim_end_matches('/').to_owned(),
            topic: topic.into(),
            strategy: SubjectNameStrategy::TopicName,
            auth: None,
            auto_register: true,
            request_timeout_ms: None,
            max_cache_entries: None,
            connect_timeout_ms: None,
            normalize_schemas: false,
            pool_max_idle_per_host: None,
        }
    }

    /// Set the subject name strategy.
    pub fn with_strategy(mut self, strategy: SubjectNameStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set authentication credentials.
    pub fn with_auth(mut self, auth: SchemaRegistryAuth) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Enable or disable automatic schema registration (default: `true`).
    pub fn with_auto_register(mut self, auto_register: bool) -> Self {
        self.auto_register = auto_register;
        self
    }

    /// Set the HTTP request timeout in milliseconds.
    pub fn with_request_timeout_ms(mut self, ms: u64) -> Self {
        self.request_timeout_ms = Some(ms);
        self
    }

    /// Set the maximum number of cached schema entries.
    pub fn with_max_cache_entries(mut self, n: usize) -> Self {
        self.max_cache_entries = Some(n);
        self
    }

    /// Set the TCP connection establishment timeout in milliseconds.
    ///
    /// Controls how long the client waits before giving up on the initial TCP
    /// handshake.  Separate from `request_timeout_ms` which covers the full
    /// HTTP request including response read.
    pub fn with_connect_timeout_ms(mut self, ms: u64) -> Self {
        self.connect_timeout_ms = Some(ms);
        self
    }

    /// Enable or disable schema normalisation on registration (default: `false`).
    ///
    /// When `true`, appends `?normalize=true` to `POST /subjects/{subject}/versions`
    /// so the registry normalises field ordering before storing the schema.  This
    /// prevents schema ID churn when logically equivalent schemas are registered
    /// from multiple producers with different field ordering.
    pub fn with_normalize_schemas(mut self, normalize: bool) -> Self {
        self.normalize_schemas = normalize;
        self
    }

    /// Set the maximum number of idle keep-alive connections per host.
    ///
    /// Lowers connection pool pressure when all producers share a single Schema
    /// Registry host. Has no effect when `None` (the default).
    pub fn with_pool_max_idle_per_host(mut self, n: usize) -> Self {
        self.pool_max_idle_per_host = Some(n);
        self
    }

    /// Create a config by reading standard environment variables.
    ///
    /// | Variable | Required | Description |
    /// |---|---|---|
    /// | `SCHEMA_REGISTRY_URL` | ✓ | Base URL, e.g. `https://schema-registry.example.com` |
    /// | `SCHEMA_REGISTRY_BEARER_TOKEN` | ✗ | OAuth/IAM bearer token (takes precedence over basic auth) |
    /// | `SCHEMA_REGISTRY_USERNAME` | ✗ | HTTP Basic auth username |
    /// | `SCHEMA_REGISTRY_PASSWORD` | ✗ | HTTP Basic auth password (requires `SCHEMA_REGISTRY_USERNAME`) |
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] if `SCHEMA_REGISTRY_URL` is not set.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use rustcdc::codec::SchemaRegistryConfig;
    /// # fn main() -> rustcdc::core::Result<()> {
    /// // SCHEMA_REGISTRY_URL=https://sr.example.com cargo run
    /// let config = SchemaRegistryConfig::from_env("events-topic")?;
    /// # Ok(()) }
    /// ```
    pub fn from_env(topic: impl Into<String>) -> Result<Self> {
        let url = std::env::var("SCHEMA_REGISTRY_URL").map_err(|_| {
            Error::ConfigError("SCHEMA_REGISTRY_URL environment variable is not set".into())
        })?;

        let auth = if let Ok(token) = std::env::var("SCHEMA_REGISTRY_BEARER_TOKEN") {
            Some(SchemaRegistryAuth::BearerToken(SecretString::new(token)))
        } else if let (Ok(user), Ok(pass)) = (
            std::env::var("SCHEMA_REGISTRY_USERNAME"),
            std::env::var("SCHEMA_REGISTRY_PASSWORD"),
        ) {
            Some(SchemaRegistryAuth::Basic {
                username: user,
                password: pass,
            })
        } else {
            None
        };

        let mut cfg = Self::new(url, topic);
        if let Some(a) = auth {
            cfg = cfg.with_auth(a);
        }
        Ok(cfg)
    }

    /// Build a [`CachedSchemaRegistry<ConfluentSchemaRegistry>`] from this config.
    ///
    /// Constructs the underlying HTTP client and wraps it with an in-memory LRU
    /// cache. Does **not** make any network connections.
    pub fn build(&self) -> Result<CachedSchemaRegistry<ConfluentSchemaRegistry>> {
        let mut builder = ConfluentSchemaRegistry::builder().url(&self.url);

        if let Some(ref auth) = self.auth {
            builder = match auth {
                SchemaRegistryAuth::Basic { username, password } => {
                    builder.basic_auth(username, password)
                }
                SchemaRegistryAuth::BearerToken(token) => {
                    let tok = token
                        .expose_secret()
                        .map_err(|e| Error::ConfigError(format!("bearer token: {e}")))?;
                    builder.bearer_token(tok)
                }
            };
        }

        if let Some(ms) = self.request_timeout_ms {
            builder = builder.request_timeout(Duration::from_millis(ms));
        }

        if let Some(ms) = self.connect_timeout_ms {
            builder = builder.connect_timeout(Duration::from_millis(ms));
        }

        if let Some(n) = self.pool_max_idle_per_host {
            builder = builder.pool_max_idle_per_host(n);
        }

        builder = builder.normalize_schemas(self.normalize_schemas);

        let registry = builder
            .build()
            .map_err(|e| Error::ConfigError(format!("schema registry build: {e}")))?;

        let cached = match self.max_cache_entries {
            Some(n) => CachedSchemaRegistry::with_max_entries(registry, n),
            None => CachedSchemaRegistry::new(registry),
        };

        Ok(cached)
    }
}

// ─── ConfluentAvroEncoder ─────────────────────────────────────────────────────

/// CDC [`Event`] → Confluent Schema Registry-framed Avro encoder.
///
/// Encodes values and keys using the Confluent wire format:
///
/// ```text
/// [0x00 magic][4-byte BE schema_id][avro payload]
/// ```
///
/// | Channel | Schema | Subject |
/// |---|---|---|
/// | Value (`encode`) | `AVRO_SCHEMA` | `strategy.subject_name(topic, "io.rustcdc.Event", Value)` |
/// | Key (`encode_key`) | `KEY_AVRO_SCHEMA` | `strategy.subject_name(topic, "io.rustcdc.EventKey", Key)` |
#[derive(Debug, Clone)]
pub struct ConfluentAvroEncoder {
    inner: AvroEncoder,
    schema_id: SchemaId,
    key_schema_id: SchemaId,
    key_schema: Arc<Schema>,
}

impl ConfluentAvroEncoder {
    /// Construct an encoder, registering (or looking up) value and key schemas.
    ///
    /// If [`SchemaRegistryConfig::auto_register`] is `true`, both schemas are
    /// registered via the registry. If `false`, the latest version is fetched.
    pub async fn new(
        registry: &impl SchemaRegistryClient,
        config: &SchemaRegistryConfig,
    ) -> Result<Self> {
        let inner = AvroEncoder::new()?;

        let value_subject = config
            .strategy
            .subject_name(&config.topic, Some("io.rustcdc.Event"), EncodeTarget::Value)
            .map_err(|e| Error::ConfigError(format!("value subject name: {e}")))?;

        let key_subject = config
            .strategy
            .subject_name(
                &config.topic,
                Some("io.rustcdc.EventKey"),
                EncodeTarget::Key,
            )
            .map_err(|e| Error::ConfigError(format!("key subject name: {e}")))?;

        let (schema_id, key_schema_id) = if config.auto_register {
            let sid = registry
                .register_schema(
                    &value_subject,
                    crate::codec::avro::AVRO_SCHEMA,
                    SchemaType::Avro,
                    &[],
                )
                .await
                .map_err(|e| {
                    Error::ConfigError(format!("register value schema '{}': {e}", value_subject))
                })?;
            let kid = registry
                .register_schema(&key_subject, KEY_AVRO_SCHEMA, SchemaType::Avro, &[])
                .await
                .map_err(|e| {
                    Error::ConfigError(format!("register key schema '{}': {e}", key_subject))
                })?;
            (sid, kid)
        } else {
            let vs = registry
                .get_latest_schema(&value_subject)
                .await
                .map_err(|e| {
                    Error::ConfigError(format!("lookup value schema '{}': {e}", value_subject))
                })?;
            let ks = registry
                .get_latest_schema(&key_subject)
                .await
                .map_err(|e| {
                    Error::ConfigError(format!("lookup key schema '{}': {e}", key_subject))
                })?;
            (vs.id, ks.id)
        };

        let key_schema = Arc::new(
            Schema::parse_str(KEY_AVRO_SCHEMA)
                .map_err(|e| Error::ConfigError(format!("key schema parse: {e}")))?,
        );

        Ok(Self {
            inner,
            schema_id,
            key_schema_id,
            key_schema,
        })
    }

    /// The schema ID embedded in every encoded **value** message.
    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// The schema ID embedded in every encoded **key** message.
    pub fn key_schema_id(&self) -> SchemaId {
        self.key_schema_id
    }
}

impl EventEncoder for ConfluentAvroEncoder {
    fn encode(&self, event: &Event) -> Result<EncodedOutput> {
        let avro = self.inner.encode(event)?;
        let framed = encode_wire_format(self.schema_id, &avro.bytes).to_vec();
        Ok(EncodedOutput::new(framed, CONFLUENT_CONTENT_TYPE))
    }

    fn content_type(&self) -> &'static str {
        CONFLUENT_CONTENT_TYPE
    }

    /// Encode the primary key as Confluent-framed Avro bytes (key channel).
    ///
    /// Always returns `Some(bytes)` — a framed `EventKey` record.  Keyless
    /// events (TRUNCATE, SCHEMA_CHANGE) produce `EventKey { key: null }`,
    /// matching Debezium\'s behaviour for tables without a primary key.
    fn encode_key(&self, event: &Event) -> Option<Vec<u8>> {
        let key_json = event
            .primary_key_values()
            .and_then(|v| serde_json::to_string(&v).ok());

        let avro_value = apache_avro::types::Value::Record(vec![(
            "key".to_string(),
            match key_json {
                Some(s) => apache_avro::types::Value::Union(
                    1,
                    Box::new(apache_avro::types::Value::String(s)),
                ),
                None => {
                    apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null))
                }
            },
        )]);

        apache_avro::to_avro_datum(&self.key_schema, avro_value)
            .ok()
            .map(|avro_bytes| encode_wire_format(self.key_schema_id, &avro_bytes).to_vec())
    }
}

// ─── ConfluentAvroDecoder ─────────────────────────────────────────────────────

/// Decodes Confluent Schema Registry-framed Avro bytes back to a CDC [`Event`].
///
/// Works with output from [`ConfluentAvroEncoder`] or Debezium\'s Avro converter.
///
/// # Decoding steps
///
/// 1. Strip the 5-byte Confluent framing header via [`schemreg::decode_wire_format`].
/// 2. Fetch (and cache) the **writer schema** from the registry by schema ID.
/// 3. Decode Avro binary using `apache_avro::from_avro_datum` with schema resolution:
///    writer schema (from registry) is reconciled against the **reader schema**
///    (local [`AVRO_SCHEMA`](crate::codec::avro::AVRO_SCHEMA) or a custom schema
///    set via [`with_reader_schema`](Self::with_reader_schema)).
/// 4. Deserialise the resolved Avro value into an [`Event`] via `apache_avro::from_value`.
pub struct ConfluentAvroDecoder<R = CachedSchemaRegistry<ConfluentSchemaRegistry>> {
    registry: Arc<R>,
    reader_schema: Arc<Schema>,
}

impl<R> Clone for ConfluentAvroDecoder<R> {
    /// Clone the decoder. Both [`Arc`] fields are cloned by reference-count
    /// bump only — the underlying registry and schema are shared.
    fn clone(&self) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
            reader_schema: Arc::clone(&self.reader_schema),
        }
    }
}

impl<R> std::fmt::Debug for ConfluentAvroDecoder<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfluentAvroDecoder")
            .field("reader_schema", &"<avro schema>")
            .finish_non_exhaustive()
    }
}

impl<R: SchemaRegistryClient> ConfluentAvroDecoder<R> {
    /// Create a decoder backed by the given registry.
    ///
    /// Parses [`AVRO_SCHEMA`](crate::codec::avro::AVRO_SCHEMA) as the reader
    /// schema at construction time.
    pub fn new(registry: Arc<R>) -> Result<Self> {
        let reader_schema = Schema::parse_str(crate::codec::avro::AVRO_SCHEMA)
            .map_err(|e| Error::ConfigError(format!("reader schema parse: {e}")))?;
        Ok(Self {
            registry,
            reader_schema: Arc::new(reader_schema),
        })
    }

    /// Create a decoder with a custom reader schema for schema-evolution testing
    /// or cross-version compatibility scenarios.
    pub fn with_reader_schema(registry: Arc<R>, reader_schema: Schema) -> Self {
        Self {
            registry,
            reader_schema: Arc::new(reader_schema),
        }
    }

    /// Decode a Confluent-framed Avro **value** message to a CDC [`Event`].
    ///
    /// `async` because schema fetching from the registry is required for schema
    /// IDs not yet in the local cache.
    pub async fn decode(&self, bytes: &[u8]) -> Result<Event> {
        let (schema_id, avro_bytes) = decode_wire_format(bytes)
            .map_err(|e| Error::SourceError(format!("confluent wire format decode: {e}")))?;

        let schemreg_schema = self
            .registry
            .get_schema_by_id(schema_id)
            .await
            .map_err(|e| {
                Error::SourceError(format!(
                    "schema registry get_schema_by_id({schema_id}): {e}"
                ))
            })?;

        let writer_schema = Schema::parse_str(&schemreg_schema.schema).map_err(|e| {
            Error::SourceError(format!("avro schema parse (schema_id={schema_id}): {e}"))
        })?;

        let value = apache_avro::from_avro_datum(
            &writer_schema,
            &mut std::io::Cursor::new(avro_bytes),
            Some(&self.reader_schema),
        )
        .map_err(|e| Error::SourceError(format!("avro decode (schema_id={schema_id}): {e}")))?;

        apache_avro::from_value::<Event>(&value).map_err(|e| {
            Error::SourceError(format!(
                "avro → Event deserialize (schema_id={schema_id}): {e}"
            ))
        })
    }
}

// ─── ConfluentAvroCodec ───────────────────────────────────────────────────────

/// A [`Codec`](crate::codec::Codec) that produces Confluent Schema Registry-framed
/// Avro for both Kafka message keys and values.
///
/// Named alias for `EncoderCodec<ConfluentAvroEncoder>`.
pub type ConfluentAvroCodec = crate::codec::EncoderCodec<ConfluentAvroEncoder>;

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ─── wire format ─────────────────────────────────────────────────────────

    #[test]
    fn encode_decode_wire_format_round_trip() {
        let payload = b"\x04\x08hello";
        let framed = encode_wire_format(42u32, payload);
        assert_eq!(framed[0], 0x00, "magic byte must be 0x00");
        let id_bytes: [u8; 4] = framed[1..5].try_into().unwrap();
        assert_eq!(u32::from_be_bytes(id_bytes), 42);
        assert_eq!(&framed[5..], payload);

        let (id, rest) = decode_wire_format(&framed).unwrap();
        assert_eq!(id.as_u32(), 42);
        assert_eq!(rest, payload);
    }

    #[test]
    fn decode_wire_format_too_short_errors() {
        assert!(decode_wire_format(&[0x00, 0x00]).is_err());
    }

    #[test]
    fn decode_wire_format_wrong_magic_errors() {
        let framed = encode_wire_format(1u32, b"data");
        let mut bad = framed.to_vec();
        bad[0] = 0xFF;
        assert!(decode_wire_format(&bad).is_err());
    }

    #[test]
    fn encode_with_zero_schema_id() {
        let framed = encode_wire_format(0u32, b"");
        let (id, rest) = decode_wire_format(&framed).unwrap();
        assert_eq!(id.as_u32(), 0);
        assert!(rest.is_empty());
    }

    #[test]
    fn encode_with_max_schema_id() {
        let framed = encode_wire_format(u32::MAX, b"payload");
        let (id, rest) = decode_wire_format(&framed).unwrap();
        assert_eq!(id.as_u32(), u32::MAX);
        assert_eq!(rest, b"payload");
    }

    // ─── SubjectNameStrategy ─────────────────────────────────────────────────

    #[test]
    fn topic_name_strategy_value_subject() {
        let s = SubjectNameStrategy::TopicName;
        let subj = s
            .subject_name(
                "pg.public.orders",
                Some("io.rustcdc.Event"),
                EncodeTarget::Value,
            )
            .unwrap();
        assert_eq!(subj, "pg.public.orders-value");
    }

    #[test]
    fn topic_name_strategy_key_subject() {
        let s = SubjectNameStrategy::TopicName;
        let subj = s
            .subject_name(
                "pg.public.orders",
                Some("io.rustcdc.EventKey"),
                EncodeTarget::Key,
            )
            .unwrap();
        assert_eq!(subj, "pg.public.orders-key");
    }

    #[test]
    fn record_name_strategy_subjects() {
        let s = SubjectNameStrategy::RecordName;
        let vs = s
            .subject_name("any", Some("io.rustcdc.Event"), EncodeTarget::Value)
            .unwrap();
        let ks = s
            .subject_name("any", Some("io.rustcdc.EventKey"), EncodeTarget::Key)
            .unwrap();
        assert_eq!(vs, "io.rustcdc.Event");
        assert_eq!(ks, "io.rustcdc.EventKey");
    }

    #[test]
    fn topic_record_name_strategy_subjects() {
        let s = SubjectNameStrategy::TopicRecordName;
        let vs = s
            .subject_name("cdc.orders", Some("io.rustcdc.Event"), EncodeTarget::Value)
            .unwrap();
        let ks = s
            .subject_name("cdc.orders", Some("io.rustcdc.EventKey"), EncodeTarget::Key)
            .unwrap();
        assert_eq!(vs, "cdc.orders-io.rustcdc.Event");
        assert_eq!(ks, "cdc.orders-io.rustcdc.EventKey");
    }

    // ─── SchemaRegistryConfig ────────────────────────────────────────────────

    #[test]
    fn config_defaults() {
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "my-topic");
        assert!(cfg.auto_register);
        assert!(cfg.auth.is_none());
        assert!(cfg.request_timeout_ms.is_none());
        assert!(cfg.max_cache_entries.is_none());
        assert_eq!(cfg.strategy, SubjectNameStrategy::TopicName);
        assert_eq!(cfg.topic, "my-topic");
        assert_eq!(cfg.url, "http://localhost:8081");
    }

    #[test]
    fn config_trailing_slash_trimmed() {
        let cfg = SchemaRegistryConfig::new("http://localhost:8081/", "t");
        assert!(!cfg.url.ends_with('/'));
    }

    #[test]
    fn config_builder_chain() {
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "topic")
            .with_auto_register(false)
            .with_strategy(SubjectNameStrategy::RecordName)
            .with_request_timeout_ms(10_000)
            .with_max_cache_entries(512)
            .with_connect_timeout_ms(3_000)
            .with_normalize_schemas(true);
        assert!(!cfg.auto_register);
        assert_eq!(cfg.strategy, SubjectNameStrategy::RecordName);
        assert_eq!(cfg.request_timeout_ms, Some(10_000));
        assert_eq!(cfg.max_cache_entries, Some(512));
        assert_eq!(cfg.connect_timeout_ms, Some(3_000));
        assert!(cfg.normalize_schemas);
    }

    #[test]
    fn config_defaults_new_fields() {
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "t");
        assert!(cfg.connect_timeout_ms.is_none());
        assert!(!cfg.normalize_schemas);
    }

    #[test]
    fn config_build_with_connect_timeout_succeeds() {
        // connect_timeout is forwarded to the reqwest builder — no network connection.
        let cfg =
            SchemaRegistryConfig::new("http://localhost:8081", "t").with_connect_timeout_ms(5_000);
        assert!(cfg.build().is_ok());
    }

    #[test]
    fn config_build_with_normalize_schemas_succeeds() {
        let cfg =
            SchemaRegistryConfig::new("http://localhost:8081", "t").with_normalize_schemas(true);
        assert!(cfg.build().is_ok());
    }

    #[test]
    fn config_build_succeeds() {
        // build() only constructs the reqwest client — no network connection.
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "t");
        assert!(cfg.build().is_ok());
    }

    // ─── Key schema ──────────────────────────────────────────────────────────

    #[test]
    fn key_avro_schema_is_valid_avro() {
        Schema::parse_str(KEY_AVRO_SCHEMA).expect("KEY_AVRO_SCHEMA must be valid Avro");
    }

    #[test]
    fn key_avro_schema_round_trips_non_null_key() {
        let schema = Schema::parse_str(KEY_AVRO_SCHEMA).unwrap();
        let key_json = r#"{"id":42}"#;
        let value = apache_avro::types::Value::Record(vec![(
            "key".to_string(),
            apache_avro::types::Value::Union(
                1,
                Box::new(apache_avro::types::Value::String(key_json.to_string())),
            ),
        )]);
        let bytes = apache_avro::to_avro_datum(&schema, value).expect("avro encode");
        let decoded =
            apache_avro::from_avro_datum(&schema, &mut std::io::Cursor::new(&bytes), None)
                .expect("avro decode");
        if let apache_avro::types::Value::Record(fields) = decoded {
            assert!(matches!(
                &fields[0].1,
                apache_avro::types::Value::Union(1, _)
            ));
        } else {
            panic!("expected Record");
        }
    }

    #[test]
    fn key_avro_schema_round_trips_null_key() {
        let schema = Schema::parse_str(KEY_AVRO_SCHEMA).unwrap();
        let value = apache_avro::types::Value::Record(vec![(
            "key".to_string(),
            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
        )]);
        let bytes = apache_avro::to_avro_datum(&schema, value).expect("avro encode");
        let decoded =
            apache_avro::from_avro_datum(&schema, &mut std::io::Cursor::new(&bytes), None)
                .expect("avro decode");
        assert!(matches!(decoded, apache_avro::types::Value::Record(_)));
    }

    // ─── Decoder ─────────────────────────────────────────────────────────────

    #[test]
    fn decoder_new_parses_reader_schema_successfully() {
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "t");
        let registry = Arc::new(cfg.build().unwrap());
        assert!(ConfluentAvroDecoder::new(Arc::clone(&registry)).is_ok());
    }

    #[test]
    fn decoder_with_reader_schema_accepts_custom_schema() {
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "t");
        let registry = Arc::new(cfg.build().unwrap());
        let reader = Schema::parse_str(KEY_AVRO_SCHEMA).unwrap();
        let _decoder = ConfluentAvroDecoder::with_reader_schema(registry, reader);
    }

    // ─── SchemaRegistryAuth Debug ─────────────────────────────────────────────

    #[test]
    fn bearer_token_debug_redacts_secret() {
        let auth =
            SchemaRegistryAuth::BearerToken(crate::core::SecretString::new("my-secret-token"));
        let dbg = format!("{auth:?}");
        assert!(
            !dbg.contains("my-secret-token"),
            "token must be redacted in Debug output"
        );
    }

    #[test]
    fn basic_auth_debug_shows_username() {
        let auth = SchemaRegistryAuth::Basic {
            username: "alice".into(),
            password: "hunter2".into(),
        };
        let dbg = format!("{auth:?}");
        assert!(dbg.contains("alice"));
    }

    // ─── pool_max_idle_per_host ───────────────────────────────────────────────

    #[test]
    fn config_pool_max_idle_per_host_defaults_to_none() {
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "t");
        assert!(cfg.pool_max_idle_per_host.is_none());
    }

    #[test]
    fn config_pool_max_idle_per_host_builder() {
        let cfg =
            SchemaRegistryConfig::new("http://localhost:8081", "t").with_pool_max_idle_per_host(4);
        assert_eq!(cfg.pool_max_idle_per_host, Some(4));
    }

    #[test]
    fn config_build_with_pool_max_idle_per_host_succeeds() {
        let cfg =
            SchemaRegistryConfig::new("http://localhost:8081", "t").with_pool_max_idle_per_host(8);
        assert!(cfg.build().is_ok());
    }

    // ─── from_env ─────────────────────────────────────────────────────────────

    #[test]
    fn from_env_fails_when_url_not_set() {
        if std::env::var("SCHEMA_REGISTRY_URL").is_ok() {
            // Skip: env var is set in this environment; error path not testable.
            return;
        }
        assert!(SchemaRegistryConfig::from_env("t").is_err());
    }

    #[test]
    fn from_env_parses_url_when_set() {
        // Only runs when SCHEMA_REGISTRY_URL is present in the environment.
        if let Ok(url) = std::env::var("SCHEMA_REGISTRY_URL") {
            let cfg = SchemaRegistryConfig::from_env("test-topic").expect("from_env");
            assert_eq!(cfg.url, url.trim_end_matches('/'));
            assert_eq!(cfg.topic, "test-topic");
        }
    }

    // ─── ConfluentAvroDecoder generics ────────────────────────────────────────

    #[test]
    fn decoder_is_generic_over_cached_registry() {
        // Verifies the default type parameter works and that Clone/Debug impls
        // do not impose R: Clone + Debug bounds.
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "t");
        let registry: Arc<CachedSchemaRegistry<ConfluentSchemaRegistry>> =
            Arc::new(cfg.build().unwrap());
        let decoder: ConfluentAvroDecoder<CachedSchemaRegistry<ConfluentSchemaRegistry>> =
            ConfluentAvroDecoder::new(registry).unwrap();
        let _cloned = decoder.clone();
        let dbg = format!("{decoder:?}");
        assert!(dbg.contains("ConfluentAvroDecoder"));
    }
}
