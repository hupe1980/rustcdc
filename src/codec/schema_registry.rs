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
pub use ::schemreg::{detect_wire_format, SchemaRegError};
pub use ::schemreg::{
    AnySchemaCache, DynSchemaRegistryClient, SchemaDecoder, SchemaEncoder, SchemaReference,
    SchemaVersion,
};
pub use ::schemreg::{
    CachedSchemaRegistry, CompatibilityLevel, EncodeTarget, RetryPolicy, SchemaId,
    SchemaRegistryClient, SchemaType, SubjectNameStrategy, DEFAULT_BASE_BACKOFF,
    DEFAULT_MAX_BACKOFF, DEFAULT_MAX_RETRIES,
};
pub use ::schemreg::{DecodedMessage, DetectedWireFormat, SchemaFormat, WireFormatDecoder};

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
    /// HTTP Basic authentication.
    Basic {
        /// Registry username.
        username: String,
        /// Registry password.
        password: String,
    },
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
    /// Schema Registry API root — the URL that serves `/subjects`, with any trailing
    /// slash trimmed.
    ///
    /// For Confluent Schema Registry this is the server root
    /// (`http://schema-registry:8081`). For a Confluent-compatible endpoint on another
    /// product it is the compatibility path itself — Apicurio, for example, serves it at
    /// `http://apicurio:8080/apis/ccompat/v7`. Paths are used as given; nothing is
    /// appended.
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
    /// Schemas this one depends on, registered as Confluent **schema references**.
    ///
    /// A reference lets a schema `import` a type that lives under a different subject,
    /// rather than inlining it. The registry then resolves the dependency at read time and
    /// enforces compatibility on it independently.
    ///
    /// rustcdc's own envelope has no dependencies, so this is empty by default. It exists
    /// for a deployment that has extended the envelope, or that registers rustcdc's schema
    /// alongside its own types in a shared subject namespace — without it, registration
    /// against such a subject fails because the referenced types cannot be resolved.
    pub references: Vec<SchemaReference>,
    /// Retry policy for transient registry failures.
    ///
    /// Without one, a single HTTP 503 or a dropped connection while resolving a schema
    /// fails the event — and on the encode path that takes the pipeline down for a
    /// condition that resolves on its own in seconds. The default retries with jittered
    /// exponential back-off and honours `Retry-After`.
    ///
    /// Only *transient* conditions are retried (transport failures, HTTP 429, HTTP 5xx).
    /// Not-found, auth, and invalid-schema are permanent and fail immediately, so an
    /// outer retry loop cannot spin on them.
    ///
    /// Set [`RetryPolicy::none`] if you retry at a higher layer and do not want the two
    /// to multiply.
    pub retry_policy: RetryPolicy,
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
            references: Vec::new(),
            retry_policy: RetryPolicy::default(),
        }
    }

    /// Declare the schema references this schema depends on.
    ///
    /// ```
    /// use rustcdc::codec::{SchemaReference, SchemaRegistryConfig};
    ///
    /// let config = SchemaRegistryConfig::new("http://localhost:8081", "cdc-events")
    ///     .with_references(vec![SchemaReference::new(
    ///         "com.example.Address",
    ///         "com.example.Address",
    ///         1,
    ///     )]);
    /// # let _ = config;
    /// ```
    #[must_use]
    pub fn with_references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = references;
        self
    }

    /// Replace the retry policy for transient registry failures.
    ///
    /// ```
    /// use rustcdc::codec::{RetryPolicy, SchemaRegistryConfig};
    /// use std::time::Duration;
    ///
    /// let config = SchemaRegistryConfig::new("http://localhost:8081", "cdc-events")
    ///     .with_retry_policy(
    ///         RetryPolicy::new()
    ///             .max_retries(5)
    ///             .base_backoff(Duration::from_millis(100))
    ///             .max_backoff(Duration::from_secs(5)),
    ///     );
    /// # let _ = config;
    /// ```
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
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
        builder = builder.retry_policy(self.retry_policy.clone());

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

// ─── Registry error classification ────────────────────────────────────────────

/// Translate a registry error into a rustcdc error with the **right retryability**.
///
/// This matters because the schema registry sits on the encode and decode paths, and
/// rustcdc's `ErrorKind` drives the embedder's retry loop. Mapping everything to
/// [`Error::SourceError`] — which classifies as `Transient`, "safe to retry with backoff"
/// — means an embedder retries a 404 forever, and never retries the 503 any differently.
///
/// `schemreg` already classifies its own errors, so this defers to it rather than
/// re-deriving the rules from status codes:
///
/// | Registry condition | rustcdc kind | Why |
/// |---|---|---|
/// | transport failure, HTTP 429, HTTP 5xx | `Transient` | resolves on its own |
/// | subject / version / schema not found | `Terminal` | needs the schema registered |
/// | auth failure | `Terminal` | needs a credential change |
/// | everything else | `Terminal` | retrying reproduces it |
fn map_registry_error(context: &str, error: ::schemreg::SchemaRegError) -> Error {
    use crate::core::SourceErrorKind;

    if error.is_retryable() {
        return Error::source_error(
            SourceErrorKind::NetworkTransient,
            format!("{context}: {error}"),
        );
    }
    if error.is_not_found() {
        return Error::source_error(
            SourceErrorKind::SchemaMismatch,
            format!(
                "{context}: {error}. The schema this message was written with is not in the \
                 registry, so the payload cannot be interpreted. Retrying will not help."
            ),
        );
    }
    if matches!(error, ::schemreg::SchemaRegError::Auth { .. }) {
        return Error::source_error(SourceErrorKind::AuthFailed, format!("{context}: {error}"));
    }

    Error::SchemaError(format!("{context}: {error}"))
}

// ─── Preflight ────────────────────────────────────────────────────────────────

/// Verify the registry is reachable and its schemas are usable, before capture starts.
///
/// Schema resolution sits on the **encode path**, so a registry problem does not surface
/// as a startup failure — it surfaces as a failed event, mid-pipeline, once traffic is
/// already flowing. This turns that into a startup check, which is where an operator can
/// still act on it.
///
/// Checks, in order of how early they fail:
///
/// 1. **Reachability** — a `health_check` round-trip.
/// 2. **Subject readiness** — with `auto_register = false`, that both the value and key
///    subjects exist *and* carry the schema rustcdc encodes with. That second half is the
///    important one: an id that resolves to a different schema produces silently wrong
///    field values downstream, not an error.
/// 3. **Compatibility** — with `auto_register = true`, that rustcdc's schema is compatible
///    with what is already registered, so the failure arrives here rather than as an
///    opaque HTTP 409 on the first event.
///
/// A registry that does not implement an optional endpoint (`health_check`,
/// `check_compatibility`) reports `NotSupported`; that is skipped rather than treated as a
/// failure, because a registry legitimately need not offer them.
///
/// Wire this into a readiness probe alongside
/// [`CdcRuntime::admin_snapshot`](crate::CdcRuntime::admin_snapshot).
///
/// # Errors
///
/// Returns [`Error::ConfigError`] naming the subject and the remedy.
pub async fn preflight_schema_registry(
    registry: &impl SchemaRegistryClient,
    config: &SchemaRegistryConfig,
) -> Result<()> {
    match registry.health_check().await {
        Ok(()) => {}
        Err(error) if is_not_supported(&error) => {
            tracing::debug!(
                target: "rustcdc::codec::schema_registry",
                "registry does not implement a health endpoint; skipping reachability check",
            );
        }
        Err(error) => {
            return Err(Error::ConfigError(format!(
                "schema registry at '{}' is not reachable: {error}. Schema resolution is on \
                 the encode path, so starting anyway would fail the first event instead of \
                 failing here.",
                config.url
            )));
        }
    }

    let value_subject = config
        .strategy
        .subject_name(&config.topic, Some("io.rustcdc.Event"), EncodeTarget::Value)
        .map_err(|error| Error::ConfigError(format!("value subject name: {error}")))?;
    let key_subject = config
        .strategy
        .subject_name(
            &config.topic,
            Some("io.rustcdc.EventKey"),
            EncodeTarget::Key,
        )
        .map_err(|error| Error::ConfigError(format!("key subject name: {error}")))?;

    for (subject, expected) in [
        (&value_subject, crate::codec::avro::AVRO_SCHEMA),
        (&key_subject, KEY_AVRO_SCHEMA),
    ] {
        if config.auto_register {
            match registry
                .check_compatible(subject, expected, SchemaType::Avro)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    return Err(Error::ConfigError(format!(
                        "rustcdc's Avro schema is INCOMPATIBLE with the schema already \
                         registered under subject '{subject}', per that subject's \
                         compatibility level. Registering it would be rejected by the \
                         registry, and forcing it past the check would break every existing \
                         consumer. Resolve the schema conflict, or use a different subject."
                    )));
                }
                // A subject that does not exist yet is the normal first-run case.
                Err(error) if is_not_supported(&error) || is_not_found(&error) => {}
                Err(error) => {
                    return Err(Error::ConfigError(format!(
                        "compatibility check failed for subject '{subject}': {error}"
                    )));
                }
            }
        } else {
            let registered = registry.get_latest_schema(subject).await.map_err(|error| {
                Error::ConfigError(format!(
                    "subject '{subject}' is not registered and `auto_register` is off: \
                     {error}. Register rustcdc's schema out of band, or enable \
                     `auto_register` for first-time setup."
                ))
            })?;
            assert_registry_schema_matches(subject, &registered.schema, expected)?;
        }
    }

    Ok(())
}

/// Whether an error means the registry does not implement an optional endpoint.
///
/// A registry legitimately need not offer `health_check` or `check_compatibility`, so
/// this is skipped rather than treated as a failure.
fn is_not_supported(error: &::schemreg::SchemaRegError) -> bool {
    matches!(error, ::schemreg::SchemaRegError::NotSupported(_))
}

/// Whether an error means the subject or schema does not exist yet.
///
/// Delegates to `schemreg`, which maps the Confluent error codes 40401–40403
/// (subject / version / schema not found). Not-found on a compatibility check is the
/// ordinary first-run case, not a problem.
fn is_not_found(error: &::schemreg::SchemaRegError) -> bool {
    error.is_not_found()
}

// ─── Schema identity ──────────────────────────────────────────────────────────

/// Reject a registry schema that is not the one the encoder will write with.
///
/// # Why this is not optional
///
/// The Confluent wire format stamps a *schema id*; the payload bytes are whatever the
/// producer actually encoded. If the two disagree, a consumer resolves the id to the
/// registry's schema and decodes the producer's bytes with it.
///
/// **Avro binary carries no field names or types** — it is positional and untagged. A
/// mismatch therefore does not fail to decode. It silently yields shifted fields and
/// values that look entirely plausible, which is the worst possible outcome and one that
/// surfaces arbitrarily far downstream.
///
/// Comparison is on Avro's **parsing canonical form** (RFC-style: strips docs, aliases
/// and default values, normalises ordering), so a registry copy that differs only in
/// formatting or in field ordering *within the JSON* is accepted, while a genuine
/// structural difference is rejected.
fn assert_registry_schema_matches(
    subject: &str,
    registry_schema: &str,
    expected_schema: &str,
) -> Result<()> {
    let registry_parsed = Schema::parse_str(registry_schema).map_err(|error| {
        Error::ConfigError(format!(
            "schema registered under subject '{subject}' is not valid Avro: {error}"
        ))
    })?;
    let expected_parsed = Schema::parse_str(expected_schema).map_err(|error| {
        Error::ConfigError(format!(
            "rustcdc's own Avro schema failed to parse: {error}"
        ))
    })?;

    if registry_parsed.canonical_form() == expected_parsed.canonical_form() {
        return Ok(());
    }

    Err(Error::ConfigError(format!(
        "the schema registered under subject '{subject}' is not the schema rustcdc \
         encodes with, so every message would be stamped with an id that resolves to a \
         different schema. Avro binary is positional and untagged, so consumers would not \
         see an error — they would silently decode shifted fields and plausible-looking \
         wrong values.\n\
         \n\
         Registry canonical form: {}\n\
         Expected canonical form: {}\n\
         \n\
         Remedy: register rustcdc's schema under this subject (set `auto_register = true` \
         for first-time setup, or register it out of band), or point `topic`/`strategy` at \
         a subject that carries it.",
        registry_parsed.canonical_form(),
        expected_parsed.canonical_form(),
    )))
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
                    &config.references,
                )
                .await
                .map_err(|e| {
                    Error::ConfigError(format!("register value schema '{}': {e}", value_subject))
                })?;
            let kid = registry
                .register_schema(
                    &key_subject,
                    KEY_AVRO_SCHEMA,
                    SchemaType::Avro,
                    &config.references,
                )
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

            // Verify the registry's schema is the one this encoder will actually write
            // with. Taking the id without checking is a silent-corruption path, and it is
            // the *safer-looking* configuration that triggers it: `auto_register = false`
            // is what a careful operator sets in a managed Kafka environment.
            //
            // Avro binary is positional and untagged, so a consumer resolving the stamped
            // id to a different schema does not get an error — it gets shifted fields and
            // plausible-looking wrong values.
            assert_registry_schema_matches(
                &value_subject,
                &vs.schema,
                crate::codec::avro::AVRO_SCHEMA,
            )?;
            assert_registry_schema_matches(&key_subject, &ks.schema, KEY_AVRO_SCHEMA)?;

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
        // Malformed framing is permanent — these exact bytes will never decode. It was
        // previously a `SourceError`, which classifies as `Transient` ("safe to retry with
        // backoff"), so an embedder following the crate's own guidance retried a message
        // that cannot succeed, forever.
        let (schema_id, avro_bytes) = decode_wire_format(bytes).map_err(|e| {
            Error::SerializationError(format!(
                "confluent wire format decode: {e}. The payload does not carry a valid \
                 5-byte Confluent header, so it was not produced by a Confluent-framed \
                 serialiser. Retrying will not change the bytes."
            ))
        })?;

        let schemreg_schema = SchemaRegistryClient::get_schema_by_id(&*self.registry, schema_id)
            .await
            .map_err(|e| map_registry_error(&format!("get_schema_by_id({schema_id})"), e))?;

        let writer_schema = Schema::parse_str(&schemreg_schema.schema).map_err(|e| {
            Error::SchemaError(format!("avro schema parse (schema_id={schema_id}): {e}"))
        })?;

        let value = apache_avro::from_avro_datum(
            &writer_schema,
            &mut std::io::Cursor::new(avro_bytes),
            Some(&self.reader_schema),
        )
        .map_err(|e| {
            Error::SerializationError(format!("avro decode (schema_id={schema_id}): {e}"))
        })?;

        // Not `apache_avro::from_value::<Event>`: `before`/`after` are Avro `bytes`
        // holding UTF-8 JSON, which a blanket serde mapping cannot reverse. That mismatch
        // meant this decoder had never successfully decoded an event — a live round trip
        // against a real registry is what exposed it.
        crate::codec::avro::avro_value_to_event(&value).map_err(|e| {
            Error::SerializationError(format!(
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

// ─── JSON Schema content type ─────────────────────────────────────────────────

const CONFLUENT_JSON_CONTENT_TYPE: &str = "application/vnd.kafka+json";

// ─── Event JSON Schema ────────────────────────────────────────────────────────

/// JSON Schema (draft 2020-12) for the canonical CDC event envelope.
///
/// Matches the serde serialization of [`crate::core::Event`]. Register this schema
/// with your Confluent-compatible registry to enable [`ConfluentJsonSchemaEncoder`]
/// framing.
///
/// The `before` and `after` fields accept any JSON value (reflecting `Option<serde_json::Value>`).
pub const EVENT_JSON_SCHEMA: &str = r#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "io.rustcdc.Event",
  "title": "Event",
  "description": "Canonical CDC event envelope — rustcdc envelope_version=1",
  "type": "object",
  "properties": {
    "before": {
      "description": "Row state before the operation, when available. null for INSERT events.",
      "type": ["null", "object", "array", "string", "number", "boolean"]
    },
    "after": {
      "description": "Row state after the operation, when available. null for DELETE events.",
      "type": ["null", "object", "array", "string", "number", "boolean"]
    },
    "op": {
      "description": "CRUD operation that produced this event.",
      "type": "string",
      "enum": ["insert", "update", "delete", "read", "schema_change", "truncate"]
    },
    "source": {
      "description": "Source identity and durable position metadata.",
      "type": "object",
      "properties": {
        "source_name": {"type": "string", "description": "Logical name of the source connector."},
        "offset":      {"type": "string", "description": "Source-specific durable position encoded as a string."},
        "timestamp":   {"type": "integer", "minimum": 0, "description": "Source timestamp associated with the position."}
      },
      "required": ["source_name", "offset", "timestamp"],
      "additionalProperties": false
    },
    "ts": {
      "description": "Event timestamp in milliseconds since epoch.",
      "type": "integer",
      "minimum": 0
    },
    "schema": {
      "description": "Schema name when the source provides one.",
      "oneOf": [{"type": "null"}, {"type": "string"}]
    },
    "table": {
      "description": "Table name.",
      "type": "string"
    },
    "primary_key": {
      "description": "Primary key column names, when known.",
      "oneOf": [
        {"type": "null"},
        {"type": "array", "items": {"type": "string"}}
      ]
    },
    "snapshot": {
      "description": "Snapshot progress information when emitted during snapshotting.",
      "oneOf": [
        {"type": "null"},
        {
          "type": "object",
          "properties": {
            "snapshot_id":  {"type": "string"},
            "chunk_index":  {"type": "integer", "minimum": 0},
            "is_last_chunk": {"type": "boolean"}
          },
          "required": ["snapshot_id", "chunk_index", "is_last_chunk"],
          "additionalProperties": false
        }
      ]
    },
    "transaction": {
      "description": "Transaction metadata when the event belongs to a multi-event transaction.",
      "oneOf": [
        {"type": "null"},
        {
          "type": "object",
          "properties": {
            "tx_id":       {"type": "integer", "minimum": 0},
            "total_events": {"oneOf": [{"type": "null"}, {"type": "integer", "minimum": 0}]},
            "event_index": {"type": "integer", "minimum": 0}
          },
          "required": ["tx_id", "event_index"],
          "additionalProperties": false
        }
      ]
    },
    "envelope_version": {
      "description": "Event envelope schema version.",
      "type": "integer",
      "minimum": 0
    },
    "before_is_key_only": {
      "description": "True when `before` contains only primary-key columns (REPLICA IDENTITY DEFAULT).",
      "type": "boolean"
    },
    "unavailable_columns": {
      "description": "Columns absent from `after` because the source could not supply them. Omitted when empty.",
      "type": "array",
      "items": {"type": "string"}
    },
    "before_unavailable_columns": {
      "description": "Columns absent from `before`. Tracked separately from `unavailable_columns` — the two sets are not the same.",
      "type": "array",
      "items": {"type": "string"}
    }
  },
  "required": ["before", "after", "op", "source", "ts", "table", "envelope_version", "before_is_key_only"],
  "additionalProperties": false
}"#;

// ─── Key JSON Schema ──────────────────────────────────────────────────────────

/// JSON Schema (draft 2020-12) for the primary-key envelope produced by
/// [`ConfluentJsonSchemaEncoder::encode_event_key`].
///
/// Mirrors [`KEY_AVRO_SCHEMA`]: a single nullable `key` field carrying the
/// JSON-encoded primary key map, or `null` for keyless events.
pub const KEY_JSON_SCHEMA: &str = r#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "io.rustcdc.EventKey",
  "title": "EventKey",
  "description": "CDC event primary-key envelope — rustcdc",
  "type": "object",
  "properties": {
    "key": {
      "description": "JSON-encoded primary key map, or null for keyless events (TRUNCATE, SCHEMA_CHANGE).",
      "oneOf": [{"type": "null"}, {"type": "string"}]
    }
  },
  "required": ["key"],
  "additionalProperties": false
}"#;

// ─── ConfluentJsonSchemaEncoder ───────────────────────────────────────────────

/// CDC [`Event`] → Confluent Schema Registry-framed JSON encoder.
///
/// Encodes events using the Confluent wire format with JSON Schema validation:
///
/// ```text
/// [0x00 magic][4-byte BE schema_id][json payload]
/// ```
///
/// Unlike [`ConfluentAvroEncoder`], encoding is inherently **async** because
/// subject/schema resolution may require a registry round-trip on the first call
/// per subject. Subsequent calls hit the in-memory cache inside the wrapped
/// [`schemreg::json::JsonSchemaEncoder`].
///
/// # Construction
///
/// ```rust,no_run
/// # use rustcdc::codec::SchemaRegistryConfig;
/// # use rustcdc::codec::ConfluentJsonSchemaEncoder;
/// # async fn example() -> rustcdc::core::Result<()> {
/// let config = SchemaRegistryConfig::new("http://localhost:8081", "cdc-events");
/// let registry = std::sync::Arc::new(config.build()?);
/// let encoder = ConfluentJsonSchemaEncoder::new(registry, &config)?;
/// # Ok(()) }
/// ```
///
/// # Validation
///
/// By default, every event is validated against [`EVENT_JSON_SCHEMA`] before
/// serialisation. Disable with [`ConfluentJsonSchemaEncoder::without_validation`]
/// for maximum throughput when producers are trusted.
#[derive(Debug, Clone)]
pub struct ConfluentJsonSchemaEncoder<C = Arc<CachedSchemaRegistry<ConfluentSchemaRegistry>>> {
    value_encoder: Arc<::schemreg::json::JsonSchemaEncoder<C>>,
    key_encoder: Arc<::schemreg::json::JsonSchemaEncoder<C>>,
    topic: String,
}

impl<C> ConfluentJsonSchemaEncoder<C> {
    /// The Kafka topic name this encoder is configured for.
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl<C> ConfluentJsonSchemaEncoder<C>
where
    C: SchemaRegistryClient + Clone,
{
    /// Construct a JSON Schema encoder that validates events on encode (default).
    ///
    /// Both value and key schemas are registered (or looked up) lazily on the
    /// first encode call for each subject. The schemas used are
    /// [`EVENT_JSON_SCHEMA`] and [`KEY_JSON_SCHEMA`].
    pub fn new(registry: C, config: &SchemaRegistryConfig) -> Result<Self> {
        Self::new_inner(registry, config, true)
    }

    /// Construct a JSON Schema encoder that skips JSON Schema validation on encode.
    ///
    /// Use this only when producers are trusted and throughput is the priority.
    /// Invalid events will be accepted by the encoder but may be rejected by
    /// consumers that validate on decode.
    pub fn without_validation(registry: C, config: &SchemaRegistryConfig) -> Result<Self> {
        Self::new_inner(registry, config, false)
    }

    fn new_inner(registry: C, config: &SchemaRegistryConfig, validate: bool) -> Result<Self> {
        let value_encoder = ::schemreg::json::JsonSchemaEncoder::builder()
            .registry(registry.clone())
            .schema(EVENT_JSON_SCHEMA)
            .strategy(config.strategy.clone())
            .validate_on_encode(validate)
            .build()
            .map_err(|e| Error::ConfigError(format!("json schema value encoder build: {e}")))?;

        let key_encoder = ::schemreg::json::JsonSchemaEncoder::builder()
            .registry(registry)
            .schema(KEY_JSON_SCHEMA)
            .strategy(config.strategy.clone())
            .validate_on_encode(validate)
            .build()
            .map_err(|e| Error::ConfigError(format!("json schema key encoder build: {e}")))?;

        Ok(Self {
            value_encoder: Arc::new(value_encoder),
            key_encoder: Arc::new(key_encoder),
            topic: config.topic.clone(),
        })
    }

    /// Encode a CDC event to Confluent-framed JSON bytes (value channel).
    ///
    /// The event is serialised to `serde_json::Value` via [`serde_json::to_value`]
    /// and then validated against [`EVENT_JSON_SCHEMA`] (unless disabled) before
    /// being wrapped with the Confluent 5-byte wire-format header.
    ///
    /// # Errors
    ///
    /// - `Error::ConfigError` on the first call for a subject if the registry
    ///   is unreachable or schema registration fails.
    /// - A classified source error when the registry is unreachable (`Transient`) or the
    ///   schema is missing (`Terminal`) — see [`Error::kind`](crate::core::Error::kind).
    /// - [`Error::SerializationError`] if JSON serialisation or schema validation fails.
    pub async fn encode_event(&self, event: &Event) -> Result<EncodedOutput> {
        let bytes = self
            .value_encoder
            .encode_ser(event, &self.topic, EncodeTarget::Value)
            .await
            .map_err(|e| map_registry_error("json schema encode event", e))?;
        Ok(EncodedOutput::new(
            bytes.to_vec(),
            CONFLUENT_JSON_CONTENT_TYPE,
        ))
    }

    /// Encode the primary key of a CDC event to Confluent-framed JSON bytes
    /// (key channel).
    ///
    /// Produces a `{"key": "<json-encoded-pk>"}` payload using [`KEY_JSON_SCHEMA`].
    /// Keyless events (TRUNCATE, SCHEMA_CHANGE, tables without a declared primary
    /// key) produce `{"key": null}`, matching Debezium's behaviour.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the schema registry client fails to resolve the key schema
    /// ID or if the Confluent wire-framing step fails.  These are typically
    /// transient registry or network errors; callers should propagate or log them
    /// rather than silently dropping the key.
    pub async fn encode_event_key(&self, event: &Event) -> Result<bytes::Bytes> {
        let key_json = event
            .primary_key_values()
            .and_then(|v| serde_json::to_string(&v).ok());
        let key_value = serde_json::json!({"key": key_json});
        self.key_encoder
            .encode(&key_value, &self.topic, EncodeTarget::Key)
            .await
            .map_err(|e| map_registry_error("json schema encode event key", e))
    }

    /// Cached schema ID for the value subject, or `None` if not yet resolved.
    ///
    /// Useful for observability without triggering a registry call.
    pub fn cached_value_schema_id(&self) -> Option<SchemaId> {
        let value_subject = self
            .value_encoder
            .cached_schema_id(&format!("{}-value", self.topic));
        let topic_record_subject = self
            .value_encoder
            .cached_schema_id(&format!("{}-io.rustcdc.Event", self.topic));
        let record_subject = self.value_encoder.cached_schema_id("io.rustcdc.Event");
        value_subject.or(topic_record_subject).or(record_subject)
    }
}

// ─── ConfluentJsonSchemaDecoder ───────────────────────────────────────────────

/// Decodes Confluent Schema Registry-framed JSON bytes back to a CDC [`Event`].
///
/// Strips the 5-byte Confluent framing header, deserialises the JSON payload,
/// and converts it to an [`Event`].
///
/// # Decoding steps
///
/// 1. Strip the 5-byte Confluent framing header via [`schemreg::decode_wire_format`].
/// 2. Deserialise the JSON payload to `serde_json::Value`.
/// 3. Optionally validate against [`EVENT_JSON_SCHEMA`] (when `validate_on_decode` is `true`).
/// 4. Convert the `serde_json::Value` to [`Event`] via `serde_json::from_value`.
pub struct ConfluentJsonSchemaDecoder<C = Arc<CachedSchemaRegistry<ConfluentSchemaRegistry>>> {
    inner: ::schemreg::json::JsonSchemaDecoder<C>,
}

impl<C> std::fmt::Debug for ConfluentJsonSchemaDecoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfluentJsonSchemaDecoder")
            .finish_non_exhaustive()
    }
}

impl<C: SchemaRegistryClient> ConfluentJsonSchemaDecoder<C> {
    /// Create a decoder backed by the given registry.
    pub fn new(registry: C) -> Self {
        Self {
            inner: ::schemreg::json::JsonSchemaDecoder::new(registry),
        }
    }

    /// Decode a Confluent-framed JSON **value** message to a CDC [`Event`].
    ///
    /// `async` because schema-ID fetching from the registry is required for
    /// schema IDs not yet in the local cache.
    ///
    /// # Errors
    ///
    /// - [`Error::SerializationError`] if the Confluent framing header is malformed or the
    ///   payload does not deserialise. Both are permanent: the same bytes will never
    ///   decode, so they classify as `Terminal` rather than inviting a retry loop.
    /// - A classified source error when the registry is unreachable (`Transient`) or the
    ///   schema id is not registered (`Terminal`).
    pub async fn decode(&self, bytes: &[u8]) -> Result<Event> {
        let value = self
            .inner
            .decode(bytes::Bytes::copy_from_slice(bytes))
            .await
            .map_err(|e| map_registry_error("json schema decode", e))?;
        // Deserialisation of an already-fetched schema is permanent, not transient.
        serde_json::from_value::<Event>(value)
            .map_err(|e| Error::SerializationError(format!("json schema → Event deserialize: {e}")))
    }
}

// ─── ConfluentJsonSchemaCodec ─────────────────────────────────────────────────

/// Async key + value codec backed by Confluent JSON Schema framing.
///
/// Unlike the synchronous [`crate::codec::Codec`] trait, JSON Schema encoding
/// is inherently async (lazy subject/schema resolution). Use the methods on
/// [`ConfluentJsonSchemaEncoder`] directly instead of the `Codec` trait when
/// building Kafka producers with JSON Schema.
pub type ConfluentJsonSchemaCodec<C> = ConfluentJsonSchemaEncoder<C>;

// ─── Tests ───────────────────────────────────────────────────────────────────

// ─── Cache warming ────────────────────────────────────────────────────────────

/// Pre-resolve a set of schema ids so the first events do not pay a registry round-trip.
///
/// Schema resolution is on the decode path, so a cold cache turns the first message for
/// each distinct schema id into a synchronous registry call. On a consumer restarting
/// against a backlog that is a burst of round-trips exactly when throughput matters most,
/// and it is also when the registry is most likely to rate-limit.
///
/// Schema ids are **immutable** — a registry never reassigns one — so a warmed entry is
/// valid for the process lifetime. Warming is therefore free of staleness risk, unlike
/// caching `get_latest_schema`, which the cache deliberately never does.
///
/// Fetches run concurrently. A failure for one id does not abort the rest: the error names
/// every id that could not be warmed, and warming is best-effort by nature — a failed warm
/// costs a round-trip later, not correctness.
///
/// # Errors
///
/// Returns [`Error::SourceError`] listing the ids that could not be fetched.
pub async fn warm_schema_cache<C>(
    registry: &CachedSchemaRegistry<C>,
    schema_ids: impl IntoIterator<Item = SchemaId>,
) -> Result<()>
where
    C: SchemaRegistryClient,
{
    registry.warm_cache(schema_ids).await.map_err(|error| {
        Error::SourceError(format!(
            "warming the schema cache failed: {error}. This is best-effort — the affected \
             ids will simply be fetched on first use — but a persistent failure usually \
             means the ids do not exist in this registry."
        ))
    })
}

// ─── Confluent Protobuf ───────────────────────────────────────────────────────

/// The compiled descriptor set for `proto/event.proto`.
///
/// Built at compile time by `build.rs` using [`protox`], a pure-Rust protobuf compiler, so
/// building rustcdc never requires `protoc` on the machine.
const EVENT_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/event_descriptor.bin"));

/// Fully-qualified name of the CDC event message in `proto/event.proto`.
const EVENT_PROTO_FULL_NAME: &str = "rustcdc.Event";

/// The `.proto` source registered as the schema, so the registry stores the real IDL.
const EVENT_PROTO_SOURCE: &str = include_str!("../../proto/event.proto");

/// Load the `rustcdc.Event` message descriptor from the compiled descriptor set.
///
/// The descriptor is what makes Confluent Protobuf framing correct. That wire format
/// carries a **message-index path** — the position of the message inside its `.proto` file
/// — and a hand-written index that happens to be wrong produces a header a Confluent
/// deserialiser misreads without erroring. Deriving it from the descriptor makes it correct
/// by construction, which is why `schemreg` requires one rather than accepting raw indexes.
fn event_message_descriptor() -> Result<prost_reflect::MessageDescriptor> {
    let pool =
        prost_reflect::DescriptorPool::decode(EVENT_FILE_DESCRIPTOR_SET).map_err(|error| {
            Error::ConfigError(format!(
                "the compiled protobuf descriptor set is not decodable: {error}. This is a build \
             problem, not a configuration one — `build.rs` produced it from proto/event.proto."
            ))
        })?;

    pool.get_message_by_name(EVENT_PROTO_FULL_NAME)
        .ok_or_else(|| {
            Error::ConfigError(format!(
                "message '{EVENT_PROTO_FULL_NAME}' is missing from the compiled descriptor \
                 set; proto/event.proto and src/codec/protobuf.rs have diverged"
            ))
        })
}

/// CDC [`Event`] → Confluent Schema Registry-framed Protobuf encoder.
///
/// Completes the three-format Confluent story alongside [`ConfluentAvroEncoder`] and
/// [`ConfluentJsonSchemaEncoder`].
///
/// # Framing
///
/// Confluent Protobuf does **not** use the plain 5-byte header. It is:
///
/// ```text
/// [0x00 magic][4-byte BE schema_id][message-index path][protobuf payload]
/// ```
///
/// The message-index path locates the message within its `.proto` file. rustcdc derives it
/// from the compiled descriptor rather than hardcoding it, so it stays correct if the file
/// gains a message ahead of `Event`.
///
/// # Payload shape
///
/// Same as [`crate::codec::ProtobufEncoder`]: `before` and `after` carry UTF-8 JSON as
/// protobuf `bytes`, which keeps the row payload schemaless while the envelope is typed.
/// A consumer decodes the message, then parses those two fields as JSON.
///
/// Requires the `schemreg` feature.
pub struct ConfluentProtobufEncoder<C> {
    inner: Arc<::schemreg::ProtobufSchemaEncoder<C>>,
    topic: String,
}

impl<C> std::fmt::Debug for ConfluentProtobufEncoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfluentProtobufEncoder")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

impl<C> ConfluentProtobufEncoder<C>
where
    C: SchemaRegistryClient + 'static,
{
    /// Build an encoder against `registry`.
    ///
    /// Subject resolution is **lazy and cached per subject**, unlike
    /// [`ConfluentAvroEncoder`] which resolves once at construction. That is the right
    /// shape for Protobuf: the `RecordName` and `TopicRecordName` strategies exist to give
    /// each message type its own subject, and resolving eagerly would defeat them.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] if the descriptor set cannot be loaded or the
    /// encoder cannot be built.
    pub fn new(registry: C, config: &SchemaRegistryConfig) -> Result<Self> {
        let descriptor = event_message_descriptor()?;

        let inner = ::schemreg::ProtobufSchemaEncoder::builder()
            .registry(registry)
            .schema(EVENT_PROTO_SOURCE)
            .descriptor(descriptor)
            .strategy(config.strategy.clone())
            .references(config.references.clone())
            .max_subject_cache_entries(config.max_cache_entries.unwrap_or(1_000))
            .build()
            .map_err(|error| {
                Error::ConfigError(format!("confluent protobuf encoder build: {error}"))
            })?;

        Ok(Self {
            inner: Arc::new(inner),
            topic: config.topic.clone(),
        })
    }

    /// The message-index path this encoder writes into every header.
    pub fn message_indexes(&self) -> &[i32] {
        self.inner.message_indexes()
    }

    /// Number of subjects currently resolved in the encoder's cache.
    pub fn cached_subject_count(&self) -> usize {
        self.inner.cached_subject_count()
    }

    /// Drop a subject's cached schema id, forcing re-resolution on the next encode.
    ///
    /// Use after a deliberate schema change so the encoder picks up the new id without a
    /// restart.
    pub fn invalidate_subject(&self, subject: &str) {
        self.inner.invalidate_subject(subject);
    }

    /// Encode an event as Confluent-framed Protobuf.
    ///
    /// # Errors
    ///
    /// Returns a classified source error when the registry is unreachable (`Transient`) or
    /// the subject cannot be resolved (`Terminal`).
    pub async fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let message = crate::codec::protobuf::ProtoEvent::from_event(event)?;
        let framed = self
            .inner
            .encode(&message, &self.topic, EncodeTarget::Value)
            .await
            .map_err(|error| map_registry_error("confluent protobuf encode", error))?;
        Ok(framed.to_vec())
    }
}

/// Confluent-framed Protobuf → CDC [`Event`] decoder.
///
/// Requires the `schemreg` feature.
pub struct ConfluentProtobufDecoder<C> {
    inner: ::schemreg::ProtobufSchemaDecoder<C>,
}

impl<C> std::fmt::Debug for ConfluentProtobufDecoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfluentProtobufDecoder").finish()
    }
}

impl<C> ConfluentProtobufDecoder<C>
where
    C: SchemaRegistryClient + 'static,
{
    /// Build a decoder against `registry`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] if the expected descriptor cannot be loaded.
    pub fn new(registry: C) -> Result<Self> {
        let descriptor = event_message_descriptor()?;
        let inner = ::schemreg::ProtobufSchemaDecoder::new(registry)
            .with_expected_descriptor(&descriptor)
            .map_err(|error| {
                Error::ConfigError(format!("confluent protobuf decoder build: {error}"))
            })?;
        Ok(Self { inner })
    }

    /// Decode a Confluent-framed Protobuf message to a CDC [`Event`].
    ///
    /// # Errors
    ///
    /// - [`Error::SerializationError`] if the framing or payload is malformed. Permanent:
    ///   the same bytes will never decode, so it classifies `Terminal` rather than inviting
    ///   a retry loop.
    /// - A classified source error when the registry is unreachable (`Transient`) or the
    ///   schema id is not registered (`Terminal`).
    pub async fn decode(&self, bytes: &[u8]) -> Result<Event> {
        let message: crate::codec::protobuf::ProtoEvent = self
            .inner
            .decode(bytes::Bytes::copy_from_slice(bytes))
            .await
            .map_err(|error| map_registry_error("confluent protobuf decode", error))?;
        message.into_event()
    }
}

// ─── Apicurio Registry (native v3 API) ────────────────────────────────────────

/// Configuration for the Apicurio Registry v3 native REST API.
///
/// Apicurio also exposes a Confluent-compatible endpoint, which
/// [`SchemaRegistryConfig`] can already talk to. Prefer this when you want the native
/// API: the compatibility shim flattens Apicurio's artifact groups and richer metadata
/// into the Confluent subject model, so group-scoped artifacts are not addressable
/// through it.
///
/// The resulting client implements [`SchemaRegistryClient`], so it drops straight into
/// [`ConfluentAvroEncoder`] and [`ConfluentJsonSchemaEncoder`] — the **wire format is
/// still the Confluent 5-byte framing**, which is what Apicurio emits in this mode.
///
/// Requires the `apicurio` feature.
#[cfg(feature = "apicurio")]
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ApicurioRegistryConfig {
    /// Apicurio server **root**, e.g. `http://apicurio:8080` — with any trailing slash
    /// trimmed.
    ///
    /// The client appends `/apis/registry/v3` itself, so passing that path here produces
    /// a doubled URL and a 404 from the server. To drive Apicurio through its
    /// Confluent-compatible API instead, use [`SchemaRegistryConfig`] with
    /// `http://apicurio:8080/apis/ccompat/v7`.
    pub url: String,
    /// Kafka topic used to derive subject names via [`SubjectNameStrategy`].
    pub topic: String,
    /// Subject name strategy. Defaults to [`SubjectNameStrategy::TopicName`].
    pub strategy: SubjectNameStrategy,
    /// Optional authentication credentials.
    pub auth: Option<SchemaRegistryAuth>,
    /// Register schemas automatically on first use. Default `true`.
    pub auto_register: bool,
    /// HTTP request timeout in milliseconds.
    pub request_timeout_ms: Option<u64>,
    /// TCP connect timeout in milliseconds.
    pub connect_timeout_ms: Option<u64>,
    /// Maximum schema entries retained in the in-memory cache.
    pub max_cache_entries: Option<usize>,
    /// Retry policy for transient registry failures. See
    /// [`SchemaRegistryConfig::retry_policy`].
    pub retry_policy: RetryPolicy,
}

#[cfg(feature = "apicurio")]
impl ApicurioRegistryConfig {
    /// Create a config with the given registry URL and Kafka topic.
    pub fn new(url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            url: url.into().trim_end_matches('/').to_owned(),
            topic: topic.into(),
            strategy: SubjectNameStrategy::TopicName,
            auth: None,
            auto_register: true,
            request_timeout_ms: None,
            connect_timeout_ms: None,
            max_cache_entries: None,
            retry_policy: RetryPolicy::default(),
        }
    }

    /// Set the subject name strategy.
    #[must_use]
    pub fn with_strategy(mut self, strategy: SubjectNameStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set authentication credentials.
    #[must_use]
    pub fn with_auth(mut self, auth: SchemaRegistryAuth) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Require schemas to exist rather than registering them on first use.
    #[must_use]
    pub fn with_auto_register(mut self, auto_register: bool) -> Self {
        self.auto_register = auto_register;
        self
    }

    /// Replace the retry policy for transient registry failures.
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Build a cached Apicurio registry client.
    ///
    /// Constructs the HTTP client and wraps it in an in-memory LRU cache. Makes **no**
    /// network connections — a wrong URL or an unreachable registry surfaces on first use,
    /// not here.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] if the URL is malformed or a credential cannot be
    /// resolved.
    pub fn build(&self) -> Result<CachedSchemaRegistry<::schemreg::ApicurioSchemaRegistry>> {
        let mut builder = ::schemreg::ApicurioSchemaRegistry::builder().url(&self.url);

        if let Some(ref auth) = self.auth {
            builder = match auth {
                SchemaRegistryAuth::Basic { username, password } => {
                    builder.basic_auth(username, password)
                }
                SchemaRegistryAuth::BearerToken(token) => {
                    let token = token
                        .expose_secret()
                        .map_err(|error| Error::ConfigError(format!("bearer token: {error}")))?;
                    builder.bearer_token(token)
                }
            };
        }

        if let Some(ms) = self.request_timeout_ms {
            builder = builder.request_timeout(Duration::from_millis(ms));
        }
        if let Some(ms) = self.connect_timeout_ms {
            builder = builder.connect_timeout(Duration::from_millis(ms));
        }
        builder = builder.retry_policy(self.retry_policy.clone());

        let registry = builder
            .build()
            .map_err(|error| Error::ConfigError(format!("apicurio registry build: {error}")))?;

        Ok(match self.max_cache_entries {
            Some(n) => CachedSchemaRegistry::with_max_entries(registry, n),
            None => CachedSchemaRegistry::new(registry),
        })
    }

    /// The equivalent [`SchemaRegistryConfig`], for the encoder constructors.
    ///
    /// The encoders take a `SchemaRegistryConfig` for subject naming and registration
    /// policy; the transport comes from the client passed alongside it. This keeps the two
    /// consistent rather than asking a caller to restate the topic and strategy.
    pub fn as_schema_registry_config(&self) -> SchemaRegistryConfig {
        SchemaRegistryConfig::new(&self.url, &self.topic)
            .with_strategy(self.strategy.clone())
            .with_auto_register(self.auto_register)
    }
}

// ─── AWS Glue Schema Registry ─────────────────────────────────────────────────

/// AWS Glue Schema Registry re-exports.
///
/// Glue is **not** a drop-in swap for a Confluent-compatible registry:
///
/// | | Confluent / Apicurio | AWS Glue |
/// |---|---|---|
/// | Wire header | 5 bytes (`0x00` + 4-byte BE id) | 18 bytes (`0x03` + compression + 16-byte UUID) |
/// | Schema identity | monotonic integer id | schema-version UUID |
/// | Compression | none | optional ZLIB |
///
/// So a consumer must know which framing to expect, or use
/// [`detect_wire_format`] to decide per message.
///
/// Requires the `glue` feature.
#[cfg(feature = "glue")]
pub mod glue {
    pub use ::schemreg::glue::{
        decode_glue_wire_format, decode_glue_wire_format_borrowed, encode_glue_wire_format,
        AwsGlueSchemaRegistry, AwsGlueSchemaRegistryBuilder, CachedGlueSchemaRegistry,
        GlueCompression, GlueDataFormat, GlueSchema, GlueSchemaRegistryClient, GlueSchemaVersionId,
    };
}

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

    // ─── JSON Schema constants ────────────────────────────────────────────────

    #[test]
    fn event_json_schema_is_valid_json() {
        serde_json::from_str::<serde_json::Value>(EVENT_JSON_SCHEMA)
            .expect("EVENT_JSON_SCHEMA must be valid JSON");
    }

    #[test]
    fn event_json_schema_has_required_id() {
        let schema: serde_json::Value = serde_json::from_str(EVENT_JSON_SCHEMA).unwrap();
        assert_eq!(
            schema.get("$id").and_then(|v| v.as_str()),
            Some("io.rustcdc.Event"),
            "$id must be 'io.rustcdc.Event'"
        );
    }

    #[test]
    fn event_json_schema_required_fields_present() {
        let schema: serde_json::Value = serde_json::from_str(EVENT_JSON_SCHEMA).unwrap();
        let required: Vec<&str> = schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        for field in &[
            "before",
            "after",
            "op",
            "source",
            "ts",
            "table",
            "envelope_version",
            "before_is_key_only",
        ] {
            assert!(
                required.contains(field),
                "EVENT_JSON_SCHEMA must list '{field}' as required"
            );
        }
    }

    #[test]
    fn event_json_schema_op_enum_is_complete() {
        let schema: serde_json::Value = serde_json::from_str(EVENT_JSON_SCHEMA).unwrap();
        let ops: Vec<&str> = schema["properties"]["op"]["enum"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        for op in &[
            "insert",
            "update",
            "delete",
            "read",
            "schema_change",
            "truncate",
        ] {
            assert!(
                ops.contains(op),
                "EVENT_JSON_SCHEMA op enum must include '{op}'"
            );
        }
    }

    #[test]
    fn key_json_schema_is_valid_json() {
        serde_json::from_str::<serde_json::Value>(KEY_JSON_SCHEMA)
            .expect("KEY_JSON_SCHEMA must be valid JSON");
    }

    #[test]
    fn key_json_schema_has_required_id() {
        let schema: serde_json::Value = serde_json::from_str(KEY_JSON_SCHEMA).unwrap();
        assert_eq!(
            schema.get("$id").and_then(|v| v.as_str()),
            Some("io.rustcdc.EventKey"),
            "$id must be 'io.rustcdc.EventKey'"
        );
    }

    #[test]
    fn key_json_schema_key_field_is_nullable_string() {
        let schema: serde_json::Value = serde_json::from_str(KEY_JSON_SCHEMA).unwrap();
        let key_prop = &schema["properties"]["key"];
        let one_of = key_prop["oneOf"].as_array().unwrap();
        let types: Vec<&str> = one_of
            .iter()
            .filter_map(|v| v.get("type")?.as_str())
            .collect();
        assert!(types.contains(&"null"), "key must allow null");
        assert!(types.contains(&"string"), "key must allow string");
    }

    #[test]
    fn json_schema_encoder_constructs_without_registry_call() {
        // Verify ConfluentJsonSchemaEncoder::new() does not make network calls
        // at construction time — registry is only contacted on first encode.
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "orders");
        let registry = Arc::new(cfg.build().unwrap());
        let encoder = ConfluentJsonSchemaEncoder::new(registry, &cfg);
        assert!(
            encoder.is_ok(),
            "encoder construction must not require a live registry"
        );
        let encoder = encoder.unwrap();
        assert_eq!(encoder.topic(), "orders");
    }

    #[test]
    fn json_schema_encoder_without_validation_constructs() {
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "orders");
        let registry = Arc::new(cfg.build().unwrap());
        let encoder = ConfluentJsonSchemaEncoder::without_validation(registry, &cfg);
        assert!(encoder.is_ok());
    }

    #[test]
    fn json_schema_decoder_constructs() {
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "orders");
        let registry = Arc::new(cfg.build().unwrap());
        let decoder = ConfluentJsonSchemaDecoder::new(registry);
        let dbg = format!("{decoder:?}");
        assert!(dbg.contains("ConfluentJsonSchemaDecoder"));
    }

    #[test]
    fn json_schema_encoder_is_clone() {
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "t");
        let registry = Arc::new(cfg.build().unwrap());
        let encoder = ConfluentJsonSchemaEncoder::new(registry, &cfg).unwrap();
        let _cloned = encoder.clone();
    }

    // ─── Schema-identity verification ────────────────────────────────────────

    #[test]
    fn a_registry_schema_matching_ours_is_accepted_despite_formatting_differences() {
        // Comparison is on Avro's parsing canonical form, so whitespace, docs and field
        // ordering *within the JSON* must not cause a spurious rejection — otherwise
        // every registry that normalises schemas on write would fail startup.
        let reformatted = crate::codec::avro::AVRO_SCHEMA
            .replace('\n', " ")
            .replace("  ", " ");
        assert_ne!(
            reformatted,
            crate::codec::avro::AVRO_SCHEMA,
            "the test input must actually differ textually"
        );

        assert_registry_schema_matches(
            "cdc-events-value",
            &reformatted,
            crate::codec::avro::AVRO_SCHEMA,
        )
        .expect("a formatting-only difference must be accepted");
    }

    #[test]
    fn a_structurally_different_registry_schema_is_rejected() {
        // The bug this prevents: with `auto_register = false` the encoder took the
        // registry's schema *id* but encoded with its own schema. Avro binary is
        // positional and untagged, so a consumer resolving that id to a different schema
        // does not get an error — it gets shifted fields and plausible wrong values.
        let foreign = r#"{
          "type": "record",
          "name": "Event",
          "namespace": "io.rustcdc",
          "fields": [{"name": "totally_different", "type": "string"}]
        }"#;

        let error = assert_registry_schema_matches(
            "cdc-events-value",
            foreign,
            crate::codec::avro::AVRO_SCHEMA,
        )
        .expect_err("a structurally different schema must be rejected, not silently used");

        let message = error.to_string();
        assert!(
            message.contains("positional and untagged"),
            "the error must explain why this is silent corruption; got: {message}"
        );
        assert!(
            message.contains("auto_register"),
            "the error must name the remedy; got: {message}"
        );
    }

    #[test]
    fn a_registry_schema_that_is_not_valid_avro_is_rejected() {
        let error =
            assert_registry_schema_matches("cdc-events-value", "{not avro", KEY_AVRO_SCHEMA)
                .expect_err("invalid Avro must be rejected");
        assert!(error.to_string().contains("not valid Avro"));
    }

    // ─── Registry error classification ───────────────────────────────────────

    #[test]
    fn a_transient_registry_failure_is_retryable() {
        use crate::core::{ErrorKind, SourceErrorKind};

        let error = map_registry_error(
            "get_schema_by_id(7)",
            ::schemreg::SchemaRegError::Http {
                status: 503,
                message: "service unavailable".into(),
            },
        );
        assert_eq!(error.kind(), ErrorKind::Transient);
        assert_eq!(error.source_kind(), Some(SourceErrorKind::NetworkTransient));
    }

    #[test]
    fn a_missing_schema_is_terminal_not_retryable() {
        use crate::core::{ErrorKind, SourceErrorKind};

        // Confluent error code 40403 = schema not found. Retrying cannot register it, so
        // classifying it `Transient` would spin an embedder's retry loop forever.
        let error = map_registry_error(
            "get_schema_by_id(7)",
            ::schemreg::SchemaRegError::Api {
                error_code: 40403,
                message: "Schema not found".into(),
            },
        );
        assert_eq!(error.kind(), ErrorKind::Terminal);
        assert_eq!(error.source_kind(), Some(SourceErrorKind::SchemaMismatch));
    }

    #[test]
    fn an_auth_failure_is_terminal() {
        use crate::core::{ErrorKind, SourceErrorKind};

        let error = map_registry_error(
            "register_schema",
            ::schemreg::SchemaRegError::Auth {
                status: 401,
                message: "unauthorized".into(),
            },
        );
        assert_eq!(error.kind(), ErrorKind::Terminal);
        assert_eq!(error.source_kind(), Some(SourceErrorKind::AuthFailed));
    }

    #[test]
    fn malformed_confluent_framing_is_not_classified_as_retryable() {
        use crate::core::ErrorKind;

        // These exact bytes will never decode. Reporting them as `Transient` — which the
        // decoder used to do, via `SourceError` — invites a retry loop that cannot end.
        let error = decode_wire_format(&[0xFF, 0x00])
            .map_err(|e| {
                crate::core::Error::SerializationError(format!("confluent wire format: {e}"))
            })
            .expect_err("a wrong magic byte must not decode");
        assert_eq!(error.kind(), ErrorKind::Terminal);
    }

    // ─── Confluent Protobuf ──────────────────────────────────────────────────

    #[test]
    fn the_event_message_descriptor_loads_from_the_compiled_descriptor_set() {
        // The descriptor is what makes the Confluent Protobuf message-index path correct.
        // If `proto/event.proto` and the prost structs drift apart, this is where it
        // surfaces — at build time rather than as a header a consumer misreads.
        let descriptor =
            event_message_descriptor().expect("build.rs must produce a loadable descriptor set");
        assert_eq!(descriptor.full_name(), EVENT_PROTO_FULL_NAME);
    }

    #[test]
    fn the_message_index_path_is_derived_not_guessed() {
        use ::schemreg::message_index_path;

        let descriptor = event_message_descriptor().unwrap();
        let indexes = message_index_path(&descriptor).expect("index path must be derivable");

        // `Event` is the 4th message in proto/event.proto (SourceMetadata,
        // SnapshotMetadata, TransactionMetadata, Event), so a hardcoded `[0]` — the
        // obvious guess, and what a single-message schema would use — would be wrong.
        // Confluent deserialisers do not error on a wrong index; they misread the header.
        assert_eq!(
            indexes,
            vec![3],
            "the index path must match Event's position in the .proto file"
        );
    }

    #[test]
    fn proto_event_round_trips_through_the_wire_representation() {
        use crate::codec::protobuf::ProtoEvent;
        use crate::core::{Operation, SourceMetadata, TransactionMetadata};

        let original = Event {
            after: Some(serde_json::json!({"id": 7, "name": "alice"})),
            op: Operation::Update,
            before: Some(serde_json::json!({"id": 7, "name": "bob"})),
            source: SourceMetadata {
                source_name: "postgres".into(),
                offset: "0/16B6A70".into(),
                timestamp: 1_700_000_000_000,
            },
            ts: 1_700_000_000_001,
            schema: Some("public".into()),
            table: "users".into(),
            primary_key: Some(vec!["id".into()]),
            transaction: Some(TransactionMetadata {
                tx_id: 42,
                total_events: Some(2),
                event_index: 1,
            }),
            unavailable_columns: vec!["big_doc".into()],
            ..Event::default()
        };

        let recovered = ProtoEvent::from_event(&original)
            .unwrap()
            .into_event()
            .expect("a well-formed ProtoEvent must convert back");

        assert_eq!(recovered, original, "protobuf must round-trip exactly");
    }

    #[test]
    fn an_unknown_total_events_round_trips_as_none_not_zero() {
        use crate::codec::protobuf::ProtoEvent;
        use crate::core::{Operation, SourceMetadata, TransactionMetadata};

        // protobuf cannot distinguish an absent scalar from zero, so `None` encodes as 0.
        // Decoding 0 back to `Some(0)` would tell a consumer the transaction is empty,
        // when in fact the source did not know its size at begin time.
        let mut event = Event {
            op: Operation::Insert,
            after: Some(serde_json::json!({"id": 1})),
            source: SourceMetadata {
                source_name: "mysql".into(),
                offset: "bin.000001:4".into(),
                timestamp: 1,
            },
            table: "t".into(),
            ..Event::default()
        };
        event.transaction = Some(TransactionMetadata {
            tx_id: 9,
            total_events: None,
            event_index: 0,
        });

        let recovered = ProtoEvent::from_event(&event)
            .unwrap()
            .into_event()
            .unwrap();
        assert_eq!(
            recovered.transaction.unwrap().total_events,
            None,
            "0 is the documented 'unknown' sentinel, not an empty transaction"
        );
    }

    #[test]
    fn an_unspecified_operation_is_rejected_rather_than_defaulted() {
        use crate::codec::protobuf::ProtoEvent;

        // protobuf's zero value is indistinguishable from an absent field. Defaulting it
        // to Insert would turn a truncated or foreign message into a fabricated row
        // creation, which a sink would apply.
        let proto = ProtoEvent {
            op: 0,
            ..Default::default()
        };
        let error = proto
            .into_event()
            .expect_err("OPERATION_UNSPECIFIED must not decode");
        assert!(error.to_string().contains("Refusing to guess"));
    }
}

#[cfg(test)]
mod event_json_schema_tests {
    use super::EVENT_JSON_SCHEMA;
    use crate::core::{Event, Operation, SnapshotMetadata, SourceMetadata, TransactionMetadata};
    use serde_json::json;

    fn validate(event: &Event) -> Result<(), String> {
        let schema: serde_json::Value =
            serde_json::from_str(EVENT_JSON_SCHEMA).expect("the published schema must parse");
        let compiled = jsonschema::validator_for(&schema).expect("the schema must compile");
        let instance = serde_json::to_value(event).expect("event serialises");
        compiled
            .validate(&instance)
            .map_err(|error| format!("{error} (instance: {instance})"))
    }

    #[test]
    fn an_insert_validates_against_the_published_schema() {
        // `before` is null for an INSERT. The schema previously expressed the row payload
        // as `oneOf: [null, {}]`, and the empty schema matches null too — so null was
        // valid under *both* branches and `oneOf` rejected it. That made every INSERT and
        // every DELETE fail validation: the JSON Schema codec could not encode a normal
        // event at all.
        let event = Event::builder("users", Operation::Insert)
            .source(SourceMetadata::new("postgres", "0/16B2E48", 1))
            .after(json!({ "id": 1, "email": "a@example.com" }))
            .ts(1)
            .build();
        validate(&event).expect("an insert must validate");
    }

    #[test]
    fn a_delete_validates_against_the_published_schema() {
        let event = Event::builder("users", Operation::Delete)
            .source(SourceMetadata::new("postgres", "0/16B2E48", 1))
            .before(json!({ "id": 1 }))
            .ts(1)
            .build();
        validate(&event).expect("a delete must validate");
    }

    #[test]
    fn an_event_carrying_unavailable_columns_validates() {
        // These fields are `skip_serializing_if = "Vec::is_empty"`, so they appear only on
        // partial payloads — and the schema declared `additionalProperties: false` without
        // listing them. Every event describing a partial row would have been rejected:
        // exactly the events whose correct handling this crate emphasises most.
        let event = Event::builder("users", Operation::Update)
            .source(SourceMetadata::new("postgres", "0/16B2E48", 1))
            .before(json!({ "id": 1 }))
            .after(json!({ "id": 1, "name": "x" }))
            .unavailable_columns(["big_kept"])
            .before_unavailable_columns(["big_changed"])
            .ts(1)
            .build();
        validate(&event).expect("a partial-payload event must validate");
    }

    #[test]
    fn a_fully_populated_event_validates() {
        let event = Event::builder("users", Operation::Read)
            .source(SourceMetadata::new("postgres", "0/16B2E48", 1))
            .schema("public")
            .before(json!({ "id": 1 }))
            .after(json!({ "id": 1 }))
            .primary_key(["id"])
            .snapshot(SnapshotMetadata::new("snap-1", 0, false))
            .transaction(TransactionMetadata::new(7, 1, Some(3)))
            .before_is_key_only(true)
            .ts(1)
            .build();
        validate(&event).expect("a fully populated event must validate");
    }

    #[test]
    fn every_operation_symbol_is_accepted_by_the_schema() {
        // The schema pins an enum; an operation the encoder can produce but the schema
        // rejects would fail only for that one operation, in production.
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
            validate(&event).unwrap_or_else(|error| panic!("operation {op:?} rejected: {error}"));
        }
    }

    #[test]
    fn an_event_with_an_unknown_field_is_still_rejected() {
        // `additionalProperties: false` is load-bearing: it is what makes a consumer's
        // schema check catch a producer that added a field the consumer cannot interpret.
        // Widening the schema to fix the two defects above must not have disabled it.
        let mut instance = serde_json::to_value(
            Event::builder("t", Operation::Insert)
                .source(SourceMetadata::new("s", "1", 1))
                .after(json!({ "id": 1 }))
                .ts(1)
                .build(),
        )
        .expect("serialise");
        instance["surprise"] = json!("value");

        let schema: serde_json::Value =
            serde_json::from_str(EVENT_JSON_SCHEMA).expect("schema parses");
        let compiled = jsonschema::validator_for(&schema).expect("schema compiles");
        assert!(
            compiled.validate(&instance).is_err(),
            "an unknown field must still be rejected"
        );
    }
}
