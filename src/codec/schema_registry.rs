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
use crate::codec::{AsyncCodec, CodecOutput, EncodedOutput, EventEncoder};
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
        // Exhaustive destructuring — no `..` rest pattern. Adding a field to
        // `SchemaRegistryConfig` breaks the build here until it is either wired into the
        // client or explicitly bound to `_` with a reason. A transport option that silently
        // stops taking effect is the same defect `as_schema_registry_config` shipped with.
        let Self {
            url,
            // Encoder-side, not transport: subject naming and registration policy are read
            // by the encoders, which take this config alongside the client built here.
            topic: _,
            strategy: _,
            references: _,
            auto_register: _,
            auth,
            request_timeout_ms,
            connect_timeout_ms,
            max_cache_entries,
            normalize_schemas,
            pool_max_idle_per_host,
            retry_policy,
        } = self;

        let mut builder = ConfluentSchemaRegistry::builder().url(url);

        if let Some(auth) = auth {
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

        if let Some(ms) = request_timeout_ms {
            builder = builder.request_timeout(Duration::from_millis(*ms));
        }

        if let Some(ms) = connect_timeout_ms {
            builder = builder.connect_timeout(Duration::from_millis(*ms));
        }

        if let Some(n) = pool_max_idle_per_host {
            builder = builder.pool_max_idle_per_host(*n);
        }

        builder = builder.normalize_schemas(*normalize_schemas);
        builder = builder.retry_policy(retry_policy.clone());

        let registry = builder
            .build()
            .map_err(|e| Error::ConfigError(format!("schema registry build: {e}")))?;

        let cached = match max_cache_entries {
            Some(n) => CachedSchemaRegistry::with_max_entries(registry, *n),
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

/// The record names and schema texts rustcdc writes for one wire format.
///
/// Both halves vary by format, and getting either wrong points the check at subjects
/// nothing writes to: Avro and JSON Schema derive subjects from the Avro-style record
/// name, Protobuf from the message's fully-qualified name (which is what `schemreg` takes
/// from the descriptor).
struct SchemaSet {
    value_record: &'static str,
    key_record: &'static str,
    value_schema: &'static str,
    key_schema: &'static str,
}

/// Resolve the schemas rustcdc encodes with for `schema_type`.
///
/// # Errors
///
/// [`Error::ConfigError`] for a format rustcdc has no schemas for. `SchemaType` is
/// `#[non_exhaustive]` in `schemreg`, and quietly falling back to the Avro schemas is
/// precisely the defect this indirection exists to prevent.
fn schema_set_for(schema_type: SchemaType) -> Result<SchemaSet> {
    Ok(match schema_type {
        SchemaType::Avro => SchemaSet {
            value_record: "io.rustcdc.Event",
            key_record: "io.rustcdc.EventKey",
            value_schema: crate::codec::avro::AVRO_SCHEMA,
            key_schema: KEY_AVRO_SCHEMA,
        },
        SchemaType::Json => SchemaSet {
            value_record: "io.rustcdc.Event",
            key_record: "io.rustcdc.EventKey",
            value_schema: EVENT_JSON_SCHEMA,
            key_schema: KEY_JSON_SCHEMA,
        },
        SchemaType::Protobuf => SchemaSet {
            value_record: EVENT_PROTO_FULL_NAME,
            key_record: EVENT_KEY_PROTO_FULL_NAME,
            value_schema: EVENT_PROTO_SOURCE,
            key_schema: KEY_PROTO_SCHEMA,
        },
        other => {
            return Err(Error::ConfigError(format!(
                "rustcdc has no schemas for schema type {other:?}: it encodes Avro, JSON \
                 Schema and Protobuf only."
            )))
        }
    })
}

/// Enforce `auto_register = false` for an encoder whose subject resolution registers.
///
/// # Why this exists
///
/// `SchemaRegistryConfig::auto_register = false` means *"require the schemas to already
/// exist"*. [`ConfluentAvroEncoder`] has always honoured it, because it resolves both
/// subjects itself at construction. The JSON Schema and Protobuf encoders delegate subject
/// resolution to `schemreg`, whose resolution path is `register_schema` — with no
/// lookup-only mode — so through 0.8 the setting was **silently ignored** by both. An
/// operator who set it got schemas registered anyway, and none of the schema-identity
/// checking that setting exists to buy.
///
/// This closes it as far as the dependency allows: at construction, both subjects must
/// already exist *and* carry exactly the schema rustcdc will write. That converts a
/// missing-subject or a permissions problem into a startup failure, and restores the
/// identity check.
///
/// **The one thing it cannot do** is prevent the later `register_schema` call. Because the
/// content is verified identical first, that call is a content-identical re-registration,
/// which a Confluent-compatible registry answers with the existing id rather than a new
/// version. A registry that rejects registration outright for this principal will still
/// fail — but now at startup, with this error, rather than on the first event.
async fn assert_subjects_preregistered<C>(
    registry: &C,
    config: &SchemaRegistryConfig,
    schema_type: SchemaType,
) -> Result<()>
where
    C: SchemaRegistryClient + ?Sized,
{
    let set = schema_set_for(schema_type)?;

    for (record, expected, target) in [
        (set.value_record, set.value_schema, EncodeTarget::Value),
        (set.key_record, set.key_schema, EncodeTarget::Key),
    ] {
        let subject = config
            .strategy
            .subject_name(&config.topic, Some(record), target)
            .map_err(|error| Error::ConfigError(format!("{target} subject name: {error}")))?;

        let registered = registry
            .get_latest_schema(&subject)
            .await
            .map_err(|error| {
                Error::ConfigError(format!(
                    "subject '{subject}' is not registered and `auto_register` is off: {error}. \
                 Register rustcdc's schema out of band, or enable `auto_register` for \
                 first-time setup."
                ))
            })?;
        assert_registry_schema_matches(&subject, &registered.schema, expected, schema_type)?;
    }

    Ok(())
}

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
pub async fn preflight_schema_registry<C>(
    registry: &C,
    config: &SchemaRegistryConfig,
    schema_type: SchemaType,
) -> Result<()>
where
    C: SchemaRegistryClient + ?Sized,
{
    // `schema_type` selects which schemas to check, and it is not optional. Through 0.8
    // this function always checked the **Avro** schemas under Avro record names, whatever
    // codec the pipeline actually used — so a JSON Schema or Protobuf deployment with
    // `auto_register = false` failed preflight against a perfectly correct registry, and
    // with `auto_register = true` ran an Avro compatibility check against a JSON subject.
    //
    // Generic over the client, and `?Sized`, so an `ApicurioSchemaRegistry` or an erased
    // `&dyn DynSchemaRegistryClient` preflights exactly like the Confluent client.
    let SchemaSet {
        value_record,
        key_record,
        value_schema,
        key_schema,
    } = schema_set_for(schema_type)?;

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
        .subject_name(&config.topic, Some(value_record), EncodeTarget::Value)
        .map_err(|error| Error::ConfigError(format!("value subject name: {error}")))?;
    let key_subject = config
        .strategy
        .subject_name(&config.topic, Some(key_record), EncodeTarget::Key)
        .map_err(|error| Error::ConfigError(format!("key subject name: {error}")))?;

    for (subject, expected) in [(&value_subject, value_schema), (&key_subject, key_schema)] {
        if config.auto_register {
            match registry
                .check_compatible(subject, expected, schema_type)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    return Err(Error::ConfigError(format!(
                        "rustcdc's {} schema is INCOMPATIBLE with the schema already \
                         registered under subject '{subject}', per that subject's \
                         compatibility level. Registering it would be rejected by the \
                         registry, and forcing it past the check would break every existing \
                         consumer. Resolve the schema conflict, or use a different subject.",
                        schema_type.as_str()
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
            assert_registry_schema_matches(subject, &registered.schema, expected, schema_type)?;
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
/// Comparison is per format, and in each case tolerant of representation but not of
/// structure:
///
/// - **Avro** — the **parsing canonical form** (RFC-style: strips docs, aliases and
///   default values, normalises ordering), so a registry copy differing only in formatting
///   or in field ordering within the JSON is accepted.
/// - **JSON Schema** — parsed to `serde_json::Value` and compared structurally, so
///   whitespace and key ordering do not matter.
/// - **Protobuf** — the registry stores `.proto` source, so comparison is on the source
///   with comments and redundant whitespace stripped.
fn assert_registry_schema_matches(
    subject: &str,
    registry_schema: &str,
    expected_schema: &str,
    schema_type: SchemaType,
) -> Result<()> {
    let (registry_form, expected_form) = match schema_type {
        SchemaType::Avro => {
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
            (
                registry_parsed.canonical_form(),
                expected_parsed.canonical_form(),
            )
        }
        SchemaType::Json => {
            let registry_parsed: serde_json::Value = serde_json::from_str(registry_schema)
                .map_err(|error| {
                    Error::ConfigError(format!(
                        "schema registered under subject '{subject}' is not valid JSON: {error}"
                    ))
                })?;
            let expected_parsed: serde_json::Value = serde_json::from_str(expected_schema)
                .map_err(|error| {
                    Error::ConfigError(format!(
                        "rustcdc's own JSON schema failed to parse: {error}"
                    ))
                })?;
            // Compared as `Value`, not as re-serialised text. Map equality is
            // order-independent for both of serde_json's map backends, so this holds
            // whether or not something in the dependency graph turns on `preserve_order` —
            // and a registry that stores the schema with its keys in a different order is
            // not a schema change. `to_string` is used only to render the mismatch.
            if registry_parsed == expected_parsed {
                return Ok(());
            }
            (registry_parsed.to_string(), expected_parsed.to_string())
        }
        SchemaType::Protobuf => (
            normalize_proto_source(registry_schema),
            normalize_proto_source(expected_schema),
        ),
        other => {
            return Err(Error::ConfigError(format!(
                "cannot compare schemas of type {other:?}: rustcdc encodes Avro, JSON \
                 Schema and Protobuf only."
            )))
        }
    };

    if registry_form == expected_form {
        return Ok(());
    }

    // The Avro wording is the sharpest case and the one that motivated this check, but the
    // failure mode generalises: the id in the header resolves to the registry's schema
    // while the bytes are whatever rustcdc encoded.
    let consequence = match schema_type {
        SchemaType::Avro => {
            "Avro binary is positional and untagged, so consumers would not see an error — \
             they would silently decode shifted fields and plausible-looking wrong values."
        }
        SchemaType::Json => {
            "Consumers validating against the registry's schema would reject rustcdc's \
             payloads, or accept fields the registry's schema does not describe."
        }
        SchemaType::Protobuf => {
            "Protobuf is tagged, so most mismatches surface as decode errors — but a field \
             whose number was reused with a compatible wire type decodes silently as the \
             wrong field."
        }
        // Unreachable: the comparison above already rejected any other type.
        _ => "Consumers would resolve the id to a schema the payload was not written with.",
    };

    Err(Error::ConfigError(format!(
        "the schema registered under subject '{subject}' is not the {} schema rustcdc \
         encodes with, so every message would be stamped with an id that resolves to a \
         different schema. {consequence}\n\
         \n\
         Registry canonical form: {registry_form}\n\
         Expected canonical form: {expected_form}\n\
         \n\
         Remedy: register rustcdc's schema under this subject (set `auto_register = true` \
         for first-time setup, or register it out of band), or point `topic`/`strategy` at \
         a subject that carries it.",
        schema_type.as_str()
    )))
}

/// Strip `//` comments and collapse whitespace in `.proto` source.
///
/// The registry stores Protobuf schemas as source text, so a byte comparison would reject
/// a registry copy that differs only in a reflowed comment. Comments carry no wire
/// semantics; token sequence does.
fn normalize_proto_source(source: &str) -> String {
    source
        .lines()
        .map(|line| match line.find("//") {
            Some(index) => &line[..index],
            None => line,
        })
        .flat_map(str::split_whitespace)
        .collect::<Vec<_>>()
        .join(" ")
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
                SchemaType::Avro,
            )?;
            assert_registry_schema_matches(
                &key_subject,
                &ks.schema,
                KEY_AVRO_SCHEMA,
                SchemaType::Avro,
            )?;

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

/// Content type for Confluent-framed Protobuf, alongside the Avro and JSON constants.
const CONFLUENT_PROTOBUF_CONTENT_TYPE: &str = "application/vnd.kafka+protobuf";

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
/// let encoder = ConfluentJsonSchemaEncoder::new(registry, &config).await?;
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
    pub async fn new(registry: C, config: &SchemaRegistryConfig) -> Result<Self> {
        Self::new_inner(registry, config, true).await
    }

    /// Construct a JSON Schema encoder that skips JSON Schema validation on encode.
    ///
    /// Use this only when producers are trusted and throughput is the priority.
    /// Invalid events will be accepted by the encoder but may be rejected by
    /// consumers that validate on decode.
    ///
    /// # Errors
    ///
    /// As [`ConfluentJsonSchemaEncoder::new`].
    pub async fn without_validation(registry: C, config: &SchemaRegistryConfig) -> Result<Self> {
        Self::new_inner(registry, config, false).await
    }

    async fn new_inner(registry: C, config: &SchemaRegistryConfig, validate: bool) -> Result<Self> {
        // `auto_register = false` was silently ignored here through 0.8: `schemreg`'s JSON
        // encoder resolves subjects by registering them, with no lookup-only mode, so the
        // setting bought nothing. Verifying up front is what it can be made to mean — see
        // `assert_subjects_preregistered` for exactly what that does and does not enforce.
        if !config.auto_register {
            assert_subjects_preregistered(&registry, config, SchemaType::Json).await?;
        }

        // `record_name` is what the `RecordName` and `TopicRecordName` strategies derive
        // the subject from. Without it those two strategies failed at *encode* time with
        // "RecordName strategy requires a record name" — a config error that only surfaced
        // once traffic was flowing, and only for the strategies that exist to give each
        // record type its own subject. The names match the `$id` of each JSON Schema and
        // the record names the Avro encoder uses, so the two codecs agree on subjects.
        let value_encoder = ::schemreg::json::JsonSchemaEncoder::builder()
            .registry(registry.clone())
            .schema(EVENT_JSON_SCHEMA)
            .record_name("io.rustcdc.Event")
            .strategy(config.strategy.clone())
            // Carried through, like the Avro and Protobuf encoders already did. Dropping
            // them made registration fail against a subject namespace whose referenced
            // types cannot be resolved — the failure `references` exists to prevent.
            .references(config.references.clone())
            .validate_on_encode(validate)
            .build()
            .map_err(|e| Error::ConfigError(format!("json schema value encoder build: {e}")))?;

        // The key subject's schema is `KEY_JSON_SCHEMA`, which imports nothing, so the
        // envelope's references deliberately do not apply to it.
        let key_encoder = ::schemreg::json::JsonSchemaEncoder::builder()
            .registry(registry)
            .schema(KEY_JSON_SCHEMA)
            .record_name("io.rustcdc.EventKey")
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
/// JSON Schema encoding is inherently async (lazy subject/schema resolution), so this
/// implements [`crate::codec::AsyncCodec`] rather than the synchronous
/// [`crate::codec::Codec`]. A sink that accepts `AsyncCodec` — or stores a
/// [`BoxedAsyncCodec`](crate::codec::BoxedAsyncCodec) — takes this and every synchronous
/// codec through the same type.
pub type ConfluentJsonSchemaCodec<C> = ConfluentJsonSchemaEncoder<C>;

/// Async key + value codec: registry-framed JSON Schema on both channels.
///
/// The key is `None` for keyless events rather than a framed `{"key": null}`, so a Kafka
/// producer round-robins them instead of collapsing every keyless event onto one
/// partition. Call [`encode_event_key`](ConfluentJsonSchemaEncoder::encode_event_key)
/// directly when the framed-always form (matching Debezium) is what you want.
#[async_trait::async_trait]
impl<C> AsyncCodec for ConfluentJsonSchemaEncoder<C>
where
    C: SchemaRegistryClient + Clone + Send + Sync + 'static,
{
    async fn encode_async(&self, event: &Event) -> Result<CodecOutput> {
        let key = match event.primary_key_values() {
            Some(_) => Some(self.encode_event_key(event).await?.to_vec()),
            None => None,
        };
        let value = self.encode_event(event).await?;
        Ok(CodecOutput::new(key, value.bytes, value.content_type))
    }

    fn content_type(&self) -> &'static str {
        CONFLUENT_JSON_CONTENT_TYPE
    }
}

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
    registry: &C,
    schema_ids: impl IntoIterator<Item = SchemaId>,
) -> Result<()>
where
    C: SchemaRegistryClient + ?Sized,
{
    use futures_util::StreamExt as _;

    // Any `SchemaRegistryClient`, not `CachedSchemaRegistry<C>` specifically. Requiring the
    // concrete cache type made this unusable behind `Arc<dyn DynSchemaRegistryClient>` —
    // and erasure is exactly what a deployment with several registry backends needs, since
    // the encoders are generic over the client and every variant would otherwise exist
    // twice. Warming matters most in precisely those multi-registry, many-subject
    // deployments, so the two features could not be used together.
    //
    // `?Sized` so `&dyn DynSchemaRegistryClient` passes directly.
    //
    // Fetching through the trait warms the same cache: `CachedSchemaRegistry`'s
    // `get_schema_by_id` is the cache-populating path, which is what `warm_cache` calls
    // internally. Against a client with **no** cache this issues the round-trips and
    // retains nothing — a wasted warm rather than a wrong one.
    let unique: std::collections::BTreeSet<SchemaId> = schema_ids.into_iter().collect();
    if unique.is_empty() {
        return Ok(());
    }

    // Bounded so that pre-warming several thousand ids on startup cannot exhaust the HTTP
    // connection pool or trip the registry's rate limiter. Matches `schemreg`'s own bound.
    const WARM_CACHE_CONCURRENCY: usize = 16;

    let failures: Vec<String> = futures_util::stream::iter(unique)
        .map(|id| async move {
            registry
                .get_schema_by_id(id)
                .await
                .err()
                .map(|error| format!("id {id}: {error}"))
        })
        .buffer_unordered(WARM_CACHE_CONCURRENCY)
        .filter_map(|failure| async move { failure })
        .collect()
        .await;

    if failures.is_empty() {
        return Ok(());
    }

    // One id failing does not abort the rest — the successful fetches are warmed, and the
    // error names every id that was not.
    Err(Error::SourceError(format!(
        "warming the schema cache failed for {} schema id(s): {}. This is best-effort — the \
         affected ids will simply be fetched on first use — but a persistent failure usually \
         means the ids do not exist in this registry.",
        failures.len(),
        failures.join("; ")
    )))
}

// ─── Confluent Protobuf ───────────────────────────────────────────────────────

/// The compiled descriptor set for `proto/event.proto`.
///
/// Built at compile time by `build.rs` using [`protox`], a pure-Rust protobuf compiler, so
/// building rustcdc never requires `protoc` on the machine.
const EVENT_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/event_descriptor.bin"));

/// The compiled descriptor set for `proto/event_key.proto`.
const EVENT_KEY_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/event_key_descriptor.bin"));

/// Fully-qualified name of the CDC event message in `proto/event.proto`.
const EVENT_PROTO_FULL_NAME: &str = "rustcdc.Event";

/// Fully-qualified name of the primary-key message in `proto/event_key.proto`.
const EVENT_KEY_PROTO_FULL_NAME: &str = "rustcdc.EventKey";

/// The `.proto` source registered as the schema, so the registry stores the real IDL.
const EVENT_PROTO_SOURCE: &str = include_str!("../../proto/event.proto");

/// The `.proto` source registered under the **key** subject by
/// [`ConfluentProtobufEncoder::encode_event_key`].
///
/// The Protobuf counterpart of [`KEY_AVRO_SCHEMA`] and [`KEY_JSON_SCHEMA`], and the third
/// leg of a three-format key story that was missing one: through 0.8, `ConfluentAvroEncoder`
/// had `encode_key` and `ConfluentJsonSchemaEncoder` had `encode_event_key`, but the
/// Protobuf encoder had no key path at all, so a fan-out mixing codecs silently paired a
/// registry-framed value with an unframed compact-JSON key and nothing in the API said so.
///
/// Carries the same payload as the other two: the primary key as a JSON object encoded in
/// a string, absent for keyless events.
pub const KEY_PROTO_SCHEMA: &str = include_str!("../../proto/event_key.proto");

/// Load the `rustcdc.Event` message descriptor from the compiled descriptor set.
///
/// The descriptor is what makes Confluent Protobuf framing correct. That wire format
/// carries a **message-index path** — the position of the message inside its `.proto` file
/// — and a hand-written index that happens to be wrong produces a header a Confluent
/// deserialiser misreads without erroring. Deriving it from the descriptor makes it correct
/// by construction, which is why `schemreg` requires one rather than accepting raw indexes.
fn event_message_descriptor() -> Result<prost_reflect::MessageDescriptor> {
    message_descriptor(
        EVENT_FILE_DESCRIPTOR_SET,
        EVENT_PROTO_FULL_NAME,
        "proto/event.proto",
    )
}

/// Load the `rustcdc.EventKey` message descriptor. See [`event_message_descriptor`].
fn event_key_message_descriptor() -> Result<prost_reflect::MessageDescriptor> {
    message_descriptor(
        EVENT_KEY_FILE_DESCRIPTOR_SET,
        EVENT_KEY_PROTO_FULL_NAME,
        "proto/event_key.proto",
    )
}

fn message_descriptor(
    descriptor_set: &[u8],
    full_name: &str,
    proto_path: &str,
) -> Result<prost_reflect::MessageDescriptor> {
    let pool = prost_reflect::DescriptorPool::decode(descriptor_set).map_err(|error| {
        Error::ConfigError(format!(
            "the compiled protobuf descriptor set for {proto_path} is not decodable: {error}. \
             This is a build problem, not a configuration one — `build.rs` produced it."
        ))
    })?;

    pool.get_message_by_name(full_name).ok_or_else(|| {
        Error::ConfigError(format!(
            "message '{full_name}' is missing from the compiled descriptor set; \
             {proto_path} and src/codec/protobuf.rs have diverged"
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
///
/// # Keys
///
/// [`encode_event_key`](Self::encode_event_key) frames the primary key against
/// [`KEY_PROTO_SCHEMA`] under the key subject, mirroring
/// [`ConfluentAvroEncoder::encode_key`] and
/// [`ConfluentJsonSchemaEncoder::encode_event_key`]. Use it rather than
/// [`crate::codec::ProtobufEncoder`]'s default compact-JSON key, which produces a key
/// framed differently from the value it accompanies.
pub struct ConfluentProtobufEncoder<C> {
    inner: Arc<::schemreg::ProtobufSchemaEncoder<C>>,
    key_encoder: Arc<::schemreg::ProtobufSchemaEncoder<C>>,
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
    pub async fn new(registry: C, config: &SchemaRegistryConfig) -> Result<Self>
    where
        C: Clone,
    {
        // See `ConfluentJsonSchemaEncoder::new_inner`: `schemreg`'s Protobuf encoder also
        // resolves subjects by registering them, so `auto_register = false` was silently
        // ignored here too.
        if !config.auto_register {
            assert_subjects_preregistered(&registry, config, SchemaType::Protobuf).await?;
        }

        let inner = ::schemreg::ProtobufSchemaEncoder::builder()
            .registry(registry.clone())
            .schema(EVENT_PROTO_SOURCE)
            .descriptor(event_message_descriptor()?)
            .strategy(config.strategy.clone())
            .references(config.references.clone())
            .max_subject_cache_entries(config.max_cache_entries.unwrap_or(1_000))
            .build()
            .map_err(|error| {
                Error::ConfigError(format!("confluent protobuf encoder build: {error}"))
            })?;

        // The key subject gets its own schema — `proto/event_key.proto`, not the event
        // file — so the registered IDL contains exactly the message the key subject uses.
        // References are deliberately *not* carried over: they belong to the event
        // envelope, and `EventKey` imports nothing.
        let key_encoder = ::schemreg::ProtobufSchemaEncoder::builder()
            .registry(registry)
            .schema(KEY_PROTO_SCHEMA)
            .descriptor(event_key_message_descriptor()?)
            .strategy(config.strategy.clone())
            .max_subject_cache_entries(config.max_cache_entries.unwrap_or(1_000))
            .build()
            .map_err(|error| {
                Error::ConfigError(format!("confluent protobuf key encoder build: {error}"))
            })?;

        Ok(Self {
            inner: Arc::new(inner),
            key_encoder: Arc::new(key_encoder),
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

    /// Encode the primary key of an event as Confluent-framed Protobuf (key channel).
    ///
    /// Produces a `rustcdc.EventKey` message registered under the key subject against
    /// [`KEY_PROTO_SCHEMA`], so key and value share the same framing. Keyless events —
    /// TRUNCATE, SCHEMA_CHANGE, tables with no declared primary key — produce a message
    /// with the `key` field absent, matching the `{"key": null}` that
    /// [`ConfluentJsonSchemaEncoder::encode_event_key`] emits and Debezium's behaviour.
    ///
    /// # Errors
    ///
    /// Returns a classified source error when the registry is unreachable (`Transient`) or
    /// the key subject cannot be resolved (`Terminal`).
    pub async fn encode_event_key(&self, event: &Event) -> Result<Vec<u8>> {
        let message = crate::codec::protobuf::ProtoEventKey::from_event(event)?;
        let framed = self
            .key_encoder
            .encode(&message, &self.topic, EncodeTarget::Key)
            .await
            .map_err(|error| map_registry_error("confluent protobuf encode key", error))?;
        Ok(framed.to_vec())
    }
}

/// Async key + value codec: registry-framed Protobuf on both channels.
///
/// The key is `None` for keyless events rather than a framed `EventKey` with an absent
/// `key` field, so a Kafka producer round-robins them instead of collapsing every keyless
/// event onto one partition. Call [`encode_event_key`](ConfluentProtobufEncoder::encode_event_key)
/// directly when the framed-always form is what you want.
#[async_trait::async_trait]
impl<C> AsyncCodec for ConfluentProtobufEncoder<C>
where
    C: SchemaRegistryClient + Send + Sync + 'static,
{
    async fn encode_async(&self, event: &Event) -> Result<CodecOutput> {
        let key = match event.primary_key_values() {
            Some(_) => Some(self.encode_event_key(event).await?),
            None => None,
        };
        let value = ConfluentProtobufEncoder::encode(self, event).await?;
        Ok(CodecOutput::new(
            key,
            value,
            CONFLUENT_PROTOBUF_CONTENT_TYPE,
        ))
    }

    fn content_type(&self) -> &'static str {
        CONFLUENT_PROTOBUF_CONTENT_TYPE
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
    /// Maximum number of idle keep-alive connections per host. See
    /// [`SchemaRegistryConfig::pool_max_idle_per_host`].
    pub pool_max_idle_per_host: Option<usize>,
    /// Schemas this one depends on, registered as artifact references. See
    /// [`SchemaRegistryConfig::references`].
    ///
    /// Apicurio calls these *artifact references* and models them with the same
    /// `(name, subject, version)` triple, so [`SchemaReference`] carries over unchanged.
    pub references: Vec<SchemaReference>,
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
            pool_max_idle_per_host: None,
            references: Vec::new(),
            retry_policy: RetryPolicy::default(),
        }
    }

    /// Set the subject name strategy.
    #[must_use]
    pub fn with_strategy(mut self, strategy: SubjectNameStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set the HTTP request timeout, in milliseconds.
    #[must_use]
    pub fn with_request_timeout_ms(mut self, ms: u64) -> Self {
        self.request_timeout_ms = Some(ms);
        self
    }

    /// Set the TCP connect timeout, in milliseconds.
    #[must_use]
    pub fn with_connect_timeout_ms(mut self, ms: u64) -> Self {
        self.connect_timeout_ms = Some(ms);
        self
    }

    /// Cap the in-memory schema cache at `n` entries.
    #[must_use]
    pub fn with_max_cache_entries(mut self, n: usize) -> Self {
        self.max_cache_entries = Some(n);
        self
    }

    /// Cap idle keep-alive connections per host.
    #[must_use]
    pub fn with_pool_max_idle_per_host(mut self, n: usize) -> Self {
        self.pool_max_idle_per_host = Some(n);
        self
    }

    /// Declare the artifact references this schema depends on.
    #[must_use]
    pub fn with_references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = references;
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
        // Exhaustive destructuring — see `SchemaRegistryConfig::build`.
        let Self {
            url,
            // Encoder-side, not transport.
            topic: _,
            strategy: _,
            references: _,
            auto_register: _,
            auth,
            request_timeout_ms,
            connect_timeout_ms,
            max_cache_entries,
            pool_max_idle_per_host,
            retry_policy,
        } = self;

        let mut builder = ::schemreg::ApicurioSchemaRegistry::builder().url(url);

        if let Some(auth) = auth {
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

        if let Some(ms) = request_timeout_ms {
            builder = builder.request_timeout(Duration::from_millis(*ms));
        }
        if let Some(ms) = connect_timeout_ms {
            builder = builder.connect_timeout(Duration::from_millis(*ms));
        }
        if let Some(n) = pool_max_idle_per_host {
            builder = builder.pool_max_idle_per_host(*n);
        }
        builder = builder.retry_policy(retry_policy.clone());

        let registry = builder
            .build()
            .map_err(|error| Error::ConfigError(format!("apicurio registry build: {error}")))?;

        Ok(match max_cache_entries {
            Some(n) => CachedSchemaRegistry::with_max_entries(registry, *n),
            None => CachedSchemaRegistry::new(registry),
        })
    }

    /// The equivalent [`SchemaRegistryConfig`], for the encoder constructors.
    ///
    /// The encoders take a `SchemaRegistryConfig` for subject naming and registration
    /// policy; the transport comes from the client passed alongside it. This keeps the two
    /// consistent rather than asking a caller to restate the topic and strategy.
    ///
    /// **Every field this type has is carried over.** The body destructures `self`
    /// exhaustively, so adding a field to [`ApicurioRegistryConfig`] without deciding how
    /// it maps is a compile error rather than a setting that quietly stops taking effect —
    /// which is how `auth`, both timeouts, `max_cache_entries` and `retry_policy` were
    /// silently dropped before 0.9.
    ///
    /// Two things do *not* carry over, because they have no Apicurio counterpart:
    ///
    /// - [`SchemaRegistryConfig::normalize_schemas`] is a Confluent query parameter
    ///   (`?normalize=true`) on `POST /subjects/{subject}/versions`. Apicurio's native v3
    ///   API has no equivalent, so this stays `false`. Apicurio canonicalises content for
    ///   its own `IfExists` de-duplication instead.
    /// - `url` is copied verbatim, and for this type that is the Apicurio **server root**,
    ///   not a Confluent API root. Do not call [`SchemaRegistryConfig::build`] on the
    ///   result — it would target `/subjects` under the v3 root and 404. Use
    ///   [`ApicurioRegistryConfig::build`] for the client, and this config only for the
    ///   policy half of an encoder constructor. (Apicurio's Confluent-compatible endpoint
    ///   lives at `{root}/apis/ccompat/v7`; point a `SchemaRegistryConfig` there directly
    ///   if that is what you want.)
    pub fn as_schema_registry_config(&self) -> SchemaRegistryConfig {
        // Exhaustive destructuring — no `..` rest pattern. This is the gate: a new field on
        // `ApicurioRegistryConfig` breaks the build here until it is mapped or explicitly
        // ignored with a reason.
        let Self {
            url,
            topic,
            strategy,
            auth,
            auto_register,
            request_timeout_ms,
            connect_timeout_ms,
            max_cache_entries,
            pool_max_idle_per_host,
            references,
            retry_policy,
        } = self;

        let mut config = SchemaRegistryConfig::new(url, topic)
            .with_strategy(strategy.clone())
            .with_auto_register(*auto_register)
            .with_references(references.clone())
            .with_retry_policy(retry_policy.clone());
        if let Some(auth) = auth {
            config = config.with_auth(auth.clone());
        }
        if let Some(ms) = request_timeout_ms {
            config = config.with_request_timeout_ms(*ms);
        }
        if let Some(ms) = connect_timeout_ms {
            config = config.with_connect_timeout_ms(*ms);
        }
        if let Some(n) = max_cache_entries {
            config = config.with_max_cache_entries(*n);
        }
        if let Some(n) = pool_max_idle_per_host {
            config = config.with_pool_max_idle_per_host(*n);
        }
        config
    }

    /// Run [`preflight_schema_registry`] against an Apicurio client using this config.
    ///
    /// Preflight was Confluent-only in practice through 0.8 — not because the check needed
    /// a Confluent client (it has always been generic over [`SchemaRegistryClient`], and
    /// `ApicurioSchemaRegistry` implements it) but because it took a
    /// [`SchemaRegistryConfig`] and there was no obvious way to get one from here. An
    /// Apicurio deployment therefore silently got no startup check while a Confluent one
    /// did.
    ///
    /// Subject names come from [`ApicurioRegistryConfig::strategy`], including
    /// [`SubjectNameStrategy::ApicurioGroupRecordName`], so a group-scoped artifact is
    /// checked at the address the encoder will actually write to.
    ///
    /// # Errors
    ///
    /// As [`preflight_schema_registry`].
    pub async fn preflight<C>(&self, registry: &C, schema_type: SchemaType) -> Result<()>
    where
        C: SchemaRegistryClient + ?Sized,
    {
        preflight_schema_registry(registry, &self.as_schema_registry_config(), schema_type).await
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
    use std::sync::Arc;

    use apache_avro::Schema;

    use crate::codec::{EncodedOutput, EventEncoder};
    use crate::core::{Error, Event, Result};

    pub use ::schemreg::glue::{
        decode_glue_wire_format, decode_glue_wire_format_borrowed, encode_glue_wire_format,
        AwsGlueSchemaRegistry, AwsGlueSchemaRegistryBuilder, CachedGlueSchemaRegistry,
        GlueCompression, GlueDataFormat, GlueSchema, GlueSchemaRegistryClient, GlueSchemaVersionId,
    };

    /// Content type for Glue-framed Avro, distinct from the Confluent one because the
    /// framing is not the same and a consumer must not treat them interchangeably.
    const GLUE_AVRO_CONTENT_TYPE: &str = "application/vnd.aws-glue+avro";

    /// Configuration for the Glue Avro encoder.
    ///
    /// Glue identifies schemas by **name**, not by the topic/subject pair Confluent uses,
    /// so there is no [`SubjectNameStrategy`](super::SubjectNameStrategy) here — the
    /// schema name is given directly.
    #[derive(Debug, Clone)]
    #[non_exhaustive]
    pub struct GlueAvroConfig {
        /// Glue schema name for the event envelope, e.g. `"cdc-events"`.
        pub schema_name: String,
        /// Glue schema name for the primary-key envelope.
        ///
        /// Defaults to `{schema_name}-key`, mirroring the Confluent `-key` suffix.
        pub key_schema_name: String,
        /// Payload compression. Glue's header carries a compression byte; Confluent's
        /// does not.
        pub compression: GlueCompression,
        /// Register the schemas on first use rather than requiring them to exist.
        ///
        /// Glue's `register_schema` is idempotent for identical content, so `true` is safe
        /// to leave on. Unlike the Confluent path there is no lookup-by-name API in
        /// `schemreg`'s Glue client, so `false` is **not** offered rather than being
        /// offered and silently ignored.
        pub auto_register: bool,
    }

    impl GlueAvroConfig {
        /// Configure the encoder for a schema name, defaulting the key schema to
        /// `{schema_name}-key`.
        pub fn new(schema_name: impl Into<String>) -> Self {
            let schema_name = schema_name.into();
            Self {
                key_schema_name: format!("{schema_name}-key"),
                schema_name,
                compression: GlueCompression::None,
                auto_register: true,
            }
        }

        /// Override the key schema name.
        #[must_use]
        pub fn with_key_schema_name(mut self, name: impl Into<String>) -> Self {
            self.key_schema_name = name.into();
            self
        }

        /// Compress payloads with ZLIB.
        #[must_use]
        pub fn with_compression(mut self, compression: GlueCompression) -> Self {
            self.compression = compression;
            self
        }
    }

    /// CDC [`Event`] → AWS Glue-framed Avro encoder.
    ///
    /// # Why this exists
    ///
    /// Through 0.8 the `glue` feature re-exported `schemreg`'s Glue types and nothing else,
    /// while the feature description promised "the AWS Glue Schema Registry as a backend".
    /// An embedder got no `Event` encoder at all and had to write the Avro conversion, the
    /// registration and the framing by hand — the work every other registry backend does
    /// for them.
    ///
    /// # Framing
    ///
    /// Glue does **not** use the Confluent 5-byte header:
    ///
    /// ```text
    /// [0x03 version][compression byte][16-byte schema-version UUID][avro payload]
    /// ```
    ///
    /// 18 bytes, a UUID rather than an integer id, and an optional ZLIB payload. A consumer
    /// must know which framing to expect, or call
    /// [`detect_wire_format`](super::detect_wire_format) per message.
    ///
    /// # Schema identity
    ///
    /// The payload is the same [`AVRO_SCHEMA`](crate::codec::avro::AVRO_SCHEMA) the
    /// Confluent Avro encoder writes, so a consumer that already decodes rustcdc's Avro
    /// envelope needs only the framing changed.
    pub struct GlueAvroEncoder<C> {
        registry: Arc<C>,
        avro: crate::codec::avro::AvroEncoder,
        key_schema: Schema,
        config: GlueAvroConfig,
        /// Resolved once at construction, like [`super::ConfluentAvroEncoder`]: Glue has one
        /// schema name per encoder, so there is nothing to resolve lazily.
        value_version_id: GlueSchemaVersionId,
        key_version_id: GlueSchemaVersionId,
    }

    impl<C> std::fmt::Debug for GlueAvroEncoder<C> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("GlueAvroEncoder")
                .field("schema_name", &self.config.schema_name)
                .field("key_schema_name", &self.config.key_schema_name)
                .field("compression", &self.config.compression)
                .finish_non_exhaustive()
        }
    }

    impl<C: GlueSchemaRegistryClient> GlueAvroEncoder<C> {
        /// Register (or resolve) both schemas and build the encoder.
        ///
        /// # Errors
        ///
        /// Returns a classified source error when Glue is unreachable (`Transient`) or
        /// rejects the schema (`Terminal`).
        pub async fn new(registry: Arc<C>, config: GlueAvroConfig) -> Result<Self> {
            let avro = crate::codec::avro::AvroEncoder::new()?;
            let key_schema = Schema::parse_str(super::KEY_AVRO_SCHEMA).map_err(|error| {
                Error::ConfigError(format!(
                    "rustcdc's key Avro schema failed to parse: {error}"
                ))
            })?;

            let value_version_id = registry
                .register_schema(
                    &config.schema_name,
                    crate::codec::avro::AVRO_SCHEMA,
                    GlueDataFormat::Avro,
                )
                .await
                .map_err(|error| super::map_registry_error("glue register value schema", error))?;
            let key_version_id = registry
                .register_schema(
                    &config.key_schema_name,
                    super::KEY_AVRO_SCHEMA,
                    GlueDataFormat::Avro,
                )
                .await
                .map_err(|error| super::map_registry_error("glue register key schema", error))?;

            Ok(Self {
                registry,
                avro,
                key_schema,
                config,
                value_version_id,
                key_version_id,
            })
        }

        /// The Glue schema-version UUID stamped into every value header.
        pub fn value_schema_version_id(&self) -> GlueSchemaVersionId {
            self.value_version_id
        }

        /// The Glue schema-version UUID stamped into every key header.
        pub fn key_schema_version_id(&self) -> GlueSchemaVersionId {
            self.key_version_id
        }

        /// Borrow the registry client.
        pub fn registry(&self) -> &Arc<C> {
            &self.registry
        }

        /// Encode an event as Glue-framed Avro.
        ///
        /// # Errors
        ///
        /// [`Error::SerializationError`] if the event cannot be encoded, or a wire-format
        /// error if framing fails.
        pub fn encode_event(&self, event: &Event) -> Result<EncodedOutput> {
            let payload = self.avro.encode(event)?;
            let framed = encode_glue_wire_format(
                self.value_version_id,
                &payload.bytes,
                self.config.compression,
            )
            .map_err(|error| super::map_registry_error("glue value framing", error))?;
            Ok(EncodedOutput::new(framed.to_vec(), GLUE_AVRO_CONTENT_TYPE))
        }

        /// Encode the primary key as Glue-framed Avro against `KEY_AVRO_SCHEMA`.
        ///
        /// Returns `None` for a keyless event — TRUNCATE, SCHEMA_CHANGE, or a table with no
        /// declared primary key — so a producer round-robins them rather than collapsing
        /// every keyless event onto one partition.
        ///
        /// # Errors
        ///
        /// As [`encode_event`](Self::encode_event).
        pub fn encode_event_key(&self, event: &Event) -> Result<Option<Vec<u8>>> {
            let Some(key) = event.primary_key_values() else {
                return Ok(None);
            };
            let key_json = serde_json::to_string(&key).map_err(|error| {
                Error::SerializationError(format!("glue key serialise: {error}"))
            })?;
            let record = apache_avro::types::Value::Record(vec![(
                "key".to_string(),
                apache_avro::types::Value::Union(
                    1,
                    Box::new(apache_avro::types::Value::String(key_json)),
                ),
            )]);
            let payload = apache_avro::to_avro_datum(&self.key_schema, record)
                .map_err(|error| Error::SerializationError(format!("glue key encode: {error}")))?;
            let framed =
                encode_glue_wire_format(self.key_version_id, &payload, self.config.compression)
                    .map_err(|error| super::map_registry_error("glue key framing", error))?;
            Ok(Some(framed.to_vec()))
        }
    }

    #[async_trait::async_trait]
    impl<C> crate::codec::AsyncCodec for GlueAvroEncoder<C>
    where
        C: GlueSchemaRegistryClient + Send + Sync + 'static,
    {
        async fn encode_async(&self, event: &Event) -> Result<crate::codec::CodecOutput> {
            let value = self.encode_event(event)?;
            Ok(crate::codec::CodecOutput::new(
                self.encode_event_key(event)?,
                value.bytes,
                value.content_type,
            ))
        }

        fn content_type(&self) -> &'static str {
            GLUE_AVRO_CONTENT_TYPE
        }
    }

    /// Glue-framed Avro → CDC [`Event`] decoder.
    ///
    /// Strips the 18-byte header, resolves the writer schema by its version UUID, and
    /// converts through the same [`avro_value_to_event`](crate::codec::avro_value_to_event)
    /// used by the bare and Confluent Avro decoders.
    pub struct GlueAvroDecoder<C> {
        registry: Arc<C>,
        reader_schema: Arc<Schema>,
    }

    impl<C> std::fmt::Debug for GlueAvroDecoder<C> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("GlueAvroDecoder").finish_non_exhaustive()
        }
    }

    impl<C> Clone for GlueAvroDecoder<C> {
        fn clone(&self) -> Self {
            Self {
                registry: Arc::clone(&self.registry),
                reader_schema: Arc::clone(&self.reader_schema),
            }
        }
    }

    impl<C: GlueSchemaRegistryClient> GlueAvroDecoder<C> {
        /// Build a decoder against `registry`.
        ///
        /// # Errors
        ///
        /// [`Error::ConfigError`] if the reader schema fails to parse.
        pub fn new(registry: Arc<C>) -> Result<Self> {
            let reader_schema = Schema::parse_str(crate::codec::avro::AVRO_SCHEMA)
                .map_err(|error| Error::ConfigError(format!("reader schema parse: {error}")))?;
            Ok(Self {
                registry,
                reader_schema: Arc::new(reader_schema),
            })
        }

        /// Decode a Glue-framed Avro message to an [`Event`].
        ///
        /// The writer schema is fetched by the header's version UUID and used for
        /// resolution, so a message written under an older compatible schema decodes
        /// correctly rather than being read positionally against the current one.
        ///
        /// # Errors
        ///
        /// - A wire-format error for a malformed header. **Permanent** — the same bytes
        ///   will never decode — so it classifies `Terminal` rather than inviting a retry
        ///   loop.
        /// - A classified source error when Glue is unreachable (`Transient`) or the
        ///   schema version is unknown (`Terminal`).
        pub async fn decode(&self, bytes: &[u8]) -> Result<Event> {
            let (version_id, payload) = decode_glue_wire_format(bytes)
                .map_err(|error| super::map_registry_error("glue wire format", error))?;

            let schema = self
                .registry
                .get_schema_by_version_id(version_id)
                .await
                .map_err(|error| super::map_registry_error("glue schema lookup", error))?;

            let writer_schema = Schema::parse_str(&schema.schema_definition).map_err(|error| {
                Error::SerializationError(format!(
                    "schema for Glue version id {version_id:?} is not valid Avro: {error}"
                ))
            })?;

            let mut reader: &[u8] = payload.as_ref();
            let value = apache_avro::from_avro_datum(
                &writer_schema,
                &mut reader,
                Some(&self.reader_schema),
            )
            .map_err(|error| Error::SerializationError(format!("glue avro decode: {error}")))?;

            crate::codec::avro::avro_value_to_event(&value)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::core::{Operation, SourceMetadata};
        use std::collections::HashMap;
        use std::sync::Mutex;

        /// In-memory stand-in for AWS Glue.
        ///
        /// Glue has no self-hostable implementation, so there is no container to point a
        /// live suite at — the gap FINDINGS.md states explicitly. What *is* verifiable
        /// without AWS is everything rustcdc owns: the Avro conversion, the 18-byte
        /// framing, the compression byte, schema-version identity, and the round trip.
        /// Only the AWS transport itself stays unexercised, and that belongs to `schemreg`.
        #[derive(Default)]
        struct FakeGlue {
            by_id: Mutex<HashMap<[u8; 16], Arc<GlueSchema>>>,
            by_name: Mutex<HashMap<String, GlueSchemaVersionId>>,
            registrations: Mutex<Vec<String>>,
        }

        impl GlueSchemaRegistryClient for FakeGlue {
            async fn get_schema_by_version_id(
                &self,
                id: GlueSchemaVersionId,
            ) -> ::schemreg::Result<Arc<GlueSchema>> {
                self.by_id
                    .lock()
                    .expect("lock")
                    .get(id.as_bytes())
                    .cloned()
                    // 40403 is the Confluent "schema not found" code `is_not_found`
                    // recognises, which is what makes this classify Terminal.
                    .ok_or_else(|| {
                        ::schemreg::SchemaRegError::api(40403, "unknown Glue schema version")
                    })
            }

            async fn register_schema<'a>(
                &'a self,
                schema_name: &'a str,
                schema: &'a str,
                data_format: GlueDataFormat,
            ) -> ::schemreg::Result<GlueSchemaVersionId> {
                self.registrations
                    .lock()
                    .expect("lock")
                    .push(schema_name.to_string());

                // Idempotent for identical content, like the real service.
                if let Some(existing) = self.by_name.lock().expect("lock").get(schema_name) {
                    return Ok(*existing);
                }

                let mut uuid = [0u8; 16];
                let index = self.by_id.lock().expect("lock").len() as u8;
                uuid[15] = index + 1;
                let id = GlueSchemaVersionId::from_bytes(uuid);

                self.by_id
                    .lock()
                    .expect("lock")
                    .insert(uuid, Arc::new(GlueSchema::new(id, data_format, schema)));
                self.by_name
                    .lock()
                    .expect("lock")
                    .insert(schema_name.to_string(), id);
                Ok(id)
            }
        }

        fn event() -> Event {
            Event::builder("users", Operation::Insert)
                .source(SourceMetadata::new("postgres", "0/1", 1_700_000_000))
                .schema("public")
                .after(serde_json::json!({ "id": 7, "email": "a@example.com" }))
                .primary_key(["id"])
                .ts(1_700_000_000)
                .build()
        }

        #[tokio::test]
        async fn a_glue_framed_event_round_trips() {
            let registry = Arc::new(FakeGlue::default());
            let encoder =
                GlueAvroEncoder::new(Arc::clone(&registry), GlueAvroConfig::new("cdc-events"))
                    .await
                    .expect("encoder");

            let input = event();
            let framed = encoder.encode_event(&input).expect("encode");

            // 18-byte header: version, compression, 16-byte UUID — not Confluent's 5.
            assert_eq!(framed.bytes[0], 0x03, "Glue header version byte");
            assert_eq!(framed.bytes[1], 0x00, "no compression");
            assert_eq!(
                &framed.bytes[2..18],
                encoder.value_schema_version_id().as_bytes(),
                "the header must carry the id the registry assigned"
            );
            assert_eq!(framed.content_type, "application/vnd.aws-glue+avro");

            let decoded = GlueAvroDecoder::new(registry)
                .expect("decoder")
                .decode(&framed.bytes)
                .await
                .expect("decode");

            // Avro is positional and untagged: "decode succeeded" is not the assertion
            // that matters, "the payload came back unchanged" is.
            assert_eq!(decoded.table, input.table);
            assert_eq!(decoded.op, input.op);
            assert_eq!(decoded.after, input.after);
            assert_eq!(decoded.source.offset, input.source.offset);
        }

        #[tokio::test]
        async fn zlib_compression_round_trips_and_sets_the_header_byte() {
            let registry = Arc::new(FakeGlue::default());
            let config = GlueAvroConfig::new("cdc-events").with_compression(GlueCompression::Zlib);
            let encoder = GlueAvroEncoder::new(Arc::clone(&registry), config)
                .await
                .expect("encoder");

            let framed = encoder.encode_event(&event()).expect("encode");
            assert_eq!(framed.bytes[1], 0x05, "ZLIB compression byte");

            let decoded = GlueAvroDecoder::new(registry)
                .expect("decoder")
                .decode(&framed.bytes)
                .await
                .expect("decode");
            assert_eq!(decoded.after, event().after);
        }

        #[tokio::test]
        async fn key_and_value_get_distinct_schema_versions() {
            let registry = Arc::new(FakeGlue::default());
            let encoder =
                GlueAvroEncoder::new(Arc::clone(&registry), GlueAvroConfig::new("cdc-events"))
                    .await
                    .expect("encoder");

            assert_ne!(
                encoder.value_schema_version_id().as_bytes(),
                encoder.key_schema_version_id().as_bytes(),
                "key and value are different schemas under different names"
            );
            assert_eq!(
                registry.registrations.lock().expect("lock").as_slice(),
                ["cdc-events", "cdc-events-key"],
                "the key schema name defaults to the `-key` suffix"
            );

            let key = encoder
                .encode_event_key(&event())
                .expect("encode key")
                .expect("keyed event");
            assert_eq!(&key[2..18], encoder.key_schema_version_id().as_bytes());

            // And the payload itself decodes to the primary key. The union branch index
            // is hand-written (1 = the `string` arm of `["null","string"]`); a wrong index
            // produces bytes that frame correctly and decode to the wrong branch, so the
            // header assertion above would not catch it.
            let key_schema = Schema::parse_str(super::super::KEY_AVRO_SCHEMA).expect("schema");
            let mut payload: &[u8] = &key[18..];
            let decoded = apache_avro::from_avro_datum(&key_schema, &mut payload, None)
                .expect("key payload decodes");
            let apache_avro::types::Value::Record(fields) = decoded else {
                panic!("key payload must be a record");
            };
            let (name, value) = &fields[0];
            assert_eq!(name, "key");
            let apache_avro::types::Value::Union(index, inner) = value else {
                panic!("key field must be a union");
            };
            assert_eq!(*index, 1, "a keyed event takes the `string` branch");
            assert_eq!(
                **inner,
                apache_avro::types::Value::String(r#"{"id":7}"#.to_string())
            );
        }

        #[tokio::test]
        async fn a_keyless_event_produces_no_key() {
            // So a producer round-robins it rather than collapsing every keyless event
            // onto one partition.
            let registry = Arc::new(FakeGlue::default());
            let encoder =
                GlueAvroEncoder::new(Arc::clone(&registry), GlueAvroConfig::new("cdc-events"))
                    .await
                    .expect("encoder");

            let keyless = Event::builder("users", Operation::Truncate)
                .source(SourceMetadata::new("postgres", "0/9", 1))
                .ts(1)
                .build();
            assert!(encoder
                .encode_event_key(&keyless)
                .expect("encode")
                .is_none());
        }

        #[tokio::test]
        async fn glue_framing_is_detected_as_glue_not_confluent() {
            // The two framings must not be confused: a Confluent consumer reading Glue
            // bytes would take the compression byte and the first three UUID bytes as a
            // schema id.
            let registry = Arc::new(FakeGlue::default());
            let encoder = GlueAvroEncoder::new(registry, GlueAvroConfig::new("cdc-events"))
                .await
                .expect("encoder");
            let framed = encoder.encode_event(&event()).expect("encode");

            let detected = format!("{:?}", super::super::detect_wire_format(&framed.bytes));
            assert!(
                detected.to_lowercase().contains("glue"),
                "Glue framing must be detected as Glue, got {detected}"
            );
        }

        #[tokio::test]
        async fn a_malformed_header_is_terminal_not_retryable() {
            // These exact bytes will never decode, so classifying them retryable makes an
            // embedder following the crate's own guidance spin forever.
            use crate::core::ErrorKind;

            let registry = Arc::new(FakeGlue::default());
            let decoder = GlueAvroDecoder::new(registry).expect("decoder");
            let error = decoder
                .decode(&[0x00, 0x01, 0x02])
                .await
                .expect_err("a truncated header must be rejected");
            assert_eq!(error.kind(), ErrorKind::Terminal);
        }

        #[tokio::test]
        async fn an_unknown_schema_version_is_terminal() {
            let registry = Arc::new(FakeGlue::default());
            let encoder =
                GlueAvroEncoder::new(Arc::clone(&registry), GlueAvroConfig::new("cdc-events"))
                    .await
                    .expect("encoder");
            let mut framed = encoder.encode_event(&event()).expect("encode").bytes;
            // Point the header at a version the registry has never issued.
            framed[17] = 0xFF;

            let error = GlueAvroDecoder::new(registry)
                .expect("decoder")
                .decode(&framed)
                .await
                .expect_err("an unknown schema version must fail");
            assert_eq!(error.kind(), crate::core::ErrorKind::Terminal);
        }
    }
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

    // ─── Apicurio → SchemaRegistryConfig carry-through ───────────────────────

    #[cfg(feature = "apicurio")]
    #[test]
    fn apicurio_config_carries_every_overlapping_field_over() {
        // Set every field to a non-default value, then assert each one survives. The
        // conversion silently dropped auth, both timeouts, max_cache_entries and
        // retry_policy through 0.8 — a caller who set a retry policy got the
        // `SchemaRegistryConfig::new` default with no indication it had been discarded.
        let apicurio = ApicurioRegistryConfig::new("http://apicurio:8080/", "orders")
            .with_strategy(SubjectNameStrategy::RecordName)
            .with_auth(SchemaRegistryAuth::Basic {
                username: "u".to_string(),
                password: "p".to_string(),
            })
            .with_auto_register(false)
            .with_request_timeout_ms(11_000)
            .with_connect_timeout_ms(2_500)
            .with_max_cache_entries(777)
            .with_pool_max_idle_per_host(9)
            .with_references(vec![SchemaReference::new(
                "com.example.Address",
                "com.example.Address",
                3,
            )])
            .with_retry_policy(RetryPolicy::none());

        let derived = apicurio.as_schema_registry_config();

        assert_eq!(derived.url, "http://apicurio:8080");
        assert_eq!(derived.topic, "orders");
        assert_eq!(derived.strategy, SubjectNameStrategy::RecordName);
        assert!(
            matches!(derived.auth, Some(SchemaRegistryAuth::Basic { .. })),
            "auth must carry over"
        );
        assert!(!derived.auto_register);
        assert_eq!(derived.request_timeout_ms, Some(11_000));
        assert_eq!(derived.connect_timeout_ms, Some(2_500));
        assert_eq!(derived.max_cache_entries, Some(777));
        assert_eq!(derived.pool_max_idle_per_host, Some(9));
        assert_eq!(derived.references.len(), 1);
        assert_eq!(derived.references[0].name, "com.example.Address");
        assert_eq!(
            derived.retry_policy.max_retries_value(),
            RetryPolicy::none().max_retries_value(),
            "retry policy must carry over, not reset to the default"
        );
        assert_ne!(
            derived.retry_policy.max_retries_value(),
            RetryPolicy::default().max_retries_value(),
            "the assertion above is only meaningful if none() differs from the default"
        );
        // Not representable on Apicurio's native v3 API; documented on the method.
        assert!(!derived.normalize_schemas);
    }

    #[cfg(feature = "apicurio")]
    #[test]
    fn apicurio_config_defaults_match_the_confluent_defaults() {
        let derived =
            ApicurioRegistryConfig::new("http://apicurio:8080", "t").as_schema_registry_config();
        let baseline = SchemaRegistryConfig::new("http://apicurio:8080", "t");

        assert_eq!(derived.strategy, baseline.strategy);
        assert_eq!(derived.auto_register, baseline.auto_register);
        assert_eq!(derived.request_timeout_ms, baseline.request_timeout_ms);
        assert_eq!(derived.connect_timeout_ms, baseline.connect_timeout_ms);
        assert_eq!(derived.max_cache_entries, baseline.max_cache_entries);
        assert_eq!(
            derived.pool_max_idle_per_host,
            baseline.pool_max_idle_per_host
        );
        assert!(derived.references.is_empty());
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

    #[tokio::test]
    async fn json_schema_encoder_constructs_without_registry_call() {
        // With `auto_register` on (the default) construction must still make no network
        // call — subject resolution stays lazy, and only `auto_register = false` trades
        // that for a startup check. `http://localhost:8081` is not listening.
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "orders");
        let registry = Arc::new(cfg.build().unwrap());
        let encoder = ConfluentJsonSchemaEncoder::new(registry, &cfg).await;
        assert!(
            encoder.is_ok(),
            "encoder construction must not require a live registry when auto_register is on"
        );
        let encoder = encoder.unwrap();
        assert_eq!(encoder.topic(), "orders");
    }

    #[tokio::test]
    async fn json_schema_encoder_with_auto_register_off_requires_a_reachable_registry() {
        // `auto_register = false` means "require the schemas to already exist". Through
        // 0.8 this encoder ignored it entirely and registered anyway; the setting now
        // fails at construction against an unreachable registry rather than buying
        // nothing.
        let cfg = SchemaRegistryConfig::new("http://127.0.0.1:1", "orders")
            .with_auto_register(false)
            .with_connect_timeout_ms(500)
            .with_request_timeout_ms(500);
        let registry = Arc::new(cfg.build().unwrap());
        let error = ConfluentJsonSchemaEncoder::new(registry, &cfg)
            .await
            .expect_err("auto_register = false must verify the subjects exist");
        assert!(
            error.to_string().contains("auto_register"),
            "the error must name the setting that caused the check: {error}"
        );
    }

    #[tokio::test]
    async fn protobuf_encoder_with_auto_register_off_requires_a_reachable_registry() {
        let cfg = SchemaRegistryConfig::new("http://127.0.0.1:1", "orders")
            .with_auto_register(false)
            .with_connect_timeout_ms(500)
            .with_request_timeout_ms(500);
        let registry = Arc::new(cfg.build().unwrap());
        let error = ConfluentProtobufEncoder::new(registry, &cfg)
            .await
            .expect_err("auto_register = false must verify the subjects exist");
        assert!(error.to_string().contains("auto_register"));
    }

    #[tokio::test]
    async fn json_schema_encoder_without_validation_constructs() {
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "orders");
        let registry = Arc::new(cfg.build().unwrap());
        let encoder = ConfluentJsonSchemaEncoder::without_validation(registry, &cfg).await;
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

    #[tokio::test]
    async fn json_schema_encoder_is_clone() {
        let cfg = SchemaRegistryConfig::new("http://localhost:8081", "t");
        let registry = Arc::new(cfg.build().unwrap());
        let encoder = ConfluentJsonSchemaEncoder::new(registry, &cfg)
            .await
            .unwrap();
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
            SchemaType::Avro,
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
            SchemaType::Avro,
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
        let error = assert_registry_schema_matches(
            "cdc-events-value",
            "{not avro",
            KEY_AVRO_SCHEMA,
            SchemaType::Avro,
        )
        .expect_err("invalid Avro must be rejected");
        assert!(error.to_string().contains("not valid Avro"));
    }

    #[test]
    fn json_schema_identity_ignores_formatting_but_not_structure() {
        let reformatted: String = serde_json::from_str::<serde_json::Value>(EVENT_JSON_SCHEMA)
            .map(|value| serde_json::to_string_pretty(&value).expect("re-serialise"))
            .expect("EVENT_JSON_SCHEMA parses");
        assert_registry_schema_matches(
            "cdc-json-value",
            &reformatted,
            EVENT_JSON_SCHEMA,
            SchemaType::Json,
        )
        .expect("a formatting-only difference must be accepted");

        // Key order specifically: a registry that stores the schema with its keys in a
        // different order has not changed the schema, and rejecting that would fail
        // startup against a perfectly correct registry.
        let reordered = r#"{"required":["b","a"],"type":"object",
                            "properties":{"b":{"type":"string"},"a":{"type":"integer"}}}"#;
        let original = r#"{"type":"object",
                           "properties":{"a":{"type":"integer"},"b":{"type":"string"}},
                           "required":["b","a"]}"#;
        assert_registry_schema_matches("cdc-json-value", reordered, original, SchemaType::Json)
            .expect("reordered object keys are the same schema");

        // Array order, by contrast, is significant in JSON Schema and must not be
        // normalised away.
        let reordered_array = r#"{"type":"object","required":["a","b"]}"#;
        assert_registry_schema_matches(
            "cdc-json-value",
            reordered_array,
            r#"{"type":"object","required":["b","a"]}"#,
            SchemaType::Json,
        )
        .expect_err("array element order is part of the schema");

        assert_registry_schema_matches(
            "cdc-json-value",
            KEY_JSON_SCHEMA,
            EVENT_JSON_SCHEMA,
            SchemaType::Json,
        )
        .expect_err("a different schema must be rejected");
    }

    #[test]
    fn protobuf_schema_identity_ignores_comments_but_not_structure() {
        // The registry stores `.proto` source, so a reflowed comment must not read as a
        // schema change — but a changed field must.
        let stripped: String = KEY_PROTO_SCHEMA
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_ne!(stripped, KEY_PROTO_SCHEMA, "the test input must differ");
        assert_registry_schema_matches(
            "cdc-proto-key",
            &stripped,
            KEY_PROTO_SCHEMA,
            SchemaType::Protobuf,
        )
        .expect("comments carry no wire semantics");

        assert_registry_schema_matches(
            "cdc-proto-key",
            EVENT_PROTO_SOURCE,
            KEY_PROTO_SCHEMA,
            SchemaType::Protobuf,
        )
        .expect_err("a different message must be rejected");
    }

    #[test]
    fn preflight_derives_the_subject_names_each_format_actually_uses() {
        // Protobuf subjects come from the message's fully-qualified name, Avro and JSON
        // from the record name. Checking the Avro names for a Protobuf pipeline — which is
        // what 0.8 did unconditionally — looks at subjects nothing writes to.
        let strategy = SubjectNameStrategy::TopicRecordName;
        assert_eq!(
            strategy
                .subject_name("cdc", Some(EVENT_PROTO_FULL_NAME), EncodeTarget::Value)
                .unwrap(),
            "cdc-rustcdc.Event"
        );
        assert_eq!(
            strategy
                .subject_name("cdc", Some("io.rustcdc.Event"), EncodeTarget::Value)
                .unwrap(),
            "cdc-io.rustcdc.Event"
        );
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
