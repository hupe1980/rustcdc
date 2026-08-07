use serde::{Deserialize, Serialize};

use super::registry::ConfluentRegistryConfig;

// ─────────────────────────────────────────────────────────────────────────────
// Codec configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Per-sink output serialisation codec.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CodecConfig {
    /// Raw JSON key + value (default).  No schema registry required.
    #[default]
    Json,

    /// Raw JSON, pretty-printed.
    ///
    /// Human-readable at the cost of size; intended for debugging an HTTP endpoint or
    /// a file sink you intend to read by eye, not for production throughput.
    JsonPretty,

    /// Plain Avro binary — **no** registry, no wire-format header.
    ///
    /// The reader must already hold `io.rustcdc.Event`'s schema out of band. Prefer
    /// `avro_confluent` whenever a registry is available: bare Avro is positional and
    /// untagged, so a reader with the wrong schema gets shifted fields rather than an
    /// error.
    Avro,

    /// Plain Protobuf binary — **no** registry, no wire-format header.
    ///
    /// `before` / `after` carry UTF-8 JSON in `bytes` fields, so the envelope is typed
    /// while the row payload stays schemaless.
    Protobuf,

    /// Avro binary framed with the Confluent 5-byte wire format
    /// (`0x00 <4-byte schema-id>`).
    ///
    /// Requires a `registry` sub-table pointing at a Confluent-compatible
    /// schema registry (Confluent Platform/Cloud, Apicurio, Karapace, Redpanda).
    AvroConfluent(RegistryCodecConfig),

    /// JSON framed with the Confluent 5-byte wire format.
    ///
    /// Same framing as `avro_confluent`, but the payload stays JSON — readable by any
    /// consumer that strips the header, and validated against rustcdc's published JSON
    /// Schema on encode unless `validate = false`.
    JsonSchemaConfluent(JsonSchemaCodecConfig),

    /// Protobuf framed with the Confluent wire format.
    ///
    /// Confluent Protobuf does not use the plain 5-byte header — it carries a
    /// message-index path locating the message inside its `.proto` file. rustcdc derives
    /// that from the compiled descriptor, so it cannot drift from the schema it registers.
    ProtobufConfluent(RegistryCodecConfig),

    /// Avro framed for the **AWS Glue Schema Registry**.
    ///
    /// Glue is not Confluent-compatible: an 18-byte header (`0x03`, a compression byte
    /// and a 16-byte schema-version UUID) rather than Confluent's 5-byte magic + integer
    /// id. The payload is the same `io.rustcdc.Event` Avro envelope, so a consumer that
    /// already decodes rustcdc's Avro events needs only the framing changed.
    ///
    /// Requires the `glue` cargo feature — on in the published container image, opt-in
    /// for a source build, because it pulls the AWS SDK.
    GlueAvro(GlueCodecConfig),

    /// CloudEvents 1.0 JSON envelope wrapping the CDC payload.
    ///
    /// Suitable for HTTP sinks targeting AWS EventBridge, Azure Event Grid,
    /// Google Cloud Eventarc, or Knative Eventing.
    CloudEvents,
}

/// Configuration for the AWS Glue Schema Registry codec.
///
/// Credentials and region come from the standard AWS chain (environment,
/// `~/.aws/credentials`, instance/task role, EKS web identity) — the same chain the
/// Kafka sink's `aws_msk_iam` mechanism uses, so one IAM identity covers both.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct GlueCodecConfig {
    /// Glue registry name (default: `default-registry`).
    #[serde(default = "default_glue_registry_name")]
    pub registry_name: String,

    /// Glue schema name for the event envelope, e.g. `cdc-events`.
    pub schema_name: String,

    /// Glue schema name for the primary-key envelope.
    ///
    /// Defaults to `{schema_name}-key`, mirroring the Confluent `-key` suffix.
    #[serde(default)]
    pub key_schema_name: Option<String>,

    /// Compress payloads with ZLIB (default: `false`).
    ///
    /// Glue's header carries a compression byte; Confluent's does not.
    #[serde(default)]
    pub zlib_compression: bool,

    /// Register the schemas on first use (default: `true`).
    ///
    /// Glue's `register_schema` is idempotent for identical content, so leaving this on
    /// is safe. Unlike the Confluent path there is **no** `false`: `schemreg`'s Glue
    /// client has no lookup-by-name API, so the setting could only have been accepted
    /// and ignored — which is the failure mode this whole config surface avoids.
    #[serde(default = "default_true")]
    pub auto_register: bool,
}

fn default_glue_registry_name() -> String {
    "default-registry".to_string()
}

impl GlueCodecConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_name.trim().is_empty() {
            return Err("codec.schema_name must not be empty for the glue_avro codec".to_string());
        }
        if self.registry_name.trim().is_empty() {
            return Err("codec.registry_name must not be empty".to_string());
        }
        if self
            .key_schema_name
            .as_deref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err("codec.key_schema_name must not be empty when set".to_string());
        }
        if !self.auto_register {
            return Err(
                "codec.auto_register = false is not supported for the glue_avro codec: \
                 the Glue client has no lookup-by-name API, so the setting could only be \
                 accepted and ignored. Register the schemas out of band and leave it true \
                 — Glue's register_schema is idempotent for identical content."
                    .to_string(),
            );
        }
        Ok(())
    }
}

impl CodecConfig {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            CodecConfig::Json
            | CodecConfig::JsonPretty
            | CodecConfig::Avro
            | CodecConfig::Protobuf
            | CodecConfig::CloudEvents => Ok(()),
            CodecConfig::AvroConfluent(cfg) | CodecConfig::ProtobufConfluent(cfg) => cfg.validate(),
            CodecConfig::JsonSchemaConfluent(cfg) => cfg.validate(),
            CodecConfig::GlueAvro(cfg) => cfg.validate(),
        }
    }

    /// Human-readable codec name, used in logs and the admin status payload.
    pub fn label(&self) -> &'static str {
        match self {
            CodecConfig::Json => "json",
            CodecConfig::JsonPretty => "json_pretty",
            CodecConfig::Avro => "avro",
            CodecConfig::Protobuf => "protobuf",
            CodecConfig::AvroConfluent(_) => "avro_confluent",
            CodecConfig::JsonSchemaConfluent(_) => "json_schema_confluent",
            CodecConfig::ProtobufConfluent(_) => "protobuf_confluent",
            CodecConfig::GlueAvro(_) => "glue_avro",
            CodecConfig::CloudEvents => "cloud_events",
        }
    }

    /// The registry binding this codec needs, if any.
    pub fn binding(&self) -> Option<&RegistryBinding> {
        match self {
            CodecConfig::AvroConfluent(cfg) | CodecConfig::ProtobufConfluent(cfg) => {
                Some(&cfg.binding)
            }
            CodecConfig::JsonSchemaConfluent(cfg) => Some(&cfg.binding),
            _ => None,
        }
    }

    /// Mutable access to the registry binding, for the loader's `registry_ref`
    /// resolution pass.
    pub fn binding_mut(&mut self) -> Option<&mut RegistryBinding> {
        match self {
            CodecConfig::AvroConfluent(cfg) | CodecConfig::ProtobufConfluent(cfg) => {
                Some(&mut cfg.binding)
            }
            CodecConfig::JsonSchemaConfluent(cfg) => Some(&mut cfg.binding),
            _ => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Registry-backed codec configs
// ─────────────────────────────────────────────────────────────────────────────

/// How a registry-backed codec names its registry.
///
/// Either inline under `[…codec.registry]`, or by name against the shared
/// `[registries.<name>]` pool. Naming the pool keeps one URL, one credential pair and
/// one retry policy for every sink that talks to the same registry — repeating the
/// block per sink is how two of them drift apart.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
pub struct RegistryBinding {
    /// Inline registry connection parameters.
    ///
    /// Populated by the loader when `registry_ref` is used, so everything downstream
    /// sees a resolved registry regardless of how it was written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry: Option<ConfluentRegistryConfig>,

    /// Name of an entry in the top-level `[registries.<name>]` table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry_ref: Option<String>,
}

impl RegistryBinding {
    /// The resolved registry, or an error naming what is missing.
    ///
    /// After `config::load` this always succeeds: the loader inlines every
    /// `registry_ref` and rejects the config if one cannot be resolved.
    pub fn resolved(&self) -> Result<&ConfluentRegistryConfig, String> {
        self.registry
            .as_ref()
            .ok_or_else(|| match &self.registry_ref {
                Some(name) => format!(
                    "codec registry_ref = \"{name}\" was not resolved against [registries.{name}]"
                ),
                None => "a registry-backed codec needs either an inline [.registry] table \
                     or registry_ref = \"<name>\""
                    .to_string(),
            })
    }

    pub fn validate(&self) -> Result<(), String> {
        match (&self.registry, &self.registry_ref) {
            (Some(_), Some(name)) => Err(format!(
                "codec declares both an inline [.registry] table and registry_ref = \
                 \"{name}\"; pick one"
            )),
            (None, None) => Err(
                "a registry-backed codec needs either an inline [.registry] \
                                 table or registry_ref = \"<name>\""
                    .to_string(),
            ),
            (Some(registry), None) => registry.validate(),
            // The name is checked against the pool by the loader, which is the only
            // place that can see the pool.
            (None, Some(_)) => Ok(()),
        }
    }
}

/// Configuration for a registry-backed codec that needs nothing beyond the registry.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
pub struct RegistryCodecConfig {
    /// Where the schema registry connection comes from.
    #[serde(flatten)]
    pub binding: RegistryBinding,
}

impl RegistryCodecConfig {
    pub fn validate(&self) -> Result<(), String> {
        self.binding.validate()
    }
}

/// Configuration for the Confluent JSON Schema codec.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct JsonSchemaCodecConfig {
    /// Where the schema registry connection comes from.
    #[serde(flatten)]
    pub binding: RegistryBinding,

    /// Validate every event against rustcdc's published JSON Schema before encoding
    /// (default: `true`).
    ///
    /// Turning this off trades the guarantee that what leaves this process matches the
    /// schema its header advertises for throughput. Only do so when the transform
    /// pipeline is trusted end to end.
    #[serde(default = "default_true")]
    pub validate: bool,
}

fn default_true() -> bool {
    true
}

impl JsonSchemaCodecConfig {
    pub fn validate(&self) -> Result<(), String> {
        self.binding.validate()
    }
}

/// Backwards-compatible alias for the Avro/Confluent codec configuration.
pub type AvroConfluentCodecConfig = RegistryCodecConfig;
