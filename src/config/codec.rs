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

    /// Avro binary framed with the Confluent 5-byte wire format
    /// (`0x00 <4-byte schema-id>`).
    ///
    /// Requires a `registry` sub-table pointing at a Confluent-compatible
    /// schema registry (Confluent Platform/Cloud, Apicurio, Karapace).
    AvroConfluent(AvroConfluentCodecConfig),

    /// CloudEvents 1.0 JSON envelope wrapping the CDC payload.
    ///
    /// Suitable for HTTP sinks targeting AWS EventBridge, Azure Event Grid,
    /// Google Cloud Eventarc, or Knative Eventing.
    CloudEvents,
}

impl CodecConfig {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            CodecConfig::Json | CodecConfig::CloudEvents => Ok(()),
            CodecConfig::AvroConfluent(cfg) => cfg.validate(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Avro / Confluent codec config
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for Confluent wire-format Avro encoding.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct AvroConfluentCodecConfig {
    /// Confluent-compatible schema registry connection parameters.
    pub registry: ConfluentRegistryConfig,
}

impl AvroConfluentCodecConfig {
    pub fn validate(&self) -> Result<(), String> {
        self.registry.validate()
    }
}
