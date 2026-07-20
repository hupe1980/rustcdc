//! Wire-format encoding for CDC events.
//!
//! Delegates to rustcdc's codec layer.  Supported codecs:
//!
//! * **Json** — `rustcdc::codec::JsonCodec` (primary-key message key).
//! * **CloudEvents** — CloudEvents 1.0 JSON via `rustcdc::codec::CloudEventsEncoder`
//!   (fully spec-compliant CNCF CloudEvents 1.0, `io.rustcdc.change.*` event types,
//!   CDC extension attributes, RFC 3339 timestamps).
//! * **AvroConfluent** — Confluent wire-format Avro via `rustcdc::ConfluentAvroEncoder`.
//!
//! All codecs implement `rustcdc::codec::Codec` and are returned as `BoxedCodec`.

use rustcdc::codec::{BoxedCodec, CloudEventsEncoder, Codec, EncoderCodec};
use rustcdc::{
    ConfluentAvroEncoder, SchemaRegistryAuth, SchemaRegistryConfig, SubjectNameStrategy,
};

pub use crate::config::codec::{AvroConfluentCodecConfig, CodecConfig};
use crate::config::registry::SubjectNameStrategy as ConfigStrategy;

// ─── Public re-export ─────────────────────────────────────────────────────────

/// Type-erased codec used by [`crate::sink::SinkBinding`].
pub use rustcdc::codec::BoxedCodec as BuiltCodec;

// ─── Build helper ─────────────────────────────────────────────────────────────

/// Build a `BoxedCodec` from an optional sink codec configuration.
///
/// `topic` is used by the Confluent Avro codec for subject-name strategies;
/// it is ignored for JSON and CloudEvents.
pub async fn build(config: Option<&CodecConfig>, topic: &str) -> Result<BoxedCodec, String> {
    match config {
        None | Some(CodecConfig::Json) => Ok(rustcdc::codec::json::JsonCodec::default().boxed()),
        Some(CodecConfig::CloudEvents) => {
            // Use rustcdc's CNCF-compliant CloudEvents 1.0 encoder.
            // Event type: \ + CDC extension attributes
            // (cdcop, cdctable, cdcschema, cdcsource, cdcoffset).
            Ok(EncoderCodec::new(CloudEventsEncoder::default()).boxed())
        }
        Some(CodecConfig::AvroConfluent(cfg)) => {
            let encoder = build_confluent_avro_encoder(cfg, topic).await?;
            Ok(EncoderCodec::new(encoder).boxed())
        }
    }
}

// ─── Confluent Avro builder ───────────────────────────────────────────────────

async fn build_confluent_avro_encoder(
    cfg: &AvroConfluentCodecConfig,
    topic: &str,
) -> Result<ConfluentAvroEncoder, String> {
    let (username, password) = cfg.registry.resolve_credentials()?;

    let auth = match (username, password) {
        (Some(u), Some(p)) => Some(SchemaRegistryAuth::Basic {
            username: u,
            password: p,
        }),
        _ => None,
    };

    let strategy = match cfg.registry.subject_name_strategy {
        ConfigStrategy::TopicName => SubjectNameStrategy::TopicName,
        ConfigStrategy::RecordName => SubjectNameStrategy::RecordName,
        ConfigStrategy::TopicRecordName => SubjectNameStrategy::TopicRecordName,
    };

    let sr_config = SchemaRegistryConfig {
        url: cfg.registry.url.trim_end_matches('/').to_owned(),
        topic: topic.to_owned(),
        strategy,
        auth,
        auto_register: true,
        request_timeout_ms: if cfg.registry.request_timeout_ms > 0 {
            Some(cfg.registry.request_timeout_ms)
        } else {
            None
        },
        max_cache_entries: None,
        connect_timeout_ms: None,
        normalize_schemas: false,
        pool_max_idle_per_host: None,
    };

    let registry = sr_config
        .build()
        .map_err(|e| format!("failed to build schema registry client: {e}"))?;

    ConfluentAvroEncoder::new(&registry, &sr_config)
        .await
        .map_err(|e| format!("failed to build Confluent Avro encoder: {e}"))
}
