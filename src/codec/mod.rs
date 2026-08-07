//! Wire-format encoding for CDC events.
//!
//! Delegates to rustcdc's codec layer. Supported codecs:
//!
//! | `type` | Framing | Registry |
//! |---|---|---|
//! | `json` (default) | raw JSON, primary-key message key | — |
//! | `json_pretty` | raw JSON, indented | — |
//! | `avro` | plain Avro binary | — |
//! | `protobuf` | plain protobuf | — |
//! | `avro_confluent` | Confluent 5-byte header + Avro | required |
//! | `json_schema_confluent` | Confluent 5-byte header + JSON | required |
//! | `protobuf_confluent` | Confluent header + message-index path + protobuf | required |
//! | `glue_avro` | Glue 18-byte header + Avro | AWS Glue (`glue` feature) |
//! | `cloud_events` | CloudEvents 1.0 JSON envelope | — |
//!
//! Confluent-framed codecs speak either the Confluent Schema Registry API or Apicurio
//! Registry's native v3 API (`registry.flavor`); both emit Confluent framing, so a
//! consumer does not need to know which one produced the message. AWS Glue is a
//! separate framing (`0x03`, a compression byte and a 16-byte version UUID) and a
//! separate config block.
//!
//! Every codec is returned as a [`BuiltCodec`] — rustcdc's `BoxedAsyncCodec`, which
//! spans the synchronous encoders and the two that resolve subjects lazily.

use std::sync::Arc;

use rustcdc::codec::{
    AsyncCodec, BoxedAsyncCodec, CloudEventsEncoder, ConfluentProtobufEncoder, EncoderCodec,
};
use rustcdc::{
    preflight_schema_registry, ApicurioRegistryConfig, AvroEncoder, ConfluentAvroEncoder,
    ConfluentJsonSchemaEncoder, DynSchemaRegistryClient, ProtobufEncoder, RetryPolicy,
    SchemaReference, SchemaRegistryAuth, SchemaRegistryConfig, SchemaType, SubjectNameStrategy,
};

pub use crate::config::codec::{CodecConfig, JsonSchemaCodecConfig, RegistryCodecConfig};
use crate::config::registry::{
    ConfluentRegistryConfig, RegistryFlavor, ResolvedRegistryAuth,
    SubjectNameStrategy as ConfigStrategy,
};

// ─── BuiltCodec ───────────────────────────────────────────────────────────────

/// A registry client erased to one concrete type.
///
/// The Confluent and Apicurio clients are different types, and the encoders are
/// generic over the client, so without erasure every encoder would exist twice in
/// the codec surface. `dyn DynSchemaRegistryClient` implements `SchemaRegistryClient`
/// in both directions, so `Arc<dyn …>` is accepted by the encoders unchanged.
type DynRegistry = Arc<dyn DynSchemaRegistryClient>;

/// The codec used by [`crate::sink::SinkBinding`].
///
/// One type for all eight codecs. The synchronous ones (JSON, CloudEvents, Avro,
/// Confluent Avro) reach it through rustcdc's blanket `impl<T: Codec> AsyncCodec for T`;
/// the two that resolve subjects lazily — Confluent JSON Schema and Protobuf, whose
/// first encode per subject is a registry round-trip — implement `AsyncCodec` directly.
/// Before rustcdc 0.9 this was a hand-rolled three-variant dispatch enum here.
pub type BuiltCodec = BoxedAsyncCodec;

// ─── Build helper ─────────────────────────────────────────────────────────────

/// Build a [`BuiltCodec`] from an optional sink codec configuration.
///
/// `topic` is used by the registry-backed codecs for subject-name strategies;
/// it is ignored by the registry-free ones.
pub async fn build(config: Option<&CodecConfig>, topic: &str) -> Result<BuiltCodec, String> {
    match config {
        None | Some(CodecConfig::Json) => {
            Ok(rustcdc::codec::json::JsonCodec::default().boxed_async())
        }
        Some(CodecConfig::JsonPretty) => {
            Ok(EncoderCodec::new(rustcdc::codec::JsonPrettyEncoder).boxed_async())
        }
        Some(CodecConfig::Avro) => {
            let encoder =
                AvroEncoder::new().map_err(|e| format!("failed to build the Avro encoder: {e}"))?;
            Ok(EncoderCodec::new(encoder).boxed_async())
        }
        Some(CodecConfig::Protobuf) => Ok(EncoderCodec::new(ProtobufEncoder).boxed_async()),
        Some(CodecConfig::CloudEvents) => {
            // rustcdc's CNCF-compliant CloudEvents 1.0 encoder: `io.rustcdc.change.*`
            // event types plus the CDC extension attributes (cdcop, cdctable,
            // cdcschema, cdcsource, cdcoffset).
            Ok(EncoderCodec::new(CloudEventsEncoder::default()).boxed_async())
        }
        Some(CodecConfig::AvroConfluent(cfg)) => {
            build_confluent_avro_codec(cfg.binding.resolved()?, topic).await
        }
        Some(CodecConfig::JsonSchemaConfluent(cfg)) => {
            build_confluent_json_schema_codec(cfg, topic).await
        }
        Some(CodecConfig::ProtobufConfluent(cfg)) => {
            build_confluent_protobuf_codec(cfg.binding.resolved()?, topic).await
        }
        Some(CodecConfig::GlueAvro(cfg)) => build_glue_avro_codec(cfg).await,
    }
}

/// Build the AWS Glue Avro codec.
///
/// Credentials and region come from the standard AWS chain, so one IAM identity covers
/// both this and the Kafka sink's `aws_msk_iam` mechanism.
#[cfg(feature = "glue")]
async fn build_glue_avro_codec(
    cfg: &crate::config::codec::GlueCodecConfig,
) -> Result<BuiltCodec, String> {
    use rustcdc::codec::glue::{AwsGlueSchemaRegistry, GlueCompression};
    use rustcdc::codec::{GlueAvroConfig, GlueAvroEncoder};

    let aws = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let registry = AwsGlueSchemaRegistry::builder(aws_sdk_glue::Client::new(&aws))
        .registry_name(cfg.registry_name.clone())
        .auto_register(cfg.auto_register)
        .build();

    let mut glue = GlueAvroConfig::new(cfg.schema_name.clone());
    if let Some(key) = &cfg.key_schema_name {
        glue = glue.with_key_schema_name(key.clone());
    }
    if cfg.zlib_compression {
        glue = glue.with_compression(GlueCompression::Zlib);
    }

    let encoder = GlueAvroEncoder::new(Arc::new(registry), glue)
        .await
        .map_err(|e| format!("failed to build the AWS Glue Avro encoder: {e}"))?;
    Ok(encoder.boxed_async())
}

/// Stub for builds without the `glue` feature.
///
/// Rejected at build time with the remedy named, rather than the codec silently
/// falling back to something else.
#[cfg(not(feature = "glue"))]
async fn build_glue_avro_codec(
    _cfg: &crate::config::codec::GlueCodecConfig,
) -> Result<BuiltCodec, String> {
    Err(
        "codec type = \"glue_avro\" requires the `glue` cargo feature, which is not \
         compiled into this binary. The published container image includes it; a source \
         build needs `cargo build --features glue`."
            .to_string(),
    )
}

// ─── Registry plumbing ────────────────────────────────────────────────────────

/// Translate the config-level registry settings into rustcdc's `SchemaRegistryConfig`.
fn schema_registry_config(
    cfg: &ConfluentRegistryConfig,
    topic: &str,
) -> Result<SchemaRegistryConfig, String> {
    let mut sr = SchemaRegistryConfig::new(cfg.url.as_str(), topic)
        .with_strategy(subject_strategy(cfg))
        .with_auto_register(cfg.auto_register)
        .with_references(schema_references(cfg))
        .with_retry_policy(retry_policy(cfg));

    if let Some(auth) = registry_auth(cfg)? {
        sr.auth = Some(auth);
    }
    sr.normalize_schemas = cfg.normalize_schemas;
    sr.request_timeout_ms = non_zero(cfg.request_timeout_ms);
    sr.connect_timeout_ms = non_zero(cfg.connect_timeout_ms);
    sr.max_cache_entries = non_zero_usize(cfg.max_cache_entries);
    sr.pool_max_idle_per_host = non_zero_usize(cfg.pool_max_idle_per_host);
    Ok(sr)
}

/// Translate the config-level registry settings into an Apicurio native-API config.
fn apicurio_registry_config(
    cfg: &ConfluentRegistryConfig,
    topic: &str,
) -> Result<ApicurioRegistryConfig, String> {
    let mut apicurio = ApicurioRegistryConfig::new(cfg.url.as_str(), topic)
        .with_strategy(subject_strategy(cfg))
        .with_auto_register(cfg.auto_register)
        .with_retry_policy(retry_policy(cfg));

    if let Some(auth) = registry_auth(cfg)? {
        apicurio = apicurio.with_auth(auth);
    }
    apicurio.request_timeout_ms = non_zero(cfg.request_timeout_ms);
    apicurio.connect_timeout_ms = non_zero(cfg.connect_timeout_ms);
    apicurio.max_cache_entries = non_zero_usize(cfg.max_cache_entries);
    Ok(apicurio)
}

fn subject_strategy(cfg: &ConfluentRegistryConfig) -> SubjectNameStrategy {
    match cfg.subject_name_strategy {
        ConfigStrategy::TopicName => SubjectNameStrategy::TopicName,
        ConfigStrategy::RecordName => SubjectNameStrategy::RecordName,
        ConfigStrategy::TopicRecordName => SubjectNameStrategy::TopicRecordName,
    }
}

fn schema_references(cfg: &ConfluentRegistryConfig) -> Vec<SchemaReference> {
    cfg.references
        .iter()
        .map(|r| SchemaReference::new(r.name.clone(), r.subject.clone(), r.version))
        .collect()
}

fn retry_policy(cfg: &ConfluentRegistryConfig) -> RetryPolicy {
    if !cfg.retry.enabled {
        return RetryPolicy::none();
    }
    RetryPolicy::new()
        .max_retries(cfg.retry.max_retries)
        .base_backoff(std::time::Duration::from_millis(cfg.retry.base_backoff_ms))
        .max_backoff(std::time::Duration::from_millis(cfg.retry.max_backoff_ms))
}

fn registry_auth(cfg: &ConfluentRegistryConfig) -> Result<Option<SchemaRegistryAuth>, String> {
    Ok(match cfg.resolve_auth()? {
        ResolvedRegistryAuth::None => None,
        ResolvedRegistryAuth::Basic { username, password } => {
            Some(SchemaRegistryAuth::Basic { username, password })
        }
        ResolvedRegistryAuth::Bearer(token) => Some(SchemaRegistryAuth::BearerToken(
            rustcdc::SecretString::new(token),
        )),
    })
}

fn non_zero(value: u64) -> Option<u64> {
    (value > 0).then_some(value)
}

fn non_zero_usize(value: usize) -> Option<usize> {
    (value > 0).then_some(value)
}

// ─── Codec builders ───────────────────────────────────────────────────────────

/// Build the registry client for whichever flavour is configured, erased to one type.
///
/// The Confluent path preflights when asked: schema resolution sits on the encode path,
/// so a registry problem otherwise surfaces as a failed event mid-pipeline rather than
/// as a startup failure. The Apicurio native client is constructed lazily and makes no
/// connection until first use, so there is nothing to preflight against.
/// `schema_type` selects which schemas the preflight checks. It is **not** cosmetic:
/// through rustcdc 0.8 preflight always checked the Avro schemas under Avro record
/// names whatever the codec was, so a JSON Schema or Protobuf deployment with
/// `auto_register = false` failed against a perfectly correct registry, and one with
/// `auto_register = true` ran an Avro compatibility check against a JSON subject.
async fn build_registry_client(
    cfg: &ConfluentRegistryConfig,
    topic: &str,
    schema_type: SchemaType,
) -> Result<(DynRegistry, SchemaRegistryConfig), String> {
    let sr = schema_registry_config(cfg, topic)?;
    let client: DynRegistry = match cfg.flavor {
        RegistryFlavor::Confluent => {
            let client = Arc::new(
                sr.build()
                    .map_err(|e| format!("failed to build the schema registry client: {e}"))?,
            );
            if cfg.preflight {
                preflight_schema_registry(client.as_ref(), &sr, schema_type)
                    .await
                    .map_err(|e| format!("schema registry preflight failed: {e}"))?;
            }
            client
        }
        RegistryFlavor::Apicurio => {
            let apicurio = apicurio_registry_config(cfg, topic)?;
            let client = Arc::new(
                apicurio
                    .build()
                    .map_err(|e| format!("failed to build the Apicurio registry client: {e}"))?,
            );
            // rustcdc 0.9 gave Apicurio a preflight entry point of its own. Before it,
            // an Apicurio deployment silently got no startup check while a Confluent one
            // did — the same asymmetry we had to document as a limitation.
            if cfg.preflight {
                apicurio
                    .preflight(client.as_ref(), schema_type)
                    .await
                    .map_err(|e| format!("Apicurio registry preflight failed: {e}"))?;
            }
            client
        }
    };
    Ok((client, sr))
}

async fn build_confluent_avro_codec(
    cfg: &ConfluentRegistryConfig,
    topic: &str,
) -> Result<BuiltCodec, String> {
    let (client, sr) = build_registry_client(cfg, topic, SchemaType::Avro).await?;
    let encoder = ConfluentAvroEncoder::new(&client, &sr)
        .await
        .map_err(|e| format!("failed to build the Confluent Avro encoder: {e}"))?;
    Ok(EncoderCodec::new(encoder).boxed_async())
}

async fn build_confluent_json_schema_codec(
    cfg: &JsonSchemaCodecConfig,
    topic: &str,
) -> Result<BuiltCodec, String> {
    let (client, sr) =
        build_registry_client(cfg.binding.resolved()?, topic, SchemaType::Json).await?;
    // Both constructors are `async` since 0.9: with `auto_register = false` they verify
    // at construction that the subjects exist and carry exactly the schema rustcdc will
    // write. Through 0.8 this encoder ignored the setting entirely and registered anyway.
    let encoder = if cfg.validate {
        ConfluentJsonSchemaEncoder::new(client, &sr).await
    } else {
        ConfluentJsonSchemaEncoder::without_validation(client, &sr).await
    }
    .map_err(|e| format!("failed to build the Confluent JSON Schema encoder: {e}"))?;
    Ok(encoder.boxed_async())
}

async fn build_confluent_protobuf_codec(
    cfg: &ConfluentRegistryConfig,
    topic: &str,
) -> Result<BuiltCodec, String> {
    let (client, sr) = build_registry_client(cfg, topic, SchemaType::Protobuf).await?;
    let encoder = ConfluentProtobufEncoder::new(client, &sr)
        .await
        .map_err(|e| format!("failed to build the Confluent Protobuf encoder: {e}"))?;
    Ok(encoder.boxed_async())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::registry::{RegistryRetryConfig, SchemaReferenceConfig};

    fn registry(url: &str) -> ConfluentRegistryConfig {
        ConfluentRegistryConfig {
            url: url.to_string(),
            flavor: RegistryFlavor::Confluent,
            username_env: None,
            password_env: None,
            token_env: None,
            subject_name_strategy: ConfigStrategy::TopicRecordName,
            allow_insecure: true,
            request_timeout_ms: 15_000,
            connect_timeout_ms: 2_000,
            auto_register: false,
            normalize_schemas: true,
            max_cache_entries: 42,
            pool_max_idle_per_host: 7,
            retry: RegistryRetryConfig::default(),
            preflight: true,
            references: vec![SchemaReferenceConfig {
                name: "com.example.Address".to_string(),
                subject: "com.example.Address".to_string(),
                version: 3,
            }],
        }
    }

    /// Every registry knob must reach rustcdc — a field that silently keeps its
    /// default is indistinguishable from one the operator never set.
    #[test]
    fn registry_config_is_carried_through_verbatim() {
        let sr = schema_registry_config(&registry("http://sr:8081/"), "cdc.orders")
            .expect("build config");

        assert_eq!(sr.url, "http://sr:8081", "trailing slash must be trimmed");
        assert_eq!(sr.topic, "cdc.orders");
        assert!(!sr.auto_register);
        assert!(sr.normalize_schemas);
        assert_eq!(sr.request_timeout_ms, Some(15_000));
        assert_eq!(sr.connect_timeout_ms, Some(2_000));
        assert_eq!(sr.max_cache_entries, Some(42));
        assert_eq!(sr.pool_max_idle_per_host, Some(7));
        assert_eq!(sr.references.len(), 1);
        assert!(matches!(sr.strategy, SubjectNameStrategy::TopicRecordName));
    }

    /// `0` means "use the client default", not "time out immediately".
    #[test]
    fn zero_timeouts_fall_back_to_client_defaults() {
        let mut cfg = registry("http://sr:8081");
        cfg.request_timeout_ms = 0;
        cfg.connect_timeout_ms = 0;
        cfg.max_cache_entries = 0;
        cfg.pool_max_idle_per_host = 0;

        let sr = schema_registry_config(&cfg, "t").expect("build config");
        assert_eq!(sr.request_timeout_ms, None);
        assert_eq!(sr.connect_timeout_ms, None);
        assert_eq!(sr.max_cache_entries, None);
        assert_eq!(sr.pool_max_idle_per_host, None);
    }

    #[tokio::test]
    async fn registry_free_codecs_build_without_a_registry() {
        for codec in [
            CodecConfig::Json,
            CodecConfig::JsonPretty,
            CodecConfig::Avro,
            CodecConfig::Protobuf,
            CodecConfig::CloudEvents,
        ] {
            build(Some(&codec), "t")
                .await
                .unwrap_or_else(|e| panic!("{} must build offline: {e}", codec.label()));
        }
    }

    /// Every registry-backed codec must round-trip through TOML under its documented
    /// `type` tag; a rename here silently invalidates every operator's config.
    #[test]
    fn codec_tags_round_trip_through_toml() {
        for (toml_src, expected) in [
            (r#"type = "json""#, "json"),
            (r#"type = "json_pretty""#, "json_pretty"),
            (r#"type = "avro""#, "avro"),
            (r#"type = "protobuf""#, "protobuf"),
            (r#"type = "cloud_events""#, "cloud_events"),
            (
                "type = \"avro_confluent\"\n[registry]\nurl = \"https://sr\"",
                "avro_confluent",
            ),
            (
                "type = \"json_schema_confluent\"\n[registry]\nurl = \"https://sr\"",
                "json_schema_confluent",
            ),
            (
                "type = \"protobuf_confluent\"\n[registry]\nurl = \"https://sr\"",
                "protobuf_confluent",
            ),
            (
                "type = \"avro_confluent\"\nregistry_ref = \"prod\"",
                "avro_confluent",
            ),
            (
                "type = \"glue_avro\"\nschema_name = \"cdc-events\"",
                "glue_avro",
            ),
        ] {
            let parsed: CodecConfig =
                toml::from_str(toml_src).unwrap_or_else(|e| panic!("{toml_src}: {e}"));
            assert_eq!(parsed.label(), expected);
            parsed.validate().expect("valid");
        }
    }

    /// Glue's client has no lookup-by-name API, so `auto_register = false` could only
    /// be accepted and ignored — the exact failure mode this config surface exists to
    /// avoid. It is rejected with the remedy named instead.
    #[test]
    fn glue_rejects_auto_register_false() {
        let parsed: CodecConfig = toml::from_str(
            "type = \"glue_avro\"\nschema_name = \"cdc-events\"\nauto_register = false",
        )
        .expect("parse");
        let err = parsed.validate().expect_err("must reject");
        assert!(err.contains("lookup-by-name"), "unexpected: {err}");
    }

    #[test]
    fn glue_defaults_the_registry_name_and_requires_a_schema_name() {
        let parsed: CodecConfig =
            toml::from_str("type = \"glue_avro\"\nschema_name = \"cdc-events\"").expect("parse");
        parsed.validate().expect("valid");
        match parsed {
            CodecConfig::GlueAvro(cfg) => {
                assert_eq!(cfg.registry_name, "default-registry");
                assert!(cfg.auto_register);
                assert_eq!(
                    cfg.key_schema_name, None,
                    "the key schema defaults downstream"
                );
            }
            other => panic!("unexpected variant: {}", other.label()),
        }

        let empty: CodecConfig =
            toml::from_str("type = \"glue_avro\"\nschema_name = \"  \"").expect("parse");
        assert!(
            empty.validate().is_err(),
            "an empty schema name must be rejected"
        );
    }

    /// Without the feature the codec must fail at build time naming the remedy, not
    /// fall back to some other framing.
    #[cfg(not(feature = "glue"))]
    #[tokio::test]
    async fn glue_codec_without_the_feature_reports_the_remedy() {
        let parsed: CodecConfig =
            toml::from_str("type = \"glue_avro\"\nschema_name = \"cdc-events\"").expect("parse");
        let err = build(Some(&parsed), "t").await.expect_err("must fail");
        assert!(err.contains("--features glue"), "unexpected: {err}");
    }

    /// A registry-backed codec with neither an inline table nor a `registry_ref`
    /// used to fail only when the sink was built, long after startup.
    #[test]
    fn registry_backed_codec_without_a_registry_is_rejected() {
        let parsed: CodecConfig = toml::from_str(r#"type = "avro_confluent""#).expect("parse");
        let err = parsed.validate().expect_err("must reject");
        assert!(err.contains("registry_ref"), "unexpected: {err}");
    }

    #[test]
    fn inline_registry_and_ref_together_are_rejected() {
        let parsed: CodecConfig = toml::from_str(
            "type = \"avro_confluent\"\nregistry_ref = \"prod\"\n[registry]\nurl = \"https://sr\"",
        )
        .expect("parse");
        let err = parsed.validate().expect_err("must reject");
        assert!(err.contains("pick one"), "unexpected: {err}");
    }

    /// The JSON Schema codec validates by default; an operator has to opt out.
    #[test]
    fn json_schema_codec_validates_by_default() {
        let parsed: CodecConfig =
            toml::from_str("type = \"json_schema_confluent\"\n[registry]\nurl = \"https://sr\"")
                .expect("parse");
        match parsed {
            CodecConfig::JsonSchemaConfluent(cfg) => assert!(cfg.validate),
            other => panic!("unexpected variant: {}", other.label()),
        }
    }
}
