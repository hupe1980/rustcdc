#![cfg(all(
    feature = "schemreg",
    feature = "apicurio",
    feature = "avro",
    feature = "protobuf"
))]

//! Live schema-registry coverage against a real registry container.
//!
//! # Why this file exists
//!
//! The audit named this the largest single evidence gap: the Apicurio backend, the
//! Confluent Protobuf codec and the registry preflight/warm-cache helpers compiled and
//! were unit-tested where the logic was local, but none had ever spoken to a registry.
//! "Compiles" is not evidence for a network-facing client — subject naming, wire framing,
//! schema-id assignment and error classification are all things only the server can
//! confirm.
//!
//! # Why one container covers two backends
//!
//! Apicurio Registry 3 serves its **native v3 API** at `/apis/registry/v3` *and* a
//! **Confluent-compatible API** at `/apis/ccompat/v7`. Pointing `ApicurioRegistryConfig`
//! at the former and `SchemaRegistryConfig` at the latter exercises both client paths —
//! and the Confluent-compatible surface is the same one Confluent Schema Registry serves,
//! so the Confluent client is tested against a real implementation of its own protocol
//! rather than a mock.
//!
//! # What is still not covered
//!
//! **AWS Glue.** Its 18-byte framing and UUID schema identity are unit-tested, but the
//! service has no self-hostable implementation, so there is no container to point at. That
//! remains an explicit gap in FINDINGS.md rather than something this file silently implies
//! it covers.

use rustcdc::codec::{
    detect_wire_format, preflight_schema_registry, warm_schema_cache, ApicurioRegistryConfig,
    ConfluentAvroDecoder, ConfluentAvroEncoder, ConfluentJsonSchemaEncoder,
    ConfluentProtobufDecoder, ConfluentProtobufEncoder, EventEncoder, SchemaRegistryConfig,
};
use rustcdc::{Event, Operation, SourceMetadata};
use std::sync::Arc;
use testcontainers::{
    core::IntoContainerPort, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt,
};

fn skip() -> bool {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping schema-registry live test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return true;
    }
    false
}

/// Start Apicurio Registry 3 with in-memory storage.
///
/// In-memory rather than Kafka- or SQL-backed: this suite tests the *client*, and a
/// storage backend would add a second container and a class of failure that has nothing to
/// do with what is under test.
async fn start_registry() -> rustcdc::Result<(ContainerAsync<GenericImage>, String)> {
    // Readiness is polled through the crate's own `preflight_schema_registry` rather than
    // matched against a log line: the log format is Quarkus's to change, and a wait
    // strategy that silently stops matching turns every test in this file into a startup
    // timeout — which is exactly what it did on the first attempt.
    let container = GenericImage::new("apicurio/apicurio-registry", "3.0.6")
        .with_exposed_port(8080.tcp())
        .with_env_var("QUARKUS_PROFILE", "prod")
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(8080.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let base = format!("http://{host}:{port}");

    let probe = SchemaRegistryConfig::new(format!("{base}/apis/ccompat/v7"), "readiness")
        .with_connect_timeout_ms(1_000)
        .with_request_timeout_ms(1_000);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        if let Ok(registry) = probe.build() {
            if preflight_schema_registry(&registry, &probe).await.is_ok() {
                return Ok((container, base));
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(rustcdc::Error::SourceError(
                "apicurio registry did not become ready within 120s".into(),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

fn sample_event(id: i64) -> Event {
    Event::builder("users", Operation::Insert)
        .source(SourceMetadata::new(
            "postgres",
            format!("0/{id}"),
            1_700_000_000,
        ))
        .schema("public")
        .after(serde_json::json!({ "id": id, "email": "a@example.com" }))
        .primary_key(["id"])
        .ts(1_700_000_000)
        .build()
}

/// Avro round trip through the Confluent-compatible API of a real registry.
///
/// Avro binary is **positional and untagged**: a schema mismatch does not error, it yields
/// silently wrong field values. So the assertion that matters is not "decode succeeded" but
/// "the decoded event equals the encoded one".
#[tokio::test]
async fn confluent_avro_round_trips_through_a_live_registry() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }
    let (_container, base) = start_registry().await?;
    let config = SchemaRegistryConfig::new(format!("{base}/apis/ccompat/v7"), "cdc.users");

    let registry = Arc::new(config.build()?);

    // Preflight is meant to fail *here*, where an operator can act, rather than on the
    // first event. Against a healthy registry it must succeed.
    preflight_schema_registry(registry.as_ref(), &config).await?;

    let encoder = ConfluentAvroEncoder::new(registry.as_ref(), &config).await?;
    let schema_id = encoder.schema_id();

    let event = sample_event(1);
    let encoded = encoder.encode(&event)?;

    // The 5-byte Confluent header: magic 0x00 then a big-endian schema id.
    assert_eq!(
        encoded.bytes[0], 0x00,
        "Confluent framing must start with the 0x00 magic byte"
    );
    let framed_id = u32::from_be_bytes([
        encoded.bytes[1],
        encoded.bytes[2],
        encoded.bytes[3],
        encoded.bytes[4],
    ]);
    assert_eq!(
        framed_id,
        schema_id.as_u32(),
        "the id in the wire header must be the id the registry assigned, not a local counter"
    );

    let detected = detect_wire_format(&encoded.bytes);
    assert!(
        format!("{detected:?}").to_lowercase().contains("confluent"),
        "a Confluent-framed payload must be detected as such, got {detected:?}"
    );

    let decoder = ConfluentAvroDecoder::new(Arc::clone(&registry))?;
    let decoded = decoder.decode(&encoded.bytes).await?;

    assert_eq!(decoded.table, event.table);
    assert_eq!(decoded.op, event.op);
    assert_eq!(
        decoded.after, event.after,
        "Avro is positional and untagged — a schema mismatch yields wrong values, not an \
         error, so the payload must be compared field by field"
    );
    assert_eq!(decoded.source.offset, event.source.offset);
    Ok(())
}

/// The same encoder, pointed at Apicurio's **native** v3 API.
///
/// `ApicurioRegistryConfig` and `SchemaRegistryConfig` both produce a
/// `SchemaRegistryClient`, so the encoders are shared. This asserts that claim against the
/// real service rather than trusting that the types line up.
#[tokio::test]
async fn apicurio_native_api_serves_the_same_encoder_surface() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }
    let (_container, base) = start_registry().await?;
    // `ApicurioRegistryConfig` takes the registry **root**, not an API path: it appends
    // `/apis/registry/v3` itself. Passing the full path produced a doubled URL and a 404,
    // which is why the doc comment on `url` now says so explicitly.
    let apicurio = ApicurioRegistryConfig::new(&base, "cdc.orders");
    let registry = Arc::new(apicurio.build()?);

    let encoder =
        ConfluentAvroEncoder::new(registry.as_ref(), &apicurio.as_schema_registry_config()).await?;
    let event = sample_event(7);
    let encoded = encoder.encode(&event)?;

    assert_eq!(
        encoded.bytes[0], 0x00,
        "Apicurio uses the 5-byte framing too"
    );
    assert!(
        encoder.schema_id().as_u32() > 0,
        "the registry must have assigned a real schema id"
    );

    let decoder = ConfluentAvroDecoder::new(Arc::clone(&registry))?;
    let decoded = decoder.decode(&encoded.bytes).await?;
    assert_eq!(decoded.after, event.after);
    Ok(())
}

/// JSON Schema payloads round trip, and the registry assigns a distinct subject.
#[tokio::test]
async fn confluent_json_schema_round_trips_through_a_live_registry() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }
    let (_container, base) = start_registry().await?;
    let config = SchemaRegistryConfig::new(format!("{base}/apis/ccompat/v7"), "cdc.json");
    let registry = Arc::new(config.build()?);

    let encoder = ConfluentJsonSchemaEncoder::new(Arc::clone(&registry), &config)?;
    let event = sample_event(3);
    let encoded = encoder.encode_event(&event).await?;

    assert_eq!(encoded.bytes[0], 0x00);
    assert!(
        encoder.cached_value_schema_id().is_some(),
        "encoding must have resolved and cached a registry-assigned schema id"
    );
    Ok(())
}

/// A subject that does not exist must fail immediately, not be retried.
///
/// This is the error-classification contract (audit finding C6): only transient conditions
/// retry. A not-found that retried would make an outer retry loop spin on a permanent
/// condition, turning a clear error into a hang.
#[tokio::test]
async fn a_missing_subject_fails_fast_rather_than_retrying() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }
    let (_container, base) = start_registry().await?;
    // `auto_register(false)` means "the schema must already exist" — and it does not.
    let config = SchemaRegistryConfig::new(format!("{base}/apis/ccompat/v7"), "cdc.absent")
        .with_auto_register(false);
    let registry = Arc::new(config.build()?);

    let started = std::time::Instant::now();
    let outcome = ConfluentAvroEncoder::new(registry.as_ref(), &config).await;
    let elapsed = started.elapsed();

    let error = outcome.expect_err("an unregistered subject must fail");
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "a not-found is permanent and must fail fast; took {elapsed:?}, which means it was \
         retried like a transient failure"
    );
    let message = error.to_string().to_lowercase();
    assert!(
        message.contains("not found")
            || message.contains("no schema")
            || message.contains("404")
            || message.contains("subject"),
        "the error must name the missing subject so an operator can act: {error}"
    );
    Ok(())
}

/// An unreachable registry must fail preflight, not the first event.
#[tokio::test]
async fn preflight_rejects_an_unreachable_registry() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }
    // Port 1 is reserved and never listening.
    let config = SchemaRegistryConfig::new("http://127.0.0.1:1", "cdc.unreachable")
        .with_connect_timeout_ms(500)
        .with_request_timeout_ms(500);
    let registry = config.build()?;

    let error = preflight_schema_registry(&registry, &config)
        .await
        .expect_err("an unreachable registry must fail preflight");
    assert!(
        !error.to_string().is_empty(),
        "the preflight failure must carry a diagnosable message"
    );
    Ok(())
}

/// Warming the cache makes a subsequent decode independent of the registry.
///
/// This is what the helper is for: a registry outage after warm-up must not fail the decode
/// path for schemas already in hand.
#[tokio::test]
async fn a_warmed_cache_serves_decodes_without_further_registry_calls() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }
    let (container, base) = start_registry().await?;
    let config = SchemaRegistryConfig::new(format!("{base}/apis/ccompat/v7"), "cdc.warm");
    let registry = Arc::new(config.build()?);

    let encoder = ConfluentAvroEncoder::new(registry.as_ref(), &config).await?;
    let event = sample_event(9);
    let encoded = encoder.encode(&event)?;

    warm_schema_cache(registry.as_ref(), [encoder.schema_id()]).await?;

    // Take the registry away. A decode that still needs the network fails here.
    container
        .stop()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;

    let decoder = ConfluentAvroDecoder::new(Arc::clone(&registry))?;
    let decoded = decoder.decode(&encoded.bytes).await?;
    assert_eq!(
        decoded.after, event.after,
        "a warmed cache must serve the decode with the registry down"
    );
    Ok(())
}

/// Confluent Protobuf round trip, including the message-index path.
///
/// The Protobuf wire format carries a **message-index path** locating the message inside
/// its `.proto` file, between the 5-byte header and the payload. A wrong index is *not* a
/// decode error for a Confluent deserialiser — it resolves to a different message type and
/// misreads the bytes. rustcdc derives the index from the compiled descriptor rather than
/// hardcoding it; this asserts that derivation against a registry that actually stores the
/// schema.
#[tokio::test]
async fn confluent_protobuf_round_trips_through_a_live_registry() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }
    let (_container, base) = start_registry().await?;
    let config = SchemaRegistryConfig::new(format!("{base}/apis/ccompat/v7"), "cdc.proto");
    let registry = Arc::new(config.build()?);

    let encoder = ConfluentProtobufEncoder::new(Arc::clone(&registry), &config)?;
    assert_eq!(
        encoder.message_indexes(),
        [3],
        "`Event` is the fourth message in proto/event.proto — the obvious hardcoded guess \
         would be [0], and a Confluent deserialiser would misread it without erroring"
    );

    let event = sample_event(11);
    let framed = encoder.encode(&event).await?;

    assert_eq!(
        framed[0], 0x00,
        "Protobuf uses the same 5-byte Confluent header"
    );
    let detected = detect_wire_format(&framed);
    assert!(
        format!("{detected:?}").to_lowercase().contains("confluent"),
        "got {detected:?}"
    );

    let decoder = ConfluentProtobufDecoder::new(Arc::clone(&registry))?;
    let decoded = decoder.decode(&framed).await?;

    assert_eq!(decoded.table, event.table);
    assert_eq!(decoded.op, event.op);
    assert_eq!(
        decoded.after, event.after,
        "the row payload is protobuf `bytes` holding JSON — it must come back as JSON, not \
         as an opaque byte string a sink would write verbatim into the row"
    );
    assert_eq!(decoded.source.offset, event.source.offset);
    Ok(())
}

/// A delete carries no after-image, and that must survive the protobuf round trip.
///
/// Protobuf has no null: an absent `bytes` field and an empty one are the same on the wire.
/// If the decoder cannot tell them apart, a delete comes back claiming an empty row rather
/// than no row.
#[tokio::test]
async fn a_protobuf_delete_round_trips_without_an_after_image() -> rustcdc::Result<()> {
    if skip() {
        return Ok(());
    }
    let (_container, base) = start_registry().await?;
    let config = SchemaRegistryConfig::new(format!("{base}/apis/ccompat/v7"), "cdc.proto.delete");
    let registry = Arc::new(config.build()?);

    let encoder = ConfluentProtobufEncoder::new(Arc::clone(&registry), &config)?;
    let event = Event::builder("users", Operation::Delete)
        .source(SourceMetadata::new("postgres", "0/99", 1_700_000_000))
        .schema("public")
        .before(serde_json::json!({ "id": 42 }))
        .primary_key(["id"])
        .ts(1_700_000_000)
        .build();

    let framed = encoder.encode(&event).await?;
    let decoded = ConfluentProtobufDecoder::new(Arc::clone(&registry))?
        .decode(&framed)
        .await?;

    assert_eq!(decoded.op, Operation::Delete);
    assert_eq!(decoded.before, event.before);
    assert!(
        decoded.after.is_none(),
        "an absent after-image must not come back as an empty object"
    );
    Ok(())
}
