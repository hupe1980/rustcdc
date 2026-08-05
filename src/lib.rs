//! Core crate surface for rustcdc.

// `deny`, not `forbid`: the Windows PID-liveness probe in
// `checkpoint::owner_lease` needs a single, tightly-scoped `unsafe` block for the
// Win32 `OpenProcess`/`GetExitCodeProcess` FFI. `forbid` cannot be locally
// overridden, so it made the crate impossible to compile for `*-pc-windows-*`
// targets — invisibly, because CI only builds Linux and macOS. Every other module
// remains unsafe-free, and each exception must carry an explicit
// `#[allow(unsafe_code)]` with a safety comment.
#![deny(unsafe_code)]
// Every public item carries documentation, and the lint keeps it that way.
//
// This is a library whose public surface *is* the product: an undocumented `pub fn` on a
// checkpoint or connector type is a reader guessing at a correctness contract. The
// backfill that made this lint pass was 416 items, and roughly a fifth of them turned out
// to be places where the *behaviour* needed explaining rather than the signature restated
// — which is the argument for enforcing it rather than leaving it aspirational.
#![deny(missing_docs)]

/// Compiles every Rust code block in the published Markdown documentation.
///
/// Documentation drift is not a cosmetic problem for a library: a sample that no
/// longer compiles is a support ticket, and one that compiles but describes a
/// superseded contract is worse. Rustdoc examples on public items were already
/// doctested; the Markdown under `docs/` and the README were not, so they could rot
/// silently — and did.
///
/// Gated on `cfg(doctest)` so the text is never embedded into the crate's own
/// rendered documentation; only the code blocks are compiled and run.
///
/// The pages live under `site/content/docs/` and are published by Zola to the project
/// site. Compiling them here is what keeps the published site and the crate in step.
///
/// Blocks that genuinely cannot run in a doctest (they need a live database, or show
/// a fragment rather than a program) must be marked `ignore` with a one-line reason,
/// or `no_run` when they should still be type-checked. An unmarked block that fails
/// to compile is a real defect in the documentation.
#[cfg(doctest)]
mod markdown_doctests {
    #[doc = include_str!("../README.md")]
    mod readme {}

    #[doc = include_str!("../site/content/docs/api.md")]
    mod api {}

    #[doc = include_str!("../site/content/docs/config-reference.md")]
    mod config_reference {}

    #[doc = include_str!("../site/content/docs/getting-started.md")]
    mod getting_started {}

    #[doc = include_str!("../site/content/docs/adapter-sdk.md")]
    mod adapter_sdk {}

    #[doc = include_str!("../site/content/docs/schema-evolution.md")]
    mod schema_evolution {}
}

pub mod checkpoint;
pub mod codec;
pub mod core;
pub mod ddl_capture;
/// Replay a captured event stream deterministically, and diff it against a golden record.
pub mod deterministic_replay;
#[cfg(feature = "test-harnesses")]
pub mod fault_injection;
#[cfg(feature = "outbox")]
pub mod outbox;
pub mod pipeline;
pub mod schema_history;
pub mod sink;
pub mod source;
#[cfg(any(test, feature = "test-harnesses"))]
pub mod testkit;
pub mod transform;
#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "tls")]
pub use crate::core::RustlsClientConfig;
pub use crate::core::{
    fingerprint_event_stable, fingerprint_event_transient, render_error_chain, AckMode, AckToken,
    CdcRuntime, ConnectionRetryPolicy, Error, ErrorChain, ErrorKind, ErrorReport, Event,
    EventBatch, EventBuilder, EventIdempotencyGuard, EventTracer, FingerprintError, HealthVerdict,
    IdempotencyOptions, MetricsCollector, NoOpEventTracer, NoOpMetricsCollector, NoRowWrite,
    Offset, Operation, PostCommitSourceConfirmPolicy, Result, RowWrite, RuntimeAdminSnapshot,
    RuntimeConfig, RuntimeObservability, RuntimeOptions, RuntimeSourceConfig, RuntimeState,
    SecretProvider, SecretString, SnapshotMetadata, SourceErrorKind, SourceMetadata,
    StructuredLogger, TransactionBoundaryPolicy, TransactionMetadata, TransformErrorPolicy,
    TransportConfig, ValidationError, ValidationErrors, EVENT_ENVELOPE_VERSION,
};
#[cfg(feature = "metrics")]
pub use crate::core::{
    MetricsReport, OTelConfig, OTelEventTracer, OTelMetricsCollector, SpanRecord,
};
pub use crate::ddl_capture::{
    extract_columns_from_create, extract_primary_keys, extract_qualified_name,
    extract_qualified_name_with_default, normalize_identifier, CapturedDdl, DdlDialect,
    DdlExtractor, DdlOperation, MysqlDdlExtractor, ParsedDdlStatement, PostgresDdlExtractor,
    SchemaDiff, SchemaDiffOperation, SqlServerDdlExtractor,
};
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
pub use crate::source::IncrementalSnapshotConfig;
#[cfg(feature = "postgres")]
pub use crate::source::IncrementalSnapshotHandle;
#[cfg(feature = "mysql")]
pub use crate::source::MysqlIncrementalSnapshotHandle;
#[cfg(feature = "sqlserver")]
pub use crate::source::SqlServerIncrementalSnapshotHandle;
pub use crate::source::{
    incremental_snapshot_state_from_offset, ChunkRow, ConnectorCapabilities, DatabaseAuthMode,
    HandoffResult, IncrementalSnapshotBackend, IncrementalSnapshotDriver, IncrementalSnapshotState,
    IncrementalSnapshotTableState, SnapshotCheckpointHelper, SnapshotEnd, SnapshotProgress,
    SnapshotProgressTracker, SnapshotTable, SnapshotTrackerConfig, SnapshotTrackerReport,
    SnapshotValidationResult, SnapshotValidator, TableProgress,
};
#[cfg(feature = "mariadb")]
pub use crate::source::{
    MariaDbConnection, MariaDbIncrementalSnapshotHandle, MariaDbSnapshotHandle,
    MariaDbSourceConfig, MariaDbStreamHandle,
};
#[cfg(feature = "mysql")]
pub use crate::source::{MysqlConnection, MysqlSourceConfig, ServerFlavor};
#[cfg(feature = "postgres")]
pub use crate::source::{PostgresConnection, PostgresSourceConfig};
#[cfg(feature = "sqlserver")]
pub use crate::source::{SqlServerConnection, SqlServerSourceConfig};
pub use crate::transform::{
    AsyncTransform, FieldMappingConfig, FieldMappingTransform, FilterField, FilterMode,
    FilterOperator, FilterProjectionConfig, FilterProjectionTransform, FilterRule, MaskHashConfig,
    MaskHashTransform, MaskRule, RouteConfig, RouteTransform, Transform, TransformPipeline,
    UnmatchedRule, UnwrapConfig, UnwrapTransform,
};
#[cfg(feature = "outbox")]
pub use crate::transform::{OutboxResult, OutboxTransform};
#[cfg(feature = "wasm")]
pub use crate::wasm::{
    TransformResult as WasmTransformResult, WasmConfig, WasmModule, WasmRuntime,
    WasmRuntimeMetrics, WasmTransform, DEFAULT_WASM_MEMORY_LIMIT_MB, DEFAULT_WASM_TIMEOUT_MS,
};

#[cfg(feature = "apicurio")]
pub use crate::codec::ApicurioRegistryConfig;
#[cfg(feature = "cloudevents")]
pub use crate::codec::CloudEventsEncoder;
#[cfg(feature = "avro")]
pub use crate::codec::{avro_value_to_event, AvroDecoder, AvroEncoder, AVRO_SCHEMA};
#[cfg(feature = "schemreg")]
pub use crate::codec::{
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
pub use crate::codec::{
    AsyncCodec, BoxedAsyncCodec, BoxedCodec, Codec, CodecOutput, EncodedOutput, EncoderCodec,
    EventEncoder, JsonCodec, JsonEncoder, JsonPrettyEncoder,
};
#[cfg(feature = "glue")]
pub use crate::codec::{GlueAvroConfig, GlueAvroDecoder, GlueAvroEncoder};
#[cfg(feature = "protobuf")]
pub use crate::codec::{ProtoEventKey, ProtobufEncoder};
pub use crate::pipeline::{
    table_matches, HeterogeneousTableRouter, TableRoute, TableRouter, TableRouterBuilder,
};
pub use crate::sink::{
    BoxedSink, FanOutSinkAdapter, FileJsonlSink, FileJsonlSinkConfig, MemorySinkAdapter,
    SinkAdapter, SinkDeliveryGuarantee, SinkDeliveryMetrics, StdoutSink,
};
