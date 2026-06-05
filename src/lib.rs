//! Core crate surface for rustcdc.

#![forbid(unsafe_code)]

pub mod checkpoint;
pub mod codec;
pub mod core;
pub mod ddl_capture;
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
    fingerprint_event_stable, fingerprint_event_transient, AckMode, AckToken, CdcRuntime,
    ConnectionRetryPolicy, Error, ErrorKind, Event, EventBatch, EventIdempotencyGuard, EventTracer,
    FingerprintError, IdempotencyOptions, MetricsCollector, NoOpEventTracer, NoOpMetricsCollector,
    Offset, Operation, PostCommitSourceConfirmPolicy, Result, RuntimeAdminSnapshot, RuntimeConfig,
    RuntimeObservability, RuntimeOptions, RuntimeSourceConfig, RuntimeState, SecretProvider,
    SecretString, SnapshotMetadata, SourceErrorKind, SourceMetadata, StructuredLogger,
    TransactionMetadata, TransformErrorPolicy, TransportConfig, ValidationError, ValidationErrors,
    EVENT_ENVELOPE_VERSION,
};
#[cfg(feature = "metrics")]
pub use crate::core::{
    MetricsReport, OTelConfig, OTelEventTracer, OTelMetricsCollector, SpanRecord,
};
pub use crate::ddl_capture::{
    CapturedDdl, DdlDialect, DdlExtractor, DdlOperation, ParsedDdlStatement, SchemaDiff,
    SchemaDiffOperation,
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
    ConnectorCapabilities, DatabaseAuthMode, HandoffResult, SnapshotCheckpointHelper, SnapshotEnd,
    SnapshotProgress, SnapshotProgressTracker, SnapshotTrackerConfig, SnapshotTrackerReport,
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
    FieldMappingConfig, FieldMappingTransform, FilterField, FilterMode, FilterOperator,
    FilterProjectionConfig, FilterProjectionTransform, FilterRule, MaskHashConfig,
    MaskHashTransform, MaskRule, RouteConfig, RouteTransform, Transform, TransformPipeline,
    UnwrapConfig, UnwrapTransform,
};
#[cfg(feature = "wasm")]
pub use crate::wasm::{
    TransformResult as WasmTransformResult, WasmConfig, WasmModule, WasmRuntime, WasmTransform,
    DEFAULT_WASM_MEMORY_LIMIT_MB, DEFAULT_WASM_TIMEOUT_MS,
};

#[cfg(feature = "avro")]
pub use crate::codec::AvroEncoder;
#[cfg(feature = "cloudevents")]
pub use crate::codec::CloudEventsEncoder;
#[cfg(feature = "protobuf")]
pub use crate::codec::ProtobufEncoder;
#[cfg(feature = "schemreg")]
pub use crate::codec::{
    decode_wire_format, encode_wire_format, CachedSchemaRegistry, CompatibilityLevel,
    ConfluentAvroCodec, ConfluentAvroDecoder, ConfluentAvroEncoder, ConfluentSchemaRegistry,
    EncodeTarget, SchemaId, SchemaRegistryAuth, SchemaRegistryClient, SchemaRegistryConfig,
    SchemaType, SubjectNameStrategy,
};
pub use crate::codec::{
    Codec, CodecOutput, EncodedOutput, EncoderCodec, EventEncoder, JsonCodec, JsonEncoder,
    JsonPrettyEncoder,
};
pub use crate::pipeline::{TableRoute, TableRouter, TableRouterBuilder};
pub use crate::sink::{FileJsonlSink, MemorySinkAdapter, SinkAdapter, StdoutSink};
