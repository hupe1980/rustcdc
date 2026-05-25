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
pub mod schema_history;
pub mod source;
pub mod sink;
pub mod testkit;
pub mod transform;
pub mod wasm;

pub use crate::core::{
    fingerprint_event, AckToken, CdcRuntime, Error, Event, EventBatch, EventIdempotencyGuard,
    EventTracer, IdempotencyOptions, MetricsCollector, NoOpEventTracer, NoOpMetricsCollector,
    Offset, Operation, Result, RuntimeAdminSnapshot, RuntimeConfig, RuntimeObservability,
    RuntimeOptions, RuntimeSourceConfig, RuntimeState, SecretProvider, SecretString,
    SnapshotMetadata, SourceMetadata, StructuredLogger, TransactionMetadata, TransformErrorPolicy,
    TransportConfig, ValidationError, EVENT_ENVELOPE_VERSION,
};
#[cfg(feature = "metrics")]
pub use crate::core::{
    MetricsReport, OTelConfig, OTelEventTracer, OTelMetricsCollector, SpanRecord,
};
pub use crate::ddl_capture::{
    CapturedDdl, DdlDialect, DdlExtractor, DdlOperation, ParsedDdlStatement, SchemaDiff,
    SchemaDiffOperation,
};
pub use crate::source::{
    ConnectorCapabilities, HandoffResult, SnapshotTrackerConfig, SnapshotTrackerReport,
    SnapshotProgressTracker, SnapshotCheckpointHelper, SnapshotEnd, SnapshotProgress,
    SnapshotValidationResult, SnapshotValidator, TableProgress,
};
#[cfg(feature = "mysql")]
pub use crate::source::{MysqlConnection, MysqlSourceConfig, ServerFlavor};
#[cfg(feature = "mysql")]
pub use crate::source::MysqlIncrementalSnapshotHandle;
#[cfg(feature = "mariadb")]
pub use crate::source::{
    MariaDbConnection, MariaDbIncrementalSnapshotHandle, MariaDbSnapshotHandle,
    MariaDbSourceConfig, MariaDbStreamHandle,
};
#[cfg(feature = "postgres")]
pub use crate::source::{PostgresConnection, PostgresSourceConfig};
#[cfg(feature = "postgres")]
pub use crate::source::IncrementalSnapshotHandle;
pub use crate::source::IncrementalSnapshotConfig;
#[cfg(feature = "sqlserver")]
pub use crate::source::{SqlServerConnection, SqlServerSourceConfig};
#[cfg(feature = "sqlserver")]
pub use crate::source::SqlServerIncrementalSnapshotHandle;
pub use crate::wasm::{
    TransformResult as WasmTransformResult, WasmConfig, WasmModule, WasmRuntime,
    DEFAULT_WASM_MEMORY_LIMIT_MB, DEFAULT_WASM_TIMEOUT_MS,
};

pub use crate::codec::{EncodedOutput, EventEncoder, JsonEncoder, JsonPrettyEncoder};
#[cfg(feature = "cloudevents")]
pub use crate::codec::CloudEventsEncoder;
#[cfg(feature = "protobuf")]
pub use crate::codec::ProtobufEncoder;
#[cfg(feature = "avro")]
pub use crate::codec::AvroEncoder;
