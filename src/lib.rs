//! Core crate surface for cdc-rs.

#![forbid(unsafe_code)]

pub mod checkpoint;
pub mod core;
pub mod ddl_capture;
pub mod deterministic_replay;
pub mod fault_injection;
#[cfg(feature = "outbox")]
pub mod outbox;
pub mod schema_history;
pub mod source;
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
    ConnectorCapabilities, HandoffResult, ParallelSnapshotConfig, ParallelSnapshotReport,
    ParallelSnapshotState, SnapshotCheckpointHelper, SnapshotEnd, SnapshotProgress,
    SnapshotValidationResult, SnapshotValidator, TableProgress,
};
#[cfg(feature = "mysql")]
pub use crate::source::{MysqlConnection, MysqlSourceConfig};
#[cfg(feature = "postgres")]
pub use crate::source::{PostgresConnection, PostgresSourceConfig};
#[cfg(feature = "sqlserver")]
pub use crate::source::{SqlServerConnection, SqlServerSourceConfig};
pub use crate::wasm::{
    TransformResult as WasmTransformResult, WasmConfig, WasmModule, WasmRuntime,
    DEFAULT_WASM_MEMORY_LIMIT_MB, DEFAULT_WASM_TIMEOUT_MS,
};
