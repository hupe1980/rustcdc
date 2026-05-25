//! Core event, error, offset, observability, and runtime primitives.

mod error;
mod event;
mod idempotency;
mod logging;
mod observability;
#[cfg(feature = "metrics")]
mod otel;
mod runtime;
mod runtime_offsets;
mod runtime_utils;
mod transport;
mod secret;

pub use error::{Error, Result};
pub use event::{
    Event, Operation, SnapshotMetadata, SourceMetadata, TransactionMetadata, ValidationError,
    EVENT_ENVELOPE_VERSION,
};
pub use idempotency::{fingerprint_event, EventIdempotencyGuard};
pub use logging::StructuredLogger;
pub use observability::{EventTracer, MetricsCollector, NoOpEventTracer, NoOpMetricsCollector};
#[cfg(feature = "metrics")]
pub use otel::{MetricsReport, OTelConfig, OTelEventTracer, OTelMetricsCollector, SpanRecord};
pub use runtime::{
    AckToken, CdcRuntime, ConnectionRetryPolicy, EventBatch, IdempotencyOptions,
    RuntimeAdminSnapshot, RuntimeConfig, RuntimeObservability, RuntimeOptions, RuntimeSourceConfig,
    RuntimeState, TransformErrorPolicy,
};
pub use transport::TransportConfig;
pub use secret::{SecretProvider, SecretString};

use std::fmt::Debug;

/// Clone helper for erased offset trait objects.
pub trait OffsetClone {
    fn clone_box(&self) -> Box<dyn Offset>;
}

impl<T> OffsetClone for T
where
    T: Offset + Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn Offset> {
        Box::new(self.clone())
    }
}

/// Describes a durable source position that can be stored in a checkpoint.
pub trait Offset: Debug + OffsetClone + Send + Sync {
    fn source_type(&self) -> &str;
    fn encode(&self) -> Result<Vec<u8>>;
}

impl Clone for Box<dyn Offset> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
