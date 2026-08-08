//! Core event, error, offset, observability, and runtime primitives.

pub(crate) mod durability;
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
mod secret;
mod transport;
#[cfg(feature = "tls")]
pub(crate) mod transport_tls;

pub use error::{
    render_error_chain, Error, ErrorChain, ErrorKind, ErrorReport, FingerprintError, Result,
    SourceErrorKind,
};
pub use event::{
    Event, EventBuilder, NoRowWrite, Operation, RowWrite, SnapshotMetadata, SourceMetadata,
    TransactionMetadata, ValidationError, ValidationErrors, EVENT_ENVELOPE_VERSION,
};
pub use idempotency::{
    fingerprint_event_stable, fingerprint_event_transient, EventIdempotencyGuard,
};
pub use logging::StructuredLogger;
pub use observability::{EventTracer, MetricsCollector, NoOpEventTracer, NoOpMetricsCollector};
#[cfg(feature = "metrics")]
pub use otel::{MetricsReport, OTelConfig, OTelEventTracer, OTelMetricsCollector, SpanRecord};
pub use runtime::{
    AckMode, AckToken, CdcRuntime, ConnectionRetryPolicy, EventBatch, HealthVerdict,
    IdempotencyOptions, PostCommitSourceConfirmPolicy, RuntimeAdminSnapshot, RuntimeConfig,
    RuntimeControl, RuntimeObservability, RuntimeOptions, RuntimeSourceConfig, RuntimeState,
    TransactionBoundaryPolicy, TransformErrorPolicy,
};
pub use secret::{SecretProvider, SecretString};
#[cfg(feature = "tls")]
pub use transport::RustlsClientConfig;
pub use transport::TransportConfig;
#[cfg(feature = "tls")]
pub use transport_tls::rustls_client_config;

use std::fmt::Debug;

/// Clone helper for erased offset trait objects.
pub trait OffsetClone {
    /// Clone into a boxed trait object.
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
    /// Connector namespace this offset belongs to, e.g. `"postgres"`.
    ///
    /// Selects the checkpoint file name and guards against resuming one connector's
    /// position on another — MySQL and MariaDB GTID formats are mutually unintelligible,
    /// so mixing them silently resumes at a wrong place.
    fn source_type(&self) -> &str;

    /// Serialize to the bytes the checkpoint store persists.
    ///
    /// Must round-trip: whatever `encode` produces has to be decodable back into a
    /// resumable position by the connector that wrote it.
    fn encode(&self) -> Result<Vec<u8>>;
}

impl Clone for Box<dyn Offset> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
