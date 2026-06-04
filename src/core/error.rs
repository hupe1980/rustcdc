//! Error types used across the crate.

/// Shared result type for rustcdc.
pub type Result<T> = std::result::Result<T, Error>;

/// Classifies the root cause of a [`Error::SourceError`].
///
/// Use this to drive retry policy, alerting, and circuit-breaker decisions
/// without parsing free-form error strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SourceErrorKind {
    /// Transient network interruption (TCP reset, timeout, short disconnect).
    NetworkTransient,
    /// Authentication or authorisation failure (wrong credentials, privilege revoked).
    AuthFailed,
    /// Source schema changed in an incompatible way.
    SchemaMismatch,
    /// Replication slot or equivalent source-side tracking object not found.
    SlotNotFound,
    /// Source quota exceeded (e.g. max connections, WAL limits).
    QuotaExceeded,
    /// Error could not be classified into one of the above categories.
    Unknown,
}

impl SourceErrorKind {
    /// Returns `true` if this kind represents a condition that may resolve on retry.
    pub fn is_recoverable(self) -> bool {
        matches!(self, Self::NetworkTransient | Self::QuotaExceeded)
    }
}

/// Dedicated error type for event fingerprint failures.
///
/// Returned by [`fingerprint_event_stable`] and [`fingerprint_event_transient`]
/// so callers can distinguish empty-field validation from serialisation failures
/// without inspecting free-form strings.
///
/// [`fingerprint_event_stable`]: crate::core::idempotency::fingerprint_event_stable
/// [`fingerprint_event_transient`]: crate::core::idempotency::fingerprint_event_transient
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FingerprintError {
    /// The event's `source.source_name` field is empty or whitespace.
    #[error("cannot fingerprint event with empty source.source_name")]
    EmptySourceName,
    /// The event's `source.offset` field is empty or whitespace.
    #[error("cannot fingerprint event with empty source.offset")]
    EmptyOffset,
    /// Serialising the event payload for hashing failed.
    #[error("fingerprint serialisation failed: {0}")]
    SerializationFailed(#[from] serde_json::Error),
}

impl From<FingerprintError> for Error {
    fn from(err: FingerprintError) -> Self {
        Self::ValidationError(vec![err.to_string()])
    }
}

/// Top-level error type for rustcdc operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Source-specific failure.
    #[error("source error: {0}")]
    SourceError(String),
    /// Failure while reading or writing checkpoint state.
    #[error("checkpoint error: {0}")]
    CheckpointError(String),
    /// Schema lookup or DDL processing failure.
    #[error("schema error: {0}")]
    SchemaError(String),
    /// Validation failures with field-scoped details.
    #[error("validation error(s): {0:?}")]
    ValidationError(Vec<String>),
    /// Configuration is invalid or incomplete.
    #[error("configuration error: {0}")]
    ConfigError(String),
    /// I/O failure bubbled up from the standard library.
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    /// Serialization or deserialization failure.
    #[error("serialization error: {0}")]
    SerializationError(String),
    /// Operation exceeded its configured timeout.
    #[error("timeout error: {0}")]
    TimeoutError(String),
    /// Fatal state that requires restart or operator intervention.
    #[error("unrecoverable error: {0}")]
    Unrecoverable(String),
    /// Invalid runtime state or illegal transition.
    #[error("state error: {0}")]
    StateError(String),
    /// Failure while applying a transform stage.
    #[error("transform error: {0}")]
    TransformError(String),
    /// Feature not implemented in the current phase.
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

impl Error {
    /// Construct a [`Error::SourceError`] with an explicit [`SourceErrorKind`] prefix.
    ///
    /// The kind is embedded in the message string so the human-readable form
    /// retains full context while still driving `is_recoverable()` correctly.
    pub fn source_error(kind: SourceErrorKind, message: impl std::fmt::Display) -> Self {
        Self::SourceError(format!("[{kind:?}] {message}"))
    }

    /// Returns whether the error represents a transient source condition worth retrying.
    ///
    /// Only [`Error::SourceError`] and [`Error::TimeoutError`] are considered
    /// recoverable — these are the only variants that can arise from a transient
    /// network or server condition and are meaningful to retry with backoff.
    ///
    /// All other variants (config, validation, serialization, state, etc.) indicate
    /// a permanent problem that retrying will not resolve.
    pub fn is_recoverable(&self) -> bool {
        matches!(self, Self::SourceError(_) | Self::TimeoutError(_))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::SerializationError(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn recoverable_flag_matches_contract() {
        assert!(Error::SourceError("conn reset".into()).is_recoverable());
        assert!(Error::TimeoutError("deadline exceeded".into()).is_recoverable());
        assert!(!Error::ConfigError("invalid".into()).is_recoverable());
        assert!(!Error::ValidationError(vec!["bad field".into()]).is_recoverable());
        assert!(!Error::CheckpointError("io".into()).is_recoverable());
        assert!(!Error::SchemaError("missing".into()).is_recoverable());
        assert!(!Error::StateError("illegal transition".into()).is_recoverable());
        assert!(!Error::TransformError("crash".into()).is_recoverable());
        assert!(!Error::Unrecoverable("boom".into()).is_recoverable());
    }

    #[test]
    fn serde_errors_map_to_serialization_errors() {
        let error = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        assert!(matches!(Error::from(error), Error::SerializationError(_)));
    }
}
