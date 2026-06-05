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

/// Coarse error category for policy decisions.
///
/// Returned by [`Error::kind`]. Callers should match on this enum rather than
/// on the raw [`Error`] variant to write policy logic that is robust to new
/// error variants being added in minor releases.
///
/// # Example
///
/// ```rust
/// use rustcdc::core::{Error, ErrorKind};
///
/// let err = Error::SourceError("connection reset".into());
/// match err.kind() {
///     ErrorKind::Transient => println!("retry with backoff"),
///     ErrorKind::Terminal => println!("escalate to operator"),
///     ErrorKind::Configuration => println!("fix config and restart"),
///     _ => println!("unknown kind — treat as terminal"),
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Transient source or network condition — safe to retry with backoff.
    ///
    /// Covers [`Error::SourceError`] and [`Error::TimeoutError`].
    Transient,
    /// Permanent failure that retrying will not resolve.
    ///
    /// Covers [`Error::Unrecoverable`], [`Error::CheckpointError`],
    /// [`Error::SchemaError`], [`Error::StateError`], [`Error::TransformError`],
    /// [`Error::SerializationError`], [`Error::IoError`], and
    /// [`Error::ValidationError`].
    Terminal,
    /// Invalid or incomplete configuration.
    ///
    /// Covers [`Error::ConfigError`] and [`Error::NotImplemented`].
    Configuration,
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

    /// Returns the coarse [`ErrorKind`] category for policy decisions.
    ///
    /// Use this to implement retry logic, circuit-breaker policy, and alerting
    /// without matching on individual [`Error`] variants. Prefer `kind()` over
    /// `is_recoverable()` when you need finer-grained routing between transient,
    /// terminal, and configuration failures.
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::SourceError(_) | Self::TimeoutError(_) => ErrorKind::Transient,
            Self::ConfigError(_) | Self::NotImplemented(_) => ErrorKind::Configuration,
            Self::Unrecoverable(_)
            | Self::CheckpointError(_)
            | Self::SchemaError(_)
            | Self::StateError(_)
            | Self::TransformError(_)
            | Self::SerializationError(_)
            | Self::IoError(_)
            | Self::ValidationError(_) => ErrorKind::Terminal,
        }
    }

    /// Returns whether the error represents a transient source condition worth retrying.
    ///
    /// Equivalent to `self.kind() == ErrorKind::Transient`. Prefer [`Error::kind`]
    /// for new code that needs to distinguish transient from terminal from
    /// configuration failures.
    ///
    /// Only [`Error::SourceError`] and [`Error::TimeoutError`] are considered
    /// recoverable — these are the only variants that can arise from a transient
    /// network or server condition and are meaningful to retry with backoff.
    ///
    /// All other variants (config, validation, serialization, state, etc.) indicate
    /// a permanent problem that retrying will not resolve.
    pub fn is_recoverable(&self) -> bool {
        self.kind() == ErrorKind::Transient
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::SerializationError(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, ErrorKind};

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
    fn error_kind_classifies_all_variants() {
        assert_eq!(Error::SourceError("x".into()).kind(), ErrorKind::Transient);
        assert_eq!(Error::TimeoutError("x".into()).kind(), ErrorKind::Transient);
        assert_eq!(
            Error::ConfigError("x".into()).kind(),
            ErrorKind::Configuration
        );
        assert_eq!(
            Error::NotImplemented("x".into()).kind(),
            ErrorKind::Configuration
        );
        assert_eq!(
            Error::CheckpointError("x".into()).kind(),
            ErrorKind::Terminal
        );
        assert_eq!(Error::SchemaError("x".into()).kind(), ErrorKind::Terminal);
        assert_eq!(Error::StateError("x".into()).kind(), ErrorKind::Terminal);
        assert_eq!(
            Error::TransformError("x".into()).kind(),
            ErrorKind::Terminal
        );
        assert_eq!(Error::Unrecoverable("x".into()).kind(), ErrorKind::Terminal);
        assert_eq!(Error::ValidationError(vec![]).kind(), ErrorKind::Terminal);
        assert_eq!(
            Error::SerializationError("x".into()).kind(),
            ErrorKind::Terminal
        );
    }

    #[test]
    fn is_recoverable_is_consistent_with_kind() {
        let errors = [
            Error::SourceError("x".into()),
            Error::TimeoutError("x".into()),
            Error::ConfigError("x".into()),
            Error::CheckpointError("x".into()),
            Error::StateError("x".into()),
        ];
        for err in &errors {
            assert_eq!(err.is_recoverable(), err.kind() == ErrorKind::Transient);
        }
    }

    #[test]
    fn serde_errors_map_to_serialization_errors() {
        let error = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        assert!(matches!(Error::from(error), Error::SerializationError(_)));
    }
}
