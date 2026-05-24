//! Error types used across the crate.

/// Shared result type for cdc-rs.
pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error type for cdc-rs operations.
#[derive(Debug, thiserror::Error)]
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
    /// Returns whether the error is safe to retry without human intervention.
    pub fn is_recoverable(&self) -> bool {
        !matches!(self, Self::Unrecoverable(_))
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
        assert!(Error::ConfigError("invalid".into()).is_recoverable());
        assert!(!Error::Unrecoverable("boom".into()).is_recoverable());
    }

    #[test]
    fn serde_errors_map_to_serialization_errors() {
        let error = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        assert!(matches!(Error::from(error), Error::SerializationError(_)));
    }
}
