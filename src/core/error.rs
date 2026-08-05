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
    ///
    /// `Unknown` is treated as recoverable, matching the classification of an
    /// unclassified [`Error::SourceError`]: the overwhelmingly common source failure
    /// is a transient network condition, and treating an unclassified one as terminal
    /// would shut a pipeline down on a blip. Classify explicitly to get the stricter
    /// behaviour — `AuthFailed`, `SchemaMismatch` and `SlotNotFound` all need an
    /// operator, and retrying them just delays the page.
    pub fn is_recoverable(self) -> bool {
        matches!(
            self,
            Self::NetworkTransient | Self::QuotaExceeded | Self::Unknown
        )
    }

    /// Stable lowercase identifier, for metric labels and structured log fields.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NetworkTransient => "network_transient",
            Self::AuthFailed => "auth_failed",
            Self::SchemaMismatch => "schema_mismatch",
            Self::SlotNotFound => "slot_not_found",
            Self::QuotaExceeded => "quota_exceeded",
            Self::Unknown => "unknown",
        }
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
    /// The runtime cannot accept more events until the caller acknowledges what it
    /// already has.
    ///
    /// **This is normal flow control, not a failure.** Retry after calling
    /// [`commit_ack`](crate::CdcRuntime::commit_ack) on the outstanding batch; the
    /// same call will then succeed.
    ///
    /// It is a distinct kind because the alternative misleads: backpressure used to
    /// surface as [`Error::StateError`] and therefore as [`ErrorKind::Terminal`],
    /// documented as *"a permanent problem that retrying will not resolve"*. An
    /// embedder following that guidance shuts the pipeline down on entirely routine
    /// flow control.
    ///
    /// Covers [`Error::Backpressure`].
    Backpressure,
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
    /// Source-specific failure with no cause classification.
    ///
    /// Treated as [`ErrorKind::Transient`]: the common source failure is a network
    /// blip, and the safe default for an unclassified one is to retry. Use
    /// [`Error::source_error`] when the cause *is* known — an auth failure or a
    /// missing replication slot retried with backoff just delays the page.
    #[error("source error: {0}")]
    SourceError(String),
    /// Source failure with a machine-readable cause.
    ///
    /// Constructed by [`Error::source_error`]. Read the cause back with
    /// [`Error::source_kind`] — it drives retry, alerting and circuit-breaker
    /// decisions without parsing the message.
    #[error("source error [{}]: {message}", kind.as_str())]
    ClassifiedSourceError {
        /// Machine-readable cause classification.
        kind: SourceErrorKind,
        /// Human-readable detail.
        message: String,
    },
    /// An error with additional context, preserving the underlying cause.
    ///
    /// Built by [`Error::context`]. Unlike `format!("...: {e}")` — which flattens the
    /// cause into a string and destroys it — this keeps the original error reachable
    /// through [`std::error::Error::source`], so chain walking and downcasting work.
    ///
    /// **`Display` shows only the outermost context.** That is the `thiserror`
    /// convention, and it means `tracing::error!("{e}")` on a contextual error prints
    /// *"acknowledging batch 7"* and nothing about the disk being full. Use
    /// [`Error::report`] to render the whole chain — that is what an operator needs to
    /// see, and it is what this crate uses at its own logging sites.
    ///
    /// [`Error::kind`] delegates to the innermost cause, so adding context never
    /// changes a retry decision.
    #[error("{context}")]
    Context {
        /// What was being attempted.
        context: String,
        /// The error that caused it.
        #[source]
        source: Box<Error>,
    },
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
    /// The runtime's in-flight buffer is full; acknowledge outstanding events first.
    ///
    /// Normal flow control, not a failure — see [`ErrorKind::Backpressure`].
    #[error("backpressure: {0}")]
    Backpressure(String),
    /// Invalid runtime state or illegal transition.
    #[error("state error: {0}")]
    StateError(String),
    /// Failure while applying a transform stage.
    #[error("transform error: {0}")]
    TransformError(String),
    /// Feature not implemented in the current phase.
    #[error("not implemented: {0}")]
    NotImplemented(String),
    /// Source slot/cursor confirmation failed after a durable checkpoint commit.
    ///
    /// The checkpoint **is safe** — replay from the last checkpoint is correct and
    /// will not lose events. Only the source-side replication slot advancement failed.
    ///
    /// Use this variant to distinguish post-commit confirmation failures from
    /// pre-commit source errors that require immediate rollback/replay attention.
    ///
    /// # Handling
    ///
    /// - Under [`crate::PostCommitSourceConfirmPolicy::FailFast`] this error is returned
    ///   to the caller of `commit_ack`. The runtime remains usable — subsequent
    ///   calls to `poll_event_batch` will trigger reconnection and the slot will
    ///   be re-confirmed on the next successful poll cycle.
    /// - Under [`crate::PostCommitSourceConfirmPolicy::Continue`] this variant is never
    ///   returned; failures are logged and silently skipped.
    #[error("post-commit confirm failed (checkpoint is safe — replay is safe): {detail}")]
    PostCommitConfirmFailed {
        /// Always `true`: the checkpoint was durably committed before confirmation
        /// was attempted. Replay from the last checkpoint is safe.
        checkpoint_safe: bool,
        /// Human-readable summary of all confirmation failures in this commit.
        detail: String,
    },
}

impl Error {
    /// Construct a source error with an explicit [`SourceErrorKind`].
    ///
    /// The kind is stored, not formatted into the message, so
    /// [`Error::source_kind`] can read it back. That is the whole point: an embedder
    /// deciding whether to retry, page, or open a circuit breaker must not have to
    /// parse a human-readable string to do it.
    ///
    /// ```
    /// use rustcdc::core::{Error, ErrorKind, SourceErrorKind};
    ///
    /// let error = Error::source_error(SourceErrorKind::AuthFailed, "password rejected");
    /// assert_eq!(error.source_kind(), Some(SourceErrorKind::AuthFailed));
    /// // An auth failure is not worth retrying — it needs an operator.
    /// assert_eq!(error.kind(), ErrorKind::Terminal);
    /// ```
    pub fn source_error(kind: SourceErrorKind, message: impl std::fmt::Display) -> Self {
        Self::ClassifiedSourceError {
            kind,
            message: message.to_string(),
        }
    }

    /// The [`SourceErrorKind`] of a classified source error, following context wrappers.
    ///
    /// Returns `None` for unclassified [`Error::SourceError`] and for every non-source
    /// variant.
    pub fn source_kind(&self) -> Option<SourceErrorKind> {
        match self {
            Self::ClassifiedSourceError { kind, .. } => Some(*kind),
            Self::Context { source, .. } => source.source_kind(),
            _ => None,
        }
    }

    /// Wrap this error with context, keeping the original reachable as a cause.
    ///
    /// Prefer this over `format!("while doing X: {error}")`, which flattens the cause
    /// into a string and makes it unrecoverable for programmatic handling.
    ///
    /// ```
    /// use rustcdc::core::{Error, ErrorKind};
    ///
    /// let error = Error::SourceError("connection reset".into())
    ///     .context("resuming the stream after reconnect");
    ///
    /// assert!(error.to_string().contains("resuming the stream"));
    /// // The cause survives...
    /// assert!(std::error::Error::source(&error).is_some());
    /// // ...and context never changes the retry decision.
    /// assert_eq!(error.kind(), ErrorKind::Transient);
    /// ```
    #[must_use]
    pub fn context(self, context: impl std::fmt::Display) -> Self {
        Self::Context {
            context: context.to_string(),
            source: Box::new(self),
        }
    }

    /// The innermost error in a [`Error::Context`] chain, or `self`.
    pub fn root_cause(&self) -> &Self {
        match self {
            Self::Context { source, .. } => source.root_cause(),
            other => other,
        }
    }

    /// Iterate this error and every cause beneath it, outermost first.
    ///
    /// ```
    /// use rustcdc::Error;
    ///
    /// let error = Error::CheckpointError("disk full".into())
    ///     .context("saving the commit barrier")
    ///     .context("acknowledging batch 7");
    ///
    /// let layers: Vec<String> = error.chain().map(ToString::to_string).collect();
    /// assert_eq!(
    ///     layers,
    ///     [
    ///         "acknowledging batch 7",
    ///         "saving the commit barrier",
    ///         "checkpoint error: disk full",
    ///     ]
    /// );
    /// ```
    pub fn chain(&self) -> ErrorChain<'_> {
        ErrorChain { next: Some(self) }
    }

    /// Render this error and its full cause chain as one line.
    ///
    /// `Display` on the error itself shows only the outermost layer, so a contextual
    /// error logged with `{e}` hides the very thing an operator needs. `report()` joins
    /// the chain with `": "`, innermost cause last:
    ///
    /// ```
    /// use rustcdc::Error;
    ///
    /// let error = Error::CheckpointError("disk full".into())
    ///     .context("saving the commit barrier")
    ///     .context("acknowledging batch 7");
    ///
    /// assert_eq!(error.to_string(), "acknowledging batch 7");
    /// assert_eq!(
    ///     error.report().to_string(),
    ///     "acknowledging batch 7: saving the commit barrier: checkpoint error: disk full",
    /// );
    /// ```
    ///
    /// Prefer this at every site that logs an error for a human:
    /// `tracing::error!(error = %err.report(), "…")`.
    pub fn report(&self) -> ErrorReport<'_> {
        ErrorReport { error: self }
    }

    /// Returns the coarse [`ErrorKind`] category for policy decisions.
    ///
    /// Use this to implement retry logic, circuit-breaker policy, and alerting
    /// without matching on individual [`Error`] variants. Prefer `kind()` over
    /// `is_recoverable()` when you need finer-grained routing between transient,
    /// terminal, and configuration failures.
    ///
    /// Context wrappers are transparent: the kind always comes from the root cause,
    /// so adding context can never turn a retryable failure into a fatal one.
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Context { source, .. } => source.kind(),
            Self::SourceError(_) | Self::TimeoutError(_) => ErrorKind::Transient,
            Self::ClassifiedSourceError { kind, .. } => {
                if kind.is_recoverable() {
                    ErrorKind::Transient
                } else {
                    ErrorKind::Terminal
                }
            }
            Self::Backpressure(_) => ErrorKind::Backpressure,
            Self::ConfigError(_) | Self::NotImplemented(_) => ErrorKind::Configuration,
            Self::Unrecoverable(_)
            | Self::CheckpointError(_)
            | Self::SchemaError(_)
            | Self::StateError(_)
            | Self::TransformError(_)
            | Self::SerializationError(_)
            | Self::IoError(_)
            | Self::ValidationError(_)
            | Self::PostCommitConfirmFailed { .. } => ErrorKind::Terminal,
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
    fn post_commit_confirm_failed_is_terminal_and_not_recoverable() {
        let err = Error::PostCommitConfirmFailed {
            checkpoint_safe: true,
            detail: "slot advance failed".into(),
        };
        assert_eq!(err.kind(), ErrorKind::Terminal);
        assert!(!err.is_recoverable());
        // Display must not expose the raw slot name or LSN
        let display = err.to_string();
        assert!(display.contains("checkpoint is safe"));
        assert!(display.contains("replay is safe"));
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

    #[test]
    fn a_classified_source_error_exposes_its_kind_without_string_parsing() {
        // The old constructor formatted the kind into the message and offered no
        // accessor, so the documented promise — "drive retry policy without parsing
        // free-form error strings" — was unachievable by construction.
        use super::SourceErrorKind;

        let error = Error::source_error(SourceErrorKind::SlotNotFound, "slot 'cdc' is gone");
        assert_eq!(error.source_kind(), Some(SourceErrorKind::SlotNotFound));
        assert!(error.to_string().contains("slot_not_found"));
        assert!(error.to_string().contains("slot 'cdc' is gone"));
    }

    #[test]
    fn non_retryable_source_causes_are_terminal_not_transient() {
        // Retrying an auth failure or a dropped replication slot with backoff cannot
        // succeed; it only delays the operator page.
        use super::SourceErrorKind::*;

        for kind in [AuthFailed, SchemaMismatch, SlotNotFound] {
            let error = Error::source_error(kind, "x");
            assert_eq!(error.kind(), ErrorKind::Terminal, "{kind:?}");
            assert!(!error.is_recoverable(), "{kind:?}");
        }
        for kind in [NetworkTransient, QuotaExceeded, Unknown] {
            let error = Error::source_error(kind, "x");
            assert_eq!(error.kind(), ErrorKind::Transient, "{kind:?}");
        }
    }

    #[test]
    fn context_preserves_the_cause_and_the_retry_decision() {
        let error = Error::CheckpointError("disk full".into())
            .context("saving the commit barrier")
            .context("acknowledging batch 7");

        assert_eq!(error.to_string(), "acknowledging batch 7");
        assert_eq!(
            error.kind(),
            ErrorKind::Terminal,
            "context must not change classification"
        );
        assert!(matches!(error.root_cause(), Error::CheckpointError(_)));

        // The chain is walkable via std::error::Error::source.
        let mut depth = 0;
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        while let Some(err) = current {
            depth += 1;
            current = err.source();
        }
        assert_eq!(depth, 3, "two context frames plus the root cause");
    }

    #[test]
    fn source_kind_is_visible_through_context_frames() {
        use super::SourceErrorKind;

        let error = Error::source_error(SourceErrorKind::AuthFailed, "denied")
            .context("connecting to the source");
        assert_eq!(error.source_kind(), Some(SourceErrorKind::AuthFailed));
        assert_eq!(error.kind(), ErrorKind::Terminal);
    }
}

/// Iterator over an [`Error`] and its causes, outermost first.
///
/// Created by [`Error::chain`].
#[derive(Debug, Clone)]
pub struct ErrorChain<'a> {
    next: Option<&'a Error>,
}

impl<'a> Iterator for ErrorChain<'a> {
    type Item = &'a Error;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.next?;
        self.next = match current {
            Error::Context { source, .. } => Some(source.as_ref()),
            _ => None,
        };
        Some(current)
    }
}

impl std::iter::FusedIterator for ErrorChain<'_> {}

/// One-line rendering of an [`Error`] and its full cause chain.
///
/// Created by [`Error::report`]. See that method for why `Display` on the error alone is
/// not enough.
#[derive(Debug, Clone, Copy)]
pub struct ErrorReport<'a> {
    error: &'a Error,
}

impl std::fmt::Display for ErrorReport<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for layer in self.error.chain() {
            if !first {
                formatter.write_str(": ")?;
            }
            write!(formatter, "{layer}")?;
            first = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod report_tests {
    use super::*;

    #[test]
    fn a_bare_error_reports_as_itself() {
        let error = Error::CheckpointError("disk full".into());
        assert_eq!(error.report().to_string(), "checkpoint error: disk full");
        assert_eq!(error.chain().count(), 1);
    }

    #[test]
    fn display_alone_hides_the_cause_which_is_why_report_exists() {
        // This asserts the defect that motivated `report()`: `tracing::error!("{e}")` on a
        // contextual error prints the context and nothing about what actually went wrong.
        let error = Error::CheckpointError("disk full".into()).context("saving the barrier");
        assert_eq!(error.to_string(), "saving the barrier");
        assert!(
            !error.to_string().contains("disk full"),
            "if Display ever starts including the cause, `report()` would double-print it"
        );
        assert!(error.report().to_string().contains("disk full"));
    }

    #[test]
    fn the_alternate_flag_does_not_walk_the_chain() {
        // The doc comment used to claim `{:#}`-style chain printers work. They do not:
        // thiserror does not implement alternate-flag chaining, so `{:#}` is identical to
        // `{}`. Pinned so the claim cannot quietly return.
        let error = Error::CheckpointError("disk full".into()).context("saving the barrier");
        assert_eq!(format!("{error:#}"), format!("{error}"));
    }

    #[test]
    fn the_chain_is_ordered_outermost_first() {
        let error = Error::SourceError("connection reset".into())
            .context("reading the binlog")
            .context("polling for events");
        let layers: Vec<String> = error.chain().map(ToString::to_string).collect();
        assert_eq!(layers.len(), 3);
        assert_eq!(layers[0], "polling for events");
        assert!(layers[2].contains("connection reset"));
    }

    #[test]
    fn report_preserves_the_retry_classification() {
        // Context must never change a retry decision — a rendering helper least of all.
        let transient = Error::SourceError("connection reset".into());
        let kind = transient.kind();
        let wrapped = transient.context("reading the binlog");
        assert_eq!(wrapped.kind(), kind);
        assert!(wrapped.report().to_string().contains("connection reset"));
    }

    #[test]
    fn the_chain_terminates_on_a_non_context_error() {
        // A missing terminator would loop forever on the innermost error.
        let error = Error::ConfigError("bad port".into())
            .context("a")
            .context("b");
        assert_eq!(error.chain().count(), 3);
    }
}

/// Render a foreign error together with its own cause chain.
///
/// # Why this exists
///
/// Connector code flattens third-party errors into a message string. Many of those types
/// have a `Display` that names only the operation and keeps the real cause behind
/// [`std::error::Error::source`] — `tokio_postgres::Error` renders as
/// *"error connecting to server"* whether the socket was refused, the DNS lookup failed,
/// or the handshake timed out. Formatting it with `{error}` throws away the one detail an
/// operator needs.
///
/// This walks the chain and joins it, so the flattened message keeps what the type's own
/// `Display` dropped.
///
/// ```
/// use rustcdc::core::render_error_chain;
///
/// #[derive(Debug)]
/// struct Outer;
/// impl std::fmt::Display for Outer {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         f.write_str("error connecting to server")
///     }
/// }
/// impl std::error::Error for Outer {
///     fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
///         Some(&Inner)
///     }
/// }
///
/// #[derive(Debug)]
/// struct Inner;
/// impl std::fmt::Display for Inner {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         f.write_str("Connection refused (os error 61)")
///     }
/// }
/// impl std::error::Error for Inner {}
///
/// assert_eq!(
///     render_error_chain(&Outer),
///     "error connecting to server: Connection refused (os error 61)",
/// );
/// ```
pub fn render_error_chain(error: &dyn std::error::Error) -> String {
    let mut rendered = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        // Some libraries already fold the cause into their own Display; repeating it
        // produces "Input/output error: Input/output error: …".
        if !rendered.ends_with(&text) {
            rendered.push_str(": ");
            rendered.push_str(&text);
        }
        source = cause.source();
    }
    rendered
}

#[cfg(test)]
mod render_chain_tests {
    use super::render_error_chain;
    use std::error::Error as StdError;
    use std::fmt;

    #[derive(Debug)]
    struct Layer {
        message: &'static str,
        cause: Option<Box<Layer>>,
    }

    impl fmt::Display for Layer {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.message)
        }
    }

    impl StdError for Layer {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            self.cause
                .as_ref()
                .map(|cause| cause.as_ref() as &(dyn StdError + 'static))
        }
    }

    fn layer(message: &'static str, cause: Option<Layer>) -> Layer {
        Layer {
            message,
            cause: cause.map(Box::new),
        }
    }

    #[test]
    fn a_hidden_cause_is_surfaced() {
        // The defect this exists for: `tokio_postgres::Error` displays only the operation.
        let error = layer(
            "error connecting to server",
            Some(layer("Connection refused (os error 61)", None)),
        );
        assert_eq!(
            render_error_chain(&error),
            "error connecting to server: Connection refused (os error 61)"
        );
    }

    #[test]
    fn an_error_with_no_cause_renders_unchanged() {
        assert_eq!(
            render_error_chain(&layer("plain failure", None)),
            "plain failure"
        );
    }

    #[test]
    fn a_cause_already_folded_into_display_is_not_repeated() {
        // `mysql_async` renders "Input/output error: <io>" and also exposes the io error
        // as its source; naive joining yields "Input/output error: X: X".
        let error = layer(
            "Input/output error: Connection refused",
            Some(layer("Connection refused", None)),
        );
        assert_eq!(
            render_error_chain(&error),
            "Input/output error: Connection refused"
        );
    }

    #[test]
    fn deep_chains_are_walked_to_the_bottom() {
        let error = layer("a", Some(layer("b", Some(layer("c", None)))));
        assert_eq!(render_error_chain(&error), "a: b: c");
    }
}
