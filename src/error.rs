use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("configuration error: {0}")]
    Config(Box<ConfigError>),

    /// A failure raised inside rustcdc.
    ///
    /// Rendered through `Error::report()` rather than `Display`. `Display` on a
    /// contextual rustcdc error shows only the outermost layer — logging one with
    /// `{e}` printed *"acknowledging batch 7"* and nothing about the disk being full,
    /// so adding context actively hid the cause. `report()` joins the whole chain,
    /// innermost cause last.
    #[error("runtime error: {}", .0.report())]
    Runtime(#[from] rustcdc::core::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP client error: {0}")]
    Http(String),

    /// A sink operation exceeded its timeout.
    ///
    /// Distinct from [`AppError::Other`] because the batch loop must be able to tell a
    /// transient sink stall (retry it) from a poison event (never retry it) without
    /// pattern-matching on message text.
    #[error("{0}")]
    SinkTimeout(String),

    /// An encoded event exceeded `runtime.max_event_bytes`.
    ///
    /// Never recoverable: the same event will be the same size on every attempt.
    /// Retrying it is a poison-pill loop that makes no forward progress.
    #[error("{0}")]
    EventTooLarge(String),

    /// The sink rejected **this record** for a reason that will not change.
    ///
    /// The broker-side analogue of [`AppError::EventTooLarge`]: `RecordTooLarge`,
    /// `InvalidRecord`, `InvalidTimestamp` and friends are properties of the payload,
    /// so the same bytes fail identically forever. Quarantining the event and advancing
    /// is the only way the pipeline makes progress.
    ///
    /// Distinct from [`AppError::SinkFatal`] because the two demand opposite responses
    /// — see [`AppError::is_dead_letterable`].
    #[error("{0}")]
    SinkPoisonRecord(String),

    /// The sink failed for a reason that is **not** the record's fault and will not
    /// resolve on retry: bad credentials, a missing topic, a revoked ACL.
    ///
    /// Neither retryable nor quarantinable. Dead-lettering it would drain the entire
    /// change stream into the DLQ one event at a time while reporting success, which is
    /// indistinguishable from data loss. The pipeline must stop and page a human.
    #[error("{0}")]
    SinkFatal(String),

    #[error("{0}")]
    Other(String),
}

impl AppError {
    /// Whether retrying the failed operation could plausibly succeed.
    ///
    /// The source poll path has always consulted `rustcdc::core::Error::is_recoverable`;
    /// the sink delivery path had no equivalent and treated **every** failure as
    /// terminal, so a few-second broker leader election became a process exit, a
    /// restart and a full replay. This is the classifier that lets both halves share
    /// one policy.
    ///
    /// The default for an unclassified error is **not** recoverable. Retrying something
    /// we do not understand risks an infinite loop against a permanent failure, and a
    /// terminal error is at least loud.
    ///
    /// # Flush errors keep their classification (since rustcdc 0.11)
    ///
    /// `TableRouter::send` passes a sink's error through untouched, so send-path failures
    /// have always kept their classification. `flush_all` and `close_all` used to flatten
    /// every branch's error into one string and return `Error::StateError`, which is
    /// `ErrorKind::Terminal`: a broker leader election surfacing during flush became a
    /// process exit, a restart and a full replay, while the identical failure surfacing
    /// from `send` was retried. Which one you got was decided by batch boundaries.
    ///
    /// We refused to work around that by matching on message text — a classifier built on
    /// string contents is worse than one that is conservative — and reported it upstream
    /// instead. 0.11 returns `Error::Aggregate { kind, detail }` from both, where `kind`
    /// is the most severe `ErrorKind` among the branches, so `Error::kind` (and therefore
    /// the `Self::Runtime` arm below) now classifies a fan-out failure by what actually
    /// went wrong. `a_transient_flush_failure_is_retryable` holds that.
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::Runtime(err) => err.is_recoverable(),
            // Transient by nature: a full disk, a reset connection, a stalled sink.
            Self::Io(_) | Self::Http(_) | Self::SinkTimeout(_) => true,
            // Deterministic: the same input fails identically every time.
            Self::Config(_) | Self::EventTooLarge(_) => false,
            Self::SinkPoisonRecord(_) | Self::SinkFatal(_) => false,
            Self::Other(_) => false,
        }
    }

    /// Whether quarantining **this event** and advancing past it is a sound response.
    ///
    /// `!is_recoverable()` used to be the sole test, which conflated two failures that
    /// want opposite handling. "This record is malformed" and "your credentials are
    /// wrong" are both permanent, but only the first is a property of the event. Treating
    /// the second as dead-letterable drains the whole change stream into the DLQ, one
    /// event at a time, while every health check reports the pipeline as running — the
    /// silent data loss the DLQ exists to prevent, delivered by the DLQ itself.
    ///
    /// Recoverable errors are excluded because the caller retries them first; the
    /// question only arises once retrying is off the table. The default for an
    /// unclassified error is **not** dead-letterable, for the same reason
    /// [`AppError::is_recoverable`] defaults to false: halting is loud, and quarantine
    /// is quiet.
    pub fn is_dead_letterable(&self) -> bool {
        match self {
            // Record-scoped: the payload itself is the problem.
            Self::EventTooLarge(_) | Self::SinkPoisonRecord(_) => true,
            // Pipeline-scoped, or not understood.
            Self::SinkFatal(_) | Self::Config(_) | Self::Other(_) => false,
            Self::Io(_) | Self::Http(_) | Self::SinkTimeout(_) => false,
            // A rustcdc `ValidationError` is the upstream spelling of "this event is
            // malformed"; every other terminal kind is environmental.
            Self::Runtime(err) => {
                matches!(err, rustcdc::core::Error::ValidationError(_))
            }
        }
    }
}

impl From<ConfigError> for AppError {
    fn from(value: ConfigError) -> Self {
        Self::Config(Box::new(value))
    }
}

impl From<clap::error::Error> for AppError {
    fn from(e: clap::error::Error) -> Self {
        AppError::Other(e.to_string())
    }
}

impl From<AppError> for rustcdc::core::Error {
    /// Convert back into a rustcdc error **without losing its classification**.
    ///
    /// `Error::kind()` is what drives the runtime's retry, circuit-breaker and
    /// alerting decisions. Flattening every failure into `SourceError` — which
    /// classifies as `Transient`, "safe to retry with backoff" — makes the runtime
    /// retry a `ConfigError` forever and treats an exhausted-retries failure as
    /// something worth retrying. A rustcdc error is therefore passed through as-is,
    /// and only genuinely local failures are wrapped.
    fn from(value: AppError) -> Self {
        match value {
            AppError::Runtime(inner) => inner,
            AppError::Config(inner) => rustcdc::core::Error::ConfigError(inner.to_string()),
            AppError::Io(inner) => rustcdc::core::Error::StateError(inner.to_string()),
            // `ValidationError` is Terminal upstream. `SourceError` — the old catch-all
            // — is Transient, so an event that is permanently too large came back as
            // "retry me", and the batch loop dutifully retried a failure that is the
            // same size every time.
            AppError::EventTooLarge(message) => {
                rustcdc::core::Error::ValidationError(vec![message])
            }
            // Terminal upstream too, which is wrong for a timeout; `TimeoutError` is the
            // Transient variant that matches what this actually is.
            AppError::SinkTimeout(message) => rustcdc::core::Error::TimeoutError(message),
            // Both are Terminal upstream, which is correct for both: neither is worth a
            // retry. They are kept apart on *this* side of the boundary because only
            // `is_dead_letterable` distinguishes them, and it reads `AppError`.
            AppError::SinkPoisonRecord(message) => {
                rustcdc::core::Error::ValidationError(vec![message])
            }
            AppError::SinkFatal(message) => rustcdc::core::Error::Unrecoverable(message),
            other => rustcdc::core::Error::SourceError(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AppError;

    /// A rustcdc error carrying context must log its root cause, not just the
    /// outermost layer — that layer is the one an operator already knows about.
    #[test]
    fn runtime_error_display_includes_the_full_cause_chain() {
        let inner = rustcdc::core::Error::StateError("No space left on device".to_string());
        let contextual = inner.context("acknowledging batch 7");
        let rendered = AppError::Runtime(contextual).to_string();

        assert!(
            rendered.contains("acknowledging batch 7"),
            "context must survive: {rendered}"
        );
        assert!(
            rendered.contains("No space left on device"),
            "root cause must survive: {rendered}"
        );
    }

    /// A broker hiccup during `flush_all` must be retried, not treated as fatal.
    ///
    /// This is the property that used to fail. `flush_all` aggregated every branch's
    /// error into `StateError`, which is Terminal, so the same reset connection was
    /// retried when it surfaced from `send` and fatal when it surfaced from `flush` —
    /// decided by where the batch boundary happened to fall. Since rustcdc 0.11 the
    /// aggregate carries the most severe `ErrorKind` among its branches instead.
    #[test]
    fn a_transient_flush_failure_is_retryable() {
        use rustcdc::core::Error as RtError;

        let aggregated = RtError::aggregate(vec![(
            "route 'public.*'".to_string(),
            RtError::SourceError("connection reset by peer".to_string()),
        )])
        .expect_err("a failure list must aggregate into an error");

        assert!(
            AppError::Runtime(aggregated).is_recoverable(),
            "a transient failure must stay retryable after aggregation"
        );
    }

    /// …but the severest branch still wins, so a real fault is not retried forever.
    #[test]
    fn a_terminal_branch_makes_the_whole_aggregate_terminal() {
        use rustcdc::core::Error as RtError;

        let aggregated = RtError::aggregate(vec![
            (
                "route 'public.*'".to_string(),
                RtError::SourceError("connection reset by peer".to_string()),
            ),
            (
                "default".to_string(),
                RtError::SchemaError("column 'total' vanished".to_string()),
            ),
        ])
        .expect_err("a failure list must aggregate into an error");

        assert!(
            !AppError::Runtime(aggregated).is_recoverable(),
            "one terminal branch must make the aggregate terminal"
        );
    }
}

// ── Config errors ─────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("no configuration file found; provide one via --config-file")]
    NoConfigFile,

    #[error("failed to load configuration: {0}")]
    Load(Box<figment::Error>),

    #[error("api_version must be \"v1\", got \"{0}\"")]
    InvalidApiVersion(String),

    #[error("no source configured; set exactly one [source.<connector>] block in the config file")]
    NoSourceConfigured,

    #[error("invalid source configuration: {0}")]
    InvalidSource(String),

    #[error("invalid state configuration: {0}")]
    InvalidState(String),
}

impl From<figment::Error> for ConfigError {
    fn from(value: figment::Error) -> Self {
        Self::Load(Box::new(value))
    }
}
