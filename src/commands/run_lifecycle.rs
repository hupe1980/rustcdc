use rustcdc::core::CdcRuntime;
use rustcdc::sink::SinkAdapter;

use crate::{
    admin::{AdminState, InstanceState},
    error::AppError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RuntimeTerminalReasonCode {
    ShutdownSignal,
    AdminServerExit,
    RuntimePollError,
    BatchDeliveryError,
    CheckpointBarrierBeginError,
    CheckpointBarrierCommitError,
    CheckpointCommitError,
    CheckpointReconciliationError,
    RecoverableBreakerEscalation,
    RuntimeStopError,
    SinkCloseError,
}

pub(super) struct RuntimeLoopFailure {
    pub(super) reason: RuntimeTerminalReasonCode,
    pub(super) error: AppError,
}

pub(super) enum RuntimeLoopOutcome {
    Clean(RuntimeTerminalReasonCode),
    Error(RuntimeLoopFailure),
}

impl RuntimeLoopFailure {
    pub(super) fn new(reason: RuntimeTerminalReasonCode, error: AppError) -> Self {
        Self { reason, error }
    }

    pub(super) fn runtime(reason: RuntimeTerminalReasonCode, error: rustcdc::core::Error) -> Self {
        Self {
            reason,
            error: AppError::Runtime(error),
        }
    }
}

impl RuntimeLoopOutcome {
    pub(super) fn error(reason: RuntimeTerminalReasonCode, error: AppError) -> Self {
        Self::Error(RuntimeLoopFailure::new(reason, error))
    }

    pub(super) fn runtime_error(
        reason: RuntimeTerminalReasonCode,
        error: rustcdc::core::Error,
    ) -> Self {
        Self::Error(RuntimeLoopFailure::runtime(reason, error))
    }
}

impl RuntimeTerminalReasonCode {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::ShutdownSignal => "shutdown_signal",
            Self::AdminServerExit => "admin_server_exit",
            Self::RuntimePollError => "runtime_poll_error",
            Self::BatchDeliveryError => "batch_delivery_error",
            Self::CheckpointBarrierBeginError => "checkpoint_barrier_begin_error",
            Self::CheckpointBarrierCommitError => "checkpoint_barrier_commit_error",
            Self::CheckpointCommitError => "checkpoint_commit_error",
            Self::CheckpointReconciliationError => "checkpoint_reconciliation_error",
            Self::RecoverableBreakerEscalation => "recoverable_breaker_escalation",
            Self::RuntimeStopError => "runtime_stop_error",
            Self::SinkCloseError => "sink_close_error",
        }
    }
}

pub(super) async fn finalize_runtime_shutdown(
    runtime: &mut CdcRuntime,
    sink: &mut crate::pipeline::router::TableRouter,
    admin_state: &AdminState,
    loop_outcome: RuntimeLoopOutcome,
) -> Result<(), AppError> {
    let (mut terminal_reason, mut terminal_error) = match loop_outcome {
        RuntimeLoopOutcome::Clean(reason) => (reason, None),
        RuntimeLoopOutcome::Error(failure) => (failure.reason, Some(failure.error)),
    };

    tracing::info!("stopping runtime");
    if let Err(e) = runtime.stop().await {
        tracing::warn!(error = %e, "runtime stop error - forcing");
        if let Err(force_err) = runtime.force_stop().await {
            tracing::error!(error = %force_err, "runtime force_stop failed during shutdown");
            if terminal_error.is_none() {
                terminal_reason = RuntimeTerminalReasonCode::RuntimeStopError;
                terminal_error = Some(AppError::Runtime(force_err));
            }
        }
    }

    if let Err(e) = sink.close().await {
        tracing::error!(error = %e, "sink close failed during shutdown");
        if terminal_error.is_none() {
            terminal_reason = RuntimeTerminalReasonCode::SinkCloseError;
            terminal_error = Some(e.into());
        }
    }

    admin_state
        .set_terminal_reason_code(terminal_reason.as_str())
        .await;

    match terminal_error {
        None => {
            admin_state.set_state(InstanceState::Stopped).await;
            tracing::info!(
                reason = terminal_reason.as_str(),
                "CDC pipeline stopped cleanly"
            );
            Ok(())
        }
        Some(err) => {
            admin_state.set_state(InstanceState::Error).await;
            tracing::error!(reason = terminal_reason.as_str(), error = %err, "CDC pipeline terminated with error");
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RuntimeLoopFailure, RuntimeLoopOutcome, RuntimeTerminalReasonCode};
    use crate::error::AppError;

    #[test]
    fn runtime_loop_failure_new_preserves_reason_and_error() {
        let failure = RuntimeLoopFailure::new(
            RuntimeTerminalReasonCode::BatchDeliveryError,
            AppError::Other("delivery failed".to_string()),
        );

        assert_eq!(
            failure.reason,
            RuntimeTerminalReasonCode::BatchDeliveryError
        );
        assert!(matches!(failure.error, AppError::Other(_)));
    }

    #[test]
    fn runtime_loop_outcome_error_constructor_sets_error_variant() {
        let outcome = RuntimeLoopOutcome::error(
            RuntimeTerminalReasonCode::AdminServerExit,
            AppError::Other("admin exited".to_string()),
        );

        let RuntimeLoopOutcome::Error(failure) = outcome else {
            panic!("expected error outcome variant");
        };
        assert_eq!(failure.reason, RuntimeTerminalReasonCode::AdminServerExit);
        assert!(matches!(failure.error, AppError::Other(_)));
    }

    #[test]
    fn runtime_loop_outcome_runtime_error_wraps_runtime_error() {
        let outcome = RuntimeLoopOutcome::runtime_error(
            RuntimeTerminalReasonCode::RuntimePollError,
            rustcdc::core::Error::SourceError("source down".to_string()),
        );

        let RuntimeLoopOutcome::Error(failure) = outcome else {
            panic!("expected runtime error outcome variant");
        };
        assert_eq!(failure.reason, RuntimeTerminalReasonCode::RuntimePollError);
        assert!(matches!(failure.error, AppError::Runtime(_)));
    }

    #[test]
    fn terminal_reason_codes_map_to_expected_labels() {
        let cases = [
            (RuntimeTerminalReasonCode::ShutdownSignal, "shutdown_signal"),
            (
                RuntimeTerminalReasonCode::AdminServerExit,
                "admin_server_exit",
            ),
            (
                RuntimeTerminalReasonCode::RuntimePollError,
                "runtime_poll_error",
            ),
            (
                RuntimeTerminalReasonCode::BatchDeliveryError,
                "batch_delivery_error",
            ),
            (
                RuntimeTerminalReasonCode::CheckpointBarrierBeginError,
                "checkpoint_barrier_begin_error",
            ),
            (
                RuntimeTerminalReasonCode::CheckpointBarrierCommitError,
                "checkpoint_barrier_commit_error",
            ),
            (
                RuntimeTerminalReasonCode::CheckpointCommitError,
                "checkpoint_commit_error",
            ),
            (
                RuntimeTerminalReasonCode::CheckpointReconciliationError,
                "checkpoint_reconciliation_error",
            ),
            (
                RuntimeTerminalReasonCode::RecoverableBreakerEscalation,
                "recoverable_breaker_escalation",
            ),
            (
                RuntimeTerminalReasonCode::RuntimeStopError,
                "runtime_stop_error",
            ),
            (
                RuntimeTerminalReasonCode::SinkCloseError,
                "sink_close_error",
            ),
        ];

        for (reason, label) in cases {
            assert_eq!(reason.as_str(), label);
        }
    }
}
