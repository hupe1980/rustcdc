//! Parking the process when the source has invalidated its replication slot.
//!
//! The slot's discarded WAL is gone from the server, so neither a retry nor a restart can
//! bring it back, and exiting would hand the orchestrator a crash loop that cannot succeed.
//! The process stays up instead — not capturing, `/livez` green, `/readyz` red, the verdict
//! on `/status` and `/metrics` — until an operator recreates the slot, re-snapshots and
//! stops it. It never re-snapshots on its own: a re-snapshot cannot replay rows deleted in
//! the gap, so a mirror would keep them, and that decision belongs to whoever owns the data.

use rustcdc::core::{CdcRuntime, Error, SourceErrorKind};

use crate::admin::{AdminState, InstanceState};

use super::batch_commit::publish_health_during_retry;
use super::event_loop::{ShutdownSignals, wait_for_admin_exit};
use super::lifecycle::{RuntimeLoopOutcome, RuntimeTerminalReasonCode};
use super::metrics::RuntimeLoopMetricsAccumulator;
use super::recovery::RecoverableErrorState;

/// Whether `error` means the source invalidated the replication slot.
pub(crate) fn is_lost_source_position(error: &Error) -> bool {
    error.source_kind() == Some(SourceErrorKind::SlotInvalidated)
}

/// Publish the verdict, then wait for a shutdown signal or the admin server to exit.
pub(crate) async fn park_on_lost_source_position(
    runtime: &CdcRuntime,
    admin_state: &AdminState,
    recoverable: &RecoverableErrorState,
    metrics_accumulator: &mut RuntimeLoopMetricsAccumulator,
    shutdown_signals: &mut ShutdownSignals,
    admin_exit_rx: &mut Option<tokio::sync::watch::Receiver<bool>>,
    error: &Error,
) -> RuntimeLoopOutcome {
    tracing::error!(
        error = %error.report(),
        "the source invalidated the replication slot; capture has stopped. This process \
         stays up to report it and will not restart capture: a restart cannot recreate the \
         discarded WAL. Recreate the slot, re-snapshot the captured tables (a re-snapshot \
         does not replay rows deleted during the gap), then stop this process with SIGTERM."
    );
    // The errors that led here say nothing about the source's availability any more, and
    // left standing they would trip `/livez`'s degraded-source timeout into the very
    // restart this park exists to avoid.
    admin_state.record_source_consecutive_errors(0).await;
    publish_health_during_retry(runtime, admin_state, recoverable, metrics_accumulator).await;

    loop {
        tokio::select! {
            biased;

            _ = shutdown_signals.recv() => {
                tracing::info!("shutdown signal received while parked on a lost replication slot");
                admin_state.record_shutdown_request_os_signal().await;
                admin_state.set_state(InstanceState::Stopping).await;
                return RuntimeLoopOutcome::Clean(RuntimeTerminalReasonCode::ShutdownSignal);
            }

            _ = wait_for_admin_exit(admin_exit_rx) => {
                if admin_exit_rx.as_ref().is_some_and(|rx| *rx.borrow()) {
                    return RuntimeLoopOutcome::error(
                        RuntimeTerminalReasonCode::AdminServerExit,
                        crate::error::AppError::Other(
                            "admin server terminated unexpectedly while parked on a lost \
                             replication slot"
                                .to_string(),
                        ),
                    );
                }
            }
        }
    }
}
