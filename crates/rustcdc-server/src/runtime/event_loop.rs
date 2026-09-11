use rustcdc::core::CdcRuntime;
use tokio::signal;

use crate::{
    admin::{AdminState, InstanceState},
    error::AppError,
    pipeline::transform,
    state::CheckpointAgeSource,
};

use super::batch_commit::handle_polled_batch;
use super::lifecycle::{RuntimeLoopOutcome, RuntimeTerminalReasonCode};
use super::metrics::RuntimeLoopMetricsAccumulator;
use super::reconciliation::CheckpointTxnReconciler;
use super::recovery::{RecoverableErrorState, RecoveryAction, RecoveryPolicyConfig, jitter_seed};

pub(crate) struct RuntimeLoopConfig {
    pub(crate) prepare_parallelism: usize,
    pub(crate) sink_flush_interval_events: usize,
    pub(crate) sink_delivery_queue_capacity: usize,
    pub(crate) sink_send_timeout_ms: u64,
    pub(crate) sink_flush_timeout_ms: u64,
    pub(crate) recoverable_error_backoff_initial_ms: u64,
    pub(crate) recoverable_error_backoff_max_ms: u64,
    pub(crate) recoverable_error_backoff_multiplier: f64,
    pub(crate) recoverable_error_backoff_jitter_ratio: f64,
    pub(crate) recoverable_error_breaker_consecutive_threshold: u64,
    pub(crate) recoverable_error_breaker_max_open_cycles: u64,
    pub(crate) recoverable_error_breaker_cooldown_ms: u64,
    pub(crate) sink_name: String,
    pub(crate) requested_delivery_contract: String,
    pub(crate) delivery_contract_satisfied: bool,
    pub(crate) sink_delivery_guarantee: String,
    pub(crate) sink_idempotent_delivery_capable: bool,
    pub(crate) sink_transactional_checkpoint_barrier_capable: bool,
    pub(crate) queue_depth_p95_window_samples: usize,
    pub(crate) correctness_dedup_window_size: usize,
}

#[allow(clippy::too_many_arguments)] // internal wiring of long-lived pipeline components
pub(crate) async fn execute_event_loop(
    runtime: &mut CdcRuntime,
    sink: &mut crate::pipeline::router::TableRouter,
    transform_pipeline: &transform::TransformPipeline,
    admin_state: &AdminState,
    admin_exit_rx: &mut Option<tokio::sync::watch::Receiver<bool>>,
    checkpoint_age_source: &CheckpointAgeSource,
    checkpoint_txn_reconciler: &mut CheckpointTxnReconciler,
    sink_metrics: &crate::sink::SinkMetricsRegistry,
    dlq: Option<&tokio::sync::Mutex<crate::dlq::DeadLetterQueue>>,
    config: RuntimeLoopConfig,
) -> RuntimeLoopOutcome {
    let recovery_policy = RecoveryPolicyConfig {
        initial_backoff_ms: config.recoverable_error_backoff_initial_ms,
        max_backoff_ms: config.recoverable_error_backoff_max_ms,
        backoff_multiplier: config.recoverable_error_backoff_multiplier,
        jitter_ratio: config.recoverable_error_backoff_jitter_ratio,
        breaker_consecutive_threshold: config.recoverable_error_breaker_consecutive_threshold,
        breaker_max_open_cycles: config.recoverable_error_breaker_max_open_cycles,
        breaker_cooldown_ms: config.recoverable_error_breaker_cooldown_ms,
        breaker_clean_window_successes: 10,
    };

    let mut recoverable = RecoverableErrorState::new(recovery_policy.initial_backoff_ms);
    let mut metrics_accumulator = RuntimeLoopMetricsAccumulator::new(
        &config.sink_name,
        &config.requested_delivery_contract,
        config.delivery_contract_satisfied,
        &config.sink_delivery_guarantee,
        config.sink_idempotent_delivery_capable,
        config.sink_transactional_checkpoint_barrier_capable,
        config.queue_depth_p95_window_samples,
        config.correctness_dedup_window_size,
    );

    // Installed before the loop, not inside the `select!`: see [`ShutdownSignals`].
    let mut shutdown_signals = ShutdownSignals::install();

    let mut loop_iteration: u64 = 0;
    'event_loop: loop {
        loop_iteration += 1;

        // Control operations (snapshot request, pause, resume, stop) are serviced by
        // rustcdc itself at the top of `poll_event_batch`, so there is nothing to drain
        // here.
        //
        // `poll_event_batch` is not cancel-safe — racing it drops events that have left
        // the source's buffer — so commands are serviced between polls, upstream, under
        // the guarantee its `# Cancel safety` section documents.

        tracing::trace!(loop_iteration, "event loop: entering select");
        tokio::select! {
            biased;

            _ = shutdown_signals.recv() => {
                tracing::info!("shutdown signal received - stopping pipeline");
                admin_state.record_shutdown_request_os_signal().await;
                admin_state.set_state(InstanceState::Stopping).await;
                break 'event_loop RuntimeLoopOutcome::Clean(RuntimeTerminalReasonCode::ShutdownSignal);
            }

            _ = wait_for_admin_exit(admin_exit_rx) => {
                if admin_exit_rx.as_ref().is_some_and(|rx| *rx.borrow()) {
                    break 'event_loop RuntimeLoopOutcome::error(
                        RuntimeTerminalReasonCode::AdminServerExit,
                        crate::error::AppError::Other(
                            "admin server terminated unexpectedly while runtime is active".to_string(),
                        ),
                    );
                }
            }

            batch_result = runtime.poll_event_batch() => {
                match batch_result {
                    Ok(batch) => {
                        tracing::trace!(loop_iteration, batch_len = batch.len(), "event loop: batch polled");
                        if let Some(outcome) = handle_polled_batch(
                            runtime,
                            sink,
                            transform_pipeline,
                            admin_state,
                            checkpoint_age_source,
                            checkpoint_txn_reconciler,
                            &config,
                            &recovery_policy,
                            &mut recoverable,
                            &mut metrics_accumulator,
                            sink_metrics,
                            dlq,
                            batch,
                        )
                        .await {
                            break 'event_loop outcome;
                        }
                    }
                    Err(e) => {
                        if let Some(outcome) =
                            handle_poll_error(e, &mut recoverable, &recovery_policy).await
                        {
                            break 'event_loop outcome;
                        }
                        // Update the admin consecutive-error counter so /readyz
                        // can signal source degradation before the circuit-breaker
                        // escalates.
                        let consecutive = recoverable.snapshot().consecutive;
                        admin_state
                            .record_source_consecutive_errors(consecutive)
                            .await;
                    }
                }
            }
        }
    }
}

// ── Signal handling ───────────────────────────────────────────────────────────

pub(crate) async fn wait_for_admin_exit(rx: &mut Option<tokio::sync::watch::Receiver<bool>>) {
    match rx {
        Some(inner) => {
            let _ = inner.changed().await;
        }
        None => std::future::pending::<()>().await,
    }
}

/// The process's SIGTERM and SIGINT listeners, installed **once**.
///
/// # Why this is a struct and not an `async fn`
///
/// It used to be `async fn shutdown_signal()`, awaited fresh inside the event loop's
/// `select!` — which called `signal::unix::signal(..)` on **every iteration**. A tokio
/// `Signal` is a receiver, not a query: a signal delivered while no receiver is registered
/// is dropped, and every loop iteration dropped the previous registration and built a new
/// one. SIGTERM was therefore only honoured if it arrived in the window where the select
/// happened to be parked — so `kill` on a busy pipeline did nothing, systemd and Kubernetes
/// hit their grace period and sent SIGKILL, the checkpoint was never flushed and the owner
/// lease was never released, leaving the successor to wait out the full lease TTL before it
/// could start.
///
/// Installed once and polled by reference, the registration outlives every iteration and
/// there is no window. [`Self::recv`] is cancel-safe, which is what lets it sit in a
/// `select!` arm that loses the race on every batch.
pub(crate) struct ShutdownSignals {
    #[cfg(unix)]
    sigterm: Option<signal::unix::Signal>,
    #[cfg(unix)]
    sigint: Option<signal::unix::Signal>,
}

impl ShutdownSignals {
    /// Register the handlers. A handler that cannot be installed is logged and omitted:
    /// losing one signal is worse than refusing to start, but silently losing it is worse
    /// still.
    pub(crate) fn install() -> Self {
        #[cfg(unix)]
        {
            fn stream(kind: signal::unix::SignalKind, name: &str) -> Option<signal::unix::Signal> {
                match signal::unix::signal(kind) {
                    Ok(stream) => Some(stream),
                    Err(error) => {
                        tracing::error!(
                            error = %error,
                            signal = name,
                            "failed to install a shutdown handler; this process will not stop \
                             gracefully on that signal"
                        );
                        None
                    }
                }
            }
            Self {
                sigterm: stream(signal::unix::SignalKind::terminate(), "SIGTERM"),
                sigint: stream(signal::unix::SignalKind::interrupt(), "SIGINT"),
            }
        }
        #[cfg(not(unix))]
        Self {}
    }

    /// Resolve when a shutdown signal arrives. Cancel-safe.
    pub(crate) async fn recv(&mut self) {
        #[cfg(unix)]
        {
            match (&mut self.sigterm, &mut self.sigint) {
                (Some(term), Some(int)) => {
                    tokio::select! {
                        _ = term.recv() => {}
                        _ = int.recv() => {}
                    }
                }
                (Some(one), None) | (None, Some(one)) => {
                    let _ = one.recv().await;
                }
                (None, None) => std::future::pending::<()>().await,
            }
        }
        #[cfg(not(unix))]
        {
            if signal::ctrl_c().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

// ── Poll-error recovery ───────────────────────────────────────────────────────

pub(crate) async fn handle_poll_error(
    error: rustcdc::core::Error,
    recoverable: &mut RecoverableErrorState,
    recovery_policy: &RecoveryPolicyConfig,
) -> Option<RuntimeLoopOutcome> {
    tracing::error!(error = %error, "event stream error");
    if !error.is_recoverable() {
        return Some(RuntimeLoopOutcome::runtime_error(
            RuntimeTerminalReasonCode::RuntimePollError,
            error,
        ));
    }

    match recoverable.on_recoverable_error(recovery_policy, jitter_seed()) {
        RecoveryAction::Retry {
            consecutive,
            backoff_base_ms,
            backoff_ms,
        } => {
            tracing::warn!(
                consecutive,
                backoff_base_ms,
                backoff_ms,
                "recoverable stream error; backing off before retry"
            );
            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            None
        }
        RecoveryAction::BreakerCooldown {
            consecutive,
            breaker_open_consecutive,
            cooldown_ms,
        } => {
            tracing::warn!(
                consecutive,
                breaker_open_consecutive,
                cooldown_ms,
                "recoverable error circuit-breaker opened; applying cooldown"
            );
            tokio::time::sleep(std::time::Duration::from_millis(cooldown_ms)).await;
            None
        }
        RecoveryAction::Escalate { message } => Some(RuntimeLoopOutcome::error(
            RuntimeTerminalReasonCode::RecoverableBreakerEscalation,
            AppError::Other(message),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::handle_poll_error;
    use crate::runtime::lifecycle::{RuntimeLoopOutcome, RuntimeTerminalReasonCode};
    use crate::runtime::recovery::{RecoverableErrorState, RecoveryPolicyConfig};

    fn deterministic_policy() -> RecoveryPolicyConfig {
        RecoveryPolicyConfig {
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            backoff_multiplier: 1.0,
            jitter_ratio: 0.0,
            breaker_consecutive_threshold: 3,
            breaker_max_open_cycles: 2,
            breaker_cooldown_ms: 0,
            breaker_clean_window_successes: 10,
        }
    }

    #[tokio::test]
    async fn non_recoverable_poll_error_maps_to_runtime_poll_terminal_reason() {
        let mut recoverable = RecoverableErrorState::new(0);
        let policy = deterministic_policy();

        let outcome = handle_poll_error(
            rustcdc::core::Error::ConfigError("bad runtime config".to_string()),
            &mut recoverable,
            &policy,
        )
        .await;

        let Some(RuntimeLoopOutcome::Error(failure)) = outcome else {
            panic!("expected terminal runtime outcome for non-recoverable poll error");
        };
        assert_eq!(failure.reason, RuntimeTerminalReasonCode::RuntimePollError);
    }

    #[tokio::test]
    async fn recoverable_poll_error_retry_path_returns_none_and_updates_state() {
        let mut recoverable = RecoverableErrorState::new(0);
        let policy = deterministic_policy();

        let outcome = handle_poll_error(
            rustcdc::core::Error::SourceError("temporary source glitch".to_string()),
            &mut recoverable,
            &policy,
        )
        .await;

        assert!(
            outcome.is_none(),
            "expected retry branch to continue event loop"
        );
        let snapshot = recoverable.snapshot();
        assert_eq!(snapshot.total, 1);
        assert_eq!(snapshot.consecutive, 1);
        assert_eq!(snapshot.breaker_open_total, 0);
    }

    #[tokio::test]
    async fn recoverable_poll_error_breaker_cooldown_path_resets_consecutive() {
        let mut recoverable = RecoverableErrorState::new(0);
        let policy = RecoveryPolicyConfig {
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            backoff_multiplier: 1.0,
            jitter_ratio: 0.0,
            breaker_consecutive_threshold: 1,
            breaker_max_open_cycles: 2,
            breaker_cooldown_ms: 0,
            breaker_clean_window_successes: 10,
        };

        let outcome = handle_poll_error(
            rustcdc::core::Error::TimeoutError("transient timeout".to_string()),
            &mut recoverable,
            &policy,
        )
        .await;

        assert!(
            outcome.is_none(),
            "expected breaker cooldown branch to continue loop"
        );
        let snapshot = recoverable.snapshot();
        assert_eq!(snapshot.total, 1);
        assert_eq!(snapshot.consecutive, 0);
        assert_eq!(snapshot.breaker_open_total, 1);
        assert_eq!(snapshot.breaker_open_consecutive, 1);
    }

    #[tokio::test]
    async fn recoverable_poll_error_escalation_maps_to_typed_terminal_reason() {
        let mut recoverable = RecoverableErrorState::new(0);
        let policy = RecoveryPolicyConfig {
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            backoff_multiplier: 1.0,
            jitter_ratio: 0.0,
            breaker_consecutive_threshold: 1,
            breaker_max_open_cycles: 1,
            breaker_cooldown_ms: 0,
            breaker_clean_window_successes: 10,
        };

        let outcome = handle_poll_error(
            rustcdc::core::Error::SourceError("persistent source outage".to_string()),
            &mut recoverable,
            &policy,
        )
        .await;

        let Some(RuntimeLoopOutcome::Error(failure)) = outcome else {
            panic!("expected escalation to produce terminal runtime outcome");
        };
        assert_eq!(
            failure.reason,
            RuntimeTerminalReasonCode::RecoverableBreakerEscalation
        );
    }
}
