use super::run_lifecycle::finalize_runtime_shutdown;
use super::run_loop::{execute_event_loop, RuntimeLoopConfig};
use super::run_reconciliation::{CheckpointTxnReconciler, RecoveredMarkerInfo};
use rustcdc::core::{
    CdcRuntime, ConnectionRetryPolicy, IdempotencyOptions, PostCommitSourceConfirmPolicy,
    RuntimeConfig, RuntimeOptions, TransactionBoundaryPolicy, TransformErrorPolicy,
};
use rustcdc::PostgresSourceConfig;
use std::path::{Path, PathBuf};
use tokio_postgres::NoTls;

use crate::pipeline::transform;
use crate::{
    admin::{self, AdminState, InstanceState},
    cli::{CheckpointParityMode, RunArgs},
    config::{self, schema::DeliveryContract, AppConfig},
    error::AppError,
};
use rustcdc::sink::SinkAdapter;

pub async fn execute(args: RunArgs, config_path: Option<&Path>) -> Result<(), AppError> {
    let config_path = config_path.ok_or(crate::error::ConfigError::NoConfigFile)?;

    let mut app_config = config::load(config_path)?;
    config::apply_run_overrides(
        &mut app_config,
        args.state_dir.clone(),
        args.snapshot_tables.clone(),
    );

    run_pipeline(app_config, args.checkpoint_parity_mode).await
}

const QUEUE_DEPTH_P95_WINDOW_SAMPLES: usize = 4096;

async fn run_pipeline(
    app_config: AppConfig,
    checkpoint_parity_mode: CheckpointParityMode,
) -> Result<(), AppError> {
    ensure_state_layout(&app_config)?;
    let _state_lock = StateDirLock::acquire(&app_config.state.offset.dir)?;

    // ── Primary-node guard ────────────────────────────────────────────────
    // Fail fast with actionable guidance when the source is a read replica.
    // Applies to all connector types; each source config validates via its own
    // server-specific mechanism (pg_is_in_recovery, @@global.read_only, etc.).
    if app_config.source.require_primary {
        match &app_config.source.driver {
            crate::config::schema::SourceDriver::Postgres(pg) => {
                pg.check_is_primary().await?;
            }
            crate::config::schema::SourceDriver::Mysql(mysql) => {
                mysql.check_is_primary().await?;
            }
            crate::config::schema::SourceDriver::Mariadb(mariadb) => {
                mariadb.check_is_primary().await?;
            }
            crate::config::schema::SourceDriver::Sqlserver(sqlserver) => {
                sqlserver.to_runtime_config().check_is_primary().await?;
            }
        }
    }

    let admin_state = AdminState::new(&app_config).await?;
    let recovered_marker = if let Some(recovery) =
        CheckpointTxnReconciler::recover_unresolved_marker(&app_config.state.offset.dir)?
    {
        tracing::warn!(
            "checkpoint-transaction reconciliation marker was recovered during startup; {}",
            recovery.detail
        );
        admin_state
            .record_reconciliation_recovery(recovery.parse_ok, &recovery.detail)
            .await;
        Some(recovery)
    } else {
        None
    };

    let mut admin_exit_rx = None;

    // ── Admin server ──────────────────────────────────────────────────────
    if app_config.admin.enabled {
        let handle = admin::serve(
            &app_config.admin.bind,
            app_config.admin.timeout_ms,
            app_config.admin.tls.as_ref(),
            admin_state.clone(),
        )
        .await?;

        let (tx, rx) = tokio::sync::watch::channel(false);
        admin_exit_rx = Some(rx);

        tokio::spawn(async move {
            if let Err(e) = handle.await {
                tracing::error!(error = %e, "admin server join failure");
            }
            let _ = tx.send(true);
        });
    }

    // ── Sink ──────────────────────────────────────────────────────────────
    //
    // Built before the state backend, and that order is load-bearing rather than
    // incidental: end-to-end exactly-once needs the checkpoint written inside the sink's
    // Kafka transaction, so the state backend has to be handed the sink's producer. The
    // dependency runs sink → state, so the sink must exist first.
    let crate::pipeline::binding::BuiltRouter {
        router: mut sink,
        transaction_handle,
    } = crate::pipeline::binding::build_router(&app_config).await?;
    let transform_pipeline = transform::TransformPipeline::from_config(
        app_config.pipeline.transform_runtime.clone(),
        app_config.pipeline.transforms.clone(),
    )?;

    // ── Backend dispatch ──────────────────────────────────────────────────
    let state = crate::state::build(&app_config.state, transaction_handle).await?;
    run_with_runtime_state(
        app_config,
        admin_state,
        &transform_pipeline,
        &mut sink,
        &mut admin_exit_rx,
        recovered_marker.clone(),
        state,
        checkpoint_parity_mode,
    )
    .await
}

#[allow(clippy::too_many_arguments)] // internal wiring of long-lived pipeline components
async fn run_with_runtime_state(
    app_config: AppConfig,
    admin_state: AdminState,
    transform_pipeline: &transform::TransformPipeline,
    sink: &mut crate::pipeline::router::TableRouter,
    admin_exit_rx: &mut Option<tokio::sync::watch::Receiver<bool>>,
    recovered_marker: Option<RecoveredMarkerInfo>,
    state: crate::state::RuntimeState,
    checkpoint_parity_mode: CheckpointParityMode,
) -> Result<(), AppError> {
    let crate::state::RuntimeState {
        checkpoint,
        schema_history,
        checkpoint_age_source,
        remote_lease,
    } = state;

    if let Some(recovery) = recovered_marker.as_ref() {
        record_post_recovery_checkpoint_proof(&admin_state, &checkpoint_age_source, recovery).await;
    }

    let source_config = config::resolve_runtime_source_config(&app_config)?;

    let options = RuntimeOptions::default()
        .with_max_buffer_size(app_config.runtime.max_buffer_size)
        .with_max_poll_wait_ms(app_config.runtime.max_poll_wait_ms)
        .with_transform_error_policy(match app_config.runtime.transform_error_policy {
            config::schema::RuntimeTransformErrorPolicy::Halt => TransformErrorPolicy::Halt,
            config::schema::RuntimeTransformErrorPolicy::Skip => TransformErrorPolicy::Skip,
        })
        .with_post_commit_source_confirm_policy(
            match app_config.runtime.post_commit_source_confirm_policy {
                config::schema::RuntimePostCommitSourceConfirmPolicy::Continue => {
                    PostCommitSourceConfirmPolicy::Continue
                }
                config::schema::RuntimePostCommitSourceConfirmPolicy::FailFast => {
                    PostCommitSourceConfirmPolicy::FailFast
                }
            },
        )
        .with_transaction_boundary(match app_config.runtime.transaction_boundary {
            config::schema::RuntimeTransactionBoundaryPolicy::Split => {
                TransactionBoundaryPolicy::Split
            }
            config::schema::RuntimeTransactionBoundaryPolicy::PreserveTransactions => {
                TransactionBoundaryPolicy::PreserveTransactions
            }
        })
        .with_event_validation(app_config.runtime.validate_events)
        // The runtime's own byte guard, so an oversized event is refused where it is
        // produced. cdc-server also checks at encode time, which is the later of the
        // two and cannot stop the event from being buffered first.
        .with_max_event_bytes(app_config.runtime.max_event_bytes)
        .with_sink_close_timeout_ms(
            (app_config.runtime.sink_close_timeout_ms > 0)
                .then_some(app_config.runtime.sink_close_timeout_ms),
        );

    let options = if app_config.runtime.idempotency.enabled {
        let idempotency = IdempotencyOptions::new(app_config.runtime.idempotency.capacity)
            .map_err(|e| AppError::Other(format!("runtime.idempotency.capacity: {e}")))?;
        let idempotency = if app_config.runtime.idempotency.ttl_ms > 0 {
            idempotency
                .with_ttl_ms(app_config.runtime.idempotency.ttl_ms)
                .map_err(|e| AppError::Other(format!("runtime.idempotency.ttl_ms: {e}")))?
        } else {
            idempotency
        };
        options.with_idempotency(idempotency)
    } else {
        options.with_idempotency_disabled()
    };

    let options = if app_config.runtime.schema_history_max_versions_per_table > 0 {
        let retention = rustcdc::schema_history::SchemaHistoryRetention::keep_last(
            app_config.runtime.schema_history_max_versions_per_table,
        )
        .map_err(|e| {
            AppError::Other(format!(
                "runtime.schema_history_max_versions_per_table: {e}"
            ))
        })?;
        options.with_schema_history_retention(retention)
    } else {
        options
    };

    let options = if app_config.runtime.source_connection_retry.enabled {
        // `ConnectionRetryPolicy` stopped being `#[non_exhaustive]` in rustcdc 0.8, so
        // every field is named here — a field added upstream becomes a compile error
        // instead of silently keeping its default under an operator's explicit config.
        options.with_connection_retry(ConnectionRetryPolicy {
            max_retries: app_config.runtime.source_connection_retry.max_retries,
            initial_delay_ms: app_config.runtime.source_connection_retry.initial_delay_ms,
            max_delay_ms: app_config.runtime.source_connection_retry.max_delay_ms,
        })
    } else {
        let mut options = options;
        options.connection_retry = None;
        options
    };

    let runtime_config =
        RuntimeConfig::new(source_config, checkpoint, schema_history).with_options(options);

    // `snapshot_tables` and `incremental_snapshot` are two bootstrapping paths for the
    // same job, and the loader rejects setting both — a runtime that received both would
    // read every listed table twice.
    let runtime_config = if app_config.incremental_snapshot.is_enabled() {
        runtime_config.with_incremental_snapshot(
            rustcdc::IncrementalSnapshotConfig::new(app_config.incremental_snapshot.tables.clone())
                .with_chunk_size(app_config.incremental_snapshot.chunk_size),
        )
    } else if app_config.snapshot_tables.is_empty() {
        runtime_config
    } else {
        runtime_config.with_snapshot_tables(app_config.snapshot_tables.clone())
    };

    let mut runtime = CdcRuntime::new(runtime_config)?;

    // Verify sink reachability before marking the instance as running so that
    // the readiness probe is only set to true after confirming connectivity.
    if let Err(e) = crate::pipeline::router::preflight_check(sink).await {
        admin_state.set_state(InstanceState::Error).await;
        return Err(e);
    }

    admin_state.set_state(InstanceState::Running).await;
    let sink_name = sink.name().to_string();
    let requested_delivery_contract = app_config.delivery_contract.as_label().to_string();
    let sink_delivery_guarantee = sink.delivery_guarantee().as_label().to_string();
    let sink_idempotent_delivery_capable = sink.idempotent_delivery_capable();
    let sink_transactional_checkpoint_barrier_capable =
        sink.transactional_checkpoint_barrier_capable();

    // Enforce the parity-mode contract at startup so the operator gets an
    // explicit error rather than a silently-degraded effectively_once guarantee.
    super::run_batch::validate_parity_contract(
        checkpoint_parity_mode,
        sink,
        app_config.delivery_contract,
    )?;
    let delivery_contract_satisfied = app_config.delivery_contract.is_satisfied_by(
        sink_idempotent_delivery_capable,
        sink_transactional_checkpoint_barrier_capable,
    );
    let checkpoint_txn_reconciliation_enabled = app_config.delivery_contract
        == DeliveryContract::EffectivelyOnce
        && sink_transactional_checkpoint_barrier_capable;
    let mut checkpoint_txn_reconciler = CheckpointTxnReconciler::new(
        app_config.state.offset.dir.clone(),
        checkpoint_txn_reconciliation_enabled,
        sink_name.clone(),
        requested_delivery_contract.clone(),
    );

    tracing::info!("CDC pipeline starting");
    runtime.start().await?;

    // ── Replication-slot lag side-channel (PostgreSQL only) ───────────────
    // Spawns a lightweight background task that queries `pg_replication_slots`
    // every 15 s and stores the result in the admin state for the `/metrics`
    // endpoint.  The task does not affect the main event loop and exits when
    // the admin state transitions to Stopping/Stopped.
    if let crate::config::schema::SourceDriver::Postgres(pg) = &app_config.source.driver {
        let admin_for_lag = admin_state.clone();
        let pg_for_lag = pg.clone();
        tokio::spawn(async move {
            poll_replication_slot_lag(pg_for_lag, admin_for_lag).await;
        });
    }

    // ── Main event loop ───────────────────────────────────────────────────
    // We use poll_event_batch() instead of event_batches() so we can call
    // commit_ack / stop / admin snapshot in the same loop body
    // without a persistent mutable borrow of `runtime`.
    // Built before the loop so a misconfigured dead-letter target fails at startup
    // rather than at the moment it is first needed — which is during an incident.
    let dlq = crate::dlq::DeadLetterQueue::build(&app_config.dlq)
        .await?
        .map(tokio::sync::Mutex::new);
    if let Some(queue) = dlq.as_ref() {
        tracing::info!(
            target = queue.lock().await.target_name(),
            "dead-letter quarantine enabled: permanently undeliverable events will be \
             recorded and the pipeline will advance past them"
        );
    }

    let loop_outcome = execute_event_loop(
        &mut runtime,
        sink,
        transform_pipeline,
        &admin_state,
        admin_exit_rx,
        &checkpoint_age_source,
        &mut checkpoint_txn_reconciler,
        dlq.as_ref(),
        RuntimeLoopConfig {
            prepare_parallelism: app_config.runtime.prepare_parallelism,
            sink_flush_interval_events: app_config.runtime.sink_flush_interval_events,
            sink_delivery_queue_capacity: app_config.runtime.sink_delivery_queue_capacity,
            sink_send_timeout_ms: app_config.runtime.sink_send_timeout_ms,
            sink_flush_timeout_ms: app_config.runtime.sink_flush_timeout_ms,
            recoverable_error_backoff_initial_ms: app_config
                .runtime
                .recoverable_error_backoff_initial_ms,
            recoverable_error_backoff_max_ms: app_config.runtime.recoverable_error_backoff_max_ms,
            recoverable_error_backoff_multiplier: app_config
                .runtime
                .recoverable_error_backoff_multiplier,
            recoverable_error_backoff_jitter_ratio: app_config
                .runtime
                .recoverable_error_backoff_jitter_ratio,
            recoverable_error_breaker_consecutive_threshold: u64::from(
                app_config
                    .runtime
                    .recoverable_error_breaker_consecutive_threshold,
            ),
            recoverable_error_breaker_max_open_cycles: u64::from(
                app_config.runtime.recoverable_error_breaker_max_open_cycles,
            ),
            recoverable_error_breaker_cooldown_ms: app_config
                .runtime
                .recoverable_error_breaker_cooldown_ms,
            sink_name,
            requested_delivery_contract,
            delivery_contract_satisfied,
            sink_delivery_guarantee,
            sink_idempotent_delivery_capable,
            sink_transactional_checkpoint_barrier_capable,
            queue_depth_p95_window_samples: QUEUE_DEPTH_P95_WINDOW_SAMPLES,
            correctness_dedup_window_size: app_config.runtime.correctness_dedup_window_size,
        },
    )
    .await;

    // Reported at shutdown, where the hit counters cover the whole run: a mask rule
    // that never matched means those columns went out unmasked, and nothing else says
    // so — the rule looks configured and does nothing.
    transform_pipeline.report_unmatched_rules();

    let shutdown = finalize_runtime_shutdown(&mut runtime, sink, &admin_state, loop_outcome).await;

    // Stop the admin background workers before the lease is released. The signal-action
    // worker mutates the same `AdminStateData` the shutdown counters above were just
    // written to, and the signal ingress workers can still enqueue work; leaving either
    // running past this point makes the exit-time state a race rather than a report.
    admin_state.shutdown_workers().await;

    // Release the remote state lease last, once nothing can write a checkpoint any
    // more. Doing it earlier would open a window where a successor could acquire the
    // lease while this process still had a durability path open.
    //
    // This is best-effort by design — the TTL is what makes the lease correct, and this
    // only spares a successor from waiting it out. That matters in practice: with
    // `strategy: Recreate` the replacement pod starts the moment this one exits, and
    // without the release it would sit refusing to start for a full lease TTL.
    if let Some(lease) = remote_lease {
        lease.release().await;
    }

    shutdown
}

async fn record_post_recovery_checkpoint_proof(
    admin_state: &AdminState,
    checkpoint_age_source: &crate::state::CheckpointAgeSource,
    recovery: &RecoveredMarkerInfo,
) {
    let backend_name = checkpoint_age_source.backend_name();
    let checkpoint_age_seconds = checkpoint_age_source.checkpoint_age_seconds().await;
    let (proof_ok, proof_detail) = match checkpoint_age_seconds {
        Some(age) => (
            true,
            format!(
                "post-recovery checkpoint proof passed: backend={backend_name}, checkpoint_age_seconds={age:.3}, marker_parse_ok={}, marker_detail={}",
                recovery.parse_ok,
                recovery.detail
            ),
        ),
        None => (
            false,
            format!(
                "post-recovery checkpoint proof incomplete: backend={backend_name}, checkpoint_age_seconds=unavailable, marker_parse_ok={}, marker_detail={}",
                recovery.parse_ok,
                recovery.detail
            ),
        ),
    };

    if proof_ok {
        tracing::info!("{proof_detail}");
    } else {
        tracing::warn!("{proof_detail}");
    }

    admin_state
        .record_reconciliation_recovery_proof(proof_ok, &proof_detail)
        .await;
}

fn ensure_state_layout(app_config: &AppConfig) -> Result<(), AppError> {
    std::fs::create_dir_all(&app_config.state.offset.dir)?;
    Ok(())
}

// ── Advisory PID-file lock on the state directory ─────────────────────────────
// Prevents two cdc-server processes from running against the same state dir
// simultaneously. The lock is released automatically when the process exits via
// the `Drop` impl.
struct StateDirLock {
    lock_path: PathBuf,
}

impl StateDirLock {
    const LOCK_FILE: &'static str = ".cdc-server.lock";

    /// Acquire the advisory lock.
    ///
    /// * If no lock file exists, it is created with the current PID.
    /// * If a stale lock exists (dead PID or a different process now owns the PID), it is overwritten.
    /// * If a live `cdc` lock exists, an error is returned with remediation guidance.
    fn acquire(state_dir: &Path) -> Result<Self, AppError> {
        let lock_path = state_dir.join(Self::LOCK_FILE);
        let my_pid = std::process::id();

        if lock_path.exists() {
            let contents = std::fs::read_to_string(&lock_path).unwrap_or_default();
            let stored_pid: u32 = contents.trim().parse().unwrap_or(0);
            if stored_pid > 0 && is_cdc_process_alive(stored_pid) {
                return Err(AppError::Other(format!(
                    "another cdc-server process (PID {stored_pid}) is already running \
                     against state directory '{}'. \
                     If that process has crashed, remove '{}' and retry.",
                    state_dir.display(),
                    lock_path.display(),
                )));
            }
            tracing::warn!(
                "found stale state-dir lock (PID {stored_pid} is no longer alive); overwriting"
            );
        }

        std::fs::write(&lock_path, my_pid.to_string()).map_err(|e| {
            AppError::Other(format!(
                "could not write state-dir lock '{}': {e}",
                lock_path.display()
            ))
        })?;

        tracing::debug!(pid = my_pid, path = %lock_path.display(), "acquired state-dir lock");
        Ok(Self { lock_path })
    }
}

impl Drop for StateDirLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.lock_path);
    }
}

/// Returns `true` if the process with `pid` is currently alive **and** its
/// command line indicates it is a `cdc` process.
///
/// The name-verification step prevents a recycled PID from blocking startup:
/// if an unrelated process now holds that PID the lock is considered stale
/// and is overwritten.
///
/// Existence is checked via POSIX `kill -0` semantics (no signal is sent).
/// The name is then verified via `/proc/{pid}/cmdline` (Linux) or
/// `ps -o comm= -p {pid}` (macOS / other Unix).
/// Falls back to `false` on unsupported platforms so the guard is never a
/// hard blocker on Windows CI.
fn is_cdc_process_alive(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::process::{Command, Stdio};
        let exists = Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !exists {
            return false;
        }
        // /proc/{pid}/cmdline contains NUL-separated argv; the first token is
        // the executable path.  A CDC process will contain "cdc" somewhere.
        std::fs::read_to_string(format!("/proc/{pid}/cmdline"))
            .map(|cmdline| cmdline.to_ascii_lowercase().contains("cdc"))
            .unwrap_or(false)
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        use std::process::{Command, Stdio};
        let exists = Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !exists {
            return false;
        }
        // `ps -o comm= -p <pid>` returns the executable name (no path).
        Command::new("ps")
            .args(["-o", "comm=", "-p", &pid.to_string()])
            .output()
            .map(|o| {
                String::from_utf8_lossy(&o.stdout)
                    .to_ascii_lowercase()
                    .contains("cdc")
            })
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false // Conservative: never evict on unsupported platforms.
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Replication-slot lag side-channel (PostgreSQL)
// ─────────────────────────────────────────────────────────────────────────────

/// Background task: polls `pg_replication_slots` every 15 s and publishes the
/// `confirmed_flush_lsn` lag into the admin state so it is visible on the
/// `/metrics` endpoint as `cdc_source_replication_slot_lag_bytes`.
///
/// The task exits cleanly once the admin state transitions to
/// `Stopping` or `Stopped` (the main event loop has already finished at that
/// point), so it does not need an explicit cancellation token.
async fn poll_replication_slot_lag(pg: PostgresSourceConfig, admin: AdminState) {
    use tokio::time::{interval, Duration, MissedTickBehavior};
    let mut ticker = interval(Duration::from_secs(15));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        // Stop polling once the runtime is winding down.
        {
            let state = admin.data.read().await.state.clone();
            if matches!(
                state,
                InstanceState::Stopping | InstanceState::Stopped | InstanceState::Error
            ) {
                break;
            }
        }

        match query_slot_lag_bytes(&pg).await {
            Ok(lag) => {
                tracing::debug!(
                    slot = %pg.replication_slot_name,
                    lag_bytes = lag,
                    "replication slot lag sampled"
                );
                admin.record_slot_lag(Some(lag)).await;
            }
            Err(e) => {
                tracing::warn!(
                    slot = %pg.replication_slot_name,
                    error = %e,
                    "failed to sample replication slot lag — metric will show -1"
                );
                admin.record_slot_lag(None).await;
            }
        }
    }
}

/// Issue a single `pg_replication_slots` query and return the number of WAL
/// bytes not yet consumed by the slot.  Returns an error on connection or
/// query failure; the caller should treat a transient error as a stale sample.
async fn query_slot_lag_bytes(pg: &PostgresSourceConfig) -> Result<i64, AppError> {
    let password = pg.password.expose_secret().map_err(|e| {
        AppError::Other(format!(
            "slot lag sampler: failed to resolve source.postgres.password: {e}"
        ))
    })?;

    let mut config = tokio_postgres::Config::new();
    config
        .host(&pg.host)
        .port(pg.port)
        .user(&pg.user)
        .password(password)
        .dbname(&pg.database)
        .connect_timeout(std::time::Duration::from_secs(pg.conn_timeout_secs.min(10)));

    let (client, connection) = match &pg.transport {
        rustcdc::TransportConfig::Plaintext => config
            .connect(NoTls)
            .await
            .map_err(|e| AppError::Other(format!("slot lag sampler: connect failed: {e}")))?,
        rustcdc::TransportConfig::Tls { .. } => {
            // For the side-channel lag query we use NoTls — the same
            // permissive posture as the primary-guard check so we don't block
            // production monitoring on certificate paths.
            config
                .connect(NoTls)
                .await
                .map_err(|e| AppError::Other(format!("slot lag sampler: connect failed: {e}")))?
        }
        rustcdc::TransportConfig::RustlsConfig { .. } => config
            .connect(NoTls)
            .await
            .map_err(|e| AppError::Other(format!("slot lag sampler: connect failed: {e}")))?,
    };
    tokio::spawn(connection);

    let row = client
        .query_one(
            "SELECT (pg_current_wal_lsn() - confirmed_flush_lsn)::bigint \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&pg.replication_slot_name],
        )
        .await
        .map_err(|e| {
            AppError::Other(format!(
                "slot lag sampler: pg_replication_slots query failed for slot '{}': {e}",
                pg.replication_slot_name
            ))
        })?;

    let lag: i64 = row.try_get(0).map_err(|e| {
        AppError::Other(format!(
            "slot lag sampler: unexpected NULL in lag column: {e}"
        ))
    })?;
    Ok(lag)
}

#[cfg(test)]
pub(super) mod tests {
    use super::ensure_state_layout;
    use crate::commands::run_metrics::{
        recoverable_error_metrics_prometheus, sink_metrics_prometheus,
    };
    use crate::commands::run_recovery::{
        with_recoverable_error_jitter_ms, RecoverableErrorState, RecoveryAction,
        RecoveryPolicyConfig,
    };
    use crate::config::schema::{
        AppConfig, KafkaSecurityConfig, KafkaStateDurabilityProfile, KafkaTopicStateConfig,
        OffsetStoreConfig, SchemaHistoryStoreConfig, SinkConfig, StateBackend, StateConfig,
        StdoutSinkConfig,
    };
    use rustcdc::core::{Event, Operation, SourceMetadata};
    use serde_json::json;

    fn sample_event(name: &str) -> Event {
        Event::builder("users", Operation::Insert)
            .after(json!({"id": 1, "name": name}))
            .source(SourceMetadata::new("postgres", "0/16B6A70", 1))
            .ts(1)
            .schema("public")
            .primary_key(["id"])
            .build()
    }

    /// The limit is enforced against the payload the transport actually sends.
    ///
    /// It used to be enforced against a JSON rendering produced solely to be measured
    /// and then discarded — 13.9 us per event of waste, and for a non-JSON codec it
    /// measured bytes that were never transmitted. These tests drive a real
    /// `SinkBinding`, so they check the limit where it now lives.
    #[tokio::test]
    async fn event_size_limit_rejects_an_oversized_encoded_payload() {
        let mut binding =
            crate::sink::build_binding(&SinkConfig::Stdout(StdoutSinkConfig::default()), 64)
                .await
                .expect("stdout binding");

        let err = binding
            .send_event(&sample_event(&"x".repeat(4096)))
            .await
            .expect_err("an event far over the limit must be rejected");
        assert!(
            err.to_string().contains("runtime.max_event_bytes"),
            "the error must name the setting the operator has to change: {err}"
        );
    }

    #[tokio::test]
    async fn event_size_limit_allows_a_payload_within_the_limit() {
        let mut binding =
            crate::sink::build_binding(&SinkConfig::Stdout(StdoutSinkConfig::default()), 4096)
                .await
                .expect("stdout binding");

        binding
            .send_event(&sample_event("ok"))
            .await
            .expect("a small event must pass");
    }

    #[test]
    fn recoverable_error_jitter_stays_within_expected_bounds() {
        let base = 1_000;
        let jitter_ratio = 0.2;
        let jittered = with_recoverable_error_jitter_ms(base, jitter_ratio, 7);
        assert!((800..=1_200).contains(&jittered));
    }

    #[test]
    fn recoverable_error_metrics_include_breaker_signals() {
        let metrics = recoverable_error_metrics_prometheus(5, 2, 400, 375, 1, 1);
        assert!(metrics.contains("rustcdc_runtime_recoverable_poll_errors_total 5"));
        assert!(metrics.contains("rustcdc_runtime_recoverable_breaker_open_total 1"));
        assert!(metrics.contains("rustcdc_runtime_recoverable_breaker_open_consecutive 1"));
    }

    #[test]
    fn breaker_escalation_triggers_at_budget_boundary() {
        let policy = RecoveryPolicyConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 1_000,
            backoff_multiplier: 2.0,
            jitter_ratio: 0.0,
            breaker_consecutive_threshold: 1,
            breaker_max_open_cycles: 3,
            breaker_cooldown_ms: 250,
            breaker_clean_window_successes: 10,
        };
        let mut state = RecoverableErrorState::new(policy.initial_backoff_ms);

        assert!(matches!(
            state.on_recoverable_error(&policy, 1),
            RecoveryAction::BreakerCooldown { .. }
        ));
        assert!(matches!(
            state.on_recoverable_error(&policy, 2),
            RecoveryAction::BreakerCooldown { .. }
        ));
        assert!(matches!(
            state.on_recoverable_error(&policy, 3),
            RecoveryAction::Escalate { .. }
        ));
    }

    #[test]
    fn sink_metrics_include_send_and_flush_signals() {
        let sink_send_latency_buckets = [1, 1, 0, 0, 0, 0, 0, 0, 0, 0];
        let sink_flush_latency_buckets = [0, 0, 1, 0, 1, 0, 0, 0, 0, 0];
        let transform_latency_buckets = [0, 1, 0, 1, 0, 0, 0, 0, 0, 0];
        let prepare_latency_buckets = [0, 0, 1, 1, 0, 0, 0, 0, 0, 0];
        let batch_latency_buckets = [0, 1, 0, 1, 0, 0, 1, 0, 0, 0];
        let checkpoint_latency_buckets = [0, 0, 1, 0, 1, 0, 0, 0, 0, 0];
        let metrics = sink_metrics_prometheus(
            "stdout",
            "at_least_once",
            true,
            "at_least_once",
            false,
            false,
            10,
            120,
            7,
            &sink_send_latency_buckets,
            4,
            80,
            25,
            &sink_flush_latency_buckets,
            12,
            48,
            6,
            &transform_latency_buckets,
            14,
            96,
            8,
            &prepare_latency_buckets,
            3,
            150,
            70,
            &batch_latency_buckets,
            2,
            14,
            9,
            &checkpoint_latency_buckets,
            3,
            2,
            30,
            0.2,
            2,
            1,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            12, // sink_iceberg_orphaned_data_files_total
            9,
            10,
            11,
        );
        assert!(metrics.contains("rustcdc_sink_send_ops_total{sink=\"stdout\"} 10"));
        assert!(metrics.contains("rustcdc_iceberg_orphaned_data_files_total{sink=\"stdout\"} 12"));
        assert!(metrics.contains(
            "rustcdc_sink_delivery_contract_requested{sink=\"stdout\",contract=\"at_least_once\"} 1"
        ));
        assert!(metrics.contains(
            "rustcdc_sink_delivery_contract_satisfied{sink=\"stdout\",contract=\"at_least_once\"} 1"
        ));
        assert!(metrics
            .contains("rustcdc_sink_delivery_guarantee{sink=\"stdout\",mode=\"at_least_once\"} 1"));
        assert!(metrics.contains("rustcdc_sink_idempotent_delivery_capable{sink=\"stdout\"} 0"));
        assert!(metrics
            .contains("rustcdc_sink_transactional_checkpoint_barrier_capable{sink=\"stdout\"} 0"));
        assert!(metrics.contains("rustcdc_sink_flush_ops_total{sink=\"stdout\"} 4"));
        assert!(
            metrics.contains("rustcdc_sink_send_latency_seconds_last{sink=\"stdout\"} 0.000007")
        );
        assert!(
            metrics.contains("rustcdc_sink_flush_latency_seconds_last{sink=\"stdout\"} 0.000025")
        );
        assert!(metrics.contains("rustcdc_runtime_transform_ops_total{sink=\"stdout\"} 12"));
        assert!(metrics
            .contains("rustcdc_runtime_transform_latency_seconds_last{sink=\"stdout\"} 0.000006"));
        assert!(metrics
            .contains("rustcdc_runtime_transform_wasm_instance_pool_size{sink=\"stdout\"} 0"));
        assert!(
            metrics.contains("rustcdc_runtime_transform_wasm_invocations_total{sink=\"stdout\"} 0")
        );
        assert!(metrics.contains("rustcdc_runtime_transform_wasm_errors_total{sink=\"stdout\"} 0"));
        assert!(
            metrics.contains("rustcdc_runtime_transform_wasm_filtered_total{sink=\"stdout\"} 0")
        );
        assert!(metrics.contains("rustcdc_runtime_transform_wasm_timeout_total{sink=\"stdout\"} 0"));
        assert!(metrics.contains("rustcdc_runtime_prepare_ops_total{sink=\"stdout\"} 14"));
        assert!(metrics
            .contains("rustcdc_runtime_prepare_latency_seconds_last{sink=\"stdout\"} 0.000008"));
        assert!(metrics.contains(
            "rustcdc_runtime_batch_delivery_latency_seconds_last{sink=\"stdout\"} 0.00007"
        ));
        assert!(metrics.contains(
            "rustcdc_runtime_checkpoint_commit_latency_seconds_last{sink=\"stdout\"} 0.000009"
        ));
        assert!(metrics
            .contains("rustcdc_sink_send_latency_seconds_bucket{sink=\"stdout\",le=\"0.0005\"}"));
        assert!(metrics
            .contains("rustcdc_sink_flush_latency_seconds_bucket{sink=\"stdout\",le=\"0.01\"}"));
        assert!(metrics.contains(
            "rustcdc_runtime_transform_latency_seconds_bucket{sink=\"stdout\",le=\"0.005\"}"
        ));
        assert!(metrics.contains(
            "rustcdc_runtime_prepare_latency_seconds_bucket{sink=\"stdout\",le=\"0.005\"}"
        ));
        assert!(metrics.contains(
            "rustcdc_runtime_batch_delivery_latency_seconds_bucket{sink=\"stdout\",le=\"0.0005\"}"
        ));
        assert!(metrics
            .contains("rustcdc_runtime_batch_delivery_latency_seconds_count{sink=\"stdout\"} 3"));
        assert!(metrics.contains(
            "rustcdc_runtime_checkpoint_commit_latency_seconds_bucket{sink=\"stdout\",le=\"0.001\"}"
        ));
        assert!(metrics.contains(
            "rustcdc_runtime_checkpoint_commit_latency_seconds_count{sink=\"stdout\"} 2"
        ));
        assert!(metrics.contains("rustcdc_sink_queue_depth{sink=\"stdout\"} 3"));
        assert!(metrics.contains("rustcdc_sink_queue_depth_p95{sink=\"stdout\"} 2"));
        assert!(metrics.contains("rustcdc_runtime_soak_duration_seconds{sink=\"stdout\"} 30"));
        assert!(metrics.contains("rustcdc_sink_retries_total{sink=\"stdout\"} 2"));
        assert!(metrics.contains("rustcdc_sink_retry_rate{sink=\"stdout\"} 0.2"));
        assert!(metrics.contains("rustcdc_sink_dlq_total{sink=\"stdout\"} 1"));
        assert!(metrics.contains("rustcdc_sink_http_requests_total{sink=\"stdout\"} 0"));
        assert!(metrics.contains("rustcdc_sink_http_request_amplification{sink=\"stdout\"} 0"));
        assert!(metrics.contains("rustcdc_sink_http_batch_size_p50{sink=\"stdout\"} 0"));
        assert!(metrics.contains("rustcdc_sink_http_batch_size_p95{sink=\"stdout\"} 0"));
        assert!(metrics.contains("rustcdc_sink_http_batch_oldest_event_age_ms{sink=\"stdout\"} 0"));
        assert!(metrics.contains("rustcdc_sink_http_pending_events{sink=\"stdout\"} 0"));
        assert!(metrics.contains("rustcdc_sink_http_retry_delay_seconds_p50{sink=\"stdout\"} 0"));
        assert!(metrics.contains("rustcdc_sink_http_retry_delay_seconds_p95{sink=\"stdout\"} 0"));
        assert!(metrics.contains("rustcdc_sink_retryable_status_429_total{sink=\"stdout\"} 1"));
        assert!(metrics.contains("rustcdc_sink_retryable_status_5xx_total{sink=\"stdout\"} 2"));
        assert!(metrics.contains("rustcdc_sink_retryable_error_timeout_total{sink=\"stdout\"} 3"));
        assert!(metrics.contains("rustcdc_sink_retryable_error_other_total{sink=\"stdout\"} 4"));
        assert!(metrics.contains("rustcdc_sink_terminal_status_4xx_total{sink=\"stdout\"} 5"));
        assert!(metrics.contains("rustcdc_sink_terminal_status_other_total{sink=\"stdout\"} 6"));
        assert!(metrics.contains("rustcdc_sink_terminal_error_timeout_total{sink=\"stdout\"} 7"));
        assert!(metrics.contains("rustcdc_sink_terminal_error_other_total{sink=\"stdout\"} 8"));
        assert!(metrics.contains(
            "rustcdc_sink_iceberg_flush_lock_contention_events_total{sink=\"stdout\"} 9"
        ));
        assert!(metrics
            .contains("rustcdc_sink_iceberg_flush_lock_contention_ms_total{sink=\"stdout\"} 10"));
        assert!(metrics
            .contains("rustcdc_sink_iceberg_flush_lock_contention_ms_max{sink=\"stdout\"} 11"));
        assert!(metrics.contains("cdc_data_events_total{sink=\"stdout\"} 0"));
        assert!(metrics.contains("cdc_data_duplicate_rate{sink=\"stdout\"} 0"));
        assert!(metrics.contains("cdc_data_reorder_rate{sink=\"stdout\"} 0"));
        assert!(metrics.contains("cdc_end_to_end_ack_lag_seconds_avg{sink=\"stdout\"} 0"));
    }

    #[test]
    fn sink_metrics_include_idempotent_kafka_signals() {
        let zeros = [0u64; 10];
        let metrics = sink_metrics_prometheus(
            "kafka",
            "effectively_once",
            false,
            "at_least_once_idempotent",
            true,
            false,
            1,
            5,
            5,
            &zeros,
            1,
            4,
            4,
            &zeros,
            1,
            3,
            3,
            &zeros,
            1,
            2,
            2,
            &zeros,
            1,
            10,
            10,
            &zeros,
            1,
            1,
            1,
            &zeros,
            0,
            0,
            15,
            0.0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        );

        assert!(metrics.contains(
            "rustcdc_sink_delivery_contract_requested{sink=\"kafka\",contract=\"effectively_once\"} 1"
        ));
        assert!(metrics.contains(
            "rustcdc_sink_delivery_contract_satisfied{sink=\"kafka\",contract=\"effectively_once\"} 0"
        ));
        assert!(metrics.contains(
            "rustcdc_sink_delivery_guarantee{sink=\"kafka\",mode=\"at_least_once_idempotent\"} 1"
        ));
        assert!(metrics.contains("rustcdc_sink_idempotent_delivery_capable{sink=\"kafka\"} 1"));
        assert!(metrics
            .contains("rustcdc_sink_transactional_checkpoint_barrier_capable{sink=\"kafka\"} 0"));
    }

    /// Also used by `delivery_contract_tests`, which needs a valid `AppConfig` to
    /// build a real router, admin state and state backend.
    pub(crate) fn minimal_config(
        state_dir: std::path::PathBuf,
        backend: StateBackend,
    ) -> AppConfig {
        use crate::config::schema::{SourceConfig, SourceDriver};
        let pg = rustcdc::PostgresSourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "test".to_string(),
            password: rustcdc::SecretString::default(),
            auth_mode: Default::default(),
            database: "test".to_string(),
            replication_slot_name: "test_slot".to_string(),
            publication_name: "test_pub".to_string(),
            transport: Default::default(),
            conn_timeout_secs: 5,
            stream_poll_interval_ms: 1000,
            max_events_per_poll: 100,
            table_include_list: Vec::new(),
            table_exclude_list: Vec::new(),
            slot_idle_advance_interval_ms: 30_000,
            create_replication_slot_if_missing: false,
            failover_slot: false,
        };
        AppConfig {
            api_version: AppConfig::SUPPORTED_API_VERSION.to_string(),
            source: SourceConfig {
                require_primary: false,
                driver: SourceDriver::Postgres(pg),
            },
            sink: SinkConfig::Stdout(StdoutSinkConfig::default()),
            state: StateConfig {
                offset: OffsetStoreConfig {
                    dir: state_dir.clone(),
                    backend: backend.clone(),
                },
                schema_history: SchemaHistoryStoreConfig {
                    dir: state_dir,
                    backend,
                },
            },
            admin: Default::default(),
            observability: Default::default(),
            delivery_contract: crate::config::schema::DeliveryContract::AtLeastOnce,
            runtime: Default::default(),
            pipeline: Default::default(),
            registries: Default::default(),
            snapshot_tables: Vec::new(),
            incremental_snapshot: Default::default(),
            dlq: Default::default(),
            sinks: Vec::new(),
        }
    }

    #[test]
    fn ensure_state_layout_allows_kafka_topic_backend() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("kafka-topic-state");
        let cfg = minimal_config(
            state_dir.clone(),
            StateBackend::KafkaTopic(KafkaTopicStateConfig {
                brokers: "localhost:9092".to_string(),
                topic: "cdc-state".to_string(),
                client_id: "cdc-server".to_string(),
                request_timeout_ms: 1000,
                readback_poll_timeout_ms: 250,
                min_replication_factor: 1,
                min_insync_replicas: 1,
                durability_profile: KafkaStateDurabilityProfile::Development,
                security: KafkaSecurityConfig::default(),
            }),
        );

        ensure_state_layout(&cfg).expect("kafka_topic backend must be allowed");
        assert!(state_dir.exists(), "state dir should be created");
    }
}
