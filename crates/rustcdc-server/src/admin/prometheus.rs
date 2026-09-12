//! Prometheus text rendering for the admin surface.
//!
//! Split out of `admin/mod.rs` when the file-size architecture guard fired at 4 000 lines.
//! The guard exists precisely because a file this large hides the kind of defect that was
//! found in it — worker lifecycle sitting three thousand lines from the state it mutates —
//! so the response was to split rather than to raise the threshold.
//!
//! Rendering is a leaf concern: it reads `AdminStateData` and produces text, and nothing
//! here mutates anything. That is what makes it the cleanest first extraction.

use super::*;

/// Render the SLO and admin metric block.
///
/// `signal_worker_alive` is passed in rather than read from `data` because it is a
/// property of a *task handle*, not of the state struct — storing it in `AdminStateData`
/// would mean something has to refresh it, and a liveness flag that needs refreshing is
/// exactly as stale as whatever forgot to refresh it.
pub(crate) fn slo_prometheus(data: &AdminStateData, signal_worker_alive: bool) -> String {
    let heartbeat_unix_seconds = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let state_code = match data.state {
        InstanceState::Starting => 0,
        InstanceState::Running => 1,
        InstanceState::Stopping => 2,
        InstanceState::Stopped => 3,
        InstanceState::Error => 4,
    };
    let terminal_reason_code = data.last_terminal_reason_code.as_deref().unwrap_or("none");
    let cold_start_to_ready_ms = elapsed_ms(data.started_at, data.first_ready_at).unwrap_or(0);
    let cold_start_to_checkpoint_advance_ms =
        elapsed_ms(data.started_at, data.first_checkpoint_advanced_at).unwrap_or(0);
    let signal_health = signal_notification_health(data, Utc::now());

    let mut metrics = format!(
        concat!(
            "# HELP rustcdc_slo_readiness_checks_total Total readiness probes\n",
            "# TYPE rustcdc_slo_readiness_checks_total counter\n",
            "rustcdc_slo_readiness_checks_total {}\n",
            "# HELP rustcdc_slo_readiness_ready_total Ready readiness probes\n",
            "# TYPE rustcdc_slo_readiness_ready_total counter\n",
            "rustcdc_slo_readiness_ready_total {}\n",
            "# HELP rustcdc_slo_readiness_rate Readiness success ratio\n",
            "# TYPE rustcdc_slo_readiness_rate gauge\n",
            "rustcdc_slo_readiness_rate {}\n",
            "# HELP rustcdc_slo_checkpoint_age_seconds Age of the checkpoint file in seconds\n",
            "# TYPE rustcdc_slo_checkpoint_age_seconds gauge\n",
            "rustcdc_slo_checkpoint_age_seconds {}\n",
            "# HELP rustcdc_source_lag_seconds Source lag proxy derived from checkpoint age in seconds\n",
            "# TYPE rustcdc_source_lag_seconds gauge\n",
            "rustcdc_source_lag_seconds {}\n",
            "# HELP rustcdc_slo_admin_api_latency_seconds Last admin API latency in seconds\n",
            "# TYPE rustcdc_slo_admin_api_latency_seconds gauge\n",
            "rustcdc_slo_admin_api_latency_seconds {}\n",
            "# HELP rustcdc_slo_restart_recovery_seconds Time to first ready batch after start\n",
            "# TYPE rustcdc_slo_restart_recovery_seconds gauge\n",
            "rustcdc_slo_restart_recovery_seconds {}\n",
            "# HELP rustcdc_slo_cold_start_to_ready_ms Time from process start to first ready state in milliseconds\n",
            "# TYPE rustcdc_slo_cold_start_to_ready_ms gauge\n",
            "rustcdc_slo_cold_start_to_ready_ms {}\n",
            "# HELP rustcdc_slo_cold_start_to_checkpoint_advance_ms Time from process start to first durable checkpoint advance in milliseconds\n",
            "# TYPE rustcdc_slo_cold_start_to_checkpoint_advance_ms gauge\n",
            "rustcdc_slo_cold_start_to_checkpoint_advance_ms {}\n",
            "# HELP rustcdc_admin_control_plane_up Admin control-plane liveness signal\n",
            "# TYPE rustcdc_admin_control_plane_up gauge\n",
            "rustcdc_admin_control_plane_up 1\n",
            // `_up` above says the HTTP surface answered — which it does even when the
            // worker behind it is dead. This says the worker that executes signals is
            // still running. Without it, a dead worker is only inferable from
            // `signal_without_terminal_notification_total` 30 s later, which names a
            // different thing and does not distinguish "slow" from "gone".
            "# HELP rustcdc_admin_signal_worker_alive Whether the signal-action worker task is still running\n",
            "# TYPE rustcdc_admin_signal_worker_alive gauge\n",
            "rustcdc_admin_signal_worker_alive {}\n",
            "# HELP rustcdc_admin_signal_worker_panics_total Signal actions that panicked and were recovered\n",
            "# TYPE rustcdc_admin_signal_worker_panics_total counter\n",
            "rustcdc_admin_signal_worker_panics_total {}\n",
            "# HELP rustcdc_admin_control_plane_heartbeat_unix_seconds Admin control-plane heartbeat timestamp\n",
            "# TYPE rustcdc_admin_control_plane_heartbeat_unix_seconds gauge\n",
            "rustcdc_admin_control_plane_heartbeat_unix_seconds {}\n",
            "# HELP rustcdc_admin_control_plane_state_code Admin control-plane state code (starting=0,running=1,stopping=2,stopped=3,error=4)\n",
            "# TYPE rustcdc_admin_control_plane_state_code gauge\n",
            "rustcdc_admin_control_plane_state_code {}\n",
            "# HELP rustcdc_admin_shutdown_requests_os_signal_total Total shutdown requests initiated by OS shutdown signals\n",
            "# TYPE rustcdc_admin_shutdown_requests_os_signal_total counter\n",
            "rustcdc_admin_shutdown_requests_os_signal_total {}\n",
            "# HELP rustcdc_admin_shutdown_completions_stopped_total Total shutdown lifecycles that completed with stopped state\n",
            "# TYPE rustcdc_admin_shutdown_completions_stopped_total counter\n",
            "rustcdc_admin_shutdown_completions_stopped_total {}\n",
            "# HELP rustcdc_admin_shutdown_completions_error_total Total shutdown lifecycles that terminated in error state\n",
            "# TYPE rustcdc_admin_shutdown_completions_error_total counter\n",
            "rustcdc_admin_shutdown_completions_error_total {}\n",
            "# HELP rustcdc_signal_without_terminal_notification_total Accepted signal actions without terminal lifecycle notifications beyond timeout budget\n",
            "# TYPE rustcdc_signal_without_terminal_notification_total gauge\n",
            "rustcdc_signal_without_terminal_notification_total {}\n",
            "# HELP rustcdc_signal_duplicate_terminal_notification_total Signal actions that emitted more than one terminal lifecycle notification\n",
            "# TYPE rustcdc_signal_duplicate_terminal_notification_total gauge\n",
            "rustcdc_signal_duplicate_terminal_notification_total {}\n",
            "# HELP rustcdc_signal_notification_lag_seconds Max observed lag in seconds between STARTED and terminal notification states\n",
            "# TYPE rustcdc_signal_notification_lag_seconds gauge\n",
            "rustcdc_signal_notification_lag_seconds {}\n",
            "# HELP rustcdc_signal_terminal_timeout_budget_seconds Timeout budget in seconds for terminal notification emission\n",
            "# TYPE rustcdc_signal_terminal_timeout_budget_seconds gauge\n",
            "rustcdc_signal_terminal_timeout_budget_seconds {}\n",
            "# HELP rustcdc_signal_action_queue_rejections_total Async signal actions rejected because the worker queue was full or unavailable\n",
            "# TYPE rustcdc_signal_action_queue_rejections_total counter\n",
            "rustcdc_signal_action_queue_rejections_total {}\n",
            "# HELP rustcdc_signal_action_started_notification_rejections_total Signal actions rejected because STARTED lifecycle notifications could not be emitted to non-admin channels\n",
            "# TYPE rustcdc_signal_action_started_notification_rejections_total counter\n",
            "rustcdc_signal_action_started_notification_rejections_total {}\n",
            "# HELP rustcdc_signal_action_queue_depth Current number of queued async signal actions waiting to be processed\n",
            "# TYPE rustcdc_signal_action_queue_depth gauge\n",
            "rustcdc_signal_action_queue_depth {}\n",
            "# HELP rustcdc_snapshot_requests_available Whether this pipeline can service an on-demand incremental snapshot (1=yes, 0=no)\n",
            "# TYPE rustcdc_snapshot_requests_available gauge\n",
            "rustcdc_snapshot_requests_available {}\n",
            "# HELP rustcdc_snapshot_requests_accepted_total On-demand incremental snapshot requests the runtime accepted\n",
            "# TYPE rustcdc_snapshot_requests_accepted_total counter\n",
            "rustcdc_snapshot_requests_accepted_total {}\n",
            "# HELP rustcdc_snapshot_requests_refused_total On-demand incremental snapshot requests the runtime refused\n",
            "# TYPE rustcdc_snapshot_requests_refused_total counter\n",
            "rustcdc_snapshot_requests_refused_total {}\n",
            "# HELP rustcdc_snapshot_tables_enqueued_total Tables enqueued for backfill across all accepted on-demand snapshot requests\n",
            "# TYPE rustcdc_snapshot_tables_enqueued_total counter\n",
            "rustcdc_snapshot_tables_enqueued_total {}\n",
            "# HELP rustcdc_signal_ingress_parse_rejections_total Signal ingress records rejected because payload parsing failed\n",
            "# TYPE rustcdc_signal_ingress_parse_rejections_total counter\n",
            "rustcdc_signal_ingress_parse_rejections_total {}\n",
            "# HELP rustcdc_signal_ingress_line_too_large_rejections_total Signal ingress records rejected because line size exceeded the limit\n",
            "# TYPE rustcdc_signal_ingress_line_too_large_rejections_total counter\n",
            "rustcdc_signal_ingress_line_too_large_rejections_total {}\n",
            "# HELP rustcdc_signal_ingress_validation_rejections_total Signal ingress records rejected by semantic validation\n",
            "# TYPE rustcdc_signal_ingress_validation_rejections_total counter\n",
            "rustcdc_signal_ingress_validation_rejections_total {}\n",
            "# HELP rustcdc_signal_ingress_max_line_bytes Maximum accepted signal ingress line size in bytes\n",
            "# TYPE rustcdc_signal_ingress_max_line_bytes gauge\n",
            "rustcdc_signal_ingress_max_line_bytes {}\n",
            "# HELP rustcdc_signal_notification_log_emitted_total Notification lifecycle events emitted to non-admin log channel\n",
            "# TYPE rustcdc_signal_notification_log_emitted_total counter\n",
            "rustcdc_signal_notification_log_emitted_total {}\n",
            "# HELP rustcdc_signal_notification_log_emit_failures_total Notification lifecycle events that failed to emit to non-admin log channel\n",
            "# TYPE rustcdc_signal_notification_log_emit_failures_total counter\n",
            "rustcdc_signal_notification_log_emit_failures_total {}\n",
            "# HELP rustcdc_admin_rate_limited_readyz_total Total `/readyz` requests denied by admin abuse controls\n",
            "# TYPE rustcdc_admin_rate_limited_readyz_total counter\n",
            "rustcdc_admin_rate_limited_readyz_total {}\n",
            "# HELP rustcdc_admin_rate_limited_status_total Total `/status` requests denied by admin abuse controls\n",
            "# TYPE rustcdc_admin_rate_limited_status_total counter\n",
            "rustcdc_admin_rate_limited_status_total {}\n",
            "# HELP rustcdc_admin_rate_limited_metrics_total Total `/metrics` requests denied by admin abuse controls\n",
            "# TYPE rustcdc_admin_rate_limited_metrics_total counter\n",
            "rustcdc_admin_rate_limited_metrics_total {}\n",
            "# HELP rustcdc_admin_rate_limiter_readyz_decisions_total Total `/readyz` limiter decisions evaluated\n",
            "# TYPE rustcdc_admin_rate_limiter_readyz_decisions_total counter\n",
            "rustcdc_admin_rate_limiter_readyz_decisions_total {}\n",
            "# HELP rustcdc_admin_rate_limiter_readyz_decision_latency_seconds_avg Average `/readyz` limiter decision latency in seconds\n",
            "# TYPE rustcdc_admin_rate_limiter_readyz_decision_latency_seconds_avg gauge\n",
            "rustcdc_admin_rate_limiter_readyz_decision_latency_seconds_avg {}\n",
            "# HELP rustcdc_admin_rate_limiter_readyz_decision_latency_seconds_max Max `/readyz` limiter decision latency in seconds\n",
            "# TYPE rustcdc_admin_rate_limiter_readyz_decision_latency_seconds_max gauge\n",
            "rustcdc_admin_rate_limiter_readyz_decision_latency_seconds_max {}\n",
            "# HELP rustcdc_admin_rate_limiter_status_decisions_total Total `/status` limiter decisions evaluated\n",
            "# TYPE rustcdc_admin_rate_limiter_status_decisions_total counter\n",
            "rustcdc_admin_rate_limiter_status_decisions_total {}\n",
            "# HELP rustcdc_admin_rate_limiter_status_decision_latency_seconds_avg Average `/status` limiter decision latency in seconds\n",
            "# TYPE rustcdc_admin_rate_limiter_status_decision_latency_seconds_avg gauge\n",
            "rustcdc_admin_rate_limiter_status_decision_latency_seconds_avg {}\n",
            "# HELP rustcdc_admin_rate_limiter_status_decision_latency_seconds_max Max `/status` limiter decision latency in seconds\n",
            "# TYPE rustcdc_admin_rate_limiter_status_decision_latency_seconds_max gauge\n",
            "rustcdc_admin_rate_limiter_status_decision_latency_seconds_max {}\n",
            "# HELP rustcdc_admin_rate_limiter_metrics_decisions_total Total `/metrics` limiter decisions evaluated\n",
            "# TYPE rustcdc_admin_rate_limiter_metrics_decisions_total counter\n",
            "rustcdc_admin_rate_limiter_metrics_decisions_total {}\n",
            "# HELP rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_avg Average `/metrics` limiter decision latency in seconds\n",
            "# TYPE rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_avg gauge\n",
            "rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_avg {}\n",
            "# HELP rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_max Max `/metrics` limiter decision latency in seconds\n",
            "# TYPE rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_max gauge\n",
            "rustcdc_admin_rate_limiter_metrics_decision_latency_seconds_max {}\n",
            "# HELP rustcdc_admin_reconciliation_recoveries_total Total startup reconciliation markers auto-recovered\n",
            "# TYPE rustcdc_admin_reconciliation_recoveries_total counter\n",
            "rustcdc_admin_reconciliation_recoveries_total {}\n",
            "# HELP rustcdc_admin_reconciliation_recovery_last_unix_seconds Last startup reconciliation recovery timestamp in unix seconds (-1 when unavailable)\n",
            "# TYPE rustcdc_admin_reconciliation_recovery_last_unix_seconds gauge\n",
            "rustcdc_admin_reconciliation_recovery_last_unix_seconds {}\n",
            "# HELP rustcdc_admin_reconciliation_recovery_last_parse_ok Whether the last recovered marker parsed successfully (1=true, 0=false, -1=unavailable)\n",
            "# TYPE rustcdc_admin_reconciliation_recovery_last_parse_ok gauge\n",
            "rustcdc_admin_reconciliation_recovery_last_parse_ok {}\n",
            "# HELP rustcdc_admin_reconciliation_recovery_proof_last_unix_seconds Last reconciliation recovery proof timestamp in unix seconds (-1 when unavailable)\n",
            "# TYPE rustcdc_admin_reconciliation_recovery_proof_last_unix_seconds gauge\n",
            "rustcdc_admin_reconciliation_recovery_proof_last_unix_seconds {}\n",
            "# HELP rustcdc_admin_reconciliation_recovery_proof_last_ok Whether the last reconciliation recovery proof succeeded (1=true, 0=false, -1=unavailable)\n",
            "# TYPE rustcdc_admin_reconciliation_recovery_proof_last_ok gauge\n",
            "rustcdc_admin_reconciliation_recovery_proof_last_ok {}\n",
            "# HELP rustcdc_admin_last_terminal_reason_code Last runtime terminal reason code (label value)\n",
            "# TYPE rustcdc_admin_last_terminal_reason_code gauge\n",
            "rustcdc_admin_last_terminal_reason_code{{reason=\"{}\"}} 1\n"
        ),
        data.readiness_checks_total,
        data.readiness_ready_total,
        readiness_rate(data),
        data.checkpoint_age_seconds.unwrap_or(0.0),
        data.checkpoint_age_seconds.unwrap_or(0.0),
        data.last_admin_api_latency_us.unwrap_or(0) as f64 / 1_000_000.0,
        data.restart_recovery_seconds.unwrap_or(0.0),
        cold_start_to_ready_ms,
        cold_start_to_checkpoint_advance_ms,
        heartbeat_unix_seconds,
        // The two new gauges sit between `_up` and `_state_code` in the format string, so
        // their arguments go here. The worker is alive whenever this render is reached
        // and the handle has not finished — see `signal_worker_alive`.
        u8::from(signal_worker_alive),
        data.signal_worker_panics_total,
        state_code,
        data.shutdown_requests_os_signal_total,
        data.shutdown_completions_stopped_total,
        data.shutdown_completions_error_total,
        signal_health.without_terminal_total,
        signal_health.duplicate_terminal_total,
        signal_health.lag_seconds,
        SIGNAL_TERMINAL_TIMEOUT_SECONDS,
        data.signal_action_queue_rejections_total,
        data.signal_action_started_notification_rejections_total,
        data.signal_action_queue_depth,
        u8::from(data.snapshot_requests_available),
        data.snapshot_requests_accepted_total,
        data.snapshot_requests_refused_total,
        data.snapshot_tables_enqueued_total,
        data.signal_ingress_parse_rejections_total,
        data.signal_ingress_line_too_large_rejections_total,
        data.signal_ingress_validation_rejections_total,
        SIGNAL_INGRESS_MAX_LINE_BYTES,
        data.notification_log_emitted_total,
        data.notification_log_emit_failures_total,
        data.admin_rate_limited_readyz_total,
        data.admin_rate_limited_status_total,
        data.admin_rate_limited_metrics_total,
        data.admin_rate_limiter_readyz_decisions_total,
        latency_avg_seconds(
            data.admin_rate_limiter_readyz_decisions_total,
            data.admin_rate_limiter_readyz_decision_latency_seconds_sum,
        ),
        data.admin_rate_limiter_readyz_decision_latency_seconds_max,
        data.admin_rate_limiter_status_decisions_total,
        latency_avg_seconds(
            data.admin_rate_limiter_status_decisions_total,
            data.admin_rate_limiter_status_decision_latency_seconds_sum,
        ),
        data.admin_rate_limiter_status_decision_latency_seconds_max,
        data.admin_rate_limiter_metrics_decisions_total,
        latency_avg_seconds(
            data.admin_rate_limiter_metrics_decisions_total,
            data.admin_rate_limiter_metrics_decision_latency_seconds_sum,
        ),
        data.admin_rate_limiter_metrics_decision_latency_seconds_max,
        data.reconciliation_recoveries_total,
        data.reconciliation_recovery_last_unix_seconds
            .unwrap_or(-1.0),
        data.reconciliation_recovery_last_parse_ok
            .map(|ok| if ok { 1 } else { 0 })
            .unwrap_or(-1),
        data.reconciliation_recovery_proof_last_unix_seconds
            .unwrap_or(-1.0),
        data.reconciliation_recovery_proof_last_ok
            .map(|ok| if ok { 1 } else { 0 })
            .unwrap_or(-1),
        terminal_reason_code,
    );

    metrics.push_str(&notification_channel_metric_lines(
        "rustcdc_signal_notification_channel_emitted_total",
        "Notification lifecycle events emitted to non-admin notification channels by channel",
        &data.notification_channel_emitted_total,
    ));
    metrics.push_str(&notification_channel_metric_lines(
        "rustcdc_signal_notification_channel_emit_failures_total",
        "Notification lifecycle events that failed to emit to non-admin notification channels by channel",
        &data.notification_channel_emit_failures_total,
    ));

    // Replication-slot lag is **not** emitted here. It comes from the runtime's own
    // measurement, rendered by `rustcdc::core::write_runtime_metrics_prometheus` as
    // `rustcdc_replication_slot_lag_bytes`.

    // Consecutive source poll errors — reflects the backoff-retry counter in
    // `RecoverableErrorState`.  A value ≥ READYZ_SOURCE_CONSECUTIVE_ERROR_THRESHOLD
    // means /readyz is already returning 503.
    {
        let n = data.source_consecutive_errors;
        metrics.push_str("# HELP rustcdc_source_consecutive_poll_errors Consecutive recoverable source poll errors since last successful batch (0 when healthy)\n");
        metrics.push_str("# TYPE rustcdc_source_consecutive_poll_errors gauge\n");
        metrics.push_str(&format!("rustcdc_source_consecutive_poll_errors {n}\n"));
    }

    let notification_channel_enabled = HashMap::from([
        ("file".to_string(), data.notification_log_enabled),
        ("kafka".to_string(), data.notification_kafka_enabled),
    ]);
    metrics.push_str(&notification_channel_gauge_lines(
        "rustcdc_signal_notification_channel_enabled",
        "Configured non-admin notification channels enabled by channel",
        &notification_channel_enabled,
    ));

    // Admin API latency histogram (for histogram_quantile / P50/P95/P99 in Prometheus).
    metrics.push_str("# HELP rustcdc_slo_admin_api_latency_seconds_histogram Admin API request latency histogram in seconds\n");
    metrics.push_str("# TYPE rustcdc_slo_admin_api_latency_seconds_histogram histogram\n");
    let mut cumulative: u64 = 0;
    for (i, &bound) in ADMIN_API_LATENCY_HISTOGRAM_BUCKETS_US.iter().enumerate() {
        cumulative += data.admin_api_latency_us_buckets[i];
        metrics.push_str(&format!(
            "rustcdc_slo_admin_api_latency_seconds_histogram_bucket{{le=\"{}\"}} {}\n",
            bound as f64 / 1_000_000.0,
            cumulative
        ));
    }
    metrics.push_str(&format!(
        "rustcdc_slo_admin_api_latency_seconds_histogram_bucket{{le=\"+Inf\"}} {}\n",
        data.admin_api_latency_us_count
    ));
    metrics.push_str(&format!(
        "rustcdc_slo_admin_api_latency_seconds_histogram_sum {}\n",
        data.admin_api_latency_us_sum / 1_000_000.0
    ));
    metrics.push_str(&format!(
        "rustcdc_slo_admin_api_latency_seconds_histogram_count {}\n",
        data.admin_api_latency_us_count
    ));

    metrics
}

/// Live incremental-snapshot progress, as Prometheus gauges.
///
/// Emitted only while a snapshot is in flight. That is deliberate: a gauge that reports
/// `0` when nothing is running is indistinguishable from one reporting a stalled snapshot,
/// and `absent()` is the honest PromQL for "no backfill".
///
/// Per-table series are labelled by table. Cardinality is bounded by
/// `incremental_snapshot.tables` plus whatever `execute_snapshot` has added — an operator's
/// explicit list, not anything derived from the data — so this cannot blow up the way a
/// per-row label would.
pub(super) fn snapshot_progress_prometheus(
    state: Option<rustcdc::IncrementalSnapshotState>,
) -> String {
    use std::fmt::Write as _;

    let Some(state) = state else {
        return String::new();
    };

    let mut out = String::new();
    out.push_str(
        "# HELP rustcdc_incremental_snapshot_active Whether an incremental snapshot is in flight\n\
         # TYPE rustcdc_incremental_snapshot_active gauge\n",
    );
    out.push_str("rustcdc_incremental_snapshot_active 1\n");

    out.push_str(
        "# HELP rustcdc_incremental_snapshot_paused Whether chunk reading is paused (the live stream is unaffected)\n\
         # TYPE rustcdc_incremental_snapshot_paused gauge\n",
    );
    let _ = writeln!(
        out,
        "rustcdc_incremental_snapshot_paused {}",
        u8::from(state.paused)
    );

    // `stopped` is distinct from "no tables left". Conflating them makes a stop silently
    // undo itself: a table absent from the
    // persisted state looks like one that has not started, so every configured table
    // restarted from row zero on the next deploy — re-running the backfill an operator had
    // just stopped, usually to take load off a production primary.
    out.push_str(
        "# HELP rustcdc_incremental_snapshot_stopped Whether the snapshot was abandoned by stop_snapshot\n\
         # TYPE rustcdc_incremental_snapshot_stopped gauge\n",
    );
    let _ = writeln!(
        out,
        "rustcdc_incremental_snapshot_stopped {}",
        u8::from(state.stopped)
    );

    // The generation distinguishes a deliberate re-snapshot from a replay. Without it the
    // two are byte-identical and the idempotency guard drops the re-snapshot — an operator
    // re-requesting a table got `enqueued: 1` and no rows.
    out.push_str(
        "# HELP rustcdc_incremental_snapshot_generation Times snapshot work has been requested on this driver\n\
         # TYPE rustcdc_incremental_snapshot_generation counter\n",
    );
    let _ = writeln!(
        out,
        "rustcdc_incremental_snapshot_generation {}",
        state.generation
    );

    // Whether a row filter is in effect, per table. "3,000,000 rows emitted" cannot on its
    // own distinguish a filter that applied from one that was silently ignored — exactly
    // the question the on-demand-filter defect made people ask, and it was unanswerable
    // from outside the process. The expression itself is deliberately **not** exported: it
    // is raw SQL and can carry column names and literal values that have no business in an
    // unauthenticated metrics scrape. `/status` reports it to an authenticated reader.
    out.push_str(
        "# HELP rustcdc_incremental_snapshot_table_filtered Whether a row filter is in effect for this table\n\
         # TYPE rustcdc_incremental_snapshot_table_filtered gauge\n",
    );
    for table in &state.tables {
        let _ = writeln!(
            out,
            "rustcdc_incremental_snapshot_table_filtered{{table=\"{}\"}} {}",
            escape_metric_label(&table.table),
            u8::from(table.condition.is_some())
        );
    }

    out.push_str(
        "# HELP rustcdc_incremental_snapshot_tables_remaining Tables not yet read to exhaustion\n\
         # TYPE rustcdc_incremental_snapshot_tables_remaining gauge\n",
    );
    let _ = writeln!(
        out,
        "rustcdc_incremental_snapshot_tables_remaining {}",
        state.tables_remaining()
    );

    out.push_str(
        "# HELP rustcdc_incremental_snapshot_rows_emitted Rows emitted by the backfill across every table\n\
         # TYPE rustcdc_incremental_snapshot_rows_emitted counter\n",
    );
    let _ = writeln!(
        out,
        "rustcdc_incremental_snapshot_rows_emitted {}",
        state.rows_emitted()
    );

    out.push_str(
        "# HELP rustcdc_incremental_snapshot_table_rows_emitted Rows emitted per table\n\
         # TYPE rustcdc_incremental_snapshot_table_rows_emitted counter\n",
    );
    for table in &state.tables {
        let _ = writeln!(
            out,
            "rustcdc_incremental_snapshot_table_rows_emitted{{table=\"{}\"}} {}",
            escape_metric_label(&table.table),
            table.rows_emitted
        );
    }

    out.push_str(
        "# HELP rustcdc_incremental_snapshot_table_complete Whether a table has been read to exhaustion\n\
         # TYPE rustcdc_incremental_snapshot_table_complete gauge\n",
    );
    for table in &state.tables {
        let _ = writeln!(
            out,
            "rustcdc_incremental_snapshot_table_complete{{table=\"{}\"}} {}",
            escape_metric_label(&table.table),
            u8::from(table.is_complete)
        );
    }

    out
}

/// Escape a Prometheus label value: backslash, quote, newline.
pub(super) fn escape_metric_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
