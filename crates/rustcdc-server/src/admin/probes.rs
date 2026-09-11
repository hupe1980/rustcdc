//! Kubernetes probe endpoints: `/healthz`, `/livez` and `/readyz`.
//!
//! Split out of `admin/mod.rs` because they are one concern with one audience — an
//! orchestrator deciding whether to restart a pod or route traffic to it — and because
//! the module had grown past the size the architecture guard allows.
//!
//! The three answer different questions, and conflating them is the usual mistake:
//!
//! | Probe | Question | Wrong answer costs |
//! |---|---|---|
//! | `/healthz` | Is the HTTP server up? | Nothing; it is a smoke test |
//! | `/livez` | Should this process be **restarted**? | A restart loop, or a wedged pod that never restarts |
//! | `/readyz` | Should this replica receive **traffic**, and should a rollout proceed? | A broken deploy reaching every pod |
//!
//! Liveness is destructive and readiness is not, which is why they weigh the runtime's
//! stall causes differently. See [`livez`] for that reasoning.

use super::*;

pub(super) async fn healthz(State(_admin): State<AdminState>, _headers: HeaderMap) -> Response {
    (StatusCode::OK, "ok").into_response()
}

/// Kubernetes `livenessProbe` endpoint.
///
/// Returns `200 ok` when the process should continue running, `503` when it
/// should be restarted.  Unlike `/readyz` (which signals traffic readiness),
/// `/livez` signals process health:
///
/// - Returns `503` when `InstanceState::Error` (unrecoverable failure).
/// - Returns `503` when `source_consecutive_errors` has been at-or-above the
///   readiness threshold for longer than `LIVEZ_DEGRADED_TIMEOUT`.  This
///   catches pipelines stuck in indefinite circuit-breaker backoff that will
///   never self-heal.
/// - Returns `503` when the runtime has reported
///   `StallCause::PollLoopNotTurning` continuously for longer than
///   `LIVEZ_STALL_TIMEOUT`.
///
/// # Why the verdict has to be consulted here
///
/// The two conditions above are both driven by *errors*.  A poll blocked inside
/// the source returns none: a TCP connection that was accepted and then went
/// silent, a database that stopped answering mid-query, a `Future` that simply
/// never completes.  `source_consecutive_errors` stays at zero because nothing
/// failed, `InstanceState` stays `Running` because nothing ended, and this probe
/// answered `200 alive` for as long as the process existed.  The pipeline was
/// dead and every signal Kubernetes had said it was fine.
///
/// The health verdict is the one signal that detects it — it measures the
/// *absence* of progress rather than the presence of failure — and nothing was
/// acting on it.
///
/// **Only `PollLoopNotTurning` fails liveness, and the exclusions are the point:**
///
/// - `UnconfirmedSourcePosition` must **not** restart the process.  A restart
///   replays from the same checkpoint and fails the same way, and meanwhile the
///   source keeps retaining log — a crash-loop here makes a full `pg_wal`
///   volume arrive *sooner*.  It is a page, not a reboot.
/// - `ConsumerNotAcknowledging` means the sink is not draining.  Restarting
///   thrashes against a downstream that is already unhealthy; `/readyz` reports
///   it instead.
///
/// This is exactly what a stable `StallCause` is for.  The reason string could
/// not be branched on without matching prose.
///
/// Kubernetes recommended configuration:
/// ```yaml
/// livenessProbe:
///   httpGet:
///     path: /livez
///     port: <admin_port>
///   failureThreshold: 3
///   periodSeconds: 10
/// ```
pub(super) const LIVEZ_DEGRADED_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes

/// How long `StallCause::PollLoopNotTurning` must persist before the process is
/// declared unrecoverable.
///
/// Deliberately several times the runtime's own stall threshold (`max_poll_wait_ms × 6`,
/// floor 30 s).  The verdict is already a considered judgement rather than a raw sample,
/// and restarting a pipeline is destructive — it drops the in-flight batch and replays
/// from the last checkpoint.  Waiting is cheap here; a restart loop is not.
///
/// With the default `max_poll_wait_ms` the pipeline must be wedged for 30 s before the
/// verdict flips and a further 120 s before this fires, so a pod with the recommended
/// `failureThreshold: 3` / `periodSeconds: 10` restarts at roughly three minutes.
pub(super) const LIVEZ_STALL_TIMEOUT: Duration = Duration::from_secs(120);

pub(super) async fn livez(State(admin): State<AdminState>) -> Response {
    let (state, source_consecutive_errors, degraded_since, stall_cause, stalled_for) = {
        let d = admin.data.read().await;
        (
            d.state.clone(),
            d.source_consecutive_errors,
            d.degraded_since,
            d.health_stall_cause.clone(),
            d.stalled_for(),
        )
    };

    if matches!(state, InstanceState::Error) {
        return (StatusCode::SERVICE_UNAVAILABLE, "error").into_response();
    }

    if source_consecutive_errors >= READYZ_SOURCE_CONSECUTIVE_ERROR_THRESHOLD
        && let Some(since) = degraded_since
        && since.elapsed() > LIVEZ_DEGRADED_TIMEOUT
    {
        return (StatusCode::SERVICE_UNAVAILABLE, "source-degraded-timeout").into_response();
    }

    // Compared against the stable `StallCause` label, never the reason prose.
    if stall_cause.as_deref() == Some(StallCause::PollLoopNotTurning.as_str())
        && stalled_for.is_some_and(|elapsed| elapsed > LIVEZ_STALL_TIMEOUT)
    {
        return (StatusCode::SERVICE_UNAVAILABLE, "poll-loop-stalled").into_response();
    }

    (StatusCode::OK, "alive").into_response()
}

pub(super) async fn readyz(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (allowed, decision_latency) =
        admin.allow_abuse_scope(AbuseLimitScope::Readyz, &headers, Some(peer_addr));
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Readyz, decision_latency)
        .await;
    if !allowed {
        admin
            .record_rate_limited_request(AbuseLimitScope::Readyz)
            .await;
        return rate_limited_response("readyz");
    }

    if !admin.authorize_readyz(&headers) {
        return unauthorized_response();
    }

    let start = std::time::Instant::now();
    let (state, source_consecutive_errors, stall_cause) = {
        let d = admin.data.read().await;
        (
            d.state.clone(),
            d.source_consecutive_errors,
            d.health_stall_cause.clone(),
        )
    };
    // Degraded: still Running but source poll errors have accumulated beyond the
    // readiness threshold.  Report 503 so Kubernetes removes us from the
    // load balancer before the circuit-breaker escalates to a terminal failure.
    let sink_degraded = matches!(state, InstanceState::Running)
        && source_consecutive_errors >= READYZ_SOURCE_CONSECUTIVE_ERROR_THRESHOLD;
    // Any stall means this replica is not doing the job it exists to do, so it is not
    // ready — and unlike `/livez`, reporting so is free: readiness stops a rollout and
    // takes the pod out of rotation, it does not destroy in-flight work. That makes the
    // cause-based exclusions unnecessary here, and a stalled replica that is *silently
    // marked ready* is how a broken deploy reaches every pod.
    //
    // Deliberately keyed on the stall cause being present rather than on the verdict
    // label, so `idle` can never reach this branch. A quiet database is ready.
    let stalled = matches!(state, InstanceState::Running) && stall_cause.is_some();
    let ready = matches!(state, InstanceState::Running | InstanceState::Stopping)
        && !sink_degraded
        && !stalled;
    admin.record_readiness_probe(ready, start.elapsed()).await;
    match state {
        InstanceState::Running if sink_degraded => {
            (StatusCode::SERVICE_UNAVAILABLE, "source-degraded").into_response()
        }
        // The cause is in the body because it is what tells an operator reading
        // `kubectl describe` whether to look at the database, this process or the sink.
        InstanceState::Running if stalled => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("stalled:{}", stall_cause.as_deref().unwrap_or("unknown")),
        )
            .into_response(),
        InstanceState::Running => (StatusCode::OK, "ready").into_response(),
        InstanceState::Stopping => (StatusCode::OK, "stopping").into_response(),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response(),
    }
}
