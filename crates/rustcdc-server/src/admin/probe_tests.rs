//! Tests for the Kubernetes probe endpoints in [`super::probes`].
//!
//! Their own file rather than more of `admin/tests.rs`, which the architecture guard had
//! already grown past: these all drive one pair of handlers through one question — does
//! the runtime's health verdict reach the orchestrator — and they read better together.

use std::net::SocketAddr;
use std::time::Duration;

use axum::{
    body::to_bytes,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
};
use rustcdc::core::{HealthVerdict, StallCause};

use super::probes::LIVEZ_STALL_TIMEOUT;
use super::tests::sample_config;
use super::{AdminState, InstanceState, livez, readyz};
use crate::config::schema::AdminProbeAuthMode;

/// An admin state whose probes accept unauthenticated loopback requests.
async fn probe_admin() -> AdminState {
    let mut cfg = sample_config();
    cfg.admin.probe_auth_mode = AdminProbeAuthMode::AllowUnauthenticatedLoopback;
    AdminState::new(&cfg).await.expect("admin state")
}

/// Drive the admin state to a given verdict, as the event loop does after a batch.
async fn set_health(admin: &AdminState, health: HealthVerdict) {
    let mut d = admin.data.write().await;
    d.state = InstanceState::Running;
    d.record_health_verdict(&health);
}

/// Backdate the current stall so the probe sees it as long-standing.
async fn backdate_stall(admin: &AdminState, by: Duration) {
    let mut d = admin.data.write().await;
    let since = d.stalled_since.expect("a stall must be in progress");
    d.stalled_since = Some(since - by);
}

fn stalled(cause: StallCause) -> HealthVerdict {
    HealthVerdict::Stalled {
        cause,
        // Deliberately different on each construction, as the real one is.
        reason: format!("{cause} for {}ms", 30_000 + rand_ish()),
    }
}

fn rand_ish() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 % 1000)
        .unwrap_or(0)
}

async fn livez_body(admin: &AdminState) -> (StatusCode, String) {
    let response = livez(State(admin.clone())).await;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1 << 16).await.expect("body");
    (status, String::from_utf8_lossy(&bytes).to_string())
}

/// `/readyz` is authenticated; these tests are about the verdict, not the authorisation,
/// which has its own suite. The loopback probe-auth mode is the same one
/// `readyz_returns_ok_while_stopping_when_probe_auth_allows_loopback` uses.
async fn readyz_body(admin: &AdminState) -> (StatusCode, String) {
    let response = readyz(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 8080))),
        HeaderMap::new(),
    )
    .await;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1 << 16).await.expect("body");
    (status, String::from_utf8_lossy(&bytes).to_string())
}

/// A poll loop wedged inside the source must eventually fail liveness.
///
/// This is the failure the health verdict exists to detect and the one nothing acted on.
/// Both existing `/livez` conditions are driven by *errors*: a poll blocked inside the
/// source produces none — no `SourceError`, no state transition, just a future that never
/// completes — so `source_consecutive_errors` stayed at zero, `InstanceState` stayed
/// `Running`, and this probe answered `200 alive` for the life of the process while the
/// pipeline was dead.
#[tokio::test]
pub(super) async fn livez_fails_when_the_poll_loop_has_been_wedged_long_enough() {
    let admin = probe_admin().await;

    set_health(&admin, stalled(StallCause::PollLoopNotTurning)).await;

    // Freshly stalled: not yet a restart candidate. Restarting a pipeline drops the
    // in-flight batch, so the probe waits out the timeout rather than reacting to the
    // first sample.
    let (status, _) = livez_body(&admin).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a stall shorter than LIVEZ_STALL_TIMEOUT must not restart the pod",
    );

    backdate_stall(&admin, LIVEZ_STALL_TIMEOUT + Duration::from_secs(1)).await;
    let (status, body) = livez_body(&admin).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, "poll-loop-stalled");
}

/// Restarting is the wrong remedy for two of the three stall causes.
///
/// `UnconfirmedSourcePosition` is the important one: a restart replays from the same
/// checkpoint and fails identically, while the source keeps retaining log. A crash-loop
/// there makes a full `pg_wal` volume arrive *sooner*, so the probe must stay green and
/// let the page do its job. `ConsumerNotAcknowledging` means the sink is not draining;
/// restarting thrashes against a downstream that is already unhealthy.
///
/// Branching on a stable `StallCause` is what makes this expressible at all — the reason
/// string could only have been matched as prose.
#[tokio::test]
pub(super) async fn livez_stays_green_for_stalls_a_restart_cannot_fix() {
    let admin = probe_admin().await;

    for cause in [
        StallCause::UnconfirmedSourcePosition,
        StallCause::ConsumerNotAcknowledging,
    ] {
        set_health(&admin, stalled(cause)).await;
        backdate_stall(&admin, LIVEZ_STALL_TIMEOUT * 10).await;

        let (status, body) = livez_body(&admin).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{cause} must not restart the process (got {body})",
        );
    }
}

/// The stall clock measures one continuous condition, not "how long something was wrong".
///
/// A pipeline alternating between two different stalls is a different situation from one
/// wedged in a single state, and only the second is a restart candidate. Without the
/// reset, unrelated conditions would accumulate into a restart.
#[tokio::test]
pub(super) async fn the_stall_clock_restarts_when_the_cause_changes() {
    let admin = probe_admin().await;

    set_health(&admin, stalled(StallCause::PollLoopNotTurning)).await;
    backdate_stall(&admin, LIVEZ_STALL_TIMEOUT * 10).await;

    // A different stall supersedes it, and the clock starts over.
    set_health(&admin, stalled(StallCause::ConsumerNotAcknowledging)).await;
    set_health(&admin, stalled(StallCause::PollLoopNotTurning)).await;

    let (status, _) = livez_body(&admin).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the clock must restart when the cause changes, not carry over",
    );

    // …and the same unchanging stall keeps accumulating. The reason string differs on
    // every construction, exactly as the real one does, so this also pins that the latch
    // compares the cause rather than the prose.
    let before = {
        let d = admin.data.read().await;
        d.stalled_since.expect("stalled")
    };
    set_health(&admin, stalled(StallCause::PollLoopNotTurning)).await;
    let after = {
        let d = admin.data.read().await;
        d.stalled_since.expect("stalled")
    };
    assert_eq!(
        before, after,
        "a changing reason string must not look like a new stall",
    );
}

/// Readiness reports every stall, and reports which one.
///
/// Unlike liveness this is free — it stops a rollout and takes the pod out of rotation
/// rather than destroying in-flight work — so there is no reason to exclude a cause. A
/// stalled replica that reports itself ready is how a broken deploy reaches every pod.
#[tokio::test]
pub(super) async fn readyz_reports_a_stall_and_names_its_cause() {
    let admin = probe_admin().await;

    for cause in [
        StallCause::PollLoopNotTurning,
        StallCause::UnconfirmedSourcePosition,
        StallCause::ConsumerNotAcknowledging,
    ] {
        set_health(&admin, stalled(cause)).await;
        let (status, body) = readyz_body(&admin).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{cause}");
        assert_eq!(
            body,
            format!("stalled:{cause}"),
            "the body must name the cause — it is what tells an operator reading \
             `kubectl describe` whether to look at the database, this process or the sink",
        );
    }
}

/// A quiet database is ready, and alive.
///
/// `Idle` is the single most common benign state a pipeline reaches, and the whole point
/// of separating it from `Stalled`. A probe that treats it as a fault would take every
/// healthy pipeline out of rotation the moment its source went quiet.
#[tokio::test]
pub(super) async fn an_idle_pipeline_is_ready_and_alive() {
    let admin = probe_admin().await;

    for verdict in [HealthVerdict::Idle, HealthVerdict::Healthy] {
        set_health(&admin, verdict.clone()).await;

        let (status, body) = readyz_body(&admin).await;
        assert_eq!(status, StatusCode::OK, "{verdict:?} must be ready ({body})");

        let (status, body) = livez_body(&admin).await;
        assert_eq!(status, StatusCode::OK, "{verdict:?} must be alive ({body})");

        let d = admin.data.read().await;
        assert!(
            d.stalled_since.is_none(),
            "a non-stalled verdict must clear the stall clock",
        );
    }
}

/// `/status` must carry the verdict, because it is where the runbook sends people first.
///
/// It did not, and the verdict was reachable only by scraping `/metrics` and grepping a
/// one-hot gauge — for the single field that says whether the pipeline is working.
#[tokio::test]
pub(super) async fn status_reports_the_verdict_and_its_cause() {
    let admin = probe_admin().await;

    set_health(&admin, HealthVerdict::Idle).await;
    let health = {
        let d = admin.data.read().await;
        super::health_json(&d)
    };
    assert_eq!(health["verdict"], "idle");
    assert!(
        health["stall_cause"].is_null(),
        "a non-stalled verdict has no cause: {health:#}",
    );
    assert!(health["stalled_for_seconds"].is_null());

    set_health(&admin, stalled(StallCause::ConsumerNotAcknowledging)).await;
    let health = {
        let d = admin.data.read().await;
        super::health_json(&d)
    };
    assert_eq!(health["verdict"], "stalled");
    assert_eq!(
        health["stall_cause"], "consumer_not_acknowledging",
        "the stable discriminant, not the prose — it is what routes a page",
    );
    assert!(
        health["stalled_for_seconds"]
            .as_f64()
            .is_some_and(|seconds| seconds >= 0.0),
        "how long this condition has held is what decides whether /livez restarts the pod",
    );
}
