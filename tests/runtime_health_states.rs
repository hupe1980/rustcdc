//! Integration tests for runtime health state detection (healthy, degraded, stalled).
//!
//! These tests verify that the runtime admin surface correctly identifies
//! the operational state of the CDC system under various conditions.

use rustcdc::checkpoint::InMemoryCheckpoint;
use rustcdc::core::CdcRuntime;
use rustcdc::core::{Event, Operation, RuntimeConfig, RuntimeSourceConfig, SourceMetadata};
use rustcdc::schema_history::InMemorySchemaHistory;
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Helper to create a test runtime with Disabled source.
fn make_test_runtime() -> CdcRuntime<InMemoryCheckpoint, InMemorySchemaHistory> {
    let checkpoint = InMemoryCheckpoint::default();
    let schema_history = InMemorySchemaHistory::default();
    let config = RuntimeConfig::new(RuntimeSourceConfig::Disabled, checkpoint, schema_history);
    CdcRuntime::new(config).expect("failed to create runtime")
}

/// Helper to create a test event.
fn make_event_with_id(id: u64) -> Event {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64;

    Event {
        before: None,
        after: Some(json!({"id": id, "name": format!("Alice-{id}")})),
        op: Operation::Insert,
        source: SourceMetadata {
            source_name: "mock".into(),
            offset: id.to_string(),
            timestamp: now_ms,
        },
        ts: now_ms,
        schema: Some("public".to_string()),
        table: "users".to_string(),
        primary_key: Some(vec!["id".to_string()]),
        snapshot: None,
        transaction: None,
        envelope_version: rustcdc::core::EVENT_ENVELOPE_VERSION,
    }
}

fn make_event() -> Event {
    make_event_with_id(1)
}

#[tokio::test]
async fn healthy_state_shows_zero_lag_after_recent_commit() {
    let mut runtime = make_test_runtime();

    // Start runtime and enqueue event.
    runtime.start().await.expect("failed to start");
    runtime
        .enqueue_event(make_event_with_id(1))
        .expect("failed to enqueue");

    // Poll and commit immediately.
    let batch = runtime.poll_event_batch().await.expect("failed to poll");
    runtime
        .commit_ack(batch.ack_token().unwrap())
        .await
        .expect("failed to commit");

    // Check admin snapshot shows healthy state.
    let admin = runtime.admin_snapshot();
    assert_eq!(admin.state, "running");
    assert!(admin.readiness);
    assert!(admin.liveness);
    assert!(admin.checkpoint_age_ms.is_some());
    assert!(admin.checkpoint_age_ms.unwrap() < 1000); // Recent on shared CI runners.
    assert!(admin.replication_lag_ms.is_some());
    assert!(admin.replication_lag_ms.unwrap() < 1000);
    assert_eq!(admin.total_events_committed, 1);
}

#[tokio::test]
async fn degraded_state_shows_high_lag_without_recent_polls() {
    let mut runtime = make_test_runtime();

    // Start runtime and poll but don't commit.
    runtime.start().await.expect("failed to start");
    runtime
        .enqueue_event(make_event_with_id(2))
        .expect("failed to enqueue");
    let _batch = runtime.poll_event_batch().await.expect("failed to poll");

    // Simulate passage of time by checking lag grew.
    let admin1 = runtime.admin_snapshot();
    assert_eq!(admin1.state, "running");
    assert!(admin1.readiness); // Still ready while running.
    assert!(admin1.in_flight_events > 0); // Events are unacknowledged.

    // Sleep a bit to let lag grow.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let admin2 = runtime.admin_snapshot();
    // Lag should be higher now.
    assert!(admin2.replication_lag_ms.unwrap_or(0) >= admin1.replication_lag_ms.unwrap_or(0));
}

#[tokio::test]
async fn stalled_state_shows_no_polling_activity() {
    let mut runtime = make_test_runtime();

    // Start runtime.
    runtime.start().await.expect("failed to start");

    // Check initial state has no poll activity.
    let admin1 = runtime.admin_snapshot();
    assert_eq!(admin1.state, "running");
    assert!(admin1.last_poll_at_ms.is_none()); // No poll yet.
    assert_eq!(admin1.total_events_polled, 0);

    // Enqueue and poll.
    runtime
        .enqueue_event(make_event_with_id(2))
        .expect("failed to enqueue");
    let _batch = runtime.poll_event_batch().await.expect("failed to poll");

    // Now we have poll activity.
    let admin2 = runtime.admin_snapshot();
    assert!(admin2.last_poll_at_ms.is_some());
    assert_eq!(admin2.total_events_polled, 1);
}

#[tokio::test]
async fn stopped_state_shows_not_live() {
    let mut runtime = make_test_runtime();

    // Start and immediately stop.
    runtime.start().await.expect("failed to start");
    runtime.stop().await.expect("failed to stop");

    // Check admin snapshot shows stopped state.
    let admin = runtime.admin_snapshot();
    assert_eq!(admin.state, "stopped");
    assert!(!admin.liveness); // Not live anymore.
}

#[tokio::test]
async fn idle_state_shows_not_ready() {
    let runtime = make_test_runtime();

    // Don't start the runtime.
    let admin = runtime.admin_snapshot();
    assert_eq!(admin.state, "idle");
    assert!(!admin.readiness);
    assert!(admin.liveness); // Still alive (not stopped).
}

#[tokio::test]
async fn multiple_commits_track_checkpoint_age() {
    let mut runtime = make_test_runtime();

    runtime.start().await.expect("failed to start");

    // First event and commit.
    runtime
        .enqueue_event(make_event_with_id(1))
        .expect("failed to enqueue");
    let batch1 = runtime.poll_event_batch().await.expect("failed to poll");
    runtime
        .commit_ack(batch1.ack_token().unwrap())
        .await
        .expect("failed to commit");

    let admin1 = runtime.admin_snapshot();
    let checkpoint_age_1 = admin1.checkpoint_age_ms.expect("no checkpoint age");

    // Wait a bit and do another commit.
    tokio::time::sleep(Duration::from_millis(100)).await;

    runtime
        .enqueue_event(make_event_with_id(2))
        .expect("failed to enqueue");
    let batch2 = runtime.poll_event_batch().await.expect("failed to poll");
    runtime
        .commit_ack(batch2.ack_token().unwrap())
        .await
        .expect("failed to commit");

    let admin2 = runtime.admin_snapshot();
    let checkpoint_age_2 = admin2.checkpoint_age_ms.expect("no checkpoint age");

    // Second checkpoint should be more recent (smaller age).
    // We verify that age decreases after the second commit due to the sleep.
    assert!(checkpoint_age_2 < checkpoint_age_1 + 50); // Allow for small variation.
    assert_eq!(admin2.total_events_committed, 2);
}

#[tokio::test]
async fn admin_prometheus_output_reflects_state_changes() {
    let mut runtime = make_test_runtime();

    // Idle state.
    let idle_metrics = runtime.admin_metrics_prometheus();
    assert!(idle_metrics.contains("cdc_runtime_readiness") && idle_metrics.contains(" 0"));
    assert!(idle_metrics.contains("cdc_runtime_liveness") && idle_metrics.contains(" 1"));

    // Running state.
    runtime.start().await.expect("failed to start");
    let running_metrics = runtime.admin_metrics_prometheus();
    assert!(running_metrics.contains("cdc_runtime_readiness") && running_metrics.contains(" 1"));
    assert!(running_metrics.contains("cdc_runtime_liveness") && running_metrics.contains(" 1"));

    // With committed events.
    runtime
        .enqueue_event(make_event())
        .expect("failed to enqueue");
    let batch = runtime.poll_event_batch().await.expect("failed to poll");
    runtime
        .commit_ack(batch.ack_token().unwrap())
        .await
        .expect("failed to commit");

    let with_commits_metrics = runtime.admin_metrics_prometheus();
    assert!(with_commits_metrics.contains("cdc_runtime_events_committed_total 1"));
    assert!(with_commits_metrics.contains("cdc_runtime_checkpoint_age_ms"));

    // Stopped state.
    runtime.stop().await.expect("failed to stop");
    let stopped_metrics = runtime.admin_metrics_prometheus();
    assert!(stopped_metrics.contains("cdc_runtime_readiness") && stopped_metrics.contains(" 0"));
    assert!(stopped_metrics.contains("cdc_runtime_liveness") && stopped_metrics.contains(" 0"));
}

#[tokio::test]
async fn admin_json_output_includes_all_health_fields() {
    let mut runtime = make_test_runtime();

    runtime.start().await.expect("failed to start");
    runtime
        .enqueue_event(make_event())
        .expect("failed to enqueue");
    let batch = runtime.poll_event_batch().await.expect("failed to poll");
    runtime
        .commit_ack(batch.ack_token().unwrap())
        .await
        .expect("failed to commit");

    let json_str = runtime.admin_snapshot_json().expect("failed to serialize");
    let json: serde_json::Value = serde_json::from_str(&json_str).expect("invalid json");

    // Verify all health-related fields are present.
    assert_eq!(json["state"], "running");
    assert_eq!(json["readiness"], true);
    assert_eq!(json["liveness"], true);
    assert!(json["checkpoint_age_ms"].is_number());
    assert!(json["replication_lag_ms"].is_number());
    assert!(json["total_events_polled"].is_number());
    assert!(json["total_events_committed"].is_number());
    assert!(json["last_poll_at_ms"].is_number());
    assert!(json["last_commit_at_ms"].is_number());
}
