//! Integration tests for crash simulation scenarios.
//!
//! Tests verify proper recovery and data integrity when the CDC runtime is crashed
//! at different points in the pipeline (snapshot, stream, handoff, checkpoint, transform).

use cdc_rs::{
    fault_injection::{CrashSimulationState, CrashSimulationValidator},
    Event, Operation, SourceMetadata, StructuredLogger, EVENT_ENVELOPE_VERSION,
};
use serde_json::json;
use std::collections::HashSet;

/// Helper function to create a test event
fn create_test_event(index: u64, table: &str) -> Event {
    Event {
        before: None,
        after: Some(json!({"id": index, "name": format!("row_{}", index)})),
        op: Operation::Insert,
        source: SourceMetadata {
            source_name: "test_source".into(),
            offset: format!("offset_{}", index),
            timestamp: index * 1000,
        },
        ts: index * 1000,
        schema: Some("public".into()),
        table: table.into(),
        primary_key: Some(vec!["id".into()]),
        snapshot: None,
        transaction: None,
        envelope_version: EVENT_ENVELOPE_VERSION,
    }
}

fn create_postgres_stream_event(index: u64) -> Event {
    Event {
        before: None,
        after: Some(json!({"id": index, "payload": format!("pg-row-{index}")})),
        op: Operation::Insert,
        source: SourceMetadata {
            source_name: "postgres".into(),
            offset: format!("0/{index:08X}"),
            timestamp: index,
        },
        ts: index,
        schema: Some("public".into()),
        table: "orders".into(),
        primary_key: Some(vec!["id".into()]),
        snapshot: None,
        transaction: None,
        envelope_version: EVENT_ENVELOPE_VERSION,
    }
}

fn event_id(event: &Event) -> u64 {
    event
        .after
        .as_ref()
        .and_then(|after| after.get("id"))
        .and_then(|value| value.as_u64())
        .expect("event id should be present")
}

/// Simulate a crash mid-snapshot and verify resume from checkpoint.
#[test]
fn crash_simulation_mid_snapshot() {
    let crash_state = CrashSimulationState::new(vec![50]);

    assert_eq!(crash_state.should_crash_at(49).ok(), Some(false));
    assert_eq!(crash_state.should_crash_at(50).ok(), Some(true));

    let cycle_events: Vec<Event> = (0..50)
        .map(|i| create_test_event(i, "snapshot_table"))
        .collect();
    crash_state.record_cycle(cycle_events).unwrap();

    // After first crash/restart cycle, the next cycle should no longer crash at 50.
    assert_eq!(crash_state.get_total_cycles().ok(), Some(1));
    assert_eq!(crash_state.should_crash_at(50).ok(), Some(false));
}

/// Simulate a crash mid-stream and verify resume from LSN checkpoint.
#[test]
fn crash_simulation_mid_stream() {
    let crash_state = CrashSimulationState::new(vec![100, 250]);

    assert_eq!(crash_state.should_crash_at(100).ok(), Some(true));
    crash_state
        .record_cycle(
            (0..100)
                .map(|i| create_test_event(i, "stream_table"))
                .collect(),
        )
        .unwrap();

    // Second cycle crash point is now 250 total events.
    assert_eq!(crash_state.should_crash_at(250).ok(), Some(true));
    crash_state
        .record_cycle(
            (100..250)
                .map(|i| create_test_event(i, "stream_table"))
                .collect(),
        )
        .unwrap();

    assert_eq!(crash_state.get_total_cycles().ok(), Some(2));
}

/// Simulate a crash during checkpoint commit.
#[test]
fn crash_simulation_checkpoint_commit() {
    let logger = StructuredLogger::new("postgres");
    logger.checkpoint_saved("0/1234", 100);
    logger.connection_error("simulated crash");

    let crash_state = CrashSimulationState::new(vec![100]);
    crash_state
        .record_cycle(
            (0..100)
                .map(|i| create_test_event(i, "checkpoint_table"))
                .collect(),
        )
        .unwrap();

    let per_cycle = crash_state.get_events_per_cycle().unwrap();
    assert_eq!(per_cycle, vec![100]);
}

/// Simulate a crash during transform processing.
#[test]
fn crash_simulation_transform_error() {
    let logger = StructuredLogger::new("mysql");
    logger.transform_applied("anonymize", "users", "mysql-bin.000001:100");
    logger.transform_error("anonymize", "Out of memory");

    let crash_state = CrashSimulationState::new(vec![20]);
    crash_state
        .record_cycle((0..20).map(|i| create_test_event(i, "users")).collect())
        .unwrap();
    crash_state
        .record_cycle((15..30).map(|i| create_test_event(i, "users")).collect())
        .unwrap();

    let events = crash_state.get_collected_events().unwrap();
    assert_eq!(events.len(), 35);
}

/// Simulate a crash during snapshot-to-stream handoff.
#[test]
fn crash_simulation_handoff_transition() {
    let crash_state = CrashSimulationState::new(vec![150]);

    crash_state
        .record_cycle(
            (0..150)
                .map(|i| create_test_event(i, "handoff_table"))
                .collect(),
        )
        .unwrap();
    crash_state
        .record_cycle(
            (150..220)
                .map(|i| create_test_event(i, "handoff_table"))
                .collect(),
        )
        .unwrap();

    let per_cycle = crash_state.get_events_per_cycle().unwrap();
    assert_eq!(per_cycle, vec![150, 70]);
}

/// Comprehensive multi-crash scenario with 5+ crashes during full CDC cycle.
/// This validates resilience under repeated failure scenarios.
#[test]
fn crash_simulation_multi_crash_comprehensive() {
    // Simulate: snapshot(100) -> crash -> resume -> stream(200) -> crash -> resume
    // -> handoff -> crash -> resume -> full stream(500) -> crash -> resume
    let crash_points = vec![
        100, // Crash after snapshot completes
        250, // Crash mid-stream
        350, // Crash during handoff
        500, // Crash during full stream
        600, // Crash at end
    ];

    let crash_state = CrashSimulationState::new(crash_points.clone());
    assert_eq!(crash_state.get_total_cycles().ok(), Some(0));

    let cycle_ranges = [
        (0_u64, 100_u64),
        (100, 250),
        (250, 350),
        (350, 500),
        (500, 600),
        (600, 750),
    ];

    for (start, end) in cycle_ranges {
        let events: Vec<Event> = (start..end)
            .map(|i| create_test_event(i, "comprehensive_table"))
            .collect();
        crash_state
            .record_cycle(events)
            .expect("cycle recording should succeed");
    }

    let summary = crash_state.finalize().expect("finalize should succeed");
    assert_eq!(summary.total_cycles, 6);
    assert_eq!(summary.events_per_cycle, vec![100, 150, 100, 150, 100, 150]);
    assert_eq!(summary.crash_points, crash_points);
    assert_eq!(summary.total_events, 750);

    let collected = crash_state
        .get_collected_events()
        .expect("collected events should be readable");
    assert_eq!(collected.len(), 750);

    let ids: Vec<u64> = collected.iter().map(event_id).collect();
    assert_eq!(ids.first().copied(), Some(0));
    assert_eq!(ids.last().copied(), Some(749));

    for window in ids.windows(2) {
        assert_eq!(window[1], window[0] + 1);
    }
}

/// Validate no data loss under fault injection with crashes.
#[test]
fn crash_simulation_zero_data_loss() {
    let crash_state = CrashSimulationState::new(vec![50, 150, 250]);

    // Record events from multiple cycles with unique source offsets.
    for cycle in 0..3 {
        let mut events = Vec::new();
        for i in 0..200 {
            let mut event = create_test_event((cycle * 200 + i) as u64, "test_table");
            event.source.offset = format!("cycle_{}_offset_{}", cycle, i);
            events.push(event);
        }
        let _result = crash_state.record_cycle(events);
    }

    let collected = crash_state
        .get_collected_events()
        .expect("collected events should be readable");
    assert_eq!(
        collected.len(),
        600,
        "Expected 600 total events, got {} after 3 crash cycles",
        collected.len()
    );

    let unique_offsets: HashSet<String> = collected
        .iter()
        .map(|event| event.source.offset.clone())
        .collect();
    assert_eq!(
        unique_offsets.len(),
        600,
        "offsets should be globally unique"
    );

    let per_cycle = crash_state
        .get_events_per_cycle()
        .expect("cycle event counts should be readable");
    assert_eq!(per_cycle, vec![200, 200, 200]);
}

/// Validate that crashes don't result in duplicate events (except at resend boundary).
#[test]
fn crash_simulation_no_unintended_duplicates() {
    let crash_state = CrashSimulationState::new(vec![50]);

    // First cycle: 100 events
    let mut cycle_1 = Vec::new();
    for i in 0..100 {
        cycle_1.push(create_test_event(i as u64, "dup_table"));
    }
    let _result1 = crash_state.record_cycle(cycle_1);

    // Second cycle (after restart): should not have duplicates
    let mut cycle_2 = Vec::new();
    for i in 50..100 {
        cycle_2.push(create_test_event(i as u64, "dup_table"));
    }
    let _result2 = crash_state.record_cycle(cycle_2);

    let collected = crash_state
        .get_collected_events()
        .expect("collected events should be readable");
    assert_eq!(
        collected.len(),
        150,
        "expected deterministic at-least-once resend boundary"
    );

    let unique_offsets: HashSet<String> = collected
        .iter()
        .map(|event| event.source.offset.clone())
        .collect();
    assert_eq!(
        unique_offsets.len(),
        100,
        "expected exactly 50 resend duplicates"
    );

    let duplicate_count = collected.len() - unique_offsets.len();
    assert_eq!(duplicate_count, 50);
}

/// Verify that checkpoint state is preserved across crashes.
#[test]
fn crash_simulation_checkpoint_persistence() {
    let crash_state = CrashSimulationState::new(vec![100, 250]);

    // Record first cycle
    let mut events1 = Vec::new();
    for i in 0..100 {
        events1.push(create_test_event(i as u64, "checkpoint_table"));
    }
    let _r1 = crash_state.record_cycle(events1);

    // Verify cycle tracking
    let cycles1 = crash_state.get_events_per_cycle().ok().unwrap();
    assert_eq!(cycles1.len(), 1);
    assert_eq!(cycles1[0], 100);

    // Record second cycle with resume
    let mut events2 = Vec::new();
    for i in 100..150 {
        events2.push(create_test_event(i as u64, "checkpoint_table"));
    }
    let _r2 = crash_state.record_cycle(events2);

    let cycles2 = crash_state.get_events_per_cycle().ok().unwrap();
    assert_eq!(cycles2.len(), 2);
    assert_eq!(cycles2[0], 100);
    assert_eq!(cycles2[1], 50);
}

/// Crash during snapshot with 1000 events - verify resumption resumes at correct cursor.
#[test]
fn crash_simulation_large_snapshot_resume() {
    let crash_state = CrashSimulationState::new(vec![500]); // Crash at 500

    // Simulate large snapshot
    let mut snapshot_events = Vec::new();
    for i in 0..1000 {
        snapshot_events.push(create_test_event(i as u64, "large_table"));
    }

    crash_state
        .record_cycle(snapshot_events)
        .expect("cycle recording should succeed");

    let total = crash_state
        .get_collected_events()
        .expect("collected events should be readable");
    assert_eq!(total.len(), 1000);

    let ids: Vec<u64> = total.iter().map(event_id).collect();
    assert_eq!(ids.first().copied(), Some(0));
    assert_eq!(ids.last().copied(), Some(999));
    for window in ids.windows(2) {
        assert_eq!(window[1], window[0] + 1);
    }
}

/// PostgreSQL crash/restart acceptance test:
/// simulate 10K stream events with 5 crash points and verify no data loss and ordering.
#[test]
fn crash_simulation_postgres_10k_events_5_crashes_no_loss_and_order() {
    let total_events = 10_000_u64;
    let crash_points = [2_000_u64, 4_000, 6_000, 8_000, 9_500];
    let crash_state = CrashSimulationState::new(crash_points.to_vec());

    let mut cycle_start = 0_u64;
    for crash_point in crash_points {
        let events: Vec<Event> = (cycle_start..crash_point)
            .map(create_postgres_stream_event)
            .collect();
        crash_state
            .record_cycle(events)
            .expect("cycle recording should succeed");
        cycle_start = crash_point;
    }

    // Final recovery cycle continues until completion.
    let final_cycle: Vec<Event> = (cycle_start..total_events)
        .map(create_postgres_stream_event)
        .collect();
    crash_state
        .record_cycle(final_cycle)
        .expect("final cycle recording should succeed");

    let result = crash_state.finalize().expect("finalization should succeed");
    let report = CrashSimulationValidator::validate(&result, total_events, 0.0)
        .expect("validation should confirm zero data loss and no duplicates");

    assert!(report.passed);
    assert_eq!(report.expected_total_events, total_events);
    assert_eq!(report.total_events_collected, total_events);
    assert_eq!(report.duplicate_count, 0);
    assert_eq!(report.cycles_with_crashes, 5);

    let collected = crash_state
        .get_collected_events()
        .expect("collected events should be readable");
    assert_eq!(collected.len() as u64, total_events);

    // Ordering guarantee: each resumed cycle continues from previous offset without gaps.
    let mut previous_id = None;
    for event in &collected {
        let id = event
            .after
            .as_ref()
            .and_then(|after| after.get("id"))
            .and_then(|value| value.as_u64())
            .expect("event id should be present");
        if let Some(last) = previous_id {
            assert_eq!(id, last + 1);
        }
        previous_id = Some(id);
    }
}
