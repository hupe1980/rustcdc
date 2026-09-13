//! Tests for the Kafka sink.
//!
//! Split out of `kafka.rs`, which outgrew the per-file line budget
//! `tests/architecture.rs` enforces — the same move `config/loader_tests.rs` made, and
//! for the same reason: a file nobody can navigate is a file nobody reviews.
//!
//! Declared with `#[path]` from `kafka.rs`, so `super::` still reaches the sink's private
//! items and nothing had to be made `pub(crate)` to be testable.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{
    BarrierState, BarrierStateMachine, KafkaSink, RtError, classify_krafka_error,
    kafka_connect_timeout,
};
use crate::config::schema::{
    KafkaCompression, KafkaSecurityConfig, KafkaSecurityProtocol, KafkaSinkConfig,
};
use crate::error::AppError;
use krafka::consumer::{AutoOffsetReset, Consumer};
use rustcdc::{Event, Operation, SourceMetadata, fingerprint_event_stable};
use serde_json::json;
use tokio::process::Command;
use tokio::sync::{Barrier, Mutex};
use tokio::time::{Duration, sleep};

#[test]
fn barrier_state_machine_rejects_invalid_transition_order() {
    let mut machine = BarrierStateMachine::new();

    let commit_without_begin = machine
        .ensure_can_commit()
        .expect_err("commit must fail when no checkpoint barrier is active");
    assert!(
        commit_without_begin.to_string().contains("expected Active"),
        "unexpected commit error: {commit_without_begin}"
    );

    machine.ensure_can_begin().expect("begin should be allowed");
    machine.mark_active();
    assert_eq!(machine.state(), BarrierState::Active);

    let begin_twice = machine
        .ensure_can_begin()
        .expect_err("second begin must fail while barrier is active");
    assert!(
        begin_twice.to_string().contains("expected NotActive"),
        "unexpected begin error: {begin_twice}"
    );

    machine
        .ensure_can_commit()
        .expect("commit should be allowed");
    machine.mark_not_active();
    assert_eq!(machine.state(), BarrierState::NotActive);
}

#[tokio::test]
async fn barrier_state_machine_contention_yields_single_begin_winner() {
    let machine = Arc::new(Mutex::new(BarrierStateMachine::new()));
    let start = Arc::new(Barrier::new(3));

    let mut joins = Vec::new();
    for _ in 0..2 {
        let machine = Arc::clone(&machine);
        let start = Arc::clone(&start);
        joins.push(tokio::spawn(async move {
            start.wait().await;
            let mut guard = machine.lock().await;
            match guard.ensure_can_begin() {
                Ok(()) => {
                    guard.mark_active();
                    Ok::<(), String>(())
                }
                Err(err) => Err(err.to_string()),
            }
        }));
    }

    start.wait().await;

    let mut success = 0_usize;
    let mut expected_error = 0_usize;
    for join in joins {
        match join.await.expect("join must succeed") {
            Ok(()) => success += 1,
            Err(err) => {
                if err.contains("expected NotActive") {
                    expected_error += 1;
                }
            }
        }
    }

    assert_eq!(success, 1, "exactly one contender should begin barrier");
    assert_eq!(
        expected_error, 1,
        "exactly one contender should be rejected by state machine"
    );
    assert_eq!(machine.lock().await.state(), BarrierState::Active);
}

fn sample_event() -> Event {
    Event::builder("users", Operation::Insert)
        .after(json!({"id": 42, "name": "bob"}))
        .source(SourceMetadata::new("postgres", "0/16B6A71", 1))
        .ts(1)
        .schema("public")
        .primary_key(["id"])
        .build()
}

fn sample_kafka_config(brokers: &str, topic: &str) -> KafkaSinkConfig {
    KafkaSinkConfig {
        brokers: brokers.to_string(),
        topic: topic.to_string(),
        topic_naming: crate::topic::TopicNamingConfig::default(),
        tombstones_on_delete: true,
        record_headers: crate::config::sink::KafkaRecordHeaders::Cdc,
        client_id: "cdc-kafka-test".to_string(),
        ack_timeout_ms: 1_000,
        retry_backoff_ms: 100,
        retry_max_attempts: 3,
        compression: KafkaCompression::None,
        compression_level: None,
        batch_size: 16 * 1024,
        linger_ms: 0,
        max_pipelined_sends: 128,
        transport: Default::default(),
        delivery_mode: crate::config::schema::KafkaDeliveryMode::AtLeastOnceIdempotent,
        transactional_id: None,
        transaction_timeout_ms: 60_000,
        security: KafkaSecurityConfig::default(),
        codec: None,
    }
}

fn live_kafka_config_from_env(brokers: &str, topic: &str) -> KafkaSinkConfig {
    let mut cfg = sample_kafka_config(brokers, topic);
    cfg.security.protocol = match std::env::var("CDC_TEST_KAFKA_PROTOCOL") {
        Ok(protocol) if protocol.eq_ignore_ascii_case("tls") => KafkaSecurityProtocol::Tls,
        _ => KafkaSecurityProtocol::Plaintext,
    };
    cfg.security.ssl_ca_location = std::env::var("CDC_TEST_KAFKA_CA")
        .ok()
        .map(std::path::PathBuf::from);
    cfg
}

fn sample_live_event(timestamp: u64, offset_prefix: &str) -> Event {
    static NEXT_SUFFIX: AtomicU64 = AtomicU64::new(1);

    let mut event = sample_event();
    let suffix = NEXT_SUFFIX.fetch_add(1, Ordering::Relaxed);
    event.source.offset = format!("{offset_prefix}{suffix}");
    event.source.timestamp = timestamp;
    event.ts = timestamp;
    event
}

fn live_kafka_target_from_env(test_name: &str) -> Option<(String, String)> {
    let Ok(brokers) = std::env::var("CDC_TEST_KAFKA_BROKERS") else {
        eprintln!("skipping {test_name} (CDC_TEST_KAFKA_BROKERS is not set)");
        return None;
    };

    let Ok(topic) = std::env::var("CDC_TEST_KAFKA_TOPIC") else {
        eprintln!("skipping {test_name} (CDC_TEST_KAFKA_TOPIC is not set)");
        return None;
    };

    Some((brokers, topic))
}

fn decode_event_offset(record_value: &[u8]) -> String {
    let event: Event =
        serde_json::from_slice(record_value).expect("kafka record value must decode as Event");
    event.source.offset
}

async fn consume_until_offsets_seen(
    consumer: &Consumer,
    expected_offsets: &BTreeSet<String>,
    attempts: usize,
) -> BTreeSet<String> {
    let mut seen_offsets = BTreeSet::new();

    for _ in 0..attempts {
        let records = consumer
            .poll(Duration::from_millis(250))
            .await
            .expect("consumer poll should succeed");

        for record in records {
            if let Some(value) = &record.value {
                seen_offsets.insert(decode_event_offset(value.as_ref()));
            }
        }

        if expected_offsets.is_subset(&seen_offsets) {
            break;
        }

        sleep(Duration::from_millis(100)).await;
    }

    seen_offsets
}

async fn consume_offset_sequence_until_seen(
    consumer: &Consumer,
    expected_offsets: &BTreeSet<String>,
    attempts: usize,
) -> Vec<String> {
    let mut seen_offsets = BTreeSet::new();
    let mut sequence = Vec::new();

    for _ in 0..attempts {
        let records = consumer
            .poll(Duration::from_millis(250))
            .await
            .expect("consumer poll should succeed");

        for record in records {
            if let Some(value) = &record.value {
                let offset = decode_event_offset(value.as_ref());
                sequence.push(offset.clone());
                seen_offsets.insert(offset);
            }
        }

        if expected_offsets.is_subset(&seen_offsets) {
            break;
        }

        sleep(Duration::from_millis(100)).await;
    }

    sequence
}

#[test]
fn fingerprint_bytes_are_stable() {
    let event = sample_event();
    let expected = fingerprint_event_stable(&event)
        .expect("fingerprint")
        .into_bytes();
    let actual = fingerprint_event_stable(&event)
        .expect("fingerprint")
        .into_bytes();
    assert_eq!(actual, expected);
}

#[test]
fn fingerprint_changes_when_source_offset_changes() {
    let first = sample_event();
    let mut second = sample_event();
    second.source.offset = "0/16B6A72".to_string();

    let first_fp = fingerprint_event_stable(&first).expect("fingerprint");
    let second_fp = fingerprint_event_stable(&second).expect("fingerprint");

    assert_ne!(first_fp, second_fp);
}

#[tokio::test]
async fn broker_degradation_fails_closed() {
    let mut cfg = sample_kafka_config("127.0.0.1:1", "cdc-events-degraded");
    cfg.ack_timeout_ms = 200;
    cfg.retry_backoff_ms = 25;
    cfg.retry_max_attempts = 1;

    match KafkaSink::new(&cfg).await {
        Ok(mut sink) => {
            let event = sample_event();
            let json = serde_json::to_vec(&event).expect("serialize");
            let err = sink
                .send_encoded(
                    &sample_event(),
                    &cfg.topic,
                    bytes::Bytes::new(),
                    bytes::Bytes::from(json),
                )
                .await
                .expect_err("send should fail when broker is unavailable");
            assert!(err.to_string().contains("Kafka sink delivery failed"));
        }
        Err(err) => {
            assert!(
                err.to_string().contains("failed to build")
                    && err.to_string().contains("krafka producer"),
                "expected krafka producer build error, got: {err}"
            );
        }
    }
}

#[tokio::test]
async fn new_rejects_blank_topic_before_build() {
    let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
    cfg.topic = "   ".to_string();

    match KafkaSink::new(&cfg).await {
        Ok(_) => panic!("expected blank topic to be rejected"),
        Err(err) => assert!(err.to_string().contains("sink.kafka.topic")),
    }
}

#[tokio::test]
async fn new_rejects_blank_client_id_before_build() {
    let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
    cfg.client_id = "   ".to_string();

    match KafkaSink::new(&cfg).await {
        Ok(_) => panic!("expected blank client_id to be rejected"),
        Err(err) => assert!(err.to_string().contains("sink.kafka.client_id")),
    }
}

#[tokio::test]
async fn new_rejects_blank_brokers_before_build() {
    let cfg = sample_kafka_config(" , , ", "cdc-events");

    match KafkaSink::new(&cfg).await {
        Ok(_) => panic!("expected blank broker list to be rejected"),
        Err(err) => assert!(err.to_string().contains("sink.kafka.brokers")),
    }
}

#[tokio::test]
async fn new_rejects_zero_ack_timeout_before_build() {
    let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
    cfg.ack_timeout_ms = 0;

    match KafkaSink::new(&cfg).await {
        Ok(_) => panic!("expected zero ack_timeout_ms to be rejected"),
        Err(err) => assert!(err.to_string().contains("sink.kafka.ack_timeout_ms")),
    }
}

#[tokio::test]
async fn new_rejects_zero_retry_backoff_before_build() {
    let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
    cfg.retry_backoff_ms = 0;

    match KafkaSink::new(&cfg).await {
        Ok(_) => panic!("expected zero retry_backoff_ms to be rejected"),
        Err(err) => assert!(err.to_string().contains("sink.kafka.retry_backoff_ms")),
    }
}

#[tokio::test]
async fn new_rejects_zero_retry_attempts_before_build() {
    let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
    cfg.retry_max_attempts = 0;

    match KafkaSink::new(&cfg).await {
        Ok(_) => panic!("expected zero retry_max_attempts to be rejected"),
        Err(err) => assert!(err.to_string().contains("sink.kafka.retry_max_attempts")),
    }
}

#[tokio::test]
async fn new_rejects_tls_with_verify_peer_disabled_before_build() {
    let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
    cfg.security.protocol = KafkaSecurityProtocol::Tls;
    cfg.security.verify_peer = false;

    match KafkaSink::new(&cfg).await {
        Ok(_) => panic!("expected insecure tls verify_peer=false to be rejected"),
        Err(err) => assert!(err.to_string().contains("sink.kafka.security.verify_peer")),
    }
}

#[tokio::test]
async fn new_rejects_tls_with_missing_ca_file_before_build() {
    let mut cfg = sample_kafka_config("localhost:9092", "cdc-events");
    cfg.security.protocol = KafkaSecurityProtocol::Tls;
    cfg.security.verify_peer = true;
    cfg.security.ssl_ca_location = Some(std::path::PathBuf::from("/tmp/no-such-kafka-ca-cert.pem"));

    match KafkaSink::new(&cfg).await {
        Ok(_) => panic!("expected missing tls CA file to be rejected"),
        Err(err) => assert!(
            err.to_string()
                .contains("sink.kafka.security.ssl_ca_location")
        ),
    }
}

#[tokio::test]
async fn live_kafka_send_and_flush_when_env_is_set() {
    let Some((brokers, topic)) = live_kafka_target_from_env("live kafka sink test") else {
        return;
    };

    let cfg = live_kafka_config_from_env(&brokers, &topic);
    let first = sample_live_event(11, "0/16B6A7");
    let second = sample_live_event(12, "0/16B6A8");

    let mut sink = KafkaSink::new(&cfg)
        .await
        .expect("live kafka producer must build");
    let send = |event: &Event| {
        let json = serde_json::to_vec(event).expect("serialize");
        (bytes::Bytes::new(), bytes::Bytes::from(json))
    };
    let (k, v) = send(&first);
    sink.send_encoded(&sample_event(), &cfg.topic, k, v)
        .await
        .expect("first send should succeed");
    let (k, v) = send(&second);
    sink.send_encoded(&sample_event(), &cfg.topic, k, v)
        .await
        .expect("second send should succeed");
    sink.flush().await.expect("flush should succeed");
    sink.close().await.expect("close should succeed");
    assert!(sink.is_closed());
}

#[tokio::test]
async fn live_kafka_recovers_after_external_churn_when_env_is_set() {
    let Some((brokers, topic)) = live_kafka_target_from_env("kafka churn test") else {
        return;
    };
    let Ok(churn_cmd) = std::env::var("CDC_TEST_KAFKA_CHURN_COMMAND") else {
        eprintln!("skipping kafka churn test (CDC_TEST_KAFKA_CHURN_COMMAND is not set)");
        return;
    };

    let cfg = live_kafka_config_from_env(&brokers, &topic);

    let mut sink = KafkaSink::new(&cfg)
        .await
        .expect("live kafka producer must build");

    // Establish baseline health before injecting churn.
    let baseline = sample_live_event(10, "0/16B6C");
    let bl_json = serde_json::to_vec(&baseline).expect("serialize");
    sink.send_encoded(
        &sample_event(),
        &cfg.topic,
        bytes::Bytes::new(),
        bytes::Bytes::from(bl_json),
    )
    .await
    .expect("baseline send should succeed");
    sink.flush().await.expect("baseline flush should succeed");

    let churn = Command::new("sh")
        .arg("-c")
        .arg(churn_cmd.as_str())
        .output()
        .await
        .expect("failed to execute churn command");
    assert!(
        churn.status.success(),
        "churn command failed: {}",
        String::from_utf8_lossy(&churn.stderr)
    );

    // After churn, give the broker/client path a bounded recovery window.
    let mut recovered = false;
    for attempt in 0..40_u64 {
        let event = sample_live_event(20 + attempt, &format!("0/16B6D{}", attempt));
        let ev_json = serde_json::to_vec(&event).expect("serialize");
        let delivered = sink
            .send_encoded(
                &sample_event(),
                &cfg.topic,
                bytes::Bytes::new(),
                bytes::Bytes::from(ev_json),
            )
            .await
            .is_ok();
        let flushed = sink.flush().await.is_ok();
        if delivered && flushed {
            recovered = true;
            break;
        }
        sleep(Duration::from_millis(250)).await;
    }

    assert!(
        recovered,
        "sink did not recover delivery after external churn command"
    );
    sink.close().await.expect("close should succeed");
}

#[tokio::test]
async fn live_kafka_consumer_group_commit_survives_rebalance_and_restart() {
    let Some((brokers, topic)) =
        live_kafka_target_from_env("kafka consumer-group commit/rebalance test")
    else {
        return;
    };

    static NEXT_GROUP_SUFFIX: AtomicU64 = AtomicU64::new(1);
    let suffix = NEXT_GROUP_SUFFIX.fetch_add(1, Ordering::Relaxed);

    let cfg = live_kafka_config_from_env(&brokers, &topic);
    let auth = cfg
        .security
        .to_auth_config()
        .expect("kafka auth config should be valid");

    let first_batch = vec![
        sample_live_event(31, &format!("0/16B6E1-{suffix}-")),
        sample_live_event(32, &format!("0/16B6E2-{suffix}-")),
    ];
    let first_offsets = first_batch
        .iter()
        .map(|event| event.source.offset.clone())
        .collect::<BTreeSet<_>>();

    let mut sink = KafkaSink::new(&cfg)
        .await
        .expect("live kafka producer must build");
    for event in &first_batch {
        let json = serde_json::to_vec(event).expect("serialize");
        sink.send_encoded(
            &sample_event(),
            &cfg.topic,
            bytes::Bytes::new(),
            bytes::Bytes::from(json),
        )
        .await
        .expect("first-batch send should succeed");
    }
    sink.flush()
        .await
        .expect("first-batch flush should succeed");

    let group_id = format!("cdc-kafka-commit-e2e-{suffix}");
    let consumer_a = Consumer::builder()
        .bootstrap_servers(brokers.clone())
        .group_id(group_id.clone())
        .client_id(format!("cdc-kafka-commit-e2e-a-{suffix}"))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .request_timeout(Duration::from_millis(cfg.ack_timeout_ms))
        .connect_timeout(kafka_connect_timeout(Duration::from_millis(
            cfg.ack_timeout_ms,
        )))
        .auth(auth.clone())
        .build()
        .await
        .expect("consumer A should build");
    consumer_a
        .subscribe(&[topic.as_str()])
        .await
        .expect("consumer A should subscribe");

    let seen_by_a = consume_until_offsets_seen(&consumer_a, &first_offsets, 40).await;
    assert!(
        first_offsets.is_subset(&seen_by_a),
        "consumer A did not receive all first-batch offsets; expected={first_offsets:?}, seen={seen_by_a:?}"
    );
    consumer_a
        .commit()
        .await
        .expect("consumer A commit should succeed");

    let consumer_b = Consumer::builder()
        .bootstrap_servers(brokers.clone())
        .group_id(group_id)
        .client_id(format!("cdc-kafka-commit-e2e-b-{suffix}"))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .request_timeout(Duration::from_millis(cfg.ack_timeout_ms))
        .connect_timeout(kafka_connect_timeout(Duration::from_millis(
            cfg.ack_timeout_ms,
        )))
        .auth(auth)
        .build()
        .await
        .expect("consumer B should build");
    consumer_b
        .subscribe(&[topic.as_str()])
        .await
        .expect("consumer B should subscribe");

    // Drive a short overlap window so group membership churn triggers rebalance.
    for _ in 0..8 {
        let _ = consumer_a
            .poll(Duration::from_millis(150))
            .await
            .expect("consumer A overlap poll should succeed");
        let _ = consumer_b
            .poll(Duration::from_millis(150))
            .await
            .expect("consumer B overlap poll should succeed");
        sleep(Duration::from_millis(75)).await;
    }

    consumer_a
        .close()
        .await
        .expect("consumer A close should succeed");

    let second_batch = vec![
        sample_live_event(41, &format!("0/16B6F1-{suffix}-")),
        sample_live_event(42, &format!("0/16B6F2-{suffix}-")),
    ];
    let expected_second_order = second_batch
        .iter()
        .map(|event| event.source.offset.clone())
        .collect::<Vec<_>>();
    let second_offsets = second_batch
        .iter()
        .map(|event| event.source.offset.clone())
        .collect::<BTreeSet<_>>();
    for event in &second_batch {
        let json = serde_json::to_vec(event).expect("serialize");
        sink.send_encoded(
            &sample_event(),
            &cfg.topic,
            bytes::Bytes::new(),
            bytes::Bytes::from(json),
        )
        .await
        .expect("second-batch send should succeed");
    }
    sink.flush()
        .await
        .expect("second-batch flush should succeed");

    let seen_by_b_sequence =
        consume_offset_sequence_until_seen(&consumer_b, &second_offsets, 40).await;
    let seen_by_b = seen_by_b_sequence.iter().cloned().collect::<BTreeSet<_>>();
    assert!(
        second_offsets.is_subset(&seen_by_b),
        "consumer B did not receive all second-batch offsets; expected={second_offsets:?}, seen={seen_by_b:?}"
    );

    let replayed = seen_by_b
        .iter()
        .filter(|offset| first_offsets.contains(*offset))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        replayed.is_empty(),
        "consumer B replayed committed first-batch offsets after rebalance/restart: {replayed:?}"
    );

    let seen_second_in_order = seen_by_b_sequence
        .iter()
        .filter(|offset| second_offsets.contains(*offset))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        seen_second_in_order, expected_second_order,
        "consumer B violated second-batch ordering after rebalance/restart"
    );

    let mut second_counts = BTreeMap::<String, usize>::new();
    for offset in &seen_second_in_order {
        *second_counts.entry(offset.clone()).or_default() += 1;
    }
    for expected in &expected_second_order {
        assert_eq!(
            second_counts.get(expected),
            Some(&1),
            "consumer B observed duplicate/missing second-batch offset {expected} after rebalance/restart"
        );
    }

    consumer_b
        .commit()
        .await
        .expect("consumer B commit should succeed");
    consumer_b
        .close()
        .await
        .expect("consumer B close should succeed");
    sink.close().await.expect("sink close should succeed");
}

// ── In-process fake-broker tests (krafka `test-broker`) ──────────────────
//
// These run the real Kafka wire protocol against krafka's in-process fake
// broker — no Docker, no env gating, always on in CI.

#[tokio::test]
async fn fake_broker_sink_delivers_all_records_durably() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.delivery", 3);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.delivery");
    let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");

    for i in 0..5u32 {
        sink.send_encoded(
            &sample_event(),
            &cfg.topic,
            bytes::Bytes::from(format!("key-{i}")),
            bytes::Bytes::from(format!("value-{i}")),
        )
        .await
        .expect("send must succeed and confirm durably");
    }
    sink.flush().await.expect("flush");

    // Every record must be in the broker log — the sum of next_offset over
    // all partitions is the total number of durably appended records.
    let total: i64 = broker.with_state(|state| {
        (0..3)
            .filter_map(|partition| state.partition("cdc.fake.delivery", partition))
            .map(|p| p.next_offset)
            .sum()
    });
    assert_eq!(
        total, 5,
        "all sends must be appended to the fake broker log"
    );

    sink.close().await.expect("close");
}

// ── Pipelined sends ──────────────────────────────────────────────────────

/// Read every record of a single-partition topic back off the fake broker, in log
/// order, as a `read_uncommitted` consumer sees it.
async fn drain_partition_values(brokers: &str, topic: &str, expected: usize) -> Vec<String> {
    let consumer = Consumer::builder()
        .bootstrap_servers(brokers.to_string())
        .group_id(format!("{topic}-verify"))
        .client_id(format!("{topic}-verify"))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .build()
        .await
        .expect("verification consumer");
    consumer.subscribe(&[topic]).await.expect("subscribe");

    let mut values = Vec::new();
    for _ in 0..40 {
        for record in consumer
            .poll(Duration::from_millis(250))
            .await
            .expect("poll")
        {
            if let Some(value) = &record.value {
                values.push(String::from_utf8_lossy(value.as_ref()).into_owned());
            }
        }
        if values.len() >= expected {
            break;
        }
    }
    consumer.close().await.expect("close verification consumer");
    values
}

/// Read a topic as a `read_committed` consumer does — aborted records excluded.
async fn read_committed_values(brokers: &str, topic: &str) -> Vec<String> {
    let consumer = Consumer::builder()
        .bootstrap_servers(brokers.to_string())
        .group_id(format!("{topic}-committed"))
        .client_id(format!("{topic}-committed"))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(krafka::consumer::IsolationLevel::ReadCommitted)
        .enable_auto_commit(false)
        .build()
        .await
        .expect("read_committed consumer");
    consumer.subscribe(&[topic]).await.expect("subscribe");

    let mut values = Vec::new();
    for _ in 0..8 {
        for record in consumer
            .poll(Duration::from_millis(250))
            .await
            .expect("poll")
        {
            if let Some(value) = &record.value {
                values.push(String::from_utf8_lossy(value.as_ref()).into_owned());
            }
        }
    }
    consumer.close().await.expect("close");
    values
}

/// **The property pipelining must not break.** Per-partition ordering is what every
/// downstream CDC consumer depends on: replaying `UPDATE balance=100` before
/// `UPDATE balance=50` silently corrupts the replica.
///
/// One partition, a window far wider than the record count, so every send is
/// outstanding at once and any reordering in the accumulator would show up here.
#[tokio::test]
async fn pipelined_sends_reach_the_partition_in_submission_order() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    let topic = "cdc.fake.pipeline.order";
    broker.create_topic(topic, 1);

    let mut cfg = sample_kafka_config(&broker.bootstrap_servers(), topic);
    cfg.max_pipelined_sends = 256;
    // A non-zero linger is the case that used to be unusable: it forces records to
    // coalesce in the accumulator, which is exactly where a reordering would happen.
    cfg.linger_ms = 5;
    let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");

    let expected: Vec<String> = (0..200).map(|i| format!("value-{i:04}")).collect();
    for value in &expected {
        sink.send_encoded(
            &sample_event(),
            &cfg.topic,
            bytes::Bytes::from_static(b"same-key"),
            bytes::Bytes::from(value.clone()),
        )
        .await
        .expect("send accepted");
    }
    sink.flush().await.expect("flush");

    let observed = drain_partition_values(&broker.bootstrap_servers(), topic, expected.len()).await;
    assert_eq!(
        observed, expected,
        "pipelined sends must reach the partition in submission order"
    );

    sink.close().await.expect("close");
}

/// `flush` is the durability boundary the checkpoint depends on: `run_loop_batch`
/// commits a checkpoint only after `process_batch_events` returns, and that calls
/// `flush`. If `flush` returned before collecting outstanding acknowledgements, the
/// checkpoint would advance past records that were never confirmed.
#[tokio::test]
async fn flush_collects_every_outstanding_acknowledgement() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    let topic = "cdc.fake.pipeline.flush";
    broker.create_topic(topic, 1);

    let mut cfg = sample_kafka_config(&broker.bootstrap_servers(), topic);
    cfg.max_pipelined_sends = 512;
    cfg.linger_ms = 20;
    let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");

    for i in 0..100u32 {
        sink.send_encoded(
            &sample_event(),
            &cfg.topic,
            bytes::Bytes::from(format!("k{i}")),
            bytes::Bytes::from(format!("v{i}")),
        )
        .await
        .expect("send accepted");
    }

    sink.flush().await.expect("flush");
    assert!(
        sink.inflight.is_empty(),
        "flush must leave no unconfirmed sends behind"
    );

    let durable = broker.next_offset(topic, 0).expect("partition exists");
    assert_eq!(
        durable, 100,
        "every accepted record must be durable once flush returns"
    );

    sink.close().await.expect("close");
}

/// `max_pipelined_sends` is a ceiling, not a hint. An inert tuning knob is worse than
/// no knob, so this asserts the window is bounded
/// while sends are outstanding, not merely that the field is read.
#[tokio::test]
async fn the_pipeline_window_is_bounded_by_its_configured_depth() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    let topic = "cdc.fake.pipeline.window";
    broker.create_topic(topic, 1);

    let mut cfg = sample_kafka_config(&broker.bootstrap_servers(), topic);
    cfg.max_pipelined_sends = 8;
    cfg.linger_ms = 20;
    let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");

    for i in 0..50u32 {
        sink.send_encoded(
            &sample_event(),
            &cfg.topic,
            bytes::Bytes::from(format!("k{i}")),
            bytes::Bytes::from(format!("v{i}")),
        )
        .await
        .expect("send accepted");
        assert!(
            sink.inflight.len() <= 8,
            "window grew to {} with max_pipelined_sends = 8",
            sink.inflight.len()
        );
    }

    sink.flush().await.expect("flush");
    sink.close().await.expect("close");
}

/// The throughput claim, measured rather than asserted in prose.
///
/// A depth-1 window is precisely the original sink: one broker round-trip per
/// record. Both runs go through the same code against the same in-process broker, so
/// the ratio isolates pipelining from everything else. The threshold is deliberately
/// loose — this runs on shared CI hardware, and the point is to catch the window
/// silently reverting to serial, not to publish a number.
#[tokio::test]
async fn pipelining_outperforms_one_round_trip_per_record() {
    async fn run(broker_servers: &str, topic: &str, depth: usize) -> Duration {
        let mut cfg = sample_kafka_config(broker_servers, topic);
        cfg.max_pipelined_sends = depth;
        cfg.linger_ms = 2;
        let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");

        let started = std::time::Instant::now();
        for i in 0..300u32 {
            sink.send_encoded(
                &sample_event(),
                &cfg.topic,
                bytes::Bytes::from(format!("k{i}")),
                bytes::Bytes::from(format!("v{i}")),
            )
            .await
            .expect("send accepted");
        }
        sink.flush().await.expect("flush");
        let elapsed = started.elapsed();
        sink.close().await.expect("close");
        elapsed
    }

    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.perf.serial", 1);
    broker.create_topic("cdc.fake.perf.pipelined", 1);

    broker.clear_requests();
    let serial = run(&broker.bootstrap_servers(), "cdc.fake.perf.serial", 1).await;
    let serial_produces = broker.request_count(krafka::protocol::ApiKey::Produce);

    broker.clear_requests();
    let pipelined = run(&broker.bootstrap_servers(), "cdc.fake.perf.pipelined", 256).await;
    let pipelined_produces = broker.request_count(krafka::protocol::ApiKey::Produce);

    eprintln!(
        "pipelining: serial {serial:?} / {serial_produces} produce requests \
         vs pipelined {pipelined:?} / {pipelined_produces} produce requests"
    );

    // The round-trip count is the deterministic signal and the one that actually
    // governs throughput on a real network; wall-clock on shared CI hardware is not.
    assert_eq!(
        serial_produces, 300,
        "a depth-1 window is one broker round-trip per record, by definition"
    );
    assert!(
        pipelined_produces * 10 < serial_produces,
        "pipelining must coalesce records into batches (serial {serial_produces} \
         produce requests, pipelined {pipelined_produces})"
    );
    assert!(
        pipelined < serial,
        "pipelining must not be slower (serial {serial:?}, pipelined {pipelined:?})"
    );
}

// ── Transactional (effectively-once) coverage ────────────────────────────
//
// The fake broker serves the full transaction protocol — commit and abort markers,
// `read_committed` isolation and the last-stable-offset — so the checkpoint-barrier
// path has broker-level evidence, not only the in-memory state machine above.

fn transactional_kafka_config(brokers: &str, topic: &str, txn_id: &str) -> KafkaSinkConfig {
    let mut cfg = sample_kafka_config(brokers, topic);
    cfg.delivery_mode = crate::config::schema::KafkaDeliveryMode::Transactional;
    cfg.transactional_id = Some(txn_id.to_string());
    cfg
}

/// A committed barrier must advance the last stable offset — that is what makes
/// the records visible to a `read_committed` consumer, and it is the property
/// `effectively_once` actually sells.
#[tokio::test]
async fn fake_broker_committed_barrier_advances_last_stable_offset() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.txn.commit", 1);

    let cfg = transactional_kafka_config(
        &broker.bootstrap_servers(),
        "cdc.fake.txn.commit",
        "cdc-txn-commit",
    );
    let mut sink = KafkaSink::new(&cfg).await.expect("transactional sink");
    assert_eq!(
        sink.delivery_guarantee(),
        super::SinkDeliveryGuarantee::EffectivelyOnce
    );

    sink.begin_checkpoint_barrier()
        .await
        .expect("begin barrier");
    for i in 0..3u32 {
        sink.send_encoded(
            &sample_event(),
            &cfg.topic,
            bytes::Bytes::from(format!("key-{i}")),
            bytes::Bytes::from(format!("value-{i}")),
        )
        .await
        .expect("send inside transaction");
    }

    // Uncommitted: the records are appended but not yet stable, so a
    // `read_committed` consumer must not be able to see them.
    let lso_before = broker
        .last_stable_offset("cdc.fake.txn.commit", 0)
        .expect("partition exists");
    assert_eq!(
        lso_before, 0,
        "records must not be stable before the barrier commits"
    );

    sink.commit_checkpoint_barrier()
        .await
        .expect("commit barrier");

    let lso_after = broker
        .last_stable_offset("cdc.fake.txn.commit", 0)
        .expect("partition exists");
    assert!(
        lso_after > lso_before,
        "committing the barrier must advance the last stable offset \
         ({lso_before} -> {lso_after})"
    );
    assert!(
        broker
            .aborted_transactions("cdc.fake.txn.commit", 0)
            .is_empty(),
        "a committed barrier must leave no abort marker"
    );

    sink.close().await.expect("close");
}

/// `effectively_once` and topic templates have to compose, because the deployment
/// that wants topic-per-table is usually the one that wants exactly-once.
///
/// One transaction, records on two topics: both become stable together on commit.
/// Nothing about that is free — a transaction spanning several topics has to add each
/// partition to it before the first record lands — so it is asserted rather than
/// assumed.
#[tokio::test]
async fn a_transaction_commits_atomically_across_the_topics_a_template_renders() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.public.orders", 1);
    broker.create_topic("cdc.public.customers", 1);

    let cfg = transactional_kafka_config(
        &broker.bootstrap_servers(),
        "cdc.${schema}.${table}",
        "cdc-txn-template",
    );
    let mut sink = KafkaSink::new(&cfg).await.expect("transactional sink");

    sink.begin_checkpoint_barrier()
        .await
        .expect("begin barrier");
    for table in ["orders", "customers"] {
        let event = Event::builder(table, Operation::Insert)
            .after(json!({"id": 1}))
            .source(SourceMetadata::new("postgres", "0/1", 1))
            .ts(1)
            .schema("public")
            .primary_key(["id"])
            .build();
        let topic = sink.topic_for(&event).expect("topic");
        sink.send_encoded(
            &sample_event(),
            &topic,
            bytes::Bytes::from_static(b"k1"),
            bytes::Bytes::from_static(b"v"),
        )
        .await
        .expect("send inside transaction");
    }

    for topic in ["cdc.public.orders", "cdc.public.customers"] {
        assert_eq!(
            broker
                .last_stable_offset(topic, 0)
                .expect("partition exists"),
            0,
            "{topic} must not be stable before the barrier commits"
        );
    }

    sink.commit_checkpoint_barrier()
        .await
        .expect("commit barrier");

    for topic in ["cdc.public.orders", "cdc.public.customers"] {
        assert!(
            broker
                .last_stable_offset(topic, 0)
                .expect("partition exists")
                > 0,
            "{topic} must be stable after the barrier commits; a transaction that \
             only covered one topic would leave the other behind"
        );
        assert!(
            broker.aborted_transactions(topic, 0).is_empty(),
            "{topic} must carry no abort marker"
        );
    }

    sink.close().await.expect("close");
}

/// An aborted barrier must leave an abort marker, so a `read_committed`
/// consumer skips the records rather than reading a partially-applied batch.
/// This is the failure path the delivery contract exists for.
#[tokio::test]
async fn fake_broker_aborted_barrier_leaves_an_abort_marker() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.txn.abort", 1);

    let cfg = transactional_kafka_config(
        &broker.bootstrap_servers(),
        "cdc.fake.txn.abort",
        "cdc-txn-abort",
    );
    let mut sink = KafkaSink::new(&cfg).await.expect("transactional sink");

    sink.begin_checkpoint_barrier()
        .await
        .expect("begin barrier");
    sink.send_encoded(
        &sample_event(),
        &cfg.topic,
        bytes::Bytes::from("key-doomed"),
        bytes::Bytes::from("value-doomed"),
    )
    .await
    .expect("send inside transaction");
    // Collect the acknowledgement so the record is genuinely in the open transaction
    // on the broker. Without this the abort has nothing to mark: a pipelined send may
    // still be outstanding, and `abort_checkpoint_barrier` cancels it rather than
    // waiting — see `SendWindow::abandon`. Both routes discard the batch, and the
    // *unflushed* one is covered below; this half needs a record that reached the log.
    sink.flush().await.expect("flush into the open transaction");

    sink.abort_checkpoint_barrier()
        .await
        .expect("abort barrier");

    assert!(
        !broker
            .aborted_transactions("cdc.fake.txn.abort", 0)
            .is_empty(),
        "an aborted barrier must record an abort marker so read_committed skips it"
    );

    // The state machine must be back to NotActive, so the next barrier can begin.
    sink.begin_checkpoint_barrier()
        .await
        .expect("a new barrier must be startable after an abort");
    sink.commit_checkpoint_barrier()
        .await
        .expect("commit the recovery barrier");

    sink.close().await.expect("close");
}

/// The other half of the abort contract: a record accepted into a pipelined window
/// but aborted before it drains must never become visible either.
///
/// This is the path `run_loop_batch` takes when delivery fails mid-batch — it aborts
/// without flushing. Cancelling an outstanding send is sound precisely because the
/// transaction it belonged to is being discarded; the assertion is that the record
/// does not survive by some other route.
#[tokio::test]
async fn aborting_before_the_window_drains_publishes_nothing() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    let topic = "cdc.fake.txn.abort.undrained";
    broker.create_topic(topic, 1);

    let mut cfg =
        transactional_kafka_config(&broker.bootstrap_servers(), topic, "cdc-txn-undrained");
    cfg.max_pipelined_sends = 128;
    cfg.linger_ms = 50;
    let mut sink = KafkaSink::new(&cfg).await.expect("transactional sink");

    sink.begin_checkpoint_barrier()
        .await
        .expect("begin barrier");
    for i in 0..20u32 {
        sink.send_encoded(
            &sample_event(),
            &cfg.topic,
            bytes::Bytes::from(format!("k{i}")),
            bytes::Bytes::from(format!("doomed-{i}")),
        )
        .await
        .expect("send accepted");
    }
    sink.abort_checkpoint_barrier()
        .await
        .expect("abort barrier");

    // Whatever reached the log is covered by an abort marker; whatever did not was
    // cancelled. Either way a read_committed consumer sees no *data*. Asserting on
    // the last stable offset would be wrong: the abort marker is itself a control
    // record and takes an offset, so the LSO advances past it on a correct abort.
    let visible = read_committed_values(&broker.bootstrap_servers(), topic).await;
    assert!(
        visible.is_empty(),
        "no aborted record may become visible to a read_committed consumer, saw {visible:?}"
    );

    sink.close().await.expect("close");
}

// ── Sink error classification ────────────────────────────────────────────

/// Every Kafka failure used to map to `SourceError`, which classifies as Transient.
/// A record the broker rejects permanently was therefore retried until the circuit
/// breaker killed the process — and then failed identically after the restart, on the
/// same record, forever. The dead-letter queue could not help, because its branch
/// only runs for a non-recoverable error and this sink never produced one.
#[test]
fn a_poison_record_is_quarantinable_not_retriable() {
    let error = krafka::error::KrafkaError::Broker {
        code: krafka::error::ErrorCode::MessageTooLarge,
        message: "record exceeds max.message.bytes".to_string(),
    };
    let classified = classify_krafka_error(&error, "sink delivery failed");

    assert!(
        !classified.is_recoverable(),
        "a record the broker will reject identically forever is not retriable"
    );
    assert!(
        classified.is_dead_letterable(),
        "an oversized record is the record's fault, so quarantine is the way forward"
    );
}

/// The mirror image, and the reason `is_dead_letterable` exists as a separate
/// question. Bad credentials are permanent *and* not the record's fault: quarantining
/// them would drain the entire change stream into the DLQ one event at a time while
/// every health check still reported the pipeline as running.
#[test]
fn an_authorization_failure_is_neither_retriable_nor_quarantinable() {
    let error = krafka::error::KrafkaError::Broker {
        code: krafka::error::ErrorCode::TopicAuthorizationFailed,
        message: "not authorized".to_string(),
    };
    let classified = classify_krafka_error(&error, "sink delivery failed");

    assert!(
        !classified.is_recoverable(),
        "an ACL will not change on retry"
    );
    assert!(
        !classified.is_dead_letterable(),
        "dead-lettering an ACL failure quarantines every event in the stream"
    );
}

/// A leader election is the common case and must stay retriable, or a routine broker
/// restart becomes a process exit and a full replay from the last checkpoint.
#[test]
fn a_transient_broker_condition_stays_retriable() {
    let error = krafka::error::KrafkaError::Broker {
        code: krafka::error::ErrorCode::LeaderNotAvailable,
        message: "leader election in progress".to_string(),
    };
    assert!(
        classify_krafka_error(&error, "sink delivery failed").is_recoverable(),
        "a leader election must be retried, not escalated"
    );
}

/// The classification has to survive the trip through `rustcdc::core::Error` and back,
/// because that is the boundary the sink's result actually crosses on its way to the
/// batch loop's dead-letter decision.
#[test]
fn classification_survives_the_round_trip_through_rustcdc() {
    let poison = classify_krafka_error(
        &krafka::error::KrafkaError::Broker {
            code: krafka::error::ErrorCode::InvalidRecord,
            message: "malformed".to_string(),
        },
        "sink delivery failed",
    );
    let round_tripped = AppError::Runtime(RtError::from(poison));
    assert!(!round_tripped.is_recoverable());
    assert!(
        round_tripped.is_dead_letterable(),
        "a poison record must still be quarantinable after crossing the sink boundary"
    );

    let fatal = classify_krafka_error(
        &krafka::error::KrafkaError::Auth {
            message: "bad credentials".to_string(),
        },
        "sink delivery failed",
    );
    let round_tripped = AppError::Runtime(RtError::from(fatal));
    assert!(!round_tripped.is_recoverable());
    assert!(
        !round_tripped.is_dead_letterable(),
        "an auth failure must not become quarantinable by crossing the sink boundary"
    );
}

/// `init_transactions` must fence the previous producer for the same
/// transactional id by bumping the epoch — that is what stops a zombie writer
/// from committing after this instance took over (KIP-447).
#[tokio::test]
async fn fake_broker_reinit_fences_the_previous_producer_epoch() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.txn.fence", 1);

    let cfg = transactional_kafka_config(
        &broker.bootstrap_servers(),
        "cdc.fake.txn.fence",
        "cdc-txn-fence",
    );

    let first = KafkaSink::new(&cfg).await.expect("first sink");
    let (id_a, epoch_a) = broker
        .transactional_producer("cdc-txn-fence")
        .expect("producer registered");

    // A second instance claiming the same transactional id — the restart case.
    let mut second = KafkaSink::new(&cfg).await.expect("second sink");
    let (id_b, epoch_b) = broker
        .transactional_producer("cdc-txn-fence")
        .expect("producer still registered");

    assert_eq!(
        id_a, id_b,
        "re-initialising the same transactional id must keep the producer id"
    );
    assert!(
        epoch_b > epoch_a,
        "re-initialising must bump the epoch to fence the previous producer \
         ({epoch_a} -> {epoch_b})"
    );

    drop(first);
    second.close().await.expect("close");
}

/// `linger_ms` must default to 0.
///
/// **This comment used to describe the opposite of what the code does.** It said the
/// sink awaited each record's broker confirmation before returning from
/// `send_encoded`, so a batch could never accumulate and every record paid the full
/// linger alone — capping throughput at `1000 / linger_ms` events per second. That
/// was true before `send_encoded` began pipelining into [`SendWindow`], and
/// `KafkaSinkConfig::linger_ms` has said so ever since; this comment did not, and a
/// design proposal built a durability argument on it.
///
/// What is actually true: `send_encoded` returns once the record is in the
/// accumulator, and acknowledgements are collected by the window or by `flush`. The
/// default is still 0 because CDC consumers are latency-sensitive, not because
/// linger is charged per record. The assertion below measures exactly that.
#[tokio::test]
async fn linger_is_not_charged_per_record_at_the_default() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.linger", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.linger");
    assert_eq!(
        cfg.linger_ms, 0,
        "the default linger must be 0: CDC consumers are latency-sensitive, and the \
         first record of a partially-filled batch waits it out"
    );

    let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");
    let started = std::time::Instant::now();
    for i in 0..20u32 {
        sink.send_encoded(
            &sample_event(),
            &cfg.topic,
            bytes::Bytes::from(format!("k{i}")),
            bytes::Bytes::from("v"),
        )
        .await
        .expect("send");
    }
    let elapsed = started.elapsed();
    sink.close().await.expect("close");

    // 20 pipelined sends against an in-process broker, drained by `close`. With a
    // 5 ms linger this took >100 ms; with 0 it is bounded by the round-trips alone.
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "20 confirmed sends took {elapsed:?}; the default linger is charging \
         per-record latency"
    );
}

/// The headline behaviour: one sink, one template, four topics.
///
/// Two schemas times two tables, through `SinkBinding` — so the codec, the size
/// limit, the key derivation and the topic resolution all run exactly as they do in
/// the pipeline. Before templating this needed four `[[sinks]]` blocks, four
/// `[[pipeline.routes]]` entries and four producers.
#[tokio::test]
async fn a_templated_topic_fans_one_sink_across_a_topic_per_table() {
    use krafka::consumer::CompactedTopicConsumer;

    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    let expected = [
        "cdc.public.orders",
        "cdc.public.customers",
        "cdc.billing.orders",
        "cdc.billing.invoices",
    ];
    for topic in expected {
        broker.create_topic(topic, 1);
    }

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.${schema}.${table}");
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    for (schema, table) in [
        ("public", "orders"),
        ("public", "customers"),
        ("billing", "orders"),
        ("billing", "invoices"),
        // A repeat, to exercise the resolver's cached path alongside the cold one.
        ("public", "orders"),
    ] {
        let event = Event::builder(table, Operation::Insert)
            .after(json!({"id": 1}))
            .source(SourceMetadata::new("postgres", "0/16B6A71", 1))
            .ts(1)
            .schema(schema)
            .primary_key(["id"])
            .build();
        binding.send_event(&event).await.expect("send");
    }
    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    for topic in expected {
        let mut consumer = CompactedTopicConsumer::from_consumer_builder(
            krafka::consumer::Consumer::builder()
                .bootstrap_servers(broker.bootstrap_servers())
                .client_id(format!("fake-template-{topic}"))
                .connect_timeout(Duration::from_secs(2))
                .request_timeout(Duration::from_secs(10)),
            topic,
        )
        .await
        .expect("compacted consumer");
        consumer
            .scan(Duration::from_millis(1_000))
            .await
            .expect("scan");
        assert!(
            !consumer.table().is_empty(),
            "topic {topic} received no records; the template did not route to it"
        );
        consumer.close().await.expect("consumer close");
    }
    binding.close().await.expect("binding close");
}

/// A table whose name Kafka cannot spell is the event's problem, not the pipeline's:
/// it must surface as `ValidationError`, which `AppError::is_dead_letterable` routes
/// to the DLQ, rather than as something that halts every other table too.
#[tokio::test]
async fn an_unrepresentable_table_name_is_a_dead_letterable_event_error() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.public.orders", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.${schema}.${table}");
    let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");

    let bad = Event::builder("my table", Operation::Insert)
        .after(json!({"id": 1}))
        .source(SourceMetadata::new("postgres", "0/1", 1))
        .ts(1)
        .schema("public")
        .build();
    let error = sink
        .topic_for(&bad)
        .expect_err("space is not a topic character");
    assert!(
        matches!(error, RtError::ValidationError(_)),
        "expected ValidationError so the event dead-letters, got {error:?}"
    );
    assert!(
        crate::error::AppError::Runtime(error).is_dead_letterable(),
        "a table Kafka cannot name must quarantine the event, not halt the stream"
    );

    // The healthy table beside it is unaffected.
    let good = Event::builder("orders", Operation::Insert)
        .after(json!({"id": 1}))
        .source(SourceMetadata::new("postgres", "0/1", 1))
        .ts(1)
        .schema("public")
        .build();
    assert_eq!(
        &*sink.topic_for(&good).expect("orders"),
        "cdc.public.orders"
    );
    sink.close().await.expect("close");
}

/// A naming collision is the opposite: not attributable to either event, and silent
/// corruption if allowed through. It must halt.
#[tokio::test]
async fn a_topic_naming_collision_halts_rather_than_dead_lettering_one_of_the_tables() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    let mut cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.${schema}.${table}");
    cfg.topic_naming.invalid_characters = crate::topic::InvalidCharacterPolicy::Replace;
    let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");

    let event = |table: &str| {
        Event::builder(table, Operation::Insert)
            .after(json!({"id": 1}))
            .source(SourceMetadata::new("postgres", "0/1", 1))
            .ts(1)
            .schema("public")
            .build()
    };

    assert_eq!(
        &*sink.topic_for(&event("my_table")).expect("first"),
        "cdc.public.my_table"
    );
    let error = sink
        .topic_for(&event("my table"))
        .expect_err("two tables must not share one topic silently");
    assert!(
        matches!(error, RtError::ConfigError(_)),
        "expected ConfigError so the pipeline halts, got {error:?}"
    );
    assert!(
        !crate::error::AppError::Runtime(error).is_dead_letterable(),
        "quarantining one of two colliding tables would drain a healthy table"
    );
    sink.close().await.expect("close");
}

/// Preflight against a template checks the topics the configuration names — and
/// fails when one of them is missing, at startup, which is the whole point.
#[tokio::test]
async fn preflight_describes_every_topic_the_configured_tables_render_to() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.public.orders", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.${schema}.${table}");
    let tables = vec![
        crate::topic::QualifiedTable::parse_concrete("public.orders").unwrap(),
        crate::topic::QualifiedTable::parse_concrete("public.customers").unwrap(),
    ];

    let mut sink = KafkaSink::new(&cfg)
        .await
        .expect("kafka sink")
        .with_preflight_tables(tables.clone());
    let error = sink
        .preflight_check()
        .await
        .expect_err("cdc.public.customers was never created");
    let message = error.to_string();
    assert!(message.contains("cdc.public.customers"), "{message}");
    assert!(!message.contains("'cdc.public.orders'"), "{message}");
    sink.close().await.expect("close");

    // Create the missing topic and the same configuration passes.
    broker.create_topic("cdc.public.customers", 1);
    let mut sink = KafkaSink::new(&cfg)
        .await
        .expect("kafka sink")
        .with_preflight_tables(tables);
    sink.preflight_check().await.expect("both topics now exist");
    sink.close().await.expect("close");
}

/// A stream-only pipeline names no tables up front. Preflight must still verify the
/// broker is reachable and must not invent a topic to check.
#[tokio::test]
async fn preflight_with_no_configured_tables_verifies_the_broker_and_nothing_more() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.${schema}.${table}");
    let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");
    sink.preflight_check()
        .await
        .expect("no tables to check is not a failure");
    sink.close().await.expect("close");
}

/// A configured table that cannot be rendered is a startup *warning*: it will
/// dead-letter if it ever produces an event, but failing the pipeline for it would
/// take out every healthy table alongside it.
#[tokio::test]
async fn preflight_warns_about_an_unrepresentable_table_instead_of_failing() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.public.orders", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.${schema}.${table}");
    let mut sink = KafkaSink::new(&cfg)
        .await
        .expect("kafka sink")
        .with_preflight_tables(vec![
            crate::topic::QualifiedTable::parse_concrete("public.orders").unwrap(),
            crate::topic::QualifiedTable::parse_concrete("public.my table").unwrap(),
        ]);
    sink.preflight_check()
        .await
        .expect("an unnameable table must not fail startup for the others");
    sink.close().await.expect("close");
}

/// A literal topic keeps the exact preflight it always had — one topic, described,
/// and a named failure when it is absent.
#[tokio::test]
async fn a_literal_topic_preflights_exactly_as_before() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.literal", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.literal");
    let mut sink = KafkaSink::new(&cfg).await.expect("kafka sink");
    sink.preflight_check().await.expect("topic exists");
    sink.close().await.expect("close");

    let missing = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.absent");
    let mut sink = KafkaSink::new(&missing).await.expect("kafka sink");
    let error = sink.preflight_check().await.expect_err("topic is absent");
    assert!(error.to_string().contains("cdc.fake.absent"), "{error}");
    sink.close().await.expect("close");
}

/// A sink built from a `KafkaSinkConfig` that never passed through `config::load` —
/// the replay command and the Kafka state backend both do this — must still reject
/// an unparseable template rather than discovering it per event.
#[tokio::test]
async fn the_sink_constructor_rejects_a_template_the_loader_never_saw() {
    let cfg = sample_kafka_config("127.0.0.1:9092", "cdc.${db}.${table}");
    let error = match KafkaSink::new(&cfg).await {
        Err(error) => error,
        Ok(_) => panic!("an unknown placeholder must not build a sink"),
    };
    assert!(error.to_string().contains("${db}"), "{error}");
}

// ── CDC provenance headers ───────────────────────────────────────────────

/// Read every record on a topic, raw, with its headers intact.
///
/// Not `CompactedTopicConsumer`: that one *applies* tombstones, so the record whose
/// headers matter most would be gone before it could be inspected.
async fn read_raw(
    broker: &krafka::testing::FakeBroker,
    topic: &str,
) -> Vec<krafka::consumer::ConsumerRecord> {
    let consumer = krafka::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .client_id(format!("raw-{topic}"))
        .connect_timeout(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(10))
        .build()
        .await
        .expect("consumer");
    consumer.assign(topic, vec![0]).await.expect("assign");
    consumer
        .seek_to_beginning(topic, 0)
        .await
        .expect("seek to beginning");

    let mut records = Vec::new();
    for _ in 0..20 {
        let polled = consumer
            .poll(Duration::from_millis(200))
            .await
            .expect("poll");
        if polled.is_empty() && !records.is_empty() {
            break;
        }
        records.extend(polled);
    }
    consumer.close().await.expect("consumer close");
    records
}

fn header<'a>(
    record: &'a krafka::consumer::ConsumerRecord,
    name: &str,
) -> Option<std::borrow::Cow<'a, str>> {
    record
        .headers
        .iter()
        .find(|(k, _)| k.as_ref() == name.as_bytes())
        .and_then(|(_, v)| v.as_ref())
        .map(|v| String::from_utf8_lossy(v))
}

#[tokio::test]
async fn every_record_carries_its_cdc_provenance_headers() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.headers", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.headers");
    assert_eq!(
        cfg.record_headers,
        crate::config::sink::KafkaRecordHeaders::Cdc,
        "headers must default to on"
    );
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    binding.send_event(&sample_event()).await.expect("send");
    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    let records = read_raw(&broker, "cdc.fake.headers").await;
    assert_eq!(records.len(), 1);
    let r = &records[0];
    assert_eq!(header(r, super::HEADER_OP).as_deref(), Some("insert"));
    assert_eq!(
        header(r, super::HEADER_SOURCE_SCHEMA).as_deref(),
        Some("public")
    );
    assert_eq!(
        header(r, super::HEADER_SOURCE_TABLE).as_deref(),
        Some("users")
    );
    assert_eq!(
        header(r, super::HEADER_SOURCE_NAME).as_deref(),
        Some("postgres")
    );
    assert_eq!(
        header(r, super::HEADER_SOURCE_OFFSET).as_deref(),
        Some("0/16B6A71")
    );
    assert_eq!(header(r, super::HEADER_SOURCE_TS_MS).as_deref(), Some("1"));
    binding.close().await.expect("binding close");
}

/// The case the headers exist for. A tombstone has a key and a null value, so without
/// headers nothing on the record names the table, the operation, or the log position.
/// Debezium tombstones are opaque for exactly this reason.
#[tokio::test]
async fn a_tombstone_carries_the_headers_that_are_its_only_provenance() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.tombhdr", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.tombhdr");
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    binding
        .send_event(&delete_event("public", "orders", 42))
        .await
        .expect("send delete");
    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    let records = read_raw(&broker, "cdc.fake.tombhdr").await;
    assert_eq!(records.len(), 2, "a delete plus its tombstone");
    let tombstone = &records[1];
    assert!(
        tombstone.value.is_none(),
        "the second record must be the null-value tombstone"
    );
    assert_eq!(
        header(tombstone, super::HEADER_OP).as_deref(),
        Some("delete"),
        "without this a tombstone says nothing about what produced it"
    );
    assert_eq!(
        header(tombstone, super::HEADER_SOURCE_TABLE).as_deref(),
        Some("orders")
    );
    assert_eq!(
        header(tombstone, super::HEADER_SOURCE_SCHEMA).as_deref(),
        Some("public")
    );
    assert_eq!(
        header(tombstone, super::HEADER_SOURCE_OFFSET).as_deref(),
        Some("0/16B6A71")
    );
    binding.close().await.expect("binding close");
}

#[tokio::test]
async fn headers_none_publishes_no_headers_at_all() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.nohdr", 1);

    let mut cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.nohdr");
    cfg.record_headers = crate::config::sink::KafkaRecordHeaders::None;
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    binding
        .send_event(&delete_event("public", "orders", 42))
        .await
        .expect("send delete");
    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    for record in read_raw(&broker, "cdc.fake.nohdr").await {
        assert!(
            record.headers.is_empty(),
            "headers = \"none\" must publish none, on the tombstone too"
        );
    }
    binding.close().await.expect("binding close");
}

/// An absent schema is *omitted*, not sent as a null or empty value — a null header
/// value is a third state on the wire, and an empty one is indistinguishable from a
/// schema genuinely named "".
#[test]
fn an_absent_schema_omits_its_header_rather_than_sending_an_empty_one() {
    let mut event = sample_event();
    event.schema = None;
    let headers = super::cdc_headers(&event);
    assert!(
        !headers
            .iter()
            .any(|(k, _)| k == super::HEADER_SOURCE_SCHEMA),
        "the schema header must be absent, not empty"
    );
    assert!(headers.iter().all(|(_, v)| v.is_some()), "no null values");

    event.schema = Some(String::new());
    assert!(
        !super::cdc_headers(&event)
            .iter()
            .any(|(k, _)| k == super::HEADER_SOURCE_SCHEMA),
        "an empty schema is how `qualified_table_name` already spells absent"
    );
}

/// A header value long enough to have the broker reject the whole record would fail the
/// *event* over a diagnostic field. The payload always carries the full value.
#[test]
fn an_absurd_identifier_is_truncated_rather_than_risking_the_record() {
    let mut event = sample_event();
    event.table = "t".repeat(4096);
    let headers = super::cdc_headers(&event);
    let table = headers
        .iter()
        .find(|(k, _)| k == super::HEADER_SOURCE_TABLE)
        .and_then(|(_, v)| v.clone())
        .expect("table header");
    assert!(
        table.len() <= super::MAX_IDENTIFIER_HEADER_BYTES + 4,
        "expected truncation, got {} bytes",
        table.len()
    );
}

// ── Tombstones ───────────────────────────────────────────────────────────

fn delete_event(schema: &str, table: &str, id: i64) -> Event {
    Event::builder(table, Operation::Delete)
        .before(json!({"id": id, "name": "bob"}))
        .source(SourceMetadata::new("postgres", "0/16B6A71", 1))
        .ts(1)
        .schema(schema)
        .primary_key(["id"])
        .build()
}

/// Scan a topic and report `(tombstones seen, is the key still live)`.
///
/// `CompactedTopicConsumer` *applies* tombstones, so a tombstoned key is absent from
/// `table()` — which on its own is indistinguishable from a delete that never
/// arrived. `tombstones_processed` is the assertion that actually separates them, and
/// both are returned so a test can say which one it means.
async fn scan_topic(broker: &krafka::testing::FakeBroker, topic: &str, key: &[u8]) -> (u64, bool) {
    use krafka::consumer::CompactedTopicConsumer;

    let mut consumer = CompactedTopicConsumer::from_consumer_builder(
        krafka::consumer::Consumer::builder()
            .bootstrap_servers(broker.bootstrap_servers())
            .client_id(format!("tombstone-scan-{topic}"))
            .connect_timeout(Duration::from_secs(2))
            .request_timeout(Duration::from_secs(10)),
        topic,
    )
    .await
    .expect("compacted consumer");
    consumer
        .scan(Duration::from_millis(1_000))
        .await
        .expect("scan");
    let seen = consumer.table().tombstones_processed();
    let live = consumer.table().get(key).is_some();
    consumer.close().await.expect("consumer close");
    (seen, live)
}

/// The headline behaviour: a delete is followed by a tombstone on the same key.
///
/// Through `SinkBinding`, so the codec, the size limit, the key derivation and the
/// topic resolution all run exactly as they do in the pipeline.
#[tokio::test]
async fn a_delete_is_followed_by_a_tombstone_on_the_same_key() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.tombstone", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.tombstone");
    assert!(
        cfg.tombstones_on_delete,
        "the default must be on, matching Debezium"
    );
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    binding
        .send_event(&delete_event("public", "orders", 42))
        .await
        .expect("send delete");
    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    let (tombstones, live) = scan_topic(&broker, "cdc.fake.tombstone", br#"{"id":42}"#).await;
    assert_eq!(
        tombstones, 1,
        "a delete must publish exactly one tombstone on the row's key"
    );
    // This is also the ordering assertion, and the reason it is worth stating: the
    // compacted view applies records in log order, so if the tombstone had been
    // reordered *ahead* of its delete, the delete would be the last record for the
    // key and it would still be live. `!live` can only hold if the tombstone came
    // second.
    assert!(
        !live,
        "the tombstone must remove the key from a compacted view, and must follow \
         the delete rather than precede it"
    );
    binding.close().await.expect("binding close");
}

/// An insert must not be tombstoned — the obvious way to get this wrong is to key the
/// decision off "has a key" alone.
#[tokio::test]
async fn a_non_delete_publishes_no_tombstone() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.insert", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.insert");
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    binding.send_event(&sample_event()).await.expect("send");
    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    let (tombstones, live) = scan_topic(&broker, "cdc.fake.insert", br#"{"id":42}"#).await;
    assert_eq!(tombstones, 0, "an insert is not a deletion");
    assert!(
        live,
        "the inserted row must remain live in a compacted view"
    );
    binding.close().await.expect("binding close");
}

#[tokio::test]
async fn tombstones_can_be_turned_off() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.no-tombstone", 1);

    let mut cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.no-tombstone");
    cfg.tombstones_on_delete = false;
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    binding
        .send_event(&delete_event("public", "orders", 42))
        .await
        .expect("send delete");
    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    let (tombstones, live) = scan_topic(&broker, "cdc.fake.no-tombstone", br#"{"id":42}"#).await;
    assert_eq!(
        tombstones, 0,
        "tombstones_on_delete = false must suppress it"
    );
    assert!(
        live,
        "without a tombstone the delete record itself is what compaction retains"
    );
    binding.close().await.expect("binding close");
}

/// A truncate is keyed by the qualified table name, not a row key. Tombstoning it
/// would compact away the truncate marker itself.
#[tokio::test]
async fn a_truncate_is_never_tombstoned() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.truncate", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.truncate");
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    let truncate = Event::builder("orders", Operation::Truncate)
        .source(SourceMetadata::new("postgres", "0/16B6A71", 1))
        .ts(1)
        .schema("public")
        .build();
    binding.send_event(&truncate).await.expect("send truncate");
    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    let (tombstones, live) = scan_topic(&broker, "cdc.fake.truncate", b"public.orders").await;
    assert_eq!(tombstones, 0, "a truncate carries no row key to tombstone");
    assert!(
        live,
        "the truncate marker must survive compaction; a tombstone on the \
         table-name key would erase it"
    );
    binding.close().await.expect("binding close");
}

/// A table with no primary key is keyed by the qualified table name, so a tombstone
/// would compact away every event the table ever produced.
#[tokio::test]
async fn a_keyless_tables_delete_is_never_tombstoned() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.keyless", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.keyless");
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    let mut keyless = delete_event("public", "orders", 42);
    keyless.primary_key = None;
    binding.send_event(&keyless).await.expect("send delete");
    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    let (tombstones, live) = scan_topic(&broker, "cdc.fake.keyless", b"public.orders").await;
    assert_eq!(
        tombstones, 0,
        "the fallback key names a table, not a row; tombstoning it would erase the \
         table's whole history"
    );
    assert!(live, "the delete record itself must survive");
    binding.close().await.expect("binding close");
}

/// Two rows of the same table, deleted: each key gets its own tombstone, and neither
/// disturbs the other.
#[tokio::test]
async fn each_deleted_row_gets_its_own_tombstone() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.rows", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.rows");
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    // One row stays, two are deleted.
    binding
        .send_event(&sample_event())
        .await
        .expect("insert 42");
    binding
        .send_event(&delete_event("public", "users", 7))
        .await
        .expect("delete 7");
    binding
        .send_event(&delete_event("public", "users", 9))
        .await
        .expect("delete 9");
    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    let (tombstones, live_42) = scan_topic(&broker, "cdc.fake.rows", br#"{"id":42}"#).await;
    assert_eq!(tombstones, 2, "one tombstone per deleted row");
    assert!(live_42, "the row that was not deleted must survive");
    binding.close().await.expect("binding close");
}

/// A tombstone is only useful if it is on the same topic as the delete. With a
/// templated topic that is not automatic — it is the resolver being asked twice.
#[tokio::test]
async fn a_tombstone_lands_on_the_same_templated_topic_as_its_delete() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.public.orders", 1);
    broker.create_topic("cdc.billing.orders", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.${schema}.${table}");
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    binding
        .send_event(&delete_event("public", "orders", 1))
        .await
        .expect("send public delete");
    binding
        .send_event(&delete_event("billing", "orders", 2))
        .await
        .expect("send billing delete");
    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    for (topic, key) in [
        ("cdc.public.orders", &br#"{"id":1}"#[..]),
        ("cdc.billing.orders", &br#"{"id":2}"#[..]),
    ] {
        let (tombstones, live) = scan_topic(&broker, topic, key).await;
        assert_eq!(
            tombstones, 1,
            "{topic} must carry its own delete's tombstone"
        );
        assert!(!live, "{topic}: the key must be compacted away");
    }
    binding.close().await.expect("binding close");
}

/// Under `effectively_once` the delete and its tombstone must commit together, or an
/// abort between them leaves a delete with no tombstone — the exact state this
/// feature exists to prevent, and one nothing downstream could detect.
///
/// Nothing opts into this: the barrier opens a transaction around the batch and both
/// records are enqueued inside it. The test asserts that the structure holds.
#[tokio::test]
async fn a_delete_and_its_tombstone_commit_in_one_transaction() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.txn.tombstone", 1);

    let cfg = transactional_kafka_config(
        &broker.bootstrap_servers(),
        "cdc.fake.txn.tombstone",
        "cdc-txn-tombstone",
    );
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    use rustcdc::sink::SinkAdapter as _;
    binding
        .begin_checkpoint_barrier()
        .await
        .expect("begin barrier");
    binding
        .send_event(&delete_event("public", "orders", 42))
        .await
        .expect("send delete");

    // Both records are in the open transaction, so neither is stable yet. If the
    // tombstone had escaped the transaction this would already be non-zero.
    assert_eq!(
        broker
            .last_stable_offset("cdc.fake.txn.tombstone", 0)
            .expect("partition exists"),
        0,
        "neither the delete nor its tombstone may be stable before the commit"
    );

    binding
        .commit_checkpoint_barrier()
        .await
        .expect("commit barrier");
    binding.flush().await.expect("flush");

    let (tombstones, live) = scan_topic(&broker, "cdc.fake.txn.tombstone", br#"{"id":42}"#).await;
    assert_eq!(tombstones, 1, "the tombstone must commit with its delete");
    assert!(!live);
    assert!(
        broker
            .aborted_transactions("cdc.fake.txn.tombstone", 0)
            .is_empty(),
        "a committed barrier must leave no abort marker"
    );
    binding.close().await.expect("binding close");
}

/// The counter that tells an operator tombstones are actually being emitted.
#[tokio::test]
async fn the_tombstone_counter_reaches_the_delivery_metrics() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.counter", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.counter");
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    for id in [1, 2, 3] {
        binding
            .send_event(&delete_event("public", "orders", id))
            .await
            .expect("send delete");
    }
    use rustcdc::sink::SinkAdapter as _;
    // The binding publishes its counters on flush; the scrape path reads them there.
    binding.flush().await.expect("flush");

    let snapshot = binding.metrics_handle().snapshot();
    assert_eq!(
        snapshot.kafka_tombstones_total, 3,
        "three deletes must report three tombstones"
    );
    assert_eq!(
        snapshot.kafka_unkeyed_deletes_total, 0,
        "these deletes all carried a row key"
    );
    binding.close().await.expect("binding close");
}

/// The alertable counter. `kafka_tombstones_total` at zero is the normal state of a
/// pipeline with no deletes; *this* at non-zero always means a key a compacted topic
/// will never reclaim.
#[tokio::test]
async fn a_delete_with_no_row_key_is_counted_separately_from_a_tombstoned_one() {
    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.unkeyed", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.unkeyed");
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    let mut keyless = delete_event("public", "orders", 1);
    keyless.primary_key = None;
    binding.send_event(&keyless).await.expect("keyless delete");
    binding
        .send_event(&delete_event("public", "orders", 2))
        .await
        .expect("keyed delete");

    // A truncate also carries no row key, and is deliberately *not* counted: nothing
    // is wrong with it, and counting it would make the alert fire on healthy
    // pipelines with nothing to do about it.
    let truncate = Event::builder("orders", Operation::Truncate)
        .source(SourceMetadata::new("postgres", "0/16B6A71", 1))
        .ts(1)
        .schema("public")
        .build();
    binding.send_event(&truncate).await.expect("truncate");

    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    let snapshot = binding.metrics_handle().snapshot();
    assert_eq!(
        snapshot.kafka_unkeyed_deletes_total, 1,
        "only the keyless delete counts; the truncate is not a defect"
    );
    assert_eq!(
        snapshot.kafka_tombstones_total, 1,
        "the keyed delete is still tombstoned"
    );
    binding.close().await.expect("binding close");
}

/// The hole that exists only because the tombstone is the second record.
///
/// A record-attributable failure is normally dead-letterable — quarantine the event,
/// advance. For a tombstone that is wrong: its delete is already in the send window,
/// so advancing publishes a delete with nothing behind it, and every counter reports
/// success. It must halt instead.
#[test]
fn a_terminal_tombstone_failure_halts_instead_of_quarantining_its_own_delete() {
    use crate::error::AppError;

    // What `classify_krafka_error` produces for MessageTooLarge / InvalidRecord /
    // Serialization / Compression, via `AppError::SinkPoisonRecord`.
    let poison = || RtError::from(AppError::SinkPoisonRecord("record rejected".to_string()));
    assert!(
        AppError::Runtime(poison()).is_dead_letterable(),
        "precondition: this classification would otherwise quarantine the event"
    );

    let message = super::escalate_tombstone_failure(poison()).to_string();
    assert!(
        !AppError::Runtime(super::escalate_tombstone_failure(poison())).is_dead_letterable(),
        "a tombstone failure must not quarantine the event whose delete is already \
         committed to the window"
    );
    assert!(
        !AppError::Runtime(super::escalate_tombstone_failure(poison())).is_recoverable(),
        "and it must not be retried forever either — it halts"
    );
    assert!(message.contains("already accepted"), "{message}");
    assert!(message.contains("record rejected"), "{message}");
}

/// Retriable failures must pass through untouched: the batch loop retries the whole
/// batch, re-sending the delete, which idempotency collapses.
#[test]
fn a_retriable_tombstone_failure_stays_retriable() {
    use crate::error::AppError;

    let transient = RtError::from(AppError::SinkTimeout("broker leader election".to_string()));
    let passed_through = super::escalate_tombstone_failure(transient);
    assert!(
        AppError::Runtime(passed_through).is_recoverable(),
        "escalating a transient failure would turn a leader election into an outage"
    );
}

/// End-to-end check of the message-key contract through `SinkBinding`:
/// events with a primary key are keyed by the PK JSON (per-row ordering);
/// keyless events fall back to the qualified table name instead of an
/// empty key (which Kafka would hash — pinning every keyless event of
/// every table to one partition).
#[tokio::test]
async fn fake_broker_message_keys_use_pk_json_with_table_name_fallback() {
    use krafka::consumer::CompactedTopicConsumer;

    let broker = krafka::testing::FakeBroker::start()
        .await
        .expect("fake broker");
    broker.create_topic("cdc.fake.keys", 1);

    let cfg = sample_kafka_config(&broker.bootstrap_servers(), "cdc.fake.keys");
    let mut binding = crate::sink::build_binding(
        &crate::config::schema::SinkConfig::Kafka(cfg),
        &crate::sink::SinkBuildContext::new(usize::MAX),
    )
    .await
    .expect("sink binding");

    let keyed = sample_event(); // primary_key = ["id"], after.id = 42
    let mut keyless = sample_event();
    keyless.primary_key = None;

    binding.send_event(&keyed).await.expect("send keyed");
    binding.send_event(&keyless).await.expect("send keyless");
    use rustcdc::sink::SinkAdapter as _;
    binding.flush().await.expect("flush");

    let mut consumer = CompactedTopicConsumer::from_consumer_builder(
        krafka::consumer::Consumer::builder()
            .bootstrap_servers(broker.bootstrap_servers())
            .client_id("fake-key-check".to_string())
            .connect_timeout(Duration::from_secs(2))
            .request_timeout(Duration::from_secs(10)),
        "cdc.fake.keys",
    )
    .await
    .expect("compacted consumer");
    consumer
        .scan(Duration::from_millis(1_000))
        .await
        .expect("scan");
    let table = consumer.table();

    assert!(
        table.contains_key(br#"{"id":42}"#.as_slice()),
        "keyed event must use the primary-key JSON as message key"
    );
    assert!(
        table.contains_key(b"public.users".as_slice()),
        "keyless event must fall back to the qualified table name key"
    );
    consumer.close().await.expect("consumer close");
    binding.close().await.expect("binding close");
}
