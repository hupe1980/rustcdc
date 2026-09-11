//! Admin-plane tests that need a **real** Kafka broker.
//!
//! Split out of `admin/tests.rs` for two reasons. The obvious one is size: that file
//! reached 4 006 lines, past the budget `tests/architecture.rs` enforces. The useful one is
//! that these two are a different *kind* of test — everything else in `admin/tests.rs` runs
//! against in-process state and always executes, while these need a broker and skip without
//! one. Mixing the two hid how much of the file never ran.
//!
//! They skip unless `CDC_TEST_KAFKA_BROKERS` and the topic variables are set.
//! `tests/integration_redpanda.rs` is the suite CI runs, against a container it manages
//! itself; these remain for pointing at an existing cluster.

use super::tests::*;
use super::*;
use crate::config::schema::{
    AdminNotificationKafkaConfig, AdminSignalIngressKafkaConfig, KafkaSecurityConfig,
    KafkaSecurityProtocol,
};
use axum::http::header::AUTHORIZATION;
use krafka::consumer::AutoOffsetReset;

#[tokio::test]
async fn signal_action_emits_non_admin_notification_kafka_events_when_configured() {
    let Ok(brokers) = std::env::var("CDC_TEST_KAFKA_BROKERS") else {
        eprintln!("skipping admin notification kafka test (CDC_TEST_KAFKA_BROKERS is not set)");
        return;
    };
    let Ok(topic) = std::env::var("CDC_TEST_KAFKA_TOPIC") else {
        eprintln!("skipping admin notification kafka test (CDC_TEST_KAFKA_TOPIC is not set)");
        return;
    };

    let suffix = test_suffix();
    let mut cfg = sample_config();
    let security = KafkaSecurityConfig {
        protocol: match std::env::var("CDC_TEST_KAFKA_PROTOCOL") {
            Ok(protocol) if protocol.eq_ignore_ascii_case("tls") => KafkaSecurityProtocol::Tls,
            _ => KafkaSecurityProtocol::Plaintext,
        },
        ssl_ca_location: std::env::var("CDC_TEST_KAFKA_CA").ok().map(PathBuf::from),
        ..KafkaSecurityConfig::default()
    };
    cfg.admin.notification_kafka = Some(AdminNotificationKafkaConfig {
        brokers: brokers.clone(),
        topic: topic.clone(),
        client_id: format!("cdc-admin-notifications-{suffix}"),
        ack_timeout_ms: 1_000,
        retry_backoff_ms: 100,
        retry_max_attempts: 3,
        compression: Default::default(),
        security,
    });

    let admin = AdminState::new(&cfg).await.expect("admin state");
    configure_test_read_write_tokens(&admin);

    let mut write_headers = HeaderMap::new();
    write_headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer write-secret"),
    );

    let signal_id = format!("sig-kafka-{suffix}");
    let correlation_id = format!("corr-kafka-{suffix}");
    let response = signal_action(
        State(admin.clone()),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9012))),
        write_headers,
        Json(SignalActionRequest {
            signal_id: Some(signal_id.clone()),
            correlation_id: Some(correlation_id),
            action_type: SignalActionType::ExecuteSnapshot,
            tables: Some(vec!["public.orders".to_string()]),
            conditions: None,
            message: Some("run snapshot".to_string()),
            additional_data: Some(serde_json::json!({"operator": "kafka-test"})),
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    let consumer = Consumer::builder()
        .bootstrap_servers(brokers)
        .group_id(format!("cdc-admin-notifications-{suffix}"))
        .client_id(format!("cdc-admin-notifications-consumer-{suffix}"))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .request_timeout(Duration::from_millis(1_000))
        .connect_timeout(crate::sink::kafka_connect_timeout(Duration::from_millis(
            1_000,
        )))
        .auth(kafka_auth_from_env())
        .build()
        .await
        .expect("consumer should build");
    consumer
        .subscribe(&[topic.as_str()])
        .await
        .expect("consumer should subscribe");

    let seen_states = consume_notification_states_until_seen(
        &consumer,
        &signal_id,
        &["STARTED", "IN_PROGRESS", "ABORTED"],
        60,
    )
    .await;

    assert!(
        ["STARTED", "IN_PROGRESS", "ABORTED"]
            .iter()
            .all(|state| seen_states.contains(*state)),
        "consumer did not observe expected kafka notification lifecycle states: {seen_states:?}"
    );

    let data = admin.data.read().await;
    assert_eq!(
        data.notification_channel_emitted_total
            .get("kafka")
            .copied(),
        Some(3)
    );
    assert_eq!(data.notification_log_emit_failures_total, 0);
}

#[tokio::test]
async fn signal_ingress_kafka_processes_execute_snapshot_actions() {
    let Ok(brokers) = std::env::var("CDC_TEST_KAFKA_BROKERS") else {
        eprintln!("skipping admin signal ingress kafka test (CDC_TEST_KAFKA_BROKERS is not set)");
        return;
    };
    let Ok(topic) = std::env::var("CDC_TEST_KAFKA_SIGNAL_INGRESS_TOPIC") else {
        eprintln!(
            "skipping admin signal ingress kafka test (CDC_TEST_KAFKA_SIGNAL_INGRESS_TOPIC is not set)"
        );
        return;
    };

    let suffix = test_suffix();
    let mut cfg = sample_config();
    let security = KafkaSecurityConfig {
        protocol: match std::env::var("CDC_TEST_KAFKA_PROTOCOL") {
            Ok(protocol) if protocol.eq_ignore_ascii_case("tls") => KafkaSecurityProtocol::Tls,
            _ => KafkaSecurityProtocol::Plaintext,
        },
        ssl_ca_location: std::env::var("CDC_TEST_KAFKA_CA").ok().map(PathBuf::from),
        ..KafkaSecurityConfig::default()
    };

    cfg.admin.signal_ingress_kafka = Some(AdminSignalIngressKafkaConfig {
        brokers: brokers.clone(),
        topic: topic.clone(),
        group_id: format!("cdc-admin-signal-ingress-{suffix}"),
        client_id: format!("cdc-admin-signal-ingress-{suffix}"),
        poll_timeout_ms: 250,
        security: security.clone(),
    });

    let admin = AdminState::new(&cfg).await.expect("admin state");

    let producer = krafka::producer::Producer::builder()
        .bootstrap_servers(brokers)
        .client_id(format!("cdc-admin-signal-ingress-producer-{suffix}"))
        .acks(krafka::producer::Acks::All)
        .request_timeout(Duration::from_millis(1_000))
        .connect_timeout(crate::sink::kafka_connect_timeout(Duration::from_millis(
            1_000,
        )))
        .auth(kafka_auth_from_env())
        .build()
        .await
        .expect("signal ingress producer should build");

    let signal_id = format!("sig-ingress-kafka-{suffix}");
    let correlation_id = format!("corr-ingress-kafka-{suffix}");
    let payload = serde_json::json!({
        "signal_id": signal_id,
        "correlation_id": correlation_id,
        "action_type": "execute_snapshot",
        "tables": ["public.orders"],
        "message": "kafka ingress execute",
        "additional_data": {"ingress": "kafka"},
    });

    let record = krafka::producer::ProducerRecord::new(
        topic,
        serde_json::to_vec(&payload).expect("serialize kafka ingress payload"),
    );
    let _metadata = producer
        .send_record(record)
        .await
        .expect("signal ingress payload send should succeed");
    producer
        .flush()
        .await
        .expect("producer flush should succeed");

    wait_for_signal_state(&admin, &signal_id, "execute_snapshot", "ABORTED").await;

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    assert!(notifications.iter().any(|notification| {
        notification.signal_id == signal_id
            && notification.action_type == "execute_snapshot"
            && notification.state == "ABORTED"
    }));
}
