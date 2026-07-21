use std::time::Duration;

use bytes::Bytes;
use krafka::admin::AdminClient;
use krafka::producer::{Acks, Producer, TransactionalProducer};
use rustcdc::core::Error as RtError;
use tokio::sync::Mutex;

use crate::config::schema::{KafkaDeliveryMode, KafkaSecurityConfig, KafkaSinkConfig};
use crate::error::AppError;

use super::SinkDeliveryGuarantee;

enum KafkaProducerClient {
    Idempotent(Producer),
    Transactional(TransactionalProducer),
}

/// Transactional checkpoint barrier state machine.
///
/// For transactional Kafka producers, the barrier lifecycle is:
/// 1. `NotActive`: Normal state (ready to begin new barrier)
/// 2. `Active`: Between begin_transaction() and commit/abort
/// 3. `Closed`: Sink is closed or producer failed catastrophically
///
/// This enum makes state transitions explicit and prevents confusion from boolean flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BarrierState {
    /// No transaction in progress; ready to begin() a new barrier
    NotActive,
    /// Transaction active; between begin_transaction() and commit/abort
    Active,
    /// Sink is closed or in error state; no further barriers permitted
    Closed,
}

#[derive(Debug)]
struct BarrierStateMachine {
    state: BarrierState,
}

impl BarrierStateMachine {
    fn new() -> Self {
        Self {
            state: BarrierState::NotActive,
        }
    }

    #[cfg(test)]
    fn state(&self) -> BarrierState {
        self.state
    }

    fn ensure_can_begin(&self) -> rustcdc::core::Result<()> {
        if self.state != BarrierState::NotActive {
            return Err(RtError::StateError(format!(
                "cannot begin checkpoint barrier: state is {:?} (expected NotActive)",
                self.state
            )));
        }
        Ok(())
    }

    fn ensure_can_commit(&self) -> rustcdc::core::Result<()> {
        if self.state != BarrierState::Active {
            return Err(RtError::StateError(format!(
                "cannot commit checkpoint barrier: state is {:?} (expected Active)",
                self.state
            )));
        }
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.state == BarrierState::Active
    }

    fn mark_active(&mut self) {
        self.state = BarrierState::Active;
    }

    fn mark_not_active(&mut self) {
        self.state = BarrierState::NotActive;
    }

    fn mark_closed(&mut self) {
        self.state = BarrierState::Closed;
    }
}

pub struct KafkaSink {
    producer: KafkaProducerClient,
    topic: String,
    delivery_guarantee: SinkDeliveryGuarantee,
    /// Guards barrier state transitions (begin / commit / abort) and the
    /// barrier state machine itself.  Merging state into the lock makes the
    /// invariant structural: no code can inspect or modify the barrier state
    /// without holding this guard.  If future refactors introduce concurrent
    /// send/commit paths, the compiler enforces the serialization requirement
    /// rather than relying on call-site discipline.
    barrier: Mutex<BarrierStateMachine>,
    closed: bool,
    /// Preflight-check fields: stored at construction for AdminClient use.
    preflight_brokers: String,
    preflight_client_id: String,
    preflight_security: KafkaSecurityConfig,
    preflight_timeout: Duration,
}

impl KafkaSink {
    pub async fn new(config: &KafkaSinkConfig) -> rustcdc::core::Result<Self> {
        let brokers = config.normalized_brokers();
        if brokers.is_empty() {
            return Err(RtError::ConfigError(
                "sink.kafka.brokers must contain at least one broker".to_string(),
            ));
        }

        if config.topic.trim().is_empty() {
            return Err(RtError::ConfigError(
                "sink.kafka.topic must not be empty".to_string(),
            ));
        }

        if config.client_id.trim().is_empty() {
            return Err(RtError::ConfigError(
                "sink.kafka.client_id must not be empty".to_string(),
            ));
        }

        if config.ack_timeout_ms == 0 {
            return Err(RtError::ConfigError(
                "sink.kafka.ack_timeout_ms must be > 0".to_string(),
            ));
        }

        if config.retry_backoff_ms == 0 {
            return Err(RtError::ConfigError(
                "sink.kafka.retry_backoff_ms must be > 0".to_string(),
            ));
        }

        if config.retry_max_attempts == 0 {
            return Err(RtError::ConfigError(
                "sink.kafka.retry_max_attempts must be > 0".to_string(),
            ));
        }

        config.security.validate().map_err(RtError::ConfigError)?;

        let auth = config
            .security
            .to_auth_config()
            .map_err(RtError::ConfigError)?;

        let (producer, delivery_guarantee) = match config.delivery_mode {
            KafkaDeliveryMode::AtLeastOnceIdempotent => {
                let producer = Producer::builder()
                    .bootstrap_servers(brokers.join(","))
                    .client_id(config.client_id.clone())
                    .acks(Acks::All)
                    .idempotent(true)
                    .compression(
                        config
                            .compression
                            .to_krafka()
                            .map_err(RtError::ConfigError)?,
                    )
                    .retries(config.retry_max_attempts)
                    .retry_backoff(Duration::from_millis(config.retry_backoff_ms))
                    .request_timeout(Duration::from_millis(config.ack_timeout_ms))
                    .connect_timeout(kafka_connect_timeout(Duration::from_millis(
                        config.ack_timeout_ms,
                    )))
                    .delivery_timeout(Self::delivery_timeout(config))
                    .max_in_flight(5)
                    .auth(auth)
                    .build()
                    .await
                    .map_err(|e| {
                        RtError::ConfigError(format!(
                            "failed to build idempotent krafka producer: {e}"
                        ))
                    })?;
                (
                    KafkaProducerClient::Idempotent(producer),
                    SinkDeliveryGuarantee::AtLeastOnceIdempotent,
                )
            }
            KafkaDeliveryMode::Transactional => {
                let transactional_id = config
                    .transactional_id
                    .as_deref()
                    .ok_or_else(|| {
                        RtError::ConfigError(
                            "sink.kafka.transactional_id is required when sink.kafka.delivery_mode=\"transactional\""
                                .to_string(),
                        )
                    })?
                    .trim();

                if transactional_id.is_empty() {
                    return Err(RtError::ConfigError(
                        "sink.kafka.transactional_id must not be empty when sink.kafka.delivery_mode=\"transactional\""
                            .to_string(),
                    ));
                }

                let producer = TransactionalProducer::builder()
                    .bootstrap_servers(brokers.join(","))
                    .client_id(config.client_id.clone())
                    .transactional_id(transactional_id)
                    .transaction_timeout(Duration::from_millis(config.transaction_timeout_ms))
                    .request_timeout(Duration::from_millis(config.ack_timeout_ms))
                    .connect_timeout(kafka_connect_timeout(Duration::from_millis(
                        config.ack_timeout_ms,
                    )))
                    .compression(
                        config
                            .compression
                            .to_krafka()
                            .map_err(RtError::ConfigError)?,
                    )
                    .retries(config.retry_max_attempts)
                    .retry_backoff(Duration::from_millis(config.retry_backoff_ms))
                    .auth(auth)
                    .build()
                    .await
                    .map_err(|e| {
                        RtError::ConfigError(format!(
                            "failed to build transactional krafka producer: {e}"
                        ))
                    })?;

                producer.init_transactions().await.map_err(|e| {
                    RtError::SourceError(format!(
                        "failed to initialize transactional producer state: {e}"
                    ))
                })?;

                (
                    KafkaProducerClient::Transactional(producer),
                    SinkDeliveryGuarantee::EffectivelyOnce,
                )
            }
        };

        Ok(Self {
            producer,
            topic: config.topic.clone(),
            delivery_guarantee,
            barrier: Mutex::new(BarrierStateMachine::new()),
            closed: false,
            preflight_brokers: config.normalized_brokers().join(","),
            preflight_client_id: format!("{}-preflight", config.client_id),
            preflight_security: config.security.clone(),
            preflight_timeout: Duration::from_millis(config.ack_timeout_ms.max(5_000)),
        })
    }

    fn delivery_timeout(config: &KafkaSinkConfig) -> Duration {
        let retry_budget = config
            .retry_backoff_ms
            .saturating_mul(config.retry_max_attempts as u64);
        Duration::from_millis(
            config
                .ack_timeout_ms
                .saturating_add(retry_budget)
                .saturating_add(5_000),
        )
    }

    /// Verify that the configured Kafka topic exists and is reachable before
    /// the pipeline starts processing events.  Uses a short-lived AdminClient
    /// so the main producer is not involved.
    pub async fn preflight_check(&self) -> Result<(), AppError> {
        let auth = self
            .preflight_security
            .to_auth_config()
            .map_err(AppError::Other)?;

        let admin = AdminClient::builder()
            .bootstrap_servers(self.preflight_brokers.clone())
            .client_id(self.preflight_client_id.clone())
            .request_timeout(self.preflight_timeout)
            .connect_timeout(kafka_connect_timeout(self.preflight_timeout))
            .auth(auth)
            .build()
            .await
            .map_err(|e| {
                AppError::Other(format!(
                    "sink preflight: failed to connect to Kafka brokers '{}': {e}",
                    self.preflight_brokers
                ))
            })?;

        let topics = admin
            .describe_topics(std::slice::from_ref(&self.topic))
            .await
            .map_err(|e| {
                AppError::Other(format!(
                    "sink preflight: failed to describe Kafka topic '{}': {e}",
                    self.topic
                ))
            })?;

        if !topics.iter().any(|t| t.0 == self.topic.as_str()) {
            return Err(AppError::Other(format!(
                "sink preflight: Kafka topic '{}' not found on brokers '{}'",
                self.topic, self.preflight_brokers
            )));
        }

        tracing::info!(
            topic = %self.topic,
            brokers = %self.preflight_brokers,
            "sink preflight: Kafka topic reachable"
        );
        Ok(())
    }

    async fn send_payload_with_key(
        &mut self,
        key: &[u8],
        payload: &[u8],
    ) -> rustcdc::core::Result<()> {
        let metadata = match &mut self.producer {
            KafkaProducerClient::Idempotent(producer) => producer
                .send(&self.topic, Some(key), payload)
                .await
                .map_err(|e| RtError::SourceError(format!("Kafka sink delivery failed: {e}")))?,
            KafkaProducerClient::Transactional(producer) => producer
                .send(&self.topic, Some(key), payload)
                .await
                .map_err(|e| {
                    RtError::SourceError(format!("Kafka transactional sink delivery failed: {e}"))
                })?,
        };

        enforce_durable_confirmation(&metadata, "sink")
    }

    pub async fn send_encoded(&mut self, key: Bytes, value: Bytes) -> rustcdc::core::Result<()> {
        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }
        self.send_payload_with_key(&key, &value).await
    }

    pub async fn flush(&mut self) -> rustcdc::core::Result<()> {
        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }
        match &mut self.producer {
            KafkaProducerClient::Idempotent(producer) => producer
                .flush()
                .await
                .map_err(|e| RtError::SourceError(format!("Kafka sink flush failed: {e}"))),
            KafkaProducerClient::Transactional(_) => Ok(()),
        }
    }

    pub async fn close(&mut self) -> rustcdc::core::Result<()> {
        if self.closed {
            return Ok(());
        }
        match &self.producer {
            KafkaProducerClient::Idempotent(producer) => {
                producer
                    .close_with_timeout(Duration::from_secs(5))
                    .await
                    .map_err(|e| RtError::SourceError(format!("Kafka sink close failed: {e}")))?;
            }
            KafkaProducerClient::Transactional(producer) => {
                producer
                    .close_with_timeout(Duration::from_secs(5))
                    .await
                    .map_err(|e| {
                        RtError::SourceError(format!("Kafka transactional sink close failed: {e}"))
                    })?;
            }
        }
        self.barrier.lock().await.mark_closed();
        self.closed = true;
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn delivery_guarantee(&self) -> SinkDeliveryGuarantee {
        self.delivery_guarantee
    }

    pub fn transactional_checkpoint_barrier_capable(&self) -> bool {
        matches!(self.producer, KafkaProducerClient::Transactional(_))
    }

    pub async fn begin_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        let mut barrier = self.barrier.lock().await;

        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        match &self.producer {
            KafkaProducerClient::Idempotent(_) => Ok(()),
            KafkaProducerClient::Transactional(producer) => {
                barrier.ensure_can_begin()?;

                producer.begin_transaction().map_err(|e| {
                    RtError::SourceError(format!(
                        "failed to begin transactional checkpoint barrier: {e}"
                    ))
                })?;
                // Transition: NotActive → Active
                barrier.mark_active();
                Ok(())
            }
        }
    }

    pub async fn commit_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        let mut barrier = self.barrier.lock().await;

        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        match &self.producer {
            KafkaProducerClient::Idempotent(_) => Ok(()),
            KafkaProducerClient::Transactional(producer) => {
                barrier.ensure_can_commit()?;

                producer.commit_transaction().await.map_err(|e| {
                    RtError::SourceError(format!(
                        "failed to commit transactional checkpoint barrier: {e}"
                    ))
                })?;
                // Transition: Active → NotActive
                barrier.mark_not_active();
                Ok(())
            }
        }
    }

    pub async fn abort_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        let mut barrier = self.barrier.lock().await;

        if self.closed {
            return Err(RtError::StateError("sink is closed".to_string()));
        }

        match &self.producer {
            KafkaProducerClient::Idempotent(_) => Ok(()),
            KafkaProducerClient::Transactional(producer) => {
                // Only abort if we're actually Active; idempotent is safe to call redundantly
                if !barrier.is_active() {
                    // Already NotActive, no-op (safe to call multiple times)
                    return Ok(());
                }

                producer.abort_transaction().await.map_err(|e| {
                    RtError::SourceError(format!(
                        "failed to abort transactional checkpoint barrier: {e}"
                    ))
                })?;
                // Transition: Active → NotActive
                barrier.mark_not_active();
                Ok(())
            }
        }
    }
}

// ── Schema registry helpers ───────────────────────────────────────────────────

/// Cap the TCP connect timeout at the request timeout.
///
/// krafka ≥ 0.13 validates `request_timeout >= connect_timeout` at build time
/// (default connect timeout: 10 s). This server deliberately runs short request
/// budgets for local state and preflight operations; a connection that cannot
/// be established within the request budget is useless to that request anyway,
/// so the connect timeout follows the request timeout downward.
pub(crate) fn kafka_connect_timeout(request_timeout: Duration) -> Duration {
    request_timeout.min(Duration::from_secs(10))
}

/// Durability tripwire on the send acknowledgement (krafka ≥ 0.13).
///
/// `RecordMetadata::delivery` states what the broker actually confirmed —
/// `offset == -1` alone cannot distinguish "idempotent-deduplicated (durable)"
/// from "acks = 0 (no guarantee at all)". Every producer in this server is
/// built with `acks = All`, so an `Unacknowledged` confirmation can only mean a
/// misconfiguration slipped through — fail the send instead of silently
/// downgrading the delivery contract.
pub(crate) fn enforce_durable_confirmation(
    metadata: &krafka::producer::RecordMetadata,
    context: &str,
) -> rustcdc::core::Result<()> {
    if metadata.is_unacknowledged() {
        return Err(RtError::StateError(format!(
            "Kafka {context} send returned an unacknowledged delivery confirmation \
             (acks = 0 semantics); refusing to treat it as durable"
        )));
    }
    if metadata.is_deduplicated() {
        tracing::debug!(
            topic = %metadata.topic,
            partition = metadata.partition,
            "Kafka {context} send deduplicated by idempotent producer — data already durable"
        );
    }
    Ok(())
}

impl rustcdc::sink::SinkAdapter for KafkaSink {
    fn name(&self) -> &str {
        "kafka"
    }

    async fn send(&mut self, event: &rustcdc::Event) -> rustcdc::core::Result<()> {
        let value =
            serde_json::to_vec(event).map_err(|e| RtError::SerializationError(e.to_string()))?;
        self.send_encoded(Bytes::new(), Bytes::from(value)).await
    }

    async fn flush(&mut self) -> rustcdc::core::Result<()> {
        self.flush().await
    }

    async fn close(&mut self) -> rustcdc::core::Result<()> {
        self.close().await
    }

    fn delivery_guarantee(&self) -> rustcdc::sink::SinkDeliveryGuarantee {
        match self.delivery_guarantee() {
            SinkDeliveryGuarantee::EffectivelyOnce => {
                rustcdc::sink::SinkDeliveryGuarantee::EffectivelyOnce
            }
            _ => rustcdc::sink::SinkDeliveryGuarantee::AtLeastOnce,
        }
    }

    fn transactional_checkpoint_barrier_capable(&self) -> bool {
        self.transactional_checkpoint_barrier_capable()
    }

    async fn begin_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        self.begin_checkpoint_barrier().await
    }

    async fn commit_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        self.commit_checkpoint_barrier().await
    }

    async fn abort_checkpoint_barrier(&mut self) -> rustcdc::core::Result<()> {
        self.abort_checkpoint_barrier().await
    }
}

/// Derive a stable Avro record name from the event's table identifier.
/// Falls back to `"CdcEvent"` when no table name is present.
#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use super::{kafka_connect_timeout, BarrierState, BarrierStateMachine, KafkaSink};
    use crate::config::schema::{
        KafkaCompression, KafkaSecurityConfig, KafkaSecurityProtocol, KafkaSinkConfig,
    };
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use rustcdc::{
        fingerprint_event_stable, Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION,
    };
    use serde_json::json;
    use tokio::process::Command;
    use tokio::sync::{Barrier, Mutex};
    use tokio::time::{sleep, Duration};

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
        Event {
            before: None,
            after: Some(json!({"id": 42, "name": "bob"})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "postgres".to_string(),
                offset: "0/16B6A71".to_string(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".to_string()),
            table: "users".to_string(),
            primary_key: Some(vec!["id".to_string()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    fn sample_kafka_config(brokers: &str, topic: &str) -> KafkaSinkConfig {
        KafkaSinkConfig {
            brokers: brokers.to_string(),
            topic: topic.to_string(),
            client_id: "cdc-kafka-test".to_string(),
            ack_timeout_ms: 1_000,
            retry_backoff_ms: 100,
            retry_max_attempts: 3,
            compression: KafkaCompression::None,
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
                    .send_encoded(bytes::Bytes::new(), bytes::Bytes::from(json))
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
        cfg.security.ssl_ca_location =
            Some(std::path::PathBuf::from("/tmp/no-such-kafka-ca-cert.pem"));

        match KafkaSink::new(&cfg).await {
            Ok(_) => panic!("expected missing tls CA file to be rejected"),
            Err(err) => assert!(err
                .to_string()
                .contains("sink.kafka.security.ssl_ca_location")),
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
        sink.send_encoded(k, v)
            .await
            .expect("first send should succeed");
        let (k, v) = send(&second);
        sink.send_encoded(k, v)
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
        sink.send_encoded(bytes::Bytes::new(), bytes::Bytes::from(bl_json))
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
                .send_encoded(bytes::Bytes::new(), bytes::Bytes::from(ev_json))
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
            sink.send_encoded(bytes::Bytes::new(), bytes::Bytes::from(json))
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
            sink.send_encoded(bytes::Bytes::new(), bytes::Bytes::from(json))
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
        let mut binding =
            crate::sink::build_binding(&crate::config::schema::SinkConfig::Kafka(cfg))
                .await
                .expect("sink binding");

        let keyed = sample_event(); // primary_key = ["id"], after.id = 42
        let mut keyless = sample_event();
        keyless.primary_key = None;

        binding.send_event(&keyed).await.expect("send keyed");
        binding.send_event(&keyless).await.expect("send keyless");
        use rustcdc::sink::SinkAdapter as _;
        binding.flush().await.expect("flush");

        // No connect_timeout on this builder — use a request budget >= the
        // 10 s connect default (see load_records for the same workaround).
        let mut consumer = CompactedTopicConsumer::builder()
            .bootstrap_servers(broker.bootstrap_servers())
            .topic("cdc.fake.keys".to_string())
            .client_id("fake-key-check".to_string())
            .request_timeout(Duration::from_secs(10))
            .build()
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
}
