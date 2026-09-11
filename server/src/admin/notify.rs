//! Kafka fan-out for signal lifecycle notifications.
//!
//! Split out of `admin/mod.rs` alongside `prometheus.rs` when the file-size guard fired.
//! This is a self-contained adapter: it owns one producer, one topic, and the durability
//! rule that an emitted notification must be acknowledged before it counts as emitted.

use super::*;

pub(super) struct KafkaNotificationPublisher {
    topic: String,
    producer: Mutex<Producer>,
}

impl KafkaNotificationPublisher {
    pub(super) async fn new(config: &AdminNotificationKafkaConfig) -> Result<Self, AppError> {
        let auth = config.security.to_auth_config().map_err(|err| {
            AppError::Other(format!(
                "invalid admin.notification_kafka security configuration: {err}"
            ))
        })?;
        let compression = config.compression.to_krafka().map_err(|err| {
            AppError::Other(format!(
                "invalid admin.notification_kafka compression configuration: {err}"
            ))
        })?;
        let producer = Producer::builder()
            .bootstrap_servers(config.normalized_brokers().join(","))
            .client_id(config.client_id.clone())
            .acks(Acks::All)
            .idempotent(true)
            .compression(compression)
            .retries(config.retry_max_attempts)
            .retry_backoff(Duration::from_millis(config.retry_backoff_ms))
            .request_timeout(Duration::from_millis(config.ack_timeout_ms))
            .connect_timeout(crate::sink::kafka_connect_timeout(Duration::from_millis(
                config.ack_timeout_ms,
            )))
            .delivery_timeout(Duration::from_millis(
                config.ack_timeout_ms.saturating_add(
                    config
                        .retry_backoff_ms
                        .saturating_mul(config.retry_max_attempts as u64),
                ),
            ))
            .auth(auth)
            .build()
            .await
            .map_err(|err| {
                AppError::Other(format!(
                    "failed to build admin.notification_kafka producer: {err}"
                ))
            })?;

        Ok(Self {
            topic: config.topic.clone(),
            producer: Mutex::new(producer),
        })
    }

    pub(super) async fn send_event(&self, event: &serde_json::Value) -> Result<(), AppError> {
        let payload = serde_json::to_vec(event).map_err(|err| {
            AppError::Other(format!("failed to serialize notification event: {err}"))
        })?;
        let key = event
            .get("signalid")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("notification")
            .as_bytes()
            .to_vec();

        let record = ProducerRecord::new(self.topic.clone(), payload).with_key(key);
        let producer = self.producer.lock().await;
        let metadata = producer.send_record(record).await.map_err(|err| {
            AppError::Other(format!(
                "failed to emit notification event to admin.notification_kafka topic {}: {err}",
                self.topic
            ))
        })?;
        crate::sink::enforce_durable_confirmation(&metadata, "notification")
            .map_err(|e| AppError::Other(e.to_string()))?;
        producer.flush().await.map_err(|err| {
            AppError::Other(format!(
                "failed to flush admin.notification_kafka topic {}: {err}",
                self.topic
            ))
        })?;

        Ok(())
    }
}

pub(super) async fn build_kafka_notification_publisher(
    config: &AdminNotificationKafkaConfig,
) -> Result<Arc<KafkaNotificationPublisher>, AppError> {
    KafkaNotificationPublisher::new(config).await.map(Arc::new)
}
