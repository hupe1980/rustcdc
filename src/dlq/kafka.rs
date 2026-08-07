//! Kafka dead-letter topic.
//!
//! This is the target Debezium's baseline uses, and it is the right one for a
//! containerised deployment: a local file lives and dies with the pod, which makes it
//! the wrong medium for the artefact you reach for during an incident.

use krafka::producer::{Acks, Producer};

use super::DeadLetterRecord;
use crate::config::dlq::KafkaDlqConfig;
use crate::error::AppError;

pub struct KafkaDlq {
    producer: Producer,
    topic: String,
}

impl KafkaDlq {
    pub async fn new(config: &KafkaDlqConfig) -> Result<Self, AppError> {
        let auth = config.security.to_auth_config().map_err(AppError::Other)?;

        let producer = Producer::builder()
            .bootstrap_servers(config.brokers.clone())
            .client_id(config.client_id.clone())
            // `acks=all` + idempotent: a dead-letter record that is not durable is the
            // same as no dead-letter record, and this write happens immediately before
            // the checkpoint advances past the event it describes.
            .acks(Acks::All)
            .idempotent(true)
            .request_timeout(std::time::Duration::from_millis(config.request_timeout_ms))
            .connect_timeout(crate::sink::kafka_connect_timeout(
                std::time::Duration::from_millis(config.request_timeout_ms),
            ))
            .auth(auth)
            .build()
            .await
            .map_err(|e| AppError::Other(format!("failed to build dead-letter producer: {e}")))?;

        Ok(Self {
            producer,
            topic: config.topic.clone(),
        })
    }

    pub async fn write(&mut self, record: &DeadLetterRecord) -> Result<(), AppError> {
        let payload = record.to_line()?;

        // Keyed by table so a compacted or partitioned dead-letter topic keeps one
        // table's failures together and in order — which is how they get triaged.
        let metadata = self
            .producer
            .send(
                &self.topic,
                Some(record.table.as_bytes()),
                payload.as_bytes(),
            )
            .await
            .map_err(|e| {
                AppError::Other(format!(
                    "failed writing dead-letter record to topic '{}': {e}",
                    self.topic
                ))
            })?;

        // Same durability bar as the state topic: an unacknowledged write here would let
        // the checkpoint advance past an event whose only remaining record was never
        // stored.
        crate::sink::enforce_durable_confirmation(&metadata, "dlq")
            .map_err(|e| AppError::Other(e.to_string()))?;

        Ok(())
    }
}
