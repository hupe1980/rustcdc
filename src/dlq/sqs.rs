//! Amazon SQS dead-letter target.
//!
//! See [`DlqTarget::Sqs`](crate::config::dlq::DlqTarget::Sqs) for why SQS is the right shape
//! for a dead-letter queue and the wrong shape for a sink.
//!
//! # The one hard trade: 256 KB
//!
//! SQS refuses a message body above 256 KiB, and "too large for the sink" is one of the
//! commonest reasons an event is quarantined in the first place — so this limit will be met
//! by exactly the records that most need to be recorded.
//!
//! Refusing the write is not the answer. A DLQ write failure is fatal by design (continuing
//! would advance the checkpoint past an event that was neither delivered nor recorded), so
//! refusing would turn one oversized event into a permanent crash loop — the failure the
//! dead-letter queue exists to prevent.
//!
//! So an oversized record has its **payload** truncated while every field an operator acts
//! on is kept: the source offset, the table, the sink, the error and the original size. The
//! source offset is what makes a manual replay possible, and it is the field that matters;
//! the payload is a convenience copy of a row that still exists upstream. The record says
//! so explicitly — `payload_truncated: true` — and the truncation is logged at WARN and
//! counted, because a silent truncation would be exactly the kind of quiet loss this whole
//! subsystem is built to refuse.

use aws_sdk_sqs::Client;

use crate::config::dlq::{SQS_MAX_MESSAGE_BYTES, SqsDlqConfig};
use crate::dlq::DeadLetterRecord;
use crate::error::AppError;

pub struct SqsDlq {
    client: Client,
    queue_url: String,
    /// FIFO queues need a `MessageGroupId`; standard queues reject one.
    fifo: Option<String>,
    truncated_records: u64,
}

impl SqsDlq {
    pub async fn new(config: &SqsDlqConfig) -> Result<Self, AppError> {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = &config.region {
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        let aws = loader.load().await;

        // Named explicitly rather than left to the SDK's own error, which reports a missing
        // region as a signing failure at send time — during an incident.
        if aws.region().is_none() {
            return Err(AppError::Other(
                "no AWS region for the SQS dead-letter queue: set `dlq.region`, AWS_REGION, \
                 or a profile region"
                    .to_string(),
            ));
        }

        let queue_url = config.queue_url.trim().to_string();
        let fifo = queue_url
            .ends_with(".fifo")
            .then(|| config.message_group_id.trim().to_string());

        Ok(Self {
            client: Client::new(&aws),
            queue_url,
            fifo,
            truncated_records: 0,
        })
    }

    /// Records whose payload was dropped to fit SQS's message limit.
    pub fn truncated_records(&self) -> u64 {
        self.truncated_records
    }

    pub async fn write(&mut self, record: &DeadLetterRecord) -> Result<(), AppError> {
        let body = self.encode_within_limit(record)?;

        let mut send = self
            .client
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(body);

        if let Some(group) = &self.fifo {
            send = send
                .message_group_id(group)
                // The source offset is a natural idempotency key: it identifies the event
                // exactly, and a retried DLQ write for the same event carries the same one.
                // SQS's window is only five minutes, which is far longer than a retry loop
                // and far shorter than anything else — so this deduplicates a retry without
                // pretending to be a durable exactly-once mechanism.
                .message_deduplication_id(deduplication_id(record));
        }

        send.send().await.map_err(|e| {
            AppError::Other(format!(
                "failed to write a dead-letter record to SQS queue '{}': {}",
                self.queue_url,
                aws_sdk_sqs::error::DisplayErrorContext(&e)
            ))
        })?;
        Ok(())
    }

    /// Serialise the record, truncating the event payload if the result would be refused.
    fn encode_within_limit(&mut self, record: &DeadLetterRecord) -> Result<String, AppError> {
        let full = record.to_line()?;
        if full.len() <= SQS_MAX_MESSAGE_BYTES {
            return Ok(full);
        }

        let truncated = record.without_payload(full.len());
        let line = truncated.to_line()?;

        self.truncated_records += 1;
        tracing::warn!(
            target: "rustcdc_audit",
            action = "dead_letter_truncated",
            table = %record.table,
            source_offset = %record.source_offset,
            original_bytes = full.len(),
            limit_bytes = SQS_MAX_MESSAGE_BYTES,
            "a quarantined event exceeded SQS's message limit; its payload was dropped and \
             the source offset kept. Replay from that offset — the row is still upstream. \
             Use a file or Kafka dead-letter target if the payload copy matters."
        );

        // Even the metadata-only form can in principle exceed the limit, if the error string
        // is enormous. Better to refuse loudly than to keep chopping fields off a record
        // until it means nothing.
        if line.len() > SQS_MAX_MESSAGE_BYTES {
            return Err(AppError::Other(format!(
                "a dead-letter record exceeds SQS's {SQS_MAX_MESSAGE_BYTES}-byte limit even \
                 with its payload removed ({} bytes); the error text alone is too large. Use \
                 a file or Kafka dead-letter target.",
                line.len()
            )));
        }
        Ok(line)
    }
}

/// A stable identity for one quarantined event.
///
/// `MessageDeduplicationId` is limited to 128 characters, and a source offset plus a table
/// name can exceed that — so it is hashed. Collisions would suppress a distinct record, so
/// this is SHA-256 rather than anything cheaper.
fn deduplication_id(record: &DeadLetterRecord) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(record.table.as_bytes());
    hasher.update([0]);
    hasher.update(record.source_offset.as_bytes());
    hasher.update([0]);
    hasher.update(record.sink.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::deduplication_id;
    use crate::dlq::DeadLetterRecord;
    use crate::error::AppError;
    use rustcdc::core::{Event, Operation, SourceMetadata};

    fn record(offset: &str, payload_bytes: usize) -> DeadLetterRecord {
        let event = Event::builder("orders", Operation::Insert)
            .after(serde_json::json!({ "id": 1, "blob": "x".repeat(payload_bytes) }))
            .source(SourceMetadata::new("postgres", offset, 1))
            .ts(1)
            .schema("public")
            .primary_key(["id"])
            .build();
        DeadLetterRecord::new("kafka", &event, &AppError::Other("too large".to_string()))
    }

    /// The deduplication id must identify the *event*, not the moment it was written —
    /// otherwise a retried DLQ write enqueues the same quarantined event twice.
    #[test]
    fn the_deduplication_id_is_stable_for_one_event_and_differs_between_events() {
        let first = record("0/100", 8);
        let second = record("0/100", 8);
        assert_eq!(
            deduplication_id(&first),
            deduplication_id(&second),
            "the same event must produce the same id even though `ts_ms` differs"
        );
        assert_ne!(
            deduplication_id(&first),
            deduplication_id(&record("0/200", 8))
        );
        // SQS caps the id at 128 characters; a hex SHA-256 is 64.
        assert_eq!(deduplication_id(&first).len(), 64);
    }

    /// Truncation must keep everything an operator acts on.
    ///
    /// The source offset is the field that makes a manual replay possible; the payload is a
    /// convenience copy of a row that still exists upstream.
    #[test]
    fn a_truncated_record_keeps_the_fields_that_make_it_actionable() {
        let original = record("0/16B6A70", 64);
        let truncated = original.without_payload(900_000);

        assert_eq!(truncated.source_offset, "0/16B6A70");
        assert_eq!(truncated.table, "public.orders");
        assert_eq!(truncated.sink, "kafka");
        assert_eq!(truncated.error, original.error);
        assert!(truncated.payload_truncated);
        assert_eq!(truncated.original_bytes, Some(900_000));
        assert!(
            truncated.event.is_null(),
            "the payload is what gets dropped, and it must be visibly absent rather than \
             a partial value someone might trust"
        );
    }
}
