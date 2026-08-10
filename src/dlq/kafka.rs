//! Kafka dead-letter topic.
//!
//! This is the target Debezium's baseline uses, and it is the right one for a
//! containerised deployment: a local file lives and dies with the pod, which makes it
//! the wrong medium for the artefact you reach for during an incident.

use bytes::Bytes;
use krafka::producer::{Acks, Producer};

use super::DeadLetterRecord;
use crate::config::dlq::KafkaDlqConfig;
use crate::error::AppError;

/// The source table the quarantined event came from, `"schema.table"`.
pub const HEADER_SOURCE_TABLE: &str = "__rustcdc.dlq.source.table";
/// The source log position the event carried, for correlating with the upstream log.
pub const HEADER_SOURCE_OFFSET: &str = "__rustcdc.dlq.source.offset";
/// The sink that could not accept the event.
pub const HEADER_SINK: &str = "__rustcdc.dlq.sink";
/// The rendered error chain that condemned the event.
pub const HEADER_EXCEPTION_MESSAGE: &str = "__rustcdc.dlq.exception.message";

/// Headers carrying the triage fields, alongside the JSON body that carries everything.
///
/// The body already holds all of this, so the headers are not new information — they are
/// what makes the information *reachable*. A dead-letter topic is read during an incident,
/// usually with `kafka-console-consumer` or a filtering consumer, and without headers
/// every question ("which table is failing?", "is this all one error?") means parsing
/// every record's payload. Header values are the answer to exactly those questions.
///
/// The `__rustcdc.dlq.*` namespace mirrors krafka's own `__krafka.dlq.*` convention
/// without borrowing its names: krafka's headers describe a Kafka record that failed to
/// produce, and these describe a *change event* that no sink would accept. The source
/// table is not a Kafka topic, and conflating the two would mislead anyone who built a
/// consumer against the documented krafka contract.
///
/// The exception message is truncated: a rendered error chain can be arbitrarily long, and
/// a broker rejecting the whole record for an oversized header would lose the dead letter
/// entirely — which is the one outcome a dead-letter queue exists to prevent. The full
/// text is always in the body.
fn dead_letter_headers(record: &DeadLetterRecord) -> Vec<(String, Bytes)> {
    /// Generous enough for any realistic error chain, small enough that the headers
    /// cannot approach a default `message.max.bytes`.
    const MAX_ERROR_HEADER_BYTES: usize = 2048;

    // Through `crate::text` so the byte budget can never split a character: a header value
    // that is not valid UTF-8 is unreadable by every consumer that assumes text, which is
    // every consumer of a diagnostic header.
    let error = crate::text::truncate_utf8(&record.error, MAX_ERROR_HEADER_BYTES, "… [truncated]");

    vec![
        (
            HEADER_SOURCE_TABLE.to_string(),
            Bytes::from(record.table.clone()),
        ),
        (
            HEADER_SOURCE_OFFSET.to_string(),
            Bytes::from(record.source_offset.clone()),
        ),
        (HEADER_SINK.to_string(), Bytes::from(record.sink.clone())),
        (
            HEADER_EXCEPTION_MESSAGE.to_string(),
            Bytes::from(error.into_owned()),
        ),
    ]
}

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
            .send_with_headers(
                &self.topic,
                Some(record.table.as_bytes()),
                payload.as_bytes(),
                dead_letter_headers(record),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use rustcdc::core::{Event, Operation, SourceMetadata};

    fn record(error: &str) -> DeadLetterRecord {
        let event = Event::builder("orders", Operation::Insert)
            .after(serde_json::json!({ "id": 7 }))
            .source(SourceMetadata::new("postgres", "0/16B6A70", 1))
            .ts(1)
            .schema("public")
            .primary_key(["id"])
            .build();
        DeadLetterRecord::new("kafka", &event, &AppError::Other(error.to_string()))
    }

    fn header<'a>(headers: &'a [(String, Bytes)], name: &str) -> &'a str {
        let value = headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_ref())
            .unwrap_or_else(|| panic!("header {name} must be present"));
        std::str::from_utf8(value).expect("header values must be valid UTF-8")
    }

    /// The triage fields are readable without parsing the payload.
    ///
    /// A dead-letter topic is consumed during an incident. Without headers, "which table
    /// is failing?" requires deserialising every record.
    #[test]
    fn a_dead_letter_carries_its_triage_fields_as_headers() {
        let headers = dead_letter_headers(&record("payload size 900 exceeds limit"));

        assert_eq!(header(&headers, HEADER_SOURCE_TABLE), "public.orders");
        assert_eq!(header(&headers, HEADER_SOURCE_OFFSET), "0/16B6A70");
        assert_eq!(header(&headers, HEADER_SINK), "kafka");
        assert!(header(&headers, HEADER_EXCEPTION_MESSAGE).contains("exceeds limit"));
    }

    /// An unbounded error chain must not cost the dead letter its delivery.
    ///
    /// A broker refusing an oversized record would drop the quarantined event entirely,
    /// which is the single outcome this whole subsystem exists to prevent. The body still
    /// carries the untruncated text.
    #[test]
    fn an_oversized_error_is_truncated_rather_than_risking_the_record() {
        let headers = dead_letter_headers(&record(&"e".repeat(64 * 1024)));
        let rendered = header(&headers, HEADER_EXCEPTION_MESSAGE);

        assert!(
            rendered.len() < 4096,
            "the header must be bounded, got {} bytes",
            rendered.len()
        );
        assert!(rendered.ends_with("[truncated]"));
    }

    /// Truncation must not slice a character in half.
    ///
    /// A header value that is not valid UTF-8 is unreadable by every consumer that assumes
    /// text — which is every consumer of a diagnostic header.
    #[test]
    fn truncation_respects_character_boundaries() {
        // Three-byte characters, so a naive byte slice at the limit lands mid-character.
        let headers = dead_letter_headers(&record(&"世".repeat(4096)));
        let value = headers
            .iter()
            .find(|(key, _)| key == HEADER_EXCEPTION_MESSAGE)
            .map(|(_, value)| value.clone())
            .expect("exception header");

        std::str::from_utf8(value.as_ref()).expect("a truncated header must still be valid UTF-8");
    }
}
