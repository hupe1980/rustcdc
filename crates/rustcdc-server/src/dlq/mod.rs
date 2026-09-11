//! Pipeline-level dead-letter queue.
//!
//! # Why it is not a sink concern
//!
//! Whether an event is undeliverable is decided by
//! [`AppError::is_recoverable`](crate::error::AppError) — a pipeline-level
//! classification; the sink only reports what happened. So the DLQ lives here and applies
//! to every sink. Without one, a poison event is a terminal error, a restart, a replay
//! and the same failure again: a crash loop with no forward progress. Targets are a
//! file, a Kafka topic or an SQS queue; a file on a container filesystem dies with the
//! pod, which is the wrong medium for an artefact you reach for during an incident.
//!
//! # Quarantining is opt-in
//!
//! With no `[dlq]` section, a non-recoverable delivery failure stays terminal. That is
//! the safe default: setting an event aside and advancing the checkpoint past it **is
//! data loss**, recorded but still loss. Halting and paging someone is the right answer
//! for a pipeline whose contents matter more than its uptime, so an operator has to ask
//! for the other trade. rustcdc's `TransformErrorPolicy::Skip` refuses to run without a
//! dead-letter handler for the same reason.

use std::path::PathBuf;

use serde::Serialize;

use crate::config::dlq::{DlqConfig, DlqTarget};
use crate::error::AppError;

mod file;
mod kafka;
mod sqs;

/// One quarantined event, plus everything needed to understand and replay it.
///
/// The payload is the event's JSON rather than the encoded sink payload: an operator
/// triaging a DLQ needs to read it, and `rustcdc replay` consumes JSONL. The encoded
/// form is reproducible from the event; the reverse is not true for a binary codec.
#[derive(Debug, Clone, Serialize)]
pub struct DeadLetterRecord {
    /// Unix milliseconds when the event was quarantined.
    pub ts_ms: u64,
    /// The sink that could not accept it.
    pub sink: String,
    /// Fully-qualified table, so a DLQ covering many tables can be filtered.
    pub table: String,
    /// The source position, so an operator can correlate with the upstream log and
    /// know exactly what was skipped.
    pub source_offset: String,
    /// Why it was undeliverable. The rendered error chain, not just the outer layer.
    pub error: String,
    /// The event itself, or `null` when it had to be dropped to fit a target's message
    /// limit — see [`DeadLetterRecord::without_payload`].
    pub event: serde_json::Value,
    /// Whether [`Self::event`] was dropped.
    ///
    /// Serialised always, not skipped when false: a consumer must be able to tell "no
    /// payload because it was too big" from "this field is absent in an older record",
    /// and a missing field cannot express the difference.
    pub payload_truncated: bool,
    /// Size of the full record, when it was truncated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_bytes: Option<usize>,
}

impl DeadLetterRecord {
    /// Build a record from the event and the failure that condemned it.
    pub fn new(sink: &str, event: &rustcdc::core::Event, error: &AppError) -> Self {
        Self {
            ts_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            sink: sink.to_string(),
            table: event.qualified_table_name(),
            source_offset: event.source.offset.clone(),
            error: error.to_string(),
            event: serde_json::to_value(event)
                .unwrap_or_else(|e| serde_json::json!({ "unserializable": e.to_string() })),
            payload_truncated: false,
            original_bytes: None,
        }
    }

    /// The same record with its payload dropped, for a target that cannot carry it.
    ///
    /// Everything an operator acts on survives — the source offset above all, because that
    /// is what makes a manual replay possible and the row still exists upstream. The
    /// payload is set to `null` rather than to a partial value: a truncated JSON object is
    /// something a consumer might read and trust.
    pub fn without_payload(&self, original_bytes: usize) -> Self {
        Self {
            event: serde_json::Value::Null,
            payload_truncated: true,
            original_bytes: Some(original_bytes),
            ..self.clone()
        }
    }

    fn to_line(&self) -> Result<String, AppError> {
        serde_json::to_string(self)
            .map_err(|e| AppError::Other(format!("failed to serialize dead-letter record: {e}")))
    }
}

/// Where quarantined events go.
pub enum DeadLetterQueue {
    File(file::FileDlq),
    Kafka(Box<kafka::KafkaDlq>),
    Sqs(Box<sqs::SqsDlq>),
}

impl DeadLetterQueue {
    /// Build the configured target, or `None` when quarantining is not enabled.
    pub async fn build(config: &DlqConfig) -> Result<Option<Self>, AppError> {
        if !config.enabled {
            return Ok(None);
        }

        match &config.target {
            DlqTarget::File(file_config) => Ok(Some(Self::File(file::FileDlq::new(
                PathBuf::from(&file_config.path),
                file_config.max_bytes,
            )))),
            DlqTarget::Kafka(kafka_config) => Ok(Some(Self::Kafka(Box::new(
                kafka::KafkaDlq::new(kafka_config).await?,
            )))),
            DlqTarget::Sqs(sqs_config) => Ok(Some(Self::Sqs(Box::new(
                sqs::SqsDlq::new(sqs_config).await?,
            )))),
        }
    }

    /// Quarantine one event.
    ///
    /// An error here is **not** swallowed. If the DLQ itself cannot accept the record
    /// the pipeline must stop: continuing would advance the checkpoint past an event
    /// that was neither delivered nor recorded anywhere, which is silent data loss of
    /// precisely the kind the DLQ exists to make visible.
    pub async fn write(&mut self, record: &DeadLetterRecord) -> Result<(), AppError> {
        match self {
            Self::File(sink) => sink.write(record).await,
            Self::Kafka(sink) => sink.write(record).await,
            Self::Sqs(sink) => sink.write(record).await,
        }
    }

    /// A short label for logs and metric context.
    pub fn target_name(&self) -> &'static str {
        match self {
            Self::File(_) => "file",
            Self::Kafka(_) => "kafka",
            Self::Sqs(_) => "sqs",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustcdc::core::{Event, Operation, SourceMetadata};

    fn sample_event() -> Event {
        Event::builder("orders", Operation::Insert)
            .after(serde_json::json!({ "id": 7, "secret_note": "x" }))
            .source(SourceMetadata::new("postgres", "0/16B6A70", 1))
            .ts(1)
            .schema("public")
            .primary_key(["id"])
            .build()
    }

    /// A quarantined record must carry enough to act on without the original logs.
    ///
    /// The source offset is the field that matters most and is easiest to forget: it is
    /// what tells an operator exactly which upstream position was skipped, and it is
    /// what makes a manual replay possible at all.
    #[test]
    fn a_record_carries_the_context_needed_to_triage_and_replay() {
        let record = DeadLetterRecord::new(
            "kafka",
            &sample_event(),
            &AppError::Other("encoded event payload size 900 exceeds limit".to_string()),
        );

        assert_eq!(record.sink, "kafka");
        assert_eq!(record.table, "public.orders");
        assert_eq!(
            record.source_offset, "0/16B6A70",
            "without the source offset an operator cannot tell what was skipped"
        );
        assert!(record.error.contains("exceeds limit"));
        assert_eq!(record.event["after"]["id"], 7);

        let line = record.to_line().expect("serialisable");
        assert!(!line.contains('\n'), "a JSONL record must be one line");
    }
}
