//! Pipeline-level dead-letter queue.
//!
//! # Why this is not a sink concern
//!
//! A dead-letter queue used to exist for exactly one sink — HTTP — and only as a local
//! file. Every other sink had nowhere to put an event it could never deliver, so a
//! poison event on the Kafka path produced a terminal error, a restart, a replay, and
//! the same event failing again: a crash-loop with no quarantine and no forward
//! progress. Debezium's baseline is a dead-letter topic available to *any* connector.
//!
//! Quarantining is also not really a transport decision. Whether an event is
//! undeliverable is decided by [`AppError::is_recoverable`](crate::error::AppError),
//! which is a pipeline-level classification; the sink only reports what happened. So
//! the DLQ lives here, applies to every sink, and can target a file *or* a Kafka topic
//! — the latter because a local file on a container filesystem is lost with the pod,
//! which makes it the wrong medium for the artefact you reach for during an incident.
//!
//! # Quarantining is opt-in, and deliberately so
//!
//! With no `[dlq]` section configured, a non-recoverable delivery failure is terminal —
//! exactly as before. That is the safe default: writing an event aside and advancing
//! the checkpoint past it **is data loss**, correctly recorded but still loss. An
//! operator has to ask for that trade, because the alternative (halt and page someone)
//! is the right answer for a pipeline whose contents matter more than its uptime.
//!
//! This mirrors the reasoning rustcdc applies to `TransformErrorPolicy::Skip`, which
//! refuses to run without a dead-letter handler for the same reason.

use std::path::PathBuf;

use serde::Serialize;

use crate::config::dlq::{DlqConfig, DlqTarget};
use crate::error::AppError;

mod file;
mod kafka;

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
    /// The event itself.
    pub event: serde_json::Value,
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
        }
    }

    /// A short label for logs and metric context.
    pub fn target_name(&self) -> &'static str {
        match self {
            Self::File(_) => "file",
            Self::Kafka(_) => "kafka",
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
