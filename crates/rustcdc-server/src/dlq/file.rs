//! JSONL dead-letter file.

use std::io::Write;
use std::path::PathBuf;

use super::DeadLetterRecord;
use crate::error::AppError;

/// Append-only JSONL, size-capped and fsynced.
pub struct FileDlq {
    path: PathBuf,
    max_bytes: u64,
}

impl FileDlq {
    pub fn new(path: PathBuf, max_bytes: u64) -> Self {
        Self { path, max_bytes }
    }

    pub async fn write(&mut self, record: &DeadLetterRecord) -> Result<(), AppError> {
        let line = record.to_line()?;
        let path = self.path.clone();
        let max_bytes = self.max_bytes;

        tokio::task::spawn_blocking(move || -> Result<(), AppError> {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;

            // Refuse rather than grow without bound. A DLQ that fills the volume takes
            // the pipeline down with it *and* takes the state directory with it if they
            // share a mount — turning a data-quality problem into an outage.
            let current = file.metadata()?.len();
            let projected = current.saturating_add(line.len() as u64 + 1);
            if projected > max_bytes {
                return Err(AppError::Other(format!(
                    "dead-letter file {} would exceed dlq.file.max_bytes ({projected} > \
                     {max_bytes}). Drain and truncate it, or raise the limit. The \
                     pipeline stops rather than silently discarding an event it has \
                     already decided it cannot deliver.",
                    path.display()
                )));
            }

            writeln!(file, "{line}")?;
            // fsync. This is the record of events that were permanently dropped — the
            // only artefact that makes the loss recoverable by hand. Without it, it is
            // the least durable write in the system, while the checkpoint that skipped
            // past the event is already fsynced.
            file.sync_data()?;
            Ok(())
        })
        .await
        .map_err(|e| AppError::Other(format!("dead-letter write task join error: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustcdc::core::{Event, Operation, SourceMetadata};

    fn record(id: u64) -> DeadLetterRecord {
        let event = Event::builder("orders", Operation::Insert)
            .after(serde_json::json!({ "id": id }))
            .source(SourceMetadata::new("postgres", id.to_string(), id))
            .ts(id)
            .primary_key(["id"])
            .build();
        DeadLetterRecord::new("kafka", &event, &AppError::Other("poison".to_string()))
    }

    #[tokio::test]
    async fn records_are_appended_as_one_json_object_per_line() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("dlq.jsonl");
        let mut dlq = FileDlq::new(path.clone(), 1 << 20);

        dlq.write(&record(1)).await.expect("first");
        dlq.write(&record(2)).await.expect("second");

        let contents = std::fs::read_to_string(&path).expect("dlq file");
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2, "one line per quarantined event");
        for line in lines {
            let parsed: serde_json::Value = serde_json::from_str(line).expect("valid JSONL");
            assert_eq!(parsed["sink"], "kafka");
        }
    }

    /// Hitting the cap must fail loudly, not drop the record.
    ///
    /// Silently discarding here would be the worst outcome available: the event is
    /// already known-undeliverable, so the DLQ entry is the *only* remaining evidence
    /// it existed, and the checkpoint is about to advance past it.
    #[tokio::test]
    async fn exceeding_the_size_cap_is_an_error_not_a_silent_drop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tiny.jsonl");
        let mut dlq = FileDlq::new(path.clone(), 64);

        let err = dlq
            .write(&record(1))
            .await
            .expect_err("a record larger than the cap must be refused");
        assert!(
            err.to_string().contains("dlq.file.max_bytes"),
            "the error must name the setting to change: {err}"
        );
    }
}
