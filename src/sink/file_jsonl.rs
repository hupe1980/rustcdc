//! [`FileJsonlSink`] — appends newline-delimited JSON to a file.

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use crate::core::{Error, Event, Result};
use crate::sink::SinkAdapter;

// ─── FileJsonlSink ────────────────────────────────────────────────────────────

/// Appends CDC events to a file as newline-delimited JSON (NDJSON / JSON Lines).
///
/// The file is opened in **append** mode when the sink is created, so multiple
/// processes or sink restarts append safely (POSIX `O_APPEND` semantics).
/// Internal writes are buffered with [`BufWriter`]; call [`flush`](SinkAdapter::flush)
/// to push buffered bytes to the OS page cache.
///
/// [`close`](SinkAdapter::close) flushes and syncs the file to durable storage via
/// `File::sync_all` before marking the sink as closed.
///
/// # When to use
///
/// - Audit trails / change logs that must survive process restarts.
/// - Offline analysis pipelines (`jq`, `DuckDB`, etc.).
/// - Integration tests that need durable event records.
///
/// # Not suitable for
///
/// Very high-throughput production workloads. For Kafka / cloud-storage pipelines,
/// implement [`SinkAdapter`] directly against your target system.
///
/// # Example
///
/// ```rust,no_run
/// use rustcdc::sink::FileJsonlSink;
/// use rustcdc::sink::SinkAdapter;
/// use rustcdc::{Event, Operation, EVENT_ENVELOPE_VERSION};
///
/// # #[tokio::main]
/// # async fn main() -> rustcdc::core::Result<()> {
/// let mut sink = FileJsonlSink::open("/tmp/events.jsonl")?;
/// let event = Event { op: Operation::Insert, ..Event::default() };
/// sink.send(&event).await?;
/// sink.flush().await?;
/// sink.close().await?;
/// # Ok(())
/// # }
/// ```
pub struct FileJsonlSink {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    events_sent: u64,
}

impl std::fmt::Debug for FileJsonlSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileJsonlSink")
            .field("path", &self.path)
            .field("closed", &self.writer.is_none())
            .field("events_sent", &self.events_sent)
            .finish()
    }
}

impl FileJsonlSink {
    /// Open (or create) a file at `path` in append mode.
    ///
    /// Returns an error if the file cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| {
                Error::StateError(format!("FileJsonlSink open '{}': {e}", path.display()))
            })?;
        Ok(Self {
            path,
            writer: Some(BufWriter::new(file)),
            events_sent: 0,
        })
    }

    /// The path this sink writes to.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Total number of events sent through this sink (not reset on flush).
    pub fn events_sent(&self) -> u64 {
        self.events_sent
    }
}

impl SinkAdapter for FileJsonlSink {
    async fn send(&mut self, event: &Event) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::StateError("FileJsonlSink is closed".into()))?;
        let mut bytes = serde_json::to_vec(event)?;
        bytes.push(b'\n');
        writer
            .write_all(&bytes)
            .map_err(|e| Error::StateError(format!("FileJsonlSink write: {e}")))?;
        self.events_sent += 1;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::StateError("FileJsonlSink is closed".into()))?;
        writer
            .flush()
            .map_err(|e| Error::StateError(format!("FileJsonlSink flush: {e}")))?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(mut writer) = self.writer.take() {
            // Flush the BufWriter buffer first.
            writer
                .flush()
                .map_err(|e| Error::StateError(format!("FileJsonlSink close/flush: {e}")))?;
            // Sync to durable storage.
            writer
                .into_inner()
                .map_err(|e| Error::StateError(format!("FileJsonlSink close/into_inner: {e}")))?
                .sync_all()
                .map_err(|e| Error::StateError(format!("FileJsonlSink close/sync_all: {e}")))?;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "file-jsonl"
    }

    fn is_closed(&self) -> Option<bool> {
        Some(self.writer.is_none())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufRead;
    use tempfile::NamedTempFile;

    fn make_event(table: &str) -> Event {
        use crate::core::{Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
        Event {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "0".into(),
                timestamp: 0,
            },
            ts: 0,
            schema: None,
            table: table.into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
        }
    }

    fn count_lines(path: &Path) -> usize {
        let f = File::open(path).unwrap();
        std::io::BufReader::new(f).lines().count()
    }

    #[tokio::test]
    async fn writes_events_as_json_lines() {
        let tmp = NamedTempFile::new().unwrap();
        let mut sink = FileJsonlSink::open(tmp.path()).unwrap();

        sink.send(&make_event("orders")).await.unwrap();
        sink.send(&make_event("products")).await.unwrap();
        sink.flush().await.unwrap();
        sink.close().await.unwrap();

        assert_eq!(count_lines(tmp.path()), 2);
    }

    #[tokio::test]
    async fn appends_across_opens() {
        let tmp = NamedTempFile::new().unwrap();

        // First open: write 1 event.
        {
            let mut sink = FileJsonlSink::open(tmp.path()).unwrap();
            sink.send(&make_event("orders")).await.unwrap();
            sink.close().await.unwrap();
        }

        // Second open: append 1 more event.
        {
            let mut sink = FileJsonlSink::open(tmp.path()).unwrap();
            sink.send(&make_event("products")).await.unwrap();
            sink.close().await.unwrap();
        }

        assert_eq!(count_lines(tmp.path()), 2);
    }

    #[tokio::test]
    async fn send_after_close_errors() {
        let tmp = NamedTempFile::new().unwrap();
        let mut sink = FileJsonlSink::open(tmp.path()).unwrap();
        sink.close().await.unwrap();
        assert!(sink.send(&make_event("orders")).await.is_err());
    }

    #[tokio::test]
    async fn flush_after_close_errors() {
        let tmp = NamedTempFile::new().unwrap();
        let mut sink = FileJsonlSink::open(tmp.path()).unwrap();
        sink.close().await.unwrap();
        assert!(sink.flush().await.is_err());
    }

    #[tokio::test]
    async fn events_sent_increments() {
        let tmp = NamedTempFile::new().unwrap();
        let mut sink = FileJsonlSink::open(tmp.path()).unwrap();
        assert_eq!(sink.events_sent(), 0);
        sink.send(&make_event("t1")).await.unwrap();
        sink.send(&make_event("t2")).await.unwrap();
        assert_eq!(sink.events_sent(), 2);
        sink.close().await.unwrap();
    }

    #[test]
    fn name_is_file_jsonl() {
        let tmp = NamedTempFile::new().unwrap();
        let sink = FileJsonlSink::open(tmp.path()).unwrap();
        assert_eq!(sink.name(), "file-jsonl");
    }

    #[test]
    fn path_returns_configured_path() {
        let tmp = NamedTempFile::new().unwrap();
        let sink = FileJsonlSink::open(tmp.path()).unwrap();
        assert_eq!(sink.path(), tmp.path());
    }

    #[tokio::test]
    async fn written_lines_are_valid_json() {
        let tmp = NamedTempFile::new().unwrap();
        let mut sink = FileJsonlSink::open(tmp.path()).unwrap();
        sink.send(&make_event("orders")).await.unwrap();
        sink.close().await.unwrap();

        let content = std::fs::read_to_string(tmp.path()).unwrap();
        for line in content.lines() {
            let _: serde_json::Value =
                serde_json::from_str(line).expect("each line should be valid JSON");
        }
    }
}
