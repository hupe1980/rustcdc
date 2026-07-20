//! [`FileJsonlSink`] — append-only NDJSON file sink with async I/O,
//! in-memory batching, size-based rotation, and configurable fsync cadence.

use std::path::{Path, PathBuf};

use tokio::io::{AsyncWriteExt, BufWriter};

use crate::core::{Error, Event, Result};
use crate::sink::SinkAdapter;

// ─── Constants ────────────────────────────────────────────────────────────────

/// Maximum number of pending lines before an automatic flush is triggered.
const BATCH_MAX_LINES: usize = 128;
/// Maximum number of pending bytes before an automatic flush is triggered.
const BATCH_MAX_BYTES: usize = 256 * 1024; // 256 KiB
/// Internal `BufWriter` capacity.
const WRITE_BUF_CAPACITY: usize = 64 * 1024; // 64 KiB

// ─── FileJsonlSinkConfig ──────────────────────────────────────────────────────

/// Configuration knobs for [`FileJsonlSink`].
///
/// Construct with [`FileJsonlSinkConfig::default`] and override as needed,
/// then pass to [`FileJsonlSink::open_with`].
///
/// ```rust
/// use rustcdc::sink::FileJsonlSinkConfig;
///
/// // 100 MiB rotation, fsync every 50 flushes.
/// let config = FileJsonlSinkConfig {
///     rotate_size_bytes: 100 * 1024 * 1024,
///     fsync_every: 50,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct FileJsonlSinkConfig {
    /// Rotate the active file when it reaches this many bytes.
    ///
    /// Set to `0` (the default) to disable rotation entirely.
    pub rotate_size_bytes: u64,

    /// Call `sync_data()` every *n* [`flush`](SinkAdapter::flush) calls.
    ///
    /// `1` (the default) syncs on every flush — safest, slowest.
    /// Higher values amortise the `sync_data` cost across more events at the
    /// expense of a larger crash-safety window.
    pub fsync_every: u32,
}

impl Default for FileJsonlSinkConfig {
    fn default() -> Self {
        Self {
            rotate_size_bytes: 0,
            fsync_every: 1,
        }
    }
}

// ─── FileJsonlSink ────────────────────────────────────────────────────────────

/// Appends CDC events to a file as newline-delimited JSON (NDJSON / JSON Lines).
///
/// Events are queued in an in-memory pending batch and written together when the
/// batch reaches 128 lines or 256 KiB, whichever comes first.  An explicit
/// [`flush`](SinkAdapter::flush) always drains the pending batch regardless of
/// its current size.
///
/// ## Rotation
///
/// When `rotate_size_bytes > 0` the active file is atomically renamed to a
/// timestamped archive (`<stem>.<timestamp_ms>.<rotation_count>.<ext>`) and a
/// fresh active file is opened before any write that would exceed the threshold.
///
/// ## fsync cadence
///
/// Every `fsync_every`-th flush triggers `sync_data()` on top of the regular
/// page-cache flush.  Use `fsync_every = 1` for maximum crash durability
/// (the default) or a higher value (e.g. `100`) to reduce I/O pressure at the
/// cost of a larger crash-safety window.
///
/// ## Example
///
/// ```rust,no_run
/// use rustcdc::sink::{FileJsonlSink, FileJsonlSinkConfig, SinkAdapter};
/// use rustcdc::{Event, Operation};
///
/// # #[tokio::main]
/// # async fn main() -> rustcdc::core::Result<()> {
/// // Simple: no rotation, fsync every flush.
/// let mut sink = FileJsonlSink::open("/tmp/events.jsonl")?;
/// let event = Event { op: Operation::Insert, ..Event::default() };
/// sink.send(&event).await?;
/// sink.close().await?;
///
/// // Advanced: 100 MiB rotation, fsync every 50 flushes.
/// let config = FileJsonlSinkConfig {
///     rotate_size_bytes: 100 * 1024 * 1024,
///     fsync_every: 50,
/// };
/// let mut sink = FileJsonlSink::open_with("/tmp/events-adv.jsonl", config)?;
/// # Ok(())
/// # }
/// ```
pub struct FileJsonlSink {
    writer: BufWriter<tokio::fs::File>,
    path: PathBuf,
    rotate_size_bytes: u64,
    fsync_every: u32,
    flush_count: u32,
    rotation_count: u64,
    bytes_written: u64,
    events_sent: u64,
    pending_lines: Vec<Vec<u8>>,
    pending_bytes: usize,
    closed: bool,
}

impl std::fmt::Debug for FileJsonlSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileJsonlSink")
            .field("path", &self.path)
            .field("closed", &self.closed)
            .field("events_sent", &self.events_sent)
            .field("pending_lines", &self.pending_lines.len())
            .field("pending_bytes", &self.pending_bytes)
            .field("bytes_written", &self.bytes_written)
            .field("rotation_count", &self.rotation_count)
            .finish()
    }
}

impl FileJsonlSink {
    /// Open (or create and append to) the JSONL file at `path`.
    ///
    /// Uses the default configuration: no rotation, `sync_data` on every flush.
    /// Parent directories are created automatically.
    ///
    /// Use [`open_with`](Self::open_with) to configure rotation or a custom
    /// fsync cadence.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, FileJsonlSinkConfig::default())
    }

    /// Open (or create and append to) the JSONL file at `path` with a custom
    /// [`FileJsonlSinkConfig`].
    ///
    /// Parent directories are created automatically.
    pub fn open_with(path: impl AsRef<Path>, config: FileJsonlSinkConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    Error::StateError(format!(
                        "FileJsonlSink: could not create parent directory '{}': {e}",
                        parent.display()
                    ))
                })?;
            }
        }

        let std_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| {
                Error::StateError(format!(
                    "FileJsonlSink: could not open '{}': {e}",
                    path.display()
                ))
            })?;

        // Seed bytes_written from the existing file size so rotation accounts
        // for content written in a previous process lifetime.
        let bytes_written = std_file.metadata().map(|m| m.len()).unwrap_or(0);

        Ok(Self {
            writer: BufWriter::with_capacity(
                WRITE_BUF_CAPACITY,
                tokio::fs::File::from_std(std_file),
            ),
            path,
            rotate_size_bytes: config.rotate_size_bytes,
            fsync_every: config.fsync_every.max(1),
            flush_count: 0,
            rotation_count: 0,
            bytes_written,
            events_sent: 0,
            pending_lines: Vec::new(),
            pending_bytes: 0,
            closed: false,
        })
    }

    // ── Inspectors ─────────────────────────────────────────────────────────────

    /// The path this sink appends to.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Total number of events successfully enqueued via [`send`](SinkAdapter::send).
    ///
    /// The counter is not reset on flush or rotation.
    pub fn events_sent(&self) -> u64 {
        self.events_sent
    }

    /// Number of events pending in the in-memory batch (not yet written to disk).
    pub fn queue_depth(&self) -> usize {
        self.pending_lines.len()
    }

    // ── Internal helpers ───────────────────────────────────────────────────────

    fn closed_err(&self) -> Error {
        Error::StateError(format!(
            "FileJsonlSink('{}') is closed",
            self.path.display()
        ))
    }

    /// Write a pre-formatted line (including the trailing `\n`) to the buffered
    /// writer, rotating the active file first if writing the line would push the
    /// cumulative byte count past the configured size threshold.
    ///
    /// The `bytes_written > 0` guard prevents producing empty rotated files on
    /// the very first write or immediately after a prior rotation.
    async fn write_line(&mut self, line: &[u8]) -> Result<()> {
        if self.rotate_size_bytes > 0
            && self.bytes_written > 0
            && self.bytes_written + line.len() as u64 > self.rotate_size_bytes
        {
            self.rotate().await?;
        }
        self.writer.write_all(line).await.map_err(Error::IoError)?;
        self.bytes_written += line.len() as u64;
        Ok(())
    }

    /// Flush the `BufWriter` to the OS page cache and, every `fsync_every`
    /// flushes, issue a `sync_data()` call for crash-safe durability.
    async fn flush_writer(&mut self) -> Result<()> {
        self.writer.flush().await.map_err(Error::IoError)?;
        self.flush_count = self.flush_count.wrapping_add(1);
        if self.flush_count.is_multiple_of(self.fsync_every) {
            self.writer
                .get_ref()
                .sync_data()
                .await
                .map_err(Error::IoError)?;
        }
        Ok(())
    }

    /// Drain the pending batch to disk and flush.
    ///
    /// Uses `drain(..)` rather than `mem::take` so the backing allocation of
    /// `pending_lines` is preserved for the next batch, avoiding reallocation
    /// on the hot path.
    async fn flush_pending(&mut self) -> Result<()> {
        if self.pending_lines.is_empty() {
            return self.flush_writer().await;
        }
        // Drain preserves the Vec's capacity; collect into a local vec so we
        // can call async methods on &mut self inside the loop below.
        let batch: Vec<Vec<u8>> = self.pending_lines.drain(..).collect();
        self.pending_bytes = 0;
        for line in batch {
            self.write_line(&line).await?;
        }
        self.flush_writer().await
    }

    /// Atomically rename the active file to a timestamped archive and open a
    /// fresh active file at the original path.
    ///
    /// ## Restart-safety invariant
    ///
    /// After rotation the original configured `path` always refers to the
    /// **current active file**.  On process restart, `open_with(path, ...)` opens
    /// this file and appends to it — no events are lost and no gap is created.
    /// The archive files are named `<stem>.<timestamp_ms>.<rotation_count>.<ext>`
    /// and are never touched again by this sink instance.
    ///
    /// Uses `tokio::fs::OpenOptions` for the new-file open to avoid blocking
    /// the executor on slow or network-backed filesystems.
    async fn rotate(&mut self) -> Result<()> {
        // Flush + sync before rename so the rotated file is fully consistent.
        self.writer.flush().await.map_err(Error::IoError)?;
        self.writer
            .get_ref()
            .sync_data()
            .await
            .map_err(Error::IoError)?;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        self.rotation_count = self.rotation_count.saturating_add(1);
        let rotated = rotated_path(&self.path, ts, self.rotation_count);

        tokio::fs::rename(&self.path, &rotated)
            .await
            .map_err(Error::IoError)?;

        // Async open: does not block the executor.
        let new_file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(Error::IoError)?;

        self.writer = BufWriter::with_capacity(WRITE_BUF_CAPACITY, new_file);
        self.bytes_written = 0;
        Ok(())
    }
}

// ─── rotated_path ─────────────────────────────────────────────────────────────

fn rotated_path(path: &Path, timestamp_ms: u128, rotation_count: u64) -> PathBuf {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("events");
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let file_name = match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) => format!("{stem}.{timestamp_ms}.{rotation_count}.{ext}"),
        None => format!("{stem}.{timestamp_ms}.{rotation_count}"),
    };
    parent.join(file_name)
}

// ─── SinkAdapter ──────────────────────────────────────────────────────────────

impl SinkAdapter for FileJsonlSink {
    fn name(&self) -> &str {
        "file-jsonl"
    }

    /// Current number of events in the in-memory pending batch.
    ///
    /// Used by the runtime for back-pressure observation.  Returns `Some(0)`
    /// when all pending events have been flushed to the OS page cache.
    fn queue_depth(&self) -> Option<usize> {
        Some(self.pending_lines.len())
    }

    async fn send(&mut self, event: &Event) -> Result<()> {
        if self.closed {
            return Err(self.closed_err());
        }
        let mut line = serde_json::to_vec(event)?;
        line.push(b'\n');
        self.pending_bytes += line.len();
        self.pending_lines.push(line);
        self.events_sent += 1;
        if self.pending_lines.len() >= BATCH_MAX_LINES || self.pending_bytes >= BATCH_MAX_BYTES {
            self.flush_pending().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.closed {
            return Err(self.closed_err());
        }
        self.flush_pending().await
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.flush_pending().await?;
        // Force a final sync_data() regardless of the fsync_every cadence.
        // close() is always a durability boundary — the caller must be able to
        // rely on all events being on stable storage after this returns.
        self.writer
            .get_ref()
            .sync_data()
            .await
            .map_err(Error::IoError)?;
        self.closed = true;
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed
    }

    fn delivery_metrics(&self) -> Option<crate::sink::SinkDeliveryMetrics> {
        Some(crate::sink::SinkDeliveryMetrics {
            events_sent: self.events_sent,
            ..Default::default()
        })
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::io::BufRead;

    use tempfile::NamedTempFile;

    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use crate::sink::SinkAdapter;

    use super::{FileJsonlSink, FileJsonlSinkConfig};

    fn make_event(table: &str) -> Event {
        Event {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "0".into(),
                timestamp: 1,
            },
            ts: 1,
            schema: None,
            table: table.into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    fn count_lines(path: &std::path::Path) -> usize {
        let f = std::fs::File::open(path).unwrap();
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
        {
            let mut sink = FileJsonlSink::open(tmp.path()).unwrap();
            sink.send(&make_event("orders")).await.unwrap();
            sink.close().await.unwrap();
        }
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
                serde_json::from_str(line).expect("each line must be valid JSON");
        }
    }

    #[test]
    fn queue_depth_reflects_pending_sends() {
        let tmp = NamedTempFile::new().unwrap();
        let sink = FileJsonlSink::open(tmp.path()).unwrap();
        assert_eq!(sink.queue_depth(), 0);
    }

    #[tokio::test]
    async fn rotation_names_stay_unique_under_rapid_rotations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");

        // rotate_size_bytes = 1 byte triggers a rotation on every write.
        let config = FileJsonlSinkConfig {
            rotate_size_bytes: 1,
            fsync_every: 1,
        };
        let mut sink = FileJsonlSink::open_with(&path, config).unwrap();
        sink.send(&make_event("t1")).await.unwrap();
        sink.flush().await.unwrap();
        sink.send(&make_event("t2")).await.unwrap();
        sink.flush().await.unwrap();
        sink.close().await.unwrap();

        let rotated_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|name| name.starts_with("events.") && name.ends_with(".jsonl"))
            .collect();

        let unique_count = rotated_files
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert_eq!(
            unique_count,
            rotated_files.len(),
            "rotated filenames must be unique: {rotated_files:?}"
        );
        assert!(
            !rotated_files.is_empty(),
            "expected at least one rotated file"
        );
    }

    #[tokio::test]
    async fn double_close_is_idempotent() {
        let tmp = NamedTempFile::new().unwrap();
        let mut sink = FileJsonlSink::open(tmp.path()).unwrap();
        sink.close().await.unwrap();
        sink.close().await.unwrap();
    }

    #[test]
    fn debug_impl_does_not_panic() {
        let tmp = NamedTempFile::new().unwrap();
        let sink = FileJsonlSink::open(tmp.path()).unwrap();
        let _ = format!("{sink:?}");
    }
}
