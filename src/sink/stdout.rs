//! [`StdoutSink`] — writes newline-delimited JSON to stdout.

use std::io::{self, Write};

use crate::core::{Error, Event, Result};
use crate::sink::SinkAdapter;

// ─── StdoutSink ───────────────────────────────────────────────────────────────

/// Writes CDC events to **stdout** as newline-delimited JSON (NDJSON).
///
/// Each event is serialised with `serde_json::to_vec` and followed by a `\n`.
/// Writes are synchronous and hold the stdout lock for the duration of each
/// `send` call; this makes output ordering deterministic under concurrent async
/// tasks.
///
/// # When to use
///
/// - Local debugging / development pipelines (`cdc-server | jq`).
/// - Integration tests that capture stdout with `assert_cmd`.
/// - Docker / Kubernetes deployments where stdout is the primary log channel.
///
/// # Not suitable for
///
/// High-throughput production pipelines. Every `send` acquires a global lock.
/// For production use, implement [`SinkAdapter`] against your streaming platform.
///
/// # Example
///
/// ```rust,no_run
/// use rustcdc::sink::StdoutSink;
/// use rustcdc::sink::SinkAdapter;
/// use rustcdc::{Event, Operation, EVENT_ENVELOPE_VERSION};
///
/// # #[tokio::main]
/// # async fn main() {
/// let mut sink = StdoutSink::new();
/// let event = Event { op: Operation::Insert, ..Event::default() };
/// sink.send(&event).await.unwrap();
/// # }
/// ```
#[derive(Debug, Default)]
pub struct StdoutSink {
    closed: bool,
    events_sent: u64,
    pretty: bool,
    /// When `Some`, every sent event is appended here for test inspection.
    captured: Option<Vec<Event>>,
}

impl StdoutSink {
    /// Create a new `StdoutSink` that writes compact (single-line) JSON.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a `StdoutSink` that writes pretty-printed JSON.
    ///
    /// Useful for human-readable development output and terminal piping.
    /// For machine-consumption pipelines, prefer [`StdoutSink::new`].
    pub fn with_pretty() -> Self {
        Self {
            pretty: true,
            ..Self::default()
        }
    }

    /// Create a `StdoutSink` that also buffers every sent event in memory.
    ///
    /// Intended for **testing only** — the buffer grows unbounded. Call
    /// [`exported_events`](SinkAdapter::exported_events) on the sink to
    /// inspect captured events after delivery.
    pub fn with_capture() -> Self {
        Self {
            captured: Some(Vec::new()),
            ..Self::default()
        }
    }

    /// Total number of events sent through this sink (not reset on flush).
    pub fn events_sent(&self) -> u64 {
        self.events_sent
    }
}

impl SinkAdapter for StdoutSink {
    async fn send(&mut self, event: &Event) -> Result<()> {
        if self.closed {
            return Err(Error::StateError("StdoutSink is closed".into()));
        }
        let mut bytes = if self.pretty {
            serde_json::to_vec_pretty(event)?
        } else {
            serde_json::to_vec(event)?
        };
        bytes.push(b'\n');
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        handle
            .write_all(&bytes)
            .map_err(|e| Error::StateError(format!("StdoutSink write: {e}")))?;
        self.events_sent += 1;
        if let Some(ref mut buf) = self.captured {
            buf.push(event.clone());
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::StateError("StdoutSink is closed".into()));
        }
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        handle
            .flush()
            .map_err(|e| Error::StateError(format!("StdoutSink flush: {e}")))?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if !self.closed {
            // Best-effort flush before close.
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            let _ = handle.flush();
        }
        self.closed = true;
        Ok(())
    }

    fn name(&self) -> &str {
        "stdout"
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

    fn exported_events(&self) -> Option<&[Event]> {
        self.captured.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_after_close_errors() {
        let mut sink = StdoutSink::new();
        sink.close().await.unwrap();
        let event = Event::default();
        assert!(sink.send(&event).await.is_err());
    }

    #[tokio::test]
    async fn flush_after_close_errors() {
        let mut sink = StdoutSink::new();
        sink.close().await.unwrap();
        assert!(sink.flush().await.is_err());
    }

    #[test]
    fn name_is_stdout() {
        let sink = StdoutSink::new();
        assert_eq!(sink.name(), "stdout");
    }

    #[test]
    fn is_closed_starts_false() {
        let sink = StdoutSink::new();
        assert!(!sink.is_closed());
    }

    #[tokio::test]
    async fn is_closed_true_after_close() {
        let mut sink = StdoutSink::new();
        sink.close().await.unwrap();
        assert!(sink.is_closed());
    }

    #[tokio::test]
    async fn events_sent_increments() {
        // We can't easily capture stdout in unit tests, but we can verify counter.
        // Use a real StdoutSink — output goes to /dev/null in CI.
        let mut sink = StdoutSink::new();
        assert_eq!(sink.events_sent(), 0);
        sink.send(&Event::default()).await.unwrap();
        assert_eq!(sink.events_sent(), 1);
        sink.send(&Event::default()).await.unwrap();
        assert_eq!(sink.events_sent(), 2);
    }
}
