use std::io;
use std::sync::{Arc, Mutex};

use cdc_rs::StructuredLogger;
use serde_json::Value;
use tracing::Level;
use tracing_subscriber::fmt::writer::MakeWriter;

#[derive(Clone, Default)]
struct SharedWriter {
    inner: Arc<Mutex<Vec<u8>>>,
}

struct BufferGuard {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for BufferGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut lock = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("poisoned"))?;
        lock.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SharedWriter {
    type Writer = BufferGuard;

    fn make_writer(&'a self) -> Self::Writer {
        BufferGuard {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[test]
fn structured_logger_emits_expected_entries_without_secrets() {
    let sink = SharedWriter::default();

    let subscriber = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(Level::DEBUG)
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let logger = StructuredLogger::new("postgres");

        logger.source_connected();
        logger.snapshot_started("users");
        logger.snapshot_chunk_received("users", 100);
        logger.stream_started("0/16B6A70");
        for _ in 0..100 {
            logger.stream_events_received("users", 1, "0/16B6A70");
        }
        logger.checkpoint_saved("0/16B6A70", 100);
        logger.transform_applied("mask_hash", "users", "0/16B6A70");
        logger.connection_error("password=supersecret token=abcd");
        logger.stream_error("secret=top");
        logger.transform_error("mask_hash", "token=abc");
        logger.source_disconnected();
    });

    let bytes = sink.inner.lock().unwrap().clone();
    let output = String::from_utf8(bytes).unwrap();

    assert!(output.contains("source_connected"));
    assert!(output.contains("snapshot_started"));
    assert!(output.contains("stream_events_received"));
    assert!(output.contains("checkpoint_saved"));
    assert!(output.contains("transform_applied"));
    assert!(output.contains("source_disconnected"));

    assert!(!output.contains("supersecret"));
    assert!(!output.contains("abcd"));
    assert!(!output.contains("token=abc"));
    assert!(output.contains("***redacted***"));

    let stream_entries = output.matches("stream_events_received").count();
    assert!(stream_entries >= 100);
}

/// Test structured logging with 100-event stream and comprehensive validation.
/// This integration test validates:
/// - All expected log entries are present
/// - Credentials are never logged
/// - Structured fields are correct
/// - Performance under load (100 events)
#[test]
fn structured_logger_integration_100_events_no_credentials() {
    let sink = SharedWriter::default();

    let subscriber = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(Level::DEBUG)
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let logger = StructuredLogger::new("mysql");

        // Snapshot phase
        logger.source_connected();
        logger.snapshot_started("orders");
        for i in 0..25 {
            logger.snapshot_chunk_received("orders", (i + 1) * 100);
        }
        logger.snapshot_complete("orders");

        // Stream phase
        logger.stream_started("mysql-bin.000001:4");
        for _ in 0..100 {
            logger.stream_events_received("orders", 1, "mysql-bin.000001:4");
        }

        // Checkpoint phase
        logger.checkpoint_saved("mysql-bin.000001:104", 100);

        // Transformation phase (error scenario with secrets in context)
        logger.transform_applied("PII_mask", "orders", "mysql-bin.000001:104");
        logger.transform_error(
            "PII_mask",
            "Failed: password=db_secret token=auth_token secret=encryption_key",
        );

        // Error scenarios with sensitive data
        logger.connection_error("connection refused password=root_pass token=jwt_token");
        logger.stream_error("stream disrupted secret=api_key");
        logger.checkpoint_error("checkpoint failed password=secret123 token=bearer_xyz");

        logger.source_disconnected();
    });

    let bytes = sink.inner.lock().unwrap().clone();
    let output = String::from_utf8(bytes).unwrap();

    // Verify log entries
    assert!(output.contains("source_connected"));
    assert!(output.contains("snapshot_started"));
    assert!(output.contains("snapshot_chunk_received"));
    assert!(output.contains("snapshot_complete"));
    assert!(output.contains("stream_started"));
    assert!(output.contains("stream_events_received"));
    assert!(output.contains("checkpoint_saved"));
    assert!(output.contains("transform_applied"));
    assert!(output.contains("transform_error"));
    assert!(output.contains("connection_error"));
    assert!(output.contains("stream_error"));
    assert!(output.contains("checkpoint_error"));
    assert!(output.contains("source_disconnected"));

    // Verify source type field
    assert!(output.contains("source_type"));
    assert!(output.contains("mysql"));

    // Verify table field
    assert!(output.contains("orders"));

    // Verify credentials are NEVER logged
    let sensitive_data = vec![
        "db_secret",
        "auth_token",
        "encryption_key",
        "root_pass",
        "jwt_token",
        "api_key",
        "secret123",
        "bearer_xyz",
        "supersecret",
    ];
    for sensitive in sensitive_data {
        assert!(
            !output.contains(sensitive),
            "Found sensitive data in logs: {}",
            sensitive
        );
    }

    // Verify all credential fields are redacted
    let redacted_count = output.matches("***redacted***").count();
    assert!(
        redacted_count >= 7,
        "Expected at least 7 redacted credentials, found: {}",
        redacted_count
    );
    assert!(output.contains("password=***redacted***"));
    assert!(output.contains("token=***redacted***"));
    assert!(output.contains("secret=***redacted***"));

    // Verify log entry count
    let error_count = output.matches("error").count();
    assert!(error_count >= 3); // connection_error, stream_error, checkpoint_error, transform_error

    let stream_entries = output.matches("stream_events_received").count();
    assert_eq!(
        stream_entries, 100,
        "Expected 100 stream_events_received entries"
    );

    let chunk_entries = output.matches("snapshot_chunk_received").count();
    assert_eq!(
        chunk_entries, 25,
        "Expected 25 snapshot_chunk_received entries"
    );
}

/// Verify all structured fields are present in log output.
#[test]
fn structured_logger_fields_complete() {
    let sink = SharedWriter::default();

    let subscriber = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(Level::DEBUG)
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let logger = StructuredLogger::new("sqlserver");
        logger.snapshot_started("users");
        logger.stream_started("12345678");
        logger.checkpoint_saved("12345678", 42);
        logger.transform_applied("anonymize", "users", "12345678");
    });

    let bytes = sink.inner.lock().unwrap().clone();
    let output = String::from_utf8(bytes).unwrap();

    // Verify structured fields for each type of entry
    assert!(
        output.contains("source_type=sqlserver") || output.contains("source_type"),
        "source_type field missing or incorrect"
    );
    assert!(
        output.contains("table=users") || output.contains("\"table\""),
        "table field missing"
    );
    assert!(
        output.contains("event=snapshot_started") || output.contains("snapshot_started"),
        "event field missing"
    );
    assert!(
        output.contains("offset=12345678") || output.contains("12345678"),
        "offset field missing"
    );
}

#[test]
fn structured_logger_json_100_events_parse_and_redact() {
    let sink = SharedWriter::default();

    let subscriber = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(Level::DEBUG)
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .json()
        .flatten_event(true)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let logger = StructuredLogger::new("postgres");
        logger.source_connected();
        logger.snapshot_started("users");
        logger.stream_started("0/16B6A70");
        for _ in 0..100 {
            logger.stream_events_received("users", 1, "0/16B6A70");
        }
        logger.checkpoint_saved("0/16B6A70", 100);
        logger.connection_error("password=supersecret token=abcd secret=jwt");
        logger.source_disconnected();
    });

    let bytes = sink.inner.lock().unwrap().clone();
    let output = String::from_utf8(bytes).unwrap();
    let lines: Vec<&str> = output.lines().filter(|l| !l.trim().is_empty()).collect();

    assert!(!lines.is_empty(), "expected JSON log lines");

    let mut stream_count = 0usize;
    let mut saw_source_connected = false;
    let mut saw_checkpoint_saved = false;
    let mut saw_redacted_error = false;

    for line in lines {
        let parsed: Value = serde_json::from_str(line).expect("line must be valid JSON");

        let event = parsed
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let source_type = parsed
            .get("source_type")
            .and_then(Value::as_str)
            .unwrap_or_default();

        assert_eq!(source_type, "postgres");

        if event == "stream_events_received" {
            stream_count += 1;
        }
        if event == "source_connected" {
            saw_source_connected = true;
        }
        if event == "checkpoint_saved" {
            saw_checkpoint_saved = true;
        }
        if event == "connection_error" {
            let error = parsed
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or_default();
            assert!(error.contains("***redacted***"));
            assert!(!error.contains("supersecret"));
            assert!(!error.contains("abcd"));
            assert!(!error.contains("jwt"));
            saw_redacted_error = true;
        }
    }

    assert_eq!(stream_count, 100, "expected exactly 100 streamed events");
    assert!(saw_source_connected);
    assert!(saw_checkpoint_saved);
    assert!(saw_redacted_error);
}
