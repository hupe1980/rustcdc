//! Structured logging helpers for runtime, source, checkpoint, and transform paths.
//!
//! Configure with the standard `RUST_LOG` environment variable, for example:
//! `RUST_LOG=info` or `RUST_LOG=cdc_rs=debug`.
//!
//! All logs include structured context fields:
//! - `source_type`: Source connector type (postgres, mysql, sqlserver)
//! - `table`: Table name (when applicable)
//! - Tracing span/trace context for correlation

use std::sync::OnceLock;

use regex::Regex;
use tracing::{debug, error, info, warn};

/// Thin structured-logging facade used by Phase 1 components.
#[derive(Debug, Clone)]
pub struct StructuredLogger {
    source_type: String,
}

impl StructuredLogger {
    /// Create a logger scoped to a source type.
    pub fn new(source_type: impl Into<String>) -> Self {
        Self {
            source_type: source_type.into(),
        }
    }

    /// Log source connection event.
    pub fn source_connected(&self) {
        info!(
            event = "source_connected",
            source_type = %self.source_type,
            "Source connected successfully"
        );
    }

    /// Log source disconnection event.
    pub fn source_disconnected(&self) {
        info!(
            event = "source_disconnected",
            source_type = %self.source_type,
            "Source disconnected"
        );
    }

    /// Log insecure transport mode warning.
    pub fn insecure_transport_warning(&self, mode: &str, details: &str) {
        warn!(
            event = "insecure_transport",
            source_type = %self.source_type,
            mode = %mode,
            details = %details,
            "Insecure transport mode enabled"
        );
    }

    /// Log connection error with sanitized context.
    pub fn connection_error(&self, context: &str) {
        error!(
            event = "connection_error",
            source_type = %self.source_type,
            error = %sanitize_context(context),
            "Connection error occurred"
        );
    }

    /// Log snapshot start event.
    pub fn snapshot_started(&self, table: &str) {
        info!(
            event = "snapshot_started",
            source_type = %self.source_type,
            table = %table,
            "Snapshot started for table"
        );
    }

    /// Log snapshot chunk received event.
    pub fn snapshot_chunk_received(&self, table: &str, chunk_size: usize) {
        debug!(
            event = "snapshot_chunk_received",
            source_type = %self.source_type,
            table = %table,
            chunk_size,
            "Snapshot chunk received"
        );
    }

    /// Log snapshot completion event.
    pub fn snapshot_complete(&self, table: &str) {
        info!(
            event = "snapshot_complete",
            source_type = %self.source_type,
            table = %table,
            "Snapshot completed for table"
        );
    }

    /// Log stream start event.
    pub fn stream_started(&self, offset: &str) {
        info!(
            event = "stream_started",
            source_type = %self.source_type,
            offset = %offset,
            "Stream started"
        );
    }

    /// Log stream events received event.
    pub fn stream_events_received(&self, table: &str, event_count: usize, offset: &str) {
        debug!(
            event = "stream_events_received",
            source_type = %self.source_type,
            table = %table,
            event_count,
            offset = %offset,
            "Stream events received"
        );
    }

    /// Log stream error.
    pub fn stream_error(&self, context: &str) {
        error!(
            event = "stream_error",
            source_type = %self.source_type,
            error = %sanitize_context(context),
            "Stream error occurred"
        );
    }

    /// Log checkpoint save event.
    pub fn checkpoint_saved(&self, offset: &str, committed_count: u64) {
        info!(
            event = "checkpoint_saved",
            source_type = %self.source_type,
            offset = %offset,
            committed_count,
            "Checkpoint saved"
        );
    }

    /// Log checkpoint load event.
    pub fn checkpoint_loaded(&self, offset: &str, committed_count: u64) {
        info!(
            event = "checkpoint_loaded",
            source_type = %self.source_type,
            offset = %offset,
            committed_count,
            "Checkpoint loaded"
        );
    }

    /// Log checkpoint error.
    pub fn checkpoint_error(&self, context: &str) {
        warn!(
            event = "checkpoint_error",
            source_type = %self.source_type,
            error = %sanitize_context(context),
            "Checkpoint error occurred"
        );
    }

    /// Log transform application event.
    pub fn transform_applied(&self, transform: &str, table: &str, offset: &str) {
        debug!(
            event = "transform_applied",
            source_type = %self.source_type,
            transform = %transform,
            table = %table,
            offset = %offset,
            "Transform applied"
        );
    }

    /// Log transform error.
    pub fn transform_error(&self, transform: &str, context: &str) {
        warn!(
            event = "transform_error",
            source_type = %self.source_type,
            transform = %transform,
            error = %sanitize_context(context),
            "Transform error occurred"
        );
    }

    /// Log generic info message with context.
    pub fn info_with_context(&self, message: &str, table: Option<&str>, additional_context: &str) {
        info!(
            source_type = %self.source_type,
            table = ?table,
            context = %additional_context,
            "{}",
            message
        );
    }

    /// Log generic error message with context.
    pub fn error_with_context(&self, message: &str, table: Option<&str>, additional_context: &str) {
        error!(
            source_type = %self.source_type,
            table = ?table,
            context = %sanitize_context(additional_context),
            "{}",
            message
        );
    }
}

/// Sanitizes context strings to remove sensitive data like passwords and tokens.
/// Prevents credentials from being logged in plaintext.
pub fn sanitize_context(input: &str) -> String {
    let with_dsn_redaction = dsn_userinfo_regex().replace_all(input, "$scheme***redacted***@");
    key_value_regex()
        .replace_all(with_dsn_redaction.as_ref(), |caps: &regex::Captures<'_>| {
            let prefix = caps.name("prefix").map(|m| m.as_str()).unwrap_or_default();
            let value = caps.name("value").map(|m| m.as_str()).unwrap_or_default();

            if value.starts_with('"') && value.ends_with('"') {
                format!("{prefix}\"***redacted***\"")
            } else if value.starts_with('\'') && value.ends_with('\'') {
                format!("{prefix}'***redacted***'")
            } else {
                format!("{prefix}***redacted***")
            }
        })
        .into_owned()
}

fn key_value_regex() -> &'static Regex {
    static KEY_VALUE_RE: OnceLock<Regex> = OnceLock::new();
    KEY_VALUE_RE.get_or_init(|| {
        Regex::new(
            r#"(?ix)
            (?P<prefix>
                \b(?:
                    password|passwd|pwd|token|secret|api[_-]?key|access[_-]?key|client[_-]?secret
                )\b(?:\s*\\?["'])?\s*[:=]\s*
            )
            (?P<value>
                \\\"(?:\\\\.|[^"\\\\])*\\\"
                |
                "(?:\\.|[^"\\])*"
                |
                '(?:\\.|[^'\\])*'
                |
                [^\s,;&]+)"#,
        )
        .expect("key-value redaction regex must compile")
    })
}

fn dsn_userinfo_regex() -> &'static Regex {
    static DSN_RE: OnceLock<Regex> = OnceLock::new();
    DSN_RE.get_or_init(|| {
        Regex::new(r"(?i)(?P<scheme>[a-z][a-z0-9+.-]*://[^/@\s:]+:)[^@\s/]+@")
            .expect("DSN userinfo redaction regex must compile")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_context_password() {
        let input = "connection failed password=secret";
        let sanitized = sanitize_context(input);
        assert!(!sanitized.contains("secret"));
        assert!(sanitized.contains("password=***redacted***"));
    }

    #[test]
    fn test_sanitize_context_token() {
        let input = "auth error token=abc123xyz";
        let sanitized = sanitize_context(input);
        assert!(!sanitized.contains("abc123xyz"));
        assert!(sanitized.contains("token=***redacted***"));
    }

    #[test]
    fn test_sanitize_context_secret() {
        let input = "encryption failed secret=mysecret";
        let sanitized = sanitize_context(input);
        assert!(!sanitized.contains("mysecret"));
        assert!(sanitized.contains("secret=***redacted***"));
    }

    #[test]
    fn test_sanitize_context_multiple_fields() {
        let input = "connection password=secret token=xyz failed";
        let sanitized = sanitize_context(input);
        assert!(!sanitized.contains("secret"));
        assert!(!sanitized.contains("xyz"));
        assert!(sanitized.contains("password=***redacted***"));
        assert!(sanitized.contains("token=***redacted***"));
    }

    #[test]
    fn test_sanitize_context_non_sensitive() {
        let input = "connection failed at 2024-01-15";
        let sanitized = sanitize_context(input);
        assert_eq!(sanitized, input);
    }

    #[test]
    fn test_structured_logger_creation() {
        let logger = StructuredLogger::new("postgres");
        assert_eq!(logger.source_type, "postgres");
    }

    #[test]
    fn test_logger_methods_are_callable_without_panics() {
        let logger = StructuredLogger::new("sqlserver");

        logger.source_connected();
        logger.source_disconnected();
        logger.connection_error("password=secret");
        logger.snapshot_started("dbo.users");
        logger.snapshot_chunk_received("dbo.users", 5000);
        logger.snapshot_complete("dbo.users");
        logger.stream_started("0x000000230000015A0008");
        logger.stream_events_received("dbo.users", 1000, "0x000000230000015A0010");
        logger.stream_error("token=abc123");
        logger.checkpoint_saved("0x000000230000015A0010", 1000);
        logger.checkpoint_loaded("0x000000230000015A0010", 1000);
        logger.checkpoint_error("secret=verysecret");
        logger.transform_applied("mask_hash", "dbo.users", "0x000000230000015A0010");
        logger.transform_error("mask_hash", "password=secret");
        logger.info_with_context("snapshot progress", Some("dbo.users"), "chunk=5");
        logger.error_with_context("stream error", None, "token=secret");
    }

    #[test]
    fn test_sanitize_context_handles_token_without_assignment() {
        let input = "authentication token expired";
        let sanitized = sanitize_context(input);
        assert_eq!(sanitized, input);
    }

    #[test]
    fn test_sanitize_context_redacts_json_style_fields() {
        let input = r#"payload={\"password\":\"secret\",\"token\":\"abc123\"}"#;
        let sanitized = sanitize_context(input);
        assert!(!sanitized.contains("secret"));
        assert!(!sanitized.contains("abc123"));
        assert!(sanitized.contains("password"));
        assert!(sanitized.contains("token"));
        assert!(sanitized.contains("***redacted***"));
    }

    #[test]
    fn test_sanitize_context_redacts_dsn_userinfo_password() {
        let input = "connect postgres://alice:supersecret@db.internal:5432/app";
        let sanitized = sanitize_context(input);
        assert!(!sanitized.contains("supersecret"));
        assert!(sanitized.contains("postgres://alice:***redacted***@db.internal:5432/app"));
    }

    #[test]
    fn test_sanitize_context_redacts_query_string_secrets() {
        let input = "request /health?api_key=abc123&token=xyz&ok=true";
        let sanitized = sanitize_context(input);
        assert!(!sanitized.contains("abc123"));
        assert!(!sanitized.contains("xyz"));
        assert!(sanitized.contains("api_key=***redacted***"));
        assert!(sanitized.contains("token=***redacted***"));
    }

    #[test]
    fn test_sanitize_context_redacts_colon_separator_values() {
        let input = "client_secret: qwerty access_key: abc";
        let sanitized = sanitize_context(input);
        assert!(!sanitized.contains("qwerty"));
        assert!(!sanitized.contains("abc"));
        assert!(sanitized.contains("client_secret: ***redacted***"));
        assert!(sanitized.contains("access_key: ***redacted***"));
    }
}
