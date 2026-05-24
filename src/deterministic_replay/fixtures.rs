/// Fixture corpus for deterministic protocol replay.
///
/// A fixture represents a captured sequence of protocol-level messages from a source connector.
/// Fixtures are versioned, tagged with metadata, and can be replayed without a live database.
use serde::{Deserialize, Serialize};

/// Metadata describing a fixture and its protocol/version context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FixtureMetadata {
    /// Fixture unique identifier (e.g., "pg_insert_100_rows_v1")
    pub id: String,

    /// Source type: "postgres", "mysql", "sqlserver"
    pub source_type: String,

    /// Protocol version (e.g., "pgoutput_v2" for PostgreSQL)
    pub protocol_version: String,

    /// Source version constraint (e.g., "postgres>=12,<17" or "mysql=8.0")
    pub source_version: String,

    /// Fixture format version
    pub fixture_version: u32,

    /// Human-readable description of what this fixture captures
    pub description: String,

    /// List of scenario tags (e.g., ["insert", "large-batch", "100k-rows"])
    pub tags: Vec<String>,

    /// Expected event count for validation
    pub expected_event_count: usize,

    /// Date fixture was captured (ISO 8601)
    pub captured_at: String,
}

/// A single message in a fixture protocol stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FixtureMessage {
    /// Sequence number in the stream
    pub seq: usize,

    /// Protocol-specific message type (e.g., "Begin", "Relation", "Insert", "Commit" for pgoutput)
    pub message_type: String,

    /// Raw message data (hex-encoded for binary protocols, JSON for structured)
    pub payload: String,

    /// Metadata tags for this message (e.g., ["transactional", "critical"])
    pub tags: Vec<String>,
}

/// A fixture corpus entry: metadata + captured message sequence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fixture {
    /// Fixture metadata
    pub metadata: FixtureMetadata,

    /// Captured protocol messages in order
    pub messages: Vec<FixtureMessage>,
}

impl Fixture {
    /// Create a new fixture from metadata and messages.
    pub fn new(metadata: FixtureMetadata, messages: Vec<FixtureMessage>) -> Self {
        assert_eq!(
            messages.len(),
            metadata.expected_event_count,
            "Message count mismatch: got {}, expected {}",
            messages.len(),
            metadata.expected_event_count
        );
        Self { metadata, messages }
    }

    /// Serialize fixture to JSON for storage.
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize fixture from JSON.
    pub fn from_json(json: &str) -> serde_json::Result<Self> {
        serde_json::from_str(json)
    }

    /// Load a fixture from a JSON file path.
    pub fn from_path(path: &std::path::Path) -> std::result::Result<Self, String> {
        let raw = std::fs::read_to_string(path)
            .map_err(|error| format!("failed reading fixture '{}': {error}", path.display()))?;
        Self::from_json(&raw)
            .map_err(|error| format!("failed parsing fixture '{}': {error}", path.display()))
    }

    /// Validate fixture structural integrity.
    pub fn validate(&self) -> Result<(), String> {
        if self.messages.is_empty() {
            return Err("Fixture has no messages".to_string());
        }

        // Verify sequence numbers are contiguous
        for (i, msg) in self.messages.iter().enumerate() {
            if msg.seq != i {
                return Err(format!(
                    "Non-contiguous sequence at index {}: expected {}, got {}",
                    i, i, msg.seq
                ));
            }

            validate_fixture_message(&self.metadata.source_type, msg)?;
        }

        Ok(())
    }
}

fn validate_fixture_message(source_type: &str, message: &FixtureMessage) -> Result<(), String> {
    let payload: serde_json::Value = serde_json::from_str(&message.payload).map_err(|error| {
        format!(
            "Invalid JSON payload for message {} ({}): {error}",
            message.seq, message.message_type
        )
    })?;

    match (source_type, message.message_type.as_str()) {
        ("postgres", "Begin" | "Commit") | ("mysql", "XidEvent") | ("sqlserver", "Control") => {
            validate_object_payload(&payload, message, &[])
        }
        ("postgres", "Insert") | ("mysql", "WriteRowsEvent") | ("sqlserver", "Capture") => {
            validate_dml_payload(&payload, message, false, true)
        }
        ("postgres", "Update") | ("mysql", "UpdateRowsEvent") | ("sqlserver", "Update") => {
            validate_dml_payload(&payload, message, true, true)
        }
        ("postgres", "Delete") | ("mysql", "DeleteRowsEvent") | ("sqlserver", "Delete") => {
            validate_dml_payload(&payload, message, true, false)
        }
        ("postgres", "Ddl") | ("sqlserver", "Ddl") => {
            validate_object_payload(&payload, message, &["statement"])
        }
        ("mysql", "QueryEvent") => validate_mysql_query_event(&payload, message),
        (unknown_source, message_type) => Err(format!(
            "Unsupported fixture message type '{}' for source '{}' at seq {}",
            message_type, unknown_source, message.seq
        )),
    }
}

fn validate_object_payload(
    payload: &serde_json::Value,
    message: &FixtureMessage,
    required_fields: &[&str],
) -> Result<(), String> {
    let object = payload.as_object().ok_or_else(|| {
        format!(
            "Fixture message {} ({}) payload must be a JSON object",
            message.seq, message.message_type
        )
    })?;

    for field in required_fields {
        if !object.contains_key(*field) {
            return Err(format!(
                "Fixture message {} ({}) missing required field '{}'",
                message.seq, message.message_type, field
            ));
        }
    }

    Ok(())
}

fn validate_dml_payload(
    payload: &serde_json::Value,
    message: &FixtureMessage,
    require_before: bool,
    require_after: bool,
) -> Result<(), String> {
    validate_object_payload(payload, message, &["table"])?;
    let object = payload.as_object().ok_or_else(|| {
        format!(
            "Fixture message {} ({}) payload must be a JSON object",
            message.seq, message.message_type
        )
    })?;

    if require_before && object.get("before").is_none() {
        return Err(format!(
            "Fixture message {} ({}) missing required field 'before'",
            message.seq, message.message_type
        ));
    }

    if require_after && object.get("after").is_none() {
        return Err(format!(
            "Fixture message {} ({}) missing required field 'after'",
            message.seq, message.message_type
        ));
    }

    if let Some(primary_key) = object.get("primary_key") {
        let values = primary_key.as_array().ok_or_else(|| {
            format!(
                "Fixture message {} ({}) field 'primary_key' must be an array",
                message.seq, message.message_type
            )
        })?;

        if values.iter().any(|item| item.as_str().is_none()) {
            return Err(format!(
                "Fixture message {} ({}) field 'primary_key' must contain only strings",
                message.seq, message.message_type
            ));
        }
    }

    Ok(())
}

fn validate_mysql_query_event(
    payload: &serde_json::Value,
    message: &FixtureMessage,
) -> Result<(), String> {
    validate_object_payload(payload, message, &[])?;
    let object = payload.as_object().ok_or_else(|| {
        format!(
            "Fixture message {} ({}) payload must be a JSON object",
            message.seq, message.message_type
        )
    })?;
    let has_query = object
        .get("query")
        .and_then(serde_json::Value::as_str)
        .is_some();
    let has_sql = object
        .get("sql")
        .and_then(serde_json::Value::as_str)
        .is_some();

    if has_query || has_sql {
        Ok(())
    } else {
        Err(format!(
            "Fixture message {} ({}) must include either 'query' or 'sql'",
            message.seq, message.message_type
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_validates_contiguous_sequences() {
        let metadata = FixtureMetadata {
            id: "test".to_string(),
            source_type: "postgres".to_string(),
            protocol_version: "pgoutput_v2".to_string(),
            source_version: "postgres>=12".to_string(),
            fixture_version: 1,
            description: "Test fixture".to_string(),
            tags: vec![],
            expected_event_count: 2,
            captured_at: "2026-05-16T00:00:00Z".to_string(),
        };

        let msg1 = FixtureMessage {
            seq: 0,
            message_type: "Begin".to_string(),
            payload: "{}".to_string(),
            tags: vec![],
        };

        let mut msg2 = FixtureMessage {
            seq: 1,
            message_type: "Commit".to_string(),
            payload: "{}".to_string(),
            tags: vec![],
        };

        // Valid fixture
        let fixture = Fixture::new(metadata.clone(), vec![msg1.clone(), msg2.clone()]);
        assert!(fixture.validate().is_ok());

        // Non-contiguous sequence should err in validate()
        msg2.seq = 5;
        let mut bad_metadata = metadata.clone();
        bad_metadata.expected_event_count = 2;
        let fixture = Fixture::new(bad_metadata, vec![msg1, msg2]);
        assert!(fixture.validate().is_err());
    }

    #[test]
    fn fixture_serialization_round_trips() {
        let metadata = FixtureMetadata {
            id: "pg_insert_test".to_string(),
            source_type: "postgres".to_string(),
            protocol_version: "pgoutput_v2".to_string(),
            source_version: "postgres>=12".to_string(),
            fixture_version: 1,
            description: "Insert test".to_string(),
            tags: vec!["insert".to_string()],
            expected_event_count: 1,
            captured_at: "2026-05-16T00:00:00Z".to_string(),
        };

        let message = FixtureMessage {
            seq: 0,
            message_type: "Insert".to_string(),
            payload: r#"{"table": "test", "columns": ["id", "value"]}"#.to_string(),
            tags: vec![],
        };

        let fixture = Fixture::new(metadata, vec![message]);
        let json = fixture.to_json().unwrap();
        let deserialized = Fixture::from_json(&json).unwrap();

        assert_eq!(fixture.metadata.id, deserialized.metadata.id);
        assert_eq!(fixture.messages.len(), deserialized.messages.len());
    }

    #[test]
    fn fixture_validate_rejects_unknown_message_type_for_source() {
        let fixture = Fixture::new(
            FixtureMetadata {
                id: "bad".to_string(),
                source_type: "postgres".to_string(),
                protocol_version: "pgoutput_v2".to_string(),
                source_version: "postgres>=12".to_string(),
                fixture_version: 1,
                description: "bad fixture".to_string(),
                tags: vec![],
                expected_event_count: 1,
                captured_at: "2026-05-16T00:00:00Z".to_string(),
            },
            vec![FixtureMessage {
                seq: 0,
                message_type: "QueryEvent".to_string(),
                payload: "{}".to_string(),
                tags: vec![],
            }],
        );

        assert!(fixture.validate().is_err());
    }

    #[test]
    fn fixture_validate_rejects_invalid_dml_payload_shape() {
        let fixture = Fixture::new(
            FixtureMetadata {
                id: "bad_dml".to_string(),
                source_type: "mysql".to_string(),
                protocol_version: "binlog_v4".to_string(),
                source_version: "mysql=8.0".to_string(),
                fixture_version: 1,
                description: "bad fixture".to_string(),
                tags: vec![],
                expected_event_count: 1,
                captured_at: "2026-05-16T00:00:00Z".to_string(),
            },
            vec![FixtureMessage {
                seq: 0,
                message_type: "WriteRowsEvent".to_string(),
                payload: r#"{"schema":"inventory"}"#.to_string(),
                tags: vec![],
            }],
        );

        assert!(fixture.validate().is_err());
    }
}
