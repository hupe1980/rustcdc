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

    /// Number of protocol messages this fixture contains.
    ///
    /// A checksum against accidental truncation, not a restatement of `messages.len()`. These
    /// fixtures are hand-maintained JSON, and an edit that drops a message from the array is
    /// otherwise invisible: replay simply produces fewer events, the golden is re-recorded to
    /// match, and the scenario the fixture was written to cover quietly stops being covered.
    ///
    /// It was previously named `message_count`, which said one thing and checked
    /// another. The count of *events* is not the count of *messages* — an aborted transaction
    /// discards its buffered events, so replay can legitimately produce fewer — and it was
    /// checked against `messages.len()` regardless. Worse, it was checked **only** in
    /// [`Fixture::new`], which the file-loading path never calls, so every fixture on disk
    /// carried an unverified number that a reader could reasonably trust.
    pub message_count: usize,

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
    /// Create a fixture from metadata and messages.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture is not structurally valid — see
    /// [`Fixture::validate`], which this runs. It used to `assert_eq!` the message count
    /// instead, panicking inside a library on caller-supplied data; every other constructor in
    /// this crate returns a `Result`, and a fixture builder is exactly the kind of caller that
    /// wants to report the problem rather than abort.
    pub fn new(metadata: FixtureMetadata, messages: Vec<FixtureMessage>) -> Result<Self, String> {
        let fixture = Self { metadata, messages };
        fixture.validate()?;
        Ok(fixture)
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
    ///
    /// Checks that the fixture has messages, that their sequence numbers are contiguous from
    /// zero, that [`FixtureMetadata::message_count`] agrees with the array, and that every
    /// payload has the shape its message type requires.
    ///
    /// The `message_count` check is the one that was missing. It existed only in
    /// [`Fixture::new`], which `from_path` and `from_json` do not call, so a fixture whose
    /// array had lost a message loaded and replayed happily — and the golden was then
    /// re-recorded around the smaller stream, retiring the scenario without a word.
    pub fn validate(&self) -> Result<(), String> {
        if self.messages.is_empty() {
            return Err("Fixture has no messages".to_string());
        }

        if self.messages.len() != self.metadata.message_count {
            return Err(format!(
                "fixture '{}' declares message_count {} but carries {} messages. Either a \
                 message was added or removed without updating the count, or the array was \
                 truncated — replaying the shorter stream would silently retire whatever \
                 scenario the missing messages covered",
                self.metadata.id,
                self.metadata.message_count,
                self.messages.len()
            ));
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
            message_count: 2,
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
        Fixture::new(metadata.clone(), vec![msg1.clone(), msg2.clone()])
            .expect("a contiguous, correctly-counted fixture is valid");

        // Non-contiguous sequence is refused rather than panicking.
        msg2.seq = 5;
        let error = Fixture::new(metadata.clone(), vec![msg1.clone(), msg2.clone()])
            .expect_err("a non-contiguous sequence must be refused");
        assert!(error.contains("Non-contiguous"), "{error}");
        msg2.seq = 1;

        // A declared count that disagrees with the array is refused, which is the check that
        // used to exist only in `new` and so never ran for a fixture loaded from a file.
        let mut miscounted = metadata.clone();
        miscounted.message_count = 3;
        let error = Fixture::new(miscounted, vec![msg1, msg2])
            .expect_err("a miscounted fixture must be refused");
        assert!(
            error.contains("message_count 3") && error.contains("carries 2"),
            "the error must name both numbers so an author can see which to change: {error}"
        );
    }

    /// The gap this closed: `message_count` was checked only in `Fixture::new`, and the loading
    /// path is `from_path` → `from_json`, which does not call it. So a fixture whose message
    /// array had lost an entry loaded and replayed happily, the golden was re-recorded around
    /// the shorter stream, and the scenario retired without a word.
    #[test]
    fn a_miscounted_fixture_is_refused_on_the_path_that_actually_loads_files() {
        let metadata = FixtureMetadata {
            id: "truncated".to_string(),
            source_type: "postgres".to_string(),
            protocol_version: "pgoutput_v2".to_string(),
            source_version: "postgres>=12".to_string(),
            fixture_version: 1,
            description: "Two messages declared, one present".to_string(),
            tags: vec![],
            message_count: 2,
            captured_at: "2026-08-09T00:00:00Z".to_string(),
        };
        let json = serde_json::to_string(&serde_json::json!({
            "metadata": metadata,
            "messages": [{ "seq": 0, "message_type": "Begin", "payload": "{}", "tags": [] }],
        }))
        .expect("serialises");

        // `from_json` itself stays a pure deserialisation, as its name says.
        let fixture = Fixture::from_json(&json).expect("the JSON is well-formed");
        let error = fixture
            .validate()
            .expect_err("validation must catch the truncation");
        assert!(error.contains("truncated"), "the id must be named: {error}");

        // And the replay entry point refuses it, which is what makes the check reachable.
        let error = crate::deterministic_replay::ReplaySession::new(fixture)
            .err()
            .expect("a replay session must refuse a miscounted fixture");
        assert!(
            error.contains("message_count 2") && error.contains("carries 1"),
            "{error}"
        );
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
            message_count: 1,
            captured_at: "2026-05-16T00:00:00Z".to_string(),
        };

        let message = FixtureMessage {
            seq: 0,
            message_type: "Insert".to_string(),
            // A valid Insert payload. This used to be `{"table":..,"columns":[..]}`, which is
            // not a shape any message type accepts — it passed only because `Fixture::new`
            // did not validate, so a round-trip test was round-tripping an invalid fixture.
            payload: r#"{"table":"test","after":{"id":"1","value":"x"}}"#.to_string(),
            tags: vec![],
        };

        let fixture = Fixture::new(metadata, vec![message]).expect("fixture is valid");
        let json = fixture.to_json().unwrap();
        let deserialized = Fixture::from_json(&json).unwrap();

        assert_eq!(fixture.metadata.id, deserialized.metadata.id);
        assert_eq!(fixture.messages.len(), deserialized.messages.len());
    }

    #[test]
    fn fixture_validate_rejects_unknown_message_type_for_source() {
        // Constructed directly rather than through `Fixture::new`, which now refuses an
        // invalid fixture — that refusal is the point, and this test needs an invalid one to
        // hand to `validate` on its own.
        let fixture = Fixture {
            metadata: FixtureMetadata {
                id: "bad".to_string(),
                source_type: "postgres".to_string(),
                protocol_version: "pgoutput_v2".to_string(),
                source_version: "postgres>=12".to_string(),
                fixture_version: 1,
                description: "bad fixture".to_string(),
                tags: vec![],
                message_count: 1,
                captured_at: "2026-05-16T00:00:00Z".to_string(),
            },
            messages: vec![FixtureMessage {
                seq: 0,
                message_type: "QueryEvent".to_string(),
                payload: "{}".to_string(),
                tags: vec![],
            }],
        };

        assert!(fixture.validate().is_err());
        assert!(
            Fixture::new(fixture.metadata.clone(), fixture.messages.clone()).is_err(),
            "the constructor must refuse what validate rejects, or the two disagree"
        );
    }

    #[test]
    fn fixture_validate_rejects_invalid_dml_payload_shape() {
        let fixture = Fixture {
            metadata: FixtureMetadata {
                id: "bad_dml".to_string(),
                source_type: "mysql".to_string(),
                protocol_version: "binlog_v4".to_string(),
                source_version: "mysql=8.0".to_string(),
                fixture_version: 1,
                description: "bad fixture".to_string(),
                tags: vec![],
                message_count: 1,
                captured_at: "2026-05-16T00:00:00Z".to_string(),
            },
            messages: vec![FixtureMessage {
                seq: 0,
                message_type: "WriteRowsEvent".to_string(),
                payload: r#"{"schema":"inventory"}"#.to_string(),
                tags: vec![],
            }],
        };

        assert!(fixture.validate().is_err());
        assert!(Fixture::new(fixture.metadata.clone(), fixture.messages.clone()).is_err());
    }
}
