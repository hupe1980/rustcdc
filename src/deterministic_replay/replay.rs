use super::fixtures::{Fixture, FixtureMessage};
/// Replay harness for executing fixtures deterministically.
///
/// Converts protocol-level fixtures into canonical CDC events and validates
/// that protocol message interpretation remains consistent across versions.
use crate::{
    core::{Event, Operation, SourceMetadata, TransactionMetadata, EVENT_ENVELOPE_VERSION},
    ddl_capture::{extract_captured_ddl, DdlDialect},
};
use serde::{Deserialize, Serialize};

const FIXTURE_TS_BASE_MS: u64 = 1_700_000_000_000;

/// A replayed canonical event with trace metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayEvent {
    /// Canonical event produced by protocol interpretation
    pub event: Event,

    /// Source fixture message sequence number
    pub fixture_seq: usize,

    /// Message type that produced this event
    pub source_message_type: String,
}

/// Result of replaying a single fixture.
#[derive(Debug, Clone)]
pub struct ReplayResult {
    /// Fixture identifier
    pub fixture_id: String,

    /// Total events replayed
    pub event_count: usize,

    /// Events by operation (for distribution analysis)
    pub events_by_op: std::collections::BTreeMap<String, usize>,

    /// Any validation errors encountered
    pub errors: Vec<String>,

    /// Whether replay succeeded
    pub success: bool,
}

/// Deterministic replay session.
pub struct ReplaySession {
    fixture: Fixture,
    events: Vec<ReplayEvent>,
}

struct ActiveTransaction {
    tx_id: u64,
    buffered: Vec<ReplayEvent>,
}

impl ReplaySession {
    /// Create a new replay session from a fixture.
    pub fn new(fixture: Fixture) -> Result<Self, String> {
        fixture.validate()?;
        Ok(Self {
            fixture,
            events: Vec::new(),
        })
    }

    /// Execute fixture replay.
    ///
    /// This interprets protocol messages according to the fixture's source type
    /// and produces canonical events as if they came from the live connector.
    pub fn replay(&mut self) -> ReplayResult {
        let mut errors = Vec::new();
        let mut events_by_op = std::collections::BTreeMap::new();
        let mut active_transaction: Option<ActiveTransaction> = None;
        self.events.clear();

        for message in &self.fixture.messages {
            if self.is_transaction_begin(message) {
                if let Some(transaction) = active_transaction.take() {
                    errors.push(format!(
                        "Message {}: encountered transaction begin while transaction {} was still open; discarded {} buffered events",
                        message.seq,
                        transaction.tx_id,
                        transaction.buffered.len()
                    ));
                }

                match self.interpret_message(message) {
                    Ok(event) => {
                        let op_str = format!("{:?}", event.event.op);
                        *events_by_op.entry(op_str).or_insert(0) += 1;
                        self.events.push(event);
                        active_transaction = Some(ActiveTransaction {
                            tx_id: message.seq as u64,
                            buffered: Vec::new(),
                        });
                    }
                    Err(e) => errors.push(format!("Message {}: {}", message.seq, e)),
                }
                continue;
            }

            if self.is_transaction_abort(message) {
                let discarded_events = active_transaction
                    .take()
                    .map(|transaction| transaction.buffered.len())
                    .unwrap_or(0);

                match self.interpret_message(message) {
                    Ok(mut event) => {
                        Self::annotate_transaction_abort(&mut event.event, discarded_events);
                        let op_str = format!("{:?}", event.event.op);
                        *events_by_op.entry(op_str).or_insert(0) += 1;
                        self.events.push(event);
                    }
                    Err(e) => errors.push(format!("Message {}: {}", message.seq, e)),
                }
                continue;
            }

            if self.is_transaction_commit(message) {
                if let Some(mut transaction) = active_transaction.take() {
                    Self::flush_transaction_events(
                        &mut transaction,
                        &mut events_by_op,
                        &mut self.events,
                    );
                } else if !self.allows_autocommit_boundary(message) {
                    errors.push(format!(
                        "Message {}: encountered transaction commit without an active transaction",
                        message.seq
                    ));
                    continue;
                }

                match self.interpret_message(message) {
                    Ok(event) => {
                        let op_str = format!("{:?}", event.event.op);
                        *events_by_op.entry(op_str).or_insert(0) += 1;
                        self.events.push(event);
                    }
                    Err(e) => errors.push(format!("Message {}: {}", message.seq, e)),
                }
                continue;
            }

            match self.interpret_message(message) {
                Ok(event) => {
                    if self.should_buffer_in_transaction(&event.event) {
                        if let Some(transaction) = active_transaction.as_mut() {
                            transaction.buffered.push(event);
                        } else {
                            let op_str = format!("{:?}", event.event.op);
                            *events_by_op.entry(op_str).or_insert(0) += 1;
                            self.events.push(event);
                        }
                    } else {
                        let op_str = format!("{:?}", event.event.op);
                        *events_by_op.entry(op_str).or_insert(0) += 1;
                        self.events.push(event);
                    }
                }
                Err(e) => {
                    errors.push(format!("Message {}: {}", message.seq, e));
                }
            }
        }

        if let Some(transaction) = active_transaction.take() {
            if !transaction.buffered.is_empty() {
                errors.push(format!(
                    "Transaction {} was not committed before end of fixture; discarded {} buffered events",
                    transaction.tx_id,
                    transaction.buffered.len()
                ));
            }
        }

        ReplayResult {
            fixture_id: self.fixture.metadata.id.clone(),
            event_count: self.events.len(),
            events_by_op,
            errors: errors.clone(),
            success: errors.is_empty(),
        }
    }

    /// Get replayed events in order.
    pub fn events(&self) -> &[ReplayEvent] {
        &self.events
    }

    /// Interpret a fixture message as a canonical event.
    ///
    /// This is the core of the replay engine: protocol-specific message parsing.
    fn interpret_message(&self, message: &FixtureMessage) -> Result<ReplayEvent, String> {
        let source_type = &self.fixture.metadata.source_type;

        let event = match source_type.as_str() {
            "postgres" => self.interpret_pgoutput_message(message)?,
            "mysql" => self.interpret_mysql_message(message)?,
            "sqlserver" => self.interpret_sqlserver_message(message)?,
            _ => return Err(format!("Unknown source type: {}", source_type)),
        };

        Ok(ReplayEvent {
            event,
            fixture_seq: message.seq,
            source_message_type: message.message_type.clone(),
        })
    }

    /// Interpret PostgreSQL pgoutput protocol message.
    fn interpret_pgoutput_message(&self, message: &FixtureMessage) -> Result<Event, String> {
        match message.message_type.as_str() {
            "Begin" => Ok(self.create_marker_event(
                message.seq,
                "transaction_begin",
                &message.message_type,
            )),
            "Insert" => self.create_data_event(message, Operation::Insert),
            "Update" => self.create_data_event(message, Operation::Update),
            "Delete" => self.create_data_event(message, Operation::Delete),
            "Commit" => {
                Ok(self.create_marker_event(message.seq, "transaction_end", &message.message_type))
            }
            "Ddl" => self.create_ddl_event(message, DdlDialect::Postgres, "statement"),
            _ => Err(format!(
                "Unknown pgoutput message type: {}",
                message.message_type
            )),
        }
    }

    /// Interpret MySQL binlog protocol message.
    fn interpret_mysql_message(&self, message: &FixtureMessage) -> Result<Event, String> {
        match message.message_type.as_str() {
            "QueryEvent" => self.interpret_mysql_query_event(message),
            "WriteRowsEvent" => self.create_data_event(message, Operation::Insert),
            "UpdateRowsEvent" => self.create_data_event(message, Operation::Update),
            "DeleteRowsEvent" => self.create_data_event(message, Operation::Delete),
            "XidEvent" => {
                Ok(self.create_marker_event(message.seq, "transaction_end", &message.message_type))
            }
            _ => Err(format!(
                "Unknown MySQL message type: {}",
                message.message_type
            )),
        }
    }

    /// Interpret SQL Server CDC protocol message.
    fn interpret_sqlserver_message(&self, message: &FixtureMessage) -> Result<Event, String> {
        match message.message_type.as_str() {
            "Capture" => self.create_data_event(message, Operation::Insert),
            "Update" => self.create_data_event(message, Operation::Update),
            "Delete" => self.create_data_event(message, Operation::Delete),
            "Control" => self.interpret_sqlserver_control_event(message),
            "Ddl" => self.create_ddl_event(message, DdlDialect::SqlServer, "statement"),
            _ => Err(format!(
                "Unknown SQL Server message type: {}",
                message.message_type
            )),
        }
    }

    fn interpret_sqlserver_control_event(&self, message: &FixtureMessage) -> Result<Event, String> {
        let marker_type = match sqlserver_control_kind(message).as_deref() {
            Some("begin_transaction") => "transaction_begin",
            Some("commit_transaction") => "transaction_end",
            Some("rollback_transaction") => "transaction_abort",
            _ => "control_event",
        };

        Ok(self.create_marker_event(message.seq, marker_type, &message.message_type))
    }

    fn interpret_mysql_query_event(&self, message: &FixtureMessage) -> Result<Event, String> {
        let payload = self.parse_payload(message)?;
        let query = payload
            .get("query")
            .or_else(|| payload.get("sql"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                "mysql QueryEvent payload missing string field 'query' or 'sql'".to_string()
            })?;

        let normalized = query.trim().trim_end_matches(';').trim();

        if normalized.eq_ignore_ascii_case("BEGIN") {
            return Ok(self.create_marker_event(
                message.seq,
                "transaction_begin",
                &message.message_type,
            ));
        }

        if normalized.eq_ignore_ascii_case("COMMIT") {
            return Ok(self.create_marker_event(
                message.seq,
                "transaction_end",
                &message.message_type,
            ));
        }

        if normalized.eq_ignore_ascii_case("ROLLBACK") {
            return Ok(self.create_marker_event(
                message.seq,
                "transaction_abort",
                &message.message_type,
            ));
        }

        if extract_captured_ddl(DdlDialect::Mysql, normalized).is_some() {
            self.create_ddl_event(message, DdlDialect::Mysql, "query")
        } else {
            Ok(self.create_marker_event(message.seq, "binlog_event", &message.message_type))
        }
    }

    /// Create a marker event (metadata, transaction boundaries, etc.)
    fn create_marker_event(&self, seq: usize, marker_type: &str, source_message: &str) -> Event {
        let ts = self.fixture_timestamp(seq);
        Event {
            before: None,
            after: Some(serde_json::json!({
                "marker_type": marker_type,
                "fixture_seq": seq,
                "source_message": source_message,
            })),
            op: Operation::Read,
            source: SourceMetadata {
                source_name: self.fixture.metadata.source_type.clone(),
                offset: format!("fixture_seq_{}", seq),
                timestamp: ts,
            },
            ts,
            schema: None,
            table: format!("__marker__{}", marker_type),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    /// Create a data event (DML operation).
    fn create_data_event(&self, message: &FixtureMessage, op: Operation) -> Result<Event, String> {
        let payload = self.parse_data_payload(message)?;
        let ts = self.fixture_timestamp(message.seq);
        Ok(Event {
            before: payload.before,
            after: payload.after,
            op,
            source: SourceMetadata {
                source_name: self.fixture.metadata.source_type.clone(),
                offset: format!("fixture_seq_{}", message.seq),
                timestamp: ts,
            },
            ts,
            schema: payload.schema,
            table: payload.table,
            primary_key: payload.primary_key,
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        })
    }

    fn create_ddl_event(
        &self,
        message: &FixtureMessage,
        dialect: DdlDialect,
        field_name: &str,
    ) -> Result<Event, String> {
        let payload = self.parse_payload(message)?;
        let statement = payload
            .get(field_name)
            .or_else(|| {
                if dialect == DdlDialect::Mysql && field_name == "query" {
                    payload.get("sql")
                } else {
                    None
                }
            })
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("ddl payload missing string field '{field_name}'"))?;
        let captured = extract_captured_ddl(dialect, statement)
            .ok_or_else(|| format!("unsupported DDL statement for replay: {statement}"))?;
        Ok(captured.to_event(
            &self.fixture.metadata.source_type,
            format!("fixture_seq_{}", message.seq),
            self.fixture_timestamp(message.seq),
        ))
    }

    fn parse_data_payload(&self, message: &FixtureMessage) -> Result<ReplayDataPayload, String> {
        let payload = self.parse_payload(message)?;
        let table = payload
            .get("table")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "data payload missing string field 'table'".to_string())?
            .to_string();
        let schema = payload
            .get("schema")
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string);
        let before = payload
            .get("before")
            .cloned()
            .filter(|value| !value.is_null());
        let after = payload
            .get("after")
            .cloned()
            .filter(|value| !value.is_null());
        let primary_key = payload
            .get("primary_key")
            .map(parse_primary_key)
            .transpose()?;

        Ok(ReplayDataPayload {
            before,
            after,
            schema,
            table,
            primary_key,
        })
    }

    fn parse_payload(&self, message: &FixtureMessage) -> Result<serde_json::Value, String> {
        serde_json::from_str(&message.payload).map_err(|error| {
            format!(
                "invalid JSON payload for message type '{}': {error}",
                message.message_type
            )
        })
    }

    fn fixture_timestamp(&self, seq: usize) -> u64 {
        FIXTURE_TS_BASE_MS + seq as u64
    }

    fn is_transaction_begin(&self, message: &FixtureMessage) -> bool {
        match self.fixture.metadata.source_type.as_str() {
            "postgres" => message.message_type == "Begin",
            "mysql" => mysql_query_matches(message, "BEGIN"),
            "sqlserver" => sqlserver_control_matches(message, "begin_transaction"),
            _ => false,
        }
    }

    fn is_transaction_commit(&self, message: &FixtureMessage) -> bool {
        match self.fixture.metadata.source_type.as_str() {
            "postgres" => message.message_type == "Commit",
            "mysql" => message.message_type == "XidEvent" || mysql_query_matches(message, "COMMIT"),
            "sqlserver" => sqlserver_control_matches(message, "commit_transaction"),
            _ => false,
        }
    }

    fn is_transaction_abort(&self, message: &FixtureMessage) -> bool {
        match self.fixture.metadata.source_type.as_str() {
            "mysql" => mysql_query_matches(message, "ROLLBACK"),
            "sqlserver" => sqlserver_control_matches(message, "rollback_transaction"),
            _ => false,
        }
    }

    fn should_buffer_in_transaction(&self, event: &Event) -> bool {
        matches!(
            event.op,
            Operation::Insert | Operation::Update | Operation::Delete | Operation::SchemaChange
        )
    }

    fn allows_autocommit_boundary(&self, message: &FixtureMessage) -> bool {
        self.fixture.metadata.source_type == "mysql" && message.message_type == "XidEvent"
    }

    fn flush_transaction_events(
        transaction: &mut ActiveTransaction,
        events_by_op: &mut std::collections::BTreeMap<String, usize>,
        target: &mut Vec<ReplayEvent>,
    ) {
        let total_events = transaction.buffered.len() as u32;

        for (event_index, mut replay_event) in transaction.buffered.drain(..).enumerate() {
            replay_event.event.transaction = Some(TransactionMetadata {
                tx_id: transaction.tx_id,
                total_events,
                event_index: event_index as u32,
            });

            let op_str = format!("{:?}", replay_event.event.op);
            *events_by_op.entry(op_str).or_insert(0) += 1;
            target.push(replay_event);
        }
    }

    fn annotate_transaction_abort(event: &mut Event, discarded_events: usize) {
        let Some(after) = event
            .after
            .as_mut()
            .and_then(serde_json::Value::as_object_mut)
        else {
            return;
        };

        after.insert(
            "discarded_events".to_string(),
            serde_json::Value::from(discarded_events as u64),
        );
    }
}

struct ReplayDataPayload {
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    schema: Option<String>,
    table: String,
    primary_key: Option<Vec<String>>,
}

fn parse_primary_key(value: &serde_json::Value) -> Result<Vec<String>, String> {
    let array = value
        .as_array()
        .ok_or_else(|| "primary_key payload must be an array of strings".to_string())?;
    array
        .iter()
        .map(|item| {
            item.as_str()
                .map(ToString::to_string)
                .ok_or_else(|| "primary_key entries must be strings".to_string())
        })
        .collect()
}

fn mysql_query_matches(message: &FixtureMessage, expected: &str) -> bool {
    if message.message_type != "QueryEvent" {
        return false;
    }

    let Ok(payload) = serde_json::from_str::<serde_json::Value>(&message.payload) else {
        return false;
    };

    payload
        .get("query")
        .or_else(|| payload.get("sql"))
        .and_then(serde_json::Value::as_str)
        .map(|value| {
            value
                .trim()
                .trim_end_matches(';')
                .trim()
                .eq_ignore_ascii_case(expected)
        })
        .unwrap_or(false)
}

fn sqlserver_control_kind(message: &FixtureMessage) -> Option<String> {
    if message.message_type != "Control" {
        return None;
    }

    let Ok(payload) = serde_json::from_str::<serde_json::Value>(&message.payload) else {
        return None;
    };

    payload
        .get("kind")
        .or_else(|| payload.get("control_kind"))
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
}

fn sqlserver_control_matches(message: &FixtureMessage, expected: &str) -> bool {
    sqlserver_control_kind(message)
        .map(|kind| kind.eq_ignore_ascii_case(expected))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deterministic_replay::fixtures::FixtureMetadata;

    #[test]
    fn replay_session_interprets_pgoutput_messages() {
        let metadata = FixtureMetadata {
            id: "test_pg".to_string(),
            source_type: "postgres".to_string(),
            protocol_version: "pgoutput_v2".to_string(),
            source_version: "postgres>=12".to_string(),
            fixture_version: 1,
            description: "Test".to_string(),
            tags: vec![],
            expected_event_count: 3,
            captured_at: "2026-05-16T00:00:00Z".to_string(),
        };

        let messages = vec![
            FixtureMessage {
                seq: 0,
                message_type: "Begin".to_string(),
                payload: "{}".to_string(),
                tags: vec![],
            },
            FixtureMessage {
                seq: 1,
                message_type: "Insert".to_string(),
                payload: r#"{"schema":"public","table":"test","after":{"id":1}}"#.to_string(),
                tags: vec![],
            },
            FixtureMessage {
                seq: 2,
                message_type: "Commit".to_string(),
                payload: "{}".to_string(),
                tags: vec![],
            },
        ];

        let fixture = Fixture::new(metadata, messages);
        let mut session = ReplaySession::new(fixture).unwrap();
        let result = session.replay();

        assert!(result.success, "Replay should succeed");
        assert_eq!(result.event_count, 3);
        assert_eq!(session.events().len(), 3);
    }

    #[test]
    fn replay_session_is_idempotent_across_multiple_runs() {
        let metadata = FixtureMetadata {
            id: "test_repeat".to_string(),
            source_type: "postgres".to_string(),
            protocol_version: "pgoutput_v2".to_string(),
            source_version: "postgres>=12".to_string(),
            fixture_version: 1,
            description: "Test repeatability".to_string(),
            tags: vec![],
            expected_event_count: 1,
            captured_at: "2026-05-16T00:00:00Z".to_string(),
        };

        let fixture = Fixture::new(
            metadata,
            vec![FixtureMessage {
                seq: 0,
                message_type: "Insert".to_string(),
                payload: r#"{"schema":"public","table":"users","after":{"id":1}}"#.to_string(),
                tags: vec![],
            }],
        );

        let mut session = ReplaySession::new(fixture).unwrap();
        let first = session.replay();
        let second = session.replay();

        assert!(first.success);
        assert!(second.success);
        assert_eq!(session.events().len(), 1);
    }

    #[test]
    fn replay_session_interprets_dml_payloads_deterministically() {
        let metadata = FixtureMetadata {
            id: "test_pg_dml".to_string(),
            source_type: "postgres".to_string(),
            protocol_version: "pgoutput_v2".to_string(),
            source_version: "postgres>=12".to_string(),
            fixture_version: 1,
            description: "Test DML parsing".to_string(),
            tags: vec![],
            expected_event_count: 1,
            captured_at: "2026-05-16T00:00:00Z".to_string(),
        };

        let messages = vec![FixtureMessage {
            seq: 0,
            message_type: "Insert".to_string(),
            payload: r#"{"schema":"public","table":"users","after":{"id":1,"name":"alice"},"primary_key":["id"]}"#.to_string(),
            tags: vec![],
        }];

        let fixture = Fixture::new(metadata, messages);
        let mut session = ReplaySession::new(fixture).unwrap();
        let result = session.replay();

        assert!(result.success);
        let event = &session.events()[0].event;
        assert_eq!(event.ts, FIXTURE_TS_BASE_MS);
        assert_eq!(event.schema.as_deref(), Some("public"));
        assert_eq!(event.table, "users");
        assert_eq!(event.primary_key.as_ref().unwrap(), &vec!["id".to_string()]);
        assert_eq!(event.after.as_ref().unwrap()["name"], "alice");
    }

    #[test]
    fn replay_session_interprets_postgres_ddl_messages() {
        let metadata = FixtureMetadata {
            id: "test_pg_ddl".to_string(),
            source_type: "postgres".to_string(),
            protocol_version: "pgoutput_v2".to_string(),
            source_version: "postgres>=12".to_string(),
            fixture_version: 1,
            description: "Test DDL parsing".to_string(),
            tags: vec![],
            expected_event_count: 1,
            captured_at: "2026-05-16T00:00:00Z".to_string(),
        };

        let messages = vec![FixtureMessage {
            seq: 0,
            message_type: "Ddl".to_string(),
            payload: r#"{"statement":"ALTER TABLE public.users REPLICA IDENTITY FULL"}"#
                .to_string(),
            tags: vec![],
        }];

        let fixture = Fixture::new(metadata, messages);
        let mut session = ReplaySession::new(fixture).unwrap();
        let result = session.replay();

        assert!(result.success, "{:?}", result.errors);
        let event = &session.events()[0].event;
        assert_eq!(event.op, Operation::SchemaChange);
        assert_eq!(event.schema.as_deref(), Some("public"));
        assert_eq!(event.table, "users__ddl_events");
        assert!(event.after.as_ref().unwrap().get("schema_diff").is_some());
    }

    #[test]
    fn replay_session_preserves_marker_source_message_types() {
        let metadata = FixtureMetadata {
            id: "test_marker_source_message".to_string(),
            source_type: "postgres".to_string(),
            protocol_version: "pgoutput_v2".to_string(),
            source_version: "postgres>=12".to_string(),
            fixture_version: 1,
            description: "Test marker provenance".to_string(),
            tags: vec![],
            expected_event_count: 2,
            captured_at: "2026-05-21T00:00:00Z".to_string(),
        };

        let fixture = Fixture::new(
            metadata,
            vec![
                FixtureMessage {
                    seq: 0,
                    message_type: "Begin".to_string(),
                    payload: "{}".to_string(),
                    tags: vec![],
                },
                FixtureMessage {
                    seq: 1,
                    message_type: "Commit".to_string(),
                    payload: "{}".to_string(),
                    tags: vec![],
                },
            ],
        );

        let mut session = ReplaySession::new(fixture).unwrap();
        let result = session.replay();

        assert!(result.success, "{:?}", result.errors);
        assert_eq!(
            session.events()[0].event.after.as_ref().unwrap()["source_message"],
            "Begin"
        );
        assert_eq!(
            session.events()[1].event.after.as_ref().unwrap()["source_message"],
            "Commit"
        );
    }

    #[test]
    fn replay_session_interprets_mysql_sql_begin_as_transaction_boundary() {
        let metadata = FixtureMetadata {
            id: "test_mysql_begin".to_string(),
            source_type: "mysql".to_string(),
            protocol_version: "binlog_v4".to_string(),
            source_version: "mysql=8.0".to_string(),
            fixture_version: 1,
            description: "Test MySQL BEGIN interpretation".to_string(),
            tags: vec![],
            expected_event_count: 1,
            captured_at: "2026-05-21T00:00:00Z".to_string(),
        };

        let fixture = Fixture::new(
            metadata,
            vec![FixtureMessage {
                seq: 0,
                message_type: "QueryEvent".to_string(),
                payload: r#"{"sql":"BEGIN"}"#.to_string(),
                tags: vec![],
            }],
        );

        let mut session = ReplaySession::new(fixture).unwrap();
        let result = session.replay();

        assert!(result.success, "{:?}", result.errors);
        let event = &session.events()[0].event;
        assert_eq!(event.table, "__marker__transaction_begin");
        assert_eq!(
            event.after.as_ref().unwrap()["source_message"],
            "QueryEvent"
        );
    }

    #[test]
    fn replay_session_interprets_mysql_sql_field_as_ddl() {
        let metadata = FixtureMetadata {
            id: "test_mysql_sql_ddl".to_string(),
            source_type: "mysql".to_string(),
            protocol_version: "binlog_v4".to_string(),
            source_version: "mysql=8.0".to_string(),
            fixture_version: 1,
            description: "Test MySQL sql-field DDL parsing".to_string(),
            tags: vec![],
            expected_event_count: 1,
            captured_at: "2026-05-21T00:00:00Z".to_string(),
        };

        let fixture = Fixture::new(
            metadata,
            vec![FixtureMessage {
                seq: 0,
                message_type: "QueryEvent".to_string(),
                payload: r#"{"sql":"ALTER TABLE inventory.products ADD COLUMN priority INT"}"#
                    .to_string(),
                tags: vec![],
            }],
        );

        let mut session = ReplaySession::new(fixture).unwrap();
        let result = session.replay();

        assert!(result.success, "{:?}", result.errors);
        let event = &session.events()[0].event;
        assert_eq!(event.op, Operation::SchemaChange);
        assert_eq!(event.schema.as_deref(), Some("inventory"));
        assert_eq!(event.table, "products__ddl_events");
    }

    #[test]
    fn replay_session_rolls_back_buffered_mysql_transaction_events() {
        let metadata = FixtureMetadata {
            id: "test_mysql_rollback".to_string(),
            source_type: "mysql".to_string(),
            protocol_version: "binlog_v4".to_string(),
            source_version: "mysql=8.0".to_string(),
            fixture_version: 1,
            description: "Test MySQL rollback interpretation".to_string(),
            tags: vec![],
            expected_event_count: 4,
            captured_at: "2026-05-21T00:00:00Z".to_string(),
        };

        let fixture = Fixture::new(
            metadata,
            vec![
                FixtureMessage {
                    seq: 0,
                    message_type: "QueryEvent".to_string(),
                    payload: r#"{"sql":"BEGIN"}"#.to_string(),
                    tags: vec![],
                },
                FixtureMessage {
                    seq: 1,
                    message_type: "WriteRowsEvent".to_string(),
                    payload: r#"{"schema":"inventory","table":"products","after":{"id":10,"sku":"sku-10"},"primary_key":["id"]}"#.to_string(),
                    tags: vec![],
                },
                FixtureMessage {
                    seq: 2,
                    message_type: "QueryEvent".to_string(),
                    payload: r#"{"sql":"ROLLBACK"}"#.to_string(),
                    tags: vec![],
                },
                FixtureMessage {
                    seq: 3,
                    message_type: "WriteRowsEvent".to_string(),
                    payload: r#"{"schema":"inventory","table":"products","after":{"id":11,"sku":"sku-11"},"primary_key":["id"]}"#.to_string(),
                    tags: vec![],
                },
            ],
        );

        let mut session = ReplaySession::new(fixture).unwrap();
        let result = session.replay();

        assert!(result.success, "{:?}", result.errors);
        assert_eq!(session.events().len(), 3);
        assert_eq!(
            session.events()[0].event.table,
            "__marker__transaction_begin"
        );
        assert_eq!(
            session.events()[1].event.table,
            "__marker__transaction_abort"
        );
        assert_eq!(
            session.events()[1].event.after.as_ref().unwrap()["discarded_events"],
            1
        );
        assert_eq!(session.events()[2].event.table, "products");
        assert!(session.events()[2].event.transaction.is_none());
    }

    #[test]
    fn replay_session_assigns_transaction_metadata_to_committed_sqlserver_events() {
        let metadata = FixtureMetadata {
            id: "test_sqlserver_transaction_metadata".to_string(),
            source_type: "sqlserver".to_string(),
            protocol_version: "cdc_change_table_v1".to_string(),
            source_version: "sqlserver>=2019".to_string(),
            fixture_version: 1,
            description: "Test SQL Server control transaction metadata".to_string(),
            tags: vec![],
            expected_event_count: 4,
            captured_at: "2026-05-21T00:00:00Z".to_string(),
        };

        let fixture = Fixture::new(
            metadata,
            vec![
                FixtureMessage {
                    seq: 0,
                    message_type: "Control".to_string(),
                    payload: r#"{"kind":"begin_transaction","tx_id":9001}"#.to_string(),
                    tags: vec![],
                },
                FixtureMessage {
                    seq: 1,
                    message_type: "Capture".to_string(),
                    payload: r#"{"schema":"dbo","table":"orders","after":{"id":100,"status":"new"},"primary_key":["id"]}"#.to_string(),
                    tags: vec![],
                },
                FixtureMessage {
                    seq: 2,
                    message_type: "Update".to_string(),
                    payload: r#"{"schema":"dbo","table":"orders","before":{"id":100,"status":"new"},"after":{"id":100,"status":"packed"},"primary_key":["id"]}"#.to_string(),
                    tags: vec![],
                },
                FixtureMessage {
                    seq: 3,
                    message_type: "Control".to_string(),
                    payload: r#"{"kind":"commit_transaction","tx_id":9001}"#.to_string(),
                    tags: vec![],
                },
            ],
        );

        let mut session = ReplaySession::new(fixture).unwrap();
        let result = session.replay();

        assert!(result.success, "{:?}", result.errors);
        assert_eq!(session.events().len(), 4);
        assert_eq!(
            session.events()[0].event.table,
            "__marker__transaction_begin"
        );
        assert_eq!(
            session.events()[1]
                .event
                .transaction
                .as_ref()
                .unwrap()
                .total_events,
            2
        );
        assert_eq!(
            session.events()[2]
                .event
                .transaction
                .as_ref()
                .unwrap()
                .event_index,
            1
        );
        assert_eq!(session.events()[3].event.table, "__marker__transaction_end");
    }

    #[test]
    fn replay_session_assigns_transaction_metadata_to_committed_events() {
        let metadata = FixtureMetadata {
            id: "test_pg_transaction_metadata".to_string(),
            source_type: "postgres".to_string(),
            protocol_version: "pgoutput_v2".to_string(),
            source_version: "postgres>=12".to_string(),
            fixture_version: 1,
            description: "Test committed transaction metadata".to_string(),
            tags: vec![],
            expected_event_count: 4,
            captured_at: "2026-05-21T00:00:00Z".to_string(),
        };

        let fixture = Fixture::new(
            metadata,
            vec![
                FixtureMessage {
                    seq: 0,
                    message_type: "Begin".to_string(),
                    payload: "{}".to_string(),
                    tags: vec![],
                },
                FixtureMessage {
                    seq: 1,
                    message_type: "Insert".to_string(),
                    payload: r#"{"schema":"public","table":"users","after":{"id":1},"primary_key":["id"]}"#.to_string(),
                    tags: vec![],
                },
                FixtureMessage {
                    seq: 2,
                    message_type: "Update".to_string(),
                    payload: r#"{"schema":"public","table":"users","before":{"id":1},"after":{"id":1,"status":"active"},"primary_key":["id"]}"#.to_string(),
                    tags: vec![],
                },
                FixtureMessage {
                    seq: 3,
                    message_type: "Commit".to_string(),
                    payload: "{}".to_string(),
                    tags: vec![],
                },
            ],
        );

        let mut session = ReplaySession::new(fixture).unwrap();
        let result = session.replay();

        assert!(result.success, "{:?}", result.errors);
        assert_eq!(session.events().len(), 4);
        let first_tx = session.events()[1].event.transaction.as_ref().unwrap();
        let second_tx = session.events()[2].event.transaction.as_ref().unwrap();
        assert_eq!(first_tx.tx_id, 0);
        assert_eq!(first_tx.total_events, 2);
        assert_eq!(first_tx.event_index, 0);
        assert_eq!(second_tx.tx_id, 0);
        assert_eq!(second_tx.total_events, 2);
        assert_eq!(second_tx.event_index, 1);
    }

    #[test]
    fn replay_session_discards_uncommitted_transaction_events() {
        let metadata = FixtureMetadata {
            id: "test_uncommitted_tx".to_string(),
            source_type: "postgres".to_string(),
            protocol_version: "pgoutput_v2".to_string(),
            source_version: "postgres>=12".to_string(),
            fixture_version: 1,
            description: "Test incomplete transaction handling".to_string(),
            tags: vec![],
            expected_event_count: 2,
            captured_at: "2026-05-21T00:00:00Z".to_string(),
        };

        let fixture = Fixture::new(
            metadata,
            vec![
                FixtureMessage {
                    seq: 0,
                    message_type: "Begin".to_string(),
                    payload: "{}".to_string(),
                    tags: vec![],
                },
                FixtureMessage {
                    seq: 1,
                    message_type: "Insert".to_string(),
                    payload: r#"{"schema":"public","table":"users","after":{"id":1},"primary_key":["id"]}"#.to_string(),
                    tags: vec![],
                },
            ],
        );

        let mut session = ReplaySession::new(fixture).unwrap();
        let result = session.replay();

        assert!(!result.success);
        assert_eq!(session.events().len(), 1);
        assert_eq!(
            session.events()[0].event.table,
            "__marker__transaction_begin"
        );
        assert!(result
            .errors
            .iter()
            .any(|error| error.contains("not committed")));
    }
}
