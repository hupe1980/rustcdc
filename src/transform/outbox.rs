//! Outbox helper transform and parsing result.

use serde_json::Value;

use crate::core::{Error, Event, Result};

use super::Transform;

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
/// Whether an event came from the configured outbox table, and what it carried.
pub enum OutboxResult {
    /// The event is an outbox record; its domain payload has been unwrapped.
    IsOutboxEvent {
        /// Aggregate the domain event belongs to — the natural partition key.
        aggregate_id: String,
        /// Domain event type, for downstream routing.
        event_type: String,
        /// The domain payload, unwrapped from the outbox row.
        payload: Value,
    },
    /// The event is not from the outbox table, or the transform is disabled.
    NotOutboxEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Unwraps the transactional-outbox pattern: turns an outbox row into its domain event.
///
/// Only `Insert` operations are treated as domain events — an outbox row is written once
/// and never updated, so an `Update` or `Delete` against the outbox table is housekeeping
/// (a cleanup job), not a new domain event.
pub struct OutboxTransform {
    /// When `false`, every event passes through untouched.
    pub enabled: bool,
    /// Table in which outbox rows are written.
    pub outbox_table: String,
}

impl OutboxTransform {
    /// Build an enabled transform for `outbox_table`.
    pub fn new(outbox_table: impl Into<String>) -> Self {
        Self {
            enabled: true,
            outbox_table: outbox_table.into(),
        }
    }

    /// Classify and unwrap an event.
    ///
    /// # Errors
    ///
    /// Returns an error if the row is missing the columns the outbox pattern requires.
    pub fn apply_outbox(&self, event: &mut Event) -> Result<OutboxResult> {
        if !self.enabled || event.table != self.outbox_table {
            return Ok(OutboxResult::NotOutboxEvent);
        }

        // Only Insert operations represent new domain events in the outbox pattern.
        // Delete / Update / Truncate operations on the outbox table are maintenance
        // rows (e.g., cleanup workers marking rows as processed) — pass them through
        // without modification rather than erroring.
        if !matches!(event.op, crate::core::Operation::Insert) {
            return Ok(OutboxResult::NotOutboxEvent);
        }

        let Some(Value::Object(after)) = event.after.as_ref() else {
            return Err(Error::TransformError(
                "outbox event requires object payload in after".into(),
            ));
        };

        let aggregate_id = after
            .get("aggregate_id")
            .ok_or_else(|| Error::TransformError("missing aggregate_id in outbox event".into()))?
            .as_str()
            .ok_or_else(|| Error::TransformError("aggregate_id must be a string".into()))?
            .to_string();

        let event_type = after
            .get("event_type")
            .ok_or_else(|| Error::TransformError("missing event_type in outbox event".into()))?
            .as_str()
            .ok_or_else(|| Error::TransformError("event_type must be a string".into()))?
            .to_string();

        let payload = after
            .get("payload")
            .ok_or_else(|| Error::TransformError("missing payload in outbox event".into()))?
            .clone();

        Ok(OutboxResult::IsOutboxEvent {
            aggregate_id,
            event_type,
            payload,
        })
    }
}

impl Transform for OutboxTransform {
    /// For events from the configured outbox table:
    /// - Rewrites `event.table` to the extracted `event_type` (analogous to
    ///   Debezium's Outbox Event Router routing to a topic per aggregate type).
    /// - Replaces `event.after` with the extracted business-event `payload`.
    ///
    /// Events from other tables pass through unchanged.
    fn apply(&self, event: &mut Event) -> Result<bool> {
        match self.apply_outbox(event)? {
            OutboxResult::NotOutboxEvent => Ok(true),
            OutboxResult::IsOutboxEvent {
                aggregate_id,
                event_type,
                payload,
            } => {
                // Rewrite table to event_type — mirrors Debezium Outbox Event Router
                // topic-per-aggregate-type routing.
                event.table = event_type;
                // Expose the business payload as the event body.
                event.after = Some(payload);
                // Expose aggregate_id as the primary key so sinks can use it as a
                // partition / ordering key (e.g., Kafka partition key for per-aggregate
                // ordering guarantees).
                event.primary_key = Some(vec![aggregate_id]);
                Ok(true)
            }
        }
    }

    fn name(&self) -> &str {
        "outbox"
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use crate::transform::Transform;

    use super::{OutboxResult, OutboxTransform};

    fn event(table: &str, after: serde_json::Value) -> Event {
        Event {
            before: None,
            after: Some(after),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "1".into(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".into()),
            table: table.into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    #[test]
    fn outbox_event_is_detected_and_parsed() {
        let transform = OutboxTransform::new("outbox");
        let mut event = event(
            "outbox",
            json!({"aggregate_id": "u1", "event_type": "user.created", "payload": {"id": 1}}),
        );

        let parsed = transform.apply_outbox(&mut event).unwrap();
        assert!(matches!(parsed, OutboxResult::IsOutboxEvent { .. }));
    }

    #[test]
    fn regular_event_returns_not_outbox() {
        let transform = OutboxTransform::new("outbox");
        let mut event = event("users", json!({"id": 1}));

        let parsed = transform.apply_outbox(&mut event).unwrap();
        assert_eq!(parsed, OutboxResult::NotOutboxEvent);
    }

    #[test]
    fn missing_outbox_fields_error() {
        let transform = OutboxTransform::new("outbox");
        let mut event = event("outbox", json!({"aggregate_id": "u1"}));
        assert!(transform.apply_outbox(&mut event).is_err());
    }

    // ── Transform trait integration ───────────────────────────────────────────

    #[tokio::test]
    async fn apply_rewrites_table_and_payload_for_outbox_events() {
        let transform = OutboxTransform::new("outbox");
        let mut e = event(
            "outbox",
            json!({
                "aggregate_id": "order-42",
                "event_type":   "order.placed",
                "payload":      {"order_id": 42, "total": 99.9}
            }),
        );

        assert!(transform.apply(&mut e).unwrap());
        // event.table must become the event_type.
        assert_eq!(e.table, "order.placed");
        // event.after must be the extracted business payload only.
        assert_eq!(e.after.unwrap(), json!({"order_id": 42, "total": 99.9}));
    }

    #[tokio::test]
    async fn apply_sets_aggregate_id_as_primary_key() {
        let transform = OutboxTransform::new("outbox");
        let mut e = event(
            "outbox",
            json!({"aggregate_id": "order-42", "event_type": "order.placed", "payload": {}}),
        );
        transform.apply(&mut e).unwrap();
        assert_eq!(
            e.primary_key.as_deref(),
            Some([String::from("order-42")].as_slice()),
            "aggregate_id must become the primary key for partition ordering"
        );
    }

    #[tokio::test]
    async fn apply_passes_through_non_outbox_events_unchanged() {
        let transform = OutboxTransform::new("outbox");
        let payload = json!({"id": 7});
        let mut e = event("users", payload.clone());

        assert!(transform.apply(&mut e).unwrap());
        // Nothing modified.
        assert_eq!(e.table, "users");
        assert_eq!(e.after.unwrap(), payload);
    }

    #[tokio::test]
    async fn apply_errors_on_malformed_outbox_event() {
        let transform = OutboxTransform::new("outbox");
        let mut e = event("outbox", json!({"aggregate_id": "x"}));
        assert!(
            transform.apply(&mut e).is_err(),
            "missing event_type must yield an error"
        );
    }

    // ── Operation guard: Delete / Update / Truncate on outbox table ──────────

    #[tokio::test]
    async fn delete_on_outbox_table_passes_through_without_error() {
        let transform = OutboxTransform::new("outbox");
        // Cleanup worker deletes a processed row — after = None.
        let mut e = Event {
            before: Some(json!({"aggregate_id": "u1", "event_type": "x", "payload": {}})),
            after: None,
            op: Operation::Delete,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "5".into(),
                timestamp: 5,
            },
            ts: 5,
            schema: Some("public".into()),
            table: "outbox".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        };
        // Must not error — cleanup rows must be passed through (or filtered upstream).
        assert!(
            transform.apply(&mut e).unwrap(),
            "Delete on outbox table must pass through"
        );
        assert_eq!(e.table, "outbox", "table must remain unchanged");
    }

    #[tokio::test]
    async fn truncate_on_outbox_table_passes_through_without_error() {
        let transform = OutboxTransform::new("outbox");
        let mut e = Event {
            before: None,
            after: None,
            op: Operation::Truncate,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "6".into(),
                timestamp: 6,
            },
            ts: 6,
            schema: Some("public".into()),
            table: "outbox".into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        };
        assert!(
            transform.apply(&mut e).unwrap(),
            "Truncate on outbox table must not error"
        );
        assert_eq!(
            e.table, "outbox",
            "table must remain unchanged for Truncate"
        );
    }

    #[tokio::test]
    async fn update_on_outbox_table_passes_through_without_processing() {
        let transform = OutboxTransform::new("outbox");
        // Update represents marking a row as processed — should not be re-emitted.
        let mut e = Event {
            before: Some(
                json!({"aggregate_id": "u1", "event_type": "user.created", "payload": {}}),
            ),
            after: Some(
                json!({"aggregate_id": "u1", "event_type": "user.created", "payload": {}, "processed": true}),
            ),
            op: Operation::Update,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "7".into(),
                timestamp: 7,
            },
            ts: 7,
            schema: Some("public".into()),
            table: "outbox".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        };
        assert!(transform.apply(&mut e).unwrap());
        // Table must NOT be rewritten — Update should pass through unchanged.
        assert_eq!(e.table, "outbox");
    }
}
