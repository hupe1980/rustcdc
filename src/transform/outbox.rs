//! Outbox helper transform and parsing result.

use async_trait::async_trait;
use serde_json::Value;

use crate::core::{Error, Event, Result};

use super::Transform;

#[derive(Debug, Clone, PartialEq)]
pub enum OutboxResult {
    IsOutboxEvent {
        aggregate_id: String,
        event_type: String,
        payload: Value,
    },
    NotOutboxEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxTransform {
    pub enabled: bool,
    pub outbox_table: String,
}

impl OutboxTransform {
    pub fn new(outbox_table: impl Into<String>) -> Self {
        Self {
            enabled: true,
            outbox_table: outbox_table.into(),
        }
    }

    pub fn apply_outbox(&self, event: &mut Event) -> Result<OutboxResult> {
        if !self.enabled || event.table != self.outbox_table {
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

#[async_trait]
impl Transform for OutboxTransform {
    async fn apply(&self, event: &mut Event) -> Result<bool> {
        let _ = self.apply_outbox(event)?;
        Ok(true)
    }

    fn name(&self) -> &str {
        "outbox"
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};

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
}
