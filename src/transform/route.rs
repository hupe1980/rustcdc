//! Route events to destination labels.

use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::core::{Error, Event, Result};

use super::Transform;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteConfig {
    pub routing_table: HashMap<String, String>,
    pub default_destination: String,
    pub add_destination_field: bool,
}

#[derive(Debug, Clone)]
pub struct RouteTransform {
    pub config: RouteConfig,
}

impl RouteTransform {
    pub fn new(config: RouteConfig) -> Self {
        Self { config }
    }

    fn destination_for(&self, table: &str) -> Result<String> {
        if let Some(mapped) = self.config.routing_table.get(table) {
            return Ok(mapped.clone());
        }
        if !self.config.default_destination.trim().is_empty() {
            return Ok(self.config.default_destination.clone());
        }
        Err(Error::TransformError(format!(
            "route transform missing destination for table={table}"
        )))
    }
}

#[async_trait]
impl Transform for RouteTransform {
    async fn apply(&self, event: &mut Event) -> Result<bool> {
        let destination = self.destination_for(&event.table)?;

        if self.config.add_destination_field {
            let target = event.after.get_or_insert_with(|| Value::Object(Map::new()));
            if let Value::Object(object) = target {
                object.insert("_destination".into(), Value::String(destination));
            }
        }

        Ok(true)
    }

    fn name(&self) -> &str {
        "route"
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use crate::transform::Transform;

    use super::{RouteConfig, RouteTransform};

    fn event(table: &str) -> Event {
        Event {
            before: None,
            after: Some(json!({"id": 1})),
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

    #[tokio::test]
    async fn routes_event_to_mapped_destination() {
        let mut map = HashMap::new();
        map.insert("users".into(), "topic-users".into());
        let transform = RouteTransform::new(RouteConfig {
            routing_table: map,
            default_destination: String::new(),
            add_destination_field: true,
        });

        let mut event = event("users");
        assert!(transform.apply(&mut event).await.unwrap());
        assert_eq!(event.after.unwrap()["_destination"], "topic-users");
    }

    #[tokio::test]
    async fn unmapped_table_uses_default_destination() {
        let transform = RouteTransform::new(RouteConfig {
            routing_table: HashMap::new(),
            default_destination: "topic-default".into(),
            add_destination_field: true,
        });

        let mut event = event("orders");
        assert!(transform.apply(&mut event).await.unwrap());
        assert_eq!(event.after.unwrap()["_destination"], "topic-default");
    }

    #[tokio::test]
    async fn missing_mapping_without_default_errors() {
        let transform = RouteTransform::new(RouteConfig::default());
        let mut event = event("orders");
        assert!(transform.apply(&mut event).await.is_err());
    }

    #[tokio::test]
    async fn routing_is_deterministic() {
        let mut map = HashMap::new();
        map.insert("users".into(), "topic-users".into());
        let transform = RouteTransform::new(RouteConfig {
            routing_table: map,
            default_destination: "topic-default".into(),
            add_destination_field: true,
        });

        let mut first = event("users");
        let mut second = event("users");
        assert!(transform.apply(&mut first).await.unwrap());
        assert!(transform.apply(&mut second).await.unwrap());
        assert_eq!(first.after, second.after);
    }
}
