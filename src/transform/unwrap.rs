//! Flatten nested JSON payloads for before/after sections.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::core::{Error, Event, Result};

use super::Transform;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnwrapConfig {
    pub enabled: bool,
    pub max_depth: u8,
    pub flatten_arrays: bool,
}

impl Default for UnwrapConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_depth: 10,
            flatten_arrays: false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct UnwrapTransform {
    pub config: UnwrapConfig,
}

impl UnwrapTransform {
    pub fn new(config: UnwrapConfig) -> Self {
        Self { config }
    }

    pub fn apply_event(&self, event: &mut Event) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        flatten_payload(&mut event.before, &self.config)?;
        flatten_payload(&mut event.after, &self.config)?;
        Ok(())
    }
}

#[async_trait]
impl Transform for UnwrapTransform {
    async fn apply(&self, event: &mut Event) -> Result<bool> {
        self.apply_event(event)?;
        Ok(true)
    }

    fn name(&self) -> &str {
        "unwrap"
    }
}

fn flatten_payload(payload: &mut Option<Value>, config: &UnwrapConfig) -> Result<()> {
    let Some(Value::Object(object)) = payload else {
        return Ok(());
    };

    let mut flat = BTreeMap::new();
    for (key, value) in object.iter() {
        flatten_into(&mut flat, key, value, 1, config)?;
    }

    let mut out = Map::new();
    for (key, value) in flat {
        out.insert(key, value);
    }
    *payload = Some(Value::Object(out));
    Ok(())
}

fn flatten_into(
    out: &mut BTreeMap<String, Value>,
    key: &str,
    value: &Value,
    depth: u8,
    config: &UnwrapConfig,
) -> Result<()> {
    if depth > config.max_depth {
        return Err(Error::TransformError(format!(
            "unwrap depth exceeded max_depth={} at key={key}",
            config.max_depth
        )));
    }

    match value {
        Value::Object(map) => {
            if map.is_empty() {
                out.insert(key.to_string(), Value::Object(Map::new()));
                return Ok(());
            }
            for (child_key, child_value) in map {
                let child = format!("{key}.{child_key}");
                flatten_into(out, &child, child_value, depth.saturating_add(1), config)?;
            }
        }
        Value::Array(items) if config.flatten_arrays => {
            if items.is_empty() {
                out.insert(key.to_string(), Value::Array(Vec::new()));
                return Ok(());
            }
            for (index, item) in items.iter().enumerate() {
                let child = format!("{key}.{index}");
                flatten_into(out, &child, item, depth.saturating_add(1), config)?;
            }
        }
        _ => {
            out.insert(key.to_string(), value.clone());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};

    use super::{UnwrapConfig, UnwrapTransform};

    fn event(after: serde_json::Value) -> Event {
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
            table: "users".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    #[test]
    fn flat_object_is_unchanged() {
        let transform = UnwrapTransform::default();
        let mut event = event(json!({"id": 1, "name": "a"}));
        transform.apply_event(&mut event).unwrap();
        assert_eq!(event.after.unwrap(), json!({"id": 1, "name": "a"}));
    }

    #[test]
    fn nested_object_is_flattened() {
        let transform = UnwrapTransform::default();
        let mut event = event(json!({"user": {"id": 1, "name": "alice"}}));
        transform.apply_event(&mut event).unwrap();
        assert_eq!(
            event.after.unwrap(),
            json!({"user.id": 1, "user.name": "alice"})
        );
    }

    #[test]
    fn arrays_are_flattened_when_enabled() {
        let transform = UnwrapTransform::new(UnwrapConfig {
            flatten_arrays: true,
            ..UnwrapConfig::default()
        });
        let mut event = event(json!({"tags": [1, 2]}));
        transform.apply_event(&mut event).unwrap();
        assert_eq!(event.after.unwrap(), json!({"tags.0": 1, "tags.1": 2}));
    }

    #[test]
    fn max_depth_is_enforced() {
        let transform = UnwrapTransform::new(UnwrapConfig {
            max_depth: 2,
            ..UnwrapConfig::default()
        });
        let mut event = event(json!({"a": {"b": {"c": 1}}}));
        assert!(transform.apply_event(&mut event).is_err());
    }

    #[test]
    fn unwrap_is_deterministic() {
        let transform = UnwrapTransform::new(UnwrapConfig {
            flatten_arrays: true,
            ..UnwrapConfig::default()
        });
        let mut first = event(json!({"user": {"name": "alice"}, "tags": [1, 2]}));
        let mut second = first.clone();

        transform.apply_event(&mut first).unwrap();
        transform.apply_event(&mut second).unwrap();

        assert_eq!(first.after, second.after);
    }
}
