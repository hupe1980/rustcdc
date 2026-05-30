//! Field mapping transform for copy/rename/set/remove operations.

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::core::{Error, Event, Result};

use super::Transform;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldMappingConfig {
    /// Copy value from source path to destination path.
    pub copy: Vec<(String, String)>,
    /// Move value from source path to destination path.
    pub rename: Vec<(String, String)>,
    /// Set a literal value at destination path.
    pub set_literals: Vec<(String, Value)>,
    /// Remove a field path.
    pub remove: Vec<String>,
    /// When enabled, missing source/remove paths return an error.
    pub strict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PathRule {
    raw: String,
    parts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MoveRule {
    from_raw: String,
    to_raw: String,
    from: Vec<String>,
    to: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct SetRule {
    to_raw: String,
    to: Vec<String>,
    value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldMappingTransform {
    pub config: FieldMappingConfig,
    copy_rules: Vec<MoveRule>,
    rename_rules: Vec<MoveRule>,
    set_rules: Vec<SetRule>,
    remove_rules: Vec<PathRule>,
}

impl FieldMappingTransform {
    pub fn new(config: FieldMappingConfig) -> Result<Self> {
        let copy_rules = config
            .copy
            .iter()
            .map(|(from, to)| {
                Ok(MoveRule {
                    from_raw: from.clone(),
                    to_raw: to.clone(),
                    from: parse_path(from)?,
                    to: parse_path(to)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let rename_rules = config
            .rename
            .iter()
            .map(|(from, to)| {
                Ok(MoveRule {
                    from_raw: from.clone(),
                    to_raw: to.clone(),
                    from: parse_path(from)?,
                    to: parse_path(to)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let set_rules = config
            .set_literals
            .iter()
            .map(|(to, value)| {
                Ok(SetRule {
                    to_raw: to.clone(),
                    to: parse_path(to)?,
                    value: value.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let remove_rules = config
            .remove
            .iter()
            .map(|path| {
                Ok(PathRule {
                    raw: path.clone(),
                    parts: parse_path(path)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            config,
            copy_rules,
            rename_rules,
            set_rules,
            remove_rules,
        })
    }

    fn apply_payload(&self, payload: &mut Option<Value>) -> Result<()> {
        if payload.is_none() && !self.set_rules.is_empty() {
            *payload = Some(Value::Object(Map::new()));
        }

        let Some(value) = payload else {
            return Ok(());
        };

        if !value.is_object() {
            return Err(Error::TransformError(
                "field_mapping requires object payloads".into(),
            ));
        }

        for rule in &self.copy_rules {
            match get_path(value, &rule.from).cloned() {
                Some(source) => set_path(value, &rule.to, source)?,
                None if self.config.strict => {
                    return Err(Error::TransformError(format!(
                        "field_mapping copy source path missing: {}",
                        rule.from_raw
                    )))
                }
                None => {}
            }
        }

        for rule in &self.rename_rules {
            match remove_path(value, &rule.from) {
                Some(source) => set_path(value, &rule.to, source)?,
                None if self.config.strict => {
                    return Err(Error::TransformError(format!(
                        "field_mapping rename source path missing: {}",
                        rule.from_raw
                    )))
                }
                None => {}
            }
        }

        for rule in &self.set_rules {
            set_path(value, &rule.to, rule.value.clone()).map_err(|error| {
                Error::TransformError(format!(
                    "field_mapping set path {} failed: {error}",
                    rule.to_raw
                ))
            })?;
        }

        for rule in &self.remove_rules {
            let removed = remove_path(value, &rule.parts);
            if removed.is_none() && self.config.strict {
                return Err(Error::TransformError(format!(
                    "field_mapping remove path missing: {}",
                    rule.raw
                )));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Transform for FieldMappingTransform {
    async fn apply(&self, event: &mut Event) -> Result<bool> {
        self.apply_payload(&mut event.before)?;
        self.apply_payload(&mut event.after)?;
        Ok(true)
    }

    fn name(&self) -> &str {
        "field_mapping"
    }
}

fn parse_path(path: &str) -> Result<Vec<String>> {
    let parts: Vec<String> = path
        .split('.')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect();

    if parts.is_empty() {
        return Err(Error::ConfigError(format!(
            "field path must not be empty: {path:?}"
        )));
    }

    Ok(parts)
}

fn get_path<'a>(root: &'a Value, parts: &[String]) -> Option<&'a Value> {
    let mut current = root;
    for part in parts {
        match current {
            Value::Object(object) => {
                current = object.get(part)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

fn set_path(root: &mut Value, parts: &[String], value: Value) -> Result<()> {
    let (last, parents) = parts
        .split_last()
        .ok_or_else(|| Error::ConfigError("path must not be empty".into()))?;

    let mut current = root;
    for part in parents {
        match current {
            Value::Object(object) => {
                if !object.contains_key(part) {
                    object.insert(part.clone(), Value::Object(Map::new()));
                }

                current = object.get_mut(part).ok_or_else(|| {
                    Error::TransformError(format!("failed to access path segment: {part}"))
                })?;

                if !current.is_object() {
                    return Err(Error::TransformError(format!(
                        "path segment is not an object: {part}"
                    )));
                }
            }
            _ => {
                return Err(Error::TransformError(
                    "cannot set nested path on non-object payload".into(),
                ));
            }
        }
    }

    match current {
        Value::Object(object) => {
            object.insert(last.clone(), value);
            Ok(())
        }
        _ => Err(Error::TransformError(
            "cannot set field on non-object payload".into(),
        )),
    }
}

fn remove_path(root: &mut Value, parts: &[String]) -> Option<Value> {
    let (last, parents) = parts.split_last()?;

    let mut current = root;
    for part in parents {
        current = match current {
            Value::Object(object) => object.get_mut(part)?,
            _ => return None,
        };
    }

    match current {
        Value::Object(object) => object.remove(last),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use crate::transform::Transform;

    use super::{FieldMappingConfig, FieldMappingTransform};

    fn event() -> Event {
        Event {
            before: Some(json!({
                "user": {"name": "old", "email": "old@example.com"},
                "legacy": true
            })),
            after: Some(json!({
                "id": 1,
                "user": {"name": "alice", "email": "alice@example.com"},
                "legacy": true
            })),
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

    #[tokio::test]
    async fn copy_rule_copies_nested_field() {
        let transform = FieldMappingTransform::new(FieldMappingConfig {
            copy: vec![("user.email".into(), "email".into())],
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut event = event();
        assert!(transform.apply(&mut event).await.unwrap());
        assert_eq!(event.after.unwrap()["email"], "alice@example.com");
    }

    #[tokio::test]
    async fn rename_rule_moves_field() {
        let transform = FieldMappingTransform::new(FieldMappingConfig {
            rename: vec![("user.name".into(), "user.full_name".into())],
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut event = event();
        assert!(transform.apply(&mut event).await.unwrap());
        let after = event.after.unwrap();
        assert_eq!(after["user"]["full_name"], "alice");
        assert!(after["user"].get("name").is_none());
    }

    #[tokio::test]
    async fn set_literal_creates_missing_path() {
        let transform = FieldMappingTransform::new(FieldMappingConfig {
            set_literals: vec![("meta.source".into(), json!("mysql"))],
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut event = event();
        assert!(transform.apply(&mut event).await.unwrap());
        assert_eq!(event.after.unwrap()["meta"]["source"], "mysql");
    }

    #[tokio::test]
    async fn remove_rule_deletes_field() {
        let transform = FieldMappingTransform::new(FieldMappingConfig {
            remove: vec!["legacy".into()],
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut event = event();
        assert!(transform.apply(&mut event).await.unwrap());
        assert!(event.after.unwrap().get("legacy").is_none());
    }

    #[tokio::test]
    async fn strict_mode_errors_on_missing_source_or_remove() {
        let transform = FieldMappingTransform::new(FieldMappingConfig {
            copy: vec![("missing".into(), "out".into())],
            strict: true,
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut first_event = event();
        assert!(transform.apply(&mut first_event).await.is_err());

        let transform = FieldMappingTransform::new(FieldMappingConfig {
            remove: vec!["missing".into()],
            strict: true,
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut second_event = event();
        assert!(transform.apply(&mut second_event).await.is_err());
    }

    #[tokio::test]
    async fn mapping_is_deterministic() {
        let transform = FieldMappingTransform::new(FieldMappingConfig {
            copy: vec![("user.email".into(), "email".into())],
            rename: vec![("user.name".into(), "user.full_name".into())],
            set_literals: vec![("meta.version".into(), json!(1))],
            remove: vec!["legacy".into()],
            strict: true,
        })
        .unwrap();

        let mut first = event();
        let mut second = event();
        assert!(transform.apply(&mut first).await.unwrap());
        assert!(transform.apply(&mut second).await.unwrap());

        assert_eq!(first.after, second.after);
        assert_eq!(first.before, second.before);
    }

    #[test]
    fn invalid_path_is_rejected() {
        let error = FieldMappingTransform::new(FieldMappingConfig {
            copy: vec![("".into(), "dest".into())],
            ..FieldMappingConfig::default()
        });

        assert!(error.is_err());
    }
}
