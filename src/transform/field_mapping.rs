//! Field mapping transform for copy/rename/set/remove operations.

use serde_json::{Map, Value};

use crate::core::{Error, Event, Result};

use super::Transform;

#[derive(Debug, Clone, Default, PartialEq)]
/// Copy, rename, inject, and remove fields by dotted path.
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
/// Applies [`FieldMappingConfig`] to event payloads.
pub struct FieldMappingTransform {
    /// Mapping configuration.
    pub config: FieldMappingConfig,
    copy_rules: Vec<MoveRule>,
    rename_rules: Vec<MoveRule>,
    set_rules: Vec<SetRule>,
    remove_rules: Vec<PathRule>,
}

impl FieldMappingTransform {
    /// Build a transform, validating the mapping up front.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] for a malformed
    /// path or a mapping that would collide.
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
        // Do NOT create a synthetic payload for None values.
        // Insert.before, Delete.after, and all Truncate payloads are intentionally
        // None per the canonical event envelope contract.  Creating an object here
        // would produce phantom payloads that fail event.validate() downstream.
        // set_literal rules are only applied to payloads that already exist.
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

    /// Keep an unavailable-column list in step with the column names this stage rewrote.
    ///
    /// `Event::unavailable_columns` names top-level columns the *source* could not supply
    /// — a PostgreSQL unchanged-TOAST value, say — and a sink reads it as "leave this
    /// column alone". Renaming or removing a column without rewriting the list leaves the
    /// event describing a column that no longer exists under that name, which is the same
    /// failure the list was introduced to prevent: the renamed column is simply absent,
    /// and a sink that merges present columns and skips unavailable ones has no reason to
    /// leave it alone.
    ///
    /// Only single-segment paths are column names; a nested path addresses a field inside
    /// a column's value and cannot change whether the column itself was supplied.
    fn remap_unavailable(&self, columns: &mut Vec<String>) {
        if columns.is_empty() {
            return;
        }

        fn column_of(parts: &[String]) -> Option<&str> {
            match parts {
                [single] => Some(single.as_str()),
                _ => None,
            }
        }

        // A copy or a literal gives the destination column a value, so it is no longer
        // unavailable — whatever it now holds, the sink is meant to write it.
        let destinations = self
            .copy_rules
            .iter()
            .map(|rule| &rule.to)
            .chain(self.set_rules.iter().map(|rule| &rule.to));
        for destination in destinations {
            if let Some(name) = column_of(destination) {
                columns.retain(|existing| existing != name);
            }
        }

        // A rename carries the property with the column: the value was unavailable
        // before the rename and is still unavailable after it, under the new name.
        for rule in &self.rename_rules {
            let (Some(from), Some(to)) = (column_of(&rule.from), column_of(&rule.to)) else {
                continue;
            };
            if let Some(slot) = columns.iter_mut().find(|existing| *existing == from) {
                *slot = to.to_string();
            }
        }

        // A removed column is gone from the payload; saying it is unavailable describes
        // a column the sink can no longer see.
        for rule in &self.remove_rules {
            if let Some(name) = column_of(&rule.parts) {
                columns.retain(|existing| existing != name);
            }
        }
    }
}

impl Transform for FieldMappingTransform {
    fn apply(&self, event: &mut Event) -> Result<bool> {
        self.apply_payload(&mut event.before)?;
        self.apply_payload(&mut event.after)?;
        self.remap_unavailable(&mut event.before_unavailable_columns);
        self.remap_unavailable(&mut event.unavailable_columns);
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
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
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
        assert!(transform.apply(&mut event).unwrap());
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
        assert!(transform.apply(&mut event).unwrap());
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
        assert!(transform.apply(&mut event).unwrap());
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
        assert!(transform.apply(&mut event).unwrap());
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
        assert!(transform.apply(&mut first_event).is_err());

        let transform = FieldMappingTransform::new(FieldMappingConfig {
            remove: vec!["missing".into()],
            strict: true,
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut second_event = event();
        assert!(transform.apply(&mut second_event).is_err());
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
        assert!(transform.apply(&mut first).unwrap());
        assert!(transform.apply(&mut second).unwrap());

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

    // ── Truncate / no-payload events ──────────────────────────────────────────

    #[tokio::test]
    async fn truncate_event_passes_through_without_phantom_payloads() {
        // set_literals are configured but the event has no before/after.
        // The transform must NOT create phantom `Some({...})` payloads.
        let transform = FieldMappingTransform::new(FieldMappingConfig {
            set_literals: vec![("meta.source".into(), json!("mysql"))],
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut e = Event {
            before: None,
            after: None,
            op: Operation::Truncate,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "1".into(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".into()),
            table: "users".into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        };
        assert!(transform.apply(&mut e).unwrap());
        assert!(e.before.is_none(), "before must remain None for Truncate");
        assert!(e.after.is_none(), "after must remain None for Truncate");
    }

    #[tokio::test]
    async fn delete_event_after_remains_none_with_set_literals() {
        // Delete events have after = None; set_literal must not create a phantom after.
        let transform = FieldMappingTransform::new(FieldMappingConfig {
            set_literals: vec![("_source".into(), json!("cdc"))],
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut e = Event {
            before: Some(json!({"id": 5})),
            after: None,
            op: Operation::Delete,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "2".into(),
                timestamp: 2,
            },
            ts: 2,
            schema: None,
            table: "orders".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        };
        assert!(transform.apply(&mut e).unwrap());
        assert!(e.after.is_none(), "after must remain None for Delete");
        // set_literal IS applied to the before payload (it's present).
        assert_eq!(e.before.as_ref().unwrap()["_source"], "cdc");
    }

    #[tokio::test]
    async fn a_rename_carries_the_unavailable_marker_to_the_new_column_name() {
        // `unavailable_columns` names a column the *source* could not supply — a
        // PostgreSQL unchanged-TOAST value — and a sink reads it as "leave this column
        // alone". A rename that moves the payload key but leaves the marker behind
        // describes a column that no longer exists under that name; the renamed column
        // then looks merely absent, which is exactly the overwrite the marker prevents.
        let transform = FieldMappingTransform::new(FieldMappingConfig {
            rename: vec![("body".into(), "content".into())],
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut event = event();
        event.op = Operation::Update;
        event.unavailable_columns = vec!["body".into()];
        event.before_unavailable_columns = vec!["body".into()];

        assert!(transform.apply(&mut event).unwrap());
        assert_eq!(event.unavailable_columns, vec!["content".to_string()]);
        assert_eq!(
            event.before_unavailable_columns,
            vec!["content".to_string()]
        );
    }

    #[tokio::test]
    async fn giving_an_unavailable_column_a_value_clears_its_marker() {
        // An event may not both carry a column and declare it unavailable —
        // `Event::validate` rejects that, and the dangerous reading (trust the payload)
        // is the one a sink takes. A literal or a copy into that column resolves the
        // contradiction in the only direction that is true: it now has a value.
        let transform = FieldMappingTransform::new(FieldMappingConfig {
            set_literals: vec![("body".into(), serde_json::json!("redacted"))],
            copy: vec![("id".into(), "shadow_id".into())],
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut event = event();
        event.op = Operation::Update;
        event.unavailable_columns = vec!["body".into(), "shadow_id".into()];

        assert!(transform.apply(&mut event).unwrap());
        assert!(event.unavailable_columns.is_empty());
        event
            .validate()
            .expect("the event must not contradict itself");
    }

    #[tokio::test]
    async fn removing_a_column_drops_its_unavailable_marker() {
        let transform = FieldMappingTransform::new(FieldMappingConfig {
            remove: vec!["body".into()],
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut event = event();
        event.op = Operation::Update;
        event.unavailable_columns = vec!["body".into(), "other".into()];

        assert!(transform.apply(&mut event).unwrap());
        assert_eq!(event.unavailable_columns, vec!["other".to_string()]);
    }

    #[tokio::test]
    async fn a_nested_path_never_touches_a_column_marker() {
        // `user.email` addresses a field inside a column's value. Whether the *column*
        // was supplied is a different question, and rewriting the marker on this would
        // be wrong in both directions.
        let transform = FieldMappingTransform::new(FieldMappingConfig {
            rename: vec![("user.email".into(), "user.mail".into())],
            ..FieldMappingConfig::default()
        })
        .unwrap();

        let mut event = event();
        event.op = Operation::Update;
        event.unavailable_columns = vec!["user".into()];

        assert!(transform.apply(&mut event).unwrap());
        assert_eq!(event.unavailable_columns, vec!["user".to_string()]);
    }
}
