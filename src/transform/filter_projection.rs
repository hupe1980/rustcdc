//! Filter and projection transform.

use std::collections::HashSet;

use async_trait::async_trait;
use serde_json::Value;

use crate::core::{Error, Event, Result};

use super::Transform;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilterProjectionConfig {
    pub filter_expr: Option<String>,
    pub include_columns: Option<Vec<String>>,
    pub exclude_columns: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct FilterProjectionTransform {
    pub config: FilterProjectionConfig,
}

impl FilterProjectionTransform {
    pub fn new(config: FilterProjectionConfig) -> Self {
        Self { config }
    }

    fn evaluate_filter(&self, event: &Event) -> Result<bool> {
        let Some(expr) = self.config.filter_expr.as_deref() else {
            return Ok(true);
        };

        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 3 {
            return Err(Error::TransformError(format!(
                "invalid filter expression: {expr}"
            )));
        }

        let field = parts[0];
        let operator = parts[1];
        let raw_value = parts[2].trim_matches('"').trim_matches('\'');

        let left = match field {
            "op" => event.op.to_string(),
            "table" => event.table.clone(),
            _ => {
                return Err(Error::TransformError(format!(
                    "unsupported filter field: {field}"
                )))
            }
        };

        match operator {
            "==" => Ok(left == raw_value),
            "!=" => Ok(left != raw_value),
            _ => Err(Error::TransformError(format!(
                "unsupported filter operator: {operator}"
            ))),
        }
    }

    fn project_payload(&self, payload: &mut Option<Value>) -> Result<()> {
        let Some(Value::Object(object)) = payload else {
            return Ok(());
        };

        if let Some(columns) = &self.config.include_columns {
            let include: HashSet<&str> = columns.iter().map(String::as_str).collect();
            object.retain(|key, _| include.contains(key.as_str()));
        }

        if let Some(columns) = &self.config.exclude_columns {
            let exclude: HashSet<&str> = columns.iter().map(String::as_str).collect();
            object.retain(|key, _| !exclude.contains(key.as_str()));
        }

        if (self.config.include_columns.is_some() || self.config.exclude_columns.is_some())
            && object.is_empty()
        {
            return Err(Error::TransformError(
                "projection removed all columns from payload".into(),
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl Transform for FilterProjectionTransform {
    async fn apply(&self, event: &mut Event) -> Result<bool> {
        if !self.evaluate_filter(event)? {
            return Ok(false);
        }

        self.project_payload(&mut event.before)?;
        self.project_payload(&mut event.after)?;
        Ok(true)
    }

    fn name(&self) -> &str {
        "filter_projection"
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};

    use super::{FilterProjectionConfig, FilterProjectionTransform};
    use crate::transform::Transform;

    fn event(op: Operation) -> Event {
        Event {
            before: Some(json!({"id": 1, "secret": "x"})),
            after: Some(json!({"id": 1, "name": "alice", "secret": "x"})),
            op,
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
    async fn event_can_be_filtered_out() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filter_expr: Some("op != 'delete'".into()),
            include_columns: None,
            exclude_columns: None,
        });

        let mut event = event(Operation::Delete);
        assert!(!transform.apply(&mut event).await.unwrap());
    }

    #[tokio::test]
    async fn include_projection_keeps_only_selected_columns() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filter_expr: None,
            include_columns: Some(vec!["id".into(), "name".into()]),
            exclude_columns: None,
        });

        let mut event = event(Operation::Insert);
        assert!(transform.apply(&mut event).await.unwrap());
        let after = event.after.unwrap();
        assert_eq!(after["id"], 1);
        assert_eq!(after["name"], "alice");
        assert!(after.get("secret").is_none());
    }

    #[tokio::test]
    async fn exclude_projection_removes_selected_columns() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filter_expr: None,
            include_columns: None,
            exclude_columns: Some(vec!["secret".into()]),
        });

        let mut event = event(Operation::Insert);
        assert!(transform.apply(&mut event).await.unwrap());
        assert!(event.after.unwrap().get("secret").is_none());
    }

    #[tokio::test]
    async fn invalid_filter_expression_errors() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filter_expr: Some("op ~~ insert".into()),
            include_columns: None,
            exclude_columns: None,
        });

        let mut event = event(Operation::Insert);
        assert!(transform.apply(&mut event).await.is_err());
    }

    #[tokio::test]
    async fn empty_projection_errors() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filter_expr: None,
            include_columns: Some(vec!["missing".into()]),
            exclude_columns: None,
        });

        let mut event = event(Operation::Insert);
        assert!(transform.apply(&mut event).await.is_err());
    }

    #[tokio::test]
    async fn filter_projection_is_deterministic() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filter_expr: Some("table == 'users'".into()),
            include_columns: Some(vec!["id".into()]),
            exclude_columns: None,
        });

        let mut first = event(Operation::Insert);
        let mut second = event(Operation::Insert);

        assert!(transform.apply(&mut first).await.unwrap());
        assert!(transform.apply(&mut second).await.unwrap());
        assert_eq!(first.after, second.after);
    }
}
