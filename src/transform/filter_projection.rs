//! Filter and projection transform.

use std::collections::HashSet;

use async_trait::async_trait;
use serde_json::Value;

use crate::core::{Error, Event, Result};

use super::Transform;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilterProjectionConfig {
    pub filter: Option<FilterRule>,
    pub include_columns: Option<Vec<String>>,
    pub exclude_columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterField {
    Op,
    Table,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOperator {
    Eq,
    Ne,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterRule {
    field: FilterField,
    operator: FilterOperator,
    value: String,
}

impl FilterRule {
    pub fn new(field: FilterField, operator: FilterOperator, value: impl Into<String>) -> Self {
        Self {
            field,
            operator,
            value: value.into(),
        }
    }
}

impl FilterProjectionConfig {
    pub fn validate(&self) -> Result<()> {
        let Some(filter) = &self.filter else {
            return Ok(());
        };

        if filter.value.trim().is_empty() {
            return Err(Error::ConfigError(format!(
                "filter value must not be empty for field {:?}",
                filter.field
            )));
        }

        Ok(())
    }
}

/// Parsed and pre-built form of [`FilterProjectionConfig`].
///
/// Constructed via [`FilterProjectionTransform::new`]. All per-event work is
/// done against the pre-parsed state, eliminating allocations on the hot path.
#[derive(Debug, Clone)]
pub struct FilterProjectionTransform {
    pub config: FilterProjectionConfig,
    /// Pre-built include set; `None` when `include_columns` is absent.
    include_set: Option<HashSet<String>>,
    /// Pre-built exclude set; `None` when `exclude_columns` is absent.
    exclude_set: Option<HashSet<String>>,
}

impl FilterProjectionTransform {
    /// Create a new transform, returning an error if the configuration is invalid.
    pub fn new(config: FilterProjectionConfig) -> Result<Self> {
        config.validate()?;

        // Pre-build column sets so project_payload has no per-event allocations.
        let include_set = config
            .include_columns
            .as_deref()
            .map(|cols| cols.iter().cloned().collect::<HashSet<String>>());
        let exclude_set = config
            .exclude_columns
            .as_deref()
            .map(|cols| cols.iter().cloned().collect::<HashSet<String>>());

        Ok(Self {
            config,
            include_set,
            exclude_set,
        })
    }

    #[inline]
    fn evaluate_filter(&self, event: &Event) -> bool {
        let Some(filter) = &self.config.filter else {
            return true;
        };

        let left: &str = match filter.field {
            FilterField::Op => event.op.to_str(),
            FilterField::Table => &event.table,
        };

        match filter.operator {
            FilterOperator::Eq => left == filter.value,
            FilterOperator::Ne => left != filter.value,
        }
    }

    fn project_payload(&self, payload: &mut Option<Value>) -> Result<()> {
        let Some(Value::Object(object)) = payload else {
            return Ok(());
        };

        if let Some(include) = &self.include_set {
            object.retain(|key, _| include.contains(key.as_str()));
        }

        if let Some(exclude) = &self.exclude_set {
            object.retain(|key, _| !exclude.contains(key.as_str()));
        }

        if (self.include_set.is_some() || self.exclude_set.is_some()) && object.is_empty() {
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
        if !self.evaluate_filter(event) {
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

    use super::{
        FilterField, FilterOperator, FilterProjectionConfig, FilterProjectionTransform, FilterRule,
    };
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
            filter: Some(FilterRule::new(
                FilterField::Op,
                FilterOperator::Ne,
                "delete",
            )),
            include_columns: None,
            exclude_columns: None,
        })
        .unwrap();

        let mut event = event(Operation::Delete);
        assert!(!transform.apply(&mut event).await.unwrap());
    }

    #[tokio::test]
    async fn include_projection_keeps_only_selected_columns() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filter: None,
            include_columns: Some(vec!["id".into(), "name".into()]),
            exclude_columns: None,
        })
        .unwrap();

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
            filter: None,
            include_columns: None,
            exclude_columns: Some(vec!["secret".into()]),
        })
        .unwrap();

        let mut event = event(Operation::Insert);
        assert!(transform.apply(&mut event).await.unwrap());
        assert!(event.after.unwrap().get("secret").is_none());
    }

    #[test]
    fn invalid_filter_rule_rejected_at_construction() {
        let err = FilterProjectionTransform::new(FilterProjectionConfig {
            filter: Some(FilterRule::new(
                FilterField::Table,
                FilterOperator::Eq,
                "   ",
            )),
            include_columns: None,
            exclude_columns: None,
        });
        assert!(err.is_err(), "expected ConfigError for empty filter value");
    }

    #[tokio::test]
    async fn empty_projection_errors() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filter: None,
            include_columns: Some(vec!["missing".into()]),
            exclude_columns: None,
        })
        .unwrap();

        let mut event = event(Operation::Insert);
        assert!(transform.apply(&mut event).await.is_err());
    }

    #[tokio::test]
    async fn filter_projection_is_deterministic() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filter: Some(FilterRule::new(
                FilterField::Table,
                FilterOperator::Eq,
                "users",
            )),
            include_columns: Some(vec!["id".into()]),
            exclude_columns: None,
        })
        .unwrap();

        let mut first = event(Operation::Insert);
        let mut second = event(Operation::Insert);

        assert!(transform.apply(&mut first).await.unwrap());
        assert!(transform.apply(&mut second).await.unwrap());
        assert_eq!(first.after, second.after);
    }
}
