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

// ─── Pre-parsed internal types ────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterField {
    Op,
    Table,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterOperator {
    Eq,
    Ne,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedFilter {
    field: FilterField,
    operator: FilterOperator,
    value: String,
}

/// Supported fields that may appear on the left-hand side of a filter expression.
const SUPPORTED_FIELDS: &[&str] = &["op", "table"];
/// Supported binary operators in filter expressions.
const SUPPORTED_OPERATORS: &[&str] = &["==", "!="];

impl FilterProjectionConfig {
    /// Validate this configuration, returning a descriptive error if the filter
    /// expression cannot be evaluated.
    ///
    /// An expression must be exactly three whitespace-separated tokens
    /// (`<field> <operator> <value>`) where `<field>` is one of `op` or `table`
    /// and `<operator>` is `==` or `!=`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] when:
    /// - The expression does not have exactly three whitespace-separated tokens.
    /// - The field name is not in `["op", "table"]`.
    /// - The operator is not `==` or `!=`.
    pub fn validate(&self) -> Result<()> {
        let Some(expr) = self.filter_expr.as_deref() else {
            return Ok(());
        };

        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 3 {
            return Err(Error::ConfigError(format!(
                "filter expression must be exactly three whitespace-separated tokens \
                 (<field> <operator> <value>), got {n} tokens in: {expr:?}",
                n = parts.len()
            )));
        }

        let field = parts[0];
        if !SUPPORTED_FIELDS.contains(&field) {
            return Err(Error::ConfigError(format!(
                "unsupported filter field {field:?}; supported fields are: {SUPPORTED_FIELDS:?}"
            )));
        }

        let operator = parts[1];
        if !SUPPORTED_OPERATORS.contains(&operator) {
            return Err(Error::ConfigError(format!(
                "unsupported filter operator {operator:?}; supported operators are: {SUPPORTED_OPERATORS:?}"
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
    /// Pre-parsed filter; `None` when `filter_expr` is absent.
    parsed_filter: Option<ParsedFilter>,
    /// Pre-built include set; `None` when `include_columns` is absent.
    include_set: Option<HashSet<String>>,
    /// Pre-built exclude set; `None` when `exclude_columns` is absent.
    exclude_set: Option<HashSet<String>>,
}

impl FilterProjectionTransform {
    /// Create a new transform, returning an error if the configuration is invalid.
    pub fn new(config: FilterProjectionConfig) -> Result<Self> {
        config.validate()?;

        // Pre-parse the filter expression once.
        let parsed_filter = config.filter_expr.as_deref().map(|expr| {
            let parts: Vec<&str> = expr.split_whitespace().collect();
            let field = match parts[0] {
                "op" => FilterField::Op,
                _ => FilterField::Table, // "table"; validate() already checked
            };
            let operator = match parts[1] {
                "==" => FilterOperator::Eq,
                _ => FilterOperator::Ne, // "!="; validate() already checked
            };
            let value = parts[2].trim_matches('"').trim_matches('\'').to_owned();
            ParsedFilter {
                field,
                operator,
                value,
            }
        });

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
            parsed_filter,
            include_set,
            exclude_set,
        })
    }

    #[inline]
    fn evaluate_filter(&self, event: &Event) -> bool {
        let Some(pf) = &self.parsed_filter else {
            return true;
        };

        let left: &str = match pf.field {
            FilterField::Op => event.op.to_str(),
            FilterField::Table => &event.table,
        };

        match pf.operator {
            FilterOperator::Eq => left == pf.value,
            FilterOperator::Ne => left != pf.value,
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
        })
        .unwrap();

        let mut event = event(Operation::Delete);
        assert!(!transform.apply(&mut event).await.unwrap());
    }

    #[tokio::test]
    async fn include_projection_keeps_only_selected_columns() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filter_expr: None,
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
            filter_expr: None,
            include_columns: None,
            exclude_columns: Some(vec!["secret".into()]),
        })
        .unwrap();

        let mut event = event(Operation::Insert);
        assert!(transform.apply(&mut event).await.unwrap());
        assert!(event.after.unwrap().get("secret").is_none());
    }

    #[test]
    fn invalid_filter_expression_rejected_at_construction() {
        // Unsupported operator: fails at new() not at apply().
        let err = FilterProjectionTransform::new(FilterProjectionConfig {
            filter_expr: Some("op ~~ insert".into()),
            include_columns: None,
            exclude_columns: None,
        });
        assert!(err.is_err(), "expected ConfigError for invalid operator");

        // Bad field name.
        let err = FilterProjectionTransform::new(FilterProjectionConfig {
            filter_expr: Some("payload == foo".into()),
            include_columns: None,
            exclude_columns: None,
        });
        assert!(err.is_err(), "expected ConfigError for unsupported field");

        // Wrong token count.
        let err = FilterProjectionTransform::new(FilterProjectionConfig {
            filter_expr: Some("op".into()),
            include_columns: None,
            exclude_columns: None,
        });
        assert!(err.is_err(), "expected ConfigError for wrong token count");
    }

    #[tokio::test]
    async fn empty_projection_errors() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filter_expr: None,
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
            filter_expr: Some("table == 'users'".into()),
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
