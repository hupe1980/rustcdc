//! Filter and projection transform.
//!
//! # Filtering
//!
//! Zero or more [`FilterRule`]s can be configured.  When `filters` is empty every
//! event passes through.  When multiple rules are present they are combined
//! according to [`FilterMode`]:
//!
//! - [`FilterMode::All`] (default) — **AND** semantics: every rule must match.
//! - [`FilterMode::Any`] — **OR** semantics: at least one rule must match.
//!
//! # Column projection
//!
//! Either `include_columns` **or** `exclude_columns` may be set (not both).
//! Setting both is rejected at [`FilterProjectionTransform::new`] time.

use std::collections::HashSet;

use async_trait::async_trait;
use regex::Regex;
use serde_json::Value;

use crate::core::{Error, Event, Result};

use super::Transform;

/// Logical combination mode for multiple [`FilterRule`]s in a
/// [`FilterProjectionConfig`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum FilterMode {
    /// Every rule must match (AND logic). This is the default.
    #[default]
    All,
    /// At least one rule must match (OR logic).
    Any,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterProjectionConfig {
    /// Rules applied to decide whether to keep (`true`) or drop (`false`) an event.
    ///
    /// When empty, all events pass through.
    /// When multiple rules are present they are combined per [`FilterMode`].
    pub filters: Vec<FilterRule>,
    /// How multiple `filters` are combined.  Default: [`FilterMode::All`] (AND logic).
    pub filter_mode: FilterMode,
    /// When set, only the listed columns are kept in `before`/`after` payloads.
    /// Mutually exclusive with `exclude_columns`.
    pub include_columns: Option<Vec<String>>,
    /// When set, the listed columns are removed from `before`/`after` payloads.
    /// Mutually exclusive with `include_columns`.
    pub exclude_columns: Option<Vec<String>>,
    /// When `true` (the default), `Truncate` events bypass **all** content-field
    /// filter rules and are always kept.
    ///
    /// Content-field rules (`AfterField` / `BeforeField`) cannot match on Truncate
    /// events because both `before` and `after` are `None`.  Without this bypass,
    /// any content-field rule under `FilterMode::All` (the default) would silently
    /// drop every Truncate event, causing downstream state divergence.
    ///
    /// Set to `false` only if you explicitly want Truncate events filtered by
    /// whatever rules happen to produce `true` (all will return `false` for
    /// content-field rules, so Truncate events would always be dropped).
    pub pass_through_truncate: bool,
}

impl Default for FilterProjectionConfig {
    fn default() -> Self {
        Self {
            filters: Vec::new(),
            filter_mode: FilterMode::default(),
            include_columns: None,
            exclude_columns: None,
            pass_through_truncate: true,
        }
    }
}

/// The event field targeted by a [`FilterRule`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FilterField {
    /// Match against the stringified [`Operation`][crate::core::Operation] (e.g. `"insert"`).
    Op,
    /// Match against `event.table`.
    Table,
    /// Match against a field **inside `event.after`** by dot-separated JSON path.
    ///
    /// Example: `"user.country"` traverses `after["user"]["country"]`.
    /// Numeric array indices are not supported.
    AfterField(String),
    /// Match against a field **inside `event.before`** by dot-separated JSON path.
    BeforeField(String),
}

/// Comparison operator for a [`FilterRule`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FilterOperator {
    /// Exact equality (string comparison of the JSON value's display form).
    Eq,
    /// Inequality.
    Ne,
    /// Check whether the field's string form contains the pattern as a substring.
    Contains,
    /// Match the field's string form against a regular expression.
    ///
    /// The pattern is pre-compiled at [`FilterProjectionTransform::new`] time.
    Regex,
    /// Numeric less-than: field value parsed as `f64` < rule value parsed as `f64`.
    Lt,
    /// Numeric less-than-or-equal.
    LtEq,
    /// Numeric greater-than.
    Gt,
    /// Numeric greater-than-or-equal.
    GtEq,
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
        // include_columns and exclude_columns are mutually exclusive — allowing both
        // produces confusing behavior (exclude is a no-op against an already-reduced set).
        if self.include_columns.is_some() && self.exclude_columns.is_some() {
            return Err(Error::ConfigError(
                "include_columns and exclude_columns are mutually exclusive".into(),
            ));
        }

        for rule in &self.filters {
            if rule.value.trim().is_empty() {
                return Err(Error::ConfigError(format!(
                    "filter value must not be empty for field {:?}",
                    &rule.field
                )));
            }

            // Pre-validate regex patterns early so construction errors surface at config time.
            if rule.operator == FilterOperator::Regex {
                Regex::new(&rule.value).map_err(|error| {
                    Error::ConfigError(format!("filter regex pattern is invalid: {error}"))
                })?;
            }
        }

        Ok(())
    }
}

/// Parsed and pre-built form of [`FilterProjectionConfig`].
///
/// Constructed via [`FilterProjectionTransform::new`].  All per-event work is
/// done against the pre-parsed state, eliminating allocations on the hot path.
#[derive(Debug, Clone)]
pub struct FilterProjectionTransform {
    pub config: FilterProjectionConfig,
    /// Pre-built include set; `None` when `include_columns` is absent.
    include_set: Option<HashSet<String>>,
    /// Pre-built exclude set; `None` when `exclude_columns` is absent.
    exclude_set: Option<HashSet<String>>,
    /// Pre-compiled regexes, parallel to `config.filters`.
    /// `None` for rules whose operator is not [`FilterOperator::Regex`].
    compiled_regexes: Vec<Option<Regex>>,
}

impl FilterProjectionTransform {
    /// Create a new transform, returning an error if the configuration is invalid.
    pub fn new(config: FilterProjectionConfig) -> Result<Self> {
        config.validate()?;

        // Pre-compile a regex for each rule that uses FilterOperator::Regex.
        let compiled_regexes = config
            .filters
            .iter()
            .map(|rule| {
                if rule.operator == FilterOperator::Regex {
                    Regex::new(&rule.value).map(Some).map_err(|error| {
                        Error::ConfigError(format!("filter regex pattern is invalid: {error}"))
                    })
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>>>()?;

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
            compiled_regexes,
        })
    }

    #[inline]
    fn evaluate_filter(&self, event: &Event) -> bool {
        // Truncate events have both before and after as None.  Content-field
        // rules cannot match, so they would always return false under FilterMode::All.
        // Honour pass_through_truncate to avoid silently dropping every TRUNCATE.
        if self.config.pass_through_truncate && matches!(event.op, crate::core::Operation::Truncate)
        {
            return true;
        }

        if self.config.filters.is_empty() {
            return true;
        }

        let mut iter = self
            .config
            .filters
            .iter()
            .zip(self.compiled_regexes.iter())
            .map(|(rule, re)| self.evaluate_rule(event, rule, re.as_ref()));

        match self.config.filter_mode {
            FilterMode::All => iter.all(|m| m),
            FilterMode::Any => iter.any(|m| m),
        }
    }

    #[inline]
    fn evaluate_rule(&self, event: &Event, rule: &FilterRule, regex: Option<&Regex>) -> bool {
        match &rule.field {
            FilterField::Op => {
                apply_operator(event.op.to_str(), &rule.operator, &rule.value, regex)
            }
            FilterField::Table => apply_operator(&event.table, &rule.operator, &rule.value, regex),
            FilterField::AfterField(path) => {
                let Some(payload) = event.after.as_ref() else {
                    return false; // No after payload → rule cannot match.
                };
                let Some(value) = extract_json_field(payload, path) else {
                    return false; // Field absent → rule cannot match.
                };
                let value_str = match value {
                    Value::String(s) => s.as_str().to_owned(),
                    other => other.to_string(),
                };
                apply_operator(&value_str, &rule.operator, &rule.value, regex)
            }
            FilterField::BeforeField(path) => {
                let Some(payload) = event.before.as_ref() else {
                    return false; // No before payload → rule cannot match.
                };
                let Some(value) = extract_json_field(payload, path) else {
                    return false; // Field absent → rule cannot match.
                };
                let value_str = match value {
                    Value::String(s) => s.as_str().to_owned(),
                    other => other.to_string(),
                };
                apply_operator(&value_str, &rule.operator, &rule.value, regex)
            }
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

/// Apply a [`FilterOperator`] to a resolved string left-hand side.
///
/// For numeric comparisons (`Lt`, `LtEq`, `Gt`, `GtEq`) both `left` and `rule_value`
/// are parsed as `f64`; if either fails to parse the comparison returns `false`.
#[inline]
fn apply_operator(
    left: &str,
    op: &FilterOperator,
    rule_value: &str,
    regex: Option<&Regex>,
) -> bool {
    match op {
        FilterOperator::Eq => left == rule_value,
        FilterOperator::Ne => left != rule_value,
        FilterOperator::Contains => left.contains(rule_value),
        FilterOperator::Regex => regex.is_some_and(|re| re.is_match(left)),
        FilterOperator::Lt | FilterOperator::LtEq | FilterOperator::Gt | FilterOperator::GtEq => {
            let Ok(lv) = left.parse::<f64>() else {
                return false;
            };
            let Ok(rv) = rule_value.parse::<f64>() else {
                return false;
            };
            match op {
                FilterOperator::Lt => lv < rv,
                FilterOperator::LtEq => lv <= rv,
                FilterOperator::Gt => lv > rv,
                FilterOperator::GtEq => lv >= rv,
                // SAFETY: outer match arm already restricts to the four numeric variants above.
                _ => unreachable!(
                    "numeric comparison arm is exhausted by the outer Lt|LtEq|Gt|GtEq restriction"
                ),
            }
        }
    }
}

/// Traverse a dot-separated path into a serde_json Value.
///
/// Returns `None` when any segment is missing or the intermediate value is not an object.
fn extract_json_field<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
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
        FilterField, FilterMode, FilterOperator, FilterProjectionConfig, FilterProjectionTransform,
        FilterRule,
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

    // ── Single-rule filtering (backward-compatible usage) ─────────────────────

    #[tokio::test]
    async fn event_can_be_filtered_out() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![FilterRule::new(
                FilterField::Op,
                FilterOperator::Ne,
                "delete",
            )],
            ..Default::default()
        })
        .unwrap();

        let mut e = event(Operation::Delete);
        assert!(!transform.apply(&mut e).await.unwrap());
    }

    #[tokio::test]
    async fn include_projection_keeps_only_selected_columns() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![],
            include_columns: Some(vec!["id".into(), "name".into()]),
            ..Default::default()
        })
        .unwrap();

        let mut e = event(Operation::Insert);
        assert!(transform.apply(&mut e).await.unwrap());
        let after = e.after.unwrap();
        assert_eq!(after["id"], 1);
        assert_eq!(after["name"], "alice");
        assert!(after.get("secret").is_none());
    }

    #[tokio::test]
    async fn exclude_projection_removes_selected_columns() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![],
            exclude_columns: Some(vec!["secret".into()]),
            ..Default::default()
        })
        .unwrap();

        let mut e = event(Operation::Insert);
        assert!(transform.apply(&mut e).await.unwrap());
        assert!(e.after.unwrap().get("secret").is_none());
    }

    #[test]
    fn include_and_exclude_columns_both_set_is_rejected() {
        let err = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![],
            include_columns: Some(vec!["id".into()]),
            exclude_columns: Some(vec!["secret".into()]),
            ..Default::default()
        });
        assert!(
            err.is_err(),
            "setting both include_columns and exclude_columns must be a ConfigError"
        );
    }

    #[test]
    fn invalid_filter_rule_rejected_at_construction() {
        let err = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![FilterRule::new(
                FilterField::Table,
                FilterOperator::Eq,
                "   ",
            )],
            ..Default::default()
        });
        assert!(err.is_err(), "expected ConfigError for empty filter value");
    }

    #[tokio::test]
    async fn empty_projection_errors() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![],
            include_columns: Some(vec!["missing".into()]),
            ..Default::default()
        })
        .unwrap();

        let mut e = event(Operation::Insert);
        assert!(transform.apply(&mut e).await.is_err());
    }

    #[tokio::test]
    async fn filter_projection_is_deterministic() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![FilterRule::new(
                FilterField::Table,
                FilterOperator::Eq,
                "users",
            )],
            include_columns: Some(vec!["id".into()]),
            ..Default::default()
        })
        .unwrap();

        let mut first = event(Operation::Insert);
        let mut second = event(Operation::Insert);

        assert!(transform.apply(&mut first).await.unwrap());
        assert!(transform.apply(&mut second).await.unwrap());
        assert_eq!(first.after, second.after);
    }

    // ── Content-field filtering ───────────────────────────────────────────────

    #[tokio::test]
    async fn after_field_eq_passes_matching_event() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![FilterRule::new(
                FilterField::AfterField("name".into()),
                FilterOperator::Eq,
                "alice",
            )],
            ..Default::default()
        })
        .unwrap();

        let mut e = event(Operation::Insert);
        assert!(
            transform.apply(&mut e).await.unwrap(),
            "event with name=alice must pass"
        );
    }

    #[tokio::test]
    async fn after_field_eq_drops_non_matching_event() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![FilterRule::new(
                FilterField::AfterField("name".into()),
                FilterOperator::Eq,
                "bob",
            )],
            ..Default::default()
        })
        .unwrap();

        let mut e = event(Operation::Insert);
        assert!(
            !transform.apply(&mut e).await.unwrap(),
            "event with name=alice must be dropped"
        );
    }

    #[tokio::test]
    async fn after_field_contains_operator() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![FilterRule::new(
                FilterField::AfterField("name".into()),
                FilterOperator::Contains,
                "lic",
            )],
            ..Default::default()
        })
        .unwrap();

        let mut e = event(Operation::Insert);
        assert!(
            transform.apply(&mut e).await.unwrap(),
            "\"alice\" contains \"lic\""
        );
    }

    #[tokio::test]
    async fn after_field_regex_operator() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![FilterRule::new(
                FilterField::AfterField("name".into()),
                FilterOperator::Regex,
                "^ali",
            )],
            ..Default::default()
        })
        .unwrap();

        let mut e = event(Operation::Insert);
        assert!(
            transform.apply(&mut e).await.unwrap(),
            "\"alice\" matches ^ali"
        );
    }

    #[test]
    fn invalid_regex_rejected_at_construction() {
        let err = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![FilterRule::new(
                FilterField::AfterField("name".into()),
                FilterOperator::Regex,
                "[invalid",
            )],
            ..Default::default()
        });
        assert!(
            err.is_err(),
            "invalid regex must be rejected at construction"
        );
    }

    #[tokio::test]
    async fn numeric_gt_operator() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![FilterRule::new(
                FilterField::AfterField("id".into()),
                FilterOperator::Gt,
                "0",
            )],
            ..Default::default()
        })
        .unwrap();

        let mut e = event(Operation::Insert); // after["id"] = 1
        assert!(transform.apply(&mut e).await.unwrap(), "id=1 > 0");
    }

    #[tokio::test]
    async fn before_field_filter() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![FilterRule::new(
                FilterField::BeforeField("secret".into()),
                FilterOperator::Eq,
                "x",
            )],
            ..Default::default()
        })
        .unwrap();

        let mut e = event(Operation::Update);
        assert!(
            transform.apply(&mut e).await.unwrap(),
            "before.secret=x must match"
        );
    }

    // ── Multi-rule AND / OR ───────────────────────────────────────────────────

    #[tokio::test]
    async fn all_mode_requires_every_rule_to_match() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![
                FilterRule::new(FilterField::Table, FilterOperator::Eq, "users"),
                FilterRule::new(FilterField::Op, FilterOperator::Eq, "insert"),
            ],
            filter_mode: FilterMode::All,
            ..Default::default()
        })
        .unwrap();

        // Both match → keep.
        let mut insert_users = event(Operation::Insert);
        assert!(transform.apply(&mut insert_users).await.unwrap());

        // Only table matches, op doesn't → drop.
        let mut update_users = event(Operation::Update);
        assert!(!transform.apply(&mut update_users).await.unwrap());
    }

    #[tokio::test]
    async fn any_mode_passes_if_at_least_one_rule_matches() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![
                FilterRule::new(FilterField::Table, FilterOperator::Eq, "orders"),
                FilterRule::new(FilterField::Op, FilterOperator::Eq, "insert"),
            ],
            filter_mode: FilterMode::Any,
            ..Default::default()
        })
        .unwrap();

        // table=users, op=insert → second rule matches.
        let mut e = event(Operation::Insert); // table = "users"
        assert!(transform.apply(&mut e).await.unwrap());
    }

    #[tokio::test]
    async fn any_mode_drops_when_no_rule_matches() {
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![
                FilterRule::new(FilterField::Table, FilterOperator::Eq, "orders"),
                FilterRule::new(FilterField::Op, FilterOperator::Eq, "delete"),
            ],
            filter_mode: FilterMode::Any,
            ..Default::default()
        })
        .unwrap();

        // table=users (≠orders), op=insert (≠delete) → neither rule matches.
        let mut e = event(Operation::Insert);
        assert!(!transform.apply(&mut e).await.unwrap());
    }

    #[tokio::test]
    async fn multi_rule_with_content_field_and_op() {
        // Keep only insert events where after.name == "alice".
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![
                FilterRule::new(FilterField::Op, FilterOperator::Eq, "insert"),
                FilterRule::new(
                    FilterField::AfterField("name".into()),
                    FilterOperator::Eq,
                    "alice",
                ),
            ],
            filter_mode: FilterMode::All,
            ..Default::default()
        })
        .unwrap();

        let mut matching = event(Operation::Insert);
        assert!(transform.apply(&mut matching).await.unwrap());

        let mut non_matching = event(Operation::Update); // op differs
        assert!(!transform.apply(&mut non_matching).await.unwrap());
    }

    #[tokio::test]
    async fn truncate_event_passes_through_with_content_filter_rule() {
        // A content-field rule can never match a Truncate event (before+after=None).
        // pass_through_truncate (default=true) must ensure Truncate is never silently dropped.
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![FilterRule::new(
                FilterField::AfterField("region".into()),
                FilterOperator::Eq,
                "EU",
            )],
            filter_mode: FilterMode::All,
            ..Default::default()
        })
        .unwrap();

        let mut e = Event {
            before: None,
            after: None,
            op: Operation::Truncate,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "0".into(),
                timestamp: 0,
            },
            ts: 0,
            schema: Some("public".into()),
            table: "orders".into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: crate::core::EVENT_ENVELOPE_VERSION,
        };
        assert!(
            transform.apply(&mut e).await.unwrap(),
            "Truncate must pass through even when content-field filter rules are present"
        );
    }

    #[tokio::test]
    async fn truncate_event_dropped_when_pass_through_truncate_false() {
        // With pass_through_truncate=false and a content-field rule, Truncate events should
        // be filtered the same as any other event — the rule returns false, dropping the event.
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![FilterRule::new(
                FilterField::AfterField("region".into()),
                FilterOperator::Eq,
                "EU",
            )],
            filter_mode: FilterMode::All,
            pass_through_truncate: false,
            ..Default::default()
        })
        .unwrap();

        let mut e = Event {
            before: None,
            after: None,
            op: Operation::Truncate,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "0".into(),
                timestamp: 0,
            },
            ts: 0,
            schema: Some("public".into()),
            table: "orders".into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: crate::core::EVENT_ENVELOPE_VERSION,
        };
        assert!(
            !transform.apply(&mut e).await.unwrap(),
            "Truncate must be dropped when pass_through_truncate=false and rule does not match"
        );
    }
}
