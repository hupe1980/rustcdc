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
//! The ordering operators compare **exact decimals**, not `f64` — see
//! [`FilterOperator::Lt`]. Column values reach a filter as text precisely because a JSON
//! number is an IEEE-754 double by the time most consumers see it, and a filter that
//! narrowed them back to `f64` would decide row membership at 53 bits of precision.
//!
//! # Column projection
//!
//! Either `include_columns` **or** `exclude_columns` may be set (not both).
//! Setting both is rejected at [`FilterProjectionTransform::new`] time.

use std::collections::HashSet;

use regex::Regex;
use serde_json::Value;

use crate::core::{Error, Event, Result};

use super::{Transform, UnmatchedRule};

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
/// Row filtering and column projection.
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
    /// Numeric less-than, compared as an **exact decimal**.
    ///
    /// Both sides are read as `[+|-]digits[.digits]` and compared digit by digit, so there
    /// is no mantissa ceiling: `bigint` past 2^53 and `numeric(38,4)` order correctly.
    /// This matches the crate's text-first value contract — routing the comparison through
    /// `f64` would reintroduce the precision loss that contract exists to avoid, at the
    /// point where it decides whether a row is kept.
    ///
    /// Exponent notation (`1e3`) is **not** accepted, and a side that is not a plain
    /// decimal numeral makes the rule evaluate to `false` rather than guess an order.
    Lt,
    /// Numeric less-than-or-equal. Exact decimal comparison; see [`FilterOperator::Lt`].
    LtEq,
    /// Numeric greater-than. Exact decimal comparison; see [`FilterOperator::Lt`].
    Gt,
    /// Numeric greater-than-or-equal. Exact decimal comparison; see [`FilterOperator::Lt`].
    GtEq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One predicate: a field, an operator, and a value to compare against.
pub struct FilterRule {
    field: FilterField,
    operator: FilterOperator,
    value: String,
}

impl FilterRule {
    /// Build a rule. Validation happens at
    /// [`FilterProjectionTransform::new`](super::FilterProjectionTransform::new).
    pub fn new(field: FilterField, operator: FilterOperator, value: impl Into<String>) -> Self {
        Self {
            field,
            operator,
            value: value.into(),
        }
    }

    /// A stable one-line rendering of the predicate, e.g. `after.user.country eq "DE"`.
    ///
    /// Used as the rule identity in [`UnmatchedRule::rule`](crate::transform::UnmatchedRule)
    /// and therefore as a Prometheus label value, so it must not vary run to run.
    pub fn describe(&self) -> String {
        let field = match &self.field {
            FilterField::Op => "op".to_string(),
            FilterField::Table => "table".to_string(),
            FilterField::AfterField(path) => format!("after.{path}"),
            FilterField::BeforeField(path) => format!("before.{path}"),
        };
        let operator = match self.operator {
            FilterOperator::Eq => "eq",
            FilterOperator::Ne => "ne",
            FilterOperator::Contains => "contains",
            FilterOperator::Regex => "regex",
            FilterOperator::Lt => "lt",
            FilterOperator::LtEq => "lteq",
            FilterOperator::Gt => "gt",
            FilterOperator::GtEq => "gteq",
        };
        format!("{field} {operator} {:?}", self.value)
    }
}

impl FilterProjectionConfig {
    /// Check the rule is well-formed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConfigError`] for an empty value
    /// or an invalid regex — caught at construction rather than silently at apply time,
    /// where the failure would arrive as dropped events.
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
                    rule.field
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
#[derive(Debug)]
pub struct FilterProjectionTransform {
    /// Filter and projection configuration.
    pub config: FilterProjectionConfig,
    /// Pre-built include set; `None` when `include_columns` is absent.
    include_set: Option<HashSet<String>>,
    /// Pre-built exclude set; `None` when `exclude_columns` is absent.
    exclude_set: Option<HashSet<String>>,
    /// Pre-compiled regexes, parallel to `config.filters`.
    /// `None` for rules whose operator is not [`FilterOperator::Regex`].
    compiled_regexes: Vec<Option<Regex>>,
    /// Times each rule was evaluated, parallel to `config.filters`.
    ///
    /// Tracked separately from `rule_matches` because [`FilterMode::All`] short-circuits:
    /// a rule that was never *reached* is not evidence of a typo, while one that was
    /// evaluated repeatedly and never matched is.
    rule_evaluations: Vec<std::sync::atomic::AtomicU64>,
    /// Times each rule returned `true`, parallel to `config.filters`.
    rule_matches: Vec<std::sync::atomic::AtomicU64>,
}

impl Clone for FilterProjectionTransform {
    /// Clones the configuration; hit counters start fresh in the clone.
    fn clone(&self) -> Self {
        Self::new(self.config.clone()).expect("configuration already validated once")
    }
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

        let rule_evaluations = config
            .filters
            .iter()
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect();
        let rule_matches = config
            .filters
            .iter()
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect();

        Ok(Self {
            config,
            include_set,
            exclude_set,
            compiled_regexes,
            rule_evaluations,
            rule_matches,
        })
    }

    /// How many events each filter rule has matched, by rule index.
    ///
    /// `None` when the index is out of range. A rule that has been evaluated many times
    /// and matched zero is the silent-misconfiguration signal — see
    /// [`Transform::unmatched_rules`].
    pub fn rule_hit_count(&self, index: usize) -> Option<u64> {
        use std::sync::atomic::Ordering;
        self.rule_matches
            .get(index)
            .map(|count| count.load(Ordering::Relaxed))
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
            .enumerate()
            .map(|(index, (rule, re))| {
                use std::sync::atomic::Ordering;
                // Two relaxed increments per rule per event. `all`/`any` below still
                // short-circuit, so a rule that was never reached records neither — which
                // is what makes the unmatched report trustworthy under `FilterMode::All`.
                self.rule_evaluations[index].fetch_add(1, Ordering::Relaxed);
                let matched = self.evaluate_rule(event, rule, re.as_ref());
                if matched {
                    self.rule_matches[index].fetch_add(1, Ordering::Relaxed);
                }
                matched
            });

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
/// For the ordering operators (`Lt`, `LtEq`, `Gt`, `GtEq`) both sides are compared as
/// **exact decimals** — see [`compare_decimal`]. When either side is not a plain decimal
/// numeral the comparison returns `false`, because there is no defined order between a
/// number and arbitrary text and guessing one silently changes which rows a filter keeps.
#[inline]
fn apply_operator(
    left: &str,
    op: &FilterOperator,
    rule_value: &str,
    regex: Option<&Regex>,
) -> bool {
    use std::cmp::Ordering;

    match op {
        FilterOperator::Eq => left == rule_value,
        FilterOperator::Ne => left != rule_value,
        FilterOperator::Contains => left.contains(rule_value),
        FilterOperator::Regex => regex.is_some_and(|re| re.is_match(left)),
        FilterOperator::Lt | FilterOperator::LtEq | FilterOperator::Gt | FilterOperator::GtEq => {
            let Some(ordering) = compare_decimal(left, rule_value) else {
                return false;
            };
            match op {
                FilterOperator::Lt => ordering == Ordering::Less,
                FilterOperator::LtEq => ordering != Ordering::Greater,
                FilterOperator::Gt => ordering == Ordering::Greater,
                FilterOperator::GtEq => ordering != Ordering::Less,
                // SAFETY: outer match arm already restricts to the four numeric variants above.
                _ => unreachable!(
                    "numeric comparison arm is exhausted by the outer Lt|LtEq|Gt|GtEq restriction"
                ),
            }
        }
    }
}

/// A decimal numeral split into sign and digit strings, with no precision loss.
struct Decimal<'a> {
    negative: bool,
    /// Integer digits with leading zeros stripped. Empty means zero.
    integer: &'a str,
    /// Fraction digits with trailing zeros stripped. Empty means none.
    fraction: &'a str,
}

impl Decimal<'_> {
    fn is_zero(&self) -> bool {
        self.integer.is_empty() && self.fraction.is_empty()
    }
}

/// Parse `[+|-]digits[.digits]`, rejecting anything else.
///
/// Deliberately does **not** accept exponent notation. `1e3` and `1000` would then have to
/// compare equal, which needs normalisation this function has nowhere to put; a filter
/// silently returning `false` for an exponent literal is easier to notice and fix than one
/// that quietly mis-orders it.
fn parse_decimal(input: &str) -> Option<Decimal<'_>> {
    let (negative, digits) = match input.as_bytes().first()? {
        b'-' => (true, &input[1..]),
        b'+' => (false, &input[1..]),
        _ => (false, input),
    };

    let (integer, fraction) = match digits.split_once('.') {
        Some((integer, fraction)) => (integer, fraction),
        None => (digits, ""),
    };

    // At least one digit overall, and nothing but digits in either part.
    if integer.is_empty() && fraction.is_empty() {
        return None;
    }
    if !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    Some(Decimal {
        negative,
        integer: integer.trim_start_matches('0'),
        fraction: fraction.trim_end_matches('0'),
    })
}

/// Order two decimal numerals **exactly**, or `None` when either side is not one.
///
/// # Why not `f64`
///
/// This crate emits every column value as text precisely because a JSON number is an
/// IEEE-754 double by the time most consumers see it, and `numeric(38,4)` and `bigint`
/// past 2^53 do not survive one. Routing a filter comparison through `f64` reintroduces
/// exactly that loss at the point where it decides whether a row is kept: with `f64`,
/// `9007199254740993 > 9007199254740992` is **false**, so a threshold filter on a
/// snowflake id or a high-precision amount silently drops or keeps the wrong rows.
///
/// Comparing digit strings has no such ceiling and needs no dependency: compare signs,
/// then integer magnitude by length and lexicographically, then fraction digits
/// lexicographically (which is correct once trailing zeros are stripped, because both
/// sides are then aligned at the decimal point).
fn compare_decimal(left: &str, right: &str) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;

    let left = parse_decimal(left.trim())?;
    let right = parse_decimal(right.trim())?;

    // `-0` and `0` are the same number, so sign is only meaningful for non-zero values.
    let left_negative = left.negative && !left.is_zero();
    let right_negative = right.negative && !right.is_zero();
    match (left_negative, right_negative) {
        (false, true) => return Some(Ordering::Greater),
        (true, false) => return Some(Ordering::Less),
        _ => {}
    }

    let magnitude = left
        .integer
        .len()
        .cmp(&right.integer.len())
        .then_with(|| left.integer.cmp(right.integer))
        .then_with(|| left.fraction.cmp(right.fraction));

    // Both negative: the larger magnitude is the smaller number.
    Some(if left_negative {
        magnitude.reverse()
    } else {
        magnitude
    })
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

impl Transform for FilterProjectionTransform {
    fn apply(&self, event: &mut Event) -> Result<bool> {
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

    fn unmatched_rules(&self) -> Vec<UnmatchedRule> {
        use std::sync::atomic::Ordering;

        const CONSEQUENCE: &str = "Rows those rules were meant to select or exclude are \
             being handled by whatever the remaining rules decide — a filter fails open \
             into the surrounding `FilterMode`, so a typo in a field path or a value that \
             never occurs produces no error, only a rule that silently contributes nothing.";

        self.config
            .filters
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                // Evaluated at least once and never matched. A rule that was never
                // reached — because an earlier rule short-circuited every event under
                // `FilterMode::All` — is not reported: it has had no chance to match.
                self.rule_evaluations[*index].load(Ordering::Relaxed) > 0
                    && self.rule_matches[*index].load(Ordering::Relaxed) == 0
            })
            .map(|(_, rule)| {
                UnmatchedRule::new(self.name(), "filter", rule.describe(), CONSEQUENCE)
            })
            .collect()
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
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
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
        assert!(!transform.apply(&mut e).unwrap());
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
        assert!(transform.apply(&mut e).unwrap());
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
        assert!(transform.apply(&mut e).unwrap());
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
        assert!(transform.apply(&mut e).is_err());
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

        assert!(transform.apply(&mut first).unwrap());
        assert!(transform.apply(&mut second).unwrap());
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
            transform.apply(&mut e).unwrap(),
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
            !transform.apply(&mut e).unwrap(),
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
            transform.apply(&mut e).unwrap(),
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
        assert!(transform.apply(&mut e).unwrap(), "\"alice\" matches ^ali");
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
        assert!(transform.apply(&mut e).unwrap(), "id=1 > 0");
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
            transform.apply(&mut e).unwrap(),
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
        assert!(transform.apply(&mut insert_users).unwrap());

        // Only table matches, op doesn't → drop.
        let mut update_users = event(Operation::Update);
        assert!(!transform.apply(&mut update_users).unwrap());
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
        assert!(transform.apply(&mut e).unwrap());
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
        assert!(!transform.apply(&mut e).unwrap());
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
        assert!(transform.apply(&mut matching).unwrap());

        let mut non_matching = event(Operation::Update); // op differs
        assert!(!transform.apply(&mut non_matching).unwrap());
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
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        };
        assert!(
            transform.apply(&mut e).unwrap(),
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
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        };
        assert!(
            !transform.apply(&mut e).unwrap(),
            "Truncate must be dropped when pass_through_truncate=false and rule does not match"
        );
    }

    // ─── Fail-open reporting ─────────────────────────────────────────────────

    #[test]
    fn a_filter_rule_that_never_matches_is_reported() {
        // A filter rule fails open into the surrounding `FilterMode`: a typo in a field
        // path makes the rule contribute nothing, with no error anywhere.
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            // The typo'd rule goes first: `FilterMode::Any` short-circuits on the first
            // `true`, so a rule placed after a matching one would never be evaluated.
            filters: vec![
                FilterRule::new(
                    FilterField::AfterField("secrt".into()), // typo for "secret"
                    FilterOperator::Eq,
                    "x",
                ),
                FilterRule::new(FilterField::Table, FilterOperator::Contains, "users"),
            ],
            filter_mode: FilterMode::Any,
            ..FilterProjectionConfig::default()
        })
        .expect("valid config");

        let mut e = event(Operation::Insert);
        e.table = "public.users".into();
        assert!(transform.apply(&mut e).unwrap());

        let reported = Transform::unmatched_rules(&transform);
        assert_eq!(reported.len(), 1, "only the typo'd rule must be reported");
        assert_eq!(reported[0].kind, "filter");
        assert!(
            reported[0].rule.contains("after.secrt"),
            "the report must identify the rule: {}",
            reported[0].rule
        );
        assert_eq!(
            transform.rule_hit_count(0),
            Some(0),
            "the typo'd rule never matched"
        );
        assert_eq!(transform.rule_hit_count(1), Some(1));
        assert_eq!(transform.rule_hit_count(2), None);
    }

    #[test]
    fn a_rule_that_was_never_reached_is_not_reported_as_unmatched() {
        // `FilterMode::All` short-circuits on the first false. A rule that never got a
        // chance to run has not failed to match — reporting it would be a false positive
        // that trains operators to ignore the signal.
        let transform = FilterProjectionTransform::new(FilterProjectionConfig {
            filters: vec![
                FilterRule::new(FilterField::Table, FilterOperator::Eq, "never.matches"),
                FilterRule::new(FilterField::Op, FilterOperator::Eq, "insert"),
            ],
            filter_mode: FilterMode::All,
            ..FilterProjectionConfig::default()
        })
        .expect("valid config");

        let mut e = event(Operation::Insert);
        e.table = "public.users".into();
        assert!(!transform.apply(&mut e).unwrap(), "the first rule drops it");

        let reported = Transform::unmatched_rules(&transform);
        assert_eq!(
            reported.len(),
            1,
            "only the evaluated-and-never-matched rule is reported, not the unreached one"
        );
        assert!(reported[0].rule.contains("table"));
        assert_eq!(
            transform.rule_hit_count(1),
            Some(0),
            "the second rule was never evaluated"
        );
    }

    #[test]
    fn describe_is_stable_and_identifies_the_rule() {
        // The rendering becomes a Prometheus label value, so it must not vary run to run.
        let rule = FilterRule::new(
            FilterField::AfterField("user.country".into()),
            FilterOperator::Eq,
            "DE",
        );
        assert_eq!(rule.describe(), r#"after.user.country eq "DE""#);
        assert_eq!(rule.describe(), rule.clone().describe());
    }
}

#[cfg(test)]
mod decimal_comparison_tests {
    use std::cmp::Ordering;

    use super::{apply_operator, compare_decimal, FilterOperator};

    /// The whole reason this crate emits column values as text: `f64` cannot tell these
    /// two integers apart, so an `f64`-based filter answered `9007199254740993 >
    /// 9007199254740992` with `false` and silently dropped the row.
    #[test]
    fn comparison_is_exact_past_the_f64_mantissa() {
        assert_eq!(
            compare_decimal("9007199254740993", "9007199254740992"),
            Some(Ordering::Greater)
        );
        assert!(apply_operator(
            "9007199254740993",
            &FilterOperator::Gt,
            "9007199254740992",
            None
        ));
        // Both round to the same f64, so the old implementation reported them equal.
        assert!(!apply_operator(
            "9007199254740992",
            &FilterOperator::Gt,
            "9007199254740993",
            None
        ));
    }

    /// `numeric(38,4)` exceeds `f64`'s 15–17 significant digits by a wide margin.
    #[test]
    fn comparison_is_exact_for_high_precision_decimals() {
        assert_eq!(
            compare_decimal("12345678901234567890.0001", "12345678901234567890.0002"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_decimal("0.30000000000000004", "0.3"),
            Some(Ordering::Greater),
            "the classic float artefact must not compare equal to 0.3"
        );
    }

    #[test]
    fn magnitude_is_ordered_by_digit_count_then_lexicographically() {
        assert_eq!(compare_decimal("100", "99"), Some(Ordering::Greater));
        assert_eq!(compare_decimal("0007", "7"), Some(Ordering::Equal));
        assert_eq!(compare_decimal("7.50", "7.5"), Some(Ordering::Equal));
        assert_eq!(compare_decimal("0.5", "0.25"), Some(Ordering::Greater));
        assert_eq!(compare_decimal("0.5", "0.55"), Some(Ordering::Less));
    }

    #[test]
    fn signs_are_ordered_and_negative_zero_equals_zero() {
        assert_eq!(compare_decimal("-1", "1"), Some(Ordering::Less));
        assert_eq!(compare_decimal("-100", "-99"), Some(Ordering::Less));
        assert_eq!(compare_decimal("-0", "0"), Some(Ordering::Equal));
        assert_eq!(compare_decimal("-0.0", "0"), Some(Ordering::Equal));
        assert_eq!(compare_decimal("+5", "5"), Some(Ordering::Equal));
    }

    /// A non-numeric side has no defined order, so the rule simply does not match.
    /// Guessing one would silently change which rows a filter keeps.
    #[test]
    fn non_numeric_and_exponent_operands_do_not_match() {
        assert_eq!(compare_decimal("abc", "1"), None);
        assert_eq!(compare_decimal("1", ""), None);
        assert_eq!(compare_decimal("1.2.3", "1"), None);
        assert_eq!(compare_decimal("1e3", "1000"), None);
        assert_eq!(compare_decimal("NaN", "0"), None);
        for op in [
            FilterOperator::Lt,
            FilterOperator::LtEq,
            FilterOperator::Gt,
            FilterOperator::GtEq,
        ] {
            assert!(
                !apply_operator("abc", &op, "1", None),
                "a non-numeric operand must never satisfy an ordering rule"
            );
        }
    }

    #[test]
    fn inclusive_and_exclusive_bounds_agree_on_equality() {
        assert!(apply_operator("5", &FilterOperator::LtEq, "5", None));
        assert!(apply_operator("5", &FilterOperator::GtEq, "5", None));
        assert!(!apply_operator("5", &FilterOperator::Lt, "5", None));
        assert!(!apply_operator("5", &FilterOperator::Gt, "5", None));
    }
}
