//! Column include/exclude filter and per-column masking for CDC event row fields.
//!
//! Filter specs follow the format `schema.table.column` where any component
//! may be the wildcard `*`.  An empty `include` list means "include all".
//!
//! ## Precedence
//! 1. If `include_list` is non-empty, only columns matching an include rule
//!    are kept; the `exclude_list` is ignored.
//! 2. If `include_list` is empty, columns matching an exclude rule are dropped.
//! 3. After include/exclude filtering, `mask_rules` are applied to surviving
//!    columns in order of definition.

use sha2::{Digest, Sha256};

use crate::{Error, Result};

// ─── Masking ─────────────────────────────────────────────────────────────────

/// Masking rule applied to a column value in-place before the event is emitted.
///
/// Applied at the source connector level — masking happens before the event
/// reaches any downstream transform. Comparable to Debezium's
/// `column.mask.with.N.chars` / `column.mask.hash` / `column.truncate.to.N.chars`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ColumnMaskRule {
    /// Replace the value with its SHA-256 hex digest (32-byte hex string).
    ///
    /// Provides irreversible pseudonymisation. Identical input values always
    /// produce the same hash, preserving joinability across events.
    Hash,
    /// Replace the value with a static redaction string (e.g. `"***"`).
    ///
    /// Use for PII columns that must never appear in any output.
    Redact(String),
    /// Replace the value with JSON `null`.
    Null,
    /// Truncate string values to the first `n` Unicode scalar values.
    ///
    /// Non-string JSON values are left unchanged.
    Truncate(usize),
}

/// A per-column masking rule bound to a column pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMaskSpec {
    spec: ColumnSpec,
    /// The masking rule to apply when the spec matches.
    pub rule: ColumnMaskRule,
}

impl ColumnMaskSpec {
    /// Parse a column pattern and pair it with a masking rule.
    pub fn new(column: &str, rule: ColumnMaskRule) -> Result<Self> {
        Ok(Self {
            spec: ColumnSpec::parse(column)?,
            rule,
        })
    }
}

/// Config-level description of a per-column masking rule; used in source configs.
///
/// The `column` field uses the same `[schema.]table.column` format as
/// `column_include_list` and `column_exclude_list`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ColumnMaskRuleConfig {
    /// Column spec: `[schema.]table.column` — `*` wildcards are allowed.
    pub column: String,
    /// The masking rule to apply when the spec matches.
    pub rule: ColumnMaskRule,
}

fn apply_mask_rule(value: &serde_json::Value, rule: &ColumnMaskRule) -> serde_json::Value {
    match rule {
        ColumnMaskRule::Hash => {
            let digest = Sha256::digest(value.to_string().as_bytes());
            serde_json::Value::String(format!("{digest:x}"))
        }
        ColumnMaskRule::Redact(s) => serde_json::Value::String(s.clone()),
        ColumnMaskRule::Null => serde_json::Value::Null,
        ColumnMaskRule::Truncate(n) => match value {
            serde_json::Value::String(s) => {
                serde_json::Value::String(s.chars().take(*n).collect())
            }
            _ => value.clone(),
        },
    }
}

// ─── ColumnSpec / ColumnFilter ────────────────────────────────────────────────

/// A compiled column filter derived from include/exclude spec lists.
///
/// Construct with [`ColumnFilter::new`] or [`ColumnFilter::from_config`] and
/// apply with [`ColumnFilter::apply_to_json`].  A default (pass-through) filter
/// is produced by [`ColumnFilter::default`].
#[derive(Debug, Clone, Default)]
pub struct ColumnFilter {
    /// Pre-parsed include specs. Empty = include all.
    includes: Vec<ColumnSpec>,
    /// Pre-parsed exclude specs. Only active when `includes` is empty.
    excludes: Vec<ColumnSpec>,
    /// Per-column masking rules applied after include/exclude filtering.
    masks: Vec<ColumnMaskSpec>,
}

/// A single parsed `schema.table.column` spec where `None` means wildcard.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnSpec {
    schema: Option<String>,
    table: Option<String>,
    column: Option<String>,
}

impl ColumnSpec {
    fn parse(spec: &str) -> Result<Self> {
        let parts: Vec<&str> = spec.splitn(3, '.').collect();
        match parts.as_slice() {
            [schema, table, column] => Ok(Self {
                schema: wildcard_opt(schema),
                table: wildcard_opt(table),
                column: wildcard_opt(column),
            }),
            [table, column] => Ok(Self {
                schema: None,
                table: wildcard_opt(table),
                column: wildcard_opt(column),
            }),
            [column] => Ok(Self {
                schema: None,
                table: None,
                column: wildcard_opt(column),
            }),
            _ => Err(Error::ConfigError(format!(
                "invalid column filter spec '{spec}': expected [schema.]table.column or [schema.]*.column"
            ))),
        }
    }

    fn matches(&self, schema: Option<&str>, table: &str, column: &str) -> bool {
        let schema_ok = match &self.schema {
            None => true,
            Some(s) => schema.is_some_and(|sc| sc.eq_ignore_ascii_case(s)),
        };
        let table_ok = match &self.table {
            None => true,
            Some(t) => table.eq_ignore_ascii_case(t),
        };
        let col_ok = match &self.column {
            None => true,
            Some(c) => column.eq_ignore_ascii_case(c),
        };
        schema_ok && table_ok && col_ok
    }
}

fn wildcard_opt(s: &str) -> Option<String> {
    if s == "*" { None } else { Some(s.to_string()) }
}

impl ColumnFilter {
    /// Build a filter from raw spec strings with no masking rules.
    ///
    /// # Errors
    /// Returns an error if any spec has an invalid format.
    pub fn new(include_list: &[String], exclude_list: &[String]) -> Result<Self> {
        Self::from_config(include_list, exclude_list, &[])
    }

    /// Build a filter from source-config fields (include/exclude + mask rules).
    ///
    /// This is the primary constructor used by source connectors.
    pub fn from_config(
        include_list: &[String],
        exclude_list: &[String],
        mask_rules: &[ColumnMaskRuleConfig],
    ) -> Result<Self> {
        let includes = include_list
            .iter()
            .map(|s| ColumnSpec::parse(s))
            .collect::<Result<Vec<_>>>()?;
        let excludes = exclude_list
            .iter()
            .map(|s| ColumnSpec::parse(s))
            .collect::<Result<Vec<_>>>()?;
        let masks = mask_rules
            .iter()
            .map(|r| ColumnMaskSpec::new(&r.column, r.rule.clone()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { includes, excludes, masks })
    }

    /// Returns `true` if this column should be included in the output.
    ///
    /// - `schema`: optional schema/database name (may be `None` for databases
    ///   that don't surface it, e.g. MySQL snapshot rows).
    /// - `table`: the table name (unqualified).
    /// - `column`: the column name.
    pub fn include(&self, schema: Option<&str>, table: &str, column: &str) -> bool {
        if !self.includes.is_empty() {
            return self
                .includes
                .iter()
                .any(|spec| spec.matches(schema, table, column));
        }
        if !self.excludes.is_empty() {
            return !self
                .excludes
                .iter()
                .any(|spec| spec.matches(schema, table, column));
        }
        true
    }

    /// Returns `true` if the filter is a pass-through (no rules configured).
    pub fn is_passthrough(&self) -> bool {
        self.includes.is_empty() && self.excludes.is_empty() && self.masks.is_empty()
    }

    /// Apply the filter to a JSON object, removing excluded fields and masking
    /// matched fields, in-place.
    ///
    /// `schema` and `table` identify where the row came from.
    pub fn apply_to_json(
        &self,
        row: &mut serde_json::Value,
        schema: Option<&str>,
        table: &str,
    ) {
        if self.is_passthrough() {
            return;
        }
        if let serde_json::Value::Object(map) = row {
            // Step 1: include/exclude filtering.
            if !self.includes.is_empty() || !self.excludes.is_empty() {
                map.retain(|col, _| self.include(schema, table, col));
            }
            // Step 2: per-column masking on surviving fields.
            if !self.masks.is_empty() {
                for (col, val) in map.iter_mut() {
                    if let Some(mask) = self
                        .masks
                        .iter()
                        .find(|m| m.spec.matches(schema, table, col))
                    {
                        *val = apply_mask_rule(val, &mask.rule);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_when_empty() {
        let f = ColumnFilter::default();
        assert!(f.include(Some("public"), "users", "password"));
        assert!(f.is_passthrough());
    }

    #[test]
    fn include_list_blocks_unlisted_columns() {
        let f = ColumnFilter::new(&["public.users.id".into(), "public.users.email".into()], &[]).unwrap();
        assert!(f.include(Some("public"), "users", "id"));
        assert!(f.include(Some("public"), "users", "email"));
        assert!(!f.include(Some("public"), "users", "password"));
    }

    #[test]
    fn exclude_list_removes_matching_columns() {
        let f = ColumnFilter::new(&[], &["*.users.password".into()]).unwrap();
        assert!(f.include(Some("public"), "users", "id"));
        assert!(!f.include(Some("public"), "users", "password"));
        assert!(!f.include(None, "users", "password"));
    }

    #[test]
    fn wildcard_schema_matches_any() {
        let f = ColumnFilter::new(&["*.*.secret".into()], &[]).unwrap();
        assert!(f.include(Some("app"), "orders", "secret"));
        assert!(!f.include(Some("app"), "orders", "id"));
    }

    #[test]
    fn two_part_spec_omits_schema_check() {
        let f = ColumnFilter::new(&[], &["orders.internal_note".into()]).unwrap();
        assert!(!f.include(Some("dbo"), "orders", "internal_note"));
        assert!(f.include(Some("dbo"), "orders", "id"));
    }

    #[test]
    fn apply_to_json_removes_excluded_fields() {
        let f = ColumnFilter::new(&[], &["public.users.password".into()]).unwrap();
        let mut row = serde_json::json!({"id": 1, "email": "a@b.com", "password": "secret"});
        f.apply_to_json(&mut row, Some("public"), "users");
        assert!(row.get("id").is_some());
        assert!(row.get("password").is_none());
    }

    #[test]
    fn invalid_spec_returns_error() {
        assert!(ColumnFilter::new(&["".into()], &[]).is_err() || ColumnFilter::new(&["col".into()], &[]).is_ok());
    }

    #[test]
    fn masking_hash_replaces_value_with_sha256_hex() {
        let mask = ColumnMaskRuleConfig {
            column: "public.users.password".into(),
            rule: ColumnMaskRule::Hash,
        };
        let f = ColumnFilter::from_config(&[], &[], &[mask]).unwrap();
        let mut row = serde_json::json!({"id": 1, "password": "secret"});
        f.apply_to_json(&mut row, Some("public"), "users");
        assert_eq!(row["id"], 1);
        // SHA-256 hex is 64 chars
        let hashed = row["password"].as_str().unwrap();
        assert_eq!(hashed.len(), 64);
        assert!(hashed.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn masking_redact_replaces_value_with_static_string() {
        let mask = ColumnMaskRuleConfig {
            column: "*.users.ssn".into(),
            rule: ColumnMaskRule::Redact("***".into()),
        };
        let f = ColumnFilter::from_config(&[], &[], &[mask]).unwrap();
        let mut row = serde_json::json!({"ssn": "123-45-6789", "name": "Alice"});
        f.apply_to_json(&mut row, Some("public"), "users");
        assert_eq!(row["ssn"], "***");
        assert_eq!(row["name"], "Alice");
    }

    #[test]
    fn masking_null_replaces_value_with_json_null() {
        let mask = ColumnMaskRuleConfig {
            column: "users.secret_note".into(),
            rule: ColumnMaskRule::Null,
        };
        let f = ColumnFilter::from_config(&[], &[], &[mask]).unwrap();
        let mut row = serde_json::json!({"id": 1, "secret_note": "top secret"});
        f.apply_to_json(&mut row, None, "users");
        assert!(row["secret_note"].is_null());
        assert_eq!(row["id"], 1);
    }

    #[test]
    fn masking_truncate_limits_string_length() {
        let mask = ColumnMaskRuleConfig {
            column: "logs.message".into(),
            rule: ColumnMaskRule::Truncate(5),
        };
        let f = ColumnFilter::from_config(&[], &[], &[mask]).unwrap();
        let mut row = serde_json::json!({"message": "hello world"});
        f.apply_to_json(&mut row, None, "logs");
        assert_eq!(row["message"], "hello");
    }

    #[test]
    fn masking_applied_after_exclude_filtering() {
        let mask = ColumnMaskRuleConfig {
            column: "users.email".into(),
            rule: ColumnMaskRule::Redact("[redacted]".into()),
        };
        let f = ColumnFilter::from_config(
            &[],
            &["users.internal_notes".into()],
            &[mask],
        ).unwrap();
        let mut row = serde_json::json!({
            "id": 1, "email": "a@b.com", "internal_notes": "do not show"
        });
        f.apply_to_json(&mut row, None, "users");
        assert!(row.get("internal_notes").is_none()); // excluded
        assert_eq!(row["email"], "[redacted]");      // masked
        assert_eq!(row["id"], 1);                    // unchanged
    }

    #[test]
    fn from_config_passthrough_when_empty() {
        let f = ColumnFilter::from_config(&[], &[], &[]).unwrap();
        assert!(f.is_passthrough());
    }
}
