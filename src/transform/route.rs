//! Route events to destination labels.
//!
//! Routing priority:
//! 1. Exact table-name match in [`RouteConfig::routing_table`].
//! 2. First matching regex pattern in [`RouteConfig::route_patterns_raw`].
//! 3. [`RouteConfig::default_destination`] when non-empty.
//! 4. `Err(TransformError)` — no route found.

use ahash::AHashMap as HashMap;
use async_trait::async_trait;
use regex::Regex;
use serde_json::Value;

use crate::core::{Error, Event, Result};

use super::Transform;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteConfig {
    /// Exact source table name → destination label (highest priority).
    pub routing_table: HashMap<String, String>,
    /// Regex pattern → destination label, matched in order against `event.table`.
    ///
    /// Patterns are stored as raw strings for `serde` compatibility.  They are
    /// compiled once at [`RouteTransform::new`] time — use [`RouteConfig::validate`]
    /// to surface compile errors before calling `new`.
    pub route_patterns_raw: Vec<(String, String)>,
    pub default_destination: String,
    /// Inject the resolved destination into `event.after["_destination"]`.
    pub add_destination_field: bool,
    /// Update `event.table` to the resolved destination.
    ///
    /// Useful when the downstream sink uses `event.table` as the physical
    /// topic / index / bucket name (e.g., Kafka topic routing).
    pub update_table: bool,
}

impl RouteConfig {
    /// Pre-validate all regex patterns in `route_patterns_raw`.
    ///
    /// Returns the first compile error found, if any.  Call this before
    /// [`RouteTransform::new`] when loading config from external sources.
    pub fn validate(&self) -> Result<()> {
        for (pattern, _) in &self.route_patterns_raw {
            Regex::new(pattern).map_err(|error| {
                Error::TransformError(format!(
                    "route transform invalid regex pattern '{pattern}': {error}"
                ))
            })?;
        }
        Ok(())
    }
}

/// A compiled routing transform.
///
/// Routing priority: exact-table → regex-pattern → default.
#[derive(Debug, Clone)]
pub struct RouteTransform {
    pub config: RouteConfig,
    /// Pre-compiled regex patterns derived from `config.route_patterns_raw`.
    patterns: Vec<(Regex, String)>,
}

impl RouteTransform {
    /// Create a new transform, pre-compiling all regex patterns.
    ///
    /// Returns an error if any pattern in `config.route_patterns_raw` is invalid.
    pub fn new(config: RouteConfig) -> Result<Self> {
        let patterns = config
            .route_patterns_raw
            .iter()
            .map(|(pattern, dest)| {
                Regex::new(pattern)
                    .map(|re| (re, dest.clone()))
                    .map_err(|error| {
                        Error::TransformError(format!(
                            "route transform invalid regex pattern '{pattern}': {error}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { config, patterns })
    }

    fn destination_for(&self, table: &str) -> Result<String> {
        // 1. Exact match.
        if let Some(mapped) = self.config.routing_table.get(table) {
            return Ok(mapped.clone());
        }
        // 2. Regex patterns in declaration order.
        for (re, dest) in &self.patterns {
            if re.is_match(table) {
                return Ok(dest.clone());
            }
        }
        // 3. Default destination.
        if !self.config.default_destination.trim().is_empty() {
            return Ok(self.config.default_destination.clone());
        }
        Err(Error::TransformError(format!(
            "route transform missing destination for table={table:?}"
        )))
    }
}

#[async_trait]
impl Transform for RouteTransform {
    async fn apply(&self, event: &mut Event) -> Result<bool> {
        let destination = self.destination_for(&event.table)?;

        if self.config.update_table {
            event.table = destination.clone();
        }

        if self.config.add_destination_field {
            // Only inject `_destination` into existing payloads.  Creating a
            // synthetic `after` object for Truncate / Delete events would violate
            // the canonical event envelope contract (those operations must have
            // `after = None`) and break downstream `event.validate()` calls.
            if let Some(Value::Object(object)) = event.after.as_mut() {
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
    use ahash::AHashMap as HashMap;

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
            before_is_key_only: false,
        }
    }

    fn simple_transform(table: &str, dest: &str) -> RouteTransform {
        let mut map = HashMap::new();
        map.insert(table.into(), dest.into());
        RouteTransform::new(RouteConfig {
            routing_table: map,
            route_patterns_raw: vec![],
            default_destination: String::new(),
            add_destination_field: true,
            update_table: false,
        })
        .expect("valid config")
    }

    #[tokio::test]
    async fn routes_event_to_mapped_destination() {
        let transform = simple_transform("users", "topic-users");
        let mut e = event("users");
        assert!(transform.apply(&mut e).await.unwrap());
        assert_eq!(e.after.unwrap()["_destination"], "topic-users");
    }

    #[tokio::test]
    async fn unmapped_table_uses_default_destination() {
        let transform = RouteTransform::new(RouteConfig {
            routing_table: HashMap::new(),
            route_patterns_raw: vec![],
            default_destination: "topic-default".into(),
            add_destination_field: true,
            update_table: false,
        })
        .unwrap();

        let mut e = event("orders");
        assert!(transform.apply(&mut e).await.unwrap());
        assert_eq!(e.after.unwrap()["_destination"], "topic-default");
    }

    #[tokio::test]
    async fn missing_mapping_without_default_errors() {
        let transform = RouteTransform::new(RouteConfig::default()).unwrap();
        let mut e = event("orders");
        assert!(transform.apply(&mut e).await.is_err());
    }

    #[tokio::test]
    async fn routing_is_deterministic() {
        let transform = simple_transform("users", "topic-users");
        let mut first = event("users");
        let mut second = event("users");
        assert!(transform.apply(&mut first).await.unwrap());
        assert!(transform.apply(&mut second).await.unwrap());
        assert_eq!(first.after, second.after);
    }

    #[tokio::test]
    async fn update_table_renames_event_table() {
        let mut map = HashMap::new();
        map.insert("users".into(), "topic-users".into());
        let transform = RouteTransform::new(RouteConfig {
            routing_table: map,
            route_patterns_raw: vec![],
            default_destination: String::new(),
            add_destination_field: false,
            update_table: true,
        })
        .unwrap();

        let mut e = event("users");
        assert!(transform.apply(&mut e).await.unwrap());
        assert_eq!(
            e.table, "topic-users",
            "event.table must be updated to destination"
        );
    }

    #[tokio::test]
    async fn update_table_and_add_destination_field_together() {
        let mut map = HashMap::new();
        map.insert("orders".into(), "topic-orders".into());
        let transform = RouteTransform::new(RouteConfig {
            routing_table: map,
            route_patterns_raw: vec![],
            default_destination: String::new(),
            add_destination_field: true,
            update_table: true,
        })
        .unwrap();

        let mut e = event("orders");
        assert!(transform.apply(&mut e).await.unwrap());
        assert_eq!(e.table, "topic-orders");
        assert_eq!(e.after.as_ref().unwrap()["_destination"], "topic-orders");
    }

    // ── Regex routing tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn regex_pattern_matches_table() {
        let transform = RouteTransform::new(RouteConfig {
            routing_table: HashMap::new(),
            route_patterns_raw: vec![("^orders_.*".into(), "topic-orders".into())],
            default_destination: "topic-default".into(),
            add_destination_field: true,
            update_table: false,
        })
        .unwrap();

        let mut e = event("orders_2024");
        assert!(transform.apply(&mut e).await.unwrap());
        assert_eq!(e.after.unwrap()["_destination"], "topic-orders");
    }

    #[tokio::test]
    async fn exact_match_beats_regex() {
        let mut map = HashMap::new();
        map.insert("orders_vip".into(), "topic-vip".into());
        let transform = RouteTransform::new(RouteConfig {
            routing_table: map,
            route_patterns_raw: vec![("^orders_.*".into(), "topic-orders".into())],
            default_destination: String::new(),
            add_destination_field: true,
            update_table: false,
        })
        .unwrap();

        let mut e = event("orders_vip");
        assert!(transform.apply(&mut e).await.unwrap());
        // Exact match wins over the regex pattern.
        assert_eq!(e.after.unwrap()["_destination"], "topic-vip");
    }

    #[tokio::test]
    async fn first_matching_regex_wins() {
        let transform = RouteTransform::new(RouteConfig {
            routing_table: HashMap::new(),
            route_patterns_raw: vec![
                ("^orders_.*".into(), "topic-orders".into()),
                (".*_archive$".into(), "topic-archive".into()),
            ],
            default_destination: String::new(),
            add_destination_field: true,
            update_table: false,
        })
        .unwrap();

        // Matches both patterns; first one wins.
        let mut e = event("orders_archive");
        assert!(transform.apply(&mut e).await.unwrap());
        assert_eq!(e.after.unwrap()["_destination"], "topic-orders");
    }

    #[tokio::test]
    async fn regex_falls_through_to_default_when_no_match() {
        let transform = RouteTransform::new(RouteConfig {
            routing_table: HashMap::new(),
            route_patterns_raw: vec![("^orders_.*".into(), "topic-orders".into())],
            default_destination: "topic-catch-all".into(),
            add_destination_field: true,
            update_table: false,
        })
        .unwrap();

        let mut e = event("payments");
        assert!(transform.apply(&mut e).await.unwrap());
        assert_eq!(e.after.unwrap()["_destination"], "topic-catch-all");
    }

    #[tokio::test]
    async fn invalid_regex_errors_at_construction() {
        let result = RouteTransform::new(RouteConfig {
            routing_table: HashMap::new(),
            route_patterns_raw: vec![("[unclosed".into(), "dest".into())],
            default_destination: String::new(),
            add_destination_field: false,
            update_table: false,
        });
        assert!(result.is_err(), "invalid regex must fail at construction");
    }

    #[tokio::test]
    async fn validate_rejects_invalid_pattern() {
        let config = RouteConfig {
            routing_table: HashMap::new(),
            route_patterns_raw: vec![("[bad".into(), "dest".into())],
            default_destination: String::new(),
            add_destination_field: false,
            update_table: false,
        };
        assert!(config.validate().is_err());
    }

    // ── Truncate / no-payload events ─────────────────────────────────────────

    #[tokio::test]
    async fn truncate_event_does_not_create_phantom_after() {
        // Truncate events have after = None.  RouteTransform must not create a
        // synthetic `after` object when add_destination_field = true.
        let mut map = HashMap::new();
        map.insert("users".into(), "topic-users".into());
        let transform = RouteTransform::new(RouteConfig {
            routing_table: map,
            route_patterns_raw: vec![],
            default_destination: String::new(),
            add_destination_field: true,
            update_table: true,
        })
        .unwrap();

        let mut e = crate::core::Event {
            before: None,
            after: None,
            op: crate::core::Operation::Truncate,
            source: crate::core::SourceMetadata {
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
            envelope_version: crate::core::EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
        };
        assert!(transform.apply(&mut e).await.unwrap());
        assert!(
            e.after.is_none(),
            "after must remain None for Truncate events — no phantom payload"
        );
        // update_table still applies even for Truncate.
        assert_eq!(e.table, "topic-users");
    }

    #[tokio::test]
    async fn regex_routing_updates_table_field() {
        let transform = RouteTransform::new(RouteConfig {
            routing_table: HashMap::new(),
            route_patterns_raw: vec![("^public\\..*".into(), "topic-public".into())],
            default_destination: String::new(),
            add_destination_field: false,
            update_table: true,
        })
        .unwrap();

        let mut e = event("public.users");
        assert!(transform.apply(&mut e).await.unwrap());
        assert_eq!(e.table, "topic-public");
    }
}
