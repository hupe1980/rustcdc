use rustcdc::wasm::{TransformResult, WasmConfig as RustcdcWasmConfig, WasmRuntime};
use rustcdc::{fingerprint_event_stable, Error, Event, Operation, Result};
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::schema::{
    TransformActionConfig, TransformKeySource, TransformMetadataField, TransformRuleConfig,
    TransformRuntimeConfig, TransformRuntimeMode, TransformWhenConfig,
};

pub struct TransformPipeline {
    rules: Vec<TransformRuleConfig>,
    runtime: TransformRuntime,
}

impl std::fmt::Debug for TransformPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let runtime_label = match &self.runtime {
            TransformRuntime::Native => "Native",
            TransformRuntime::Wasm(..) => "Wasm",
        };
        f.debug_struct("TransformPipeline")
            .field("rules_count", &self.rules.len())
            .field("runtime", &runtime_label)
            .finish()
    }
}

enum TransformRuntime {
    Native,
    /// rustcdc `WasmRuntime` behind a `Mutex` (auto-inits on first transform).
    Wasm(Arc<Mutex<WasmRuntime>>),
}

/// Snapshot of WASM runtime metrics sourced from `WasmRuntime::metrics()`.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct WasmRuntimeMetricsSnapshot {
    pub(crate) instance_pool_size: u64,
    pub(crate) transform_total: u64,
    pub(crate) transform_error_total: u64,
    pub(crate) filtered_total: u64,
    pub(crate) timeout_total: u64,
}

impl TransformPipeline {
    /// Build a `TransformPipeline` from configuration.
    ///
    /// For `mode = "wasm"`, this loads, validates, and pre-compiles the WASM
    /// module synchronously via `rustcdc::WasmRuntime::new_with_config`.  Any
    /// ABI contract violations (missing exports, wrong ABI version, forbidden
    /// imports) are returned as `Err` here so the server fails fast at startup.
    ///
    /// The WASM epoch ticker is started lazily on the first `apply()` call.
    pub fn from_config(
        runtime_cfg: TransformRuntimeConfig,
        rules: Vec<TransformRuleConfig>,
    ) -> Result<Self> {
        let runtime = match runtime_cfg.mode {
            TransformRuntimeMode::Native => TransformRuntime::Native,
            TransformRuntimeMode::Wasm => {
                let cfg = &runtime_cfg.wasm;
                let module_path = cfg.module_path.as_ref().ok_or_else(|| {
                    Error::ConfigError(
                        "transform_runtime.wasm.module_path must be set when mode = \"wasm\""
                            .to_string(),
                    )
                })?;
                let wasm_config = RustcdcWasmConfig {
                    module_path: module_path.clone(),
                    timeout_ms: cfg.timeout_ms,
                    // Use the tighter of max_memory_bytes and max_event_bytes as the
                    // memory limit so WasmRuntime::transform() enforces the per-event
                    // size guard in its single serialization — no second to_vec() needed
                    // in apply().  Ceiling-divide to bytes → MiB to stay within the
                    // operator's byte-precise intent.
                    memory_limit_mb: {
                        let limit_bytes = cfg.max_memory_bytes.min(cfg.max_event_bytes);
                        let mb = (limit_bytes as u64).div_ceil(1024 * 1024);
                        u64::max(1, mb)
                    },
                    instance_pool_size: cfg.instance_pool_size,
                    fuel_async_yield_interval: cfg.fuel_yield_interval,
                };
                let runtime = WasmRuntime::new_with_config(wasm_config)?;
                TransformRuntime::Wasm(Arc::new(Mutex::new(runtime)))
            }
        };

        Ok(Self { rules, runtime })
    }

    /// Returns a live metrics snapshot from the WASM runtime, or a zeroed
    /// snapshot when the pipeline uses native mode.
    pub(crate) async fn wasm_metrics(&self) -> WasmRuntimeMetricsSnapshot {
        match &self.runtime {
            TransformRuntime::Native => WasmRuntimeMetricsSnapshot::default(),
            TransformRuntime::Wasm(runtime) => {
                let guard = runtime.lock().await;
                let m = guard.metrics();
                WasmRuntimeMetricsSnapshot {
                    instance_pool_size: m.instance_pool_size as u64,
                    transform_total: m.transform_total,
                    transform_error_total: m.transform_error_total,
                    filtered_total: m.filtered_total,
                    timeout_total: m.timeout_total,
                }
            }
        }
    }

    pub async fn apply(&self, event: Event) -> Result<Option<Event>> {
        let native_rules_active = !self.rules.is_empty();
        let transformed = apply_rules(event, &self.rules)?;
        let Some(event) = transformed else {
            return Ok(None);
        };

        match &self.runtime {
            TransformRuntime::Native => {
                if native_rules_active {
                    Ok(Some(finalize_transformed(event)?))
                } else {
                    // Pass-through: the source already validated its own envelope;
                    // re-validating every event here would only add hot-path cost.
                    Ok(Some(event))
                }
            }
            TransformRuntime::Wasm(runtime) => {
                // WasmRuntime::transform() serializes the event exactly once
                // and enforces memory_limit_mb (which we set to min(max_memory_bytes,
                // max_event_bytes) at construction time).  No pre-serialization
                // is needed here — doing so would allocate and serialize twice.
                let mut guard = runtime.lock().await;
                match guard.transform(&event).await? {
                    TransformResult::Ok(transformed) => {
                        Ok(Some(finalize_transformed(*transformed)?))
                    }
                    TransformResult::Filtered => Ok(None),
                }
            }
        }
    }
}

/// Post-transform envelope hygiene: reconcile the availability lists with the
/// (possibly rewritten) payloads, then fail fast on a contract violation.
///
/// Runs only when a native rule or WASM module actually touched the event, so the
/// pass-through hot path stays validation-free. A rejected event is surfaced as a
/// transform error and handled by the configured `TransformErrorPolicy` — far better
/// than shipping a self-contradictory envelope that a correct downstream consumer
/// must refuse.
fn finalize_transformed(mut event: Event) -> Result<Event> {
    reconcile_availability_lists(&mut event);
    event.validate().map_err(|errors| {
        Error::ConfigError(format!(
            "transform produced an invalid event envelope for table '{}': {errors}",
            event.table
        ))
    })?;
    Ok(event)
}

/// Drop availability-list entries for columns a transform has materialized.
///
/// `unavailable_columns` / `before_unavailable_columns` are the source's claim that a
/// column's value could not be supplied (PostgreSQL unchanged-TOAST). A transform that
/// inserts or renames a column into the payload supersedes that claim — the payload now
/// carries a value, and `Event::validate()` (rustcdc ≥ 0.7.0) rejects the
/// present-*and*-listed contradiction because the dangerous reading (trust the payload)
/// is the one a sink takes.
fn reconcile_availability_lists(event: &mut Event) {
    if !event.unavailable_columns.is_empty() {
        if let Some(Value::Object(after)) = event.after.as_ref() {
            event
                .unavailable_columns
                .retain(|column| !after.contains_key(column));
        }
    }
    if !event.before_unavailable_columns.is_empty() {
        if let Some(Value::Object(before)) = event.before.as_ref() {
            event
                .before_unavailable_columns
                .retain(|column| !before.contains_key(column));
        }
    }
}

pub fn apply_rules(event: Event, rules: &[TransformRuleConfig]) -> Result<Option<Event>> {
    let mut current = event;

    for rule in rules {
        if !matches_when(&current, &rule.when) {
            continue;
        }

        for action in &rule.actions {
            let Some(next) = apply_action(current, action)? else {
                return Ok(None);
            };
            current = next;
        }
    }

    Ok(Some(current))
}

fn matches_when(event: &Event, when: &TransformWhenConfig) -> bool {
    matches_values(&event.table, &when.tables)
        && matches_optional_value(event.schema.as_deref(), &when.schemas)
        && matches_values(op_name(&event.op), &when.ops)
}

fn matches_values(value: &str, allowed: &[String]) -> bool {
    allowed.is_empty()
        || allowed
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(value))
}

fn matches_optional_value(value: Option<&str>, allowed: &[String]) -> bool {
    if allowed.is_empty() {
        return true;
    }
    let Some(value) = value else {
        return false;
    };
    allowed
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(value))
}

fn apply_action(event: Event, action: &TransformActionConfig) -> Result<Option<Event>> {
    match action {
        TransformActionConfig::Unwrap { field } => unwrap_after_field(event, field).map(Some),
        TransformActionConfig::Flatten { field, prefix } => {
            flatten_after_field(event, field, prefix.as_deref()).map(Some)
        }
        TransformActionConfig::Filter {
            include_tables,
            include_schemas,
            include_ops,
        } => {
            if !matches_values(&event.table, include_tables)
                || !matches_optional_value(event.schema.as_deref(), include_schemas)
                || !matches_values(op_name(&event.op), include_ops)
            {
                return Ok(None);
            }
            Ok(Some(event))
        }
        TransformActionConfig::Route { table, schema } => {
            let mut event = event;
            if let Some(table) = table {
                event.table = table.clone();
            }
            if let Some(schema) = schema {
                event.schema = Some(schema.clone());
            }
            Ok(Some(event))
        }
        TransformActionConfig::MetadataProjection {
            target_field,
            fields,
        } => project_metadata(event, target_field, fields).map(Some),
        TransformActionConfig::KeyShaping {
            target_field,
            source,
        } => shape_key(event, target_field, *source).map(Some),
    }
}

fn unwrap_after_field(mut event: Event, field: &str) -> Result<Event> {
    let table = event.table.clone();
    let map = ensure_after_object_mut(&mut event)?;
    let nested = map.remove(field).ok_or_else(|| {
        Error::ConfigError(format!(
            "transform unwrap failed: after.{field} is missing for table '{table}'"
        ))
    })?;

    let Value::Object(object) = nested else {
        return Err(Error::ConfigError(format!(
            "transform unwrap failed: after.{field} is not an object for table '{}'",
            event.table
        )));
    };

    event.after = Some(Value::Object(object));
    // The payload was replaced wholesale — the source's per-column availability
    // claims described the *old* shape and no longer apply to the unwrapped one.
    event.unavailable_columns.clear();
    Ok(event)
}

fn flatten_after_field(mut event: Event, field: &str, prefix: Option<&str>) -> Result<Event> {
    let table = event.table.clone();
    let map = ensure_after_object_mut(&mut event)?;
    let nested = map.remove(field).ok_or_else(|| {
        Error::ConfigError(format!(
            "transform flatten failed: after.{field} is missing for table '{table}'"
        ))
    })?;

    let Value::Object(object) = nested else {
        return Err(Error::ConfigError(format!(
            "transform flatten failed: after.{field} is not an object for table '{}'",
            event.table
        )));
    };

    for (key, value) in object {
        let merged_key = match prefix {
            Some(prefix) => format!("{prefix}{key}"),
            None => key,
        };
        map.insert(merged_key, value);
    }

    Ok(event)
}

fn project_metadata(
    mut event: Event,
    target_field: &str,
    fields: &[TransformMetadataField],
) -> Result<Event> {
    let mut metadata = Map::new();

    for field in fields {
        match field {
            TransformMetadataField::SourceName => {
                metadata.insert(
                    "source_name".to_string(),
                    Value::String(event.source.source_name.clone()),
                );
            }
            TransformMetadataField::Offset => {
                metadata.insert(
                    "offset".to_string(),
                    Value::String(event.source.offset.clone()),
                );
            }
            TransformMetadataField::SourceTimestamp => {
                metadata.insert(
                    "source_timestamp".to_string(),
                    Value::Number(event.source.timestamp.into()),
                );
            }
            TransformMetadataField::EventTimestamp => {
                metadata.insert(
                    "event_timestamp".to_string(),
                    Value::Number(event.ts.into()),
                );
            }
            TransformMetadataField::Schema => {
                metadata.insert(
                    "schema".to_string(),
                    event
                        .schema
                        .as_ref()
                        .map_or(Value::Null, |schema| Value::String(schema.clone())),
                );
            }
            TransformMetadataField::Table => {
                metadata.insert("table".to_string(), Value::String(event.table.clone()));
            }
            TransformMetadataField::Operation => {
                metadata.insert(
                    "operation".to_string(),
                    Value::String(op_name(&event.op).to_string()),
                );
            }
            TransformMetadataField::PrimaryKey => {
                metadata.insert(
                    "primary_key".to_string(),
                    event.primary_key.as_ref().map_or(Value::Null, |keys| {
                        Value::Array(keys.iter().map(|key| Value::String(key.clone())).collect())
                    }),
                );
            }
        }
    }

    let map = ensure_after_object_mut(&mut event)?;
    map.insert(target_field.to_string(), Value::Object(metadata));
    Ok(event)
}

fn shape_key(mut event: Event, target_field: &str, source: TransformKeySource) -> Result<Event> {
    let key_value = match source {
        TransformKeySource::PrimaryKey => {
            match (event.primary_key.as_ref(), event.after.as_ref()) {
                (Some(primary_keys), Some(Value::Object(after))) => {
                    let mut shaped = Map::new();
                    for key in primary_keys {
                        let value = after.get(key).cloned().unwrap_or(Value::Null);
                        shaped.insert(key.clone(), value);
                    }
                    Value::Object(shaped)
                }
                _ => Value::Null,
            }
        }
        TransformKeySource::Fingerprint => {
            let fingerprint = fingerprint_event_stable(&event)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            Value::String(fingerprint)
        }
    };

    let map = ensure_after_object_mut(&mut event)?;
    map.insert(target_field.to_string(), key_value);
    Ok(event)
}

fn ensure_after_object_mut(event: &mut Event) -> Result<&mut Map<String, Value>> {
    if event.after.is_none() {
        event.after = Some(Value::Object(Map::new()));
    }

    match event.after.as_mut() {
        Some(Value::Object(map)) => Ok(map),
        _ => Err(Error::ConfigError(format!(
            "transform requires event.after object for table '{}'",
            event.table
        ))),
    }
}

fn op_name(op: &Operation) -> &'static str {
    match op {
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
        Operation::Read => "read",
        Operation::SchemaChange => "schema_change",
        Operation::Truncate => "truncate",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use crate::config::schema::{
        TransformRuntimeConfig, TransformRuntimeMode, WasmTransformConfig,
    };
    use rustcdc::core::{SourceMetadata, EVENT_ENVELOPE_VERSION};
    use serde_json::json;
    use tempfile::TempDir;

    fn sample_event() -> Event {
        Event {
            before: None,
            after: Some(json!({
                "id": 42,
                "customer": {
                    "name": "alice",
                    "tier": "gold"
                },
                "region": "eu-west-1"
            })),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "postgres".to_string(),
                offset: "0/16B6A71".to_string(),
                timestamp: 10,
            },
            ts: 11,
            schema: Some("public".to_string()),
            table: "users".to_string(),
            primary_key: Some(vec!["id".to_string()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    #[test]
    fn ordered_rules_are_deterministic() {
        let event = sample_event();
        let rules = vec![
            TransformRuleConfig {
                name: "route_a".to_string(),
                when: TransformWhenConfig::default(),
                actions: vec![TransformActionConfig::Route {
                    table: Some("users_a".to_string()),
                    schema: None,
                }],
            },
            TransformRuleConfig {
                name: "route_b".to_string(),
                when: TransformWhenConfig::default(),
                actions: vec![TransformActionConfig::Route {
                    table: Some("users_b".to_string()),
                    schema: None,
                }],
            },
        ];

        let transformed = apply_rules(event, &rules)
            .expect("apply")
            .expect("kept event");
        assert_eq!(transformed.table, "users_b");
    }

    #[test]
    fn filter_can_drop_event() {
        let event = sample_event();
        let rules = vec![TransformRuleConfig {
            name: "drop_non_orders".to_string(),
            when: TransformWhenConfig::default(),
            actions: vec![TransformActionConfig::Filter {
                include_tables: vec!["orders".to_string()],
                include_schemas: Vec::new(),
                include_ops: Vec::new(),
            }],
        }];

        assert!(apply_rules(event, &rules).expect("apply").is_none());
    }

    #[test]
    fn unwrap_flatten_projection_and_key_shape_work_together() {
        let event = sample_event();
        let rules = vec![TransformRuleConfig {
            name: "pipeline".to_string(),
            when: TransformWhenConfig {
                tables: vec!["users".to_string()],
                schemas: vec!["public".to_string()],
                ops: vec!["insert".to_string()],
            },
            actions: vec![
                TransformActionConfig::Flatten {
                    field: "customer".to_string(),
                    prefix: Some("customer_".to_string()),
                },
                TransformActionConfig::MetadataProjection {
                    target_field: "_meta".to_string(),
                    fields: vec![
                        TransformMetadataField::SourceName,
                        TransformMetadataField::Operation,
                        TransformMetadataField::Table,
                    ],
                },
                TransformActionConfig::KeyShaping {
                    target_field: "_key".to_string(),
                    source: TransformKeySource::PrimaryKey,
                },
            ],
        }];

        let transformed = apply_rules(event, &rules)
            .expect("apply")
            .expect("kept event");

        let after = transformed.after.expect("after payload");
        let obj = after.as_object().expect("object payload");
        assert_eq!(obj.get("customer_name"), Some(&json!("alice")));
        assert_eq!(obj.get("customer_tier"), Some(&json!("gold")));
        assert_eq!(obj.get("_key"), Some(&json!({"id": 42})));
        assert_eq!(
            obj.get("_meta"),
            Some(&json!({
                "source_name": "postgres",
                "operation": "insert",
                "table": "users"
            }))
        );
    }

    #[test]
    fn fingerprint_key_shape_is_stable() {
        let event = sample_event();
        let rules = vec![TransformRuleConfig {
            name: "fp".to_string(),
            when: TransformWhenConfig::default(),
            actions: vec![TransformActionConfig::KeyShaping {
                target_field: "_fingerprint".to_string(),
                source: TransformKeySource::Fingerprint,
            }],
        }];

        let first = apply_rules(event.clone(), &rules)
            .expect("apply")
            .expect("kept");
        let second = apply_rules(event, &rules).expect("apply").expect("kept");

        let first_after = first.after.expect("after");
        let second_after = second.after.expect("after");
        assert_eq!(
            first_after.get("_fingerprint"),
            second_after.get("_fingerprint")
        );
    }

    /// A transform that materializes a column supersedes the source's claim that
    /// the column was unavailable; entries for still-absent columns must survive.
    #[test]
    fn finalize_drops_materialized_availability_entries_and_keeps_real_holes() {
        let mut event = sample_event();
        event.unavailable_columns = vec!["_meta".to_string(), "large_toast_doc".to_string()];

        let rules = vec![TransformRuleConfig {
            name: "meta".to_string(),
            when: TransformWhenConfig::default(),
            actions: vec![TransformActionConfig::MetadataProjection {
                target_field: "_meta".to_string(),
                fields: vec![TransformMetadataField::SourceName],
            }],
        }];

        let transformed = apply_rules(event, &rules)
            .expect("apply")
            .expect("kept event");
        let finalized = finalize_transformed(transformed).expect("valid envelope");

        assert_eq!(
            finalized.unavailable_columns,
            vec!["large_toast_doc".to_string()],
            "materialized column must leave the list; the genuine TOAST hole must stay"
        );
        assert!(finalized.validate().is_ok());
    }

    /// Unwrap replaces `after` wholesale — stale availability claims about the old
    /// shape must not survive into the new one.
    #[test]
    fn unwrap_clears_stale_availability_claims() {
        let mut event = sample_event();
        event.unavailable_columns = vec!["region".to_string()];

        let rules = vec![TransformRuleConfig {
            name: "unwrap".to_string(),
            when: TransformWhenConfig::default(),
            actions: vec![TransformActionConfig::Unwrap {
                field: "customer".to_string(),
            }],
        }];

        let transformed = apply_rules(event, &rules)
            .expect("apply")
            .expect("kept event");
        assert!(transformed.unavailable_columns.is_empty());
        assert!(finalize_transformed(transformed).is_ok());
    }

    /// A (mis-)transform that rewrites the op to TRUNCATE while leaving an
    /// availability list behind produces a contract violation the runtime must
    /// reject rather than ship.
    #[test]
    fn finalize_rejects_envelope_contract_violations() {
        let mut event = sample_event();
        event.op = Operation::Truncate;
        event.before = None;
        event.after = None;
        event.primary_key = None;
        event.unavailable_columns = vec!["ghost".to_string()];

        let err = finalize_transformed(event).expect_err("must reject");
        assert!(
            err.to_string().contains("invalid event envelope"),
            "unexpected error: {err}"
        );
    }

    fn wasm_fixture_dir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    fn write_wasm_module(dir: &TempDir, file_name: &str, wat_source: &str) -> PathBuf {
        let wasm_bytes = wat::parse_str(wat_source).expect("valid wat");
        let path = dir.path().join(file_name);
        std::fs::write(&path, wasm_bytes).expect("write wasm");
        path
    }

    #[tokio::test]
    async fn wasm_runtime_can_pass_through_event_json() {
        let dir = wasm_fixture_dir();
        let module_path = write_wasm_module(
            &dir,
            "passthrough.wasm",
            r#"
            (module
              (memory (export "memory") 2 2)
              (global $heap (mut i32) (i32.const 8))
              (func (export "alloc") (param $size i32) (result i32)
                (local $ptr i32)
                global.get $heap
                local.tee $ptr
                local.get $size
                i32.add
                global.set $heap
                local.get $ptr)
              (func (export "dealloc") (param i32) (param i32))
              (func (export "rustcdc_abi_version") (result i32) i32.const 2)
              (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
                i32.const 65536
                local.get $ptr
                local.get $len
                memory.copy

                i32.const 65536
                i64.extend_i32_u
                i64.const 32
                i64.shl
                local.get $len
                i64.extend_i32_u
                i64.or))
            "#,
        );

        let runtime_cfg = TransformRuntimeConfig {
            mode: TransformRuntimeMode::Wasm,
            wasm: WasmTransformConfig {
                module_path: Some(module_path),
                max_memory_bytes: 128 * 1024,
                max_event_bytes: 64 * 1024,
                ..WasmTransformConfig::default()
            },
        };

        let pipeline = TransformPipeline::from_config(runtime_cfg, Vec::new()).expect("pipeline");
        let event = sample_event();
        let transformed = pipeline
            .apply(event.clone())
            .await
            .expect("apply")
            .expect("kept");

        assert_eq!(transformed.table, event.table);
        assert_eq!(transformed.after, event.after);
    }

    #[tokio::test]
    async fn wasm_runtime_can_drop_event_with_zero_length_result() {
        let dir = wasm_fixture_dir();
        let module_path = write_wasm_module(
            &dir,
            "drop.wasm",
            r#"
            (module
              (memory (export "memory") 1 1)
              (global $heap (mut i32) (i32.const 8))
              (func (export "alloc") (param $size i32) (result i32)
                (local $ptr i32)
                global.get $heap
                local.tee $ptr
                local.get $size
                i32.add
                global.set $heap
                local.get $ptr)
              (func (export "dealloc") (param i32) (param i32))
              (func (export "rustcdc_abi_version") (result i32) i32.const 2)
              (func (export "transform") (param i32) (param i32) (result i64)
                i64.const 0))
            "#,
        );

        let runtime_cfg = TransformRuntimeConfig {
            mode: TransformRuntimeMode::Wasm,
            wasm: WasmTransformConfig {
                module_path: Some(module_path),
                ..WasmTransformConfig::default()
            },
        };

        let pipeline = TransformPipeline::from_config(runtime_cfg, Vec::new()).expect("pipeline");
        let dropped = pipeline.apply(sample_event()).await.expect("apply");

        assert!(dropped.is_none());
    }

    #[tokio::test]
    async fn wasm_runtime_isolates_each_event_invocation() {
        let dir = wasm_fixture_dir();
        let module_path = write_wasm_module(
            &dir,
            "passthrough-reuse.wasm",
            r#"
            (module
              (memory (export "memory") 2 2)
              (global $heap (mut i32) (i32.const 8))
              (func (export "alloc") (param $size i32) (result i32)
                (local $ptr i32)
                global.get $heap
                local.tee $ptr
                local.get $size
                i32.add
                global.set $heap
                local.get $ptr)
              (func (export "dealloc") (param i32) (param i32))
              (func (export "rustcdc_abi_version") (result i32) i32.const 2)
              (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
                i32.const 65536
                local.get $ptr
                local.get $len
                memory.copy

                i32.const 65536
                i64.extend_i32_u
                i64.const 32
                i64.shl
                local.get $len
                i64.extend_i32_u
                i64.or))
            "#,
        );

        let runtime_cfg = TransformRuntimeConfig {
            mode: TransformRuntimeMode::Wasm,
            wasm: WasmTransformConfig {
                module_path: Some(module_path),
                instance_pool_size: 2,
                max_memory_bytes: 128 * 1024,
                max_event_bytes: 64 * 1024,
                ..WasmTransformConfig::default()
            },
        };

        let pipeline = TransformPipeline::from_config(runtime_cfg, Vec::new()).expect("pipeline");
        for i in 0..50 {
            let mut event = sample_event();
            event.ts += i;
            let transformed = pipeline
                .apply(event.clone())
                .await
                .expect("apply")
                .expect("kept event");
            assert_eq!(transformed.table, event.table);
            assert_eq!(transformed.after, event.after);
        }
    }

    #[tokio::test]
    async fn wasm_runtime_worker_pool_handles_many_invocations() {
        let dir = wasm_fixture_dir();
        let module_path = write_wasm_module(
            &dir,
            "passthrough-pool.wasm",
            r#"
            (module
              (memory (export "memory") 2 2)
              (global $heap (mut i32) (i32.const 8))
              (func (export "alloc") (param $size i32) (result i32)
                (local $ptr i32)
                global.get $heap
                local.tee $ptr
                local.get $size
                i32.add
                global.set $heap
                local.get $ptr)
              (func (export "dealloc") (param i32) (param i32))
              (func (export "rustcdc_abi_version") (result i32) i32.const 2)
              (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
                i32.const 65536
                local.get $ptr
                local.get $len
                memory.copy

                i32.const 65536
                i64.extend_i32_u
                i64.const 32
                i64.shl
                local.get $len
                i64.extend_i32_u
                i64.or))
            "#,
        );

        let runtime_cfg = TransformRuntimeConfig {
            mode: TransformRuntimeMode::Wasm,
            wasm: WasmTransformConfig {
                module_path: Some(module_path),
                instance_pool_size: 4,
                max_memory_bytes: 128 * 1024,
                max_event_bytes: 64 * 1024,
                ..WasmTransformConfig::default()
            },
        };

        let pipeline = TransformPipeline::from_config(runtime_cfg, Vec::new()).expect("pipeline");
        for i in 0..200 {
            let mut event = sample_event();
            event.ts += i;
            let transformed = pipeline
                .apply(event.clone())
                .await
                .expect("apply")
                .expect("kept event");
            assert_eq!(transformed.table, event.table);
            assert_eq!(transformed.after, event.after);
        }
    }
}
