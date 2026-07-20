use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline (transforms + routing)
// ─────────────────────────────────────────────────────────────────────────────

/// Transform rules and execution settings bundled as a single configuration
/// block.
///
/// # TOML structure
///
/// ```toml
/// [pipeline.transform_runtime]
/// mode = "native"   # native (default) | wasm
///
/// [[pipeline.transforms]]
/// name = "redact-pii"
/// [pipeline.transforms.when]
/// tables = ["public.users"]
/// [pipeline.transforms.actions]
/// # ...
/// ```
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct PipelineConfig {
    /// Ordered transform rules applied before sink delivery.
    #[serde(default)]
    pub transforms: Vec<TransformRuleConfig>,

    /// Transform runtime execution mode and safety limits.
    #[serde(default)]
    pub transform_runtime: TransformRuntimeConfig,

    /// Table-routing rules: each entry maps a glob pattern to a named sink.
    ///
    /// Rules are evaluated in order; the first match wins.  Events that match
    /// no rule are delivered to the default `[sink]`.
    ///
    /// Requires corresponding entries in the top-level `[[sinks]]` array.
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Routing rules
// ─────────────────────────────────────────────────────────────────────────────

/// A single table-routing rule.
///
/// ```toml
/// [[pipeline.routes]]
/// table_pattern = "public.orders"   # glob (supports * and ?)
/// sink = "kafka_avro"               # references [[sinks]] name
/// ```
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct RouteConfig {
    /// Glob pattern matched against `event.table` (e.g. `"public.*"`, `"*.orders"`).
    pub table_pattern: String,
    /// Name of the target sink in the `[[sinks]]` array.
    pub sink: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Transform runtime
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct TransformRuntimeConfig {
    #[serde(default)]
    pub mode: TransformRuntimeMode,

    #[serde(default)]
    pub wasm: WasmTransformConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TransformRuntimeMode {
    #[default]
    Native,
    Wasm,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WasmTransformConfig {
    /// Path to a compiled `.wasm` module implementing the transform ABI.
    #[serde(default)]
    pub module_path: Option<PathBuf>,

    /// Timeout in milliseconds for a single transform invocation.
    /// Maps to `rustcdc::WasmConfig::timeout_ms`.
    #[serde(default = "default_wasm_timeout_ms")]
    pub timeout_ms: u64,

    /// Maximum allowed linear memory size for one transform instance (bytes).
    /// Converted to megabytes when passed to `rustcdc::WasmConfig::memory_limit_mb`.
    #[serde(default = "default_wasm_max_memory_bytes")]
    pub max_memory_bytes: usize,

    /// Maximum accepted serialized event size passed to the wasm module (bytes).
    /// Enforced by cdc-server before invoking the WASM runtime.
    #[serde(default = "default_wasm_max_event_bytes")]
    pub max_event_bytes: usize,

    /// Number of reusable wasm instance pool slots.
    #[serde(default = "default_wasm_instance_pool_size")]
    pub instance_pool_size: usize,

    /// Cooperative async yield interval in fuel units (`None` disables fuel).
    /// Maps to `rustcdc::WasmConfig::fuel_async_yield_interval`.
    #[serde(default)]
    pub fuel_yield_interval: Option<u64>,
}

impl Default for WasmTransformConfig {
    fn default() -> Self {
        Self {
            module_path: None,
            timeout_ms: default_wasm_timeout_ms(),
            max_memory_bytes: default_wasm_max_memory_bytes(),
            max_event_bytes: default_wasm_max_event_bytes(),
            instance_pool_size: default_wasm_instance_pool_size(),
            fuel_yield_interval: None,
        }
    }
}

impl TransformRuntimeConfig {
    pub fn validate(&self) -> Result<(), String> {
        match self.mode {
            TransformRuntimeMode::Native => Ok(()),
            TransformRuntimeMode::Wasm => self.wasm.validate(),
        }
    }
}

impl WasmTransformConfig {
    pub fn validate(&self) -> Result<(), String> {
        let module_path = self.module_path.as_ref().ok_or_else(|| {
            "transform_runtime.wasm.module_path must be set when mode = \"wasm\"".to_string()
        })?;

        if module_path.as_os_str().is_empty() {
            return Err(
                "transform_runtime.wasm.module_path must not be empty when mode = \"wasm\""
                    .to_string(),
            );
        }

        if !module_path.is_file() {
            return Err(format!(
                "transform_runtime.wasm.module_path does not point to a file: {}",
                module_path.display()
            ));
        }

        if self.timeout_ms == 0 {
            return Err("transform_runtime.wasm.timeout_ms must be > 0".to_string());
        }

        if self.max_memory_bytes == 0 {
            return Err("transform_runtime.wasm.max_memory_bytes must be > 0".to_string());
        }

        if self.max_event_bytes == 0 {
            return Err("transform_runtime.wasm.max_event_bytes must be > 0".to_string());
        }

        if self.max_event_bytes > self.max_memory_bytes {
            return Err(
                "transform_runtime.wasm.max_event_bytes must be <= max_memory_bytes".to_string(),
            );
        }

        if self.instance_pool_size == 0 {
            return Err("transform_runtime.wasm.instance_pool_size must be > 0".to_string());
        }

        if self.instance_pool_size > 256 {
            return Err("transform_runtime.wasm.instance_pool_size must be <= 256".to_string());
        }

        Ok(())
    }
}

fn default_wasm_timeout_ms() -> u64 {
    50
}

fn default_wasm_max_memory_bytes() -> usize {
    8 * 1024 * 1024
}

fn default_wasm_max_event_bytes() -> usize {
    1024 * 1024
}

fn default_wasm_instance_pool_size() -> usize {
    1
}

// ─────────────────────────────────────────────────────────────────────────────
// Transform rules
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct TransformRuleConfig {
    /// Rule name for logs and debugging.
    pub name: String,

    /// Match predicate; empty predicate matches all events.
    #[serde(default)]
    pub when: TransformWhenConfig,

    /// Ordered actions to apply when predicate matches.
    #[serde(default)]
    pub actions: Vec<TransformActionConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct TransformWhenConfig {
    #[serde(default)]
    pub tables: Vec<String>,

    #[serde(default)]
    pub schemas: Vec<String>,

    /// Operation names: insert, update, delete, read, schema_change, truncate.
    #[serde(default)]
    pub ops: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransformActionConfig {
    /// Replace `after` with `after[field]` when that value is an object.
    Unwrap { field: String },

    /// Merge object fields from `after[field]` into `after`.
    Flatten {
        field: String,
        #[serde(default)]
        prefix: Option<String>,
    },

    /// Keep event only when include lists match (empty list = wildcard).
    Filter {
        #[serde(default)]
        include_tables: Vec<String>,
        #[serde(default)]
        include_schemas: Vec<String>,
        #[serde(default)]
        include_ops: Vec<String>,
    },

    /// Rewrite event routing target.
    Route {
        #[serde(default)]
        table: Option<String>,
        #[serde(default)]
        schema: Option<String>,
    },

    /// Project event metadata into `after[target_field]` object.
    MetadataProjection {
        #[serde(default = "default_metadata_target_field")]
        target_field: String,
        #[serde(default)]
        fields: Vec<TransformMetadataField>,
    },

    /// Materialize deterministic key material into `after[target_field]`.
    KeyShaping {
        #[serde(default = "default_key_target_field")]
        target_field: String,
        #[serde(default)]
        source: TransformKeySource,
    },
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransformMetadataField {
    SourceName,
    Offset,
    SourceTimestamp,
    EventTimestamp,
    Schema,
    Table,
    Operation,
    PrimaryKey,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TransformKeySource {
    #[default]
    PrimaryKey,
    Fingerprint,
}

impl TransformRuleConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.actions.is_empty() {
            return Err(format!(
                "transform rule '{}' must include at least one action",
                self.name
            ));
        }

        for action in &self.actions {
            match action {
                TransformActionConfig::Unwrap { field }
                | TransformActionConfig::Flatten { field, .. } => {
                    if field.trim().is_empty() {
                        return Err(format!(
                            "transform rule '{}' has empty field name",
                            self.name
                        ));
                    }
                }
                TransformActionConfig::Route { table, schema } => {
                    if table
                        .as_deref()
                        .map(str::trim)
                        .is_some_and(|value| value.is_empty())
                    {
                        return Err(format!(
                            "transform rule '{}' route.table must not be empty",
                            self.name
                        ));
                    }
                    if schema
                        .as_deref()
                        .map(str::trim)
                        .is_some_and(|value| value.is_empty())
                    {
                        return Err(format!(
                            "transform rule '{}' route.schema must not be empty",
                            self.name
                        ));
                    }
                }
                TransformActionConfig::MetadataProjection { target_field, .. }
                | TransformActionConfig::KeyShaping { target_field, .. } => {
                    if target_field.trim().is_empty() {
                        return Err(format!(
                            "transform rule '{}' has empty target_field",
                            self.name
                        ));
                    }
                }
                TransformActionConfig::Filter { .. } => {}
            }
        }

        Ok(())
    }
}

fn default_metadata_target_field() -> String {
    "_meta".to_string()
}

fn default_key_target_field() -> String {
    "_key".to_string()
}
