use rustcdc::SecretString;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

fn bool_true() -> bool {
    true
}

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

    /// Mask, hash or encrypt fields by dotted JSON path.
    ///
    /// ```toml
    /// [[pipeline.transforms]]
    /// name = "redact_pii"
    /// [[pipeline.transforms.actions]]
    /// type = "mask"
    ///   [pipeline.transforms.actions.rules]
    ///   email          = { type = "redact", placeholder = "***" }
    ///   ssn            = { type = "hmac_sha256", key = { env = "PII_HMAC_KEY" } }
    ///   "emails.*"     = { type = "redact" }
    ///   card_number    = { type = "truncate", keep = 4 }
    /// ```
    ///
    /// A trailing `.*` matches every child one level down — the only way to cover a
    /// variable-length array, since enumerating `emails.0`, `emails.1`, … leaks
    /// whatever the operator did not guess. A rule on an object- or array-valued field
    /// masks the whole subtree.
    Mask {
        /// Rules keyed by dotted JSON path.
        #[serde(default)]
        rules: BTreeMap<String, MaskRuleConfig>,

        /// Rule applied to fields not named in `rules` (default: `passthrough`).
        #[serde(default)]
        default_rule: MaskRuleConfig,

        /// Log a WARN naming every rule that never matched (default: `true`).
        ///
        /// Rules match by exact dotted path, so a typo or a renamed column disables
        /// one **silently** and the field keeps flowing in clear text. The warning is
        /// the difference between a masking rule that is off and one that looks on.
        #[serde(default = "bool_true")]
        warn_on_unmatched: bool,
    },

    /// Copy, rename, remove and set fields by dotted JSON path.
    FieldMapping {
        /// `[from, to]` pairs; the source value is copied and left in place.
        #[serde(default)]
        copy: Vec<[String; 2]>,
        /// `[from, to]` pairs; the source value is moved.
        #[serde(default)]
        rename: Vec<[String; 2]>,
        /// Literal values to set, keyed by destination path.
        #[serde(default)]
        set: BTreeMap<String, serde_json::Value>,
        /// Paths to delete.
        #[serde(default)]
        remove: Vec<String>,
        /// Fail the event when a source or removal path is missing (default: `false`).
        ///
        /// Off, a renamed-away column silently produces no output field. On, it is a
        /// transform error routed through `runtime.transform_error_policy`.
        #[serde(default)]
        strict: bool,
    },

    /// Unwrap the transactional-outbox pattern.
    ///
    /// An `INSERT` into `table` whose row carries `aggregate_id`, `event_type` and
    /// `payload` is rewritten into the domain event it represents: `event.table`
    /// becomes the `event_type`, `after` becomes the `payload`, and `aggregate_id`
    /// becomes the key — so each aggregate keeps its own ordering downstream.
    ///
    /// Only `INSERT` is treated as a domain event; an `UPDATE` or `DELETE` against the
    /// outbox table is a cleanup job's housekeeping and passes through untouched.
    Outbox {
        /// The table outbox rows are written to, e.g. `outbox_events`.
        table: String,
    },
}

/// One masking rule.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MaskRuleConfig {
    /// Leave the value unchanged (the default for unlisted fields).
    #[default]
    Passthrough,

    /// Deterministic SHA-256 of the value, unsalted.
    ///
    /// Obfuscation only — **not** GDPR-safe pseudonymisation. A low-cardinality field
    /// (a postcode, a birth date) falls to a rainbow table in seconds. Use
    /// `hmac_sha256` when the result has to resist that.
    UnsaltedSha256,

    /// Replace the value with a fixed placeholder string.
    Redact {
        #[serde(default = "default_redact_placeholder")]
        placeholder: String,
    },

    /// Replace the value with JSON `null`.
    ///
    /// Indistinguishable downstream from a genuine `NULL`; use `redact` when a
    /// consumer must be able to tell the two apart.
    Null,

    /// Keep the first `keep` characters of a string; leave non-strings unchanged.
    Truncate { keep: usize },

    /// Keyed, deterministic HMAC-SHA256 pseudonymisation.
    ///
    /// GDPR-safe while the key stays secret, and stable — the same input always
    /// produces the same tag, so joins and dedup on the masked field still work.
    HmacSha256 { key: SecretString },

    /// AES-256-GCM encryption to `enc:v1:<nonce>:<ciphertext>`, reversible with
    /// `decrypt` and the same key.
    ///
    /// Ciphertexts are bound to `table + JSON path` as associated data, so a value
    /// relocated to another column fails authentication instead of decrypting as
    /// authentic. **Non-deterministic** — a fresh nonce per call means the same input
    /// encrypts differently every time, so never apply it to a primary-key column or
    /// to any field a downstream deduplicates on.
    Encrypt { key: SecretString },

    /// Reverse `encrypt`.
    Decrypt { key: SecretString },
}

fn default_redact_placeholder() -> String {
    "***".to_string()
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
                TransformActionConfig::Mask { rules, .. } => {
                    if rules.is_empty() {
                        return Err(format!(
                            "transform rule '{}' mask action has no rules; an empty mask \
                             silently passes every field through in clear text",
                            self.name
                        ));
                    }
                    for (path, rule) in rules {
                        if path.trim().is_empty() {
                            return Err(format!(
                                "transform rule '{}' mask action has an empty path",
                                self.name
                            ));
                        }
                        // Both of these leave an empty string, which downstream cannot
                        // distinguish from a genuinely empty column — so the masking is
                        // *invisible* rather than merely useless. Almost always a typo
                        // for `redact` or `null`. rustcdc rejects them too; catching it
                        // here names the rule and the path.
                        match rule {
                            MaskRuleConfig::Truncate { keep: 0 } => {
                                return Err(format!(
                                    "transform rule '{}' mask path '{path}': truncate.keep = 0 \
                                     leaves an empty string, which is indistinguishable \
                                     downstream from a genuinely empty column; use \
                                     type = \"redact\" or type = \"null\" if that is the intent",
                                    self.name
                                ));
                            }
                            MaskRuleConfig::Redact { placeholder } if placeholder.is_empty() => {
                                return Err(format!(
                                    "transform rule '{}' mask path '{path}': an empty redact \
                                     placeholder is indistinguishable downstream from a \
                                     genuinely empty column; use a non-empty placeholder, or \
                                     type = \"null\" if the value should read as NULL",
                                    self.name
                                ));
                            }
                            _ => {}
                        }
                    }
                }
                TransformActionConfig::FieldMapping {
                    copy,
                    rename,
                    set,
                    remove,
                    ..
                } => {
                    if copy.is_empty() && rename.is_empty() && set.is_empty() && remove.is_empty() {
                        return Err(format!(
                            "transform rule '{}' field_mapping action does nothing; set at \
                             least one of copy / rename / set / remove",
                            self.name
                        ));
                    }
                    for [from, to] in copy.iter().chain(rename.iter()) {
                        if from.trim().is_empty() || to.trim().is_empty() {
                            return Err(format!(
                                "transform rule '{}' field_mapping pair must be \
                                 [\"<from>\", \"<to>\"] with both sides non-empty",
                                self.name
                            ));
                        }
                    }
                }
                TransformActionConfig::Outbox { table } => {
                    if table.trim().is_empty() {
                        return Err(format!(
                            "transform rule '{}' outbox.table must not be empty",
                            self.name
                        ));
                    }
                }
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
