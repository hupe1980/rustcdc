use rustcdc::outbox::OutboxTransform;
use rustcdc::transform::UnmatchedRule;
use rustcdc::wasm::{TransformResult, WasmConfig as RustcdcWasmConfig, WasmRuntime};
use rustcdc::{
    BeforeImage, Error, Event, FieldMappingConfig, FieldMappingTransform, MaskHashConfig,
    MaskHashTransform, MaskRule, Operation, Result, fingerprint_event_stable,
};
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::pipeline::MaskRuleConfig;
use crate::config::schema::{
    TransformActionConfig, TransformKeySource, TransformMetadataField, TransformRuleConfig,
    TransformRuntimeConfig, TransformRuntimeMode, TransformWhenConfig,
};

pub struct TransformPipeline {
    rules: Vec<CompiledRule>,
    runtime: TransformRuntime,
}

/// A transform rule with its actions built once, at startup.
///
/// The rustcdc stages are stateful — `MaskHashTransform` counts per-rule hits so an
/// operator can find a rule that never fires — and building one per event would both
/// allocate on the hot path and reset those counters every time.
struct CompiledRule {
    name: String,
    when: TransformWhenConfig,
    actions: Vec<CompiledAction>,
}

enum CompiledAction {
    /// An action implemented directly against the event payload.
    Inline(TransformActionConfig),
    /// A prebuilt rustcdc stage (masking, field mapping, outbox).
    ///
    /// Kept rather than rebuilt per event: these are stateful — `MaskHashTransform`
    /// counts per-rule hits so an operator can find a rule that never fires — and
    /// rebuilding one per event would both allocate on the hot path and reset those
    /// counters every time.
    Native {
        transform: Box<dyn rustcdc::transform::Transform>,
        /// Report this stage's never-matched rules — `unmatched_rules()` is on the
        /// `Transform` trait, so every stage answers it uniformly.
        warn_on_unmatched: bool,
    },
}

fn compile_rules(rules: Vec<TransformRuleConfig>) -> Result<Vec<CompiledRule>> {
    rules
        .into_iter()
        .map(|rule| {
            let actions = rule
                .actions
                .into_iter()
                .map(|action| compile_action(&rule.name, action))
                .collect::<Result<Vec<_>>>()?;
            Ok(CompiledRule {
                name: rule.name,
                when: rule.when,
                actions,
            })
        })
        .collect()
}

fn compile_action(rule_name: &str, action: TransformActionConfig) -> Result<CompiledAction> {
    match action {
        TransformActionConfig::Mask {
            rules,
            default_rule,
            warn_on_unmatched,
        } => {
            let mut config = MaskHashConfig {
                default_rule: mask_rule(rule_name, "default_rule", default_rule)?,
                ..MaskHashConfig::default()
            };
            for (path, rule) in rules {
                let compiled = mask_rule(rule_name, &path, rule)?;
                config.mask_rules.insert(path, compiled);
            }
            // Fallible: `Truncate(0)`, `Redact("")` and an empty rule path are rejected
            // here. All three make the masking *invisible* rather
            // than merely useless — an empty string is indistinguishable downstream from
            // a genuinely empty column.
            let transform = MaskHashTransform::new(config).map_err(|e| {
                Error::ConfigError(format!(
                    "transform rule '{rule_name}' mask action is invalid: {e}"
                ))
            })?;
            Ok(CompiledAction::Native {
                transform: Box::new(transform),
                warn_on_unmatched,
            })
        }
        TransformActionConfig::FieldMapping {
            copy,
            rename,
            set,
            remove,
            strict,
        } => {
            let config = FieldMappingConfig {
                copy: copy.into_iter().map(|[a, b]| (a, b)).collect(),
                rename: rename.into_iter().map(|[a, b]| (a, b)).collect(),
                set_literals: set.into_iter().collect(),
                remove,
                strict,
            };
            let transform = FieldMappingTransform::new(config).map_err(|e| {
                Error::ConfigError(format!(
                    "transform rule '{rule_name}' field_mapping action is invalid: {e}"
                ))
            })?;
            Ok(CompiledAction::Native {
                transform: Box::new(transform),
                warn_on_unmatched: true,
            })
        }
        TransformActionConfig::Outbox { table } => Ok(CompiledAction::Native {
            transform: Box::new(OutboxTransform::new(table)),
            warn_on_unmatched: true,
        }),
        other => Ok(CompiledAction::Inline(other)),
    }
}

fn mask_rule(rule_name: &str, path: &str, rule: MaskRuleConfig) -> Result<MaskRule> {
    // Resolve the secret at startup so an unset environment variable fails here rather
    // than on the first event that happens to carry the field — a masking rule that
    // fails late has already let unmasked events through.
    let check_key = |secret: &rustcdc::SecretString| -> Result<()> {
        secret.expose_secret().map(|_| ()).map_err(|e| {
            Error::ConfigError(format!(
                "transform rule '{rule_name}' mask path '{path}': key could not be \
                 resolved: {e}"
            ))
        })
    };
    Ok(match rule {
        MaskRuleConfig::Passthrough => MaskRule::Passthrough,
        MaskRuleConfig::UnsaltedSha256 => MaskRule::UnsaltedSha256,
        MaskRuleConfig::Redact { placeholder } => MaskRule::Redact(placeholder),
        MaskRuleConfig::Null => MaskRule::Null,
        MaskRuleConfig::Truncate { keep } => MaskRule::Truncate(keep),
        MaskRuleConfig::HmacSha256 { key } => {
            check_key(&key)?;
            MaskRule::HmacSha256(key)
        }
        MaskRuleConfig::Encrypt { key } => {
            check_key(&key)?;
            MaskRule::Encrypt(key)
        }
        MaskRuleConfig::Decrypt { key } => {
            check_key(&key)?;
            MaskRule::Decrypt(key)
        }
    })
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
    /// A pool of independently-lockable rustcdc `WasmRuntime`s (each auto-inits on
    /// first transform).
    ///
    /// This is a pool of *runtimes* rather than the single runtime it used to be,
    /// because `WasmRuntime::transform` takes `&mut self`. That signature forces an
    /// exclusive lock around every transform, which serialised the whole stage — so
    /// both `wasm.instance_pool_size` and `runtime.prepare_parallelism` did nothing in
    /// WASM mode. `instance_pool_size` allocated N wasmtime instances of which exactly
    /// one could ever run.
    ///
    /// Upstream is built for concurrency one layer down: `WasmModule::transform_bytes`
    /// takes `&self` and dispatches across a semaphore-guarded instance pool. Only the
    /// public wrapper's `&mut self` — needed for a lazy `initialized` flag, since its
    /// counters are already atomics — makes that unreachable. Holding N runtimes of one
    /// instance each restores the intended concurrency with the same total instance
    /// count; the cost is compiling the module once per runtime at startup.
    ///
    /// The real fix is upstream: `&self` on `transform` would make the existing pool
    /// work for every embedder and let this collapse back to a single runtime.
    Wasm(Arc<WasmPool>),
}

/// Round-robin over independently-lockable WASM runtimes.
pub(crate) struct WasmPool {
    runtimes: Vec<Mutex<WasmRuntime>>,
    /// Dispatch cursor. `Relaxed` is right: this only has to spread load, and a missed
    /// increment costs one extra contended lock, not correctness.
    next: std::sync::atomic::AtomicUsize,
    /// Slots currently held.
    in_use: std::sync::atomic::AtomicUsize,
    /// The most slots ever held at once.
    ///
    /// This is the only number that answers "is `instance_pool_size` doing anything?".
    /// `instance_pool_size` reports what was *configured*; a pool of eight that never has
    /// more than one slot in use looks identical to a pool of one from the outside, and
    /// costs eight compiled modules' worth of memory to do it.
    peak_in_use: std::sync::atomic::AtomicUsize,
}

/// A pool slot, checked out.
///
/// Exists to decrement `in_use` on drop; a bare `MutexGuard` cannot, and doing it at each
/// call site would be forgotten on the first early return.
pub(crate) struct PooledRuntime<'a> {
    guard: tokio::sync::MutexGuard<'a, WasmRuntime>,
    in_use: &'a std::sync::atomic::AtomicUsize,
}

impl std::ops::Deref for PooledRuntime<'_> {
    type Target = WasmRuntime;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl std::ops::DerefMut for PooledRuntime<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for PooledRuntime<'_> {
    fn drop(&mut self) {
        self.in_use
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl WasmPool {
    /// Take the next runtime in rotation, waiting if it is busy.
    ///
    /// Deliberately not "scan for a free one": under saturation every slot is busy and
    /// the scan degrades into a spin, while round-robin queues fairly behind the slot
    /// whose turn it is. Under light load the cursor almost always lands on an idle
    /// slot anyway.
    async fn acquire(&self) -> PooledRuntime<'_> {
        use std::sync::atomic::Ordering;
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.runtimes.len();
        let guard = self.runtimes[index].lock().await;
        let held = self.in_use.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_in_use.fetch_max(held, Ordering::Relaxed);
        PooledRuntime {
            guard,
            in_use: &self.in_use,
        }
    }

    /// The most slots ever held at once. See [`WasmPool::peak_in_use`].
    fn peak_in_use(&self) -> usize {
        self.peak_in_use.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// A runtime for read-only metric collection.
    async fn any(&self) -> tokio::sync::MutexGuard<'_, WasmRuntime> {
        self.runtimes[0].lock().await
    }

    fn len(&self) -> usize {
        self.runtimes.len()
    }
}

/// Snapshot of WASM runtime metrics sourced from `WasmRuntime::metrics()`.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct WasmRuntimeMetricsSnapshot {
    pub(crate) instance_pool_size: u64,
    /// High-water mark of simultaneously-busy pool slots.
    pub(crate) instance_pool_peak_in_use: u64,
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
                // One runtime per requested pool slot, each holding a single wasmtime
                // instance, so the total instance count matches what the operator asked
                // for and the slots are genuinely schedulable in parallel.
                let pool_size = cfg.instance_pool_size.max(1);
                let mut runtimes = Vec::with_capacity(pool_size);
                for _ in 0..pool_size {
                    let mut slot_config = wasm_config.clone();
                    slot_config.instance_pool_size = 1;
                    runtimes.push(Mutex::new(WasmRuntime::new_with_config(slot_config)?));
                }

                TransformRuntime::Wasm(Arc::new(WasmPool {
                    runtimes,
                    next: std::sync::atomic::AtomicUsize::new(0),
                    in_use: std::sync::atomic::AtomicUsize::new(0),
                    peak_in_use: std::sync::atomic::AtomicUsize::new(0),
                }))
            }
        };

        Ok(Self {
            rules: compile_rules(rules)?,
            runtime,
        })
    }

    /// Every configured rule that has never matched, across every stage.
    ///
    /// Transform rules match by pattern against a permissive default, so a typo or a
    /// renamed column disables one **silently** and nothing errors. The consequence
    /// differs per stage and is carried on each entry: a mask rule that never fires
    /// means a column is shipping in clear text; a route rule that never fires means
    /// events are going to the default destination.
    ///
    /// Exported as `rustcdc_transform_rules_unmatched` — emitted only for rules that
    /// are unmatched, so the metric's absence is the healthy state and `> 0` is a
    /// complete alert rule.
    pub fn unmatched_rules(&self) -> Vec<UnmatchedRule> {
        self.rules
            .iter()
            .flat_map(|rule| {
                rule.actions
                    .iter()
                    .filter_map(|action| match action {
                        CompiledAction::Native {
                            transform,
                            warn_on_unmatched: true,
                        } => Some(transform.unmatched_rules()),
                        _ => None,
                    })
                    .flatten()
                    .map(|mut unmatched| {
                        // rustcdc names the *stage* (`mask_hash`); the operator wrote
                        // `name = "redact_pii"` in the config and that is what they will
                        // grep for. Qualify with both so the alert label points at the
                        // rule they can actually go and fix.
                        unmatched.transform = format!("{}/{}", rule.name, unmatched.transform);
                        unmatched
                    })
            })
            .collect()
    }

    /// Log a WARN naming every never-matched rule. Returns how many were reported.
    ///
    /// Call at shutdown, where the counters cover the whole run. Prefer the
    /// `rustcdc_transform_rules_unmatched` metric for alerting — a log line at
    /// shutdown is something an operator has to go looking for.
    pub fn report_unmatched_rules(&self) -> usize {
        let unmatched = self.unmatched_rules();
        if unmatched.is_empty() {
            return 0;
        }
        for rule in &unmatched {
            tracing::warn!(
                transform = %rule.transform,
                kind = %rule.kind,
                rule = %rule.rule,
                consequence = %rule.consequence,
                "transform rule never matched"
            );
        }
        unmatched.len()
    }

    /// Returns a live metrics snapshot from the WASM runtime, or a zeroed
    /// snapshot when the pipeline uses native mode.
    /// High-water mark of simultaneously-busy WASM pool slots, or `None` outside WASM mode.
    ///
    /// Public because `tests/wasm_transform.rs` asserts the pool's concurrency with it.
    /// That test used to compare wall-clock time for N concurrent transforms against a
    /// per-event floor, which measures the right property unreliably: under a full
    /// `cargo test` the worker threads contend with every other test binary and the
    /// comparison fails against a pool that is working. A high-water mark is the same
    /// property observed directly, and does not care how busy the machine is.
    ///
    /// It is also the number an operator needs. `instance_pool_size` reports what was
    /// configured; a pool of eight that never exceeds one slot in use is
    /// indistinguishable from a pool of one except for the memory it wastes.
    pub async fn wasm_pool_peak_in_use(&self) -> Option<u64> {
        match &self.runtime {
            TransformRuntime::Wasm(pool) => Some(pool.peak_in_use() as u64),
            TransformRuntime::Native => None,
        }
    }

    pub(crate) async fn wasm_metrics(&self) -> WasmRuntimeMetricsSnapshot {
        match &self.runtime {
            TransformRuntime::Native => WasmRuntimeMetricsSnapshot::default(),
            TransformRuntime::Wasm(pool) => {
                let guard = pool.any().await;
                let m = guard.metrics();
                WasmRuntimeMetricsSnapshot {
                    // The runtime reports its own (now always 1) pool size; the number
                    // the operator configured and that actually bounds concurrency is
                    // the number of runtimes.
                    instance_pool_size: pool.len() as u64,
                    instance_pool_peak_in_use: pool.peak_in_use() as u64,
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
            TransformRuntime::Wasm(pool) => {
                // WasmRuntime::transform() serializes the event exactly once
                // and enforces memory_limit_mb (which we set to min(max_memory_bytes,
                // max_event_bytes) at construction time).  No pre-serialization
                // is needed here — doing so would allocate and serialize twice.
                let mut guard = pool.acquire().await;
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
/// `unavailable_columns` — on the event for the after-image, and inside
/// `BeforeImage::Full` for the pre-image — is the source's claim that a column's value
/// could not be supplied (PostgreSQL unchanged-TOAST). A transform that inserts or
/// renames a column into the payload supersedes that claim — the payload now carries a
/// value, and `Event::validate()` (rustcdc ≥ 0.7.0) rejects the present-*and*-listed
/// contradiction because the dangerous reading (trust the payload) is the one a sink
/// takes.
///
/// Only `BeforeImage::Full` carries such a list: a key-only image omits its non-key
/// columns by design rather than by TOAST, so there is nothing there to reconcile.
fn reconcile_availability_lists(event: &mut Event) {
    if !event.unavailable_columns.is_empty()
        && let Some(Value::Object(after)) = event.after.as_ref()
    {
        event
            .unavailable_columns
            .retain(|column| !after.contains_key(column));
    }
    if let BeforeImage::Full {
        row: Value::Object(before),
        unavailable_columns,
    } = &mut event.before
        && !unavailable_columns.is_empty()
    {
        unavailable_columns.retain(|column| !before.contains_key(column));
    }
}

fn apply_rules(event: Event, rules: &[CompiledRule]) -> Result<Option<Event>> {
    let mut current = event;

    for rule in rules {
        if !matches_when(&current, &rule.when) {
            continue;
        }

        for action in &rule.actions {
            match action {
                CompiledAction::Inline(action) => {
                    let Some(next) = apply_action(current, action)? else {
                        return Ok(None);
                    };
                    current = next;
                }
                CompiledAction::Native { transform, .. } => {
                    if !transform.apply(&mut current)? {
                        return Ok(None);
                    }
                }
            }
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
        // Built once by `compile_action` and dispatched through `CompiledAction`, so
        // they never reach this per-event path.
        TransformActionConfig::Mask { .. }
        | TransformActionConfig::FieldMapping { .. }
        | TransformActionConfig::Outbox { .. } => Err(Error::ConfigError(format!(
            "internal: {} is a prebuilt stage and must not be dispatched per event",
            action_label(action)
        ))),
    }
}

fn action_label(action: &TransformActionConfig) -> &'static str {
    match action {
        TransformActionConfig::Unwrap { .. } => "unwrap",
        TransformActionConfig::Flatten { .. } => "flatten",
        TransformActionConfig::Filter { .. } => "filter",
        TransformActionConfig::Route { .. } => "route",
        TransformActionConfig::MetadataProjection { .. } => "metadata_projection",
        TransformActionConfig::KeyShaping { .. } => "key_shaping",
        TransformActionConfig::Mask { .. } => "mask",
        TransformActionConfig::FieldMapping { .. } => "field_mapping",
        TransformActionConfig::Outbox { .. } => "outbox",
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
    use rustcdc::core::SourceMetadata;
    use serde_json::json;
    use tempfile::TempDir;

    /// Compile config rules and run one event through them.
    fn apply_rules(event: Event, rules: &[TransformRuleConfig]) -> Result<Option<Event>> {
        let compiled = compile_rules(rules.to_vec())?;
        super::apply_rules(event, &compiled)
    }

    fn sample_event() -> Event {
        Event::builder("users", Operation::Insert)
            .after(json!({
                "id": 42,
                "customer": {
                    "name": "alice",
                    "tier": "gold"
                },
                "region": "eu-west-1"
            }))
            .source(SourceMetadata::new("postgres", "0/16B6A71", 10))
            .ts(11)
            .schema("public")
            .primary_key(["id"])
            .build()
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

    // ─── mask / field_mapping / outbox ───────────────────────────────────────

    fn mask_rule_config(rules: Vec<(&str, MaskRuleConfig)>) -> TransformRuleConfig {
        TransformRuleConfig {
            name: "mask".to_string(),
            when: TransformWhenConfig::default(),
            actions: vec![TransformActionConfig::Mask {
                rules: rules
                    .into_iter()
                    .map(|(path, rule)| (path.to_string(), rule))
                    .collect(),
                default_rule: MaskRuleConfig::Passthrough,
                warn_on_unmatched: true,
            }],
        }
    }

    #[test]
    fn mask_redacts_and_truncates_by_path() {
        let rules = vec![mask_rule_config(vec![
            (
                "region",
                MaskRuleConfig::Redact {
                    placeholder: "***".to_string(),
                },
            ),
            ("customer.name", MaskRuleConfig::Truncate { keep: 2 }),
        ])];

        let masked = apply_rules(sample_event(), &rules)
            .expect("apply")
            .expect("kept");
        let after = masked.after.expect("after");

        assert_eq!(after.get("region"), Some(&json!("***")));
        assert_eq!(after.pointer("/customer/name"), Some(&json!("al")));
        assert_eq!(
            after.pointer("/customer/tier"),
            Some(&json!("gold")),
            "unlisted fields must pass through untouched"
        );
    }

    /// Hashing must be stable across events, or a downstream join on the masked
    /// column silently stops matching.
    #[test]
    fn unsalted_sha256_is_deterministic() {
        let rules = vec![mask_rule_config(vec![(
            "region",
            MaskRuleConfig::UnsaltedSha256,
        )])];

        let first = apply_rules(sample_event(), &rules)
            .expect("apply")
            .expect("kept");
        let second = apply_rules(sample_event(), &rules)
            .expect("apply")
            .expect("kept");

        let a = first.after.expect("after");
        let b = second.after.expect("after");
        assert_eq!(a.get("region"), b.get("region"));
        assert_ne!(a.get("region"), Some(&json!("eu-west-1")));
    }

    /// A rule whose path does not exist is the dangerous case: the column ships in
    /// clear text and nothing fails. The counter is what makes it visible.
    #[test]
    fn unmatched_mask_rules_are_reported() {
        let rules = vec![mask_rule_config(vec![
            (
                "region",
                MaskRuleConfig::Redact {
                    placeholder: "***".to_string(),
                },
            ),
            // Renamed or mistyped column.
            (
                "e_mail",
                MaskRuleConfig::Redact {
                    placeholder: "***".to_string(),
                },
            ),
        ])];

        let pipeline = TransformPipeline {
            rules: compile_rules(rules).expect("compile"),
            runtime: TransformRuntime::Native,
        };
        super::apply_rules(sample_event(), &pipeline.rules).expect("apply");

        let unmatched = pipeline.unmatched_rules();
        assert_eq!(
            unmatched.len(),
            1,
            "only the rule that never matched must be reported: {unmatched:?}"
        );
        assert_eq!(unmatched[0].rule, "e_mail");
        assert_eq!(unmatched[0].kind, "mask");
        assert!(
            unmatched[0].transform.starts_with("mask/"),
            "the operator's own rule name must survive into the report: {}",
            unmatched[0].transform
        );
        // The consequence is what makes the alert actionable — a mask rule that never
        // fired means that column shipped in clear text.
        assert!(
            !unmatched[0].consequence.is_empty(),
            "every unmatched rule must carry its consequence"
        );
        assert_eq!(pipeline.report_unmatched_rules(), 1);
    }

    #[test]
    fn field_mapping_renames_sets_and_removes() {
        let rules = vec![TransformRuleConfig {
            name: "map".to_string(),
            when: TransformWhenConfig::default(),
            actions: vec![TransformActionConfig::FieldMapping {
                copy: Vec::new(),
                rename: vec![["region".to_string(), "location".to_string()]],
                set: [("source_system".to_string(), json!("crm"))]
                    .into_iter()
                    .collect(),
                remove: vec!["customer.tier".to_string()],
                strict: true,
            }],
        }];

        let mapped = apply_rules(sample_event(), &rules)
            .expect("apply")
            .expect("kept");
        let after = mapped.after.expect("after");

        assert_eq!(after.get("location"), Some(&json!("eu-west-1")));
        assert_eq!(after.get("region"), None, "rename must move, not copy");
        assert_eq!(after.get("source_system"), Some(&json!("crm")));
        assert_eq!(after.pointer("/customer/tier"), None);
    }

    /// `strict = true` exists so a renamed-away column is an error rather than a
    /// silently absent output field.
    #[test]
    fn strict_field_mapping_rejects_a_missing_source_path() {
        let rules = vec![TransformRuleConfig {
            name: "map".to_string(),
            when: TransformWhenConfig::default(),
            actions: vec![TransformActionConfig::FieldMapping {
                copy: Vec::new(),
                rename: vec![["does_not_exist".to_string(), "x".to_string()]],
                set: Default::default(),
                remove: Vec::new(),
                strict: true,
            }],
        }];

        apply_rules(sample_event(), &rules).expect_err("strict mapping must fail");
    }

    #[test]
    fn outbox_unwraps_an_insert_into_its_domain_event() {
        let rules = vec![TransformRuleConfig {
            name: "outbox".to_string(),
            when: TransformWhenConfig::default(),
            actions: vec![TransformActionConfig::Outbox {
                table: "outbox_events".to_string(),
            }],
        }];

        let event = Event::builder("outbox_events", Operation::Insert)
            .after(json!({
                "aggregate_id": "order-42",
                "event_type": "OrderPlaced",
                "payload": {"order_id": 42, "total": 99.5}
            }))
            .source(SourceMetadata::new("postgres", "0/1", 1))
            .ts(1)
            .schema("public")
            .build();

        let routed = apply_rules(event, &rules).expect("apply").expect("kept");

        assert_eq!(routed.table, "OrderPlaced");
        assert_eq!(
            routed.after.expect("after"),
            json!({"order_id": 42, "total": 99.5})
        );
    }

    /// An `UPDATE` against the outbox table is a cleanup job marking a row processed,
    /// not a new domain event; rewriting it would fabricate a duplicate.
    #[test]
    fn outbox_passes_non_insert_operations_through() {
        let rules = vec![TransformRuleConfig {
            name: "outbox".to_string(),
            when: TransformWhenConfig::default(),
            actions: vec![TransformActionConfig::Outbox {
                table: "outbox_events".to_string(),
            }],
        }];

        let event = Event::builder("outbox_events", Operation::Update)
            .after(json!({"id": 1, "processed_at": 1700000000}))
            .source(SourceMetadata::new("postgres", "0/2", 2))
            .ts(2)
            .schema("public")
            .build();

        let passed = apply_rules(event, &rules).expect("apply").expect("kept");
        assert_eq!(passed.table, "outbox_events");
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
        event.before = BeforeImage::Unavailable;
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
