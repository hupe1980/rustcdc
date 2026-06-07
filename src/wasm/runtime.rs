//! WASM runtime for transform loading and execution.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use async_trait::async_trait;
use wasmparser::{ExternalKind, Parser, Payload, TypeRef, ValType};
use wasmtime::{
    Caller, Config, Engine, Extern, Instance, Linker, Memory, Module, Store, TypedFunc,
};

use crate::core::{Error, Event, Result};

pub const DEFAULT_WASM_TIMEOUT_MS: u64 = 50;
pub const DEFAULT_WASM_MEMORY_LIMIT_MB: u64 = 16;
/// Default number of concurrent WASM instances in the pool.
pub const DEFAULT_WASM_INSTANCE_POOL_SIZE: usize = 1;

// ─── Metrics snapshot ────────────────────────────────────────────────────────

/// Point-in-time snapshot of observable WASM runtime counters.
///
/// Obtain via [`WasmRuntime::metrics`]. All counters are monotonically
/// increasing and accumulate from the moment the runtime is initialised.
///
/// Counters are updated with `Relaxed` ordering; values are internally
/// consistent within a snapshot but not synchronized with in-flight
/// transforms that are concurrently executing.
#[derive(Debug, Clone, Default)]
pub struct WasmRuntimeMetrics {
    /// Number of WASM instances in the pool (static after construction).
    pub instance_pool_size: usize,
    /// Total number of transform invocations attempted since runtime init.
    pub transform_total: u64,
    /// Total number of transform invocations that returned an error.
    pub transform_error_total: u64,
    /// Total number of events filtered (dropped) by the WASM module.
    pub filtered_total: u64,
    /// Total number of transforms that exceeded the configured timeout.
    pub timeout_total: u64,
}

/// Shared counter set updated by every pool slot on the hot path.
///
/// Kept behind an `Arc` so `WasmRuntime` can hold a reference for `metrics()`
/// without the counters living inside the `dyn WasmModule` trait object.
#[derive(Debug, Default)]
struct WasmCounters {
    transform_total: AtomicU64,
    transform_error_total: AtomicU64,
    filtered_total: AtomicU64,
    timeout_total: AtomicU64,
}

impl WasmCounters {
    fn snapshot(&self, pool_size: usize) -> WasmRuntimeMetrics {
        WasmRuntimeMetrics {
            instance_pool_size: pool_size,
            transform_total: self.transform_total.load(Ordering::Relaxed),
            transform_error_total: self.transform_error_total.load(Ordering::Relaxed),
            filtered_total: self.filtered_total.load(Ordering::Relaxed),
            timeout_total: self.timeout_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WasmConfig {
    pub module_path: PathBuf,
    pub timeout_ms: u64,
    pub memory_limit_mb: u64,
    /// Number of concurrent WASM instances.
    ///
    /// When greater than 1, a pool of independent `wasmtime::Store` instances is
    /// maintained. Concurrent transform calls are distributed across pool slots
    /// via a semaphore. Each slot runs single-threaded; transforms within one slot
    /// are sequential while different slots run concurrently.
    ///
    /// Default: `1` (equivalent to the original single-instance behaviour).
    pub instance_pool_size: usize,
    /// Cooperative async yield interval in fuel units.
    ///
    /// When `Some(n)`, wasmtime's fuel mechanism is enabled and the Tokio
    /// executor receives a yield point approximately every `n` WASM instructions
    /// per `call_async` call. Prevents CPU-bound WASM from monopolising a thread
    /// between epoch ticks.
    ///
    /// `None` (default) disables fuel; epoch-based hard interruption still applies.
    ///
    /// Recommended starting value: `10_000` (≈ 10 µs @ 1 GHz instruction rate).
    pub fuel_async_yield_interval: Option<u64>,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            module_path: PathBuf::new(),
            timeout_ms: DEFAULT_WASM_TIMEOUT_MS,
            memory_limit_mb: DEFAULT_WASM_MEMORY_LIMIT_MB,
            instance_pool_size: DEFAULT_WASM_INSTANCE_POOL_SIZE,
            fuel_async_yield_interval: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransformResult {
    /// The WASM module transformed the event successfully.
    Ok(Box<Event>),
    /// The WASM module explicitly filtered the event (returned `null`/`None`).
    ///
    /// This is a normal, expected outcome — not an error. The event should be silently dropped.
    Filtered,
}

#[async_trait]
pub trait WasmModule: Send + Sync {
    /// Execute the WASM transform on a pre-serialised JSON event payload.
    ///
    /// The caller is responsible for serialising the event and validating the
    /// payload size against the configured memory limit *before* calling this
    /// method. `WasmRuntime::transform` does both steps automatically; use that
    /// method unless you have a specific reason to call the module directly.
    ///
    /// Returns the transformed event, or `None` when the module filtered
    /// (dropped) the event.
    async fn transform_bytes(&self, event_json: &[u8]) -> Result<Option<Event>>;
    fn timeout_ms(&self) -> u64;

    async fn init(&self, _config: &WasmConfig) -> Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

pub struct WasmRuntime {
    config: WasmConfig,
    module_bytes: Vec<u8>,
    module: Arc<dyn WasmModule>,
    initialized: bool,
    /// Shared counter set; also referenced by the `RealWasmModule` via `Arc`.
    counters: Arc<WasmCounters>,
}

struct RealWasmModule {
    timeout_ms: u64,
    /// When `Some(n)`, fuel consumption is enabled and `call_async` yields to
    /// the Tokio executor every `n` WASM instructions.
    fuel_async_yield_interval: Option<u64>,
    /// Kept alive so the epoch-ticker task can call `engine.increment_epoch()`.
    _engine: Arc<Engine>,
    /// Instance pool. All slots are compiled from the same `Module`.
    /// The semaphore count equals pool size, guaranteeing a free slot is
    /// always available once a permit is acquired.
    pool: Vec<tokio::sync::Mutex<RealWasmState>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    /// Sentinel kept alive for the lifetime of this module.
    ///
    /// The background epoch-ticker task holds a `Weak` to this sentinel.
    /// When `RealWasmModule` is dropped the strong count reaches zero, the
    /// `Weak::upgrade()` in the ticker loop returns `None`, and the task
    /// exits naturally within one tick interval (≤ 1 ms).
    _ticker_sentinel: Arc<()>,
    /// Guards against spawning the ticker task more than once across repeated
    /// `init()` calls (e.g. after `shutdown()` + re-`init()`).
    ticker_started: AtomicBool,
}

struct RealWasmState {
    store: Store<()>,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    /// Release a previously-allocated memory region.
    dealloc: TypedFunc<(i32, i32), ()>,
    /// Packed `(out_ptr << 32) | out_len`; returns 0 to drop the event.
    transform: TypedFunc<(i32, i32), i64>,
    init: Option<TypedFunc<(i32, i32), i32>>,
    shutdown: Option<TypedFunc<(), i32>>,
}

impl WasmRuntime {
    pub fn new(wasm_module_path: &str) -> Result<Self> {
        let config = WasmConfig {
            module_path: PathBuf::from(wasm_module_path),
            ..WasmConfig::default()
        };
        Self::new_with_config(config)
    }

    pub fn new_with_config(config: WasmConfig) -> Result<Self> {
        validate_wasm_config(&config)?;
        let module_bytes = std::fs::read(&config.module_path)?;
        validate_wasm_contract(&module_bytes)?;
        let counters = Arc::new(WasmCounters::default());
        let module = RealWasmModule::new(&module_bytes, &config, Arc::clone(&counters))?;

        Ok(Self {
            config,
            module_bytes,
            module: Arc::new(module),
            initialized: false,
            counters,
        })
    }

    pub fn with_module(mut self, module: Arc<dyn WasmModule>) -> Self {
        // Reset counters so that test modules start from zero.
        self.counters = Arc::new(WasmCounters::default());
        self.module = module;
        self
    }

    pub async fn init(&mut self) -> Result<()> {
        self.module.init(&self.config).await?;
        self.initialized = true;
        Ok(())
    }

    pub async fn transform(&mut self, event: &Event) -> Result<TransformResult> {
        if !self.initialized {
            self.init().await?;
        }

        // Serialise the event once and validate its size before handing bytes
        // to the WASM module.  This avoids a second serialisation inside the
        // module's hot path.
        let event_json = serde_json::to_vec(event)?;
        let limit_bytes = self.config.memory_limit_mb.saturating_mul(1024 * 1024);
        if (event_json.len() as u64) > limit_bytes {
            return Err(Error::TransformError(format!(
                "event payload exceeds configured WASM memory limit ({} bytes > {} bytes)",
                event_json.len(),
                limit_bytes
            )));
        }

        let effective_timeout_ms = self.module.timeout_ms().min(self.config.timeout_ms).max(1);
        self.counters
            .transform_total
            .fetch_add(1, Ordering::Relaxed);
        let result = tokio::time::timeout(
            Duration::from_millis(effective_timeout_ms),
            self.module.transform_bytes(&event_json),
        )
        .await;

        match result {
            Err(_) => {
                self.counters.timeout_total.fetch_add(1, Ordering::Relaxed);
                self.counters
                    .transform_error_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(Error::TimeoutError(format!(
                    "WASM transform exceeded timeout ({} ms)",
                    effective_timeout_ms
                )))
            }
            Ok(Err(e)) => {
                self.counters
                    .transform_error_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
            Ok(Ok(Some(transformed))) => Ok(TransformResult::Ok(Box::new(transformed))),
            Ok(Ok(None)) => {
                self.counters.filtered_total.fetch_add(1, Ordering::Relaxed);
                Ok(TransformResult::Filtered)
            }
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.module.shutdown().await?;
        self.initialized = false;
        Ok(())
    }

    pub fn config(&self) -> &WasmConfig {
        &self.config
    }

    pub fn module_size_bytes(&self) -> usize {
        self.module_bytes.len()
    }

    /// Return a point-in-time snapshot of observable WASM runtime counters.
    ///
    /// Counters accumulate from runtime initialisation and are never reset.
    /// Reads use `Relaxed` ordering; values are accurate to within one
    /// in-flight transform invocation.
    pub fn metrics(&self) -> WasmRuntimeMetrics {
        self.counters
            .snapshot(self.config.instance_pool_size.max(1))
    }
}

impl RealWasmModule {
    fn new(module_bytes: &[u8], config: &WasmConfig, _counters: Arc<WasmCounters>) -> Result<Self> {
        // Enable epoch interruption so that a runaway (infinite-loop) WASM
        // module cannot block the Tokio worker thread indefinitely.
        // The background ticker thread increments the engine epoch every 1 ms;
        // we set a per-call deadline of `timeout_ms` ticks so that WASM execution
        // is forcibly interrupted after approximately `timeout_ms` milliseconds,
        // regardless of what the WASM code is doing (including tight spin loops).
        let mut engine_config = Config::new();
        engine_config.epoch_interruption(true);
        // Enable fuel tracking only when the caller opts in to cooperative yielding.
        // Fuel adds a per-instruction counter overhead unnecessary when epoch alone suffices.
        let fuel_yield: Option<u64> = config.fuel_async_yield_interval.filter(|&n| n > 0);
        if fuel_yield.is_some() {
            engine_config.consume_fuel(true);
        }
        let engine = Arc::new(Engine::new(&engine_config).map_err(|error| {
            Error::ConfigError(format!("failed to create WASM engine: {error}"))
        })?);

        let module = Module::new(&engine, module_bytes).map_err(|error| {
            Error::ConfigError(format!("failed to compile WASM module: {error}"))
        })?;

        let mut linker = Linker::new(&engine);
        linker
            .func_wrap(
                "env",
                "log",
                |_caller: Caller<'_, ()>, _level: i32, _ptr: i32, _len: i32| {},
            )
            .map_err(|error| Error::ConfigError(format!("failed to bind env.log: {error}")))?;
        linker
            .func_wrap(
                "env",
                "get_metric",
                |_caller: Caller<'_, ()>, _ptr: i32| -> i64 { 0 },
            )
            .map_err(|error| {
                Error::ConfigError(format!("failed to bind env.get_metric: {error}"))
            })?;
        linker
            .func_wrap(
                "env",
                "record_metric",
                |_caller: Caller<'_, ()>, _ptr: i32, _value: i64| {},
            )
            .map_err(|error| {
                Error::ConfigError(format!("failed to bind env.record_metric: {error}"))
            })?;

        // Call rustcdc_abi_version() before the epoch ticker starts so that
        // incompatible modules are rejected at load time, not at first invocation.
        {
            let mut probe_store = Store::new(&engine, ());
            let probe_instance =
                linker
                    .instantiate(&mut probe_store, &module)
                    .map_err(|error| {
                        Error::ConfigError(format!(
                            "failed to instantiate WASM module for ABI probe: {error}"
                        ))
                    })?;
            let version_fn = get_typed_func::<(), i32>(
                &mut probe_store,
                &probe_instance,
                "rustcdc_abi_version",
            )?;
            probe_store.set_epoch_deadline(config.timeout_ms);
            let reported = version_fn.call(&mut probe_store, ()).map_err(|e| {
                Error::ConfigError(format!("rustcdc_abi_version() call failed: {e}"))
            })?;
            const CURRENT_ABI_VERSION: i32 = 2;
            if reported != CURRENT_ABI_VERSION {
                return Err(Error::ConfigError(format!(
                    "WASM module reports ABI version {reported} but host requires \
                     {CURRENT_ABI_VERSION}. Rebuild the module against the rustcdc \
                     WASM ABI documentation."
                )));
            }
        }

        let pool_size = config.instance_pool_size.max(1);
        let mut pool = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let state = Self::create_instance_state(&engine, &module, &linker, fuel_yield)?;
            pool.push(tokio::sync::Mutex::new(state));
        }

        // Sentinel for the epoch-ticker task lifecycle.  The task is spawned
        // lazily in `init()` so that construction remains synchronous and does
        // not require a running Tokio runtime.
        let ticker_sentinel = Arc::new(());

        let module = Self {
            timeout_ms: config.timeout_ms,
            fuel_async_yield_interval: fuel_yield,
            _engine: engine,
            semaphore: Arc::new(tokio::sync::Semaphore::new(pool_size)),
            pool,
            _ticker_sentinel: ticker_sentinel,
            ticker_started: AtomicBool::new(false),
        };

        module.validate_memory_limit(config.memory_limit_mb)?;
        Ok(module)
    }

    /// Create one `RealWasmState` (a single `Store` + `Instance`) from the shared engine.
    fn create_instance_state(
        engine: &Engine,
        module: &Module,
        linker: &Linker<()>,
        fuel_async_yield_interval: Option<u64>,
    ) -> Result<RealWasmState> {
        let mut store = Store::new(engine, ());
        if let Some(interval) = fuel_async_yield_interval {
            store
                .set_fuel(u64::MAX)
                .map_err(|e| Error::ConfigError(format!("failed to set WASM fuel: {e}")))?;
            store
                .fuel_async_yield_interval(Some(interval))
                .map_err(|e| {
                    Error::ConfigError(format!("failed to set fuel yield interval: {e}"))
                })?;
        }
        let instance = linker.instantiate(&mut store, module).map_err(|error| {
            Error::ConfigError(format!("failed to instantiate WASM module: {error}"))
        })?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| Error::ConfigError("WASM module missing memory export".to_string()))?;

        let alloc = get_typed_func::<i32, i32>(&mut store, &instance, "alloc")?;
        let dealloc = get_typed_func::<(i32, i32), ()>(&mut store, &instance, "dealloc")?;
        let transform = get_typed_func::<(i32, i32), i64>(&mut store, &instance, "transform")?;
        let init = optional_typed_func::<(i32, i32), i32>(&mut store, &instance, "init")?;
        let shutdown = optional_typed_func::<(), i32>(&mut store, &instance, "shutdown")?;

        Ok(RealWasmState {
            store,
            memory,
            alloc,
            dealloc,
            transform,
            init,
            shutdown,
        })
    }

    fn validate_memory_limit(&self, limit_mb: u64) -> Result<()> {
        // All pool slots share the same module memory layout; checking the first is sufficient.
        // Use try_lock (sync) — called from synchronous `new()` before any concurrent access.
        let state = self
            .pool
            .first()
            .expect("pool is non-empty")
            .try_lock()
            .map_err(|_| {
                Error::StateError("WASM runtime pool lock busy during validation".to_string())
            })?;
        let limit_bytes = limit_mb.saturating_mul(1024 * 1024);
        let current = state.memory.data_size(&state.store) as u64;
        if current > limit_bytes {
            return Err(Error::ConfigError(format!(
                "WASM memory export exceeds configured limit ({} bytes > {} bytes)",
                current, limit_bytes
            )));
        }
        Ok(())
    }

    fn alloc_and_write(state: &mut RealWasmState, payload: &[u8]) -> Result<i32> {
        let len = i32::try_from(payload.len()).map_err(|_| {
            Error::TransformError(format!(
                "WASM payload too large for i32 length: {} bytes",
                payload.len()
            ))
        })?;

        let ptr = state
            .alloc
            .call(&mut state.store, len)
            .map_err(|error| Error::TransformError(format!("WASM alloc call failed: {error}")))?;

        // Address 0 is reserved; treat as alloc failure.
        if ptr <= 0 {
            return Err(Error::TransformError(format!(
                "WASM alloc returned invalid pointer {ptr} (address 0 is reserved)"
            )));
        }

        state
            .memory
            .write(&mut state.store, ptr as usize, payload)
            .map_err(|error| Error::TransformError(format!("WASM memory write failed: {error}")))?;
        Ok(ptr)
    }

    fn read_output(state: &mut RealWasmState, ptr: i32, len: i32) -> Result<Vec<u8>> {
        // Address 0 is reserved; reject zero and negative pointers.
        if ptr <= 0 {
            return Err(Error::TransformError(format!(
                "WASM output pointer is invalid: {ptr} (address 0 is reserved)"
            )));
        }
        if len < 0 {
            return Err(Error::TransformError(format!(
                "WASM output length is negative: {len}"
            )));
        }

        let mut out = vec![0_u8; len as usize];
        state
            .memory
            .read(&state.store, ptr as usize, &mut out)
            .map_err(|error| Error::TransformError(format!("WASM memory read failed: {error}")))?;
        Ok(out)
    }
}

fn get_typed_func<Params, Results>(
    store: &mut Store<()>,
    instance: &Instance,
    name: &str,
) -> Result<TypedFunc<Params, Results>>
where
    Params: wasmtime::WasmParams,
    Results: wasmtime::WasmResults,
{
    instance
        .get_typed_func::<Params, Results>(store, name)
        .map_err(|error| Error::ConfigError(format!("WASM export '{name}' type mismatch: {error}")))
}

fn optional_typed_func<Params, Results>(
    store: &mut Store<()>,
    instance: &Instance,
    name: &str,
) -> Result<Option<TypedFunc<Params, Results>>>
where
    Params: wasmtime::WasmParams,
    Results: wasmtime::WasmResults,
{
    match instance.get_export(&mut *store, name) {
        Some(Extern::Func(_)) => Ok(Some(get_typed_func::<Params, Results>(
            store, instance, name,
        )?)),
        Some(_) => Err(Error::ConfigError(format!(
            "WASM export '{name}' exists but is not a function"
        ))),
        None => Ok(None),
    }
}

#[async_trait]
impl WasmModule for RealWasmModule {
    async fn transform_bytes(&self, event_json: &[u8]) -> Result<Option<Event>> {
        // Acquire a permit — blocks until a pool slot is available.
        let _permit =
            self.semaphore.acquire().await.map_err(|_| {
                Error::StateError("WASM instance pool semaphore closed".to_string())
            })?;

        // The semaphore guarantees exactly one free slot; try_lock() is sync.
        let mut state = None;
        for slot in &self.pool {
            if let Ok(guard) = slot.try_lock() {
                state = Some(guard);
                break;
            }
        }
        let mut state = state.expect("WASM semaphore invariant violated: no free pool slot found");

        // Arm the epoch deadline so that a runaway WASM module is interrupted
        // after approximately `timeout_ms` milliseconds by the ticker task.
        state.store.set_epoch_deadline(self.timeout_ms);
        if self.fuel_async_yield_interval.is_some() {
            state
                .store
                .set_fuel(u64::MAX)
                .map_err(|e| Error::TransformError(format!("WASM fuel refill failed: {e}")))?;
        }

        let input_len = i32::try_from(event_json.len()).map_err(|_| {
            Error::TransformError(format!(
                "WASM input length exceeds i32: {}",
                event_json.len()
            ))
        })?;

        // Clone once; used twice below (input dealloc + output dealloc).
        let dealloc = state.dealloc.clone();

        let input_ptr = Self::alloc_and_write(&mut state, event_json)?;

        let transform = state.transform.clone();
        let packed = transform
            .call_async(&mut state.store, (input_ptr, input_len))
            .await
            .map_err(|error| {
                Error::TransformError(format!("WASM transform call failed: {error}"))
            })?;

        // Always dealloc the input region, even on event-drop or error.
        let _ = dealloc.call(&mut state.store, (input_ptr, input_len));

        // Return value 0 means "drop this event".
        if packed == 0 {
            return Ok(None);
        }

        // Decode the packed return: high 32 bits = output ptr, low 32 bits = output length.
        let output_ptr = (packed >> 32) as i32;
        let output_len = (packed & 0xFFFF_FFFF) as i32;

        if output_ptr <= 0 {
            return Err(Error::TransformError(format!(
                "WASM transform returned invalid output pointer {output_ptr} \
                 (address 0 is reserved)"
            )));
        }
        if output_len <= 0 {
            return Err(Error::TransformError(format!(
                "WASM transform returned non-zero pointer with invalid length {output_len}"
            )));
        }

        let output = Self::read_output(&mut state, output_ptr, output_len)?;
        let _ = dealloc.call(&mut state.store, (output_ptr, output_len));

        let transformed = serde_json::from_slice::<Event>(&output).map_err(|error| {
            Error::TransformError(format!(
                "WASM transform output is not canonical Event JSON: {error}"
            ))
        })?;

        Ok(Some(transformed))
    }

    fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    async fn init(&self, config: &WasmConfig) -> Result<()> {
        if self
            .ticker_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let weak_sentinel: Weak<()> = Arc::downgrade(&self._ticker_sentinel);
            let ticker_engine = Arc::clone(&self._engine);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    if weak_sentinel.upgrade().is_none() {
                        break;
                    }
                    ticker_engine.increment_epoch();
                }
            });
        }

        let config_payload = serde_json::json!({
            "timeout_ms": config.timeout_ms,
            "memory_limit_mb": config.memory_limit_mb,
        });
        let config_bytes = serde_json::to_vec(&config_payload)?;
        let config_len = i32::try_from(config_bytes.len()).map_err(|_| {
            Error::ConfigError("WASM init config payload exceeds i32 length".to_string())
        })?;

        for slot in &self.pool {
            let mut state = slot.lock().await;

            let Some(init) = state.init.clone() else {
                continue;
            };

            state.store.set_epoch_deadline(self.timeout_ms);
            if self.fuel_async_yield_interval.is_some() {
                state
                    .store
                    .set_fuel(u64::MAX)
                    .map_err(|e| Error::ConfigError(format!("WASM fuel refill failed: {e}")))?;
            }
            let ptr = Self::alloc_and_write(&mut state, &config_bytes)?;

            let status = init
                .call_async(&mut state.store, (ptr, config_len))
                .await
                .map_err(|error| Error::ConfigError(format!("WASM init call failed: {error}")))?;

            let dealloc = state.dealloc.clone();
            let _ = dealloc.call(&mut state.store, (ptr, config_len));

            if status != 0 {
                return Err(Error::ConfigError(format!(
                    "WASM init returned non-zero status: {status}"
                )));
            }
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        for slot in &self.pool {
            let mut state = slot.lock().await;

            let Some(shutdown) = state.shutdown.clone() else {
                continue;
            };

            state.store.set_epoch_deadline(self.timeout_ms);
            if self.fuel_async_yield_interval.is_some() {
                state
                    .store
                    .set_fuel(u64::MAX)
                    .map_err(|e| Error::StateError(format!("WASM fuel refill failed: {e}")))?;
            }
            let status = shutdown
                .call_async(&mut state.store, ())
                .await
                .map_err(|error| {
                    Error::StateError(format!("WASM shutdown call failed: {error}"))
                })?;
            if status != 0 {
                return Err(Error::StateError(format!(
                    "WASM shutdown returned non-zero status: {status}"
                )));
            }
        }
        Ok(())
    }
}

fn validate_wasm_config(config: &WasmConfig) -> Result<()> {
    if config.timeout_ms == 0 {
        return Err(Error::ConfigError(
            "WASM timeout_ms must be greater than zero".to_string(),
        ));
    }

    if config.memory_limit_mb == 0 {
        return Err(Error::ConfigError(
            "WASM memory_limit_mb must be greater than zero".to_string(),
        ));
    }

    if config.module_path.as_os_str().is_empty() {
        return Err(Error::ConfigError(
            "WASM module path must not be empty".to_string(),
        ));
    }

    if !config.module_path.exists() {
        return Err(Error::ConfigError(format!(
            "WASM module does not exist: {}",
            config.module_path.display()
        )));
    }

    if !is_wasm_extension(&config.module_path) {
        return Err(Error::ConfigError(format!(
            "WASM module path must end with .wasm: {}",
            config.module_path.display()
        )));
    }

    Ok(())
}

fn is_wasm_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("wasm"))
}

#[derive(Clone, Debug)]
struct FunctionSig {
    params: Vec<ValType>,
    results: Vec<ValType>,
}

fn validate_wasm_contract(module_bytes: &[u8]) -> Result<()> {
    let mut type_sigs: Vec<FunctionSig> = Vec::new();
    let mut function_type_indices: Vec<u32> = Vec::new();
    let mut exported_funcs: std::collections::HashMap<String, u32> =
        std::collections::HashMap::new();
    let mut exported_memories: HashSet<String> = HashSet::new();

    for payload in Parser::new(0).parse_all(module_bytes) {
        let payload =
            payload.map_err(|error| Error::ConfigError(format!("invalid wasm module: {error}")))?;

        match payload {
            Payload::TypeSection(types) => {
                for entry in types.into_iter_err_on_gc_types() {
                    let func_ty = entry.map_err(|error| {
                        Error::ConfigError(format!("invalid wasm type section entry: {error}"))
                    })?;
                    type_sigs.push(FunctionSig {
                        params: func_ty.params().to_vec(),
                        results: func_ty.results().to_vec(),
                    });
                }
            }
            Payload::ImportSection(imports) => {
                for entry in imports.into_imports() {
                    let import = entry.map_err(|error| {
                        Error::ConfigError(format!("invalid wasm import entry: {error}"))
                    })?;

                    if import.module != "env" {
                        return Err(Error::ConfigError(format!(
                            "WASM import from unsupported module '{}.{}'",
                            import.module, import.name
                        )));
                    }

                    let allowed = matches!(import.name, "log" | "get_metric" | "record_metric");
                    if !allowed {
                        return Err(Error::ConfigError(format!(
                            "WASM static analysis rejected forbidden host import: {}.{}",
                            import.module, import.name
                        )));
                    }

                    if let TypeRef::Func(type_index) = import.ty {
                        function_type_indices.push(type_index);
                    }
                }
            }
            Payload::FunctionSection(functions) => {
                for type_index in functions {
                    function_type_indices.push(type_index.map_err(|error| {
                        Error::ConfigError(format!("invalid wasm function section entry: {error}"))
                    })?);
                }
            }
            Payload::ExportSection(exports) => {
                for entry in exports {
                    let export = entry.map_err(|error| {
                        Error::ConfigError(format!("invalid wasm export entry: {error}"))
                    })?;

                    match export.kind {
                        ExternalKind::Func => {
                            exported_funcs.insert(export.name.to_string(), export.index);
                        }
                        ExternalKind::Memory => {
                            exported_memories.insert(export.name.to_string());
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    let required_func_exports = ["alloc", "dealloc", "transform", "rustcdc_abi_version"];
    for name in required_func_exports {
        if !exported_funcs.contains_key(name) {
            return Err(Error::ConfigError(format!(
                "WASM module missing required export '{name}'. \
                 See the rustcdc WASM ABI documentation."
            )));
        }
    }

    if !exported_memories.contains("memory") {
        return Err(Error::ConfigError(
            "WASM module missing required memory export 'memory'".to_string(),
        ));
    }

    validate_export_signature(
        "alloc",
        &exported_funcs,
        &function_type_indices,
        &type_sigs,
        &[ValType::I32],
        &[ValType::I32],
    )?;
    // transform(ptr: i32, len: i32) -> i64  (packed out_ptr<<32 | out_len)
    validate_export_signature(
        "transform",
        &exported_funcs,
        &function_type_indices,
        &type_sigs,
        &[ValType::I32, ValType::I32],
        &[ValType::I64],
    )?;
    // dealloc(ptr: i32, size: i32)
    validate_export_signature(
        "dealloc",
        &exported_funcs,
        &function_type_indices,
        &type_sigs,
        &[ValType::I32, ValType::I32],
        &[],
    )?;
    // rustcdc_abi_version() -> i32
    validate_export_signature(
        "rustcdc_abi_version",
        &exported_funcs,
        &function_type_indices,
        &type_sigs,
        &[],
        &[ValType::I32],
    )?;

    if exported_funcs.contains_key("init") {
        validate_export_signature(
            "init",
            &exported_funcs,
            &function_type_indices,
            &type_sigs,
            &[ValType::I32, ValType::I32],
            &[ValType::I32],
        )?;
    }

    if exported_funcs.contains_key("shutdown") {
        validate_export_signature(
            "shutdown",
            &exported_funcs,
            &function_type_indices,
            &type_sigs,
            &[],
            &[ValType::I32],
        )?;
    }

    Ok(())
}

fn validate_export_signature(
    name: &str,
    exported_funcs: &std::collections::HashMap<String, u32>,
    function_type_indices: &[u32],
    type_sigs: &[FunctionSig],
    expected_params: &[ValType],
    expected_results: &[ValType],
) -> Result<()> {
    let function_index = exported_funcs
        .get(name)
        .copied()
        .ok_or_else(|| Error::ConfigError(format!("missing required export '{name}'")))?;

    let type_index = function_type_indices
        .get(function_index as usize)
        .copied()
        .ok_or_else(|| {
            Error::ConfigError(format!(
                "WASM export '{name}' references out-of-range function index {function_index}"
            ))
        })?;

    let sig = type_sigs.get(type_index as usize).ok_or_else(|| {
        Error::ConfigError(format!(
            "WASM export '{name}' references unknown type index {type_index}"
        ))
    })?;

    if sig.params.as_slice() != expected_params || sig.results.as_slice() != expected_results {
        return Err(Error::ConfigError(format!(
            "WASM export '{name}' has invalid signature"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use serde_json::json;

    use crate::{Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};

    use super::*;

    struct MockWasmModule {
        init_calls: AtomicUsize,
        transform_calls: AtomicUsize,
        shutdown_calls: AtomicUsize,
        transform_delay_ms: u64,
    }

    impl MockWasmModule {
        fn new(transform_delay_ms: u64) -> Self {
            Self {
                init_calls: AtomicUsize::new(0),
                transform_calls: AtomicUsize::new(0),
                shutdown_calls: AtomicUsize::new(0),
                transform_delay_ms,
            }
        }
    }

    #[async_trait]
    impl WasmModule for MockWasmModule {
        async fn transform_bytes(&self, event_json: &[u8]) -> Result<Option<Event>> {
            self.transform_calls.fetch_add(1, Ordering::Relaxed);
            if self.transform_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(self.transform_delay_ms)).await;
            }
            let event = serde_json::from_slice::<Event>(event_json)?;
            Ok(Some(event))
        }

        fn timeout_ms(&self) -> u64 {
            1_000
        }

        async fn init(&self, _config: &WasmConfig) -> Result<()> {
            self.init_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn shutdown(&self) -> Result<()> {
            self.shutdown_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn minimal_event() -> Event {
        Event {
            before: None,
            after: Some(json!({"id": 1, "name": "alice"})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "test".to_string(),
                offset: "1".to_string(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".to_string()),
            table: "users".to_string(),
            primary_key: Some(vec!["id".to_string()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
        }
    }

    fn write_module(path: &Path, content: &[u8]) {
        std::fs::write(path, content).expect("write module file");
    }

    fn write_wat_module(path: &Path, wat_src: &str) {
        let wasm = wat::parse_str(wat_src).expect("valid wat fixture");
        write_module(path, &wasm);
    }

    fn minimal_conformant_wat() -> &'static str {
        r#"(module
                        (memory (export "memory") 1)
                        (global $heap (mut i32) (i32.const 8))
                        (func (export "alloc") (param $len i32) (result i32)
                            (local $ptr i32)
                            global.get $heap
                            local.tee $ptr
                            local.get $len
                            i32.add
                            global.set $heap
                            local.get $ptr)
                        (func (export "dealloc") (param i32) (param i32))
                        (func (export "rustcdc_abi_version") (result i32) i32.const 2)
                        (func (export "transform") (param i32 i32) (result i64)
                            i64.const 0))"#
    }

    #[tokio::test]
    async fn rejects_module_missing_rustcdc_abi_version() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module_path = temp_dir.path().join("module.wasm");
        write_wat_module(
            &module_path,
            r#"(module
                (memory (export "memory") 1)
                (global $heap (mut i32) (i32.const 8))
                (func (export "alloc") (param $len i32) (result i32)
                    (local $ptr i32)
                    global.get $heap
                    local.tee $ptr
                    local.get $len
                    i32.add
                    global.set $heap
                    local.get $ptr)
                (func (export "dealloc") (param i32) (param i32))
                (func (export "transform") (param i32 i32) (result i64)
                    i64.const 0))"#,
        );
        let result = WasmRuntime::new(module_path.to_str().expect("utf8"));
        assert!(
            matches!(result, Err(Error::ConfigError(ref msg)) if msg.contains("missing required")),
            "expected module without rustcdc_abi_version to be rejected"
        );
    }

    #[tokio::test]
    async fn module_loads_and_reports_size() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module_path = temp_dir.path().join("module.wasm");
        write_wat_module(&module_path, minimal_conformant_wat());

        let runtime = WasmRuntime::new(module_path.to_str().expect("utf8")).expect("runtime");
        assert!(runtime.module_size_bytes() > 0);
    }

    #[tokio::test]
    async fn init_shutdown_and_transform_are_called() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module_path = temp_dir.path().join("module.wasm");
        write_wat_module(&module_path, minimal_conformant_wat());

        let mock = Arc::new(MockWasmModule::new(0));
        let mut runtime = WasmRuntime::new(module_path.to_str().expect("utf8"))
            .expect("runtime")
            .with_module(mock.clone());

        runtime.init().await.expect("init");
        let result = runtime
            .transform(&minimal_event())
            .await
            .expect("transform");
        assert!(matches!(result, TransformResult::Ok(_)));
        runtime.shutdown().await.expect("shutdown");

        assert_eq!(mock.init_calls.load(Ordering::Relaxed), 1);
        assert_eq!(mock.transform_calls.load(Ordering::Relaxed), 1);
        assert_eq!(mock.shutdown_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn timeout_is_enforced() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module_path = temp_dir.path().join("module.wasm");
        write_wat_module(&module_path, minimal_conformant_wat());

        let mut runtime = WasmRuntime::new_with_config(WasmConfig {
            module_path: module_path.clone(),
            timeout_ms: 10,
            memory_limit_mb: DEFAULT_WASM_MEMORY_LIMIT_MB,
            instance_pool_size: 1,
            fuel_async_yield_interval: None,
        })
        .expect("runtime")
        .with_module(Arc::new(MockWasmModule::new(50)));

        runtime.init().await.expect("init");
        let error = runtime
            .transform(&minimal_event())
            .await
            .expect_err("timeout");
        assert!(matches!(error, Error::TimeoutError(_)));
    }

    #[tokio::test]
    async fn memory_limit_is_enforced() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module_path = temp_dir.path().join("module.wasm");
        write_wat_module(&module_path, minimal_conformant_wat());

        let mut runtime = WasmRuntime::new_with_config(WasmConfig {
            module_path: module_path.clone(),
            timeout_ms: DEFAULT_WASM_TIMEOUT_MS,
            memory_limit_mb: 1,
            instance_pool_size: 1,
            fuel_async_yield_interval: None,
        })
        .expect("runtime")
        .with_module(Arc::new(MockWasmModule::new(0)));
        runtime.init().await.expect("init");

        let mut large = minimal_event();
        large.after = Some(json!({"blob": "x".repeat(2 * 1024 * 1024)}));

        let error = runtime
            .transform(&large)
            .await
            .expect_err("memory limit error");
        assert!(matches!(error, Error::TransformError(_)));
    }

    #[tokio::test]
    async fn static_analysis_rejects_file_io_imports() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module_path = temp_dir.path().join("module.wasm");
        write_wat_module(
            &module_path,
            r#"(module
                (import "env" "fd_write" (func (param i32 i32 i32 i32) (result i32)))
                (memory (export "memory") 1)
                (func (export "alloc") (param i32) (result i32) i32.const 8)
                (func (export "dealloc") (param i32) (param i32))
                (func (export "rustcdc_abi_version") (result i32) i32.const 2)
                (func (export "transform") (param i32 i32) (result i64) i64.const 0))"#,
        );

        let result = WasmRuntime::new(module_path.to_str().expect("utf8"));
        assert!(matches!(result, Err(Error::ConfigError(_))));
    }

    #[tokio::test]
    async fn rejects_module_missing_required_exports() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module_path = temp_dir.path().join("module.wasm");
        // Missing alloc, dealloc, rustcdc_abi_version — only has transform.
        write_wat_module(
            &module_path,
            r#"(module
                (memory (export "memory") 1)
                (func (export "transform") (param i32 i32) (result i64) i64.const 0))"#,
        );

        let result = WasmRuntime::new(module_path.to_str().expect("utf8"));
        assert!(
            matches!(result, Err(Error::ConfigError(ref message)) if message.contains("missing required"))
        );
    }

    #[tokio::test]
    async fn metrics_counts_successful_transforms() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module_path = temp_dir.path().join("module.wasm");
        write_wat_module(&module_path, minimal_conformant_wat());

        let mock = Arc::new(MockWasmModule::new(0));
        let mut runtime = WasmRuntime::new(module_path.to_str().expect("utf8"))
            .expect("runtime")
            .with_module(mock);

        runtime.init().await.expect("init");
        let _ = runtime
            .transform(&minimal_event())
            .await
            .expect("transform 1");
        let _ = runtime
            .transform(&minimal_event())
            .await
            .expect("transform 2");

        let m = runtime.metrics();
        assert_eq!(m.transform_total, 2);
        assert_eq!(m.transform_error_total, 0);
        assert_eq!(m.filtered_total, 0);
        assert_eq!(m.timeout_total, 0);
        assert_eq!(m.instance_pool_size, 1);
    }

    #[tokio::test]
    async fn metrics_counts_timeout_as_error_and_timeout() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module_path = temp_dir.path().join("module.wasm");
        write_wat_module(&module_path, minimal_conformant_wat());

        let mut runtime = WasmRuntime::new_with_config(WasmConfig {
            module_path: module_path.clone(),
            timeout_ms: 10,
            memory_limit_mb: DEFAULT_WASM_MEMORY_LIMIT_MB,
            instance_pool_size: 1,
            fuel_async_yield_interval: None,
        })
        .expect("runtime")
        .with_module(Arc::new(MockWasmModule::new(50)));

        runtime.init().await.expect("init");
        let _ = runtime
            .transform(&minimal_event())
            .await
            .expect_err("timeout");

        let m = runtime.metrics();
        assert_eq!(m.transform_total, 1);
        assert_eq!(m.transform_error_total, 1);
        assert_eq!(m.timeout_total, 1);
    }

    struct FilteringMockModule;

    #[async_trait]
    impl WasmModule for FilteringMockModule {
        async fn transform_bytes(&self, _event_json: &[u8]) -> Result<Option<Event>> {
            Ok(None) // filtered
        }

        fn timeout_ms(&self) -> u64 {
            1_000
        }

        async fn init(&self, _config: &WasmConfig) -> Result<()> {
            Ok(())
        }

        async fn shutdown(&self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn metrics_counts_filtered_events() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module_path = temp_dir.path().join("module.wasm");
        write_wat_module(&module_path, minimal_conformant_wat());

        let mut runtime = WasmRuntime::new(module_path.to_str().expect("utf8"))
            .expect("runtime")
            .with_module(Arc::new(FilteringMockModule));

        runtime.init().await.expect("init");
        let r = runtime
            .transform(&minimal_event())
            .await
            .expect("transform");
        assert!(matches!(r, TransformResult::Filtered));

        let m = runtime.metrics();
        assert_eq!(m.transform_total, 1);
        assert_eq!(m.filtered_total, 1);
        assert_eq!(m.transform_error_total, 0);
    }
}
