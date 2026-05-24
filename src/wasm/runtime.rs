//! WASM runtime for transform loading and execution.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use wasmparser::{ExternalKind, Parser, Payload, TypeRef, ValType};
use wasmtime::{Caller, Engine, Extern, Instance, Linker, Memory, Module, Store, TypedFunc};

use crate::core::{Error, Event, Result};

pub const DEFAULT_WASM_TIMEOUT_MS: u64 = 10;
pub const DEFAULT_WASM_MEMORY_LIMIT_MB: u64 = 16;

#[derive(Debug, Clone)]
pub struct WasmConfig {
    pub module_path: PathBuf,
    pub timeout_ms: u64,
    pub memory_limit_mb: u64,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            module_path: PathBuf::new(),
            timeout_ms: DEFAULT_WASM_TIMEOUT_MS,
            memory_limit_mb: DEFAULT_WASM_MEMORY_LIMIT_MB,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransformResult {
    Ok(Box<Event>),
    Err(String),
}

#[async_trait]
pub trait WasmModule: Send + Sync {
    async fn transform(&self, event: &Event) -> Result<Option<Event>>;
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
}

struct RealWasmModule {
    timeout_ms: u64,
    inner: Mutex<RealWasmState>,
}

struct RealWasmState {
    store: Store<()>,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    transform: TypedFunc<(i32, i32), i32>,
    output_len: TypedFunc<(), i32>,
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
        let module = RealWasmModule::new(&module_bytes, &config)?;

        Ok(Self {
            config,
            module_bytes,
            module: Arc::new(module),
            initialized: false,
        })
    }

    pub fn with_module(mut self, module: Arc<dyn WasmModule>) -> Self {
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
            return Err(Error::StateError(
                "WASM runtime must be initialized before transform()".to_string(),
            ));
        }

        validate_event_within_memory_limit(event, self.config.memory_limit_mb)?;

        let effective_timeout_ms = self.module.timeout_ms().min(self.config.timeout_ms).max(1);
        let operation = tokio::time::timeout(
            Duration::from_millis(effective_timeout_ms),
            self.module.transform(event),
        )
        .await
        .map_err(|_| {
            Error::TimeoutError(format!(
                "WASM transform exceeded timeout ({} ms)",
                effective_timeout_ms
            ))
        })??;

        match operation {
            Some(transformed) => Ok(TransformResult::Ok(Box::new(transformed))),
            None => Ok(TransformResult::Err(
                "WASM transform filtered event".to_string(),
            )),
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
}

impl RealWasmModule {
    fn new(module_bytes: &[u8], config: &WasmConfig) -> Result<Self> {
        let engine = Engine::default();
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

        let mut store = Store::new(&engine, ());
        let instance = linker.instantiate(&mut store, &module).map_err(|error| {
            Error::ConfigError(format!("failed to instantiate WASM module: {error}"))
        })?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| Error::ConfigError("WASM module missing memory export".to_string()))?;

        let alloc = get_typed_func::<i32, i32>(&mut store, &instance, "alloc")?;
        let transform = get_typed_func::<(i32, i32), i32>(&mut store, &instance, "transform")?;
        let output_len = get_typed_func::<(), i32>(&mut store, &instance, "output_len")?;
        let init = optional_typed_func::<(i32, i32), i32>(&mut store, &instance, "init")?;
        let shutdown = optional_typed_func::<(), i32>(&mut store, &instance, "shutdown")?;

        let module_state = RealWasmState {
            store,
            memory,
            alloc,
            transform,
            output_len,
            init,
            shutdown,
        };

        let module = Self {
            timeout_ms: config.timeout_ms,
            inner: Mutex::new(module_state),
        };

        module.validate_memory_limit(config.memory_limit_mb)?;
        Ok(module)
    }

    fn validate_memory_limit(&self, limit_mb: u64) -> Result<()> {
        let state = self
            .inner
            .lock()
            .map_err(|_| Error::StateError("WASM runtime lock poisoned".to_string()))?;
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

        if ptr < 0 {
            return Err(Error::TransformError(format!(
                "WASM alloc returned negative pointer: {ptr}"
            )));
        }

        state
            .memory
            .write(&mut state.store, ptr as usize, payload)
            .map_err(|error| Error::TransformError(format!("WASM memory write failed: {error}")))?;
        Ok(ptr)
    }

    fn read_output(state: &mut RealWasmState, ptr: i32, len: i32) -> Result<Vec<u8>> {
        if ptr < 0 {
            return Err(Error::TransformError(format!(
                "WASM output pointer is negative: {ptr}"
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
    async fn transform(&self, event: &Event) -> Result<Option<Event>> {
        let input = serde_json::to_vec(event)?;
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Error::StateError("WASM runtime lock poisoned".to_string()))?;

        let input_ptr = Self::alloc_and_write(&mut state, &input)?;
        let transform = state.transform.clone();
        let output_ptr = transform
            .call(
                &mut state.store,
                (
                    input_ptr,
                    i32::try_from(input.len()).map_err(|_| {
                        Error::TransformError(format!(
                            "WASM input length exceeds i32: {}",
                            input.len()
                        ))
                    })?,
                ),
            )
            .map_err(|error| {
                Error::TransformError(format!("WASM transform call failed: {error}"))
            })?;

        if output_ptr == -1 {
            return Ok(None);
        }
        if output_ptr < -1 {
            return Err(Error::TransformError(format!(
                "WASM transform returned failure code: {output_ptr}"
            )));
        }

        let output_len_fn = state.output_len.clone();
        let output_len = output_len_fn.call(&mut state.store, ()).map_err(|error| {
            Error::TransformError(format!("WASM output_len call failed: {error}"))
        })?;
        let output = Self::read_output(&mut state, output_ptr, output_len)?;

        if output.is_empty() {
            return Ok(None);
        }

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
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Error::StateError("WASM runtime lock poisoned".to_string()))?;
        let Some(init) = state.init.clone() else {
            return Ok(());
        };

        let config_payload = serde_json::json!({
            "timeout_ms": config.timeout_ms,
            "memory_limit_mb": config.memory_limit_mb,
        });
        let config_bytes = serde_json::to_vec(&config_payload)?;
        let ptr = Self::alloc_and_write(&mut state, &config_bytes)?;

        let status = init
            .call(
                &mut state.store,
                (
                    ptr,
                    i32::try_from(config_bytes.len()).map_err(|_| {
                        Error::ConfigError(
                            "WASM init config payload exceeds i32 length".to_string(),
                        )
                    })?,
                ),
            )
            .map_err(|error| Error::ConfigError(format!("WASM init call failed: {error}")))?;
        if status != 0 {
            return Err(Error::ConfigError(format!(
                "WASM init returned non-zero status: {status}"
            )));
        }

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Error::StateError("WASM runtime lock poisoned".to_string()))?;
        let Some(shutdown) = state.shutdown.clone() else {
            return Ok(());
        };

        let status = shutdown
            .call(&mut state.store, ())
            .map_err(|error| Error::StateError(format!("WASM shutdown call failed: {error}")))?;
        if status != 0 {
            return Err(Error::StateError(format!(
                "WASM shutdown returned non-zero status: {status}"
            )));
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
                for entry in imports {
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

    let required_func_exports = ["alloc", "transform", "output_len"];
    for name in required_func_exports {
        if !exported_funcs.contains_key(name) {
            return Err(Error::ConfigError(format!(
                "WASM module missing required export '{name}'"
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
    validate_export_signature(
        "transform",
        &exported_funcs,
        &function_type_indices,
        &type_sigs,
        &[ValType::I32, ValType::I32],
        &[ValType::I32],
    )?;
    validate_export_signature(
        "output_len",
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

fn validate_event_within_memory_limit(event: &Event, memory_limit_mb: u64) -> Result<()> {
    let serialized = serde_json::to_vec(event)?;
    let limit_bytes = memory_limit_mb.saturating_mul(1024 * 1024);
    if (serialized.len() as u64) > limit_bytes {
        return Err(Error::TransformError(format!(
            "event payload exceeds configured WASM memory limit ({} bytes > {} bytes)",
            serialized.len(),
            limit_bytes
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
        async fn transform(&self, event: &Event) -> Result<Option<Event>> {
            self.transform_calls.fetch_add(1, Ordering::Relaxed);
            if self.transform_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(self.transform_delay_ms)).await;
            }
            Ok(Some(event.clone()))
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
                        (global $heap (mut i32) (i32.const 1024))
                        (global $out_len (mut i32) (i32.const 0))
                        (func (export "alloc") (param $len i32) (result i32)
                            (local $ptr i32)
                            global.get $heap
                            local.set $ptr
                            global.get $heap
                            local.get $len
                            i32.add
                            global.set $heap
                            local.get $ptr)
                        (func (export "output_len") (result i32)
                            global.get $out_len)
                        (func (export "transform") (param i32 i32) (result i32)
                            i32.const -1))"#
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
                (func (export "alloc") (param i32) (result i32) i32.const 0)
                (func (export "output_len") (result i32) i32.const 0)
                (func (export "transform") (param i32 i32) (result i32) i32.const -1))"#,
        );

        let result = WasmRuntime::new(module_path.to_str().expect("utf8"));
        assert!(matches!(result, Err(Error::ConfigError(_))));
    }

    #[tokio::test]
    async fn rejects_module_missing_required_exports() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module_path = temp_dir.path().join("module.wasm");
        write_wat_module(
            &module_path,
            r#"(module
                (memory (export "memory") 1)
                (func (export "transform") (param i32 i32) (result i32) i32.const -1))"#,
        );

        let result = WasmRuntime::new(module_path.to_str().expect("utf8"));
        assert!(
            matches!(result, Err(Error::ConfigError(message)) if message.contains("missing required export"))
        );
    }

    #[tokio::test]
    async fn rejects_module_with_invalid_transform_signature() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module_path = temp_dir.path().join("module.wasm");
        write_wat_module(
            &module_path,
            r#"(module
                (memory (export "memory") 1)
                (func (export "alloc") (param i32) (result i32) i32.const 0)
                (func (export "output_len") (result i32) i32.const 0)
                (func (export "transform") (param i32) (result i32) i32.const 0))"#,
        );

        let result = WasmRuntime::new(module_path.to_str().expect("utf8"));
        assert!(
            matches!(result, Err(Error::ConfigError(message)) if message.contains("invalid signature"))
        );
    }
}
