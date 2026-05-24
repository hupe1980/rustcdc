//! WASM transform runtime foundation.

mod runtime;

pub use runtime::{
    TransformResult, WasmConfig, WasmModule, WasmRuntime, DEFAULT_WASM_MEMORY_LIMIT_MB,
    DEFAULT_WASM_TIMEOUT_MS,
};
