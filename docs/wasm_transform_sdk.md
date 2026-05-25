# WASM Transform SDK

## Scope
This document defines the contract for running user-provided WASM transforms in rustcdc.
The runtime provides a concrete execution engine with static contract validation, lifecycle hooks, memory IO, and timeout/memory guardrails.

## ABI Contract

### Host imports available to guest (`env.*`)
- `log(level: i32, ptr: i32, len: i32)`
- `get_metric(ptr: i32) -> i64`
- `record_metric(ptr: i32, value: i64)`

### Guest exports

Required:
- `memory`
- `alloc(len: i32) -> i32`
- `transform(event_ptr: i32, event_len: i32) -> i32`
- `output_len() -> i32`

Optional:
- `init(config_ptr: i32, config_len: i32) -> i32`
- `shutdown() -> i32`

## Event and Memory Model
- Event serialization format: JSON.
- Host calls `alloc` to reserve guest memory, then writes serialized bytes into guest linear memory.
- `transform` input is a pointer/length pair to canonical `Event` JSON.
- `transform` return semantics:
  - `-1`: filtered event (host returns `None`)
  - `>= 0`: output pointer in guest memory
  - `< -1`: transform failure
- Host calls `output_len()` after successful `transform` to read exactly that many bytes from returned output pointer.
- Successful output bytes must deserialize into canonical `Event` JSON.

## Security and Reliability
- WASM runs sandboxed (no direct file I/O or network access).
- Static import scanning rejects all imports outside `env.log`, `env.get_metric`, and `env.record_metric`.
- Timeout enforced per transform invocation:
  - default `10ms`
  - configurable via `WasmConfig.timeout_ms`
- Memory limit enforced per runtime instance:
  - default `16MB`
  - configurable via `WasmConfig.memory_limit_mb`
- Panics are treated as transform failures and surfaced as runtime errors.

## Performance Targets
- Native overhead target: `< 5x`
- Per-event transform latency target: `< 1ms`
- Throughput target: `> 1K events/sec per transform instance`

## Rust API Reference
Implemented in [src/wasm/runtime.rs](../src/wasm/runtime.rs):
- `WasmRuntime`
  - `new(wasm_module_path: &str) -> Result<Self>`
  - `init(&mut self) -> Result<()>`
  - `transform(&mut self, event: &Event) -> Result<TransformResult>`
  - `shutdown(&mut self) -> Result<()>`
- `WasmModule`
  - `async fn transform(&self, event: &Event) -> Result<Option<Event>>`
  - `fn timeout_ms(&self) -> u64`
- `TransformResult`
  - `Ok(Event)`
  - `Err(String)`
- `WasmConfig`
  - `{ module_path, timeout_ms, memory_limit_mb }`

## Example Guest Transform Skeleton (ABI shape)
```rust
#[no_mangle]
pub extern "C" fn alloc(len: i32) -> i32 {
  let mut buf = Vec::<u8>::with_capacity(len as usize);
  let ptr = buf.as_mut_ptr();
  std::mem::forget(buf);
  ptr as i32
}

#[no_mangle]
pub extern "C" fn output_len() -> i32 {
  LAST_OUTPUT_LEN.load(std::sync::atomic::Ordering::Relaxed)
}

#[no_mangle]
pub extern "C" fn init(_config_ptr: i32, _config_len: i32) -> i32 {
    0
}

#[no_mangle]
pub extern "C" fn transform(event_ptr: i32, event_len: i32) -> i32 {
  let _ = (event_ptr, event_len);
  // Parse input bytes -> Event JSON, produce transformed Event JSON,
  // allocate output in linear memory, store length for output_len(), return pointer.
  -1
}

#[no_mangle]
pub extern "C" fn shutdown() -> i32 {
    0
}

static LAST_OUTPUT_LEN: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);
```

## Compilation Instructions
1. Add target:
```bash
rustup target add wasm32-unknown-unknown
```
2. Build module:
```bash
cargo build --release --target wasm32-unknown-unknown
```

## Non-Goals
- No full WASI runtime integration.
- No cross-module orchestration.
