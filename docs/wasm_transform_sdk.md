# WASM Transform SDK

## Scope
This document defines the contract for running user-provided WASM transforms in rustcdc.
The runtime provides a concrete execution engine with static contract validation, lifecycle hooks, memory IO, and timeout/memory guardrails.

## ABI Contract

### Host imports available to guest (`env.*`)
- `log(level: i32, ptr: i32, len: i32)`
- `get_metric(ptr: i32) -> i64`
- `record_metric(ptr: i32, value: i64)`

Any import outside this set is rejected at load time by static analysis.

### Guest exports

**Required:**
- `memory`
- `alloc(size: i32) -> i32` — allocate `size` bytes; must never return 0 (address 0 is reserved)
- `dealloc(ptr: i32, size: i32)` — release the region `[ptr .. ptr+size)`
- `transform(event_ptr: i32, event_len: i32) -> i64` — see return semantics below
- `rustcdc_abi_version() -> i32` — must return `2`

**Optional:**
- `init(config_ptr: i32, config_len: i32) -> i32`
- `shutdown() -> i32`

Missing required exports or a wrong `rustcdc_abi_version` return value are hard errors at load time.

### `transform` return semantics

The return value is a packed `i64`:

| Value | Meaning |
|---|---|
| `0` | Drop the event (filter-out). No output memory is allocated. |
| `(out_ptr << 32) \| out_len` | Transformed event. High 32 bits: output pointer. Low 32 bits: output byte length. |

- `out_ptr` must be > 0 (address 0 is reserved).
- `out_len` must be > 0 when a non-zero packed value is returned.
- The bytes at `[out_ptr .. out_ptr+out_len)` must deserialise into canonical `Event` JSON.
- The host calls `dealloc(out_ptr, out_len)` after reading the output.

### Memory ownership

1. Host calls `alloc(event_len)` → gets `input_ptr`.
2. Host writes serialised `Event` JSON into `[input_ptr .. input_ptr+event_len)`.
3. Host calls `transform(input_ptr, event_len)` → gets packed `i64`.
4. Host calls `dealloc(input_ptr, event_len)` unconditionally.
5. If packed ≠ 0, host reads output, then calls `dealloc(out_ptr, out_len)`.

## Event and Memory Model
- Event serialisation format: JSON.
- Input and output events must both be canonical `Event` JSON.
- Address 0 is reserved; `alloc` must never return it.

## Security and Reliability
- WASM runs sandboxed (no direct file I/O or network access).
- Static import scanning rejects all imports outside the three `env.*` functions above.
- Timeout enforced per transform invocation:
  - default `50ms`
  - configurable via `WasmConfig.timeout_ms`
- Memory limit enforced per runtime instance:
  - default `16MB`
  - configurable via `WasmConfig.memory_limit_mb`
- Traps are surfaced as `Error::TransformError` at the call site.

## Performance Targets
- Native overhead target: `< 5x`
- Per-event transform latency target: `< 1ms`
- Throughput target: `> 1K events/sec per transform instance`

## Threading Model and Concurrency

**Each `WasmRuntime` instance is single-threaded.** Internally, the WASM execution state is protected by a `Mutex`, so concurrent calls to `transform()` on the same instance serialize — only one event is being transformed at a time.

For a single-stream CDC pipeline this is not a bottleneck. However, **if you are running high-throughput multi-table pipelines with WASM transforms**, consider the following patterns:

### Scaling with a WasmRuntime pool

Instantiate multiple `WasmRuntime` instances (one per logical shard or per available core) and dispatch events across them. Wasmtime module compilation is the expensive step; compile once and share the bytes.

```rust
// Pseudo-code: pool of runtime instances
let wasm_bytes = std::fs::read("transform.wasm")?;
let pool: Vec<_> = (0..num_cpus::get())
    .map(|_| WasmRuntime::new_with_config(config.clone()))
    .collect::<Result<Vec<_>, _>>()?;

// Dispatch: pick an instance by thread-local index or round-robin.
```

### Key constraints
- Do **not** share a single `WasmRuntime` across threads without external synchronization — doing so will serialize all transforms and nullify parallelism.
- Each runtime instance owns its own linear memory space; guest state is not shared between pool members.
- Memory and timeout limits apply per-instance, per-invocation.

## Rust API Reference
Implemented in [src/wasm/runtime.rs](../src/wasm/runtime.rs):
- `WasmRuntime`
  - `new(wasm_module_path: &str) -> Result<Self>`
  - `new_with_config(config: WasmConfig) -> Result<Self>`
  - `init(&mut self) -> Result<()>`
  - `transform(&mut self, event: &Event) -> Result<TransformResult>`
  - `shutdown(&mut self) -> Result<()>`
  - `config(&self) -> &WasmConfig`
  - `module_size_bytes(&self) -> usize`
- `TransformResult`
  - `Ok(Box<Event>)` — transformed event
  - `Filtered` — event was dropped by the module (normal outcome)
- `WasmConfig`
  - `{ module_path: PathBuf, timeout_ms: u64, memory_limit_mb: u64 }`

## Example Guest Transform Skeleton (Rust)

```rust
use std::sync::atomic::{AtomicI32, Ordering};

static HEAP: AtomicI32 = AtomicI32::new(8); // address 0 is reserved

#[no_mangle]
pub extern "C" fn rustcdc_abi_version() -> i32 { 2 }

#[no_mangle]
pub extern "C" fn alloc(len: i32) -> i32 {
    HEAP.fetch_add(len, Ordering::Relaxed)
}

#[no_mangle]
pub extern "C" fn dealloc(_ptr: i32, _len: i32) {} // no-op for bump allocator

#[no_mangle]
pub extern "C" fn init(_config_ptr: i32, _config_len: i32) -> i32 { 0 }

#[no_mangle]
pub extern "C" fn transform(event_ptr: i32, event_len: i32) -> i64 {
    // 1. Read input bytes from [event_ptr .. event_ptr+event_len).
    // 2. Parse, transform, serialise output.
    // 3. Allocate output buffer via alloc(out_len).
    // 4. Write output bytes into buffer.
    // 5. Return packed: (out_ptr as i64) << 32 | (out_len as i64)
    //    or 0 to drop the event.
    let _ = (event_ptr, event_len);
    0 // drop the event (example: filter everything)
}

#[no_mangle]
pub extern "C" fn shutdown() -> i32 { 0 }
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

---

## ABI Versioning and Deprecation Policy

The WASM guest ABI is versioned via the `rustcdc_abi_version()` export. The **current ABI is v2**.

### Guarantees

| Guarantee | Policy |
|---|---|
| **Breaking ABI bumps** | Only introduced in **minor or major** crate version increments (never in patch releases). |
| **Minimum notice period** | A new ABI version is introduced as an **opt-in** accepted version first, alongside the current version. The old ABI version is accepted for at least **one full minor-version cycle** (i.e., until the next minor release after the new ABI stabilises). |
| **Breaking rejection** | The old ABI is rejected (hard load error) only in the **next major crate version** after the deprecation notice period ends. |
| **Deprecation announcement** | Announced via `CHANGELOG.md`, the crate-level `#[doc]` on `validate_wasm_contract`, and a `tracing::warn!` at module load time when a deprecated-but-still-accepted version is detected. |

### Current version history

| ABI Version | Introduced | Status | Crate version range |
|---|---|---|---|
| v1 | < 0.5 | **Removed** (rejected at load) | < 0.5.0 |
| v2 | 0.5.0 | **Current** | ≥ 0.5.0 |

### Migration guide: v2 → v3 (future)

When ABI v3 is introduced:

1. The runtime will accept **both v2 and v3** modules and emit a `WARN` log for v2 modules.
2. Guest modules must update `rustcdc_abi_version()` to return `3` and adapt to any changed calling convention documented in the v3 release notes.
3. After one minor-version cycle, v2 modules will be **rejected at load time** with a structured `Error::ConfigError` explaining the required migration.

### Checking your module's ABI version

```bash
# Inspect a compiled WASM module's exports:
wasm-objdump -x your_transform.wasm | grep rustcdc_abi_version
```

Or verify at runtime — the loader surfaces the detected version in the error message on rejection:
```
Error: ConfigError("WASM module ABI version mismatch: got 1, expected 2. \
                    Recompile against rustcdc >= 0.5.0 or use the abi_version = 2 skeleton.")
```
