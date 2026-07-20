# Writing WASM transforms

rustcdc supports **arbitrary event transforms** compiled to WebAssembly. Any
language that targets WASM can be used — Rust, AssemblyScript, Go (TinyGo),
C/C++, and others.

This guide walks through writing, testing, and deploying a transform module from
scratch using Rust (the most ergonomic choice) and AssemblyScript (the lightest
option for JavaScript-familiar teams). The low-level ABI contract is in
the [ABI reference](#1-how-transforms-work) section below.

---

## Table of contents

1. [How transforms work](#1-how-transforms-work)
2. [Writing a transform in Rust](#2-writing-a-transform-in-rust)
3. [Writing a transform in AssemblyScript](#3-writing-a-transform-in-assemblyscript)
4. [Transform patterns](#4-transform-patterns)
5. [Testing transforms](#5-testing-transforms)
6. [Deploying transforms](#6-deploying-transforms)
7. [Troubleshooting](#7-troubleshooting)

---

## 1. How transforms work

The WASM module is loaded once at startup and executed in a sandboxed pool of
`instance_pool_size` instances (default: 1). For every event:

1. The host serialises the event to JSON and copies it into the module's linear memory.
2. The host calls `transform(ptr, len) → i64`.
3. The module reads the input JSON, applies logic, writes output JSON.
4. Return `0` to **drop** the event (not delivered to sink).
5. Return `(out_ptr << 32) | out_len` to emit a modified (or identical) event.

The module runs in a **deterministic, synchronous** sandbox with no access to
the network, filesystem, or system clock.

### ABI v2 required exports

```
rustcdc_abi_version() → i32     must return 2
alloc(size: i32) → i32          allocate linear memory; never return 0
dealloc(ptr: i32, size: i32)    release memory after host reads it
transform(ptr: i32, len: i32) → i64   main entrypoint
```

---

## 2. Writing a transform in Rust

### Prerequisites

```bash
# Add the wasm32 target
rustup target add wasm32-unknown-unknown

# Optional: install wasm-opt for size reduction
cargo install wasm-opt  # or: brew install binaryen
```

### Create the project

```bash
cargo new --lib rustcdc-transform
cd rustcdc-transform
```

`Cargo.toml`:

```toml
[package]
name    = "rustcdc-transform"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]   # required for WASM output

[dependencies]
serde      = { version = "1", features = ["derive"] }
serde_json = "1"

[profile.release]
opt-level = "z"     # minimise binary size
lto       = true
strip     = true
```

### Minimal pass-through transform

`src/lib.rs`:

```rust
use std::alloc::{alloc, dealloc, Layout};

// ── ABI required exports ──────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn rustcdc_abi_version() -> i32 {
    2
}

#[no_mangle]
pub extern "C" fn alloc(size: i32) -> i32 {
    let layout = Layout::from_size_align(size as usize, 1).unwrap();
    unsafe { alloc(layout) as i32 }
}

#[no_mangle]
pub extern "C" fn dealloc(ptr: i32, size: i32) {
    let layout = Layout::from_size_align(size as usize, 1).unwrap();
    unsafe { dealloc(ptr as *mut u8, layout) }
}

// ── Transform entrypoint ──────────────────────────────────────────────────

/// Read input JSON, return (optionally modified) output JSON.
/// Return 0 to drop the event.
#[no_mangle]
pub extern "C" fn transform(ptr: i32, len: i32) -> i64 {
    let input = unsafe {
        std::slice::from_raw_parts(ptr as *const u8, len as usize)
    };

    // Parse into a generic JSON value for manipulation
    let mut event: serde_json::Value = match serde_json::from_slice(input) {
        Ok(v) => v,
        Err(_) => return 0,   // drop malformed events
    };

    // ── Your logic here ────────────────────────────────────────────────────
    // Example: add a processing timestamp field
    if let Some(obj) = event.as_object_mut() {
        obj.insert(
            "_processed_by".to_string(),
            serde_json::Value::String("my-transform-v1".to_string()),
        );
    }
    // ──────────────────────────────────────────────────────────────────────

    // Serialise output
    let output = match serde_json::to_vec(&event) {
        Ok(b) => b,
        Err(_) => return 0,
    };

    // Allocate output buffer and copy bytes
    let layout = Layout::from_size_align(output.len(), 1).unwrap();
    let out_ptr = unsafe { alloc(layout) };
    if out_ptr.is_null() {
        return 0;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(output.as_ptr(), out_ptr, output.len());
    }

    // Pack (out_ptr, out_len) into i64
    (out_ptr as i64) << 32 | output.len() as i64
}
```

### Build

```bash
cargo build --release --target wasm32-unknown-unknown

# Optional: optimise with wasm-opt (reduces size ~30-50%)
wasm-opt -Oz \
  target/wasm32-unknown-unknown/release/rustcdc_transform.wasm \
  -o transform.wasm

# Without wasm-opt:
cp target/wasm32-unknown-unknown/release/rustcdc_transform.wasm transform.wasm
```

---

## 3. Writing a transform in AssemblyScript

AssemblyScript compiles a TypeScript-like language to WASM and is a good choice
when the team is more comfortable with JavaScript/TypeScript than Rust.

### Prerequisites

```bash
npm init -y
npm install --save-dev assemblyscript
npx asinit .
```

### `assembly/index.ts`

```typescript
import { JSON } from "assemblyscript-json/assembly";

// ── ABI required exports ──────────────────────────────────────────────────

export function rustcdc_abi_version(): i32 {
  return 2;
}

// AssemblyScript's runtime provides alloc/dealloc via __new/__pin/__unpin.
// Re-export the memory management functions the host expects:

export function alloc(size: i32): i32 {
  return heap.alloc(size) as i32;
}

export function dealloc(ptr: i32, size: i32): void {
  heap.free(ptr as usize);
}

// ── Transform entrypoint ──────────────────────────────────────────────────

export function transform(ptr: i32, len: i32): i64 {
  // Read input JSON bytes
  const inputBytes = Uint8Array.wrap(memory.buffer, ptr, len);
  const inputStr   = String.UTF8.decode(inputBytes.buffer, false, ptr, len);

  // Parse
  const event = JSON.parse(inputStr);
  if (!(event instanceof JSON.Obj)) return 0;
  const obj = event as JSON.Obj;

  // ── Your logic here ────────────────────────────────────────────────────
  // Example: drop events for the sessions table
  const table = obj.getString("table");
  if (table !== null && table.valueOf() === "sessions") {
    return 0;   // drop
  }
  // ──────────────────────────────────────────────────────────────────────

  // Serialise output (pass through unchanged in this example)
  const outputStr  = event.stringify();
  const outputBytes = String.UTF8.encode(outputStr);
  const outLen      = outputBytes.byteLength;
  const outPtr      = heap.alloc(outLen) as i32;
  memory.copy(outPtr, changetype<usize>(outputBytes), outLen);

  return (outPtr as i64) << 32 | (outLen as i64);
}
```

### Build

```bash
npx asc assembly/index.ts \
  --target release \
  --outFile transform.wasm \
  --optimizeLevel 3 \
  --shrinkLevel 1
```

---

## 4. Transform patterns

### The envelope contract (availability lists)

The event JSON may carry `unavailable_columns` / `before_unavailable_columns` —
columns the source could not supply (PostgreSQL unchanged-TOAST). Those columns
are **absent** from the payload, not `null`; see
[Core concepts — partial row images](concepts.md#partial-row-images-unchanged-toast).

Rules for transforms:

- **Don't invent values for listed columns.** If your module materializes a
  column that appears in a list (adds it to `after`, or renames another column
  onto that name), the server automatically drops the entry — the payload now
  carries a value, so the "unavailable" claim no longer holds.
- **Keep the lists when reshaping.** If you forward the event, forward the lists;
  downstream consumers rely on them to avoid writing `NULL` over unchanged data.
- **Validation is enforced.** After every native rule or WASM transform, the
  server validates the envelope (rustcdc `Event::validate()`). An event whose op,
  payloads, and availability lists contradict each other is rejected and handled
  by `transform_error_policy` (`halt` stops the pipeline; `skip` drops the event
  and **counts it in `rustcdc_runtime_events_skipped_total` — data loss**).

### Redact a column

```rust
if let Some(after) = event.get_mut("after").and_then(|v| v.as_object_mut()) {
    after.insert("email".to_string(), serde_json::Value::String("***".to_string()));
}
```

### Drop events for specific tables

```rust
if event.get("table").and_then(|v| v.as_str()) == Some("audit_log") {
    return 0;  // drop
}
```

### Add a derived field

```rust
if let (Some(qty), Some(price)) = (
    event["after"]["quantity"].as_f64(),
    event["after"]["unit_price"].as_f64(),
) {
    if let Some(after) = event["after"].as_object_mut() {
        after.insert("total".to_string(), serde_json::json!(qty * price));
    }
}
```

### Route events by op type

Use the `op` field to fork logic:

```rust
match event.get("op").and_then(|v| v.as_str()) {
    Some("delete") => {
        // Emit a tombstone-style event with only the primary key
        let key = event["before"]["id"].clone();
        let tombstone = serde_json::json!({ "op": "delete", "key": key });
        // ... write tombstone to output buffer
    }
    _ => { /* pass through */ }
}
```

### Flatten a nested JSONB column

```rust
if let Some(after) = event["after"].as_object_mut() {
    if let Some(serde_json::Value::String(raw)) = after.remove("metadata") {
        if let Ok(nested) = serde_json::from_str::<serde_json::Value>(&raw) {
            if let Some(nested_obj) = nested.as_object() {
                for (k, v) in nested_obj {
                    after.insert(format!("meta_{k}"), v.clone());
                }
            }
        }
    }
}
```

---

## 5. Testing transforms

### Unit testing (Rust)

The transform function is pure Rust — test it directly without WASM:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn call_transform(input: serde_json::Value) -> Option<serde_json::Value> {
        let bytes = serde_json::to_vec(&input).unwrap();
        // Manually invoke the core logic (extract it into a separate fn):
        apply_transform(input)
    }

    #[test]
    fn adds_processed_by_field() {
        let input = serde_json::json!({
            "op": "insert",
            "schema": "public",
            "table": "orders",
            "after": { "id": 1, "status": "pending" }
        });
        let output = call_transform(input).unwrap();
        assert_eq!(output["_processed_by"], "my-transform-v1");
    }

    #[test]
    fn drops_session_table_events() {
        let input = serde_json::json!({
            "op": "insert", "schema": "public", "table": "sessions", "after": {}
        });
        assert!(call_transform(input).is_none());
    }
}
```

Refactor `transform()` to call an inner `apply_transform(event: Value) -> Option<Value>` function to keep the ABI boundary and the logic separately testable.

### Integration testing with `rustcdc dry-run`

```bash
# Build the WASM module first
cargo build --release --target wasm32-unknown-unknown
cp target/wasm32-unknown-unknown/release/rustcdc_transform.wasm /etc/rustcdc/transform.wasm

# Fire 20 synthetic events through the full pipeline (including the WASM module)
rustcdc dry-run --config cdc.toml --event-count 20
```

`cdc.toml` for testing:

```toml
api_version = "v1"

[source]
type = "postgres"
# ... (source config)

[sink]
type = "stdout"   # inspect output

[pipeline.transform_runtime]
mode = "wasm"

  [pipeline.transform_runtime.wasm]
  module_path        = "/etc/rustcdc/transform.wasm"
  timeout_ms         = 100
  max_memory_bytes   = 8388608
  instance_pool_size = 1
```

### Benchmarking

```bash
# Replay a captured event file through the WASM transform
rustcdc replay events.jsonl --config cdc.toml --sink stdout --limit 10000
# Watch: rustcdc_runtime_events_committed_total rate in /metrics
```

---

## 6. Deploying transforms

### Binary path

Mount the `.wasm` file into the container and reference it in `cdc.toml`:

```toml
[pipeline.transform_runtime]
mode = "wasm"

  [pipeline.transform_runtime.wasm]
  module_path          = "/etc/rustcdc/transform.wasm"
  timeout_ms           = 50
  max_memory_bytes     = 8388608    # 8 MiB — raise if your logic allocates large buffers
  max_event_bytes      = 1048576    # 1 MiB per event
  instance_pool_size   = 4          # concurrent transform threads
  fuel_yield_interval  = 10000      # yield to Tokio every N fuel units; null = disable
```

### Kubernetes ConfigMap + volume

```yaml
# Create a ConfigMap from the .wasm binary
kubectl create configmap rustcdc-transform \
  --from-file=transform.wasm=./transform.wasm

# Mount it in the Deployment:
volumes:
  - name: transform
    configMap:
      name: rustcdc-transform
      items:
        - key: transform.wasm
          path: transform.wasm

volumeMounts:
  - name: transform
    mountPath: /etc/rustcdc/transform.wasm
    subPath: transform.wasm
    readOnly: true
```

### Updating a deployed transform

1. Build the new `.wasm` binary.
2. Update the ConfigMap:
   ```bash
   kubectl create configmap rustcdc-transform \
     --from-file=transform.wasm=./transform.wasm \
     --dry-run=client -o yaml | kubectl apply -f -
   ```
3. Rolling-restart the Deployment (rustcdc reloads the module on startup):
   ```bash
   kubectl rollout restart deployment/rustcdc
   ```

---

## 7. Troubleshooting

### Module rejected at startup

**Error:** `wasm transform module rejected: missing export rustcdc_abi_version`

**Fix:** add `rustcdc_abi_version() → i32` returning `2`.

---

**Error:** `wasm transform module rejected: abi version mismatch (got 1, expected 2)`

**Fix:** update your exports to match the ABI v2 contract described in [How transforms work](#1-how-transforms-work).

---

### Events silently dropped

If events stop appearing at the sink after adding a transform, check whether the
transform returns `0`:

1. Temporarily add a log line to the transform (write to stderr — rustcdc forwards
   WASM stderr to the structured log at `warn` level):
   ```rust
   eprintln!("[transform] dropping event: op={:?}", event.get("op"));
   ```
2. Check `/metrics` for `rustcdc_runtime_events_committed_total` — if it's growing but
   nothing arrives at the sink, the transform is dropping events.
3. Set `transform_error_policy = "skip"` temporarily to see if trapping/OOM is
   the cause.

---

### Transform timeout

**Error:** `wasm transform timed out after 50ms`

**Fix:** increase `timeout_ms`, or profile the module to find the bottleneck.
Common causes: expensive JSON parsing of large arrays, unbounded loops,
excessive allocations.

---

### OOM inside module

**Error:** `wasm alloc returned null pointer — module OOM`

**Fix:** increase `max_memory_bytes`. Also check for memory leaks — ensure every
`alloc` call in your module has a matching `dealloc`.

---

## See also

- [Configuration reference → WASM transform runtime](configuration.md#wasm-transform-runtime)
- [Core concepts → Transform pipeline](concepts.md#4-transform-pipeline)
- [Operations guide → Dry run](operations.md#7-dry-run)
