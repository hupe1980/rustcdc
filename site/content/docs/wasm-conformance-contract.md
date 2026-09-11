+++
title = "WASM conformance contract"
description = "The ABI a rustcdc WASM transform module must implement, and the sandbox guarantees around it."
weight = 80
+++

This document is the normative conformance profile for third-party WASM transform modules used with rustcdc.

## Conformance Requirements

A module is conformant only if it satisfies all requirements in this document.

## Export Requirements

### Required exports

- `memory`
- `alloc(i32) -> i32`
- `dealloc(i32, i32)`
- `transform(i32, i32) -> i64`
- `rustcdc_abi_version() -> i32` — must return `2`

### Optional exports

- `init(i32, i32) -> i32`
- `shutdown() -> i32`

Missing required exports or a wrong `rustcdc_abi_version` return value are hard conformance failures.

## Return Code Requirements

`transform` return value is a packed `i64`:

| Value | Meaning |
|---|---|
| `0` | Event dropped (filter-out) |
| `(out_ptr << 32) \| out_len` | Transformed event; `out_ptr > 0`, `out_len > 0` |

Returning a non-zero packed value with `out_ptr == 0` or `out_len <= 0` is a conformance failure.

`init` and `shutdown` return codes:

- `0`: success
- non-zero: lifecycle failure

## Memory Ownership Requirements

- `alloc` must never return 0 (address 0 is reserved).
- The guest must own and manage memory for the output region until the host has read it.
- The host calls `dealloc(input_ptr, input_len)` after `transform` returns.
- The host calls `dealloc(out_ptr, out_len)` after reading the output (if packed ≠ 0).

## Import Requirements

Allowed imports are limited to:

- `env.log`
- `env.get_metric`
- `env.record_metric`

Any additional import is a conformance failure.

## Data Contract Requirements

- Input payload must be canonical `Event` JSON.
- Successful output payload must be canonical `Event` JSON.
- Output pointer and length must address a valid, readable region of linear memory.

Malformed output payloads are conformance failures.

## Runtime Constraint Requirements

The module must execute correctly under runtime constraints:

- Memory cap configured by host (`WasmConfig.memory_limit_mb`, default 16 MiB).
- Timeout budget configured by host (`WasmConfig.timeout_ms`, default 50 ms).
- Sandboxed execution without direct file or network access.

Timeout and trap handling must not produce undefined behavior.

## Verification Procedure

Run conformance tests:

```bash
cargo test --all-features --test wasm_conformance_contract
cargo test --all-features wasm::runtime::tests --lib
```

Reference artifacts:

- `../fixtures/wasm/pass_through.wat`
- `../fixtures/wasm/filter_out_all.wat`
- `../tests/wasm_conformance_contract.rs`

## Pass Criteria

A module is conformant when:

1. All required exports are present and correctly typed.
2. `rustcdc_abi_version()` returns `2`.
3. No forbidden imports are present.
4. Memory ownership and `dealloc` contracts are satisfied.
5. Lifecycle and transform return contracts are satisfied.
6. Output payload deserialises into canonical `Event` JSON.
7. Conformance tests pass in the target embedding project.

## Related Documentation

- [wasm_transform_sdk.md](@/docs/wasm-transform-sdk.md)
- [api.md](@/docs/api.md)
