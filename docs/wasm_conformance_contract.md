# WASM Transform Conformance Contract

This document is the normative conformance profile for third-party WASM transform modules used with cdc-rs.

## Conformance Requirements

A module is conformant only if it satisfies all requirements in this document.

## Export Requirements

### Required exports

- `memory`
- `alloc(i32) -> i32`
- `transform(i32, i32) -> i32`
- `output_len() -> i32`

### Optional exports

- `init(i32, i32) -> i32`
- `shutdown() -> i32`

Missing required exports are a hard conformance failure.

## Return Code Requirements

`transform` return codes:

- `-1`: event filtered
- `>= 0`: pointer to output payload
- `< -1`: guest transform failure

`init` and `shutdown` return codes:

- `0`: success
- non-zero: lifecycle failure

## Import Requirements

Allowed imports are limited to:

- `env.log`
- `env.get_metric`
- `env.record_metric`

Any additional import is a conformance failure.

## Data Contract Requirements

- input payload must be canonical `Event` JSON
- successful output payload must be canonical `Event` JSON
- `output_len()` must match exact output byte length

Malformed output payloads are conformance failures.

## Runtime Constraint Requirements

The module must execute correctly under runtime constraints:

- memory cap configured by host
- timeout budget configured by host
- sandboxed execution without direct file or network access

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

1. required exports are present and correctly typed
2. no forbidden imports are present
3. lifecycle and transform return contracts are satisfied
4. output payload and length contracts are satisfied
5. conformance tests pass in the target embedding project

## Related Documentation

- [wasm_transform_sdk.md](wasm_transform_sdk.md)
- [api.md](api.md)
