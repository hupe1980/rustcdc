use rustcdc::{Event, Operation, SourceMetadata, WasmConfig, WasmRuntime, WasmTransformResult};
use serde_json::json;

fn build_event(table: &str) -> Event {
    Event::builder(table.to_string(), Operation::Insert)
        .after(json!({"id": 1, "name": "alice"}))
        .source(SourceMetadata::new(
            "wasm-conformance".to_string(),
            "1".to_string(),
            1000,
        ))
        .ts(1000)
        .schema("public".to_string())
        .primary_key(["id"])
        .build()
}
fn compile_wat_fixture(name: &str) -> tempfile::NamedTempFile {
    let wat_path = std::path::Path::new("fixtures/wasm").join(name);
    let wat_src = std::fs::read_to_string(&wat_path).expect("read wat fixture");
    let wasm = wat::parse_str(&wat_src).expect("compile wat fixture");

    let wasm_file = tempfile::NamedTempFile::new().expect("create temp wasm file");
    std::fs::write(wasm_file.path(), wasm).expect("write wasm fixture");
    wasm_file
}

#[tokio::test]
async fn pass_through_fixture_is_conformant() {
    let wasm_file = compile_wat_fixture("pass_through.wat");
    let wasm_path = wasm_file.path().with_extension("wasm");
    std::fs::copy(wasm_file.path(), &wasm_path).expect("copy with .wasm extension");

    let mut runtime = WasmRuntime::new_with_config(WasmConfig {
        module_path: wasm_path,
        timeout_ms: 10,
        memory_limit_mb: 16,
        instance_pool_size: 1,
        fuel_async_yield_interval: None,
    })
    .expect("create runtime");

    runtime.init().await.expect("init runtime");

    let input = build_event("users");
    let result = runtime.transform(&input).await.expect("transform event");
    let transformed = match result {
        WasmTransformResult::Ok(event) => event,
        WasmTransformResult::Filtered => panic!("unexpected filter-out on passthrough fixture"),
    };

    assert_eq!(transformed.table, "users");
    assert_eq!(transformed.after, input.after);

    runtime.shutdown().await.expect("shutdown runtime");
}

/// The pass-through contract must hold for a module that carries a `data` segment.
///
/// Every module a real toolchain produces has one — Rust, AssemblyScript and TinyGo
/// all emit a data segment for string literals and rodata — and wasmtime evaluates
/// the store's epoch deadline while initialising it. A host that arms the deadline
/// after `instantiate` therefore rejects every real module while a data-segment-free
/// WAT suite reports full conformance.
#[tokio::test]
async fn data_segment_fixture_is_conformant() {
    let wasm_file = compile_wat_fixture("data_segment.wat");
    let wasm_path = wasm_file.path().with_extension("wasm");
    std::fs::copy(wasm_file.path(), &wasm_path).expect("copy with .wasm extension");

    let mut runtime = WasmRuntime::new_with_config(WasmConfig {
        module_path: wasm_path,
        timeout_ms: 10,
        memory_limit_mb: 16,
        // More than one slot: each is instantiated separately, so a slot-level
        // ordering bug survives a pool of one.
        instance_pool_size: 2,
        fuel_async_yield_interval: None,
    })
    .expect("module with a data segment must load");

    runtime.init().await.expect("init runtime");

    let input = build_event("users");
    let result = runtime.transform(&input).await.expect("transform event");
    let transformed = match result {
        WasmTransformResult::Ok(event) => event,
        WasmTransformResult::Filtered => panic!("unexpected filter-out on data-segment fixture"),
    };

    assert_eq!(transformed.table, "users");
    assert_eq!(transformed.after, input.after);

    runtime.shutdown().await.expect("shutdown runtime");
}

#[tokio::test]
async fn filter_fixture_is_conformant() {
    let wasm_file = compile_wat_fixture("filter_out_all.wat");
    let wasm_path = wasm_file.path().with_extension("wasm");
    std::fs::copy(wasm_file.path(), &wasm_path).expect("copy with .wasm extension");

    let mut runtime = WasmRuntime::new(wasm_path.to_str().expect("utf8 path")).expect("runtime");
    runtime.init().await.expect("init runtime");

    let input = build_event("orders");
    let result = runtime.transform(&input).await.expect("transform event");
    assert!(matches!(result, WasmTransformResult::Filtered));

    runtime.shutdown().await.expect("shutdown runtime");
}
