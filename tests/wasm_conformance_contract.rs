use cdc_rs::{
    Event, Operation, SourceMetadata, WasmConfig, WasmRuntime, WasmTransformResult,
    EVENT_ENVELOPE_VERSION,
};
use serde_json::json;

fn build_event(table: &str) -> Event {
    Event {
        before: None,
        after: Some(json!({"id": 1, "name": "alice"})),
        op: Operation::Insert,
        source: SourceMetadata {
            source_name: "wasm-conformance".to_string(),
            offset: "1".to_string(),
            timestamp: 1000,
        },
        ts: 1000,
        schema: Some("public".to_string()),
        table: table.to_string(),
        primary_key: Some(vec!["id".to_string()]),
        snapshot: None,
        transaction: None,
        envelope_version: EVENT_ENVELOPE_VERSION,
    }
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
        ..Default::default()
    })
    .expect("create runtime");

    runtime.init().await.expect("init runtime");

    let input = build_event("users");
    let result = runtime.transform(&input).await.expect("transform event");
    let transformed = match result {
        WasmTransformResult::Ok(event) => event,
        WasmTransformResult::Err(message) => panic!("unexpected filter result: {message}"),
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
    assert!(matches!(result, WasmTransformResult::Err(_)));

    runtime.shutdown().await.expect("shutdown runtime");
}
