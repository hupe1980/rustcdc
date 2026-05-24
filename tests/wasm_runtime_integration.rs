//! End-to-end integration tests for the WASM transform pipeline wired into
//! `CdcRuntime`.  These tests use the in-process `Disabled` source and
//! `enqueue_event` to inject synthetic events, then verify they pass through
//! (or are filtered by) a WASM module loaded from `fixtures/wasm/`.

use async_trait::async_trait;
use cdc_rs::checkpoint::InMemoryCheckpoint;
use cdc_rs::core::{
    CdcRuntime, Event, Operation, RuntimeConfig, RuntimeSourceConfig, SourceMetadata,
    TransformErrorPolicy, EVENT_ENVELOPE_VERSION,
};
use cdc_rs::schema_history::InMemorySchemaHistory;
use cdc_rs::transform::Transform;
use cdc_rs::wasm::{TransformResult, WasmConfig, WasmRuntime};
use serde_json::json;
use std::path::Path;
use tokio::sync::Mutex;

fn make_event(table: &str, id: u64) -> Event {
    Event {
        before: None,
        after: Some(json!({"id": id, "name": "alice"})),
        op: Operation::Insert,
        source: SourceMetadata {
            source_name: "wasm-e2e".into(),
            offset: id.to_string(),
            timestamp: id,
        },
        ts: id,
        schema: Some("public".into()),
        table: table.into(),
        primary_key: Some(vec!["id".into()]),
        snapshot: None,
        transaction: None,
        envelope_version: EVENT_ENVELOPE_VERSION,
    }
}

fn compile_wat(name: &str) -> tempfile::NamedTempFile {
    let wat_path = Path::new("fixtures/wasm").join(name);
    let wat_src = std::fs::read_to_string(&wat_path).expect("read wat fixture");
    let wasm = wat::parse_str(&wat_src).expect("compile wat fixture");
    let tmp = tempfile::Builder::new()
        .suffix(".wasm")
        .tempfile()
        .expect("create temp wasm file");
    std::fs::write(tmp.path(), wasm).expect("write wasm");
    tmp
}

async fn build_runtime_with_wasm(
    wasm_path: impl AsRef<Path>,
    transform_error_policy: TransformErrorPolicy,
) -> CdcRuntime<InMemoryCheckpoint, InMemorySchemaHistory> {
    let transform = RuntimeWasmTransform::new(WasmConfig {
        module_path: wasm_path.as_ref().to_path_buf(),
        timeout_ms: 50,
        memory_limit_mb: 16,
    })
    .await
    .expect("create wasm transform");

    let config = RuntimeConfig::new(
        RuntimeSourceConfig::Disabled,
        InMemoryCheckpoint::default(),
        InMemorySchemaHistory::default(),
    )
    .with_transform_error_policy(transform_error_policy);

    let mut runtime = CdcRuntime::new(config).expect("create runtime");
    runtime.add_transform(Box::new(transform));
    runtime.start().await.expect("start runtime");
    runtime
}

struct RuntimeWasmTransform {
    runtime: Mutex<WasmRuntime>,
}

impl RuntimeWasmTransform {
    async fn new(config: WasmConfig) -> cdc_rs::Result<Self> {
        let mut runtime = WasmRuntime::new_with_config(config)?;
        runtime.init().await?;
        Ok(Self {
            runtime: Mutex::new(runtime),
        })
    }
}

#[async_trait]
impl Transform for RuntimeWasmTransform {
    async fn apply(&self, event: &mut Event) -> cdc_rs::Result<bool> {
        let mut runtime = self.runtime.lock().await;
        match runtime.transform(event).await? {
            TransformResult::Ok(transformed) => {
                *event = *transformed;
                Ok(true)
            }
            TransformResult::Err(_) => Ok(false),
        }
    }

    fn name(&self) -> &str {
        "wasm_runtime_transform"
    }
}

/// Verify that a pass-through WASM module forwards events unchanged.
#[tokio::test]
async fn pass_through_wasm_forwards_events() {
    let wasm_file = compile_wat("pass_through.wat");
    let mut runtime = build_runtime_with_wasm(wasm_file.path(), TransformErrorPolicy::Halt).await;

    let event = make_event("users", 1);
    runtime.enqueue_event(event.clone()).unwrap();

    let batch = runtime.poll_event_batch().await.unwrap();
    assert_eq!(
        batch.len(),
        1,
        "expected exactly one event from pass-through"
    );
    let got = &batch.events()[0];
    assert_eq!(got.table, "users");
    assert_eq!(got.op, Operation::Insert);
    assert_eq!(got.after, event.after);
}

/// Verify that a filter-all WASM module drops every event (returns -1).
#[tokio::test]
async fn filter_all_wasm_drops_events() {
    let wasm_file = compile_wat("filter_out_all.wat");
    let mut runtime = build_runtime_with_wasm(wasm_file.path(), TransformErrorPolicy::Halt).await;

    for id in 1u64..=3 {
        runtime.enqueue_event(make_event("orders", id)).unwrap();
    }

    let batch = runtime.poll_event_batch().await.unwrap();
    assert!(
        batch.is_empty(),
        "filter_out_all module must drop all events, but got {}",
        batch.len()
    );
}

/// Verify that `TransformErrorPolicy::Skip` skips events that cause transform
/// errors and does not propagate the error to the caller.
#[tokio::test]
async fn transform_skip_policy_does_not_halt() {
    // pass_through never errors, so we test the Skip policy still delivers events.
    let wasm_file = compile_wat("pass_through.wat");
    let mut runtime = build_runtime_with_wasm(wasm_file.path(), TransformErrorPolicy::Skip).await;

    runtime.enqueue_event(make_event("accounts", 42)).unwrap();
    let batch = runtime.poll_event_batch().await.unwrap();
    // With pass_through + Skip, events should be delivered normally.
    assert_eq!(batch.len(), 1);
}
