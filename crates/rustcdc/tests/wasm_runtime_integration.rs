//! End-to-end integration tests for the WASM transform pipeline wired into
//! `CdcRuntime`.  These tests use the in-process `Disabled` source and
//! `enqueue_event` to inject synthetic events, then verify they pass through
//! (or are filtered by) a WASM module loaded from `fixtures/wasm/`.

use rustcdc::checkpoint::InMemoryCheckpoint;
use rustcdc::core::{
    CdcRuntime, Event, Operation, RuntimeConfig, RuntimeOptions, RuntimeSourceConfig,
    SourceMetadata, TransformErrorPolicy,
};
use rustcdc::schema_history::InMemorySchemaHistory;
use rustcdc::transform::AsyncTransform;
use rustcdc::wasm::{TransformResult, WasmConfig, WasmRuntime};
use serde_json::json;
use std::path::Path;
use tokio::sync::Mutex;

fn make_event(table: &str, id: u64) -> Event {
    Event::builder(table, Operation::Insert)
        .after(json!({"id": id, "name": "alice"}))
        .source(SourceMetadata::new("wasm-e2e", id.to_string(), id))
        .ts(id)
        .schema("public")
        .primary_key(["id"])
        .build()
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
) -> (
    CdcRuntime,
    std::sync::mpsc::Receiver<(Event, rustcdc::Error)>,
) {
    let transform = RuntimeWasmTransform::new(WasmConfig {
        module_path: wasm_path.as_ref().to_path_buf(),
        timeout_ms: 50,
        memory_limit_mb: 16,
        instance_pool_size: 1,
        fuel_async_yield_interval: None,
    })
    .await
    .expect("create wasm transform");

    // `Skip` drops the event *and* advances the checkpoint past it, so a dead-letter
    // handler is mandatory — the runtime refuses to build without one. Nothing is
    // expected to reach it here; a non-empty channel at the end of a test means an
    // event was silently discarded.
    let (dead_letters, dead_letter_rx) = std::sync::mpsc::channel();
    let config = RuntimeConfig::new(
        RuntimeSourceConfig::Disabled,
        InMemoryCheckpoint::default(),
        InMemorySchemaHistory::default(),
    )
    .with_transform_error_policy(transform_error_policy)
    .with_options(
        RuntimeOptions::new().with_dead_letter_handler(move |event, error| {
            let _ = dead_letters.send((event, error));
        }),
    );

    let mut runtime = CdcRuntime::new(config).expect("create runtime");
    runtime.add_async_transform(Box::new(transform));
    runtime.start().await.expect("start runtime");
    (runtime, dead_letter_rx)
}

struct RuntimeWasmTransform {
    runtime: Mutex<WasmRuntime>,
}

impl std::fmt::Debug for RuntimeWasmTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeWasmTransform")
            .finish_non_exhaustive()
    }
}

impl RuntimeWasmTransform {
    async fn new(config: WasmConfig) -> rustcdc::Result<Self> {
        let mut runtime = WasmRuntime::new_with_config(config)?;
        runtime.init().await?;
        Ok(Self {
            runtime: Mutex::new(runtime),
        })
    }
}

// WASM must await, so it implements the async variant of the trait.
#[async_trait::async_trait]
impl AsyncTransform for RuntimeWasmTransform {
    async fn apply(&self, event: &mut Event) -> rustcdc::Result<bool> {
        let mut runtime = self.runtime.lock().await;
        match runtime.transform(event).await? {
            TransformResult::Ok(transformed) => {
                *event = *transformed;
                Ok(true)
            }
            TransformResult::Filtered => Ok(false),
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
    let (mut runtime, _dead_letters) =
        build_runtime_with_wasm(wasm_file.path(), TransformErrorPolicy::Halt).await;

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
    let (mut runtime, _dead_letters) =
        build_runtime_with_wasm(wasm_file.path(), TransformErrorPolicy::Halt).await;

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
    let (mut runtime, dead_letters) =
        build_runtime_with_wasm(wasm_file.path(), TransformErrorPolicy::Skip).await;

    runtime.enqueue_event(make_event("accounts", 42)).unwrap();
    let batch = runtime.poll_event_batch().await.unwrap();
    // With pass_through + Skip, events should be delivered normally.
    assert_eq!(batch.len(), 1);
    assert!(
        dead_letters.try_recv().is_err(),
        "Skip must not dead-letter an event the transform handled successfully"
    );
}
