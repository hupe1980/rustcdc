//! WASM transform integration tests.
//!
//! These tests exercise the full WASM transform pipeline from WAT source
//! through the ABI v2 contract: alloc/dealloc, the `rustcdc_abi_version()` probe,
//! pass-through, event-drop, field mutation, and rejection of invalid ABI
//! modules.  The `wat` crate compiles WAT directly to WASM bytes at test time
//! so no pre-built `.wasm` artifacts are needed.

use std::path::PathBuf;

use rustcdc::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
use serde_json::json;
use tempfile::TempDir;

use rustcdc_server::config::schema::{
    TransformRuleConfig, TransformRuntimeConfig, TransformRuntimeMode, WasmTransformConfig,
};
use rustcdc_server::pipeline::transform::TransformPipeline;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn sample_event() -> Event {
    Event {
        before: None,
        after: Some(json!({
            "id": 1,
            "name": "alice",
            "amount": 99.50
        })),
        op: Operation::Insert,
        source: SourceMetadata {
            source_name: "postgres".to_string(),
            offset: "0/1".to_string(),
            timestamp: 1_000_000,
        },
        ts: 1_000_001,
        schema: Some("public".to_string()),
        table: "orders".to_string(),
        primary_key: Some(vec!["id".to_string()]),
        snapshot: None,
        transaction: None,
        envelope_version: EVENT_ENVELOPE_VERSION,
        before_is_key_only: false,
        unavailable_columns: Vec::new(),
        before_unavailable_columns: Vec::new(),
    }
}

/// Compile WAT source to a `.wasm` file and return its path.
fn compile_wasm(dir: &TempDir, name: &str, wat: &str) -> PathBuf {
    let bytes = wat::parse_str(wat).expect("valid WAT source");
    let path = dir.path().join(name);
    std::fs::write(&path, bytes).expect("write wasm");
    path
}

/// Passthrough WAT: copies input to output offset 65536 using a byte loop.
fn passthrough_wat(memory_pages: u32) -> String {
    format!(
        r#"(module
          (memory (export "memory") {memory_pages} {memory_pages})
          (global $heap (mut i32) (i32.const 8))
          (func (export "alloc") (param $size i32) (result i32)
            (local $ptr i32)
            global.get $heap
            local.tee $ptr
            local.get $size
            i32.add
            global.set $heap
            local.get $ptr)
          (func (export "dealloc") (param i32) (param i32))
          (func (export "rustcdc_abi_version") (result i32) i32.const 2)
          (func $copy_bytes (param $dst i32) (param $src i32) (param $n i32)
            (local $i i32)
            i32.const 0
            local.set $i
            (block $brk
              (loop $lp
                local.get $i
                local.get $n
                i32.ge_u
                br_if $brk
                local.get $dst
                local.get $i
                i32.add
                local.get $src
                local.get $i
                i32.add
                i32.load8_u
                i32.store8
                local.get $i
                i32.const 1
                i32.add
                local.set $i
                br $lp)))
          (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
            i32.const 65536
            local.get $ptr
            local.get $len
            call $copy_bytes
            i32.const 65536
            i64.extend_i32_u
            i64.const 32
            i64.shl
            local.get $len
            i64.extend_i32_u
            i64.or))"#
    )
}

fn drop_wat() -> &'static str {
    r#"(module
          (memory (export "memory") 1 1)
          (global $heap (mut i32) (i32.const 8))
          (func (export "alloc") (param $size i32) (result i32)
            (local $ptr i32)
            global.get $heap
            local.tee $ptr
            local.get $size
            i32.add
            global.set $heap
            local.get $ptr)
          (func (export "dealloc") (param i32) (param i32))
          (func (export "rustcdc_abi_version") (result i32) i32.const 2)
          (func (export "transform") (param i32) (param i32) (result i64)
            i64.const 0))"#
}

fn wasm_cfg(path: PathBuf) -> TransformRuntimeConfig {
    TransformRuntimeConfig {
        mode: TransformRuntimeMode::Wasm,
        wasm: WasmTransformConfig {
            module_path: Some(path),
            max_memory_bytes: 256 * 1024,
            max_event_bytes: 64 * 1024,
            instance_pool_size: 2,
            ..WasmTransformConfig::default()
        },
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pass-through
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wasm_passthrough_preserves_all_event_fields() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = compile_wasm(&dir, "passthrough.wasm", &passthrough_wat(2));

    let pipeline =
        TransformPipeline::from_config(wasm_cfg(path), vec![]).expect("pipeline from config");

    let event = sample_event();
    let output = pipeline
        .apply(event.clone())
        .await
        .expect("apply")
        .expect("event not dropped");

    assert_eq!(output.table, event.table);
    assert_eq!(output.schema, event.schema);
    assert_eq!(output.ts, event.ts);
    assert_eq!(output.after, event.after);
    assert_eq!(output.op, event.op);
}

// ─────────────────────────────────────────────────────────────────────────────
// Event drop (return 0)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wasm_drop_transform_returns_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = compile_wasm(&dir, "drop.wasm", drop_wat());

    let pipeline = TransformPipeline::from_config(wasm_cfg(path), vec![]).expect("pipeline");

    let result = pipeline.apply(sample_event()).await.expect("apply");
    assert!(result.is_none(), "drop transform must return None");
}

// ─────────────────────────────────────────────────────────────────────────────
// Field mutation (table rename via JSON manipulation in WASM)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wasm_transform_can_mutate_event_table_name() {
    // This test uses a Rust-like approach: compile a WAT module that reads the
    // incoming JSON, replaces the table field value, and writes new JSON.
    // Rather than implementing a full JSON parser in WAT (which would be
    // hundreds of lines), we pre-write a known replacement JSON into the module
    // data section and return it unconditionally — the test verifies that a
    // WASM module can return _different_ content from its input.
    let dir = tempfile::tempdir().expect("tempdir");

    // Build a replacement event with the table renamed to "renamed_orders".
    let replacement_event = Event {
        table: "renamed_orders".to_string(),
        ..sample_event()
    };
    let replacement_json = serde_json::to_vec(&replacement_event).expect("serialize");
    let json_len = replacement_json.len();

    // Encode as a WAT data segment that we load into memory at offset 65536.
    // The transform function ignores the input and returns this fixed payload.
    let data_hex: String = replacement_json
        .iter()
        .map(|b| format!("\\{b:02x}"))
        .collect();

    let wat_src = format!(
        r#"(module
          (memory (export "memory") 2 2)
          (data (i32.const 65536) "{data_hex}")
          (global $heap (mut i32) (i32.const 8))
          (func (export "alloc") (param $size i32) (result i32)
            (local $ptr i32)
            global.get $heap
            local.tee $ptr
            local.get $size
            i32.add
            global.set $heap
            local.get $ptr)
          (func (export "dealloc") (param i32) (param i32))
          (func (export "rustcdc_abi_version") (result i32) i32.const 2)
          (func (export "transform") (param i32) (param i32) (result i64)
            i32.const 65536
            i64.extend_i32_u
            i64.const 32
            i64.shl
            i64.const {json_len}
            i64.or))"#
    );

    let path = compile_wasm(&dir, "mutate.wasm", &wat_src);

    let pipeline = TransformPipeline::from_config(wasm_cfg(path), vec![]).expect("pipeline");

    let output = pipeline
        .apply(sample_event())
        .await
        .expect("apply")
        .expect("event not dropped");

    assert_eq!(
        output.table, "renamed_orders",
        "WASM transform must be able to rename the event table"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// ABI validation — rejects modules without alloc/dealloc
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wasm_pipeline_rejects_module_missing_alloc_export() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = compile_wasm(
        &dir,
        "no_alloc.wasm",
        r#"(module
          (memory (export "memory") 1 1)
          (func (export "transform") (param i32) (param i32) (result i64)
            i64.const 0))"#,
    );

    let err = TransformPipeline::from_config(wasm_cfg(path), vec![])
        .expect_err("must reject missing alloc");
    assert!(
        err.to_string().contains("alloc"),
        "error must mention missing 'alloc': {err}"
    );
}

#[tokio::test]
async fn wasm_pipeline_rejects_module_missing_dealloc_export() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = compile_wasm(
        &dir,
        "no_dealloc.wasm",
        r#"(module
          (memory (export "memory") 1 1)
          (func (export "alloc") (param i32) (result i32) i32.const 8)
          (func (export "transform") (param i32) (param i32) (result i64)
            i64.const 0))"#,
    );

    let err = TransformPipeline::from_config(wasm_cfg(path), vec![])
        .expect_err("must reject missing dealloc");
    assert!(
        err.to_string().contains("dealloc"),
        "error must mention missing 'dealloc': {err}"
    );
}

#[tokio::test]
async fn wasm_pipeline_rejects_module_missing_rustcdc_abi_version() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = compile_wasm(
        &dir,
        "no_abi_version.wasm",
        r#"(module
          (memory (export "memory") 1 1)
          (global $heap (mut i32) (i32.const 8))
          (func (export "alloc") (param $size i32) (result i32)
            (local $ptr i32)
            global.get $heap
            local.tee $ptr
            local.get $size
            i32.add
            global.set $heap
            local.get $ptr)
          (func (export "dealloc") (param i32) (param i32))
          (func (export "transform") (param i32) (param i32) (result i64)
            i64.const 0))"#,
    );

    let err = TransformPipeline::from_config(wasm_cfg(path), vec![])
        .expect_err("must reject missing rustcdc_abi_version");
    assert!(
        err.to_string().contains("rustcdc_abi_version"),
        "error must mention missing 'rustcdc_abi_version': {err}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// ABI version mismatch rejection
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wasm_pipeline_rejects_wrong_abi_version() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Report ABI version 99 — host requires version 2.
    let path = compile_wasm(
        &dir,
        "wrong_abi.wasm",
        r#"(module
          (memory (export "memory") 1 1)
          (global $heap (mut i32) (i32.const 8))
          (func (export "alloc") (param $size i32) (result i32)
            (local $ptr i32)
            global.get $heap
            local.tee $ptr
            local.get $size
            i32.add
            global.set $heap
            local.get $ptr)
          (func (export "dealloc") (param i32) (param i32))
          (func (export "rustcdc_abi_version") (result i32) i32.const 99)
          (func (export "transform") (param i32) (param i32) (result i64) i64.const 0))"#,
    );

    let err = TransformPipeline::from_config(wasm_cfg(path), vec![])
        .expect_err("must reject ABI version mismatch");
    assert!(
        err.to_string().contains("ABI version"),
        "error must mention ABI version: {err}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Concurrent pool invocations
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wasm_pool_handles_concurrent_invocations() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = compile_wasm(&dir, "pool.wasm", &passthrough_wat(2));

    let cfg = TransformRuntimeConfig {
        mode: TransformRuntimeMode::Wasm,
        wasm: WasmTransformConfig {
            module_path: Some(path),
            max_memory_bytes: 256 * 1024,
            max_event_bytes: 64 * 1024,
            instance_pool_size: 4,
            ..WasmTransformConfig::default()
        },
    };

    let pipeline =
        std::sync::Arc::new(TransformPipeline::from_config(cfg, vec![]).expect("pipeline"));

    let handles: Vec<_> = (0..20)
        .map(|i| {
            let p = std::sync::Arc::clone(&pipeline);
            // ts must stay non-zero: the pipeline validates the envelope after
            // every WASM transform, and `ts == 0` violates the event contract.
            let mut event = sample_event();
            event.ts = i + 1;
            tokio::spawn(async move { p.apply(event).await })
        })
        .collect();

    for handle in handles {
        let result = handle.await.expect("task panicked");
        let event = result.expect("apply error").expect("event dropped");
        assert_eq!(event.table, "orders");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Native pipeline with transform rules
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn native_pipeline_filter_rule_drops_delete_events() {
    use rustcdc_server::config::schema::{TransformActionConfig, TransformWhenConfig};

    // Rule: for ALL events, keep only inserts/updates — drops deletes.
    let rules = vec![TransformRuleConfig {
        name: "drop_deletes".to_string(),
        when: TransformWhenConfig::default(), // matches all events
        actions: vec![TransformActionConfig::Filter {
            include_tables: vec![],                                        // all tables
            include_schemas: vec![],                                       // all schemas
            include_ops: vec!["insert".to_string(), "update".to_string()], // only keep these
        }],
    }];

    let cfg = TransformRuntimeConfig {
        mode: TransformRuntimeMode::Native,
        wasm: WasmTransformConfig::default(),
    };

    let pipeline = TransformPipeline::from_config(cfg, rules).expect("pipeline");

    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    // Delete event must be dropped (not in include_ops).
    let mut delete_event = sample_event();
    delete_event.op = Operation::Delete;
    let dropped = rt.block_on(pipeline.apply(delete_event)).expect("apply");
    assert!(dropped.is_none(), "filter rule must drop delete event");

    // Insert event must pass through.
    let kept = rt
        .block_on(pipeline.apply(sample_event()))
        .expect("apply")
        .expect("insert should be kept");
    assert_eq!(kept.table, "orders");
}
