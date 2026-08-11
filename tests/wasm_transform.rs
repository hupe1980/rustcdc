//! WASM transform integration tests.
//!
//! These tests exercise the full WASM transform pipeline from WAT source
//! through the ABI v2 contract: alloc/dealloc, the `rustcdc_abi_version()` probe,
//! pass-through, event-drop, field mutation, and rejection of invalid ABI
//! modules.  The `wat` crate compiles WAT directly to WASM bytes at test time
//! so no pre-built `.wasm` artifacts are needed.

use std::path::PathBuf;

use rustcdc::core::{Event, Operation, SourceMetadata};
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
    Event::builder("orders", Operation::Insert)
        .after(json!({
            "id": 1,
            "name": "alice",
            "amount": 99.50
        }))
        .source(SourceMetadata::new("postgres", "0/1", 1_000_000))
        .ts(1_000_001)
        .schema("public")
        .primary_key(["id"])
        .build()
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

/// A module carrying a **data segment** must load.
///
/// The regression guard for a defect that made the entire WASM feature unusable: wasmtime
/// evaluates the store's epoch deadline while initialising data segments, a fresh `Store`
/// starts at deadline `0` — the engine's starting epoch — so arming the deadline after
/// `linker.instantiate(..)` rejects every module carrying a data segment with
/// `wasm trap: interrupt`. That is every module a real Rust, AssemblyScript or TinyGo build
/// produces, since string literals and rodata land there.
///
/// A data-segment-free WAT fixture suite stays green while nothing real can load, so this
/// module embeds the replacement event as a data segment and covers the class by
/// construction.
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
    let mut replacement_event = sample_event();
    replacement_event.table = "renamed_orders".to_string();
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

/// The instance **pool** must also accept data-segment modules.
///
/// The 0.8 defect had two sites: the ABI probe and `create_instance_state`, which runs
/// once per pool slot. A single-instance test would have passed against a fix applied
/// only to the probe, so this drives several slots concurrently.
#[tokio::test]
async fn wasm_pool_slots_accept_a_data_segment_module() {
    let dir = tempfile::tempdir().expect("tempdir");
    let module_path = compile_wasm(
        &dir,
        "pool_data.wasm",
        r#"
        (module
          (memory (export "memory") 2 2)
          (data (i32.const 1024) "rodata-like-any-real-toolchain-emits")
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
          (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
            i32.const 65536
            local.get $ptr
            local.get $len
            memory.copy
            i32.const 65536
            i64.extend_i32_u
            i64.const 32
            i64.shl
            local.get $len
            i64.extend_i32_u
            i64.or))
        "#,
    );

    let mut cfg = wasm_cfg(module_path);
    cfg.wasm.instance_pool_size = 4;
    let pipeline = TransformPipeline::from_config(cfg, vec![]).expect("pool must instantiate");

    for i in 0..20u64 {
        let mut event = sample_event();
        event.ts += i;
        let out = pipeline
            .apply(event.clone())
            .await
            .expect("apply")
            .expect("kept");
        assert_eq!(out.after, event.after);
    }
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

/// **The pool must actually run transforms in parallel.**
///
/// `wasm.instance_pool_size` allocated N wasmtime instances while a single mutex around
/// `WasmRuntime::transform` ensured exactly one could ever run — so the knob, and
/// `runtime.prepare_parallelism` with it, did nothing at all in WASM mode. An operator
/// tuning either measured no change.
///
/// The module below busy-spins for a fixed number of iterations, giving each transform
/// a floor on its duration. If the pipeline were still serialised, N concurrent
/// transforms would take N × that floor. Asserting wall-clock is the only way to
/// distinguish real concurrency from a pool that merely exists — a test that just
/// checked the outputs would pass equally well against the serialised version.
///
/// **`multi_thread` is required.** `#[tokio::test]` defaults to a current-thread
/// runtime, where spawned tasks interleave but CPU-bound guest execution cannot
/// overlap — the test would fail against a perfectly good pool. Production runs on a
/// multi-threaded runtime, so this matches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_wasm_pool_transforms_concurrently() {
    const POOL: usize = 4;

    let dir = tempfile::tempdir().expect("tempdir");
    let module_path = compile_wasm(
        &dir,
        "spin.wasm",
        r#"
        (module
          (memory (export "memory") 2 2)
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
          ;; The spin loop must do work the optimiser cannot remove.
          ;;
          ;; It used to be a bare counter with no side effect, which Cranelift deleted
          ;; outright — so each transform took well under a millisecond and there was
          ;; essentially nothing to run in parallel.
          ;;
          ;; Each iteration now loads a byte from the *input* buffer at a computed
          ;; address and accumulates it, and the total is stored to scratch memory before
          ;; returning. A load from a runtime-computed pointer cannot be folded to a
          ;; constant and the store cannot be dropped, so the loop survives optimisation
          ;; and the guest genuinely occupies a core long enough for `POOL` concurrent
          ;; invocations to overlap.
          (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
            (local $i i32)
            (local $acc i32)
            i32.const 0
            local.set $i
            i32.const 0
            local.set $acc
            (block $done
              (loop $spin
                local.get $i
                i32.const 40000000
                i32.ge_s
                br_if $done
                ;; acc += input[ptr + (i & 7)]
                local.get $acc
                local.get $ptr
                local.get $i
                i32.const 7
                i32.and
                i32.add
                i32.load8_u
                i32.add
                local.set $acc
                local.get $i
                i32.const 1
                i32.add
                local.set $i
                br $spin))
            ;; Offsets 0..8 are scratch: `alloc` hands out from 8 upwards and the result
            ;; is written at 65536, so this cannot corrupt either.
            i32.const 0
            local.get $acc
            i32.store
            i32.const 65536
            local.get $ptr
            local.get $len
            memory.copy
            i32.const 65536
            i64.extend_i32_u
            i64.const 32
            i64.shl
            local.get $len
            i64.extend_i32_u
            i64.or))
        "#,
    );

    let mut cfg = wasm_cfg(module_path);
    cfg.wasm.instance_pool_size = POOL;
    cfg.wasm.timeout_ms = 30_000;
    let pipeline =
        std::sync::Arc::new(TransformPipeline::from_config(cfg, vec![]).expect("pool builds"));

    // Warm every slot first. Each `WasmRuntime` initialises its guest lazily on first
    // transform, and counting that one-off cost inside the measurement would understate
    // the concurrency it is trying to detect.
    for _ in 0..POOL {
        pipeline
            .apply(sample_event())
            .await
            .expect("warm-up transform")
            .expect("event survives");
    }

    // Run `POOL` transforms concurrently, then ask the pool how many slots were ever
    // busy at the same moment.
    //
    // This used to compare wall-clock time for `POOL` concurrent transforms against a
    // per-event floor measured on its own. That measures the right property and measures
    // it unreliably: under a full `cargo test` the four worker threads contend with every
    // other test binary, so the comparison fails against a pool that is working
    // perfectly. The high-water mark is the same property observed directly, and a busy
    // machine cannot turn it into a false negative.
    let mut handles = Vec::with_capacity(POOL);
    for i in 0..POOL {
        let pipeline = std::sync::Arc::clone(&pipeline);
        handles.push(tokio::spawn(async move {
            let mut event = sample_event();
            event.ts += i as u64;
            pipeline.apply(event).await.expect("transform").is_some()
        }));
    }
    for handle in handles {
        assert!(handle.await.expect("join"), "every event must survive");
    }

    let peak = pipeline
        .wasm_pool_peak_in_use()
        .await
        .expect("a wasm pipeline reports a pool high-water mark");
    assert!(
        peak > 1,
        "the pool never had more than {peak} slot busy at once across {POOL} concurrent \
         transforms, so it is running one guest at a time. An ideal {POOL}-slot pool \
         reaches {POOL}; anything above 1 is what proves the slots are genuinely \
         schedulable in parallel."
    );
}
