//! [`AsyncTransform`]-trait adapter for `WasmRuntime`.
//!
//! WASM is the one stage in the crate that genuinely must `await`: guest execution runs
//! behind an async mutex. Everything rustcdc ships otherwise implements the synchronous
//! [`crate::transform::Transform`], which avoids a boxed future per event.

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{
    core::{Event, Result},
    transform::AsyncTransform,
    wasm::{TransformResult, WasmConfig, WasmRuntime},
};

/// A `Transform` implementation that delegates to an embedded WASM module.
///
/// # Example
///
/// ```rust,ignore
/// use rustcdc::wasm::{WasmConfig, WasmTransform};
/// use rustcdc::transform::TransformPipeline;
///
/// let transform = WasmTransform::new(WasmConfig {
///     module_path: "my_transform.wasm".into(),
///     timeout_ms: 10,
///     memory_limit_mb: 16,
///     instance_pool_size: 1,
///     fuel_async_yield_interval: None,
/// }).await?;
/// let mut pipeline = TransformPipeline::default();
/// pipeline.add_transform(Box::new(transform));
/// ```
pub struct WasmTransform {
    runtime: Mutex<WasmRuntime>,
    /// Human-readable name derived from the module path, used in transform
    /// error messages and tracing spans.
    name: String,
}

impl WasmTransform {
    /// Create and initialise a new `WasmTransform` from the given config.
    pub async fn new(config: WasmConfig) -> Result<Self> {
        let name = config
            .module_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("wasm")
            .to_string();
        let mut rt = WasmRuntime::new_with_config(config)?;
        rt.init().await?;
        Ok(Self {
            runtime: Mutex::new(rt),
            name,
        })
    }
}

impl std::fmt::Debug for WasmTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmTransform")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AsyncTransform for WasmTransform {
    async fn apply(&self, event: &mut Event) -> Result<bool> {
        let mut guard = self.runtime.lock().await;
        match guard.transform(event).await? {
            TransformResult::Ok(transformed) => {
                *event = *transformed;
                // A module can rewrite any value under any name; no name comparison sees it.
                event.shape = None;
                Ok(true)
            }
            TransformResult::Filtered => Ok(false),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    /// Acquire the runtime lock **once for the batch** rather than once per event.
    ///
    /// The default implementation would re-take the async mutex for every event. That
    /// mutex serialises all callers for the duration of guest execution, so re-taking it
    /// per event multiplies the contention by the batch size for no benefit — the guest
    /// is single-threaded either way.
    async fn apply_batch(&self, events: &mut Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut guard = self.runtime.lock().await;
        let mut kept = Vec::with_capacity(events.len());
        for mut event in std::mem::take(events) {
            match guard.transform(&event).await? {
                TransformResult::Ok(transformed) => {
                    event = *transformed;
                    event.shape = None;
                    kept.push(event);
                }
                TransformResult::Filtered => {}
            }
        }
        *events = kept;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::core::{Operation, RowShape, ShapeColumn, SourceMetadata};

    /// Returns the event JSON it was given, byte for byte: column names unchanged, so only
    /// the adapter's own removal can drop the shape.
    const PASS_THROUGH_WAT: &str = r#"
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
    "#;

    async fn pass_through() -> (tempfile::TempDir, WasmTransform) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pass_through.wasm");
        std::fs::write(&path, wat::parse_str(PASS_THROUGH_WAT).unwrap()).unwrap();
        let transform = WasmTransform::new(WasmConfig {
            module_path: path,
            ..WasmConfig::default()
        })
        .await
        .unwrap();
        (dir, transform)
    }

    fn shaped(offset: &str) -> Event {
        let shape = RowShape::new(
            "app",
            "items",
            vec![ShapeColumn::new("id", "bigint", false)],
            vec!["id".into()],
        )
        .unwrap();
        Event::builder("items", Operation::Insert)
            .schema("app")
            .after(json!({"id": 1}))
            .primary_key(["id"])
            .source(SourceMetadata::new("test", offset, 1))
            .shape(Arc::new(shape))
            .build()
    }

    #[tokio::test]
    async fn a_module_s_output_carries_no_shape() {
        let (_dir, transform) = pass_through().await;
        let mut event = shaped("1");
        assert!(transform.apply(&mut event).await.unwrap());
        assert_eq!(
            event.after,
            Some(json!({"id": 1})),
            "the module passed the row through"
        );
        assert!(
            event.shape.is_none(),
            "a module's output is not the table's row"
        );
    }

    #[tokio::test]
    async fn a_module_s_batch_output_carries_no_shape() {
        let (_dir, transform) = pass_through().await;
        let mut events = vec![shaped("1"), shaped("2")];
        transform.apply_batch(&mut events).await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| event.shape.is_none()));
    }
}
