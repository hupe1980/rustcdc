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
                    kept.push(event);
                }
                TransformResult::Filtered => {}
            }
        }
        *events = kept;
        Ok(())
    }
}
