//! `Transform`-trait adapter for `WasmRuntime`.

use tokio::sync::Mutex;

use crate::{
    core::{Event, Result},
    transform::Transform,
    wasm::{TransformResult, WasmConfig, WasmRuntime},
};

/// A `Transform` implementation that delegates to an embedded WASM module.
///
/// # Example
///
/// ```rust,ignore
/// use rustcdc::wasm::{WasmConfig, WasmTransform};
/// use rustcdc::transform::{BoxTransform, TransformPipeline};
///
/// let transform = WasmTransform::new(WasmConfig {
///     module_path: "my_transform.wasm".into(),
///     timeout_ms: 10,
///     memory_limit_mb: 16,
/// }).await?;
/// let mut pipeline = TransformPipeline::default();
/// pipeline.add_transform(BoxTransform::new(transform));
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

impl Transform for WasmTransform {
    async fn apply<'a>(&'a self, event: &'a mut Event) -> Result<bool> {
        let mut guard = self.runtime.lock().await;
        match guard.transform(event).await? {
            TransformResult::Ok(transformed) => {
                *event = *transformed;
                Ok(true)
            }
            TransformResult::Err(_reason) => Ok(false),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}
