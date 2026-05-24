//! Transform pipeline building blocks.

use async_trait::async_trait;

use crate::core::{Error, Event, Result};

pub mod filter_projection;
pub mod mask_hash;
#[cfg(feature = "outbox")]
pub mod outbox;
pub mod route;
pub mod unwrap;

pub use filter_projection::{FilterProjectionConfig, FilterProjectionTransform};
pub use mask_hash::{MaskHashConfig, MaskHashTransform, MaskRule};
#[cfg(feature = "outbox")]
pub use outbox::{OutboxResult, OutboxTransform};
pub use route::{RouteConfig, RouteTransform};
pub use unwrap::{UnwrapConfig, UnwrapTransform};

#[async_trait]
pub trait Transform: Send + Sync {
    /// Apply transform in-place; return true to keep event, false to drop it.
    async fn apply(&self, event: &mut Event) -> Result<bool>;
    fn name(&self) -> &str;
}

#[derive(Default)]
pub struct TransformPipeline {
    transforms: Vec<Box<dyn Transform>>,
}

impl TransformPipeline {
    pub fn add_transform(&mut self, transform: Box<dyn Transform>) {
        self.transforms.push(transform);
    }

    pub async fn apply(&self, mut event: Event) -> Result<Option<Event>> {
        for transform in &self.transforms {
            let keep = transform
                .apply(&mut event)
                .await
                .map_err(|error| Error::TransformError(format!("{}: {error}", transform.name())))?;
            if !keep {
                return Ok(None);
            }
        }
        Ok(Some(event))
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::json;

    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};

    use super::{Transform, TransformPipeline};

    struct AppendSuffix;

    struct DropEvent;

    struct FailTransform;

    #[async_trait]
    impl Transform for AppendSuffix {
        async fn apply(&self, event: &mut Event) -> crate::core::Result<bool> {
            if let Some(serde_json::Value::Object(after)) = &mut event.after {
                after.insert("suffix".into(), json!("ok"));
            }
            Ok(true)
        }

        fn name(&self) -> &str {
            "append_suffix"
        }
    }

    #[async_trait]
    impl Transform for DropEvent {
        async fn apply(&self, _event: &mut Event) -> crate::core::Result<bool> {
            Ok(false)
        }

        fn name(&self) -> &str {
            "drop_event"
        }
    }

    #[async_trait]
    impl Transform for FailTransform {
        async fn apply(&self, _event: &mut Event) -> crate::core::Result<bool> {
            Err(crate::core::Error::ConfigError("boom".into()))
        }

        fn name(&self) -> &str {
            "fail_transform"
        }
    }

    fn event() -> Event {
        Event {
            before: None,
            after: Some(json!({"id": 1})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "1".into(),
                timestamp: 1,
            },
            ts: 1,
            schema: None,
            table: "items".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    #[tokio::test]
    async fn pipeline_applies_transforms_in_order() {
        let mut pipeline = TransformPipeline::default();
        pipeline.add_transform(Box::new(AppendSuffix));
        let output = pipeline.apply(event()).await.unwrap().unwrap();
        assert_eq!(output.after.unwrap()["suffix"], "ok");
    }

    #[tokio::test]
    async fn pipeline_stops_when_transform_filters_event() {
        let mut pipeline = TransformPipeline::default();
        pipeline.add_transform(Box::new(DropEvent));
        pipeline.add_transform(Box::new(AppendSuffix));

        assert!(pipeline.apply(event()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pipeline_wraps_transform_errors_with_context() {
        let mut pipeline = TransformPipeline::default();
        pipeline.add_transform(Box::new(FailTransform));

        let error = pipeline.apply(event()).await.unwrap_err();
        assert!(
            matches!(error, crate::core::Error::TransformError(message) if message.contains("fail_transform"))
        );
    }

    #[tokio::test]
    async fn empty_pipeline_returns_input_event() {
        let pipeline = TransformPipeline::default();
        let output = pipeline.apply(event()).await.unwrap().unwrap();
        assert_eq!(output.table, "items");
    }
}
