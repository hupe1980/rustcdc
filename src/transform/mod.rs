//! Transform pipeline building blocks.

use async_trait::async_trait;

use crate::core::{Error, Event, Result};

pub mod field_mapping;
pub mod filter_projection;
pub mod mask_hash;
#[cfg(feature = "outbox")]
pub mod outbox;
pub mod route;
pub mod unwrap;

pub use field_mapping::{FieldMappingConfig, FieldMappingTransform};
pub use filter_projection::{
    FilterField, FilterMode, FilterOperator, FilterProjectionConfig, FilterProjectionTransform,
    FilterRule,
};
pub use mask_hash::{MaskHashConfig, MaskHashTransform, MaskRule};
#[cfg(feature = "outbox")]
pub use outbox::{OutboxResult, OutboxTransform};
pub use route::{RouteConfig, RouteTransform};
pub use unwrap::{UnwrapConfig, UnwrapTransform};

#[async_trait]
pub trait Transform: Send + Sync + std::fmt::Debug {
    /// Apply transform in-place; return true to keep event, false to drop it.
    async fn apply(&self, event: &mut Event) -> Result<bool>;
    fn name(&self) -> &str;
}

#[derive(Default)]
pub struct TransformPipeline {
    transforms: Vec<Box<dyn Transform>>,
}

impl std::fmt::Debug for TransformPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.transforms.iter().map(|t| t.name()).collect();
        f.debug_struct("TransformPipeline")
            .field("transforms", &names)
            .finish()
    }
}

impl TransformPipeline {
    /// Add a transform to the end of the pipeline (mutating form).
    pub fn add_transform(&mut self, transform: Box<dyn Transform>) {
        self.transforms.push(transform);
    }

    /// Add a transform to the end of the pipeline (fluent builder form).
    ///
    /// ```ignore
    /// let pipeline = TransformPipeline::default()
    ///     .with(MaskHashTransform::new(config))
    ///     .with(RouteTransform::new(route_config).unwrap());
    /// ```
    #[must_use]
    pub fn with<T: Transform + 'static>(mut self, transform: T) -> Self {
        self.transforms.push(Box::new(transform));
        self
    }

    /// Number of transforms in the pipeline.
    pub fn len(&self) -> usize {
        self.transforms.len()
    }

    /// Returns `true` when the pipeline contains no transforms.
    pub fn is_empty(&self) -> bool {
        self.transforms.is_empty()
    }

    pub async fn apply(&self, mut event: Event) -> Result<Option<Event>> {
        // Whether the event had a resolvable message key before any transform ran.
        // Only meaningful for data-change events; READ/SCHEMA_CHANGE/TRUNCATE carry no
        // row identity to preserve.
        let key_before = event.op.is_data_change() && event.primary_key_values().is_some();

        for transform in &self.transforms {
            let keep = transform
                .apply(&mut event)
                .await
                .map_err(|error| Error::TransformError(format!("{}: {error}", transform.name())))?;
            if !keep {
                return Ok(None);
            }

            // A transform must not silently destroy the message key.
            //
            // `Event::primary_key` names the key *columns*; the key *values* are read
            // out of the row payload. So any transform that removes, renames, or
            // rewrites a primary-key column detaches the two — `primary_key_values()`
            // starts returning `None`, `encode_key` yields `None`, and the record is
            // emitted **unkeyed** with no error and no warning. Downstream, log
            // compaction stops collapsing the row and upsert consumers start inserting
            // duplicates, long after the config change that caused it.
            //
            // The realistic ways to hit this are all ordinary-looking config:
            //   * `FilterProjectionConfig::include_columns` that omits the PK (the
            //     existing guard only fires when the row becomes *empty*),
            //   * `FieldMappingTransform` renaming a PK column,
            //   * `MaskRule::Encrypt` on a PK column — a fresh nonce per call gives
            //     every event for the same row a different key.
            if key_before && event.primary_key_values().is_none() {
                return Err(Error::TransformError(format!(
                    "{}: transform removed the event's primary-key values for table '{}'. \
                     The event had a resolvable key before this transform and does not \
                     after it, so it would be emitted unkeyed — breaking log compaction \
                     and upsert consumers downstream. Primary key columns are {:?}. \
                     Check whether this transform projects away, renames, or rewrites a \
                     key column; exclude the key columns from it, or clear \
                     `Event::primary_key` deliberately if the events are genuinely \
                     keyless.",
                    transform.name(),
                    event.qualified_table_name(),
                    event.primary_key.as_deref().unwrap_or(&[]),
                )));
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

    #[derive(Debug)]
    struct AppendSuffix;

    #[derive(Debug)]
    struct DropEvent;

    #[derive(Debug)]
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
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
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
