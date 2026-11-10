//! Row shapes at delivery: the `runtime.row_shape` mode, and the shapes `GET /schemas/{id}`
//! resolves.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rustcdc::RowShape;

use crate::config::schema::RowShapeDelivery;

/// Every complete shape this process has delivered, by id.
///
/// *Stated limit:* in memory, since the process started. An id delivered by an earlier run
/// or another replica is unknown here; this is not a durable registry.
#[derive(Debug, Clone, Default)]
pub struct DeliveredShapes {
    shapes: Arc<RwLock<HashMap<String, Arc<RowShape>>>>,
}

impl DeliveredShapes {
    pub fn get(&self, id: &str) -> Option<Arc<RowShape>> {
        self.shapes
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    fn record(&self, shape: &Arc<RowShape>) {
        if self.get(shape.id()).is_some() {
            return;
        }
        self.shapes
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(shape.id().to_string())
            .or_insert_with(|| Arc::clone(shape));
    }
}

/// Applies the delivery mode to each event and records what was delivered.
#[derive(Debug, Clone, Default)]
pub struct RowShapeDeliveryPlan {
    mode: RowShapeDelivery,
    delivered: DeliveredShapes,
}

impl RowShapeDeliveryPlan {
    pub fn new(mode: RowShapeDelivery, delivered: DeliveredShapes) -> Self {
        Self { mode, delivered }
    }

    /// Apply the mode; returns the complete shape to record once the event is delivered.
    pub fn prepare(&self, event: &mut rustcdc::Event) -> Option<Arc<RowShape>> {
        let shape = event.shape.clone().filter(|shape| !shape.is_compact())?;
        // A narrowed shape is in no source's history, so an id alone would leave a consumer
        // whose lookup misses (another replica, a restart) nothing to fall back on.
        if self.mode == RowShapeDelivery::Id && !shape.is_narrowed() {
            event.shape = Some(Arc::new(shape.compact()));
        }
        Some(shape)
    }

    /// Record a shape after the event carrying it was handed to the sink.
    pub fn delivered(&self, shape: &Arc<RowShape>) {
        self.delivered.record(shape);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustcdc::{Event, Operation, ShapeColumn, SourceMetadata};

    fn shape() -> Arc<RowShape> {
        Arc::new(
            RowShape::new(
                "public",
                "users",
                vec![
                    ShapeColumn::new("id", "integer", false),
                    ShapeColumn::new("email", "text", true),
                ],
                vec!["id".into()],
            )
            .unwrap(),
        )
    }

    fn event(shape: Arc<RowShape>) -> Event {
        Event::builder("users", Operation::Insert)
            .source(SourceMetadata::new("postgres", "0/1", 1))
            .after(serde_json::json!({"id": "1", "email": "a@example.com"}))
            .shape(shape)
            .ts(1)
            .build()
    }

    #[test]
    fn compact_mode_keeps_only_the_id() {
        let plan = RowShapeDeliveryPlan::new(RowShapeDelivery::Id, DeliveredShapes::default());
        let mut row = event(shape());
        let full = plan.prepare(&mut row).expect("a complete shape to record");
        let delivered = row.shape.as_deref().unwrap();
        assert!(delivered.is_compact());
        assert_eq!(delivered.id(), full.id());

        let inline =
            RowShapeDeliveryPlan::new(RowShapeDelivery::Inline, DeliveredShapes::default());
        let mut row = event(shape());
        inline.prepare(&mut row);
        assert!(!row.shape.unwrap().is_compact(), "inline is the default");
    }

    #[tokio::test]
    async fn compact_mode_keeps_a_narrowed_shape_inline() {
        // A projection is what narrows a shape; the library's guard does it.
        let pipeline = rustcdc::TransformPipeline::default().with(
            rustcdc::FilterProjectionTransform::new(rustcdc::FilterProjectionConfig {
                include_columns: Some(vec!["id".into()]),
                ..rustcdc::FilterProjectionConfig::default()
            })
            .unwrap(),
        );
        let mut row = pipeline.apply(event(shape())).await.unwrap().unwrap();
        assert!(row.shape.as_ref().unwrap().is_narrowed());

        let plan = RowShapeDeliveryPlan::new(RowShapeDelivery::Id, DeliveredShapes::default());
        plan.prepare(&mut row);
        assert!(!row.shape.unwrap().is_compact());
    }

    #[test]
    fn only_delivered_shapes_resolve() {
        let delivered = DeliveredShapes::default();
        let plan = RowShapeDeliveryPlan::new(RowShapeDelivery::Id, delivered.clone());
        let mut row = event(shape());
        let full = plan.prepare(&mut row).unwrap();
        assert!(
            delivered.get(full.id()).is_none(),
            "prepared is not delivered"
        );
        plan.delivered(&full);
        assert_eq!(delivered.get(full.id()).as_deref(), Some(full.as_ref()));
    }
}
