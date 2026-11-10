//! Keeping [`Event::shape`] true across a transform chain.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use crate::core::Event;

/// The columns a row had on the way into a transform chain, checked again on the way out.
///
/// A consumer that trusts [`Event::shape`] parses the row's values with the declared types it
/// lists, so a shape that no longer describes the row is worse than none. Comparing the
/// column names of `before` and `after` (and the unavailable-column lists) on entry and exit:
///
/// * **unchanged** — the shape is kept;
/// * **only removed** (a projection) — the shape is narrowed to the remaining columns, with a
///   new id. A removed primary-key column removes the shape instead;
/// * **anything added, or an image appearing or disappearing** (a rename, an unwrap, a
///   metadata projection) — the shape is removed.
///
/// A stage that rewrites values under unchanged names cannot be seen from names; such stages
/// remove the shape themselves (masking and hashing, a field mapping that renames, copies or
/// sets, the outbox router, and the server's unwrap/flatten actions and WASM modules).
///
/// One type rather than a check per stage, so a stage added later cannot forget; the server's
/// own chain uses it too.
#[derive(Debug)]
pub struct ShapeGuard {
    /// `None` when the event arrived without a shape: there is nothing to keep true.
    entry: Option<Columns>,
}

#[derive(Debug, PartialEq)]
struct Columns {
    before: Option<BTreeSet<String>>,
    after: Option<BTreeSet<String>>,
    unavailable: BTreeSet<String>,
}

impl Columns {
    fn of(event: &Event) -> Self {
        Self {
            before: object_keys(event.before.row()),
            after: object_keys(event.after.as_ref()),
            unavailable: event
                .unavailable_columns
                .iter()
                .chain(event.before.unavailable_columns())
                .cloned()
                .collect(),
        }
    }
}

fn object_keys(value: Option<&serde_json::Value>) -> Option<BTreeSet<String>> {
    match value? {
        serde_json::Value::Object(object) => Some(object.keys().cloned().collect()),
        _ => None,
    }
}

/// What the chain did to the column set.
enum Change {
    None,
    Dropped(HashSet<String>),
    Reshaped,
}

fn compare(entry: &Columns, exit: &Columns) -> Change {
    let mut dropped = HashSet::new();
    for (old, new) in [(&entry.before, &exit.before), (&entry.after, &exit.after)] {
        match (old, new) {
            (None, None) => {}
            (Some(old), Some(new)) => {
                if new.difference(old).next().is_some() {
                    return Change::Reshaped;
                }
                dropped.extend(old.difference(new).cloned());
            }
            _ => return Change::Reshaped,
        }
    }
    // A column that left the unavailable list without reaching a payload was dropped too:
    // otherwise a projection of a TOASTed column would keep it in the narrowed shape.
    let present = |name: &String| {
        exit.after.as_ref().is_some_and(|keys| keys.contains(name))
            || exit.before.as_ref().is_some_and(|keys| keys.contains(name))
    };
    dropped.extend(
        entry
            .unavailable
            .difference(&exit.unavailable)
            .filter(|name| !present(name))
            .cloned(),
    );
    if dropped.is_empty() {
        Change::None
    } else {
        Change::Dropped(dropped)
    }
}

impl ShapeGuard {
    /// Record the columns an event arrives with.
    #[must_use]
    pub fn capture(event: &Event) -> Self {
        Self {
            entry: event.shape.is_some().then(|| Columns::of(event)),
        }
    }

    /// Narrow or remove the event's [`Event::shape`] to match what the chain left.
    pub fn release(self, event: &mut Event) {
        let Some(entry) = self.entry else {
            return;
        };
        let Some(shape) = event.shape.as_ref() else {
            return;
        };
        match compare(&entry, &Columns::of(event)) {
            Change::None => {}
            Change::Reshaped => event.shape = None,
            Change::Dropped(dropped) => {
                event.shape = shape.without_columns(&dropped).map(Arc::new);
            }
        }
    }
}

/// Where an event came from, to pair it with its guard after a batch stage dropped some.
///
/// None of these fields is something a transform rewrites (a route renames the table, an
/// outbox replaces the payload), so an event keeps its identity through the chain.
#[derive(Debug, PartialEq)]
struct Origin {
    source_name: String,
    offset: String,
    ts: u64,
    transaction: Option<(u64, u32)>,
}

impl Origin {
    fn of(event: &Event) -> Self {
        Self {
            source_name: event.source.source_name.clone(),
            offset: event.source.offset.clone(),
            ts: event.ts,
            transaction: event
                .transaction
                .as_ref()
                .map(|tx| (tx.tx_id, tx.event_index)),
        }
    }
}

/// [`ShapeGuard`] over a batch whose stages may drop events.
pub(crate) struct BatchShapeGuard {
    entries: Vec<(Origin, ShapeGuard)>,
}

impl BatchShapeGuard {
    pub(crate) fn capture(events: &[Event]) -> Option<Self> {
        events
            .iter()
            .any(|event| event.shape.is_some())
            .then(|| Self {
                entries: events
                    .iter()
                    .map(|event| (Origin::of(event), ShapeGuard::capture(event)))
                    .collect(),
            })
    }

    /// Pair each surviving event with its guard, in order. A survivor that matches no entry
    /// — a stage that rewrote the position, or reordered the batch — loses its shape rather
    /// than keeping one nothing checked.
    pub(crate) fn release(self, events: &mut [Event]) {
        let mut entries = self.entries.into_iter();
        for event in events.iter_mut() {
            if event.shape.is_none() {
                continue;
            }
            let origin = Origin::of(event);
            match entries.by_ref().find(|(entry, _)| *entry == origin) {
                Some((_, guard)) => guard.release(event),
                None => event.shape = None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::core::{BeforeImage, Operation, RowShape, ShapeColumn, SourceMetadata};
    use crate::transform::{
        FieldMappingConfig, FieldMappingTransform, FilterProjectionConfig,
        FilterProjectionTransform, MaskHashConfig, MaskHashTransform, MaskRule, Transform,
        TransformPipeline,
    };

    fn shape() -> Arc<RowShape> {
        Arc::new(
            RowShape::new(
                "app",
                "items",
                vec![
                    ShapeColumn::new("id", "bigint", false),
                    ShapeColumn::new("name", "varchar(64)", true),
                    ShapeColumn::new("secret", "varchar(64)", true),
                ],
                vec!["id".into()],
            )
            .unwrap(),
        )
    }

    fn row(offset: &str) -> Event {
        Event::builder("items", Operation::Insert)
            .schema("app")
            .source(SourceMetadata::new("mysql", offset, 1))
            .ts(1)
            .after(json!({"id": "1", "name": "a", "secret": "s"}))
            .primary_key(["id"])
            .shape(shape())
            .build()
    }

    fn projection(keep: &[&str]) -> FilterProjectionTransform {
        let config = FilterProjectionConfig {
            include_columns: Some(keep.iter().map(|column| (*column).to_string()).collect()),
            ..FilterProjectionConfig::default()
        };
        FilterProjectionTransform::new(config).unwrap()
    }

    #[tokio::test]
    async fn a_projection_narrows_the_shape() {
        let pipeline = TransformPipeline::default().with(projection(&["id", "name"]));
        let output = pipeline.apply(row("1")).await.unwrap().unwrap();

        let narrowed = output.shape.expect("a projection keeps a narrowed shape");
        let names: Vec<_> = narrowed
            .columns()
            .unwrap()
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(names, ["id", "name"]);
        assert_ne!(narrowed.id(), shape().id());
        assert_eq!(narrowed.computed_id().as_deref(), Some(narrowed.id()));
        assert!(narrowed.is_narrowed());
    }

    /// The runtime runs the batch path, not the per-event one.
    #[tokio::test]
    async fn a_projection_narrows_the_shape_on_the_batch_path_too() {
        let pipeline = TransformPipeline::default().with(projection(&["id", "name"]));
        let mut batch = vec![row("1"), row("2")];
        pipeline.apply_batch(&mut batch).await.unwrap();
        for event in &batch {
            assert_eq!(event.shape.as_ref().unwrap().columns().unwrap().len(), 2);
        }
    }

    /// A stage that renames a column without saying so: the guard sees a column dropped and
    /// another added, and removes the shape rather than narrowing it.
    #[tokio::test]
    async fn a_rename_removes_the_shape() {
        #[derive(Debug)]
        struct SilentRename;
        impl Transform for SilentRename {
            fn apply(&self, event: &mut Event) -> crate::core::Result<bool> {
                if let Some(serde_json::Value::Object(after)) = &mut event.after
                    && let Some(value) = after.remove("name")
                {
                    after.insert("label".into(), value);
                }
                Ok(true)
            }
            fn name(&self) -> &str {
                "silent_rename"
            }
        }
        let pipeline = TransformPipeline::default().with(SilentRename);
        let output = pipeline.apply(row("1")).await.unwrap().unwrap();
        assert!(output.shape.is_none());
    }

    /// The field mapping removes the shape itself: a `set` or `copy` onto an existing name
    /// rewrites a value under a name the guard sees unchanged.
    #[test]
    fn a_field_mapping_rename_removes_the_shape_itself() {
        let mut config = FieldMappingConfig::default();
        config.rename.push(("name".into(), "label".into()));
        let mut event = row("1");
        FieldMappingTransform::new(config)
            .unwrap()
            .apply(&mut event)
            .unwrap();
        assert!(event.shape.is_none());
    }

    #[tokio::test]
    async fn a_mask_removes_the_shape() {
        let mut config = MaskHashConfig::default();
        config
            .mask_rules
            .insert("secret".into(), MaskRule::Redact("***".into()));
        let pipeline = TransformPipeline::default().with(MaskHashTransform::new(config).unwrap());
        let output = pipeline.apply(row("1")).await.unwrap().unwrap();
        assert!(output.shape.is_none());
    }

    #[tokio::test]
    async fn a_stage_that_adds_a_column_removes_the_shape() {
        #[derive(Debug)]
        struct AddColumn;
        impl Transform for AddColumn {
            fn apply(&self, event: &mut Event) -> crate::core::Result<bool> {
                if let Some(serde_json::Value::Object(after)) = &mut event.after {
                    after.insert("extra".into(), json!("x"));
                }
                Ok(true)
            }
            fn name(&self) -> &str {
                "add_column"
            }
        }
        let pipeline = TransformPipeline::default().with(AddColumn);
        let mut batch = vec![row("1")];
        pipeline.apply_batch(&mut batch).await.unwrap();
        assert!(batch[0].shape.is_none());
    }

    /// `before` is compared too: a delete carries its row there, and a projection of it must
    /// narrow just like one of `after`.
    #[tokio::test]
    async fn a_delete_with_a_projected_before_is_compared() {
        let delete = Event::builder("items", Operation::Delete)
            .schema("app")
            .source(SourceMetadata::new("mysql", "1", 1))
            .ts(1)
            .before_image(BeforeImage::full(
                json!({"id": "1", "name": "a", "secret": "s"}),
            ))
            .primary_key(["id"])
            .shape(shape())
            .build();
        let pipeline = TransformPipeline::default().with(projection(&["id", "name"]));
        let output = pipeline.apply(delete).await.unwrap().unwrap();
        assert_eq!(output.shape.unwrap().columns().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_stage_that_leaves_the_columns_alone_keeps_the_shape() {
        #[derive(Debug)]
        struct RewriteValue;
        impl Transform for RewriteValue {
            fn apply(&self, event: &mut Event) -> crate::core::Result<bool> {
                if let Some(serde_json::Value::Object(after)) = &mut event.after {
                    after.insert("name".into(), json!("b"));
                }
                Ok(true)
            }
            fn name(&self) -> &str {
                "rewrite_value"
            }
        }
        let pipeline = TransformPipeline::default().with(RewriteValue);
        let output = pipeline.apply(row("1")).await.unwrap().unwrap();
        assert_eq!(output.shape.unwrap().id(), shape().id());
    }

    #[test]
    fn a_batch_survivor_matching_no_entry_loses_its_shape() {
        let events = vec![row("1"), row("2")];
        let guard = BatchShapeGuard::capture(&events).unwrap();
        let mut survivors = vec![row("2"), row("9")];
        guard.release(&mut survivors);
        assert!(survivors[0].shape.is_some(), "paired by origin");
        assert!(survivors[1].shape.is_none(), "unpaired");
    }
}
