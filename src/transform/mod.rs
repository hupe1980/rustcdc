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

// ─── Rule coverage ────────────────────────────────────────────────────────────

/// A configured rule that has never matched anything.
///
/// # Why every pattern-matching transform reports this
///
/// Masking, filtering and routing all match by **pattern against a permissive default**,
/// so a typo, an upstream column rename, or an earlier stage that moved a field disables
/// a rule *silently*. Nothing errors; the pipeline keeps running and produces plausible
/// output. What it does instead is the specific harm each transform exists to prevent:
///
/// | Transform | A rule that never fires means |
/// |---|---|
/// | [`MaskHashTransform`] | a column is shipping in **clear text** |
/// | [`FilterProjectionTransform`] | rows meant to be excluded are being delivered |
/// | [`RouteTransform`] | events are going to the **default destination**, not the one configured |
///
/// Failing closed is not the answer — it would refuse to start over a rule for an
/// optional column, and operators would respond by deleting rules. So each rule carries a
/// hit counter, the runtime exposes the never-fired ones as
/// `rustcdc_transform_rules_unmatched`, and the condition becomes an alert rule rather
/// than a log line someone has to grep for at shutdown.
///
/// Zero hits is only meaningful **after real traffic**. Evaluate against a representative
/// sample, not at startup.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct UnmatchedRule {
    /// The stage the rule belongs to, from [`Transform::name`].
    pub transform: String,
    /// What kind of rule it is — `"mask"`, `"filter"`, `"route"`.
    ///
    /// A string rather than an enum: it is a metric label, and a stage outside this crate
    /// must be able to name its own rule kinds without a breaking change here.
    pub kind: String,
    /// The configured pattern that never fired — a JSON path, a filter predicate, a
    /// routing table key or regex.
    pub rule: String,
    /// What is silently happening because the rule never fired.
    ///
    /// Carried on the value rather than left to the reader: the consequence is what makes
    /// the alert actionable, and it differs per transform.
    pub consequence: String,
}

impl UnmatchedRule {
    /// Build a report entry.
    pub fn new(
        transform: impl Into<String>,
        kind: impl Into<String>,
        rule: impl Into<String>,
        consequence: impl Into<String>,
    ) -> Self {
        Self {
            transform: transform.into(),
            kind: kind.into(),
            rule: rule.into(),
            consequence: consequence.into(),
        }
    }
}

/// Shared body of [`Transform::warn_on_unmatched_rules`] and its async twin.
fn warn_on_unmatched_rules(name: &str, unmatched: Vec<UnmatchedRule>) -> usize {
    if unmatched.is_empty() {
        return 0;
    }
    let rules: Vec<&str> = unmatched.iter().map(|rule| rule.rule.as_str()).collect();
    tracing::warn!(
        target: "rustcdc::transform",
        transform = name,
        unmatched_rules = ?rules,
        "{} rule(s) on '{name}' have never matched. {}",
        unmatched.len(),
        unmatched[0].consequence,
    );
    unmatched.len()
}

/// A **synchronous** stage in the event pipeline.
///
/// Stages run in order, per event, and may mutate the event in place or drop it.
///
/// # Why this is not `async`
///
/// Every transform this crate ships — masking, filtering, projection, field mapping,
/// routing, unwrapping, outbox — is pure CPU work over an in-memory event. When the trait
/// was `async`, `#[async_trait]` boxed a future for each of them on **every event**:
/// O(events × stages) heap allocations on the hottest path in the library, all to await
/// something that never yields.
///
/// A stage that genuinely must await — a WASM sandbox, a network enrichment lookup —
/// implements [`AsyncTransform`] instead. [`TransformPipeline`] holds both and pays the
/// boxing cost only for the stages that need it.
///
/// # What a transform must not do
///
/// **Destroy the message key.** `Event::primary_key` names the key *columns*; the values
/// live in the row payload. Projecting away, renaming, or re-encrypting a key column
/// detaches the two, and the event is emitted unkeyed — log compaction stops collapsing
/// it and upsert consumers start inserting duplicates. The pipeline rejects this rather
/// than letting it through silently, but a transform should not attempt it.
pub trait Transform: Send + Sync + std::fmt::Debug {
    /// Apply transform in-place; return true to keep event, false to drop it.
    fn apply(&self, event: &mut Event) -> Result<bool>;

    /// Stage name, used in error messages and metrics.
    fn name(&self) -> &str;

    /// Configured rules that have never matched anything.
    ///
    /// See [`UnmatchedRule`] for why this is a first-class part of the trait rather than
    /// a per-transform accessor. The default is empty: a stage whose behaviour does not
    /// depend on patterns matching has nothing to report.
    ///
    /// Implementations must count *matches*, not invocations, and must return an empty
    /// vector before any events have flowed — a rule cannot be judged unmatched until
    /// there has been traffic for it to miss.
    fn unmatched_rules(&self) -> Vec<UnmatchedRule> {
        Vec::new()
    }

    /// Log a WARN naming every rule of this stage that has never matched, if any.
    ///
    /// Returns the number of unmatched rules, so a caller can fail a readiness check on
    /// it. Prefer the `rustcdc_transform_rules_unmatched` metric for alerting — a log
    /// line at shutdown is something an operator has to go looking for.
    fn warn_on_unmatched_rules(&self) -> usize {
        warn_on_unmatched_rules(self.name(), self.unmatched_rules())
    }

    /// Apply to a whole batch, dropping events the stage filters out.
    ///
    /// The default runs [`Transform::apply`] over the batch in place. Override when a
    /// stage can amortise per-batch setup — compiling a pattern once, resolving a lookup
    /// table once — rather than repeating it per event.
    fn apply_batch(&self, events: &mut Vec<Event>) -> Result<()> {
        let mut error = None;
        events.retain_mut(|event| {
            if error.is_some() {
                return true;
            }
            match self.apply(event) {
                Ok(keep) => keep,
                Err(failure) => {
                    error = Some(failure);
                    true
                }
            }
        });
        match error {
            Some(failure) => Err(failure),
            None => Ok(()),
        }
    }
}

/// A stage that must `await` — a WASM sandbox, a network lookup.
///
/// Prefer [`Transform`] wherever the work is pure CPU: this variant boxes a future per
/// event, which is exactly the cost the split exists to avoid paying for stages that do
/// not need it.
#[async_trait]
pub trait AsyncTransform: Send + Sync + std::fmt::Debug {
    /// Apply transform in-place; return true to keep event, false to drop it.
    async fn apply(&self, event: &mut Event) -> Result<bool>;

    /// Stage name, used in error messages and metrics.
    fn name(&self) -> &str;

    /// Configured rules that have never matched anything. See [`Transform::unmatched_rules`].
    fn unmatched_rules(&self) -> Vec<UnmatchedRule> {
        Vec::new()
    }

    /// Log a WARN naming every rule of this stage that has never matched, if any.
    ///
    /// See [`Transform::warn_on_unmatched_rules`].
    fn warn_on_unmatched_rules(&self) -> usize {
        warn_on_unmatched_rules(self.name(), self.unmatched_rules())
    }

    /// Apply to a whole batch.
    ///
    /// The default awaits [`AsyncTransform::apply`] per event. Override to amortise a
    /// per-batch cost — a WASM stage, for instance, can acquire its instance lock once for
    /// the batch instead of once per event.
    async fn apply_batch(&self, events: &mut Vec<Event>) -> Result<()> {
        let mut kept = Vec::with_capacity(events.len());
        for mut event in std::mem::take(events) {
            if self.apply(&mut event).await? {
                kept.push(event);
            }
        }
        *events = kept;
        Ok(())
    }
}

/// One stage of a [`TransformPipeline`], sync or async.
enum Stage {
    Sync(Box<dyn Transform>),
    Async(Box<dyn AsyncTransform>),
}

impl Stage {
    fn name(&self) -> &str {
        match self {
            Self::Sync(transform) => transform.name(),
            Self::Async(transform) => transform.name(),
        }
    }
}

/// An ordered chain of [`Transform`] and [`AsyncTransform`] stages.
#[derive(Default)]
pub struct TransformPipeline {
    transforms: Vec<Stage>,
}

impl std::fmt::Debug for TransformPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.transforms.iter().map(Stage::name).collect();
        f.debug_struct("TransformPipeline")
            .field("transforms", &names)
            .finish()
    }
}

impl TransformPipeline {
    /// Add a synchronous transform to the end of the pipeline (mutating form).
    pub fn add_transform(&mut self, transform: Box<dyn Transform>) {
        self.transforms.push(Stage::Sync(transform));
    }

    /// Add an async transform to the end of the pipeline (mutating form).
    pub fn add_async_transform(&mut self, transform: Box<dyn AsyncTransform>) {
        self.transforms.push(Stage::Async(transform));
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
        self.transforms.push(Stage::Sync(Box::new(transform)));
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

    /// Every rule across every stage that has never matched anything.
    ///
    /// This is what [`CdcRuntime::admin_snapshot`](crate::CdcRuntime::admin_snapshot)
    /// surfaces and what the `rustcdc_transform_rules_unmatched` metric is built from.
    /// **Meaningful only after real traffic** — see [`UnmatchedRule`].
    pub fn unmatched_rules(&self) -> Vec<UnmatchedRule> {
        self.transforms
            .iter()
            .flat_map(|stage| match stage {
                Stage::Sync(transform) => transform.unmatched_rules(),
                Stage::Async(transform) => transform.unmatched_rules(),
            })
            .collect()
    }

    /// Log a WARN for every stage that has rules which never matched.
    ///
    /// Returns the total number of unmatched rules across the pipeline.
    pub fn warn_on_unmatched_rules(&self) -> usize {
        self.transforms
            .iter()
            .map(|stage| match stage {
                Stage::Sync(transform) => transform.warn_on_unmatched_rules(),
                Stage::Async(transform) => transform.warn_on_unmatched_rules(),
            })
            .sum()
    }

    /// Run a whole batch through every stage, in order, stage by stage.
    ///
    /// Prefer this over calling [`TransformPipeline::apply`] per event. It runs stage 1
    /// over the whole batch, then stage 2, and so on — so a stage that overrides
    /// `apply_batch` amortises its per-batch cost once instead of once per event, and an
    /// async stage acquires its lock once instead of `batch.len()` times.
    ///
    /// Events a stage drops are removed from the batch; the caller sees only survivors.
    ///
    /// # Errors
    ///
    /// Propagates a stage failure with the stage name as context, preserving the original
    /// error kind. Also fails if a stage left an event without resolvable primary-key
    /// values — see [`TransformPipeline::apply`] for why that is fatal rather than a
    /// warning.
    pub async fn apply_batch(&self, events: &mut Vec<Event>) -> Result<()> {
        if self.transforms.is_empty() || events.is_empty() {
            return Ok(());
        }

        // Which events arrived with a resolvable message key. Captured before any stage
        // runs, because the guard below asks whether a stage *destroyed* one — an event
        // that never had a key is not a regression.
        let had_key: Vec<bool> = events
            .iter()
            .map(|event| event.op.is_data_change() && event.primary_key_values().is_some())
            .collect();
        let keyed_tables: Vec<String> = events
            .iter()
            .zip(&had_key)
            .filter(|(_, keyed)| **keyed)
            .map(|(event, _)| event.qualified_table_name())
            .collect();

        for stage in &self.transforms {
            match stage {
                Stage::Sync(transform) => transform.apply_batch(events),
                Stage::Async(transform) => transform.apply_batch(events).await,
            }
            .map_err(|error| error.context(format!("transform '{}' failed", stage.name())))?;

            // Key destruction is checked per stage, so the error names the stage that did
            // it rather than the last one to run.
            if !keyed_tables.is_empty() {
                if let Some(event) = events
                    .iter()
                    .find(|event| event.op.is_data_change() && event.primary_key_values().is_none())
                {
                    return Err(Error::TransformError(format!(
                        "{}: transform removed the event's primary-key values for table \
                         '{}'. The event had a resolvable key before this transform and does \
                         not after it, so it would be emitted unkeyed — breaking log \
                         compaction and upsert consumers downstream. Primary key columns \
                         are {:?}. Check whether this transform projects away, renames, or \
                         rewrites a key column; exclude the key columns from it, or clear \
                         `Event::primary_key` deliberately if the events are genuinely \
                         keyless.",
                        stage.name(),
                        event.qualified_table_name(),
                        event.primary_key.as_deref().unwrap_or(&[]),
                    )));
                }
            }
        }

        Ok(())
    }

    /// Run an event through every stage in order.
    ///
    /// Returns `Ok(None)` if a stage dropped the event — an ordinary filtering outcome,
    /// not an error.
    ///
    /// # Errors
    ///
    /// Propagates a stage failure with the stage name as context, **preserving the
    /// original error kind** so a `ConfigError` raised inside a transform still routes as
    /// a configuration problem rather than a terminal one. Also fails if a stage left the
    /// event without resolvable primary-key values.
    pub async fn apply(&self, mut event: Event) -> Result<Option<Event>> {
        // Whether the event had a resolvable message key before any transform ran.
        // Only meaningful for data-change events; READ/SCHEMA_CHANGE/TRUNCATE carry no
        // row identity to preserve.
        let key_before = event.op.is_data_change() && event.primary_key_values().is_some();

        for transform in &self.transforms {
            // Wrap with context rather than re-wrapping as `TransformError`.
            //
            // Re-wrapping laundered the variant: a `ConfigError` raised inside a
            // transform (a mask rule naming a column that does not exist, say) came
            // out as `TransformError`, whose `ErrorKind` is `Terminal` rather than
            // `Configuration`. An embedder routing configuration problems to "fix the
            // config and restart" and terminal ones to "page someone" got the wrong
            // one, and `TransformErrorPolicy::Skip` would quietly discard events over
            // a typo. `Error::context` keeps the cause — and therefore the kind —
            // while still naming the transform that failed.
            let keep = match transform {
                Stage::Sync(transform) => transform.apply(&mut event),
                Stage::Async(transform) => transform.apply(&mut event).await,
            }
            .map_err(|error| error.context(format!("transform '{}' failed", transform.name())))?;
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
    use serde_json::json;

    use crate::core::{Event, Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};

    use super::{Transform, TransformPipeline};

    #[derive(Debug)]
    struct AppendSuffix;

    #[derive(Debug)]
    struct DropEvent;

    #[derive(Debug)]
    struct FailTransform;

    impl Transform for AppendSuffix {
        fn apply(&self, event: &mut Event) -> crate::core::Result<bool> {
            if let Some(serde_json::Value::Object(after)) = &mut event.after {
                after.insert("suffix".into(), json!("ok"));
            }
            Ok(true)
        }

        fn name(&self) -> &str {
            "append_suffix"
        }
    }

    impl Transform for DropEvent {
        fn apply(&self, _event: &mut Event) -> crate::core::Result<bool> {
            Ok(false)
        }

        fn name(&self) -> &str {
            "drop_event"
        }
    }

    impl Transform for FailTransform {
        fn apply(&self, _event: &mut Event) -> crate::core::Result<bool> {
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
    async fn pipeline_names_the_failing_transform_without_laundering_the_cause() {
        // This test previously asserted that the pipeline re-wrapped every failure as
        // `TransformError`. It did — and that flipped a `ConfigError` raised inside a
        // transform from `ErrorKind::Configuration` to `ErrorKind::Terminal`, sending
        // an embedder's error routing to the wrong branch. The transform name is still
        // reported; the cause is now preserved alongside it.
        let mut pipeline = TransformPipeline::default();
        pipeline.add_transform(Box::new(FailTransform));

        let error = pipeline.apply(event()).await.unwrap_err();
        assert!(
            error.to_string().contains("fail_transform"),
            "the failing transform must still be named; got: {error}"
        );
        // `FailTransform` raises a `ConfigError` — a mask rule naming a column that
        // does not exist is exactly this shape. The kind must survive the pipeline so
        // an embedder routes it to "fix the config and restart" rather than to a page.
        assert!(
            matches!(error.root_cause(), crate::core::Error::ConfigError(_)),
            "the original cause must survive; got: {:?}",
            error.root_cause()
        );
        assert_eq!(error.kind(), crate::core::ErrorKind::Configuration);
    }

    #[tokio::test]
    async fn empty_pipeline_returns_input_event() {
        let pipeline = TransformPipeline::default();
        let output = pipeline.apply(event()).await.unwrap().unwrap();
        assert_eq!(output.table, "items");
    }
}
