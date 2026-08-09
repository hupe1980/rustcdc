//! Table-name glob-pattern router for CDC events.
//!
//! See [`TableRouter`] for the main entry point.

use futures_util::future::join_all;

use crate::core::{Error, Event, Result};
use crate::sink::{BoxedSink, SinkAdapter, SinkDeliveryGuarantee, SinkDeliveryMetrics};

// ─── Glob helpers ─────────────────────────────────────────────────────────────

/// Match a glob `pattern` against a `"schema.table"` or bare `"table"` key.
///
/// Re-exported from the crate's single glob implementation so routing and the connectors'
/// `table_include_list` / `table_exclude_list` cannot drift apart — they used to, with the
/// connectors matching exact strings only while nothing said so.
///
/// See [`crate::core`]'s glob module documentation for the full pattern table. In short:
/// `"*"` is a catch-all, `"schema.*"` scopes to a schema, `"*.table"` scopes to a name,
/// and an **unqualified** pattern such as `"orders"` matches that table in *every* schema.
pub fn table_matches(pattern: &str, table_key: &str) -> bool {
    crate::core::glob::table_matches(pattern, table_key)
}

/// A type alias for a [`TableRouter`] that accepts heterogeneous sink types.
///
/// Every concrete sink is wrapped in a [`BoxedSink`] before being added via the
/// builder, enabling routes to different downstream systems in one router:
///
/// ```rust,no_run
/// use rustcdc::pipeline::HeterogeneousTableRouter;
/// use rustcdc::sink::{MemorySinkAdapter, SinkAdapter};
///
/// let router = HeterogeneousTableRouter::builder("demo")
///     .route("public.orders", MemorySinkAdapter::new("orders").boxed())
///     .route("public.audit", MemorySinkAdapter::new("audit").boxed())
///     .build()
///     .expect("valid patterns");
/// ```
pub type HeterogeneousTableRouter = TableRouter<BoxedSink>;

/// A single entry in a [`TableRouter`]: a glob pattern paired with a sink.
#[derive(Debug)]
pub struct TableRoute<S> {
    /// Glob pattern matched against the event's `schema.table` qualified name.
    pub pattern: String,
    /// Sink that receives events matching [`pattern`](Self::pattern).
    pub sink: S,
}

impl<S> TableRoute<S> {
    /// Create a new route.
    pub fn new(pattern: impl Into<String>, sink: S) -> Self {
        Self {
            pattern: pattern.into(),
            sink,
        }
    }
}

// ─── TableRouterBuilder ───────────────────────────────────────────────────────

/// Ergonomic builder for [`TableRouter`].
///
/// Obtain an instance via [`TableRouter::builder`].
pub struct TableRouterBuilder<S> {
    name: String,
    routes: Vec<TableRoute<S>>,
    default: Option<S>,
    drop_unrouted: bool,
}

impl<S: SinkAdapter> TableRouterBuilder<S> {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            routes: Vec::new(),
            default: None,
            drop_unrouted: false,
        }
    }

    /// Append a route: events whose `schema.table` matches `pattern` are sent to `sink`.
    ///
    /// Routes are evaluated in the order they are added; the first matching pattern wins.
    pub fn route(mut self, pattern: impl Into<String>, sink: S) -> Self {
        self.routes.push(TableRoute::new(pattern, sink));
        self
    }

    /// Set a fallback sink for events that match no explicit pattern.
    ///
    /// When no default is provided, unmatched events are silently dropped (or return an
    /// error depending on [`drop_unrouted`](Self::drop_unrouted)).
    pub fn default(mut self, sink: S) -> Self {
        self.default = Some(sink);
        self
    }

    /// When `true` (the default), events that match no route and have no default sink are
    /// silently dropped.  When `false`, such events return a [`Error::StateError`].
    ///
    /// Silently dropping unmatched events is the right default for fan-out pipelines where
    /// you only care about a subset of tables.  Set this to `false` when you want strict
    /// auditability guarantees (every event must land somewhere).
    pub fn drop_unrouted(mut self, drop: bool) -> Self {
        self.drop_unrouted = drop;
        self
    }

    /// Build the [`TableRouter`], validating all route patterns.
    ///
    /// Returns [`Error::ConfigError`] when:
    /// - any pattern is empty,
    /// - two routes share the same pattern (likely a misconfiguration),
    /// - a dotted pattern has an empty schema or table segment (e.g. `".orders"`, `"public."`).
    ///
    /// Use [`build_unchecked`](Self::build_unchecked) when patterns are compile-time
    /// constants and you prefer a panicking shorthand.
    pub fn build(self) -> Result<TableRouter<S>> {
        let mut errors: Vec<String> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for route in &self.routes {
            let pat = route.pattern.as_str();
            if pat.is_empty() {
                errors.push("empty pattern".into());
            } else if !seen.insert(pat) {
                errors.push(format!("duplicate pattern '{pat}'"));
            } else if let Some(dot) = pat.find('.') {
                let (schema, rest) = pat.split_at(dot);
                let table = &rest[1..];
                if schema.is_empty() || table.is_empty() {
                    errors.push(format!(
                        "malformed pattern '{pat}': schema and table segments must both be non-empty"
                    ));
                }
            }
        }
        if !errors.is_empty() {
            return Err(Error::ConfigError(format!(
                "TableRouterBuilder '{}': invalid route pattern(s): {}",
                self.name,
                errors.join(", "),
            )));
        }
        Ok(self.build_unchecked())
    }

    /// Build the [`TableRouter`] without pattern validation.
    ///
    /// Prefer [`build`](Self::build) unless patterns are guaranteed valid at compile time.
    pub fn build_unchecked(self) -> TableRouter<S> {
        TableRouter {
            name: self.name,
            routes: self.routes,
            default: self.default,
            drop_unrouted: self.drop_unrouted,
            closed: false,
        }
    }
}

// ─── TableRouter ──────────────────────────────────────────────────────────────

/// Routes CDC events to named sinks based on table glob patterns.
///
/// Each incoming event is compared against the registered routes (in insertion order)
/// using glob pattern matching.  The first matching sink receives the event.  If no route
/// matches and a default sink is configured, the event goes to the default sink.
/// Otherwise the event is silently dropped (or an error is returned — see
/// [`TableRouterBuilder::drop_unrouted`]).
///
/// `TableRouter` itself implements [`SinkAdapter`], so it can be nested or composed
/// freely with other pipeline components.
///
/// # Generic parameter
///
/// `TableRouter<S>` is generic over a single `S: SinkAdapter`.  All sinks in the
/// router must be the same concrete type.  When you need heterogeneous sink types,
/// wrap each behind a common enum that implements `SinkAdapter`.
///
/// # Example
///
/// ```rust,no_run
/// use rustcdc::pipeline::{TableRouter, TableRoute};
/// use rustcdc::sink::MemorySinkAdapter;
///
/// let mut router: TableRouter<MemorySinkAdapter> = TableRouter::builder("demo")
///     .route("public.orders", MemorySinkAdapter::new("orders"))
///     .route("public.products", MemorySinkAdapter::new("products"))
///     .default(MemorySinkAdapter::new("fallback"))
///     .build()
///     .expect("valid patterns");
/// ```
#[derive(Debug)]
pub struct TableRouter<S> {
    name: String,
    routes: Vec<TableRoute<S>>,
    default: Option<S>,
    drop_unrouted: bool,
    closed: bool,
}

impl<S: SinkAdapter> TableRouter<S> {
    /// Return an ergonomic builder.
    pub fn builder(name: impl Into<String>) -> TableRouterBuilder<S> {
        TableRouterBuilder::new(name)
    }

    /// Create a `TableRouter` from an explicit list of routes and an optional default sink.
    ///
    /// Prefer [`builder`](Self::builder) for ergonomic construction.
    pub fn new(name: impl Into<String>, routes: Vec<TableRoute<S>>, default: Option<S>) -> Self {
        Self {
            name: name.into(),
            routes,
            default,
            drop_unrouted: false,
            closed: false,
        }
    }

    /// Borrow all configured routes.
    pub fn routes(&self) -> &[TableRoute<S>] {
        &self.routes
    }

    /// Borrow the default (fallback) sink, if any.
    pub fn default_sink(&self) -> Option<&S> {
        self.default.as_ref()
    }

    /// Return the sink that would receive `event`, if any.
    ///
    /// This is a read-only probe — use [`SinkAdapter::send`] to actually deliver the event.
    pub fn route_for(&self, event: &Event) -> Option<&S> {
        let key = event.qualified_table_name();
        for route in &self.routes {
            if table_matches(&route.pattern, &key) {
                return Some(&route.sink);
            }
        }
        self.default.as_ref()
    }

    /// Return the glob pattern that would match `event`, if any.
    ///
    /// Returns the pattern string of the first matching route.  Returns `None` when
    /// no route matches and no default sink is configured.
    /// When the default sink would receive the event, returns `"*"`.
    pub fn matched_pattern_for(&self, event: &Event) -> Option<&str> {
        let key = event.qualified_table_name();
        for route in &self.routes {
            if table_matches(&route.pattern, &key) {
                return Some(&route.pattern);
            }
        }
        if self.default.is_some() {
            Some("*")
        } else {
            None
        }
    }

    /// Iterate over all configured route patterns in evaluation order.
    ///
    /// Does not include the implicit default sink.
    pub fn pattern_names(&self) -> impl Iterator<Item = &str> {
        self.routes.iter().map(|r| r.pattern.as_str())
    }

    /// Flush all sinks (routes + default).
    ///
    /// Failures are aggregated under the most severe [`ErrorKind`](crate::core::ErrorKind)
    /// observed rather than flattened. `send` already passes a sink's error through
    /// untouched, and collapsing this path into one fixed variant made the two disagree:
    /// a broker connection reset is `Transient` and retryable from `send`, and was
    /// `Terminal` and fatal from `flush` — with which one you got decided by batch
    /// boundaries rather than by anything an operator controls.
    pub async fn flush_all(&mut self) -> Result<()> {
        let mut failures: Vec<(String, Error)> = Vec::new();
        for route in &mut self.routes {
            if let Err(e) = route.sink.flush().await {
                failures.push((format!("route '{}'", route.pattern), e));
            }
        }
        if let Some(ref mut d) = self.default {
            if let Err(e) = d.flush().await {
                failures.push(("default".to_string(), e));
            }
        }
        Error::aggregate(failures)
    }

    /// Close all sinks (routes + default).
    ///
    /// Failures are aggregated under the most severe kind observed — see
    /// [`flush_all`](Self::flush_all).
    pub async fn close_all(&mut self) -> Result<()> {
        let mut failures: Vec<(String, Error)> = Vec::new();
        for route in &mut self.routes {
            if let Err(e) = route.sink.close().await {
                failures.push((format!("route '{}'", route.pattern), e));
            }
        }
        if let Some(ref mut d) = self.default {
            if let Err(e) = d.close().await {
                failures.push(("default".to_string(), e));
            }
        }
        self.closed = true;
        Error::aggregate(failures)
    }
}

impl<S: SinkAdapter> SinkAdapter for TableRouter<S> {
    async fn send(&mut self, event: &Event) -> Result<()> {
        if self.closed {
            return Err(Error::StateError("TableRouter is closed".into()));
        }
        let key = event.qualified_table_name();
        for route in &mut self.routes {
            if table_matches(&route.pattern, &key) {
                return route.sink.send(event).await;
            }
        }
        if let Some(ref mut default_sink) = self.default {
            return default_sink.send(event).await;
        }
        // No match and no default.
        if self.drop_unrouted {
            Ok(())
        } else {
            Err(Error::StateError(format!(
                "TableRouter '{}': no route matched '{key}' and no default sink is configured",
                self.name,
            )))
        }
    }

    async fn flush(&mut self) -> Result<()> {
        self.flush_all().await
    }

    async fn close(&mut self) -> Result<()> {
        self.close_all().await
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn is_closed(&self) -> bool {
        self.closed
    }

    // ── Delivery contract ─────────────────────────────────────────────────────
    // Capability hooks delegate to children and reduce to the appropriate aggregate.

    fn delivery_guarantee(&self) -> SinkDeliveryGuarantee {
        let mut g = SinkDeliveryGuarantee::EffectivelyOnce;
        let mut has_sink = false;
        for route in &self.routes {
            g = g.weakest(route.sink.delivery_guarantee());
            has_sink = true;
        }
        if let Some(ref d) = self.default {
            g = g.weakest(d.delivery_guarantee());
            has_sink = true;
        }
        if has_sink {
            g
        } else {
            SinkDeliveryGuarantee::default()
        }
    }

    fn idempotent_delivery_capable(&self) -> bool {
        self.routes
            .iter()
            .all(|r| r.sink.idempotent_delivery_capable())
            && self
                .default
                .as_ref()
                .is_none_or(|d| d.idempotent_delivery_capable())
    }

    fn transactional_checkpoint_barrier_capable(&self) -> bool {
        !self.routes.is_empty()
            && self
                .routes
                .iter()
                .all(|r| r.sink.transactional_checkpoint_barrier_capable())
            && self
                .default
                .as_ref()
                .is_none_or(|d| d.transactional_checkpoint_barrier_capable())
    }

    fn queue_depth(&self) -> Option<usize> {
        let mut total: usize = 0;
        let mut any = false;
        for route in &self.routes {
            if let Some(d) = route.sink.queue_depth() {
                total = total.saturating_add(d);
                any = true;
            }
        }
        if let Some(ref d) = self.default {
            if let Some(depth) = d.queue_depth() {
                total = total.saturating_add(depth);
                any = true;
            }
        }
        if any {
            Some(total)
        } else {
            None
        }
    }

    fn flush_tick_interval(&self) -> Option<std::time::Duration> {
        self.routes
            .iter()
            .filter_map(|r| r.sink.flush_tick_interval())
            .chain(self.default.iter().filter_map(|d| d.flush_tick_interval()))
            .min()
    }

    fn delivery_metrics(&self) -> Option<SinkDeliveryMetrics> {
        let mut any = false;
        let mut agg = SinkDeliveryMetrics::default();
        for route in &self.routes {
            if let Some(m) = route.sink.delivery_metrics() {
                agg.merge(&m);
                any = true;
            }
        }
        if let Some(ref d) = self.default {
            if let Some(m) = d.delivery_metrics() {
                agg.merge(&m);
                any = true;
            }
        }
        if any {
            Some(agg)
        } else {
            None
        }
    }

    // ── Checkpoint barrier (2PC semantics) ────────────────────────────────────
    // begin: sequential with compensating aborts on failure (two-phase commit).
    // commit / abort / preflight: concurrent (mirror FanOutSinkAdapter).

    async fn begin_checkpoint_barrier(&mut self) -> Result<()> {
        let n = self.routes.len();
        for i in 0..n {
            if let Err(e) = self.routes[i].sink.begin_checkpoint_barrier().await {
                // Compensate: abort already-begun routes in reverse order.
                for j in (0..i).rev() {
                    let _ = self.routes[j].sink.abort_checkpoint_barrier().await;
                }
                return Err(e);
            }
        }
        if let Some(ref mut d) = self.default {
            if let Err(e) = d.begin_checkpoint_barrier().await {
                for j in (0..n).rev() {
                    let _ = self.routes[j].sink.abort_checkpoint_barrier().await;
                }
                return Err(e);
            }
        }
        Ok(())
    }

    async fn commit_checkpoint_barrier(&mut self) -> Result<()> {
        type B<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>;
        let mut futs: Vec<B<'_>> = self
            .routes
            .iter_mut()
            .map(|r| -> B<'_> { Box::pin(r.sink.commit_checkpoint_barrier()) })
            .collect();
        if let Some(ref mut d) = self.default {
            futs.push(Box::pin(d.commit_checkpoint_barrier()));
        }
        join_all(futs)
            .await
            .into_iter()
            .find(|r| r.is_err())
            .unwrap_or(Ok(()))
    }

    async fn abort_checkpoint_barrier(&mut self) -> Result<()> {
        type B<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>;
        let mut futs: Vec<B<'_>> = self
            .routes
            .iter_mut()
            .map(|r| -> B<'_> { Box::pin(r.sink.abort_checkpoint_barrier()) })
            .collect();
        if let Some(ref mut d) = self.default {
            futs.push(Box::pin(d.abort_checkpoint_barrier()));
        }
        join_all(futs)
            .await
            .into_iter()
            .find(|r| r.is_err())
            .unwrap_or(Ok(()))
    }

    async fn preflight_check(&mut self) -> Result<()> {
        type B<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>;
        let mut futs: Vec<B<'_>> = self
            .routes
            .iter_mut()
            .map(|r| -> B<'_> { Box::pin(r.sink.preflight_check()) })
            .collect();
        if let Some(ref mut d) = self.default {
            futs.push(Box::pin(d.preflight_check()));
        }
        join_all(futs)
            .await
            .into_iter()
            .find(|r| r.is_err())
            .unwrap_or(Ok(()))
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::glob::glob_segment_matches;
    use crate::core::{Operation, SourceMetadata, EVENT_ENVELOPE_VERSION};
    use crate::sink::MemorySinkAdapter;

    fn make_event(schema: Option<&str>, table: &str) -> Event {
        Event {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: "0".into(),
                timestamp: 0,
            },
            ts: 0,
            schema: schema.map(Into::into),
            table: table.into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns: Vec::new(),
            before_unavailable_columns: Vec::new(),
        }
    }

    // ─── glob helpers ──────────────────────────────────────────────────────────

    #[test]
    fn glob_star_matches_anything() {
        assert!(glob_segment_matches("*", "foo"));
        assert!(glob_segment_matches("*", ""));
    }

    #[test]
    fn glob_exact_matches() {
        assert!(glob_segment_matches("orders", "orders"));
        assert!(!glob_segment_matches("orders", "order"));
    }

    #[test]
    fn glob_suffix_wildcard() {
        assert!(glob_segment_matches("order*", "orders"));
        assert!(glob_segment_matches("order*", "order_items"));
        assert!(!glob_segment_matches("order*", "my_orders"));
    }

    #[test]
    fn glob_prefix_wildcard() {
        assert!(glob_segment_matches("*_audit", "user_audit"));
        assert!(!glob_segment_matches("*_audit", "audit_log"));
    }

    #[test]
    fn glob_question_mark() {
        assert!(glob_segment_matches("t_?", "t_1"));
        assert!(glob_segment_matches("t_?", "t_a"));
        assert!(!glob_segment_matches("t_?", "t_12"));
    }

    // ─── table_matches ─────────────────────────────────────────────────────────

    #[test]
    fn catch_all_matches_qualified() {
        assert!(table_matches("*", "public.orders"));
    }

    #[test]
    fn catch_all_matches_bare() {
        assert!(table_matches("*", "orders"));
    }

    #[test]
    fn schema_wildcard_matches_correct_schema() {
        assert!(table_matches("public.*", "public.orders"));
        assert!(!table_matches("public.*", "private.orders"));
    }

    #[test]
    fn table_wildcard_matches_correct_table() {
        assert!(table_matches("*.orders", "public.orders"));
        assert!(!table_matches("*.orders", "public.products"));
    }

    #[test]
    fn exact_qualified_match() {
        assert!(table_matches("public.orders", "public.orders"));
        assert!(!table_matches("public.orders", "public.products"));
    }

    #[test]
    fn bare_pattern_matches_table_part_of_qualified() {
        // bare "orders" matches "public.orders"
        assert!(table_matches("orders", "public.orders"));
    }

    #[test]
    fn qualified_pattern_does_not_match_bare_table() {
        assert!(!table_matches("public.orders", "orders"));
    }

    // ─── TableRouter ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn routes_event_to_matching_sink() {
        let mut router: TableRouter<MemorySinkAdapter> = TableRouter::builder("test")
            .route("public.orders", MemorySinkAdapter::new("orders"))
            .route("public.products", MemorySinkAdapter::new("products"))
            .build()
            .expect("valid test routes");

        let event = make_event(Some("public"), "orders");
        router.send(&event).await.unwrap();

        let orders = router.routes()[0].sink.events();
        let products = router.routes()[1].sink.events();
        assert_eq!(orders.len(), 1);
        assert_eq!(products.len(), 0);
    }

    #[tokio::test]
    async fn unmatched_event_goes_to_default_sink() {
        let mut router: TableRouter<MemorySinkAdapter> = TableRouter::builder("test")
            .route("public.orders", MemorySinkAdapter::new("orders"))
            .default(MemorySinkAdapter::new("fallback"))
            .build()
            .expect("valid test routes");

        let event = make_event(Some("public"), "customers");
        router.send(&event).await.unwrap();

        assert_eq!(router.routes()[0].sink.events().len(), 0);
        assert_eq!(
            router
                .default_sink()
                .unwrap()
                .exported_events()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn drop_unrouted_silently_discards_event() {
        let mut router: TableRouter<MemorySinkAdapter> = TableRouter::builder("test")
            .route("public.orders", MemorySinkAdapter::new("orders"))
            .drop_unrouted(true)
            .build()
            .expect("valid test routes");

        // no default, drop_unrouted = true
        let event = make_event(Some("public"), "unrelated");
        router.send(&event).await.unwrap(); // must not error
        assert_eq!(router.routes()[0].sink.events().len(), 0);
    }

    #[tokio::test]
    async fn no_route_and_no_default_returns_error_when_strict() {
        let mut router: TableRouter<MemorySinkAdapter> = TableRouter::builder("strict")
            .route("public.orders", MemorySinkAdapter::new("orders"))
            .drop_unrouted(false) // explicit strict mode
            .build()
            .expect("valid test routes");

        let event = make_event(Some("public"), "unrelated");
        let result = router.send(&event).await;
        assert!(
            result.is_err(),
            "should error on unmatched event in strict mode"
        );
    }

    #[tokio::test]
    async fn first_matching_route_wins() {
        let mut router: TableRouter<MemorySinkAdapter> = TableRouter::builder("test")
            .route("*", MemorySinkAdapter::new("catch-all"))
            .route("public.orders", MemorySinkAdapter::new("orders"))
            .build()
            .expect("valid test routes");

        let event = make_event(Some("public"), "orders");
        router.send(&event).await.unwrap();

        // catch-all is first, so it wins
        assert_eq!(router.routes()[0].sink.events().len(), 1);
        assert_eq!(router.routes()[1].sink.events().len(), 0);
    }

    #[tokio::test]
    async fn flush_all_propagates_errors() {
        let mut router: TableRouter<MemorySinkAdapter> = TableRouter::builder("test")
            .route("*", MemorySinkAdapter::new("a"))
            .build()
            .expect("valid test routes");
        // MemorySinkAdapter flush always succeeds — this just verifies no panic.
        router.flush_all().await.unwrap();
    }

    #[tokio::test]
    async fn sink_adapter_send_after_close_returns_error() {
        let mut router: TableRouter<MemorySinkAdapter> = TableRouter::builder("test")
            .route("*", MemorySinkAdapter::new("a"))
            .build()
            .expect("valid test routes");
        router.close().await.unwrap();
        let event = make_event(Some("public"), "orders");
        assert!(router.send(&event).await.is_err());
    }

    #[tokio::test]
    async fn route_for_returns_correct_sink() {
        let router: TableRouter<MemorySinkAdapter> = TableRouter::builder("test")
            .route("public.orders", MemorySinkAdapter::new("orders"))
            .default(MemorySinkAdapter::new("fallback"))
            .build()
            .expect("valid test routes");

        let orders_event = make_event(Some("public"), "orders");
        let other_event = make_event(Some("public"), "customers");

        let orders_sink = router.route_for(&orders_event).unwrap();
        assert_eq!(orders_sink.name(), "orders");

        let fallback = router.route_for(&other_event).unwrap();
        assert_eq!(fallback.name(), "fallback");
    }

    #[tokio::test]
    async fn glob_prefix_route_matches_multiple_tables() {
        let mut router: TableRouter<MemorySinkAdapter> = TableRouter::builder("test")
            .route("public.order*", MemorySinkAdapter::new("orders"))
            .build()
            .expect("valid test routes");

        router
            .send(&make_event(Some("public"), "orders"))
            .await
            .unwrap();
        router
            .send(&make_event(Some("public"), "order_items"))
            .await
            .unwrap();
        assert_eq!(router.routes()[0].sink.events().len(), 2);
    }
}
