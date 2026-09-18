//! Pipeline assembly: build a [`TableRouter`] from configuration.

use crate::config::schema::AppConfig;
use crate::error::AppError;
use crate::pipeline::router::{self, TableRouter};
use crate::sink::{self, KafkaTransactionHandle, SinkMetricsRegistry};

/// A built router plus whatever the pipeline needs that the router itself erases.
pub struct BuiltRouter {
    pub router: TableRouter,
    /// A share in the sink's Kafka transaction, when exactly one transactional Kafka sink
    /// was built.
    ///
    /// Collected here rather than fetched later because `TableRouter` boxes its sinks as
    /// `BoxedSink`, which exposes only `SinkAdapter` — once a binding is inside the router
    /// there is no way back to its concrete producer. The handle has to be taken on the
    /// way in.
    ///
    /// `None` when there is no transactional Kafka sink, and — deliberately — also when
    /// there is more than one. Two sinks mean two producers, two transactions and no
    /// atomicity across them; the configuration loader rejects that combination for
    /// `effectively_once` rather than letting this silently pick one.
    pub transaction_handle: Option<KafkaTransactionHandle>,
    /// Read handles onto every binding's extended delivery counters.
    ///
    /// Collected here for the same reason as `transaction_handle`: `TableRouter` boxes its
    /// sinks as `BoxedSink`, whose `SinkAdapter::delivery_metrics()` carries four generic
    /// fields. Thirty of this server's metric families have no room in those four and are
    /// unreachable once a binding is inside the router, so the handle is taken on the way
    /// in. Without it those families render as a constant zero and the alert rules that
    /// watch them can never fire.
    pub sink_metrics: SinkMetricsRegistry,
}

/// Build a [`TableRouter`] from the top-level application configuration.
///
/// * If `config.pipeline.routes` is empty, returns a simple single-sink
///   router wrapping the default `config.sink`.
/// * If routes are configured, also builds the named sinks from
///   `config.sinks` and compiles the routing table.
///
/// Returns a configuration error if:
/// * A route references a sink name not present in `config.sinks`.
/// * A `[[sinks]]` entry is declared that no route references.
/// * A glob pattern in a route is syntactically invalid.
pub async fn build_router(config: &AppConfig) -> Result<BuiltRouter, AppError> {
    // Reference errors are decided before anything is built. Building first and checking
    // after would open a Kafka producer, an HTTP client and a TLS handshake per named sink
    // only to drop them, and would report a typo'd route name after the connection
    // attempts rather than before.
    validate_route_references(&config.pipeline.routes, &config.sinks)?;

    // Each sink preflights the topics *it* will be asked for, not the pipeline's whole
    // table list. A named sink behind `table_pattern = "public.orders"` that demanded a
    // topic for `public.customers` — a table routed elsewhere — would fail startup over
    // a topic that will never receive an event.
    //
    // Schema events are published under `<table>__ddl_events`, so a known table can also
    // need a topic for its schema events. Whether it does is the transforms' decision, and
    // where they go is the routes'.
    let mut known = sink::SinkBuildContext::known_tables_from_config(config);
    let schema_events =
        crate::pipeline::transform::schema_event_tables(&config.pipeline.transforms, &known);
    known.extend(schema_events);
    let assignment = assign_known_tables(known, &config.pipeline.routes);
    let context_for = |tables: Vec<crate::topic::QualifiedTable>| {
        sink::SinkBuildContext::new(config.runtime.max_event_bytes).with_known_tables(tables)
    };

    // Always build the default sink binding.
    let default_binding =
        sink::build_binding(&config.sink, &context_for(assignment.default)).await?;
    let mut sink_metrics = SinkMetricsRegistry::default();
    sink_metrics.register(default_binding.metrics_handle());

    if config.pipeline.routes.is_empty() {
        let transaction_handle = default_binding.transaction_handle();
        return Ok(BuiltRouter {
            router: router::single(default_binding),
            transaction_handle,
            sink_metrics,
        });
    }

    // Named sinks are built inside the route loop rather than up front, because a sink's
    // build context depends on which route claims it — a sink preflights the tables its
    // own route sends it. `validate_route_references` has already established both
    // invariants this loop relies on; they are re-checked here because violating either
    // would mean building a producer twice or not at all, and a `?` is cheaper than
    // trusting a caller two hundred lines away.
    let declared: std::collections::HashMap<&str, &crate::config::sink::SinkConfig> = config
        .sinks
        .iter()
        .map(|named| (named.name.as_str(), &named.sink))
        .collect();
    let mut claimed: std::collections::HashSet<&str> =
        std::collections::HashSet::with_capacity(config.sinks.len());

    let mut routes: Vec<(String, crate::sink::SinkBinding)> =
        Vec::with_capacity(config.pipeline.routes.len());

    for (index, route) in config.pipeline.routes.iter().enumerate() {
        let sink_config = declared.get(route.sink.as_str()).ok_or_else(|| {
            AppError::Config(Box::new(crate::error::ConfigError::Invalid(format!(
                "pipeline route references unknown sink {:?}; \
                 add a [[sinks]] entry with that name",
                route.sink
            ))))
        })?;
        if !claimed.insert(route.sink.as_str()) {
            // The old loop consumed bindings out of a map, so the second route to name a
            // sink reported "unknown sink" for one that was plainly declared. Whatever
            // this says, it should not say that.
            return Err(AppError::Config(Box::new(
                crate::error::ConfigError::Invalid(format!(
                    "sink {:?} is referenced by more than one [[pipeline.routes]] entry; \
                     one binding cannot serve two routes",
                    route.sink
                )),
            )));
        }
        let tables = assignment.per_route.get(index).cloned().unwrap_or_default();
        let binding = sink::build_binding(sink_config, &context_for(tables)).await?;
        sink_metrics.register(binding.metrics_handle());
        routes.push((route.table_pattern.clone(), binding));
    }

    // Exactly one, or none. See `BuiltRouter::transaction_handle` for why more than one is
    // treated as none rather than as a choice.
    let mut handles = std::iter::once(&default_binding)
        .chain(routes.iter().map(|(_, binding)| binding))
        .filter_map(|binding| binding.transaction_handle());
    let transaction_handle = match (handles.next(), handles.next()) {
        (Some(handle), None) => Some(handle),
        _ => None,
    };

    Ok(BuiltRouter {
        router: router::with_routes(default_binding, routes)?,
        transaction_handle,
        sink_metrics,
    })
}

/// Which of the configuration's known tables each sink will actually be asked for.
struct KnownTableAssignment {
    /// Tables no route claims — what the default `[sink]` receives.
    default: Vec<crate::topic::QualifiedTable>,
    /// Tables per route, in route order.
    per_route: Vec<Vec<crate::topic::QualifiedTable>>,
}

/// Split the configuration's concrete tables the way the router will split its events.
///
/// Only the Kafka sink's topic preflight consumes this, and only when its `topic` is a
/// template — but getting the split wrong is a startup failure over a topic that will
/// never receive an event, so it uses the router's own matcher and the router's own
/// first-match-wins order rather than an approximation of them.
///
/// A table matching no route lands on the default sink, which is exactly what
/// `TableRouter::route_for` does.
fn assign_known_tables(
    tables: Vec<crate::topic::QualifiedTable>,
    routes: &[crate::config::pipeline::RouteConfig],
) -> KnownTableAssignment {
    let mut assignment = KnownTableAssignment {
        default: Vec::new(),
        per_route: vec![Vec::new(); routes.len()],
    };

    for table in tables {
        let key = table.display();
        match routes
            .iter()
            .position(|route| rustcdc::pipeline::table_matches(&route.table_pattern, &key))
        {
            Some(index) => assignment.per_route[index].push(table),
            None => assignment.default.push(table),
        }
    }

    assignment
}

/// Check that `[[pipeline.routes]]` and `[[sinks]]` name each other consistently.
///
/// Runs before any sink is constructed, so a naming mistake costs no connection attempt.
/// Three distinct errors, because they have three distinct fixes:
///
/// * a route naming a sink that does not exist — a typo, or a missing `[[sinks]]` entry;
/// * a `[[sinks]]` entry no route names — inert configuration, and the shape a mistyped
///   route leaves behind. It used to be built, connected and dropped unused;
/// * two routes naming the same sink — one binding cannot serve two routes, and the
///   second route silently lost its sink before this check existed.
fn validate_route_references(
    routes: &[crate::config::pipeline::RouteConfig],
    sinks: &[crate::config::sink::NamedSinkConfig],
) -> Result<(), AppError> {
    fn invalid(message: String) -> AppError {
        AppError::Config(Box::new(crate::error::ConfigError::Invalid(message)))
    }

    if routes.is_empty() {
        if !sinks.is_empty() {
            let mut declared: Vec<&str> = sinks.iter().map(|s| s.name.as_str()).collect();
            declared.sort_unstable();
            return Err(invalid(format!(
                "[[sinks]] entries {declared:?} are declared but [[pipeline.routes]] is empty, \
                 so none of them can ever receive an event. Add routes that reference them, \
                 or remove the entries."
            )));
        }
        return Ok(());
    }

    let declared: std::collections::HashSet<&str> = sinks.iter().map(|s| s.name.as_str()).collect();
    let mut claimed: std::collections::HashSet<&str> = std::collections::HashSet::new();

    for route in routes {
        if !declared.contains(route.sink.as_str()) {
            return Err(invalid(format!(
                "pipeline route {:?} references unknown sink {:?}; \
                 add a [[sinks]] entry with that name",
                route.table_pattern, route.sink
            )));
        }
        if !claimed.insert(route.sink.as_str()) {
            return Err(invalid(format!(
                "two [[pipeline.routes]] entries both reference sink {:?}. A named sink is \
                 owned by exactly one route; give the second route its own [[sinks]] entry, \
                 or merge the two patterns into one route.",
                route.sink
            )));
        }
    }

    let mut unused: Vec<&str> = declared.difference(&claimed).copied().collect();
    if !unused.is_empty() {
        unused.sort_unstable();
        return Err(invalid(format!(
            "[[sinks]] entries {unused:?} are declared but no [[pipeline.routes]] entry \
             references them. A named sink with no route never receives an event, and \
             building it opens a client and a connection for nothing; either add a route \
             with `sink = \"<name>\"` or remove the entry."
        )));
    }

    Ok(())
}

#[cfg(test)]
#[path = "binding_preflight_tests.rs"]
mod preflight_tests;

#[cfg(test)]
mod tests {
    use crate::config::pipeline::RouteConfig;
    use crate::config::sink::{NamedSinkConfig, SinkConfig};
    use crate::topic::QualifiedTable;

    fn stdout_sink(name: &str) -> NamedSinkConfig {
        NamedSinkConfig {
            name: name.to_string(),
            sink: SinkConfig::Stdout(Default::default()),
        }
    }

    /// The concrete tables among `entries`, the way the config path derives them.
    fn concrete(entries: &[&str]) -> Vec<QualifiedTable> {
        entries
            .iter()
            .filter_map(|entry| QualifiedTable::parse_concrete(entry))
            .collect()
    }

    fn route(pattern: &str, sink: &str) -> RouteConfig {
        RouteConfig {
            table_pattern: pattern.to_string(),
            sink: sink.to_string(),
        }
    }

    /// The defect: a `[[sinks]]` entry no route referenced was *built* — Kafka producer,
    /// HTTP client, TLS handshake — and then dropped at the end of `build_router`. An
    /// operator who mistyped a route's `sink` name got a pipeline that started cleanly
    /// and ignored the sink they had configured.
    #[test]
    fn a_declared_sink_that_no_route_references_is_refused() {
        let sinks = vec![stdout_sink("archive"), stdout_sink("forgotten")];
        let routes = vec![route("public.*", "archive")];

        let err = super::validate_route_references(&routes, &sinks)
            .expect_err("an unreferenced named sink must be refused");
        let message = err.to_string();
        assert!(message.contains("forgotten"), "{message}");
        assert!(!message.contains("archive"), "{message}");
    }

    #[test]
    fn a_route_naming_a_missing_sink_is_refused_before_anything_is_built() {
        let sinks = vec![stdout_sink("archive")];
        let routes = vec![route("public.*", "arcive")];

        let err =
            super::validate_route_references(&routes, &sinks).expect_err("typo must be refused");
        assert!(err.to_string().contains("arcive"), "{err}");
    }

    /// Two routes cannot share one binding, and before this check the second route
    /// silently lost its sink to the first: `named_sink_map.remove` had already consumed
    /// it, so the pipeline reported "unknown sink" for a sink that was plainly declared.
    #[test]
    fn two_routes_referencing_one_sink_are_refused_with_the_real_reason() {
        let sinks = vec![stdout_sink("archive")];
        let routes = vec![route("public.*", "archive"), route("audit.*", "archive")];

        let err =
            super::validate_route_references(&routes, &sinks).expect_err("sharing must be refused");
        let message = err.to_string();
        assert!(message.contains("exactly one route"), "{message}");
    }

    /// A sink must preflight the topics *it* will be asked for.
    ///
    /// Giving every sink the pipeline's whole table list would make a named Kafka sink
    /// behind `table_pattern = "public.orders"` demand a topic for `billing.invoices` —
    /// a table routed somewhere else — and fail startup over a topic that will never
    /// receive an event.
    #[test]
    fn each_sink_preflights_only_the_tables_its_route_claims() {
        let assignment = super::assign_known_tables(
            concrete(&["public.orders", "public.customers", "billing.invoices"]),
            &[route("public.orders", "a"), route("billing.*", "b")],
        );

        let names = |tables: &[QualifiedTable]| {
            tables
                .iter()
                .map(QualifiedTable::display)
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&assignment.per_route[0]), vec!["public.orders"]);
        assert_eq!(names(&assignment.per_route[1]), vec!["billing.invoices"]);
        // Everything no route claims falls to the default sink, exactly as the router
        // itself falls through.
        assert_eq!(names(&assignment.default), vec!["public.customers"]);
    }

    /// First match wins, in both the router and this split. A later route that also
    /// matches must not steal the table from the earlier one.
    #[test]
    fn the_table_split_follows_the_routers_first_match_wins_order() {
        let assignment = super::assign_known_tables(
            concrete(&["public.orders"]),
            &[route("public.orders", "a"), route("public.*", "b")],
        );
        assert_eq!(
            assignment.per_route[0]
                .iter()
                .map(QualifiedTable::display)
                .collect::<Vec<_>>(),
            vec!["public.orders"]
        );
        assert!(assignment.per_route[1].is_empty());
        assert!(assignment.default.is_empty());
    }

    /// `table_include_list` takes glob patterns, so it cannot be rendered into topic
    /// names wholesale. Only its concrete entries contribute.
    /// `table_include_list` takes glob patterns, so it cannot be rendered into topic
    /// names wholesale — and a patterned entry must not silently become a table.
    #[test]
    fn a_glob_entry_contributes_no_preflight_table_and_reaches_no_route() {
        let assignment = super::assign_known_tables(
            // `concrete` applies the same filter the config path does.
            concrete(&["public.orders", "public.tmp_*"]),
            &[route("public.*", "a")],
        );
        assert_eq!(
            assignment.per_route[0]
                .iter()
                .map(QualifiedTable::display)
                .collect::<Vec<_>>(),
            vec!["public.orders"]
        );
    }

    #[test]
    fn a_routeless_pipeline_with_no_named_sinks_is_accepted() {
        super::validate_route_references(&[], &[]).expect("the single-sink shape stays valid");
    }

    /// The regression this whole side channel exists for.
    ///
    /// The scrape path used to read counters back through `TableRouter`, whose
    /// `SinkAdapter::delivery_metrics()` carries four generic fields. Thirty families —
    /// every HTTP status class, both retry histograms, pending bytes, the Iceberg
    /// counters, Kafka OAUTHBEARER token health — have no room in those four and rendered
    /// a constant zero for the life of the process. Four shipped alert rules watched four
    /// of them and could never fire.
    ///
    /// The assertion is deliberately on `http_requests_total`, which is **not**
    /// representable in `rustcdc::sink::SinkDeliveryMetrics`: reverting to the router path
    /// puts it back to zero and fails this test. Asserting on `retries_total` would not —
    /// that one field did survive.
    #[tokio::test]
    async fn extended_sink_counters_reach_the_registry_through_the_router() {
        use rustcdc::core::{Event, Operation, SourceMetadata};
        use rustcdc::sink::SinkAdapter as _;

        // Port 1 on loopback is reserved and never listening, so the request is refused
        // immediately and the sink's own accounting records a real attempt.
        let unreachable: SinkConfig = serde_json::from_value(serde_json::json!({
            "type": "http",
            "url": "http://127.0.0.1:1/events",
            "batch_max_events": 1,
            "max_retries": 0,
            "backoff_initial_ms": 1,
            "backoff_max_ms": 1,
        }))
        .expect("http sink config");

        let binding =
            crate::sink::build_binding(&unreachable, &crate::sink::SinkBuildContext::new(1 << 20))
                .await
                .expect("http binding");
        let mut registry = super::SinkMetricsRegistry::default();
        registry.register(binding.metrics_handle());

        assert_eq!(
            registry.snapshot().http_requests_total,
            0,
            "nothing has been sent yet"
        );

        // Everything from here on goes through `BoxedSink`, exactly as the run loop does.
        let mut router = crate::pipeline::router::single(binding);
        let event = Event::builder("orders", Operation::Insert)
            .after(serde_json::json!({"id": 1}))
            .source(SourceMetadata::new("postgres", "0/16B6A70", 1))
            .ts(1)
            .schema("public")
            .primary_key(["id"])
            .build();
        let _ = router.send(&event).await;
        let _ = router.flush().await;

        let snapshot = registry.snapshot();
        assert!(
            snapshot.http_requests_total > 0,
            "the HTTP request counter must survive the BoxedSink erasure: {snapshot:?}"
        );
        // `max_retries = 0`, so the first attempt is also the last. Classification used to
        // live inside the `attempt < max_retries` branch, which meant the attempt that
        // actually failed the batch was never counted — at `max_retries = 0` that was
        // every attempt, and all eight status/error-class families stayed at zero however
        // hard the endpoint failed.
        assert_eq!(
            snapshot.retryable_error_other_total, 1,
            "the give-up attempt is an observation and must be classified: {snapshot:?}"
        );
    }
}
