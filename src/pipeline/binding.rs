//! Pipeline assembly: build a [`TableRouter`] from configuration.

use crate::config::schema::AppConfig;
use crate::error::AppError;
use crate::pipeline::router::{self, TableRouter};
use crate::sink::{self, KafkaTransactionHandle};

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
/// * A glob pattern in a route is syntactically invalid.
pub async fn build_router(config: &AppConfig) -> Result<BuiltRouter, AppError> {
    // Always build the default sink binding.
    let default_binding = sink::build_binding(&config.sink, config.runtime.max_event_bytes).await?;

    if config.pipeline.routes.is_empty() {
        let transaction_handle = default_binding.transaction_handle();
        return Ok(BuiltRouter {
            router: router::single(default_binding),
            transaction_handle,
        });
    }

    // Build named sink bindings and validate route references.
    let mut named_sink_map: std::collections::HashMap<String, crate::sink::SinkBinding> =
        std::collections::HashMap::with_capacity(config.sinks.len());
    for named in &config.sinks {
        let built = sink::build_binding(&named.sink, config.runtime.max_event_bytes).await?;
        named_sink_map.insert(named.name.clone(), built);
    }

    // Compile routes in order, consuming named sink bindings.
    let mut routes: Vec<(String, crate::sink::SinkBinding)> =
        Vec::with_capacity(config.pipeline.routes.len());

    for route in &config.pipeline.routes {
        let binding = named_sink_map.remove(&route.sink).ok_or_else(|| {
            AppError::Config(Box::new(crate::error::ConfigError::InvalidState(format!(
                "pipeline route references unknown sink {:?}; \
                 add a [[sinks]] entry with that name",
                route.sink
            ))))
        })?;
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
    })
}
