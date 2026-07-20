//! Pipeline assembly: build a [`TableRouter`] from configuration.

use crate::config::schema::AppConfig;
use crate::error::AppError;
use crate::pipeline::router::{self, TableRouter};
use crate::sink;

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
pub async fn build_router(config: &AppConfig) -> Result<TableRouter, AppError> {
    // Always build the default sink binding.
    let default_binding = sink::build_binding(&config.sink).await?;

    if config.pipeline.routes.is_empty() {
        return Ok(router::single(default_binding));
    }

    // Build named sink bindings and validate route references.
    let mut named_sink_map: std::collections::HashMap<String, crate::sink::SinkBinding> =
        std::collections::HashMap::with_capacity(config.sinks.len());
    for named in &config.sinks {
        let built = sink::build_binding(&named.sink).await?;
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

    router::with_routes(default_binding, routes)
}
