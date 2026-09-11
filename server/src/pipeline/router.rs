//! Table-level event routing.
//!
//! Routing, lifecycle and capability-hook delegation all live upstream in
//! `TableRouter<BoxedSink>`; this module adds the `AppError`-returning builders and the
//! thin wrappers that keep `SinkAdapter` out of the command modules. Boxing the sink is
//! what lets different route patterns point at different concrete sink types without a
//! unification enum.

use rustcdc::sink::{BoxedSink, SinkAdapter};

use crate::error::AppError;
use crate::sink::SinkBinding;

// Re-export so command modules can refer to `TableRouter` without importing rustcdc directly.
pub use rustcdc::{HeterogeneousTableRouter as TableRouter, table_matches};

/// Create a no-routing router wrapping a single sink binding.
pub fn single(sink: SinkBinding) -> TableRouter {
    TableRouter::builder("pipeline")
        .default(BoxedSink::new(sink))
        .build_unchecked()
}

/// Create a router with explicit routing rules.
///
/// `routes` is a list of `(glob_pattern, named_sink)` pairs in evaluation
/// order.  `default` receives events that match no pattern.
///
/// Returns a configuration error if any pattern is empty or duplicate.
pub fn with_routes(
    default: SinkBinding,
    routes: Vec<(String, SinkBinding)>,
) -> Result<TableRouter, AppError> {
    let mut builder = TableRouter::builder("router").default(BoxedSink::new(default));
    for (pattern, binding) in routes {
        builder = builder.route(pattern, BoxedSink::new(binding));
    }
    builder.build().map_err(AppError::Runtime)
}

// Adapt the `SinkAdapter` API (`rustcdc::core::Error`) to the command layer's `AppError`.

/// Encode and deliver `event` via the routing table.
///
/// Equivalent to `router.send(event).await` with error mapped to `AppError`.
pub async fn send_event(
    router: &mut TableRouter,
    event: &rustcdc::core::Event,
) -> Result<(), AppError> {
    router.send(event).await.map_err(AppError::Runtime)
}

/// Validate sink connectivity before the pipeline is marked ready.
///
/// Equivalent to `router.preflight_check().await` with error mapped to `AppError`.
pub async fn preflight_check(router: &mut TableRouter) -> Result<(), AppError> {
    router.preflight_check().await.map_err(AppError::Runtime)
}

// Delivery counters are **not** read through the router.
//
// `TableRouter<BoxedSink>` exposes `SinkAdapter::delivery_metrics()`, which carries four
// generic fields. This server exports thirty families that have no room in those four —
// HTTP status classes, batch-size and retry-delay histograms, pending bytes, Iceberg
// orphaned files and lock contention, Kafka OAUTHBEARER token health. A helper here used
// to map the one field that survived into a local struct and leave the rest at their
// defaults, so those thirty families rendered a constant zero and the four alert rules
// watching them could never fire.
//
// The counters now travel out through `crate::sink::SinkMetricsRegistry`, whose handles
// are collected in `pipeline::binding` while each binding is still concrete. See that
// module for the full argument.
