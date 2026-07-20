//! Table-level event routing.
//!
//! Since rustcdc 0.6.0 the full `TableRouter<S>` implementation — including
//! all capability-hook delegation — lives in the upstream crate.  This module
//! re-exports the relevant types and exposes `AppError`-returning builder
//! helpers that binding.rs uses.
//!
//! ## Design
//!
//! We use `HeterogeneousTableRouter` (= `TableRouter<BoxedSink>`) so that
//! different route patterns can point to different concrete sink types (Kafka,
//! HTTP, Iceberg, …) without a manual unification enum.
//!
//! The local `SinkBinding` type is wrapped in a `BoxedSink` at build time;
//! from that point all routing, lifecycle, and capability-hook delegation is
//! handled by rustcdc's `TableRouter<BoxedSink>` implementation.

use rustcdc::sink::{BoxedSink, SinkAdapter};

use crate::error::AppError;
use crate::sink::{SinkBinding, SinkDeliveryMetrics};

// Re-export so command modules can refer to `TableRouter` without importing rustcdc directly.
pub use rustcdc::{table_matches, HeterogeneousTableRouter as TableRouter};

// ─────────────────────────────────────────────────────────────────────────────
// Builder helpers
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// Command-layer helpers
// ─────────────────────────────────────────────────────────────────────────────
//
// These thin wrappers adapt the SinkAdapter trait API (rustcdc::core::Error) to
// the AppError boundary used by the commands layer, without requiring every
// command module to import the SinkAdapter trait directly.

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

/// Snapshot of delivery counters from all sinks in the router.
///
/// Returns the cdc-server-local `SinkDeliveryMetrics` populated from the
/// generic `SinkAdapter::delivery_metrics()` fields available through the
/// abstract routing layer.  HTTP-specific histogram counters are not available
/// through this path; they are tracked internally by `HttpSink`.
pub fn delivery_metrics(router: &TableRouter) -> SinkDeliveryMetrics {
    router
        .delivery_metrics()
        .map(|m| SinkDeliveryMetrics {
            retries_total: m.events_retried,
            ..SinkDeliveryMetrics::default()
        })
        .unwrap_or_default()
}
