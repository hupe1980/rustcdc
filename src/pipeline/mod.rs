//! Pipeline assembly utilities for connecting CDC sources to downstream sinks.
//!
//! This module provides higher-level plumbing on top of the core [`SinkAdapter`] and
//! [`Transform`] traits. The primary component is [`TableRouter`], which routes each
//! incoming CDC event to one of several named sinks based on configurable glob patterns.
//!
//! # Why `TableRouter`?
//!
//! Many production CDC deployments fan events out to different downstream systems
//! depending on the source table.  For example:
//!
//! - `public.orders` → Kafka topic `orders-cdc`
//! - `public.audit_*` → a dedicated audit sink
//! - `*` → a dead-letter / fallback sink
//!
//! `TableRouter` encodes that logic declaratively, removing the need to write
//! boilerplate `match`/`if` chains in user code.
//!
//! # Example
//!
//! ```rust,no_run
//! use rustcdc::pipeline::{TableRouter, TableRoute};
//! use rustcdc::sink::MemorySinkAdapter;
//!
//! // Build a router that sends orders to one sink and everything else to another.
//! let router: TableRouter<MemorySinkAdapter> = TableRouter::builder("my-router")
//!     .route("public.orders", MemorySinkAdapter::new("orders"))
//!     .route("public.products", MemorySinkAdapter::new("products"))
//!     .default(MemorySinkAdapter::new("fallback"))
//!     .build()
//!     .expect("valid patterns");
//! ```
//!
//! [`SinkAdapter`]: crate::sink::SinkAdapter
//! [`Transform`]: crate::transform::Transform

pub mod router;

pub use router::{
    table_matches, HeterogeneousTableRouter, TableRoute, TableRouter, TableRouterBuilder,
};
