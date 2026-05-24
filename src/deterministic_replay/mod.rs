pub mod diff;
/// Deterministic replay framework for testing CDC protocol correctness without live databases.
///
/// This module provides infrastructure for:
/// - Capturing protocol-specific WAL/binlog fixtures
/// - Golden canonical event snapshots for regression testing
/// - Deterministic replay without live database connections
/// - Semantic diff tooling for envelope changes
pub mod fixtures;
pub mod replay;

pub use diff::{semantic_diff, DiffLevel, EventDiff};
pub use fixtures::{Fixture, FixtureMetadata};
pub use replay::{ReplayEvent, ReplayResult, ReplaySession};
