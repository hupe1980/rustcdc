//! Fault-injection helpers for robustness and recovery testing.
//!
//! # Safety Guard
//!
//! The `test-harnesses` feature intentionally enables fault-injection, mock sources,
//! and other non-production helpers. Enabling it in an optimised (release) build is
//! almost certainly a mistake and could silently ship test-only code paths into
//! production.

// Reject accidental release-profile use of test-harnesses.
#[cfg(all(feature = "test-harnesses", not(debug_assertions)))]
compile_error!(
    "The `test-harnesses` feature must not be enabled in release builds. \
     Remove it from your production feature set, or scope it to \
     `[profile.test]` / `[profile.dev]` in Cargo.toml only."
);

/// Checkpoint-store fault injection.
pub mod checkpoint;
pub mod crash;
/// Data-loss detection over a captured event stream.
pub mod data_loss;
/// Source-connector fault injection.
pub mod source;

pub use checkpoint::{CheckpointFault, FaultInjectingCheckpoint};
pub use crash::{CrashSimulationResult, CrashSimulationState, CrashSimulationValidator};
pub use data_loss::{DataLossReport, DataLossValidator};
pub use source::{FaultInjectingSource, SourceFault};
