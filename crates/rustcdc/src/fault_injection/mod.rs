//! Fault-injection helpers for robustness and recovery testing.
//!
//! # Safety Guard
//!
//! The `test-harnesses` feature intentionally enables fault-injection, mock sources,
//! and other non-production helpers. Enabling it in an optimised (release) build is
//! almost certainly a mistake and could silently ship test-only code paths into
//! production.

// Reject accidental release-profile use of test-harnesses.
//
// `debug_assertions` is a proxy for "optimised build", and it is the only signal
// available — but it cannot tell a shipped binary from a benchmark, and benchmarks are a
// legitimate optimised consumer. `rustcdc-server`'s benches reach this through their
// dev-dependency on `rustcdc/test-harnesses`, so without an escape hatch the only way to
// build them was `[profile.bench] debug-assertions = true` at the workspace root — which
// applies to *every* member, and quietly turns assertions on inside the library's own
// throughput benchmark, the one number an operator quotes.
//
// The hatch is deliberately awkward and deliberately explicit:
//
//     RUSTFLAGS='--cfg rustcdc_optimised_test_harnesses' cargo bench
//
// It cannot be reached by adding a feature to a dependency list, which is the accident
// this guard exists to catch.
#[cfg(all(
    feature = "test-harnesses",
    not(debug_assertions),
    not(rustcdc_optimised_test_harnesses)
))]
compile_error!(
    "The `test-harnesses` feature must not be enabled in release builds. \
     Remove it from your production feature set, or scope it to \
     `[profile.test]` / `[profile.dev]` in Cargo.toml only. \
     To benchmark code that legitimately needs the harnesses, build with \
     RUSTFLAGS='--cfg rustcdc_optimised_test_harnesses'."
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
