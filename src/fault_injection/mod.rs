//! Fault-injection helpers for robustness and recovery testing.

pub mod checkpoint;
pub mod crash;
pub mod data_loss;
pub mod source;

pub use checkpoint::{CheckpointFault, FaultInjectingCheckpoint};
pub use crash::{CrashSimulationResult, CrashSimulationState, CrashSimulationValidator};
pub use data_loss::{DataLossReport, DataLossValidator};
pub use source::{FaultInjectingSource, SourceFault};
