//! The capture pipeline's engine: poll a batch, transform it, deliver it, checkpoint it. These
//! modules are what `rustcdc run`, `dry-run` and `replay` start; they are not commands.
//!
//! | module | what it does |
//! |---|---|
//! | `event_loop` | the `select!` over shutdown, admin exit and `poll_event_batch` |
//! | `batch_commit` | one polled batch: barrier, retry policy, checkpoint, commit |
//! | `batch` | the prepare/deliver pipeline *within* a batch |
//! | `metrics` | the accumulator and the Prometheus render |
//! | `recovery` | backoff and the circuit breaker |
//! | `reconciliation` | the checkpoint-transaction marker |
//! | `lifecycle` | terminal outcomes and shutdown finalisation |
//! | `park` | holding a pipeline whose source position is lost |

/// Batch prepare-and-deliver, `pub` so `benches/throughput.rs` can measure the real thing.
///
/// The pipeline's throughput is the number this project's premise rests on, and a benchmark
/// that reconstructs an approximation of this function measures the approximation. The
/// crate is `publish = false`, so this widens no external API.
pub mod batch;
pub(crate) mod batch_commit;
pub(crate) mod event_loop;
pub(crate) mod lifecycle;
pub(crate) mod metrics;
pub(crate) mod park;
pub(crate) mod reconciliation;
pub(crate) mod recovery;
pub mod row_shapes;

/// Crash-injecting end-to-end proof of the delivery contracts.
///
/// Lives beside the engine it exercises rather than in `tests/`, because it drives
/// `batch_commit::handle_polled_batch` directly — the production entry point — with a
/// fault-injecting checkpoint store underneath. An integration test could only reach it
/// through a whole process.
#[cfg(test)]
mod delivery_contract_tests;
