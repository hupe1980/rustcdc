//! The capture pipeline's engine: poll a batch, transform it, deliver it, checkpoint it.
//!
//! # Why this is not in `commands/`
//!
//! These eight modules lived in `src/commands/` under `run_*` names, alongside the ten CLI
//! entry points. They are not commands. `rustcdc run` is a command; the select loop, the
//! prepare/deliver pipeline, the metrics accumulator, the recovery state machine and the
//! checkpoint-transaction reconciler are the machine that command starts, and they are also
//! reached by `dry-run` and `replay`.
//!
//! Nineteen files in `commands/`, of which eight were engine internals, made the directory
//! unreadable as a list of what the binary can do. Worse, the flat `run_` prefix produced
//! three names that cannot be told apart — `run_batch`, `run_loop`, `run_loop_batch` — for
//! three genuinely different things.
//!
//! The split *itself* was never gratuitous: combined these are ~5 700 lines, well past the
//! 3 700-line budget `tests/architecture.rs` enforces, and `metrics.rs` alone is 2 900. What
//! was wrong was the location and the naming, so both changed:
//!
//! | was | is | what it actually does |
//! |---|---|---|
//! | `run_loop.rs` | [`event_loop`] | the `select!` over shutdown, admin exit and `poll_event_batch` |
//! | `run_loop_batch.rs` | [`batch_commit`] | one polled batch: barrier, retry policy, checkpoint, commit |
//! | `run_batch.rs` | [`batch`] | the prepare/deliver pipeline *within* a batch |
//! | `run_metrics.rs` | [`metrics`] | the accumulator and the Prometheus render |
//! | `run_recovery.rs` | [`recovery`] | backoff and the circuit breaker |
//! | `run_reconciliation.rs` | [`reconciliation`] | the checkpoint-transaction marker |
//! | `run_lifecycle.rs` | [`lifecycle`] | terminal outcomes and shutdown finalisation |

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
pub(crate) mod reconciliation;
pub(crate) mod recovery;

/// Crash-injecting end-to-end proof of the delivery contracts.
///
/// Lives beside the engine it exercises rather than in `tests/`, because it drives
/// `batch_commit::handle_polled_batch` directly — the production entry point — with a
/// fault-injecting checkpoint store underneath. An integration test could only reach it
/// through a whole process.
#[cfg(test)]
mod delivery_contract_tests;
