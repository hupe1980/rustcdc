+++
title = "Reliability testing"
description = "Deterministic replay, fault injection and crash simulation for verifying a rustcdc pipeline."
weight = 130
+++

This guide documents the rustcdc reliability validation toolchain and how to use it in CI and local development.

## Audience

- Runtime maintainers validating correctness and regression safety
- Connector maintainers extending source behavior
- Integrators who want deterministic, repeatable failure testing

## Coverage Areas

The reliability stack is split into three complementary layers:

1. Deterministic replay (`rustcdc::deterministic_replay`) for protocol-level regression checks.
2. Fault injection (`rustcdc::fault_injection`) for crash and error-path validation.
3. Adapter conformance harness (`rustcdc::testkit`) for sink lifecycle contract checks.

Use all three layers together for high confidence before releases.

## Deterministic Replay

### Purpose

Deterministic replay verifies that canonical event interpretation stays stable across parser and envelope changes.

### Key Types

- `Fixture`, `FixtureMetadata`, `FixtureVersioning`
- `ReplaySession`, `ReplayResult`, `ReplayEvent`
- `semantic_diff`, `EventDiff`, `DiffLevel`

### Typical Workflow

1. Capture protocol-level fixtures from real source traffic.
2. Replay fixtures into canonical events with `ReplaySession`.
3. Compare against golden output using semantic diff.
4. Fail CI on high-severity semantic drift.

### Why Semantic Diff

Semantic diff intentionally ignores noisy changes (for example field ordering) and highlights behavior regressions (for example operation/type/key changes).

## Fault Injection

### Purpose

Fault injection exercises code paths that are difficult to hit with live systems alone, including checkpoint failures and simulated process crashes.

### Key Components

- `FaultInjectingSource`, `SourceFault`
- `FaultInjectingCheckpoint`, `CheckpointFault`
- `CrashSimulationValidator`, `CrashSimulationResult`, `CrashSimulationState`
- `DataLossValidator`, `DataLossReport`

### Recommended Scenarios

1. Inject transient checkpoint save failures and verify recovery with no silent data loss.
2. Inject source stream errors and verify retry policy behavior.
3. Simulate crash/restart cycles around commit boundaries and validate replay correctness.

### Suite Classification

- Synthetic coverage: `tests/crash_recovery_model.rs`, `tests/data_loss_detection.rs`
- Live connector/process-kill coverage: `tests/runtime_postgres_process_crash_integration.rs`, `tests/runtime_mysql_process_crash_integration.rs`, `tests/runtime_mariadb_process_crash_integration.rs`, `tests/runtime_sqlserver_process_crash_integration.rs`
- Live process-kill suites require Docker and the `CDC_RS_RUN_DOCKER_TESTS=1` gate.

### Guarantee Boundaries

- Synthetic suites validate internal state-transition and recovery invariants under modeled faults.
- Synthetic suite pass status is not a substitute for OS-level process-kill restart validation.
- Process-kill suites validate restart behavior across real process termination boundaries.
- Production readiness claims for crash recovery must reference both synthetic and process-kill evidence.

## Adapter Conformance Harness

`rustcdc::testkit` provides a reference `SinkAdapter` contract test suite.

### Key Types

- `SinkAdapter`
- `AdapterConformanceSuite`
- `BasicAdapterConformance`
- `MemorySinkAdapter`

The conformance suite runs all baseline scenarios (`single_event`, `batch_send`,
`ordering`, `crash_recovery`) through `AdapterConformanceSuite::run_all`.

### Minimum CI Gate For New Adapters

1. Run the conformance suite with single-event, batch, ordering, and crash-recovery fixtures.
2. Add at least one fixture asserting idempotent handling for duplicate deliveries.
3. Record conformance failures as release blockers.

## CI Integration Pattern

A practical CI strategy is:

1. Fast path on every PR:
   - Deterministic replay golden fixture validation
   - Adapter conformance tests for touched adapters
2. Nightly path:
   - Fault injection soak matrix
   - Longer crash-recovery simulations

## Local Validation Commands

```bash
cargo test deterministic_replay_golden_fixtures
cargo test fault_injection_soak_matrix
cargo test runtime_postgres_process_crash_integration
cargo test runtime_mysql_process_crash_integration --features mysql --bins
cargo test runtime_mariadb_process_crash_integration --features mariadb --bins
cargo test runtime_sqlserver_process_crash_integration --features sqlserver --bins
cargo test data_loss_detection
```

## Best Practices

1. Keep fixture corpora versioned and reviewed like code.
2. Prefer deterministic fixtures over timing-sensitive end-to-end tests for parser regressions.
3. Use fault injection to validate observability signals, not only functional outcomes.
4. Treat data-loss and commit-barrier regressions as release-blocking defects.

## Feature Gate: `test-harnesses`

The `test-harnesses` feature exposes `rustcdc::testkit`, `CrashSimulationValidator`,
`DataLossValidator`, and related types that are only safe to use in test and
validation environments.

A compile-time guard prevents `test-harnesses` from being active in standard
release builds:

```rust
#[cfg(all(feature = "test-harnesses", not(debug_assertions)))]
compile_error!("...");
```

> **Edge case:** This guard relies on the `debug_assertions` cfg flag, which is
> `false` in standard `--release` builds.  A custom Cargo profile that sets
> `debug-assertions = true` in release mode (for example, a `[profile.release-dbg]`
> profile) will bypass the guard and allow `test-harnesses` to be compiled into a
> release artifact.  If your project uses custom profiles, audit them to ensure
> `debug-assertions` is `false` before shipping.

## Benchmark evidence

Benchmarks are produced by `scripts/ci-benchmark-gate.sh`. They are Criterion microbenchmarks
over the transform and codec paths with **no connector I/O**, so they are a regression signal
for in-process work — not an end-to-end throughput or latency claim. End-to-end capture
latency comes from the separate Docker-backed latency harness described under
[Coverage Areas](#coverage-areas).

A local run is classified as non-release evidence:

```bash
bash scripts/ci-benchmark-gate.sh
```

Release-grade classification requires commit-pinned metadata and a named Criterion baseline,
so that a reported delta is between two known trees rather than between a tree and whatever
happened to be cached:

```bash
BENCHMARK_STRICT=1 \
BENCHMARK_MAX_REGRESSION_PERCENT=5 \
BENCHMARK_BASELINE_COMMIT="$(git rev-parse HEAD)" \
BENCHMARK_BASELINE_ARTIFACT="commit:$(git rev-parse HEAD)" \
CRITERION_BASELINE="ci-baseline" \
bash scripts/ci-benchmark-gate.sh
```

Use the same `CRITERION_BASELINE=ci-baseline` value in CI and locally, or the two are not
comparing against the same reference. Regenerate `BENCHMARK_REPORT.md` on a clean tree before
citing any number from it.

## Related Documentation

- [API Guide](@/docs/api.md)
- [Architecture](@/docs/architecture.md)
- [Operator Runbook](@/docs/runbook.md)
- [Troubleshooting Guide](@/docs/troubleshooting.md)
