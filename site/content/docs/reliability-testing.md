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

### A fourth layer: resume-coordinate suites

The three layers above all exercise the runtime against a *cooperative* source, so none of them
can see a defect in the source's own resume coordinate — they supply that coordinate themselves.
That blind spot covers the worst failures a CDC library has: a checkpoint recording a position the
source cannot resume from, or one running ahead of the rows it describes. Both are silent.

The suites below therefore assert on the coordinate itself, against a real server, in the
configuration where it goes wrong:

| Suite | Configuration it needs | What it asserts |
|---|---|---|
| `mysql_binlog_compression_integration` | MySQL 8.0, `binlog_transaction_compression = ON`, file+position (not GTID) | Every event carries a resumable binlog position, and a stream resumed from one captured inside a compressed transaction receives what follows it |
| `sqlserver_window_truncation_integration` | SQL Server 2022, **two** capture instances, `max_events_per_poll = 5` | No row is lost when an LSN window truncates at different positions per capture instance |
| `postgres_snapshot_integration` (`…resumes_at_the_chunk_boundary_after_a_restart`) | PostgreSQL, incremental snapshot | A restart resumes at a chunk boundary rather than skipping or restarting the table |

Alongside them, `source::postgres::wire::tests` drives the replication client against an
**in-process fake server** over loopback. That covers the cases a real server cannot be asked
for: the TLS handshake (the containers run `ssl = off`), a message deliberately split across two
TCP writes so a poll budget expires mid-frame, a server that declines the TLS upgrade, and a
server that accepts the connection and then goes silent. Ten tests, no Docker, under half a
second — which is the argument for reaching for a fake server whenever the interesting cases are
*failures* rather than successes.

The rule for writing new ones: **the configuration is the test.** Each row above needs a
*non-default* server option or cardinality to fail at all — one capture instance instead of two,
compression off, a chunk that drains inside a single poll — and is silent under the forgiving
setting. When adding a connector or a resume path, ask which option your suite is holding at its
most permissive value, and pin the other one.

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

### Producing and regenerating goldens

A golden is the recorded answer for a fixture, generated from the replay engine itself:

```bash
UPDATE_GOLDENS=1 cargo test --test deterministic_replay_golden_fixtures
```

Two rules make that safe rather than circular.

**Regenerating is legitimate when the fixture changed or the envelope changed on purpose. It
is not the way to make a failing test pass.** Re-blessing a regression records the regression,
and the suite then defends it. The diff of the golden files is the entire review surface for
this suite — read it.

**A golden must record every field the envelope serializes.** `Event` fields carry
`#[serde(default)]`, so a golden recorded before a field existed still loads: the missing
field silently becomes `false`, `None` or empty, and the comparison passes whenever the
default happens to be the right answer. The suite is then agreeing with itself rather than
pinning anything.

That was live until 0.13.0. Forty of the forty-one goldens predated `before_is_key_only` and
did not record it; each loaded as `false`, which was correct for those fixtures, so nothing
failed — and nothing would have failed if a later field's default had been *wrong* for them.
The loader now compares each golden's keys against what the event actually serializes and
fails with "golden is stale, regenerate", so adding an envelope field forces a conscious
regeneration instead of a silent default. Fields with `skip_serializing_if` are legitimately
absent when empty, so the comparison is per event rather than against a fixed field list.

Envelope validation also runs **before** the regeneration branch, so a malformed event cannot
be blessed in the first place.

### Why Semantic Diff

Semantic diff intentionally ignores changes that differ every run and highlights behaviour
regressions. Comparing raw JSON instead would fail every fixture on wall-clock timestamps, and a
suite that fails for uninteresting reasons gets disabled.

### What it compares — and why the list matters more than it looks

`semantic_diff` is the **sole** comparison the golden-fixture suite performs. A field it does not
compare is therefore invisible to every fixture, however many fixtures there are. That is worth
stating plainly, because until 0.12.0 it was true of fields whose regressions are exactly what the
fixtures exist to catch: `primary_key`, `unavailable_columns`, `before_unavailable_columns`,
`envelope_version`, `source.offset`, `transaction` and `snapshot` were all unchecked. A change to
any of them left every golden green — including a regression that stopped reporting an
unchanged-TOAST column, which makes a sink write `NULL` over live data.

**Compared** — every field that is a deterministic function of the replayed input: `op`, `table`,
`schema`, `source.source_name`, `source.offset`, `before`, `after`, `before_is_key_only`,
`unavailable_columns`, `before_unavailable_columns`, `primary_key`, `envelope_version`,
`transaction`, and the chunk position within `snapshot`.

**Not compared**, each for a stated reason rather than by omission:

| Field | Why |
|---|---|
| `ts`, `source.timestamp` | Wall-clock at capture; differs every run by construction |
| `snapshot.snapshot_id` | Embeds the millisecond the snapshot began, so it differs per run while carrying no correctness meaning of its own |

A field added to `Event` in future belongs in one of those two lists. One that lands in neither is
a field the fixtures cannot see, so `every_deterministic_field_is_actually_compared` asserts each
compared field produces a diff when mutated, and `per_run_varying_fields_stay_ignored` asserts the
other two do not. All recorded goldens pass unchanged under the stricter comparison, which is what
shows the additions describe real behaviour rather than being tightened arbitrarily.

### A comparison is only as good as what the fixtures can express

Comparing a field is necessary but not sufficient. Until 0.12.0 the replay engine hardcoded
`before_is_key_only`, `unavailable_columns` and `before_unavailable_columns` to their empty
defaults, so **both sides of those comparisons were structurally always equal** — the fields had
the appearance of coverage and none of the substance, and an unchanged-TOAST regression was still
invisible.

The fixture payload now carries all three, and `postgres_unchanged_toast_v1` exercises them with
both cases in one file: a TOASTed column that was *not* modified (absent from `after`, named in
`unavailable_columns`, key-only before-image) and one that *was* (present in `after`, absent from
`before`, named in `before_unavailable_columns`). The second is why the two lists are never
merged — merging them marks a genuinely changed column as unwritable and silently drops the
update.

Absent fields mean "complete payload", so fixtures written before this are unaffected. A wrong
*shape* is rejected rather than ignored, because a silently-dropped `unavailable_columns` would
record a golden asserting the opposite of what its author wrote.

### Fixture integrity is checked on the path that loads files

`Fixture::validate` checks that a fixture has messages, that their sequence numbers are contiguous
from zero, that `message_count` agrees with the array, and that every payload has the shape its
message type requires. `ReplaySession::new` runs it, so nothing replays unvalidated.

The `message_count` check exists because these fixtures are hand-maintained JSON and a lost message
is otherwise invisible: replay produces fewer events, the golden is re-recorded to match, and the
scenario retires without a word. It is a checksum against truncation, not a restatement of
`messages.len()`.

It was previously named `expected_event_count` and checked only in `Fixture::new` — which
`from_path` and `from_json` do not call — so every fixture on disk carried an unverified number. The
name was also wrong: the count of *events* is not the count of *messages*, since an aborted
transaction discards its buffered events, and it was compared against the message count regardless.

`Fixture::new` returns `Result` rather than asserting, so a fixture-building tool reports a problem
instead of aborting.

### Every replayed event is validated, not just compared

Matching a golden is not the same as being correct: a golden recorded once from a malformed
envelope would be defended by the suite forever, because both sides share the malformation. Each
replayed event is run through `Event::validate()` before comparison, which enforces the
partial-payload rules — a column may not be both listed as unavailable and present in the payload,
and a key-only before-image may not also carry unavailable columns. All fixtures pass, so none was
pinning a contract violation.

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

# Resume-coordinate suites (need Docker; see the table above for why each configuration matters)
CDC_RS_RUN_DOCKER_TESTS=1 cargo test --features mysql --test mysql_binlog_compression_integration
CDC_RS_RUN_DOCKER_TESTS=1 cargo test --features sqlserver --test sqlserver_window_truncation_integration
CDC_RS_RUN_DOCKER_TESTS=1 cargo test --features postgres --test postgres_snapshot_integration
CDC_RS_RUN_DOCKER_TESTS=1 cargo test --features postgres --test postgres_wal_transport_parity_integration

# The fake replication server needs no Docker
cargo test --features postgres --lib wire::tests
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

## Measured performance

From `tests/postgres_latency_evidence.rs` and `tests/postgres_wal_transport_backlog_evidence.rs`
on one developer machine (PostgreSQL 16 in Docker, loopback). These are **relative** comparisons
on identical hardware and workload, not absolute claims — take the ratio as the signal and
re-measure before setting an SLO.

### Latency and throughput

| WAL transport | p50 | p95 | p99 | max | throughput |
|---|---|---|---|---|---|
| `StreamingReplication` (default) | 27.8 ms | 51.1 ms | 53.5 ms | 68.5 ms | **815 events/s** |
| `SqlPeek` | **12.7 ms** | 19.1 ms | 31.2 ms | 42.5 ms | 434 events/s |

Streaming carries roughly twice the throughput. **Peek shows a lower p50 on this workload** — it
polls in tight, small batches — and that is not hidden here because it is real. The gap is not a
batching artifact: re-running streaming with `max_events_per_poll = 50` made both metrics worse
(p50 50.6 ms, 561 events/s).

### Capture time with WAL behind the slot

Same capture, measured with 146 MiB and 292 MiB of WAL between the slot's `restart_lsn` and the
current end of WAL (the second achieved by holding a transaction open so `restart_lsn` cannot
advance):

| WAL transport | 146 MiB behind | 292 MiB behind |
|---|---|---|
| `StreamingReplication` | 58 ms | 39 ms |
| `SqlPeek` | 218 ms | 209 ms |

**`SqlPeek` is consistently 4–5× slower for identical work.** That is the measured, reproducible
difference, and it is the honest basis for preferring the default.

**What is *not* demonstrated:** that peek degrades further as the WAL behind the slot grows.
Doubling the distance did not slow it measurably. The re-scan itself is real —
`pg_logical_slot_get_changes_guts` calls `XLogBeginRead(reader, restart_lsn)` on every
invocation, so each poll reads from the slot's restart point — but at these volumes the WAL had
just been written and was served from page cache. Whether the mechanism becomes expensive on a
server where that WAL is cold, or evicted by other load, is **not** established by this harness.
Treat it as a mechanism to monitor (`pg_replication_slots.restart_lsn` against
`pg_current_wal_lsn()`), not as a quantified cost.

Reproduce:

```bash
CDC_RS_RUN_DOCKER_TESTS=1 cargo test --features postgres \
  --test postgres_latency_evidence --test postgres_wal_transport_backlog_evidence -- --nocapture
```

## Benchmark evidence

Benchmarks are produced by `scripts/ci-benchmark-gate.sh`. Most are Criterion microbenchmarks
over the transform and codec paths with **no connector I/O**, so they are a regression signal
for in-process work rather than a throughput claim. End-to-end capture latency comes from the
separate Docker-backed latency harness described under [Coverage Areas](#coverage-areas).

### End-to-end runtime throughput

One benchmark is not a microbenchmark. `cargo bench --bench throughput` drives the **whole
runtime** — source poll, idempotency guard, transform pipeline, sink, ack token, commit
barrier, durable checkpoint write — over a synthetic source, and reports events per second.

Database I/O is excluded deliberately. The figure is what the library costs *on top of*
whatever the server and the sink cost; a connector-inclusive number measured against a
container on a laptop would be a property of the laptop. This closes what previous audits
recorded as the last open evidence condition: *no end-to-end throughput measurement*.

Representative run — Apple M-series, Darwin, APFS on SSD, `--release`, one Tokio worker:

| Checkpoint store | Events per acknowledgement | Throughput |
|---|---|---|
| `InMemoryCheckpoint` | 1024 | ~1.33 M events/s |
| `InMemoryCheckpoint` | 64 | ~1.17 M events/s |
| `FileCheckpoint` | 1024 | ~90 K events/s |
| `FileCheckpoint` | 64 | ~6.5 K events/s |

**Read the ratio, not the absolutes.** A durable commit is two `fsync`s — the record, then the
directory holding the rename — so once the checkpoint is on disk, batch size rather than event
rate is the throughput knob: the same runtime moves roughly 13× more events per second at 1024
events per acknowledgement than at 64. The tuning lever is `max_buffer_size` and how often the
driver calls `commit_ack`, not the poll loop.

`fsync` is unusually expensive on macOS and cheaper on Linux; network storage is worse than
both. Re-run it on the hardware you intend to deploy on rather than quoting the table.

### The integration matrix, and what "evidence" means

```bash
bash scripts/run_full_integration_matrix_evidence.sh
```

Thirty-eight container suites across PostgreSQL, MySQL, MariaDB and SQL Server, plus the
reliability and latency gates. It writes `target/integration-full-matrix-evidence.txt`, which
is the artifact a release quotes.

**A suite that did not run is not a suite that passed.** An image-pull failure — a Docker Hub
rate-limit, a registry 404, a truncated layer — is CI infrastructure rather than a code
regression, so it is recorded as `STATUS: SKIP` rather than `FAIL`. Until 0.13.0 a skip was
also counted as a pass, which meant a registry outage during a release run could skip every
container suite and still print *"Full integration matrix completed successfully"* and exit 0:
the evidence artifact then certified a matrix that never ran.

Skips are now listed by name in the report and **fail the run**:

```text
Skipped suites (image pull failed — these produced NO evidence):
  - mariadb connection
Full integration matrix is INCOMPLETE: 1 suite(s) never ran.
```

Set `ALLOW_IMAGE_PULL_SKIPS=1` to accept a partial run for local iteration. The report says
plainly that such a run is not release evidence.

The transient is also mitigated rather than merely detected: CI warms every image the matrices
instantiate from a public mirror before the matrix starts, and the policy gate's
`run_relational_image_drift_check` fails the build if that warm list drifts from the versions
the tests actually use.

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
