+++
title = "Comparison & scope"
description = "How rustcdc compares to Debezium, Supabase etl, go-mysql and Flink CDC, and which capabilities are explicit non-goals."
weight = 150
+++

This document defines how rustcdc evaluates feature completeness against other embeddable CDC libraries, while explicitly excluding full platform/daemon expectations.

## Purpose

Use this matrix to answer:
- Is rustcdc complete enough for library use in production?
- Which missing features are true gaps versus intentional non-goals?

## Baseline Comparison Set

Primary comparators (library/protocol-level):
- Debezium Engine (embedded Java mode)
- go-mysql (Go)
- python-mysql-replication (Python)
- pglogrepl (Go)
- wal2json (PostgreSQL output plugin in C)

Out-of-scope comparators for parity gating:
- Managed CDC platforms and control planes
- Standalone daemons as end-state products (for example, Maxwell)

## Scoring Model

Each capability is assigned one of:
- Must-have: required for library-grade release confidence
- Should-have: materially improves integrator ergonomics and operability
- Non-goal: intentionally outside embedded-library scope

Status values:
- Implemented
- Partial
- Missing
- Non-goal

## Must-Have Capabilities (Release Gate)

| Capability | Why this is mandatory for a library | rustcdc status | Evidence |
|---|---|---|---|
| Multi-source CDC capture (Postgres/MySQL/SQL Server) | Core value proposition of unified library surface | Implemented | src/source/, src/lib.rs |
| Snapshot + streaming handoff semantics | Prevents data-loss windows during bootstrap | Implemented | src/core/runtime.rs, tests/*snapshot* |
| Ack/checkpoint commit barrier semantics | Supports at-least-once delivery discipline in embedders | Implemented | src/core/runtime.rs |
| Crash/restart correctness validation | Ensures resume and offset safety after failure | Implemented | tests/crash_recovery_model.rs, tests/data_loss_detection.rs, tests/runtime_postgres_process_crash_integration.rs, tests/runtime_mysql_process_crash_integration.rs, tests/runtime_sqlserver_process_crash_integration.rs |
| Deterministic replay and fault-injection coverage | Reproducible correctness verification under adverse paths | Implemented | src/deterministic_replay/, src/fault_injection/, tests/fault_injection_soak_matrix.rs |
| Capability reporting matches connector behavior | Prevents control-plane and operational misconfiguration | Implemented | src/core/runtime.rs, src/source/postgres.rs, src/source/mysql.rs, src/source/sqlserver.rs |
| Public docs/API contract aligned with implementation | Prevents integration failures caused by stale guidance | **Implemented** — every Rust block in `README.md` and `docs/{api,config_reference,getting_started,adapter_sdk,schema_evolution}.md` is compiled and run by `cargo test --doc --all-features`, gated in CI. Wiring this up surfaced 36 broken samples out of 96 (wrong field names, methods moved between types, an `Event` literal missing two fields); all are fixed. Blocks that cannot run are marked `ignore` with a stated reason. | `markdown_doctests` in src/lib.rs, .github/workflows/ci.yml |

## Should-Have Capabilities

| Capability | Why it matters | rustcdc status | Evidence |
|---|---|---|---|
| Durable schema history backend beyond in-memory | Improves restart durability for long-lived deployments | Implemented | src/schema_history/mod.rs |
| Runtime health/admin introspection depth | Faster incident response and safer operations | Implemented | src/core/runtime.rs |
| Structured observability (metrics/tracing/logging) | Production diagnosis and SLO ownership | Implemented | tests/otel_metrics_integration.rs, tests/otel_tracing_integration.rs, tests/logging_structured.rs |
| Built-in field mapping transform primitives (copy/rename/set/remove) | Covers common schema-alignment workloads without mandatory custom/WASM code | Implemented | src/transform/field_mapping.rs, src/transform/mod.rs, site/content/docs/config-reference.md |
| Example/build matrix across sources | Prevents connector-specific integration drift | Implemented | .github/workflows/ci.yml, scripts/ci-policy-gate.sh, examples/ |
| Connector version-compatibility test depth | Reduces production surprises on engine upgrades | Implemented (connector-specific depth varies) | tests/postgres_version_matrix.rs |
| Resume-coordinate correctness under non-default server options | The coordinate a checkpoint records is only as good as the server option it was captured under, and the defaults hide the failures | Implemented | tests/mysql_binlog_compression_integration.rs (`binlog_transaction_compression = ON`), tests/sqlserver_window_truncation_integration.rs (two capture instances, truncated window), src/checkpoint/mod.rs (`stream_position_regression`) |

## Intentional Non-Goals (Do Not Gate Library Releases)

| Capability | Classification rationale |
|---|---|
| Managed SaaS control plane and hosted UI | Service/platform concern, outside embeddable crate boundary |
| Turnkey sink ecosystem with hundreds of connectors | Platform distribution concern; library exposes traits/APIs instead |
| Full orchestration and fleet management | Application/platform responsibility |

## Known Architectural Limits

These are not gaps against the comparison set so much as consequences of the Rust ecosystem, and
they belong in an honest matrix because they shape the operational envelope:

| Limit | Consequence | Why it stands |
|---|---|---|
| PostgreSQL decoding goes through `pg_logical_slot_peek_binary_changes`, not the streaming replication protocol (`START_REPLICATION`) | Each poll re-reads WAL from the slot's `restart_lsn`, so a long-running transaction on the source that pins `restart_lsn` far behind `confirmed_flush_lsn` makes every poll re-scan that gap. Latency is also bounded by the poll interval rather than pushed. | `tokio-postgres` exposes no `CopyBoth` / replication-mode API, and no published crate supplies one for it. Comparators in other languages (Debezium via the PGJDBC replication API, pglogrepl, wal2json consumers) all use the streaming protocol. Mitigated, not solved: the poll window halves on each timeout so a saturated server still makes forward progress instead of stalling. |
| SQL Server capture is polling-based | p99 latency ≈ `stream_poll_interval_ms` plus the capture agent's own delay | Inherent to SQL Server CDC; no log-reading interface exists. Do not compare its latency numbers against the log-based connectors. |
| MySQL binlog timestamps have whole-second resolution | Any lag figure derived from `SourceMetadata::timestamp` over-reports by up to 1 000 ms (median ~480 ms) | The binlog common header carries seconds. PostgreSQL and SQL Server both carry microsecond commit timestamps and are exact. |

## Current Completeness Verdict

For embedded-library scope, rustcdc is release-viable with conditions:
- Must-have set is implemented for the primary connector paths, and each release
	must confirm connector-specific restart evidence remains current.
- Should-have set is broadly implemented; remaining risk is concentrated in deployment-specific policy tuning and continuous evidence rigor.
- The architectural limits above are documented rather than closed, and a deployment whose
	workload sits against one of them (a PostgreSQL source with long-running transactions, a
	latency SLO below the SQL Server poll interval) should be evaluated against it explicitly.

## Release Decision Rules

Use these rules during audit and release gates:
1. Block release if any Must-have is Missing.
2. Block release if a Must-have is Partial and can cause incorrect runtime behavior or incorrect integration assumptions.
3. Do not block release on Non-goals.
4. Track Should-have items on roadmap unless they become reliability prerequisites.

## Governance And Update Cadence

Update this matrix:
- when adding a connector family
- when introducing a new runtime invariant
- when changing documented feature scope
- at each release planning cycle

Owners:
- Runtime maintainers: Must-have correctness rows
- Documentation maintainers: evidence links and status accuracy
- Release lead: final gate decision based on this matrix
