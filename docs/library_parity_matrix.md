# Library Parity Matrix (Embeddable CDC Scope)

This document defines how cdc-rs evaluates feature completeness against other embeddable CDC libraries, while explicitly excluding full platform/daemon expectations.

## Purpose

Use this matrix to answer:
- Is cdc-rs complete enough for library use in production?
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

| Capability | Why this is mandatory for a library | cdc-rs status | Evidence |
|---|---|---|---|
| Multi-source CDC capture (Postgres/MySQL/SQL Server) | Core value proposition of unified library surface | Implemented | src/source/, src/lib.rs |
| Snapshot + streaming handoff semantics | Prevents data-loss windows during bootstrap | Implemented | src/core/runtime.rs, tests/*snapshot* |
| Ack/checkpoint commit barrier semantics | Supports at-least-once delivery discipline in embedders | Implemented | src/core/runtime.rs |
| Crash/restart correctness validation | Ensures resume and offset safety after failure | Implemented | tests/crash_simulation_integration.rs, tests/data_loss_detection.rs, tests/runtime_postgres_process_crash_integration.rs, tests/runtime_mysql_process_crash_integration.rs, tests/runtime_sqlserver_process_crash_integration.rs |
| Deterministic replay and fault-injection coverage | Reproducible correctness verification under adverse paths | Implemented | src/deterministic_replay/, src/fault_injection/, tests/fault_injection_soak_matrix.rs |
| Capability reporting matches connector behavior | Prevents control-plane and operational misconfiguration | Implemented | src/core/runtime.rs, src/source/postgres.rs, src/source/mysql.rs, src/source/sqlserver.rs |
| Public docs/API contract aligned with implementation | Prevents integration failures caused by stale guidance | Implemented | docs/api.md, docs/schema_evolution.md, docs/config_reference.md |

## Should-Have Capabilities

| Capability | Why it matters | cdc-rs status | Evidence |
|---|---|---|---|
| Durable schema history backend beyond in-memory | Improves restart durability for long-lived deployments | Implemented | src/schema_history/mod.rs |
| Runtime health/admin introspection depth | Faster incident response and safer operations | Implemented | src/core/runtime.rs |
| Structured observability (metrics/tracing/logging) | Production diagnosis and SLO ownership | Implemented | tests/otel_metrics_integration.rs, tests/otel_tracing_integration.rs, tests/logging_structured.rs |
| Example/build matrix across sources | Prevents connector-specific integration drift | Implemented | .github/workflows/ci.yml, scripts/ci-preflight.sh, examples/ |
| Connector version-compatibility test depth | Reduces production surprises on engine upgrades | Implemented (connector-specific depth varies) | tests/postgres_version_matrix.rs |

## Intentional Non-Goals (Do Not Gate Library Releases)

| Capability | Classification rationale |
|---|---|
| Managed SaaS control plane and hosted UI | Service/platform concern, outside embeddable crate boundary |
| Turnkey sink ecosystem with hundreds of connectors | Platform distribution concern; library exposes traits/APIs instead |
| Full orchestration and fleet management | Application/platform responsibility |

## Current Completeness Verdict

For embedded-library scope, cdc-rs is release-viable with conditions:
- Must-have set is fully implemented.
- Should-have set is now fully implemented; remaining risk is concentrated in deployment-specific policy tuning.

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
