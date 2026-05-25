# Feature Policy Matrix (Library Scope)

This document clarifies rustcdc feature intent by separating implemented capabilities, intentional non-goals, and roadmap candidates.

## Scope Statement

rustcdc is an embedded CDC library for Rust applications.

Primary goals:
- correctness-first event capture and delivery semantics
- embeddable runtime control inside application process boundaries
- explicit extension points for checkpointing, schema history, transforms, and adapters

Not a goal:
- matching service-platform breadth (managed control planes, hundreds of turnkey connectors)

Companion release-gating matrix:
- `docs/library_parity_matrix.md` defines must-have/should-have/non-goal parity criteria against embeddable libraries.

## Capability Policy Matrix

| Area | Current policy | Status |
|---|---|---|
| PostgreSQL source | Supported and maintained | Implemented |
| MySQL source | Supported and maintained | Implemented |
| SQL Server source | Supported and maintained | Implemented |
| Snapshot + stream + handoff runtime | Core behavior, correctness-critical | Implemented |
| Ack/commit barrier semantics | Core behavior, correctness-critical | Implemented |
| Deterministic replay + fault-injection tests | Core reliability practice | Implemented |
| Built-in sink catalog | Trait-based integration model preferred | Intentional non-goal |
| Managed control plane / hosted UI | Outside library boundary | Intentional non-goal |
| Additional non-relational connectors | Considered when maintainability and testability meet bar | Roadmap candidate |
| Runtime-emitted schema-change events | Emitted by current relational connectors; parser coverage evolves per dialect | Implemented |

## Acceptance Criteria For New Connector Families

A new connector family should meet all of the following:
- deterministic integration test coverage in CI
- replay/fault behavior validated against existing correctness invariants
- clear source offset model with resume semantics
- operational documentation (config, runbook, troubleshooting)
- maintenance owner commitment for bugfix and version drift

## Change Classification

Use this guide when proposing features:
- Core: affects correctness invariants or delivery semantics
- Extension: adds connector/adapter/transform capability without weakening invariants
- Platform: introduces service/control-plane behavior outside embedded-library scope

Default policy:
- accept Core and Extension changes that preserve invariants
- reject Platform changes unless project scope is explicitly revised
