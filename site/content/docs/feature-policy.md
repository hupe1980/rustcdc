+++
title = "Feature policy"
description = "How rustcdc's Cargo features are scoped, and what the default profile deliberately excludes."
weight = 140
+++

This document separates what rustcdc implements from what it deliberately does not, so an evaluation can be made against current behaviour rather than intent.

## Scope Statement

rustcdc is an embedded CDC library for Rust applications.

Primary goals:
- correctness-first event capture and delivery semantics
- embeddable runtime control inside application process boundaries
- explicit extension points for checkpointing, schema history, transforms, and adapters

Not a goal:
- matching service-platform breadth (managed control planes, hundreds of turnkey connectors)

Companion release-gating matrix:
- [Library parity matrix](@/docs/library-parity-matrix.md) defines must-have/should-have/non-goal parity criteria against embeddable libraries.

## Capability Policy Matrix

| Area | Current policy | Status |
|---|---|---|
| PostgreSQL source | Supported and maintained | Implemented |
| MySQL source | Supported and maintained | Implemented |
| SQL Server source | Supported and maintained | Implemented |
| Snowflake source (`CHANGES` clause) | Supported; transport supplied by the embedder, because no self-hostable server exists to test one against | Implemented, with the container-evidence exception below |
| Snapshot + stream + handoff runtime | Core behavior, correctness-critical | Implemented |
| Ack/commit barrier semantics | Core behavior, correctness-critical | Implemented |
| Deterministic replay + fault-injection tests | Core reliability practice | Implemented |
| Built-in sink catalog | Trait-based integration model preferred | Intentional non-goal |
| Managed control plane / hosted UI | Outside library boundary | Intentional non-goal |
| Additional non-relational connectors | Considered when maintainability and testability meet bar | Not implemented |
| Runtime-emitted schema-change events | Emitted by current relational connectors; parser coverage evolves per dialect | Implemented |

## Acceptance Criteria For New Connector Families

A new connector family should meet all of the following:
- deterministic integration test coverage in CI
- replay/fault behavior validated against existing correctness invariants
- clear source offset model with resume semantics
- operational documentation (config, runbook, troubleshooting)
- maintenance owner commitment for bugfix and version drift

### The one standing exception, and its terms

The Snowflake source meets every criterion except the first, and cannot meet it: Snowflake has
no self-hostable implementation, so there is no container to test against. It was accepted on
these terms, which are the general terms for any future connector to a service that cannot be
run locally:

1. **The untestable part is not in the crate.** The transport — HTTPS, JWT/OAuth/WIF — is a
   trait the embedder implements. What ships is the part that *is* testable without the
   service: statement construction, window arithmetic, row mapping, error classification.
2. **The semantics are covered by unit tests through a scripted transport**, at the same
   density as a connector with containers behind it.
3. **The gap is stated wherever the connector is claimed** — in the module docs, on the
   documentation page, in the README status section, and in the parity matrix's
   "where the comparison still favours the alternatives" table. An unverified claim that
   announces itself is a different thing from one that does not.
4. **The event contract's weaker guarantees are enumerated rather than glossed** —
   no transaction metadata, net-effect windows, no source order within a window.

A connector that cannot satisfy all four does not get the exception.

## Change Classification

Use this guide when proposing features:
- Core: affects correctness invariants or delivery semantics
- Extension: adds connector/adapter/transform capability without weakening invariants
- Platform: introduces service/control-plane behavior outside embedded-library scope

Default policy:
- accept Core and Extension changes that preserve invariants
- reject Platform changes unless project scope is explicitly revised
