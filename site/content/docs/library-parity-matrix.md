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
| PostgreSQL streaming replication protocol (`START_REPLICATION ... LOGICAL`) | The mechanism logical replication was designed around; the SQL alternative re-reads WAL on every poll and cannot be pushed | Implemented | src/source/postgres/wire/, tests/postgres_wal_transport_parity_integration.rs (parity with `SqlPeek`, SCRAM-SHA-256, MD5, checkpoint resume) |
| Incremental snapshot that never writes to the source | A snapshot needing write access to the captured database is refused outright in many environments (read replicas, least-privilege roles, regulated estates) | Implemented — read-only **by construction** on all three connectors. Watermarks come from the source's own log coordinate (`pg_current_wal_lsn`, `SHOW MASTER STATUS`/`SHOW BINARY LOG STATUS`, `sys.fn_cdc_get_max_lsn`), so nothing is inserted into the source. Debezium's default incremental snapshot writes markers to a signal table and needs a separate read-only variant (MySQL + GTID only) to avoid it. | src/source/incremental_snapshot/driver.rs, src/source/{postgres,mysql,sqlserver}/incremental_snapshot.rs |
| Incremental snapshot survives a mid-flight reconnect | A snapshot of a large table spans a long window; any transient disconnect inside it must not lose progress | Implemented | tests/postgres_incremental_snapshot_reconnect_integration.rs (terminates the walsender mid-snapshot and asserts completion without duplicates) |

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
| SQL Server capture is polling-based | p99 latency ≈ `stream_poll_interval_ms` plus the capture agent's own delay | Inherent to SQL Server CDC; no log-reading interface exists. Do not compare its latency numbers against the log-based connectors. |
| MySQL binlog timestamps have whole-second resolution | Any lag figure derived from `SourceMetadata::timestamp` over-reports by up to 1 000 ms (median ~480 ms) | The binlog common header carries seconds. PostgreSQL and SQL Server both carry microsecond commit timestamps and are exact. |
| PostgreSQL streaming replication does not implement SCRAM channel binding (`SCRAM-SHA-256-PLUS`) | A server that offers *only* `-PLUS` cannot be authenticated by the streaming transport; use `WalTransport::SqlPeek`, which authenticates through `tokio-postgres` | Certificate verification is on by default (`verify_full`) and `sslmode=require` is enforced, so the MITM channel binding defends against already needs a certificate chaining to a trusted root for the right hostname. The residual risk is a downgrade, which is logged when the server advertises `-PLUS`. |
| `SqlPeek` fallback is 4–5× slower than the default transport | Environments that cannot grant a replication connection pay a measured 4–5× on capture time, and each poll re-reads WAL from the slot's `restart_lsn` rather than continuing from the last position | Inherent to `pg_logical_slot_peek_binary_changes` being non-consuming. Numbers and method: [measured performance](@/docs/reliability-testing.md#measured-performance) |
| No pgoutput `proto_version '2'` streaming of in-progress transactions | PostgreSQL buffers a transaction server-side until commit, spilling past `logical_decoding_work_mem` (64 MB default) to `pg_replslot/<slot>/`. A very large transaction causes disk churn on the primary and delivers as a burst. Bounded and observable via `pg_stat_replication_slots`. | v2 moves the buffering to the client, which must hold each transaction until `Stream Commit` and discard it on `Stream Abort`. Mishandling the abort emits changes the source rolled back. **Debezium's pgoutput decoder also negotiates `proto_version 1` and handles the same ten v1 message types**, so this is a shared frontier rather than a deficit. Mitigations: [config reference](@/docs/config-reference.md#large-transactions-spill-on-the-server). |
| No two-phase-commit (`PREPARE TRANSACTION`) decoding on PostgreSQL | Prepared transactions are decoded at `COMMIT PREPARED`, not at `PREPARE` | Follows from `proto_version '1'`, and **Debezium does not decode them either**. rustcdc additionally *rejects* v3 messages rather than letting them fall through unhandled, so a plugin mismatch is loud instead of a silent misreading of transaction boundaries. Relevant only to workloads using explicit 2PC on the source. |

## Current Completeness Verdict

For embedded-library scope, rustcdc is release-viable with conditions:
- Must-have set is implemented for the primary connector paths, and each release
	must confirm connector-specific restart evidence remains current.
- Should-have set is broadly implemented; remaining risk is concentrated in deployment-specific policy tuning and continuous evidence rigor.
- The architectural limits above are documented rather than closed, and a deployment whose
	workload sits against one of them (a latency SLO below the SQL Server poll interval, a
	PostgreSQL server that requires SCRAM channel binding) should be evaluated against it
	explicitly.

## Release Decision Rules

Use these rules during audit and release gates:
1. Block release if any Must-have is Missing.
2. Block release if a Must-have is Partial and can cause incorrect runtime behavior or incorrect integration assumptions.
3. Do not block release on Non-goals.
4. Do not block a release on a Should-have item unless it has become a reliability prerequisite.

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
