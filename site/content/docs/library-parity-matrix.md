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
- Debezium Engine (embedded Java mode) — the reference implementation for CDC semantics, and the
  source of the DBLog incremental snapshot both projects implement
- Supabase `etl` (Rust) — the closest thing to a peer: also embeddable, also at-least-once, also
  built on PostgreSQL logical replication
- go-mysql (Go)
- python-mysql-replication (Python)
- pglogrepl (Go)
- wal2json (PostgreSQL output plugin in C)

### Where Supabase `etl` sits

Checked against the repository, not its marketing, because it is the comparison a Rust reader
will actually make:

| | Supabase `etl` | rustcdc |
|---|---|---|
| Distribution | Git dependency only — **not published on crates.io** | crates.io |
| PostgreSQL client | `tokio-postgres` and `postgres-replication` from a **fork** (`iambriccardo/rust-postgres`, pinned by revision) | stock `tokio-postgres`, plus a replication client this crate implements over the documented wire protocol |
| Sources | PostgreSQL 14–18 | PostgreSQL, MySQL, MariaDB, SQL Server behind one `Source` trait |
| Destinations | A catalogue it owns — BigQuery stable, ClickHouse/DuckLake/Snowflake in progress | `SinkAdapter`: you write the destination, the runtime owns ordering and acknowledgement |
| Delivery | at-least-once | at-least-once |

The fork is the load-bearing difference, and it is a consequence rather than a choice: stock
`tokio-postgres` exposes no replication-mode API, so a project that wants one either patches the
client — which forecloses crates.io, since a published crate may not depend on a Git revision —
or implements the protocol. rustcdc took the second path, which is why it installs as an
ordinary dependency.

The destination model is the other split, and it is a genuine trade rather than a deficit on
either side: `etl` ships warehouse sinks you do not have to write, and rustcdc ships a sink
contract for the systems nobody ships. Pick accordingly.

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
| The LSN read point never advances past what has been harvested | A CDC watermark taken from a capture job's harvest position stands still while the database is quiet. Clamping the window to stay non-inverted makes the next advance step from the clamped end, so every empty poll pushes the read point further above the harvested maximum — and a later change is captured only if its LSN is still above wherever it crept to. The obvious repair, parking one step past the maximum, *skips* that parked LSN once the maximum moves | Implemented — an empty window is represented (`start > end`) rather than clamped, and the lower bound moves only when something was consumed, so an empty window reopens from its original start with nothing skipped. Six unit tests pin the transitions including that skip; three live SQL Server suites exercise real window advancement | src/source/sqlserver/stream_window.rs (`a_quiet_database_does_not_walk_the_read_point_forward`, `an_empty_window_reopens_without_skipping_the_lsn_it_was_parked_at`) |
| Watermark bracket accounts for **commit visibility**, not just log position | A read-only watermark is a log coordinate, and a transaction reaches the log *before* it becomes visible to a new snapshot — PostgreSQL clears the proc array after the WAL flush, MySQL engine-commits after the binlog flush. A chunk read starting in that gap holds a pre-image whose event sits *below* the low watermark, so the position test never suppresses it and the stale value overwrites the newer one, silently. Every read-only watermark implementation shares the exposure, including Debezium's read-only incremental snapshot | Implemented on PostgreSQL — the bracket is `position <= high && (position > low \|\| in_flight.contains(tx_id))`, with the id set read from `pg_current_snapshot()` between the low watermark and the chunk read. That call order is what makes the two tests exhaustive. SQL Server needs nothing: its watermark comes from the capture tables, so it lags visibility rather than leading it. MySQL closes it by a **different mechanism**: no in-flight id exists on a scale a binlog event shares, so it brackets by executed-GTID **set difference** instead — `Executed_Gtid_Set` is updated after the engine commit while the binlog coordinate advances before it. Requires `gtid_mode = ON`; without it, the ordinal test and its documented residual window apply | src/source/incremental_snapshot/driver.rs (`a_transaction_below_the_low_watermark_but_still_invisible_is_suppressed`, `the_high_watermark_still_bounds_the_in_flight_set`), src/source/postgres/incremental_snapshot.rs, [architecture](@/docs/architecture.md#the-low-watermark-is-not-the-whole-bracket) |
| Unchanged-TOAST columns recovered inside the snapshot bracket | Suppressing a complete chunk row in favour of an incomplete stream event puts the omitted column in *neither* delivery, because a `Merge` into a row the consumer does not have yet applies nothing. Emitting the chunk row instead resurrects every other column's stale value. Debezium's incremental snapshot has the same shape and takes the loss | Implemented — the event is repaired from the chunk's own image of that row and delivered as a complete `Replace`. Sound where a fresh read is not: the value comes from a `SELECT` at a snapshot the driver knows the position of, `unavailable_columns` means the `UPDATE` did not modify those columns, and every write in between is itself inside the bracket and already folded in. Bounded to columns the chunk read, for that key's own row; anything unfillable is logged rather than passed silently | src/source/incremental_snapshot/driver.rs (`an_omitted_toast_column_is_filled_from_the_chunks_own_image`, `a_later_event_is_filled_from_an_earlier_events_value_not_the_chunks`, `an_event_past_the_high_watermark_is_not_repaired`), [architecture](@/docs/architecture.md#a-complete-chunk-row-suppressed-by-an-incomplete-event) |
| A deliberate re-snapshot is distinguishable from a replay | A snapshot row's identity is the row, not a log position, so re-reading an unchanged row is byte-identical to the first read — and a content-derived dedup guard drops it. The operator's re-snapshot request then succeeds and delivers nothing, silently, because both halves are individually behaving correctly | Implemented — the snapshot state carries a `generation` that advances per request and across a stop, and it is part of the row's synthetic offset. A chunk re-read after a mid-snapshot reconnect stays in its generation and is still deduplicated, so replay suppression is not traded away for it | src/source/incremental_snapshot/driver.rs (`a_re_snapshotted_row_survives_the_idempotency_guard`, `a_replay_within_one_generation_is_still_suppressed`), [architecture](@/docs/architecture.md#a-re-snapshot-must-not-look-like-a-replay) |
| Composite primary keys are all-or-nothing | A key built from the columns that happen to be present looks valid and addresses **every row sharing them**: `DELETE FROM t WHERE tenant_id = 7` deletes the tenant, and an upsert collapses it onto one row. As a message key it merges distinct rows into one compaction group | Implemented — `Event::primary_key_values()` returns `None` unless every declared key column is present, so the event routes to `RowWrite::None { MissingPrimaryKey }` and a sink must handle it explicitly. The transform pipeline enforces the same invariant from the other side, rejecting a stage that drops *any* key column | src/core/event.rs (`primary_key_values_refuses_a_partial_composite_key`, `a_partial_composite_key_yields_no_row_write_rather_than_a_wide_delete`), src/transform/mod.rs |
| Fixture integrity is verified where fixtures are actually loaded | A corpus of hand-maintained JSON degrades silently: a lost message means fewer replayed events, the golden is re-recorded to match, and the scenario retires unnoticed. A metadata field that is only checked in a constructor the loading path never calls is documentation, not validation | Implemented — `message_count` is verified by `Fixture::validate`, which `ReplaySession::new` runs, and the error names the fixture and both numbers. Renamed from `expected_event_count`, which said *event* count while checking *message* count — genuinely different, since an aborted transaction discards its buffered events. `Fixture::new` returns `Result` instead of asserting, and making it validate exposed three tests that had depended on it not doing so | src/deterministic_replay/fixtures.rs (`a_miscounted_fixture_is_refused_on_the_path_that_actually_loads_files`), [reliability testing](@/docs/reliability-testing.md#fixture-integrity-is-checked-on-the-path-that-loads-files) |
| Replay fixtures can express an *incomplete* payload | Comparing a field is necessary but not sufficient: if the fixture format cannot produce a non-empty `unavailable_columns`, both sides of that comparison are always equal and the field has the appearance of coverage with none of the substance. This was true of `before_is_key_only` for as long as the diff had compared it | Implemented — the fixture payload carries `before_is_key_only` and both unavailable-column lists, with a wrong shape rejected rather than ignored, and `postgres_unchanged_toast_v1` exercises a TOASTed column that was not modified alongside one that was — the pair that shows why the two lists are never merged. Every replayed event is also validated against the envelope contract, so a golden cannot freeze a malformed event in place | src/deterministic_replay/replay.rs, fixtures/deterministic_replay/postgres_unchanged_toast_v1.*, [reliability testing](@/docs/reliability-testing.md#a-comparison-is-only-as-good-as-what-the-fixtures-can-express) |
| The replay harness compares every deterministic field | A golden-fixture suite is only as strong as its comparison: a field the diff ignores is invisible to every fixture, however many there are, and the suite then reports success on exactly the regressions it exists to catch | Implemented — `semantic_diff` compares all seven previously-unchecked deterministic fields (`primary_key`, both unavailable-column lists, `envelope_version`, `source.offset`, `transaction`, snapshot chunk position). The two fields that vary per run are excluded *with a documented reason*, and a test asserts each compared field produces a diff when mutated while the excluded ones do not — so a field added to the envelope later cannot land unwatched | src/deterministic_replay/diff.rs (`every_deterministic_field_is_actually_compared`, `per_run_varying_fields_stay_ignored`), [reliability testing](@/docs/reliability-testing.md#what-it-compares-and-why-the-list-matters-more-than-it-looks) |
| The partial-payload contract survives every codec | A schema declaring a field is not an encoder writing it. If one output format drops `before_unavailable_columns`, its consumers cannot tell a TOASTed before-image column from a genuine `NULL` — while consumers of the *same stream* in another format can, so the contract silently depends on which codec you picked | Implemented — JSON, Avro, Protobuf and CloudEvents all carry `before_is_key_only` and both unavailable-column lists, and the Avro and Protobuf decoders read them back. The three are written by one loop rather than independent branches, and a test asserts the whole envelope as a set rather than field by field, because the failure mode is a field added to `Event` and not to a codec | src/codec/cloudevents.rs (`the_cloudevents_data_carries_every_partial_payload_field`, `no_envelope_field_is_silently_dropped`), src/codec/avro.rs, src/codec/protobuf.rs |
| A binary column's representation is a property of the column | Deciding text-vs-hex from the bytes makes the same column render both ways across rows, and no decoder is then correct for it: hex-decoding corrupts the text rows, reading text corrupts the binary ones, and a `VARCHAR` holding `deadbeef` becomes indistinguishable from a `VARBINARY` holding those bytes | Implemented — MySQL uses the binlog's charset metadata (collation `63` is `binary`), because the column type cannot tell `BLOB` from `TEXT` or `VARBINARY` from `VARCHAR`. The charset resolution is `mysql_common`'s own, the same one its value parser uses, so an off-by-one in character-column indexing — which would hex-encode real text — is not this crate's to make. Each connector's exact encoding is documented per connector rather than claimed uniform | src/source/mysql/query.rs (`a_binary_column_is_hex_encoded_whatever_its_bytes_happen_to_be`, `the_charset_and_not_the_type_decides`, `non_character_columns_are_never_hex_encoded`), [config reference](@/docs/config-reference.md#binary-column-encoding-per-connector) |
| Filter thresholds compare exact decimals | Column values are text precisely because a JSON number is an IEEE-754 double downstream; a filter that narrows them back to `f64` decides row membership at 53 bits, so `id > 9007199254740992` silently mis-sorts snowflake ids and `numeric(38,4)` amounts | Implemented — the ordering operators compare sign, then integer digits by length and lexicographically, then fraction digits, with no mantissa ceiling and no dependency. A non-numeric or exponent operand evaluates to `false` rather than guessing an order | src/transform/filter_projection.rs (`comparison_is_exact_past_the_f64_mantissa`, `comparison_is_exact_for_high_precision_decimals`) |
| One glob semantics for every table pattern | The sink router matched globs while `table_include_list` / `table_exclude_list` matched exact strings, and nothing said so — `table_exclude_list = ["public.tmp_*"]` excluded nothing, which is indistinguishable from tables that never changed. Debezium's equivalents take regexes, so operators arrive expecting patterns to work | Implemented — one matcher, shared by routing and connector filtering, with the pattern table documented and an unqualified entry's schema-agnostic widening warned about at `connect()`. The matcher is also greedy-with-one-backtrack rather than doubly recursive, so a config typo cannot hang a pipeline on a pathological pattern | src/core/glob.rs (`a_pathological_pattern_returns_promptly`, `an_unqualified_pattern_matches_every_schema`), src/source/mod.rs, [config reference](@/docs/config-reference.md#table-filter-patterns) |
| Incremental snapshot survives a mid-flight reconnect | A snapshot of a large table spans a long window; any transient disconnect inside it must not lose progress | Implemented | tests/postgres_incremental_snapshot_reconnect_integration.rs (terminates the walsender mid-snapshot and asserts completion without duplicates) |
| Chunk emitted **at** the high watermark, not after later log events | A batched log reader straddles the watermark, and delivering the batch whole before the chunk resurrects the row's pre-`SELECT` value — the failure the override window exists to prevent, moved one step later | Implemented — a straddling batch is split at the first event past the high watermark: head, chunk, tail, with no durable position reported while the tail is held back | src/source/incremental_snapshot/driver.rs (`a_batch_straddling_the_high_watermark_delivers_the_chunk_before_the_tail`, `a_chunk_row_is_non_persistent_while_later_log_events_are_held_back`) |
| Acknowledgement tokens are single-use | `AckToken` is `Clone` and `ack_mode()` mints copies, so a double-ack advanced the checkpoint over events the caller was never handed | Implemented — tokens carry an epoch the accepting commit spends | src/core/runtime.rs (`an_ack_token_cannot_be_committed_twice`) |
| DDL recording is idempotent under replay | At-least-once means a crash between recording a schema change and committing replays it; re-applying an `ALTER` diff wedged the pipeline on every subsequent restart | Implemented — `record_ddl(ddl_id, ddl)` returns the version already assigned | src/schema_history/mod.rs (`a_replayed_alter_diff_returns_the_version_it_already_got`, `a_ddl_replayed_after_a_restart_is_recognised_from_the_file`) |
| Durable state stores never block the embedder's executor | The crate runs inside the caller's Tokio runtime; an inline `fsync` stalls every task on that worker and wedges a current-thread runtime | Implemented — `FileCheckpoint` and `FileSchemaHistory` run all filesystem work on `spawn_blocking`, matching what `FileJsonlSink` already did | src/checkpoint/mod.rs (`on_blocking_worker`), src/schema_history/mod.rs |
| Restart resumes at a boundary, not at the last event's own position | PostgreSQL logical decoding filters at transaction granularity, so a checkpoint holding a change LSN replays that whole transaction on every restart — a *guaranteed* duplicate per deploy, and under `SqlPeek` an unbounded re-emit per poll. Nudging the LSN forward does not fix it | Implemented — `StreamHandle::resume_offset_for` returns the pgoutput COMMIT `end_lsn`; MySQL and SQL Server already resume exclusively and need no override | tests/postgres_restart_resume_integration.rs (both transports, against PostgreSQL 16), src/core/runtime.rs (`the_checkpoint_uses_the_connectors_resume_position_not_the_events_own`) |
| Cancel-safety stated, and honoured by the crate's own APIs | `tokio::select!` is the obvious way to add a shutdown signal, and racing a poll that is mid-transform discards events that have left the source | Implemented — documented under `# Cancel safety`, and `run_to_completion` / `event_batches_cancellable` check the token between polls rather than racing it | src/core/runtime/runtime_poll.rs, src/core/runtime/runtime_lifecycle.rs |
| Aggregated failures keep their retry classification | A fan-out flush that reports a transient broker reset as terminal halts a pipeline that should have retried — and makes the outcome depend on which call surfaced the failure | Implemented — `Error::Aggregate { kind, .. }` under `ErrorKind::severity()` | src/core/error.rs (`an_aggregate_reports_the_most_severe_kind_not_the_last_one`) |
| Checkpoint rewind guard usable by third-party backends | Every non-trivial deployment has a `Checkpoint` that is not a `FileCheckpoint`, and the per-source reasoning is exactly what gets reimplemented wrong | Implemented — `stream_position_regression` and `validate_checkpoint_progress` are public, and `FileCheckpoint` calls the shared helper so the two cannot drift | src/checkpoint/mod.rs |
| Snapshot pause / resume / stop | Debezium ships `pause-snapshot`, `resume-snapshot` and `stop-snapshot` signals and operators arrive expecting them. Without them, taking backfill load off a production primary means stopping the pipeline and clearing the checkpoint — which also stops capture | Implemented — `CdcRuntime::{pause,resume,stop}_incremental_snapshot`. Pause takes effect at a chunk boundary, and **both** pause and stop are durable in the checkpoint, so neither silently lifts on a deploy. Stop is recorded as an explicit flag rather than inferred from absent cursors: a configured table with no cursor is indistinguishable from one that has not started, so inferring it re-ran the whole backfill on the next restart | src/source/incremental_snapshot/driver.rs (`pausing_stops_chunk_reads_while_the_live_stream_keeps_flowing`, `the_paused_flag_is_durable_across_a_restart`, `a_stopped_snapshot_stays_stopped_across_a_restart`, `requesting_a_table_clears_the_stopped_flag`) |
| Control operations reachable from another task | An event loop holds `&mut CdcRuntime` for its lifetime, so every control operation is otherwise unreachable from an admin endpoint without the embedder hand-building an mpsc/oneshot bridge | Implemented — `CdcRuntime::control_handle()` returns a cloneable `RuntimeControl`; commands apply between polls, progress reads from a per-poll published snapshot and cannot block | src/core/runtime/control.rs, src/core/runtime.rs (`a_control_handle_drives_the_runtime_from_another_task`, `a_control_command_fails_fast_once_the_runtime_is_gone`) |
| Snapshot restricted to a subset of rows, **including on-demand requests** | A backfill is otherwise all-or-nothing, so restricting it to one tenant or one time range means not using it. And the filter belongs to the *request*: routing a one-off through static configuration means editing a file and restarting to run what was meant to be a signal — which is why Debezium's `execute-snapshot` carries `data-collections` and `additional-conditions` together | Implemented — `IncrementalSnapshotConfig::with_table_condition` for the deployment, `SnapshotRequest::with_condition` for a request, with the request overriding the configuration per table. Bounds the chunk reads only; the live stream keeps carrying every change to the table. The effective filter is reported per table in `IncrementalSnapshotState`, so "did my filter apply?" is observable rather than inferable from row counts | src/source/incremental_snapshot/driver.rs, tests/postgres_snapshot_filter_integration.rs, src/source/sqlserver/parser.rs (`a_row_filter_is_parenthesised_so_an_or_cannot_widen_the_keyset_seek`) |
| Every integration suite is actually run by CI | An allow-list guard is silent about suites nobody added, and a test that never runs still reads as evidence in a review — sixteen had accumulated outside CI, including the end-to-end coverage of `register_source` | Implemented — the policy gate requires every `tests/*.rs` to appear in the workflow, in a script CI runs, or in an explicit helper list | scripts/ci-policy-gate.sh (`run_test_suite_coverage_check`) |
| One value representation across every connector and capture path | A snapshot row and a stream row for the same column arriving with different JSON types forces every sink to branch on which path delivered it — and a JSON number silently corrupts `numeric(38,4)` and `bigint` past 2^53 | Implemented — every scalar is a JSON string rendered by the column type's own output function, so a snapshot and the stream agree character for character | tests/postgres_value_representation_integration.rs, src/source/postgres/query.rs (`row_as_text_json`), src/source/sqlserver/parser.rs (`decode_row_json_as_text`) |
| Delivery loop supplied by the library | `poll → send → flush → acknowledge` has exactly one safe order, and reversing the last two is silent data loss that surfaces months later | Implemented — `CdcRuntime::run_to_completion` | src/core/runtime/runtime_lifecycle.rs (`run_to_completion_flushes_the_sink_before_acknowledging`) |

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

## Where the comparison still favours the alternatives

Recorded here rather than left implicit, because a matrix that only lists wins is not evidence:

| Gap | Consequence |
|---|---|
| No Oracle, Db2, MongoDB or Cassandra connector | Debezium covers engines rustcdc does not, and Oracle in particular is where a large share of real CDC demand sits |
| No published end-to-end throughput figure | Every performance number here is a microbenchmark or a latency percentile. Third-party throughput benchmarks exist for Debezium and Flink CDC; inferring one from the microbenchmarks would be dishonest |
| No pgoutput `'M'` logical-decoding message support | Debezium decodes these; they are the tableless outbox pattern on PostgreSQL |

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
