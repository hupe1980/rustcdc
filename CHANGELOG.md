# Changelog

All notable changes to this project are documented here.

The project is pre-1.0. Minor version bumps may contain breaking changes; each one lists
what breaks and what to do about it.

## 0.12.0

A second correctness pass over the whole tree, plus round-2 feedback from the `rustcdc-server`
maintainers against the released 0.11.0. Thirty findings, all closed here, and the shape of them differs from last time:
none was visible by reading a function in isolation, and half required reasoning about what a
database actually guarantees rather than about what this code says.

Four were silent-corruption class, one was a server-triggered denial of service, and one was an
operator action that quietly undid itself on the next deploy. Two are flaws in the DBLog
incremental snapshot as commonly implemented rather than in the code implementing it — the
watermark bracket ignoring commit visibility, and the override window discarding
unchanged-TOAST columns it could have recovered. Both are present in every read-only watermark
CDC implementation available to check, Debezium's included.

Every fix carries a regression test that was **confirmed to fail with the fix reverted**, by
reverting it, rather than asserted to.

It is a **breaking** release in five ways: a truncated composite primary key no longer resolves
to a key at all; `table_include_list` / `table_exclude_list` entries are glob patterns rather
than exact strings; `EventEncoder::encode_key` returns `Result<Option<_>>`;
`StreamHandle::request_snapshot_tables` takes a `SnapshotRequest`; and the replay fixture format
renames `expected_event_count` to `message_count`. `IncrementalSnapshotBackend` gains a method and
`IncrementalSnapshotState` two fields, but both are defaulted so existing implementations compile
and existing checkpoints load unchanged — though accepting the backend default is a correctness
decision, and the trait says so. Each has its own entry below with the migration.

Two conditions remain open on a 1.0 release, one of them new and a database limitation rather
than a defect. [FINDINGS.md](FINDINGS.md) has the full audit.

Four of the fifteen came from `rustcdc-server`'s round-2 report (B-4, F-8, F-9, F-10). Every
claim in it was verified against the source before acting; all four held, and B-4's mechanism
was diagnosed exactly right. One consequence of B-4 was worse than reported — see its entry.

### Fixed: MySQL's incremental-snapshot commit-visibility window, via GTID sets

The `rustcdc-server` maintainers asked why this needed our own wire protocol, as PostgreSQL's WAL
stream did. It does not — and answering that properly showed the gap was never a MySQL limitation
at all, only a limitation of how this crate bracketed a chunk read. An earlier entry in this
release called it a database limitation; that was wrong and is corrected here.

`SHOW MASTER STATUS`'s file-and-position advances at the binlog **flush** stage, before the InnoDB
engine commit that makes rows visible. So a transaction can sit *below* the low watermark and still
have been invisible to the chunk `SELECT` — the chunk holds its pre-image, the ordinal test finds
nothing to suppress, and the stale value is emitted over the newer one.

`Executed_Gtid_Set` is updated **after** the engine commit, so a GTID present in it belongs to a
transaction whose rows are already visible. The bracket becomes a set difference: inside iff the
event's GTID is in `high` and not in `low`, after iff it is not in `high`. This is the mechanism
Debezium's read-only incremental snapshot uses, and it requires `gtid_mode = ON`. The set is the
last column of `SHOW MASTER STATUS`, so it costs no extra round trip.

**Both bounds come from the set, deliberately.** Mixing a set-based lower bound with an ordinal
upper bound is unsound and easy to reach: an event inside the ordinal high bound but absent from
`high`'s set committed *after* that read, so suppressing it would discard the newer value. Two
tests assert exactly this pair of divergences from the ordinal test — one where ordinal says
`Before` and membership says `Inside`, one where ordinal says `Inside` and membership says `After`.

**New on the trait, defaulted:**

```rust
fn event_in_bracket(&self, event, position, low, high, in_flight) -> BracketPosition
```

The bracket decision belongs to the backend, because only it knows whether its watermark is an
ordered coordinate or a set — and `>` cannot express membership in a GTID set, which is only
*partially* ordered. The default is the ordinal test the driver used to inline, so PostgreSQL and
SQL Server are unchanged and a third-party backend keeps compiling. `BracketPosition` is exported.

The binlog coordinate still orders the watermark, but only to answer a different question: has the
stream caught up to the high watermark, so the chunk can be emitted? That is safe on the
coordinate, since every GTID in a watermark's set was written to the binlog before that watermark
was read. The set takes no part in ordering, and a test pins that.

**Two documented fallbacks**, both to the ordinal test rather than a guess: `gtid_mode = OFF`,
where there is no set to use; and an event with no GTID while the watermarks do have sets — a
non-transactional or synthetic event, which must not be read as "absent from `high`" and deferred
past the chunk on no evidence.

A malformed `Executed_Gtid_Set` is an error, not an empty set. Silently shrinking a watermark is
the failure this mechanism exists to prevent: a shrunken low watermark suppresses chunk rows it
should not, and a shrunken high watermark fails to suppress rows it must.

**Still needs a live server to confirm end to end.** The GTID logic, the bracket, the fallbacks and
the ordering property are covered by 20 unit tests; what no test here can establish is that a real
MySQL's `Executed_Gtid_Set` and its binlog GTIDs line up as expected. That belongs in the
container-backed matrix.

### Fixed: the OpenTelemetry and Prometheus metric names were two disjoint namespaces

Every runbook alert threshold silently never fired for anyone on the OTel path.

The crate exposes metrics two ways — its own `/metrics` text exposition, and OpenTelemetry under
the `metrics` feature — and they named overlapping quantities differently. OTel emitted
`rustcdc.replication_lag_ms`, `rustcdc.buffer_size`, `rustcdc.checkpoint.committed_count`; the text
exposition emitted `rustcdc_runtime_replication_lag_ms`, `rustcdc_runtime_buffer_depth`,
`rustcdc_runtime_events_committed_total`. The runbook documents **only** the latter.

So an operator enabling `metrics`, exporting through a collector to Prometheus, and copying the
runbook's thresholds got alerts matching nothing. An alert that silently does not fire is worse
than no alert: it looks like coverage.

Every OTel instrument is now named so that the standard OTel → Prometheus translation — dots to
underscores, `_total` appended to monotonic counters — produces exactly the documented series
name. Nothing documented changed name, so existing alerts keep working; the undocumented namespace
was the one that moved.

Units stay in the metric name rather than declared via `with_unit`, because a declared unit makes
the exporter append a unit suffix and break that correspondence — and unit-in-name is the
Prometheus convention regardless. The reasoning is recorded at the instrument definitions, where
the next person to add one will see it.

Two caveats worth stating rather than leaving to be discovered. The two surfaces still expose
**different quantities**, not merely different names: the text exposition has health, liveness,
readiness, and the idempotency and skip counters; OTel has lag-in-events, checkpoint offset,
snapshot progress and the duration histograms. And `rustcdc.runtime.events_filtered` has no
runtime caller — it is an embedder-facing hook on `OTelMetricsCollector`, not something the
pipeline feeds, so it reads zero unless you call it.

### Fixed: SQL Server's LSN read point crept forward on a quiet database

`fn_cdc_get_max_lsn()` reports what the capture job has harvested, so it stands still while nothing
changes. The window was clamped to stay non-inverted — after reading `[S, M]` the next window became
`[M+1, M+1]` — and the next advance incremented from that clamped end. Every empty poll therefore
pushed the read point one minimal LSN step above the harvested maximum, indefinitely, and a change
committed later was captured only if its LSN was still above wherever the point had crept to.

**Two attempts; the first was worse than the bug.** Parking one step past the maximum and stopping
means that when the maximum later moves, the advance increments from the parked *end* and skips the
parked LSN — one that was never readable while the window sat there. That was caught by a test
written for the attempt and withdrawn rather than shipped.

The rule now is that `lsn_end` never exceeds the harvested maximum, so an **empty window is
represented** (`lsn_start > lsn_end`) rather than clamped, and the lower bound moves only when
something was consumed. An empty window whose maximum later jumps reopens from its original
`start`, so nothing between them is skipped. The per-instance fetch short-circuits on an empty
window rather than issuing a round trip per capture instance for a range that cannot contain
anything.

Six unit tests on the pure `next_window`, including the exact skip that broke the first attempt, and
validated against a live SQL Server 2022 through `sqlserver_stream_integration`,
`sqlserver_window_truncation_integration` and `sqlserver_snapshot_integration`.

`sqlserver_idle_window_integration` validates it behaviourally: poll repeatedly against a
**standing** harvested maximum, then write, and require the change to arrive. Its idle phase
deliberately withholds `sys.sp_cdc_scan`, which the other SQL Server suites call on empty polls —
forcing a scan there would keep the maximum advancing and mask the exact condition the creep needs.
A first version of the test omitted that reasoning, reported zero events, and looked like a
connector fault.

### Changed: `EventEncoder::encode_key` distinguishes "no key" from "encoding failed"

**Breaking**: the signature is now `Result<Option<Vec<u8>>>` rather than `Option<Vec<u8>>`.
A call site becomes `encoder.encode_key(&event)?`; there is one non-default implementor in the
crate.

A bare `Option` conflated two outcomes that must not be:

| Outcome | Meaning |
|---|---|
| `Ok(None)` | The event genuinely has no key — TRUNCATE, SCHEMA_CHANGE, no declared primary key, or a payload missing a key column |
| `Err(..)` | Encoding **failed**, for an event that does have a key |

Collapsing the second into the first is a silent correctness failure, not a lost error message. A
keyed sink reads `None` as "unkeyed" and publishes the record without a key: partition routing
becomes round-robin, **ordering for that row is lost**, log compaction stops collapsing it — and
the record still arrives, so nothing looks wrong.

`ConfluentAvroEncoder::encode_key` made that outcome reachable, swallowing both of its failure
paths with `.ok()` while its own documentation said it "always returns `Some(bytes)`". Neither path
is reachable today — the key schema is fixed and single-field, and `serde_json` cannot fail on a
`Map<String, Value>` — which is precisely why swallowing them was cheap to write and would have
stayed invisible if either ever became reachable. `EncoderCodec` was propagating the same `Option`
into `CodecOutput { key: None, .. }`, so a failure would have reached a sink as "this event has no
key".

The crate already refuses this from the other direction: a transform that destroys an event's key
is rejected with an error naming the stage rather than emitting the record unkeyed. Letting an
encoder cause the same thing quietly was the inconsistency.

Covered by `a_keyless_event_is_ok_none_and_never_an_error`, which pins every legitimate `Ok(None)`
case including the truncated-composite-key one, and
`the_combined_codec_propagates_a_key_failure_rather_than_reporting_no_key`.

### Fixed: the CloudEvents encoder dropped `before_unavailable_columns`

Found by asking the same question of the codecs that F21 asked of the fixtures: the schemas
declare these fields, but does every encoder actually write them?

Avro and Protobuf do, and both decoders read them back. The **CloudEvents** encoder wrote
`before_is_key_only` and `unavailable_columns` and not `before_unavailable_columns` — omitted when
the field was added to the envelope, with nothing to notice.

The consequence is narrow and sharp: a CloudEvents consumer had a **weaker contract than a JSON,
Avro or Protobuf consumer of the same stream**, and could not tell a before-image column absent
*because it was TOASTed* from one that was genuinely `NULL`. That is precisely the distinction the
field exists to make, and the one a row diff or a compensating write depends on — the crate's own
documentation says "do not read its absence as 'was NULL'", which a CloudEvents consumer had no
way to honour.

The three fields are now written by one loop rather than three independent branches, so a fourth
cannot be forgotten the same way.

Two tests: one asserting both unavailable-column lists appear with their values, and
`no_envelope_field_is_silently_dropped`, which checks the whole envelope as a **set** rather than
field by field — because the failure mode is a field added to `Event` and not to this encoder,
which is exactly what happened. Both fail with the fix reverted.

`ts` and `source.timestamp` are equal on every connector path, so CloudEvents carrying only `time`
loses nothing today. Recorded here because they are separate fields with separate contracts, and
the equality is a property of the connectors rather than a guarantee of the envelope.

### Fixed: a truncated replay fixture loaded and replayed silently

**Breaking** (fixture format): `FixtureMetadata::expected_event_count` is now `message_count`,
and all 41 fixtures are migrated.

The field was documented "Expected event count for validation" and was checked in exactly one
place — `Fixture::new` — which the loading path never calls. Fixtures are read with
`from_path` → `from_json`, and `Fixture::validate` did not look at it at all. So every fixture on
disk carried an unverified number that a reader could reasonably trust.

The failure that matters is a fixture losing a message to an edit. Replay produces fewer events,
the golden is re-recorded to match, and whatever scenario the missing messages covered quietly
stops being covered — the same shape as the three findings below, where a green harness meant less
than it looked.

The name was also wrong in a way that mattered. It said *event* count and was compared against
`messages.len()`, and those genuinely differ: an aborted transaction discards its buffered events,
so replay can legitimately produce fewer events than messages. Renaming it to `message_count`
makes it mean what it checks, so the check can now live in `validate` without ambiguity.

`Fixture::new` also stopped panicking. It used `assert_eq!` on caller-supplied data inside a
library; every other constructor in the crate returns a `Result`, and a fixture builder is exactly
the caller that wants to report a problem rather than abort. It now runs the full `validate` and
returns `Result<Fixture, String>`.

**Making the constructor validate immediately found three tests that depended on it not doing
so** — two deliberately built invalid fixtures to exercise `validate` (now constructed directly,
which is what they meant), and a serialization round-trip whose `Insert` payload was
`{"table":..,"columns":[..]}`: a shape no message type accepts. That test had been round-tripping
an invalid fixture since it was written.

Covered by `a_miscounted_fixture_is_refused_on_the_path_that_actually_loads_files`, which goes
through `from_json` and `ReplaySession::new` rather than the constructor, plus assertions that the
constructor and `validate` agree. Dropping a message from a real fixture on disk now fails by
name, reporting `declares message_count 5 but carries 4 messages`.

### Fixed: the replay fixture format could not express an incomplete payload

The other half of the diff finding below, and the half that actually mattered.

Extending `semantic_diff` to compare `unavailable_columns` and `before_unavailable_columns` made
the comparison correct. It did not make it *effective*: `ReplaySession::create_data_event`
hardcoded those two fields — and `before_is_key_only`, which the diff had always compared — to
their empty defaults, and `parse_data_payload` never read them. The fixture format could not
express an incomplete payload at all.

So both sides of every comparison were structurally always equal, and the fields had the
appearance of replay coverage with none of the substance. A regression that stopped reporting an
unchanged-TOAST column — making a sink write `NULL` over live data — still could not have been
caught by any golden. The diff's field list and the fixture format had simply never been
reconciled.

The replay engine now reads all three from the fixture payload, rejecting a wrong shape rather
than ignoring it: a silently-dropped `unavailable_columns` would produce a golden asserting the
opposite of what its author wrote, which is worse than no fixture. Absent means "complete
payload", so every fixture written before this is unaffected.

New fixture `postgres_unchanged_toast_v1` exercises the contract end to end, and deliberately
carries both cases in one file:

- an `UPDATE` whose TOASTed `body` was **not** modified — absent from `after`, named in
  `unavailable_columns`, with a key-only before-image under `REPLICA IDENTITY DEFAULT`;
- an `UPDATE` whose `body` **was** modified — present in `after`, absent from `before`, named in
  `before_unavailable_columns`.

The second is why the two lists are tracked separately and must never be merged: merging them
would mark a column that genuinely changed as unwritable and silently drop the update. Reverting
the engine fix now fails that fixture by name, reporting
`unavailable_columns changed from ["body"] to []`.

### Added: every replayed event is validated, not just compared

Matching a recorded golden is not the same as being correct. A golden recorded once from a
malformed envelope would be defended by the suite forever — the comparison would pass precisely
because both sides share the malformation.

`assert_matches_golden` now runs `Event::validate()` on every replayed event before comparing.
That covers the partial-payload rules specifically: a column may not be both listed as unavailable
and present in the payload, and a key-only before-image may not also carry unavailable columns.
All 41 fixtures pass, so no existing golden was pinning a contract violation — which is worth
knowing rather than assuming.

### Fixed: the deterministic-replay diff was blind to the fields that matter most

`semantic_diff` is the **sole** comparison the golden-fixture suite performs — 40 fixtures across
three connectors, and the evidence behind the crate's deterministic-replay claim. It compared
`op`, `table`, `schema`, `source.source_name`, `before`, `after` and `before_is_key_only`.

It did **not** compare `primary_key`, `unavailable_columns`, `before_unavailable_columns`,
`envelope_version`, `source.offset`, `transaction`, or `snapshot`. Every one of those is a
deterministic function of the replayed input, and every one is a field whose regression this
release's own findings show matters:

- a change to `unavailable_columns` makes a sink write `NULL` over live data;
- a change to `primary_key` stops the event resolving a key at all, since
  `primary_key_values` is all-or-nothing over that list;
- a change to `source.offset` costs a guaranteed duplicate — or a gap — on every restart.

Any of those could have landed with all 40 goldens green. The harness reported success on
precisely the regressions it exists to catch.

All seven are now compared, and the two fields that legitimately vary per run — `ts` /
`source.timestamp`, and `snapshot_id`, which embeds the millisecond the snapshot began — are
documented as excluded with the reason, rather than being absent by omission. `snapshot` is
compared on its chunk position only, for that reason.

**All 40 recorded goldens pass unchanged under the stricter comparison**, which is what shows the
additions describe real behaviour rather than tightening arbitrarily.

Two tests keep the list honest in both directions:
`every_deterministic_field_is_actually_compared` mutates each compared field and asserts a diff
appears — it fails with the additions reverted, naming the field the fixtures would be blind to —
and `per_run_varying_fields_stay_ignored` asserts the excluded ones stay excluded, so nobody
"fixes" the suite into failing on wall-clock noise.

### Changed: the fingerprint documentation named the wrong ordering property

`fingerprint_event_stable` and `hash_json_value` both documented their determinism as
`serde_json::Map` "preserving insertion order". It does not: `preserve_order` is deliberately not
enabled, so `Map` is a `BTreeMap` and keys serialise **sorted**.

The conclusion held — sorted is deterministic, and better than insertion order here, because
insertion order would make a persisted digest depend on a connector's column ordering and two
capture paths for one row would hash apart. But the stated reason was wrong, and it is
load-bearing: a reader checking whether the digest is safe to persist was checking the wrong
property, and enabling `preserve_order` anywhere in the dependency graph would silently change
every stable fingerprint.

Both comments now name the property actually relied on. Two tests pin it:
`a_fingerprint_does_not_depend_on_column_insertion_order`, and one asserting the digest for a
fixed event against a literal — consumers persist these in dedup stores, so the value may only
move as a documented breaking change.

### Fixed: a MySQL binary column's representation depended on its value

`MysqlValue::Bytes` carries almost everything MySQL sends as a string — `VARCHAR`, `TEXT`,
`JSON`, `DECIMAL`, and also `VARBINARY` and `BLOB` — and the connector decided how to render it
from the **bytes**: text when they were valid UTF-8, hex when they were not.

That is not a representation a consumer can decode. A `BLOB` holding `hello` arrived as
`"hello"`; the same column holding `0xDEADBEEF` arrived as `"deadbeef"`, with nothing in the
event saying which happened. A consumer that hex-decodes corrupts the first row (or fails, if the
text is not all hex digits); one that reads text corrupts the second, silently. And a `VARCHAR`
containing the literal text `deadbeef` is indistinguishable from a `VARBINARY` holding those four
bytes.

The configuration reference promised binary columns were "hex-encoded", which was true only for
values that happened not to be valid UTF-8. The type-fidelity test used `X'DEADBEEF'` and
`X'0001FF'` — both invalid UTF-8 — so it passed through the hex branch and never exercised the
other one.

The column's **charset** now decides: collation `63` is MySQL's `binary`, and a character-typed
column carrying it is a binary column. The column *type* cannot tell you — `BLOB` and `TEXT`
share `MYSQL_TYPE_BLOB`, `VARBINARY` and `VARCHAR` share `MYSQL_TYPE_VAR_STRING`, `BINARY` and
`CHAR` share `MYSQL_TYPE_STRING` — which is why only the charset works.

No index arithmetic was written for this: `mysql_common` already resolves each binlog column's
charset from the table-map's `DEFAULT_CHARSET`/`COLUMN_CHARSET` metadata using the same
character-column indexing its own value parser uses, and exposes it as
`Column::character_set()`. The result-set path gets the collation id from the column-definition
packet. Getting that indexing wrong would hex-encode real text, which is worse than the bug, so
it matters that it is the library's and already exercised by its own parsing.

A charset of `0` — metadata absent — keeps the previous byte-derived behaviour rather than
reclassifying every string column as binary. `binlog_row_metadata = FULL` is already required for
column names and key flags, so present is the normal case.

The integration fixture gains `ascii_bytes VARBINARY(16)` holding `X'68656C6C6F'` — valid UTF-8 in
a binary column, the case that was never covered — and the `VARBINARY` assertion is now exact
equality with `"deadbeef"` rather than "hex or base64", which accepted anything.

Covered by `a_binary_column_is_hex_encoded_whatever_its_bytes_happen_to_be` and
`the_charset_and_not_the_type_decides`, both confirmed to fail with the charset check disabled,
plus `non_character_columns_are_never_hex_encoded` — hex-encoding a `DECIMAL` or `JSON` value
would be unrecoverable garbage, and both arrive as `Bytes`.

### Fixed: the column type mapping table still described pre-0.11 JSON numbers

Documentation, and it contradicted both the implementation and the README.

0.11.0 made every column value text. The configuration reference's mapping table was not updated
with it, and still listed integers and floating point as JSON `number` and booleans as
`boolean or number`. An integrator reading it would build a consumer expecting `after.id` to be a
JSON number and find a quoted string — the exact confusion the text contract exists to prevent.

The table now says what happens, with the rule stated once rather than implied per row, and a
note that it described numbers before 0.11.0 so a reader upgrading knows what changed.

The binary row also claimed a single "hex-encoded" form for all three connectors, which was never
true: PostgreSQL emits its own `\x`-prefixed hex, MySQL bare lowercase hex, and SQL Server
whatever `FOR JSON PATH` produces for `varbinary`. There is now a per-connector table. SQL
Server's exact form is deferred to its integration test rather than asserted here, because it is
`FOR JSON PATH`'s behaviour rather than something this crate chooses.

**Nothing pinned the text contract in a test.** `postgres_value_representation_integration` asserted
that the snapshot and stream paths *agree* on each column's JSON type — which two paths both
emitting numbers would satisfy. It now also asserts every scalar is a string, on both paths.

### Fixed: a deliberate re-snapshot was silently dropped by the idempotency guard

Found while adding the on-demand row filter below, which makes re-snapshotting a table a
first-class operation and so made this reachable in a way it had not been.

A snapshot `Read` event's offset identifies the **row**, not a log position — it has no log
position — so re-reading an unchanged row produced a byte-identical event. The runtime's
idempotency guard is **on by default**, and it correctly classified that event as a replay and
dropped it.

So an operator who re-requested a snapshot got `enqueued: 1` back and **no rows**. The component
whose entire purpose is protecting delivery discarded the delivery that was asked for, with
nothing logged. Same failure for a re-snapshot with a narrower filter, which is the shape the new
`SnapshotRequest` makes easy to ask for.

Neither half was wrong on its own, which is why it survived: the guard's fingerprint deliberately
covers content and position rather than wall-clock time, and the snapshot offset is deliberately
row-derived and stable across restarts. What was missing is that nothing recorded *which snapshot
attempt* produced a row.

`IncrementalSnapshotState` gains `generation: u32`, included in the synthetic offset. It advances
on every request and across a stop — a stop discards the table list, so a later request would
otherwise restart at generation 0 and collide with the run it abandoned. A chunk re-read after a
mid-snapshot reconnect stays in its generation and is still deduplicated, so the guard keeps doing
its job. Persisted, so the offsets remain stable across a restart as documented.

`#[serde(default)]`, so existing checkpoints load as generation 0.

The guard's own documentation now carries this, because the dependency runs the opposite way from
how it looks: the guard knows nothing about snapshots, and the driver is responsible for making
distinct reads distinguishable. `a_re_snapshotted_row_survives_the_idempotency_guard` drives the
real driver twice through the real guard and fails if either half regresses;
`a_replay_within_one_generation_is_still_suppressed` pins the behaviour that must **not** change.

### Fixed: `table_conditions` was silently ignored for on-demand snapshots

**Reported by the `rustcdc-server` maintainers (B-4), reproduced against a live PostgreSQL, and
confirmed here.** Their diagnosis was exactly right, including the mechanism.

The row filter was applied at two of the three places a table gets resolved — the startup
tables and the tables adopted from a checkpoint — and **not** at `enqueue_tables`, which
services `StreamHandle::request_snapshot_tables` and therefore every on-demand request. The
driver did not retain the config at all: it was a by-value parameter to `new`, dropped once the
startup tables were resolved, so honouring the filter there was *structurally impossible*.

An operator scoping a backfill to one tenant and firing the request got the entire table.
Nothing reported it; the only symptom is volume, indistinguishable from "that table is big".

**One thing the report understated.** The two paths did not merely disagree about whether to
filter — they disagreed with each other. A runtime-requested table ran unfiltered, and then a
restart adopted it from the checkpoint **with** the filter applied. The rows delivered for that
table therefore corresponded to no single predicate, and where the split fell depended on when
the process happened to restart. That is worse than being unfiltered throughout, because the
result is not reproducible.

Fixed by resolving the condition in **one** function, `describe_with_condition`, called from all
three sites, with the configured conditions retained on the driver for the lifetime of the
snapshot. `describe_table` still leaves `condition` unset, so a backend cannot get it wrong
either.

Also fixed on the same path: re-requesting a **finished** table rewound it but kept its old
spec, so a new request ran under the *previous* request's filter. It now adopts the freshly
resolved spec.

Consumers that guarded against this by rejecting `table_conditions` keys absent from `tables`
can drop the guard.

Covered by `a_runtime_requested_table_gets_the_configured_condition`,
`the_runtime_and_restart_paths_resolve_the_same_condition`,
`re_requesting_a_finished_table_adopts_the_new_condition` — all confirmed to fail with the fix
reverted.

### Added: an on-demand snapshot request carries its own row filter

**Requested as F-8**, and the clean fix for B-4 above.

The filter is a property of the *request*, not the deployment. "Backfill tenant 42's orders" is
a one-off, and routing it through static configuration means editing a config file and
restarting the process to run something that was meant to be a signal. Debezium's
`execute-snapshot` carries `data-collections` and `additional-conditions` together for the same
reason, so a consumer exposing that shape over an API now has somewhere to put the condition.

New `SnapshotRequest`, and the request path takes it:

```rust
use rustcdc::source::SnapshotRequest;

runtime
    .request_incremental_snapshot_filtered(
        SnapshotRequest::new(["public.orders"]).with_condition("public.orders", "tenant_id = 42"),
    )
    .await?;
```

`RuntimeControl::request_incremental_snapshot_filtered` is the same operation from a `&self`
handle, which is the shape an admin endpoint actually has.

A request condition **overrides** the configured one for the same table; a table with no
override keeps its configured filter, so static configuration stays meaningful. Same trust note
as the config field: raw SQL, trusted input, not a tenancy boundary.

**Breaking:** `StreamHandle::request_snapshot_tables` now takes `SnapshotRequest` instead of
`Vec<String>`. `SnapshotRequest: From<Vec<S>>`, so a call site passing a vector needs `.into()`.
`CdcRuntime::request_incremental_snapshot` and `RuntimeControl::request_incremental_snapshot`
keep their signatures and delegate with an empty condition map.

### Added: `IncrementalSnapshotState` reports the effective row filter

**Requested as F-9**, and it is the cheapest possible defence against B-4 recurring.

`IncrementalSnapshotTableState` gains `condition: Option<String>`, holding the filter actually
in effect after merging the request over the configuration. Without it, an operator looking at
`orders: 3,000,000 rows emitted` has no way to tell a filter that applied from one that was
silently ignored — which is precisely the question B-4 makes people ask, and it was
unanswerable from outside.

`#[serde(default)]` and `skip_serializing_if`, so existing checkpoints load unchanged and
unfiltered tables add nothing to the record.

### Changed: the rewind guard no longer refuses a custom source's opaque offset

**Reported as F-10.** Filed as documentation; it was slightly more than that.

`StoredCheckpointRecord::from_offset` did `serde_json::from_slice(&offset.encode()?)`, so it
returned `SerializationError` for any `Offset` whose encoding is not JSON. The rewind guard was
made public *for* third-party checkpoint backends, and this made it unavailable to exactly those
backends — at runtime, with an error naming nothing useful.

The refusal also bought nothing. `stream_position_regression` reads named fields and only knows
the source types this crate ships; for anything else it declines to guess and returns `None`. So
the position comparison the JSON was needed for would have been skipped either way.

A non-JSON offset from a source type the guard does not compare now records `null` and keeps the
committed-event-count check, which is the half that does apply. For a **built-in** source type a
non-JSON encoding is a defect rather than a design choice, and still fails — with an error that
names the source type and says why the encoding matters.

New `checkpoint::compares_stream_position(source_type)` makes the distinction inspectable rather
than something to infer, and `Offset::encode`'s documentation now states what a JSON encoding
does and does not buy: not the shipped comparison, but the ability to write your own on top of
the decoded record.

Covered by `a_custom_sources_non_json_offset_still_yields_a_usable_record`,
`a_built_in_sources_non_json_offset_is_refused_with_a_named_reason`, and
`the_advertised_scope_matches_what_the_guard_actually_compares`, which pins the advertised scope
against the match arms so the two cannot drift.

### Fixed: `stop_incremental_snapshot` was silently undone by the next restart

**Breaking** (state format; `#[serde(default)]`, so old checkpoints load unchanged).

A stop cleared the per-table cursors, and the driver seeds one entry per **configured** table
on startup — so a configured table with no persisted entry looked exactly like a table that had
not started yet. The next deploy re-ran the whole backfill from row zero.

That is the opposite of what the call is for. An operator stops a multi-hour snapshot to take
load off a production primary; the load comes back on the next restart, larger, because it
starts over. And the driver's own log line claimed the opposite would happen: *"the next
checkpoint clears the persisted state, so a restart will not resume it"* — true only for tables
requested at runtime, which are the ones absence *does* correctly describe.

`IncrementalSnapshotState` gains an explicit `stopped: bool`. Absence of entries and
abandonment are now different things: a stopped snapshot seeds no configured tables and stays
stopped until `request_incremental_snapshot` asks for them again, which clears the flag — the
flag must not become a one-way latch, or a re-request would vanish on the next restart for the
same reason.

`#[serde(default)]`, so a checkpoint written before the field existed loads as "not stopped",
which is the previous behaviour and the right reading of a state written by a build with no way
to express a stop.

The remaining durability note is unchanged and deliberate: the flag becomes durable with the
next checkpoint write, and a crash before it resumes a snapshot that can simply be stopped
again. Forcing a synchronous checkpoint would let an operator action rewrite the stream
position, which is the worse trade.

Covered by `a_stopped_snapshot_stays_stopped_across_a_restart` (fails with the fix reverted),
`requesting_a_table_clears_the_stopped_flag`, `a_state_without_the_flag_is_not_read_as_stopped`.

### Fixed: an unbounded SCRAM iteration count was a server-triggered CPU denial of service

The SCRAM-SHA-256 iteration count is chosen by the **server** and is the loop bound of a PBKDF2
derivation the **client** then performs. PostgreSQL's `scram_iterations` accepts anything up to
`INT_MAX`, and neither RFC 5802 nor libpq imposes a ceiling, so an `i=4294967295` — from a
misconfiguration or a hostile server — asked this client for roughly four billion HMAC-SHA256
rounds. That is minutes to hours of pure CPU per connection attempt, free for the server to
trigger and indistinguishable on the wire from a slow handshake.

Two changes:

- **A cap.** Counts above 1,000,000 are refused with an error naming the server setting to
  change. That is ~250× PostgreSQL's default of 4096 and past any deliberate hardening (OWASP's
  PBKDF2-SHA256 guidance is 600k), so a legitimate server never reaches it while the worst case
  stays under about a second.
- **Off the caller's executor.** The derivation now runs on `spawn_blocking`. It is CPU work of
  remote-chosen duration, and this crate runs inside the embedder's Tokio runtime — deriving
  inline stalls a worker thread for the whole derivation, and on a current-thread runtime stalls
  every other task in the process. The same reasoning already puts `FileCheckpoint`'s `fsync` on
  a blocking worker. A handshake happens once per connection, so the spawn costs nothing
  measurable.

`ScramExchange::client_final` is therefore `async` now. It is `pub(super)`, so this is not a
public API change.

Covered by `an_absurd_iteration_count_is_refused_before_the_derivation_runs` and
`an_iteration_count_at_the_cap_is_still_honoured`; the RFC 7677 vectors still pass unchanged.

### Fixed: the in-flight transaction query could error instead of working

Follow-up to the watermark fix below, found while hardening code that has no local test server.
The id reduction was written as `pg_snapshot_xip(...)::text::bigint` and masked in Rust —
but that cast **errors** once an `xid8` exceeds `i64::MAX`, which would turn a
wraparound-epoch database into a hard failure of every chunk read rather than a working
snapshot. The reduction now happens in SQL via `numeric`, which cannot overflow, and the modulo
guarantees the result fits `bigint` before it is read.

### Added: a live test for the one thing that could silently break the watermark fix

The watermark bracket rests on the in-flight transaction ids being on the **same scale** as the
`tx_id` the connector reports. They are different types at source — `pg_snapshot_xip` yields
epoch-extended `xid8`, pgoutput's `BEGIN` carries a bare 32-bit `xid` — so the connector strips
the epoch.

If that reduction is ever wrong, nothing fails loudly: the set simply never matches, the bracket
degrades to the position-only test it replaced, and the race returns with every driver-level
test still green, because those use a fake backend that defines both scales itself. Only a live
server can check the two real ones line up.

`tests/postgres_snapshot_visibility_integration.rs` does it deterministically, without racing an
fsync: open a transaction and leave it uncommitted, assert the backend's own query reports its
id on the reduced scale, then commit and assert the delivered event's `transaction.tx_id` is
that same number. The last step is the half a unit test cannot fake — it is pgoutput's own
value. Wired into the CI matrix and the evidence script.

### Fixed: a truncated composite primary key produced a write that addressed the whole tenant

**Breaking.** `Event::primary_key_values()` used to build a key from whichever declared key
columns happened to be present in the row image, returning `None` only when *none* of them
were. For a single-column key that is the same thing. For a composite key it is not:

```text
primary_key = ["tenant_id", "id"]
after       = { "tenant_id": 7, "name": "…" }     // `id` absent
before      → Some({ "tenant_id": 7 })            // looks like a valid key
```

Nothing downstream could tell that apart from a real key. `RowWrite::Delete { key }` carried it
into `DELETE FROM t WHERE tenant_id = 7`, which removes **every row of that tenant**; an upsert
collapsed the tenant onto one row; as a message key it merged distinct rows into a single
log-compaction group. All silent, and none of it recoverable from the event stream — the
delivered events never described those rows.

`primary_key_values()` is now all-or-nothing: any missing key column yields `None`, so the event
routes to `RowWrite::None { reason: NoRowWrite::MissingPrimaryKey }` and a sink has to handle it
explicitly. A visible gap beats an invisible over-write.

The transform-pipeline guard that rejects a stage for destroying the message key got stronger
for free: a projection or rename that drops *one* column of a composite key now trips it, where
before it silently emitted the partial key.

**Migration:** a sink that matched `RowWrite::Delete`/`Replace` and ignored `RowWrite::None` now
sees `None` for events it previously "handled" with a wrong key. That is the bug surfacing, not
a new one — but log or alert on `NoRowWrite::MissingPrimaryKey` rather than dropping it, because
it means the source is not supplying the full key (on PostgreSQL, usually a `REPLICA IDENTITY`
that does not cover it).

### Fixed: a custom source's `resume_offset_for` was discarded

`StreamHandle::resume_offset_for` is documented as the hook a connector uses to say "an
event's own offset is not where a restart resumes", and as what "the runtime uses for both the
durable checkpoint and the source-side confirmation". It was only ever consulted on the
PostgreSQL path. `build_checkpoint_offset`'s generic branch — the one every custom `impl Source`
takes — read `event.source.offset` directly.

So a third-party connector whose log filters at transaction granularity, which is the exact
situation the override exists for, implemented it correctly and then took the guaranteed
duplicate-per-restart the PostgreSQL connector was fixed for. Nothing surfaced it: the override
was called by no one, the checkpoint looked plausible, and the duplicates arrived one deploy
later.

Every branch now routes through it, so the built-in connectors and a registered custom source
get identical treatment. The default still returns `None` and falls back to the event's own
offset, so MySQL and SQL Server — whose offsets are already exclusive boundaries — are
unchanged. Covered by `a_custom_sources_resume_position_reaches_the_checkpoint`, which fails
with the fix reverted.

This also repaired `cargo clippy --lib --no-default-features -D warnings`, which had been
failing on `resume_offset_for` as dead code: with no connector features enabled, nothing called
it. CI lints `--all-features` only, so the foundation-only profile the README documents did not
pass its own lint gate.

### Fixed: the incremental-snapshot watermark bracket ignored commit visibility

The DBLog override window suppressed a chunk row when a live event for the same key landed in
`(low, high]`. That test assumes anything at or below the low watermark is already visible to
the chunk `SELECT`, and **on every supported database it is not**: reaching the log and becoming
visible are separate steps. PostgreSQL advances `pg_current_wal_lsn()` when it writes the commit
record, flushes it, and only then clears the transaction from the proc array. MySQL's binlog
position advances at the flush stage, before the InnoDB engine commit.

A transaction caught in that gap sits *below* the low watermark and is still invisible to the
chunk read. The chunk therefore held its **pre-image**, the position test did not suppress it,
and the chunk row was emitted after the newer stream event — resurrecting the stale value. The
window is one commit's flush-to-visibility gap, an fsync long under `synchronous_commit = on`;
over a multi-hour snapshot of a hot table it is not theoretical.

`IncrementalSnapshotBackend` gains:

```rust
async fn in_flight_transactions(&mut self) -> Result<HashSet<u64>>   // default: empty
```

The driver calls it **after** the low watermark and **before** the chunk read, and tests
`position <= high && (position > low || in_flight.contains(tx_id))`. That call order is what
makes the pair exhaustive: a transaction invisible to the chunk's snapshot either was running
when the set was read — so it is in the set — or started afterwards, in which case it commits
after the low watermark was read and the position test catches it. There is no third case.

Per connector:

- **PostgreSQL** implements it from `pg_current_snapshot()`, truncating `xid8` to the 32-bit xid
  pgoutput's `BEGIN` reports. Closed.
- **SQL Server** needs nothing. `fn_cdc_get_max_lsn()` reports what the capture job has already
  harvested, so its watermark *lags* visibility instead of leading it — the safe direction, at
  the cost of harmless over-suppression.
- **MySQL / MariaDB: closed, by a different mechanism.** No in-flight *transaction id* is
  available on a scale a binlog event shares, so `in_flight_transactions` stays empty here — but
  the executed-GTID set closes the same gap without needing one, and this release adopts it. See
  the entry below.

`in_flight_transactions` has a default so third-party backends still compile, but accepting the
default is a correctness decision — the trait documentation says so, and says which way to go
when your database has no usable list.

Regression tests drive the real state machine and fail with the fix reverted:
`a_transaction_below_the_low_watermark_but_still_invisible_is_suppressed`,
`an_unrelated_transaction_below_the_low_watermark_suppresses_nothing`,
`the_high_watermark_still_bounds_the_in_flight_set`.

### Fixed: the override window lost unchanged-TOAST columns it could have recovered

Suppressing a *complete* chunk row in favour of an *incomplete* stream event traded one gap for
another. A PostgreSQL unchanged-TOAST `UPDATE` omits the large column, so its event is a
`RowWrite::Merge` — and a merge into a row the consumer does not have yet, the normal case during
a first snapshot, applies nothing. The chunk row that carried the column had just been dropped,
so no delivery contained it. Silent, and narrow enough to survive a staging soak: it needs a row
whose value crosses the ~8 KB TOAST threshold, updated without touching that column, inside the
watermark bracket of the chunk containing it.

Emitting the chunk row anyway is not the fix — placing its pre-image after a newer event
resurrects every *other* column's stale value, which is worse. Instead the **event is now
repaired from the chunk's own image of that row** and delivered as a complete
`RowWrite::Replace`, so the suppression costs nothing.

`Event::unavailable_columns` says such a value is unrecoverable because reading it back
out-of-band races concurrent writes. That objection does not apply here, and the reason is the
whole argument:

- The value is not a fresh read. It is **this chunk's** `SELECT`, at a snapshot whose position
  the driver knows.
- `unavailable_columns` means the `UPDATE` did not modify those columns, so their post-event
  value equals their value at the start of the event.
- Anything that could have changed a column in between is another transaction committing after
  the chunk snapshot — therefore also inside the bracket, therefore already folded into the
  chunk's image. If it modified the column it carried it; if not, the chunk value stands.

The driver knows every write between the read and the event. That is exactly what an out-of-band
read does not, and it is why this is a repair rather than a guess. The chunk row doubles as the
shadow image, updated by each in-bracket event, so a later event omitting a column an earlier one
wrote is filled with the *new* value.

Deliberately bounded. Only columns the chunk actually read, only for that key's own row. An event
for a key outside the current chunk passes through untouched — nothing is being suppressed for it,
so nothing is lost and nothing may be invented. An event past the high watermark is left alone
too: the chunk is delivered first, so the consumer has the row and the merge is correct. A column
that genuinely cannot be filled — a schema change inside the chunk window — is logged at WARN with
the table, key and columns rather than passing silently.

The `Event::unavailable_columns` and API-guide notes now carry the exception, so the
documentation no longer contradicts the behaviour.

Six tests, three of which fail with the repair reverted:
`an_omitted_toast_column_is_filled_from_the_chunks_own_image`,
`a_later_event_is_filled_from_an_earlier_events_value_not_the_chunks`,
`a_partial_before_image_is_filled_from_the_pre_event_state`,
`a_repaired_event_still_validates`, `an_event_for_a_key_outside_the_chunk_is_left_alone`,
`an_event_past_the_high_watermark_is_not_repaired`.

### Fixed: filter thresholds compared through `f64`

The crate emits column values as text specifically because a JSON number is an IEEE-754 double
by the time most consumers see it. `FilterProjectionTransform`'s ordering operators then parsed
both sides back into `f64`, reintroducing that loss at the point where it decides whether a row
is kept:

```text
9007199254740993 > 9007199254740992   // f64: false. Both round to the same double.
```

A threshold filter on a snowflake id or a `numeric(38,4)` amount silently dropped or kept the
wrong rows. `Lt`/`LtEq`/`Gt`/`GtEq` now compare exact decimals — sign, then integer digits by
length and lexicographically, then fraction digits — with no mantissa ceiling and no new
dependency.

**Behaviour change:** exponent notation (`1e3`) is no longer accepted and evaluates to `false`,
as any non-numeric operand already did. Normalising `1e3` against `1000` needs machinery this
does not have, and a rule that quietly mis-orders is worse than one that visibly matches
nothing. Write the expanded form.

### Changed: table include/exclude lists take glob patterns

**Breaking.** `table_include_list` and `table_exclude_list` matched **exact strings only**, while
the sink router's `table_matches` matched globs, and nothing documented the difference. So
`table_exclude_list = ["public.tmp_*"]` excluded nothing at all — indistinguishable from a set of
tables that never changed — and on the include side an allowlist matching nothing is
indistinguishable from an idle database. Debezium's equivalents take regexes, so operators
arrive expecting patterns to work.

There is now one matcher, shared by routing and connector filtering, with the pattern table
documented in the [configuration reference](site/content/docs/config-reference.md). `*` and `?`
work inside a segment and do not cross the `.`; blank entries are ignored rather than treated as
catch-alls.

Two related fixes came with it:

- **An unqualified entry is schema-agnostic**, so `table_include_list = ["users"]` captures
  `public.users` *and* `tenant_private.users`. That was already true and undocumented, and on an
  allowlist it is a widening of the thing the list exists to bound. It stays — MySQL callers name
  tables bare — but `connect()` now logs a WARN naming each unqualified include entry. The public
  `table_matches` doc table also claimed a bare pattern matched "bare-table only", which
  contradicted both the code and its own test; it now describes what happens.
- **The glob matcher no longer backtracks exponentially.** The recursive form ("consume nothing,
  else consume one byte, recurse both ways") does not return in useful time on
  `a*a*a*a*a*b` against a run of `a`s. Patterns come from operator config rather than untrusted
  input, so this was a latency cliff rather than a vulnerability — but a config typo should not
  be able to hang a pipeline. Replaced with greedy matching and a single backtrack point.

**Migration:** if a list carried a literal `*` as a no-op placeholder, it is now a catch-all.
Audit both lists before upgrading. Entries without `*` or `?` behave exactly as before.

## 0.11.0

A correctness release. Six blockers found and closed — four distinct failure modes: silent
loss of events the caller never saw, guaranteed duplication on every restart, stale rows
written over newer values, and two states a pipeline could not recover from without
hand-editing files. Every one carries a regression test that fails without its fix, most of
them against a live database.

It is also a **breaking** release for anyone reading column values: they are now text on every
connector and every capture path. See the entry below for why, and for the one-line migration.

Four conditions remain open on a 1.0 release, none of them a correctness gate: there is no
end-to-end throughput measurement (stated as an evidence gap rather than inferred from the
microbenchmarks); the `sqlserver` feature's advisory set needs re-checking on each `tiberius`
release; `stop_incremental_snapshot` is durable only from the next checkpoint write; and
`core/runtime.rs` remains large enough to be worth splitting further.

Some of these came from the `rustcdc-server` maintainers reporting against 0.10.0. Every
report was checked against the source before acting on it; one had a diagnosis that was right
about the symptom and wrong about the mechanism, and its suggested fix would not have worked.

### Fixed: a clean restart re-delivered the last transaction, every time

Reproduced against PostgreSQL 16 and now covered by
`tests/postgres_restart_resume_integration.rs`: start, insert one row, idle, shut down
cleanly, restart with **no new writes** — and the row arrives again.

The report diagnosed `START_REPLICATION` as inclusive and suggested resuming from
`checkpoint_lsn + 1`. The symptom was right and the mechanism was not, which matters because
the suggested fix does not work. PostgreSQL logical decoding filters at **transaction**
granularity (`SnapBuildXactNeedsSkip`: skip iff the transaction's *commit record* LSN is
below the requested start). A change's own LSN always precedes its transaction's commit
record, so resuming from `X` — or from `X + 1` — still satisfies `commit_lsn >= start` and
replays the entire transaction, not just the record at `X`.

Under `SqlPeek` it was not a bounded duplicate at all. The peek is non-consuming and has no
client-side position filter, so the transaction was re-emitted on **every poll**: the
reproduction saw 20 copies in 20 polls. The report measured "the whole last batch", which
undercounted it.

The only position that skips the transaction is the one *after* its commit record, which
pgoutput reports as the COMMIT message's `end_lsn`. New on the trait:

```rust
fn resume_offset_for(&self, event: &Event) -> Option<String>
```

`StreamHandle::resume_offset_for` translates a delivered event into the position a restart
must resume from once it is committed. The runtime uses it for **both** the durable
checkpoint and the source-side confirmation, so replication-slot retention and restart
duplicates are fixed together. The default returns `None`, meaning "the event's own offset is
already a boundary" — which is correct for MySQL (binlog `log_pos` is the *next* event's
position) and SQL Server (the window query already calls `sys.fn_cdc_increment_lsn`), and
both were verified rather than assumed.

The PostgreSQL checkpoint offset is now a transaction boundary, which also makes it
monotonic — the previous non-monotonicity that `stream_position_regression` had to tolerate
was a consequence of storing per-change LSNs.

### Fixed: `poll_event_batch` was raced inside `select!` by the crate's own APIs

The report is correct that the future is not cancel-safe: between the source handing over a
batch and the runtime staging it, the events exist only inside the future, which awaits the
durable schema-history write and the transform pipeline first. Dropping it there discards
events that have left the source's buffer, and events added by `enqueue_event` have no source
to replay from at all.

It is now documented under a `# Cancel safety` heading — and, more to the point, **the crate
was doing exactly this itself**. `event_batches_cancellable` raced the token against the poll
in a `select!`, and `run_to_completion` (added earlier in this cycle) copied the pattern. Both
now check the token *between* polls, so cancellation costs up to `max_poll_wait_ms` — the
budget a poll already returns within — instead of silently dropping a batch.

### Fixed: `flush_all` / `close_all` flattened every sink error to Terminal

`TableRouter::send` passes a sink's error through untouched, so a broker connection reset is
`ErrorKind::Transient` and retried. The flush and close paths collapsed everything into
`StateError`, i.e. `Terminal`. The same failure was therefore retried or fatal depending on
*where* it surfaced — decided by batch boundaries rather than by anything an operator
controls.

New `Error::Aggregate { kind, detail }` reports several failures under the most severe kind
present, with `Error::kind()` returning it so `match error.kind()` keeps working. Severity is
ordered by `ErrorKind::severity()` — `Transient` < `Backpressure` < `Configuration` <
`Terminal`, the order of how much a caller must change before retrying — exposed as a method
rather than an `Ord` impl so the ranking cannot drift with declaration order.

`Error` is `#[non_exhaustive]`, so the new variant is additive.

### Fixed: the `SqlPeek` rustdoc named a reason that is not one

It listed `TransportConfig::RustlsConfig` among the reasons to fall back to `SqlPeek`,
"whose pre-built verifier the streaming client cannot consume". The streaming client builds
its own connector and uses an injected config as-is. The bullet was pushing embedders toward
the transport whose cost grows with the source's longest-running transaction, for no reason.

### New: `CdcRuntime::incremental_snapshot_state()`

```rust
pub fn incremental_snapshot_state(&self) -> Option<IncrementalSnapshotState>
```

Live per-table progress — snapshot id, keyset cursor, completion flag, row and chunk counters
— read from the driver rather than from a persisted checkpoint. Also on
`RuntimeAdminSnapshot::incremental_snapshot`, so anything already rendering that struct gets
it for free.

`&self` is the point: an embedder's event loop holds `&mut CdcRuntime` for its lifetime, so
`&mut` would force the answer through a channel. Before this, an operator who triggered
`request_incremental_snapshot` could learn how many tables were accepted and nothing after
that, which for a multi-hour backfill was the whole operational experience.

### New: the checkpoint rewind guard is public

`stream_position_regression` is now `pub`, and `validate_checkpoint_progress` applies it plus
the event-count rules in the same order `FileCheckpoint::save` does — over a new
`StoredCheckpointRecord`. `FileCheckpoint` now calls the shared helper rather than its own
copy, so the two cannot drift.

The documentation says to call it **before** the durable write. That is the mistake the
report describes making: a mirror-then-validate ordering let a rewound position reach the
authoritative store and reported the error afterwards.

### Fixed: replication-slot lag was only sampled while the pipeline was idle

`last_slot_lag_bytes` was assigned only inside the idle-advance branch, which is gated on the
slot being caught up *and* on `slot_idle_advance_interval_ms > 0`. So the metric refreshed
only when lag was uninteresting, went stale exactly while the pipeline was behind, and was
never sampled at all when idle advance was disabled.

New `measure_slot_lag` on the provider is read-only — a `pg_current_wal_lsn() -
confirmed_flush_lsn` read for `SqlPeek`, and free for streaming replication, which already
receives the server's write position on every keepalive. It is sampled on a timer regardless
of the caught-up state; `idle_advance` keeps its guard, because that one jumps the slot to
the current WAL position and would discard an unconsumed backlog.

`replication_slot_lag_bytes()` now returns `None` until the first *measurement* rather than
until the first idle advance.

### New: `rustcdc::rustls_client_config`

The `TransportConfig` → `rustls::ClientConfig` mapping moved from `source::postgres::query`
into `core`, gated on `tls` alone, and is public. An embedder opening one extra connection to
the same source — a lag sampler, a schema probe, an operator tool — no longer reimplements
which root store is used when `ca_cert_path` is absent, the refusal of
`allow_invalid_certificates`, or requiring `client_cert_path` and `client_key_path` together.

It also installs the crypto provider explicitly. `rustls::ClientConfig::builder()` **panics**
rather than erroring when no process-wide provider is installed, which happens as soon as a
dependency graph links more than one — and on a background task that panic takes out a
worker thread. That trap is now behind the export instead of ahead of every embedder.

### New: snapshot pause / resume / stop

```rust
runtime.pause_incremental_snapshot().await?;    // idempotent, returns the previous state
runtime.resume_incremental_snapshot().await?;
runtime.stop_incremental_snapshot().await?;     // returns tables abandoned
```

The live change stream is untouched in every case: only chunk reading is affected, so a
backfill loading a production primary during business hours can be held until the evening
without stopping capture. Before this the only answer was stopping the pipeline and clearing
the checkpoint, which also stopped capture.

Three decisions worth stating, because they are the ones that make it usable rather than
merely present:

* **Pause takes effect at a chunk boundary.** A chunk already read is merged and delivered
  first. Stopping mid-chunk would either discard a read the source has already paid for, or
  strand a merged chunk whose cursor can never be promoted — and the cursor is what makes the
  snapshot resumable at all.
* **The paused flag is durable.** It rides in `IncrementalSnapshotState` next to the chunk
  cursors, so the same atomic checkpoint record carries both. Without that, a pause taken to
  protect a primary would silently lift on the next deploy — the opposite of what was asked
  for. The field is `#[serde(default)]`, so an older checkpoint loads as "not paused".
* **Stop drops undelivered *snapshot* rows but keeps held-back *log* events.** The snapshot
  rows are reads the operator has just asked to stop producing; the log events belong to the
  live stream and discarding them would lose change data.

Stop is deliberately not durable in the same way: the persisted state is cleared by the next
checkpoint write, so a crash in that window resumes the snapshot. Forcing a synchronous
checkpoint from a control path would let an operator action rewrite the stream position, which
is a worse trade than a rare resume of something that can simply be stopped again.

`StreamHandle` gains `set_snapshot_paused` and `stop_snapshot`, both defaulting to
`NotImplemented` so a handle that drives no snapshot says so rather than silently accepting.

### New: `CdcRuntime::control_handle()` — control operations from another task

```rust
let control = runtime.control_handle();     // cloneable, `&self`
tokio::spawn(async move {
    control.pause_incremental_snapshot().await
});
runtime.run_to_completion(shutdown).await?;
```

An event loop holds `&mut CdcRuntime` for its whole lifetime, so every control operation was
otherwise unreachable from an admin endpoint without the embedder hand-building an
mpsc/oneshot bridge and a drain point. `RuntimeControl` is that bridge, written once in the
place that owns the invariants — and the next control operation lands for free.

Reads and writes are handled differently on purpose:

* **Commands** go through the queue and are applied between polls. Not as a `select!` arm —
  `poll_event_batch` is not cancel-safe, so racing it would drop events. Latency is therefore
  bounded by the poll interval, and a loop that has stopped turning leaves a command waiting;
  wrap in `tokio::time::timeout` if the caller has an SLO. Dropping the runtime resolves
  outstanding commands with an error rather than hanging, and `is_connected()` reports it.
* **Progress** is read from a snapshot the runtime republishes every poll, so
  `RuntimeControl::incremental_snapshot_state()` is a plain non-blocking `fn` that cannot be
  starved by a busy pipeline or hang behind a stalled one. It is stale by at most one poll,
  which is the right trade for a number an operator refreshes in a dashboard.

`IncrementalSnapshotState` also gains `rows_emitted()` and `tables_remaining()`, so the common
progress readout is one call rather than a fold.

### Fixed: MariaDB wrote a corrupted binlog file name into the checkpoint

Found by writing the restart-resume suite the docs' claims had never been measured against.
Against MariaDB 10.6 the ROTATE event's name field arrives as
`mysql-bin.000002\x57\x07\x03\x52` — the four CRC32 bytes appended raw, two of them
printable ASCII. The server appends the checksum to the *fake* rotate it sends before the
FORMAT_DESCRIPTION_EVENT, which is before the reader has been told which checksum algorithm
is in force. MySQL does not; only MariaDB showed the damage.

Two failures followed from that one string, and neither was loud:

* **File+position resume failed outright.** The server answers *"Could not find first log file
  name in binary log index file"* and the stream never starts. GTID positioning still worked,
  which is what kept this hidden.
* **The checkpoint rewind guard silently switched itself off.** `binlog_coordinate` parses the
  sequence suffix with `"000002WR".parse::<u64>()`; the `None` that produces reads as "not
  comparable", so a genuinely regressed MariaDB coordinate would have been written without
  objection.

Truncating at the first byte that looks wrong is not enough — `0x57` is `W`. The suffix after
the final `.` must be digits, and that is now enforced; a name that cannot be made valid is an
error rather than a guess written into a durable checkpoint.

Covered live on MySQL 8.0 and MariaDB 10.6 by `tests/mysql_restart_resume_integration.rs`,
which also confirms what the docs previously only asserted: both engines already resume
exclusively, so `resume_offset_for` keeps its default there.

### New: per-table snapshot row filters

```rust
IncrementalSnapshotConfig::new(vec!["public.orders".into()])
    .with_table_condition("public.orders", "created_at >= '2026-01-01'")
```

Debezium's `additional-condition`, on all three connectors: backfill one tenant or one time
range instead of the whole table. It bounds the **chunk reads only** — the live stream keeps
carrying every change to the table, because a filter that reached the stream would quietly
become a capture filter and drop change data.

The expression is parenthesised into the keyset seek, which is load-bearing:
`a > b AND x = 1 OR y = 2` binds as `(a > b AND x = 1) OR y = 2`, returning rows *before* the
cursor so every chunk re-reads them and the snapshot never advances. It is raw SQL and trusted
input, at the same level as the connection string.

### Fixed: sixteen integration suites were never run by CI

The workflow drift guard asserted that *named* suites appeared in the workflow — an allow-list,
silent about suites nobody added. A test that never runs is indistinguishable from one that
does not exist, while still looking like evidence in a review.

Among the uncovered: `custom_source_end_to_end` (the crate's headline extension-point claim),
`logging_structured` (the documented log schema), `crash_recovery_model`, `postgres_query_integration`,
and both example smoke tests. All are now in the matrix and all pass.

The guard is now complete-by-construction: every `tests/*.rs` must appear in the workflow, in a
script CI runs, or in an explicit helper-module list with a reason.

### Fixed: neither OpenTelemetry example could connect to a normal server

Surfaced the moment the example smoke tests were added to CI — the first thing gating them
did was fail.

`sqlserver_to_otel` and `postgres_to_otel` hardcoded `TransportConfig::tls()` with no way to
override it, while every other setting was already env-driven. SQL Server presents a
**self-signed** certificate out of the box and the TLS stack `tiberius` pins rejects it
outright — `invalid peer certificate: Other(UnsupportedCertVersion)`, because that
certificate is X.509 v1 and the verifier requires v3. That is every default install and every
test container, so the example could not be run against one at all. The PostgreSQL example
had the same gap latent: a TLS transport implies `sslmode=require`, which a server with
`ssl = off` refuses.

Both now take `CDC_RS_PLAINTEXT` / `--plaintext`, matching `pg_to_stdout`. The default stays
TLS, so the examples still demonstrate the secure setting.

### Fixed: CI pre-pulled images no test uses, and missed two it does

The pre-pull exists to fetch from a mirror rather than from rate-limited Docker Hub. It warmed
`mysql:8.1` and `mariadb:10.11`, which no test instantiates, while `mysql:8.4` and
`mariadb:10.5` — which the matrices do use — were fetched at run time from exactly the
registry the script was written to avoid. Fixed, and guarded by a check that fails when the
list and the matrices drift.

The README's version claim was corrected to match: PostgreSQL 12/14/15/16, MySQL 8.0/8.4,
MariaDB 10.5/10.6.

### Breaking: column values are now text on every connector and every capture path

**The contract:** every scalar column value is a JSON string; SQL `NULL` is JSON `null`; a
`json`-typed column keeps its structure.

```json
{"id": "42", "amount": "12345678901234.5678", "active": "t", "notes": null}
```

Uncovered while adding the snapshot-filter test. The same column arrived as `{"id": 1}` from a
PostgreSQL incremental snapshot (`row_to_json`, typed) and `{"id": "1"}` from the live stream
(pgoutput text). A sink reaching for `as_i64()` read one and silently saw `None` for the other.
MySQL emitted JSON numbers from both paths; SQL Server parsed its `FOR JSON PATH` payload
straight into `Value`, which routes a `DECIMAL(38,4)` through `f64`.

Nothing asserted cross-path consistency, so nothing objected — each per-path type-fidelity
suite checked its own path and agreed with itself.

**Why text and not typed JSON.** A JSON number is an IEEE-754 double by the time most
consumers see it: `numeric(38,4)` loses its low digits and `bigint` above 2^53 is corrupted
outright, silently and in the value rather than the type. Text carries both exactly.

**Getting to text is two conversions, and only one of them matches.** `row_to_json` then
`json_each_text` fixes the type but not the value — a `boolean` becomes JSON `true`, whose
text is `"true"`. Nor does `::text`: PostgreSQL's `bool`→`text` *cast* also yields `true`.
pgoutput emits `t`, because it calls the type's **output function**, and `format('%s', …)` is
what invokes that same function. The snapshot now builds its payload column by column with
`format`, guarded by a `CASE` so SQL NULL stays NULL rather than collapsing to the empty
string that `format` would produce. Snapshot and stream now agree character for character.

MySQL's two near-identical value converters — one in the query path, one copied into the
incremental snapshot — were deduplicated onto a single one that renders numerics as text.
SQL Server decodes through `serde_json::value::RawValue`, which preserves each value's
original token text.

**This fixed a live precision bug.** SQL Server's `DECIMAL(20,6)` and `NUMERIC(10,4)`
assertions were written as `starts_with` prefixes because the decoder was quietly rounding
them. They are exact equalities now.

**Migration:** read values with `value.as_str()` and parse. `serde_json`'s `raw_value` feature
is now enabled.

### Fixed: five documentation pages were never compiled, and four had rotted

`markdown_doctests` covered the README and five pages; `deployment.md`, `runbook.md`,
`troubleshooting.md`, `reliability-testing.md` and `wasm-transform-sdk.md` were outside it —
including the two pages an operator copies from under pressure.

They were outside it because rustdoc compiles an *unannotated* ` ``` ` block as Rust, and
those pages are full of log output, SQL and shell. Fourteen bare fences are now annotated
(which also fixes their syntax highlighting on the site), and wiring the pages in immediately
surfaced four broken samples:

* `deployment.md` had a `match runtime.poll_event_batch()` fragment that referenced an
  undeclared `ErrorKind` and returned from a non-`Result` scope, and an axum handler with an
  **unterminated raw string** — `r#"…")` instead of `r#"…"#)`.
* `troubleshooting.md`'s SQL Server latency-tuning snippet used `SqlServerSourceConfig`
  without importing it.
* `wasm-transform-sdk.md`'s pooling example referenced an undefined `config` and used `?` in a
  non-`Result` scope.

All four now compile. The axum one stays `ignore` with a stated reason (axum is not a
dependency), but its raw-string bug is fixed either way.

The `admin_snapshot()` JSON sample in `deployment.md` also gained the new
`incremental_snapshot` field, so it matches what the runtime actually emits.

### Other

- `RuntimeSource`'s connector variants are boxed. Inline, the MySQL variant put 632 bytes into
  every `CdcRuntime` regardless of which connector was configured — and into every future
  holding one across an await, which is the cost this crate already boxes futures to avoid.

---

Also in this cycle: a correctness pass over the parts that only fail after a crash. Five
defects, three of them silent data loss and one a **permanent pipeline wedge**, plus the DX
gap that made everyone hand-write the one loop where getting the order wrong loses data.

### Fixed: an incremental-snapshot batch straddling the high watermark resurrected stale rows

The DBLog override window suppresses a chunk row whose key was modified *between* the two
watermarks. An event **past** the high watermark is correctly not suppressed — it committed
after the `SELECT` finished, so the chunk row is still needed as the row's base state — but
the algorithm then requires the chunk to be emitted **at** the high watermark, ahead of that
event. DBLog gets this for free by emitting the buffered chunk the moment it reads the
high-watermark marker out of the log.

rustcdc reads the log in batches, and one batch routinely straddles the high watermark: an
event at LSN 900 (inside the window) and one at 1200 (past it) arrive together. The whole
batch was returned and the chunk followed, so the consumer applied the 1200 value and then
the chunk's older value on top — the exact stale-row resurrection the override window exists
to prevent, moved one step later.

A straddling batch is now split at the first event past the high watermark: head, then chunk,
then tail. While the tail is held back the driver reports no durable position, so the
held-back events cannot be marked consumed before they are delivered.

### Fixed: an ack token could be committed twice, skipping events the caller never saw

`AckToken` is `Clone` and `EventBatch::ack_mode()` mints a fresh copy on every call, so
nothing stopped a second `commit_ack` with the same token. It matched the delivery id, saw a
shorter remaining prefix, and advanced the checkpoint over the **next** N events — which the
caller had never been handed. Ordinary double-ack, silent permanent loss.

Tokens now carry an epoch that the accepting commit spends. A replayed token is refused with
an error naming the cause.

**Breaking:** `AckToken::split_at(n) -> (Self, Option<Self>)` is replaced by
`AckToken::accept_prefix(n) -> Self`. The remainder token was a loaded gun — acknowledging it
claimed events the caller had just declined to process — and both callers in this repository
discarded it. The uncommitted tail is redelivered by the next poll with a fresh token, which
is now the only way to get one.

### Fixed: a replayed DDL wedged the pipeline permanently

Delivery is at-least-once, so a crash between recording a schema change and committing the
checkpoint replays the DDL on restart. Re-applying an `AlterTableDiff` to a schema that
already has its columns — `ADD COLUMN` on a column that now exists — returned `SchemaError`,
failed the poll, and failed identically on **every** subsequent restart from the same
checkpoint. The pipeline never started again, and the only exit was hand-editing state.

`SchemaHistory::record_ddl` now takes a `ddl_id` and is idempotent on it; the runtime passes
the source log position. A recognised replay returns the version already assigned and writes
nothing.

A history that genuinely cannot accept a statement — an `ALTER TABLE` diff for a table an
`InMemorySchemaHistory` no longer remembers after a restart — is now logged at ERROR with the
remedy instead of failing the poll. A gap in an auxiliary index does not justify a dead
pipeline; the event itself is self-describing and still reaches the consumer. A store that is
*broken* rather than inconsistent still fails the poll.

**Breaking:** `record_ddl(ddl)` → `record_ddl(ddl_id, ddl)`. Pass `""` to opt out of replay
suppression.

### Fixed: `fsync` ran on the caller's executor thread

`FileCheckpoint` and `FileSchemaHistory` did every filesystem call — `open`, `write_all`,
`sync_all`, `rename`, and the parent-directory `fsync` — inline in an `async fn`. `fsync` is
unbounded on a contended or networked filesystem, and this crate runs inside the caller's
Tokio runtime: a commit held one of their worker threads, stalling every other task scheduled
on it, and wedging a current-thread runtime outright. The file sink already got this right.

Both now run their filesystem work on `spawn_blocking`.

**Breaking:** `FileCheckpoint::checkpoint_dir` and `::file_mode` are no longer public fields.
Use `checkpoint_dir()`, `file_mode()` and `with_file_mode(mode)`.

### Fixed: `force_stop` returned drained events out of delivery order

Injected events came first, though `poll_event_batch` reaches that queue **last**. An embedder
applying the returned events in order wrote the older value of a row after the newer one. They
are now returned in the order they would have been delivered.

### Fixed: a table rewound behind the snapshot cursor was never read

`request_incremental_snapshot` on an already-finished table that sits *before* the one being
read rewound it, and then the forward-only scan skipped it: the driver parked reporting the
snapshot complete with a table it never touched.

### Fixed: a corrupt pgoutput `TRUNCATE` frame could abort the process

The relation count is a `u32` read off the wire and was used directly as a `Vec` capacity, so
a desynchronised frame asked for a 16 GB allocation instead of producing the decode error the
next read would have raised. It is now bounded by what the frame can actually contain.

### New: `CdcRuntime::run_to_completion` — the delivery loop, in the library

```rust
runtime.register_sink(StdoutSink::new());
runtime.start().await?;
let delivered = runtime.run_to_completion(shutdown).await?;
runtime.stop().await?;
```

`poll → send → flush → acknowledge`, until the token is cancelled. The value of having it here
is the *order*: acknowledging before the flush advances the durable checkpoint past events the
sink never accepted, and a crash in that gap loses them with no error anywhere. One line to get
wrong, failing months later as rows that are simply missing.

This also makes `register_sink` honest. It previously did nothing but close the sink at
shutdown, while its name promised delivery — and the sink had moved into the runtime, so it
could not be used for delivery either.

`poll_event_batch` + `commit_ack` remain fully supported, and are still the right choice when
the write must be coordinated with something the runtime cannot see.

### Other

- `CancellationToken` is re-exported at the crate root. Embedders no longer add `tokio-util`
  themselves and keep its version in step with this crate's — a mismatched copy compiles and
  then cancels nothing, because the two token types are unrelated.
- `examples/mariadb_to_stdout` now demonstrates the runtime-driven loop; `examples/pg_to_stdout`
  stays on the manual one, so both shapes are compiled.
- `start()` resets the per-run counters through one function instead of three drifted copies.
  `total_events_skipped` was reset by none of them, and the `Disabled` source path set no
  `started_at_ms`, so `uptime_ms` stayed at zero for every embedder testing a custom source.

## 0.10.0

A correctness release, plus the one architectural gap the previous release documented rather
than closed.

Six defects, three of them **silent data loss** and one a **security downgrade**, found by
auditing the resume coordinate of each connector against what the source actually guarantees
about it. Every one has a regression test that fails without its fix, most of them against a
live server.

### New: on-demand snapshots — `CdcRuntime::request_incremental_snapshot`

Snapshot additional tables on a **running** pipeline, without a restart:

```rust
runtime.request_incremental_snapshot(vec!["public.orders".to_string()]).await?;
```

This is the equivalent of Debezium's `execute-snapshot` signal, and it needs none of the
machinery: no signal table in the source, so it works against a read-only role and a read replica.
Use it to backfill a table just added to the publication, rebuild a downstream store, or re-run
history through a corrected transform. The live stream is never paused — new tables are chunked
into it exactly like the configured ones, under the same watermark suppression.

A table not tracked is added and read from the start; one already in progress is a **no-op**, so
retrying a request is safe; one already complete is rewound and read again. Every name is resolved
against the catalog before anything is mutated, so a typo fails the whole call rather than
half-applying it.

Requests are **durable**. Because a requested table is not in `with_incremental_snapshot`'s static
list, the driver now also adopts *unfinished* tables from the checkpoint on startup: the config is
the initial set, the checkpoint is the record of work in flight. Without that the request would
look honoured and then silently stop at the next restart. Finished tables are deliberately not
adopted, so a completed snapshot is never repeated.

Pause, resume and stop are not implemented; a snapshot runs to completion or is abandoned by
clearing the checkpoint. New: `StreamHandle::request_snapshot_tables` (default returns
`NotImplemented`).

### `with_incremental_snapshot` never worked through `CdcRuntime`

The first commit containing an incremental-snapshot row failed with
`StateError("snapshot events are pending commit but snapshot handle is unavailable")`.

`start()` deliberately leaves `self.snapshot` as `None` for an incremental snapshot, because the
driver *is* the stream — there is no separate handle. But the commit path demanded one whenever a
pending row carried snapshot metadata, so the very first acknowledgement failed. The feature was
usable only by driving the `StreamHandle` directly, which is exactly what its tests did, so a
green suite reported a working feature that no `CdcRuntime` embedder could use.

The commit path now distinguishes the two kinds of snapshot. A **bulk** snapshot persists progress
through connector-native state and still requires its handle — a missing one with rows pending is
a real state error. An **incremental** snapshot needs no write here at all: its chunk cursors ride
inside the stream's own offset, which the commit barrier already writes in the same atomic record
as the stream position.

### An incremental snapshot silently stopped after a reconnect

The reconnect path rebuilt the stream with `start_stream`, ignoring
`RuntimeConfig::incremental_snapshot`. Since an incremental snapshot is delivered by a driver that
*wraps* the log stream, that dropped the driver and did two damaging things at once, neither
visible:

1. The snapshot **stopped progressing** — no further chunk was read, so it never completed.
2. A plain stream reports no snapshot state, so every checkpoint written afterwards **erased the
   progress record**. A later restart found no snapshot in flight at all, and the un-read tables
   were neither resumed nor reported missing.

Any transient network error during a snapshot reached this path, and a snapshot of a large table is
a long window.

*Measured with the fix reverted:* killing the walsender 25 rows into a 400-row snapshot left it
stuck at 25 forever. `tests/postgres_incremental_snapshot_reconnect_integration.rs` provokes the
disconnect the way production does — `pg_terminate_backend` on the walsender — and asserts the
snapshot still completes with no duplicates.

Boxing was required alongside the fix: inlining `start_incremental_snapshot` into
`poll_event_batch`'s already-large future pushed it past the default 2 MiB thread stack and
aborted with a stack overflow. Both branches of the resume helper are `Box::pin`ned.

### PostgreSQL now uses the streaming replication protocol

`WalTransport::StreamingReplication` is the new default: `START_REPLICATION ... LOGICAL` over
the streaming replication protocol, the mechanism PostgreSQL's own subscribers and
`pg_recvlogical` use. The server pushes WAL as it is written and progress is reported with
Standby Status Updates.

The previous transport, `pg_logical_slot_peek_binary_changes`, is **non-consuming**: PostgreSQL
begins decoding at the slot's `restart_lsn` and only *emits* past `confirmed_flush_lsn`, so any
long-running transaction on the source pinned `restart_lsn` and every poll re-read the WAL gap
between the two. Latency was also bounded by the poll interval rather than pushed. It remains
available as `WalTransport::SqlPeek`, because it needs neither the `REPLICATION` role attribute
nor a direct connection — the fallback for a managed service that withholds one or a connection
that must route through a pooler. Selecting it logs a warning naming the trade-off.

**rustcdc implements the wire protocol itself** (`source::postgres::wire`, ~900 lines): startup,
TLS upgrade, SCRAM-SHA-256 / MD5 / cleartext authentication, the `CopyBoth` loop, and feedback.
`tokio-postgres` exposes no `CopyBoth` or replication-mode API, so the protocol is unreachable
through it; the published crate that does implement it declares `rustls` without
`default-features = false`, which would force rustls's `aws-lc-rs` provider across the whole
build next to the `ring` backend this crate standardises on — and Cargo unifies features, so a
dependent cannot opt out. One crypto backend was worth more than the saved lines.

Two things caught while building it, both worth knowing if you are implementing this yourself:

- **Framing has to be buffered.** A poll has a time budget, so the read must be cancellable, and
  reading a message field by field under a timeout is not cancel-safe: a budget expiring between
  a message's tag and its payload discards bytes that have already left the kernel, and every
  later read is misaligned. The timeout now wraps only the socket fill; decoding consumes only
  complete frames.
- **A poll must block for the first record, then stop.** Waiting the full budget once data has
  arrived makes every record wait for the last one. Against a live server that was the
  difference between a 4-second and a 94-second parity run.

New: `WalTransport`, `PostgresSourceConfig::wal_transport`. `tests/postgres_wal_transport_parity_integration.rs`
captures one workload through both transports and asserts the resulting events — LSNs included —
are identical, so their checkpoints stay interchangeable, and covers SCRAM-SHA-256, MD5 and
checkpoint resume against live servers.

### Breaking: `TransportConfig::Tls` now actually requires TLS on PostgreSQL

`tokio-postgres` defaults to `sslmode=prefer`, which **silently falls back to an unencrypted
connection** when the server refuses the SSL request, and rustcdc never overrode it. A connector
configured for TLS against a server with `ssl = off` therefore sent credentials and change data
in the clear, with no error and no warning — detectable only with a packet capture. Every
PostgreSQL integration suite in this repository was running that way, which is how invisible it
was.

`sslmode=require` is now set on both connections whenever the transport is TLS, and the
replication transport enforces the same rule in its own handshake.

**What breaks:** a deployment pointing a TLS-configured connector at a server without TLS now
fails to connect instead of quietly downgrading. Either enable TLS on the server or state
`TransportConfig::plaintext()` explicitly.

### PostgreSQL: connect could hang forever on a server that went silent

`ReplicationStream::connect` wrapped only the TCP connect in `conn_timeout_secs`. Everything
after it waits on a server reply — the TLS handshake, each authentication round trip,
`ReadyForQuery`, `CopyBothResponse` — so a server that accepted the connection and then stopped
responding hung startup indefinitely, with no diagnostic. A firewall dropping the session
mid-handshake, a server accepting into a backlog it never services, and a TCP proxy pointed at a
dead backend all produce exactly that shape, and an indefinite hang is indistinguishable from a
slow database.

The timeout now covers the whole setup sequence, and the error names the likely causes. Found by
writing the test for it: `wire::tests::a_connect_timeout_is_reported_against_the_configured_budget`.

### Reconnect: the dead stream is now dropped before the backoff, not after

For a source that holds a server-side resource for the life of its stream — a PostgreSQL
replication slot is held by its walsender until the socket closes — the backoff window is
exactly the time the server needs to release it. Closing *after* sleeping made every reconnect
race the server's own cleanup and get refused with *"replication slot is active for PID N"*,
burning an attempt each time. Ordinary retry eventually succeeded, so this cost recovery time
rather than correctness.

### An in-process fake replication server

`source::postgres::wire::tests` drives the real client against a scripted server over loopback,
covering what neither the byte-level unit tests nor the container suites can:

- **The TLS path end to end** — SSLRequest, the rustls handshake, and reading WAL back through
  the TLS socket. The container suites run with `ssl = off`, because provisioning a server
  certificate with the ownership PostgreSQL demands inside a throwaway image is awkward; a fake
  server presents one in-process instead.
- **Cancel safety under a split frame.** The server writes a message's tag and length, waits,
  then writes the payload, while the client's poll budget expires in between. Provoking that
  against a real server means winning a race.
- **Protocol failures a healthy server will not produce on demand** — an `ErrorResponse` instead
  of `CopyBothResponse`, a server declining the TLS upgrade, a cleartext password request over an
  unencrypted connection (refused), and a silent server (the hang above).

Ten tests, no Docker, 0.4 s. `rcgen` is a new **dev**-dependency for the certificate, pinned to
`ring` with default features off so it cannot drag in a second crypto backend.

### Breaking: an out-of-band slot operation needs the pipeline stopped first

Under streaming replication a walsender holds the replication slot for the life of the stream,
and PostgreSQL refuses `pg_replication_slot_advance` or `pg_drop_replication_slot` on an active
slot. `CdcRuntime::stop()` releases it; an operator script that advances or drops a slot must
run after that, not alongside a live pipeline. This did not apply to the SQL-peek transport,
where nothing held the slot persistently.

### MySQL: transaction compression corrupted the resume position

`binlog_transaction_compression = ON` (MySQL 8.0.20+) writes each transaction as one zstd
`Transaction_payload_event`. The driver decompresses it transparently and yields the inner
`BEGIN` / `TABLE_MAP` / rows / `XID` events — whose headers carry **`log_pos = 0`**, because
they were never written to the file individually and have no position of their own. MySQL's own
rule is that the resume coordinate for anything inside a compressed transaction is the *end
position of the payload event*.

Taking the zero at face value made every commit inside a compressed transaction checkpoint at
`<file>:0`. The server rejects a dump request below position 4 outright, so a restart after any
compressed transaction **could not resume at all** — and the checkpoint's monotonicity guard did
not object, because the committed-event count still advanced. GTID-positioned streams were
shielded by their GTID set; the default file+position configuration was not.

Verified against MySQL 8.0 with compression enabled: before the fix the captured offset is
`mysql-bin.000003:0`, after it every event carries the payload's end position and a stream
resumed from one picks up the changes that follow. `tests/mysql_binlog_compression_integration.rs`.

### Incremental snapshot: a mid-chunk restart skipped the chunk

The DBLog driver advanced its keyset cursor when a chunk was **read**, not when it was
delivered. That cursor is embedded in the checkpoint record on *every* commit — including
commits of the live stream events that flow past while the chunk sits in its collect phase — so
the cursor became durable before its rows existed anywhere. A restart resumed *after* them: up
to `chunk_size` rows missing from the snapshot, permanently, with no error and no counter to
notice it by.

The cursor and its row counters are now promoted together, once the chunk's emit queue drains.
A restart re-reads at most one chunk, which is the at-least-once behaviour the pipeline already
documents.

### SQL Server: a truncated window across two capture instances dropped rows

Every capture instance in an LSN window is queried with its own `TOP (max_events_per_poll)`, so
instances truncate at different positions and the only safe stopping point is the minimum
last-row position among them. That "truncation cursor" was a local variable, applied only if the
buffer happened to drain in the same poll. With two or more capture instances a window routinely
yields more events than one poll returns — so it did not drain there, the cursor was discarded,
and the deferred window advance stepped straight over the unread remainder.

Measured against SQL Server 2022 with two capture instances and `max_events_per_poll = 5`:
**55 of 60 rows silently lost.** The cursor is now parked on the stream and applied at the drain
point, which is also the only place it can be applied without making a position durable ahead of
buffered rows. `tests/sqlserver_window_truncation_integration.rs`.

### SQL Server: adding a table to CDC was reported as purged retention

Capture instances do not all begin at the same LSN. An instance enabled after the stream started
— or simply enabled second — has a floor *later* than the current window, and asking
`cdc.fn_cdc_get_all_changes_*` below that floor makes SQL Server raise error 313, the same error
it raises when the cleanup job has purged changes. The connector read that as data loss and
stopped with `Unrecoverable`, telling the operator to re-snapshot and restart from a fresh
checkpoint. **`sys.sp_cdc_enable_table` on a running pipeline took the pipeline down with a
false data-loss alarm.**

Each capture instance now carries its own capture floor and is read from
`max(window_start, floor)`, skipping windows that end before it. The floor is deliberately *not*
refreshed for an instance the stream already knows: if cleanup advances a known instance's floor
past an unread window, that is real data loss and must still surface. Genuine retention loss is
reported exactly as before.

### Breaking: a checkpoint may no longer rewind the stream position

`FileCheckpoint::save` now compares the connector-native coordinate against the record it is
replacing and **refuses a regression**, naming both positions. The five existing safety
invariants are all expressed in terms of the committed-event *count*, and a count keeps rising
while a connector offers a position it cannot have reached — which is worse than forgetting
progress, because the counters report health while the recorded resume point sits before data the
sink has already committed. The MySQL defect above had exactly this shape and nothing objected.

The guard is only as strict as each source allows, because "the position went backwards" is not
universally a defect:

- **MySQL/MariaDB file+position** — compared by binlog sequence then position, since every event
  in a transaction carries the commit position and the binlog is written in commit order. A
  rollover past `binlog.999999` is ordered numerically, not as text, so it is not a regression.
- **MySQL/MariaDB with GTID** — not compared at all. Binlog coordinates are server-local and a
  promoted replica's are routinely lower; the GTID set is what resumes the stream.
- **SQL Server** — the commit LSN only. Both cursor encodings occur in one stream (`{lsn}` from
  per-event checkpoints, `{lsn}:{seqval}:{op}` from an orderly shutdown) and the bare form is a
  *prefix* of the other, so comparing whole strings would read the first commit after a graceful
  restart as a rewind.
- **PostgreSQL** — only a zero LSN. pgoutput emits changes in *commit* order while each keeps its
  own WAL position, so two transactions interleaved in the WAL arrive out of LSN order and the
  checkpoint legitimately moves backwards. A general comparison here would have wedged every
  pipeline with concurrent writers.
- **Anything else** — left alone rather than guessed at.

**What breaks:** a deployment that was silently writing rewound positions now fails loudly at
`save`. That is the intended outcome, but it is a new error where there was none.
[Troubleshooting](site/content/docs/troubleshooting.md) covers how to tell a migration or
failover apart from a defect.

## 0.9.0

Breaking release, driven almost entirely by downstream feedback from rustcdc-server's 0.7 →
0.8 upgrade. Themes: **the WASM feature actually works**, **the schema-registry surface stops
lying about what it carries**, and **a silent misconfiguration becomes an alert**.

### Every WASM module with a data segment failed to load

**This was a critical defect: the `wasm` transform feature was unusable for any real module.**

```
ConfigError("failed to instantiate WASM module for ABI probe: wasm trap: interrupt")
```

wasmtime evaluates the store's epoch deadline while initialising a module's `data` segments.
A fresh `Store` starts at deadline `0`, which equals the engine's starting epoch, so the check
tripped immediately. `WasmRuntime` armed the deadline *after* `linker.instantiate(..)` at two
sites — the ABI probe and every instance-pool slot — so **every module carrying a data segment
was rejected**. Rust, AssemblyScript and TinyGo all emit one for string literals and rodata,
which is every module a real toolchain produces.

It shipped because the entire WAT fixture suite happened to be data-segment-free: a fully
green conformance run while no real module could load. There are now three regression
fixtures with a `data` segment — a unit test, a multi-slot pool test, and
`fixtures/wasm/data_segment.wat` in the conformance contract — because a one-line fixture
covers the whole class.

The load-time epoch ticker now also covers pool instantiation, not just the probe, so a module
whose `start` function never returns is interrupted rather than hanging construction.

### `AsyncCodec`: one type for every registry format

`Codec` and `EventEncoder` are synchronous. `ConfluentJsonSchemaEncoder` and
`ConfluentProtobufEncoder` resolve subjects lazily — correctly, since `RecordName` and
`TopicRecordName` exist to give each type its own subject — so their `encode` is `async` and
fitted neither trait. A sink holding "some codec" could not hold all three Confluent formats,
and every embedder wrote the same three-variant dispatch enum by hand.

`AsyncCodec` + `BoxedAsyncCodec`, with a blanket `impl<T: Codec> AsyncCodec for T`, is that
enum once, in the library. The method is `encode_async`, **not** `encode`: a trait
blanket-implemented over another must not reuse its method names, or `codec.encode(..)`
becomes an `E0034` ambiguity on every synchronous codec with both traits in scope.

### `ConfluentProtobufEncoder` has a key encoder

`ConfluentAvroEncoder` had `encode_key`, `ConfluentJsonSchemaEncoder` had `encode_event_key`,
and the Protobuf encoder had **no key path at all** — so a fan-out mixing codecs silently
paired a registry-framed value with `ProtobufEncoder`'s unframed compact-JSON key, with
nothing in the API signalling the mismatch.

New `KEY_PROTO_SCHEMA` (`proto/event_key.proto`, its own file so the key subject's registered
IDL contains exactly the message it uses) and `ConfluentProtobufEncoder::encode_event_key`.
Keyless events produce a message with the `key` field absent — not empty — matching the
`{"key": null}` the JSON Schema encoder emits and Debezium's behaviour.

### `preflight_schema_registry` checked the wrong schemas

It always checked the **Avro** schemas under Avro record names, whatever codec the pipeline
used. A JSON Schema or Protobuf deployment with `auto_register = false` therefore failed
preflight against a perfectly correct registry, and one with `auto_register = true` ran an
Avro compatibility check against a JSON subject.

It now takes a `SchemaType` and checks that format's schemas under the subject names that
format actually uses — Protobuf derives them from the message's fully-qualified name
(`rustcdc.Event`), not the Avro record name. Schema-identity comparison is per format too:
Avro canonical form, structural JSON, and comment-stripped `.proto` source.

It is also generic over the client (and `?Sized`), and `ApicurioRegistryConfig::preflight` is
a direct entry point — an Apicurio deployment silently got no startup check while a Confluent
one did.

### `ConfluentJsonSchemaEncoder` never set a record name

So `SubjectNameStrategy::RecordName` and `TopicRecordName` failed at **encode** time with
"RecordName strategy requires a record name" — a config error that surfaced only once traffic
was flowing, and only for the two strategies that exist to give each record type its own
subject. Fixed to `io.rustcdc.Event` / `io.rustcdc.EventKey`, matching each schema's `$id` and
the record names the Avro encoder uses.

### `ApicurioRegistryConfig::as_schema_registry_config` silently dropped five fields

`auth`, `request_timeout_ms`, `connect_timeout_ms`, `max_cache_entries` and `retry_policy` all
vanished. A caller who set a retry policy got the `SchemaRegistryConfig::new` default with no
indication their setting had been discarded — from a method whose documented purpose was
keeping the two consistent.

Every field now carries over, and the conversion destructures `self` **exhaustively**, so
adding a field without deciding how it maps is a compile error rather than a setting that
quietly stops taking effect. `pool_max_idle_per_host` and `references` were added to
`ApicurioRegistryConfig` to close the gap; `normalize_schemas` has no Apicurio v3 equivalent
and the method says so.

### `warm_schema_cache` works behind `dyn` erasure

It required the concrete `CachedSchemaRegistry<C>`, so erasure to
`Arc<dyn DynSchemaRegistryClient>` made it uncallable — and erasure is exactly what a
multi-registry deployment needs, since the encoders are generic over the client and every
variant would otherwise exist twice. Warming is most valuable in precisely those deployments,
so the two features could not be used together.

It now takes any `SchemaRegistryClient + ?Sized`, warming through the same cache-populating
path `CachedSchemaRegistry` uses internally.

### An unmatched transform rule is now a metric, not a log line

Masking, filtering and routing all match by pattern against a permissive default, so a typo or
a renamed column disables a rule *silently*. A mask rule that never fires means a column is
shipping in **clear text**; a route rule that never fires means events are going to the
default destination. Nothing errors.

`MaskHashTransform` had a hit counter and an accessor for this. It is now uniform:

* `Transform::unmatched_rules() -> Vec<UnmatchedRule>` and `warn_on_unmatched_rules()` are on
  the trait (default: empty), so `FilterProjectionTransform` and `RouteTransform` report too,
  as does any stage an embedder writes.
* `RuntimeAdminSnapshot::unmatched_transform_rules` aggregates the whole pipeline.
* **`rustcdc_transform_rules_unmatched`** is emitted per unmatched rule, labelled
  `transform`/`kind`/`rule` — and *only* when one is unmatched, so its absence is the healthy
  state and `> 0` is a complete alert rule. Rule identifiers are Prometheus-escaped: a quote in
  an operator-written path would otherwise take the whole scrape endpoint down.
* Each `UnmatchedRule` carries the **consequence**, because that is what makes the alert
  actionable and it differs per transform.

Filter rules count evaluations separately from matches: `FilterMode::All` short-circuits, so a
rule an earlier one prevented from running has not failed to match, and reporting it would be a
false positive that trains operators to ignore the signal.

### `MaskRule::Truncate(0)` is rejected at construction

It produces an empty string, which downstream cannot distinguish from a genuinely empty column
— so the masking is *invisible*, not merely useless, and it is almost always a typo for
`Redact` or `Null`. `Redact("")` has the same defect, and an empty rule path can never match.
All three are now rejected by the new `MaskHashConfig::validate()`, matching what
`FilterProjectionConfig` and `RouteConfig` already did.

### `auto_register = false` was silently ignored by two of the three encoders

`SchemaRegistryConfig::auto_register = false` means *"require the schemas to already exist"* —
the setting a careful operator picks in a managed Kafka environment. `ConfluentAvroEncoder`
honoured it, because it resolves both subjects itself at construction. The JSON Schema and
Protobuf encoders delegate subject resolution to `schemreg`, whose resolution path **is**
`register_schema` with no lookup-only mode — so both **ignored the setting entirely**. An
operator who set it got schemas registered anyway, and none of the schema-identity checking
that setting exists to buy (the C5 Critical from the 0.8 audit).

Found by auditing the same class the Apicurio conversion belonged to: a configured field that
reaches the code and does nothing.

Both encoders now verify at construction that the subjects exist and carry exactly the schema
rustcdc will write, which makes `new` `async` on both — matching `ConfluentAvroEncoder`. With
`auto_register = true` construction still performs no I/O. The one thing that cannot be
prevented is the later `register_schema` call itself; because the content is verified identical
first, a Confluent-compatible registry answers it with the existing id rather than a new
version. That limit is stated on the API rather than glossed.

`ConfluentJsonSchemaEncoder` was also dropping `config.references`, which the Avro and Protobuf
encoders both passed.

### AWS Glue is a backend now, not a promise

The `glue` feature described itself as *"the AWS Glue Schema Registry as a backend"* and
shipped **type re-exports only** — no `Event` encoder, no decoder. An embedder got none of what
every other registry backend does for them and had to write the Avro conversion, the
registration and the 18-byte framing by hand.

New `GlueAvroEncoder`, `GlueAvroDecoder` and `GlueAvroConfig`. The payload is the same
`AVRO_SCHEMA` envelope the Confluent encoder writes, so a consumer that already decodes
rustcdc's Avro events needs only the framing changed. The decoder resolves the **writer**
schema by the header's version UUID and uses it for resolution, so a message written under an
older compatible schema decodes correctly rather than being read positionally against the
current one. `GlueAvroConfig` deliberately has no `auto_register = false`: `schemreg`'s Glue
client has no lookup-by-name API, so the setting could only have been accepted and ignored —
which is the defect above.

Glue remains the one backend with no live-service evidence, because it has no self-hostable
implementation. Everything rustcdc owns — Avro conversion, framing, compression byte, schema
identity, error classification, round trip, key union branch — is covered against an in-memory
fake. That is stated in the feature docs and the API guide rather than implied away.

### Crate-root re-export parity, enforced

Five public items were reachable only as `rustcdc::codec::X` while their direct counterparts
were `rustcdc::X`: `ConfluentProtobufEncoder`/`Decoder`, `AvroDecoder`, `avro_value_to_event`,
and `OutboxTransform`/`OutboxResult`. Nothing was broken — it just cost a docs search per item
and made the surface look arbitrary.

0.8 added a module→parent gate for exactly this class; it now extends one level further, to
crate-root parity — and running it across **every** module found more of the same: the three
concrete `DdlExtractor` implementations sat below the trait, and `IncrementalSnapshotBackend`
— the custom-source extension point the audit calls a differentiator — sat below the
`IncrementalSnapshotConfig` and connector handles that were already at the root.

The rule is now **all-or-nothing per module** and configures itself: if `lib.rs` re-exports
anything from a module, it must re-export everything that module re-exports. Modules kept
namespaced by design (`checkpoint`, `testkit`, `fault_injection`, `deterministic_replay`,
`schema_history`) have no crate-root surface to be inconsistent with and are skipped; adding a
single item from one of them opts it in, which is the intended tripwire.

### Both registry `build()` methods are drift-proofed too

`SchemaRegistryConfig::build` and `ApicurioRegistryConfig::build` now destructure `self`
exhaustively, with the encoder-side fields bound to `_` and a reason. Neither was dropping a
field, but both had the same latent shape as the conversion that was — a new transport option
would have compiled and silently done nothing.

### `sqlserver` brings a second, older TLS stack — and now says so

Everything else in the crate is on `rustls 0.23`. `tiberius 0.12.3` hard-pins
`tokio-rustls 0.24`, so enabling `sqlserver` links `rustls 0.21` / `rustls-webpki 0.101.7`,
carrying RUSTSEC-2026-0098, -0099 and -0104 plus the unmaintained `rustls-pemfile 1.0`. The
per-advisory reachability analysis was already in `site/content/docs/security.md` and
`deny.toml` — but nothing in the README feature table, the Cargo feature list or the connector's
own rustdoc said the feature changed the TLS stack, so a reader choosing features never saw it.
All three now do.

### Breaking changes

| Was | Now | Why |
|---|---|---|
| `preflight_schema_registry(registry, config)` | `preflight_schema_registry(registry, config, schema_type)` | It checked Avro schemas for every codec |
| `MaskHashTransform::new(config) -> Self` | `-> Result<Self>` | `Truncate(0)` and `Redact("")` are now rejected |
| `MaskHashTransform::unmatched_rules() -> Vec<&str>` | `unmatched_rule_paths()`; the trait method returns `Vec<UnmatchedRule>` | The trait method is uniform across stages |
| `warm_schema_cache(&CachedSchemaRegistry<C>, ..)` | `warm_schema_cache(&impl SchemaRegistryClient + ?Sized, ..)` | Unusable behind `dyn` erasure |
| `RuntimeAdminSnapshot` gained `unmatched_transform_rules` | — | `#[non_exhaustive]`; use `..` in patterns |
| `ConfluentProtobufEncoder::new` now requires `C: Clone` | — | The key encoder needs its own registry handle |
| `ConfluentJsonSchemaEncoder::new` / `without_validation` are sync | `async` | They now enforce `auto_register = false` |
| `ConfluentProtobufEncoder::new` is sync | `async` | Same |

### The doc build only ever ran with every feature on

CI built documentation once, with `--all-features`. That configuration is structurally
**blind to a link from an ungated doc comment into a feature-gated item**: with every gate
on, every such link resolves. Turn a gate off — as any downstream crate does when it runs
`cargo doc` on its own dependency set — and the link is broken.

Twelve were, and had been for some time: `TransportConfig::RustlsConfig` (`tls`),
`SqlServerSourceConfig::capture_truncate_events` (`sqlserver`), `MaskRule::HmacSha256` and
`MaskRule::Encrypt` (`encryption`), and five more this release added in the `AsyncCodec` docs
pointing at the `schemreg` encoders. All now name the gated item as plain code rather than
claiming a link target that may not exist, with a note saying why.

CI gained a second lane, `cargo doc --no-default-features --no-deps` under `-D warnings`. The
two extremes are complementary: an ungated item cannot link to a gated one without one of them
failing. The workflow-drift guard requires both, anchored on the `run:` line rather than the
step name — an unanchored pattern is satisfied by the label alone and would still match after
the command underneath it changed.

The build is verified clean across eight feature combinations, not just the two CI runs.

### Docs

`api.md` gained an "AWS Glue" section, an "Unmatched rules" section and a "Holding several codecs behind one type"
section; the Protobuf, preflight, cache-warming and Apicurio sections were rewritten against
the new surfaces. `config-reference.md` and `runbook.md` document
`rustcdc_transform_rules_unmatched`, the latter with per-`kind` remediation. The
`IdempotencyOptions` rustdoc now shows the `?`-per-step form with a `compile_fail` example of
the chain that does not work.

## 0.8.0

Breaking release. Themes: **restart correctness**, **evidence that can fail**, a **full
dependency refresh**, and **documentation that cannot rot**.

### One incremental snapshot, not three

The DBLog watermark algorithm was copied once per connector — 2,771 lines across three files
implementing the same state machine, the same override window and the same `StreamHandle`
contract, differing only in the position type and the SQL dialect. The copies drifted: the
C1 resume-from-cursor fix had to be applied three times because the same missing feature
existed three times, and the cursor-arity check that guards a changed primary key existed in
only two of them.

It is now one implementation, `IncrementalSnapshotDriver`, plus a six-method
`IncrementalSnapshotBackend` per connector. Connector-specific code dropped to 263 / 348 / 422
lines.

Three consequences worth stating:

* **A custom source can have incremental snapshots.** The API guide previously said it could
  not — "the DBLog watermark algorithm needs connector-native watermark queries that the
  `Source` trait does not expose". The backend trait *is* that surface, it is public, and it is
  not gated behind any connector feature. The built-in connectors take no private path.
* **Row identity is now derived identically on both sides of the override window.** Chunk rows
  and stream events both fingerprint from the row payload through one function, so they agree
  by construction. Previously each connector derived the two sides independently — PostgreSQL
  compared text-cast cursor values against JSON payload values, and the two agreeing was a
  property of careful construction rather than of the code.
* **The cursor-arity check runs for every connector**, hoisted out of the two that had it.
* `BinlogPos` and `CdcLsn` implement `Ord` explicitly rather than deriving it. A derived `Ord`
  on `(String, u32)` compares `binlog.000010` as *less than* `binlog.000009`, which would make
  the override window compare backwards at every file rollover.

Verified against PostgreSQL 16, MySQL 8.0, MariaDB 10.5/10.6 and SQL Server 2022, including
the mid-snapshot restart test that fails against the pre-fix behaviour.

### `Event` is `#[non_exhaustive]`, with a builder

`Event`, `SourceMetadata`, `SnapshotMetadata` and `TransactionMetadata` are now
`#[non_exhaustive]`. Adding a field to the envelope was previously a breaking change for every
construction site — it broke this crate's own published adapter SDK example in 0.7.0.

Downstream code builds them through `Event::builder(table, op)` and `SourceMetadata::new(..)`.
The builder sets `envelope_version`, which is not a compile error to get wrong by hand but
makes the event fail validation at the far end of the pipeline. `build_validated()` enforces
the envelope contract where the event is produced rather than where it is consumed.

**Migration:** replace `Event { .. }` with the builder. Struct literals still work inside this
crate; they stop compiling in yours.

### Type fidelity: two silent-corruption defects found and fixed

MySQL and SQL Server had no type-fidelity coverage — every integration schema was `BIGINT` +
`VARCHAR`. That is the same gap that let the original SQL Server null-substitution defect
survive. Adding the suites immediately found two more, both of the worst shape: a *plausible
wrong value* delivered as authentic.

* **`ENUM` was delivered as its ordinal.** A row holding `'happy'` arrived as `1`. That is a
  valid-looking integer that means something different the moment the enum's declaration order
  changes. The labels are in the binlog table-map's optional metadata, which
  `binlog_row_metadata=FULL` already supplies; the connector now resolves them.
* **`SET` was delivered as an unreadable control character.** The binlog carries a
  little-endian bitmask in raw bytes; reading those bytes as text yields control characters
  that are *valid UTF-8*, so the wrong reading failed silently rather than erroring. It now
  expands to comma-joined labels.
* **`DATE` gained a midnight time**, reported as `2026-07-20T00:00:00.000000`. `mysql_common`
  collapses `DATE`, `DATETIME` and `TIMESTAMP` into one value variant, so the column type is
  the only thing that separates them — truncating whenever the time is zero would instead
  strip the time from a `DATETIME` that genuinely falls at midnight. The connector now consults
  the column type. (The first attempt at this fix changed nothing: MySQL writes
  `MYSQL_TYPE_NEWDATE` in the binlog and reserves `MYSQL_TYPE_DATE` for the wire protocol.)

The full mapping is documented under the column type mapping section in the configuration reference, and
the SQL Server suite asserts non-null on every `NOT NULL` column specifically to catch a
regression of the original null-substitution shape.

### Fixed: the PostgreSQL stream could stop delivering permanently under load

`pg_logical_slot_peek_binary_changes` is **non-consuming** — it re-decodes the entire
un-acked backlog on every call. When a peek exceeded its `statement_timeout`, the connector
retried with the *same* window, which meant repeating the identical decode that had just
failed. On a saturated server that never succeeds: the pipeline stops delivering
permanently while the changes sit unread in the WAL.

This is what CI was reporting as *"no new events for 90s at 1994/2000 committed; the writer
committed all 2000 rows, so the events exist and the pipeline stopped delivering them"* — a
livelock, not a slow machine. It reproduced only under load, which is why three CI runs saw
it and no local run did.

The peek window is now adaptive: a timeout halves it (floor 1), so every retry asks the
server for strictly less work than the attempt that just failed and the sequence converges
on a window that decodes — forward progress is guaranteed rather than hoped for. A
successful poll doubles it back toward `max_events_per_poll`, so a transient spike does not
permanently cap throughput. The shrink logs a WARN naming both windows.

The existing `slot_is_caught_up` guard already stopped a timed-out poll from being mistaken
for an idle slot (which would have advanced the slot past the backlog and *lost* it). That
guard was correct and remains; it prevented data loss but not the livelock.

### Latency evidence fails on a stall, not on a slow machine

All three latency suites used a fixed total budget — "collect 2,000 events within 180 s". A
CI runner hit that wall at **1,995 of 2,000**: the pipeline was still delivering, and the
test reported a timeout. The same run takes 5.5 s locally, so the budget was calibrated for a
machine roughly 30× faster than a loaded runner.

A latency test that cannot distinguish *slow machine* from *stuck pipeline* provides no
evidence either way. The deadline is now progress-based (`ProgressDeadline`): it fails when
no new events arrive for a sustained window — the same signal the runtime's own
`HealthVerdict` treats as alertable, and one that does not depend on machine speed. A
generous absolute backstop remains so a pathological trickle cannot hang CI, and its message
distinguishes the two cases.

**That immediately paid off, and corrected the first diagnosis.** The next run reported *"no
new events for 90s at 1996/2000"* — 90 seconds of zero progress is not a slow machine, so the
initial reading ("healthy, just slow") was wrong. The suites now also publish writer progress
(`WriterStatus`), because the writer task's `Result` is only observable *after* the loop, and
a stalled loop never gets there: a writer that dies at row 1996 is indistinguishable from a
stalled pipeline. A dead writer now fails the run at once with its own error, and a stall
names which side stopped — *"the writer had only committed 1996/2000 rows, so the missing
events were never produced"* versus *"the writer committed all 2000 rows, so the pipeline
stopped delivering them"*. Six unit tests cover progress, stall, backstop, writer failure and
both attributions.

### CI failures fixed

Three unrelated CI failures, all real:

* **The four process-kill suites tripped the checkpoint owner lease.** Each opened a
  `FileCheckpoint::new(dir)` purely to *read* the checkpoint after killing the worker, then
  built a runtime against the same directory — two writer instances, one lease. The C4 fix
  that added the lease was correct; only one of the seven call sites had been converted to
  `FileCheckpoint::read_only`. All four suites (PostgreSQL, MySQL, MariaDB, SQL Server) now
  use the read-only handle for inspection.
* **Nightly renamed `AtomicUsize::fetch_update` to `try_update`.** CI lints nightly with
  `-D warnings`, so the deprecation broke the build; naming either method directly breaks
  one toolchain or the other. Replaced with an explicit `compare_exchange_weak` loop, which
  is stable on both.
* **MSRV raised from 1.92 to 1.94.** `sqlx` 0.9 (a dev-dependency) requires 1.94, and
  Cargo's resolver considers dev-dependencies, so the MSRV job failed. The library itself
  still compiles on 1.92, so this could have been papered over by excluding dev-deps from
  the resolve — but that leaves two MSRV numbers to keep straight and a special tool in CI
  to explain. One number, verified on exactly the toolchain it names, is worth the bump.

  **Migration:** requires Rust 1.94 or newer.

### `SqlServerOffset` accepts pre-0.8 checkpoints

`SqlServerOffset::from_bytes` did a strict struct parse, so a checkpoint written by 0.7.x —
where the cursor was a bare JSON string — failed to load with a serde type error, leaving an
operator to guess whether capture had lost its position. It now accepts both forms, which
also makes the checkpoint loader agree with `sqlserver_cursor_from_offset_bytes`, which
already did.

### Errors an operator can actually read

* **`Error::report()` and `Error::chain()`.** `Display` on a contextual error shows only the
  outermost layer — that is the `thiserror` convention, and `{:#}` is identical because
  `thiserror` does not implement alternate-flag chaining. So `tracing::error!("{e}")` printed
  *"acknowledging batch 7"* and nothing about the disk being full: **adding context actively
  hid the cause**. `report()` renders the whole chain on one line, `chain()` iterates it
  outermost-first, and the crate's own eight error-logging sites now use `report()`. The doc
  comment that claimed `{:#}`-style chain printers work has been corrected, and a test pins
  the real behaviour.
* **`render_error_chain` for foreign errors.** `tokio_postgres::Error` displays as *"error
  connecting to server"* whether the socket was refused, DNS failed, or the handshake timed
  out — the real cause sits behind `source()`. Connector code that formatted it with
  `{error}` threw that away. Connection failures now read
  `postgres tls connection failed: error connecting to server: Connection refused (os error 61)`.
  A cause a library has already folded into its own `Display` is not repeated —
  `mysql_async` does that, and naive joining printed it twice.

The previously recommended bulk `.context(..)` migration was **withdrawn** after measuring:
the remaining sites already name both the operation and the cause, and wrapping them would
add a layer without adding information.

### The custom-source extension point, driven end to end for the first time

`register_source` is the crate's headline claim for third-party connectors. It had never
been driven through the runtime by a test. Doing so found four defects, three of them in
promises the docs already made.

* **A custom source's offset did not round-trip.** The runtime persisted
  `serde_json::to_vec(&event.source.offset)`, so a connector whose offset was `42` was
  handed back `"42"` — quotes included — on restart. The `Source` docs say the offset is
  persisted *verbatim*, and `Offset::encode` requires that "whatever `encode` produces has
  to be decodable back into a resumable position by the connector that wrote it". Now
  persisted as raw bytes.
* **`ConnectorCapabilities` could not be constructed outside this crate.** It is
  `#[non_exhaustive]` with no `Default` and no builder, so `..none()` was rejected too —
  the only reachable value was `none()` itself, making `Source::capabilities` impossible to
  override honestly. **New:** `const with_*` builders for every capability, plus `Default`.
* **`HandoffResult` had no `Default`**, despite being the required return of a method every
  custom source must implement. Added.
* **`PreserveTransactions` did not deliver the guarantee it documents.** The trim consulted
  only the queue *behind* the cut, so an empty queue was read as "there is no rest" rather
  than "I have not seen the rest yet". A transaction spread across two source polls — the
  normal case for a streaming connector, not the exception — was delivered split anyway.
  The runtime now withholds a trailing transaction until it has positive proof the
  transaction ended: either the event declares its own position
  (`event_index + 1 == total_events`), or a later event belongs to a different transaction.

  Fixing that exposed a **wedge**: the runtime drains its queued events before polling the
  source, so withholding a whole batch meant the rest of the transaction could never
  arrive — the same events were re-cut and re-withheld forever. The poll path now falls
  through to the source when everything was withheld. `max_buffer_size` remains the escape
  hatch for a transaction that genuinely cannot fit, and it still ships split with a WARN.

### Two unreachable public types, and a gate so it cannot recur

`ConfluentProtobufEncoder` and `ConfluentProtobufDecoder` were public in
`codec::schema_registry` but never re-exported from `codec`, so nothing outside the crate
could name them — the codec with no live test coverage was also the one nobody could
import. `AVRO_SCHEMA` was in the same state, while the module docs told readers to register
it with their registry.

The policy gate now checks that every public item in a codec or driver module is named by
its parent, negative-tested in both directions.

### Live registry coverage — three defects in codecs that had never spoken to a registry

The audit named this the largest single evidence gap: the Apicurio backend, the Confluent
Protobuf codec and the registry helpers compiled and were unit-tested where the logic was
local, but none had ever talked to a real registry. A suite against Apicurio Registry 3 —
which serves both its native v3 API and a Confluent-compatible one, so one container covers
both client paths — found three defects on the first run.

* **`ConfluentAvroDecoder` had never successfully decoded an event.** `before` and `after`
  are deliberately Avro `bytes` holding UTF-8 JSON, which keeps the Avro schema stable
  regardless of table structure — and `apache_avro::from_value::<Event>` cannot reverse
  that. Every decode failed with *"invalid type: byte array, expected any valid JSON
  value"*. There was no working Avro → `Event` path at all: `AvroEncoder` had no
  counterpart, and the encoder's tests decoded to a raw Avro value and inspected individual
  fields rather than reconstructing an event. **New:** `AvroDecoder` and
  `avro_value_to_event`, hand-written to mirror the encoder, with round-trip tests covering
  every operation, both availability lists, snapshot and transaction metadata, and the
  `None`-vs-`Some(null)` distinction. An unknown operation symbol is rejected rather than
  defaulted — defaulting to `Insert` would turn a foreign message into a row creation a sink
  would apply.
* **`EVENT_JSON_SCHEMA` rejected every INSERT and every DELETE.** The row payload was
  `oneOf: [{"type": "null"}, {}]`, and the empty schema matches `null` too — so `null` was
  valid under *both* branches and `oneOf` rejected it. The JSON Schema codec could not
  encode a normal event.
* **…and every partial-payload event.** `unavailable_columns` and
  `before_unavailable_columns` are `skip_serializing_if = "Vec::is_empty"`, so they appear
  only on partial payloads — and the schema declared `additionalProperties: false` without
  listing them. Exactly the events whose correct handling this crate emphasises most were
  the ones it would have rejected. Both fixed, with tests validating real events against the
  published schema through the same validator the encoder uses.

Also clarified: `SchemaRegistryConfig::url` is the API root that serves `/subjects`, while
`ApicurioRegistryConfig::url` is the server root and the client appends `/apis/registry/v3`
itself. Passing the full path to the latter produced a doubled URL and a 404 — the doc
comment said only "registry base URL".

**AWS Glue remains untested against a live service.** Its framing and identity are
unit-tested, but there is no self-hostable implementation to point a container at, so the
absence of live coverage is stated in `site/content/docs/api.md` rather than left for a reader
to infer from a green suite.

### Evidence labelling

* `tests/crash_simulation_integration.rs` is now `tests/crash_recovery_model.rs`. It drives an
  in-memory validator; nothing is killed and no database is involved. The old name read as
  though it were one of the four real process-kill suites, which the audit flagged as
  misleading evidence. Its module docs now say what it does and point at the real ones.
* The stale local `BENCHMARK_REPORT.md` was deleted. It carried three "do not cite this"
  warnings, was pinned to a dirty tree at an old commit, and is gitignored — a generated
  artifact whose stale copy was the only problem.

### Measurement fixed, and it immediately found a real defect

The latency gate measured the wrong thing. It inserted every row *before* the measurement
loop started, so `poll_latency` timed draining an already-populated in-process `VecDeque`
and `commit_latency` timed one fsync — microbenchmarks of the runtime's own bookkeeping
against a pipeline that was never under load. The p95 ≤ 500 ms threshold sat two to four
orders of magnitude above a `VecDeque` drain, so **the gate could not fail for performance
reasons.**

It now measures **capture latency**: wall-clock time from the writer committing a row to
the event reaching the consumer, with writes running concurrently with polling, measured
against a single clock (the writer and reader are the same process, so container/host drift
cannot contaminate it).

Turning it on immediately exposed a genuine MySQL connector defect. Batch assembly was
bounded only by `max_events_per_poll`, with a per-event read timeout and **no wall-clock
limit** — so under a writer that kept producing, the loop never broke early and accumulated
until it hit the cap. The first event of a 1,000-event batch waited for the other 999,
which is exactly what the caller's `max_poll_wait_ms` was supposed to bound and did not:

| MySQL 8, 2,000 rows | before | after |
|---|---:|---:|
| capture p50 | 431 ms | **55 ms** |
| capture p95 | 1,559 ms | **99 ms** |
| capture p99 | 1,970 ms | **117 ms** |
| sustained throughput | 135 ev/s | **375 ev/s** |

PostgreSQL, unaffected by the same bug, measures p50 12 ms / p95 18 ms / p99 19 ms.

The gate now also refuses to pass on a run it could not measure: it requires a minimum
sample count and zero unmeasured events, where the previous assertion was `batches > 0`.

### Breaking changes

#### Incremental snapshot progress is persisted (was: re-read everything on every restart)

The DBLog incremental snapshot tracked its per-table keyset cursor in memory only.
`save_position` persisted the stream offset and dropped the cursor, so **every restart
re-read every configured table from row zero** — a duplicate flood proportional to the whole
dataset rather than to the crash window, repeating until a snapshot happened to finish inside
a single process lifetime. The module documentation claimed each chunk was "independently
resumable after a crash".

Chunk cursors now travel inside the connector checkpoint offset, so they become durable in
the same atomic, fsynced, checksummed write as the stream position — a cursor is only
meaningful relative to the position it was captured against, and two separately-written
records could disagree after a crash between them. Fixed on all three connectors.

**Breaking:** `PostgresOffset` and `MysqlOffset` gain an `incremental_snapshot` field, so
struct-literal construction needs `..Default::default()` or the new `PostgresOffset::new` /
`MysqlOffset::new` constructors. SQL Server offsets move from a bare JSON string to a typed
`SqlServerOffset { cursor, incremental_snapshot }`; existing SQL Server checkpoint files are
not readable and must be re-seeded (see `examples/seed_checkpoint.rs`).

`StreamHandle` gains `position_offset()` and `incremental_snapshot_state()`, both defaulted.

#### `commit_ack` no longer wedges the runtime on a checkpoint-store failure

Acceptance and the durable write were two steps. If the write failed, the acceptance marks
stayed applied, so the natural retry failed with *"acceptance notification exceeds pending
records"* **forever**, `add_event` refused because the barrier stayed `Flushing`, and
`stop()` refused because events were pending. The only exit was `force_stop()`, which
discards them. One transient disk-full was enough.

`CommitBarrier::accept_and_commit` is now one transactional operation that restores the exact
pre-call state on failure, so retrying the identical `commit_ack` is correct.

#### The idempotency guard no longer drops rows it cannot identify

The fingerprint is content-derived, so two genuinely distinct rows that are byte-identical
hash identically. `INSERT INTO pings VALUES ('ok'), ('ok')` on a keyless table, on a
connector with no intra-transaction sequencing, produced two events sharing one source
offset — and the guard dropped the second. The checkpoint then advanced past it: permanent,
silent, unrecoverable data loss, in the component whose job is to protect delivery.

The guard now suppresses only events it can identify (transaction metadata, or a primary key
whose columns are actually present in the row image). Everything else passes through and is
counted. Passing a duplicate through is at-least-once — the documented contract. Dropping a
distinct row is not recoverable by anyone.

**Breaking:** deployments relying on the guard to deduplicate keyless tables will now see
those duplicates. Add a primary key, or deduplicate in the sink on a key you control.

#### One writable `FileCheckpoint` / `FileSchemaHistory` per directory, enforced

A second instance on the same path in the same process wrote the same `HOSTNAME:PID`, so the
on-disk decision table classified it as a *re-entrant* acquire and let it through. Both then
held independent in-memory state and rewrote the whole file, silently destroying each other's
records.

A second **writable** instance is now refused. Reading is not dangerous and is not
restricted: `FileCheckpoint::read_only(dir)` takes no lease and can inspect a directory a
runtime owns — a readiness endpoint, an operator tool, a test assertion — while refusing to
write.

Durable writes are additionally **fenced**: the lease file is re-read before every write and
the write is refused if the token is no longer ours. Acquiring a lease once is not holding
it — an operator can delete the sentinel file, and a peer that saw this process as dead can
take it over.

#### `Transform` is synchronous; `AsyncTransform` is the escape hatch

Every transform this crate ships — masking, filtering, projection, field mapping, routing,
unwrapping, outbox — is pure CPU work over an in-memory event. The trait was nonetheless
`async`, so `#[async_trait]` boxed a future for each of them on **every event**: O(events ×
stages) heap allocations on the hottest path in the library, to await something that never
yields.

`Transform::apply` is now `fn`. A stage that genuinely must await — WASM, a network
enrichment lookup — implements the new `AsyncTransform` instead, registered via
`CdcRuntime::add_async_transform`. `TransformPipeline` holds both and pays the boxing cost
only where it is needed.

Both traits gain `apply_batch`, and `TransformPipeline::apply_batch` runs a whole delivery
through each stage in turn rather than each event through the whole pipeline. The runtime
uses it under the default `Halt` policy. `Skip` keeps the per-event path, because it needs
to attribute the failure to a specific event for the dead-letter handler.

**Measured honestly:** on a two-stage pipeline of trivial transforms over 1,000 events, the
batch path is ~7% faster (233 µs vs 249 µs, overlapping confidence intervals). That is a
smaller number than the allocation analysis suggests, because JSON manipulation inside each
stage dominates. The structural wins are the ones that matter:

* no boxed future per event per stage;
* `apply_batch` gives a stage a place to amortise per-batch setup;
* the WASM stage now takes its instance lock **once per batch** instead of once per event —
  that mutex serialises every caller for the duration of guest execution, so re-taking it
  per event multiplied contention by the batch size for no benefit.

The benchmark comparing the two paths was also made symmetric: both variants now build
their events outside the timed region. The previous one built inside it, which is exactly
the confound that makes a performance number unciteable.

**Breaking:** `impl Transform` blocks drop `#[async_trait]` and `async fn apply` becomes
`fn apply`. Async stages move to `AsyncTransform` + `add_async_transform`.

#### Schema registry: the registered schema must be the schema you write

With `auto_register = false` — the safer-looking setting, and the one a careful operator
picks in a managed Kafka environment — `ConfluentAvroEncoder` took the registry's schema
**id** and then encoded the payload with rustcdc's own hardcoded schema. If the two
differed, every message was stamped with an id that resolved to a different schema.

**Avro binary carries no field names or types.** It is positional and untagged, so the
mismatch does not fail to decode — it silently yields shifted fields and plausible-looking
wrong values, arbitrarily far downstream. That is the exact failure class this project
exists to prevent, in the configuration an operator chooses *because* it looks safer.

The encoder now verifies the registered schema matches what it will write, comparing Avro
**parsing canonical form** so formatting and JSON field-order differences are accepted while
structural ones are a hard error naming the remedy.

**Breaking:** a deployment whose registry subject carries a schema other than rustcdc's now
fails at construction instead of silently emitting undecodable messages.

#### Schema registry: errors carry the right retryability

Every registry and codec failure previously became `Error::SourceError`, which classifies as
`ErrorKind::Transient` — documented as "safe to retry with backoff". So:

* a **malformed Confluent header** was retryable, though those exact bytes can never decode;
* an Avro or JSON **deserialisation failure** was retryable, for the same reason;
* a **404 schema-not-found** was retryable and indistinguishable from a **503**.

Classification now defers to `schemreg`'s own `is_retryable()` / `is_not_found()`: transport
failures, 429 and 5xx are `Transient`; not-found, auth, and every framing or deserialisation
failure are `Terminal`.

#### Error model: causes preserved, exhausted retries are not "retryable"

* `Error::source_error(kind, msg)` now **stores** the `SourceErrorKind` instead of formatting
  it into the message, and `Error::source_kind()` reads it back. The documented promise —
  "drive retry policy without parsing free-form error strings" — was previously unachievable
  by construction. `AuthFailed`, `SchemaMismatch` and `SlotNotFound` classify as
  `ErrorKind::Terminal`; retrying them only delays the operator page.
* New `Error::Context { context, source }` with `Error::context(..)`, `root_cause()`, and a
  real `#[source]` chain — the first in the crate. `kind()` delegates to the root cause, so
  adding context can never change a retry decision.
* `TransformPipeline` no longer re-wraps every failure as `TransformError`. That laundered a
  `ConfigError` raised inside a transform from `ErrorKind::Configuration` to `Terminal`.
* *"connection retries exhausted"* and *"stream restart retries exhausted"* were
  `SourceError` → `Transient`, so an embedder following the crate's own guidance retried a
  failure whose entire meaning is that retrying is over. Both are now `Unrecoverable`.

#### `#[non_exhaustive]` placement inverted

Added to `RuntimeSourceConfig`, `AckMode`, `SinkDeliveryGuarantee` and `DatabaseAuthMode`.
Removed from `ConnectionRetryPolicy` and `IdempotencyOptions`, small value-like config
structs where the attribute broke three documented examples for no benefit.

#### Other API changes

* `MariaDbSourceConfig::with_user` / `with_database` take `impl Into<String>`; new
  `with_password`.
* `StreamHandle::next_events` implementations must treat the timeout as a bound on **batch
  assembly**, not only on waiting for the first event.

### Added

* **`TransactionBoundaryPolicy`.** Batches are cut on `max_buffer_size`, `max_event_bytes`
  and free barrier capacity, none of which know anything about transactions — so a batch
  could end mid-transaction and a sink would commit rows 1–3 of five, holding a state that
  never existed in the source. `PreserveTransactions` trims the trailing partial transaction
  and delivers it with the next batch. A transaction larger than `max_buffer_size` is still
  delivered split, with a WARN, because a permanent silent stall would be worse. Default
  stays `Split`.
* **Custom sources are first-class.** `Source::connect` and `Source::close` are trait methods
  (defaulted), and `CdcRuntime::register_source` drives the runtime from any `impl Source`.
  Previously connection setup dispatched through a closed enum of the shipped connectors, so
  a third-party `impl Source` could not be started at all — in a library whose premise is
  embeddability.
* **Apicurio Registry v3** (`apicurio` feature) and **AWS Glue Schema Registry** (`glue`
  feature) as schema-registry backends. Apicurio implements `SchemaRegistryClient`, so it
  drops into the existing encoders unchanged; Glue uses its own 18-byte framing and UUID
  schema identity, so it is a distinct path. `detect_wire_format` picks between them.
* **Confluent Protobuf codec** (`ConfluentProtobufEncoder` / `ConfluentProtobufDecoder`),
  completing the three-format Confluent story alongside Avro and JSON Schema. Confluent
  Protobuf does not use the plain 5-byte header — it carries a **message-index path**
  locating the message inside its `.proto` file, and an index that happens to be wrong
  produces a header a Confluent deserialiser misreads *without erroring*. rustcdc derives
  it from the compiled descriptor rather than hardcoding it; a test asserts the derived
  value is `[3]`, which is what `Event`'s position in `proto/event.proto` requires and not
  the obvious `[0]` guess.

  The descriptor is compiled at build time by [`protox`], a **pure-Rust** protobuf
  compiler, so building rustcdc still does not require `protoc` on the machine.

  `ProtoEvent::into_event` is new — the protobuf path previously encoded only. It rejects
  `OPERATION_UNSPECIFIED` rather than defaulting it: protobuf's zero value is
  indistinguishable from an absent field, so defaulting to `Insert` would turn a truncated
  or foreign message into a fabricated row creation.
* **Schema references** (`SchemaRegistryConfig::with_references`), for a deployment that
  registers rustcdc's schema in a subject namespace where types are shared rather than
  inlined. Without them, registration against such a subject fails to resolve.
* **`warm_schema_cache`**, to pre-resolve schema ids so a consumer restarting against a
  backlog does not turn its first message per id into a synchronous registry round-trip —
  the moment throughput matters most and the registry is most likely to rate-limit. Schema
  ids are immutable, so a warmed entry is valid for the process lifetime.
* The object-safe `SchemaEncoder` / `SchemaDecoder` / `DynSchemaRegistryClient` /
  `AnySchemaCache` traits are re-exported, for embedders that need `Arc<dyn …>`.
* **`preflight_schema_registry`.** Schema resolution is on the encode path, so a registry
  problem surfaced as a failed event mid-pipeline rather than as a startup failure. This
  checks reachability, then either that the subjects carry rustcdc's schema
  (`auto_register = false`) or that rustcdc's schema is compatible with what is registered
  (`auto_register = true`) — so an incompatible auto-registration fails with a clear message
  instead of an opaque HTTP 409 on the first event. Optional endpoints a registry does not
  implement are skipped, not treated as failures.
* **Registry retry policy**, on by default: jittered exponential back-off honouring
  `Retry-After`. Schema resolution is on the encode path, so a single 503 previously failed
  the event and took the pipeline down for something that clears itself in seconds. Only
  transient conditions retry; not-found, auth and invalid-schema fail immediately.
* **MariaDB-specific binlog events are decoded.** `mysql_common`'s `EventType` enum stops
  below MariaDB's 160–164 range, so `read_data()` returned `Ok(None)` and those events
  vanished. `GTID_EVENT` (162) is now decoded, so MariaDB checkpoints carry a real GTID
  instead of a binlog file and position — which is server-local and resumes somewhere
  unrelated after a failover. `START_ENCRYPTION_EVENT` (164) is now a hard error: every
  following event is ciphertext this connector cannot decode, so continuing would silently
  drop all changes from that point on.
* **Masking reports when it is doing nothing.** Rules match by exact dotted path, so a typo
  or a renamed column disables one silently and the field flows through in clear text. Every
  rule now carries a hit counter; `MaskHashTransform::unmatched_rules()` names rules that
  have never fired.
* New metrics: `rustcdc_runtime_idempotency_evictions_total`,
  `rustcdc_runtime_idempotency_unidentifiable_total`.
* `SourceMetadata::timestamp` now documents its **per-connector resolution**. MySQL and
  MariaDB read it from the binlog common header, which stores whole seconds — so lag derived
  from it over-reports by up to 1,000 ms (measured median ~480 ms). PostgreSQL and SQL Server
  are exact. Surfaced by the new latency harness, which reports the skew explicitly.

### Dependencies

Full refresh; 21 crates upgraded.

* `schemreg` 0.3 → **0.4** (Protobuf codec, Apicurio, Glue, retry policy, wire-format detection)
* `opentelemetry` / `_sdk` / `-otlp` 0.27 → **0.32** (runtime type parameter gone,
  `Resource` is builder-constructed, `SdkTracerProvider` replaces `TracerProvider`;
  `shutdown()` now flushes a retained provider because
  `global::shutdown_tracer_provider()` no longer exists)
* `wasmtime` 44 → **47**, `wasmparser` 0.246 → **0.255**
* `mysql_async` 0.36 → **0.37**, `mysql_common` 0.35 → **0.37** (kept aligned; a mismatched
  pair produces two incompatible `Sid`/`Value` types in one graph)
* RustCrypto: `sha2` 0.10 → **0.11**, `aes-gcm` 0.10 → **0.11**, `hkdf`/`hmac` 0.12 → **0.13**.
  Digests no longer implement `LowerHex`, so hex encoding is explicit — the stable
  fingerprint's output shape is unchanged, which matters because a change there would
  silently invalidate every persisted dedup record downstream. The AES-GCM nonce now uses
  `Generate::try_generate`, the fallible path: the infallible one panics if the OS entropy
  source fails, and a predictable or repeated nonce under the same key is a key-recovery
  weakness, not a quality problem.
* `prost` 0.13 → **0.14**, `apache-avro` 0.17 → **0.21**, `base64` 0.22 → **0.23**,
  `tokio-postgres-rustls` 0.13 → **0.14**
* Dev: `sqlx` 0.8 → **0.9**, `testcontainers` 0.25 → **0.27**, `criterion` 0.7 → **0.8**

**`rustls-pemfile` removed.** It has been unmaintained since August 2025
(RUSTSEC-2025-0134); PEM parsing moved to `rustls_pki_types::pem::PemObject`, which is the
same implementation its final release wrapped. mTLS key parsing also no longer uses the
deprecated panicking `Nonce::from_slice`.

The `testcontainers` and `sqlx` upgrades resolved **six** previously-ignored advisories
(RUSTSEC-2026-0066/0112/0113/0145, RUSTSEC-2025-0134 via testcontainers, RUSTSEC-2023-0071
RSA Marvin via sqlx-mysql). Those ignores are deleted rather than commented out: `cargo deny`
warns on an ignore that matches nothing, and leaving them would train the reader to ignore
that warning — which is how a genuinely stale exception survives.

### Documentation

* **`docs/` is now `site/` — a Zola static site**, published to GitHub Pages by
  `.github/workflows/pages.yml` and built + link-checked on every PR by the `docs-site`
  CI job. The fifteen guides moved to `site/content/docs/` with TOML front matter and
  kebab-case names, behind a landing page and a task-oriented sidebar (Start / Build /
  Extend / Operate / Verify). SEO scaffolding is per-page rather than site-wide: page-first
  `<title>`, per-page description, canonical URL, Open Graph and Twitter cards, a
  `SoftwareSourceCode` / `TechArticle` JSON-LD graph, sitemap, Atom feed and a client-side
  search index. No webfonts, no external requests, light/dark theme with a pre-paint script.
  The two index pages (`docs/README.md`, `docs/documentation.md`) were hand-maintained
  cross-reference maps that the sidebar now generates; they are deleted rather than ported.
* Cross-document links use Zola's checked `@/docs/*.md` form, so `zola check` resolves every
  one of them and the policy gate fails on a miss. That immediately caught a broken anchor
  (`#health-verdict--idle-vs-stalled`) that plain Markdown had carried silently.
* **New policy gate: config-docs coverage.** Every public field of `RuntimeConfig`,
  `RuntimeOptions` and the three connector configs must appear in the configuration
  reference. The reference used to carry hand-copied `pub struct` dumps, which had drifted:
  **eleven fields existed in code and were documented nowhere** — `table_include_list` and
  `table_exclude_list` on all three connectors, `slot_idle_advance_interval_ms`,
  `server_flavor`, `handoff_overlap_drain_budget_ms`, `capture_truncate_events`, and
  `incremental_snapshot`. The dumps are now field tables with types, defaults and the
  failure each option prevents, and the gate fails if either side moves without the other.
* Corrected two documented defaults that were simply wrong: `max_buffer_size` is 10 000
  (documented as 1 000) and `max_poll_wait_ms` is 5 000 (documented as 100).
* `TransactionBoundaryPolicy` gained a section in the configuration reference. It was a
  headline correctness option reachable only from the API guide.
* **Getting started was rewritten.** It was a contributor setup page — `cargo check`
  invocations and a feature list — while the README pointed at it for the runtime loop it
  never contained. It is now an actual walkthrough: provision the slot, configure the
  runtime, run the poll/apply/ack loop, handle partial rows, backfill, and alert on health.
* **The README was restructured.** License sat in the middle of the file, Quick Start came
  after it, and the documentation map pointed at ten paths that no longer exist. It now
  leads with what the crate is, why it exists, install, and a compiling quick start, and
  defers reference material to the site. Stale counts fixed (797 → 812 unit tests, 84 → 92
  doctests).
* **`#![deny(missing_docs)]`**, gated in CI. The backfill covered **416 items**; roughly a
  fifth were places where the behaviour needed explaining rather than the signature restated.
* Every Rust block in `README.md` and `site/content/docs/{api,config-reference,
  getting-started,adapter-sdk,schema-evolution}.md` is compiled and run by
  `cargo test --doc --all-features`, gated in CI.
  Turning it on immediately failed **36 of 96 samples** — `FilterProjectionConfig::filter`
  (the field is `filters: Vec<_>`), `rustcdc::idempotency::…` (not a module),
  `with_connection_retry` on the wrong type, an `Event` literal missing two fields,
  `MariaDbSourceConfig` built as a struct literal when it is a newtype. All fixed.
* Schema registries are documented in the API guide for the first time.
* Corrected: the claim that mask rules on container fields "are currently not applied" (they
  are), the AES-GCM key-rotation note, `MaskRule::Hash` references (no such variant), the
  `systemctl stop rustcdc # Flushes pending events` comment (it does not — flushing is a
  property of your wrapper calling `drain_and_stop`), the lease-conflict procedure (`ps -p`
  against a `HOSTNAME:PID` string errored out), and the "start fresh" procedures that deleted
  only `checkpoint_<src>.json` and left the snapshot checkpoint behind.

### Fixed

* `event_batches()` busy-spun with no yield when the source returned empty synchronously — an
  async fn that never awaits, which starves its tokio worker and can wedge a single-threaded
  runtime.
* SQL Server stream resume against the typed offset. Caught by running the Docker suite,
  which is the verification this release was explicitly gated on.
* Untagged Markdown code fences in the published docs were compiled as Rust by rustdoc.

## 0.7.0

Breaking release. The theme is closing paths where a wrong result could be produced
**silently** — no error, no log line, just data that is quietly incorrect.

### Breaking changes

#### `Event::unavailable_columns` split per image

`unavailable_columns` now describes the **`after`** image only. A new
`before_unavailable_columns` field describes `before`.

The two sets are not the same, and the previous single merged list was wrong: a TOASTed
column that *was* modified arrives present in `after` and absent from `before`. Merging
marked it unavailable, so a correct sink would skip writing a value that genuinely changed.

**Migration:** if you read `unavailable_columns` when applying the after-image, no change is
needed — the semantics are now what you already assumed. If you used it while consuming the
before-image, read `before_unavailable_columns` instead.

#### Checkpoint files carry an integrity checksum

Checkpoint files now include a `content_checksum` (SHA-256 over the other fields), verified
on every load. This closes a silent-corruption path: a flipped bit in an LSN or binlog
position does not fail to parse — it resumes capture from a *wrong* position, skipping
events with no error raised anywhere.

**Migration:** checkpoint files can no longer be written or edited by hand. Use
`FileCheckpoint::restore_from_record`, or the new `examples/seed_checkpoint.rs`:

```bash
cargo run --example seed_checkpoint --features postgres -- \
  --dir /var/rustcdc/checkpoints \
  --source-type postgres \
  --committed-event-count 0 \
  --offset '{"lsn": 281474976711680, "slot_name": "your_slot"}'
```

#### Envelope validation is stricter

`Event::validate()` now rejects:

- a column listed in an availability list that is also present in the corresponding payload
  (a contradiction, where the dangerous reading — trust the payload — is the one a sink takes)
- `before_unavailable_columns` set together with `before_is_key_only`
- either availability list set on `TRUNCATE` / `SCHEMA_CHANGE`, which carry no row payload

#### Wire schemas gained fields

`schemas/event.avsc` and `proto/event.proto` both carry `unavailable_columns` and
`before_unavailable_columns`. The Avro schema previously carried **neither**, so Avro
consumers had no way to know a payload was partial. Both fields have defaults, so existing
readers continue to decode.

`schemas/event.avsc` is now the single source of truth, embedded via `include_str!` — the
file and the encoder can no longer drift apart.

### Added

- **`Event::row_write()`** returns a `RowWrite` — the one write that is correct for an event:
  `Replace` (complete row), `Merge` (partial; carries *only* the columns the source actually
  supplied), `Delete`, `Truncate`, or `None { reason }`. Prefer it over reading `after`
  directly: the classic CDC corruption — upserting a full row from a partial payload and
  writing `NULL` over values that never changed — is not expressible through it.
  `RowWrite::is_partial()` lets sinks that cannot express a partial update branch explicitly.
- **`RuntimeAdminSnapshot::health`** is a `HealthVerdict`
  (`Healthy | Idle | Stalled { reason } | NotRunning`). `RuntimeState` alone could not
  distinguish a connector streaming from a quiet database from one hung on a dead socket —
  both reported `Running` with flat counters. `Stalled` names both the condition and the
  remedy; `is_alertable()` is true for exactly that variant. Exposed as
  `rustcdc_runtime_health{verdict="…"}` with exactly one gauge active, so an alert rule is
  unambiguous. Alongside it, `rustcdc_runtime_events_skipped_total` — any non-zero value
  means events were dropped rather than delivered.
- **`Event::has_complete_after_image()`**.
- **`RuntimeOptions::new()`**. `RuntimeOptions` is `#[non_exhaustive]`, so external callers
  previously had no constructor at all, despite the README documenting this one.
- **`examples/seed_checkpoint.rs`** for disaster recovery.

### Fixed

- PostgreSQL `UPDATE` events merged before- and after-image TOAST holes into a single list,
  causing a correct sink to skip writing columns that genuinely changed.
- PostgreSQL `DELETE` events reported before-image holes in the after-image list, on events
  where `after` is `None`.
- `docs/api.md` claimed `REPLICA IDENTITY FULL` avoids unchanged-TOAST. It does not —
  replica identity governs the old tuple only, and the after-image omits unmodified TOASTed
  values under every setting. Now verified against a real server in
  `tests/postgres_type_fidelity_integration.rs`.
- `examples/pg_to_stdout.rs` was never updated for the replication-slot guard, so the
  documented first-run command failed against an empty database. It now provisions its own
  slot by default, with `--no-create-slot` for the production posture.
