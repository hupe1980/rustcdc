# Changelog

All notable changes to this project are documented here.

The project is pre-1.0. Minor version bumps may contain breaking changes; each one lists
what breaks and what to do about it.

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
