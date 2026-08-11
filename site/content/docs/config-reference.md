+++
title = "Configuration reference"
description = "Every rustcdc runtime and connector option, with the failure each one prevents."
weight = 40
+++

**Version:** v0.1+  
**Audience:** Platform engineers and application developers embedding rustcdc

---

## Table of Contents

1. [RuntimeConfig](#runtimeconfig)
2. [Runtime Consumption Model](#runtime-consumption-model)
3. [On-demand snapshots](#on-demand-snapshots)
4. [Transaction boundaries](#transaction-boundaries)
5. [Connector Capabilities](#connector-capabilities)
6. [Table filter patterns](#table-filter-patterns)
7. [PostgreSQL Source Configuration](#postgresql-source-configuration)
8. [Column type mapping](#column-type-mapping)
9. [MySQL Source Configuration](#mysql-source-configuration)
10. [MariaDB Source Configuration](#mariadb-source-configuration)
11. [SQL Server Source Configuration](#sql-server-source-configuration)
12. [Snowflake Source Configuration](#snowflake-source-configuration)
13. [Checkpoint Configuration](#checkpoint-configuration)
14. [Observability Configuration](#observability-configuration)
15. [Production Recommendations](#production-recommendations)

---

## RuntimeConfig

Core runtime configuration for CDC operations.

### Fields

| Field | Type | Purpose |
|---|---|---|
| `source` | `RuntimeSourceConfig` | Typed source connector configuration; see [Source selection](#postgresql-source-configuration) below. |
| `snapshot_tables` | `Vec<String>` | Tables for the **classic blocking snapshot**, applied on first run when no checkpoint exists. `"schema.table"` format. Empty means stream-only. |
| `incremental_snapshot` | `Option<IncrementalSnapshotConfig>` | Tables for the **non-blocking DBLog snapshot**. Supersedes `snapshot_tables`; set one or the other, never both. |
| `checkpoint` | `C: Checkpoint` | Durable position store. `InMemoryCheckpoint` for tests, `FileCheckpoint` or your own backend otherwise. |
| `schema_history` | `H: SchemaHistory` | Schema/DDL history store, with the same in-memory-vs-durable choice. |
| `options` | `RuntimeOptions` | Operational knobs; see the table below. |

### RuntimeOptions

`RuntimeOptions` carries the operational knobs. Every default below is chosen to make a
failure visible rather than to keep the pipeline running through one.

| Field | Type | Default | Purpose |
|---|---|---|---|
| `observability` | `RuntimeObservability` | no-op | Metrics collector and event tracer. Nothing is exported until you set these. |
| `max_buffer_size` | `usize` | 10 000 | Maximum events per delivered batch. |
| `max_poll_wait_ms` | `u64` | 5 000 | How long `poll_event_batch` waits before returning an empty batch. |
| `max_event_bytes` | `Option<usize>` | `None` | Upper bound on serialized bytes per batch. `None` relies on `max_buffer_size` alone — which is a poor proxy when row sizes vary by orders of magnitude. |
| `transform_error_policy` | `TransformErrorPolicy` | `Halt` | What a failing transform does. `Halt` preserves failure visibility; `Skip` requires a `dead_letter_handler`. |
| `dead_letter_handler` | `Option<Arc<dyn Fn(Event, Error)>>` | `None` | Invoked for events discarded under `Skip`. Mandatory with that policy — a skipped event is otherwise unrecoverable. |
| `post_commit_source_confirm_policy` | `PostCommitSourceConfirmPolicy` | `FailFast` | Behaviour when source confirmation fails *after* the durable checkpoint commit. `FailFast` surfaces the divergence; `Continue` is the availability-biased opt-in. |
| `idempotency` | `Option<IdempotencyOptions>` | on, 100 000 keys | Runtime duplicate suppression. Disable with `with_idempotency_disabled()`. |
| `validate_events` | `bool` | `true` | Enforce the event envelope contract on every event. |
| `schema_history_retention` | `Option<SchemaHistoryRetention>` | `keep_last(256)` | Bounds unbounded schema-history growth. |
| `connection_retry` | `Option<ConnectionRetryPolicy>` | enabled | Jittered exponential back-off for recoverable source connection errors. `None` propagates immediately. |
| `sink_close_timeout_ms` | `Option<u64>` | `None` | Timeout applied to a registered sink's `close` during orderly shutdown. |
| `transaction_boundary` | `TransactionBoundaryPolicy` | `Split` | Whether a delivered batch may end mid-transaction. See [Transaction boundaries](#transaction-boundaries). |

### RuntimeConfig Builder Example

```rust
use rustcdc::{
  checkpoint::InMemoryCheckpoint,
  schema_history::InMemorySchemaHistory,
  PostgresSourceConfig,
  RuntimeConfig,
  RuntimeSourceConfig,
  SecretString,
};

let checkpoint = InMemoryCheckpoint::default();
let schema_history = InMemorySchemaHistory::default();
let source = PostgresSourceConfig {
  host: "localhost".into(),
  port: 5432,
  user: "postgres".into(),
  password: SecretString::from_callback("postgres-password", || {
    std::env::var("CDC_RS_POSTGRES_PASSWORD")
      .map_err(|error| rustcdc::Error::ConfigError(error.to_string()))
  }),
  database: "mydb".into(),
  replication_slot_name: "rustcdc_slot".into(),
  publication_name: "rustcdc_publication".into(),
  conn_timeout_secs: 30,
  ..PostgresSourceConfig::default()
};

let config = RuntimeConfig::new(RuntimeSourceConfig::postgres(source), checkpoint, schema_history)
    .with_snapshot_tables(vec!["public.users".to_string(), "public.orders".to_string()])
    .with_max_buffer_size(50_000)
    .with_max_poll_wait_ms(2_000)
    .with_transform_error_policy(rustcdc::TransformErrorPolicy::Halt);
```

For env-driven bootstrapping, use explicit argument parsing in your host
application and map values into typed source configs.

Prefer the associated constructors when selecting a source in embedder code:

- `RuntimeSourceConfig::postgres(...)`
- `RuntimeSourceConfig::mysql(...)`
- `RuntimeSourceConfig::mariadb(...)`
- `RuntimeSourceConfig::sqlserver(...)`
- `RuntimeSourceConfig::disabled()`

## Runtime Consumption Model

The preferred embedder surface is now batch-oriented rather than count-oriented.

`poll_event_batch()` returns an `EventBatch` containing the delivered events plus an `AckMode`. Re-polling before acknowledgement redelivers the same in-flight batch, which keeps retry behavior loss-safe.

```rust
use rustcdc::{CdcRuntime, Result};

async fn consume_once(runtime: &mut CdcRuntime) -> Result<()> {
  let batch = runtime.poll_event_batch().await?;
  if batch.is_empty() {
    return Ok(());
  }

  runtime.commit_ack(batch.ack_mode()).await?;

  Ok(())
}
```

For partial acknowledgement, split the token and commit only the accepted prefix. The remaining suffix will be re-delivered on the next poll.

```rust
use rustcdc::AckMode;
# use rustcdc::{CdcRuntime, Result};
# async fn example(runtime: &mut CdcRuntime) -> Result<()> {
let batch = runtime.poll_event_batch().await?;
if let AckMode::Required(token) = batch.ack_mode() {
  let accepted = token.accept_prefix(10)?;
  runtime.commit_ack(accepted).await?;
}
# Ok(())
# }
```

`event_batches()` exposes the same model as a stream of non-empty `EventBatch` values.

```rust
use futures_util::StreamExt;
# use rustcdc::{CdcRuntime, Result};
# async fn example(runtime: &mut CdcRuntime) -> Result<()> {
let mut batches = runtime.event_batches();
while let Some(batch) = batches.next().await {
  let batch = batch?;
  let _ = batch;
}
# Ok(())
# }
```

`poll_event_batch()` + `commit_ack(batch.ack_mode())` is now the canonical runtime acknowledgement API.

## On-demand snapshots

`CdcRuntime::request_incremental_snapshot(tables)` snapshots additional tables on a **running**
pipeline — the equivalent of Debezium's `execute-snapshot` signal, without its prerequisite: there
is no signal table, so it works against a read-only role and a read replica.

```rust
# use rustcdc::CdcRuntime;
# async fn example(runtime: &mut CdcRuntime) -> rustcdc::Result<()> {
let enqueued = runtime
    .request_incremental_snapshot(vec!["public.orders".to_string()])
    .await?;
# let _ = enqueued;
# Ok(())
# }
```

Use it to backfill a table just added to the publication, rebuild a downstream store, or re-run
history through a corrected transform. The live stream is never paused: the new tables are chunked
into it exactly like the configured ones, under the same watermark suppression.

| Table state | Effect |
|---|---|
| Not tracked | Added, read from the start |
| Already in progress | No-op, so retrying a request is safe |
| Already complete | Rewound and read again |

Every name is resolved against the catalog before anything is mutated, so one bad reference fails
the whole call rather than half-applying it. Enqueued tables reach the checkpoint with the next
commit and are picked up again after a restart, even though they are not in
`with_incremental_snapshot`'s static list.

Requires the runtime to have been built with `with_incremental_snapshot`; otherwise the call
returns `NotImplemented` rather than reporting a backfill that never happens.

### Pause, resume, stop

A backfill of a large table adds read load to the source. When that load lands during business
hours the operator wants it paused until the evening, not cancelled — and certainly not at the
cost of stopping capture.

```rust
# use rustcdc::CdcRuntime;
# async fn example(runtime: &mut CdcRuntime) -> rustcdc::Result<()> {
let was_paused = runtime.pause_incremental_snapshot().await?;   // idempotent
// …later…
runtime.resume_incremental_snapshot().await?;

// Or abandon it entirely; capture keeps running.
let abandoned_tables = runtime.stop_incremental_snapshot().await?;
# let _ = (was_paused, abandoned_tables);
# Ok(())
# }
```

**A request may carry its own row filter**, which is usually what you want: "backfill tenant 42's
orders" is a one-off, and expressing it through `table_conditions` means editing configuration
and restarting to run something that was meant to be a signal. This is the shape Debezium's
`execute-snapshot` has, with `data-collections` and `additional-conditions` together.

```rust,no_run
# use rustcdc::CdcRuntime;
use rustcdc::source::SnapshotRequest;
# async fn example(runtime: &mut CdcRuntime) -> rustcdc::Result<()> {
runtime
    .request_incremental_snapshot_filtered(
        SnapshotRequest::new(["public.orders"])
            .with_condition("public.orders", "tenant_id = 42"),
    )
    .await?;
# Ok(())
# }
```

Re-requesting a table that already finished is a **deliberate re-snapshot**: it rewinds the
cursor, adopts the new request's filter, and re-reads the table. The rows are delivered again
rather than being suppressed as duplicates — before 0.12.0 the idempotency guard dropped every one
of them, so the request reported success and delivered nothing.

A request condition **overrides** the configured `table_conditions` entry for the same table; a
table with no override keeps its configured filter. Both carry the same trust level — raw SQL,
never built from untrusted input, and not a tenancy boundary.

> **Before 0.12.0, `table_conditions` was silently ignored for on-demand requests.** The filter
> was applied when resolving startup tables and tables adopted from a checkpoint, but not on the
> request path — so a request read the whole table, and the only symptom was volume. Worse, the
> two paths disagreed: a runtime-requested table ran unfiltered and then a restart adopted it
> *with* the filter, so the emitted rows matched no single predicate. Both are fixed, and
> `IncrementalSnapshotState` now reports the filter actually in effect per table so the question
> is answerable from outside.

| Operation | Effect on chunk reads | Effect on the live stream |
|---|---|---|
| `pause_incremental_snapshot` | Stops at the next chunk boundary | None — capture continues |
| `resume_incremental_snapshot` | Continues from the chunk it stopped at | None |
| `stop_incremental_snapshot` | Abandoned; cursors discarded, and the stop survives a restart | None — capture continues |

Both pause and resume are idempotent and return the **previous** state, so a retried operator
action is safe and a caller can still tell whether it changed anything.

**A stop survives a restart, including for configured tables.** It is recorded as an explicit
flag rather than inferred from the absence of cursors — which is what made it silently
ineffective, because a configured table with no cursor looks exactly like one that has not
started, so the next deploy re-ran the whole backfill. Re-request the tables to start over; that
clears the flag.

**Pause takes effect at a chunk boundary.** A chunk already read is merged and delivered
first. Stopping mid-chunk would either throw away a read the source has already paid for, or
strand a merged chunk whose cursor can never be promoted — and the cursor is what makes the
snapshot resumable.

**The paused flag is durable.** It rides in `IncrementalSnapshotState` alongside the chunk
cursors, so it is written by the same atomic checkpoint record. Without that, a pause taken to
protect a production primary would silently lift on the next deploy.

**Stop is not durable in the same way.** The persisted state is cleared by the next checkpoint
write, so a crash in that window resumes the snapshot. Forcing a synchronous checkpoint from a
control path would let an operator action rewrite the stream position, which is a worse trade
than a rare resume of something that can simply be stopped again.

### Driving all of this from another task

`request_incremental_snapshot`, `pause_*`, `resume_*` and `stop_*` all take `&mut self`, and an
event loop holds `&mut CdcRuntime` for its whole lifetime. `CdcRuntime::control_handle()`
returns a cloneable `RuntimeControl` that closes that gap:

```rust
# use rustcdc::{CdcRuntime, CancellationToken};
# async fn example(mut runtime: CdcRuntime, shutdown: CancellationToken) -> rustcdc::Result<()> {
let control = runtime.control_handle();

tokio::spawn(async move {
    // An admin endpoint, a signal handler, a scheduler.
    let _ = control.pause_incremental_snapshot().await;
});

runtime.run_to_completion(shutdown).await?;
# Ok(())
# }
```

Commands are applied **between** polls, never raced against one — `poll_event_batch` is not
cancel-safe, so servicing them as a `select!` arm would discard events. Latency is therefore
bounded by how often you poll, and a loop that has stopped turning leaves a command waiting;
wrap the call in `tokio::time::timeout` if the caller has an SLO. Dropping the runtime
resolves outstanding commands with an error rather than hanging.

`RuntimeControl::incremental_snapshot_state()` is different: it is a plain non-blocking `fn`
reading a snapshot the runtime republishes every poll, so a progress readout can never be
starved by a busy pipeline or hang behind a stalled one. It is stale by at most one poll.

## Transaction boundaries

By default a delivered batch may end in the middle of a source transaction. Batches are cut on
`max_buffer_size`, `max_event_bytes` and free commit-barrier capacity — none of which know
anything about transactions. That is `TransactionBoundaryPolicy::Split`, and for most sinks it
is the right trade: lowest latency, strictly bounded memory, and a transaction of any size is
delivered across as many batches as needed.

It is the wrong trade when your sink must apply each source transaction atomically — a ledger,
a materialized view with cross-row invariants, anything where a half-applied transaction is a
state that never existed upstream:

```rust
use rustcdc::{RuntimeOptions, TransactionBoundaryPolicy};

let options = RuntimeOptions::new()
    .with_transaction_boundary(TransactionBoundaryPolicy::PreserveTransactions);
# let _ = options;
```

Under `PreserveTransactions` the runtime withholds the trailing partial transaction from each
batch and delivers it with the next one, so every batch ends on a transaction boundary.

**How the runtime knows a transaction ended.** Two signals count, and nothing else does:
the event declares its own position (`event_index + 1 == total_events`), or a later event
belongs to a different transaction. Absence of a signal is not proof of an ending, so a
transaction whose remaining events have not arrived yet is **withheld** rather than
delivered partially — including when the rest is simply still in flight from the source,
which for a streaming connector is the normal case rather than the exception.


**The one case it cannot honour.** A single transaction larger than `max_buffer_size` does not
fit in any batch. Trimming it would produce an empty batch forever — a silent, permanent stall,
strictly worse than the split it is trying to avoid. The runtime therefore delivers such a
transaction split and logs a `WARN` naming the transaction id and `max_buffer_size`. If the
guarantee has to hold absolutely, raise `max_buffer_size` above the largest transaction the
source produces.

Events with no transaction metadata — snapshot rows, and connectors that do not report
transaction boundaries — are treated as their own boundary and are never trimmed.

## Connector Capabilities

Runtime source selection now exposes explicit connector capabilities through `ConnectorCapabilities`.

```rust
use rustcdc::{ConnectorCapabilities, RuntimeSourceConfig};

let source = RuntimeSourceConfig::Disabled;
let caps: ConnectorCapabilities = source.capabilities();
assert!(!caps.snapshot);
assert!(!caps.handoff);
assert!(!caps.ddl_capture);
```

When running a runtime instance, the same view is available from `source_capabilities()`:

```rust
# use rustcdc::CdcRuntime;
# fn example(runtime: &CdcRuntime) {
let caps = runtime.source_capabilities();
if !caps.snapshot {
  // Guard feature wiring in embedders before attempting snapshot mode.
}
# }
```

For configured PostgreSQL/MySQL/MariaDB/SQL Server sources, the runtime advertises
`snapshot=true`, `handoff=true`, `ddl_capture=true`, `heartbeat=true`, and
`schema_introspection=true`.

The runtime now also provides an embeddable admin/introspection surface that includes
capabilities, readiness/liveness, buffer depth, and delivery counters.

```rust
# use rustcdc::{CdcRuntime, Result};
# fn example(runtime: &CdcRuntime) -> Result<()> {
let admin = runtime.admin_snapshot();
assert_eq!(admin.state, "running");

let json = runtime.admin_snapshot_json()?;
let prometheus = runtime.admin_metrics_prometheus();
# let _ = (json, prometheus);
# Ok(())
# }
```

`admin_snapshot_json()` is intended for control-plane APIs, and
`admin_metrics_prometheus()` emits Prometheus-friendly text for embedding in
lightweight health endpoints.

The runtime constructor enforces capability guards. For example, configuring `snapshot_tables` with a source that does not support snapshots is rejected at construction time.

---

## Table filter patterns

Every connector's `table_include_list` and `table_exclude_list` take the same **case-insensitive
glob patterns**, matched against `"schema.table"` — or against the bare table name when the
source reports no schema. The sink router's `table_matches` calls the same matcher on the same
key, so a pattern that selects an event also routes it.

> **That equivalence was not true before 0.13.0, and the gap was silent.** The matcher was
> shared, but only the connector filter lowered its inputs; the router compared case-sensitively.
> A server that folds identifiers — PostgreSQL to lower, Snowflake to upper, MySQL depending on
> the host filesystem — could therefore pass a table through the include list and then match no
> route, and `drop_unrouted` is on by default. Case folding now lives in the matcher, so there is
> one answer rather than two. It is **ASCII** folding, which is what every supported server's
> identifier rules are defined in terms of.
>
> The consequence worth knowing: on a case-sensitive MySQL (`lower_case_table_names = 0`), two
> tables differing only in case cannot be told apart by a pattern. That was already true of the
> include and exclude lists; it is now also true of routing.

| Pattern | Matches |
|---|---|
| `*` | everything, qualified or bare — a true catch-all |
| `*.*` | any **qualified** `schema.table`, never a bare name |
| `public.*` | every table in `public` |
| `*.audit_log` | `audit_log` in any schema |
| `public.orders` | that table in that schema, and nothing bare |
| `public.tmp_*` | every `public` table whose name starts with `tmp_` |
| `audit_?` | `audit_1`, not `audit_10` — `?` is exactly one character |

`*` and `?` do not cross the `.` boundary; use `*.*` to span it. A qualified pattern never
matches an event the source reports without a schema, because there is nothing to match the
schema half against. Blank entries are ignored rather than treated as catch-alls.

> **An unqualified pattern is schema-agnostic, and on an allowlist that is a widening.**
> `table_include_list = ["users"]` captures `public.users` **and** `tenant_private.users`.
> The behaviour is deliberate — MySQL callers name tables bare, and demanding a schema there
> would make every pattern database-specific — but an allowlist exists to bound what leaves the
> database, so `connect()` logs a WARN naming each unqualified include entry. Write
> `"public.users"` when the schema matters.

### What the lists cover

Every event a connector can emit for a table, including its **schema-change events**. That
was not true before 0.13.0: `ALTER TABLE` / `CREATE TABLE` / `DROP_TABLE` events were built
straight from connector metadata and never consulted the lists, so an operator who
allow-listed one table still received the full column list of every other table the
publication, binlog or `cdc.change_tables` carried. All three connectors now apply the
filter to that path:

| Connector | Where the schema event comes from | Now filtered |
|---|---|---|
| PostgreSQL | a changed pgoutput `RELATION` message | yes — the relation *cache* still tracks every table, because the decoder needs it to attribute rows |
| MySQL / MariaDB | a captured DDL statement in the binlog | yes — the binlog position still advances, or the statement would replay forever |
| SQL Server | a capture-instance metadata refresh | yes, at load: an excluded instance is not polled either |

Note that a schema-change event is published under a synthetic `<table>__ddl_events`
name, which is why the filter has to be applied at the source — no downstream matcher on
the real table name can see it.

**These lists used to match exact strings only.** The globs are new in 0.12.0. Before that,
`table_exclude_list = ["public.tmp_*"]` excluded nothing at all, which is indistinguishable from
a set of tables that never changed; on the include side an allowlist matching nothing is
indistinguishable from an idle database. If you carried a literal `*` in a list as a
no-op placeholder, it is now a catch-all — check your lists before upgrading.

---

## PostgreSQL Source Configuration

| Field | Type | Default | Purpose |
|---|---|---|---|
| `host` | `String` | — | Host FQDN or IP. |
| `port` | `u16` | 5432 | |
| `user` | `String` | — | Needs the `REPLICATION` role. |
| `password` | `SecretString` | — | Build with `SecretString::new`, `from_provider`, or `from_callback`. |
| `auth_mode` | `DatabaseAuthMode` | `Password` | `AwsIamToken` switches to short-lived IAM token semantics and requires TLS. |
| `database` | `String` | — | Database to replicate from. |
| `replication_slot_name` | `String` | — | e.g. `"rustcdc_slot"`. |
| `publication_name` | `String` | — | Publication used by pgoutput. |
| `create_replication_slot_if_missing` | `bool` | `false` | **Read the note below before setting this.** |
| `failover_slot` | `bool` | `false` | Create the slot with `failover = true` (PostgreSQL 17+) so it survives a promotion. Only applies when this connector creates the slot. |
| `table_include_list` | `Vec<String>` | `[]` | Allowlist of `"schema.table"` [glob patterns](#table-filter-patterns). Non-empty means *only* matching tables; takes precedence over the exclude list. Empty means all tables the publication carries. |
| `table_exclude_list` | `Vec<String>` | `[]` | Blocklist of `"schema.table"` [glob patterns](#table-filter-patterns). Ignored when the include list is non-empty. |
| `transport` | `TransportConfig` | TLS | TLS by default when the `tls` feature is on. |
| `conn_timeout_secs` | `u64` | 30 | Range 1–300. |
| `stream_poll_interval_ms` | `u64` | 50 | Range 1–60 000. |
| `max_events_per_poll` | `usize` | 1 000 | Range 1–100 000. |
| `slot_idle_advance_interval_ms` | `u64` | 30 000 | See "Idle slots retain WAL" below. `0` disables. |
| `wal_transport` | `WalTransport` | `StreamingReplication` | How the WAL stream is read; see below. |

**`create_replication_slot_if_missing` is not a convenience flag.** A slot that vanishes
mid-life — dropped by an operator, lost to a failover onto a replica that never had it, or
invalidated by `max_slot_wal_keep_size` — is a *data-loss event*: the WAL it was retaining is
gone. Recreating it silently restarts capture at the current WAL position and skips everything
in between, which looks exactly like healthy operation. Set it `true` only for first-time
provisioning or ephemeral test databases; otherwise create the slot out of band:

```sql
SELECT pg_create_logical_replication_slot('rustcdc_slot', 'pgoutput');
```

**Idle slots retain WAL.** When no committed events are delivered — an idle database, or a
burst of rolled-back transactions — the slot's `confirmed_flush_lsn` stays pinned and
PostgreSQL cannot recycle WAL segments. `slot_idle_advance_interval_ms` makes the connector
confirm the server's current WAL position after that much time without events. Disabling it on a
long-lived stream is how a disk fills up.


### `wal_transport`

| Value | Mechanism | Choose it when |
|---|---|---|
| `StreamingReplication` (default) | `START_REPLICATION ... LOGICAL` | Always, unless the environment forbids it |
| `SqlPeek` | `pg_logical_slot_peek_binary_changes` | A replication connection cannot be arranged |

`StreamingReplication` requires two things `SqlPeek` does not:

- the connecting role must have the **`REPLICATION`** attribute (`ALTER ROLE … REPLICATION`, or
  `rds_replication` on RDS);
- the connection must be **direct** — a pooler in transaction-pooling mode cannot carry a
  replication stream.

Reach for `SqlPeek` only when neither can be arranged. Both read the same slot and produce the
same events, LSNs included, so switching is an access-and-performance decision rather than a
correctness one — but `SqlPeek` measures **4–5× slower for identical capture work**, and it
re-reads WAL from the slot's `restart_lsn` on every poll rather than once per connection.
Selecting it logs a warning naming the trade-off, so nobody inherits it by accident.

Why, and what each measures at: [WAL transport](@/docs/architecture.md#wal-transport) ·
[measured performance](@/docs/reliability-testing.md#measured-performance).

Authentication on the streaming transport: **SCRAM-SHA-256** (PostgreSQL 14+ default), **MD5**
(deprecated by PostgreSQL; logs a warning), and **cleartext**, refused unless the transport is
TLS. `SCRAM-SHA-256-PLUS` (channel binding) is not implemented — a server offering only `-PLUS`
needs `SqlPeek`.

> **TLS means TLS.** `sslmode=require` is set on both connections whenever the transport is TLS.
> `tokio-postgres` defaults to `prefer`, which silently falls back to plaintext against a server
> with `ssl = off` — a config that says TLS and delivers cleartext, visible only in a packet
> capture. A server that refuses TLS now fails the connection instead.

### Large transactions spill on the server

The connector negotiates pgoutput **`proto_version '1'`**, so PostgreSQL buffers a transaction's
decoded output **server-side** until it commits. Past `logical_decoding_work_mem` (default
**64 MB**) the surplus is written under `pg_replslot/<slot>/`, and nothing is delivered until
`COMMIT` — a bulk `UPDATE` over millions of rows means disk churn on the primary, then a burst.

Bounded and observable:

```sql
SELECT slot_name, spill_txns, spill_count, spill_bytes
FROM   pg_stat_replication_slots;   -- PostgreSQL 14+
```

Mitigations, in order of preference: keep source transactions small (the only one that removes
the cost rather than moving it); raise `logical_decoding_work_mem` (it is per-walsender, so
budget it against concurrent replication connections); alert on `spill_bytes` *growth* rather
than its level.

`proto_version '2'` (PostgreSQL 14+) would let the server stream an in-progress transaction
instead of spilling it. rustcdc does not negotiate it, and the decoder **rejects** v2 and v3
messages rather than skipping them: v2 moves the buffering to the client, which must then hold
each transaction until `Stream Commit` and *discard* it on `Stream Abort`. Mishandling that abort
emits changes the source rolled back. Debezium's pgoutput decoder negotiates `proto_version 1`
too, so this is a shared frontier rather than a gap — and rejecting the tags means a plugin
mismatch is loud instead of a silent misreading of transaction boundaries.

### Secret Loading Patterns

Connector passwords are now modeled as `SecretString`, not raw `String` values.

```rust
use rustcdc::{SecretProvider, SecretString};
use std::sync::Arc;

struct VaultProvider;

impl SecretProvider for VaultProvider {
  fn resolve_secret(&self, reference: &str) -> rustcdc::Result<String> {
    Ok(format!("vault://{reference}"))
  }
}

let inline_secret = SecretString::new("postgres");
let provider_secret = SecretString::from_provider(
  "vault",
  "database/postgres/password",
  Arc::new(VaultProvider),
);
let callback_secret = SecretString::from_callback("runtime-refresh", || {
  std::env::var("CDC_RS_ROTATED_PASSWORD")
    .map_err(|error| rustcdc::Error::ConfigError(error.to_string()))
});
```

Deferred secrets are resolved at validation/connect time and remain redacted in `Debug`/`Display` output.

### Feature-Gated Encryption Transforms

Enable the `encryption` feature to use field-level AES-GCM encryption and decryption through the existing `MaskHashTransform` surface.

```rust
use rustcdc::{MaskHashConfig, MaskHashTransform, MaskRule, SecretString};
// `MaskHashConfig::mask_rules` is an `ahash::AHashMap`, re-exported by the crate's
// dependency — `std::collections::HashMap` will not coerce.
use ahash::AHashMap;

let mut encrypt_rules = AHashMap::new();
encrypt_rules.insert(
  "profile.phone".to_string(),
  MaskRule::Encrypt(SecretString::from_callback("field-key", || {
    std::env::var("CDC_RS_FIELD_KEY")
      .map_err(|error| rustcdc::Error::ConfigError(error.to_string()))
  })),
);

let encrypt_transform = MaskHashTransform::new(MaskHashConfig {
  mask_rules: encrypt_rules,
  default_rule: MaskRule::Null,
});

let mut decrypt_rules = AHashMap::new();
decrypt_rules.insert(
  "profile.phone".to_string(),
  MaskRule::Decrypt(SecretString::from_callback("field-key", || {
    std::env::var("CDC_RS_FIELD_KEY")
      .map_err(|error| rustcdc::Error::ConfigError(error.to_string()))
  })),
);

let decrypt_transform = MaskHashTransform::new(MaskHashConfig {
  mask_rules: decrypt_rules,
  default_rule: MaskRule::Null,
});
```

Encrypted fields are emitted as `enc:<nonce_b64>:<ciphertext_b64>` strings and decrypted back into their original JSON values with the matching key.

Format/KDF contract for current unversioned payloads:
- AEAD: AES-256-GCM
- Nonce: 12 random bytes (base64 encoded)
- KDF: HKDF-SHA-256, 32-byte output, no salt
- HKDF info label: `b"rustcdc-field-encryption"`

Future backward-compatibility rollout plan (when versioning becomes necessary):
- phase 1: decrypt supports both legacy unversioned and new versioned payloads
- phase 2: encrypt emits only the new versioned payload format
- phase 3: after migration window, remove legacy decrypt support with release-note callout

### Field Mapping Transform

Use `FieldMappingTransform` for high-value schema-alignment operations without
custom code:

- copy fields (`copy`)
- rename/move fields (`rename`)
- inject static literals (`set_literals`)
- remove fields (`remove`)

Paths use dot notation (`profile.email`, `meta.source`).

```rust
use rustcdc::{FieldMappingConfig, FieldMappingTransform};
use serde_json::json;

# fn example() -> rustcdc::Result<()> {
let transform = FieldMappingTransform::new(FieldMappingConfig {
  copy: vec![("user.email".into(), "email".into())],
  rename: vec![("user.name".into(), "user.full_name".into())],
  set_literals: vec![("meta.pipeline".into(), json!("orders"))],
  remove: vec!["legacy_flag".into()],
  strict: true,
})?;
# Ok(())
# }
```

`strict = true` fails fast when copy/rename/remove source paths are missing,
which helps catch drift during schema evolution and replay.

**Replay determinism caveat (important):**
- `MaskRule::Encrypt` is intentionally nonce-based and therefore non-deterministic.
- Replaying the same logical event will produce different ciphertext bytes.
- Use encryption rules only when your downstream dedup/idempotency logic does not depend on byte-identical payload replay.
- For replay-sensitive pipelines, prefer deterministic masking rules — `UnsaltedSha256`,
  `HmacSha256` (keyed, and the GDPR-appropriate choice), `Redact`, `Truncate`, `Null` — on
  fields that participate in replay comparisons. (There is no `MaskRule::Hash`.)

**Transport Selection:**
- `TransportConfig::tls()` (default with `tls` feature): TLS with system trust store
- `TransportConfig::tls_with_ca_cert_path(path)`: TLS with explicit CA bundle
- `TransportConfig::tls_insecure_skip_verify()`: TLS with certificate/hostname verification disabled (testing or tightly controlled air-gapped environments only)
- `TransportConfig::plaintext()`: unencrypted transport — credentials and data transmitted in the clear

Use TLS transport for all production connector configurations.
`TransportConfig::plaintext()` is provided as an explicit escape hatch for trusted
private networks and local integration testing only — never use it in production.

**Connection Retry Policy:**

Set `RuntimeOptions.connection_retry` to automatically retry recoverable source
connection failures with truncated exponential backoff:

```rust
use rustcdc::{ConnectionRetryPolicy, RuntimeOptions};
# use rustcdc::{checkpoint::InMemoryCheckpoint, schema_history::InMemorySchemaHistory,
#     RuntimeConfig, RuntimeSourceConfig};
# let (source, checkpoint, schema_history) = (
#     RuntimeSourceConfig::Disabled,
#     InMemoryCheckpoint::default(),
#     InMemorySchemaHistory::default(),
# );
// `with_connection_retry` lives on `RuntimeOptions`, not on `RuntimeConfig`.
let config = RuntimeConfig::new(source, checkpoint, schema_history)
    .with_options(RuntimeOptions::new().with_connection_retry(
        ConnectionRetryPolicy::new()
            .with_max_retries(Some(5))    // None retries indefinitely
            .with_initial_delay_ms(300)   // first retry after 300 ms
            .with_max_delay_ms(10_000),   // backoff capped at 10 s
    ));
# let _ = config;
```

Retry applies to recoverable errors only: an unclassified `SourceError`, a `TimeoutError`,
or a classified source error whose `SourceErrorKind` is recoverable
(`NetworkTransient`, `QuotaExceeded`, `Unknown`). `AuthFailed`, `SchemaMismatch` and
`SlotNotFound` are **not** retried — they need an operator, and retrying only delays the page.
Fatal errors (`ConfigError`, `ValidationError`, `Unrecoverable`) propagate immediately.

> **Operational warning — `max_retries: None` (indefinite retry):**
> Setting `max_retries: None` causes the runtime to retry failed source
> connections forever. This is appropriate for highly-available deployments
> where the source database is expected to recover (e.g., failover, transient
> network blips), but it **masks dead source connections indefinitely**.
> If your monitoring relies on `poll_event_batch` returning an error to
> trigger alerts or circuit-breaking logic, indefinite retry will prevent
> that signal from surfacing.
>
> **Recommendations for `max_retries: None`:**
> - Set a `replication_lag_ms` alert threshold in your observability stack;
>   rising lag indicates the source is unreachable even when the runtime
>   does not surface an error.
> - Emit a dead-man's-switch metric: if `total_events_polled` stops growing
>   for an unexpectedly long window, treat the pipeline as stalled.
> - Consider bounded retry (`max_retries: Some(N)`) with external restart
>   orchestration (e.g., Kubernetes pod restart policy) so stalled pipelines
>   surface cleanly rather than silently burning CPU in a backoff loop.

### Connector-Specific Post-Commit Confirmation Semantics

`commit_ack()` has a uniform API but connector confirmation semantics are intentionally connector-specific:

- PostgreSQL:
  - Runtime confirms durable progress via replication-slot LSN confirmation.
  - Post-commit confirmation failures are governed by `PostCommitSourceConfirmPolicy`.
- MySQL:
  - Runtime durability is checkpoint-first.
  - `confirm_lsn` is a connector compatibility hook and does not provide PostgreSQL-style slot advancement semantics.
- SQL Server:
  - Runtime durability is checkpoint-first.
  - `confirm_lsn` is a connector compatibility hook and does not provide PostgreSQL-style slot advancement semantics.

Operationally, all connectors remain at-least-once at the runtime boundary; downstream idempotency remains mandatory.

**Resumable Snapshot Cursoring:**
- Snapshot resume uses primary-key keyset cursoring (not `ctid`).
- Tables configured for resumable snapshots must expose a primary key.
- Tables without a primary key are rejected for resumable snapshots.
- This prevents physical tuple cursor instability during long-running snapshots with concurrent writes.

---

## Column type mapping

Every event payload is JSON, so each source type is rendered into a JSON value. Where the
mapping is not obvious — or where getting it wrong produces a *plausible* wrong value rather
than an error — it is pinned by the type-fidelity integration suites
(`tests/{postgres,mysql,sqlserver}_type_fidelity_integration.rs`), which assert exact decoded
values against real databases.

### The general rules

| Source shape | JSON | Note |
|---|---|---|
| Integers, of any width | string | Including values inside `i64`. One rule beats a width-dependent one: a consumer must not have to know whether a column crossed 2^53 to know how to read it. |
| Exact numerics (`DECIMAL`, `NUMERIC`, `MONEY`) | string | A JSON number round-trips through a float and loses the low digits. |
| Floating point | string | The source's own shortest round-trip rendering. |
| Booleans | string | PostgreSQL sends `"t"`/`"f"`; MySQL `TINYINT(1)` sends `"1"`/`"0"`. The source's form is preserved rather than normalised, because normalising would discard the distinction between a real `BOOLEAN` and a `TINYINT` used as one. |
| Text | string | |
| Binary (`BYTEA`, `VARBINARY`, `BLOB`) | string | Encoded, never transcoded — lossy UTF-8 transcoding would deliver a replacement character as though it were the stored value. **The encoding differs per connector**; see below. |
| JSON / JSONB | string | The source's own serialization, verbatim, as a string containing JSON — not a nested object. Parse it if you need the structure. |
| `NULL` | `null` | Present as a key with a null value — **not** an absent key. |
| Value the source could not supply | *key absent* | Listed in `unavailable_columns`; see [partial payloads](@/docs/api.md#partial-payloads-read-this-before-writing-a-sink). |

> **Every scalar is a JSON string.** That is the whole rule, and it is why the table has no
> `number` or `boolean` row. A JSON number is an IEEE-754 double by the time most consumers see
> it, so `numeric(38,4)` and `bigint` past 2^53 do not survive one — and a representation that
> depended on the value's magnitude would be undecodable without inspecting each value first.
> Read with `value.as_str()` and parse. This table described JSON numbers before 0.11.0; if you
> built against that, integers and floats now arrive quoted.

### Binary column encoding, per connector

There is no single form, because each connector's lossless path produces a different one. The
encoding is a property of the **connector**, not of the value, so a consumer picks one decoder
per source and never inspects a value to decide:

| Connector | Form | Example for the bytes `DE AD BE EF` |
|---|---|---|
| PostgreSQL | PostgreSQL's own hex output, `\x`-prefixed | `"\\xdeadbeef"` |
| MySQL / MariaDB | lowercase hex, no prefix | `"deadbeef"` |
| SQL Server | whatever `FOR JSON PATH` emits for `varbinary`, which the connector passes through unaltered | asserted by `tests/sqlserver_type_fidelity_integration.rs` rather than restated here |

**MySQL's form used to depend on the value.** A binary column whose bytes happened to be valid
UTF-8 arrived as text, and the same column's other rows arrived as hex — so no single decoder was
correct for the column. The binlog's charset metadata (collation `63` is `binary`) now decides,
and a column type alone cannot: `BLOB` and `TEXT` share one type, `VARBINARY` and `VARCHAR`
share another. Fixed in 0.12.0.

The last two rows are the distinction that matters most: a missing key means "no information",
a `null` means "the value is NULL". Collapsing them is the classic CDC corruption.

### MySQL and MariaDB temporal and enumerated types

The binlog stores these in encodings that do not survive a naive read, so the connector
consults the table-map metadata rather than the value alone.

| Column type | JSON | Why it needs the metadata |
|---|---|---|
| `DATE` | `"2026-07-20"` | The binlog value is a full timestamp tuple shared with `DATETIME`. Rendering by value alone reports a midnight time the source never carried; truncating whenever the time is zero would instead strip the time from a `DATETIME` that genuinely falls at midnight. Only the column type separates them. |
| `DATETIME`, `TIMESTAMP` | `"2026-07-20T12:34:56"` or `...T12:34:56.789012` | Fractional seconds appear only when the column declares them; a fixed width would fabricate or truncate precision. |
| `TIME` | `"d:hh:mm:ss.uuuuuu"` | MySQL `TIME` can exceed 24 hours, so it is not a clock time. |
| `ENUM` | the **label**, e.g. `"happy"` | The binlog carries the 1-based ordinal. Forwarding it delivers `1` where the row holds `'happy'` — a plausible integer that silently means something different as soon as the enum's declaration order changes. The labels come from the table-map optional metadata. |
| `SET` | comma-joined labels, e.g. `"read,write"` | The binlog carries a little-endian bitmask in raw bytes. Reading those bytes as text yields control characters that are valid UTF-8, so the wrong reading fails *silently*. |

Both `ENUM` and `SET` labels require `binlog_row_metadata=FULL`, which rustcdc already demands
for column names and key flags. Without it the values fall back to the raw ordinal and mask.

> **ENUM ordinal `0`** is MySQL's "invalid value" slot, produced by a non-strict-mode insert of
> an unlisted value. It maps to the empty string, as MySQL itself displays it — not to the
> first variant.

### SQL Server

`DECIMAL`, `NUMERIC`, `MONEY`, `DATETIME2`, `DATETIMEOFFSET`, `TIME`, `UNIQUEIDENTIFIER`,
`VARBINARY` and `XML` all decode to their exact values. This is called out explicitly because
an earlier version of the connector returned `null` for five of them — indistinguishable from a
genuine SQL NULL, delivered as an authentic value, with no error anywhere. The fidelity suite
now asserts non-null on every `NOT NULL` column precisely to catch a regression of that shape.

## MySQL Source Configuration

### Required server configuration

`MysqlConnection::connect()` validates these and **fails loud** if they are unsuitable. Each
unsuitable value would otherwise cause *silent* corruption rather than an error at decode time,
so none of them can be downgraded to a warning.

| Variable | Required | MySQL 8 default | Why |
|---|---|---|---|
| `log_bin` | `ON` | `ON` | No binlog, no CDC. |
| `binlog_format` | `ROW` | `ROW` | Statement-based logging cannot identify which rows changed. |
| `binlog_row_metadata` | **`FULL`** | ⚠️ **`MINIMAL`** | Under `MINIMAL` the binlog carries **no column names and no primary-key flags**. Events would be emitted with positional placeholder keys (`@0`, `@1`, …) instead of real column names, and `primary_key: None` — which additionally disables snapshot/stream duplicate suppression and incremental-snapshot override suppression. |
| `binlog_row_image` | **`FULL`** | `FULL` | Under `MINIMAL`/`NOBLOB` the binlog records only a subset of columns, so UPDATE after-images are emitted as if complete while silently missing columns. A consumer performing an upsert would erase them. |
| `binlog_row_value_options` | **empty** | empty | `PARTIAL_JSON` makes the server write JSON *diffs* instead of complete values. rustcdc cannot apply those diffs, and the failure recurs on every restart because it precedes the checkpoint advance — stalling the pipeline permanently. |

```sql
SET GLOBAL binlog_row_metadata     = FULL;
SET GLOBAL binlog_row_image        = FULL;
SET GLOBAL binlog_row_value_options = '';
```

Persist these in `my.cnf` so they survive a restart. Note `binlog_row_metadata` only affects binlog
events written **after** the change — existing binlog content keeps the old encoding.

> **MariaDB:** `binlog_row_metadata` and `binlog_row_value_options` do not exist. The connector
> detects their absence and skips those two checks rather than failing.

### Binlog transaction compression

`binlog_transaction_compression = ON` (MySQL 8.0.20+) is **supported and needs no
configuration change.** The server writes each transaction as a single zstd
`Transaction_payload_event`; the connector reads it transparently and emits the same events
it would for an uncompressed transaction.

The one thing worth knowing is the resume coordinate. Events unpacked from a compressed
payload carry no position of their own — they were never written to the binlog file
individually — so the coordinate for every row inside a compressed transaction is the **end
position of the payload event**, exactly as MySQL specifies for `START REPLICA … UNTIL` and
`sql_replica_skip_counter`. Several rows therefore share one `source.offset`, which is
already true of any multi-row transaction.

Evidence: `tests/mysql_binlog_compression_integration.rs` runs against MySQL 8.0 with
compression enabled and asserts both that every event carries a resumable position and that a
stream restarted from a position captured inside a compressed transaction picks up the changes
that follow it.

### GTID positioning

When `gtid_mode_enabled` is set, the connector resumes by **GTID set** rather than by
binlog file+position. This matters for failover: binlog coordinates are *server-local*, so
`binlog.000042:88371` addresses an unrelated point on a promoted replica. GTIDs are globally
meaningful, which is the reason they exist.

The checkpoint accumulates a full executed set (`uuid:1-500,uuid2:1-7`), coalescing adjacent
intervals so it stays compact over a long-running stream. Encoding of the
`COM_BINLOG_DUMP_GTID` packet is delegated to `mysql_common`, so the text form written to the
checkpoint and the bytes sent to the server cannot drift apart.

> **The checkpoint must be a set, never a single GTID.** Resuming from a bare `uuid:501`
> tells the server the replica has executed only transaction 501, and it replays 1–500.

### Binlog retention and resume safety

On resume, the connector verifies the server still retains everything the checkpointed
position has not consumed, using `GTID_SUBSET(@@GLOBAL.gtid_purged, <checkpoint position>)`.
If the check fails it stops with an `Unrecoverable` error naming the exact purged-but-unread
transactions, rather than letting the server fail with a generic *"could not find first log
file"* that says nothing about how much was lost.

Set `binlog_expire_logs_seconds` so retention comfortably exceeds your maximum expected
connector downtime. (Note `expire_logs_days` was **removed** in MySQL 8.4 — using it now
raises an error at startup.)

> The subset direction matters and is easy to invert. The correct test is "everything the
> server purged, I already consumed". The intuitive inverse — "my position is a subset of
> what the server executed" — **fails open**: it reports available in precisely the gap case.

| Field | Type | Default | Purpose |
|---|---|---|---|
| `host` | `String` | — | Host FQDN or IP. |
| `port` | `u16` | 3306 | |
| `user` | `String` | — | Needs `REPLICATION CLIENT` and `SELECT`. |
| `password` | `SecretString` | — | |
| `auth_mode` | `DatabaseAuthMode` | `Password` | `AwsIamToken` requires TLS. |
| `database` | `String` | — | Database to replicate from. |
| `server_id` | `u32` | `0` — **invalid on purpose** | Replication server id for the binlog client. MySQL treats `0` as unassigned and `validate()` rejects it, so you must set a unique id per connector instance. |
| `server_flavor` | `ServerFlavor` | `Mysql` | Set `MariaDb` when connecting to MariaDB: `source_type()` then returns `"mariadb"` and checkpoints use a separate `checkpoint_mariadb.json`. |
| `gtid_mode_enabled` | `bool` | `false` | Whether GTID mode is enabled on the server. |
| `binlog_format_check` | `bool` | `true` | Validate `binlog_format = ROW` before streaming. |
| `table_include_list` | `Vec<String>` | `[]` | Allowlist of `"schema.table"` [glob patterns](#table-filter-patterns); takes precedence over the exclude list. |
| `table_exclude_list` | `Vec<String>` | `[]` | Blocklist of `"schema.table"` [glob patterns](#table-filter-patterns). |
| `transport` | `TransportConfig` | TLS | |
| `conn_timeout_secs` | `u64` | 30 | Range 1–300. |
| `stream_poll_interval_ms` | `u64` | 50 | Range 1–60 000. |
| `max_events_per_poll` | `usize` | 1 000 | Range 1–100 000. |
| `handoff_overlap_drain_budget_ms` | `u64` | `stream_poll_interval_ms * 8` | Wall-clock budget for draining overlap events during snapshot-to-stream handoff. `0` disables the budget (unlimited drain). |

**Why `server_id` has no auto-generated default.** Auto-generation was removed because
PID-hash collisions caused silent event loss in multi-instance deployments: two readers
sharing a `server_id` cause the server to evict one, and the eviction surfaces only as a
generic disconnect. A deliberately invalid default forces the decision to be made once,
explicitly.

**Why the handoff drain has a time budget.** The previous implementation capped overlap
draining at a hard-coded eight polls. On a high-traffic table with large batches that cap was
exhausted before the overlap was drained, and the connector silently delivered duplicate rows.
The budget is wall-clock instead, and exhausting it emits a `WARN` naming the residual count
rather than passing the duplicates off as normal.


### MySQL GTID String Format

```text
GTID Set Format: "source_id:interval[, ...]"
Example: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
```

---

## MariaDB Source Configuration

MariaDB uses the same MySQL-protocol transport stack, but rustcdc exposes it as a first-class source identity through [`MariaDbSourceConfig`] and `RuntimeSourceConfig::mariadb(...)`.

Use MariaDB when you need distinct checkpoint naming, source labeling, or runtime routing while keeping the same underlying binlog transport semantics as MySQL.

```rust
use rustcdc::{MariaDbSourceConfig, RuntimeSourceConfig};

// `MariaDbSourceConfig` is a newtype over `MysqlSourceConfig` that forces
// `server_flavor = MariaDb`, so it is built with the `with_*` builders rather than
// struct-literal syntax.
let source = MariaDbSourceConfig::default()
    .with_host("localhost")
    .with_port(3306)
    .with_user("cdc_user")
    .with_password("cdc_password") // prefer SecretString::from_callback in production
    .with_database("events");

let runtime_source = RuntimeSourceConfig::mariadb(source);
# let _ = runtime_source;
```

MariaDB supports the same startup, snapshot, and streaming modes as MySQL, but emits `source_type() == "mariadb"` and uses MariaDB-specific checkpoint identifiers.

> **GTID Format Warning:** MariaDB uses a distinct GTID format — `domain_id-server_id-sequence_no`
> (e.g. `0-1-12345`) — that is **incompatible** with MySQL's `uuid:interval` format
> (e.g. `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5`). Never mix checkpoint files between
> MySQL and MariaDB instances, even if the schemas are identical. Doing so will produce
> invalid GTID resume positions and cause the connector to silently restart replication
> from the beginning or raise a fatal position error. Always use
> `RuntimeSourceConfig::mariadb(...)` (not `RuntimeSourceConfig::mysql(...)`) when
> connecting to a MariaDB server to ensure correct checkpoint namespace isolation.

---

## SQL Server Source Configuration

| Field | Type | Default | Purpose |
|---|---|---|---|
| `host` | `String` | — | Host FQDN or IP. |
| `port` | `u16` | 1433 | |
| `user` | `String` | — | Needs the `CDC_ADMIN` role. |
| `password` | `SecretString` | — | |
| `database` | `String` | — | CDC must be enabled on this database. |
| `instance_name` | `Option<String>` | `None` | Named instance; `None` uses the default instance. |
| `cdc_enabled` | `bool` | `true` | Require CDC to be enabled on the database, and fail connect if it is not. |
| `cdc_schema` | `String` | `"cdc"` | Schema holding the CDC capture tables. |
| `capture_truncate_events` | `bool` | `false` | Capture `TRUNCATE TABLE` via a DDL trigger; see below. |
| `table_include_list` | `Vec<String>` | `[]` | Allowlist of `"schema.table"` [glob patterns](#table-filter-patterns); takes precedence over the exclude list. |
| `table_exclude_list` | `Vec<String>` | `[]` | Blocklist of `"schema.table"` [glob patterns](#table-filter-patterns). |
| `transport` | `TransportConfig` | TLS | |
| `conn_timeout_secs` | `u64` | 30 | Range 1–300. |
| `prereq_pool_size` | `usize` | 4 | Concurrent connections used by prerequisite checks. Range 1–64. |
| `stream_poll_interval_ms` | `u64` | 5 000 | Range 1–60 000. **See the latency note below.** |
| `max_events_per_poll` | `usize` | 10 000 | Range 1–100 000. Per **capture instance**, not per poll; see below. |

> **⚠️ SQL Server CDC is polling-based, not event-driven.** p99 latency is approximately
> `stream_poll_interval_ms` plus the CDC capture agent's own delay. Reduce the interval to
> 500–1 000 ms for latency-sensitive workloads, and do not compare SQL Server latency numbers
> against the log-based connectors as though they measured the same thing.

**Capturing TRUNCATE.** SQL Server's `cdc.fn_cdc_get_all_changes_*` cannot see `TRUNCATE
TABLE`, because TRUNCATE bypasses row-level logging. With `capture_truncate_events = true`,
rustcdc creates a shadow table (`[<cdc_schema>].[rustcdc_truncate_events]`) and a
database-level DDL trigger (`rustcdc_truncate_capture`) on first connect. The trigger records
the affected schema and table along with the current CDC maximum LSN from
`sys.fn_cdc_get_max_lsn()`; rustcdc polls that shadow table alongside the change tables and
emits `Operation::Truncate` positioned after all DML at or before that LSN.

The connected user needs `db_owner`, `db_ddladmin` or `sysadmin` to create those objects —
already required for CDC administration. They are created idempotently and survive restarts.
Ordering is **best-effort**: the truncate lands after every DML change whose commit LSN is at
or before the LSN captured when the trigger fired, which is as precise as SQL Server allows
for an operation that bypasses row-level logging.

### How the LSN window is read across capture instances

The connector reads one LSN window at a time and queries **every** capture instance in that
window with its own `TOP (max_events_per_poll)`. Two consequences are worth knowing when you
tune it, because both are about not losing rows rather than about throughput:

* **Instances truncate at different positions.** When any instance returns a full page, the
  window still holds rows the connector has not read. The only globally safe stopping point is
  the *minimum* last-row position across the truncated instances; rows beyond it are dropped
  from the batch and re-read on the next poll. The window is not advanced until the whole
  window has actually been read, so a crash mid-window costs at most one window of duplicate
  delivery — never a gap.
* **A window can therefore yield more events than one poll returns.** With *n* capture
  instances a single fill can buffer up to *n × max_events_per_poll* events, delivered a page
  at a time. Setting `max_events_per_poll` very low relative to your write rate makes this the
  normal case rather than the exception, which costs extra round trips.

Evidence: `tests/sqlserver_window_truncation_integration.rs` drives two capture instances with
`max_events_per_poll = 5` and asserts every row arrives.

### Adding a table to a running pipeline

`sys.sp_cdc_enable_table` may be run at any time. Capture instances do not all begin at the
same LSN, so a newly enabled one has a **capture floor** later than the stream's current
position; the connector reads each instance from `max(window_start, its own floor)` and skips
an instance entirely for windows that end before its floor. The next metadata refresh emits a
`CREATE_TABLE` schema-change event for it.

This is deliberately *not* the same as retention loss. Asking a capture instance for changes
below its floor and asking for changes the cleanup job has already purged both raise SQL Server
error 313 (*"An insufficient number of arguments were supplied for the procedure or
function"* — a genuine SQL Server oddity, not a driver defect). The connector distinguishes
them: the floor case is normal and handled silently, while a floor that has advanced past a
window this connector had not yet read means changes were purged and is reported as an
`Unrecoverable` error naming the affected capture instance. See
[Troubleshooting](@/docs/troubleshooting.md).

### AWS IAM database authentication for RDS and Aurora

rustcdc does **not** depend on the AWS SDK. IAM auth works by supplying the token as a
deferred secret: `SecretString::from_callback` (or a `SecretProvider`) mints one with
`rds:generate-db-auth-token` — or `aws_config` + `aws_sdk_rds::auth` — and the connector
resolves it when it needs a password.

```rust,ignore
// The callback runs per connection, so each one gets a token minted moments earlier.
let password = SecretString::from_callback("rds-iam", || {
    Ok(generate_rds_auth_token(&host, port, &user)?)   // your AWS SDK call
});
```

Set `auth_mode = AwsIamToken` as well. It is not decoration: it makes TLS mandatory —
`connect()` refuses a plaintext transport — which RDS requires for IAM auth anyway, and
which matters because the token is a bearer credential that a passive observer could
otherwise replay for its remaining lifetime.

> **The connectors reach the same guarantee by different means, and one of them costs you
> the connection pool.**
>
> **PostgreSQL** builds its connection configuration — and so resolves the secret — for
> *every* connection, including each replication reconnect. Nothing to configure.
>
> **MySQL / MariaDB** cannot do that through a pool. `mysql_async::Opts` is immutable and the
> driver exposes no per-connection credential hook, so a pool authenticates every connection
> it ever opens with the password resolved when it was built. A 15-minute token would work
> until the pool next opened a connection — after a server-side `wait_timeout`, a transient
> error, or a demand spike — and then fail in a way that reads like an intermittent
> credentials problem.
>
> So `auth_mode = AwsIamToken` **disables pooling** on MySQL and opens a freshly
> authenticated connection per request, re-resolving the token each time. `connect()` logs
> an INFO line saying so. The cost is a handshake per connection, which is affordable at a
> CDC connector's request rate: one long-lived binlog connection, a handful of metadata
> queries at startup, one query per snapshot chunk (10 000 rows by default), and one
> heartbeat per interval. It applies only when you opt in — a static password, including one
> fetched from a secret manager, keeps the pool.
>
> **SQL Server** has no IAM mode. RDS for SQL Server does not offer IAM database
> authentication, and Azure SQL's Entra ID tokens go in the password field as an ordinary
> deferred secret, resolved per connection by that connector.

The token is checked only when a connection is **established** — an open connection is not
dropped when its token expires, so a stable pipeline can run for days on a token minted once.
That is why the failure surfaces at reconnect rather than on a timer.

### SQL Server Connection String Format

```text
sqlserver://user:password@host:port;database=dbname;TrustServerCertificate=no;Encrypt=yes
```

---

## Snowflake Source Configuration

`SnowflakeSourceConfig` — feature `snowflake`. Reads through the **`CHANGES` clause**, not
through Snowflake Streams; [the Snowflake page](@/docs/snowflake.md) has the reasoning, which
comes down to Streams requiring a write to the source account to advance and turning a crash
into silent data loss.

Two things are unlike every other connector here.

**You supply the transport.** Snowflake speaks no wire protocol this crate could implement,
so `SnowflakeSource::new` takes an `Arc<dyn SnowflakeQueryExecutor>` — your HTTPS + key-pair
JWT client, or an existing driver. The feature adds **no dependencies**.

**There is no `RuntimeSourceConfig::Snowflake`.** That enum holds fully serializable
configuration, and a transport object is not. Register the source instead, which is the same
path a third-party connector takes and gets the same runtime guarantees:

```rust,ignore
// Needs a live Snowflake account and your own executor, so it cannot run as a doctest.
let mut runtime = CdcRuntime::new(config)?;   // RuntimeSourceConfig::Disabled
runtime.register_source(Box::new(SnowflakeSource::new(snowflake_config, executor)?));
```

| Field | Type | Default | Purpose |
|---|---|---|---|
| `database` | `String` | — | Database holding the tracked tables. |
| `schema` | `String` | — | Schema holding the tracked tables. |
| `tables` | `Vec<String>` | `[]` | Tables to capture, named within `schema`. Each needs `CHANGE_TRACKING = TRUE`. |
| `primary_keys` | `HashMap<String, Vec<String>>` | `{}` | Key columns per table. Required for a snapshot; without one, events carry no key. |
| `append_only` | `bool` | `false` | Use `INFORMATION => APPEND_ONLY` — cheaper, but deletes and updates are silently absent. |
| `poll_interval_ms` | `u64` | 30 000 | Window length. A **cost** dial as much as a latency one; see below. |
| `max_events_per_poll` | `usize` | 10 000 | Soft cap: warns rather than truncating, because a truncated window would skip tables. |
| `snapshot_chunk_size` | `usize` | 10 000 | Rows per keyset-paginated snapshot chunk. |
| `table_include_list` | `Vec<String>` | `[]` | Allowlist of `"schema.table"` [glob patterns](#table-filter-patterns). An excluded table is never queried at all. |
| `table_exclude_list` | `Vec<String>` | `[]` | Blocklist of `"schema.table"` [glob patterns](#table-filter-patterns). |
| `source_name` | `String` | `"snowflake"` | `Event::source.source_name` for this connector's events. |

### Identifiers are used exactly as written

Snowflake folds an unquoted identifier to **upper** case, and this connector quotes whatever
you configure. A table created as `orders` is `ORDERS` on the server, so `tables = ["ORDERS"]`
finds it and `tables = ["orders"]` does not. The same applies to `primary_keys` columns, which
are matched against result-set column names — also folded.

### Prerequisites

```sql
ALTER TABLE analytics.public.orders SET CHANGE_TRACKING = TRUE;
-- Retention must exceed the longest outage the pipeline has to survive.
ALTER TABLE analytics.public.orders SET DATA_RETENTION_TIME_IN_DAYS = 7;
```

Change tracking only records changes made **after** it is enabled, and a window whose start
has fallen outside retention makes the query fail. rustcdc reports that as data loss with the
remedy named, rather than restarting from the current time and hiding it.

### The poll interval is a cost and a fidelity dial

Every poll runs queries on a warehouse that bills by the second it is awake — unlike the
log-based connectors, where an idle stream costs nothing. The interval also decides how much
detail survives: `CHANGES` reports the **net effect** of the window, so a row updated three
times inside one window yields one event, and a row inserted and then deleted inside it yields
none at all. Shorter windows report more; they also cost more.

### What the event stream cannot carry

| | |
|---|---|
| `Event::transaction` | always `None` — `CHANGES` has no transaction id and no commit grouping |
| Intermediate row versions | collapsed within a window; see above |
| Source order within a window | none exists; events are sorted by `METADATA$ROW_ID` so a re-read is byte-identical |
| `Operation::Truncate` | not reported |
| Schema-change events | not reported — this connector does no DDL capture |

### Snapshot and handoff

The initial load reads `SELECT … AT(TIMESTAMP => T)` in keyset-paginated chunks and the stream
opens its first window at the same `T`. Because Snowflake's time travel serves every chunk from
one table version, the two phases meet exactly: there is **no overlap window and no watermark
bracket**, which every other connector in this crate needs. The trade is that `T` must stay
inside retention for the whole snapshot.

---

## Checkpoint Configuration

### InMemoryCheckpoint

**Use Case:** Development, testing, single-machine deployments (volatile)

```rust
use rustcdc::checkpoint::InMemoryCheckpoint;

let checkpoint = InMemoryCheckpoint::default();
// Keeps checkpoint in memory; lost on process restart
```

### FileCheckpoint

**Use Case:** Local machine deployments; single-machine production (persistent but not HA)

```rust
use rustcdc::checkpoint::FileCheckpoint;

// Default: 0o600 (owner read/write only — enforced at load time).
let checkpoint = FileCheckpoint::new("/var/rustcdc/checkpoints");
// Stores checkpoint in JSON file; atomically updated via write-rename.
```

File permissions are enforced at load time: if the checkpoint file on disk has
mode bits accessible to group or other (e.g. 0o644), the load is rejected with
a `CheckpointError`. This protects connection credentials embedded in the
checkpoint from unauthorized access. Do not set a mode wider than 0o600.

**File Location Format:**
```text
/var/rustcdc/checkpoints/checkpoint_postgres.json
/var/rustcdc/checkpoints/checkpoint_mysql.json
/var/rustcdc/checkpoints/checkpoint_sqlserver.json
```

**File Content Example:**
```json
{
  "checkpoint_format_version": 1,
  "source_type": "postgres",
  "committed_event_count": 12345,
  "offset": {
    "lsn": 281474976711680,
    "slot_name": "rustcdc_postgres_abc123"
  },
  "content_checksum": "9f2b...(SHA-256 over the four fields above)"
}
```

**Checkpoint Format Version Policy:**
- `checkpoint_format_version = 1` is the current write format.
- `checkpoint_format_version` is required for all file checkpoints.
- Unknown or missing versions are rejected at load time.
- rustcdc intentionally enforces fail-closed checkpoint decoding for format safety.

**Integrity:**

`content_checksum` is a SHA-256 over the other fields. It is verified on every load, and a
mismatch is a hard error rather than a resume.

This matters because checkpoint corruption is otherwise **silent**. A flipped bit in an LSN
or binlog position does not fail to parse — it resumes capture from a *wrong* position,
skipping events with no error raised anywhere.

The practical consequence: **checkpoint files cannot be edited or generated by hand.** For
disaster recovery use the bundled seeding tool, which computes the checksum, writes
atomically, applies the required file mode and fsyncs the parent directory:

```bash
cargo run --example seed_checkpoint --features postgres -- \
  --dir /var/rustcdc/checkpoints \
  --source-type postgres \
  --committed-event-count 0 \
  --offset '{"lsn": 281474976711680, "slot_name": "rustcdc_postgres_new"}'
```

Programmatically, the same operation is `FileCheckpoint::restore_from_record`.

### Custom Durable Checkpoint Backend

**Use Case:** High-availability or centralized checkpoint management

rustcdc currently ships with `FileCheckpoint` and `InMemoryCheckpoint`.
For HA or centralized state, implement the `Checkpoint` trait against your
own storage backend (for example PostgreSQL, Redis, object storage, or a
platform metadata service).

---

## Observability Configuration

### NoOp Observability (Default)

```rust,ignore
use rustcdc::{RuntimeConfig, RuntimeObservability};

// Metrics and tracing are disabled by default via explicit runtime observability options.
let config = RuntimeConfig::new(...)
  .with_observability(RuntimeObservability::default());
```

### OpenTelemetry Observability

```rust,ignore
// Requires --features metrics. `RuntimeConfig::new(...)` stands in for your own
// source/checkpoint/schema-history arguments.
use rustcdc::{OTelConfig, OTelEventTracer, OTelMetricsCollector, RuntimeConfig, RuntimeObservability};
use std::sync::Arc;

let otel_config = OTelConfig::new(
    "http://otel-collector:4317",  // OTLP gRPC endpoint
    "rustcdc",                        // Service name
    env!("CARGO_PKG_VERSION"),        // Service version
    "production",                    // Environment
);

let metrics = Arc::new(OTelMetricsCollector::with_otlp_exporter(otel_config.clone())?);
let tracer = Arc::new(OTelEventTracer::with_otlp_exporter(otel_config)?);

let config = RuntimeConfig::new(...)
  .with_observability(
    RuntimeObservability::default()
      .with_metrics(metrics)
      .with_tracer(tracer)
  );
```

### Runtime Admin Metrics (`CdcRuntime::admin_metrics_prometheus()`)

| Metric | Type | Description |
|--------|------|-------------|
| `rustcdc_runtime_readiness` | Gauge | Runtime readiness (1 ready, 0 not ready) |
| `rustcdc_runtime_liveness` | Gauge | Runtime liveness (1 alive, 0 stopped) |
| `rustcdc_runtime_buffer_depth` | Gauge | Buffered events waiting for delivery |
| `rustcdc_runtime_in_flight_events` | Gauge | Delivered but uncommitted events |
| `rustcdc_runtime_events_polled_total` | Counter | Total events delivered by runtime batches |
| `rustcdc_runtime_events_committed_total` | Counter | Total acknowledged and checkpointed events |
| `rustcdc_runtime_events_deduplicated_total` | Counter | Total events suppressed by idempotency guard |
| `rustcdc_runtime_events_skipped_total` | Counter | Events permanently dropped by `TransformErrorPolicy::Skip`. **Any non-zero value means data was lost** — the checkpoint advances past skipped events, so they are never replayed. Alert on any increase. |
| `rustcdc_runtime_idempotency_evictions_total` | Counter | Fingerprints evicted because the idempotency window filled. Growing steadily means the window is too small for this deployment's replay distance; raise `IdempotencyOptions::capacity`. |
| `rustcdc_runtime_idempotency_unidentifiable_total` | Counter | Events passed through undeduplicated because they carry neither transaction metadata nor a resolvable primary key. Expected for keyless tables. |
| `rustcdc_transform_rules_unmatched` | Gauge | Configured transform rules that have never matched an event, one series per rule (`transform`, `kind`, `rule` labels). **Emitted only when a rule is unmatched**, so its absence is the healthy state and `rustcdc_transform_rules_unmatched > 0` is a complete alert rule. A masking rule that never fires means a column is shipping in clear text; a routing rule that never fires means events are going to the default destination. Only meaningful after real traffic — every rule is unmatched before the first event. |
| `rustcdc_runtime_health` | Gauge | Derived health verdict, one series per `verdict` label. **`rustcdc_runtime_health{verdict="stalled"} == 1` is the alert rule** — `state` alone cannot distinguish healthy-idle from stalled. |
| `rustcdc_runtime_checkpoint_age_ms` | Gauge | Age of last durable checkpoint |
| `rustcdc_runtime_replication_lag_ms` | Gauge | Estimated source lag in milliseconds |
| `rustcdc_replication_slot_lag_bytes` | Gauge | PostgreSQL replication slot WAL lag (`pg_current_wal_lsn - confirmed_flush_lsn`). **The single most operationally critical PostgreSQL signal**: a monotonically growing value means the slot is pinning WAL on the primary until the disk fills. Page on sustained growth. Sampled on a timer that does **not** depend on the pipeline being caught up — it used to refresh only from the idle-advance path, so it went stale exactly while the pipeline was behind, and never sampled at all when `slot_idle_advance_interval_ms = 0`. The cadence follows `slot_idle_advance_interval_ms`, or 15 s when idle advance is disabled. |
| `rustcdc_runtime_source_capability` | Gauge | Connector capability flags, one series per `capability` label |

### OpenTelemetry Exported Metrics (`OTelMetricsCollector`)

| Metric | Type | Description |
|--------|------|-------------|
| `rustcdc.events.processed` | Counter | Total events successfully processed |
| `rustcdc.events.filtered` | Counter | Events dropped by transform pipeline |
| `rustcdc.errors` | Counter | Total errors encountered |
| `rustcdc.checkpoint.committed_count` | Counter | Total events committed to checkpoint |
| `rustcdc.replication_lag_ms` | Gauge | Estimated replication lag in milliseconds |
| `rustcdc.replication_lag_events` | Gauge | Estimated events not yet consumed |
| `rustcdc.checkpoint_offset` | Gauge | Current checkpoint offset (source-specific encoding) |
| `rustcdc.buffer_size` | Gauge | Current buffered event count |
| `rustcdc.snapshot_progress` | Gauge | Current snapshot completion percentage |
| `rustcdc.event_processing_duration` | Histogram | Event processing latency (ms) |
| `rustcdc.checkpoint_commit_duration` | Histogram | Checkpoint commit latency (ms) |

### Structured Log Fields

All logs include:
- `source_type`: Connector type (postgres, mysql, sqlserver)
- `timestamp`: ISO 8601 timestamp
- `level`: ERROR, WARN, INFO, DEBUG, TRACE
- `message`: Human-readable description
- Context fields (when applicable):
  - `table`: Table name
  - `event_count`: Number of events
  - `offset`: Source-specific position
  - `error`: Error details (sanitized)

**Enable Logging:**

```bash
# Set environment variable
export RUST_LOG=rustcdc=info,rustcdc::source=debug

# Run with structured JSON output
export RUST_LOG_FORMAT=json
```

---

## Production Recommendations

### Checkpoint Store Selection

| Scenario | Recommendation | Rationale |
|----------|---|----------|
| Single machine, restarts acceptable | FileCheckpoint | Simple, no external dependencies |
| HA cluster, centralized state | Custom `Checkpoint` backend | Integrates with your existing HA metadata store |
| Development/testing | InMemoryCheckpoint | Fast iteration; ephemeral OK |

### Buffer Size Tuning

```text
Throughput-Focused (High Latency Acceptable):
  max_buffer_size = 100_000
  max_poll_wait_ms = 5_000
  → Batches large groups; fewer commits

Latency-Focused (Lower Throughput):
  max_buffer_size = 10_000
  max_poll_wait_ms = 1_000
  → Frequent commits; sub-second latency

Balanced (Recommended):
  max_buffer_size = 50_000
  max_poll_wait_ms = 2_000
  → ~50-100ms latency; 1K-2K commits/sec
```

### Connector Scaling Envelopes

Use these as baseline production profiles, then tune with real workload evidence.

**SQL Server connector tuning (`SqlServerSourceConfig`):**

| Profile | `prereq_pool_size` | `stream_poll_interval_ms` | `max_events_per_poll` | Suggested Use |
|---|---:|---:|---:|---|
| Low-latency | 4 | 250 | 5000 | Near-real-time dashboards, lower throughput |
| Balanced (default-ish) | 4-8 | 1000 | 10000-20000 | General production workloads |
| Throughput-heavy | 8-16 | 2000-5000 | 20000-50000 | Backfills, bursty write workloads |

**PostgreSQL connector tuning (`PostgresSourceConfig`):**

| Profile | `stream_poll_interval_ms` | `max_events_per_poll` | Suggested Use |
|---|---:|---:|---|
| Low-latency | 10-25 | 250-500 | Interactive workloads where update freshness is prioritized |
| Balanced (default-ish) | 50-250 | 1000-5000 | General production workloads |
| Throughput-heavy | 250-1000 | 5000-20000 | Backfills, high sustained ingest |

**MySQL connector tuning (`MysqlSourceConfig`):**

| Profile | `stream_poll_interval_ms` | `max_events_per_poll` | Suggested Use |
|---|---:|---:|---|
| Low-latency | 10-25 | 250-500 | Interactive workloads where update freshness is prioritized |
| Balanced (default-ish) | 50-250 | 1000-5000 | General production workloads |
| Throughput-heavy | 250-1000 | 5000-20000 | Backfills, high sustained ingest |

For sustained saturation, combine connector tuning with runtime delivery controls (`RuntimeOptions.max_buffer_size`, `RuntimeOptions.max_poll_wait_ms`) and horizontal partitioning.

### TLS Best Practices

```rust
use rustcdc::TransportConfig;

// Recommended: explicit CA bundle in production.
let transport =
    TransportConfig::tls_with_ca_cert_path(Some("/etc/ssl/certs/company-ca.pem".to_string()));

// Also valid: rely on system trust store.
let transport = TransportConfig::tls();

// Testing/air-gapped fallback only: disable certificate + hostname verification.
let transport = TransportConfig::tls_insecure_skip_verify();

// Plaintext: only for trusted private networks or local integration testing.
// Credentials and event data are transmitted unencrypted.
let transport = TransportConfig::plaintext();
```

Connector config helpers now provide explicit transport selection APIs:

```rust,ignore
// Requires the mysql, postgres and sqlserver features together.
let mysql_cfg = MysqlSourceConfig::default().with_plaintext_transport();
let pg_cfg = PostgresSourceConfig::default().with_plaintext_transport();
let mssql_cfg = SqlServerSourceConfig::default().with_plaintext_transport();

let mysql_tls = mysql_cfg.with_tls_transport();
```

### Monitoring Checklist

- [ ] Alert on `rustcdc_runtime_replication_lag_ms > 30000` (30s)
- [ ] Alert on `rustcdc_runtime_liveness == 0`
- [ ] Alert on `rustcdc_runtime_checkpoint_age_ms > 10000`
- [ ] Alert on `rustcdc_runtime_events_polled_total` trend deviation > 20%
- [ ] Dashboard: Replication lag trend over 24h
- [ ] Dashboard: Event processing rate (events/sec)
- [ ] Dashboard: Checkpoint commit latency distribution

---

**Last Updated:** May 25, 2026  
**Version:** Configuration Reference v0.1+
