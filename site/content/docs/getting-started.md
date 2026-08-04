+++
title = "Getting started"
description = "Build your first rustcdc pipeline: provision a replication slot, stream changes from PostgreSQL, and acknowledge them durably."
weight = 10
+++

By the end of this page you will have a Rust binary that streams row changes out of a
PostgreSQL database, applies them to a sink, and records durable progress so a restart
resumes where it left off rather than replaying from the beginning.

The example uses PostgreSQL because it needs the least server-side setup. MySQL, MariaDB and
SQL Server differ only in the source config and the server prerequisites — see the
[configuration reference](@/docs/config-reference.md).

## Prerequisites

- Rust 1.92 or newer
- A PostgreSQL 10+ server you can configure, with `wal_level = logical`
- Docker, if you want to run the connector-backed test suites

A throwaway server for this walkthrough:

```bash
docker run --rm -d --name rustcdc-pg -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  postgres:16 -c wal_level=logical
```

## 1. Add the dependency

```toml
[dependencies]
rustcdc = { version = "0.8", features = ["postgres"] }
tokio = { version = "1", features = ["full"] }
```

## 2. Prepare the source database

CDC on PostgreSQL needs two server-side objects: a **publication** naming the tables to
capture, and a **replication slot** holding the WAL position. Create both before the first
run:

```sql
CREATE TABLE users (id bigserial PRIMARY KEY, email text NOT NULL, name text);

CREATE PUBLICATION rustcdc_publication FOR TABLE users;
SELECT pg_create_logical_replication_slot('rustcdc_slot', 'pgoutput');
```

rustcdc will **not** create the slot for you unless you ask it to. A slot that vanished
mid-life is a data-loss event, and silently recreating it restarts capture at the *current*
WAL position — everything written in between is gone with no error raised anywhere. Provision
it out of band, or set `create_replication_slot_if_missing = true` for first-time setup only.

> **A slot holds WAL until it is consumed.** If your pipeline stops for long enough, the
> server accumulates WAL and eventually runs out of disk. Monitor slot lag from day one — see
> [the runbook](@/docs/runbook.md).

## 3. Configure the runtime

`RuntimeConfig` binds four things: which source to read, where durable progress is recorded,
where schema history lives, and the runtime options.

```rust
use rustcdc::{
    checkpoint::FileCheckpoint, schema_history::FileSchemaHistory,
    PostgresSourceConfig, RuntimeConfig, RuntimeSourceConfig,
};

# async fn build() -> rustcdc::Result<()> {
let source = PostgresSourceConfig {
    host: "localhost".into(),
    port: 5432,
    user: "postgres".into(),
    password: "postgres".into(),
    database: "postgres".into(),
    replication_slot_name: "rustcdc_slot".into(),
    publication_name: "rustcdc_publication".into(),
    ..PostgresSourceConfig::default()
};

let config = RuntimeConfig::new(
    RuntimeSourceConfig::Postgres(source),
    FileCheckpoint::new("/var/lib/rustcdc/checkpoints"),
    FileSchemaHistory::new("/var/lib/rustcdc/schema-history.json").await?,
)
.with_max_buffer_size(1_000);
# let _ = config;
# Ok(())
# }
```

`InMemoryCheckpoint` and `InMemorySchemaHistory` exist and are convenient in tests, but they
lose everything on restart — which means a restart re-reads from the beginning, or from
nothing at all. Use the file-backed pair, or your own durable backend, for anything you care
about.

## 4. Run the loop

The delivery contract is: poll a batch, apply it, then acknowledge it. The acknowledgement is
a separate step on purpose — the durable position must not advance past what your sink has
actually committed, or a crash in between silently skips those rows.

```rust
use rustcdc::{CdcRuntime, Event, RuntimeConfig};

# fn apply(_event: &Event) -> rustcdc::Result<()> { Ok(()) }
async fn run(config: RuntimeConfig) -> rustcdc::Result<()> {
    let mut runtime = CdcRuntime::new(config)?;
    runtime.start().await?;

    loop {
        let batch = runtime.poll_event_batch().await?;

        for event in batch.events() {
            apply(event)?;                      // your sink
        }
        // flush_sink().await?;                 // make the writes durable FIRST

        runtime.commit_ack(batch.ack_mode()).await?;
    }
}
```

`batch.ack_mode()` returns an `AckToken` that `commit_ack` consumes. There is no other way to
advance the checkpoint, so a pipeline that forgets to acknowledge stalls visibly instead of
losing data quietly. An empty batch yields `AckMode::NotRequired` and `commit_ack` is a no-op,
so the loop above is correct as written.

Order matters: flush the sink **before** acknowledging. Acknowledge first and a crash in the
gap drops every event in the batch.

## 5. Apply events correctly

`apply` above is where the one genuinely subtle part of CDC lives. Not every event carries a
complete row, and writing a partial one as if it were complete overwrites untouched columns
with `NULL`. Match on `row_write()` rather than reaching for `event.after`:

```rust
use rustcdc::RowWrite;
# use rustcdc::Event;
# struct Sink;
# impl Sink {
#     fn replace(&self, _key: Option<serde_json::Value>, _row: &serde_json::Value) {}
#     fn update_only(&self, _key: serde_json::Value, _cols: &serde_json::Value) {}
#     fn delete(&self, _key: serde_json::Value) {}
#     fn truncate(&self) {}
# }
# fn example(event: &Event, sink: &Sink) {
match event.row_write() {
    // The source supplied the whole row: replace it.
    RowWrite::Replace { key, row } => sink.replace(key, row),
    // Partial row: SET only these columns, leave the rest alone.
    RowWrite::Merge { key, columns, .. } => sink.update_only(key, columns),
    RowWrite::Delete { key } => sink.delete(key),
    RowWrite::Truncate => sink.truncate(),
    // DDL, or an event with no addressable row.
    RowWrite::None { .. } => {}
    _ => {}
}
# }
```

Full treatment, including which fields tell you *why* a payload was partial, is in
[Partial payloads](@/docs/api.md#partial-payloads-read-this-before-writing-a-sink).

## 6. Backfill existing rows

The steps so far capture changes made *after* the slot was created. To also load rows that
already exist, run a snapshot. Prefer the incremental one:

```rust
use rustcdc::source::IncrementalSnapshotConfig;
# use rustcdc::{checkpoint::InMemoryCheckpoint, schema_history::InMemorySchemaHistory,
#     RuntimeConfig, RuntimeSourceConfig};
# let config = RuntimeConfig::new(
#     RuntimeSourceConfig::Disabled,
#     InMemoryCheckpoint::default(),
#     InMemorySchemaHistory::default(),
# );

let config = config.with_incremental_snapshot(
    IncrementalSnapshotConfig::new(vec!["public.users".to_string()]),
);
# let _ = config;
```

This is the DBLog watermark algorithm: it interleaves keyset-paginated chunk reads with the
live replication stream rather than holding one long transaction, so the stream never pauses
and no transaction ID backlog accumulates. Chunk cursors are persisted inside the checkpoint
offset — the same atomic, fsynced, checksummed write as the stream position — so a restart
mid-snapshot resumes at the chunk boundary instead of starting the table over.

`RuntimeConfig::with_snapshot_tables` is the older blocking path, kept for the case where you
want one consistent read and do not care that the stream waits for it. Set one or the other,
never both.

## 7. Know whether it is running

`RuntimeState` cannot distinguish a connector streaming from a quiet database from one hung on
a dead socket — both report `Running`. Ask for the health verdict instead:

```rust
# use rustcdc::{CdcRuntime, HealthVerdict};
# async fn example(runtime: &CdcRuntime) {
let snapshot = runtime.admin_snapshot();
if snapshot.health.is_alertable() {
    // `Stalled { reason }` — the reason names both the condition and the remedy.
    eprintln!("cdc stalled: {:?}", snapshot.health);
}
# }
```

The same verdict is exported as `rustcdc_runtime_health{verdict="stalled"}` for Prometheus.
Alert on exactly that; see [health verdict](@/docs/runbook.md#health-verdict-idle-vs-stalled)
for why `Idle` is not an alert condition.

## Delivery guarantees

rustcdc is **at-least-once**. After a crash, a restart, or a partial ack window, you will see
duplicates. Ordering is preserved within committed ack prefixes. Deduplicate sink-side on a
stable key — source, table, primary key, source offset — and test that dedup before you rely
on it.

There is no exactly-once mode, and no configuration that produces one.

## Feature profiles

The default build is `postgres` + `tls`. Everything else is additive:

```bash
cargo build                                # postgres + tls
cargo build --features mysql               # or mariadb, sqlserver
cargo build --features wasm                # WASM transform sandbox (~15 MB overhead)
cargo build --no-default-features          # foundation only, no connector
cargo build --all-features
```

Relational connector features enable `tls` transitively. Private-CA and mutual-TLS
deployments need no extra feature — configure `TransportConfig::tls_with_ca_cert_path(...)`
or `TransportConfig::mtls(...)` directly. `TransportConfig::tls_insecure_skip_verify()` exists
for local testing and air-gapped environments where CA distribution is impractical; it
disables certificate *and* hostname verification and does not belong in production.

## Where to go next

- [Architecture](@/docs/architecture.md) — the commit barrier and checkpoint model, and why
  the ack is a separate step
- [API guide](@/docs/api.md) — transforms, codecs, schema registries, custom sinks
- [Configuration reference](@/docs/config-reference.md) — every option and the failure it
  prevents, including the MySQL and SQL Server server-side prerequisites
- [Reliability testing](@/docs/reliability-testing.md) — replay a captured stream inside your
  own test suite
- [Runbook](@/docs/runbook.md) — what to alert on before this goes to production
