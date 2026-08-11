+++
title = "Snowflake source"
description = "Capture changes from Snowflake with the CHANGES clause: setup, the transport you supply, what the event stream can and cannot carry, and why Streams are unsafe for an external reader."
weight = 160
+++

rustcdc reads Snowflake through the **`CHANGES` clause**: read-only, no server-side cursor,
the durable position in your checkpoint with every other connector's. Feature `snowflake`,
which adds **no dependencies**.

```bash
cargo add rustcdc --features snowflake
```

## Setup

```sql
ALTER TABLE analytics.public.orders SET CHANGE_TRACKING = TRUE;
-- Retention must exceed the longest outage the pipeline has to survive.
ALTER TABLE analytics.public.orders SET DATA_RETENTION_TIME_IN_DAYS = 7;
```

Change tracking records changes made **after** it is enabled. The role needs `SELECT` on the
tables and a usable warehouse — and nothing else. No write grant, which is the property the
next section is about.

## You supply the transport

Snowflake speaks neither the PostgreSQL nor the MySQL wire protocol. Reaching it means HTTPS
plus RSA key-pair JWT signing or OAuth — a dependency tree this crate does not carry, and one
that could never be tested in CI, because there is no self-hostable Snowflake to test it
against. So the transport is a trait, the same shape every other connector here uses
internally:

```rust
use async_trait::async_trait;
use rustcdc::{Result, SnowflakeQueryExecutor, SnowflakeResultSet};

#[derive(Debug)]
struct RestExecutor {
    // your HTTPS client, account URL, JWT signer, warehouse …
}

#[async_trait]
impl SnowflakeQueryExecutor for RestExecutor {
    async fn query(&self, statement: &str) -> Result<SnowflakeResultSet> {
        // POST /api/v2/statements  → { resultSetMetaData: { rowType: [{name}] }, data: [[…]] }
        //
        // Hand the result set back unchanged: same columns, same order, every value as the
        // text Snowflake rendered, `None` for NULL. The REST API already does exactly this.
        let columns: Vec<String> = todo!("rowType[].name");
        let rows: Vec<Vec<Option<String>>> = todo!("data");
        Ok(SnowflakeResultSet::new(columns, rows))
    }
}
```

### Authentication: every method, because rustcdc implements none

The executor owns the session, so whichever method Snowflake supports, this connector
supports — there is no credential type in `SnowflakeSourceConfig` to constrain you, and no
auth code here to fall behind Snowflake's roadmap.

| Method | Where it lives |
|---|---|
| Key-pair (RSA) JWT | your signer; the passphrase of an encrypted private key never reaches rustcdc |
| Workload identity federation — AWS IAM, Entra ID, GCP service accounts, OIDC/Kubernetes, SPIFFE | your provider's attestation token, exchanged by your client |
| OAuth (external IdP or Snowflake OAuth) | your token endpoint |
| Programmatic access tokens (PAT) | your secret store |

That matters more than it looks: Snowflake is retiring single-factor passwords, and by the
end of 2026 a service user may authenticate only by key-pair, OAuth, PAT or WIF. A connector
that had hard-coded one of them would be a migration problem; one that holds none of them is
not.

> **The executor must refresh its own credentials.** Every method above is short-lived — a
> key-pair JWT lasts at most an hour, WIF attestations and OAuth tokens less. Unlike the
> database connectors, which re-resolve a `SecretString` on every reconnect (which is how AWS
> IAM database auth works there), a Snowflake executor is constructed once and lives for the
> process: nothing will ever hand it a fresh credential. `query` takes `&self`, so cache the
> token behind a `Mutex`/`RwLock` or an `ArcSwap` and mint a new one before expiry. An
> executor that signs once at construction works in testing and starts returning `401` an
> hour into production.

Three rules make an implementation correct:

- **Do not re-type the values.** Every value is text, exactly as Snowflake rendered it. Parsing
  a `NUMBER(38,4)` into a float on the way through loses precision that
  [the crate's value contract](@/docs/api.md#column-values-are-text-on-every-connector-and-every-path)
  exists to keep.
- **Do not reorder or filter.** The connector reads columns by name and rows by position.
- **Return the server's error message intact.** rustcdc inspects it to tell a time-travel
  retention failure — data loss, and terminal — from an ordinary transient error.

## Wiring it up

There is no `RuntimeSourceConfig::Snowflake`: that enum holds fully serializable
configuration and a transport object is not. Register the source instead, which is the same
path a third-party connector takes and carries the same runtime guarantees — commit barrier,
checkpointing, transforms, the idempotency guard, health verdicts, metrics.

```rust,ignore
// Needs a live account and your own executor, so this cannot run as a doctest.
use std::sync::Arc;
use rustcdc::{CdcRuntime, SnowflakeSource, SnowflakeSourceConfig};

let snowflake = SnowflakeSourceConfig::new("ANALYTICS", "PUBLIC")
    .with_tables(["ORDERS", "CUSTOMERS"])
    .with_primary_key("ORDERS", ["ID"])
    .with_primary_key("CUSTOMERS", ["ID"])
    .with_poll_interval_ms(60_000);

let mut runtime = CdcRuntime::new(config)?;   // RuntimeSourceConfig::Disabled
runtime.register_source(Box::new(SnowflakeSource::new(
    snowflake,
    Arc::new(RestExecutor { /* … */ }) as Arc<dyn rustcdc::SnowflakeQueryExecutor>,
)?));
runtime.start().await?;
```

Every field is in the [configuration reference](@/docs/config-reference.md#snowflake-source-configuration).

> **Identifiers are used exactly as written.** Snowflake folds an unquoted identifier to
> **upper** case and this connector quotes whatever you configure, so a table created as
> `orders` is `ORDERS` on the server: `["ORDERS"]` finds it, `["orders"]` does not. Declared
> key columns are matched against result-set column names, which are folded the same way.

## How it works

Each poll reads the server's current instant, then asks each table what changed since the last
committed window:

```sql
SELECT * FROM "ANALYTICS"."PUBLIC"."ORDERS"
CHANGES(INFORMATION => DEFAULT)
AT(TIMESTAMP => TO_TIMESTAMP_LTZ(1754902800000000000, 9))
END(TIMESTAMP => TO_TIMESTAMP_LTZ(1754902860000000000, 9));
```

Three decisions in that statement are load-bearing:

**The upper bound comes from Snowflake, not from the process clock.** A client running even
milliseconds fast would ask for a window ending in the future and silently skip whatever
commits in the gap.

**The offset is an integer, not a rendered timestamp.** A rendered timestamp carries a session
time zone and a format, so one instant has many spellings and none of them order
lexicographically across a DST boundary — and the checkpoint's rewind guard needs a total order
to tell a legitimate resume from a connector that lost its place.

**The position advances only after every selected table has been read, and only after the
commit barrier accepts the batch.** Advancing per table would skip the tables that had not been
read when the next window opened. Advancing on the poll rather than the commit would lose
whatever the sink had not accepted — which is exactly the failure the Streams model has.

A crash re-reads the window: at-least-once, the same contract as every other connector here.

### Snapshot and handoff

The initial load reads each table in keyset-paginated chunks with `AT(TIMESTAMP => T)`, and the
stream opens its first window at the same `T`. Snowflake's time travel serves every chunk from
one table version, so the two phases meet exactly — **no overlap to deduplicate, no gap to lose
changes in, and no watermark bracket**, which every other connector in this crate needs because
a chunk `SELECT` and a log position refer to different moments.

The keyset needs a total order, so a snapshot requires the table's key in
`primary_keys`. `METADATA$ROW_ID` is Snowflake's internal row identity — it is not one of your
columns and cannot be ordered by. Refusing is much better than `OFFSET` pagination, which
re-scans from the start on every chunk and, without a total order, can skip or repeat rows
between them.

The trade is that `T` has to stay inside retention for the whole snapshot. A snapshot that
outruns it fails loudly rather than producing a partial copy that reports success.

## What the event stream cannot carry

`CHANGES` is not a transaction log, and this connector does not pretend otherwise.

| | |
|---|---|
| `Event::transaction` | always `None` — there is no transaction id and no commit grouping |
| Intermediate row versions | collapsed: a row updated three times inside one window yields one event, and a row inserted then deleted inside it yields none |
| Source order within a window | none exists; events are sorted by `METADATA$ROW_ID` so re-reading a window is byte-identical |
| `Operation::Truncate` | not reported |
| Schema-change events | not reported; this connector does no DDL capture |

Updates *are* reported as updates. Snowflake emits them as two rows — a `DELETE` and an
`INSERT` sharing a `METADATA$ROW_ID`, both flagged `METADATA$ISUPDATE` — and the connector
collapses the pair into one `Operation::Update` with both images. Passed through verbatim they
would delete and re-insert the row, which downstream is a momentary absence and, on a compacted
log, a tombstone that can outlive the re-insert.

A shorter poll interval collapses less. It also costs more: every poll runs queries on a
warehouse that bills by the second it is awake, unlike a log-based connector, where an idle
stream costs nothing.

## Why not Snowflake Streams

The obvious mechanism, and it cannot be read safely from outside Snowflake.

A stream advances its offset **only when it is consumed inside a DML transaction**. Querying it
does nothing. An external reader therefore has to `INSERT`, `CREATE TABLE AS SELECT` or `COPY
INTO` in the source account to make progress, which breaks three things at once:

**It writes to the source.** Every capture path in this crate is read-only by construction —
which is what makes a snapshot safe against a read replica and a CDC role grantable with no
write privilege.

**It moves the durable position out of the checkpoint.** rustcdc's restart story is one record
written `fsync`-then-rename holding the stream position and the committed event count together.
A stream offset is Snowflake's state, advanced by Snowflake's commit, and the two writes cannot
be made atomic.

**The failure mode is loss, not duplication.** The consuming DML commits *first*. A crash before
rustcdc's checkpoint is durable leaves those changes gone from the stream and never in the
checkpoint — at-most-once, silently. Snowflake documents a sharper edge still: consuming a
stream in a DML statement can advance the offset even when the surrounding transaction rolls
back, in some autocommit scenarios.

Staging the stream into a scratch table inside the advancing transaction would work, at the
cost of a write-ahead log implemented in a data warehouse, billed in warehouse credits, with two
durable positions to reconcile. `CHANGES` gives the same thing away for free.

## Evidence, and the gap in it

Statement construction and identifier quoting, window arithmetic and boundary joins, the
update-pair collapse, the text-value contract, retention-failure classification, keyset
advance, and the snapshot-to-stream handoff are covered by 35 unit tests driven through a
scripted executor — no account, no network, no warehouse.

**What no test here establishes** is that a real Snowflake agrees with the statements: that
`AT`/`END` bracket the interval as documented, that `DATE_PART(EPOCH_NANOSECOND, …)` and
`TO_TIMESTAMP_LTZ(…, 9)` round-trip, and that `METADATA$ISUPDATE` arrives spelled as expected.
Snowflake has no self-hostable implementation, so unlike PostgreSQL, MySQL, MariaDB and SQL
Server — each pinned against a container in CI — that part is unverified here. It is stated
rather than implied away. If you run this against a live account, an issue reporting what the
server actually did is the most useful thing you can contribute.
