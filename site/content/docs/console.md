+++
title = "Console"
description = "The operator console at /ui: is the pipeline healthy, whose problem is it if not, and incremental snapshots — in the browser."
weight = 65
+++

`rustcdc run` serves a console on the admin listener at **`/ui`** — `http://127.0.0.1:8080/ui` by
default. It answers the first question about a pipeline, *is it working, and if not, whose problem
is it?*, in one sentence, and drives incremental snapshots without a `curl` in sight.

<picture>
  <source srcset="../../console/health-dark.webp" media="(prefers-color-scheme: dark)">
  <img src="../../console/health-light.webp" alt="The Health place: a healthy verdict, the source → rustcdc → destination strip, throughput and the last batch" width="1280" height="800" loading="lazy">
</picture>

## Health

A sentence first, then the evidence: the verdict, the pipeline strip from source to destination,
events per second over the last minutes, the last batch, source lag and uptime. When the pipeline
stalls, the sentence names **who can fix it** — the source database, the destination or rustcdc
itself — says what to do, says whether a restart would help, and links the runbook section. The
strip lights the hop that owns it.

<picture>
  <source srcset="../../console/stalled-health-dark.webp" media="(prefers-color-scheme: dark)">
  <img src="../../console/stalled-health-light.webp" alt="A stall: the destination keeps refusing a batch; the destination hop is lit; restarting will not help" width="1280" height="800" loading="lazy">
</picture>

While a PostgreSQL transaction is being streamed before its commit, the sentence says so —
*Reading a transaction in progress — 2.1M changes so far*, how long it has been open and whether
it is held in memory or on disk — where a quiet pipeline would otherwise just read as idle. Its
rows arrive when it commits ([Large transactions](@/docs/connectors/postgres.md#large-transactions)).

<picture>
  <source srcset="../../console/streaming-health-dark.webp" media="(prefers-color-scheme: dark)">
  <img src="../../console/streaming-health-light.webp" alt="A transaction streaming in before its commit: 2.1M changes so far, open for 4 min, 1.3 GB held on disk" width="1280" height="800" loading="lazy">
</picture>

The owner and whether a restart helps come from the server (`/status` → `health.owner`,
`health.restart_helps`), the same routing `/livez` acts on; the console only words it. The browser
tab follows the verdict too — `● Healthy`, `▲ Stalled` — so it can be watched from another tab.

## Snapshots

Pick tables, or name them, and start an incremental snapshot; pause, resume or stop the one in
flight and watch each table's rows and chunks. **Only some rows** takes a SQL condition per table —
`updated_at > now() - interval '1 day'` re-reads just the last day. It is raw SQL with the trust of
the connection string, not a tenancy boundary.

A signal is accepted before it runs, so the console follows it to what actually happened: a refusal
with the server's reason, or "Snapshot complete — 417 rows" once every table is read — not when the
request was queued. Each action carries its own signal id, so a double click or a retry cannot start
a second snapshot. **The request this sends** shows the `curl`, with the token as a variable.

<picture>
  <source srcset="../../console/snapshots-dark.webp" media="(prefers-color-scheme: dark)">
  <img src="../../console/snapshots-light.webp" alt="Snapshots: per-table progress and the start form" width="1280" height="800" loading="lazy">
</picture>

Snapshots need an `[incremental_snapshot]` section in the configuration; without it the page says so.

## Activity and Shapes

**Activity** is the live notification stream and the audit trail, newest first, filterable.
**Mark** writes a marker — *deploy 4.2 starts* — into the stream and the audit trail, so the moment
can be found later, downstream as well as here. Each signal is one row, at its latest step.

<picture>
  <source srcset="../../console/activity-dark.webp" media="(prefers-color-scheme: dark)">
  <img src="../../console/activity-light.webp" alt="Activity: a marker, a completed snapshot signal and the state transition, one row each" width="1280" height="800" loading="lazy">
</picture>

**Shapes** looks up a row shape by the id a row carries (the `__rustcdc.row_shape` Kafka header, or
the CloudEvents `dataschema`): its columns, declared types, nullability and key.

<picture>
  <source srcset="../../console/shapes-dark.webp" media="(prefers-color-scheme: dark)">
  <img src="../../console/shapes-light.webp" alt="Shapes: the columns of public.orders with declared types and the key" width="1280" height="800" loading="lazy">
</picture>

`⌘K` goes anywhere; `⌘1`–`⌘4` switch places.

## Access

The console is a client of the [admin API](@/docs/operations.md) and nothing more: it has no
endpoint, session or privilege of its own.

- **With admin tokens configured** it asks for the read token (`admin.read_token_env`), and for the
  write token (`admin.write_token_env`) the first time an action needs it. Both stay in the tab's
  session storage — never a cookie or a URL — and are gone when the tab closes.
- **Without tokens** it opens directly, and can do exactly what an unauthenticated `curl` can.
- Its files are served under their own Content-Security-Policy (this origin only, never framed);
  API responses carry `default-src 'none'`.

## Turning it off

```toml
[admin.ui]
enabled = false
```

Or build the server without the `ui` feature. A build compiled without the console bundle serves a
page at `/ui` saying how to build it.

## Building it from source

The release binaries, the container image and Homebrew include it. From a checkout, with Node 24:

```sh
cargo xtask console             # builds crates/rustcdc-server/ui/dist
cargo build -p rustcdc-server   # embeds it
cargo xtask console-dev         # live reload against `rustcdc run` (RUSTCDC_URL, default :8080)
```
