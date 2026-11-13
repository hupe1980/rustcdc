+++
title = "Performance"
description = "Rows per second and latency from a real PostgreSQL through the release binary to a real Kafka, under both delivery contracts, and the command that measures it on your machine."
weight = 135
+++

One command measures the whole pipeline on your machine: a real PostgreSQL, the release `rustcdc`
binary built the way the container image builds it, and a real Apache Kafka, under each delivery
contract.

```bash
cargo xtask pipeline-bench      # Docker; about three minutes after the first build
```

It writes 200 000 rows in transactions of 1 000, with PostgreSQL itself as the writer, then one
long transaction: 100 000 rows updated three times, held open about 20 s. It reads the topic back with a consumer
(`read_committed` for `effectively_once`) and **fails rather than reporting a rate if any row never
arrives**.

## Measured

```text
rustcdc pipeline bench · 2026-11-13 · Apple M4 Pro, 14 cores · Darwin 25.6.0 · rustcdc 0.21.0
PostgreSQL 18.6 → rustcdc (release) → Apache Kafka 4.4.0 · 200 000 rows, 1 000 per transaction

                      rows/s       p50       p95       p99   write → broker
at_least_once         23 611   4494 ms   7645 ms   7905 ms
effectively_once      21 742   4836 ms   8081 ms   8372 ms

One transaction of 300 000 updates (100 000 rows × 3), open 21.2 s
  primary                  streamed 106 MB, spilled 0 MB to its own disk
  first row at the broker  +0.5 s after the commit   (rows are released at the commit)
  last row at the broker   +11.1 s after the commit

Latency: PostgreSQL's write time to the consumer's arrival, clocks aligned within 17.2 ms.
Reproduce: cargo xtask pipeline-bench  (default settings)
```

Default settings throughout, on a laptop, with both containers in Docker Desktop's VM.

**`rows/s`** counts distinct rows that reached the topic, from the first write to the last
arrival. The two contracts cost about the same here: one Kafka transaction per source
transaction is cheap next to everything else in the path.

**The latency columns are queueing, not capture latency.** The writer is PostgreSQL itself and
finishes in a few seconds, faster than the pipeline drains, so a row's latency is mostly the
backlog in front of it. For capture latency under a paced load, see
[the latency evidence](@/docs/reliability-testing.md#latency-and-throughput).

**The long transaction is where streaming shows.** Its rows reach the topic only after the
commit either way: rustcdc releases nothing a rollback could take back. What changes is the
primary, and how much is left to send once the commit lands. The same run with
`PIPELINE_BENCH_POSTGRES='stream_in_progress_transactions = false'`:

| 300 000 updates, open 21 s | v1 | v2 (default) |
|---|---|---|
| Spilled to the primary's own disk | 106 MB | **0 MB** — streamed instead |
| First row at the broker | +1.2 s after the commit | +0.5 s |
| Last row at the broker | +16.0 s after the commit | **+11.1 s** |

The run fails rather than reporting a number when the transaction neither streamed nor spilled:
too small to pass `logical_decoding_work_mem`, it would measure nothing.
See [Large transactions](@/docs/connectors/postgres.md#large-transactions).

## What these numbers do not say

- **They belong to the machine that produced them.** Run the command on yours; the card names the
  machine, the versions and any setting that was changed.
- **One table, one partition, one connector.** Wide rows, many tables, MySQL, SQL Server or a sink
  other than Kafka are not measured here.
- **Not tuned.** Every setting is the default unless the card says otherwise.
- **Runs differ by a few percent.** Compare runs from the same machine, not single numbers.

## Trying a setting

Two variables put TOML lines into the server's configuration, separated by `;`:

```bash
PIPELINE_BENCH_POSTGRES='max_events_per_poll = 5000' \
PIPELINE_BENCH_RUNTIME='sink_flush_interval_events = 1000' \
cargo xtask pipeline-bench
```

`PIPELINE_BENCH_POSTGRES` goes into `[source.postgres]`, `PIPELINE_BENCH_RUNTIME` into `[runtime]`.
The card prints both whenever they are set, so a tuned run never reads as the default.
`PIPELINE_BENCH_ROWS`, `PIPELINE_BENCH_PER_TXN`, `PIPELINE_BENCH_BULK_ROWS`,
`PIPELINE_BENCH_LONG_PASSES` (`0` skips the long transaction) and `PIPELINE_BENCH_LONG_SECONDS`
change the load. The JSON lands in `target/pipeline-bench.json`.

**`cargo xtask pipeline-bench --smoke`** runs the same harness as a check, not a measurement: a
debug binary, 5 000 rows, no long transaction, about two minutes. It fails when a row never
arrives or, under `effectively_once`, arrives twice. CI runs it on every pull request.
