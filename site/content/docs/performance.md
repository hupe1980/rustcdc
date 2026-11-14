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
rustcdc pipeline bench · 2026-11-14 · Apple M4 Pro, 14 cores · Darwin 27.2.0 · rustcdc 0.23.0
PostgreSQL 18.7 → rustcdc (release) → Apache Kafka 4.4.0 · 200 000 rows, 1 000 per transaction

                      rows/s       p50       p95       p99   write → broker
at_least_once         32 805   3257 ms   5434 ms   5618 ms
effectively_once      25 972   4064 ms   6969 ms   7233 ms
Rows that arrived more than once: at_least_once 0 · effectively_once 0

One transaction of 300 000 updates (100 000 rows × 3), open 20.9 s
  primary                  streamed 108 MB, spilled 0 MB to its own disk
  first row at the broker  +0.4 s after the commit   (rows are released at the commit)
  last row at the broker   +8.0 s after the commit

Initial snapshot of 1 000 000 rows, one table
  PostgreSQL scan            973 505 rows/s   (COPY TO STDOUT: the source's ceiling)
  rustcdc → file              18 953 rows/s     6.9 µs CPU per row
  rustcdc → Kafka             34 411 rows/s    17.5 µs CPU per row

Latency: PostgreSQL's write time to the consumer's arrival, clocks aligned within 16.1 ms.
Reproduce: cargo xtask pipeline-bench  (default settings)
```

Default settings throughout, on a laptop, with both containers in Docker Desktop's VM.

**`rows/s`** counts distinct rows that reached the topic, from the first write to the last
arrival. `effectively_once` is about a fifth slower: it commits a Kafka transaction, with the
checkpoint in it, for each source transaction, where `at_least_once` writes the checkpoint once a
second.

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
| Spilled to the primary's own disk | 108 MB | **0 MB** — streamed instead |
| First row at the broker | +0.8 s after the commit | +0.4 s |
| Last row at the broker | +8.5 s after the commit | +8.0 s |

The run fails rather than reporting a number when the transaction neither streamed nor spilled:
too small to pass `logical_decoding_work_mem`, it would measure nothing.
See [Large transactions](@/docs/connectors/postgres.md#large-transactions).

**The initial snapshot** reads a 1 000 000-row table three ways. PostgreSQL is not the limit: reading a chunk is a few percent of its time. rustcdc's work per
row is, and to Kafka most of that is the producer's. The file sink is slower than Kafka on this
laptop because each of its flushes is an `F_FULLFSYNC`, which on macOS waits for the drive itself.
Not measured on Linux. `PIPELINE_BENCH_SNAPSHOT_ROWS` sets the table size; `0` skips it.

**The checkpoint is written once a second, not per batch.** A crash replays at most that second of
delivered rows and loses none; see [`at_least_once`](@/docs/concepts.md#at-least-once-default).
Writing it per batch instead (`PIPELINE_BENCH_RUNTIME='checkpoint_interval_ms = 0'`), two
alternating runs each on this laptop:

| | per batch | once a second (default) |
|---|---|---|
| `at_least_once`, rows/s | 26 294 · 25 484 | **31 941 · 31 424** |
| `at_least_once`, p99 | 7.1 s · 7.4 s | **5.8 s · 5.9 s** |
| Snapshot → Kafka, rows/s | 28 746 · 27 259 | **32 968 · 34 005** |
| Snapshot → file, rows/s | 16 264 · 16 256 | **19 560 · 19 821** |

The CPU per row did not change; the gain is time spent waiting on the drive. `effectively_once`
writes the checkpoint in each Kafka transaction either way, so it does not move.

## What these numbers do not say

- **They belong to the machine that produced them.** Run the command on yours; the card names the
  machine, the versions and any setting that was changed.
- **One table, one partition, one connector.** Wide rows, many tables, MySQL, SQL Server or a sink
  other than Kafka are not measured here.
- **Not tuned.** Every setting is the default unless the card says otherwise.
- **Runs vary.** Two runs of this build on this laptop ranged from 23 800 to 32 800 rows/s and
  the long transaction's last row from +8.0 to +8.2 s; a compile running alongside halved
  `at_least_once`. Compare several runs on one quiet machine, not single numbers.

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
