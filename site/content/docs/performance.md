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
`UPDATE` of 100 000 rows in a single transaction. It reads the topic back with a consumer
(`read_committed` for `effectively_once`) and **fails rather than reporting a rate if any row never
arrives**.

## Measured

```text
rustcdc pipeline bench · 2026-11-13 · Apple M4 Pro, 14 cores · Darwin 25.6.0 · rustcdc 0.21.0
PostgreSQL 18.6 → rustcdc (release) → Apache Kafka 4.4.0 · 200 000 rows, 1 000 per transaction

                      rows/s       p50       p95       p99   write → broker
at_least_once         23 484   4470 ms   7606 ms   7874 ms
effectively_once      23 075   4369 ms   7800 ms   8086 ms

One transaction of 100 000 updates, committed 0.3 s after it began
  first row at the broker  +0.9 s after the commit   (protocol v1 sends nothing before it)
  last row at the broker   +3.8 s after the commit

Latency: PostgreSQL's write time to the consumer's arrival, clocks aligned within 17.3 ms.
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

**The bulk update shows what pgoutput protocol version 1 does**: PostgreSQL sends a transaction's
changes only after it commits, so the first row cannot arrive before the commit, and a large
transaction reaches the topic as one burst after it.

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
`PIPELINE_BENCH_ROWS`, `PIPELINE_BENCH_PER_TXN` and `PIPELINE_BENCH_BULK_ROWS` change the load.
The JSON lands in `target/pipeline-bench.json`.
