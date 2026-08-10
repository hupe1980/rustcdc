# Benchmark baselines

Recorded on the maintainer's machine (Apple Silicon, `darwin 25.5.0`), `cargo bench`
with `[profile.bench]` as committed (`opt-level = 3`, `debug-assertions = true`).

**These numbers are hardware-specific.** Use them for order-of-magnitude sanity, not as
CI thresholds — take a local baseline with `cargo bench -- --save-baseline main` and
compare against that.

| Benchmark | Median | Per event | Notes |
|---|---|---|---|
| `encode/json_serialize` | 561 µs / 1 000 events | **0.56 µs** | JSON encode of a representative `Update` (before + after, 5 columns, ~520 B) |
| `build/event_builder` | 618 µs / 1 000 events | 0.62 µs | Fixture construction — subtract this from any figure that builds its own events |
| `size_check/64B` | 167 µs / 500 events | 0.33 µs | |
| `size_check/1024B` | 298 µs / 500 events | 0.60 µs | |
| `size_check/16384B` | 2.36 ms / 500 events | 4.71 µs | Encode cost dominates; scales with payload |

## Why these exist

Nothing measured throughput before, which is how the send path came to serialise every
event to JSON purely to measure it and then discard the buffer. The latency
histograms would not have shown it either: they were quantised to whole milliseconds
against a lowest bucket of 1 ms, so every per-event operation reported zero.

## A correction worth recording

The first measurement of the discarded encode was taken in a **debug** build and came to
13.9 µs/event. The release figure above is **0.56 µs/event** — roughly 25× cheaper. The
debug number materially overstated the cost, and the performance claim that rested on it
was corrected accordingly. Benchmark in the profile you will ship.

The fix still stands on its own: it deletes code, and it makes `runtime.max_event_bytes`
measure the payload the transport actually sends rather than a JSON rendering that, for
Avro or Protobuf, is never transmitted. That semantic defect was always the more
important half.

Benchmark in the profile you will ship.

## Pipeline throughput (`benches/throughput.rs`)

Same machine and profile as above. `Melem/s` and `Kelem/s` are events per second as
criterion reports them.

| Benchmark | Median | Per event | Throughput |
|---|---|---|---|
| `transform/no_rules` | 423 µs / 1 000 events | **0.42 µs** | 2.36 M events/s |
| `transform/one_mask_rule` | 916 µs / 1 000 events | 0.92 µs | 1.09 M events/s |
| `pipeline/parallelism/1` | 47.6 ms / 1 000 events | 47.6 µs | 21.0 K events/s |
| `pipeline/parallelism/4` | 49.1 ms / 1 000 events | 49.1 µs | 20.4 K events/s |
| `pipeline/parallelism/16` | 51.8 ms / 1 000 events | 51.8 µs | 19.3 K events/s |

### What these say

**The transform stage is not the bottleneck.** A pipeline with zero rules costs 0.42 µs per
event and one with a mask rule 0.92 µs — against 47.6 µs per event for the full batch path
through a `file_jsonl` sink. Delivery is roughly **100×** the transform. A review had
hypothesised the opposite (that per-event allocation in `TransformPipeline::apply` dominated
a no-op pipeline); the measurement refutes it, and the optimisation it recommended would
have been premature.

**`prepare_parallelism` does nothing here, and that is expected once you look at the
shape.** 1 → 16 moves throughput by less than measurement noise, and the p-values say the
apparent decline at 16 is real but tiny. The prepare stage is `.buffered(prepare_parallelism)`
while delivery is a single sequential loop, so parallelism only helps when *prepare* is
expensive relative to *deliver*. With a durable sink it is 1 % of the cost. The knob earns
its keep for WASM transforms and heavy codecs, not for throughput in general — the docs now
say so.

**The `pipeline/*` figure is fsync-dominated.** `file_jsonl` defaults to `fsync_every = 1`
and the benchmark flushes every 100 events, so ~10 fsyncs per iteration at roughly 4.7 ms
each on this machine's filesystem. That is deliberate: a benchmark against a null sink
measures a pipeline nobody deploys. It does mean the absolute number is a property of the
disk as much as the code, so **compare ratios across a baseline, not the number itself**.

### A methodology correction worth recording

The first version of `transform/no_rules` reported 0.61 µs/event. That was `Event::clone`:
the loop iterated `batch.iter().cloned()` *inside* the measured region.
`benches/pipeline.rs` measures event construction at 0.62 µs/event, and the two figures
agreeing was the tell. The clone now happens in `iter_batched`'s setup closure, and the
figure dropped to 0.42 µs/event — a 34 % measurement error in the direction that would have
confirmed the hypothesis being tested.

This is the same trap the note at the top of this file warns about, made by the person who
wrote the note. Build fixtures in setup, not in the loop.

## Kafka sink pipelining

Measured by `pipelining_outperforms_one_round_trip_per_record` in `src/sink/kafka.rs`,
against krafka's in-process `FakeBroker`, 300 records to a single-partition topic,
`linger_ms = 2`:

| `max_pipelined_sends` | Wall clock | Produce requests |
|---|---|---|
| `1` (one round-trip per record) | 712 ms | 300 |
| `256` | 7.9 ms | 3 |

**Read the right column.** Wall clock against an in-process broker understates the gain,
because the round-trip it removes is a function call rather than a network hop; the
request count is what carries over to a real cluster, and it is what the test asserts on.
Against a broker at 1 ms RTT the depth-1 configuration is bounded at roughly 1 000
events/s per sink regardless of hardware.

The test asserts on request count rather than time deliberately: a wall-clock threshold on
shared CI hardware fails on noise and gets disabled, which is worse than no gate.
