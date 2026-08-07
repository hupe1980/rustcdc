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
