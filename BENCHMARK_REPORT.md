# Benchmark Report

> WARNING: This report is non-release-local. Do not treat it as release baseline evidence.
> Reason: strict_mode must be 1.

Generated: 2026-05-26T02:02:33Z

## Environment

```text
Darwin mb.fritz.box 25.1.0 Darwin Kernel Version 25.1.0: Mon Oct 20 19:34:05 PDT 2025; root:xnu-12377.41.6~2/RELEASE_ARM64_T6041 arm64
git_commit=0f40d30063559b27fe49b4eb3f3858a8c276098f
git_tree_state=dirty
baseline_commit=
baseline_artifact=
rustc 1.92.0 (ded5c06cf 2025-12-08)
binary: rustc
commit-hash: ded5c06cf21d2b93bffd5d884aa6e96934ee4234
commit-date: 2025-12-08
host: aarch64-apple-darwin
release: 1.92.0
LLVM version: 21.1.3
gated_benches=quality_perf
regression_threshold_percent=5
strict_mode=0
sample_size=10
measurement_secs=1
warmup_secs=1
confirm_runs=0
preheat_runs=0
report_classification=non-release-local
report_classification_reason=strict_mode must be 1
```

## Benchmarks

### quality_perf

#### Gate Run Output

```text
snapshot_validator/10000
                        time:   [214.28 µs 217.60 µs 220.66 µs]
                        thrpt:  [45.320 Melem/s 45.956 Melem/s 46.667 Melem/s]
                 change:
                        time:   [+3.5527% +9.5380% +13.682%] (p = 0.00 < 0.05)
                        thrpt:  [−12.035% −8.7075% −3.4308%]
                        Performance has regressed.

stream_events/stream_1k_events
                        time:   [574.71 µs 581.23 µs 588.28 µs]
                        thrpt:  [1.6999 Melem/s 1.7205 Melem/s 1.7400 Melem/s]
                 change:
                        time:   [−8.2910% −6.3566% −4.4101%] (p = 0.00 < 0.05)
                        thrpt:  [+4.6136% +6.7881% +9.0405%]
                        Performance has improved.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild

full_cycle_snapshot_stream_handoff
                        time:   [1.1591 ms 1.1650 ms 1.1736 ms]
                        change: [−75.648% −74.851% −74.256%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild

parallel_snapshot_4_tables_100k
                        time:   [24.264 ms 24.418 ms 24.734 ms]
                        change: [−85.379% −85.166% −84.955%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild

quality_gates/snapshot_10k_rows
                        time:   [210.57 µs 211.70 µs 213.13 µs]
                        change: [−7.0761% −3.3249% −0.2827%] (p = 0.09 > 0.05)
                        No change in performance detected.
quality_gates/stream_1k_events_target
                        time:   [550.11 µs 553.93 µs 559.09 µs]
                        change: [−13.725% −12.131% −10.941%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 2 outliers among 10 measurements (20.00%)
  2 (20.00%) high severe

event_json_roundtrip    time:   [957.77 ns 963.89 ns 969.47 ns]
                        change: [−3.1326% −2.0434% −1.0242%] (p = 0.00 < 0.05)
                        Performance has improved.

transform_pipeline_two_stages
                        time:   [559.11 ns 573.46 ns 592.01 ns]
                        change: [−9.0414% −7.7467% −6.0699%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe

```

#### Retry Output

```text
snapshot_validator/10000
                        time:   [209.74 µs 215.83 µs 223.38 µs]
                        thrpt:  [44.766 Melem/s 46.333 Melem/s 47.679 Melem/s]
                 change:
                        time:   [−4.0269% −2.0246% +0.2154%] (p = 0.11 > 0.05)
                        thrpt:  [−0.2149% +2.0664% +4.1959%]
                        No change in performance detected.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild

stream_events/stream_1k_events
                        time:   [555.08 µs 561.57 µs 568.41 µs]
                        thrpt:  [1.7593 Melem/s 1.7807 Melem/s 1.8015 Melem/s]
                 change:
                        time:   [−5.4733% −4.1428% −2.6911%] (p = 0.00 < 0.05)
                        thrpt:  [+2.7655% +4.3218% +5.7902%]
                        Performance has improved.

full_cycle_snapshot_stream_handoff
                        time:   [1.1509 ms 1.1586 ms 1.1644 ms]
                        change: [−2.4644% −1.0254% +0.3750%] (p = 0.21 > 0.05)
                        No change in performance detected.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild

parallel_snapshot_4_tables_100k
                        time:   [24.154 ms 24.459 ms 24.760 ms]
                        change: [−2.4305% −0.8553% +0.6501%] (p = 0.33 > 0.05)
                        No change in performance detected.

quality_gates/snapshot_10k_rows
                        time:   [214.46 µs 216.30 µs 218.05 µs]
                        change: [+1.1051% +2.0084% +2.9339%] (p = 0.00 < 0.05)
                        Performance has regressed.
quality_gates/stream_1k_events_target
                        time:   [545.84 µs 547.78 µs 550.76 µs]
                        change: [−1.3242% −0.5691% +0.0275%] (p = 0.15 > 0.05)
                        No change in performance detected.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild

event_json_roundtrip    time:   [981.31 ns 1.0101 µs 1.0595 µs]
                        change: [+2.2050% +5.7527% +11.177%] (p = 0.01 < 0.05)
                        Performance has regressed.
Found 2 outliers among 10 measurements (20.00%)
  2 (20.00%) high severe

transform_pipeline_two_stages
                        time:   [556.29 ns 559.96 ns 563.81 ns]
                        change: [−3.2878% −1.3430% +0.1069%] (p = 0.17 > 0.05)
                        No change in performance detected.

```

#### Preheat Run 1 Output

```text
snapshot_validator/10000
                        time:   [206.66 µs 213.34 µs 224.99 µs]
                        thrpt:  [44.446 Melem/s 46.873 Melem/s 48.388 Melem/s]
                 change:
                        time:   [+7.9510% +11.121% +14.823%] (p = 0.00 < 0.05)
                        thrpt:  [−12.910% −10.008% −7.3654%]
                        Performance has regressed.

stream_events/stream_1k_events
                        time:   [552.51 µs 555.01 µs 559.48 µs]
                        thrpt:  [1.7874 Melem/s 1.8018 Melem/s 1.8099 Melem/s]
                 change:
                        time:   [−5.7903% −5.1833% −4.4430%] (p = 0.00 < 0.05)
                        thrpt:  [+4.6496% +5.4667% +6.1461%]
                        Performance has improved.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild

full_cycle_snapshot_stream_handoff
                        time:   [1.0644 ms 1.0890 ms 1.1284 ms]
                        change: [−75.689% −75.177% −74.473%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe

parallel_snapshot_4_tables_100k
                        time:   [22.950 ms 23.097 ms 23.223 ms]
                        change: [−85.897% −85.794% −85.673%] (p = 0.00 < 0.05)
                        Performance has improved.

quality_gates/snapshot_10k_rows
                        time:   [190.28 µs 191.45 µs 193.04 µs]
                        change: [−2.6429% −2.1302% −1.5462%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
quality_gates/stream_1k_events_target
                        time:   [535.28 µs 536.51 µs 537.56 µs]
                        change: [−9.8247% −8.7872% −8.0345%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) low mild
  1 (10.00%) high mild

event_json_roundtrip    time:   [997.32 ns 999.30 ns 1.0013 µs]
                        change: [+7.4987% +8.2141% +8.7564%] (p = 0.00 < 0.05)
                        Performance has regressed.

transform_pipeline_two_stages
                        time:   [549.62 ns 559.18 ns 575.80 ns]
                        change: [−8.6221% −7.2616% −5.1716%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe

```
