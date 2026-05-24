#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

BENCHMARK_REPORT_BENCHES="cdc_perf" bash scripts/run_benchmark_report.sh
