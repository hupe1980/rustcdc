#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for latency gate" >&2
  exit 1
fi

# Execute connector-backed latency evidence first.
if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for latency evidence runs" >&2
  exit 1
fi

export CDC_RS_RUN_DOCKER_TESTS=1
latency_evidence_report_path="target/latency-evidence.txt"
mkdir -p target
: > "$latency_evidence_report_path"

run_latency_step() {
  local label="$1"
  shift
  echo "==> $label" | tee -a "$latency_evidence_report_path"
  "$@" 2>&1 | tee -a "$latency_evidence_report_path"
  echo | tee -a "$latency_evidence_report_path"
}

run_latency_step "postgres connector latency evidence" \
  cargo test --test postgres_latency_evidence --features postgres -- --nocapture

run_latency_step "mysql connector latency evidence" \
  cargo test --test mysql_latency_evidence --features mysql -- --nocapture

run_latency_step "sqlserver connector latency evidence" \
  cargo test --test sqlserver_latency_evidence --features sqlserver -- --nocapture

for artifact in target/postgres-latency-evidence.md target/mysql-latency-evidence.md target/sqlserver-latency-evidence.md; do
  if [[ -f "$artifact" ]]; then
    {
      echo "==> $(basename "$artifact")"
      cat "$artifact"
      echo
    } | tee -a "$latency_evidence_report_path"
  fi
done

echo "Latency evidence run completed successfully." | tee -a "$latency_evidence_report_path"
echo "Report written to $latency_evidence_report_path" | tee -a "$latency_evidence_report_path"

# ─── Thresholds ──────────────────────────────────────────────────────────────
#
# These gate CAPTURE LATENCY: wall-clock time from the writer committing a row to the
# event reaching the consumer, measured against a single clock with writes running
# concurrently with polling. See tests/latency_evidence_common.rs.
#
# The previous thresholds (p95 <= 500 ms) gated `poll_latency`, which timed draining an
# already-populated in-process VecDeque. That is a sub-millisecond operation, so a 500 ms
# ceiling could not fail for performance reasons — the gate was decorative.
#
# Values below are set per connector from observed local runs, with roughly an order of
# magnitude of headroom for slower and noisier CI hardware. They are meant to catch a
# structural regression (a poll interval left at seconds, a lost wakeup, an accidental
# sleep in the hot path), not to certify a performance number.
#
#   PostgreSQL  observed p95 ~18 ms   -> gate 250 ms
#   MySQL       observed p95 ~30 ms   -> gate 400 ms
#   SQL Server  capture-agent based; latency is dominated by the capture job scan
#               cadence, not by the connector -> gate 5000 ms
DEFAULT_P95_MS="${LATENCY_GATE_DEFAULT_P95_MS:-250}"
DEFAULT_P99_MS="${LATENCY_GATE_DEFAULT_P99_MS:-500}"

POSTGRES_P95_MS="${LATENCY_GATE_POSTGRES_P95_MS:-$DEFAULT_P95_MS}"
POSTGRES_P99_MS="${LATENCY_GATE_POSTGRES_P99_MS:-$DEFAULT_P99_MS}"
MYSQL_P95_MS="${LATENCY_GATE_MYSQL_P95_MS:-400}"
MYSQL_P99_MS="${LATENCY_GATE_MYSQL_P99_MS:-800}"
# SQL Server CDC is not low-latency by design: rows are invisible to any consumer until
# the capture agent has scanned the log. This ceiling reflects that architecture rather
# than a slower connector, and is documented as such in site/content/docs/config-reference.md.
SQLSERVER_P95_MS="${LATENCY_GATE_SQLSERVER_P95_MS:-5000}"
SQLSERVER_P99_MS="${LATENCY_GATE_SQLSERVER_P99_MS:-10000}"

# Minimum measured events required for a percentile to mean anything. A p99 over a
# handful of samples is noise; the old gate's only assertion was `batches > 0`.
MIN_MEASURED_EVENTS="${LATENCY_GATE_MIN_MEASURED_EVENTS:-500}"

report_path="target/latency-gate.txt"
: > "$report_path"

assert_le() {
  local metric_label="$1"
  local actual="$2"
  local limit="$3"

  if awk -v actual="$actual" -v limit="$limit" 'BEGIN { exit (actual <= limit ? 0 : 1) }'; then
    echo "PASS: ${metric_label}=${actual} <= ${limit}" | tee -a "$report_path"
  else
    echo "FAIL: ${metric_label}=${actual} > ${limit}" | tee -a "$report_path"
    return 1
  fi
}

assert_ge() {
  local metric_label="$1"
  local actual="$2"
  local floor="$3"

  if awk -v actual="$actual" -v floor="$floor" 'BEGIN { exit (actual >= floor ? 0 : 1) }'; then
    echo "PASS: ${metric_label}=${actual} >= ${floor}" | tee -a "$report_path"
  else
    echo "FAIL: ${metric_label}=${actual} < ${floor}" | tee -a "$report_path"
    return 1
  fi
}

gate_file() {
  local connector="$1"
  local file="$2"
  local p95_limit="$3"
  local p99_limit="$4"

  if [[ ! -f "$file" ]]; then
    echo "FAIL: missing latency artifact for ${connector}: ${file}" | tee -a "$report_path"
    return 1
  fi

  local capture_p95 capture_p99 measured unstamped
  capture_p95="$(jq -r '.capture_latency_ms_p95' "$file")"
  capture_p99="$(jq -r '.capture_latency_ms_p99' "$file")"
  measured="$(jq -r '.events_measured' "$file")"
  unstamped="$(jq -r '.unstamped_events' "$file")"

  local connector_failed=0

  # Sample validity first: a threshold applied to two samples proves nothing, and a run
  # where events were not measurable at all must not read as a pass.
  assert_ge "${connector}.events_measured" "$measured" "$MIN_MEASURED_EVENTS" || connector_failed=1
  assert_le "${connector}.unstamped_events" "$unstamped" 0 || connector_failed=1

  # The operator-facing metric: source commit -> consumer delivery.
  assert_le "${connector}.capture.p95_ms" "$capture_p95" "$p95_limit" || connector_failed=1
  assert_le "${connector}.capture.p99_ms" "$capture_p99" "$p99_limit" || connector_failed=1

  # Runtime bookkeeping is reported for context but not gated: it is sub-millisecond by
  # construction, so any ceiling on it is either decorative or a flake generator.
  local poll_p95 commit_p95 throughput
  poll_p95="$(jq -r '.poll_latency_ms_p95' "$file")"
  commit_p95="$(jq -r '.commit_latency_ms_p95' "$file")"
  throughput="$(jq -r '.events_per_second' "$file")"
  echo "INFO: ${connector}.poll.p95_ms=${poll_p95} ${connector}.commit.p95_ms=${commit_p95} ${connector}.throughput_eps=${throughput}" | tee -a "$report_path"

  return "$connector_failed"
}

failed=0

gate_file "postgres" "target/postgres-latency-evidence.json" "$POSTGRES_P95_MS" "$POSTGRES_P99_MS" || failed=1
gate_file "mysql" "target/mysql-latency-evidence.json" "$MYSQL_P95_MS" "$MYSQL_P99_MS" || failed=1
gate_file "sqlserver" "target/sqlserver-latency-evidence.json" "$SQLSERVER_P95_MS" "$SQLSERVER_P99_MS" || failed=1

if [[ "$failed" -ne 0 ]]; then
  echo "Latency gate failed. See $report_path" | tee -a "$report_path"
  exit 1
fi

echo "Latency gate passed. See $report_path" | tee -a "$report_path"
