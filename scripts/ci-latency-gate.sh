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

# Global defaults are intentionally conservative to avoid flake-driven noise.
DEFAULT_P95_MS="${LATENCY_GATE_DEFAULT_P95_MS:-500}"
DEFAULT_P99_MS="${LATENCY_GATE_DEFAULT_P99_MS:-1000}"

POSTGRES_P95_MS="${LATENCY_GATE_POSTGRES_P95_MS:-$DEFAULT_P95_MS}"
POSTGRES_P99_MS="${LATENCY_GATE_POSTGRES_P99_MS:-$DEFAULT_P99_MS}"
MYSQL_P95_MS="${LATENCY_GATE_MYSQL_P95_MS:-$DEFAULT_P95_MS}"
MYSQL_P99_MS="${LATENCY_GATE_MYSQL_P99_MS:-$DEFAULT_P99_MS}"
SQLSERVER_P95_MS="${LATENCY_GATE_SQLSERVER_P95_MS:-$DEFAULT_P95_MS}"
SQLSERVER_P99_MS="${LATENCY_GATE_SQLSERVER_P99_MS:-$DEFAULT_P99_MS}"

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

gate_file() {
  local connector="$1"
  local file="$2"
  local p95_limit="$3"
  local p99_limit="$4"

  if [[ ! -f "$file" ]]; then
    echo "FAIL: missing latency artifact for ${connector}: ${file}" | tee -a "$report_path"
    return 1
  fi

  local poll_p95 commit_p95 poll_p99 commit_p99
  poll_p95="$(jq -r '.poll_latency_ms_p95' "$file")"
  commit_p95="$(jq -r '.commit_latency_ms_p95' "$file")"
  poll_p99="$(jq -r '.poll_latency_ms_p99' "$file")"
  commit_p99="$(jq -r '.commit_latency_ms_p99' "$file")"

  assert_le "${connector}.poll.p95_ms" "$poll_p95" "$p95_limit"
  assert_le "${connector}.commit.p95_ms" "$commit_p95" "$p95_limit"
  assert_le "${connector}.poll.p99_ms" "$poll_p99" "$p99_limit"
  assert_le "${connector}.commit.p99_ms" "$commit_p99" "$p99_limit"
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
