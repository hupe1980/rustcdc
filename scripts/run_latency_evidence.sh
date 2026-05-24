#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for latency evidence runs" >&2
  exit 1
fi

export CDC_RS_RUN_DOCKER_TESTS=1

report_path="target/latency-evidence.txt"
mkdir -p target
: > "$report_path"

run_step() {
  local label="$1"
  shift
  echo "==> $label" | tee -a "$report_path"
  "$@" 2>&1 | tee -a "$report_path"
  echo | tee -a "$report_path"
}

run_step "postgres connector latency evidence" \
  cargo test --test postgres_latency_evidence --features postgres -- --nocapture

run_step "mysql connector latency evidence" \
  cargo test --test mysql_latency_evidence --features mysql -- --nocapture

run_step "sqlserver connector latency evidence" \
  cargo test --test sqlserver_latency_evidence --features sqlserver -- --nocapture

for artifact in target/postgres-latency-evidence.md target/mysql-latency-evidence.md target/sqlserver-latency-evidence.md; do
  if [[ -f "$artifact" ]]; then
    {
      echo "==> $(basename "$artifact")"
      cat "$artifact"
      echo
    } | tee -a "$report_path"
  fi
done

echo "Latency evidence run completed successfully." | tee -a "$report_path"
echo "Report written to $report_path"
