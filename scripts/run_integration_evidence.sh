#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for integration evidence runs" >&2
  exit 1
fi

export CDC_RS_RUN_DOCKER_TESTS=1

report_path="target/integration-evidence.txt"
mkdir -p target
: > "$report_path"

run_suite() {
  local label="$1"
  shift
  echo "==> $label" | tee -a "$report_path"
  "$@" 2>&1 | tee -a "$report_path"
  echo | tee -a "$report_path"
}

run_suite "postgres snapshot integration" cargo test --test postgres_snapshot_integration --features postgres
run_suite "mysql snapshot integration" cargo test --test mysql_snapshot_integration --features mysql
run_suite "sqlserver snapshot integration" cargo test --test sqlserver_snapshot_integration --features sqlserver
run_suite "mariadb connection integration" cargo test --test mariadb_connection_integration --features mysql

echo "Integration evidence run completed successfully." | tee -a "$report_path"
echo "Report written to $report_path"
