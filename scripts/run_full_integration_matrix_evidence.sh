#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for integration matrix evidence runs" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for release evidence artifact validation" >&2
  exit 1
fi

export CDC_RS_RUN_DOCKER_TESTS=1
report_path="target/integration-full-matrix-evidence.txt"
mkdir -p target
: > "$report_path"

failed_labels=()
sqlserver_retry_attempts="${SQLSERVER_MATRIX_RETRY_ATTEMPTS:-3}"
sqlserver_retry_delay_secs="${SQLSERVER_MATRIX_RETRY_DELAY_SECS:-2}"
mysql_process_crash_retry_attempts="${MYSQL_PROCESS_CRASH_RETRY_ATTEMPTS:-2}"
mysql_process_crash_retry_delay_secs="${MYSQL_PROCESS_CRASH_RETRY_DELAY_SECS:-2}"
postgres_process_crash_retry_attempts="${POSTGRES_PROCESS_CRASH_RETRY_ATTEMPTS:-2}"
postgres_process_crash_retry_delay_secs="${POSTGRES_PROCESS_CRASH_RETRY_DELAY_SECS:-2}"

is_transient_sqlserver_failure() {
  local log_file="$1"
  rg -qi "deadlock victim|code:\s*1205|timed out waiting for sql server|timeout.*sqlserver|temporar(y|ily) unavailable" "$log_file"
}

is_transient_mysql_process_crash_failure() {
  local log_file="$1"
  rg -qi "container is not ready|io_setup\(\) failed with EAGAIN|Cannot initialize AIO sub-system|timed out waiting for crash worker marker" "$log_file"
}

is_transient_postgres_process_crash_failure() {
  local log_file="$1"
  rg -qi "container is not ready|database system is starting up|could not connect to server|connection refused|timed out waiting for crash worker marker" "$log_file"
}

require_file() {
  local file="$1"
  if [[ ! -f "$file" ]]; then
    echo "FAIL: required artifact missing: $file" | tee -a "$report_path"
    exit 1
  fi
  if [[ ! -s "$file" ]]; then
    echo "FAIL: required artifact is empty: $file" | tee -a "$report_path"
    exit 1
  fi
}

check_latency_json() {
  local file="$1"
  require_file "$file"
  if ! jq -e '
    has("poll_latency_ms_p95") and
    has("poll_latency_ms_p99") and
    has("commit_latency_ms_p95") and
    has("commit_latency_ms_p99") and
    (.poll_latency_ms_p95 | type == "number") and
    (.poll_latency_ms_p99 | type == "number") and
    (.commit_latency_ms_p95 | type == "number") and
    (.commit_latency_ms_p99 | type == "number")
  ' "$file" >/dev/null; then
    echo "FAIL: malformed latency JSON artifact: $file" | tee -a "$report_path"
    exit 1
  fi
}

validate_release_artifacts() {
  require_file "target/integration-full-matrix-evidence.txt"
  require_file "target/latency-evidence.txt"
  require_file "target/latency-gate.txt"
  require_file "target/benchmark-ci-env.txt"
  require_file "BENCHMARK_REPORT.md"

  check_latency_json "target/postgres-latency-evidence.json"
  check_latency_json "target/mysql-latency-evidence.json"
  check_latency_json "target/sqlserver-latency-evidence.json"
}

run_case() {
  local label="$1"
  shift

  echo "==> $label" | tee -a "$report_path"
  local attempts=1
  local max_attempts=1
  if [[ "$label" == "sqlserver snapshot" ]]; then
    max_attempts="$sqlserver_retry_attempts"
  elif [[ "$label" == "postgres process crash" ]]; then
    max_attempts="$postgres_process_crash_retry_attempts"
  elif [[ "$label" == "mysql process crash" ]]; then
    max_attempts="$mysql_process_crash_retry_attempts"
  fi

  local passed=0
  local transient=0
  local temp_log
  temp_log="$(mktemp)"
  while ((attempts <= max_attempts)); do
    : > "$temp_log"
    if "$@" >"$temp_log" 2>&1; then
      cat "$temp_log" | tee -a "$report_path"
      if [[ "${1:-}" == "cargo" && "${2:-}" == "test" ]] && rg -q '^running 0 tests$' "$temp_log"; then
        echo "STATUS: FAIL - $label (zero-test pass detected)" | tee -a "$report_path"
        break
      fi
      echo "STATUS: PASS - $label (attempt ${attempts}/${max_attempts})" | tee -a "$report_path"
      passed=1
      break
    fi

    cat "$temp_log" | tee -a "$report_path"

    if ((attempts < max_attempts)); then
      if [[ "$label" == "sqlserver snapshot" ]] && is_transient_sqlserver_failure "$temp_log"; then
        transient=1
        echo "Transient SQL Server failure detected for '$label'; retrying in ${sqlserver_retry_delay_secs}s (attempt ${attempts}/${max_attempts})" | tee -a "$report_path"
        sleep "$sqlserver_retry_delay_secs"
        ((attempts++))
        continue
      fi
      if [[ "$label" == "postgres process crash" ]] && is_transient_postgres_process_crash_failure "$temp_log"; then
        transient=1
        echo "Transient PostgreSQL process-crash failure detected for '$label'; retrying in ${postgres_process_crash_retry_delay_secs}s (attempt ${attempts}/${max_attempts})" | tee -a "$report_path"
        sleep "$postgres_process_crash_retry_delay_secs"
        ((attempts++))
        continue
      fi
      if [[ "$label" == "mysql process crash" ]] && is_transient_mysql_process_crash_failure "$temp_log"; then
        transient=1
        echo "Transient MySQL process-crash failure detected for '$label'; retrying in ${mysql_process_crash_retry_delay_secs}s (attempt ${attempts}/${max_attempts})" | tee -a "$report_path"
        sleep "$mysql_process_crash_retry_delay_secs"
        ((attempts++))
        continue
      fi
    fi

    break
  done

  rm -f "$temp_log"

  if ((passed == 0)); then
    if ((transient == 1)); then
      echo "STATUS: FAIL - $label (after transient retries)" | tee -a "$report_path"
    else
      echo "STATUS: FAIL - $label" | tee -a "$report_path"
    fi
    failed_labels+=("$label")
  fi
  echo | tee -a "$report_path"
}

run_test_case() {
  local label="$1"
  local test_target="$2"
  local features="$3"
  run_case "$label" cargo test --test "$test_target" --features "$features"
}

run_xtask_worker_build_case() {
  local label="$1"
  local worker_bin="$2"
  local features="$3"
  run_case "$label" cargo build -p xtask --bin "$worker_bin" --features "$features"
}

postgres_suites=(
  "postgres runtime|runtime_postgres|postgres"
  "postgres version matrix|postgres_version_matrix|postgres"
  "postgres snapshot|postgres_snapshot_integration|postgres"
  "postgres stream|postgres_stream_integration|postgres"
  "postgres handoff|postgres_handoff_integration|postgres"
  "postgres checkpoint|checkpoint_file_integration|postgres"
  "postgres process crash|runtime_postgres_process_crash_integration|postgres"
  "postgres parallel snapshot stress|parallel_snapshot_stress_integration|postgres"
  "postgres otel metrics|otel_metrics_integration|postgres,metrics"
  "postgres otel tracing|otel_tracing_integration|postgres,metrics"
  "postgres snapshot resumable|snapshot_resumable_integration|postgres"
)

mysql_suites=(
  "mysql connection|mysql_connection_integration|mysql"
  "mysql snapshot|mysql_snapshot_integration|mysql"
  "mysql stream|mysql_stream_integration|mysql"
  "mysql handoff|mysql_handoff_integration|mysql"
  "mysql process crash|runtime_mysql_process_crash_integration|mysql"
)

mariadb_suites=(
  "mariadb connection|mariadb_connection_integration|mariadb"
  "mariadb e2e|mariadb_e2e_integration|mariadb"
)

sqlserver_suites=(
  "sqlserver version matrix|sqlserver_version_matrix|sqlserver"
  "sqlserver snapshot|sqlserver_snapshot_integration|sqlserver"
  "sqlserver stream|sqlserver_stream_integration|sqlserver"
  "sqlserver handoff|sqlserver_handoff_integration|sqlserver"
  "sqlserver process crash|runtime_sqlserver_process_crash_integration|sqlserver"
)

reliability_suites=(
  "reliability data loss detection|data_loss_detection|postgres,test-harnesses"
  "reliability deterministic replay failure fixtures|deterministic_replay_failure_fixtures|postgres,test-harnesses"
  "reliability deterministic replay golden fixtures|deterministic_replay_golden_fixtures|postgres,test-harnesses"
  "reliability runtime health states|runtime_health_states|postgres,test-harnesses"
  "reliability fault injection soak matrix|fault_injection_soak_matrix|postgres,test-harnesses"
  "reliability wasm runtime integration|wasm_runtime_integration|postgres,test-harnesses"
  "reliability wasm conformance contract|wasm_conformance_contract|postgres,test-harnesses"
)

for entry in "${postgres_suites[@]}"; do
  IFS='|' read -r label test_target features <<< "$entry"
  if [[ "$test_target" == "runtime_postgres_process_crash_integration" ]]; then
    run_xtask_worker_build_case "postgres crash worker build" "postgres_crash_worker" "postgres"
  fi
  run_test_case "$label" "$test_target" "$features"
done

for entry in "${mysql_suites[@]}"; do
  IFS='|' read -r label test_target features <<< "$entry"
  if [[ "$test_target" == "runtime_mysql_process_crash_integration" ]]; then
    run_xtask_worker_build_case "mysql crash worker build" "mysql_crash_worker" "mysql"
  fi
  run_test_case "$label" "$test_target" "$features"
done

for entry in "${mariadb_suites[@]}"; do
  IFS='|' read -r label test_target features <<< "$entry"
  run_test_case "$label" "$test_target" "$features"
done

for entry in "${sqlserver_suites[@]}"; do
  IFS='|' read -r label test_target features <<< "$entry"
  if [[ "$test_target" == "runtime_sqlserver_process_crash_integration" ]]; then
    run_xtask_worker_build_case "sqlserver crash worker build" "sqlserver_crash_worker" "sqlserver"
  fi
  run_test_case "$label" "$test_target" "$features"
done

for entry in "${reliability_suites[@]}"; do
  IFS='|' read -r label test_target features <<< "$entry"
  run_test_case "$label" "$test_target" "$features"
done

run_case "latency evidence gate" bash scripts/ci-latency-gate.sh

if ((${#failed_labels[@]} > 0)); then
  echo "Full integration matrix completed with failures." | tee -a "$report_path"
  echo "Failed suites:" | tee -a "$report_path"
  for label in "${failed_labels[@]}"; do
    echo "  - $label" | tee -a "$report_path"
  done
  echo "Report written to $report_path"
  exit 1
fi

validate_release_artifacts

echo "Full integration matrix completed successfully." | tee -a "$report_path"
echo "Report written to $report_path"
