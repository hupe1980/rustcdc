#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for integration matrix evidence runs" >&2
  exit 1
fi

export CDC_RS_RUN_DOCKER_TESTS=1
export CDC_RS_ALLOW_INSECURE_TEST_TRANSPORT=1

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

run_case "postgres runtime" cargo test --test runtime_postgres --features postgres
run_case "postgres version matrix" cargo test --test postgres_version_matrix --features postgres
run_case "postgres snapshot" cargo test --test postgres_snapshot_integration --features postgres
run_case "postgres stream" cargo test --test postgres_stream_integration --features postgres
run_case "postgres handoff" cargo test --test postgres_handoff_integration --features postgres
run_case "postgres checkpoint" cargo test --test postgres_checkpoint_integration --features postgres
run_case "postgres crash worker build" cargo build -p xtask --bin postgres_crash_worker --features postgres
run_case "postgres process crash" cargo test --test runtime_postgres_process_crash_integration --features postgres
run_case "postgres parallel snapshot stress" cargo test --test parallel_snapshot_stress_integration --features postgres
run_case "postgres otel metrics" cargo test --test otel_metrics_integration --features postgres,metrics
run_case "postgres otel tracing" cargo test --test otel_tracing_integration --features postgres,metrics
run_case "postgres snapshot resumable" cargo test --test snapshot_resumable_integration --features postgres
run_case "mysql connection" cargo test --test mysql_connection_integration --features mysql,tls,insecure-test-overrides
run_case "mysql snapshot" cargo test --test mysql_snapshot_integration --features mysql,tls,insecure-test-overrides
run_case "mysql stream" cargo test --test mysql_stream_integration --features mysql,tls,insecure-test-overrides
run_case "mysql handoff" cargo test --test mysql_handoff_integration --features mysql,tls,insecure-test-overrides
run_case "mysql crash worker build" cargo build -p xtask --bin mysql_crash_worker --features mysql,tls,insecure-test-overrides
run_case "mysql process crash" cargo test --test runtime_mysql_process_crash_integration --features mysql,tls,insecure-test-overrides
run_case "mariadb connection" cargo test --test mariadb_connection_integration --features mysql,tls,insecure-test-overrides
run_case "mariadb e2e" cargo test --test mariadb_e2e_integration --features mysql,tls,insecure-test-overrides
run_case "sqlserver version matrix" cargo test --test sqlserver_version_matrix --features sqlserver,tls,insecure-test-overrides
run_case "sqlserver snapshot" cargo test --test sqlserver_snapshot_integration --features sqlserver,tls,insecure-test-overrides
run_case "sqlserver stream" cargo test --test sqlserver_stream_integration --features sqlserver,tls,insecure-test-overrides
run_case "sqlserver handoff" cargo test --test sqlserver_handoff_integration --features sqlserver,tls,insecure-test-overrides
run_case "sqlserver crash worker build" cargo build -p xtask --bin sqlserver_crash_worker --features sqlserver,tls,insecure-test-overrides
run_case "sqlserver process crash" cargo test --test runtime_sqlserver_process_crash_integration --features sqlserver,tls,insecure-test-overrides

if ((${#failed_labels[@]} > 0)); then
  echo "Full integration matrix completed with failures." | tee -a "$report_path"
  echo "Failed suites:" | tee -a "$report_path"
  for label in "${failed_labels[@]}"; do
    echo "  - $label" | tee -a "$report_path"
  done
  echo "Report written to $report_path"
  exit 1
fi

echo "Full integration matrix completed successfully." | tee -a "$report_path"
echo "Report written to $report_path"
