#!/usr/bin/env bash
set -euo pipefail

# Local CI preflight runner.
# - Mirrors the workflow command matrix for check/quality jobs.
# - Integration suites are optional, but when enabled they use matrix-equivalent
#   connector feature wiring by default.

WITH_INTEGRATION=0
WITH_FULL_MATRIX=0

print_integration_skip_warning() {
  cat <<'WARN'

WARNING: Docker-backed integration suites were skipped.
This run does not validate connector-backed runtime/integration behavior.

Skipped release-gate surfaces include:
  - postgres: runtime/version/snapshot/stream/handoff suites
  - mysql: connection/snapshot/stream/handoff suites
  - docker-gated process-crash suites
  - latency evidence gate

To run matrix-equivalent Docker integration validation locally:
  ./scripts/ci-preflight.sh --with-integration

To run full release-gate validation locally:
  ./scripts/ci-preflight.sh --with-full-matrix
WARN
}

for arg in "$@"; do
  case "$arg" in
    --with-integration)
      WITH_INTEGRATION=1
      ;;
    --with-full-matrix)
      WITH_INTEGRATION=1
      WITH_FULL_MATRIX=1
      ;;
    -h|--help)
      cat <<'USAGE'
Usage: ./scripts/ci-preflight.sh [--with-integration] [--with-full-matrix]

Options:
  --with-integration   Run matrix-equivalent Docker-backed integration suites.
  --with-full-matrix   Run full release-gate integration evidence matrix.
  -h, --help           Show this help text.
USAGE
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      echo "Try --help" >&2
      exit 2
      ;;
  esac
done

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required" >&2
  exit 1
fi

run_step() {
  local label="$1"
  shift
  echo
  echo ">>> $label"
  "$@"
}

has_toolchain() {
  local toolchain="$1"
  rustup toolchain list 2>/dev/null | awk '{print $1}' | grep -Eq "^${toolchain}(-|$)"
}

TOOLCHAINS=()
for tc in stable nightly; do
  if has_toolchain "$tc"; then
    TOOLCHAINS+=("$tc")
  else
    echo "warning: rust toolchain '$tc' not installed; skipping"
  fi
done

if [[ ${#TOOLCHAINS[@]} -eq 0 ]]; then
  echo "No configured stable/nightly toolchains found; using default cargo toolchain only"
  TOOLCHAINS=("")
fi

PROFILE_NAMES=(
  "no-default-features"
  "default-features"
  "all-features"
)
PROFILE_ARGS=(
  "--no-default-features"
  "--features default"
  "--all-features"
)

for tc in "${TOOLCHAINS[@]}"; do
  if [[ -n "$tc" ]]; then
    CARGO=(cargo "+$tc")
    TOOLCHAIN_LABEL="$tc"
  else
    CARGO=(cargo)
    TOOLCHAIN_LABEL="default"
  fi

  for i in "${!PROFILE_NAMES[@]}"; do
    name="${PROFILE_NAMES[$i]}"
    args="${PROFILE_ARGS[$i]}"

    # shellcheck disable=SC2206
    split_args=($args)
    run_step "cargo check ($TOOLCHAIN_LABEL / $name)" "${CARGO[@]}" check "${split_args[@]}"
    run_step "cargo test --lib ($TOOLCHAIN_LABEL / $name)" "${CARGO[@]}" test --lib "${split_args[@]}"
  done
done

run_step "cargo fmt --check" cargo fmt --check
run_step "cargo check --all-targets --all-features (warnings as errors)" env RUSTFLAGS="-D warnings" cargo check --all-targets --all-features
run_step "cargo clippy --all-targets --all-features -- -D warnings" cargo clippy --all-targets --all-features -- -D warnings
run_step "cargo doc --no-deps" cargo doc --no-deps
run_step "markdown link check" bash scripts/ci-doc-links.sh
run_step "cargo build --example pg_to_stdout --features postgres" cargo build --example pg_to_stdout --features postgres

if [[ "$WITH_INTEGRATION" -eq 1 ]]; then
  export CDC_RS_RUN_DOCKER_TESTS=1
  export CDC_RS_ALLOW_INSECURE_TEST_TRANSPORT=1

  for tc in "${TOOLCHAINS[@]}"; do
    if [[ -n "$tc" ]]; then
      CARGO=(cargo "+$tc")
      TOOLCHAIN_LABEL="$tc"
    else
      CARGO=(cargo)
      TOOLCHAIN_LABEL="default"
    fi

    for suite in runtime_postgres postgres_version_matrix postgres_snapshot_integration postgres_stream_integration postgres_handoff_integration; do
      run_step "integration postgres ($TOOLCHAIN_LABEL / $suite)" "${CARGO[@]}" test --test "$suite" --features postgres
    done

    for suite in mysql_connection_integration mysql_snapshot_integration mysql_stream_integration mysql_handoff_integration; do
      run_step "integration mysql ($TOOLCHAIN_LABEL / $suite)" "${CARGO[@]}" test --test "$suite" --features mysql,insecure-test-overrides
    done

    for suite in mariadb_connection_integration mariadb_e2e_integration; do
      run_step "integration mariadb ($TOOLCHAIN_LABEL / $suite)" "${CARGO[@]}" test --test "$suite" --features mariadb,insecure-test-overrides
    done

    for suite in sqlserver_version_matrix sqlserver_snapshot_integration sqlserver_stream_integration sqlserver_handoff_integration; do
      run_step "integration sqlserver ($TOOLCHAIN_LABEL / $suite)" "${CARGO[@]}" test --test "$suite" --features sqlserver,insecure-test-overrides
    done
  done

  run_step "latency evidence gate (p95/p99)" bash scripts/ci-latency-gate.sh

  if [[ "$WITH_FULL_MATRIX" -eq 1 ]]; then
    run_step "full integration matrix evidence" bash scripts/run_full_integration_matrix_evidence.sh
  else
    echo
    echo "note: --with-integration now uses matrix-equivalent connector feature wiring; use --with-full-matrix for crash-worker + extended evidence lanes"
  fi
else
  print_integration_skip_warning
fi

echo
echo "Preflight completed successfully."
