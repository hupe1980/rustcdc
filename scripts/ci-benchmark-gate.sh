#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

raw_out="target/benchmark-ci-gate.txt"
retry_out="target/benchmark-ci-gate-retry.txt"
regression_threshold="${BENCHMARK_MAX_REGRESSION_PERCENT:-5}"
strict_mode="${BENCHMARK_STRICT:-1}"
gated_benches_raw="${BENCHMARK_GATED_BENCHES:-quality_perf cdc_perf}"
critical_group_prefixes_raw="${BENCHMARK_CRITICAL_GROUP_PREFIXES:-quality_gates wasm_transform}"
allow_non_strict="${ALLOW_BENCHMARK_NON_STRICT:-0}"
bench_sample_size="${BENCHMARK_SAMPLE_SIZE:-30}"
bench_measurement_secs="${BENCHMARK_MEASUREMENT_SECS:-8}"
bench_warm_up_secs="${BENCHMARK_WARMUP_SECS:-3}"
confirm_runs="${BENCHMARK_CONFIRM_RUNS:-2}"
preheat_runs="${BENCHMARK_PREHEAT_RUNS:-1}"
baseline_commit="${BENCHMARK_BASELINE_COMMIT:-}"
baseline_artifact="${BENCHMARK_BASELINE_ARTIFACT:-}"
benchmark_require_historical_baseline="${BENCHMARK_REQUIRE_HISTORICAL_BASELINE:-1}"
benchmark_enforce_release_policy="${BENCHMARK_ENFORCE_RELEASE_POLICY:-0}"
benchmark_validate_policy_only="${BENCHMARK_VALIDATE_POLICY_ONLY:-0}"
benchmark_fallback_benches=""
# CRITERION_BASELINE: name of a saved Criterion baseline to compare against
# (stored under target/criterion/<bench>/<name>/). If unset, Criterion compares
# against the previous run. Set this in CI to a named baseline pinned to a
# reference commit to get stable regression detection across machines.
#
# BENCHMARK_SAVE_BASELINE: if set, saves the current run as a named Criterion
# baseline after measuring. Use this to update the CI reference baseline:
#   BENCHMARK_SAVE_BASELINE=ci-baseline ./scripts/ci-benchmark-gate.sh

current_commit="$(git rev-parse HEAD 2>/dev/null || true)"
git_tree_state="unknown"
sanitized_kernel="$(uname -srm)"
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if git diff --quiet --ignore-submodules HEAD --; then
    git_tree_state="clean"
  else
    git_tree_state="dirty"
  fi
fi

contains_bench_target() {
  local wanted="$1"
  shift
  local bench
  for bench in "$@"; do
    if [[ "$bench" == "$wanted" ]]; then
      return 0
    fi
  done
  return 1
}

benchmark_policy_status() {
  local status="release-gate-eligible"
  local reason="strict benchmark policy satisfied"

  if [[ "$strict_mode" != "1" ]]; then
    status="non-release-local"
    reason="strict_mode must be 1"
  elif awk -v val="$regression_threshold" 'BEGIN { exit (val + 0 <= 5 ? 0 : 1) }'; then
    true
  else
    status="non-release-local"
    reason="threshold_percent must be <= 5"
  fi

  if [[ "$status" == "release-gate-eligible" ]]; then
    if ! contains_bench_target "quality_perf" "${gated_benches[@]}"; then
      status="non-release-local"
      reason="gated_benches must include quality_perf"
    elif ! contains_bench_target "cdc_perf" "${gated_benches[@]}"; then
      status="non-release-local"
      reason="gated_benches must include cdc_perf"
    elif [[ -z "$current_commit" ]]; then
      status="non-release-local"
      reason="current git commit must be available"
    elif [[ -z "$baseline_commit" ]]; then
      status="non-release-local"
      reason="baseline_commit must be set"
    elif [[ "$benchmark_require_historical_baseline" == "1" && "$baseline_commit" == "$current_commit" ]]; then
      status="non-release-local"
      reason="baseline_commit must reference a prior commit"
    elif [[ "$benchmark_require_historical_baseline" == "1" ]] && ! git merge-base --is-ancestor "$baseline_commit" "$current_commit" >/dev/null 2>&1; then
      status="non-release-local"
      reason="baseline_commit must be an ancestor of current commit"
    elif [[ -z "$baseline_artifact" ]]; then
      status="non-release-local"
      reason="baseline_artifact must be set"
    elif [[ "$benchmark_require_historical_baseline" == "1" && "$baseline_artifact" == "BENCHMARK_REPORT.md" ]]; then
      status="non-release-local"
      reason="baseline_artifact must reference immutable baseline evidence"
    elif [[ "$git_tree_state" != "clean" ]]; then
      status="non-release-local"
      reason="git tree must be clean for release baseline evidence"
    fi
  fi

  echo "$status|$reason"
}

if [[ "$strict_mode" != "1" && "$allow_non_strict" != "1" ]]; then
  echo "Refusing non-strict benchmark gate. Set ALLOW_BENCHMARK_NON_STRICT=1 to override intentionally."
  exit 1
fi

IFS=' ' read -r -a gated_benches <<< "$gated_benches_raw"
IFS=' ' read -r -a critical_group_prefixes <<< "$critical_group_prefixes_raw"

if [[ "${#gated_benches[@]}" -eq 0 ]]; then
  echo "No benchmark targets configured for gate."
  exit 1
fi

if [[ "${#critical_group_prefixes[@]}" -eq 0 ]]; then
  echo "No critical benchmark group prefixes configured for gate."
  exit 1
fi

mkdir -p target

policy_info="$(benchmark_policy_status)"
policy_status="${policy_info%%|*}"
policy_reason="${policy_info#*|}"

{
  echo "timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "kernel=${sanitized_kernel}"
  echo "git_commit=${current_commit:-unknown}"
  echo "git_tree_state=${git_tree_state}"
  echo "baseline_commit=${baseline_commit}"
  echo "baseline_artifact=${baseline_artifact}"
  echo "require_historical_baseline=${benchmark_require_historical_baseline}"
  echo "enforce_release_policy=${benchmark_enforce_release_policy}"
  echo "rustc=$(rustc -Vv | tr '\n' ';')"
  echo "threshold_percent=${regression_threshold}"
  echo "strict_mode=${strict_mode}"
  echo "gated_benches=${gated_benches_raw}"
  echo "critical_group_prefixes=${critical_group_prefixes_raw}"
  echo "sample_size=${bench_sample_size}"
  echo "measurement_secs=${bench_measurement_secs}"
  echo "warmup_secs=${bench_warm_up_secs}"
  echo "confirm_runs=${confirm_runs}"
  echo "preheat_runs=${preheat_runs}"
  echo "report_classification=${policy_status}"
  echo "report_classification_reason=${policy_reason}"
} > target/benchmark-ci-env.txt

if [[ "$benchmark_enforce_release_policy" == "1" && "$policy_status" != "release-gate-eligible" ]]; then
  echo "Benchmark policy gate failed: ${policy_reason}" >&2
  echo "Set BENCHMARK_ENFORCE_RELEASE_POLICY=0 only for non-release/local exploratory runs." >&2
  exit 1
fi

if [[ "$benchmark_validate_policy_only" == "1" ]]; then
  echo "Benchmark policy preflight passed: ${policy_status} (${policy_reason})"
  exit 0
fi

emit_benchmark_report() {
  local report_path="BENCHMARK_REPORT.md"

  {
    echo "# Benchmark Report"
    echo
    if [[ "$policy_status" != "release-gate-eligible" ]]; then
      echo "> WARNING: This report is ${policy_status}. Do not treat it as release baseline evidence."
      echo "> Reason: ${policy_reason}."
      echo
    fi
    if [[ -n "$benchmark_fallback_benches" ]]; then
      echo "> NOTICE: Criterion baseline auto-bootstrap fallback was used for: ${benchmark_fallback_benches}."
      echo
    fi
    echo "Generated: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo
    echo "## Environment"
    echo
    echo '```text'
    echo "${sanitized_kernel}"
    echo "git_commit=${current_commit:-unknown}"
    echo "git_tree_state=${git_tree_state}"
    echo "baseline_commit=${baseline_commit}"
    echo "baseline_artifact=${baseline_artifact}"
    echo "require_historical_baseline=${benchmark_require_historical_baseline}"
    echo "enforce_release_policy=${benchmark_enforce_release_policy}"
    rustc -Vv
    echo "gated_benches=${gated_benches_raw}"
    echo "critical_group_prefixes=${critical_group_prefixes_raw}"
    echo "regression_threshold_percent=${regression_threshold}"
    echo "strict_mode=${strict_mode}"
    echo "sample_size=${bench_sample_size}"
    echo "measurement_secs=${bench_measurement_secs}"
    echo "warmup_secs=${bench_warm_up_secs}"
    echo "confirm_runs=${confirm_runs}"
    echo "preheat_runs=${preheat_runs}"
    echo "report_classification=${policy_status}"
    echo "report_classification_reason=${policy_reason}"
    echo '```'
    echo
    echo "## Benchmarks"
  } > "$report_path"

  for bench in "${gated_benches[@]}"; do
    local bench_raw_out="target/benchmark-ci-gate-${bench}.txt"
    local bench_retry_out="target/benchmark-ci-gate-retry-${bench}.txt"

    {
      echo
      echo "### ${bench}"
      echo
      echo "#### Gate Run Output"
      echo
      echo '```text'
      if [[ -f "$bench_raw_out" ]]; then
        cat "$bench_raw_out"
      else
        echo "missing gate output: $bench_raw_out"
      fi
      echo '```'
    } >> "$report_path"

    if [[ -f "$bench_retry_out" ]]; then
      {
        echo
        echo "#### Retry Output"
        echo
        echo '```text'
        cat "$bench_retry_out"
        echo '```'
      } >> "$report_path"
    fi

    local preheat_file
    local preheat_index=1
    while true; do
      preheat_file="target/benchmark-ci-gate-preheat-${bench}-${preheat_index}.txt"
      if [[ ! -f "$preheat_file" ]]; then
        break
      fi
      {
        echo
        echo "#### Preheat Run ${preheat_index} Output"
        echo
        echo '```text'
        cat "$preheat_file"
        echo '```'
      } >> "$report_path"
      preheat_index=$((preheat_index + 1))
    done

    local confirm_file
    local run_index=1
    while true; do
      confirm_file="target/benchmark-ci-gate-confirm-${bench}-${run_index}.txt"
      if [[ ! -f "$confirm_file" ]]; then
        break
      fi
      {
        echo
        echo "#### Confirmation Run ${run_index} Output"
        echo
        echo '```text'
        cat "$confirm_file"
        echo '```'
      } >> "$report_path"
      run_index=$((run_index + 1))
    done
  done

  echo "Benchmark report written to $report_path"
}

record_baseline_fallback_bench() {
  local bench_name="$1"
  if [[ -z "$benchmark_fallback_benches" ]]; then
    benchmark_fallback_benches="$bench_name"
    return
  fi

  case ",$benchmark_fallback_benches," in
    *",$bench_name,"*)
      ;;
    *)
      benchmark_fallback_benches+=" $bench_name"
      ;;
  esac
}

run_bench() {
  local bench_name="$1"
  local out_file="$2"
  local mode="${3:-compare}"
  local allow_missing_baseline_retry="${4:-1}"
  local baseline_args=()
  local cargo_args=(
    cargo bench --bench "$bench_name" --
    --sample-size "$bench_sample_size"
    --measurement-time "$bench_measurement_secs"
    --warm-up-time "$bench_warm_up_secs"
  )
  # When CRITERION_BASELINE is set, compare against a saved Criterion baseline
  # instead of the previous run. Use BENCHMARK_SAVE_BASELINE to persist a new
  # named baseline (e.g. for updating the CI reference point).
  if [[ "$mode" == "compare" && -n "${CRITERION_BASELINE:-}" ]]; then
    baseline_args=(--baseline "$CRITERION_BASELINE")
  fi
  if [[ "$mode" == "save" && -n "${CRITERION_BASELINE:-}" ]]; then
    baseline_args+=(--save-baseline "$CRITERION_BASELINE")
  elif [[ -n "${BENCHMARK_SAVE_BASELINE:-}" ]]; then
    baseline_args+=(--save-baseline "$BENCHMARK_SAVE_BASELINE")
  fi
  if (( ${#baseline_args[@]} > 0 )); then
    cargo_args+=("${baseline_args[@]}")
  fi

  set +e
  "${cargo_args[@]}" 2>&1 | tee "$out_file"
  local bench_exit_code=${PIPESTATUS[0]}
  set -e

  if [[ "$bench_exit_code" -eq 0 ]]; then
    return 0
  fi

  if [[ "$mode" == "compare" && "$allow_missing_baseline_retry" == "1" && -n "${CRITERION_BASELINE:-}" ]] \
    && rg -q "Baseline '.*' must exist before comparison is allowed" "$out_file"; then
    local bootstrap_out="target/benchmark-ci-gate-bootstrap-${bench_name}.txt"
    record_baseline_fallback_bench "$bench_name"
    echo "Criterion baseline entries missing for '${bench_name}' under '${CRITERION_BASELINE}'. Bootstrapping and retrying compare..."
    run_bench "$bench_name" "$bootstrap_out" save 0
    run_bench "$bench_name" "$out_file" compare 0
    return 0
  fi

  return "$bench_exit_code"
}

criterion_named_baseline_exists() {
  local baseline_name="$1"
  [[ -n "$baseline_name" ]] || return 1
  [[ -d target/criterion ]] || return 1

  find target/criterion -path "*/${baseline_name}/estimates.json" -print -quit | grep -q .
}

ensure_named_baseline() {
  local bench_name="$1"
  local baseline_name="${CRITERION_BASELINE:-}"

  [[ -n "$baseline_name" ]] || return 0

  if criterion_named_baseline_exists "$baseline_name"; then
    return 0
  fi

  local bootstrap_out="target/benchmark-ci-gate-bootstrap-${bench_name}.txt"
  echo "Bootstrapping missing Criterion baseline '${baseline_name}' for ${bench_name}..."
  run_bench "$bench_name" "$bootstrap_out" save

  if ! criterion_named_baseline_exists "$baseline_name"; then
    echo "Benchmark baseline bootstrap failed: Criterion baseline '${baseline_name}' was not created for ${bench_name}." >&2
    exit 1
  fi
}

run_preheat() {
  local bench_name="$1"
  local run_index="$2"
  local out_file="target/benchmark-ci-gate-preheat-${bench_name}-${run_index}.txt"
  echo "Running preheat benchmark pass ${run_index} for ${bench_name} to reduce first-run jitter..."
  cargo bench --bench "$bench_name" -- --sample-size 10 --measurement-time 2 --warm-up-time 1 > "$out_file"
}

collect_regressions() {
  local in_file="$1"
  awk '
    /^[^[:space:]].*/ {
      bench = $1
    }
    /(time|change):[[:space:]]+\[\+/ {
      line = $0
      sub(/^.*\[/, "", line)
      sub(/\].*$/, "", line)
      count = split(line, parts, " ")
      upper = parts[count]
      gsub(/\+|%/, "", upper)
      next
    }
    /Performance has regressed\./ {
      if (upper == "") {
        upper = "999"
      }
      printf "%s|%s\n", bench, upper
    }
  ' "$in_file"
}

has_significant_regression() {
  local in_file="$1"
  local threshold="$2"
  local found=1

  while IFS='|' read -r bench upper; do
    [[ -z "$bench" ]] && continue
    if awk -v val="$upper" -v limit="$threshold" 'BEGIN { exit (val + 0 >= limit + 0 ? 0 : 1) }'; then
      echo "Significant regression: $bench (+$upper% >= +$threshold%)" >&2
      found=0
    else
      echo "Ignoring minor regression marker: $bench (+$upper% < +$threshold%)" >&2
    fi
  done < <(collect_regressions "$in_file")

  return "$found"
}

is_critical_group_bench() {
  local bench_name="$1"
  local prefix
  for prefix in "${critical_group_prefixes[@]}"; do
    [[ -z "$prefix" ]] && continue
    if [[ "$bench_name" == "$prefix"/* ]]; then
      return 0
    fi
  done
  return 1
}

has_significant_critical_group_regression() {
  local in_file="$1"
  local threshold="$2"
  local found=1

  while IFS='|' read -r bench_name upper; do
    [[ -z "$bench_name" ]] && continue
    if ! is_critical_group_bench "$bench_name"; then
      continue
    fi
    if awk -v val="$upper" -v limit="$threshold" 'BEGIN { exit (val + 0 >= limit + 0 ? 0 : 1) }'; then
      echo "Significant critical-group regression: $bench_name (+$upper% >= +$threshold%)" >&2
      found=0
    else
      echo "Ignoring minor critical-group regression marker: $bench_name (+$upper% < +$threshold%)" >&2
    fi
  done < <(collect_regressions "$in_file")

  return "$found"
}

run_confirmation_bench() {
  local bench_name="$1"
  local run_index="$2"
  local out_file="target/benchmark-ci-gate-confirm-${bench_name}-${run_index}.txt"
  run_bench "$bench_name" "$out_file"
}

count_significant_critical_group_regressions() {
  local threshold="$1"
  shift
  local significant_count=0
  local file
  for file in "$@"; do
    [[ -f "$file" ]] || continue
    if has_significant_critical_group_regression "$file" "$threshold"; then
      significant_count=$((significant_count + 1))
    fi
  done
  echo "$significant_count"
}

echo "Running benchmark regression gate..."
echo "Policy: threshold=+$regression_threshold%, strict_mode=$strict_mode"
for bench in "${gated_benches[@]}"; do
  bench_raw_out="target/benchmark-ci-gate-${bench}.txt"
  bench_retry_out="target/benchmark-ci-gate-retry-${bench}.txt"

  echo
  echo "Benchmark target: ${bench}"

  if [[ "$preheat_runs" =~ ^[0-9]+$ ]] && (( preheat_runs > 0 )); then
    for (( preheat_idx=1; preheat_idx<=preheat_runs; preheat_idx++ )); do
      run_preheat "$bench" "$preheat_idx"
    done
  fi

  ensure_named_baseline "$bench"
  run_bench "$bench" "$bench_raw_out"

  if rg -q "Performance has regressed\." "$bench_raw_out"; then
    echo "Potential benchmark regression detected for ${bench}; running confirmation set..."
    run_bench "$bench" "$bench_retry_out"

    confirmation_outputs=("$bench_raw_out" "$bench_retry_out")
    if [[ "$confirm_runs" =~ ^[0-9]+$ ]] && (( confirm_runs > 0 )); then
      for (( run_idx=1; run_idx<=confirm_runs; run_idx++ )); do
        run_confirmation_bench "$bench" "$run_idx"
        confirmation_outputs+=("target/benchmark-ci-gate-confirm-${bench}-${run_idx}.txt")
      done
    fi

    significant_runs="$(count_significant_critical_group_regressions "$regression_threshold" "${confirmation_outputs[@]}")"

    if [[ "$strict_mode" == "1" ]]; then
      if (( significant_runs >= 2 )); then
        echo "Benchmark regression gate failed: strict mode observed reproducible significant critical-group regressions (${bench}) in ${significant_runs} runs."
        rg -n "Performance has regressed\.|Change within noise threshold\.|No change in performance detected\." "$bench_raw_out" || true
        rg -n "Performance has regressed\.|Change within noise threshold\.|No change in performance detected\." "$bench_retry_out" || true
        for out_file in "${confirmation_outputs[@]}"; do
          [[ "$out_file" == "$bench_raw_out" || "$out_file" == "$bench_retry_out" ]] && continue
          rg -n "Performance has regressed\.|Change within noise threshold\.|No change in performance detected\." "$out_file" || true
        done
        exit 1
      fi

      echo "Strict benchmark confirmation set for ${bench} did not show reproducible significant critical-group regressions; treating initial marker as noise."
      continue
    fi

    if ! has_significant_critical_group_regression "$bench_raw_out" "$regression_threshold"; then
      echo "Only minor regression markers detected for ${bench} in initial run (threshold: +$regression_threshold%)."
      continue
    fi

    if (( significant_runs >= 2 )); then
      echo "Benchmark regression gate failed: criterion reported reproducible regressions for ${bench} on retry."
      rg -n "Performance has regressed\.|Change within noise threshold\.|No change in performance detected\." "$bench_raw_out" || true
      rg -n "Performance has regressed\.|Change within noise threshold\.|No change in performance detected\." "$bench_retry_out" || true
      for out_file in "${confirmation_outputs[@]}"; do
        [[ "$out_file" == "$bench_raw_out" || "$out_file" == "$bench_retry_out" ]] && continue
        rg -n "Performance has regressed\.|Change within noise threshold\.|No change in performance detected\." "$out_file" || true
      done
      exit 1
    fi

    echo "Benchmark confirmation set for ${bench} did not reproduce significant regressions; treating first-run result as noise."
  fi
done

echo "criterion_baseline_bootstrap_fallback_benches=${benchmark_fallback_benches:-none}" >> target/benchmark-ci-env.txt

emit_benchmark_report

echo "Benchmark regression gate passed: no reproducible critical-group regressions detected."
