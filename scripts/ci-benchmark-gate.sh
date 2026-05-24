#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

raw_out="target/benchmark-ci-gate.txt"
retry_out="target/benchmark-ci-gate-retry.txt"
regression_threshold="${BENCHMARK_MAX_REGRESSION_PERCENT:-5}"
strict_mode="${BENCHMARK_STRICT:-1}"
gated_benches_raw="${BENCHMARK_GATED_BENCHES:-quality_perf cdc_perf}"
allow_non_strict="${ALLOW_BENCHMARK_NON_STRICT:-0}"
bench_sample_size="${BENCHMARK_SAMPLE_SIZE:-30}"
bench_measurement_secs="${BENCHMARK_MEASUREMENT_SECS:-8}"
bench_warm_up_secs="${BENCHMARK_WARMUP_SECS:-3}"
confirm_runs="${BENCHMARK_CONFIRM_RUNS:-2}"
preheat_runs="${BENCHMARK_PREHEAT_RUNS:-1}"
baseline_commit="${BENCHMARK_BASELINE_COMMIT:-}"
baseline_artifact="${BENCHMARK_BASELINE_ARTIFACT:-}"

current_commit="$(git rev-parse HEAD 2>/dev/null || true)"
git_tree_state="unknown"
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
    elif [[ -z "$baseline_commit" ]]; then
      status="non-release-local"
      reason="baseline_commit must be set"
    elif [[ "$baseline_commit" != "$current_commit" ]]; then
      status="non-release-local"
      reason="baseline_commit must match current git commit"
    elif [[ -z "$baseline_artifact" ]]; then
      status="non-release-local"
      reason="baseline_artifact must be set"
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

if [[ "${#gated_benches[@]}" -eq 0 ]]; then
  echo "No benchmark targets configured for gate."
  exit 1
fi

mkdir -p target

policy_info="$(benchmark_policy_status)"
policy_status="${policy_info%%|*}"
policy_reason="${policy_info#*|}"

{
  echo "timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "kernel=$(uname -a)"
  echo "git_commit=${current_commit:-unknown}"
  echo "git_tree_state=${git_tree_state}"
  echo "baseline_commit=${baseline_commit}"
  echo "baseline_artifact=${baseline_artifact}"
  echo "rustc=$(rustc -Vv | tr '\n' ';')"
  echo "threshold_percent=${regression_threshold}"
  echo "strict_mode=${strict_mode}"
  echo "gated_benches=${gated_benches_raw}"
  echo "sample_size=${bench_sample_size}"
  echo "measurement_secs=${bench_measurement_secs}"
  echo "warmup_secs=${bench_warm_up_secs}"
  echo "confirm_runs=${confirm_runs}"
  echo "preheat_runs=${preheat_runs}"
  echo "report_classification=${policy_status}"
  echo "report_classification_reason=${policy_reason}"
} > target/benchmark-ci-env.txt

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
    echo "Generated: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo
    echo "## Environment"
    echo
    echo '```text'
    uname -a
    echo "git_commit=${current_commit:-unknown}"
    echo "git_tree_state=${git_tree_state}"
    echo "baseline_commit=${baseline_commit}"
    echo "baseline_artifact=${baseline_artifact}"
    rustc -Vv
    echo "gated_benches=${gated_benches_raw}"
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

run_bench() {
  local bench_name="$1"
  local out_file="$2"
  cargo bench --bench "$bench_name" -- --sample-size "$bench_sample_size" --measurement-time "$bench_measurement_secs" --warm-up-time "$bench_warm_up_secs" | tee "$out_file"
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

is_quality_gate_bench() {
  local bench_name="$1"
  [[ "$bench_name" == quality_gates/* ]]
}

has_quality_gate_regression_marker() {
  local in_file="$1"
  while IFS='|' read -r gate_bench _upper; do
    [[ -z "$gate_bench" ]] && continue
    if is_quality_gate_bench "$gate_bench"; then
      echo "Quality-gate regression marker: $gate_bench"
      return 0
    fi
  done < <(collect_regressions "$in_file")

  return 1
}

has_significant_quality_gate_regression() {
  local in_file="$1"
  local threshold="$2"
  local found=1

  while IFS='|' read -r gate_bench upper; do
    [[ -z "$gate_bench" ]] && continue
    if ! is_quality_gate_bench "$gate_bench"; then
      continue
    fi
    if awk -v val="$upper" -v limit="$threshold" 'BEGIN { exit (val + 0 >= limit + 0 ? 0 : 1) }'; then
      echo "Significant quality-gate regression: $gate_bench (+$upper% >= +$threshold%)" >&2
      found=0
    else
      echo "Ignoring minor quality-gate regression marker: $gate_bench (+$upper% < +$threshold%)" >&2
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

count_significant_quality_gate_regressions() {
  local threshold="$1"
  shift
  local significant_count=0
  local file
  for file in "$@"; do
    [[ -f "$file" ]] || continue
    if has_significant_quality_gate_regression "$file" "$threshold"; then
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

    significant_runs="$(count_significant_quality_gate_regressions "$regression_threshold" "${confirmation_outputs[@]}")"

    if [[ "$strict_mode" == "1" ]]; then
      if (( significant_runs >= 2 )); then
        echo "Benchmark regression gate failed: strict mode observed reproducible significant quality-gate regressions (${bench}) in ${significant_runs} runs."
        rg -n "Performance has regressed\.|Change within noise threshold\.|No change in performance detected\." "$bench_raw_out" || true
        rg -n "Performance has regressed\.|Change within noise threshold\.|No change in performance detected\." "$bench_retry_out" || true
        for out_file in "${confirmation_outputs[@]}"; do
          [[ "$out_file" == "$bench_raw_out" || "$out_file" == "$bench_retry_out" ]] && continue
          rg -n "Performance has regressed\.|Change within noise threshold\.|No change in performance detected\." "$out_file" || true
        done
        exit 1
      fi

      echo "Strict benchmark confirmation set for ${bench} did not show reproducible significant quality-gate regressions; treating initial marker as noise."
      continue
    fi

    if ! has_significant_quality_gate_regression "$bench_raw_out" "$regression_threshold"; then
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

emit_benchmark_report

echo "Benchmark regression gate passed: no reproducible quality-gate regressions detected."
