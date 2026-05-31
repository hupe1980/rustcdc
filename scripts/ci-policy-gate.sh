#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

need_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
}

need_cmd rg
need_cmd sort
need_cmd sed
need_cmd jq

CI_WORKFLOW=".github/workflows/ci.yml"

run_markdown_link_check() {
  local failed=0
  local checked=0

  check_markdown_file() {
    local markdown_file="$1"
    local markdown_dir
    markdown_dir="$(dirname "$markdown_file")"

    while IFS= read -r match; do
      local target
      target="$(printf '%s' "$match" | sed -E 's/.*\(([^)]+)\)/\1/')"
      target="${target#<}"
      target="${target%>}"
      target="${target//%20/ }"
      target="${target%%#*}"
      target="${target%%\?*}"

      if [[ -z "$target" ]]; then
        continue
      fi

      case "$target" in
        http://*|https://*|mailto:*|\#*)
          continue
          ;;
      esac

      local resolved
      if [[ "$target" = /* ]]; then
        resolved="$repo_root/$target"
      else
        resolved="$markdown_dir/$target"
      fi

      checked=$((checked + 1))
      if [[ ! -e "$resolved" ]]; then
        echo "broken markdown link in $markdown_file -> $target" >&2
        failed=$((failed + 1))
      fi
    done < <(rg --no-line-number --no-filename --pcre2 -o '\[[^][]+\]\(([^)]+)\)' "$markdown_file")
  }

  check_markdown_file "README.md"
  while IFS= read -r file; do
    check_markdown_file "$file"
  done < <(find docs -type f -name '*.md' | sort)

  if [[ "$failed" -gt 0 ]]; then
    echo "markdown link check failed: $failed broken links out of $checked checked" >&2
    exit 1
  fi

  echo "markdown link check passed: $checked links checked"
}

check_equal_sets() {
  local label="$1"
  local expected="$2"
  local actual="$3"

  if ! diff -u "$expected" "$actual" >/dev/null; then
    echo "Policy gate failed for $label" >&2
    echo "--- expected" >&2
    cat "$expected" >&2
    echo "--- actual" >&2
    cat "$actual" >&2
    echo "--- diff" >&2
    diff -u "$expected" "$actual" >&2 || true
    exit 1
  fi
}

run_schema_contract_check() {
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' RETURN

  local rust_event_fields="$tmp_dir/rust_event_fields.txt"
  local rust_operation_symbols="$tmp_dir/rust_operation_symbols.txt"
  local proto_event_fields="$tmp_dir/proto_event_fields.txt"
  local proto_operation_symbols="$tmp_dir/proto_operation_symbols.txt"
  local avro_event_fields="$tmp_dir/avro_event_fields.txt"
  local avro_operation_symbols="$tmp_dir/avro_operation_symbols.txt"

  sed -n '/pub struct Event {/,/^}/p' src/core/event.rs \
    | sed -nE 's/^[[:space:]]*pub[[:space:]]+([a-z_][a-z0-9_]*)[[:space:]]*:.*/\1/p' \
    | sort -u > "$rust_event_fields"

  sed -n '/pub enum Operation {/,/^}/p' src/core/event.rs \
    | sed -nE 's/^[[:space:]]*([A-Z][A-Za-z0-9_]*)[[:space:]]*,.*/\1/p' \
    | sed -E 's/([a-z0-9])([A-Z])/\1_\2/g' \
    | tr '[:upper:]' '[:lower:]' \
    | sort -u > "$rust_operation_symbols"

  sed -n '/message Event {/,/^}/p' proto/event.proto \
    | sed -E 's,//.*$,,' \
    | sed -nE 's/^[[:space:]]*(optional[[:space:]]+|repeated[[:space:]]+)?[A-Za-z_][A-Za-z0-9_]*[[:space:]]+([a-z_][a-z0-9_]*)[[:space:]]*=.*/\2/p' \
    | sort -u > "$proto_event_fields"

  sed -n '/enum Operation {/,/^}/p' proto/event.proto \
    | sed -E 's,//.*$,,' \
    | sed -nE 's/^[[:space:]]*([A-Z_]+)[[:space:]]*=.*/\1/p' \
    | rg -v '^OPERATION_UNSPECIFIED$' \
    | tr '[:upper:]' '[:lower:]' \
    | sort -u > "$proto_operation_symbols"

  jq -r '.fields[].name' schemas/event.avsc | sort -u > "$avro_event_fields"

  jq -r '
    .fields[]
    | select(.name == "op")
    | .type.symbols[]
  ' schemas/event.avsc \
    | tr '[:upper:]' '[:lower:]' \
    | sort -u > "$avro_operation_symbols"

  check_equal_sets "event fields (Rust vs Protobuf)" "$rust_event_fields" "$proto_event_fields"
  check_equal_sets "event fields (Rust vs Avro)" "$rust_event_fields" "$avro_event_fields"
  check_equal_sets "operation symbols (Rust vs Protobuf)" "$rust_operation_symbols" "$proto_operation_symbols"
  check_equal_sets "operation symbols (Rust vs Avro)" "$rust_operation_symbols" "$avro_operation_symbols"

  echo "Schema contract gate passed."
}

run_deprecated_usage_check() {
  local pattern='\#\[\s*deprecated|deprecated\('
  local matches
  matches="$(rg -n --hidden --glob '!.git' --glob '!target' "$pattern" src tests docs scripts .github xtask Cargo.toml README.md || true)"

  if [[ -n "$matches" ]]; then
    echo "Deprecated marker/usage gate failed. Remove deprecated APIs/usages before merging." >&2
    echo "$matches" >&2
    exit 1
  fi

  echo "Deprecated usage gate passed."
}

run_async_trait_policy_check() {
  local connector_files=(
    "src/source/postgres.rs"
    "src/source/mysql.rs"
    "src/source/sqlserver.rs"
  )

  if rg -n '#\[async_trait::async_trait\]' "${connector_files[@]}"; then
    echo "Async-trait policy check failed: use imported #[async_trait] form in connector internals." >&2
    exit 1
  fi

  echo "Async-trait policy check passed."
}

require_match() {
  local pattern="$1"
  local file="$2"
  local label="$3"

  if ! rg -q -- "$pattern" "$file"; then
    echo "FAIL: missing ${label} in ${file}" >&2
    exit 1
  fi
}

require_absent() {
  local pattern="$1"
  local file="$2"
  local label="$3"

  if rg -q -- "$pattern" "$file"; then
    echo "FAIL: found forbidden ${label} in ${file}" >&2
    exit 1
  fi
}

require_job_not_dispatch_gated() {
  local job_name="$1"
  local file="$2"

  if ! awk -v job_name="$job_name" '
    $0 == "  " job_name ":" {
      in_job = 1
      saw_job = 1
      next
    }
    in_job && /^  [a-z0-9-]+:/ {
      in_job = 0
    }
    in_job && $0 == "    if: github.event_name == '\''workflow_dispatch'\''" {
      saw_dispatch_if = 1
    }
    END {
      exit !(saw_job && !saw_dispatch_if)
    }
  ' "$file"; then
    echo "FAIL: job ${job_name} must be part of default CI signal in ${file}" >&2
    exit 1
  fi
}

require_file_absent() {
  local file="$1"

  if [[ -f "$file" ]]; then
    echo "FAIL: deprecated workflow file still present: ${file}" >&2
    exit 1
  fi
}

run_workflow_drift_check() {
  require_file_absent ".github/workflows/publish.yml"
  require_file_absent ".github/workflows/nightly-evidence.yml"
  require_file_absent ".github/workflows/benchmark-baseline-refresh.yml"

  require_match "^name: ci$" "$CI_WORKFLOW" "single workflow name"
  require_match "^  pull_request:$" "$CI_WORKFLOW" "pull request trigger"
  require_match "^  push:$" "$CI_WORKFLOW" "push trigger"
  require_match "^      - \"v\*\"$" "$CI_WORKFLOW" "tag trigger for releases"
  require_match "bash scripts/ci-policy-gate.sh" "$CI_WORKFLOW" "policy gate"
  require_match "bash scripts/ci-pull-relational-images.sh --relational-smoke" "$CI_WORKFLOW" "relational smoke image pull mode"
  require_match "bash scripts/ci-benchmark-gate.sh" "$CI_WORKFLOW" "benchmark policy gate"
  require_match "bash scripts/run_full_integration_matrix_evidence.sh" "$CI_WORKFLOW" "full matrix evidence run"
  require_match "BENCHMARK_ENFORCE_RELEASE_POLICY: \"1\"" "$CI_WORKFLOW" "benchmark policy enforcement"
  require_match "  release-evidence:" "$CI_WORKFLOW" "release evidence job"
  require_match "  publish:" "$CI_WORKFLOW" "publish job"
  require_match "needs: release-evidence" "$CI_WORKFLOW" "publish dependency on release evidence"

  require_match "mysql_snapshot_integration" "$CI_WORKFLOW" "mysql depth suite"
  require_match "mariadb_e2e_integration" "$CI_WORKFLOW" "mariadb depth suite"
  require_match "sqlserver_stream_integration" "$CI_WORKFLOW" "sqlserver depth suite"

  core_jobs=(
    integration-postgres
    integration-postgres-encryption
    integration-reliability
    integration-mysql
    integration-mysql-encryption
    integration-mariadb
    integration-mariadb-encryption
    integration-sqlserver
    integration-sqlserver-encryption
  )

  for job in "${core_jobs[@]}"; do
    require_job_not_dispatch_gated "$job" "$CI_WORKFLOW"
  done

  require_absent "docker pull mysql:8.0" "$CI_WORKFLOW" "inline mysql pull"
  require_absent "docker pull mariadb:10.6" "$CI_WORKFLOW" "inline mariadb pull"
  require_absent "docker pull mcr.microsoft.com/mssql/server:2019-latest" "$CI_WORKFLOW" "inline sqlserver pull"
  require_absent "if: github.event_name == 'workflow_dispatch'" "$CI_WORKFLOW" "workflow-dispatch-only CI lanes"

  echo "Workflow drift guard passed."
}

run_markdown_link_check
run_schema_contract_check
run_deprecated_usage_check
run_async_trait_policy_check
run_workflow_drift_check

echo "Policy gate passed."
