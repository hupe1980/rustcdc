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
        @/*)
          # Zola internal link. `zola build` resolves and validates these against
          # the content tree and fails on a miss, so re-checking them here as file
          # paths would only produce false positives.
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

  while IFS= read -r file; do
    check_markdown_file "$file"
  done < <(find . -maxdepth 1 -type f -name '*.md' | sort)
  while IFS= read -r file; do
    check_markdown_file "$file"
  done < <(find site/content -type f -name '*.md' | sort)

  if [[ "$failed" -gt 0 ]]; then
    echo "markdown link check failed: $failed broken links out of $checked checked" >&2
    exit 1
  fi

  echo "markdown link check passed: $checked links checked"

  # Zola owns the internal `@/...` links, the template/shortcode surface and the
  # taxonomy of the content tree. If it is installed, build the site so a broken
  # cross-reference fails the gate rather than the deploy.
  if command -v zola >/dev/null 2>&1; then
    if ! zola --root site check >/dev/null; then
      echo "zola check failed: the documentation site does not build" >&2
      exit 1
    fi
    echo "zola check passed"
  else
    echo "zola not installed: skipping documentation site check"
  fi
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
  matches="$(rg -n --hidden --glob '!.git' --glob '!target' "$pattern" src tests site/content scripts .github xtask Cargo.toml README.md || true)"

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

run_cargo_profile_safety_check() {
  # Reject any Cargo profile that enables debug-assertions = true unless it is the
  # built-in "dev" or "test" profile (which expect debug-assertions in development).
  #
  # This catches release/bench/custom profiles that accidentally enable assertions,
  # not just profiles whose name contains "release".
  local failed=0

  local matches
  matches="$(rg -n 'debug-assertions\s*=\s*true' Cargo.toml 2>/dev/null || true)"

  if [[ -z "$matches" ]]; then
    echo "Cargo profile safety check passed (no debug-assertions = true found)."
    return 0
  fi

  # Walk Cargo.toml: track the current [profile.<name>] section and report any
  # debug-assertions = true that appears outside the allowed set (dev, test).
  local bad_profiles
  bad_profiles="$(awk '
    /^\[profile\./ {
      # Extract profile name from "[profile.foo]" or "[profile.foo.bar]"
      match($0, /\[profile\.([^.\]]+)/, arr)
      current_profile = arr[1]
    }
    /debug-assertions\s*=\s*true/ {
      if (current_profile != "dev" && current_profile != "test") {
        print "[profile." current_profile "]: " $0
      }
    }
  ' Cargo.toml || true)"

  if [[ -n "$bad_profiles" ]]; then
    echo "FAIL: debug-assertions = true found in non-dev/non-test Cargo profile:" >&2
    echo "$bad_profiles" >&2
    echo "Only [profile.dev] and [profile.test] may enable debug-assertions." >&2
    echo "All other profiles (release, bench, custom) must use debug-assertions = false." >&2
    failed=1
  fi

  if [[ $failed -eq 0 ]]; then
    echo "Cargo profile safety check passed."
  else
    exit 1
  fi
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
  # Both doc lanes, not just one. The all-features build is blind to a link from an
  # ungated doc comment into a feature-gated item, because every gate is on; the
  # no-default-features build is what catches it. Losing either lane restores the blind
  # spot that hid twelve such links.
  #
  # Anchored on `run:` rather than on the bare command: a step's `name:` usually repeats
  # the command, so an unanchored pattern is satisfied by the label alone and would still
  # match after the command itself was changed.
  require_match "^ +run: cargo doc --all-features --no-deps$" "$CI_WORKFLOW" "all-features doc build"
  require_match "^ +run: cargo doc --no-default-features --no-deps$" "$CI_WORKFLOW" "no-default-features doc build"
  require_match "bash scripts/ci-pull-relational-images.sh --relational-smoke" "$CI_WORKFLOW" "relational smoke image pull mode"
  require_match "bash scripts/ci-benchmark-gate.sh" "$CI_WORKFLOW" "benchmark policy gate"
  require_match "bash scripts/run_full_integration_matrix_evidence.sh" "$CI_WORKFLOW" "full matrix evidence run"
  require_match "BENCHMARK_ENFORCE_RELEASE_POLICY: \"1\"" "$CI_WORKFLOW" "benchmark policy enforcement"
  require_match "  release-evidence:" "$CI_WORKFLOW" "release evidence job"
  require_match "  release-evidence-verify:" "$CI_WORKFLOW" "release evidence verification job"
  require_match "  publish:" "$CI_WORKFLOW" "publish job"
  require_match "needs: release-evidence-verify" "$CI_WORKFLOW" "publish dependency on release evidence verification"

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

  run_test_suite_coverage_check
  run_relational_image_drift_check

  echo "Workflow drift guard passed."
}

# The pre-pull list must cover every version the test matrices instantiate.
#
# The pre-pull exists to fetch images from a mirror rather than from rate-limited Docker Hub.
# When it drifts from the matrices it fails silently in the worst direction: the warmed
# images go unused and the images the tests actually need are fetched at run time, from
# exactly the registry the script was written to avoid. It had drifted by two of four.
run_relational_image_drift_check() {
  local pull_script="scripts/ci-pull-relational-images.sh"
  local missing=()

  local mysql_versions mariadb_versions
  mysql_versions="$(grep -oE '"[0-9]+\.[0-9]+"' tests/mysql_version_matrix.rs | tr -d '"' | sort -u)"
  mariadb_versions="$(grep -oE '"1[0-9]\.[0-9]+"' tests/mariadb_e2e_integration.rs | tr -d '"' | sort -u)"

  local version
  for version in $mysql_versions; do
    grep -q "\"mysql:${version}\"" "$pull_script" || missing+=("mysql:${version}")
  done
  for version in $mariadb_versions; do
    grep -q "\"mariadb:${version}\"" "$pull_script" || missing+=("mariadb:${version}")
  done

  if (( ${#missing[@]} > 0 )); then
    echo "FAIL: test matrices instantiate images the pre-pull list does not warm:" >&2
    printf '  - %s\n' "${missing[@]}" >&2
    echo "Add them to NON_POSTGRES_RELATIONAL_SMOKE_IMAGES in ${pull_script}, or drop the" >&2
    echo "version from the matrix. Leaving them out sends those pulls to Docker Hub." >&2
    exit 1
  fi

  echo "Relational image drift check passed (pre-pull covers every matrix version)."
}

# Every integration suite under tests/ must actually be run by something.
#
# The checks above are an *allow-list*: they assert that named suites appear in the
# workflow. That is silent about suites nobody added — and a test that never runs is
# indistinguishable from a test that does not exist, while looking like evidence in a
# review. Sixteen suites had accumulated outside CI when this check was written, including
# the end-to-end coverage of `register_source` (the crate's headline extension-point claim)
# and the structured-log schema.
#
# A suite counts as covered when its name appears in the CI workflow, in a script the
# workflow runs, or as a `#[path]`-included helper module of another suite. Anything else
# must be added to one of those, or listed in HELPER_SUITES with a reason.
run_test_suite_coverage_check() {
  # Helper modules included by other suites via `#[path = "..."] mod ...;`. Cargo also
  # builds each as its own (empty) test binary, so they appear in tests/ without being
  # suites in their own right.
  local helper_suites=(
    latency_evidence_common
    process_crash_marker
    process_crash_worker
    sqlserver_testkit
  )

  local uncovered=()
  local suite
  for path in tests/*.rs; do
    suite="$(basename "$path" .rs)"

    local is_helper=0
    for helper in "${helper_suites[@]}"; do
      if [[ "$suite" == "$helper" ]]; then
        is_helper=1
        break
      fi
    done
    if [[ "$is_helper" == "1" ]]; then
      continue
    fi

    if grep -q -- "$suite" "$CI_WORKFLOW"; then
      continue
    fi
    if grep -rq --include='*.sh' -- "$suite" scripts/; then
      continue
    fi
    if grep -rq "path = \"${suite}.rs\"" tests/; then
      continue
    fi

    uncovered+=("$suite")
  done

  if (( ${#uncovered[@]} > 0 )); then
    echo "FAIL: integration suites are never run by CI or any script:" >&2
    for suite in "${uncovered[@]}"; do
      echo "  - tests/${suite}.rs" >&2
    done
    echo "Add each to a matrix in ${CI_WORKFLOW}, to a script CI runs, or to" >&2
    echo "helper_suites in scripts/ci-policy-gate.sh with a reason." >&2
    exit 1
  fi

  echo "Test suite coverage check passed (every tests/*.rs is run by CI or a script)."
}

# Every public field of a user-facing config struct must appear in the configuration
# reference. These tables used to be hand-copied `pub struct` dumps in the docs, which
# drifted silently: eleven fields existed in code and were documented nowhere. A field
# nobody can find is a field nobody sets, and the defaults here are load-bearing.
run_config_docs_coverage_check() {
  local doc="site/content/docs/config-reference.md"
  local failed=0

  check_struct_fields_documented() {
    local file="$1"
    local struct_name="$2"
    local bt='`'

    # Public field names inside the struct body, up to its closing brace at column 0.
    local fields
    fields="$(awk -v s="pub struct $struct_name" '
      index($0, s) == 1 { inside = 1; next }
      inside && /^}/ { exit }
      inside && match($0, /^[ \t]+pub [a-z_0-9]+:/) {
        line = $0
        sub(/^[ \t]+pub /, "", line)
        sub(/:.*$/, "", line)
        print line
      }
    ' "$file")"

    if [[ -z "$fields" ]]; then
      echo "config docs coverage: struct $struct_name not found in $file" >&2
      failed=$((failed + 1))
      return
    fi

    local field
    while IFS= read -r field; do
      [[ -z "$field" ]] && continue
      # The reference documents fields in table rows: | `field` | type | ... |
      if ! rg -q "^[|] ${bt}${field}${bt}" "$doc"; then
        echo "config docs coverage: $struct_name::$field is not documented in $doc" >&2
        failed=$((failed + 1))
      fi
    done <<< "$fields"
  }

  check_struct_fields_documented "src/core/runtime.rs" "RuntimeConfig"
  check_struct_fields_documented "src/core/runtime.rs" "RuntimeOptions"
  check_struct_fields_documented "src/source/postgres.rs" "PostgresSourceConfig"
  check_struct_fields_documented "src/source/mysql.rs" "MysqlSourceConfig"
  check_struct_fields_documented "src/source/sqlserver.rs" "SqlServerSourceConfig"

  if [[ "$failed" -gt 0 ]]; then
    echo "config docs coverage check failed: $failed undocumented field(s)" >&2
    exit 1
  fi

  echo "Config docs coverage check passed."
}

run_markdown_link_check
# A public type inside a submodule that its parent never re-exports is unreachable in
# practice: `codec::schema_registry` is `pub`, but every doc example and every downstream
# import names `codec::…`. `ConfluentProtobufEncoder` and `ConfluentProtobufDecoder` sat
# unexported for an entire release cycle — the codec with no live test coverage was also
# the one nobody could import.
run_reexport_coverage_check() {
  local failed=0

  check_module_reexports() {
    local child="$1"
    local parent="$2"

    local items
    items="$(rg --no-line-number --no-filename -o \
      '^pub (?:struct|enum|trait|const|type|fn|async fn) ([A-Za-z_][A-Za-z0-9_]*)' \
      --replace '$1' "$child" || true)"

    local item
    while IFS= read -r item; do
      [[ -z "$item" ]] && continue
      if ! rg -q "\\b$item\\b" "$parent"; then
        echo "re-export coverage: $child defines public \`$item\` but $parent never names it" >&2
        failed=$((failed + 1))
      fi
    done <<< "$items"
  }

  # Crate-root parity, one level further up than module→parent.
  #
  # Module→parent alone left items reachable only as `rustcdc::codec::X` while their
  # direct counterparts were `rustcdc::X`: `ConfluentProtobufEncoder`/`Decoder` next to
  # the Avro and JSON Schema pairs, `AvroDecoder` next to `AvroEncoder`,
  # `OutboxTransform`/`OutboxResult` next to every other shipped transform, the three
  # concrete `DdlExtractor` implementations next to the trait, and
  # `IncrementalSnapshotBackend` — the custom-source extension point the audit calls a
  # differentiator — next to the `IncrementalSnapshotConfig` and handles already there.
  # Nothing was broken; it cost a docs search per item and made the surface look
  # arbitrary.
  #
  # The rule is **all-or-nothing per module**, and it configures itself: if `src/lib.rs`
  # re-exports anything from a module, it must re-export everything that module
  # re-exports. Modules `lib.rs` deliberately keeps namespaced — `checkpoint`,
  # `testkit`, `fault_injection`, `deterministic_replay`, `schema_history` — have no
  # crate-root surface to be inconsistent with and are skipped. Adding a single item
  # from one of them to `lib.rs` opts it in, which is the intended tripwire.
  #
  # Every public *item* (`pub struct|enum|trait|const|type|fn`) declared anywhere in the
  # crate. Used to tell items apart from the module path segments that also appear inside
  # a `pub use` statement — `avro` in `pub use avro::{…}` is a `pub mod`, not an item, and
  # crate-root parity for modules is a separate question this gate deliberately leaves to
  # `pub mod` declarations in lib.rs.
  local public_items
  # Anchored at column 0 so an inherent method (`    pub fn route(..)` inside an `impl`)
  # is not mistaken for a module-level item.
  public_items="$(rg --no-line-number --no-filename -o \
    '^pub (?:struct|enum|trait|const|type|fn|async fn) ([A-Za-z_][A-Za-z0-9_]*)' \
    --replace '$1' src | sort -u)"

  # Names re-exported by `<module>/mod.rs` must also be named by `src/lib.rs`.
  check_crate_root_reexports() {
    local module_mod="$1"
    local module_name
    module_name="$(basename "$(dirname "$module_mod")")"

    # Skip modules with no crate-root surface at all — they are namespaced by design.
    if ! rg -q "^pub use crate::${module_name}::" src/lib.rs; then
      return
    fi

    # Take the name after `as` when a re-export is aliased: that is the name `lib.rs`
    # would have to use, and matching the pre-alias name reports a false positive.
    local names
    names="$(rg --no-line-number --no-filename --multiline -o \
      'pub use [^;]+;' "$module_mod" \
      | sed -E 's/[A-Za-z_][A-Za-z0-9_]* +as +([A-Za-z_][A-Za-z0-9_]*)/\1/g' \
      | rg -o '\b[A-Za-z_][A-Za-z0-9_]*\b' \
      | sort -u || true)"

    local item
    while IFS= read -r item; do
      [[ -z "$item" ]] && continue
      # Keep only real items; skip `pub`/`use`/`crate` and module path segments.
      grep -qx -- "$item" <<< "$public_items" || continue
      if ! rg -q "\\b$item\\b" src/lib.rs; then
        echo "crate-root parity: $module_mod re-exports \`$item\` but src/lib.rs never names it" >&2
        failed=$((failed + 1))
      fi
    done <<< "$names"
  }

  check_module_reexports "src/codec/schema_registry.rs" "src/codec/mod.rs"
  check_module_reexports "src/codec/avro.rs" "src/codec/mod.rs"
  check_module_reexports "src/codec/json.rs" "src/codec/mod.rs"
  check_module_reexports "src/source/incremental_snapshot/driver.rs" "src/source/mod.rs"

  # Every module directory; the function itself skips those with no crate-root surface.
  for module_mod in src/*/mod.rs; do
    check_crate_root_reexports "$module_mod"
  done

  if [[ "$failed" -gt 0 ]]; then
    echo "re-export coverage check failed: $failed unreachable public item(s)" >&2
    exit 1
  fi

  echo "Re-export coverage check passed."
}

run_config_docs_coverage_check
run_reexport_coverage_check
run_schema_contract_check
run_deprecated_usage_check
run_async_trait_policy_check
run_cargo_profile_safety_check
run_workflow_drift_check

echo "Policy gate passed."
