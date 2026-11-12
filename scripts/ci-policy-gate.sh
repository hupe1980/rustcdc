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
RELEASE_WORKFLOW=".github/workflows/release.yml"

run_markdown_link_check() {
  local failed=0
  local checked=0

  # `local_only` is set for trees that are deliberately not in the repository — the
  # gitignored `concepts/` notes. There, "this target is gitignored" is the normal case
  # rather than the defect, so only existence is checked.
  check_markdown_file() {
    local markdown_file="$1"
    local local_only="${2:-}"
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
      elif [[ -z "$local_only" ]] && git -C "$repo_root" check-ignore -q "$resolved" 2>/dev/null; then
        # Existence on the author's disk is not the test: a gitignored target is not in
        # the repository, so the link is broken for CI and for every reader while looking
        # fine locally.
        echo "markdown link in $markdown_file -> $target resolves only locally (gitignored)" >&2
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

  # `concepts/` is gitignored, so it is absent in CI and present on a maintainer's disk.
  # Checking it only when it is there is the point rather than a compromise: nothing else
  # validates these notes.
  #
  # The notes are the flat files, so `-maxdepth 1` leaves out `concepts/references/`:
  # verbatim third-party specifications whose links address the upstream site's layout,
  # not this tree.
  if [[ -d concepts ]]; then
    while IFS= read -r file; do
      check_markdown_file "$file" local_only
    done < <(find concepts -maxdepth 1 -type f -name '*.md' | sort)
  fi

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
  local rust_serialized_fields="$tmp_dir/rust_serialized_fields.txt"
  local rust_operation_symbols="$tmp_dir/rust_operation_symbols.txt"
  local proto_event_fields="$tmp_dir/proto_event_fields.txt"
  local proto_operation_symbols="$tmp_dir/proto_operation_symbols.txt"
  local avro_event_fields="$tmp_dir/avro_event_fields.txt"
  local avro_operation_symbols="$tmp_dir/avro_operation_symbols.txt"

  # The Rust side of this comparison is `EventWire`, not `Event`.
  #
  # `Event` is the in-memory shape and `EventWire` is the canonical statement of the
  # envelope *on the wire* — they stopped being field-for-field identical when the
  # pre-image became one `BeforeImage` field spanning the three wire fields `before`,
  # `before_is_key_only` and `before_unavailable_columns`. Comparing `Event` to Protobuf
  # after that compares an in-memory shape against a wire shape and reports a difference
  # that is by design, which would leave only two ways out: nest the row under a variant
  # tag in every codec (breaking every JSON path over the stream), or exempt the two
  # highest-risk fields in the envelope from the gate entirely.
  #
  # So the comparison is wire to wire, four ways: `EventWire` (what the JSON decoder
  # reads) is also checked against the hand-written `Serialize` (what the JSON encoder
  # writes), because a field written but never read — or read but never written — is
  # exactly the silent envelope divergence this gate exists to catch.
  sed -n '/^struct EventWire {/,/^}/p' crates/rustcdc/src/core/event.rs \
    | sed -nE 's/^[[:space:]]*([a-z_][a-z0-9_]*)[[:space:]]*:.*/\1/p' \
    | sort -u > "$rust_event_fields"

  sed -n '/^impl Serialize for Event {/,/^}/p' crates/rustcdc/src/core/event.rs \
    | grep -oE '(serialize_field|skip_field)\("[a-z_]+"' \
    | sed -E 's/.*\("([a-z_]+)"/\1/' \
    | sort -u > "$rust_serialized_fields"

  sed -n '/pub enum Operation {/,/^}/p' crates/rustcdc/src/core/event.rs \
    | sed -nE 's/^[[:space:]]*([A-Z][A-Za-z0-9_]*)[[:space:]]*,.*/\1/p' \
    | sed -E 's/([a-z0-9])([A-Z])/\1_\2/g' \
    | tr '[:upper:]' '[:lower:]' \
    | sort -u > "$rust_operation_symbols"

  sed -n '/message Event {/,/^}/p' crates/rustcdc/proto/event.proto \
    | sed -E 's,//.*$,,' \
    | sed -nE 's/^[[:space:]]*(optional[[:space:]]+|repeated[[:space:]]+)?[A-Za-z_][A-Za-z0-9_]*[[:space:]]+([a-z_][a-z0-9_]*)[[:space:]]*=.*/\2/p' \
    | sort -u > "$proto_event_fields"

  sed -n '/enum Operation {/,/^}/p' crates/rustcdc/proto/event.proto \
    | sed -E 's,//.*$,,' \
    | sed -nE 's/^[[:space:]]*([A-Z_]+)[[:space:]]*=.*/\1/p' \
    | rg -v '^OPERATION_UNSPECIFIED$' \
    | tr '[:upper:]' '[:lower:]' \
    | sort -u > "$proto_operation_symbols"

  jq -r '.fields[].name' crates/rustcdc/schemas/event.avsc | sort -u > "$avro_event_fields"

  jq -r '
    .fields[]
    | select(.name == "op")
    | .type.symbols[]
  ' crates/rustcdc/schemas/event.avsc \
    | tr '[:upper:]' '[:lower:]' \
    | sort -u > "$avro_operation_symbols"

  check_equal_sets "event fields (EventWire vs Serialize impl)" \
    "$rust_event_fields" "$rust_serialized_fields"
  check_equal_sets "event fields (Rust vs Protobuf)" "$rust_event_fields" "$proto_event_fields"
  check_equal_sets "event fields (Rust vs Avro)" "$rust_event_fields" "$avro_event_fields"
  check_equal_sets "operation symbols (Rust vs Protobuf)" "$rust_operation_symbols" "$proto_operation_symbols"
  check_equal_sets "operation symbols (Rust vs Avro)" "$rust_operation_symbols" "$avro_operation_symbols"

  # The JSON Schema is the fourth definition of the envelope, and the row shape is nested in
  # all four, so both levels are compared. An empty extraction would compare equal to another
  # empty one; every list is required to be non-empty first.
  local json_schema="$tmp_dir/event_json_schema.json"
  sed -n '/^pub const EVENT_JSON_SCHEMA: &str = r##"{/,/^}"##;/p' crates/rustcdc/src/codec/schema_registry.rs \
    | sed -e '1s/.*/{/' -e '$s/.*/}/' > "$json_schema"
  local json_event_fields="$tmp_dir/json_event_fields.txt"
  jq -r '.properties | keys[]' "$json_schema" | sort -u > "$json_event_fields"
  check_equal_sets "event fields (Rust vs EVENT_JSON_SCHEMA)" "$rust_event_fields" "$json_event_fields"

  local nested
  for nested in RowShape ShapeColumn; do
    local rust_nested="$tmp_dir/rust_${nested}.txt"
    local serialized_nested="$tmp_dir/serialized_${nested}.txt"
    local proto_nested="$tmp_dir/proto_${nested}.txt"
    local avro_nested="$tmp_dir/avro_${nested}.txt"
    local json_nested="$tmp_dir/json_${nested}.txt"

    if [[ "$nested" == "RowShape" ]]; then
      # `RowShapeWire` is what decodes; the hand-written `Serialize` is what encodes.
      sed -n '/^struct RowShapeWire {/,/^}/p' crates/rustcdc/src/core/row_shape.rs \
        | sed -nE 's/^[[:space:]]*([a-z_][a-z0-9_]*)[[:space:]]*:.*/\1/p' \
        | sort -u > "$rust_nested"
      sed -n '/^impl Serialize for RowShape {/,/^}/p' crates/rustcdc/src/core/row_shape.rs \
        | grep -oE '(serialize_field|skip_field)\("[a-z_]+"' \
        | sed -E 's/.*\("([a-z_]+)"/\1/' \
        | sort -u > "$serialized_nested"
    else
      # Derived serde on both directions: the struct is the wire.
      sed -n '/^pub struct ShapeColumn {/,/^}/p' crates/rustcdc/src/core/row_shape.rs \
        | sed -nE 's/^[[:space:]]*pub[[:space:]]+([a-z_][a-z0-9_]*)[[:space:]]*:.*/\1/p' \
        | sort -u > "$rust_nested"
      cp "$rust_nested" "$serialized_nested"
    fi
    sed -n "/^message ${nested} {/,/^}/p" crates/rustcdc/proto/event.proto \
      | sed -E 's,//.*$,,' \
      | sed -nE 's/^[[:space:]]*(optional[[:space:]]+|repeated[[:space:]]+)?[A-Za-z_][A-Za-z0-9_]*[[:space:]]+([a-z_][a-z0-9_]*)[[:space:]]*=.*/\2/p' \
      | sort -u > "$proto_nested"
    jq -r --arg name "$nested" '.. | objects | select(.type == "record" and .name == $name) | .fields[].name' \
      crates/rustcdc/schemas/event.avsc | sort -u > "$avro_nested"
    jq -r --arg name "$nested" '."$defs"[$name].properties | keys[]' "$json_schema" | sort -u > "$json_nested"

    local list
    for list in "$rust_nested" "$serialized_nested" "$proto_nested" "$avro_nested" "$json_nested"; do
      if [[ ! -s "$list" ]]; then
        echo "Policy gate failed: no ${nested} fields extracted into $(basename "$list")" >&2
        exit 1
      fi
    done
    check_equal_sets "${nested} fields (decode vs Serialize impl)" "$rust_nested" "$serialized_nested"
    check_equal_sets "${nested} fields (Rust vs Protobuf)" "$rust_nested" "$proto_nested"
    check_equal_sets "${nested} fields (Rust vs Avro)" "$rust_nested" "$avro_nested"
    check_equal_sets "${nested} fields (Rust vs EVENT_JSON_SCHEMA)" "$rust_nested" "$json_nested"
  done

  echo "Schema contract gate passed."
}

run_deprecated_usage_check() {
  local pattern='\#\[\s*deprecated|deprecated\('
  local matches
  matches="$(rg -n --hidden --glob '!.git' --glob '!target' --glob '!crates/rustcdc-server/fuzz/target' "$pattern" crates/rustcdc/src crates/rustcdc/tests crates/rustcdc-server/src crates/rustcdc-server/tests site/content scripts .github crates/xtask Cargo.toml crates/rustcdc/Cargo.toml crates/rustcdc-server/Cargo.toml README.md crates/rustcdc/README.md crates/rustcdc-server/README.md || true)"

  if [[ -n "$matches" ]]; then
    echo "Deprecated marker/usage gate failed. Remove deprecated APIs/usages before merging." >&2
    echo "$matches" >&2
    exit 1
  fi

  echo "Deprecated usage gate passed."
}

run_async_trait_policy_check() {
  local connector_files=(
    "crates/rustcdc/src/source/postgres.rs"
    "crates/rustcdc/src/source/mysql.rs"
    "crates/rustcdc/src/source/sqlserver.rs"
  )

  if rg -n '#\[async_trait::async_trait\]' "${connector_files[@]}"; then
    echo "Async-trait policy check failed: use imported #[async_trait] form in connector internals." >&2
    exit 1
  fi

  echo "Async-trait policy check passed."
}

run_cargo_profile_safety_check() {
  # Delegated to `cargo xtask profile-check`, which is compiled and unit-tested.
  #
  # A check whose correctness depends on which `awk` is installed is not a check: GNU and
  # BSD awk disagree, and a syntax error under `|| true` prints "passed" having read
  # nothing.
  if ! cargo xtask profile-check; then
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

require_file_present() {
  local file="$1"

  if [[ ! -f "$file" ]]; then
    echo "FAIL: required workflow file is missing: ${file}" >&2
    exit 1
  fi
}

# Every published crate must carry the licence texts, and they must be the repository's.
#
# `license = "MIT OR Apache-2.0"` is an SPDX expression, not a grant. `cargo package`
# collects only files beneath the crate directory, so the crate needs its own copies of the
# texts; this is what stops the copies drifting from the originals.
# Every path a workflow names must exist.
#
# A `working-directory:`, a local `uses: ./…` or a script path that points at a directory
# the repository does not have fails **only when that job runs** — on a pull request, in a
# lane that may be slow or rarely reached, with an error from bash rather than from
# anything that reads like a path problem:
#
#   An error occurred trying to start process '/usr/bin/bash' with working directory
#   '/home/runner/work/rustcdc/rustcdc/server'. No such file or directory
#
# A rename leaves this behind when the search-and-replace misses one spelling of a path.
# Every job must say which half of the workspace it covers.
#
# GitHub shows a job's `name:` in the checks list, falling back to the job id. Without a
# scope prefix a reader sees `Check`, `Test` and `Fuzz (smoke)` and cannot tell the library
# from the server — which is exactly how a red check gets attributed to the wrong half.
#
# `lib:` the library · `server:` the binary · `workspace:` both · `release:` the tag
# pipeline. `CI` is the aggregator branch protection requires and is deliberately bare.
run_release_evidence_contract_check() {
  # `release.yml` gates every release on an artifact that `ci.yml` uploads. That name is
  # the only contract between the two files, and nothing else would notice it breaking:
  # the release simply fails at the tag, which is the worst moment to find out.
  #
  # The contract is the artifact name, not a job name: the GitHub API returns a job's
  # display name, which a rename changes silently.
  local expected uploaded
  expected="$(rg -o --no-heading --no-line-number \
    "const EVIDENCE_ARTIFACT = '([^']+)'" -r '$1' .github/workflows/release.yml || true)"
  if [[ -z "$expected" ]]; then
    echo "FAIL: .github/workflows/release.yml no longer declares EVIDENCE_ARTIFACT." >&2
    exit 1
  fi

  uploaded="$(rg -n --no-heading "name: ${expected}\$" .github/workflows/ci.yml || true)"
  if [[ -z "$uploaded" ]]; then
    echo "FAIL: release.yml gates on artifact '${expected}', which ci.yml never uploads." >&2
    echo "The release would fail at the tag. Keep both ends of the name in step." >&2
    exit 1
  fi
  echo "Release evidence contract check passed (ci.yml uploads '${expected}')."
}

run_cargo_scope_check() {
  # A cargo command that names a target or a feature must also name a package.
  #
  # Without `-p`, cargo selects every default workspace member and unifies features
  # across the whole set — so a flag meant for one crate silently reshapes another
  # crate's build. For example, an unscoped `cargo bench` pulls in `rustcdc-server`'s
  # dev-dependency on `rustcdc/test-harnesses` and trips the release guard in
  # `fault_injection`; an unscoped `cargo test --test <mysql suite>` enables both rustls
  # providers and panics on the CryptoProvider. Neither fails at the seam that is wrong,
  # nor under the scoped command CI runs, which is why this is a static check.
  #
  # `--workspace` counts as naming the scope: that is the deliberate whole-workspace case.
  local hits
  hits="$(rg -n --no-heading --hidden -e 'cargo (build|test|bench|run) ' \
    --glob 'scripts/**/*.sh' \
    --glob '.github/workflows/*.yml' \
    --glob '!scripts/ci-policy-gate.sh' \
    . \
    | grep -E -- '--features|--test |--bench |--bin |--example ' \
    | grep -v -E -- '(-p |--package|--workspace)' \
    | grep -v -E '^[^:]+:[0-9]+: *#' \
    | grep -v -E '^[^:]+:[0-9]+: *(- )?name:' || true)"

  if [[ -n "$hits" ]]; then
    echo "FAIL: cargo invocation selects a target or feature without naming a package." >&2
    echo "Add \`-p <crate>\` (or \`--workspace\` if every member is meant):" >&2
    printf '%s\n' "$hits" >&2
    exit 1
  fi
  echo "Cargo scope check passed (every targeted cargo command names its package)."
}

run_bench_invocation_check() {
  # Benchmarks build in the release profile, where the guard in `fault_injection` rejects
  # `test-harnesses` — and `rustcdc-server` dev-depends on `rustcdc` with that feature, so
  # cargo's feature unification turns it on for any unscoped build. Two bench targets are
  # also named `throughput`, one per crate, so `--bench throughput` is ambiguous.
  # `cargo xtask bench` carries the hatch and the `-p`.
  #
  # Allowed to say `cargo bench`: the xtask that wraps it, the gate that scopes it, the
  # instructions that explain why not to, and CHANGELOG.md, which is a record of what was
  # true when written and is not rewritten.
  local hits
  hits="$(rg -n --no-heading --hidden -e 'cargo bench' \
    --glob '!target/**' \
    --glob '!.git/**' \
    --glob '!CHANGELOG.md' \
    --glob '!crates/xtask/src/main.rs' \
    --glob '!scripts/ci-benchmark-gate.sh' \
    --glob '!scripts/ci-policy-gate.sh' \
    --glob '!AGENTS.md' \
    . | grep -v 'cargo xtask bench' || true)"

  if [[ -n "$hits" ]]; then
    echo "FAIL: bare \`cargo bench\` is not a working command in this repository." >&2
    echo "Use \`cargo xtask bench\` (add \`-p <crate> --bench <name>\` to narrow):" >&2
    printf '%s\n' "$hits" >&2
    exit 1
  fi
  echo "Bench invocation check passed (no bare \`cargo bench\` outside its wrapper)."
}

run_job_naming_check() {
  local failed=0

  while IFS= read -r hit; do
    local file value
    file="${hit%%:*}"
    value="$(printf '%s' "$hit" | sed -E 's/.*name:[[:space:]]*//; s/^"//; s/"$//')"
    case "$value" in
      lib:*|server:*|workspace:*|release:*|CI) ;;
      *)
        echo "FAIL: ${file}: job name \"${value}\" has no scope prefix \
(lib: / server: / workspace: / release:)" >&2
        failed=1
        ;;
    esac
  done < <(rg -n --no-heading -e '^    name:' .github/workflows/ci.yml .github/workflows/release.yml || true)

  if [[ "$failed" -ne 0 ]]; then
    exit 1
  fi
  echo "Job naming check passed (every job names the half it covers)."
}

run_workflow_path_check() {
  local failed=0

  while IFS= read -r hit; do
    local file value
    file="${hit%%:*}"
    value="$(printf '%s' "$hit" | sed -E 's/.*:[[:space:]]*//')"
    # Absolute paths are created by the job itself (`/tmp/digests`), not by the repository.
    case "$value" in /*) continue ;; esac
    if [[ ! -e "$value" ]]; then
      echo "FAIL: ${file} names a path that does not exist: ${value}" >&2
      failed=1
    fi
  done < <(rg -n --no-heading -e '^\s*working-directory:\s*\S+' .github/workflows/ || true)

  while IFS= read -r hit; do
    local file value
    file="${hit%%:*}"
    value="$(printf '%s' "$hit" | sed -E 's/.*uses:[[:space:]]*//; s/@.*//')"
    if [[ ! -e "$value" ]]; then
      echo "FAIL: ${file} uses a local action that does not exist: ${value}" >&2
      failed=1
    fi
  done < <(rg -n --no-heading -e '^\s*-?\s*uses:\s*\./' .github/workflows/ || true)

  if [[ "$failed" -ne 0 ]]; then
    exit 1
  fi
  echo "Workflow path check passed (every referenced path exists)."
}

run_licence_presence_check() {
  local failed=0

  while IFS= read -r manifest; do
    local dir
    dir="$(dirname "$manifest")"
    # Only crates that are actually published; `publish = false` ships no artefact.
    if rg -q '^publish = false' "$manifest"; then
      continue
    fi
    for licence in LICENSE-MIT LICENSE-APACHE; do
      if [[ ! -f "$dir/$licence" ]]; then
        echo "FAIL: $dir is published but has no $licence; cargo package collects only \
files beneath the crate directory, so the published artefact would carry the SPDX \
expression without the grant it names" >&2
        failed=1
      elif ! diff -q "$licence" "$dir/$licence" >/dev/null; then
        echo "FAIL: $dir/$licence differs from the repository's $licence" >&2
        failed=1
      fi
    done
  done < <(find crates -mindepth 2 -maxdepth 2 -name Cargo.toml | sort)

  if [[ "$failed" -ne 0 ]]; then
    exit 1
  fi
  echo "Licence presence check passed (every published crate carries both texts)."
}

# The release pipeline's job graph, asserted as a graph rather than as four loose
# greps for "needs: <something>", which pass no matter which job carries which edge.
#
# Compared in file order, so the jobs must also be *written* in the order they run. A
# release file that reads top to bottom is the whole point of having one.
require_release_job_order() {
  local expected actual
  # Build first (proves both architectures compile, pushing only untagged digests), then
  # the irreversible crates.io publish, then the image tagging that makes it consumable,
  # then the announcement. See the ordering note at the top of release.yml.
  # The binaries are a build too, so they gate the crate like the image does; Homebrew
  # follows the GitHub release whose assets its formula downloads.
  expected="container-build=verify
binary-build=verify
publish-crate=[container-build, binary-build]
container-publish=publish-crate
github-release=container-publish
homebrew-tap=github-release
homebrew-install=homebrew-tap"

  actual="$(awk '
    /^jobs:$/            { in_jobs = 1; next }
    in_jobs && /^  [a-z][a-z0-9-]*:$/ { job = substr($1, 1, length($1) - 1); next }
    in_jobs && /^    needs: / && job != "" { sub(/^    needs: /, ""); print job "=" $0; job = "" }
  ' "$RELEASE_WORKFLOW")"

  if [[ "$actual" != "$expected" ]]; then
    echo "FAIL: release.yml job order changed." >&2
    echo "expected:" >&2
    printf '  %s\n' "$expected" >&2
    echo "actual:" >&2
    printf '  %s\n' "$actual" >&2
    exit 1
  fi
  echo "Release job order check passed (builds -> crates.io -> image tags -> release -> Homebrew)."
}

run_workflow_drift_check() {
  require_file_absent ".github/workflows/publish.yml"
  require_file_absent ".github/workflows/nightly-evidence.yml"
  require_file_absent ".github/workflows/benchmark-baseline-refresh.yml"

  require_match "^name: ci$" "$CI_WORKFLOW" "single workflow name"
  require_match "^  pull_request:$" "$CI_WORKFLOW" "pull request trigger"
  require_match "^  push:$" "$CI_WORKFLOW" "push trigger"
  require_match "bash scripts/ci-policy-gate.sh" "$CI_WORKFLOW" "policy gate"
  # Both doc lanes, not just one. The all-features build is blind to a link from an
  # ungated doc comment into a feature-gated item, because every gate is on; the
  # no-default-features build is what catches it.
  #
  # Anchored on `run:` rather than on the bare command: a step's `name:` usually repeats
  # the command, so an unanchored pattern is satisfied by the label alone and would still
  # match after the command itself was changed.
  require_match "^ +run: cargo doc -p rustcdc --all-features --no-deps$" "$CI_WORKFLOW" "all-features doc build"
  require_match "^ +run: cargo doc -p rustcdc --no-default-features --no-deps$" "$CI_WORKFLOW" "no-default-features doc build"
  require_match "bash scripts/ci-pull-relational-images.sh --relational-smoke" "$CI_WORKFLOW" "relational smoke image pull mode"
  require_match "bash scripts/ci-benchmark-gate.sh" "$CI_WORKFLOW" "benchmark policy gate"
  require_match "bash scripts/run_full_integration_matrix_evidence.sh" "$CI_WORKFLOW" "full matrix evidence run"
  require_match "BENCHMARK_ENFORCE_RELEASE_POLICY: \"1\"" "$CI_WORKFLOW" "benchmark policy enforcement"
  require_match "  release-evidence:" "$CI_WORKFLOW" "release evidence job"
  # The evidence the release gate reads is produced here; the gate that reads it lives in
  # `release.yml`, and asserting both ends keeps them from drifting apart.
  require_match "  release-evidence:" "$CI_WORKFLOW" "release evidence job"
  require_match "  package:" "$CI_WORKFLOW" "packaging job"
  require_match "verify successful release-evidence run exists for this commit" \
    "$RELEASE_WORKFLOW" "the release gate checks CI passed for this exact commit"
  require_match "does not match Cargo.toml version" "$RELEASE_WORKFLOW" "tag/version agreement check"
  # Every job in `release-evidence`'s `needs:` is a gate the release depends on
  # transitively. These four are the ones whose loss would be least visible.
  for required_gate in quality policy-gate server-test server-check; do
    if ! rg -q "^      - ${required_gate}$|^ +- ${required_gate}$" "$CI_WORKFLOW"; then
      echo "FAIL: ${required_gate} is not required by anything in ${CI_WORKFLOW}" >&2
      exit 1
    fi
  done
  # `cargo package` builds from the collected copy, which is the only thing that catches a
  # crate compiling in the repository but not from the registry. Running it on every pull
  # request is what keeps that discovery off the release tag.
  require_match "^ +run: cargo package -p rustcdc --locked$" "$CI_WORKFLOW" "packaged crate is built"

  # Repository-level gates must cover *both* workspace members; scoping either to one
  # package leaves the other unlinted.
  require_match "^ +run: cargo fmt --all --check$" "$CI_WORKFLOW" "workspace-wide formatting"
  require_match "^ +run: cargo clippy --workspace --all-targets --all-features -- -D warnings$" \
    "$CI_WORKFLOW" "workspace-wide clippy"
  # The MSRV job must *derive* the toolchain from the manifest rather than restate it:
  # two sources of truth for one number drift.
  require_match "steps.msrv.outputs.version" "$CI_WORKFLOW" "MSRV derived from the manifest"

  # One workflow for the whole workspace. Two files could not express the only thing
  # that matters at release time — `needs:` cannot name a job in another workflow — so a
  # tag published the library with the server's tests unverified, and a third workflow
  # pushed the container image depending on nothing at all.
  require_file_absent ".github/workflows/server-ci.yml"
  require_file_absent ".github/workflows/publish-container.yml"
  require_match "  server-connector-matrix:" "$CI_WORKFLOW" "server connector feature matrix"
  require_match "  server-integration:" "$CI_WORKFLOW" "server integration job"
  require_match "  server-fuzz:" "$CI_WORKFLOW" "server fuzz smoke job"

  # The release chain, in order. Each link is a property, not a style preference:
  #
  #   container-smoke   the image builds — checked on every pull request, so the risky
  #                     part is known good long before the irreversible one
  #   publish           crates.io. Irreversible: a version can never be reused, not even
  #                     after a yank, so it goes last among steps that can still fail
  #   container-build   only after the crate exists, so no image advertises a release
  #                     crates.io never accepted. A push is idempotent; a re-run fixes it
  #   github-release    last, because it is the announcement
  require_match "  container-smoke:" "$CI_WORKFLOW" "container build on pull requests"
  require_match "^ +push: false$" "$CI_WORKFLOW" "the pull-request container build does not push"

  # One required status check, and it must tolerate skipped jobs. GitHub leaves a
  # required check pending forever when its job never runs, and treats a *skipped*
  # required check as success — so the aggregator needs `if: always()` or it inverts.
  require_match "  required-checks-passed:" "$CI_WORKFLOW" "branch-protection aggregator"
  require_match "^    if: always\(\)$" "$CI_WORKFLOW" "the aggregator runs even when a dependency fails"

  # ── The release, and its ordering ───────────────────────────────────────────
  #
  require_file_present ".github/workflows/release.yml"
  require_match "^name: release$" "$RELEASE_WORKFLOW" "release workflow name"
  require_release_job_order

  # The tag trigger belongs to release.yml and to nothing else. A tag trigger in ci.yml
  # would re-run the whole matrix on a commit main already tested, and release.yml reads
  # evidence from the run on main anyway.
  require_match "^    tags:$" "$RELEASE_WORKFLOW" "release is what a tag triggers"
  require_absent "^      - \"v\\*\"$" "$CI_WORKFLOW" "tag trigger in ci.yml (releases are release.yml's job)"
  require_match "\-\-notes-file release-notes.md" "$RELEASE_WORKFLOW" "release notes come from CHANGELOG.md"
  # The tap token lives only in the `homebrew` environment, and the job holding it pins
  # every action by commit.
  require_match "^    environment: homebrew$" "$RELEASE_WORKFLOW" "Homebrew job runs in the homebrew environment"
  require_absent "HOMEBREW_APP_PRIVATE_KEY" "$CI_WORKFLOW" "Homebrew secret outside release.yml"
  # Trusted publishing pins the workflow *filename*, which is why the release lives in
  # its own file rather than in ci.yml: a compromised action in the test matrix must not
  # be able to mint a crates.io token.
  require_match "rust-lang/crates-io-auth-action" "$RELEASE_WORKFLOW" "crates.io trusted publishing"
  require_match "^      id-token: write$" "$RELEASE_WORKFLOW" "OIDC identity for trusted publishing"
  if rg -qF 'CARGO_REGISTRY_TOKEN: ${{ secrets' "$RELEASE_WORKFLOW"; then
    echo "FAIL: the release still uses a long-lived crates.io secret; trusted publishing \
mints a short-lived token instead" >&2
    exit 1
  fi

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

# Database and broker versions are named in one place, crates/rustcdc/tests/support/matrix.txt.
#
# Every suite, the CI pre-pull and the workflows read it, so moving the matrix is one edit and
# nothing can lag behind it. The published supported-versions page and the examples are written
# by hand, so this holds them to it: a page that lists versions the suites no longer run is the
# drift `cargo xtask support-window` exists to catch, one step removed.
run_relational_image_drift_check() {
  local matrix="crates/rustcdc/tests/support/matrix.txt"
  local page="site/content/docs/supported-versions.md"
  local hits
  hits="$(git grep -nE 'GenericImage::new\("(postgres|mysql|mariadb)", *"|"(postgres|mysql|mariadb):[0-9]|mssql/server:20[0-9]{2}|redpanda:v[0-9]|apache/kafka:[0-9]|"[0-9]+-alpine"|ci-pull-relational-images\.sh +[0-9]|--sqlserver-soft +20' \
    -- crates/*/tests scripts .github ':!scripts/ci-policy-gate.sh' || true)"
  if [[ -n "$hits" ]]; then
    echo "FAIL: a database or broker version is named outside ${matrix}:" >&2
    echo "$hits" >&2
    echo "Read it from the matrix instead (tests: support/matrix.rs; scripts: ci-pull-relational-images.sh)." >&2
    exit 1
  fi

  local missing=()
  local engine full tags label row tag version
  while read -r engine full tags; do
    case "$engine" in
      postgres) label="PostgreSQL" ;;
      mysql) label="MySQL" ;;
      mariadb) label="MariaDB" ;;
      sqlserver) label="SQL Server" ;;
      kafka) label="Apache Kafka" ;;
      redpanda) label="Redpanda" ;;
      *) echo "FAIL: unknown engine '${engine}' in ${matrix}" >&2; exit 1 ;;
    esac
    row="$(grep -E "^\| ${label} \|" "$page" || true)"
    for tag in $full $tags; do
      version="${tag%-alpine}"; version="${version#v}"
      [[ "$engine" == "sqlserver" ]] && version="${tag:0:4}"
      grep -qE "(^|[^0-9.])${version//./\\.}([^0-9.]|$)" <<<"$row" || missing+=("${label} ${version}")
    done
    if [[ "$engine" == "postgres" ]]; then
      local major="${full%%-*}" example
      for example in docker/compose.example.yml demo/compose.yml demo/seed.sh site/content/docs/getting-started.md site/content/docs/embedding.md; do
        if grep -oE 'postgres:[0-9]+' "$example" | grep -vqx "postgres:${major}"; then
          missing+=("${example}: an example names a PostgreSQL other than ${major}")
        fi
      done
    fi
  done < <(grep -vE '^[[:space:]]*(#|$)' "$matrix")

  if (( ${#missing[@]} > 0 )); then
    echo "FAIL: the published versions disagree with ${matrix}:" >&2
    printf '  - %s\n' "${missing[@]}" >&2
    echo "Update the table in ${page} (and the examples) to match." >&2
    exit 1
  fi

  echo "Database version check passed (one matrix; the published page and the examples agree)."
}

# Every integration suite under crates/rustcdc/tests/ must actually be run by something.
#
# The checks above are an *allow-list*: they assert that named suites appear in the
# workflow. That is silent about suites nobody added — and a test that never runs is
# indistinguishable from a test that does not exist, while looking like evidence in a
# review.
#
# A suite counts as covered when its name appears in the CI workflow, in a script the
# workflow runs, or as a `#[path]`-included helper module of another suite. Anything else
# must be added to one of those, or listed in HELPER_SUITES with a reason.
run_test_suite_coverage_check() {
  # Helper modules included by other suites via `#[path = "..."] mod ...;`. Cargo also
  # builds each as its own (empty) test binary, so they appear in crates/rustcdc/tests/ without being
  # suites in their own right.
  local helper_suites=(
    latency_evidence_common
    process_crash_marker
    process_crash_worker
    sqlserver_testkit
  )

  local uncovered=()
  local suite
  for path in crates/rustcdc/tests/*.rs; do
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
    if grep -rq "path = \"${suite}.rs\"" crates/rustcdc/tests/; then
      continue
    fi

    uncovered+=("$suite")
  done

  if (( ${#uncovered[@]} > 0 )); then
    echo "FAIL: integration suites are never run by CI or any script:" >&2
    for suite in "${uncovered[@]}"; do
      echo "  - crates/rustcdc/tests/${suite}.rs" >&2
    done
    echo "Add each to a matrix in ${CI_WORKFLOW}, to a script CI runs, or to" >&2
    echo "helper_suites in scripts/ci-policy-gate.sh with a reason." >&2
    exit 1
  fi

  echo "Test suite coverage check passed (every crates/rustcdc/tests/*.rs is run by CI or a script)."
}

# Every public field of a user-facing config struct must appear in the configuration
# reference. A field nobody can find is a field nobody sets, and the defaults here are
# load-bearing.
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

  check_struct_fields_documented "crates/rustcdc/src/core/runtime.rs" "RuntimeConfig"
  check_struct_fields_documented "crates/rustcdc/src/core/runtime.rs" "RuntimeOptions"
  check_struct_fields_documented "crates/rustcdc/src/source/postgres.rs" "PostgresSourceConfig"
  check_struct_fields_documented "crates/rustcdc/src/source/mysql.rs" "MysqlSourceConfig"
  check_struct_fields_documented "crates/rustcdc/src/source/sqlserver.rs" "SqlServerSourceConfig"
  check_struct_fields_documented "crates/rustcdc/src/source/snowflake.rs" "SnowflakeSourceConfig"

  if [[ "$failed" -gt 0 ]]; then
    echo "config docs coverage check failed: $failed undocumented field(s)" >&2
    exit 1
  fi

  echo "Config docs coverage check passed."
}

run_markdown_link_check
# A public type inside a submodule that its parent never re-exports is unreachable in
# practice: `codec::schema_registry` is `pub`, but every doc example and every downstream
# import names `codec::…`.
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
  # Module→parent alone can leave an item reachable only as `rustcdc::codec::X` while
  # its direct counterpart is `rustcdc::X` (say, `AvroDecoder` next to `AvroEncoder`),
  # which costs a docs search per item and makes the surface look arbitrary.
  #
  # The rule is **all-or-nothing per module**, and it configures itself: if `crates/rustcdc/src/lib.rs`
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
    --replace '$1' crates/rustcdc/src | sort -u)"

  # Names re-exported by `<module>/mod.rs` must also be named by `crates/rustcdc/src/lib.rs`.
  check_crate_root_reexports() {
    local module_mod="$1"
    local module_name
    module_name="$(basename "$(dirname "$module_mod")")"

    # Skip modules with no crate-root surface at all — they are namespaced by design.
    if ! rg -q "^pub use crate::${module_name}::" crates/rustcdc/src/lib.rs; then
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
      if ! rg -q "\\b$item\\b" crates/rustcdc/src/lib.rs; then
        echo "crate-root parity: $module_mod re-exports \`$item\` but crates/rustcdc/src/lib.rs never names it" >&2
        failed=$((failed + 1))
      fi
    done <<< "$names"
  }

  check_module_reexports "crates/rustcdc/src/codec/schema_registry.rs" "crates/rustcdc/src/codec/mod.rs"
  check_module_reexports "crates/rustcdc/src/codec/avro.rs" "crates/rustcdc/src/codec/mod.rs"
  check_module_reexports "crates/rustcdc/src/codec/json.rs" "crates/rustcdc/src/codec/mod.rs"
  check_module_reexports "crates/rustcdc/src/source/incremental_snapshot/driver.rs" "crates/rustcdc/src/source/mod.rs"

  # Every module directory; the function itself skips those with no crate-root surface.
  for module_mod in crates/rustcdc/src/*/mod.rs; do
    check_crate_root_reexports "$module_mod"
  done

  if [[ "$failed" -gt 0 ]]; then
    echo "re-export coverage check failed: $failed unreachable public item(s)" >&2
    exit 1
  fi

  echo "Re-export coverage check passed."
}

run_published_docs_history_check() {
  # Published docs describe current behaviour; history belongs in CHANGELOG.md. A page that
  # narrates releases ("until 0.12", "used to", "we reported") reads as a changelog and goes
  # stale with every release. A line that must name a past version carries <!-- history-ok -->.
  local pattern hits
  pattern='([Uu]ntil|[Ss]ince|[Bb]efore|[Aa]s of) (rustcdc |v)?0\.[0-9]+([^0-9]|$)|[Ss]ince [a-z][a-z0-9_-]* [0-9]+\.[0-9]|[Aa]n earlier (version|release)|[Ee]arlier releases|(^|[.:] )(Previously|Formerly)|[Uu]sed to (be|say|call|read|return|accept|reject|fail)|(was|did) not always|[Tt]he previous (implementation|representation|release|behaviou?r)|[Ww]e reported|[a-z]+ 0\.[0-9]+ (removed|added|fixed|changed)|its 0\.[0-9]+ release|[ (]now (fails|rejects|refuses|carries|emits) |[Nn]o [a-z ]*suite yet|[Uu]ntil it does'
  hits="$(git grep -nE "$pattern" -- 'site/content/**/*.md' README.md 'crates/*/README.md' 'site/templates/*.html' \
    | grep -v 'history-ok' || true)"
  if [[ -n "$hits" ]]; then
    echo "published docs history check failed: describe current behaviour; history goes in CHANGELOG.md" >&2
    echo "$hits" >&2
    exit 1
  fi
  echo "Published docs history check passed (no release narration in site/, READMEs or templates)."
}

run_floating_image_tag_check() {
  # No container image a suite, script or workflow pulls may use a `latest` tag, which
  # tests a different server on each pull, so a failure can be neither reproduced nor
  # attributed. (Version tags such as `16-alpine` still take patch releases.) Tracked files
  # only; comments are skipped, and the release workflow's `:latest` is the tag rustcdc
  # *publishes*, not one it pulls.
  # Image references only: `repo/name:…latest`, a quoted `"…latest"` tag passed to a
  # container helper in test code, and the CI pre-pull argument. A runner label such as
  # `ubuntu-latest` is none of these.
  local hits
  hits="$( {
    git grep -nE '[a-z0-9.-]+/[a-z0-9._/-]+:[A-Za-z0-9._-]*latest([^A-Za-z0-9_]|$)' -- \
      'crates/*/tests/*' 'scripts/*' 'docker/*' 'demo/*' '.github/workflows/*' \
      ':!.github/workflows/release.yml' ':!scripts/ci-policy-gate.sh'
    git grep -nE '"[^"]*latest"' -- 'crates/*/tests/*.rs'
    git grep -nE -- '--sqlserver-soft [^ ]*latest' -- '.github/workflows/*'
  } | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(#|//)' || true)"
  if [[ -n "$hits" ]]; then
    echo "floating image tag check failed: pin these to a version" >&2
    echo "$hits" >&2
    exit 1
  fi
  echo "Floating image tag check passed (no :latest or -latest image in tests, scripts or workflows)."
}

run_rust_version_pin_check() {
  # Every Rust toolchain pinned in a Dockerfile must be the workspace MSRV.
  #
  # CI derives its own toolchain from `rust-version`, and the main Dockerfile tracks it by
  # hand — but nothing compared the two, so `docker/Dockerfile.example` sat two minor
  # versions *below* the MSRV and could not have built the crate it demonstrates. A pin
  # that drifts down is a broken example; one that drifts up silently raises the real
  # minimum past what the manifest promises downstream.
  local msrv
  msrv="$(sed -n 's/^rust-version *= *"\(.*\)"/\1/p' Cargo.toml | head -1)"
  if [[ -z "$msrv" ]]; then
    echo "rust version pin check failed: no rust-version in [workspace.package]" >&2
    exit 1
  fi

  local failed=0
  local checked=0
  while IFS= read -r dockerfile; do
    while IFS= read -r pinned; do
      checked=$((checked + 1))
      if [[ "$pinned" != "$msrv" ]]; then
        echo "rust pin in $dockerfile is $pinned, but Cargo.toml rust-version is $msrv" >&2
        failed=$((failed + 1))
      fi
    done < <(rg --no-line-number --no-filename --pcre2 -o \
      '(?:FROM rust:|ARG RUST_VERSION=)([0-9]+\.[0-9]+(?:\.[0-9]+)?)' \
      -r '$1' "$dockerfile" 2>/dev/null)
  # Tracked files only: the verdict is about the tree under review. A filesystem walk also
  # read gitignored worktrees and scratch copies, so the gate failed on files no commit has.
  done < <(git ls-files -- ':(glob)**/Dockerfile*' | sort)

  if [[ "$failed" -gt 0 ]]; then
    echo "rust version pin check failed: $failed pin(s) disagree with the manifest" >&2
    exit 1
  fi

  echo "Rust version pin check passed ($checked pin(s) match rust-version $msrv)."
}

run_config_docs_coverage_check
run_reexport_coverage_check
run_schema_contract_check
run_deprecated_usage_check
run_async_trait_policy_check
run_cargo_profile_safety_check
run_job_naming_check
run_bench_invocation_check
run_cargo_scope_check
run_release_evidence_contract_check
run_workflow_path_check
run_licence_presence_check
run_workflow_drift_check
run_rust_version_pin_check
run_floating_image_tag_check
run_published_docs_history_check

echo "Policy gate passed."
