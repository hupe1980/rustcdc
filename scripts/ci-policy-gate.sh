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
      elif git -C "$repo_root" check-ignore -q "$resolved" 2>/dev/null; then
        # Existence on the author's disk is not the test: a gitignored target is not in
        # the repository, so the link is broken for CI and for every reader while looking
        # fine locally. This is how a link to a local-only audit note reached a released
        # CHANGELOG — it resolved for the person who wrote it and for nobody else.
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
  # Comparing wire to wire is what this check always meant, and it is now four-way rather
  # than three: `EventWire` (what the JSON decoder reads) is also checked against the
  # hand-written `Serialize` (what the JSON encoder writes). That seam did not exist while
  # both directions were derived, and a field written but never read — or read but never
  # written — is exactly the silent envelope divergence this gate exists to catch.
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
  # This was awk, and on macOS it did nothing at all: the profile name was extracted with
  # gawk's three-argument `match($0, /…/, arr)`, a GNU extension that BSD awk rejects as a
  # syntax error. The program aborted, `|| true` swallowed it, and the gate printed
  # "passed" having read nothing — silently vacuous for every developer not on Linux, and
  # it stayed that way through a change that added the exact profile it rejects.
  #
  # A check whose correctness depends on which `awk` is installed is not a check.
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
# collects only files beneath the crate directory, so moving the library into `crates/`
# silently dropped both texts from the `.crate` — the metadata still claimed the licences
# while the artefact no longer contained them. Copies are the only option cargo offers;
# this is what stops the copies drifting from the originals.
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

  # Repository-level gates must cover *both* workspace members. Scoping either to one
  # package is how the server's 50k lines went unlinted for as long as it was a separate
  # repository with its own, narrower, CI.
  require_match "^ +run: cargo fmt --all --check$" "$CI_WORKFLOW" "workspace-wide formatting"
  require_match "^ +run: cargo clippy --workspace --all-targets --all-features -- -D warnings$" \
    "$CI_WORKFLOW" "workspace-wide clippy"
  # The MSRV job must *derive* the toolchain from the manifest rather than restate it.
  # Two sources of truth for one number is exactly how the previous pin drifted.
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
  # Reversible before irreversible. A crates.io version can never be overwritten,
  # deleted, or reused — `cargo yank` only stops new resolution — while a GHCR package
  # version can be deleted and restored. So the image ships first and the crate last: if
  # crates.io fails, the image is deleted and the same tag retried; the other order
  # spends a version number that can never be reclaimed.
  require_file_present ".github/workflows/release.yml"
  require_match "^name: release$" "$RELEASE_WORKFLOW" "release workflow name"
  require_match "    needs: verify" "$RELEASE_WORKFLOW" "the release is gated on the tag being verified"
  require_match "    needs: container-build" "$RELEASE_WORKFLOW" "the manifest follows the platform builds"
  require_match "    needs: container-publish" "$RELEASE_WORKFLOW" "crates.io is published after the image"
  require_match "    needs: publish-crate" "$RELEASE_WORKFLOW" "the GitHub release is last"
  require_match "\-\-notes-file release-notes.md" "$RELEASE_WORKFLOW" "release notes come from CHANGELOG.md"
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
  mysql_versions="$(grep -oE '"[0-9]+\.[0-9]+"' crates/rustcdc/tests/mysql_version_matrix.rs | tr -d '"' | sort -u)"
  mariadb_versions="$(grep -oE '"1[0-9]\.[0-9]+"' crates/rustcdc/tests/mariadb_e2e_integration.rs | tr -d '"' | sort -u)"

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

# Every integration suite under crates/rustcdc/tests/ must actually be run by something.
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

run_config_docs_coverage_check
run_reexport_coverage_check
run_schema_contract_check
run_deprecated_usage_check
run_async_trait_policy_check
run_cargo_profile_safety_check
run_licence_presence_check
run_workflow_drift_check

echo "Policy gate passed."
