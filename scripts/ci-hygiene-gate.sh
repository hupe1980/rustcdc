#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

failures=0

forbid_match() {
  local pathspec="$1"
  local pattern="$2"
  local description="$3"

  if rg -n --pcre2 "$pattern" $pathspec >/dev/null; then
    echo "hygiene gate failed: $description"
    rg -n --pcre2 "$pattern" $pathspec || true
    failures=$((failures + 1))
  fi
}

# Deprecated markers should not be introduced in tracked source and tests.
forbid_match "src tests" "#\s*\[\s*deprecated\b|#!\s*\[\s*deprecated\b|allow\s*\(\s*deprecated\s*\)" "deprecated attributes/lint suppressions are not allowed in src/ or tests/"

# Keep production and test code free of unresolved debt markers.
forbid_match "src tests" "\bTODO\b|\bFIXME\b|\bHACK\b" "TODO/FIXME/HACK markers are not allowed in src/ or tests/"

# Connector SQL format strings must not interpolate raw identifier-like variables.
# Dynamic SQL is only allowed through pre-validated/quoted helper outputs.
forbid_match \
  "src/source/postgres.rs src/source/mysql.rs src/source/sqlserver.rs" \
  "format!\(\s*\"[^\"]*(SELECT|FROM|JOIN|UPDATE|DELETE|INSERT)[^\"]*\{(table|schema|name|table_name|schema_name|column|columns)\}" \
  "raw identifier interpolation in connector SQL format strings is not allowed; use validated table_ref/quoted helper outputs"

if [[ "$failures" -gt 0 ]]; then
  echo "hygiene gate failed: $failures issue(s)"
  exit 1
fi

echo "hygiene gate passed"
