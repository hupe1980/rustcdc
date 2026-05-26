#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

failures=0

has_rg=0
if command -v rg >/dev/null 2>&1; then
  has_rg=1
fi

match_pattern() {
  local file="$1"
  local pattern="$2"
  if [[ "$has_rg" -eq 1 ]]; then
    rg -q --pcre2 "$pattern" "$file"
  else
    grep -E -q "$pattern" "$file"
  fi
}

require_match() {
  local file="$1"
  local pattern="$2"
  local description="$3"
  if ! match_pattern "$file" "$pattern"; then
    echo "docs contract check failed: $description ($file)"
    failures=$((failures + 1))
  fi
}

forbid_match() {
  local file="$1"
  local pattern="$2"
  local description="$3"
  if match_pattern "$file" "$pattern"; then
    echo "docs contract check failed: $description ($file)"
    failures=$((failures + 1))
  fi
}

# API capabilities must claim schema change support.
require_match "docs/api.md" "Schema change events\s*\|\s*✅\s*\|\s*✅\s*\|\s*✅\s*\|\s*✅" "schema change capability table must show support"
forbid_match "docs/api.md" "runtime connectors do not yet emit schema-change events" "stale schema-change unsupported note must not be present"

# Schema evolution docs must not reference non-existent surfaces.
forbid_match "docs/schema_evolution.md" "PersistentSchemaHistory|SchemaHistoryOptions" "non-existent schema history surfaces must not be documented"
require_match "docs/schema_evolution.md" "FileSchemaHistory" "schema evolution docs must include built-in durable backend"
require_match "docs/schema_evolution.md" "write-rename persistence" "schema durability semantics must be documented"

# Feature policy and config docs must align with current capability contract.
require_match "docs/feature_policy.md" "Runtime-emitted schema-change events\s*\|\s*Emitted by current relational connectors.*\|\s*Implemented" "feature policy must mark runtime schema-change events as implemented"
require_match "docs/config_reference.md" "configured PostgreSQL/MySQL/SQL Server sources" "config reference must describe configured relational source capability behavior"
require_match "docs/config_reference.md" "ddl_capture=true" "config reference must include configured source ddl_capture=true contract"

# Runbook recovery snippets must match strict checkpoint decoder requirements.
require_match "docs/runbook.md" '"checkpoint_format_version"\s*:\s*2' "runbook replacement checkpoint example must include checkpoint_format_version=2"
require_match "docs/runbook.md" "jq -e" "runbook recovery flow must validate checkpoint JSON schema before restart"
require_match "docs/runbook.md" "mv /var/rustcdc/checkpoint_postgres.json.new /var/rustcdc/checkpoint_postgres.json" "runbook recovery flow must atomically replace checkpoint file"

if [[ "$failures" -gt 0 ]]; then
  echo "docs contract check failed: $failures issue(s)"
  exit 1
fi

echo "docs contract check passed"
