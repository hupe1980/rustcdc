#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONNECTOR_FILES=(
  "src/source/postgres.rs"
  "src/source/mysql.rs"
  "src/source/sqlserver.rs"
)

echo "Checking connector internals for banned fully-qualified async-trait attribute usage..."
if rg -n '#\[async_trait::async_trait\]' "${CONNECTOR_FILES[@]}"; then
  echo "error: use imported #[async_trait] form in connector internals; fully-qualified attribute is disallowed" >&2
  exit 1
fi

echo "Async-trait policy checks passed."
