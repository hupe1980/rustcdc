#!/usr/bin/env bash
set -euo pipefail

# Pull the database images the connector CI lanes need, with retry, from mirrors rather than
# rate-limited Docker Hub. The tags come from the matrix every suite reads, so this list cannot
# drift from what the tests start.
# Usage:
#   bash scripts/ci-pull-relational-images.sh postgres full     # the full-suite tag
#   bash scripts/ci-pull-relational-images.sh postgres tested   # every tested tag
#   bash scripts/ci-pull-relational-images.sh --relational-smoke  # every tested MySQL and MariaDB tag
#   bash scripts/ci-pull-relational-images.sh --sqlserver-soft [full|tested]
MATRIX="crates/rustcdc/tests/support/matrix.txt"

# matrix_tags <engine> full|tested
matrix_tags() {
  awk -v engine="$1" -v which="$2" '
    $1 == engine {
      if (which == "full") { print $2 } else { for (i = 3; i <= NF; i++) print $i }
      found = 1
    }
    END { if (!found) exit 1 }
  ' "$MATRIX" || { echo "no ${1} line in ${MATRIX}" >&2; exit 2; }
}

pull_with_retry() {
  local image="$1"
  local max_attempts="${CI_PULL_MAX_ATTEMPTS:-4}"
  local base_backoff_secs="${CI_PULL_BACKOFF_SECS:-10}"
  local attempt=1

  while true; do
    if docker pull "$image"; then
      return 0
    fi

    if [[ "$attempt" -ge "$max_attempts" ]]; then
      echo "failed to pull $image after $max_attempts attempts" >&2
      return 1
    fi

    local sleep_secs=$((attempt * base_backoff_secs))
    echo "retrying pull for $image in ${sleep_secs}s (attempt $((attempt + 1))/${max_attempts})" >&2
    sleep "$sleep_secs"
    attempt=$((attempt + 1))
  done
}

if [[ "$#" -eq 0 ]]; then
  echo "usage: $0 postgres full|tested | --relational-smoke | --sqlserver-soft [full|tested]" >&2
  exit 2
fi

if [[ "${1:-}" == "--relational-smoke" ]]; then
  images=()
  for tag in $(matrix_tags mysql tested); do images+=("mysql:${tag}"); done
  for tag in $(matrix_tags mariadb tested); do images+=("mariadb:${tag}"); done
  for image in "${images[@]}"; do
    # Pull from the ECR public mirror (no Docker Hub rate-limits / auth required)
    # and re-tag to the bare name so testcontainers finds it in the local cache.
    ecr_image="public.ecr.aws/docker/library/${image}"
    pull_with_retry "$ecr_image"
    docker tag "$ecr_image" "$image"
    echo "prepared ${image} from ${ecr_image}"
  done
  exit 0
fi

# Pull the SQL Server image with a soft failure: if MCR is unavailable
# (rate-limit, access denial, or retired tag), warn and exit 0 so that
# pre-pull failures do not block unrelated CI lanes.  The sqlserver tests
# themselves emit a "SKIP:" sentinel when the image cannot be pulled.
if [[ "${1:-}" == "--sqlserver-soft" ]]; then
  for tag in $(matrix_tags sqlserver "${2:-full}"); do
    image="mcr.microsoft.com/mssql/server:${tag}"
    if pull_with_retry "$image"; then
      echo "prepared ${image}"
    else
      echo "WARNING: could not pull ${image} — sqlserver tests will be skipped" >&2
    fi
  done
  exit 0
fi

if [[ "${1:-}" != "postgres" || ( "${2:-}" != "full" && "${2:-}" != "tested" ) ]]; then
  echo "usage: $0 postgres full|tested | --relational-smoke | --sqlserver-soft [full|tested]" >&2
  exit 2
fi

for tag in $(matrix_tags postgres "$2"); do
  image="public.ecr.aws/docker/library/postgres:${tag}"
  pull_with_retry "$image"
  docker tag "$image" "postgres:${tag}"
  echo "prepared postgres:${tag} from ${image}"
done
