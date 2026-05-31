#!/usr/bin/env bash
set -euo pipefail

# Pull Docker images used by connector CI lanes with retry.
# Usage:
#   bash scripts/ci-pull-relational-images.sh 16-alpine
#   bash scripts/ci-pull-relational-images.sh 12-alpine 14-alpine 15-alpine 16-alpine
#   bash scripts/ci-pull-relational-images.sh --relational-smoke

# Non-postgres smoke images. Postgres tags are passed explicitly as positional args.
NON_POSTGRES_RELATIONAL_SMOKE_IMAGES=(
  "mysql:8.0"
  "mysql:8.1"
  "mariadb:10.6"
  "mariadb:10.11"
  "mcr.microsoft.com/mssql/server:2019-latest"
)

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
  echo "usage: $0 <tag> [<tag> ...] | --relational-smoke" >&2
  exit 2
fi

if [[ "${1:-}" == "--relational-smoke" ]]; then
  for image in "${NON_POSTGRES_RELATIONAL_SMOKE_IMAGES[@]}"; do
    pull_with_retry "$image"
    echo "prepared ${image}"
  done
  exit 0
fi

for tag in "$@"; do
  image="public.ecr.aws/docker/library/postgres:${tag}"
  pull_with_retry "$image"
  docker tag "$image" "postgres:${tag}"
  echo "prepared postgres:${tag} from ${image}"
done
