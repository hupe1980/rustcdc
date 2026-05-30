#!/usr/bin/env bash
set -euo pipefail

# Pull PostgreSQL images from public ECR and tag them as postgres:<tag>.
# Usage:
#   bash scripts/ci-pull-postgres-images.sh 16-alpine
#   bash scripts/ci-pull-postgres-images.sh 12-alpine 14-alpine 15-alpine 16-alpine

if [[ "$#" -eq 0 ]]; then
  echo "usage: $0 <tag> [<tag> ...]" >&2
  exit 2
fi

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

for tag in "$@"; do
  image="public.ecr.aws/docker/library/postgres:${tag}"
  pull_with_retry "$image"
  docker tag "$image" "postgres:${tag}"
  echo "prepared postgres:${tag} from ${image}"
done
