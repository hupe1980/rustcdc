#!/usr/bin/env bash
# Can this commit be tagged as a release?  release-ready.sh [<commit>]   (default: origin/main)
#
# Yes when CI's run on main for it succeeded and uploaded the release evidence. `release.yml`'s
# verify step runs this same script, so the answer here is the one the release will get.
#
# Exit 0 ready, 1 not ready, 2 cannot tell (GitHub did not answer). Needs `gh`, authenticated
# locally or through GH_TOKEN in Actions.
set -uo pipefail

EVIDENCE=release-gate-evidence
in_ci=${GITHUB_ACTIONS:-}

say_no() { # not ready: one line why, then optional hints
  if [[ -n $in_ci ]]; then echo "::error::$1"; else echo "not ready  $1"; fi
  shift
  for line in "$@"; do echo "           $line"; done
  exit 1
}
cannot_tell() {
  local why=$1
  if [[ $why == *"gh: "* ]]; then why="GitHub did not answer: ${why##*gh: }"; else why="GitHub did not answer: ${why%%$'\n'*}"; fi
  set -- "$why"
  if [[ -n $in_ci ]]; then echo "::error::cannot tell: $1"; else echo "can't tell $1"; fi
  echo "           Nothing is ready until GitHub answers."
  exit 2
}

if [[ $# -gt 0 ]]; then
  sha=$(git rev-parse --verify -q "$1^{commit}") || say_no "$1 is not a commit here (git fetch?)"
else
  git fetch -q origin main --tags 2>/dev/null || cannot_tell "git fetch origin failed"
  sha=$(git rev-parse origin/main)
fi
short=${sha:0:7}
subject=$(git log -1 --format=%s "$sha")
version=$(git show "$sha:Cargo.toml" | awk -F'"' '/^version = /{ print $2; exit }')
tag="v$version"

# A tag that already names another commit is a version nobody bumped.
if existing=$(git rev-parse -q --verify "refs/tags/$tag^{commit}"); then
  [[ $existing == "$sha" ]] || say_no "$tag already tags ${existing:0:7}; bump the version first"
  if [[ -z $in_ci ]]; then
    echo "tagged     $tag already tags $short ($subject); nothing to do"
    exit 0
  fi
fi

runs=$(gh api "repos/{owner}/{repo}/actions/workflows/ci.yml/runs?head_sha=$sha&event=push&per_page=20" \
  --jq '.workflow_runs[] | [.id, .status, (.conclusion // "-"), .created_at, .html_url] | @tsv' 2>&1) ||
  cannot_tell "$runs"
[[ -n $runs ]] || say_no "no CI run on main for $short ($subject): is it merged?"

# A run that passed and uploaded the evidence settles it, whatever else exists.
while IFS=$'\t' read -r id status conclusion created url; do
  [[ $status == completed && $conclusion == success ]] || continue
  has=$(gh api "repos/{owner}/{repo}/actions/runs/$id/artifacts?per_page=100" \
    --jq "[.artifacts[] | select(.name == \"$EVIDENCE\")] | length" 2>&1) ||
    cannot_tell "$has"
  if [[ $has -gt 0 ]]; then
    if [[ -n $in_ci ]]; then
      echo "Using validated evidence from $url"
    else
      echo "ready      $tag → $short ($subject)"
      echo "           CI on main passed · evidence uploaded · $url"
      echo "           git tag -a $tag -m \"rustcdc $version\" $short && git push origin $tag"
    fi
    exit 0
  fi
  say_no "CI on main passed for $short but uploaded no '$EVIDENCE': the release evidence job did not run" "$url"
done <<<"$runs"

# Newest first: the run that decides is the latest one.
IFS=$'\t' read -r id status conclusion created url <<<"$(head -1 <<<"$runs")"
if [[ $status != completed ]]; then
  minutes=$((($(date +%s) - $(date -d "$created" +%s 2>/dev/null || date -j -u -f '%Y-%m-%dT%H:%M:%SZ' "$created" +%s)) / 60))
  say_no "CI on main for $short is $status ($minutes min so far; a run takes ~70)" "$url"
fi
failed=$(gh api --paginate "repos/{owner}/{repo}/actions/runs/$id/jobs?per_page=100" \
  --jq '.jobs[] | select(.conclusion == "failure" and .name != "CI") | .name' 2>/dev/null |
  paste -sd ';' - | sed 's/;/; /g')
say_no "CI on main for $short ended $conclusion${failed:+: $failed}" "$url" "gh run rerun $id --failed"
