#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

failed=0
checked=0

check_file() {
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
    esac

    local resolved
    if [[ "$target" = /* ]]; then
      resolved="$repo_root/$target"
    else
      resolved="$markdown_dir/$target"
    fi

    checked=$((checked + 1))
    if [[ ! -e "$resolved" ]]; then
      echo "broken markdown link in $markdown_file -> $target"
      failed=$((failed + 1))
    fi
  done < <(rg --no-line-number --no-filename --pcre2 -o '\[[^][]+\]\(([^)]+)\)' "$markdown_file")
}

check_file "README.md"
while IFS= read -r file; do
  check_file "$file"
done < <(find docs -type f -name '*.md' | sort)

if [[ "$failed" -gt 0 ]]; then
  echo "markdown link check failed: $failed broken links out of $checked checked"
  exit 1
fi

echo "markdown link check passed: $checked links checked"
