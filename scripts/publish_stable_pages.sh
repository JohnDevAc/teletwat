#!/usr/bin/env bash
set -euo pipefail

expected_commit="${1:?Expected publication commit is required}"
attempts="${2:-60}"
repository="${GH_REPO:?GH_REPO is required}"
public_url="https://johndevac.github.io/TeleTool/apt-repo/dists/stable/InRelease"

# GITHUB_TOKEN pushes do not trigger legacy Pages builds automatically.
gh api --method POST "repos/$repository/pages/builds" >/dev/null
for ((attempt=0; attempt<attempts; attempt++)); do
  build="$(gh api "repos/$repository/pages/builds/latest" --jq '[.commit, .status] | @tsv')"
  read -r commit status <<<"$build"
  if [ "$commit" = "$expected_commit" ]; then
    if [ "$status" = "errored" ]; then
      echo "GitHub Pages failed to build the published stable release." >&2
      exit 1
    fi
    if [ "$status" = "built" ] && cmp -s apt-repo/dists/stable/InRelease \
      <(curl --fail --silent --show-error --max-time 10 "$public_url?release=$expected_commit"); then
      echo "The public stable APT site matches the signed publication."
      exit 0
    fi
  fi
  sleep 5
done
echo "The public stable APT site did not reach the expected signed release." >&2
exit 1
