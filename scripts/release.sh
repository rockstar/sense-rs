#!/usr/bin/env bash
# Tag and push a release, which kicks off the release workflow.
#
# If the version in Cargo.toml is already untagged (e.g. it was bumped by hand
# to satisfy cargo-semver-checks), that version is tagged and released as-is.
# Otherwise the version is bumped (patch by default) and committed first.
#
# Usage: scripts/release.sh [major|minor|patch|X.Y.Z]
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if [ $# -gt 1 ]; then
  echo "usage: $0 [major|minor|patch|X.Y.Z]" >&2
  exit 1
fi
bump="${1:-}"

if [ "$(git branch --show-current)" != "main" ]; then
  echo "error: releases must be cut from main" >&2
  exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
  echo "error: working tree is not clean" >&2
  exit 1
fi

git fetch --quiet origin main
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
  echo "error: main is not in sync with origin/main" >&2
  exit 1
fi

# pkgid ends in "#<version>" or "#<name>@<version>".
crate_version() {
  cargo pkgid | sed 's/.*[#@]//'
}

tag_exists() {
  git rev-parse -q --verify "refs/tags/$1" >/dev/null
}

confirm() {
  read -r -p "$1 [y/N] " answer
  [[ "$answer" =~ ^[Yy]$ ]]
}

version=$(crate_version)

if ! tag_exists "$version"; then
  if [ -n "$bump" ]; then
    echo "error: Cargo.toml is already at untagged $version; run without an argument to release it" >&2
    exit 1
  fi

  confirm "Release $version (already set in Cargo.toml)?" || exit 1

  git tag -a "$version" -m "$version"
  git push origin "$version"
else
  if ! cargo set-version --help >/dev/null 2>&1; then
    echo "error: cargo-edit is required (cargo install cargo-edit)" >&2
    exit 1
  fi

  case "${bump:-patch}" in
    major | minor | patch) cargo set-version --bump "${bump:-patch}" ;;
    *) cargo set-version "$bump" ;;
  esac
  version=$(crate_version)

  if tag_exists "$version"; then
    echo "error: tag $version already exists" >&2
    git checkout -- Cargo.toml Cargo.lock
    exit 1
  fi

  if ! confirm "Release $version?"; then
    git checkout -- Cargo.toml Cargo.lock
    exit 1
  fi

  # Refresh Cargo.lock and make sure everything still builds.
  cargo check --all-targets --quiet

  git add Cargo.toml Cargo.lock
  git commit -m "chore: release $version"
  git tag -a "$version" -m "$version"
  git push --atomic origin main "$version"
fi

echo "Pushed $version; the release workflow will publish it to crates.io."
