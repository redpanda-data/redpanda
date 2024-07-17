#!/bin/bash
set -eo pipefail

git_version=$(git describe --always --match=v*)
echo "GIT_VERSION ${git_version}"

git_sha1=$(git rev-parse HEAD)
echo "GIT_SHA1 ${git_sha1}"

if git diff-index --quiet HEAD --; then
  echo "GIT_CLEAN_DIRTY "
else
  echo "GIT_CLEAN_DIRTY -dirty"
fi
