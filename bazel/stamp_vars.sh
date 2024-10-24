#!/usr/bin/env bash

set -eo pipefail

# What is this file?
# This allows us to inject external variables into our Bazel build.
# Care must be taken when modifying this, as incorrect usage could
# cause bazel to invalidate the cache too often.
#
# To RTFM, see: https://bazel.build/docs/user-manual#workspace-status-command
#
# Bazel only runs this when --config=stamp is used. At that point bazel invokes
# this script to generate key-value information that represents the status of the
# workspace. The output should be like
#
# KEY1 VALUE1
# KEY2 VALUE2
#
# If the script exits with non-zero code, the build will fail.
#
# Note that keys starting with "STABLE_" are part of the stable set, which if
# changed, invalidate any stampted targets (which by default is only binaries
# if the --stamp flag is passed to bazel, otherwise nothing). Keys which do
# not start with "STABLE_" are part of the volatile set, which will be used
# but do not invalidate stamped targets.

git_rev=$(git rev-parse HEAD)
echo "STABLE_GIT_COMMIT ${git_rev}"

git_tag=$(git describe --always --abbrev=0 --match='v*')
echo "STABLE_GIT_LATEST_TAG ${git_tag}"

# Check whether there are any uncommitted changes
if git diff-index --quiet HEAD --; then
  echo "STABLE_GIT_TREE_DIRTY "
else
  echo "STABLE_GIT_TREE_DIRTY -dirty"
fi
