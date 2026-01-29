#!/usr/bin/env bash

# This script is a trampoline for bazel tools, allowing running
# of tools which are bazel targets, with:
# - minimal bazel output
# - the working directory set to the user's original working directory
# - avoiding a failure when bazel is run from the output directory
# - allowing the tool to be run from anywhere, even outside the workspace
#
# Use it by calling it with the target name as an environment variable:
#   BAZEL_TOOL_TRAMPOLINE_TARGET=//tools/bazel-tools:my_tool \
#   tools/bazel-tools/support/bazel-tool-trampoline.sh [args...]

set -euo pipefail

debug() {
  if [[ ${BAZEL_TRAMPOLINE_DEBUG:-0} -gt 0 ]]; then
    echo "DEBUG: bazel-tool-trampoline.sh:" "$@" >&2
  fi
}

# true in that symlinks are resolved
support_script_dir="$(cd -- "$(dirname -- "$(realpath "${BASH_SOURCE[0]}")")" &>/dev/null && pwd)"

debug "support_script_dir: $support_script_dir"

if [[ -z ${BAZEL_TOOL_TRAMPOLINE_TARGET-} ]]; then
  # If BAZEL_TOOL_TRAMPOLINE_TARGET is not set, error and exit
  echo "ERROR: bazel-tool-trampoline.sh: BAZEL_TOOL_TRAMPOLINE_TARGET not set" >&2
  exit 1
fi

if [[ $BAZEL_TOOL_TRAMPOLINE_TARGET == *clang-tidy ]]; then
  if [[ -z ${BUILD_WORKSPACE_DIRECTORY-} ]]; then
    export BAZEL_BIN=$(bazel info bazel-bin)
  else
    export BAZEL_BIN="${BUILD_WORKSPACE_DIRECTORY}/bazel-bin"
  fi
  export LOADS=" -load ${BAZEL_BIN}/bazel/clang_tidy/plugins/plugins.so"
fi

export BAZEL_TOOL_TRAMPOLINE_CWD=$PWD

# switch to the directory containing this script so that bazel has the right
# context to be invoked (it just has to be anywhere under the workspace root)
if ! cd "$support_script_dir"; then
  echo "ERROR: bazel-tool-trampoline.sh: failed to cd to script directory" >&2
  exit 1
fi

debug "running $BAZEL_TOOL_TRAMPOLINE_TARGET"
debug "bazel invoke cwd: $(pwd)"
debug "target cwd      : $BAZEL_TOOL_TRAMPOLINE_CWD"

# These flags are to hide a bunch of Bazel's built-in output.

exec bazel run "$BAZEL_TOOL_TRAMPOLINE_TARGET" \
  --run_under=//tools/bazel-tools/support:run \
  --noshow_progress \
  --ui_event_filters=,+error,+fail \
  --show_result=0 \
  --logging=0 \
  -- \
  ${LOADS-} \
  "$@"
