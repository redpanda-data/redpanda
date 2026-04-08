#!/usr/bin/env bash

# Make a symlink named the same as a clang tool pointing to this
# shim and that tool will be invoked via bazel run.
#

tools_dir="$(cd -- "$(dirname -- "$(realpath "${BASH_SOURCE[0]}")")" &>/dev/null && pwd)"

export BAZEL_TOOL_TRAMPOLINE_TARGET="@clang-22_llvm_toolchain//:bin/$(basename "$0")"

if [[ ${BAZEL_TRAMPOLINE_DEBUG:-0} -gt 0 ]]; then
  echo "DEBUG: clang-tool.sh: CWD        : $(pwd)" >&2
  echo "DEBUG: clang-tool.sh: tools_dir  : $tools_dir" >&2
  echo "DEBUG: clang-tool.sh: target     : $BAZEL_TOOL_TRAMPOLINE_TARGET" >&2
fi

# This shim just calls the general bazel-tool-trampoline.sh script
# with the target set appropriately.
exec "$tools_dir/bazel-tools/support/bazel-tool-trampoline.sh" "$@"
