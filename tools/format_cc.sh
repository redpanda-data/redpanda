#!/bin/bash

clang_format=$(readlink -f $1)
# See: https://bazel.build/docs/user-manual#running-executables
root_dir=$BUILD_WORKSPACE_DIRECTORY
working_dir=$BUILD_WORKING_DIRECTORY

pushd "$working_dir"
git ls-files --full-name "*.cc" "*.java" "*.h" "*.proto" | xargs -n1 "$clang_format" -i -style=file -fallback-style=none
popd
