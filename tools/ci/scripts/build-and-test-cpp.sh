#!/bin/bash
set -ex

if [ "$COMPILER" == "clang" ]; then
  export CLANG_FLAG="--clang=/usr/local/bin/clang"
fi

# print CPU on CI machines to track illegal instructions
command -v lscpu >/dev/null && lscpu

tools/build.py $CLANG_FLAG \
  --log=debug \
  --fmt=false \
  --build="$BUILD_TYPE" \
  --external=false \
  --targets=cpp
