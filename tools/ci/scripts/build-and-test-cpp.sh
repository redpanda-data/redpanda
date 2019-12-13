#!/bin/bash
set -ex

if [ "$COMPILER" == "clang" ]; then
  CLANG_FLAG="--clang=internal"
fi

# print CPU on CI machines to track illegal instructions
command -v lscpu >/dev/null && lscpu

tools/build.py $CLANG_FLAG \
  --log=debug \
  --fmt=false \
  --build="$BUILD_TYPE" \
  --build-dir=/v \
  --external-skip-build=true \
  --targets=cpp

mkdir -p build/$BUILD_TYPE/bin
mv /v/$BUILD_TYPE/bin/redpanda build/$BUILD_TYPE/bin/
