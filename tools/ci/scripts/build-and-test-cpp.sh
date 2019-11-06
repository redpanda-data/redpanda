#!/bin/bash
set -ex

if [ "$COMPILER" == "clang" ]; then
  export CLANG_FLAG="--clang=/usr/local/bin/clang"
fi

tools/build.py $CLANG_FLAG \
  --log=debug \
  --fmt=false \
  --build="$BUILD_TYPE" \
  --external=false \
  --targets=cpp
