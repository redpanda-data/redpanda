#!/bin/bash
set -ex

if [ "$COMPILER" == "clang" ]; then
  export CLANG_FLAG="--clang=/usr/local/bin/clang"
fi

# we create a repo to avoid having to COPY .git/ which would
# invalidate the cache every time we rebuild the image
git init .
git config user.name "${COMPILER}:${BUILD_TYPE}"
git config user.email "${COMPILER}-${BUILD_TYPE}@vectorized.io"
git commit --allow-empty -m "${COMPILER}:${BUILD_TYPE}"

tools/build.py $CLANG_FLAG \
  --log=debug \
  --fmt=false \
  --build="$BUILD_TYPE" \
  --external=false \
  --targets=cpp
