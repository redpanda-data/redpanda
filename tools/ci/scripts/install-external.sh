#!/bin/bash
set -ex

if [ "$COMPILER" == "clang" ]; then
  CLANG_FLAG="--clang=internal"
fi

## we create a repo to avoid having to COPY .git/ which would
## invalidate the cache every time we rebuild the image
git init .
git config user.name "Foo Fighter"
git config user.email "foo@fighter.com"
git commit --allow-empty -m "I got another confession to make..."

tools/build.py $CLANG_FLAG \
  --log=debug \
  --fmt=false \
  --build="$BUILD_TYPE" \
  --build-dir=/v \
  --external-only=true \
  --targets=cpp

# FIXME https://app.asana.com/0/1149841353291489/1153763539998305
mv /v/$BUILD_TYPE/v_deps_build/seastar-prefix/src/seastar-build/apps/iotune/iotune /v/

rm -rf /v/$BUILD_TYPE/CMakeCache.txt
rm -rf /v/$BUILD_TYPE/v_deps_build

if [ "$COMPILER" == "clang" ]; then
  rm -rf /v/llvm/llvm-build /v/llvm/llvm-src
fi
