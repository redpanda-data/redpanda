#!/bin/bash
set -ex

export INSTALL_PREFIX=/usr/local

if [ "$COMPILER" == "clang" ]; then
  export CLANG_FLAG="--clang=internal"
fi

# we create a repo to avoid having to COPY .git/ which would
# invalidate the cache every time we rebuild the image
git init .
git config user.name "Foo Fighter"
git config user.email "foo@fighter.com"
git commit --allow-empty -m "I got another confession to make..."

tools/build.py $CLANG_FLAG \
  --log=debug \
  --fmt=false \
  --build="$BUILD_TYPE" \
  --external=true \
  --external-only=true \
  --external-install-prefix="$INSTALL_PREFIX" \
  --targets=cpp

cp "build/$BUILD_TYPE/v_deps_build/seastar-prefix/src/seastar-build/apps/iotune/iotune" \
   "$INSTALL_PREFIX/bin/"

if [ "$COMPILER" == "clang" ]; then
  echo "$INSTALL_PREFIX/lib/" > /etc/ld.so.conf.d/usrlocallib.conf
  echo "$INSTALL_PREFIX/lib64/" > /etc/ld.so.conf.d/usrlocallib64.conf
  ldconfig
fi

# TODO: remove after github.com/vectorizedio/v/issues/191 is fixed
sed -i \
  's/  set (_seastar_dep_args_.*)//' \
  "$INSTALL_PREFIX/lib64/cmake/Seastar/SeastarDependencies.cmake"
