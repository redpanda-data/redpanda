#!/bin/bash
set -e
set -x

CCACHE_DIR=$HOME/redpanda-meson-ccache
CC="ccache clang"
CXX="ccache clang++"
CC_LD="lld"
CXX_LD="lld"
NAME=redpanda-meson

mkdir -p $CCACHE_DIR
docker build -t $NAME .

function run {
  local args=$*
  docker run -v $PWD:/src/redpanda \
    -v $CCACHE_DIR:/mnt/ccache \
    -e CCACHE_DIR=/mnt/ccache -e CC="$CC" \
    -e CC_LD="$CC_LD" -e CXX_LD="$CXX_LD" \
    -e CXX="$CXX" -u $(id -u):$(id -g) -it $NAME $args
}

function reset_build {
  rm -rf builddir
  run meson builddir
}

function purge_subprojects {
  run meson subprojects purge --confirm
}

function build {
  reset_build
  run meson compile -C builddir
}

function build_targets_isolated {
  reset_build
  targets=$(run meson introspect builddir --targets | jq -r "reverse | .[] | select(.subproject == null).name")
  for target in ${targets}; do
    reset_build
    run meson compile -C builddir $target
  done
}

function run_tests {
  run meson test -C builddir
}

build
