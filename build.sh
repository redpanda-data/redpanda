#!/bin/bash
set -x
root=$(git rev-parse --show-toplevel)
if [[ -z ${CC} ]]; then export CC=/usr/bin/gcc; fi
if [[ -z ${CXX} ]]; then export CXX=/usr/bin/g++; fi
if [[ ! -z ${CCACHE_DIR} && -e /dev/shm ]]; then export CCACHE_DIR=/dev/shm/redpanda; fi

# Change Debug via  -DCMAKE_BUILD_TYPE=Debug
cmake -DCMAKE_BUILD_TYPE=Release \
      -B$root/build \
      -H$root \
      -GNinja \
      -DCMAKE_C_COMPILER=$CC \
      -DCMAKE_CXX_COMPILER=$CXX

(cd $root/build && ninja && ctest -R _rpunit)
