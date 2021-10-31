#!/bin/bash
# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

set -ex
root="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
if [[ -z ${CC} ]]; then export CC=clang; fi
if [[ -z ${CXX} ]]; then export CXX=clang++; fi
if [[ -z ${CCACHE_DIR} && -e /dev/shm ]]; then
  mkdir -p /dev/shm/redpanda
  export CCACHE_DIR=/dev/shm/redpanda
fi

# Change Debug via  -DCMAKE_BUILD_TYPE=Debug
cmake -DCMAKE_BUILD_TYPE=Release \
  -B$root/build \
  -H$root \
  -GNinja \
  -DCMAKE_C_COMPILER=$CC \
  -DCMAKE_CXX_COMPILER=$CXX \
  -DDEPOT_TOOLS_DIR="/opt/depot_tools" \
  "$@"

(cd $root/build && ninja && LD_LIBRARY_PATH=$PWD/deps_install/lib ctest --output-on-failure -R _rpunit)
