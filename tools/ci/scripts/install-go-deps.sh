#!/bin/bash
set -ex

if [ "$COMPILER" == "clang" ]; then
  export CC="/usr/local/bin/clang
  export CXX="/usr/local/bin/clang++
fi

go mod download
