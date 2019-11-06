#!/bin/bash
set -ex

if [ "$COMPILER" == "clang" ]; then
  export CC="/usr/local/bin/clang"
  export CXX="/usr/local/bin/clang++"
fi

cd src/go/pkg/
go test ./...
go install -a -v -x -tags netgo -ldflags '-w' ./...
