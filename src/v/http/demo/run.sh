#!/bin/bash
# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

if [[ ! -d "./src/v/http/demo" ]]; then
  echo "The script should be started from root v directory"
  exit 1
fi
python ./src/v/http/demo/echo.py &
echo "Starting web server..."
sleep 3

demo_client_bin=
if [[ -f "./vbuild/release/gcc/bin/http_demo_client" ]]; then
  demo_client_bin="./vbuild/release/gcc/bin/http_demo_client"
fi
if [[ -f "./vbuild/release/clang/bin/http_demo_client" ]]; then
  demo_client_bin=${demo_client_bin:-"./vbuild/release/clang/bin/http_demo_client"}
fi
if [[ -f "./vbuild/debug/gcc/bin/http_demo_client" ]]; then
  demo_client_bin=${demo_client_bin:-"./vbuild/debug/gcc/bin/http_demo_client"}
fi
if [[ -f "./vbuild/debug/clang/bin/http_demo_client" ]]; then
  demo_client_bin=${demo_client_bin:-"./vbuild/debug/clang/bin/http_demo_client"}
fi

# ./vbuild is a build path used by vtools
if [[ -f "./vbuild/release/gcc/bin/http_demo_client" ]]; then
  demo_client_bin="./vbuild/release/gcc/bin/http_demo_client"
fi
if [[ -f "./vbuild/release/clang/bin/http_demo_client" ]]; then
  demo_client_bin=${demo_client_bin:-"./vbuild/release/clang/bin/http_demo_client"}
fi
if [[ -f "./vbuild/debug/gcc/bin/http_demo_client" ]]; then
  demo_client_bin=${demo_client_bin:-"./vbuild/debug/gcc/bin/http_demo_client"}
fi
if [[ -f "./vbuild/debug/clang/bin/http_demo_client" ]]; then
  demo_client_bin=${demo_client_bin:-"./vbuild/debug/clang/bin/http_demo_client"}
fi

${demo_client_bin} --port=8080 --target=/echo --method=POST --data="Hello World"
echo "Stopping web server..."
pkill -f echo.py
wait
