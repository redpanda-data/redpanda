#!/bin/bash
# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

set -ex
root=$(git rev-parse --show-toplevel)
js_root="${root}/src/js/"
cd "$js_root"
build_dir_base="${root}/build/node/output"
rm -rf "$build_dir_base"
mkdir -p "$build_dir_base"
npm run generate:serialization
npm run test
npm run build:ts -- --project . --outDir "${build_dir_base}"
echo "$build_dir_base"
cp build-package.json "$build_dir_base"/package.json
cd "$build_dir_base" && npm install
