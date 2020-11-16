#!/bin/bash
# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

cd ../../js
npm install
npm run generate:serialization
npm run build:public
cd -
sed -i 's/%CONTENT%/"$(cat vectorizedDependency.js)"/g' pkg/cli/cmd/wasm/template/vectorized_dependency.go
