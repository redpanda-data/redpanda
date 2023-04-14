// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package template

import "fmt"

const packageJSON = `{
  "name": "wasm-panda",
  "version": "0.0.1",
  "description": "inline wasm transforms sdk",
  "main": "bin/index.js",
  "bin": { "iwt": "./bin/index.js" },
  "scripts": {
    "build": "./webpack.js",
    "test": "node_modules/mocha/bin/mocha"
  },
  "keywords": ["inline-wasm-transform", "redpanda"],
  "author": "",
  "license": "ISC",
  "dependencies": {
    "@vectorizedio/wasm-api": ">=%s"
  },
  "devDependencies": {
    "ts-loader": "8.0.4",
    "webpack": "4.44.2",
    "mocha": "8.1.3"
  }
}
`

func PackageJSON(version string) string {
	return fmt.Sprintf(packageJSON, version)
}
