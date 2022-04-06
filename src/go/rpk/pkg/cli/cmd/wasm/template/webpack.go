// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package template

const webpack = `#!/usr/bin/env node
const webpack = require("webpack");
const fs = require("fs");

fs.readdir("./src/", (err, files) => {
  if (err) {
    console.log(err);
    return;
  }
  const webPackOptions = files.map((fileName) => {
    return {
      mode: "production",
      entry: { main: ` + "`./src/${fileName}`" + ` },
      output: {
        filename: fileName,
        libraryTarget: "commonjs2",
      },
      performance: {
        hints: false
      },
      target: 'node',
    };
  });
  webpack(webPackOptions, (err, stat) => {
    if (err) {
      console.log(err);
    }
    const info = stat.toJson();
    if (stat.hasErrors()) {
      console.error(info.errors);
    }
    if (stat.hasWarnings()) {
      console.warn(info.warnings);
    }
  });
});
`

func Webpack() string {
	return webpack
}
