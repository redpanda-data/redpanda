#!/usr/bin/env node
const webpack = require("webpack");
const path = require("path");

/**
 * for testing, the result file from the public folder is going to save
 * in /sdk folder, after test, this file is going to publish to npm repository
 */

webpack(
  {
    mode: "production",
    entry: { main: `./modules/public/index.ts` },
    output: {
      filename: "vectorizedDependency.js",
      path: path.resolve(__dirname),
      libraryTarget: "commonjs2",
    },
    resolve: {
      extensions: [".ts", ".js"],
    },
    module: {
      rules: [{ test: /\.ts$/, use: "ts-loader" }],
    },
  },
  (err, stat) => {
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
  }
);
