#!/usr/bin/env node
/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

const { exec } = require("child_process");
const webpack = require("webpack");
const path = require("path");
const fs = require("fs");
/**
 * for testing, the result file from the public folder is going to be saved
 * in the /sdk folder. After the test, this file is going to be publish to
 * the npm registry
 */
const jsFolder = path.resolve(__dirname);
const dependencyName = "wasm-api.js";
const publicFolder = path.join(jsFolder, "public");
const publishPackageJson = path.join(publicFolder, "package.json");
const version = process.argv.splice(2)[0];

const wasmPackageJson = `{
  "name": "@vectorizedio/wasm-api",
  "version": "${version}",
  "description": "wasm api helps to define wasm function",
  "main": "${dependencyName}",
  "scripts": {
    "test": "echo \\"Error: no test specified\\" && exit 1"
  },
  "license": "ISC"
}
`;

const build = () => {
  return new Promise((resolve, reject) => {
    webpack(
      {
        mode: "production",
        entry: { main: `./modules/public/index.ts` },
        output: {
          filename: dependencyName,
          path: publicFolder,
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
          reject(err);
        }
        const info = stat.toJson();
        if (stat.hasErrors()) {
          reject(info.errors);
        }
        if (stat.hasWarnings()) {
          console.warn(info.warnings);
        }
        resolve();
      }
    );
  });
};

const executeCommands = (command) =>
  new Promise((resolve, reject) => {
    exec(command, (error, stdout, stderr) => {
      if (error) {
        return reject(`error: ${error.message}`);
      }
      if (stderr) {
        return reject(`stderr: ${stderr}`);
      }
      return resolve(stdout);
    });
  });

if (version === undefined) {
  console.error("Please set the wasm-api version");
  return 1;
} else {
  build()
    .then(() => fs.promises.writeFile(publishPackageJson, wasmPackageJson))
    .then(() => console.log("create public folder"))
    .then(() => executeCommands("cd public && npm publish --access=public"))
    .then(console.log)
    .catch((e) => {
      console.error(e);
      return 1;
    });
}
