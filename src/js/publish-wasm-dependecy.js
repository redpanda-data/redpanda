#!/usr/bin/env node
/**
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
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
const readmeName = "README.md";
const publicFolder = path.join(jsFolder, "public");
const artifactFolder = path.join(jsFolder, "publish_artifacts");
const publishPackageJson = path.join(publicFolder, "package.json");
const readmeArtifact = path.join(artifactFolder, readmeName);
const publishReadme = path.join(publicFolder, readmeName);
const publishCmd = "cd public && npm publish --access=public";

var version = "1.0.0";
var publish = true;

if (process.argv.length == 4) {
  const arr = process.argv.splice(2);
  const skip = arr[0];
  version = arr[1];
  if (skip == "--skip-publish") {
    publish = false;
  }
} else if (process.argv.length == 3) {
  version = process.argv.splice(2)[0];
} else {
  console.error(
    "Incorrect number of args pased, usage: [node ./publish-wasm-dep.js --skip-publish 1.0.1]"
  );
  return 1;
}

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

const publishWasmLib = () =>
  new Promise((resolve, reject) => {
    exec(publishCmd, (error, stdout, stderr) => {
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
    .then(() => fs.promises.copyFile(readmeArtifact, publishReadme))
    .then(() => console.log(`created public folder: ${publicFolder}`))
    .then(() => {
      if (publish == false) {
        return Promise.resolve("Publish skipped");
      } else {
        return publishWasmLib();
      }
    })
    .then(console.log)
    .catch((e) => {
      console.error(e);
      return 1;
    });
}
