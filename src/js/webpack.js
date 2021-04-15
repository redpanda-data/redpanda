#!/usr/bin/env node
const webpack = require("webpack");
const os = require("os");
const fs = require("fs");
const path = require("path");
const nodeExternals = require("webpack-node-externals");

const webpackOptions = {
  context: __dirname,
  mode: "production",
  target: "node",
  entry: { main: `./modules/rpc/service.ts` },
  output: {
    path: path.resolve(__dirname, "dist"),
    filename: `main.js`,
    libraryTarget: "commonjs2",
  },
  resolve: {
    extensions: [".ts", ".js", ".node"],
  },
  module: {
    noParse: [/\/require-native.ts$/, /\/fast-crc32c\/loader\.js$/],
    rules: [
      {
        test: /\.ts?$/,
        use: "ts-loader",
        exclude: /node_modules/,
      },
      {
        test: /\.node$/,
        loader: "node-loader",
      },
    ],
  },
};
webpack([webpackOptions], (err, stat) => {
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
