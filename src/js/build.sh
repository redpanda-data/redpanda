#!/usr/bin/bash

root=$(git rev-parse --show-toplevel)
rm -rf "$root"/build/node/output/*
npm run generate:serialization
npm run test
npm run build:ts
cp build-package.json "$root"/build/node/output/package.json
