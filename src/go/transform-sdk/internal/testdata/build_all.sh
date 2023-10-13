#!/bin/bash

PKGS=(
  identity
  setup-panic
  transform-error
  transform-panic
  schema-registry
  wasi
  dynamic
)

for PKG in "${PKGS[@]}"; do
  echo "Building $PKG..."
  # Build using the buildpack, you need to install rpk and initialize
  # a project for this to work first.
  ~/.local/rpk/buildpacks/tinygo/bin/tinygo build -target wasi -opt=z \
    -panic print -scheduler none \
    -o "$PKG.wasm" ./$PKG
  echo "done ✔️"
done
echo "All packages built 🚀"
