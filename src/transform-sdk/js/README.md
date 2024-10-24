# Redpanda Data Transform JS SDK

This holds the JS SDK code for Redpanda's Data Transforms. This SDK is implemented by
compiling the QuickJS JavaScript VM into WebAssembly and running it within our C++ SDK.

To use this SDK manually here are the steps:

```shell
# Compile the C++
docker run -v `pwd`/..:/src -w /src/js ghcr.io/webassembly/wasi-sdk \
  /bin/bash -c 'apt update && apt install -y git && cmake --preset release-static && cmake --build --preset release-static -- redpanda_js_transform -j32'
# Convert the JavaScript file into a WebAssembly text file
cat user_code.js | ./generate_js_provider.py > js_user_code.wat
# Download wasm merge from Binaryen, and use it to merge the wasm files together, resolving the imports in C++ for the source code.
wasm-merge build/release-static/redpanda_js_transform js_vm ./js_user_code.wat redpanda_js_provider -mvp --enable-simd --enable-bulk-memory --enable-multimemory -o js_transform.wasm
# Deploy away!
rpk transform deploy --file=js_transform.wasm --name=my-transform -i foo -o bar
```

There is still a lot of work todo to provide a full experience of NodeJS compatibility, but this is a start.
