#!/usr/bin/env python3
"""
A small script that will download wasm-merge and create a tarball with wasm-merge and the JavaScript VM compiled as Wasm.

This script assumes it's run from the JS SDK root directory (src/transform-sdk/js) and you must have already built the Wasm binary via:

docker run -v `pwd`/..:/src -w /src/js ghcr.io/webassembly/wasi-sdk \
  /bin/bash -c 'apt update && apt install -y git && cmake --preset release-static && cmake --build --preset release-static -- redpanda_js_transform'
"""

import subprocess
import tempfile
from pathlib import Path
import tarfile
import shutil


def download_via_curl(url, file):
    """
    Download by shelling to curl. Simpler than than the pure python approach 🤷
    """
    print(url)
    subprocess.run(["curl", "-SL", "-o", file, url], check=True)


BINARYEN_VERSION = 117
BINARYEN_BASE_URL = f"https://github.com/WebAssembly/binaryen/releases/download/version_{BINARYEN_VERSION}/binaryen-version_{BINARYEN_VERSION}-"

BINARYEN_TARGET_MAPPING = {
    ("linux", "arm64"): "aarch64-linux",
    ("linux", "amd64"): "x86_64-linux",
    ("darwin", "arm64"): "arm64-macos",
    ("darwin", "amd64"): "x86_64-macos",
}

install_dir = Path.cwd() / "build"
# This script assumes you've already built the final binary
js_wasm_vm = install_dir / "release-static" / "redpanda_js_transform"
with tempfile.TemporaryDirectory() as temp_dir:
    temp_dir = Path(temp_dir)
    for os in ["darwin", "linux"]:
        for arch in ["arm64", "amd64"]:
            file = f"{BINARYEN_TARGET_MAPPING[(os, arch)]}.tar.gz"
            download_via_curl(BINARYEN_BASE_URL + file, temp_dir / file)
            with tarfile.open(temp_dir / file) as tar:
                tar.extractall(path=temp_dir / f"{os}-{arch}",
                               filter='fully_trusted')
            output = install_dir / f"javascript-{os}-{arch}.tar.gz"
            with tarfile.open(output, mode='w:gz') as tar:
                wasm_merge = temp_dir / f"{os}-{arch}" / f"binaryen-version_{BINARYEN_VERSION}" / "bin" / "wasm-merge"
                tar.add(wasm_merge, arcname=f"bin/{wasm_merge.name}")
                tar.add(js_wasm_vm, arcname=f"bin/{js_wasm_vm.name}")
                if os == "darwin":
                    dylib = temp_dir / f"{os}-{arch}" / f"binaryen-version_{BINARYEN_VERSION}" / "lib" / "libbinaryen.dylib"
                    tar.add(dylib, arcname=f"lib/libbinaryen.dylib")
