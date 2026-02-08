#!/bin/bash
set -euo pipefail

# Configuration
LLVM_VERSION="${LLVM_VERSION:-20.1.8}"  # or 21.1.6
LLVM_MAJOR_VERSION=$(echo "$LLVM_VERSION" | cut -d. -f1)
BUILD_DATE=$(date -u +%Y-%m-%d)
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/llvm-build}"
INSTALL_PREFIX="${OUTPUT_DIR}/install"
ARCH=$(uname -m)  # arm64 or x86_64

# Normalize arch name
if [ "$ARCH" = "arm64" ]; then
    ARCH="aarch64"
fi

OUTPUT_FILE="llvm-${LLVM_VERSION}-darwin-${ARCH}-${BUILD_DATE}.tar.zst"

echo "Building LLVM ${LLVM_VERSION} for macOS ${ARCH}"
echo "Output will be: ${OUTPUT_FILE}"

# Clean up any previous build
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

# Clone LLVM
echo "Cloning LLVM repository..."
git clone --depth 1 --branch "release/${LLVM_MAJOR_VERSION}.x" \
    https://github.com/llvm/llvm-project.git "${OUTPUT_DIR}/llvm-project"

cd "${OUTPUT_DIR}/llvm-project"

# Configure LLVM build
# Note: We build with X86 and AArch64 backends to enable cross-compilation
echo "Configuring LLVM build..."
cmake -G Ninja \
    -S llvm -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
    -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra;lld" \
    -DLLVM_ENABLE_RUNTIMES="libcxx;libcxxabi;compiler-rt" \
    -DLLVM_TARGETS_TO_BUILD="X86;AArch64" \
    -DLLVM_ENABLE_LIBCXX=ON \
    -DLLVM_ENABLE_LTO=Thin \
    -DLLVM_PARALLEL_LINK_JOBS=4 \
    -DLLVM_INCLUDE_TESTS=OFF \
    -DLLVM_INCLUDE_EXAMPLES=OFF \
    -DLLVM_INCLUDE_BENCHMARKS=OFF

# Build LLVM (this takes 1-2 hours)
echo "Building LLVM (this will take 1-2 hours)..."
ninja -C build install

# Create tarball
echo "Creating tarball: ${OUTPUT_FILE}"
cd "${OUTPUT_DIR}"
tar -cf - -C "${INSTALL_PREFIX}" . | zstd -19 -T0 -o "${OUTPUT_FILE}"

# Calculate SHA256
echo ""
echo "=========================================="
echo "Toolchain build complete!"
echo "=========================================="
echo "File: ${OUTPUT_FILE}"
echo "SHA256:"
shasum -a 256 "${OUTPUT_FILE}"
echo ""
echo "Upload this file to:"
echo "https://github.com/redpanda-data/llvm-project/releases/download/llvmorg-${LLVM_VERSION}/"
echo ""
