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
    -DLLVM_ENABLE_RUNTIMES="libcxx;libcxxabi;libunwind;compiler-rt" \
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

# ====================================
# Phase 2: Cross-compile libc++ for Linux targets
# ====================================
echo ""
echo "=========================================="
echo "Phase 2: Cross-compiling libc++ for Linux"
echo "=========================================="

# Download Linux sysroots for cross-compilation
echo "Downloading Linux x86_64 sysroot..."
SYSROOT_X86_URL="https://github.com/redpanda-data/llvm-project/releases/download/llvmorg-19.1.7/sysroot-ubuntu-22.04-x86_64-2025-02-24.tar.zst"
curl -L -o "${OUTPUT_DIR}/linux-sysroot-x86_64.tar.zst" "${SYSROOT_X86_URL}"
mkdir -p "${OUTPUT_DIR}/sysroot-x86_64"
tar -xf "${OUTPUT_DIR}/linux-sysroot-x86_64.tar.zst" -C "${OUTPUT_DIR}/sysroot-x86_64"

echo "Downloading Linux aarch64 sysroot..."
SYSROOT_ARM_URL="https://github.com/redpanda-data/llvm-project/releases/download/llvmorg-19.1.7/sysroot-ubuntu-22.04-aarch64-2025-02-27.tar.zst"
curl -L -o "${OUTPUT_DIR}/linux-sysroot-aarch64.tar.zst" "${SYSROOT_ARM_URL}"
mkdir -p "${OUTPUT_DIR}/sysroot-aarch64"
tar -xf "${OUTPUT_DIR}/linux-sysroot-aarch64.tar.zst" -C "${OUTPUT_DIR}/sysroot-aarch64"

# Cross-compile libc++ for Linux x86_64
echo ""
echo "Building libc++ for Linux x86_64..."
cmake -G Ninja \
    -S runtimes -B build-linux-x86_64 \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}/lib/x86_64-unknown-linux-gnu" \
    -DCMAKE_CROSSCOMPILING=ON \
    -DCMAKE_SYSTEM_NAME=Linux \
    -DCMAKE_SYSTEM_PROCESSOR=x86_64 \
    -DCMAKE_C_COMPILER="${INSTALL_PREFIX}/bin/clang" \
    -DCMAKE_CXX_COMPILER="${INSTALL_PREFIX}/bin/clang++" \
    -DCMAKE_C_COMPILER_TARGET=x86_64-unknown-linux-gnu \
    -DCMAKE_CXX_COMPILER_TARGET=x86_64-unknown-linux-gnu \
    -DCMAKE_SYSROOT="${OUTPUT_DIR}/sysroot-x86_64" \
    -DLLVM_ENABLE_RUNTIMES="libcxx;libcxxabi" \
    -DLIBCXX_CXX_ABI=libcxxabi \
    -DLIBCXX_ENABLE_STATIC_ABI_LIBRARY=ON \
    -DLIBCXX_ENABLE_SHARED=OFF \
    -DLIBCXX_ENABLE_STATIC=ON \
    -DLIBCXXABI_ENABLE_SHARED=OFF \
    -DLIBCXXABI_ENABLE_STATIC=ON \
    -DLIBCXX_USE_COMPILER_RT=OFF

ninja -C build-linux-x86_64 cxx cxxabi
ninja -C build-linux-x86_64 install-cxx install-cxxabi

# Cross-compile libc++ for Linux aarch64
echo ""
echo "Building libc++ for Linux aarch64..."
cmake -G Ninja \
    -S runtimes -B build-linux-aarch64 \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}/lib/aarch64-unknown-linux-gnu" \
    -DCMAKE_CROSSCOMPILING=ON \
    -DCMAKE_SYSTEM_NAME=Linux \
    -DCMAKE_SYSTEM_PROCESSOR=aarch64 \
    -DCMAKE_C_COMPILER="${INSTALL_PREFIX}/bin/clang" \
    -DCMAKE_CXX_COMPILER="${INSTALL_PREFIX}/bin/clang++" \
    -DCMAKE_C_COMPILER_TARGET=aarch64-unknown-linux-gnu \
    -DCMAKE_CXX_COMPILER_TARGET=aarch64-unknown-linux-gnu \
    -DCMAKE_SYSROOT="${OUTPUT_DIR}/sysroot-aarch64" \
    -DLLVM_ENABLE_RUNTIMES="libcxx;libcxxabi" \
    -DLIBCXX_CXX_ABI=libcxxabi \
    -DLIBCXX_ENABLE_STATIC_ABI_LIBRARY=ON \
    -DLIBCXX_ENABLE_SHARED=OFF \
    -DLIBCXX_ENABLE_STATIC=ON \
    -DLIBCXXABI_ENABLE_SHARED=OFF \
    -DLIBCXXABI_ENABLE_STATIC=ON \
    -DLIBCXX_USE_COMPILER_RT=OFF

ninja -C build-linux-aarch64 cxx cxxabi
ninja -C build-linux-aarch64 install-cxx install-cxxabi

echo ""
echo "Verifying cross-compiled libraries..."
ls -lh "${INSTALL_PREFIX}/lib/x86_64-unknown-linux-gnu/"
ls -lh "${INSTALL_PREFIX}/lib/aarch64-unknown-linux-gnu/"

# Create tarball (now includes Linux libc++ libraries)
echo ""
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
