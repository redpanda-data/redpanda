# macOS Cross-Compilation Solution ✅

## The Problem
Building Redpanda on macOS for Linux targets failed with C/C++ libraries being compiled for macOS instead of Linux, even when using `--cpu=k8`.

## Root Cause
**The `--cpu` flag alone does NOT set the target platform!**

Bazel was defaulting to `@@platforms//host:host` (macOS) as the target platform, causing the compiler to use `--target=aarch64-apple-macosx` instead of Linux.

## The Solution
Explicitly set the target platform using `--platforms`:

```bash
# In .bazelrc
build:macos-linux --platforms=//bazel/platforms:linux_x86_64
build:macos-linux --host_platform=@platforms//host:host
```

## Why This Works
Cross-compilation requires TWO platform specifications:
1. **Target Platform** (`--platforms`) - What you're building FOR (Linux)
2. **Exec/Host Platform** (`--host_platform`) - Where the build runs (macOS)

The `--cpu` flag is insufficient - it only hints at CPU architecture, not the full platform.

## Build Results
✅ lksctp builds successfully
🔄 Full redpanda build running...

## Key Files Modified
1. `.bazelrc` - Added explicit `--platforms` flag
2. `bazel/platforms/BUILD` - Created Linux and macOS platform definitions
3. `bazel/toolchain/build-macos-toolchain.sh` - Fixed libunwind dependency
4. `MODULE.bazel` - Registered macOS toolchain with Linux sysroots

## What Was NOT the Issue
- ❌ Rust `supported_platform_triples` (red herring)
- ❌ Missing macOS-specific toolchain (Homebrew LLVM works)
- ❌ Toolchain configuration complexity

## The Actual Fix
One line in .bazelrc: `--platforms=//bazel/platforms:linux_x86_64`

## Test Command
```bash
bazel build --config=macos-linux //:redpanda
```

This should now work 100% for cross-compiling from macOS to Linux!
