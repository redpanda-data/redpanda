# macOS Toolchain Options for Cross-Compilation

This document outlines different approaches for cross-compiling Redpanda from macOS to Linux, inspired by Zig's excellent cross-compilation support.

## Option 1: Use System Clang (Simplest)

macOS ships with clang/LLVM via Xcode Command Line Tools. We can use this with our hermetic Linux sysroots.

### Pros
- ✅ Already installed on every Mac
- ✅ No downloads required
- ✅ Zero setup time
- ✅ Apple maintains and updates it

### Cons
- ❌ Version tied to macOS/Xcode version
- ❌ May have Apple-specific patches
- ❌ Older macOS = older LLVM

### Implementation

Add to `MODULE.bazel`:

```python
llvm.toolchain(
    name = "system_llvm_toolchain",
    toolchain_roots = {
        "darwin-aarch64": "@local_config_cc//:toolchain",
        "darwin-x86_64": "@local_config_cc//:toolchain",
    },
    # ... rest of config
)
```

## Option 2: Use Homebrew LLVM (Recommended)

Homebrew provides pre-built LLVM packages that are easy to install and update.

### Pros
- ✅ Easy installation: `brew install llvm@20`
- ✅ Pre-built binaries optimized for macOS
- ✅ Control over LLVM version
- ✅ Regular updates available

### Cons
- ❌ Requires Homebrew
- ❌ Users must install manually
- ❌ ~600MB download per version

### Implementation

```bash
# Install LLVM 20
brew install llvm@20

# Add to MODULE.bazel
llvm.toolchain(
    name = "homebrew_llvm_toolchain",
    toolchain_roots = {
        # Homebrew LLVM 20 location
        "darwin-aarch64": "/opt/homebrew/opt/llvm@20",
        "darwin-x86_64": "/usr/local/opt/llvm@20",
    },
    # ... rest of config
)
```

## Option 3: Use Zig as C/C++ Compiler (Novel)

Zig provides `zig cc` which acts as a drop-in replacement for clang with built-in cross-compilation support.

### Pros
- ✅ Excellent cross-compilation support
- ✅ Bundles libc for all platforms
- ✅ Single ~60MB download
- ✅ No separate sysroot needed
- ✅ Custom linker that supports Apple Silicon

### Cons
- ❌ Requires Zig installation
- ❌ Less common in C++ build systems
- ❌ May have compatibility issues with some C++ features

### Implementation

```bash
# Install Zig
brew install zig

# Configure Bazel to use zig cc
# Create wrapper scripts in bazel/toolchain/zig/
```

**Wrapper script** (`bazel/toolchain/zig/zig-cc`):
```bash
#!/bin/bash
exec zig cc -target x86_64-linux-gnu "$@"
```

## Option 4: Pre-built LLVM from Official Sources

Download official LLVM releases from releases.llvm.org or GitHub.

### Available Sources

1. **LLVM Official Releases** (when available):
   - https://github.com/llvm/llvm-project/releases
   - Limited macOS pre-built binaries in minor releases
   - Major releases (X.0.0) more likely to have macOS builds

2. **LLVM Homebrew Bottles** (mirrors):
   - https://github.com/llvm-hs/homebrew-llvm
   - Pre-built bottles for various macOS versions

### Pros
- ✅ Official LLVM binaries
- ✅ Hermetic (downloaded by Bazel)
- ✅ Reproducible builds

### Cons
- ❌ Not all versions have macOS builds
- ❌ Large downloads (~600-800MB)
- ❌ Slow first build

## Option 5: Build Custom Toolchain (Original Plan)

Use the `build-macos-toolchain.sh` script to build a custom LLVM from source.

### Pros
- ✅ Full control over configuration
- ✅ Can optimize for specific needs
- ✅ Can match exact Linux toolchain version

### Cons
- ❌ 1-2 hours build time per architecture
- ❌ Requires significant disk space (~20GB during build)
- ❌ Manual process to update

## Recommended Approach: Hybrid Strategy

Use different strategies for different scenarios:

### For Local Development
**Use Homebrew LLVM** (Option 2)
- Developers install: `brew install llvm@20`
- Fast, easy, works well

### For CI/CD
**Use system clang** (Option 1) or **Homebrew LLVM** (Option 2)
- GitHub Actions macOS runners have Xcode pre-installed
- Can install Homebrew LLVM in CI setup step

### For Maximum Portability
**Use Zig** (Option 3)
- Single small download
- Built-in cross-compilation
- Excellent compatibility

### For Production Releases
**Build custom toolchain** (Option 5)
- Ensure consistent compiler version
- Optimize for specific targets
- Full control over features

## Implementation Priority

1. **Start with Option 2 (Homebrew)** - Fastest path to working cross-compilation
2. **Add Option 3 (Zig)** as alternative - Great for developers who want minimal setup
3. **Add Option 1 (System clang)** for CI - Zero overhead
4. **Consider Option 5** - Only if specific compiler features are needed

## Next Steps

See `MACOS_HOMEBREW_IMPLEMENTATION.md` for implementing Option 2 (Homebrew LLVM).
