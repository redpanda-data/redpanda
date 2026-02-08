# Building Redpanda on macOS

This guide explains how to build Linux binaries for Redpanda on macOS using cross-compilation.

## Overview

Redpanda supports cross-compilation from macOS (both Apple Silicon and Intel) to Linux targets. This enables macOS developers to build Redpanda binaries that can be deployed on Linux systems without requiring a Linux VM or remote Linux machine.

**Approach**: We use locally-installed LLVM toolchains (via Homebrew or Xcode) with hermetic Linux sysroots, inspired by [Zig's excellent cross-compilation support](https://ziglang.org/learn/overview/).

**Architecture**:
- **Execution Platform**: macOS (where the build runs) - darwin-arm64 or darwin-x86_64
- **Target Platform**: Linux (output binary format) - linux-x86_64 or linux-aarch64
- **Toolchain**: Homebrew LLVM or system clang that generates Linux ELF binaries
- **Sysroot**: Hermetic Ubuntu 22.04 sysroot for Linux dependencies

## Quick Start

```bash
# 1. Install prerequisites
xcode-select --install
brew install llvm@20

# 2. Run setup script
./bazel/toolchain/setup-macos-toolchain.sh

# 3. Build for Linux
bazel build --config=macos-linux-local //:redpanda
```

## Prerequisites

### Required Software

1. **Xcode Command Line Tools**:
   ```bash
   xcode-select --install
   ```

2. **Homebrew** (if not already installed):
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```

3. **LLVM via Homebrew** (recommended):
   ```bash
   brew install llvm@20
   ```

   Alternatively, you can use the system clang from Xcode (already installed).

4. **Bazelisk** (required for building Redpanda):
   ```bash
   wget -O ~/bin/bazel https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-darwin-arm64
   chmod +x ~/bin/bazel
   export PATH="$HOME/bin:$PATH"
   ```

   For Intel Macs, use `bazelisk-darwin-amd64` instead.

### Automated Setup

Run the setup script to configure your environment:

```bash
./bazel/toolchain/setup-macos-toolchain.sh
```

This script will:
- Check for Homebrew LLVM installation
- Install LLVM 20 if not present
- Create `user.bazelrc` with local toolchain configuration

### Disk Space Requirements

- Homebrew LLVM: ~600MB
- Build artifacts: ~10-20GB of disk space
- Subsequent builds reuse cached dependencies

## Building Linux Binaries

### Basic Build Commands

After running the setup script, use the `-local` config variants:

**Build Linux x86_64 binary** (default):
```bash
bazel build --config=macos-linux-local //:redpanda
```

**Build Linux ARM64 binary**:
```bash
bazel build --config=macos-linux-arm64-local //:redpanda
```

**Build release binary** (optimized):
```bash
bazel build --config=macos-release --config=macos-local //:redpanda
```

### Alternative: Using System Clang

If you don't want to install Homebrew LLVM, you can use macOS system clang (from Xcode):

```bash
# Create user.bazelrc manually
echo "build:macos-linux-local --config=macos-linux" > user.bazelrc

# Build with system clang
bazel build --config=macos-linux-local //:redpanda
```

This uses the clang from Xcode Command Line Tools with the hermetic Linux sysroots.

### Build Output Location

Built binaries are located in:
```bash
bazel-bin/src/v/redpanda/redpanda
```

### Verify Binary Format

To verify that you've built a Linux binary:
```bash
file bazel-bin/src/v/redpanda/redpanda
# Expected output:
# bazel-bin/src/v/redpanda/redpanda: ELF 64-bit LSB executable, x86-64, dynamically linked
```

On macOS, use `docker` to inspect Linux binary details:
```bash
docker run --rm -v "$PWD:/work" -w /work ubuntu:22.04 ldd bazel-bin/src/v/redpanda/redpanda
```

## Testing

### Building Tests

Build unit tests for Linux:
```bash
# Build all tests
bazel build --config=macos-linux //src/v/...

# Build specific test
bazel build --config=macos-linux //src/v/storage:storage_test
```

**Note**: Built test binaries are Linux ELF format and cannot execute directly on macOS.

### Running Tests

Since the test binaries are built for Linux, they must be executed in a Linux environment:

#### Option 1: Docker

```bash
# Build test
bazel build --config=macos-linux //src/v/storage:storage_test

# Run in Docker
docker run --rm \
  -v "$PWD:/work" \
  -w /work \
  ubuntu:22.04 \
  ./bazel-bin/src/v/storage/storage_test
```

#### Option 2: Remote Linux Machine

```bash
# Build test
bazel build --config=macos-linux //src/v/storage:storage_test

# Copy to Linux machine
scp bazel-bin/src/v/storage/storage_test user@linux-host:/tmp/

# SSH and run
ssh user@linux-host /tmp/storage_test
```

#### Option 3: Lima VM (Recommended for macOS)

[Lima](https://github.com/lima-vm/lima) provides a fast Linux VM on macOS:

```bash
# Install Lima
brew install lima

# Start Ubuntu VM
limactl start default

# Run test in VM
lima ./bazel-bin/src/v/storage/storage_test
```

## Building the CLI (rpk)

The Go-based CLI tool `rpk` can be built for both macOS and Linux:

**Build for Linux**:
```bash
bazel build --config=macos-linux //:rpk
```

**Build for native macOS** (if supported):
```bash
bazel build //:rpk
```

## Advanced Configuration

### Custom Build Flags

You can combine multiple configurations:

```bash
# Debug build for Linux
bazel build --config=macos-linux --config=debug //:redpanda

# Release build with sanitizers
bazel build --config=macos-release --config=asan //:redpanda
```

### Targeting Specific CPU Architectures

The default Linux x86_64 target uses `-march=westmere` for broad compatibility. For native performance:

```bash
# Build for specific CPU (requires custom .bazelrc or flags)
bazel build --config=macos-linux --copt=-march=native //:redpanda
```

**Warning**: Binaries built with `-march=native` may not run on other machines.

### Clean Build

To perform a clean build:

```bash
bazel clean
bazel build --config=macos-release //:redpanda
```

## Troubleshooting

### Issue: "Toolchain not found" Error

**Symptom**:
```
ERROR: No matching toolchains found for types @bazel_tools//tools/cpp:toolchain_type
```

**Solution**:
1. Ensure you've run `sudo ./bazel/install-deps.sh`
2. Verify the toolchain SHA256 hashes in `MODULE.bazel` are correct
3. Clear Bazel cache: `bazel clean --expunge`

### Issue: "Platform not found" Error

**Symptom**:
```
ERROR: Target pattern parsing failed. No targets found beneath 'platforms'
```

**Solution**:
Ensure you're using the correct platform specification. The configurations use `@platforms//os:linux` and `@platforms//cpu:x86_64`.

### Issue: Binary Fails to Run on Linux

**Symptom**: Binary executes on macOS but fails on Linux with "cannot execute binary file"

**Solution**:
Verify you used the `--config=macos-linux` flag. Without it, Bazel builds native macOS binaries.

```bash
# Check binary format
file bazel-bin/src/v/redpanda/redpanda
# Should show: ELF 64-bit LSB executable (Linux)
# NOT: Mach-O 64-bit executable (macOS)
```

### Issue: Slow First Build

**Symptom**: First build takes 10+ minutes

**Cause**: Bazel downloads ~500MB LLVM toolchain and dependencies on first build.

**Solution**: This is expected. Subsequent builds reuse cached toolchains and are much faster.

### Issue: Linker Errors

**Symptom**:
```
undefined reference to `std::__1::...`
```

**Solution**:
This indicates mixing macOS and Linux libraries. Ensure:
1. You're using `--config=macos-linux`
2. The hermetic sysroot is properly configured
3. No manual library paths are interfering

### Issue: Docker Cannot Run Binary

**Symptom**: "Exec format error" when running binary in Docker

**Cause**: Binary architecture mismatch (e.g., ARM64 binary on x86_64 Docker)

**Solution**:
- For x86_64 Docker: Use `--config=macos-linux` (not `macos-linux-arm64`)
- For ARM64 Docker: Use `--config=macos-linux-arm64`
- Check Docker architecture: `docker info | grep Architecture`

## Performance Considerations

### Build Times

| Build Type | First Build | Incremental Build |
|------------|-------------|-------------------|
| Debug | 30-45 min | 2-5 min |
| Release | 45-60 min | 5-10 min |
| Single Target | 5-15 min | 30-120 sec |

**Notes**:
- First build downloads toolchains (~500MB) and builds all dependencies
- Incremental builds only rebuild changed files
- Apple Silicon Macs are ~20-30% faster than Intel Macs

### Optimization Tips

1. **Use incremental builds**: Don't run `bazel clean` unless necessary
2. **Limit parallelism on resource-constrained machines**:
   ```bash
   bazel build --config=macos-linux --jobs=4 //:redpanda
   ```
3. **Use local disk (not network shares)**: Network filesystems significantly slow down builds
4. **Close other applications**: Bazel is CPU and memory intensive

## Deployment

### Deploying to Linux

After building on macOS, deploy to Linux:

```bash
# Build release binary
bazel build --config=macos-release //:redpanda

# Copy to Linux server
scp bazel-bin/src/v/redpanda/redpanda user@linux-host:/opt/redpanda/bin/

# On Linux server, verify and run
file /opt/redpanda/bin/redpanda
/opt/redpanda/bin/redpanda --version
```

### Creating Distributable Archives

```bash
# Build release binary
bazel build --config=macos-release //:redpanda

# Create tarball
tar -czf redpanda-linux-x86_64.tar.gz \
  -C bazel-bin/src/v/redpanda \
  redpanda

# Transfer to Linux
scp redpanda-linux-x86_64.tar.gz user@linux-host:/tmp/
```

## Continuous Integration

For CI/CD pipelines on macOS runners (GitHub Actions, Buildkite):

```yaml
# .github/workflows/build-macos.yml
name: macOS Cross-Compilation Build

on:
  pull_request:
  push:
    branches: [dev]

jobs:
  build-macos:
    runs-on: macos-14  # Apple Silicon runner
    steps:
      - uses: actions/checkout@v4

      - name: Install Bazelisk
        run: brew install bazelisk

      - name: Install Dependencies
        run: sudo ./bazel/install-deps.sh

      - name: Build Linux x86_64 Binary
        run: bazel build --config=macos-linux //:redpanda

      - name: Verify Binary Format
        run: |
          file bazel-bin/src/v/redpanda/redpanda | grep -q "ELF 64-bit"
```

## Additional Resources

- **Bazel Documentation**: https://bazel.build/extending/platforms
- **LLVM Cross-Compilation**: https://clang.llvm.org/docs/CrossCompilation.html
- **Redpanda Main README**: [/README.md](/README.md)
- **Contributing Guide**: [/CONTRIBUTING.md](/CONTRIBUTING.md)
- **Toolchain Build Script**: [/bazel/toolchain/build-macos-toolchain.sh](/bazel/toolchain/build-macos-toolchain.sh)

## Getting Help

If you encounter issues not covered in this guide:

1. Check existing GitHub issues: https://github.com/redpanda-data/redpanda/issues
2. Ask in Redpanda Community Slack
3. Open a new GitHub issue with:
   - macOS version and architecture (`uname -a`)
   - Bazel version (`bazel version`)
   - Full error message
   - Build command used

## Known Limitations

1. **Test Execution**: Tests cannot run directly on macOS - use Docker or remote Linux
2. **Debugging**: Native macOS debuggers (lldb) cannot debug Linux binaries - use remote debugging
3. **Integration Tests**: Full ducktape test suite requires Linux environment
4. **Performance Profiling**: macOS profiling tools cannot analyze Linux binaries
