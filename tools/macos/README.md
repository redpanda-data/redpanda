# macOS Development Tools

This directory contains tools and scripts for developing Redpanda on macOS.

## Files

### Brewfile

Homebrew bundle file that installs all dependencies needed for building Redpanda on macOS.

**Install all dependencies:**
```bash
brew bundle --file=tools/macos/Brewfile
```

**Or from this directory:**
```bash
cd tools/macos
brew bundle
```

**Check what's installed:**
```bash
brew bundle check --file=tools/macos/Brewfile
```

**Update all dependencies:**
```bash
brew bundle --file=tools/macos/Brewfile --no-lock
```

### docker-build.sh

Wrapper script for building Redpanda using Docker on macOS. Useful when native cross-compilation encounters issues.

**Usage:**
```bash
./tools/macos/docker-build.sh build //:redpanda
./tools/macos/docker-build.sh test //src/v/storage:storage_test
```

## Quick Start for macOS Developers

### 1. Install Dependencies

```bash
# Install Homebrew if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install all Redpanda build dependencies
brew bundle --file=tools/macos/Brewfile
```

### 2. Set Up Cross-Compilation

```bash
# Run the setup script
./bazel/toolchain/setup-macos-toolchain.sh
```

This creates `user.bazelrc` with your local LLVM configuration.

### 3. Build Redpanda

```bash
# Build for Linux x86_64 (most common)
bazel build --config=macos-linux-local //:redpanda

# Build for Linux ARM64
bazel build --config=macos-linux-arm64-local //:redpanda

# Build rpk CLI
bazel build --config=macos-linux-local //:rpk

# Build release binary
bazel build --config=macos-release --config=macos-local //:redpanda
```

### 4. Verify Build

```bash
# Check that it's a Linux binary
file bazel-bin/src/v/redpanda/redpanda
# Expected: ELF 64-bit LSB executable, x86-64

# Run in Docker
docker run --rm -v "$PWD:/work" -w /work ubuntu:22.04 \
  ./bazel-bin/src/v/redpanda/redpanda --version
```

## Troubleshooting

### Missing Build Tools

If you get errors about missing commands (e.g., `autoreconf: command not found`):

```bash
# Install autotools
brew install autoconf automake libtool

# Or reinstall everything
brew bundle --file=tools/macos/Brewfile
```

### Bazelisk Not Found

```bash
brew install bazelisk
```

### LLVM Not Found

```bash
brew install llvm@20
./bazel/toolchain/setup-macos-toolchain.sh
```

### Wasmtime Platform Issues

The main Redpanda binary (`//:redpanda`) currently cannot be built on macOS due to Wasmtime platform constraints. Workarounds:

1. **Use Docker** (recommended):
   ```bash
   ./tools/macos/docker-build.sh build //:redpanda
   ```

2. **Build on remote Linux**:
   ```bash
   git push
   ssh linux-host "cd redpanda && bazel build //:redpanda"
   ```

3. **Build rpk CLI** (works on macOS):
   ```bash
   bazel build --config=macos-linux-local //:rpk
   ```

## Additional Resources

- **Complete Guide**: [/docs/BUILDING_ON_MACOS.md](/docs/BUILDING_ON_MACOS.md)
- **Setup Script**: [/bazel/toolchain/setup-macos-toolchain.sh](/bazel/toolchain/setup-macos-toolchain.sh)
- **Main README**: [/README.md](/README.md)
- **Contributing Guide**: [/CONTRIBUTING.md](/CONTRIBUTING.md)

## Support

For issues or questions:

1. Check the troubleshooting section above
2. Read the full documentation in `/docs/BUILDING_ON_MACOS.md`
3. Search existing issues: https://github.com/redpanda-data/redpanda/issues
4. Ask in Redpanda Community Slack

## Minimum Requirements

- macOS 12.0+ (Monterey or later)
- Xcode Command Line Tools
- Homebrew
- 10GB+ free disk space
- 8GB+ RAM (16GB recommended)
