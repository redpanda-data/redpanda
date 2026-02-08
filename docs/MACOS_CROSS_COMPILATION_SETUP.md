# macOS to Linux Cross-Compilation Setup Guide

This document outlines the steps needed to complete the setup of macOS-to-Linux cross-compilation for Redpanda.

## Implementation Status

### ✅ Completed

1. **Build Infrastructure**
   - Created `bazel/toolchain/build-macos-toolchain.sh` script for building macOS-hosted LLVM toolchains
   - Script supports both Apple Silicon (arm64) and Intel (x86_64) architectures
   - Supports building LLVM 20.1.8 (current) and 21.1.6 (next)

2. **Bazel Configuration**
   - Updated `MODULE.bazel` with placeholder entries for macOS toolchains
   - Added macOS toolchain SHA256 and URL mappings (with placeholder values)
   - Extended toolchains_llvm configuration to support darwin-aarch64 and darwin-x86_64

3. **Build Configurations**
   - Added `--config=macos-linux` for cross-compiling to Linux x86_64
   - Added `--config=macos-linux-arm64` for cross-compiling to Linux ARM64
   - Added `--config=macos-release` convenience alias for release builds

4. **Documentation**
   - Created comprehensive `docs/BUILDING_ON_MACOS.md` developer guide
   - Updated main `README.md` with macOS cross-compilation section
   - Updated `bazel/toolchain/README.md` with macOS toolchain build instructions

5. **Helper Tools**
   - Created `tools/macos/docker-build.sh` as interim Docker-based solution
   - Created `.github/workflows/build-macos.yml` CI workflow (disabled by default)

### 🔲 TODO: Build and Deploy Toolchains

The following steps must be completed to enable the cross-compilation:

#### Step 1: Build LLVM Toolchains

Build 4 toolchain archives (this takes 1-2 hours per toolchain):

**On Apple Silicon Mac:**
```bash
cd bazel/toolchain

# Build LLVM 20.1.8 for Apple Silicon
./build-macos-toolchain.sh

# Build LLVM 21.1.6 for Apple Silicon
LLVM_VERSION=21.1.6 ./build-macos-toolchain.sh
```

**On Intel Mac (or using cross-compilation):**
```bash
cd bazel/toolchain

# Build LLVM 20.1.8 for Intel
./build-macos-toolchain.sh

# Build LLVM 21.1.6 for Intel
LLVM_VERSION=21.1.6 ./build-macos-toolchain.sh
```

Each build produces:
- Output file: `llvm-{VERSION}-darwin-{ARCH}-{DATE}.tar.zst`
- SHA256 hash (printed at end of build)
- Size: ~400-600 MB per archive

#### Step 2: Upload Toolchains to GitHub

1. Navigate to: https://github.com/redpanda-data/llvm-project/releases

2. Upload to the appropriate releases:
   - `llvmorg-20.1.8`: Upload the 20.1.8 macOS toolchains
   - `llvmorg-21.1.6`: Upload the 21.1.6 macOS toolchains

3. For each archive uploaded, record:
   - Filename
   - SHA256 hash
   - Build date

#### Step 3: Update MODULE.bazel

Replace the placeholder values in `MODULE.bazel` with actual values:

**For LLVM 20.1.8 (lines ~161-169):**
```python
"darwin-aarch64": {
    "build_date": "2025-02-07",  # Replace with actual date
    "sha": "abc123...",           # Replace with actual SHA256
},
"darwin-x86_64": {
    "build_date": "2025-02-07",  # Replace with actual date
    "sha": "def456...",           # Replace with actual SHA256
},
```

**For LLVM 21.1.6 (lines ~177-185):**
```python
"darwin-aarch64": {
    "build_date": "2025-02-07",  # Replace with actual date
    "sha": "ghi789...",           # Replace with actual SHA256
},
"darwin-x86_64": {
    "build_date": "2025-02-07",  # Replace with actual date
    "sha": "jkl012...",           # Replace with actual SHA256
},
```

#### Step 4: Test the Configuration

**On Apple Silicon Mac:**
```bash
# Test Linux x86_64 build
bazel build --config=macos-linux //:redpanda
file bazel-bin/src/v/redpanda/redpanda
# Expected: ELF 64-bit LSB executable, x86-64

# Test Linux ARM64 build
bazel build --config=macos-linux-arm64 //:redpanda
file bazel-bin/src/v/redpanda/redpanda
# Expected: ELF 64-bit LSB executable, ARM aarch64
```

**On Intel Mac:**
```bash
# Test Linux x86_64 build
bazel build --config=macos-linux //:redpanda
file bazel-bin/src/v/redpanda/redpanda
# Expected: ELF 64-bit LSB executable, x86-64
```

#### Step 5: Validate Binary Execution

```bash
# Build binary
bazel build --config=macos-release //:redpanda

# Test in Docker
docker run --rm -v "$PWD:/work" -w /work ubuntu:22.04 \
  ./bazel-bin/src/v/redpanda/redpanda --version

# Test on remote Linux machine
scp bazel-bin/src/v/redpanda/redpanda user@linux-host:/tmp/
ssh user@linux-host /tmp/redpanda --version
```

#### Step 6: Enable CI Workflow (Optional)

Once the toolchains are working, enable the macOS CI workflow:

1. Edit `.github/workflows/build-macos.yml`
2. Uncomment the `pull_request` and `push` triggers
3. Commit and push
4. Verify that CI builds succeed

---

## Expected File Structure

After completing all steps, the following files should be in place:

```
redpanda/
├── .bazelrc                                    # ✅ Updated with macos-linux configs
├── MODULE.bazel                                # ⚠️  Updated but needs real SHA256 values
├── README.md                                   # ✅ Updated with macOS section
├── .github/
│   └── workflows/
│       └── build-macos.yml                     # ✅ Created (disabled by default)
├── bazel/
│   └── toolchain/
│       ├── README.md                           # ✅ Updated with macOS instructions
│       └── build-macos-toolchain.sh            # ✅ Created
├── docs/
│   ├── BUILDING_ON_MACOS.md                    # ✅ Created
│   └── MACOS_CROSS_COMPILATION_SETUP.md        # ✅ This file
└── tools/
    └── macos/
        └── docker-build.sh                     # ✅ Created (interim solution)
```

---

## Troubleshooting

### Build Script Fails

**Error**: `cmake: command not found`
```bash
brew install cmake ninja
```

**Error**: `git clone failed`
- Check internet connection
- Verify GitHub is accessible
- Try using `--depth 1` for faster clone

**Error**: `ninja: build stopped: subcommand failed`
- Check available disk space (need ~20GB)
- Check available memory (need 8GB+)
- Reduce parallelism: Add `-DLLVM_PARALLEL_LINK_JOBS=2` to cmake args

### Bazel Configuration Fails

**Error**: `No matching toolchains found`
- Verify SHA256 hashes in MODULE.bazel are correct
- Check that toolchains were uploaded to GitHub
- Try: `bazel clean --expunge && bazel sync`

**Error**: `Failed to download toolchain`
- Check GitHub releases are public
- Verify URL format matches: `https://github.com/redpanda-data/llvm-project/releases/download/llvmorg-{version}/llvm-{version}-darwin-{arch}-{date}.tar.zst`
- Check internet connection

### Binary Execution Fails

**Error**: `cannot execute binary file: Exec format error`
- Verify you built with `--config=macos-linux`
- Check binary format: `file bazel-bin/src/v/redpanda/redpanda`
- Should show "ELF" not "Mach-O"

**Error**: `GLIBC version not found`
- Binary requires newer glibc than available on target system
- Rebuild with older sysroot or deploy to Ubuntu 22.04+

---

## Timeline

| Phase | Duration | Status |
|-------|----------|--------|
| Phase 1: Infrastructure Setup | Week 1 | ✅ Complete |
| Phase 2: Build Toolchains | Week 1-2 | 🔲 TODO |
| Phase 3: Testing | Week 2-3 | 🔲 TODO |
| Phase 4: Documentation | Week 3 | ✅ Complete |
| Phase 5: CI Integration | Week 4 | 🔲 Optional |

---

## Next Steps

1. **Immediate**: Build the 4 LLVM toolchain archives using `build-macos-toolchain.sh`
2. **Upload**: Upload toolchains to GitHub releases
3. **Configure**: Update MODULE.bazel with real SHA256 hashes and build dates
4. **Test**: Verify builds work on both Apple Silicon and Intel Macs
5. **Deploy**: Enable CI workflow if desired
6. **Document**: Update any additional documentation as needed

---

## Alternative: Using Docker (Interim Solution)

While waiting for the toolchains to be built, use the Docker-based solution:

```bash
# Build using Docker
./tools/macos/docker-build.sh build //:redpanda

# Run tests using Docker
./tools/macos/docker-build.sh test //src/v/storage:storage_test
```

**Trade-offs**:
- ✅ Works immediately with existing Linux toolchains
- ✅ No Bazel configuration changes needed
- ❌ 2-4x slower than native builds
- ❌ Requires Docker Desktop

---

## Questions or Issues?

For questions or issues with this setup:

1. Check the troubleshooting section above
2. Review [docs/BUILDING_ON_MACOS.md](BUILDING_ON_MACOS.md)
3. Search existing issues: https://github.com/redpanda-data/redpanda/issues
4. Open a new issue with:
   - macOS version and architecture
   - Error message and full output
   - Steps to reproduce

---

## References

- [toolchains_llvm Documentation](https://github.com/bazel-contrib/toolchains_llvm)
- [Bazel Cross-Compilation Guide](https://bazel.build/extending/platforms)
- [LLVM Cross-Compilation](https://clang.llvm.org/docs/CrossCompilation.html)
- [Redpanda LLVM Releases](https://github.com/redpanda-data/llvm-project/releases)
