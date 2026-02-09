# Toolchain Research for macOS Cross-Compilation

## toolchains_llvm Capabilities

Based on the module source and documentation:

### Supported Configurations

1. **Downloaded toolchains** - Most common
   - Provides URLs and SHA256
   - Toolchain downloaded during build
   - Works for hermetic builds

2. **Local toolchains** - Less documented
   - `absolute_paths = True` - Searches standard locations
   - `distribution` attribute - Points to local installation
   - Challenges: Must match expected structure

### Cross-Compilation Support

The module DOES support cross-compilation via:
- `llvm.sysroot()` - Attaches sysroots to specific targets
- Platform-aware toolchain selection
- Separate exec and target platforms

### Current Issues

1. **Distribution attribute**: Doesn't support env var expansion
2. **absolute_paths**: Tries to download from GitHub if not found locally
3. **Structure mismatch**: Homebrew LLVM structure != expected structure

## Possible Solutions

### Solution A: Upload Built Toolchain
Once LLVM builds successfully:
1. Create tarball: `llvm-20.1.8-darwin-aarch64-DATE.tar.zst`
2. Upload to GitHub releases or S3
3. Configure MODULE.bazel with URL and SHA256
4. Attach Linux sysroots

**Pros**: Clean, hermetic, works like Linux toolchains
**Cons**: Requires hosting, ~600MB upload

### Solution B: Local Filegroup
Create a BUILD file that wraps Homebrew LLVM:
1. Define filegroup pointing to /opt/homebrew/opt/llvm@20
2. Use in MODULE.bazel via `distribution`
3. Attach Linux sysroots

**Pros**: No upload needed, uses existing Homebrew
**Cons**: Not hermetic, complex BUILD file

### Solution C: Simpler Platform Fix
Instead of fighting toolchain configuration:
1. Remove macOS from Rust `supported_platform_triples`
2. Configure Wasmtime to build for Linux only
3. Use existing Linux toolchains for cross-compilation

**Pros**: Simpler, fixes root cause
**Cons**: May break Wasmtime build tools

## Recommended Path

**Primary**: Solution A (Upload built toolchain)
- Most reliable
- Matches existing pattern
- Once working, it's stable

**Fallback**: Solution C (Platform fix)
- If Solution A doesn't work
- Addresses root cause
- Simpler configuration

## Implementation Plan for Solution A

### Step 1: Complete LLVM Build
- ✅ Fixed libunwind dependency
- 🔄 Building now
- Target: Get llvm-20.1.8-darwin-aarch64-DATE.tar.zst

### Step 2: Test Locally
Before uploading, test with local file:
```python
llvm.toolchain(
    name = "macos_toolchain",
    llvm_version = "20.1.8",
    sha256 = {"darwin-aarch64": "..."},
    urls = {"darwin-aarch64": ["file:///tmp/llvm-build/llvm-20.1.8-darwin-aarch64-DATE.tar.zst"]},
)
```

### Step 3: Upload and Configure
1. Upload to GitHub releases
2. Get URL and SHA256
3. Update MODULE.bazel
4. Test build

### Step 4: Add Sysroots
```python
llvm.sysroot(
    name = "macos_toolchain",
    label = "@x86_64_sysroot//:sysroot",
    targets = ["linux-x86_64"],
)
```

### Step 5: Test Cross-Compilation
```bash
bazel build --config=macos-linux //:redpanda
```

## Key Insights

1. **Don't fight the framework**: Use toolchains_llvm as designed
2. **Match the pattern**: Do what works for Linux toolchains
3. **Local testing first**: Test with file:// URLs before uploading
4. **Sysroot is separate**: Toolchain + sysroot = cross-compilation

## Next Actions

1. ⏳ Wait for LLVM build to complete
2. ⏳ Create tarball
3. ⏳ Test with file:// URL
4. ⏳ Upload to GitHub releases
5. ⏳ Configure MODULE.bazel
6. ⏳ Test full build
