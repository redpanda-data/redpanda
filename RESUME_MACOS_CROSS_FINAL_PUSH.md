# Resume Instructions: macOS to Linux Cross-Compilation - Final Push to 100%

**Date**: 2026-02-08
**Branch**: `platform-fix-experiment`
**Goal**: Reach 100% clean build for macOS → Linux cross-compilation

## Current Status

**Build Progress**: ~98.3% (8,697 / 8,847 actions)

### ✅ Completed Fixes (2/6)
1. **zlib** - Fixed with static linking patch for darwin exec
   - Patch: `bazel/patches/zlib_static_darwin.patch`
   - Forces `linkstatic = True` on macOS to avoid System library link errors

2. **Protobuf generation** - Fixed with cfg="exec" in pbgen.bzl
   - All 14 protobuf errors resolved

### ⏳ In Progress (1/6)
3. **liburing** - macOS `echo -e` compatibility issue
   - Problem: configure script uses `env echo -e` which doesn't work on macOS
   - Error: `make: *** No rule to make target 'print-VERSION_MAJOR'`
   - Solution: Need to patch configure to replace `env echo -e` with `printf "%b"`
   - Patch file: `bazel/patches/liburing_macos_echo_fix.patch` (WIP)

### ⏱️ Remaining (4/6)
4. **base64** - CMake cross-compilation failure
5. **c-ares** - CMake cross-compilation failure
6. **openssl** - Cross-compilation config mismatch
7. **ragel** - Missing autoreconf tool

## Key Insights

### 1. **Sysroot Has Everything We Need!**
- Ubuntu 22.04 sysroot at `@x86_64_sysroot`
- **Has Linux kernel headers**: `/usr/include/linux/io_uring.h` ✓
- Has standard C library headers ✓
- Has library files ✓

**Strategy**: Use sysroot for proper cross-compilation instead of skipping libraries or building natively.

### 2. **Root Cause of foreign_cc Failures**
Foreign_cc rules apply cross-compilation settings to exec tools. Exec tools should build natively for darwin, but they're receiving Linux cross-compilation flags.

## Working Method

This session has used a structured approach:

1. **Small, Incremental Commits**: Every fix committed separately
2. **Structured Notes**: `/tmp/build-notes.txt` contains detailed timeline
3. **Background Agent Every ~10 Steps**: Spawn expert agent to review strategy
4. **No Permission Prompts**: For builds, restarts, code generation
5. **Frequent Pushes**: Keep remote branch updated

## Files Modified

### Core Configuration
- `MODULE.bazel` - Added patches for zlib and liburing (WIP)
- `.bazelrc` - Removed global stdlib linkopt, added C++ modules fix
- `bazel/pbgen/pbgen.bzl` - Added cfg="exec" for protoc plugin

### Patches Created
- `bazel/patches/BUILD` - Exports patch files
- `bazel/patches/zlib_static_darwin.patch` - Forces static linking on darwin
- `bazel/patches/liburing_macos_echo_fix.patch` - WIP echo -e fix

### Documentation
- `/tmp/build-notes.txt` - Complete timeline of all fixes
- `/tmp/remaining-issues.md` - Analysis of remaining failures

## Next Steps to 100%

### Immediate (Step 29-30)
1. **Fix liburing configure script**
   - Replace `env echo -e "$VAR"` with `printf "%b" "$VAR"`
   - Lines 474-475 in configure script
   - This will unblock liburing build

### Short-term (Step 31-40)
2. **Configure foreign_cc to use sysroot**
   - base64: Set proper CMAKE flags for sysroot
   - c-ares: Same as base64
   - openssl: Configure for native darwin or use sysroot
   - ragel: Install autotools or use prebuilt binary

3. **Spawn Background Agent** (around step 35)
   - Review overall progress
   - Validate sysroot configuration approach
   - Identify any remaining edge cases

### Final (Step 41-45)
4. **Verify 100% build completion**
5. **Create final commit with summary**
6. **Update PR with complete solution**

## How to Resume

```bash
cd /Users/agallego/workspace/redpanda
git checkout platform-fix-experiment
git pull origin platform-fix-experiment

# Check current build status
cat /tmp/build-notes.txt | tail -50

# Continue from liburing fix
# Edit: bazel/patches/liburing_macos_echo_fix.patch
# Test: bazel build --config=macos-linux @liburing//:generate_headers

# Or start fresh build to see current state
nohup bazel build --config=macos-linux //:redpanda > /tmp/build-current.log 2>&1 &
```

## Build Commands

```bash
# Full build
bazel build --config=macos-linux //:redpanda

# Test specific target
bazel build --config=macos-linux @liburing//:generate_headers
bazel build --config=macos-linux @@+non_module_dependencies+base64//:base64

# Check errors
grep -E "(ERROR|FAIL)" /tmp/build-current.log | head -20

# Verbose failures
bazel build --config=macos-linux <target> --verbose_failures
```

## Context Files

- **Agent Analysis**: Task a0fd2e9 output at `/private/tmp/claude-501/-Users-agallego-workspace-redpanda/tasks/a0fd2e9.output`
- **Build Notes**: `/tmp/build-notes.txt`
- **Remaining Issues**: `/tmp/remaining-issues.md`
- **Recent Logs**: `/tmp/test-*.log`

## Agent Instructions

When resuming, tell Claude:

> I'm continuing the macOS to Linux cross-compilation work. Read RESUME_MACOS_CROSS_FINAL_PUSH.md for current status. We're at 98.3% completion (8,697/8,847 actions). Currently fixing liburing echo -e issue. Goal is 100% clean build. Continue with:
> - Small incremental commits
> - Structured notes in /tmp/build-notes.txt
> - Spawn background agent every ~10 steps
> - Don't ask permission for builds/restarts/code generation
> - Push commits frequently

## Technical Details

### liburing echo -e Issue
The configure script (line 473-475) does:
```bash
MAKE_PRINT_VARS="include Makefile.common\nprint-%: ; @echo \$(\$*)\n"
VERSION_MAJOR=$(env echo -e "$MAKE_PRINT_VARS" | make -s --no-print-directory -f - print-VERSION_MAJOR)
VERSION_MINOR=$(env echo -e "$MAKE_PRINT_VARS" | make -s --no-print-directory -f - print-VERSION_MINOR)
```

On macOS, `env echo` calls `/bin/echo` which doesn't support `-e`. Need to replace with:
```bash
VERSION_MAJOR=$(printf "%b" "$MAKE_PRINT_VARS" | make -s --no-print-directory -f - print-VERSION_MAJOR)
VERSION_MINOR=$(printf "%b" "$MAKE_PRINT_VARS" | make -s --no-print-directory -f - print-VERSION_MINOR)
```

### Sysroot Configuration
For foreign_cc builds (base64, c-ares, openssl), need to ensure they use sysroot headers and libraries:
- Set `CMAKE_SYSROOT` to sysroot path
- Set `CMAKE_FIND_ROOT_PATH` to sysroot
- Don't set `CMAKE_SYSTEM_NAME` for exec builds (build natively)

## Commit Message Template

```
<Component>: <brief description>

<Detailed explanation of what was fixed and why>

Changes:
- <file>: <change>
- <file>: <change>

Remaining: <N> targets to reach 100%

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

## Success Criteria

- ✅ `bazel build --config=macos-linux //:redpanda` completes with 0 errors
- ✅ All 8,847 actions succeed
- ✅ Can produce Linux x86_64 binary from macOS arm64
- ✅ Clean, well-documented commits
- ✅ Structured notes for future reference

---

**Remember**: We're SO CLOSE! Just 6 targets remaining. The sysroot insight is the key - use it properly and we'll hit 100%.
