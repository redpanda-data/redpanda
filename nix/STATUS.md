# Nix Flake for Redpanda: Progress & Status

## Architecture

```
flake.nix                 # Root entry point
nix/
  bcr.nix                 # Pinned Bazel Central Registry snapshot        [DONE]
  redpanda.nix            # C++ server (Bazel build + FOD repo cache)     [BLOCKED]
  rpk.nix                 # Go CLI (buildGoModule)                        [DONE, WORKING]
  shell.nix               # Development shell                             [DONE]
```

## Status

| Package | Status | Notes |
|---------|--------|-------|
| `rpk` | **Working** | `nix build .#rpk` succeeds, binary runs |
| `devShell` | **Working** | `nix develop` provides bazel, python, etc. |
| `redpanda` | **Blocked** | Fundamental Bazel/Nix sandbox conflict; pivoting to Bazel fork |

## Phase 1: What We Tried (and What We Learned)

### Two-phase FOD Build

The original approach used custom derivations for a two-phase build:

1. **FOD phase** (`bazelRepoCache`): `bazel fetch` with network access
2. **Build phase**: `bazel build` offline with cached repos

### bwrap Approach: Failed

Wrapped Bazel with bubblewrap to mount `/lib64` for pre-built ELF binaries.

**Discovery**: bwrap mount namespaces silently fail inside the Nix sandbox.
The Nix sandbox root is read-only; `unshare(CLONE_NEWNS)` from bwrap either
fails or produces a namespace where bind mounts don't take effect. The
`/lib64` overlay never actually existed — Bazel itself ran fine (Nix store
binary) but all downloaded ELF binaries failed with ENOENT.

### patchelf Approach: Partially Worked

Switched to patchelf with Nix store paths:
```
patchelf --set-interpreter ${glibc}/lib/ld-linux-x86-64.so.2 \
         --set-rpath ${glibc}/lib:${gcc-unwrapped.lib}/lib:${zlib}/lib:... \
         <binary>
```

**Results**:
- Python: **SOLVED** — CheckHostInterpreter passes after patchelf
- Cargo: **SOLVED** — `cargo 1.86.0` runs after patchelf
- But Bazel **re-creates repo rules** on each fetch, copying fresh (unpatched)
  binaries from cached archives, losing our patches
- Multi-pass fetch/patch loop is fragile and unreliable

### Root Cause Analysis

The fundamental problem: **Bazel downloads and immediately executes pre-built
ELF binaries in the same `repository_ctx` invocation.** There is no hook
between download and execution. Explored the Bazel source code and confirmed:

- `repository_ctx.execute()` → Java `Command` → `Runtime.exec()` — no wrapper
- `repository_ctx.download_and_extract()` → directly to disk, no post-hook
- `ProcessWrapper` only handles timeouts/signals, not execution interception
- No `--repo_exec_wrapper` or similar flag exists

## Sandbox Compatibility Patches (still needed)

| Patch | Why |
|-------|-----|
| Remove `tools/bazel` | Bazelisk wrapper has `/usr/bin/env` shebang; not needed |
| Remove `.bazelversion` | nixpkgs ships Bazel 8.x but repo pins specific version |
| Add `exec_os = "linux"` to `llvm.toolchain()` | Prevents reading `/etc/os-release` (absent in sandbox) |

## Lessons Learned

1. **bwrap inside Nix sandbox doesn't work**: The Nix sandbox's read-only root
   and namespace restrictions prevent bwrap from creating effective mount overlays.

2. **patchelf works but is fragile**: Patching downloaded binaries to use Nix
   store interpreter/RPATH works perfectly — but Bazel re-creates repo rules
   from cached archives, wiping patches each time.

3. **No hook point in Bazel**: `repository_ctx.execute()` goes directly to
   subprocess creation. There is no interception mechanism between download
   and execution in repo rules.

4. **Bazel's sandbox conflicts with Nix's sandbox**: Both systems create
   isolated environments, but they don't compose. Bazel's linux-sandbox
   creates nested mount namespaces that lose parent mounts. Disabling it
   (`--spawn_strategy=local`) helps for build actions but not for repo rules.

5. **The downloaded toolchain problem is universal**: Python, Rust, Go, LLVM —
   all Bazel toolchain rules download platform-specific pre-built ELFs. This
   is the single biggest obstacle to Bazel-on-Nix integration.

6. **Existing sandbox flags are insufficient**: Bazel has `--sandbox_add_mount_pair`,
   `--sandbox_writable_path`, `--sandbox_tmpfs_path` for build action sandboxing,
   but NONE of these apply to repository rule execution.

## Phase 2: Bazel Fork ("nixify" branch)

### New Direction

Rather than working around Bazel's limitations with increasingly fragile hacks,
we are forking Bazel to add first-class Nix integration. The thesis:

> Nix and Bazel are complementary hermetic build systems. Nix provides
> deterministic, content-addressed toolchains and dependencies. Bazel provides
> fast incremental builds with fine-grained caching. If Bazel can recognize
> and trust Nix-provided inputs, the combination is more powerful than either
> system alone.

### Fork Details

- **Upstream**: `bazelbuild/bazel` (commit matching Bazel 8.x)
- **Fork**: `randomizedcoder/bazel`
- **Branch**: `nixify`
- **Goal**: Minimal, targeted changes that solve Nix integration
- **Strategy**: Develop on fork, later extract as patches or propose upstream

### Design Document

See `NIXIFY_DESIGN.md` in the Bazel fork for the full design, decision table,
and implementation plan.

## Next Steps

1. Finalize design approach for Bazel changes (see decision table)
2. Implement chosen approach on `nixify` branch
3. Build patched Bazel and test with Redpanda's `nix build .#redpanda`
4. Once FOD fetch succeeds, capture output hash and verify full build
5. Test resulting binary: `./result/bin/redpanda --version`
