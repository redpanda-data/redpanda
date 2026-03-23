# Building Redpanda with Nix

## Quick Start

### Prerequisites

- Nix with flakes enabled (`experimental-features = nix-command flakes` in
  `/etc/nix/nix.conf` or `~/.config/nix/nix.conf`)
- ~20 GB disk for the first build

### Minimal Build (no nix.conf changes)

This works out of the box with just Nix flakes enabled:

```bash
nix build .#redpanda --print-build-logs
result/bin/redpanda --version
```

Each build compiles from scratch (~30 min) because the Bazel action cache
is discarded when the sandbox exits. For one-off builds or CI this is fine.

### Cached Build (recommended for development)

For fast iterative rebuilds (~5-6 min warm), configure a persistent Bazel
cache that survives across builds. This requires two `nix.conf` changes:

1. **`/bin/bash` in sandbox** — Bazel repo rules execute scripts with
   `#!/bin/bash` shebangs, but the Nix sandbox only provides `/bin/sh`.

2. **Persistent Bazel cache passthrough** — allows the sandbox to read/write
   a shared Bazel cache directory so warm builds skip recompilation.

Add to `/etc/nix/nix.conf`:

```
extra-sandbox-paths = /bin/bash=/run/current-system/sw/bin/bash /var/cache/bazel-nix
```

If you're not on NixOS, find your bash path with `readlink $(which bash)` and
use that instead. For example:

```
extra-sandbox-paths = /bin/bash=/nix/store/...-bash-5.x/bin/bash /var/cache/bazel-nix
```

NixOS (`configuration.nix`):

```nix
nix.settings.extra-sandbox-paths = [
  "/bin/bash=${pkgs.bash}/bin/bash"
  "/var/cache/bazel-nix"
];
```

After editing, restart the Nix daemon:

```bash
sudo systemctl restart nix-daemon
```

Create the cache directory:

```bash
sudo mkdir -p /var/cache/bazel-nix
sudo chown root:nixbld /var/cache/bazel-nix
sudo chmod 1775 /var/cache/bazel-nix
```

The `nixbld` group ownership and sticky bit allow all Nix builder users to
share the cache while preventing cross-user file deletion.

Then build with the cached target:

```bash
# First build (cold, ~30 min):
nix build .#redpanda-cached --print-build-logs

# Subsequent builds (warm, ~5-6 min):
nix build .#redpanda-cached --print-build-logs
```

## How It Works

### Two-Derivation Architecture

The Nix build uses two derivations:

1. **Download** (`bazel-repo-cache.nix`) — 365 individual `fetchurl`
   derivations assembled into a content-addressed `linkFarm`. Each archive
   is cached independently in the Nix store, so adding or removing one
   archive only rebuilds that single fetch.

2. **Build** (`redpanda.nix`) — runs Bazel offline using the pre-fetched
   cache. Bazel extracts the archives, the nixify pipeline patches them,
   then Bazel compiles the C++ server.

See [nix-bazel-module-precacher-design.md](nix-bazel-module-precacher-design.md)
for the full design rationale.

### Pre-built Dependencies

13 third-party C/C++ libraries are built from nixpkgs (or from source with
Nix) instead of inside Bazel, saving ~10 minutes of configure/make/cmake
time on cold builds:

| Library | Type | Source |
|---------|------|--------|
| c-ares | static | nixpkgs override |
| krb5 | shared | nixpkgs |
| libxml2 | static | nixpkgs override |
| hwloc | static | nixpkgs override |
| openssl | shared | nixpkgs |
| ragel | binary | nixpkgs |
| xxhash | static | nixpkgs override |
| hdrhistogram | static | nixpkgs |
| croaring | static | nixpkgs override |
| lksctp | static | nixpkgs override |
| ada | static | from source (clang/libc++) |
| base64 | static | from source |
| openssl-fips | shared | from source (FIPS 3.1.2) |

These are injected into Bazel via `new_local_repository` rules that point to
Nix store paths. C++ static libraries must be compiled with clang/libc++ to
match the Bazel toolchain ABI.

### Bazel Cache Persistence

The `redpanda-cached` target passes `--output_base=/var/cache/bazel-nix/output_base`
to Bazel, persisting the action cache across Nix rebuilds. Two environment
variable sanitizations prevent cache key poisoning:

- **`NIX_CFLAGS_COMPILE`** — Nix injects `-frandom-seed=<derivation hash>`
  which changes every build. Bazel already sets its own per-object
  `-frandom-seed`, so the Nix one is stripped.
- **`NIX_LDFLAGS`** — Nix injects `-rpath $out/lib` where `$out` is the
  output store path (changes per derivation). Stripped because the final
  binary gets its rpath from `patchelf` in the install phase.

This achieves 100% action cache hit rate on warm builds (~7,700 actions
cached, 0 recompiles).

### The Fetch-Nixify-Build Loop

Bazel extracts archives and immediately tries to execute downloaded ELF
binaries (Python, Go, protoc, etc.), but these fail in the Nix sandbox
because they have the wrong interpreter and rpath. The build uses a
multi-pass loop:

1. **Fetch** — `bazel fetch` extracts archives from the repo cache (may fail
   when repo rules try to execute unpatched binaries)
2. **Nixify** — patch all ELF binaries in `output_base/external/` with
   `patchelf` (set interpreter + rpath) and fix shebangs (`#!/bin/bash` →
   Nix store bash)
3. **Retry** — run `bazel fetch` again; repeat until all repos resolve
4. **Build** — `bazel build` with all repos nixified

The fixup rules are defined in `nixify-rules.nix`, separate from the build
logic.

## Cache Management

### Clearing Caches

```bash
# Clear Bazel action cache (forces full recompile):
nix run .#clear-bazel

# Clear Nix derivation cache (forces re-fetch + re-nixify):
nix run .#clear-nix

# Clear both (full cold build):
nix run .#clear-all

# Manual equivalent:
sudo rm -rf /var/cache/bazel-nix/*    # Bazel cache
date > nix/entropy                     # Nix cache (changes derivation hash)
```

### Cache Troubleshooting

- **Permission denied on cache** — fix ownership:
  ```bash
  sudo chown -R root:nixbld /var/cache/bazel-nix
  sudo chmod -R g+w /var/cache/bazel-nix
  ```
- **Stale Bazel lock file** — remove it:
  ```bash
  sudo rm /var/cache/bazel-nix/output_base/*.lock
  ```
- **Warm build still recompiling** — check that `-frandom-seed` stripping
  is active in `redpanda.nix` `buildPhase`. Bazel should report ~7,700
  action cache hits with 0 local actions.

## Build Targets

### Packages

| Target | Command | Description |
|--------|---------|-------------|
| `redpanda` | `nix build .#redpanda` | Build without persistent cache |
| `redpanda-cached` | `nix build .#redpanda-cached` | Build with persistent Bazel cache |
| `rpk` | `nix build .#rpk` | Build the rpk Go CLI only |

### OCI Container Images

The default image targets work without any nix.conf changes. The `-cached`
variants use the persistent Bazel cache for faster rebuilds (requires
`/var/cache/bazel-nix` sandbox passthrough — see [Cached Build](#cached-build-recommended-for-development)).

| Target | Command | Description |
|--------|---------|-------------|
| `redpanda-image` | `nix build .#redpanda-image` | Minimal server image |
| `redpanda-image-debug` | `nix build .#redpanda-image-debug` | Server image with bash/coreutils |
| `redpanda-image-cached` | `nix build .#redpanda-image-cached` | Server image (persistent cache) |
| `redpanda-image-debug-cached` | `nix build .#redpanda-image-debug-cached` | Debug image (persistent cache) |
| `rpk-image` | `nix build .#rpk-image` | rpk CLI image |

Because the Nix images contain only the exact runtime closure (no package
manager, no extras), they are significantly smaller than the official
Redpanda Docker images:

| Image | Size | Notes |
|-------|------|-------|
| `redpanda:nix` | ~313 MB | vs ~481 MB official (35% smaller) |
| `redpanda:nix-debug` | ~327 MB | adds bash + coreutils |
| `redpanda-rpk:nix` | ~117 MB | rpk built with `-s -w` to strip debug symbols |

The smoke test (`nix run .#test-images`) prints current sizes after loading.

#### Loading and Running

```bash
# Load into Docker:
nix build .#redpanda-image && ./result | docker load

# Run:
docker run -p 9092:9092 -p 9644:9644 -p 8081:8081 -p 8082:8082 \
  redpanda:nix --smp=1 --memory=1G --default-log-level=info

# rpk:
nix build .#rpk-image && ./result | docker load
docker run --net=host redpanda-rpk:nix cluster info
```

### Testing

Run the automated smoke test (requires Docker):

    nix run .#test-images

This builds all three images, starts a redpanda server, verifies Kafka
produce/consume via rpk, and checks the debug image shell. Uses `--net=host`
so ports 9092 and 9644 must be free.

### Dev Shell

```bash
nix develop
```

Provides clang, LLVM, Python, JDK, autotools, and other build tools.
Generates `.bazelrc.nix` with Nix-specific Bazel settings.

### Benchmarking

| Target | Command | Description |
|--------|---------|-------------|
| `bench-warm` | `nix run .#bench-warm` | Both caches present |
| `bench-cold-nix` | `nix run .#bench-cold-nix` | Bazel cache present, Nix derivation rebuilt |
| `bench-cold-bazel` | `nix run .#bench-cold-bazel` | Nix derivation cached, Bazel cache cleared |
| `bench-cold-all` | `nix run .#bench-cold-all` | Both caches cleared |
| `bench-3x-warm` | `nix run .#bench-3x-warm` | 3x warm runs |
| `bench-3x-cold-nix` | `nix run .#bench-3x-cold-nix` | 3x cold-nix runs |
| `bench-matrix` | `nix run .#bench-matrix` | Full 9-build matrix |

## File Reference

### Core Build Files

| File | Description |
|------|-------------|
| `flake.nix` | Flake entry point; defines packages, apps, dev shell, checks |
| `nix/redpanda.nix` | Main C++ server build derivation (~1100 lines) |
| `nix/rpk.nix` | Go CLI package (`buildGoModule`, stripped with `-s -w` ldflags) |
| `nix/shell.nix` | Development shell with clang, LLVM, Python, JDK, autotools |
| `nix/bench.nix` | Benchmark and cache-clearing targets |
| `nix/test-images.nix` | OCI container image smoke test (`nix run .#test-images`) |
| `nix/redpanda-image.nix` | OCI container image for the redpanda server |
| `nix/rpk-image.nix` | OCI container image for the rpk CLI |

### Dependency Management

| File | Description |
|------|-------------|
| `nix/bazel-repo-cache.nix` | Builds content-addressed linkFarm from `fetchurl` derivations |
| `nix/bazel-deps.nix` | Generated list of 365 archive URLs and sha256 hashes |
| `nix/bcr.nix` | Pinned Bazel Central Registry snapshot (`fetchFromGitHub`) |
| `nix/MODULE.bazel.lock.nix` | Pre-generated lockfile matching patched MODULE.bazel |

### Pre-built Dependency Overrides

| File | Description |
|------|-------------|
| `nix/c-ares-static.nix` | Static c-ares DNS resolver |
| `nix/hwloc-static.nix` | Static hardware locality library (includes hwloc-calc/distrib) |
| `nix/libxml2-static.nix` | Static XML parsing library |
| `nix/xxhash-static.nix` | Static xxHash library |
| `nix/croaring-static.nix` | Static CRoaring bitset library |
| `nix/lksctp-static.nix` | Static SCTP protocol library |
| `nix/ada-static.nix` | Static URL parser (built from source with clang/libc++) |
| `nix/base64-static.nix` | Static base64 encoder/decoder (built from source) |
| `nix/openssl-fips.nix` | FIPS 140-2 provider (OpenSSL 3.1.2, NIST cert #4985) |

### Build Pipeline Scripts

| File | Description |
|------|-------------|
| `nix/nixify-rules.nix` | Configurable ELF/shebang fixup rules for the nixify pipeline |
| `nix/patch-module-bazel.py` | Patches MODULE.bazel for Nix sandbox compatibility |
| `nix/gen-bazel-deps.py` | Generates `bazel-deps.nix` from lockfile + BCR + MODULE.bazel |
| `nix/extract-missing-deps.py` | Finds archives in Bazel cache not yet in `bazel-deps.nix` |

## Design Documentation

- [nix-bazel-module-precacher-design.md](nix-bazel-module-precacher-design.md) —
  Primary design document covering the two-derivation architecture, per-archive
  caching, nixify pipeline, and cache persistence
- [STATUS.md](STATUS.md) — Current build status and historical lessons learned
- [rules-python-local-toolchain-patch.md](rules-python-local-toolchain-patch.md) —
  Documents the active rules_python patch for local toolchain support
