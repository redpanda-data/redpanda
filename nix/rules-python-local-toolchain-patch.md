# Patch: `python.local_toolchain()` for rules_python

## Problem

Bazel's `rules_python` (v1.5.1) unconditionally downloads a pre-built
CPython binary when `python.toolchain()` is used in MODULE.bazel. These
binaries are built for FHS environments and contain a hardcoded ELF
interpreter path (`/lib64/ld-linux-x86-64.so.2`). Inside a Nix build
sandbox — which provides a non-FHS filesystem with no `/lib64` — these
binaries cannot execute.

The Nix environment already provides a working Python at a `/nix/store/...`
path with a correct Nix-store ELF interpreter. If Bazel used this Python
instead of its downloaded one, no patching or FHS compatibility layer would
be needed.

rules_python already has a `local_runtime_repo` mechanism that discovers a
system Python via PATH lookup, but it is only available for legacy WORKSPACE
files — not through the MODULE.bazel extension system.

## What the Patch Does

The patch modifies a single file (`python/private/python.bzl`) in
rules_python to expose the existing `local_runtime_repo` mechanism through
the `python` module extension. It adds a `python.local_toolchain()` tag
class that can be used alongside `python.toolchain()` in MODULE.bazel.

### Changes (applied to rules_python v1.5.1, branch `nix-local-toolchain`)

**File: `python/private/python.bzl`** — 70 lines changed, zero new files.

| Location | What |
|----------|------|
| Lines 21–22 | Import `local_runtime_repo` and `local_runtime_toolchains_repo` (already exist in the codebase) |
| Lines 507–525 | Processing loop in `_python_impl()`: for each `local_toolchain` tag, create a `local_runtime_repo` (discovers the system Python) and a `local_runtime_toolchains_repo` (generates `toolchain()` targets) |
| Line 960 | Add `"PATH"` to the extension's `environ` list so it re-evaluates when PATH changes |
| Lines 1107–1153 | `_local_toolchain` tag class with three attributes: `interpreter_path` (default `"python3"`), `python_version` (mandatory), `on_failure` (`"skip"`, `"warn"`, or `"fail"`) |
| Line 1421 | Register `"local_toolchain": _local_toolchain` in the extension's `tag_classes` dict |

No new repository rules, no new BUILD generation logic, no changes to how
Python is discovered or queried. The patch wires existing code into the
module extension.

### How It Works

1. User adds `python.local_toolchain(python_version = "3.12")` to
   MODULE.bazel
2. During extension evaluation, `_python_impl()` iterates `local_toolchain`
   tags and calls `local_runtime_repo(name = "local_python_3_12",
   interpreter_path = "python3")`
3. `local_runtime_repo` calls `_resolve_interpreter_path()`, which runs
   `which python3` — in a Nix environment this resolves to
   `/nix/store/...-python3-3.12.12/bin/python3`
4. It executes `get_local_runtime_info.py` against that interpreter to query
   version, ABI flags, library paths, and include directories
5. It generates a `BUILD.bazel` with `py_runtime(interpreter_path =
   "/nix/store/.../bin/python3")` — a *platform runtime* (no downloaded
   files in runfiles)
6. `local_runtime_toolchains_repo` generates `toolchain()` targets that
   reference the runtime repo
7. The consuming MODULE.bazel calls `register_toolchains(
   "@local_python_3_12_toolchains//:all")` to make them available

The `python.toolchain()` call remains for non-Nix builds. When both are
present, toolchain resolution order determines which is used — the local
toolchain is registered first and wins when a local Python is found. When
`on_failure = "warn"` (the default) and no local Python exists, the repo
generates an incompatible-platform stub and the downloaded toolchain serves
as fallback.

## MODULE.bazel Usage

```starlark
bazel_dep(name = "rules_python", version = "1.5.1")
local_path_override(
    module_name = "rules_python",
    path = "/path/to/patched/rules_python",
)

python = use_extension("@rules_python//python/extensions:python.bzl", "python", dev_dependency = True)
python.toolchain(
    ignore_root_user_error = True,
    is_default = True,
    python_version = "3.12",
)
python.local_toolchain(
    python_version = "3.12",
    interpreter_path = "python3",
)
use_repo(python, "local_python_3_12", "local_python_3_12_toolchains")
register_toolchains("@local_python_3_12_toolchains//:all")
```

## Outcome

Tested inside `nix develop` on the Redpanda repository (Bazel 8.4.1,
nixpkgs Python 3.12.12, bazelisk 1.28.1).

### `bazelisk query --output=build @local_python_3_12//:_py3_runtime`

```starlark
py_runtime(
  name = "_py3_runtime",
  implementation_name = "cpython",
  interpreter_path = "/nix/store/flbw79qdmvzbdrafd93avy5a7d29m2vb-python3-3.12.12/bin/python3",
  interpreter_version_info = {"major": "3", "micro": "12", "minor": "12"},
  python_version = "PY3",
)
```

### `bazelisk query @local_python_3_12_toolchains//:all`

```
@local_python_3_12_toolchains//:0000_toolchain
@local_python_3_12_toolchains//:0000_py_cc_toolchain
@local_python_3_12_toolchains//:0000_py_exec_tools_toolchain
@local_python_3_12_toolchains//:0001_default_toolchain
@local_python_3_12_toolchains//:0001_default_py_cc_toolchain
@local_python_3_12_toolchains//:0001_default_py_exec_tools_toolchain
```

### `bazelisk build @local_python_3_12//:python_runtimes`

```
INFO: Analyzed target @@rules_python++python+local_python_3_12//:python_runtimes
      (10 packages loaded, 23 targets configured).
INFO: Found 1 target...
INFO: Build completed successfully, 1 total action
```

### What this means

- The Nix-provided Python (`/nix/store/...-python3-3.12.12/bin/python3`) is
  used directly as the toolchain interpreter
- No FHS binary is downloaded and executed
- No `/lib64/ld-linux-x86-64.so.2` is needed
- No `patchelf` is needed
- The build succeeds inside `nix develop`

## Applicability Beyond Nix

This patch benefits any environment where Python is pre-installed and a
download is unnecessary or undesirable:

- **NixOS / Nix flakes** — non-FHS environment, downloaded binaries fail
- **Guix** — same non-FHS pattern as Nix
- **CI with pre-installed Python** — avoids redundant download on every build
- **Air-gapped environments** — no network access for downloads
- **Custom Python builds** — debug builds, instrumented builds, patched
  interpreters

## Upstream Potential

The patch is intentionally minimal (70 lines, one file, no new abstractions)
to make it suitable for an upstream contribution to `rules_python`. It uses
only existing, tested infrastructure — `local_runtime_repo` and
`local_runtime_toolchains_repo` have been in rules_python since v1.4.0.
