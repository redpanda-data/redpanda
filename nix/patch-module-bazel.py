#!/usr/bin/env python3
"""Patch MODULE.bazel for Nix sandbox build.

Removes dev_dependency extensions not needed for //src/v/redpanda:redpanda
and applies other Nix-specific patches.

Extensions REMOVED:
  - buildifier_prebuilt (dev, code formatter)
  - rules_shell (dev, shell rules)
  - toolchains_llvm + LLVM section (dev, CI LLVM — use system-clang)
  - rules_oci + OCI section (dev, Docker images)

Extensions REMOVED (pip replaced with nixpkgs):
  - pip (replaced by nixpkgs python packages via stub extension)

Extensions KEPT:
  - go_sdk (dev, provides Go toolchain via host())
  - go_deps (non-dev, provides @org_golang_google_protobuf for pbgen)
  - rust (dev, compiles wasmtime_c)
  - crate (non-dev, provides wasmtime_c source)
  - python (dev, toolchain for code gen)

Other patches:
  - go_sdk.download() → go_sdk.host() (use Go from PATH)
  - Remove go_sdk_with_systemcrypto block
  - Add rules_buf override (stub downloads)
  - Add rules_cc override (fix shebangs)
  - Replace pip extension with stub @python_deps repo (nixpkgs provides packages)

Usage: python3 nix/patch-module-bazel.py MODULE.bazel
"""

import re
import sys


def main():
    path = sys.argv[1]
    with open(path) as f:
        lines = f.read().split('\n')

    output = []
    i = 0

    def skip_block(idx):
        """Skip from current line through matching close paren/bracket."""
        depth = 0
        started = False
        while idx < len(lines):
            for ch in lines[idx]:
                if ch in '([':
                    depth += 1
                    started = True
                elif ch in ')]':
                    depth -= 1
            idx += 1
            if started and depth <= 0:
                break
        return idx

    def peek_block(idx):
        """Return the text of the block starting at idx."""
        j = idx
        block = []
        depth = 0
        started = False
        while j < len(lines):
            block.append(lines[j])
            for ch in lines[j]:
                if ch in '([':
                    depth += 1
                    started = True
                elif ch in ')]':
                    depth -= 1
            j += 1
            if started and depth <= 0:
                break
        return '\n'.join(block), j

    def is_section_header(idx, title):
        """Check if idx..idx+2 is a ====\ntitle\n==== section header."""
        if idx + 1 >= len(lines):
            return False
        return (lines[idx].strip().startswith('# ====')
                and title.lower() in lines[idx + 1].lower())

    while i < len(lines):
        line = lines[i]

        # ── Remove buildifier_prebuilt bazel_dep ──
        if 'bazel_dep(name = "buildifier_prebuilt"' in line:
            i = skip_block(i)
            continue

        # Remove its single_version_override
        if 'single_version_override(' in line:
            block_text, j = peek_block(i)
            if '"buildifier_prebuilt"' in block_text:
                i = j
                continue

        # ── Remove rules_shell ──
        if 'bazel_dep(name = "rules_shell"' in line:
            i += 1
            continue

        # ── Remove toolchains_llvm bazel_dep ──
        if 'bazel_dep(name = "toolchains_llvm"' in line:
            i = skip_block(i)
            continue

        # Remove toolchains_llvm archive_override
        if 'archive_override(' in line:
            block_text, j = peek_block(i)
            if '"toolchains_llvm"' in block_text:
                i = j
                continue

        # ── Remove entire LLVM toolchain section ──
        if is_section_header(i, 'llvm toolchain'):
            # Skip header (====, title, ====)
            i += 2
            if i < len(lines) and lines[i].strip().startswith('# ===='):
                i += 1
            # Skip blank lines
            while i < len(lines) and lines[i].strip() == '':
                i += 1
            # Skip everything until next section
            while i < len(lines):
                if is_section_header(i, 'go toolchain'):
                    break
                if is_section_header(i, 'rust toolchain'):
                    break
                i += 1
            continue

        # ── Replace go_sdk.download() with go_sdk.host() ──
        if 'go_sdk = use_extension(' in line:
            output.append('go_sdk = use_extension("@rules_go//go:extensions.bzl", "go_sdk", dev_dependency = True)')
            i = skip_block(i)
            # Skip go_sdk.download/nogo/host calls and replace with host()
            while i < len(lines) and (lines[i].strip().startswith('go_sdk.') or lines[i].strip() == ''):
                if lines[i].strip().startswith('go_sdk.'):
                    i = skip_block(i)
                else:
                    i += 1
            output.append('go_sdk.host()')
            output.append('')
            continue

        # ── Remove go_sdk_with_systemcrypto block ──
        if '# The microsoft compiler versions' in line:
            # Skip until the closing ) of the go_sdk block
            while i < len(lines):
                if lines[i].strip() == ')' and i > 0 and 'go_sdk' in '\n'.join(lines[max(0,i-10):i]):
                    i += 1
                    break
                i += 1
            continue

        # ── Remove OCI section ──
        if is_section_header(i, 'oci base images'):
            i += 2
            if i < len(lines) and lines[i].strip().startswith('# ===='):
                i += 1
            # Skip everything until http_file/http_archive declarations
            while i < len(lines):
                if lines[i].strip().startswith('http_file') or lines[i].strip().startswith('http_archive'):
                    break
                if is_section_header(i, ''):
                    break
                i += 1
            continue

        # Remove rules_oci bazel_dep
        if 'bazel_dep(name = "rules_oci"' in line:
            i += 1
            continue

        # Remove oci extension and its use_repo
        if 'oci = use_extension(' in line:
            i = skip_block(i)
            continue
        if line.strip().startswith('oci.'):
            i = skip_block(i)
            continue
        if 'use_repo(' in line:
            block_text, j = peek_block(i)
            if re.search(r'\boci\b', block_text) and 'rules_oci' not in block_text:
                i = j
                continue

        # Remove register_toolchains for llvm
        if 'register_toolchains(' in line and 'llvm_toolchain' in line:
            i = skip_block(i)
            continue

        # ── Remove pip extension (replaced by nixpkgs packages) ──
        if 'pip = use_extension(' in line:
            i = skip_block(i)
            continue
        if line.strip().startswith('pip.'):
            i = skip_block(i)
            continue
        if 'use_repo(' in line:
            block_text, j = peek_block(i)
            if re.search(r'\bpip\b', block_text) and 'rules_python' not in block_text and 'pip_tools' not in block_text:
                i = j
                continue

        output.append(line)
        i += 1

    text = '\n'.join(output)

    # Clean up multiple blank lines
    text = re.sub(r'\n{3,}', '\n\n', text)

    # ── Add Nix-specific overrides ──
    if 'module_name = "rules_buf"' not in text:
        text += '''
# Nix: stub out buf toolchain downloads (not needed for the build)
single_version_override(
    module_name = "rules_buf",
    patch_strip = 1,
    patches = ["//nix/patches:rules_buf-nix-no-download.patch"],
)
'''

    # ── Add local_path_override for rules_python ──
    # Uses the patched fork (nix-local-toolchain branch) embedded in
    # third_party/rules_python by the redpanda-src-patched derivation.
    if 'module_name = "rules_python"' not in text:
        text += '''
# Nix: use patched rules_python with local toolchain support
local_path_override(
    module_name = "rules_python",
    path = "third_party/rules_python",
)
'''

    # ── Add cc_configure extension for system-clang config ──
    # Registers @local_config_cc_toolchains so build:system-clang works.
    if 'cc_configure' not in text:
        text = text.replace(
            'bazel_dep(name = "rules_cc",',
            'bazel_dep(name = "rules_cc",',
            1,
        )
        # Insert after rules_cc bazel_dep line
        text = re.sub(
            r'(bazel_dep\(name = "rules_cc"[^)]*\))',
            r'''\1

cc_configure = use_extension("@rules_cc//cc:extensions.bzl", "cc_configure_extension")
use_repo(cc_configure, "local_config_cc_toolchains")''',
            text,
            count=1,
        )

    # ── Add python.local_toolchain for Nix system Python ──
    # The local_toolchain() call tells rules_python to use Python from PATH
    # instead of downloading a hermetic interpreter (which fails in the sandbox).
    if 'python.local_toolchain' not in text:
        text = re.sub(
            r'(python\.toolchain\([^)]*\))',
            r'''\1
python.local_toolchain(
    python_version = "3.12",
    interpreter_path = "python3",
)
use_repo(python, "local_python_3_12", "local_python_3_12_toolchains")
register_toolchains("@local_python_3_12_toolchains//:all")''',
            text,
            count=1,
        )

    # ── Replace pip with stub python_deps extension ──
    # nixpkgs provides jinja2/jsonschema via python312.withPackages.
    # The stub extension creates empty py_library targets so Bazel can
    # resolve @python_deps//jinja2 etc. — the real packages are on sys.path.
    if 'python_deps_ext' not in text:
        text += '''
# Nix: stub @python_deps repo (real packages from nixpkgs system Python)
python_deps = use_extension("//bazel:python_deps.bzl", "python_deps_ext")
use_repo(python_deps, "python_deps")
'''

    # ── Table-driven module shebang fixes ──
    # Each entry: (module_name, files, search_name, replacement)
    # search_name is the interpreter to match (e.g. "bash", "perl")
    # The sed pattern `s|#!.*<search_name>|#!<replacement>|g` replaces
    # #!...<search_name> with #!<replacement> wherever it appears.
    # Works for line-1 shebangs AND embedded shebangs in .bzl templates
    # (e.g. `return "#!/usr/bin/env bash"`).
    #
    # To add a new module, just add an entry to this table.
    MODULE_SHEBANG_FIXES = [
        # (module, files_to_fix, search_name, replacement)
        # search_name: the interpreter name to look for in shebangs (e.g. "bash")
        # replacement: what to put after #! (e.g. "$BAZEL_SH" which Nix sets)
        ("rules_foreign_cc", [
            "foreign_cc/private/framework/toolchains/linux_commands.bzl",
            "foreign_cc/private/framework/toolchains/macos_commands.bzl",
            "foreign_cc/private/framework/toolchains/freebsd_commands.bzl",
            "foreign_cc/private/runnable_binary_wrapper.sh",
        ], "bash", "$BAZEL_SH"),
        ("rules_cc", [
            "cc/private/toolchain/generate_system_module_map.sh",
            "cc/private/toolchain/grep-includes.sh",
            "cc/private/toolchain/link_dynamic_library.sh",
        ], "bash", "$BAZEL_SH"),
    ]

    for module, files, search_name, replacement in MODULE_SHEBANG_FIXES:
        if f'module_name = "{module}"' not in text:
            files_str = ' '.join(files)
            text += f'''
# Nix: fix shebangs in {module}.
# Replace #!...{search_name} with #!{replacement} (handles embedded shebangs in .bzl too).
single_version_override(
    module_name = "{module}",
    patch_cmds = [
        "sed -i \\"s|#!.*{search_name}|#!{replacement}|g\\" {files_str}",
    ],
)
'''

    # ── Pre-built protoc toolchain ──
    # Register nix_protoc toolchain FIRST so it has highest priority.
    # This uses nixpkgs protoc instead of compiling it from source (~240 actions saved).
    # Must go right after the module() block.
    if 'nix_protoc' not in text:
        text = text.replace(
            'module(\n    name = "redpanda",\n    repo_name = "com_github_redpanda_data_redpanda",\n)',
            'module(\n    name = "redpanda",\n    repo_name = "com_github_redpanda_data_redpanda",\n)\n\nregister_toolchains("//nix_protoc:nix_protoc_toolchain")',
            1,
        )

    # Fix liburing: with --spawn_strategy=local, the generate_headers genrule
    # runs ./configure in the source tree, creating config-host.h there. The
    # cc_library then sees both the genrule output AND the source-tree copy.
    # Bazel flags the source-tree copy as "undeclared inclusion". Fix by
    # cleaning up the source-tree copies after the genrule copies to output.
    if 'module_name = "liburing"' not in text:
        text += '''
# Nix: fix liburing undeclared inclusion of config-host.h.
# With --spawn_strategy=local, ./configure creates files in source tree.
# Clean them up after copying to Bazel output to avoid include validation errors.
single_version_override(
    module_name = "liburing",
    patch_cmds = [
        "sed -i '/^            done$/a\\\\            pushd $$(dirname $(location configure))\\\\n              rm -f config-host.h config-host.mak src/include/liburing/compat.h src/include/liburing/io_uring_version.h\\\\n            popd' BUILD.bazel",
    ],
)
'''

    with open(path, 'w') as f:
        f.write(text)

    print(f"Patched {path}", file=sys.stderr)


if __name__ == '__main__':
    main()
