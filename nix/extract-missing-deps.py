#!/usr/bin/env python3
"""Extract URLs and sha256 hashes for archives missing from bazel-deps.nix.

Scans the Bazel repository cache, identifies entries not already in
bazel-deps.nix, determines their type and constructs download URLs.

Outputs Nix list entries to stdout for appending to bazel-deps.nix.
"""

import gzip
import json
import os
import re
import struct
import subprocess
import sys
import tarfile
from pathlib import Path

CACHE_DIR = Path.home() / ".cache/bazel/_bazel_das/cache/repos/v1/content_addressable/sha256"
DEPS_FILE = Path(__file__).parent / "bazel-deps.nix"


def load_existing_hashes():
    """Load sha256 hashes already in bazel-deps.nix."""
    hashes = set()
    with open(DEPS_FILE) as f:
        for line in f:
            m = re.search(r'sha256\s*=\s*"([0-9a-f]{64})"', line)
            if m:
                hashes.add(m.group(1))
    return hashes


def identify_crate(filepath):
    """If file is a .crate (gzipped tarball), extract crate name and version."""
    try:
        # Check gzip original filename first (faster than extracting)
        with open(filepath, 'rb') as f:
            magic = f.read(2)
            if magic != b'\x1f\x8b':
                return None
            f.read(1)  # method
            flags = struct.unpack('B', f.read(1))[0]
            f.read(6)  # mtime, xfl, os
            has_orig_name = flags & 0x08
            if flags & 0x04:  # FEXTRA
                xlen = struct.unpack('<H', f.read(2))[0]
                f.read(xlen)
            if has_orig_name:
                name = b''
                while True:
                    c = f.read(1)
                    if c == b'\x00' or c == b'':
                        break
                    name += c
                name = name.decode('utf-8', errors='replace')
                # e.g. "tinyjson-2.5.1.crate"
                m = re.match(r'^(.+)-(\d+\.\d+\.\d+(?:[^.]*))\.crate$', name)
                if m:
                    return m.group(1), m.group(2)

        # Fallback: try to list tarball contents
        try:
            with tarfile.open(filepath, 'r:gz') as tf:
                members = tf.getnames()[:5]
                for member in members:
                    # e.g. "tinyjson-2.5.1/Cargo.toml"
                    m = re.match(r'^([^/]+)-(\d+\.\d+\.\d+(?:\S*))/Cargo\.toml$', member)
                    if m:
                        return m.group(1), m.group(2)
                    m = re.match(r'^([^/]+)-(\d+\.\d+\.\d+(?:\S*))/[^/]', member)
                    if m:
                        return m.group(1), m.group(2)
        except Exception:
            pass
    except Exception:
        pass
    return None


def identify_rust_toolchain(filepath):
    """Check if this is a Rust toolchain archive."""
    try:
        size = os.path.getsize(filepath)
        if size < 1_000_000:  # toolchain archives are large
            return None

        # Try as xz or gzipped tarball
        try:
            with tarfile.open(filepath, 'r:*') as tf:
                members = tf.getnames()[:20]
                for member in members:
                    # e.g. "rust-std-1.86.0-x86_64-unknown-linux-gnu/..."
                    m = re.match(r'^(rust-std|rustc|cargo|clippy|rustfmt|llvm-tools|rust-src)-(\d+\.\d+\.\d+)(-[^/]+)?/', member)
                    if m:
                        component = m.group(1)
                        version = m.group(2)
                        target = m.group(3) or ""
                        target = target.lstrip('-')
                        return component, version, target
        except Exception:
            pass

        # Check XZ files separately
        with open(filepath, 'rb') as f:
            magic = f.read(6)
            if magic[:5] == b'\xfd7zXZ':
                # It's XZ compressed - use command line
                try:
                    result = subprocess.run(
                        ['tar', 'tJf', filepath],
                        capture_output=True, text=True, timeout=10
                    )
                    for line in result.stdout.splitlines()[:20]:
                        m = re.match(r'^(rust-std|rustc|cargo|clippy|rustfmt|llvm-tools|rust-src)-(\d+\.\d+\.\d+)(-[^/]+)?/', line)
                        if m:
                            return m.group(1), m.group(2), (m.group(3) or "").lstrip('-')
                except Exception:
                    pass
    except Exception:
        pass
    return None


def identify_cargo_bazel(filepath):
    """Check if this is the cargo-bazel binary."""
    try:
        result = subprocess.run(['file', '-b', filepath], capture_output=True, text=True)
        if 'ELF' in result.stdout and os.path.getsize(filepath) > 20_000_000:
            return True
    except Exception:
        pass
    return False


def identify_python_package(filepath):
    """Check if this is a Python wheel or sdist."""
    try:
        name = None
        with open(filepath, 'rb') as f:
            magic = f.read(4)

        # ZIP (wheel)
        if magic[:2] == b'PK':
            import zipfile
            try:
                with zipfile.ZipFile(filepath) as zf:
                    for n in zf.namelist():
                        m = re.match(r'^([^/]+)\.dist-info/METADATA$', n)
                        if m:
                            # Read METADATA to get name and version
                            with zf.open(n) as meta:
                                content = meta.read().decode('utf-8', errors='replace')
                                name_match = re.search(r'^Name:\s*(.+)$', content, re.M)
                                ver_match = re.search(r'^Version:\s*(.+)$', content, re.M)
                                if name_match and ver_match:
                                    return name_match.group(1).strip(), ver_match.group(1).strip(), 'wheel'
            except Exception:
                pass

        # Gzip (sdist)
        if magic[:2] == b'\x1f\x8b':
            try:
                with tarfile.open(filepath, 'r:gz') as tf:
                    for member in tf.getnames()[:10]:
                        m = re.match(r'^([^/]+)-(\d+\.\d+(?:\.\d+)?)/PKG-INFO$', member)
                        if m:
                            return m.group(1), m.group(2), 'sdist'
            except Exception:
                pass
    except Exception:
        pass
    return None


def construct_crate_url(name, version):
    """Construct the crates.io download URL for a crate."""
    return f"https://static.crates.io/crates/{name}/{name}-{version}.crate"


def construct_rust_toolchain_url(component, version, target):
    """Construct the Rust toolchain download URL."""
    if target:
        return f"https://static.rust-lang.org/dist/rust-{component}-{version}-{target}.tar.xz"
    return f"https://static.rust-lang.org/dist/rust-{component}-{version}.tar.xz"


def main():
    existing = load_existing_hashes()
    print(f"# Existing entries in bazel-deps.nix: {len(existing)}", file=sys.stderr)

    all_hashes = sorted(os.listdir(CACHE_DIR))
    missing = [h for h in all_hashes if h not in existing and len(h) == 64]
    print(f"# Missing from bazel-deps.nix: {len(missing)}", file=sys.stderr)

    results = {
        'crates': [],
        'rust_toolchain': [],
        'cargo_bazel': [],
        'python': [],
        'unknown_large': [],
        'small_skipped': 0,
    }

    for i, h in enumerate(missing):
        filepath = CACHE_DIR / h / "file"
        if not filepath.exists():
            continue

        size = filepath.stat().st_size

        # Skip very small files (BCR registry files, <2KB)
        if size < 2000:
            results['small_skipped'] += 1
            continue

        if i % 50 == 0:
            print(f"# Processing {i}/{len(missing)}...", file=sys.stderr)

        # Try to identify as a crate
        crate = identify_crate(str(filepath))
        if crate:
            name, version = crate
            url = construct_crate_url(name, version)
            results['crates'].append({
                'sha256': h,
                'url': url,
                'name': f"{name}-{version}.crate",
            })
            continue

        # Try Rust toolchain
        rtc = identify_rust_toolchain(str(filepath))
        if rtc:
            component, version, target = rtc
            if target:
                fname = f"{component}-{version}-{target}.tar.xz"
            else:
                fname = f"{component}-{version}.tar.xz"
            url = f"https://static.rust-lang.org/dist/{fname}"
            results['rust_toolchain'].append({
                'sha256': h,
                'url': url,
                'name': fname,
            })
            continue

        # Try cargo-bazel binary
        if identify_cargo_bazel(str(filepath)):
            # Need to determine the exact URL - check rules_rust version
            results['cargo_bazel'].append({
                'sha256': h,
                'url': 'CARGO_BAZEL_PLACEHOLDER',
                'name': 'cargo-bazel',
                'size': size,
            })
            continue

        # Try Python package
        pyinfo = identify_python_package(str(filepath))
        if pyinfo:
            name, version, pkg_type = pyinfo
            results['python'].append({
                'sha256': h,
                'name': f"{name}-{version}",
                'pkg_name': name,
                'pkg_version': version,
                'pkg_type': pkg_type,
                'size': size,
            })
            continue

        # Unknown large file
        if size > 10000:
            ftype = subprocess.run(['file', '-b', str(filepath)],
                                   capture_output=True, text=True).stdout.strip()[:80]
            results['unknown_large'].append({
                'sha256': h,
                'size': size,
                'type': ftype,
            })

    # Output summary to stderr
    print(f"\n# === Results ===", file=sys.stderr)
    print(f"# Crates: {len(results['crates'])}", file=sys.stderr)
    print(f"# Rust toolchain: {len(results['rust_toolchain'])}", file=sys.stderr)
    print(f"# Cargo-bazel: {len(results['cargo_bazel'])}", file=sys.stderr)
    print(f"# Python packages: {len(results['python'])}", file=sys.stderr)
    print(f"# Unknown large: {len(results['unknown_large'])}", file=sys.stderr)
    print(f"# Small skipped: {results['small_skipped']}", file=sys.stderr)

    # Output unknown large files for debugging
    if results['unknown_large']:
        print(f"\n# === Unknown large files ===", file=sys.stderr)
        for u in results['unknown_large']:
            print(f"#   {u['sha256']} ({u['size']} bytes): {u['type']}", file=sys.stderr)

    if results['python']:
        print(f"\n# === Python packages ===", file=sys.stderr)
        for p in results['python']:
            print(f"#   {p['pkg_name']} {p['pkg_version']} ({p['pkg_type']}, {p['size']} bytes)", file=sys.stderr)

    # Output Nix entries to stdout
    print("# Additional archives for dev_dependency extensions")
    print("# (Rust crates, toolchain, cargo-bazel)")
    print("# Generated by extract-missing-deps.py")
    print("[")

    for entry in sorted(results['crates'], key=lambda e: e['name']):
        print(f'  {{')
        print(f'    url = "{entry["url"]}";')
        print(f'    sha256 = "{entry["sha256"]}";')
        print(f'    name = "{entry["name"]}";')
        print(f'  }}')

    for entry in results['rust_toolchain']:
        print(f'  {{')
        print(f'    url = "{entry["url"]}";')
        print(f'    sha256 = "{entry["sha256"]}";')
        print(f'    name = "{entry["name"]}";')
        print(f'  }}')

    for entry in results['cargo_bazel']:
        print(f'  # TODO: determine exact cargo-bazel URL')
        print(f'  # sha256 = "{entry["sha256"]}" ({entry["size"]} bytes)')

    print("]")


if __name__ == '__main__':
    main()
