#!/usr/bin/env bash
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# Check an extracted sysroot for dangling references.
#
# A sysroot can be structurally complete and still fail to link: glibc ships
# some libraries as GNU ld scripts holding absolute paths (libm.so names
# libmvec.so.1 via AS_NEEDED, libc.so names libc.so.6 and libc_nonshared.a),
# and those paths are resolved *inside* the sysroot at link time. If the
# referenced file was not copied in, nothing notices until ld.lld fails on some
# unrelated target.
#
# Comparing the file list against the previous sysroot does not catch it: when
# a glibc bump makes a linker script reference something new, the file is
# missing from both old and new, so the diff is empty. The reference is what
# changed, not the file set.
#
# Usage: check-sysroot.sh <extracted-sysroot-dir>
set -euo pipefail

root=${1:?usage: check-sysroot.sh <extracted-sysroot-dir>}
root=${root%/}
fail=0

# GNU ld scripts are short text files; find them among the libraries.
while IFS= read -r script; do
  [[ $(head -c 4 "$script" 2>/dev/null) == $'\x7fELF' ]] && continue
  grep -q 'GNU ld script' "$script" 2>/dev/null || continue

  # Absolute paths named by GROUP / INPUT / AS_NEEDED are sysroot-relative.
  for ref in $(grep -oE '/[A-Za-z0-9_./+-]+\.(so[0-9.]*|a)' "$script" | sort -u); do
    if [[ ! -e "$root$ref" ]]; then
      echo "DANGLING: ${script#$root} references $ref, absent from the sysroot"
      fail=1
    fi
  done
done < <(find "$root" -type f \( -name '*.so' -o -name '*.a' \))

# Bazel rejects absolute symlinks in directory artifacts.
while IFS= read -r link; do
  echo "ABSOLUTE SYMLINK: ${link#$root} -> $(readlink "$link")"
  fail=1
done < <(find "$root" -type l -lname '/*')

# A symlink pointing at a file that was not copied in is the same class of bug.
while IFS= read -r link; do
  if [[ ! -e $link ]]; then
    echo "BROKEN SYMLINK: ${link#$root} -> $(readlink "$link")"
    fail=1
  fi
done < <(find "$root" -type l)

if ((fail)); then
  echo "FAIL: $root has dangling references"
  exit 1
fi
echo "OK: $root has no dangling linker-script references or broken symlinks"
