# croaring-static.nix — Pre-built static CRoaring for Bazel.
#
# Redpanda uses a fork of CRoaring, but the fork's changes are minor.
# Nixpkgs croaring already builds a static library. We add the same
# compile-time defines that the Bazel build uses to disable NEON and AVX.
#
# Note: nixpkgs v4.3.1 vs Bazel fork — API-compatible for roaring bitmap
# operations used by redpanda (roaring_bitmap_t, roaring64_bitmap_t).
{ croaring }:

croaring.overrideAttrs (old: {
  cmakeFlags = (old.cmakeFlags or []) ++ [
    "-DROARING_DISABLE_AVX=ON"
    "-DROARING_DISABLE_NEON=ON"
  ];
})
