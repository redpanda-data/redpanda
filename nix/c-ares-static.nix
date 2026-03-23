# c-ares-static.nix — Pre-built static c-ares library for Bazel.
#
# Nixpkgs c-ares only ships shared libraries by default. Redpanda's Bazel
# build requires a static libcares.a (see bazel/thirdparty/c-ares.BUILD).
# This override builds c-ares with CARES_STATIC=ON and CARES_SHARED=OFF.
#
# Version is tracked from nixpkgs (currently 1.34.6, matching the pinned
# version in bazel/repositories.bzl).
{ c-ares }:

c-ares.overrideAttrs (old: {
  cmakeFlags = (old.cmakeFlags or []) ++ [
    "-DCARES_STATIC=ON"
    "-DCARES_SHARED=OFF"
  ];
})
