# hwloc-static.nix — Pre-built static hwloc library for Bazel.
#
# Nixpkgs hwloc ships shared libraries by default. Redpanda's Bazel
# build requires a static libhwloc.a (see bazel/thirdparty/hwloc.BUILD).
# This override builds a minimal static hwloc matching the Bazel
# configure options (no GPU/display backends, no libudev).
#
# Also provides hwloc-calc and hwloc-distrib binaries used by
# the packaging rules.
#
# Version is tracked from nixpkgs (currently 2.12.2, close to the pinned
# 2.11.2 in bazel/repositories.bzl).
{ hwloc }:

hwloc.overrideAttrs (old: {
  configureFlags = (old.configureFlags or []) ++ [
    "--disable-shared"
    "--enable-static"
    "--disable-libudev"
    "--disable-gl"
    "--disable-opencl"
    "--disable-nvml"
    "--disable-cuda"
    "--disable-rsmi"
  ];
})
