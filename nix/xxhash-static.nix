# xxhash-static.nix — Pre-built static xxHash for Bazel.
#
# Nixpkgs xxHash uses cmake and only ships shared libraries by default.
# Redpanda's Bazel build requires a static libxxhash.a.
{ xxHash }:

xxHash.overrideAttrs (old: {
  cmakeFlags = (old.cmakeFlags or []) ++ [
    "-DBUILD_SHARED_LIBS=OFF"
  ];
})
