# Build a Bazel repository cache directory from a list of archive specs.
#
# Bazel's repo cache is content-addressed: it stores archives under
#   content_addressable/sha256/<hex-digest>/file
# This function fetches each archive with fetchurl and assembles a
# linkFarm in that layout. Each archive is fetched independently so
# adding/removing one archive only rebuilds that single fetchurl.
#
# Usage:
#   callPackage ./bazel-repo-cache.nix {} { archives = import ./bazel-deps.nix; }
#
{ lib, linkFarm, fetchurl }:

{ archives }:

let
  # Each archive: { url, sha256, name }
  # sha256 must be hex-encoded (not SRI).
  cacheEntries = map (a: {
    name = "content_addressable/sha256/${a.sha256}/file";
    path = fetchurl {
      url = a.url;
      sha256 = a.sha256;
      name = a.name;
    };
  }) archives;
in
linkFarm "bazel-repo-cache" cacheEntries
