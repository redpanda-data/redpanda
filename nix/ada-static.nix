# ada-static.nix — Build ada 3.2.4 (single-header) from source.
#
# The Bazel build uses a single-header distribution (ada.h + ada.cpp)
# which bundles everything. Nixpkgs ada 3.4.2 pulls in simdutf and has
# a different build structure, so we build from the same tarball Bazel uses.
#
# Must be built with clang/libc++ to match the Bazel build toolchain.
{ stdenv, fetchurl, llvmPackages_20, unzip }:

let
  clangStdenv = llvmPackages_20.libcxxStdenv;
in
clangStdenv.mkDerivation rec {
  pname = "ada";
  version = "3.2.4";

  src = fetchurl {
    url = "https://vectorized-public.s3.us-west-2.amazonaws.com/dependencies/ada-${version}.single-header.zip";
    sha256 = "bd89fcf57c93e965e6e2488448ab9d1cf8005311808c563b288f921d987e4924";
  };

  nativeBuildInputs = [ unzip ];
  sourceRoot = ".";

  buildPhase = ''
    $CXX -c -O2 -DADA_INCLUDE_URL_PATTERN=0 -std=c++20 -o ada.o ada.cpp
    ar rcs libada.a ada.o
  '';

  installPhase = ''
    mkdir -p $out/{include,lib}
    cp ada.h ada_c.h $out/include/
    cp libada.a $out/lib/
  '';

  doCheck = false;
}
