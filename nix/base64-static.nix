# base64-static.nix — Build aklomp/base64 from source as a static library.
#
# This library is not packaged in nixpkgs. We build from the same tarball
# that Bazel fetches, with matching cmake flags for x86_64 SIMD support.
{ stdenv, cmake, ninja, fetchurl }:

stdenv.mkDerivation rec {
  pname = "base64";
  version = "0.5.0";

  src = fetchurl {
    url = "https://vectorized-public.s3.amazonaws.com/dependencies/base64-v${version}.tar.gz";
    sha256 = "b21be58a90d31302ba86056db7ef77a481393b9359c505be5337d7d54e8a0559";
  };

  nativeBuildInputs = [ cmake ninja ];

  cmakeFlags = [
    "-DBUILD_SHARED_LIBS=OFF"
    "-DCMAKE_INSTALL_LIBDIR=lib"
    "-DBASE64_WITH_OpenMP=OFF"
    "-DBASE64_WERROR=OFF"
  ] ++ (if stdenv.hostPlatform.isx86_64 then [
    "-DBASE64_WITH_SSSE3=ON"
    "-DBASE64_WITH_SSE41=ON"
    "-DBASE64_WITH_SSE42=ON"
    "-DBASE64_WITH_AVX=OFF"
    "-DBASE64_WITH_AVX2=OFF"
    "-DBASE64_WITH_NEON32=OFF"
    "-DBASE64_WITH_NEON64=OFF"
  ] else [
    "-DBASE64_WITH_SSSE3=OFF"
    "-DBASE64_WITH_SSE41=OFF"
    "-DBASE64_WITH_SSE42=OFF"
    "-DBASE64_WITH_AVX=OFF"
    "-DBASE64_WITH_AVX2=OFF"
    "-DBASE64_WITH_NEON32=OFF"
    "-DBASE64_WITH_NEON64=ON"
  ]);
}
