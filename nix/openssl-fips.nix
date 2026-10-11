# openssl-fips.nix — Build OpenSSL 3.1.2 FIPS module from source.
#
# NIST FIPS certificate #4985 requires exactly OpenSSL 3.1.2.
# This builds only the FIPS provider (fips.so + fipsmodule.cnf),
# not the full OpenSSL library.
{ stdenv, fetchurl, perl, gnumake, coreutils }:

stdenv.mkDerivation rec {
  pname = "openssl-fips";
  version = "3.1.2";

  src = fetchurl {
    url = "https://vectorized-public.s3.amazonaws.com/dependencies/openssl-${version}.tar.gz";
    sha256 = "a0ce69b8b97ea6a35b96875235aa453b966ba3cba8af2de23657d8b6767d6539";
  };

  nativeBuildInputs = [ perl gnumake ];

  configurePhase = ''
    patchShebangs Configure
    ./Configure enable-fips --prefix=/ --openssldir=/etc/ssl --libdir=lib no-tests
  '';

  buildPhase = ''
    make -j$NIX_BUILD_CORES
  '';

  installPhase = ''
    make install_fips DESTDIR=$out

    # Remove the 'activate = 1' line from fipsmodule.cnf
    # (matches the Bazel genrule behavior)
    sed -i '/activate = 1/d' $out/etc/ssl/fipsmodule.cnf
  '';

  # Don't try to run OpenSSL tests
  doCheck = false;
}
