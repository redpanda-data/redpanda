# lksctp-static.nix — Pre-built static lksctp-tools for Bazel.
#
# Nixpkgs lksctp-tools only ships shared libraries by default.
# Seastar only needs the netinet/sctp.h header (no functions are
# actually called), but a static lib is provided for link completeness.
{ lksctp-tools }:

lksctp-tools.overrideAttrs (old: {
  configureFlags = (old.configureFlags or []) ++ [
    "--disable-shared"
    "--enable-static"
  ];
})
