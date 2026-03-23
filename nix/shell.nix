{
  mkShell,
  bazelisk,
  llvmPackages_20,
  stdenv,
  python312,
  jdk_headless,
  autoconf,
  automake,
  libtool,
  bison,
  pkg-config,
  elfutils,
  xfsprogs,
  valgrind,
  git,
  zstd,
}:

let
  # GCC runtime lib (libstdc++.so.6) — needed by exec-config binaries
  # like protoc_minimal that are built with the auto-detected CC toolchain.
  gccLib = stdenv.cc.cc.lib;
  libPath = "${llvmPackages_20.libcxx}/lib:${gccLib}/lib";
  pythonEnv = python312.withPackages (ps: [
    ps.jinja2
    ps.jsonschema
    ps.kafka-python-ng
  ]);
in

mkShell {
  packages = [
    bazelisk
    llvmPackages_20.libcxxClang
    llvmPackages_20.lld
    llvmPackages_20.llvm
    llvmPackages_20.libcxx
    pythonEnv
    jdk_headless
    autoconf
    automake
    libtool
    bison
    pkg-config
    elfutils
    xfsprogs
    valgrind
    git
    zstd
  ];

  shellHook = ''
    # Bazelisk reads .bazelversion to pick the right Bazel version.
    # Alias so that "bazel" invokes bazelisk.
    alias bazel=bazelisk

    export CC=clang
    export CXX=clang++

    # Generate .bazelrc.nix with Nix-specific Bazel settings:
    # - shell_executable: NixOS has no /bin/bash
    # - PATH: sandbox needs Nix store paths for genrules
    # - NIX_*: the Nix clang/ld wrappers read these to inject
    #   -isystem, -L, and linker flags
    cat > .bazelrc.nix <<RCEOF
build --shell_executable=$(which bash)
build --action_env=PATH=$PATH
build --host_action_env=PATH=$PATH
build --action_env=NIX_LDFLAGS
build --host_action_env=NIX_LDFLAGS
build --action_env=NIX_CFLAGS_COMPILE
build --host_action_env=NIX_CFLAGS_COMPILE
build --action_env=NIX_CC
build --host_action_env=NIX_CC
build --action_env=NIX_BINTOOLS
build --host_action_env=NIX_BINTOOLS
build --action_env=NIX_CC_WRAPPER_TARGET_HOST_x86_64_unknown_linux_gnu
build --host_action_env=NIX_CC_WRAPPER_TARGET_HOST_x86_64_unknown_linux_gnu
build --action_env=NIX_BINTOOLS_WRAPPER_TARGET_HOST_x86_64_unknown_linux_gnu
build --host_action_env=NIX_BINTOOLS_WRAPPER_TARGET_HOST_x86_64_unknown_linux_gnu
build --action_env=NIX_HARDENING_ENABLE
build --host_action_env=NIX_HARDENING_ENABLE
build --action_env=NIX_ENFORCE_NO_NATIVE
build --host_action_env=NIX_ENFORCE_NO_NATIVE
build --action_env=ACLOCAL_PATH=$ACLOCAL_PATH
build --host_action_env=ACLOCAL_PATH=$ACLOCAL_PATH
build --action_env=LIBRARY_PATH=${libPath}
build --host_action_env=LIBRARY_PATH=${libPath}
build --action_env=LD_LIBRARY_PATH=${libPath}
build --host_action_env=LD_LIBRARY_PATH=${libPath}
build --linkopt=-Wl,-rpath,${llvmPackages_20.libcxx}/lib
build --linkopt=-Wl,-rpath,${gccLib}/lib
build --host_linkopt=-Wl,-rpath,${llvmPackages_20.libcxx}/lib
build --host_linkopt=-Wl,-rpath,${gccLib}/lib
build --@protobuf//bazel/toolchains:allow_nonstandard_protoc
RCEOF
  '';
}
