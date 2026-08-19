{
  lib,
  stdenv,
  callPackage,
  runCommand,
  writeShellApplication,
  fetchurl,
  bazel_8,
  bazelisk,
  llvmPackages_20,
  python312,
  go,
  jdk_headless,
  autoconf,
  automake,
  libtool,
  bison,
  pkg-config,
  elfutils,
  xfsprogs,
  valgrind,
  patchelf,
  file,
  findutils,
  git,
  cacert,
  zstd,
  coreutils,
  bash,
  gnused,
  gnumake,
  gnugrep,
  gawk,
  perl,
  m4,
  glibc,
  gcc-unwrapped,
  zlib,
  openssl,
  c-ares,
  hwloc,
  krb5,
  libxml2,
  ragel,
  xxHash,
  hdrhistogram_c,
  croaring,
  lksctp-tools,
  # openssl is already in inputs (used for nixify rpath); reused here
  # for the pre-built openssl substitution.
  curl,
  lndir,
  protobuf,
  bazelCacheDir ? "",
  optimizationLevel ? "default", # "default" | "release" | "lto"
  pgoMode ? "off", # "off" | "instrument" | "optimize"
  pgoProfilePath ? null, # path to .profdata file (required when pgoMode == "optimize")
}:

assert builtins.elem optimizationLevel [ "default" "release" "lto" ];
assert builtins.elem pgoMode [ "off" "instrument" "optimize" ];
assert pgoMode == "optimize" -> pgoProfilePath != null;

let
  version = "0.0.0-dev";

  # Python with build-time code generation dependencies (jinja2, jsonschema).
  # Replaces the pip extension — nixpkgs provides the packages directly.
  pythonWithDeps = python312.withPackages (ps: [
    ps.jinja2
    ps.jsonschema
  ]);

  c-aresStatic = callPackage ./c-ares-static.nix { };
  hwlocStatic = callPackage ./hwloc-static.nix { };
  libxml2Static = callPackage ./libxml2-static.nix { };
  base64Static = callPackage ./base64-static.nix { };
  opensslFips = callPackage ./openssl-fips.nix { };
  xxhashStatic = callPackage ./xxhash-static.nix { };
  adaStatic = callPackage ./ada-static.nix { };
  croaringStatic = callPackage ./croaring-static.nix { };
  lksctpStatic = callPackage ./lksctp-static.nix { };

  gccLib = stdenv.cc.cc.lib;

  # Local source with filter to exclude build artifacts
  localSrc = lib.cleanSourceWith {
    src = ./..;
    filter = path: type:
      !(builtins.elem (baseNameOf path) [
        ".git" "bazel-bin" "bazel-out" "bazel-redpanda"
        "bazel-testlogs" "result" "result-rpk"
      ]);
  };

  # Fetch the patched rules_python (nix-local-toolchain branch) and
  # embed it in the source tree so local_path_override works in the sandbox.
  rulesPythonSrc = builtins.fetchGit {
    url = "/home/das/Downloads/rules_python";
    ref = "nix-local-toolchain";
    rev = "a9b9c43c62e1fbc14dc162153dc8a5e623f83f3d";
  };

  src = runCommand "redpanda-src-patched" { } ''
    cp -r --no-preserve=mode ${localSrc} $out

    # Embed rules_python in tree
    mkdir -p $out/third_party
    cp -r --no-preserve=mode ${rulesPythonSrc} $out/third_party/rules_python

    # Create stub @python_deps extension (nixpkgs provides the real packages)
    cat > $out/bazel/python_deps.bzl <<'PYEXT'
"""Stub @python_deps repo for Nix builds.

Creates empty py_library targets for each Python package.
The real packages (jinja2, jsonschema, etc.) are provided by nixpkgs'
python312.withPackages and are on sys.path automatically.
"""

def _python_deps_impl(rctx):
    packages = [
        "jinja2", "jsonschema", "markupsafe",
        "aioboto3", "boto3", "psutil", "pyyaml", "s3transfer",
    ]
    rctx.file("BUILD.bazel", "")
    for pkg in packages:
        rctx.file("{}/BUILD.bazel".format(pkg), 'py_library(name = "{}", visibility = ["//visibility:public"])'.format(pkg))

_python_deps_repo = repository_rule(implementation = _python_deps_impl)

def _python_deps_ext_impl(ctx):
    _python_deps_repo(name = "python_deps")

python_deps_ext = module_extension(implementation = _python_deps_ext_impl)
PYEXT

    # Create nix_protoc/ — pre-built protoc toolchain from nixpkgs.
    # Avoids compiling protoc + abseil + zlib from source (~240 actions).
    mkdir -p $out/nix_protoc/bin
    ln -s ${protobuf}/bin/protoc $out/nix_protoc/bin/protoc
    cat > $out/nix_protoc/BUILD.bazel <<'PROTOC_BUILD'
load("@protobuf//bazel/toolchains:proto_toolchain.bzl", "proto_toolchain")
exports_files(["bin/protoc"])
proto_toolchain(
    name = "nix_protoc",
    proto_compiler = "bin/protoc",
)
PROTOC_BUILD

    # Create nix_cares/ — pre-built c-ares from nixpkgs.
    # Avoids running cmake build (~51s wall time).
    mkdir -p $out/nix_cares/{include,lib}
    for h in ${c-aresStatic.dev}/include/ares*.h; do
      ln -s "$h" $out/nix_cares/include/
    done
    ln -s ${c-aresStatic}/lib/libcares.a $out/nix_cares/lib/

    cat > $out/bazel/thirdparty/c-ares-prebuilt.BUILD <<'CARES_BUILD'
cc_import(
    name = "cares_lib",
    static_library = "lib/libcares.a",
)
cc_library(
    name = "c-ares",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],
    deps = [":cares_lib"],
    visibility = ["//visibility:public"],
)
CARES_BUILD

    # Create nix_krb5/ — pre-built krb5 from nixpkgs.
    # Avoids running configure_make build (~145s wall time).
    # Uses shared libs (krb5 has duplicate-symbol issues with static linking).
    mkdir -p $out/nix_krb5/{include,lib}
    for item in ${krb5.dev}/include/*; do
      ln -s "$item" $out/nix_krb5/include/
    done
    for lib in libcom_err.so.3 libgssapi_krb5.so.2 libk5crypto.so.3 libkrb5.so.3 libkrb5support.so.0; do
      ln -s ${krb5.lib}/lib/$lib $out/nix_krb5/lib/
    done

    cat > $out/bazel/thirdparty/krb5-prebuilt.BUILD <<'KRB5_BUILD'
cc_import(name = "com_err_lib", shared_library = "lib/libcom_err.so.3")
cc_import(name = "gssapi_krb5_lib", shared_library = "lib/libgssapi_krb5.so.2")
cc_import(name = "k5crypto_lib", shared_library = "lib/libk5crypto.so.3")
cc_import(name = "krb5_lib", shared_library = "lib/libkrb5.so.3")
cc_import(name = "krb5support_lib", shared_library = "lib/libkrb5support.so.0")
cc_library(
    name = "krb5",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],
    deps = [":com_err_lib", ":gssapi_krb5_lib", ":k5crypto_lib", ":krb5_lib", ":krb5support_lib"],
    visibility = ["//visibility:public"],
)
KRB5_BUILD

    # Create nix_libxml2/ — pre-built libxml2 from nixpkgs.
    # Avoids running configure_make build (~78s wall time).
    mkdir -p $out/nix_libxml2/{include,lib}
    ln -s ${libxml2Static.dev}/include/libxml2/libxml $out/nix_libxml2/include/libxml
    ln -s ${libxml2Static.out}/lib/libxml2.a $out/nix_libxml2/lib/

    cat > $out/bazel/thirdparty/libxml2-prebuilt.BUILD <<'LIBXML2_BUILD'
cc_import(
    name = "libxml2_lib",
    static_library = "lib/libxml2.a",
)
cc_library(
    name = "libxml2",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],
    deps = [":libxml2_lib"],
    visibility = ["//visibility:public"],
)
LIBXML2_BUILD

    # Create nix_hwloc/ — pre-built hwloc from nixpkgs.
    # Avoids running configure_make build (~72s wall time).
    # Includes static lib, headers, and the two binaries used by packaging.
    mkdir -p $out/nix_hwloc/{include,lib,bin}
    for item in ${hwlocStatic.dev}/include/*; do
      ln -s "$item" $out/nix_hwloc/include/
    done
    ln -s ${hwlocStatic.lib}/lib/libhwloc.a $out/nix_hwloc/lib/
    ln -s ${hwlocStatic}/bin/hwloc-calc $out/nix_hwloc/bin/
    ln -s ${hwlocStatic}/bin/hwloc-distrib $out/nix_hwloc/bin/

    cat > $out/bazel/thirdparty/hwloc-prebuilt.BUILD <<'HWLOC_BUILD'
cc_import(
    name = "hwloc_lib",
    static_library = "lib/libhwloc.a",
)
cc_library(
    name = "hwloc",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],
    deps = [":hwloc_lib"],
    visibility = ["//visibility:public"],
)
exports_files(["bin/hwloc-calc", "bin/hwloc-distrib"])
filegroup(
    name = "hwloc_calc",
    srcs = ["bin/hwloc-calc"],
    visibility = ["//visibility:public"],
)
filegroup(
    name = "hwloc_distrib",
    srcs = ["bin/hwloc-distrib"],
    visibility = ["//visibility:public"],
)
HWLOC_BUILD

    # Create nix_openssl/ — pre-built openssl from nixpkgs.
    # Avoids running Configure + make build (~118s wall time).
    # Provides shared libs, headers, binary, ssl data, and the
    # build settings that openssl-fips.BUILD cross-references.
    mkdir -p $out/nix_openssl/{include,lib,bin,etc}
    ln -s ${openssl.dev}/include/openssl $out/nix_openssl/include/openssl
    for lib in libssl.so.3 libcrypto.so.3; do
      ln -s ${openssl.out}/lib/$lib $out/nix_openssl/lib/
    done
    ln -s ${openssl.bin}/bin/openssl $out/nix_openssl/bin/
    ln -s ${openssl.out}/etc/ssl $out/nix_openssl/etc/ssl

    cat > $out/bazel/thirdparty/openssl-prebuilt.BUILD <<'OPENSSL_BUILD'
load("@bazel_skylib//rules:common_settings.bzl", "int_flag", "string_flag")

# Settings referenced by openssl-fips.BUILD
int_flag(
    name = "build_jobs",
    build_setting_default = 8,
    make_variable = "BUILD_JOBS",
    visibility = ["@openssl-fips//:__pkg__"],
)
string_flag(
    name = "build_mode",
    build_setting_default = "default",
    values = ["debug", "release", "default"],
)
config_setting(
    name = "debug_mode",
    flag_values = {":build_mode": "debug"},
)
config_setting(
    name = "release_mode",
    flag_values = {":build_mode": "release"},
)

cc_import(name = "ssl_lib", shared_library = "lib/libssl.so.3")
cc_import(name = "crypto_lib", shared_library = "lib/libcrypto.so.3")

cc_library(
    name = "openssl_foreign_cc",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],
    deps = [":ssl_lib", ":crypto_lib"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "openssl",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],
    deps = [":ssl_lib", ":crypto_lib"],
    visibility = ["//visibility:public"],
)

exports_files(["bin/openssl"])
filegroup(
    name = "openssl_binary",
    srcs = ["bin/openssl"],
    visibility = ["//visibility:public"],
)
OPENSSL_BUILD

    # Create nix_ragel/ — pre-built ragel binary from nixpkgs.
    # Avoids running configure_make + autoreconf build (~29s wall time).
    mkdir -p $out/nix_ragel/bin
    ln -s ${ragel}/bin/ragel $out/nix_ragel/bin/ragel

    cat > $out/bazel/thirdparty/ragel-prebuilt.BUILD <<'RAGEL_BUILD'
load("@bazel_skylib//rules:common_settings.bzl", "int_flag")

int_flag(
    name = "build_jobs",
    build_setting_default = 8,
    make_variable = "BUILD_JOBS",
)

exports_files(["bin/ragel"])
filegroup(
    name = "ragel_bin",
    srcs = ["bin/ragel"],
    visibility = ["//visibility:public"],
)
RAGEL_BUILD

    # Create nix_base64/ — pre-built base64 static library.
    # Avoids running cmake build (~15-25s wall time).
    mkdir -p $out/nix_base64/{include,lib}
    ln -s ${base64Static}/include/libbase64.h $out/nix_base64/include/
    ln -s ${base64Static}/lib/libbase64.a $out/nix_base64/lib/

    cat > $out/bazel/thirdparty/base64-prebuilt.BUILD <<'BASE64_BUILD'
cc_import(
    name = "base64_lib",
    static_library = "lib/libbase64.a",
)
cc_library(
    name = "base64",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],
    deps = [":base64_lib"],
    visibility = ["//visibility:public"],
)
BASE64_BUILD

    # Create nix_openssl_fips/ — pre-built OpenSSL 3.1.2 FIPS module.
    # Avoids running Configure + make build (~45-60s wall time).
    # Provides fips.so and fipsmodule.cnf matching NIST cert #4985.
    mkdir -p $out/nix_openssl_fips/{lib/ossl-modules,etc/ssl}
    ln -s ${opensslFips}/lib/ossl-modules/fips.so $out/nix_openssl_fips/lib/ossl-modules/
    ln -s ${opensslFips}/etc/ssl/fipsmodule.cnf $out/nix_openssl_fips/etc/ssl/

    cat > $out/bazel/thirdparty/openssl-fips-prebuilt.BUILD <<'FIPS_BUILD'
exports_files([
    "lib/ossl-modules/fips.so",
    "etc/ssl/fipsmodule.cnf",
])
filegroup(
    name = "fipsmodule_so",
    srcs = ["lib/ossl-modules/fips.so"],
    visibility = ["//visibility:public"],
)
filegroup(
    name = "fipsmodule_cnf",
    srcs = ["etc/ssl/fipsmodule.cnf"],
    visibility = ["//visibility:public"],
)
FIPS_BUILD

    # Create nix_xxhash/ — pre-built xxhash from nixpkgs.
    # Avoids compiling xxhash.c (~2-5s).
    mkdir -p $out/nix_xxhash/{include,lib}
    ln -s ${xxhashStatic}/include/xxhash.h $out/nix_xxhash/include/
    ln -s ${xxhashStatic}/include/xxh3.h $out/nix_xxhash/include/
    ln -s ${xxhashStatic}/lib/libxxhash.a $out/nix_xxhash/lib/

    cat > $out/bazel/thirdparty/xxhash-prebuilt.BUILD <<'XXHASH_BUILD'
cc_import(
    name = "xxhash_lib",
    static_library = "lib/libxxhash.a",
)
cc_library(
    name = "xxhash",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],
    deps = [":xxhash_lib"],
    visibility = ["//visibility:public"],
)
XXHASH_BUILD

    # Create nix_hdrhistogram/ — pre-built hdrhistogram from nixpkgs.
    # Avoids compiling 8 .c files (~5-10s).
    mkdir -p $out/nix_hdrhistogram/{include/hdr,lib}
    for h in ${hdrhistogram_c}/include/hdr/*.h; do
      ln -s "$h" $out/nix_hdrhistogram/include/hdr/
    done
    ln -s ${hdrhistogram_c}/lib/libhdr_histogram_static.a $out/nix_hdrhistogram/lib/

    cat > $out/bazel/thirdparty/hdrhistogram-prebuilt.BUILD <<'HDR_BUILD'
cc_import(
    name = "hdrhistogram_lib",
    static_library = "lib/libhdr_histogram_static.a",
)
cc_library(
    name = "hdrhistogram",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],
    deps = [":hdrhistogram_lib", "@zlib"],
    visibility = ["//visibility:public"],
)
HDR_BUILD

    # Create nix_ada/ — pre-built ada URL parser from nixpkgs.
    # Avoids compiling ada.cpp (~3-5s).
    mkdir -p $out/nix_ada/{include,lib}
    for item in ${adaStatic}/include/*; do
      ln -s "$item" $out/nix_ada/include/
    done
    ln -s ${adaStatic}/lib/libada.a $out/nix_ada/lib/

    cat > $out/bazel/thirdparty/ada-prebuilt.BUILD <<'ADA_BUILD'
cc_import(
    name = "ada_lib",
    static_library = "lib/libada.a",
)
cc_library(
    name = "ada",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],
    defines = ["ADA_INCLUDE_URL_PATTERN=0"],
    deps = [":ada_lib"],
    visibility = ["//visibility:public"],
)
ADA_BUILD

    # Create nix_roaring/ — pre-built CRoaring from nixpkgs.
    # Avoids compiling C sources (~5-10s).
    mkdir -p $out/nix_roaring/{include,lib}
    for item in ${croaringStatic}/include/*; do
      ln -s "$item" $out/nix_roaring/include/
    done
    ln -s ${croaringStatic}/lib/libroaring.a $out/nix_roaring/lib/

    cat > $out/bazel/thirdparty/roaring-prebuilt.BUILD <<'ROARING_BUILD'
cc_import(
    name = "roaring_lib",
    static_library = "lib/libroaring.a",
)
cc_library(
    name = "roaring",
    hdrs = glob(["include/**/*.h", "include/**/*.hh"]),
    strip_include_prefix = "include",
    deps = [":roaring_lib"],
    visibility = ["//visibility:public"],
)
ROARING_BUILD

    # Create nix_lksctp/ — pre-built lksctp-tools from nixpkgs.
    # Avoids compiling .c files + generating header (~2-3s).
    mkdir -p $out/nix_lksctp/{include/netinet,lib}
    ln -s ${lksctpStatic}/include/netinet/sctp.h $out/nix_lksctp/include/netinet/
    ln -s ${lksctpStatic}/lib/libsctp.a $out/nix_lksctp/lib/

    cat > $out/bazel/thirdparty/lksctp-prebuilt.BUILD <<'LKSCTP_BUILD'
cc_import(
    name = "lksctp_lib",
    static_library = "lib/libsctp.a",
)
cc_library(
    name = "lksctp",
    hdrs = glob(["include/**/*.h"]),
    strip_include_prefix = "include",
    deps = [":lksctp_lib"],
    visibility = ["//visibility:public"],
)
LKSCTP_BUILD

    # Apply MODULE.bazel patches for Nix sandbox:
    # - Remove unneeded dev extensions (toolchains_llvm, rules_oci, buildifier, rules_shell)
    # - Replace go_sdk.download() with go_sdk.host()
    # - Replace pip extension with nixpkgs stub
    # - Add rules_buf and rules_cc overrides (fix shebangs, stub downloads)
    ${pythonWithDeps}/bin/python3 ${./patch-module-bazel.py} $out/MODULE.bazel

    # Remove dead libpciaccess use_repo line from MODULE.bazel
    # (its http_archive is deleted from repositories.bzl below)
    sed -i '/use_repo(non_module_dependencies, "libpciaccess")/d' $out/MODULE.bazel

    # Export rules_buf patch so Bazel can resolve the label
    mkdir -p $out/nix/patches
    cat > $out/nix/patches/BUILD <<'PATCHBUILD'
exports_files(["rules_buf-nix-no-download.patch"])
PATCHBUILD

    # Export prebuilt BUILD files so Bazel can resolve labels
    cat >> $out/bazel/thirdparty/BUILD <<'EXPORTS'
exports_files([
    "c-ares-prebuilt.BUILD",
    "krb5-prebuilt.BUILD",
    "libxml2-prebuilt.BUILD",
    "hwloc-prebuilt.BUILD",
    "openssl-prebuilt.BUILD",
    "ragel-prebuilt.BUILD",
    "base64-prebuilt.BUILD",
    "openssl-fips-prebuilt.BUILD",
    "xxhash-prebuilt.BUILD",
    "hdrhistogram-prebuilt.BUILD",
    "ada-prebuilt.BUILD",
    "roaring-prebuilt.BUILD",
    "lksctp-prebuilt.BUILD",
])
EXPORTS

    # ── Consolidated repositories.bzl patcher ──
    # Replaces http_archive entries with new_local_repository for all
    # pre-built deps, deletes dead entries, in a single Python invocation.
    ${pythonWithDeps}/bin/python3 - $out/bazel/repositories.bzl << 'REPOS_PATCH'
import sys, re

path = sys.argv[1]
with open(path) as f:
    text = f.read()

# Add new_local_repository import if not present
if 'new_local_repository' not in text:
    text = text.replace(
        'load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")',
        'load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")\n'
        'load("@bazel_tools//tools/build_defs/repo:local.bzl", "new_local_repository")',
    )

# Table of deps to replace: (name, local_path, build_file)
replacements = [
    ("c-ares",      "nix_cares",        "c-ares-prebuilt.BUILD"),
    ("krb5",        "nix_krb5",         "krb5-prebuilt.BUILD"),
    ("libxml2",     "nix_libxml2",      "libxml2-prebuilt.BUILD"),
    ("hwloc",       "nix_hwloc",        "hwloc-prebuilt.BUILD"),
    ("ragel",       "nix_ragel",        "ragel-prebuilt.BUILD"),
    ("base64",      "nix_base64",       "base64-prebuilt.BUILD"),
    ("xxhash",      "nix_xxhash",       "xxhash-prebuilt.BUILD"),
    ("hdrhistogram","nix_hdrhistogram",  "hdrhistogram-prebuilt.BUILD"),
    ("ada",         "nix_ada",          "ada-prebuilt.BUILD"),
    ("roaring",     "nix_roaring",      "roaring-prebuilt.BUILD"),
    ("lksctp",      "nix_lksctp",       "lksctp-prebuilt.BUILD"),
]

# These need \n    \) pattern because patch_cmds contain ) chars
needs_multiline_close = {"openssl", "openssl-fips"}

# Special: openssl and openssl-fips (have patch_cmds with parens)
replacements_special = [
    ("openssl",     "nix_openssl",      "openssl-prebuilt.BUILD"),
    ("openssl-fips","nix_openssl_fips",  "openssl-fips-prebuilt.BUILD"),
]

# Dead entries to delete
deletions = ["libpciaccess"]

for name, local_path, build_file in replacements:
    pattern = r'    http_archive\(\s*name = "' + re.escape(name) + r'".*?\)'
    match = re.search(pattern, text, re.DOTALL)
    if match:
        new = f"""    new_local_repository(
        name = "{name}",
        path = "{local_path}",
        build_file = "//bazel/thirdparty:{build_file}",
    )"""
        text = text.replace(match.group(0), new)

for name, local_path, build_file in replacements_special:
    pattern = r'    http_archive\(\s*name = "' + re.escape(name) + r'".*?\n    \)'
    match = re.search(pattern, text, re.DOTALL)
    if match:
        new = f"""    new_local_repository(
        name = "{name}",
        path = "{local_path}",
        build_file = "//bazel/thirdparty:{build_file}",
    )"""
        text = text.replace(match.group(0), new)

for name in deletions:
    pattern = r'    http_archive\(\s*name = "' + re.escape(name) + r'".*?\)'
    match = re.search(pattern, text, re.DOTALL)
    if match:
        text = text.replace(match.group(0), "")

with open(path, 'w') as f:
    f.write(text)
REPOS_PATCH

    # Replace default BCR registry with local copy (the --registry flag is
    # a list flag — CLI values append rather than replace, so we must patch
    # the .bazelrc source to remove the remote URL)
    sed -i 's|common --registry=https://bcr.bazel.build|common --registry=file://${registry}|' $out/.bazelrc

    # Remove .bazelrc lines that reference removed modules
    # (toolchains_llvm, current/next_llvm_toolchain, rules_go, go_sdk)
    sed -i '/^common --@toolchains_llvm/d' $out/.bazelrc
    sed -i '/^common --extra_toolchains=@current_llvm_toolchain/d' $out/.bazelrc
    sed -i '/^common:clang-21 --extra_toolchains=@next_llvm_toolchain/d' $out/.bazelrc
    sed -i '/^build --@rules_go/d' $out/.bazelrc
    sed -i '/^build:gofips/d' $out/.bazelrc
    sed -i '/^build:lto --@rules_rust/d' $out/.bazelrc
    sed -i '/^test:lldb --run_under=.*llvm_toolchain/d' $out/.bazelrc

    # Use pre-generated lockfile that matches the patched MODULE.bazel.
    # Generated by running: bazelisk mod deps --lockfile_mode=update
    # with the same MODULE.bazel patches applied locally.
    # This prevents lockfile staleness from triggering module re-resolution
    # (which would need network access unavailable in the sandbox).
    cp ${./MODULE.bazel.lock.nix} $out/MODULE.bazel.lock

    # Enable use_default_shell_env so --action_env reaches these actions.
    # Without it, Bazel runs actions with `env -` (empty environment),
    # so BAZEL_SH and PATH from --action_env are never delivered.
    # The py_binary wrapper (rules_python bootstrap) needs bash, which
    # it discovers via BAZEL_SH — absent in the Nix sandbox's empty env.
    # Verified: build fails with "bash not found. Set BAZEL_SH" without this.
    cd $out
    sed -i 's/use_default_shell_env = False/use_default_shell_env = True/' \
      src/v/version/expand_with_stamp_vars.bzl
    sed -i '/mnemonic = "RedpandaProtoGen",/a\        use_default_shell_env = True,' \
      bazel/pbgen/pbgen.bzl
  '';

  registry = callPackage ./bcr.nix { };

  bazel = bazel_8;  # actual binary, used via USE_BAZEL_VERSION
  # Platform-specific wrapper (avoids the generic `bazel` wrapper which
  # re-reads USE_BAZEL_VERSION and double-resolves).
  bazelPlatformBin = let
    os = if stdenv.isLinux then "linux" else "darwin";
    arch = stdenv.hostPlatform.uname.processor;
  in "${bazel}/bin/bazel-8.5.0-${os}-${arch}";
  targets = [ "//src/v/redpanda:redpanda" ];

  # Optimization level flags (orthogonal to PGO).
  # See .bazelrc: --config=release (opt + secure), --config=lto (ThinLTO).
  optimizationArgs = lib.optionals (optimizationLevel == "release") [
    "--config=release"
  ] ++ lib.optionals (optimizationLevel == "lto") [
    "--config=lto"
  ];

  # PGO (Profile-Guided Optimization) flags.
  # Phase 1 (instrument): build with profiling instrumentation enabled.
  # Phase 3 (optimize): rebuild using collected profile data.
  # PGO configs already include --config=lto internally.
  # See .bazelrc for the underlying config definitions.
  pgoArgs = lib.optionals (pgoMode == "instrument") [
    "--config=pgo-instrument"
  ] ++ lib.optionals (pgoMode == "optimize") [
    "--config=pgo-optimize"
    "--fdo_optimize=${pgoProfilePath}"
  ];

  # ── Nixify pipeline configuration ──
  # Imported from nixify-rules.nix — centralizes all fixup config.
  nixifyRules = import ./nixify-rules.nix {
    inherit lib bash perl glibc gcc-unwrapped zlib openssl curl;
    python3 = pythonWithDeps;
  };

  # Extract patchelf and shebang config from nixify rules
  patchelfRule = lib.findFirst (r: r.type == "patchelf") null nixifyRules;
  nixInterp = patchelfRule.interpreter;
  nixRpath = patchelfRule.rpath;

  shebangsRule = lib.findFirst (r: r.type == "fix-shebangs") null nixifyRules;
  interpreterMap = shebangsRule.interpreters;

  # Build a sed expression that handles ALL known interpreters in one pass.
  # For each interpreter X with Nix path P, generates a sed branch:
  #   /^#!.*X/{ s|^#!.*|#!P|; }
  # The match is deliberately simple: if line 1 starts with #! and contains
  # the interpreter name ANYWHERE, replace the entire line. No fussy path
  # or spacing patterns — just look for the word. This handles every shebang
  # variant we're ever likely to see:
  #   #!/usr/bin/env perl, #! /bin/perl, #!/usr/local/bin/perl, etc.
  #
  # python3 is checked before python to avoid premature match.
  interpreterSedScript = let
    # Order matters: longer names first so "python3" matches before "python"
    orderedNames = lib.sort (a: b: builtins.stringLength a > builtins.stringLength b)
      (lib.attrNames interpreterMap);
  in lib.concatStringsSep "\n" (
    map (name:
      "/^#!.*${name}/{s|^#!.*|#!${interpreterMap.${name}}|;}"
    ) orderedNames
  );

  # ── Pre-built cargo-bazel for crate_universe extension ──
  # module_ctx.download() does NOT use --repository_cache, so we must
  # provide the binary locally via CARGO_BAZEL_GENERATOR_URL.
  cargoBazel = runCommand "cargo-bazel-patched" {
    nativeBuildInputs = [ patchelf ];
  } ''
    mkdir -p $out/bin
    cp ${fetchurl {
      url = "https://github.com/bazelbuild/rules_rust/releases/download/0.60.0/cargo-bazel-x86_64-unknown-linux-gnu";
      sha256 = "e4f70e4fccedb95cab5efd95ac54953d0e693c05c0552376d542c44df6df6977";
    }} $out/bin/cargo-bazel
    chmod u+wx $out/bin/cargo-bazel
    patchelf --set-interpreter ${nixInterp} --set-rpath ${nixRpath} $out/bin/cargo-bazel
  '';

  # ── Go module proxy cache ──
  # gazelle's go_repository uses fetch_repo which downloads from GOPROXY
  # (proxy.golang.org). This doesn't go through Bazel's --repository_cache.
  # We pre-download the needed Go modules and serve them via GOPROXY=file://.
  # pbgen (Go protobuf code gen) only needs google.golang.org/protobuf and
  # github.com/golang/protobuf.
  goProxyCache = runCommand "go-proxy-cache" { } ''
    mkdir -p $out/google.golang.org/protobuf/@v
    ln -s ${fetchurl { url = "https://proxy.golang.org/google.golang.org/protobuf/@v/v1.36.11.info"; sha256 = "2156715d128777c2a6fae6107d12a0ab60c2b1deed4fba5a2b2481c68911082a"; }} $out/google.golang.org/protobuf/@v/v1.36.11.info
    ln -s ${fetchurl { url = "https://proxy.golang.org/google.golang.org/protobuf/@v/v1.36.11.mod"; sha256 = "a75c105a852fbd8da8d8cfac09c2eab9a206cfd27ed37c973737e23f632ca96e"; }} $out/google.golang.org/protobuf/@v/v1.36.11.mod
    ln -s ${fetchurl { url = "https://proxy.golang.org/google.golang.org/protobuf/@v/v1.36.11.zip"; sha256 = "14983d36c56a814ed91b6d652f2b8f895baba1b84eb43b28a0b132c8637cd274"; }} $out/google.golang.org/protobuf/@v/v1.36.11.zip
    echo '{"Version":"v1.36.11"}' > $out/google.golang.org/protobuf/@v/list

    mkdir -p $out/github.com/golang/protobuf/@v
    ln -s ${fetchurl { url = "https://proxy.golang.org/github.com/golang/protobuf/@v/v1.5.4.info"; sha256 = "840270c813a1c9b8cfe1b66d534336c71dad9da2e1c57c9df3743aaa5eaca219"; }} $out/github.com/golang/protobuf/@v/v1.5.4.info
    ln -s ${fetchurl { url = "https://proxy.golang.org/github.com/golang/protobuf/@v/v1.5.4.mod"; sha256 = "c5f873c621cfaaf563f8b66a0501a5be14390cb0859e5187ce616d0312a6c8f8"; }} $out/github.com/golang/protobuf/@v/v1.5.4.mod
    ln -s ${fetchurl { url = "https://proxy.golang.org/github.com/golang/protobuf/@v/v1.5.4.zip"; sha256 = "9a2f43d3eac8ceda506ebbeb4f229254b87235ce90346692a0e233614182190b"; }} $out/github.com/golang/protobuf/@v/v1.5.4.zip
    echo '{"Version":"v1.5.4"}' > $out/github.com/golang/protobuf/@v/list
  '';

  # ── Per-archive repo cache (linkFarm) ──
  # Each archive is fetched independently via fetchurl, then assembled
  # into a content_addressable/sha256/<hex>/file layout that Bazel
  # recognizes as a repository cache. Adding/removing an archive only
  # rebuilds that single fetchurl — no monolithic FOD hash to manage.
  repoCache = callPackage ./bazel-repo-cache.nix { } {
    archives = import ./bazel-deps.nix;
  };

  nativeBuildInputsDeps = [
    bazelisk
    llvmPackages_20.libcxxClang
    llvmPackages_20.lld
    llvmPackages_20.llvm
    llvmPackages_20.libcxx
    pythonWithDeps
    go
    jdk_headless
    autoconf
    automake
    libtool
    bison
    pkg-config
    elfutils
    xfsprogs
    valgrind
    patchelf
    file
    findutils
    git
    cacert
    zstd
    coreutils
    bash
    gnused
    gnumake
    gnugrep
    gawk
    perl
    m4
  ];

  nixPath = lib.makeBinPath (nativeBuildInputsDeps ++ [ bazel stdenv.cc stdenv.cc.bintools ]);

  # Table-driven script for patching Bazel-downloaded binaries.
  # Fixes two problems in the Nix sandbox:
  #   1. ELF binaries have /lib64/ld-linux-x86-64.so.2 interpreter (doesn't exist)
  #   2. Scripts have shebangs pointing to paths that don't exist in sandbox
  #
  # Shebang handling uses the interpreter map from nixify-rules.nix.
  # A single sed script handles ALL known interpreters in one pass,
  # matching any shebang variant: #!/usr/bin/env X, #! /bin/X,
  # #!/usr/local/bin/X, etc. — resilient to spaces and path differences.
  bazelPatcher = writeShellApplication {
    name = "bazel-sandbox-patcher";
    runtimeInputs = [ patchelf file findutils coreutils gnused ];
    text = ''
      NIX_INTERP="${nixInterp}"
      NIX_RPATH="${nixRpath}"

      # Generated from nixify-rules.nix interpreter map.
      # Each line matches shebangs containing the interpreter name
      # (after a / or space) and replaces the entire shebang line.
      SHEBANG_SED_SCRIPT=$(cat <<'SEDEOF'
      ${interpreterSedScript}
      SEDEOF
      )

      patch_one_elf() {
        local f="$1"
        local desc
        desc=$(file -b "$f" 2>/dev/null) || return 0
        case "$desc" in
          ELF*dynamically\ linked*) ;;
          *) return 0 ;;
        esac
        local interp
        interp=$(patchelf --print-interpreter "$f" 2>/dev/null) || return 0
        case "$interp" in
          /nix/store/*) return 0 ;;
        esac
        echo "  patchelf: $f (was: $interp)"
        chmod u+w "$f" 2>/dev/null || true
        patchelf --set-interpreter "$NIX_INTERP" "$f" 2>/dev/null || true
        local old_rpath
        old_rpath=$(patchelf --print-rpath "$f" 2>/dev/null) || old_rpath=""
        patchelf --set-rpath "$NIX_RPATH:$old_rpath" "$f" 2>/dev/null || true
      }

      patch_elfs() {
        local dir="$1"
        echo "Scanning $dir for ELF binaries to patch..."
        while IFS= read -r -d "" f; do
          patch_one_elf "$f"
        done < <(find -L "$dir" -type f \
          \( -executable -o -name "*.so" -o -name "*.so.*" \) \
          -print0 2>/dev/null)
      }

      fix_shebangs() {
        local dir="$1"
        echo "Fixing shebangs in $dir..."
        while IFS= read -r -d "" f; do
          # Skip binary files
          [[ "$(file -bL --mime-type "$f" 2>/dev/null)" == text/* ]] || continue
          local firstline
          firstline=$(head -n1 "$f" 2>/dev/null) || continue
          # Only process files that have a shebang
          case "$firstline" in
            '#!'*) ;;
            *) continue ;;
          esac
          # Skip files already pointing to /nix/store
          case "$firstline" in
            '#!/nix/store/'*) continue ;;
          esac
          # Apply the table-driven sed script (matches any known interpreter)
          chmod u+w "$(dirname "$f")" 2>/dev/null || true
          chmod u+w "$f" 2>/dev/null || true
          local old="$firstline"
          sed -i "1{$SHEBANG_SED_SCRIPT}" "$f" 2>/dev/null || true
          local new
          new=$(head -n1 "$f" 2>/dev/null) || continue
          if [[ "$old" != "$new" ]]; then
            echo "  fixed shebang: $f ($old -> $new)"
          fi
        done < <(find -L "$dir" \
          -path "*/bazel_tools/*" -prune -o \
          -path "*go_sdk+main___host*" -prune -o \
          -type f \
          \( -name "*.sh" -o -name "*.pl" -o -name "*.py" \
             -o -name "Configure" -o -name "configure" -o -executable \) \
          -print0 2>/dev/null)
      }

      command="''${1:-}"
      shift || true

      case "$command" in
        patch-all)
          for dir in "$@"; do
            if [ -d "$dir" ]; then
              patch_elfs "$dir"
              fix_shebangs "$dir"
            fi
          done
          ;;
        patch-elfs)
          for dir in "$@"; do
            [ -d "$dir" ] && patch_elfs "$dir"
          done
          ;;
        fix-shebangs)
          for dir in "$@"; do
            [ -d "$dir" ] && fix_shebangs "$dir"
          done
          ;;
        *)
          echo "Usage: bazel-sandbox-patcher {patch-all|patch-elfs|fix-shebangs} DIR..."
          exit 1
          ;;
      esac
    '';
  };

  # Generate user.bazelrc content (same settings as shell.nix shellHook)
  bazelrcNix = ''
    build --config=system-clang
    build --shell_executable=${bash}/bin/bash
    build --action_env=BAZEL_SH=${bash}/bin/bash
    build --host_action_env=BAZEL_SH=${bash}/bin/bash
    build --host_linkopt=-stdlib=libc++
    build --host_linkopt=--unwindlib=libgcc
    build --action_env=PATH=${nixPath}
    build --host_action_env=PATH=${nixPath}
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
    build --action_env=ACLOCAL_PATH=${lib.concatStringsSep ":" [
      "${automake}/share/aclocal"
      "${libtool}/share/aclocal"
      "${pkg-config}/share/aclocal"
    ]}
    build --host_action_env=ACLOCAL_PATH=${lib.concatStringsSep ":" [
      "${automake}/share/aclocal"
      "${libtool}/share/aclocal"
      "${pkg-config}/share/aclocal"
    ]}
    build --action_env=LIBRARY_PATH=${llvmPackages_20.libcxx}/lib:${gccLib}/lib
    build --host_action_env=LIBRARY_PATH=${llvmPackages_20.libcxx}/lib:${gccLib}/lib
    build --action_env=LD_LIBRARY_PATH=${llvmPackages_20.libcxx}/lib:${gccLib}/lib:${zlib}/lib
    build --host_action_env=LD_LIBRARY_PATH=${llvmPackages_20.libcxx}/lib:${gccLib}/lib:${zlib}/lib
    build --linkopt=-Wl,-rpath,${llvmPackages_20.libcxx}/lib
    build --linkopt=-Wl,-rpath,${gccLib}/lib
    build --host_linkopt=-Wl,-rpath,${llvmPackages_20.libcxx}/lib
    build --host_linkopt=-Wl,-rpath,${gccLib}/lib
    build --@protobuf//bazel/toolchains:allow_nonstandard_protoc
  '';

  commonArgs = [
    "--repository_cache=repo_cache"
    "--shell_executable=${bash}/bin/bash"
    "--action_env=PATH=${nixPath}"
    "--repo_env=PATH=${nixPath}"
    "--repo_env=BAZEL_SH=${bash}/bin/bash"
    "--action_env=BAZEL_SH=${bash}/bin/bash"
    "--repo_env=SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt"
    "--action_env=SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt"
    "--repo_env=GOPROXY=file://${goProxyCache},off"
    "--repo_env=GONOSUMCHECK=*"
    "--repo_env=GONOSUMDB=*"
    "--repo_env=GOFLAGS=-modcacherw"
    "--spawn_strategy=local"
  ];

  # Startup flags (before the subcommand) — forces Bazel to use the
  # persistent cache directory instead of computing one from MD5(workspace_path).
  bazelStartupArgs = lib.optionals (bazelCacheDir != "") [
    "--output_base=${bazelCacheDir}/output_base"
  ];

  # Patch all Bazel external dirs in the output base
  patchBazelDirs = if bazelCacheDir != "" then ''
    ${bazelPatcher}/bin/bazel-sandbox-patcher patch-all \
      "${bazelCacheDir}/output_base/external" \
      "${bazelCacheDir}/output_base/modextwd"
  '' else ''
    ${bazelPatcher}/bin/bazel-sandbox-patcher patch-all \
      "$HOME"/.cache/bazel/_bazel_*/*/external \
      "$HOME"/.cache/bazel/_bazel_*/*/modextwd
  '';

in
stdenv.mkDerivation {
  name = "redpanda-${version}";
  inherit src version;
  sourceRoot = "redpanda-src-patched";

  nativeBuildInputs = nativeBuildInputsDeps ++ [ bazel bazelPatcher lndir ];

  requiredSystemFeatures = [ "big-parallel" ];

  passthru = {
    inherit repoCache;
  };

  buildPhase = ''
    runHook preBuild

    export HOME=/tmp/bazel-home
    mkdir -p $HOME

    # Allow shared output_base across builds by different nixbld users.
    # Nix assigns different builder users (nixbld1, nixbld2, ...) for
    # different derivation hashes. umask 002 makes all files group-writable
    # so the nixbld group can share the persistent cache.
    # Restored to 022 before installPhase (Nix rejects group-writable outputs).
    umask 002
    ${lib.optionalString (bazelCacheDir != "") ''
      # Pre-create the output_base lock file with group-writable permissions.
      # Bazel acquires this lock during startup (before our umask takes effect
      # on Bazel-created files). Without this, the first builder creates it
      # with 644, and subsequent builders (different nixbld users) get EACCES.
      mkdir -p "${bazelCacheDir}/output_base"
      touch "${bazelCacheDir}/output_base/lock"
      chmod g+w "${bazelCacheDir}/output_base/lock"
    ''}

    # ── Sanitize Nix stdenv env vars for Bazel cache stability ──
    # Nix injects derivation-hash-dependent values into these env vars:
    #   NIX_CFLAGS_COMPILE: -frandom-seed=<output-hash-prefix>
    #   NIX_LDFLAGS: -rpath <output-store-path>/lib
    # Since these are passed to Bazel via --action_env, they become part of
    # every action's cache key. When the derivation hash changes (e.g. touching
    # nix/entropy), ALL compilation actions get new cache keys → 100% miss.
    # Stripping these is safe:
    #   - Bazel sets its own per-object -frandom-seed in command args
    #   - The $out rpath is meaningless inside Bazel (patchelf fixes it later)
    export NIX_CFLAGS_COMPILE="$(echo "$NIX_CFLAGS_COMPILE" | sed 's/-frandom-seed=[^[:space:]]*//')"
    export NIX_LDFLAGS="$(echo "$NIX_LDFLAGS" | sed 's|-rpath /nix/store/[^[:space:]]*/lib[[:space:]]*||')"

    # Write user.bazelrc (try-import %workspace%/user.bazelrc is in .bazelrc)
    cat > user.bazelrc <<'BAZELRC'
    ${bazelrcNix}
    BAZELRC

    export CC=clang
    export CXX=clang++

    # Point bazelisk at the nixpkgs bazel_8 platform wrapper (no runtime
    # download). Must NOT point to the generic `bazel` wrapper — it re-reads
    # USE_BAZEL_VERSION and double-resolves.
    export USE_BAZEL_VERSION=${bazelPlatformBin}

    # Bazel needs to find bash for patch_cmds, repo rule shell commands, etc.
    # --repo_env only makes it available to repository_ctx.os.environ, but
    # patch_cmds (module resolution phase) reads the process environment.
    export BAZEL_SH=${bash}/bin/bash

    # Provide cargo-bazel binary for crate_universe extension evaluation.
    # module_ctx.download() doesn't use --repository_cache, so we provide
    # a pre-built, patchelf'd binary via env var (file:// URL).
    # The path must match what's recorded in the lockfile to prevent
    # extension re-evaluation (which would need cargo + crate index).
    cp ${cargoBazel}/bin/cargo-bazel /tmp/cargo-bazel
    chmod +x /tmp/cargo-bazel
    export CARGO_BAZEL_GENERATOR_URL=file:///tmp/cargo-bazel

    # Disable canonical ID so Bazel uses pure content-addressing
    export BAZEL_HTTP_RULES_URLS_AS_DEFAULT_CANONICAL_ID=0

    # Tell cargo not to access the network (crate sources are in repo cache,
    # cargo-bazel splice only needs the lockfile metadata).
    export CARGO_NET_OFFLINE=true

    # Go module proxy cache for gazelle's go_repository rules.
    # fetch_repo uses GOPROXY, not Bazel's --repository_cache.
    export GOPROXY=file://${goProxyCache},off
    export GONOSUMCHECK='*'
    export GONOSUMDB='*'
    export GOFLAGS=-modcacherw

    # Create a writable copy of the repo cache. The linkFarm is in the
    # read-only Nix store, but Bazel needs to write to the cache dir
    # (e.g. caching registry file lookups). lndir creates a writable
    # directory tree with symlinks to the actual archive files.
    mkdir -p repo_cache
    ${lndir}/bin/lndir -silent ${repoCache} repo_cache

    # ── Phase A: Initial fetch ──
    # Extracts archives from the pre-populated repo cache.
    # Will fail because downloaded ELF binaries (Rust, Go, etc.)
    # can't execute in the Nix sandbox without patching.
    echo "=== Fetch attempt 1 ==="
    bazelisk \
      ${lib.escapeShellArgs bazelStartupArgs} \
      fetch \
      --keep_going \
      ${lib.escapeShellArgs commonArgs} \
      ${lib.escapeShellArgs targets} || true

    # ── Phase B+C: Nixify + re-fetch loop ──
    # Patch extracted binaries, then re-fetch. Multiple rounds because
    # Bazel may re-extract archives (losing patches) when repo rules
    # re-run with now-working binaries.
    for attempt in 2 3 4; do
      echo "=== Nixify pass (before attempt $attempt) ==="
      ${patchBazelDirs}

      echo "=== Fetch attempt $attempt ==="
      if bazelisk \
        ${lib.escapeShellArgs bazelStartupArgs} \
        fetch \
        --keep_going \
        ${lib.escapeShellArgs commonArgs} \
        ${lib.escapeShellArgs targets}; then
        echo "=== Fetch succeeded on attempt $attempt ==="
        break
      fi
    done

    # Final nixify pass before build
    echo "=== Final nixify pass ==="
    ${patchBazelDirs}

    # ── Phase D: Build ──
    bazelisk \
      ${lib.escapeShellArgs bazelStartupArgs} \
      build \
      --verbose_failures \
      ${lib.escapeShellArgs commonArgs} \
      ${lib.escapeShellArgs optimizationArgs} \
      ${lib.escapeShellArgs pgoArgs} \
      ${lib.escapeShellArgs targets}

    # Shut down the persistent server
    bazelisk ${lib.escapeShellArgs bazelStartupArgs} shutdown || true

    # Restore strict umask for installPhase outputs (Nix rejects group-writable).
    umask 022

    runHook postBuild
  '';

  installPhase = ''
    mkdir -p $out/bin $out/etc/redpanda
    install -m755 bazel-bin/src/v/redpanda/redpanda $out/bin/redpanda
    install -m644 conf/redpanda.yaml $out/etc/redpanda/redpanda.yaml

    # Add runtime library paths for pre-built shared dependencies.
    # Bazel's cc_import doesn't embed rpath for shared libs, so the
    # binary can't find them at runtime without this.
    patchelf --add-rpath ${krb5.lib}/lib:${openssl.out}/lib $out/bin/redpanda
  '';

  meta = {
    description = "Redpanda: a Kafka-compatible streaming data platform";
    homepage = "https://redpanda.com/";
    license = lib.licenses.bsl11;
    platforms = [ "x86_64-linux" "aarch64-linux" ];
    mainProgram = "redpanda";
  };
}
