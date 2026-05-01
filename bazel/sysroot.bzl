"""Repository rule that downloads a Linux sysroot tarball and exposes it as:

  - `:sysroot` — single source-directory entry consumed by the cc_toolchain.
    Mirrors @toolchains_llvm//toolchain:sysroot.bzl so bazel 9's merkle_cache
    treats the sysroot as one input instead of ~1700 files.
  - `:runtime` — versioned shared libraries plus the glibc dynamic loader.
    Packaging consumes this to ship the relevant `.so`s alongside the binary
    and to set the binary's INTERP.

BUILD lives in a `sysroot/` subdirectory because http_archive can't put a
build_file in a sub-path, and a top-level `srcs = ["."]` source-directory
trips Bazel's package-boundary check.
"""

_BUILD_FILE = """
filegroup(
    name = "sysroot",
    srcs = ["."],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "runtime",
    srcs = glob([
        "lib*/ld-linux-*.so.*",
        "lib/*/lib*.so.*",
        "usr/lib/*/lib*.so.*",
    ], allow_empty = False),
    visibility = ["//visibility:public"],
)
"""

def _sysroot_repository_impl(rctx):
    rctx.file("sysroot/BUILD.bazel", _BUILD_FILE)
    rctx.download_and_extract(
        url = rctx.attr.urls,
        sha256 = rctx.attr.sha256,
        output = "sysroot",
    )

sysroot_repository = repository_rule(
    implementation = _sysroot_repository_impl,
    attrs = {
        "urls": attr.string_list(mandatory = True),
        "sha256": attr.string(mandatory = True),
    },
)
