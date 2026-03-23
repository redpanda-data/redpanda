load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "libxml2",
    autogen = True,
    autoreconf = True,
    autoreconf_options = ["-ivf"],
    configure_in_place = True,
    configure_options = select({
        # When cross-compiling from macOS to Linux, configure tries to run
        # compiled test programs on the exec machine. Pass --host to tell it
        # the target architecture so it skips that check.
        "@platforms//cpu:x86_64": ["--host=x86_64-linux-gnu"],
        "@platforms//cpu:aarch64": ["--host=aarch64-linux-gnu"],
        "//conditions:default": [],
    }) + [
        "--without-python",
        "--disable-shared",
        "--enable-static",
        "--with-zlib=$$EXT_BUILD_DEPS/zlib",
        "--without-lzma",
    ],
    env = {
        # On Apple Silicon, Homebrew installs to /opt/homebrew/bin, which is not
        # in the default Bazel sandbox PATH (/bin:/usr/bin:/usr/local/bin).
        # autoreconf requires autoconf/automake from Homebrew, so add it here.
        "PATH": "/opt/homebrew/bin:/bin:/usr/bin:/usr/local/bin",
    },
    lib_source = ":srcs",
    out_include_dir = "include/libxml2",
    out_static_libs = ["libxml2.a"],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@zlib",
    ],
)
