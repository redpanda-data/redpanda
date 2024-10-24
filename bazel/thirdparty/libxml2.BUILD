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
    configure_options = [
        "--without-python",
        "--disable-shared",
        "--enable-static",
        "--with-zlib=$$EXT_BUILD_DEPS/zlib",
        "--without-lzma",
    ],
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
