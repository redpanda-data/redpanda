load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "nettle",
    autogen = True,
    autogen_command = ".bootstrap",
    configure_in_place = True,
    configure_options = [
        "--disable-documentation",
        "--disable-shared",
        "--enable-static",
        "--enable-x86-aesni",
        "--libdir=$$INSTALLDIR/lib",
    ],
    lib_source = ":srcs",
    out_static_libs = [
        "libnettle.a",
        "libhogweed.a",
    ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@gmp",
    ],
)
