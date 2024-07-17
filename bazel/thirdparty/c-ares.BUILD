load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

cmake(
    name = "c-ares",
    cache_entries = {
        "BUILD_SHARED_LIBS": "OFF",
        "CARES_SHARED": "OFF",
        "CARES_STATIC": "ON",
        "CMAKE_INSTALL_LIBDIR": "lib",
        "CARES_BUILD_TOOLS": "OFF",
        "CARES_INSTALL": "ON",
    },
    generate_args = ["-GNinja"],
    lib_source = ":srcs",
    out_static_libs = ["libcares.a"],
    visibility = [
        "//visibility:public",
    ],
)
