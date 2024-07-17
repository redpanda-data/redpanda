load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

cmake(
    name = "hdrhistogram",
    cache_entries = {
        "BUILD_SHARED_LIBS": "OFF",
        "CMAKE_INSTALL_LIBDIR": "lib",
        "HDR_HISTOGRAM_BUILD_PROGRAMS": "OFF",
        "HDR_HISTOGRAM_BUILD_SHARED": "OFF",
        "HDR_HISTOGRAM_BUILD_STATIC": "ON",
    },
    generate_args = ["-GNinja"],
    lib_source = ":srcs",
    out_static_libs = ["libhdr_histogram_static.a"],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@zlib",
    ],
)
