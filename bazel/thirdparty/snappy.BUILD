load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

cmake(
    name = "snappy",
    cache_entries = {
        "BUILD_SHARED_LIBS": "OFF",
        "CMAKE_INSTALL_LIBDIR": "lib",
        "SNAPPY_INSTALL": "ON",
        "SNAPPY_BUILD_TESTS": "OFF",
        "SNAPPY_BUILD_BENCHMARKS": "OFF",
        "CMAKE_SHARED_LINKER_FLAGS": "-Wno-fuse-ld-path",
    },
    generate_args = ["-GNinja"],
    lib_source = ":srcs",
    out_static_libs = ["libsnappy.a"],
    visibility = [
        "//visibility:public",
    ],
)
