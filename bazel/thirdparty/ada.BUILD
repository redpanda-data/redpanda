load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

cmake(
    name = "ada",
    cache_entries = {
        "BUILD_SHARED_LIBS": "OFF",
        "ADA_TESTING": "OFF",
        "ADA_TOOLS": "OFF",
        "ADA_BENCHMARKS": "OFF",
        "CMAKE_INSTALL_LIBDIR": "lib",
    },
    env = {
        # prevent cmake from trying to open the ccache read/write directory
        # which is outside the bazel sandbox.
        "CCACHE_DISABLE": "1",
    },
    generate_args = ["-GNinja"],
    lib_source = ":srcs",
    out_static_libs = ["libada.a"],
    visibility = [
        "//visibility:public",
    ],
)
