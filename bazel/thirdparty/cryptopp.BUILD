load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

cmake(
    name = "cryptopp",
    cache_entries = {
        "BUILD_SHARED": "OFF",
        "BUILD_TESTING": "OFF",
        "CMAKE_INSTALL_LIBDIR": "lib",
    },
    generate_args = ["-GNinja"],
    lib_source = ":srcs",
    out_static_libs = ["libcryptopp.a"],
    visibility = [
        "//visibility:public",
    ],
)
