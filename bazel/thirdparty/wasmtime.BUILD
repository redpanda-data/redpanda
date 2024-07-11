load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

options = [
    "--no-default-features",
    "--features=async",
    "--features=addr2line",
    "--features=wat",
]

cmake(
    name = "wasmtime",
    cache_entries = {
        "CMAKE_INSTALL_LIBDIR": "lib",
        "WASMTIME_USER_CARGO_BUILD_OPTIONS": ";".join(options),
        "WASMTIME_ALWAYS_BUILD": "OFF",
    },
    lib_source = ":srcs",
    out_static_libs = ["libwasmtime.a"],
    visibility = [
        "//visibility:public",
    ],
    working_directory = "crates/c-api",
)
