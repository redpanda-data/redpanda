load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

cmake(
    name = "unordered_dense",
    lib_source = ":srcs",
    out_headers_only = True,
    visibility = [
        "//visibility:public",
    ],
)
