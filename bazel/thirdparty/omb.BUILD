load("@bazel_skylib//rules:native_binary.bzl", "native_binary")

filegroup(
    name = "omb_bin",
    srcs = ["omb/bin/benchmark"],
    visibility = ["//visibility:public"],
)

native_binary(
    name = "omb",
    src = ":omb_bin",
    visibility = ["//visibility:public"],
)
