load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "stm",
    srcs = [
        "transform_offsets_stm.cc",
    ],
    hdrs = [
        "transform_offsets_stm.h",
    ],
    include_prefix = "transform/stm",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster",
        "//src/v/model",
        "@seastar",
    ],
)
