load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "v8_engine",
    hdrs = [
        "data_policy.h",
    ],
    include_prefix = "v8_engine",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/reflection:adl",
        "//src/v/serde",
    ],
)
