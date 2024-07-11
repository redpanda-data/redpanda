load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "config",
    hdrs = [
        "config.h",
    ],
    include_prefix = "storage",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)
