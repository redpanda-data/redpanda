load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "errors",
    hdrs = [
        "errors.h",
    ],
    include_prefix = "errors",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)
