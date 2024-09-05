load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "debug_bundle",
    srcs = [
        "error.cc",
    ],
    hdrs = [
        "error.h",
    ],
    include_prefix = "debug_bundle",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)
