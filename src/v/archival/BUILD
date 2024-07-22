load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "run_quota",
    hdrs = [
        "run_quota.h",
    ],
    include_prefix = "archival",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/utils:named_type",
    ],
)
