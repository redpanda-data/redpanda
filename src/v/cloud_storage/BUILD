load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "remote_label",
    hdrs = [
        "remote_label.h",
    ],
    include_prefix = "cloud_storage",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:uuid",
    ],
)
