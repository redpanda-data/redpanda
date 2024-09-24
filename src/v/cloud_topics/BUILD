load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "cloud_topics",
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "types",
    hdrs = [
        "dl_placeholder.h",
        "types.h",
        "errc.h",
    ],
    include_prefix = "cloud_topics",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:uuid",
    ],
)
