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
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "errc.h",
        "types.h",
    ],
    include_prefix = "cloud_topics",
    deps = [
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "placeholder",
    hdrs = [
        "dl_placeholder.h",
    ],
    include_prefix = "cloud_topics",
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:uuid",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@fmt",
    ],
)
