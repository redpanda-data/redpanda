load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "coordinator",
    srcs = [
        "coordinator.cc",
    ],
    hdrs = [
        "coordinator.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:logger",
        "//src/v/metrics",
        "//src/v/utils:uuid",
        "@seastar",
    ],
)
