load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "partition_change_notifier_api",
    hdrs = [
        "partition_change_notifier.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster:notification",
        "//src/v/cluster:topic_configuration",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_change_notifier_impl",
    srcs = [
        "partition_change_notifier_impl.cc",
    ],
    hdrs = [
        "partition_change_notifier_impl.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":partition_change_notifier_api",
        "//src/v/cluster",
        "//src/v/container:chunked_hash_map",
        "//src/v/raft",
        "@seastar",
    ],
)
