load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logger",
    srcs = ["logger.cc"],
    hdrs = ["logger.h"],
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_proxy",
    srcs = [
        "partition_proxy.cc",
        "replicated_partition.cc",
    ],
    hdrs = [
        "partition_proxy.h",
        "replicated_partition.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/cluster",
        "@boost//:numeric_conversion",
    ],
    include_prefix = "kafka/data",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_storage",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server:errors",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/storage",
        "//src/v/utils:truncating_logger",
        "@boost//:outcome",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "record_batcher",
    srcs = [
        "record_batcher.cc",
    ],
    hdrs = [
        "record_batcher.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/storage:record_batch_builder",
        "//src/v/utils:human",
        "//src/v/utils:truncating_logger",
        "@abseil-cpp//absl/algorithm:container",
    ],
    include_prefix = "kafka/data",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "@seastar",
    ],
)
