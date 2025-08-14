load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logger",
    srcs = ["logger.cc"],
    hdrs = ["logger.h"],
    visibility = ["//visibility:__subpackages__"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_proxy",
    srcs = [
        "cloud_topic_partition.cc",
        "partition_proxy.cc",
        "replicated_partition.cc",
    ],
    hdrs = [
        "cloud_topic_partition.h",
        "partition_proxy.h",
        "replicated_partition.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/cluster",
        "@boost//:numeric_conversion",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:state_accessors",
        "//src/v/cloud_topics/frontend",
        "//src/v/cloud_topics/frontend:errc",
        "//src/v/cloud_topics/level_zero/frontend_reader",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server:errors",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
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
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "@seastar",
    ],
)
