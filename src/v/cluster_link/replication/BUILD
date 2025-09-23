load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "deps",
    hdrs = [
        "deps.h",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/raft",
    ],
)

redpanda_cc_library(
    name = "partition_replicator",
    srcs = [
        "partition_replicator.cc",
    ],
    hdrs = [
        "partition_replicator.h",
    ],
    visibility = ["__subpackages__"],
    deps = [
        ":deps",
        "//src/v/cluster_link:logger",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/utils:backoff_policy",
        "//src/v/utils:named_type",
        "//src/v/utils:notification_list",
        "//src/v/utils:prefix_logger",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "link_replication_mgr",
    srcs = [
        "link_replication_mgr.cc",
    ],
    hdrs = [
        "link_replication_mgr.h",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        ":deps",
        ":partition_replicator",
        "//src/v/base",
        "//src/v/cluster_link:logger",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/ssx:work_queue",
        "//src/v/utils:to_string",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_data_queue",
    srcs = [
        "partition_data_queue.cc",
    ],
    hdrs = [
        "partition_data_queue.h",
    ],
    visibility = ["__subpackages__"],
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "mux_remote_consumer",
    srcs = [
        "mux_remote_consumer.cc",
    ],
    hdrs = [
        "mux_remote_consumer.h",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        ":partition_data_queue",
        "//src/v/cluster_link:logger",
        "//src/v/container:chunked_hash_map",
        "//src/v/kafka/client/direct_consumer",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "deps_impl",
    srcs = [
        "deps_impl.cc",
    ],
    hdrs = [
        "deps_impl.h",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        ":deps",
        ":mux_remote_consumer",
        "//src/v/cluster",
        "//src/v/cluster_link:logger",
        "//src/v/kafka/server:write_at_offset_stm",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)
