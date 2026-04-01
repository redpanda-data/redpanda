load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "types",
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "types.h",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/ssx:semaphore",
    ],
)

redpanda_cc_library(
    name = "deps",
    srcs = [
        "deps.cc",
    ],
    hdrs = [
        "deps.h",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        ":types",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/ssx:semaphore",
    ],
)

redpanda_cc_library(
    name = "partition_replicator",
    srcs = [
        "partition_replicator.cc",
        "replication_probe.cc",
    ],
    hdrs = [
        "partition_replicator.h",
        "replication_probe.h",
    ],
    visibility = [
        "__subpackages__",
        "//src/v/cluster_link:__subpackages__",
    ],
    deps = [
        ":deps",
        ":types",
        "//src/v/cluster_link:logger",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/metrics",
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
    implementation_deps = [
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        ":deps",
        ":partition_replicator",
        ":types",
        "//src/v/base",
        "//src/v/cluster_link:logger",
        "//src/v/constants:common_constants",
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
        ":types",
        "//src/v/container:chunked_vector",
        "//src/v/model",
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
        ":types",
        "//src/v/base",
        "//src/v/cluster_link:logger",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/kafka/client/direct_consumer",
        "//src/v/kafka/server",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)
