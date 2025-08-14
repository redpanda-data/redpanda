load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "deps",
    hdrs = [
        "deps.h",
    ],
    visibility = ["__subpackages__"],
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
    visibility = ["//visibility:public"],
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
