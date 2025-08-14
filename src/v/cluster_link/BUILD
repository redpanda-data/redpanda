load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "errc",
    srcs = [
        "errc.cc",
    ],
    hdrs = [
        "errc.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "utils",
    srcs = [
        "utils.cc",
    ],
    hdrs = [
        "utils.h",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        "//src/v/cluster_link/model",
        "//src/v/kafka/client:configuration",
    ],
)

redpanda_cc_library(
    name = "impl",
    srcs = [
        "deps.cc",
        "link.cc",
        "manager.cc",
        "task.cc",
    ],
    hdrs = [
        "deps.h",
        "link.h",
        "manager.h",
        "task.h",
        "types.h",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        ":errc",
        ":fwd",
        ":logger",
        ":utils",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster_link/model",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/client:cluster",
        "//src/v/kafka/data/rpc",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/ssx:work_queue",
        "//src/v/utils:mutex",
        "//src/v/utils:notification_list",
        "//src/v/utils:prefix_logger",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "fwd",
    hdrs = [
        "fwd.h",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "source_topic_syncer",
    srcs = [
        "source_topic_syncer.cc",
    ],
    hdrs = [
        "source_topic_syncer.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/cluster_link/model",
        "//src/v/kafka/server:topic_config_utils",
        "@fmt",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        ":impl",
        "//src/v/kafka/client:cluster",
    ],
)

redpanda_cc_library(
    name = "cluster_link",
    srcs = [
        "service.cc",
    ],
    hdrs = [
        "service.h",
    ],
    implementation_deps = [
        ":impl",
        ":logger",
        ":source_topic_syncer",
        "//src/v/cluster_link/model",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster/utils:partition_change_notifier_api",
        "//src/v/model",
        "//src/v/raft",
        "@seastar",
    ],
)
