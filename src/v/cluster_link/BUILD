load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

redpanda_cc_rpc_library(
    name = "shadow_link_rpc_gen",
    src = "shadow_linking_rpc.json",
    visibility = ["//visibility:public"],
)

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
        "link_probe.cc",
        "link_status_reconciler.cc",
        "manager.cc",
        "task.cc",
        "topic_reconciler.cc",
    ],
    hdrs = [
        "deps.h",
        "link.h",
        "link_probe.h",
        "link_status_reconciler.h",
        "manager.h",
        "task.h",
        "topic_reconciler.h",
        "types.h",
    ],
    implementation_deps = [
        "//src/v/cluster_link/utils:topic_properties_utils",
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
        "//src/v/cluster_link/replication:deps",
        "//src/v/cluster_link/replication:link_replication_mgr",
        "//src/v/cluster_link/replication:partition_replicator",
        "//src/v/cluster_link/replication:types",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/client:cluster",
        "//src/v/kafka/data/rpc",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/ssx:mutex",
        "//src/v/ssx:work_queue",
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
        "//src/v/model",
        "@fmt",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        ":impl",
        "//src/v/kafka/client:cluster",
    ],
)

redpanda_cc_library(
    name = "group_mirroring_task",
    srcs = [
        "group_mirroring_task.cc",
    ],
    hdrs = [
        "group_mirroring_task.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/base",
        "//src/v/cluster_link/model",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:find_coordinator",
        "//src/v/kafka/protocol:list_offset",
        "//src/v/kafka/server",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:future_util",
        "@fmt",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        ":impl",
        "//src/v/kafka/client:cluster",
    ],
)

redpanda_cc_library(
    name = "security_migrator",
    srcs = [
        "security_migrator.cc",
    ],
    hdrs = [
        "security_migrator.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/kafka/server:security",
        "//src/v/security",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        ":impl",
        "//src/v/cluster_link/model",
        "//src/v/kafka/client:cluster",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:describe_acls",
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
        ":group_mirroring_task",
        ":impl",
        ":logger",
        ":security_migrator",
        ":source_topic_syncer",
        "//src/v/cluster_link/model",
        "//src/v/config",
        "//src/v/kafka/data:make_exact_offset_replicator",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        ":fwd",
        ":shadow_link_rpc_gen",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster/utils:partition_change_notifier_api",
        "//src/v/cluster_link/replication:deps",
        "//src/v/cluster_link/replication:mux_remote_consumer",
        "//src/v/cluster_link/replication:types",
        "//src/v/kafka/client/direct_consumer",
        "//src/v/kafka/data/rpc",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/rpc",
        "//src/v/ssx:mutex",
        "//src/v/ssx:work_queue",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rpc_service",
    srcs = [
        "rpc_service.cc",
    ],
    hdrs = [
        "rpc_service.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cluster_link",
        ":shadow_link_rpc_gen",
        "//src/v/cluster",
        "@seastar",
    ],
)
