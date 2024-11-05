load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "snapshot_manager",
    srcs = [
        "snapshot_manager.cc",
    ],
    hdrs = [
        "snapshot_manager.h",
    ],
    implementation_deps = [
        ":logger",
        ":managed_partition",
        ":snapshot_io",
        "//src/v/cloud_topics:dl_snapshot",
        "//src/v/cloud_topics/dl_stm",
        "//src/v/cloud_topics/dl_stm:dl_stm_api",
        "//src/v/cluster",
        "//src/v/raft",
        "//src/v/serde",
        "//src/v/utils:retry_chain_node",
    ],
    include_prefix = "cloud_topics/recovery",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster:notification",
        "//src/v/model",
        "//src/v/raft:fwd",
        "@abseil-cpp//absl/container:node_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "managed_partition",
    srcs = ["managed_partition.cc"],
    hdrs = ["managed_partition.h"],
    implementation_deps = ["//src/v/cluster"],
    include_prefix = "cloud_topics/recovery",
    deps = [
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "snapshot_io",
    srcs = [
        "snapshot_io.cc",
    ],
    hdrs = [
        "snapshot_io.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io:transfer_details",
        "//src/v/cloud_storage_clients",
    ],
    include_prefix = "cloud_topics/recovery",
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:dl_version",
        "//src/v/model",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "cloud_topics/recovery",
    deps = [
        "@seastar",
    ],
)
