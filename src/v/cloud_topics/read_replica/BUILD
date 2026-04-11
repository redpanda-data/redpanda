load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/read_replica:__subpackages__"])

redpanda_cc_library(
    name = "metadata_provider",
    hdrs = [
        "metadata_provider.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":partition_metadata",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "metadata_manager",
    srcs = [
        "metadata_manager.cc",
    ],
    hdrs = [
        "metadata_manager.h",
        "partition_metadata.h",
    ],
    implementation_deps = [
        ":snapshot_manager",
        ":state_refresh_loop",
        ":stm",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/metastore:manifest_io",
        "//src/v/ssx:work_queue",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":metadata_provider",
        ":partition_metadata",
        "//src/v/cloud_storage:remote_label",
        "//src/v/cluster",
        "//src/v/cluster/utils:partition_change_notifier_api",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_metadata",
    hdrs = [
        "partition_metadata.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "snapshot_provider",
    hdrs = [
        "snapshot_provider.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/lsm",
        "//src/v/model",
        "//src/v/utils:detailed_error",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "snapshot_manager",
    srcs = [
        "snapshot_manager.cc",
    ],
    hdrs = [
        "snapshot_manager.h",
    ],
    implementation_deps = [
        ":snapshot_metastore",
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/common:file_io",
        "//src/v/cloud_topics/level_one/metastore:manifest_io",
        "//src/v/config",
        "//src/v/lsm/io:cloud_persistence",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sleep_abortable",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":snapshot_provider",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/container:chunked_hash_map",
        "//src/v/lsm",
        "//src/v/model",
        "//src/v/ssx:semaphore",
        "//src/v/utils:detailed_error",
        "//src/v/utils:prefix_logger",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "snapshot_metastore",
    srcs = [
        "snapshot_metastore.cc",
    ],
    hdrs = [
        "snapshot_metastore.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/metastore/lsm:keys",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_reader",
        "//src/v/lsm",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "state_refresh_loop",
    srcs = [
        "state_refresh_loop.cc",
    ],
    hdrs = [
        "state_refresh_loop.h",
    ],
    implementation_deps = [
        ":snapshot_metastore",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/metastore:manifest_io",
        "//src/v/cloud_topics/level_one/metastore:metastore_manifest",
        "//src/v/config",
        "//src/v/hashing:murmur",
        "//src/v/model",
        "//src/v/model:batch_builder",
        "//src/v/serde",
        "//src/v/ssx:sleep_abortable",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":snapshot_manager",
        ":stm",
        "//src/v/cloud_storage:remote_label",
        "//src/v/cloud_storage_clients",
        "//src/v/utils:detailed_error",
        "//src/v/utils:prefix_logger",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "stm",
    srcs = [
        "stm.cc",
    ],
    hdrs = [
        "stm.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/model:batch_builder",
        "//src/v/serde",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/cluster:state_machine_registry",
        "//src/v/lsm",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/utils:detailed_error",
        "@seastar",
    ],
)
