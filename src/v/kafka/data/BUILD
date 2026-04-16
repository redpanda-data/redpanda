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
    name = "exact_offset_replicator",
    hdrs = ["exact_offset_replicator.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/raft",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "make_exact_offset_replicator",
    srcs = ["make_exact_offset_replicator.cc"],
    hdrs = ["make_exact_offset_replicator.h"],
    implementation_deps = [
        ":partition_proxy",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":exact_offset_replicator",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_proxy",
    srcs = [
        "cloud_topic_partition.cc",
        "cloud_topic_read_replica.cc",
        "partition_proxy.cc",
        "replicated_partition.cc",
    ],
    hdrs = [
        "cloud_topic_partition.h",
        "cloud_topic_read_replica.h",
        "partition_proxy.h",
        "replicated_partition.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics/level_one/frontend_reader:reader",
        "//src/v/cloud_topics/read_replica:metadata_provider",
        "//src/v/cloud_topics/read_replica:partition_metadata",
        "//src/v/cloud_topics/read_replica:snapshot_metastore",
        "//src/v/cloud_topics/read_replica:snapshot_provider",
        "//src/v/cloud_topics/read_replica:stm",
        "//src/v/cluster",
        "//src/v/kafka/server:write_at_offset_stm",
        "//src/v/raft",
        "//src/v/storage",
        "@boost//:numeric_conversion",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":exact_offset_replicator",
        "//src/v/base",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics:state_accessors",
        "//src/v/cloud_topics/frontend",
        "//src/v/cloud_topics/frontend:errc",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_zero/frontend_reader",
        "//src/v/kafka/data:log_reader_config",
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

redpanda_cc_library(
    name = "log_reader_config",
    srcs = [
        "log_reader_config.cc",
    ],
    hdrs = [
        "log_reader_config.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/utils:named_type",
        "//src/v/utils:to_string",
        "@seastar",
    ],
)
