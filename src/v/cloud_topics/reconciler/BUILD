load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [":__subpackages__"])

redpanda_cc_library(
    name = "adaptive_interval",
    srcs = [
        "adaptive_interval.cc",
    ],
    hdrs = [
        "adaptive_interval.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/config",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "reconciliation_consumer",
    srcs = [
        "reconciliation_consumer.cc",
    ],
    hdrs = [
        "reconciliation_consumer.h",
    ],
    deps = [
        ":reconciler_probe",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/model",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "reconciler_probe",
    srcs = [
        "reconciler_probe.cc",
    ],
    hdrs = [
        "reconciler_probe.h",
    ],
    deps = [
        "//src/v/config",
        "//src/v/metrics",
        "//src/v/utils:hdr_hist",
        "//src/v/utils:log_hist",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "reconciler",
    srcs = [
        "reconciler.cc",
        "reconciliation_source.cc",
    ],
    hdrs = [
        "reconciler.h",
        "reconciliation_source.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/frontend",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_api",
        "//src/v/config",
        "//src/v/kafka/utils:txn_reader",
        "//src/v/utils:retry_chain_node",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":adaptive_interval",
        ":reconciler_probe",
        ":reconciliation_consumer",
        "//src/v/base",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics:object_utils",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/common:object_utils",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
        "//src/v/cloud_topics/level_one/metastore:retry",
        "//src/v/cluster",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/utils:retry_chain_node",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
