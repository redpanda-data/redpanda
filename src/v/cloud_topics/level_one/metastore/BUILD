load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_rpc_library(
    name = "generated_level_one_rpc",
    src = "rpc.json",
)

redpanda_cc_library(
    name = "domain_uuid",
    hdrs = [
        "domain_uuid.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "metastore_manifest",
    hdrs = [
        "metastore_manifest.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":domain_uuid",
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/serde",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "manifest_io",
    srcs = [
        "manifest_io.cc",
    ],
    hdrs = [
        "manifest_io.h",
    ],
    implementation_deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/serde",
        "//src/v/ssx:future_util",
        "//src/v/utils:retry_chain_node",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":metastore_manifest",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage:remote_label",
        "//src/v/model",
        "//src/v/utils:detailed_error",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "garbage_collector",
    srcs = [
        "garbage_collector.cc",
    ],
    hdrs = [
        "garbage_collector.h",
    ],
    implementation_deps = [
        ":simple_stm",
        ":state_update",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/common:abstract_io",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "leader_router",
    srcs = [
        "leader_router.cc",
        "leader_router_probe.cc",
        "service.cc",
    ],
    hdrs = [
        "leader_router.h",
        "leader_router_probe.h",
        "service.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/domain:domain_manager",
        "//src/v/cloud_topics/level_one/domain:domain_supervisor",
        "//src/v/config",
        "//src/v/hashing:murmur",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":generated_level_one_rpc",
        ":rpc_types",
        "//src/v/base",
        "//src/v/cloud_storage:remote_label",
        "//src/v/cluster",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/rpc",
        "//src/v/utils:log_hist",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "metastore",
    hdrs = [
        "metastore.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":metastore_manifest",
        ":offset_interval_set",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "retry",
    hdrs = [
        "retry.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":metastore",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "replicated_metastore",
    srcs = [
        "replicated_metastore.cc",
    ],
    hdrs = [
        "replicated_metastore.h",
    ],
    implementation_deps = [
        ":leader_router",
        ":rpc_types",
        ":state_update",
        "//src/v/cloud_topics:logger",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":manifest_io",
        ":metastore",
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/model",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rpc_types",
    hdrs = [
        "rpc_types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":domain_uuid",
        ":offset_interval_set",
        ":state_update",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:enum",
    ],
)

redpanda_cc_library(
    name = "simple",
    srcs = [
        "simple_metastore.cc",
    ],
    hdrs = [
        "simple_metastore.h",
    ],
    implementation_deps = [
        ":state_update",
        "//src/v/cloud_topics:logger",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":metastore",
        ":offset_interval_set",
        ":state",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "simple_stm",
    srcs = [
        "simple_stm.cc",
    ],
    hdrs = [
        "simple_stm.h",
    ],
    implementation_deps = [
        ":state_update",
        "//src/v/cloud_topics:logger",
        "//src/v/serde",
        "//src/v/ssx:future_util",
        "//src/v/utils:prefix_logger",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":state",
        "//src/v/base",
        "//src/v/cluster:state_machine_registry",
        "//src/v/model",
        "//src/v/raft",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "state",
    srcs = [
        "state.cc",
    ],
    hdrs = [
        "state.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":offset_interval_set",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:map",
        "//src/v/serde:named_type",
        "//src/v/serde:optional",
        "//src/v/serde:set",
        "//src/v/serde:uuid",
        "//src/v/serde:vector",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "state_update_utils",
    srcs = [
        "state_update_utils.cc",
    ],
    hdrs = [
        "state_update_utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":offset_interval_set",
        ":state",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "state_update",
    srcs = [
        "state_update.cc",
    ],
    hdrs = [
        "state_update.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":state",
        ":state_update_utils",
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@abseil-cpp//absl/container:btree",
    ],
)

redpanda_cc_library(
    name = "offset_interval_set",
    srcs = [
        "offset_interval_set.cc",
    ],
    hdrs = [
        "offset_interval_set.h",
    ],
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/container:interval_set",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:map",
    ],
)

redpanda_cc_library(
    name = "topic_purger",
    srcs = [
        "topic_purger.cc",
    ],
    hdrs = [
        "topic_purger.h",
    ],
    implementation_deps = [
        ":metastore",
        ":retry",
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/model:batch_builder",
        "//src/v/ssx:sleep_abortable",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/ssx:work_queue",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "flush_loop",
    srcs = [
        "flush_loop.cc",
    ],
    hdrs = [
        "flush_loop.h",
    ],
    implementation_deps = [
        ":metastore",
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/config",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:time",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/ssx:actor",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_validator",
    srcs = [
        "partition_validator.cc",
    ],
    hdrs = [
        "partition_validator.h",
    ],
    implementation_deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics/level_one/common:object_utils",
        "//src/v/cloud_topics/level_one/metastore/lsm:keys",
        "//src/v/utils:retry_chain_node",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_reader",
        "//src/v/cloud_topics/level_one/metastore/lsm:values",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/utils:detailed_error",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "extent_metadata_reader",
    srcs = [
        "extent_metadata_reader.cc",
    ],
    hdrs = [
        "extent_metadata_reader.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":metastore",
        ":retry",
        "//src/v/model",
        "@seastar",
    ],
)
