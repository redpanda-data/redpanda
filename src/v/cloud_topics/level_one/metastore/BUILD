load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_rpc_library(
    name = "generated_level_one_rpc",
    src = "rpc.json",
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
        "service.cc",
    ],
    hdrs = [
        "leader_router.h",
        "service.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/domain:domain_manager",
        "//src/v/cloud_topics/level_one/domain:domain_supervisor",
        "//src/v/hashing:murmur",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":generated_level_one_rpc",
        ":rpc_types",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/rpc",
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
        ":metastore",
        "//src/v/base",
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
    deps = [
        ":offset_interval_set",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:map",
        "//src/v/serde:named_type",
        "//src/v/serde:set",
        "//src/v/serde:uuid",
        "//src/v/serde:vector",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
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
