load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "domain_manager",
    hdrs = [
        "domain_manager.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_one/metastore:partition_validator",
        "//src/v/cloud_topics/level_one/metastore:rpc_types",
        "//src/v/cloud_topics/level_one/metastore/lsm:write_batch_row",
    ],
)

redpanda_cc_library(
    name = "domain_supervisor",
    srcs = [
        "domain_supervisor.cc",
    ],
    hdrs = [
        "domain_supervisor.h",
    ],
    implementation_deps = [
        ":db_domain_manager",
        ":domain_manager_probe",
        ":simple_domain_manager",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/metastore/lsm:stm",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/model",
        "//src/v/ssx:when_all",
        "//src/v/ssx:work_queue",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":domain_manager",
        "//src/v/base",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "simple_domain_manager",
    srcs = [
        "simple_domain_manager.cc",
    ],
    hdrs = [
        "simple_domain_manager.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:garbage_collector",
        "//src/v/cloud_topics/level_one/metastore:rpc_types",
        "//src/v/cloud_topics/level_one/metastore:state_update",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/model:batch_builder",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sleep_abortable",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":domain_manager",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/cloud_topics/level_one/metastore:simple_stm",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "domain_manager_probe",
    srcs = [
        "domain_manager_probe.cc",
    ],
    hdrs = [
        "domain_manager_probe.h",
    ],
    deps = [
        "//src/v/config",
        "//src/v/metrics",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "db_domain_manager",
    srcs = [
        "db_domain_manager.cc",
    ],
    hdrs = [
        "db_domain_manager.h",
        "entity_lock_map.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/cloud_topics/level_one/metastore:state_update",
        "//src/v/cloud_topics/level_one/metastore/lsm:garbage_collector",
        "//src/v/cloud_topics/level_one/metastore/lsm:keys",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_reader",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_update",
        "//src/v/config",
        "//src/v/lsm/io:cloud_persistence",
        "//src/v/lsm/proto:manifest_redpanda_proto",
        "//src/v/model:batch_builder",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sleep_abortable",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":domain_manager",
        ":domain_manager_probe",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:rpc_types",
        "//src/v/cloud_topics/level_one/metastore/lsm:replicated_db",
        "//src/v/cloud_topics/level_one/metastore/lsm:stm",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/ssx:checkpoint_mutex",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
