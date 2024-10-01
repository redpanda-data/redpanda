load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel/thirdparty:seastar.bzl", "seastar_cc_swagger_library")

seastar_cc_swagger_library(
    name = "broker",
    src = "api-doc/broker.json",
)

seastar_cc_swagger_library(
    name = "cluster",
    src = "api-doc/cluster.json",
)

seastar_cc_swagger_library(
    name = "cluster_config",
    src = "api-doc/cluster_config.json",
)

seastar_cc_swagger_library(
    name = "config",
    src = "api-doc/config.json",
    definitions = [
        "api-doc/config.def.json",
    ],
)

seastar_cc_swagger_library(
    name = "debug",
    src = "api-doc/debug.json",
)

seastar_cc_swagger_library(
    name = "features",
    src = "api-doc/features.json",
)

seastar_cc_swagger_library(
    name = "hbadger",
    src = "api-doc/hbadger.json",
)

seastar_cc_swagger_library(
    name = "partition",
    src = "api-doc/partition.json",
)

seastar_cc_swagger_library(
    name = "raft",
    src = "api-doc/raft.json",
)

seastar_cc_swagger_library(
    name = "recovery",
    src = "api-doc/recovery.json",
)

seastar_cc_swagger_library(
    name = "security",
    src = "api-doc/security.json",
    definitions = [
        "api-doc/security.def.json",
    ],
)

seastar_cc_swagger_library(
    name = "shadow_indexing",
    src = "api-doc/shadow_indexing.json",
)

seastar_cc_swagger_library(
    name = "status",
    src = "api-doc/status.json",
)

seastar_cc_swagger_library(
    name = "transaction",
    src = "api-doc/transaction.json",
)

seastar_cc_swagger_library(
    name = "transform",
    src = "api-doc/transform.json",
)

seastar_cc_swagger_library(
    name = "usage",
    src = "api-doc/usage.json",
)

seastar_cc_swagger_library(
    name = "migration",
    src = "api-doc/migration.json",
    definitions = [
        "api-doc/migration.def.json",
    ],
)

seastar_cc_swagger_library(
    name = "debug_bundle",
    src = "api-doc/debug_bundle.json",
    definitions = [
        "api-doc/debug_bundle.def.json",
    ],
)

redpanda_cc_library(
    name = "cluster_config_schema_util",
    srcs = [
        "cluster_config_schema_util.cc",
    ],
    hdrs = [
        "api-doc/cluster_config.json.hh",
        "cluster_config_schema_util.h",
    ],
    include_prefix = "redpanda/admin",
    visibility = ["//visibility:public"],
    deps = [
        ":cluster_config",
        "//src/v/config",
        "//src/v/json",
        "@boost//:range",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "data_migration_utils",
    srcs = [
        "data_migration_utils.cc",
    ],
    hdrs = [
        "data_migration_utils.h",
    ],
    include_prefix = "redpanda/admin",
    visibility = ["//visibility:public"],
    deps = [
        ":migration",
        "//src/v/cluster",
        "//src/v/json",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "admin",
    srcs = [
        "debug.cc",
        "debug_bundle.cc",
        "kafka.cc",
        "migrations.cc",
        "partition.cc",
        "recovery.cc",
        "security.cc",
        "server.cc",
        "topics.cc",
        "transaction.cc",
        "transform.cc",
        "usage.cc",
        "util.cc",
    ],
    hdrs = [
        "api-doc/broker.json.hh",
        "api-doc/cluster.json.hh",
        "api-doc/cluster_config.json.hh",
        "api-doc/config.json.hh",
        "api-doc/debug.json.hh",
        "api-doc/debug_bundle.json.hh",
        "api-doc/features.json.hh",
        "api-doc/hbadger.json.hh",
        "api-doc/migration.json.hh",
        "api-doc/partition.json.hh",
        "api-doc/raft.json.hh",
        "api-doc/recovery.json.hh",
        "api-doc/security.json.hh",
        "api-doc/shadow_indexing.json.hh",
        "api-doc/status.json.hh",
        "api-doc/transaction.json.hh",
        "api-doc/transform.json.hh",
        "api-doc/usage.json.hh",
        "debug_bundle.h",
        "server.h",
        "util.h",
    ],
    include_prefix = "redpanda/admin",
    visibility = ["//visibility:public"],
    deps = [
        ":broker",
        ":cluster",
        ":cluster_config",
        ":cluster_config_schema_util",
        ":config",
        ":data_migration_utils",
        ":debug",
        ":debug_bundle",
        ":features",
        ":hbadger",
        ":migration",
        ":partition",
        ":raft",
        ":recovery",
        ":security",
        ":shadow_indexing",
        ":status",
        ":transaction",
        ":transform",
        ":usage",
        "//src/v/base",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:fragmented_vector",
        "//src/v/container:lw_shared_container",
        "//src/v/debug_bundle",
        "//src/v/debug_bundle:error",
        "//src/v/debug_bundle:json",
        "//src/v/debug_bundle:types",
        "//src/v/features",
        "//src/v/features:enterprise_features",
        "//src/v/finjector",
        "//src/v/json",
        "//src/v/kafka/server",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/net",
        "//src/v/pandaproxy",
        "//src/v/pandaproxy:config",
        "//src/v/raft",
        "//src/v/reflection:type_traits",
        "//src/v/resource_mgmt:cpu_profiler",
        "//src/v/resource_mgmt:memory_sampling",
        "//src/v/rpc",
        "//src/v/security",
        "//src/v/security:request_auth",
        "//src/v/security/audit",
        "//src/v/security/audit:types",
        "//src/v/serde",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "//src/v/strings:string_switch",
        "//src/v/strings:utf8",
        "//src/v/transform",
        "//src/v/utils:functional",
        "//src/v/utils:unresolved_address",
        "//src/v/wasm:api",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:algorithm",
        "@boost//:lexical_cast",
        "@boost//:range",
        "@fmt",
        "@rapidjson",
        "@seastar",
    ],
)
