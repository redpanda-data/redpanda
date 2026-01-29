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
    definitions = [
        "api-doc/cluster_config.def.json",
    ],
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
    definitions = [
        "api-doc/shadow_indexing.def.json",
    ],
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
    visibility = ["//visibility:public"],
    deps = [
        ":migration",
        "//src/v/cluster",
        "//src/v/json",
        "//src/v/model",
        "@abseil-cpp//absl/strings",
    ],
)

redpanda_cc_library(
    name = "aip_common_config",
    hdrs = ["aip_common_config.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/serde/protobuf:base",
        "//src/v/serde/protobuf:rpc",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "aip_filter",
    srcs = [
        "aip_filter.cc",
    ],
    hdrs = [
        "aip_filter.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":aip_common_config",
        "//src/v/base",
        "//src/v/serde/protobuf:base",
        "//src/v/serde/protobuf:field_mask",
        "//src/v/serde/protobuf:rpc",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/time",
        "@fmt",
        "@lexy",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "aip_ordering",
    srcs = [
        "aip_ordering.cc",
    ],
    hdrs = [
        "aip_ordering.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":aip_common_config",
        "//src/v/base",
        "//src/v/serde/protobuf:base",
        "//src/v/serde/protobuf:field_mask",
        "//src/v/serde/protobuf:rpc",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/time",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "kafka_connections_service_impl",
    hdrs = [
        "kafka_connections_service_impl.h",
    ],
    visibility = ["//src/v/redpanda/admin:__subpackages__"],
    deps = [
        "//proto/redpanda/core/admin/v2:kafka_connections_redpanda_proto",
        "//src/v/base",
        "//src/v/container:priority_queue",
        "//src/v/ssx:async_algorithm",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "kafka_connections_service",
    srcs = [
        "kafka_connections_service.cc",
    ],
    hdrs = [
        "kafka_connections_service.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":aip_filter",
        ":aip_ordering",
        ":kafka_connections_service_impl",
        "//proto/redpanda/core/admin/v2:cluster_redpanda_proto",
        "//proto/redpanda/core/admin/v2:kafka_connections_redpanda_proto",
        "//src/v/base",
        "//src/v/container:priority_queue",
        "//src/v/kafka/server",
        "//src/v/random:generators",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "admin",
    srcs = [
        "cluster_recovery.cc",
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
        "@fmt",
    ],
    implementation_deps = [
        "//src/v/features:enterprise_feature_messages",
        "//src/v/utils:uuid",
    ],
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
        ":kafka_connections_service",
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
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_io:cache",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/container:lw_shared_container",
        "//src/v/debug_bundle",
        "//src/v/debug_bundle:error",
        "//src/v/debug_bundle:json",
        "//src/v/debug_bundle:types",
        "//src/v/features",
        "//src/v/features:enterprise_features",
        "//src/v/finjector",
        "//src/v/json",
        "//src/v/kafka/client:config_utils",
        "//src/v/kafka/client:configuration",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/kafka/server",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/net",
        "//src/v/pandaproxy/rest:server",
        "//src/v/pandaproxy/schema_registry:config",
        "//src/v/pandaproxy/schema_registry:server",
        "//src/v/raft",
        "//src/v/redpanda/admin/services:broker",
        "//src/v/reflection:type_traits",
        "//src/v/resource_mgmt:cpu_profiler",
        "//src/v/resource_mgmt:memory_sampling",
        "//src/v/rpc",
        "//src/v/security",
        "//src/v/security:request_auth",
        "//src/v/security/audit",
        "//src/v/security/audit:types",
        "//src/v/serde",
        "//src/v/serde/protobuf:rpc",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "//src/v/strings:string_switch",
        "//src/v/strings:utf8",
        "//src/v/transform",
        "//src/v/transform:fwd",
        "//src/v/utils:arch",
        "//src/v/utils:functional",
        "//src/v/utils:unresolved_address",
        "//src/v/version",
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
