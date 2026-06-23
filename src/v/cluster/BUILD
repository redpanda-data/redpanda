load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

redpanda_cc_library(
    name = "run_quota",
    hdrs = [
        "archival/run_quota.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_rpc_library(
    name = "bootstrap_rpc",
    src = "cluster_bootstrap.json",
)

redpanda_cc_rpc_library(
    name = "controller_rpc",
    src = "controller.json",
    deps = [
        ":cluster_link_rpc_types",
        ":plugin_rpc_types",
        ":security_types",
    ],
)

redpanda_cc_rpc_library(
    name = "metadata_rpc",
    src = "metadata_dissemination_rpc.json",
)

redpanda_cc_rpc_library(
    name = "id_allocator_rpc",
    src = "id_allocator.json",
)

redpanda_cc_rpc_library(
    name = "tx_gateway_rpc",
    src = "tx_gateway.json",
)

redpanda_cc_rpc_library(
    name = "partition_balancer_rpc",
    src = "partition_balancer_rpc.json",
)

redpanda_cc_rpc_library(
    name = "node_status_rpc",
    src = "node_status_rpc.json",
)

redpanda_cc_rpc_library(
    name = "ephemeral_credential_rpc",
    src = "ephemeral_credential_rpc.json",
    deps = [
        ":ephemeral_credential_serde",
    ],
)

redpanda_cc_rpc_library(
    name = "self_test_rpc",
    src = "self_test_rpc.json",
)

redpanda_cc_rpc_library(
    name = "topic_recovery_status_rpc",
    src = "topic_recovery_status_rpc.json",
)

redpanda_cc_rpc_library(
    name = "offsets_recovery_rpc",
    src = "offsets_recovery_rpc.json",
)

redpanda_cc_rpc_library(
    name = "tx_manager_migrator_rpc",
    src = "migrations/tx_manager_migrator.json",
    out = "tx_manager_migrator_service.h",
    visibility = ["//visibility:public"],
)

redpanda_cc_rpc_library(
    name = "data_migrations_rpc",
    src = "data_migration_rpc.json",
)

redpanda_cc_library(
    name = "errc",
    srcs = [
        "errors.cc",
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
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "leader_balancer_strategy",
    srcs = [
        "scheduling/leader_balancer_constraints.cc",
        "scheduling/leader_balancer_greedy.cc",
        "scheduling/leader_balancer_probe.cc",
    ],
    hdrs = [
        "scheduling/leader_balancer_constraints.h",
        "scheduling/leader_balancer_greedy.h",
        "scheduling/leader_balancer_probe.h",
        "scheduling/leader_balancer_random.h",
        "scheduling/leader_balancer_strategy.h",
        "scheduling/leader_balancer_types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@roaring",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "version",
    hdrs = [
        "version.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_library(
    name = "features",
    hdrs = [
        "feature_update_action.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/serde:sstring",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "notification",
    hdrs = [
        "notification.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_library(
    name = "snapshot",
    hdrs = [
        "snapshot.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "topic_rules",
    hdrs = [
        "topic_rules.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "nt_revision",
    hdrs = [
        "nt_revision.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "//src/v/serde",
        "@abseil-cpp//absl/hash",
    ],
)

redpanda_cc_library(
    name = "topic_configuration",
    srcs = [
        "remote_topic_properties.cc",
        "topic_configuration.cc",
        "topic_properties.cc",
    ],
    hdrs = [
        "remote_topic_properties.h",
        "topic_configuration.h",
        "topic_properties.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_storage:remote_label",
        "//src/v/model",
        "//src/v/pandaproxy/schema_registry:subject_name_strategy",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/reflection:adl",
        "//src/v/serde",
        "//src/v/serde:chrono",
        "//src/v/serde:optional",
        "//src/v/serde:tristate",
        "//src/v/storage",
        "//src/v/utils:tristate",
        "//src/v/utils:uuid",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "offsets_lookup",
    srcs = [
        "cloud_metadata/offsets_lookup.cc",
        "cloud_metadata/offsets_lookup_batcher.cc",
        "cloud_metadata/offsets_recoverer.cc",
        "cloud_metadata/offsets_recovery_manager.cc",
        "cloud_metadata/offsets_uploader.cc",
    ],
    hdrs = [
        "cloud_metadata/offsets_lookup.h",
        "cloud_metadata/offsets_lookup_batcher.h",
        "cloud_metadata/offsets_lookup_rpc_types.h",
        "cloud_metadata/offsets_recoverer.h",
        "cloud_metadata/offsets_recovery_manager.h",
        "cloud_metadata/offsets_uploader.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cloud_metadata_key_utils",
        ":cloud_metadata_offsets_recovery_router",
        ":cloud_metadata_types",
        ":errc",
        ":fwd",
        ":logger",
        ":members_table",
        ":offsets_recovery_rpc",
        ":offsets_snapshot",
        ":partition_leaders_table",
        ":shard_table",
        "//src/v/base",
        "//src/v/cloud_io:cache",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage_clients",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/rpc",
        "//src/v/serde",
        "//src/v/ssx:future_util",
        "//src/v/utils:retry_chain_node",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:node_hash_set",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "topic_memory_per_partition_default",
    hdrs = [
        "scheduling/topic_memory_per_partition_default.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

# TODO the following headers are the only headers in redpanda which are excluded
# from clang-tidy. if you remove the exclusion then you'll observe a tangled web
# of header dependencies. after many hours those have not yet been resolved, so
# they are excluded here so that we don't hold up clang-tidy from being applied
# to the entire tree.

redpanda_cc_library(
    name = "cloud_metadata_offsets_recovery_router",
    hdrs = [
        "cloud_metadata/offsets_recovery_router.h",
    ],
    tags = ["noclangtidy"],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "cloud_metadata_offsets_recovery_service",
    hdrs = [
        "cloud_metadata/offsets_recovery_service.h",
    ],
    tags = ["noclangtidy"],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "cloud_metadata_offsets_upload_router",
    hdrs = [
        "cloud_metadata/offsets_upload_router.h",
    ],
    tags = ["noclangtidy"],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "state_machine_registry",
    hdrs = [
        "state_machine_registry.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/raft",
    ],
)

redpanda_cc_library(
    name = "table_utils",
    srcs = [
        "cluster_link/table_utils.cc",
    ],
    hdrs = [
        "cluster_link/table_utils.h",
    ],
    implementation_deps = [
        "//src/v/ssx:async_algorithm",
    ],
    visibility = ["__subpackages__"],
    deps = [
        "//src/v/base",
        "//src/v/cluster_link/model",
        "//src/v/container:chunked_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "fwd",
    hdrs = [
        "fwd.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "tx_errc",
    srcs = [
        "tx_errc.cc",
    ],
    hdrs = [
        "tx_errc.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "cluster_link_errc",
    srcs = [
        "cluster_link/errc.cc",
    ],
    hdrs = [
        "cluster_link/errc.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "simple_batch_builder",
    hdrs = [
        "simple_batch_builder.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/reflection:adl",
        "//src/v/storage:record_batch_builder",
    ],
)

redpanda_cc_library(
    name = "namespaced_cache",
    hdrs = [
        "namespaced_cache.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/config",
        "//src/v/container:intrusive",
        "@abseil-cpp//absl/container:node_hash_map",
        "@boost//:intrusive",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "tx_hash_ranges",
    hdrs = [
        "tx_hash_ranges.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/hashing:murmur",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/reflection:adl",
        "@abseil-cpp//absl/container:node_hash_set",
    ],
)

redpanda_cc_library(
    name = "ntp_callbacks",
    hdrs = [
        "ntp_callbacks.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":notification",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
)

redpanda_cc_library(
    name = "shard_table",
    hdrs = [
        "shard_table.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        ":notification",
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/utils:notification_list",
        "@abseil-cpp//absl/container:node_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "node_status_rpc_types",
    hdrs = [
        "node_status_rpc_types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "//src/v/serde",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "node_status_table",
    hdrs = [
        "node_status_table.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/config",
        "//src/v/model",
        "//src/v/rpc",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
)

redpanda_cc_library(
    name = "offsets_snapshot",
    hdrs = [
        "offsets_snapshot.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "security_types",
    hdrs = [
        "security_types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        "//src/v/model",
        "//src/v/security",
        "//src/v/serde",
        "//src/v/serde:vector",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "plugin_rpc_types",
    hdrs = [
        "plugin_rpc_types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/utils:uuid",
    ],
)

redpanda_cc_library(
    name = "partition_balancer_types",
    srcs = [
        "partition_balancer_types.cc",
    ],
    hdrs = [
        "partition_balancer_types.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/utils:human",
        "//src/v/utils:to_string",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/serde:map",
        "//src/v/serde:optional",
        "//src/v/serde:set",
        "//src/v/serde:vector",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
)

redpanda_cc_library(
    name = "cluster_link_rpc_types",
    hdrs = [
        "cluster_link_rpc_types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cluster_link_errc",
        ":errc",
        "//src/v/cluster_link/model",
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "client_quota_serde",
    srcs = [
        "client_quota_serde.cc",
    ],
    hdrs = [
        "client_quota_serde.h",
    ],
    implementation_deps = [
        "//src/v/serde:set",
        "//src/v/serde:sstring",
        "//src/v/utils:to_string",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        "//src/v/base",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:variant",
        "//src/v/strings:string_switch",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "self_test_rpc_types",
    srcs = [
        "self_test_rpc_types.cc",
    ],
    hdrs = [
        "self_test_rpc_types.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/config",
        "//src/v/json",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:chrono",
        "//src/v/serde:enum",
        "//src/v/serde:iobuf",
        "//src/v/serde:optional",
        "//src/v/serde:sstring",
        "//src/v/serde:uuid",
        "//src/v/serde:vector",
        "//src/v/utils:uuid",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "tm_stm_types",
    srcs = [
        "tm_stm_types.cc",
    ],
    hdrs = [
        "tm_stm_types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cloud_metadata_types",
    hdrs = [
        "cloud_metadata/error_outcome.h",
        "cloud_metadata/types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/utils:named_type",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "cloud_metadata_key_utils",
    srcs = [
        "cloud_metadata/key_utils.cc",
    ],
    hdrs = [
        "cloud_metadata/key_utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cloud_metadata_types",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage_clients",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "cloud_metadata_cluster_manifest",
    srcs = [
        "cloud_metadata/cluster_manifest.cc",
    ],
    hdrs = [
        "cloud_metadata/cluster_manifest.h",
    ],
    implementation_deps = [
        ":cloud_metadata_key_utils",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/utils:uuid",
        "@boost//:lexical_cast",
        "@boost//:uuid",
        "@rapidjson",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cloud_metadata_types",
        "//src/v/base",
        "//src/v/cloud_storage",
        "//src/v/json",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "data_policy",
    hdrs = [
        "data_policy.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/reflection:adl",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "types",
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "types.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/reflection:adl",
        "//src/v/utils:to_string",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cloud_metadata_cluster_manifest",
        ":cluster_link_errc",
        ":data_policy",
        ":errc",
        ":features",
        ":fwd",
        ":nt_revision",
        ":partition_balancer_types",
        ":security_types",
        ":topic_configuration",
        ":tx_errc",
        ":version",
        "//src/v/cloud_storage:remote_label",
        "//src/v/cluster_link/model",
        "//src/v/container:chunked_vector",
        "//src/v/container:contiguous_range_map",
        "//src/v/model",
        "//src/v/pandaproxy/schema_registry:subject_name_strategy",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/raft",
        "//src/v/security",
        "//src/v/security:license",
        "//src/v/serde",
        "//src/v/serde:bool_class",
        "//src/v/serde:chrono",
        "//src/v/serde:enum",
        "//src/v/serde:inet_address",
        "//src/v/serde:map",
        "//src/v/serde:named_type",
        "//src/v/serde:sstring",
        "//src/v/serde:vector",
        "//src/v/storage",
        "//src/v/utils:tristate",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:node_hash_map",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "data_migration_types",
    srcs = [
        "data_migration_types.cc",
    ],
    hdrs = [
        "data_migration_types.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/ssx:sformat",
        "//src/v/utils:to_string",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        ":offsets_snapshot",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/serde:named_type",
        "//src/v/serde:optional",
        "//src/v/serde:sstring",
        "//src/v/serde:variant",
        "//src/v/serde:vector",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "node_types",
    srcs = [
        "node/types.cc",
    ],
    hdrs = [
        "node/types.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/utils:to_string",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/model",
        "//src/v/reflection:adl",
        "//src/v/storage:disk",
        "//src/v/utils:human",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rm_stm_types",
    srcs = [
        "rm_stm_types.cc",
    ],
    hdrs = [
        "rm_stm_types.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/storage:record_batch_builder",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/kafka/protocol",
        "//src/v/reflection:adl",
    ],
)

redpanda_cc_library(
    name = "tx_protocol_types",
    srcs = [
        "tx_protocol_types.cc",
    ],
    hdrs = [
        "tx_protocol_types.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/utils:to_string",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        ":tx_errc",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/serde:bool_class",
        "//src/v/serde:chrono",
        "//src/v/serde:enum",
        "//src/v/serde:vector",
    ],
)

redpanda_cc_library(
    name = "plugin_table",
    srcs = [
        "plugin_table.cc",
    ],
    hdrs = [
        "plugin_table.h",
    ],
    implementation_deps = [
        ":types",
        "//src/v/base",
        "@seastar",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_set",
    ],
)

redpanda_cc_library(
    name = "data_migrated_resources",
    srcs = [
        "data_migrated_resources.cc",
    ],
    hdrs = [
        "data_migrated_resources.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":data_migration_types",
        ":fwd",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "notification_latch",
    srcs = [
        "notification_latch.cc",
    ],
    hdrs = [
        "notification_latch.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        "//src/v/model",
        "//src/v/utils:expiring_promise",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cluster_link_types",
    srcs = [
        "cluster_link/types.cc",
    ],
    hdrs = [
        "cluster_link/types.h",
    ],
    implementation_deps = [
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cluster_link_errc",
        "//src/v/base",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "commands",
    hdrs = [
        "commands.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":client_quota_serde",
        ":cluster_link_rpc_types",
        ":data_migration_types",
        ":security_types",
        ":simple_batch_builder",
        ":types",
        "//src/v/bytes:iobuf_parser",
        "//src/v/cluster_link/model",
        "//src/v/model",
        "//src/v/reflection:adl",
        "//src/v/security",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "bootstrap_types",
    hdrs = [
        "bootstrap_types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/model",
        "//src/v/serde",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "cluster_recovery_state",
    hdrs = [
        "cluster_recovery_state.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cloud_metadata_cluster_manifest",
        ":types",
        "//src/v/model",
        "//src/v/serde",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "data_migration_group_proxy",
    hdrs = [
        "data_migration_group_proxy.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":offsets_snapshot",
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cluster_uuid",
    hdrs = [
        "cluster_uuid.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "drain_status",
    srcs = [
        "drain_status.cc",
    ],
    hdrs = [
        "drain_status.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "distributed_kv_stm",
    hdrs = [
        "distributed_kv_stm.h",
        "distributed_kv_stm_types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        ":fwd",
        ":logger",
        ":simple_batch_builder",
        "//src/v/base",
        "//src/v/hashing:murmur",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/serde:iobuf",
        "//src/v/utils:fixed_string",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rm_group_proxy",
    hdrs = [
        "rm_group_proxy.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":tx_protocol_types",
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "topic_table_probe",
    hdrs = [
        "topic_table_probe.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        ":fwd",
        "//src/v/metrics",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_set",
    ],
)

redpanda_cc_library(
    name = "controller_snapshot",
    srcs = [
        "controller_snapshot.cc",
    ],
    hdrs = [
        "controller_snapshot.h",
    ],
    implementation_deps = [
        "//src/v/security",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":client_quota_serde",
        ":cluster_recovery_state",
        ":data_migration_types",
        ":security_types",
        ":types",
        "//src/v/cluster_link/model",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/features",
        "//src/v/serde",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/container:node_hash_map",
    ],
)

redpanda_cc_library(
    name = "controller_log_limiter",
    srcs = [
        "controller_log_limiter.cc",
    ],
    hdrs = [
        "controller_log_limiter.h",
    ],
    implementation_deps = [
        "//src/v/ssx:sformat",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        "//src/v/metrics",
        "//src/v/utils:token_bucket",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "health_monitor_types",
    srcs = [
        "health_monitor_types.cc",
    ],
    hdrs = [
        "health_monitor_types.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/features",
        "//src/v/utils:to_string",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":drain_status",
        ":errc",
        ":node_types",
        ":types",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/rpc",
        "//src/v/serde",
        "//src/v/serde:bool_class",
        "//src/v/serde:optional",
        "//src/v/serde:vector",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/container:node_hash_map",
        "@abseil-cpp//absl/container:node_hash_set",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "members_table",
    srcs = [
        "members_table.cc",
    ],
    hdrs = [
        "members_table.h",
    ],
    implementation_deps = [
        ":controller_snapshot",
        ":errc",
        ":logger",
        "//src/v/base",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        ":notification",
        ":types",
        "//src/v/model",
        "//src/v/utils:waiter_queue",
        "@abseil-cpp//absl/container:node_hash_map",
    ],
)

redpanda_cc_library(
    name = "feature_backend",
    srcs = [
        "feature_backend.cc",
    ],
    hdrs = [
        "feature_backend.h",
    ],
    implementation_deps = [
        ":controller_snapshot",
        ":logger",
        "//src/v/config",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        ":fwd",
        "//src/v/features",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rpc_utils",
    srcs = [
        "rpc_utils.cc",
    ],
    hdrs = [
        "rpc_utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/config",
        "//src/v/model",
        "//src/v/model:fips_config",
        "//src/v/net",
        "//src/v/rpc",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cluster_utils",
    srcs = [
        "cluster_utils.cc",
    ],
    hdrs = [
        "cluster_utils.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/base",
        "//src/v/rpc",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        ":members_table",
        ":types",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/utils:functional",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cluster_recovery_table",
    srcs = [
        "cluster_recovery_table.cc",
    ],
    hdrs = [
        "cluster_recovery_table.h",
    ],
    implementation_deps = [
        ":logger",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cloud_metadata_cluster_manifest",
        ":cluster_recovery_state",
        ":commands",
        ":controller_snapshot",
        ":types",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "client_quota_store",
    srcs = [
        "client_quota_store.cc",
    ],
    hdrs = [
        "client_quota_store.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":client_quota_serde",
        ":controller_snapshot",
        "//src/v/container:chunked_vector",
        "@abseil-cpp//absl/container:node_hash_map",
    ],
)

redpanda_cc_library(
    name = "producer_state",
    srcs = [
        "producer_state.cc",
    ],
    hdrs = [
        "producer_state.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/base",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":namespaced_cache",
        ":rm_stm_types",
        ":types",
        "//src/v/container:intrusive",
        "//src/v/model",
        "//src/v/ssx:mutex",
        "//src/v/utils:prefix_logger",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_properties_stm",
    srcs = [
        "partition_properties_stm.cc",
    ],
    hdrs = [
        "partition_properties_stm.h",
    ],
    implementation_deps = [
        ":errc",
        ":logger",
        ":snapshot",
        ":types",
        "//src/v/base",
        "//src/v/storage",
        "@fmt",
        "@seastar",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":state_machine_registry",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "prefix_truncate_record",
    hdrs = [
        "prefix_truncate_record.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "ephemeral_credential_serde",
    hdrs = [
        "ephemeral_credential_serde.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        "//src/v/security",
        "//src/v/serde",
        "//src/v/serde:enum",
    ],
)

redpanda_cc_library(
    name = "log_eviction_stm",
    srcs = [
        "log_eviction_stm.cc",
    ],
    hdrs = [
        "log_eviction_stm.h",
    ],
    implementation_deps = [
        ":errc",
        ":logger",
        ":prefix_truncate_record",
        "//src/v/serde",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":state_machine_registry",
        "//src/v/base",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "ephemeral_credential_frontend",
    srcs = [
        "ephemeral_credential_frontend.cc",
    ],
    hdrs = [
        "ephemeral_credential_frontend.h",
    ],
    implementation_deps = [
        ":ephemeral_credential_rpc",
        ":ephemeral_credential_serde",
        ":logger",
        "//src/v/random:generators",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        "//src/v/features",
        "//src/v/model",
        "//src/v/rpc",
        "//src/v/security",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "producer_state_manager",
    srcs = [
        "producer_state_manager.cc",
    ],
    hdrs = [
        "producer_state_manager.h",
    ],
    implementation_deps = [
        ":logger",
        ":types",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":namespaced_cache",
        ":producer_state",
        "//src/v/base",
        "//src/v/config",
        "//src/v/metrics",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "client_quota_backend",
    srcs = [
        "client_quota_backend.cc",
    ],
    hdrs = [
        "client_quota_backend.h",
    ],
    implementation_deps = [
        ":client_quota_store",
        ":controller_snapshot",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        ":fwd",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "ephemeral_credential_service",
    srcs = [
        "ephemeral_credential_service.cc",
    ],
    hdrs = [
        "ephemeral_credential_service.h",
    ],
    implementation_deps = [
        ":ephemeral_credential_frontend",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":ephemeral_credential_rpc",
        ":fwd",
    ],
)

redpanda_cc_library(
    name = "topic_table_partition_generator",
    srcs = [
        "topic_table_partition_generator.cc",
    ],
    hdrs = [
        "topic_table_partition_generator.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":topic_table",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "topic_metrics_watcher",
    srcs = [
        "topic_metrics_watcher.cc",
    ],
    hdrs = [
        "topic_metrics_watcher.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/metrics",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":topic_table",
        "//src/v/base",
        "//src/v/config",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "topic_recovery_validator",
    srcs = [
        "topic_recovery_validator.cc",
    ],
    hdrs = [
        "topic_recovery_validator.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/cloud_storage:types",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/cloud_storage",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "id_allocator_stm",
    srcs = [
        "id_allocator_stm.cc",
    ],
    hdrs = [
        "id_allocator_stm.h",
    ],
    implementation_deps = [
        ":logger",
        ":snapshot",
        "//src/v/bytes:iobuf",
        "//src/v/config",
        "//src/v/ssx:future_util",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "@seastar",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":state_machine_registry",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/ssx:mutex",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
)

redpanda_cc_library(
    name = "cloud_metadata_manifest_downloads",
    srcs = [
        "cloud_metadata/manifest_downloads.cc",
    ],
    hdrs = [
        "cloud_metadata/manifest_downloads.h",
    ],
    implementation_deps = [
        ":cloud_metadata_key_utils",
        ":logger",
        "//src/v/cloud_storage",
        "//src/v/utils:uuid",
        "@boost//:uuid",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cloud_metadata_cluster_manifest",
        ":cloud_metadata_types",
        "//src/v/base",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "inventory_service",
    srcs = [
        "inventory_service.cc",
    ],
    hdrs = [
        "inventory_service.h",
    ],
    implementation_deps = [
        "//src/v/cloud_storage:logger",
        "//src/v/config",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":partition_leaders_table",
        "//src/v/cloud_storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "tx_helpers",
    srcs = [
        "tx_helpers.cc",
    ],
    hdrs = [
        "tx_helpers.h",
    ],
    implementation_deps = [
        ":logger",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "tm_stm",
    srcs = [
        "tm_stm.cc",
    ],
    hdrs = [
        "tm_stm.h",
    ],
    implementation_deps = [
        ":snapshot",
        ":types",
        "//src/v/base",
        "//src/v/kafka/protocol",
        "//src/v/ssx:future_util",
        "//src/v/storage:record_batch_builder",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":logger",
        ":state_machine_registry",
        ":tm_stm_types",
        ":tx_hash_ranges",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/ssx:mutex",
        "//src/v/storage",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "data_migration_table",
    srcs = [
        "data_migration_table.cc",
    ],
    hdrs = [
        "data_migration_table.h",
    ],
    implementation_deps = [
        ":controller_snapshot",
        ":logger",
        "//src/v/model",
        "@seastar",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        ":data_migrated_resources",
        ":data_migration_types",
        ":errc",
        ":topic_table",
        "//src/v/container:chunked_vector",
        "//src/v/utils:named_type",
        "//src/v/utils:notification_list",
        "@abseil-cpp//absl/container:node_hash_map",
    ],
)

redpanda_cc_library(
    name = "plugin_backend",
    srcs = [
        "plugin_backend.cc",
    ],
    hdrs = [
        "plugin_backend.h",
    ],
    implementation_deps = [
        ":controller_snapshot",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        ":fwd",
        ":plugin_table",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "remote_topic_configuration_source",
    srcs = [
        "remote_topic_configuration_source.cc",
    ],
    hdrs = [
        "remote_topic_configuration_source.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/model",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        ":types",
        "//src/v/cloud_storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "scheduling_allocation",
    srcs = [
        "scheduling/allocation_node.cc",
        "scheduling/allocation_state.cc",
        "scheduling/allocation_strategy.cc",
        "scheduling/constraints.cc",
        "scheduling/partition_allocator.cc",
        "scheduling/types.cc",
    ],
    hdrs = [
        "scheduling/allocation_node.h",
        "scheduling/allocation_state.h",
        "scheduling/allocation_strategy.h",
        "scheduling/constraints.h",
        "scheduling/partition_allocator.h",
        "scheduling/types.h",
    ],
    implementation_deps = [
        ":cluster_utils",
        ":controller_snapshot",
        ":errc",
        ":logger",
        ":topic_memory_per_partition_default",
        "//src/v/random:generators",
        "//src/v/resource_mgmt:memory_groups",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:sformat",
        "//src/v/utils:exceptions",
        "//src/v/utils:human",
        "//src/v/utils:to_string",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":members_table",
        ":partition_balancer_types",
        ":types",
        "//src/v/base",
        "//src/v/config",
        "//src/v/features",
        "//src/v/model",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:node_hash_map",
        "@abseil-cpp//absl/container:node_hash_set",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "node_local_monitor",
    srcs = [
        "node/local_monitor.cc",
    ],
    hdrs = [
        "node/local_monitor.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/ssx:future_util",
        "//src/v/storage:disk",
        "//src/v/utils:human",
        "//src/v/version",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":node_types",
        "//src/v/base",
        "//src/v/config",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "node_status_backend",
    srcs = [
        "node_status_backend.cc",
    ],
    hdrs = [
        "node_status_backend.h",
    ],
    implementation_deps = [
        ":errc",
        ":logger",
        ":members_table",
        ":node_status_rpc",
        "//src/v/base",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":node_status_rpc_types",
        ":node_status_table",
        ":notification",
        "//src/v/config",
        "//src/v/features",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/rpc",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "node_status_rpc_handler",
    srcs = [
        "node_status_rpc_handler.cc",
    ],
    hdrs = [
        "node_status_rpc_handler.h",
    ],
    implementation_deps = [
        ":node_status_backend",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":node_status_rpc",
        ":node_status_rpc_types",
    ],
)

redpanda_cc_library(
    name = "cluster_link_table",
    srcs = [
        "cluster_link/table.cc",
    ],
    hdrs = [
        "cluster_link/table.h",
    ],
    implementation_deps = [
        ":commands",
        ":errc",
        ":logger",
        ":table_utils",
        "//src/v/base",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cluster_link_errc",
        ":controller_snapshot",
        "//src/v/cluster_link/model",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "topic_validators",
    hdrs = [
        "topic_validators.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
    ],
)

redpanda_cc_library(
    name = "self_test",
    srcs = [
        "self_test/cloudcheck.cc",
        "self_test/diskcheck.cc",
        "self_test/netcheck.cc",
        "self_test_backend.cc",
        "self_test_frontend.cc",
        "self_test_rpc_handler.cc",
    ],
    hdrs = [
        "self_test/cloudcheck.h",
        "self_test/diskcheck.h",
        "self_test/metrics.h",
        "self_test/netcheck.h",
        "self_test_backend.h",
        "self_test_frontend.h",
        "self_test_rpc_handler.h",
    ],
    implementation_deps = [
        ":logger",
        ":members_table",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/json",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "//src/v/utils:directory_walker",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:range",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":node_local_monitor",
        ":self_test_rpc",
        ":self_test_rpc_types",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_storage",
        "//src/v/model",
        "//src/v/rpc",
        "//src/v/ssx:abort_source",
        "//src/v/ssx:mutex",
        "//src/v/utils:hdr_hist",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:uuid",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "topic_table",
    srcs = [
        "topic_table.cc",
        "topic_table_probe.cc",
    ],
    hdrs = [
        "topic_table.h",
    ],
    implementation_deps = [
        ":cluster_utils",
        ":controller_snapshot",
        ":data_migrated_resources",
        ":data_migration_types",
        ":errc",
        ":topic_validators",
        ":types",
        "//src/v/base",
        "//src/v/config",
        "//src/v/metrics",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/storage",
        "//src/v/utils:uuid",
        "@seastar",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        ":fwd",
        ":logger",
        ":notification",
        ":topic_table_probe",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:contiguous_range_map",
        "//src/v/model",
        "//src/v/utils:stable_iterator",
        "@abseil-cpp//absl/container:node_hash_map",
    ],
)

redpanda_cc_library(
    name = "partition_leaders_table",
    srcs = [
        "partition_leaders_table.cc",
    ],
    hdrs = [
        "partition_leaders_table.h",
    ],
    implementation_deps = [
        ":logger",
        ":topic_table",
        "//src/v/utils:expiring_promise",
        "@abseil-cpp//absl/container:btree",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":health_monitor_types",
        ":ntp_callbacks",
        ":types",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/container:contiguous_range_map",
        "//src/v/model",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:mutex",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cluster",
    srcs = [
        "archival/adjacent_segment_merger.cc",
        "archival/adjacent_segment_run.cc",
        "archival/archival_metadata_stm.cc",
        "archival/archival_policy.cc",
        "archival/archiver_manager.cc",
        "archival/async_data_uploader.cc",
        "archival/ntp_archiver_service.cc",
        "archival/probe.cc",
        "archival/purger.cc",
        "archival/replica_state_validator.cc",
        "archival/retention_calculator.cc",
        "archival/scrubber.cc",
        "archival/segment_reupload.cc",
        "archival/types.cc",
        "archival/upload_controller.cc",
        "archival/upload_housekeeping_service.cc",
        "bootstrap_backend.cc",
        "bootstrap_service.cc",
        "client_quota_frontend.cc",
        "cloud_metadata/cluster_recovery_backend.cc",
        "cloud_metadata/producer_id_recovery_manager.cc",
        "cloud_metadata/uploader.cc",
        "cloud_storage_size_reducer.cc",
        "cluster_discovery.cc",
        "cluster_epoch_service.cc",
        "cluster_link/frontend.cc",
        "cluster_recovery_manager.cc",
        "cluster_recovery_reconciler.cc",
        "config_frontend.cc",
        "config_manager.cc",
        "controller.cc",
        "controller_api.cc",
        "controller_backend.cc",
        "controller_forced_reconfiguration_manager.cc",
        "controller_probe.cc",
        "controller_stm.cc",
        "controller_utils.cc",
        "crash_reporter.cc",
        "data_migration_backend.cc",
        "data_migration_frontend.cc",
        "data_migration_irpc_frontend.cc",
        "data_migration_router.cc",
        "data_migration_service_handler.cc",
        "data_migration_worker.cc",
        "drain_manager.cc",
        "feature_barrier.cc",
        "feature_manager.cc",
        "health_manager.cc",
        "health_monitor_backend.cc",
        "health_monitor_frontend.cc",
        "id_allocator.cc",
        "id_allocator_frontend.cc",
        "members_backend.cc",
        "members_frontend.cc",
        "members_manager.cc",
        "metadata_cache.cc",
        "metadata_dissemination_handler.cc",
        "metadata_dissemination_service.cc",
        "metrics_reporter.cc",
        "migrations/tx_manager_migrator.cc",
        "node_isolation_watcher.cc",
        "partition.cc",
        "partition_balancer_backend.cc",
        "partition_balancer_planner.cc",
        "partition_balancer_rpc_handler.cc",
        "partition_balancer_state.cc",
        "partition_kafka_offsets.cc",
        "partition_manager.cc",
        "partition_probe.cc",
        "partition_recovery_manager.cc",
        "plugin_frontend.cc",
        "rm_partition_frontend.cc",
        "rm_stm.cc",
        "scheduling/leader_balancer.cc",
        "security_frontend.cc",
        "security_manager.cc",
        "service.cc",
        "shard_balancer.cc",
        "shard_placement_table.cc",
        "topic_recovery_service.cc",
        "topic_recovery_status_frontend.cc",
        "topic_recovery_status_rpc_handler.cc",
        "topic_recovery_status_types.cc",
        "topic_updates_dispatcher.cc",
        "topics_frontend.cc",
        "tx_gateway.cc",
        "tx_gateway_frontend.cc",
        "tx_topic_manager.cc",
    ],
    hdrs = [
        "archival/adjacent_segment_merger.h",
        "archival/adjacent_segment_run.h",
        "archival/archival_metadata_stm.h",
        "archival/archival_policy.h",
        "archival/archiver_manager.h",
        "archival/async_data_uploader.h",
        "archival/fwd.h",
        "archival/logger.h",
        "archival/ntp_archiver_service.h",
        "archival/probe.h",
        "archival/purger.h",
        "archival/replica_state_validator.h",
        "archival/retention_calculator.h",
        "archival/scrubber.h",
        "archival/scrubber_scheduler.h",
        "archival/segment_reupload.h",
        "archival/types.h",
        "archival/upload_controller.h",
        "archival/upload_housekeeping_service.h",
        "bootstrap_backend.h",
        "bootstrap_service.h",
        "client_quota_frontend.h",
        "cloud_metadata/cluster_recovery_backend.h",
        "cloud_metadata/offsets_recovery_rpc_types.h",
        "cloud_metadata/offsets_upload_rpc_types.h",
        "cloud_metadata/producer_id_recovery_manager.h",
        "cloud_metadata/uploader.h",
        "cloud_storage_size_reducer.h",
        "cluster_bootstrap_service.h",
        "cluster_discovery.h",
        "cluster_epoch_service.h",
        "cluster_link/frontend.h",
        "cluster_link/fwd.h",
        "cluster_recovery_manager.h",
        "cluster_recovery_reconciler.h",
        "config_frontend.h",
        "config_manager.h",
        "controller.h",
        "controller_api.h",
        "controller_backend.h",
        "controller_forced_reconfiguration_manager.h",
        "controller_probe.h",
        "controller_service.h",
        "controller_stm.h",
        "controller_utils.h",
        "crash_reporter.h",
        "data_migration_backend.h",
        "data_migration_frontend.h",
        "data_migration_irpc_frontend.h",
        "data_migration_router.h",
        "data_migration_rpc_service.h",
        "data_migration_service_handler.h",
        "data_migration_worker.h",
        "drain_manager.h",
        "ephemeral_credential_rpc_service.h",
        "feature_barrier.h",
        "feature_manager.h",
        "health_manager.h",
        "health_monitor_backend.h",
        "health_monitor_frontend.h",
        "id_allocator.h",
        "id_allocator_frontend.h",
        "id_allocator_service.h",
        "leader_router.h",
        "members_backend.h",
        "members_frontend.h",
        "members_manager.h",
        "metadata_cache.h",
        "metadata_dissemination_handler.h",
        "metadata_dissemination_rpc_service.h",
        "metadata_dissemination_service.h",
        "metadata_dissemination_types.h",
        "metrics_reporter.h",
        "migrations/tx_manager_migrator.h",
        "migrations/tx_manager_migrator_handler.h",
        "migrations/tx_manager_migrator_types.h",
        "node_isolation_watcher.h",
        "node_status_rpc_service.h",
        "offsets_recovery_rpc_service.h",
        "partition.h",
        "partition_balancer_backend.h",
        "partition_balancer_planner.h",
        "partition_balancer_rpc_handler.h",
        "partition_balancer_rpc_service.h",
        "partition_balancer_state.h",
        "partition_kafka_offsets.h",
        "partition_manager.h",
        "partition_probe.h",
        "partition_recovery_manager.h",
        "plugin_frontend.h",
        "raft0_utils.h",
        "rm_partition_frontend.h",
        "rm_stm.h",
        "scheduling/leader_balancer.h",
        "security_frontend.h",
        "security_manager.h",
        "service.h",
        "shard_balancer.h",
        "shard_placement_table.h",
        "topic_recovery_service.h",
        "topic_recovery_status_frontend.h",
        "topic_recovery_status_rpc_handler.h",
        "topic_recovery_status_rpc_service.h",
        "topic_recovery_status_types.h",
        "topic_updates_dispatcher.h",
        "topics_frontend.h",
        "tx_coordinator_mapper.h",
        "tx_gateway.h",
        "tx_gateway_frontend.h",
        "tx_gateway_service.h",
        "tx_topic_manager.h",
        "tx_utils.h",
    ],
    implementation_deps = [
        ":table_utils",
        ":topic_memory_per_partition_default",
        "//src/v/cloud_storage:logger",
        "//src/v/cloud_topics/read_replica:stm",
        "//src/v/features:enterprise_feature_messages",
        "//src/v/resource_mgmt:memory_groups",
        "//src/v/ssx:abort_source",
        "//src/v/ssx:work_queue",
        "//src/v/utils:backoff_policy",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":bootstrap_rpc",
        ":bootstrap_types",
        ":client_quota_backend",
        ":client_quota_serde",
        ":client_quota_store",
        ":cloud_metadata_cluster_manifest",
        ":cloud_metadata_key_utils",
        ":cloud_metadata_manifest_downloads",
        ":cloud_metadata_types",
        ":cluster_link_errc",
        ":cluster_link_rpc_types",
        ":cluster_link_table",
        ":cluster_link_types",
        ":cluster_recovery_state",
        ":cluster_recovery_table",
        ":cluster_utils",
        ":cluster_uuid",
        ":commands",
        ":controller_log_limiter",
        ":controller_snapshot",
        ":data_migrated_resources",
        ":data_migration_group_proxy",
        ":data_migration_table",
        ":data_migration_types",
        ":data_migrations_rpc",
        ":distributed_kv_stm",
        ":drain_status",
        ":ephemeral_credential_frontend",
        ":ephemeral_credential_rpc",
        ":ephemeral_credential_serde",
        ":ephemeral_credential_service",
        ":errc",
        ":feature_backend",
        ":features",
        ":fwd",
        ":health_monitor_types",
        ":id_allocator_rpc",
        ":id_allocator_stm",
        ":inventory_service",
        ":leader_balancer_strategy",
        ":log_eviction_stm",
        ":logger",
        ":members_table",
        ":metadata_rpc",
        ":namespaced_cache",
        ":node_local_monitor",
        ":node_status_backend",
        ":node_status_rpc",
        ":node_status_rpc_handler",
        ":node_status_rpc_types",
        ":node_status_table",
        ":node_types",
        ":notification",
        ":notification_latch",
        ":nt_revision",
        ":ntp_callbacks",
        ":offsets_recovery_rpc",
        ":offsets_snapshot",
        ":partition_balancer_rpc",
        ":partition_balancer_types",
        ":partition_leaders_table",
        ":partition_properties_stm",
        ":plugin_backend",
        ":plugin_rpc_types",
        ":plugin_table",
        ":prefix_truncate_record",
        ":producer_state",
        ":producer_state_manager",
        ":remote_topic_configuration_source",
        ":rm_group_proxy",
        ":rm_stm_types",
        ":rpc_utils",
        ":run_quota",
        ":scheduling_allocation",
        ":security_types",
        ":self_test",
        ":self_test_rpc",
        ":self_test_rpc_types",
        ":shard_table",
        ":simple_batch_builder",
        ":snapshot",
        ":state_machine_registry",
        ":tm_stm",
        ":tm_stm_types",
        ":topic_configuration",
        ":topic_metrics_watcher",
        ":topic_recovery_status_rpc",
        ":topic_recovery_validator",
        ":topic_rules",
        ":topic_table",
        ":topic_table_partition_generator",
        ":topic_table_probe",
        ":topic_validators",
        ":tx_errc",
        ":tx_gateway_rpc",
        ":tx_hash_ranges",
        ":tx_helpers",
        ":tx_manager_migrator_rpc",
        ":tx_protocol_types",
        ":types",
        ":version",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_io:cache",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:remote_label",
        "//src/v/cloud_storage:topic_mount_manifest_path",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:state_accessors",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:retry",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_api",
        "//src/v/cluster_link/model",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/container:contiguous_range_map",
        "//src/v/container:intrusive",
        "//src/v/crash_tracker",
        "//src/v/features",
        "//src/v/features:enterprise_features",
        "//src/v/finjector",
        "//src/v/hashing:murmur",
        "//src/v/hashing:secure",
        "//src/v/hashing:xx",
        "//src/v/http",
        "//src/v/json",
        "//src/v/kafka/data:log_reader_config",
        "//src/v/kafka/protocol",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/model:fips_config",
        "//src/v/net",
        "//src/v/net:tls",
        "//src/v/net:types",
        "//src/v/pandaproxy/schema_registry:config",
        "//src/v/pandaproxy/schema_registry:fwd",
        "//src/v/pandaproxy/schema_registry:subject_name_strategy",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/random:time_jitter",
        "//src/v/reflection:adl",
        "//src/v/rpc",
        "//src/v/security",
        "//src/v/security:license",
        "//src/v/serde",
        "//src/v/serde:bool_class",
        "//src/v/serde:chrono",
        "//src/v/serde:enum",
        "//src/v/serde:inet_address",
        "//src/v/serde:iobuf",
        "//src/v/serde:map",
        "//src/v/serde:named_type",
        "//src/v/serde:optional",
        "//src/v/serde:set",
        "//src/v/serde:sstring",
        "//src/v/serde:tristate",
        "//src/v/serde:uuid",
        "//src/v/serde:variant",
        "//src/v/serde:vector",
        "//src/v/ssx:abort_source",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:async_clear",
        "//src/v/ssx:checkpoint_mutex",
        "//src/v/ssx:event",
        "//src/v/ssx:future_util",
        "//src/v/ssx:minimum_interval_timer",
        "//src/v/ssx:mutex",
        "//src/v/ssx:rate_limited_function",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:sformat",
        "//src/v/ssx:single_sharded",
        "//src/v/ssx:sleep_abortable",
        "//src/v/ssx:when_all",
        "//src/v/storage",
        "//src/v/storage:disk",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage:record_batch_utils",
        "//src/v/strings:string_switch",
        "//src/v/strings:utf8",
        "//src/v/utils:adjustable_semaphore",
        "//src/v/utils:auto_fmt",
        "//src/v/utils:available_promise",
        "//src/v/utils:directory_walker",
        "//src/v/utils:exceptions",
        "//src/v/utils:execution_monitor",
        "//src/v/utils:expiring_promise",
        "//src/v/utils:file_io",
        "//src/v/utils:fixed_string",
        "//src/v/utils:hdr_hist",
        "//src/v/utils:human",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:move_canary",
        "//src/v/utils:moving_average",
        "//src/v/utils:named_type",
        "//src/v/utils:notification_list",
        "//src/v/utils:prefix_logger",
        "//src/v/utils:retry",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:stable_iterator",
        "//src/v/utils:stream_provider",
        "//src/v/utils:stream_utils",
        "//src/v/utils:to_string",
        "//src/v/utils:token_bucket",
        "//src/v/utils:tristate",
        "//src/v/utils:unresolved_address",
        "//src/v/utils:uuid",
        "//src/v/utils:waiter_queue",
        "//src/v/version",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/container:node_hash_map",
        "@abseil-cpp//absl/container:node_hash_set",
        "@abseil-cpp//absl/hash",
        "@abseil-cpp//absl/strings",
        "@boost//:algorithm",
        "@boost//:circular_buffer",
        "@boost//:intrusive",
        "@boost//:iterator",
        "@boost//:lexical_cast",
        "@boost//:msm",
        "@boost//:outcome",
        "@boost//:random",
        "@boost//:range",
        "@boost//:uuid",
        "@fmt",
        "@rapidjson",
        "@roaring",
        "@seastar",
    ],
)
