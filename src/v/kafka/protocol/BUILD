load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "protocol",
    srcs = [
        "batch_reader.cc",
        "errors.cc",
        "kafka_batch_adapter.cc",
        "logger.cc",
        "types.cc",
    ],
    hdrs = [
        "batch_consumer.h",
        "batch_reader.h",
        "errors.h",
        "exceptions.h",
        "fwd.h",
        "kafka_batch_adapter.h",
        "legacy_message.h",
        "logger.h",
        "types.h",
        "wire.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/compression",
        "//src/v/hashing:crc32",
        "//src/v/hashing:crc32c",
        "//src/v/model",
        "//src/v/storage:parser_utils",
        "//src/v/storage:record_batch_builder",
        "//src/v/strings:utf8",
        "//src/v/utils:base64",
        "//src/v/utils:named_type",
        "//src/v/utils:to_string",
        "//src/v/utils:vint",
        "@abseil-cpp//absl/container:btree",
        "@boost//:numeric_conversion",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "flex_versions",
    srcs = [
        "flex_versions.cc",
    ],
    hdrs = [
        "flex_versions.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:add_offsets_to_txn_request",
        "//src/v/kafka/protocol/schemata:add_offsets_to_txn_response",
        "//src/v/kafka/protocol/schemata:add_partitions_to_txn_request",
        "//src/v/kafka/protocol/schemata:add_partitions_to_txn_response",
        "//src/v/kafka/protocol/schemata:alter_client_quotas_request",
        "//src/v/kafka/protocol/schemata:alter_client_quotas_response",
        "//src/v/kafka/protocol/schemata:alter_configs_request",
        "//src/v/kafka/protocol/schemata:alter_configs_response",
        "//src/v/kafka/protocol/schemata:alter_partition_reassignments_request",
        "//src/v/kafka/protocol/schemata:alter_partition_reassignments_response",
        "//src/v/kafka/protocol/schemata:api_versions_request",
        "//src/v/kafka/protocol/schemata:api_versions_response",
        "//src/v/kafka/protocol/schemata:create_acls_request",
        "//src/v/kafka/protocol/schemata:create_acls_response",
        "//src/v/kafka/protocol/schemata:create_partitions_request",
        "//src/v/kafka/protocol/schemata:create_partitions_response",
        "//src/v/kafka/protocol/schemata:create_topics_request",
        "//src/v/kafka/protocol/schemata:create_topics_response",
        "//src/v/kafka/protocol/schemata:delete_acls_request",
        "//src/v/kafka/protocol/schemata:delete_acls_response",
        "//src/v/kafka/protocol/schemata:delete_groups_request",
        "//src/v/kafka/protocol/schemata:delete_groups_response",
        "//src/v/kafka/protocol/schemata:delete_records_request",
        "//src/v/kafka/protocol/schemata:delete_records_response",
        "//src/v/kafka/protocol/schemata:delete_topics_request",
        "//src/v/kafka/protocol/schemata:delete_topics_response",
        "//src/v/kafka/protocol/schemata:describe_acls_request",
        "//src/v/kafka/protocol/schemata:describe_acls_response",
        "//src/v/kafka/protocol/schemata:describe_client_quotas_request",
        "//src/v/kafka/protocol/schemata:describe_client_quotas_response",
        "//src/v/kafka/protocol/schemata:describe_configs_request",
        "//src/v/kafka/protocol/schemata:describe_configs_response",
        "//src/v/kafka/protocol/schemata:describe_groups_request",
        "//src/v/kafka/protocol/schemata:describe_groups_response",
        "//src/v/kafka/protocol/schemata:describe_log_dirs_request",
        "//src/v/kafka/protocol/schemata:describe_log_dirs_response",
        "//src/v/kafka/protocol/schemata:describe_producers_request",
        "//src/v/kafka/protocol/schemata:describe_producers_response",
        "//src/v/kafka/protocol/schemata:describe_transactions_request",
        "//src/v/kafka/protocol/schemata:describe_transactions_response",
        "//src/v/kafka/protocol/schemata:end_txn_request",
        "//src/v/kafka/protocol/schemata:end_txn_response",
        "//src/v/kafka/protocol/schemata:fetch_request",
        "//src/v/kafka/protocol/schemata:fetch_response",
        "//src/v/kafka/protocol/schemata:find_coordinator_request",
        "//src/v/kafka/protocol/schemata:find_coordinator_response",
        "//src/v/kafka/protocol/schemata:heartbeat_request",
        "//src/v/kafka/protocol/schemata:heartbeat_response",
        "//src/v/kafka/protocol/schemata:incremental_alter_configs_request",
        "//src/v/kafka/protocol/schemata:incremental_alter_configs_response",
        "//src/v/kafka/protocol/schemata:init_producer_id_request",
        "//src/v/kafka/protocol/schemata:init_producer_id_response",
        "//src/v/kafka/protocol/schemata:join_group_request",
        "//src/v/kafka/protocol/schemata:join_group_response",
        "//src/v/kafka/protocol/schemata:leave_group_request",
        "//src/v/kafka/protocol/schemata:leave_group_response",
        "//src/v/kafka/protocol/schemata:list_groups_request",
        "//src/v/kafka/protocol/schemata:list_groups_response",
        "//src/v/kafka/protocol/schemata:list_offset_request",
        "//src/v/kafka/protocol/schemata:list_offset_response",
        "//src/v/kafka/protocol/schemata:list_partition_reassignments_request",
        "//src/v/kafka/protocol/schemata:list_partition_reassignments_response",
        "//src/v/kafka/protocol/schemata:list_transactions_request",
        "//src/v/kafka/protocol/schemata:list_transactions_response",
        "//src/v/kafka/protocol/schemata:metadata_request",
        "//src/v/kafka/protocol/schemata:metadata_response",
        "//src/v/kafka/protocol/schemata:offset_commit_request",
        "//src/v/kafka/protocol/schemata:offset_commit_response",
        "//src/v/kafka/protocol/schemata:offset_delete_request",
        "//src/v/kafka/protocol/schemata:offset_delete_response",
        "//src/v/kafka/protocol/schemata:offset_fetch_request",
        "//src/v/kafka/protocol/schemata:offset_fetch_response",
        "//src/v/kafka/protocol/schemata:offset_for_leader_epoch_request",
        "//src/v/kafka/protocol/schemata:offset_for_leader_epoch_response",
        "//src/v/kafka/protocol/schemata:produce_request",
        "//src/v/kafka/protocol/schemata:produce_response",
        "//src/v/kafka/protocol/schemata:sasl_authenticate_request",
        "//src/v/kafka/protocol/schemata:sasl_authenticate_response",
        "//src/v/kafka/protocol/schemata:sasl_handshake_request",
        "//src/v/kafka/protocol/schemata:sasl_handshake_response",
        "//src/v/kafka/protocol/schemata:sync_group_request",
        "//src/v/kafka/protocol/schemata:sync_group_response",
        "//src/v/kafka/protocol/schemata:txn_offset_commit_request",
        "//src/v/kafka/protocol/schemata:txn_offset_commit_response",
        "//src/v/utils:vint",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "api_versions",
    hdrs = [
        "api_versions.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:api_versions_request",
        "//src/v/kafka/protocol/schemata:api_versions_response",
    ],
)

redpanda_cc_library(
    name = "join_group",
    hdrs = [
        "join_group.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:join_group_request",
        "//src/v/kafka/protocol/schemata:join_group_response",
    ],
)

redpanda_cc_library(
    name = "metadata",
    hdrs = [
        "metadata.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:metadata_request",
        "//src/v/kafka/protocol/schemata:metadata_response",
    ],
)

redpanda_cc_library(
    name = "produce",
    hdrs = [
        "produce.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:produce_request",
        "//src/v/kafka/protocol/schemata:produce_response",
    ],
)

redpanda_cc_library(
    name = "delete_records",
    hdrs = [
        "delete_records.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:delete_records_request",
        "//src/v/kafka/protocol/schemata:delete_records_response",
    ],
)

redpanda_cc_library(
    name = "sync_group",
    hdrs = [
        "sync_group.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:sync_group_request",
        "//src/v/kafka/protocol/schemata:sync_group_response",
    ],
)

redpanda_cc_library(
    name = "fetch",
    hdrs = [
        "fetch.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:fetch_request",
        "//src/v/kafka/protocol/schemata:fetch_response",
    ],
)

redpanda_cc_library(
    name = "offset_for_leader_epoch",
    hdrs = [
        "offset_for_leader_epoch.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:offset_for_leader_epoch_request",
        "//src/v/kafka/protocol/schemata:offset_for_leader_epoch_response",
    ],
)

redpanda_cc_library(
    name = "offset_commit",
    hdrs = [
        "offset_commit.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:offset_commit_request",
        "//src/v/kafka/protocol/schemata:offset_commit_response",
    ],
)

redpanda_cc_library(
    name = "describe_groups",
    hdrs = [
        "describe_groups.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:describe_groups_request",
        "//src/v/kafka/protocol/schemata:describe_groups_response",
    ],
)

redpanda_cc_library(
    name = "offset_fetch",
    hdrs = [
        "offset_fetch.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:offset_fetch_request",
        "//src/v/kafka/protocol/schemata:offset_fetch_response",
    ],
)

redpanda_cc_library(
    name = "find_coordinator",
    hdrs = [
        "find_coordinator.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:find_coordinator_request",
        "//src/v/kafka/protocol/schemata:find_coordinator_response",
    ],
)

redpanda_cc_library(
    name = "heartbeat",
    hdrs = [
        "heartbeat.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:heartbeat_request",
        "//src/v/kafka/protocol/schemata:heartbeat_response",
    ],
)

redpanda_cc_library(
    name = "sasl_authenticate",
    hdrs = [
        "sasl_authenticate.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:sasl_authenticate_request",
        "//src/v/kafka/protocol/schemata:sasl_authenticate_response",
    ],
)

redpanda_cc_library(
    name = "sasl_handshake",
    hdrs = [
        "sasl_handshake.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:sasl_handshake_request",
        "//src/v/kafka/protocol/schemata:sasl_handshake_response",
    ],
)

redpanda_cc_library(
    name = "leave_group",
    hdrs = [
        "leave_group.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:leave_group_request",
        "//src/v/kafka/protocol/schemata:leave_group_response",
    ],
)

redpanda_cc_library(
    name = "add_offsets_to_txn",
    hdrs = [
        "add_offsets_to_txn.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:add_offsets_to_txn_request",
        "//src/v/kafka/protocol/schemata:add_offsets_to_txn_response",
    ],
)

redpanda_cc_library(
    name = "add_partitions_to_txn",
    hdrs = [
        "add_partitions_to_txn.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:add_partitions_to_txn_request",
        "//src/v/kafka/protocol/schemata:add_partitions_to_txn_response",
    ],
)

redpanda_cc_library(
    name = "alter_configs",
    hdrs = [
        "alter_configs.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:alter_configs_request",
        "//src/v/kafka/protocol/schemata:alter_configs_response",
    ],
)

redpanda_cc_library(
    name = "alter_client_quotas",
    hdrs = [
        "alter_client_quotas.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:alter_client_quotas_request",
        "//src/v/kafka/protocol/schemata:alter_client_quotas_response",
    ],
)

redpanda_cc_library(
    name = "alter_partition_reassignments",
    hdrs = [
        "alter_partition_reassignments.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:alter_partition_reassignments_request",
        "//src/v/kafka/protocol/schemata:alter_partition_reassignments_response",
    ],
)

redpanda_cc_library(
    name = "create_acls",
    hdrs = [
        "create_acls.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:create_acls_request",
        "//src/v/kafka/protocol/schemata:create_acls_response",
    ],
)

redpanda_cc_library(
    name = "create_partitions",
    hdrs = [
        "create_partitions.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:create_partitions_request",
        "//src/v/kafka/protocol/schemata:create_partitions_response",
    ],
)

redpanda_cc_library(
    name = "create_topics",
    hdrs = [
        "create_topics.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:create_topics_request",
        "//src/v/kafka/protocol/schemata:create_topics_response",
    ],
)

redpanda_cc_library(
    name = "delete_acls",
    hdrs = [
        "delete_acls.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:delete_acls_request",
        "//src/v/kafka/protocol/schemata:delete_acls_response",
    ],
)

redpanda_cc_library(
    name = "delete_groups",
    hdrs = [
        "delete_groups.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:delete_groups_request",
        "//src/v/kafka/protocol/schemata:delete_groups_response",
    ],
)

redpanda_cc_library(
    name = "delete_topics",
    hdrs = [
        "delete_topics.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:delete_topics_request",
        "//src/v/kafka/protocol/schemata:delete_topics_response",
    ],
)

redpanda_cc_library(
    name = "describe_acls",
    hdrs = [
        "describe_acls.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:describe_acls_request",
        "//src/v/kafka/protocol/schemata:describe_acls_response",
    ],
)

redpanda_cc_library(
    name = "describe_client_quotas",
    hdrs = [
        "describe_client_quotas.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:describe_client_quotas_request",
        "//src/v/kafka/protocol/schemata:describe_client_quotas_response",
    ],
)

redpanda_cc_library(
    name = "describe_configs",
    hdrs = [
        "describe_configs.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:describe_configs_request",
        "//src/v/kafka/protocol/schemata:describe_configs_response",
    ],
)

redpanda_cc_library(
    name = "describe_log_dirs",
    hdrs = [
        "describe_log_dirs.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:describe_log_dirs_request",
        "//src/v/kafka/protocol/schemata:describe_log_dirs_response",
    ],
)

redpanda_cc_library(
    name = "describe_producers",
    hdrs = [
        "describe_producers.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:describe_producers_request",
        "//src/v/kafka/protocol/schemata:describe_producers_response",
    ],
)

redpanda_cc_library(
    name = "describe_transactions",
    hdrs = [
        "describe_transactions.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:describe_transactions_request",
        "//src/v/kafka/protocol/schemata:describe_transactions_response",
    ],
)

redpanda_cc_library(
    name = "end_txn",
    hdrs = [
        "end_txn.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:end_txn_request",
        "//src/v/kafka/protocol/schemata:end_txn_response",
    ],
)

redpanda_cc_library(
    name = "incremental_alter_configs",
    hdrs = [
        "incremental_alter_configs.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:incremental_alter_configs_request",
        "//src/v/kafka/protocol/schemata:incremental_alter_configs_response",
    ],
)

redpanda_cc_library(
    name = "init_producer_id",
    hdrs = [
        "init_producer_id.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:init_producer_id_request",
        "//src/v/kafka/protocol/schemata:init_producer_id_response",
    ],
)

redpanda_cc_library(
    name = "list_groups",
    hdrs = [
        "list_groups.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:list_groups_request",
        "//src/v/kafka/protocol/schemata:list_groups_response",
    ],
)

redpanda_cc_library(
    name = "list_offsets",
    hdrs = [
        "list_offsets.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:list_offset_request",
        "//src/v/kafka/protocol/schemata:list_offset_response",
    ],
)

redpanda_cc_library(
    name = "list_partition_reassignments",
    hdrs = [
        "list_partition_reassignments.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:list_partition_reassignments_request",
        "//src/v/kafka/protocol/schemata:list_partition_reassignments_response",
    ],
)

redpanda_cc_library(
    name = "list_transactions",
    hdrs = [
        "list_transactions.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:list_transactions_request",
        "//src/v/kafka/protocol/schemata:list_transactions_response",
    ],
)

redpanda_cc_library(
    name = "offset_delete",
    hdrs = [
        "offset_delete.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:offset_delete_request",
        "//src/v/kafka/protocol/schemata:offset_delete_response",
    ],
)

redpanda_cc_library(
    name = "txn_offset_commit",
    hdrs = [
        "txn_offset_commit.h",
    ],
    include_prefix = "kafka/protocol",
    visibility = ["//visibility:public"],
    deps = [
        ":protocol",
        "//src/v/kafka/protocol/schemata:txn_offset_commit_request",
        "//src/v/kafka/protocol/schemata:txn_offset_commit_response",
    ],
)
