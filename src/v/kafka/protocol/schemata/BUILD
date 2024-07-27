load("@rules_python//python:defs.bzl", "py_binary")
load("//src/v/kafka/protocol/schemata:generator.bzl", "redpanda_cc_kafka_message_library")

py_binary(
    name = "generator",
    srcs = ["generator.py"],
    main = "generator.py",
    visibility = ["//visibility:public"],
    deps = [
        "@python_deps//jinja2",
        "@python_deps//jsonschema",
    ],
)

redpanda_cc_kafka_message_library(name = "add_offsets_to_txn_request")

redpanda_cc_kafka_message_library(name = "add_offsets_to_txn_response")

redpanda_cc_kafka_message_library(name = "add_partitions_to_txn_request")

redpanda_cc_kafka_message_library(name = "add_partitions_to_txn_response")

redpanda_cc_kafka_message_library(name = "alter_client_quotas_request")

redpanda_cc_kafka_message_library(name = "alter_client_quotas_response")

redpanda_cc_kafka_message_library(name = "alter_configs_request")

redpanda_cc_kafka_message_library(name = "alter_configs_response")

redpanda_cc_kafka_message_library(name = "alter_partition_reassignments_request")

redpanda_cc_kafka_message_library(name = "alter_partition_reassignments_response")

redpanda_cc_kafka_message_library(name = "api_versions_request")

redpanda_cc_kafka_message_library(name = "api_versions_response")

redpanda_cc_kafka_message_library(name = "create_acls_request")

redpanda_cc_kafka_message_library(name = "create_acls_response")

redpanda_cc_kafka_message_library(name = "create_partitions_request")

redpanda_cc_kafka_message_library(name = "create_partitions_response")

redpanda_cc_kafka_message_library(name = "create_topics_request")

redpanda_cc_kafka_message_library(name = "create_topics_response")

redpanda_cc_kafka_message_library(name = "delete_acls_request")

redpanda_cc_kafka_message_library(name = "delete_acls_response")

redpanda_cc_kafka_message_library(name = "delete_groups_request")

redpanda_cc_kafka_message_library(name = "delete_groups_response")

redpanda_cc_kafka_message_library(name = "delete_records_request")

redpanda_cc_kafka_message_library(name = "delete_records_response")

redpanda_cc_kafka_message_library(name = "delete_topics_request")

redpanda_cc_kafka_message_library(name = "delete_topics_response")

redpanda_cc_kafka_message_library(name = "describe_acls_request")

redpanda_cc_kafka_message_library(name = "describe_acls_response")

redpanda_cc_kafka_message_library(name = "describe_client_quotas_request")

redpanda_cc_kafka_message_library(name = "describe_client_quotas_response")

redpanda_cc_kafka_message_library(name = "describe_configs_request")

redpanda_cc_kafka_message_library(name = "describe_configs_response")

redpanda_cc_kafka_message_library(name = "describe_groups_request")

redpanda_cc_kafka_message_library(name = "describe_groups_response")

redpanda_cc_kafka_message_library(name = "describe_log_dirs_request")

redpanda_cc_kafka_message_library(name = "describe_log_dirs_response")

redpanda_cc_kafka_message_library(name = "describe_producers_request")

redpanda_cc_kafka_message_library(name = "describe_producers_response")

redpanda_cc_kafka_message_library(name = "describe_transactions_request")

redpanda_cc_kafka_message_library(name = "describe_transactions_response")

redpanda_cc_kafka_message_library(name = "end_txn_request")

redpanda_cc_kafka_message_library(name = "end_txn_response")

redpanda_cc_kafka_message_library(name = "fetch_request")

redpanda_cc_kafka_message_library(name = "fetch_response")

redpanda_cc_kafka_message_library(name = "find_coordinator_request")

redpanda_cc_kafka_message_library(name = "find_coordinator_response")

redpanda_cc_kafka_message_library(name = "heartbeat_request")

redpanda_cc_kafka_message_library(name = "heartbeat_response")

redpanda_cc_kafka_message_library(name = "incremental_alter_configs_request")

redpanda_cc_kafka_message_library(name = "incremental_alter_configs_response")

redpanda_cc_kafka_message_library(name = "init_producer_id_request")

redpanda_cc_kafka_message_library(name = "init_producer_id_response")

redpanda_cc_kafka_message_library(name = "join_group_request")

redpanda_cc_kafka_message_library(name = "join_group_response")

redpanda_cc_kafka_message_library(name = "leave_group_request")

redpanda_cc_kafka_message_library(name = "leave_group_response")

redpanda_cc_kafka_message_library(name = "list_groups_request")

redpanda_cc_kafka_message_library(name = "list_groups_response")

redpanda_cc_kafka_message_library(name = "list_offset_request")

redpanda_cc_kafka_message_library(name = "list_offset_response")

redpanda_cc_kafka_message_library(name = "list_partition_reassignments_request")

redpanda_cc_kafka_message_library(name = "list_partition_reassignments_response")

redpanda_cc_kafka_message_library(name = "list_transactions_request")

redpanda_cc_kafka_message_library(name = "list_transactions_response")

redpanda_cc_kafka_message_library(name = "metadata_request")

redpanda_cc_kafka_message_library(name = "metadata_response")

redpanda_cc_kafka_message_library(name = "offset_commit_request")

redpanda_cc_kafka_message_library(name = "offset_commit_response")

redpanda_cc_kafka_message_library(name = "offset_delete_request")

redpanda_cc_kafka_message_library(name = "offset_delete_response")

redpanda_cc_kafka_message_library(name = "offset_fetch_request")

redpanda_cc_kafka_message_library(name = "offset_fetch_response")

redpanda_cc_kafka_message_library(name = "offset_for_leader_epoch_request")

redpanda_cc_kafka_message_library(name = "offset_for_leader_epoch_response")

redpanda_cc_kafka_message_library(name = "produce_request")

redpanda_cc_kafka_message_library(name = "produce_response")

redpanda_cc_kafka_message_library(name = "sasl_authenticate_request")

redpanda_cc_kafka_message_library(name = "sasl_authenticate_response")

redpanda_cc_kafka_message_library(name = "sasl_handshake_request")

redpanda_cc_kafka_message_library(name = "sasl_handshake_response")

redpanda_cc_kafka_message_library(name = "sync_group_request")

redpanda_cc_kafka_message_library(name = "sync_group_response")

redpanda_cc_kafka_message_library(name = "txn_offset_commit_request")

redpanda_cc_kafka_message_library(name = "txn_offset_commit_response")
