"""
This module contains functions for working with Kafka messages.
"""

load("//bazel:build.bzl", "redpanda_cc_library")

visibility(["//src/v/kafka/protocol/..."])

MESSAGES = [
    "add_offsets_to_txn",
    "add_partitions_to_txn",
    "alter_client_quotas",
    "alter_configs",
    "alter_partition_reassignments",
    "api_versions",
    "create_acls",
    "create_partitions",
    "create_topics",
    "delete_acls",
    "delete_groups",
    "delete_records",
    "delete_topics",
    "describe_acls",
    "describe_client_quotas",
    "describe_configs",
    "describe_groups",
    "describe_log_dirs",
    "describe_producers",
    "describe_transactions",
    "end_txn",
    "fetch",
    "find_coordinator",
    "heartbeat",
    "incremental_alter_configs",
    "init_producer_id",
    "join_group",
    "leave_group",
    "list_groups",
    "list_offset",
    "list_partition_reassignments",
    "list_transactions",
    "metadata",
    "offset_commit",
    "offset_delete",
    "offset_fetch",
    "offset_for_leader_epoch",
    "produce",
    "sasl_authenticate",
    "sasl_handshake",
    "sync_group",
    "txn_offset_commit",
]

_MESSAGE_REQUESTS = ["//src/v/kafka/protocol/schemata:" + m + "_request" for m in MESSAGES]
_MESSAGE_RESPONSES = ["//src/v/kafka/protocol/schemata:" + m + "_response" for m in MESSAGES]

MESSAGE_TYPES = _MESSAGE_REQUESTS + _MESSAGE_RESPONSES

def generate_kafka_messages(name = "generate_kafka_messages"):
    """
    Generate Kafka message implementations.

    For each message a library is generated for the request and response types
    of the message.

    Args:
      name: unused
    """
    for message in MESSAGES:
        for ttype in ["request", "response"]:
            name = message + "_" + ttype
            schema = name + ".json"
            header = name + ".h"
            source = name + ".cc"

            native.genrule(
                name = name + "_genrule",
                srcs = [
                    schema,
                ],
                outs = [
                    source,
                    header,
                ],
                cmd = "$(execpath //src/v/kafka/protocol/schemata:generator) $< $(OUTS)",
                tools = [
                    "//src/v/kafka/protocol/schemata:generator",
                ],
            )

            redpanda_cc_library(
                name = name,
                srcs = [source],
                hdrs = [header],
                include_prefix = "kafka/protocol/schemata",
                visibility = ["//visibility:public"],
                deps = [
                    "//src/v/base",
                    "//src/v/container:fragmented_vector",
                    "//src/v/kafka/protocol",
                    "//src/v/model",
                    "//src/v/utils:to_string",
                    "@fmt",
                ],
            )
