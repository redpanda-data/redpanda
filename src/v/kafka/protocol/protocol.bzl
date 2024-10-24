"""
This module contains functions for working with Kafka messages.
"""

load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/kafka/protocol/schemata:generator.bzl", "MESSAGES")

MESSAGE_TYPES = ["//src/v/kafka/protocol:" + m for m in MESSAGES]

def generate_kafka_message_libs(name = "generate_kafka_messages_libs"):
    for message in MESSAGES:
        redpanda_cc_library(
            name = message,
            hdrs = [
                message + ".h",
            ],
            include_prefix = "kafka/protocol",
            visibility = ["//visibility:public"],
            deps = [
                ":protocol",
                "//src/v/kafka/protocol/schemata:" + message + "_request",
                "//src/v/kafka/protocol/schemata:" + message + "_response",
            ],
        )
