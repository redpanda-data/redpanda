"""
This module contains functions for working with Kafka messages.
"""

load("//bazel:build.bzl", "redpanda_cc_library")

def redpanda_cc_kafka_message_library(name):
    """
    Generate Redpanda Kafka message library.

    Args:
      name: message specification
    """
    src = name + ".json"
    out_h = name + ".h"
    out_cc = name + ".cc"

    native.genrule(
        name = name + "_genrule",
        srcs = [
            src,
        ],
        outs = [
            out_h,
            out_cc,
        ],
        cmd = "$(location //src/v/kafka/protocol/schemata:generator) $< $(OUTS)",
        tools = [
            "//src/v/kafka/protocol/schemata:generator",
        ],
    )

    redpanda_cc_library(
        name = name,
        srcs = [
            out_cc,
        ],
        hdrs = [
            out_h,
        ],
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
