load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/kafka/protocol:protocol.bzl", "generate_kafka_message_libs")
load("//src/v/kafka/protocol/schemata:generator.bzl", "MESSAGE_TYPES")

package(default_visibility = ["//src/v/kafka/protocol:__subpackages__"])

redpanda_cc_library(
    name = "messages",
    hdrs = [
        "messages.h",
    ],
    include_prefix = "kafka/protocol",
    deps = MESSAGE_TYPES,
)

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
        "timeout.h",
        "topic_properties.h",
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
        ":messages",
        ":protocol",
        "//src/v/utils:vint",
        "@seastar",
    ] + MESSAGE_TYPES,
)

generate_kafka_message_libs()
