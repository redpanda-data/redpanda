load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "gmock_iceberg_matchers",
    hdrs = ["gmock_iceberg_matchers.h"],
    deps = [
        "//src/v/iceberg:values",
        "@googletest//:gtest",
    ],
)

proto_library(
    name = "person_proto",
    srcs = ["testdata/person.proto"],
)

proto_library(
    name = "complex_proto",
    srcs = ["testdata/complex.proto"],
    deps = ["@protobuf//:timestamp_proto"],
)

proto_library(
    name = "test_messages_proto",
    srcs = ["testdata/iceberg_ready_test_messages_edition2023.proto"],
)

proto_library(
    name = "not_supported_proto",
    srcs = ["testdata/not_supported.proto"],
)

proto_library(
    name = "proto2_proto",
    srcs = ["testdata/proto2.proto"],
)

cc_proto_library(
    name = "person_cc_proto",
    deps = [":person_proto"],
)

cc_proto_library(
    name = "complex_cc_proto",
    deps = [":complex_proto"],
)

cc_proto_library(
    name = "test_messages_cc_proto",
    deps = [":test_messages_proto"],
)

cc_proto_library(
    name = "not_supported_cc_proto",
    deps = [":not_supported_proto"],
)

cc_proto_library(
    name = "proto2_cc_proto",
    deps = [":proto2_proto"],
)

#TODO: this wrapper can be removed when we completely remove cmake tests support
redpanda_test_cc_library(
    name = "proto_definitions_wrapper",
    hdrs = ["proto_definitions.h"],
    deps = [
        ":complex_cc_proto",
        ":not_supported_cc_proto",
        ":person_cc_proto",
        ":proto2_cc_proto",
        ":test_messages_cc_proto",
    ],
)

redpanda_cc_gtest(
    name = "iceberg_avro_tests",
    timeout = "short",
    srcs = [
        "iceberg_avro_tests.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/iceberg:avro_decimal",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg/conversion:schema_avro",
        "//src/v/iceberg/conversion:values_avro",
        "//src/v/serde/avro/tests:data_generator",
        "//src/v/test_utils:gtest",
        "@avro",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "iceberg_parquet_tests",
    timeout = "short",
    srcs = [
        "iceberg_parquet_tests.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg/conversion:schema_parquet",
        "//src/v/iceberg/conversion:values_parquet",
        "//src/v/iceberg/tests:value_generator",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "iceberg_protobuf_test",
    timeout = "short",
    srcs = [
        "iceberg_protobuf_tests.cc",
    ],
    deps = [
        ":complex_cc_proto",
        ":gmock_iceberg_matchers",
        ":person_cc_proto",
        ":proto2_cc_proto",
        ":proto_definitions_wrapper",
        ":test_messages_cc_proto",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg/conversion:schema_protobuf",
        "//src/v/iceberg/conversion:values_protobuf",
        "//src/v/serde/protobuf",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@protobuf",
        "@protobuf//src/google/protobuf/compiler:importer",
        "@protobuf//src/google/protobuf/io:tokenizer",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "iceberg_json_tests",
    timeout = "short",
    srcs = [
        "iceberg_json_tests.cc",
    ],
    deps = [
        ":gmock_iceberg_matchers",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:values",
        "//src/v/iceberg/conversion:conversion_outcome",
        "//src/v/iceberg/conversion:ir_json",
        "//src/v/iceberg/conversion:schema_json",
        "//src/v/iceberg/conversion:values_json",
        "//src/v/iceberg/conversion/json_schema:frontend",
        "//src/v/iceberg/conversion/json_schema:ir",
        "//src/v/json",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@rapidjson",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "time_rfc3339_tests",
    timeout = "short",
    srcs = [
        "time_rfc3339_tests.cc",
    ],
    deps = [
        "//src/v/iceberg:values",
        "//src/v/iceberg/conversion:conversion_outcome",
        "//src/v/iceberg/conversion:time_rfc3339",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
