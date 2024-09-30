load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

proto_library(
    name = "person_proto",
    srcs = ["testdata/person.proto"],
)

proto_library(
    name = "complex_proto",
    srcs = ["testdata/complex.proto"],
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
    defines = ["BAZEL_TEST=1"],
    include_prefix = "datalake/tests",
    deps = [
        ":complex_cc_proto",
        ":not_supported_cc_proto",
        ":person_cc_proto",
        ":proto2_cc_proto",
        ":test_messages_cc_proto",
    ],
)

redpanda_cc_gtest(
    name = "datalake_protobuf_test",
    timeout = "short",
    srcs = [
        "datalake_protobuf_tests.cc",
    ],
    deps = [
        ":complex_cc_proto",
        ":person_cc_proto",
        ":proto2_cc_proto",
        ":proto_definitions_wrapper",
        ":test_messages_cc_proto",
        "//src/v/datalake:schema_protobuf",
        "//src/v/datalake:values_protobuf",
        "//src/v/iceberg:datatypes",
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
