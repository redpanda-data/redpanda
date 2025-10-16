load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel:golden.bzl", "redpanda_golden_test")
load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "two_proto",
    srcs = ["two.proto"],
)

cc_proto_library(
    name = "two_cc_proto",
    deps = [":two_proto"],
)

proto_library(
    name = "three_proto",
    srcs = ["three.proto"],
)

cc_proto_library(
    name = "three_cc_proto",
    deps = [":three_proto"],
)

proto_library(
    name = "test_messages_edition2023_proto",
    srcs = ["test_messages_edition2023.proto"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
    ],
)

cc_proto_library(
    name = "test_messages_edition2023_cc_proto",
    deps = [":test_messages_edition2023_proto"],
)

redpanda_proto_library(
    name = "test_messages_edition2023_redpanda_proto",
    protos = [":test_messages_edition2023_proto"],
)

proto_library(
    name = "codegen_test_proto",
    srcs = ["codegen_test.proto"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
        "@protobuf//:duration_proto",
        "@protobuf//:field_mask_proto",
        "@protobuf//:timestamp_proto",
    ],
)

cc_proto_library(
    name = "codegen_test_cc_proto",
    deps = [":codegen_test_proto"],
)

redpanda_proto_library(
    name = "codegen_test_redpanda_proto",
    protos = [":codegen_test_proto"],
    deps = [
        "//src/v/serde/protobuf:field_mask",
        "@abseil-cpp//absl/time",
    ],
)

redpanda_cc_gtest(
    name = "parser_test",
    timeout = "short",
    srcs = [
        "parser_test.cc",
    ],
    cpu = 1,
    memory = "128MiB",
    deps = [
        ":test_messages_edition2023_cc_proto",
        ":three_cc_proto",
        ":two_cc_proto",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/random:generators",
        "//src/v/serde/protobuf",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@libprotobuf_mutator",
        "@protobuf",
        "@protobuf//:differencer",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "data_generator",
    srcs = [
        "data_generator.cc",
    ],
    hdrs = [
        "data_generator.h",
    ],
    implementation_deps = [
        "//src/v/random:generators",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@protobuf",
    ],
)

redpanda_cc_gtest(
    name = "wire_format_test",
    timeout = "short",
    srcs = [
        "wire_format_test.cc",
    ],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/serde/protobuf:wire_format",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "round_trip_test",
    timeout = "short",
    srcs = [
        "round_trip_test.cc",
    ],
    cpu = 1,
    memory = "128MiB",
    deps = [
        ":codegen_test_cc_proto",
        ":codegen_test_redpanda_proto",
        ":test_messages_edition2023_cc_proto",
        ":test_messages_edition2023_redpanda_proto",
        "//proto/redpanda/core/testing:example_cc_proto",
        "//proto/redpanda/core/testing:example_redpanda_proto",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/serde/protobuf:json",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/strings",
        "@fmt",
        "@googletest//:gtest",
        "@libprotobuf_mutator",
        "@protobuf//:differencer",
        "@protobuf//:json",
    ],
)

redpanda_cc_gtest(
    name = "field_mask_test",
    timeout = "short",
    srcs = [
        "field_mask_test.cc",
    ],
    cpu = 1,
    memory = "128MiB",
    deps = [
        ":codegen_test_cc_proto",
        ":codegen_test_redpanda_proto",
        ":test_messages_edition2023_cc_proto",
        ":test_messages_edition2023_redpanda_proto",
        "//src/v/serde/protobuf:field_mask",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "reflection_test",
    timeout = "short",
    srcs = [
        "reflection_test.cc",
    ],
    cpu = 1,
    memory = "128MiB",
    deps = [
        ":codegen_test_cc_proto",
        ":codegen_test_redpanda_proto",
        ":test_messages_edition2023_cc_proto",
        ":test_messages_edition2023_redpanda_proto",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
    ],
)

redpanda_golden_test(
    name = "protogen_golden_conformance_test",
    srcs = {
        "test_messages_edition2023.proto.cc": ":test_messages_edition2023_redpanda_proto_generated_srcs",
        "test_messages_edition2023.proto.h": ":test_messages_edition2023_redpanda_proto_generated_hdrs",
        "codegen_test.proto.h": ":codegen_test_redpanda_proto_generated_hdrs",
        "codegen_test.proto.cc": ":codegen_test_redpanda_proto_generated_srcs",
    },
)
