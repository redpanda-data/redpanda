load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel:test.bzl", "redpanda_cc_gtest")

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
)

cc_proto_library(
    name = "test_messages_edition2023_cc_proto",
    deps = [":test_messages_edition2023_proto"],
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
        "@googletest//:gtest_main",
        "@libprotobuf_mutator",
        "@protobuf",
        "@protobuf//:differencer",
        "@seastar",
    ],
)
