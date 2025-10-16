load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "aip_test_message_proto",
    srcs = ["aip_test_message.proto"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
        "@protobuf//:duration_proto",
        "@protobuf//:timestamp_proto",
    ],
)

redpanda_proto_library(
    name = "aip_test_message_redpanda_proto",
    protos = [":aip_test_message_proto"],
    visibility = ["//visibility:public"],
    deps = ["@abseil-cpp//absl/time:time"],
)

redpanda_test_cc_library(
    name = "aip_test_message_helpers",
    hdrs = ["aip_test_message_helpers.h"],
    deps = [
        ":aip_test_message_redpanda_proto",
        "@abseil-cpp//absl/time",
    ],
)

redpanda_cc_gtest(
    name = "aip_filter_test",
    timeout = "short",
    srcs = [
        "aip_filter_test.cc",
    ],
    deps = [
        ":aip_test_message_helpers",
        ":aip_test_message_redpanda_proto",
        "//src/v/redpanda/admin:aip_filter",
        "//src/v/serde/protobuf:rpc",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/time",
        "@fmt",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "aip_ordering_test",
    timeout = "short",
    srcs = [
        "aip_ordering_test.cc",
    ],
    deps = [
        ":aip_test_message_helpers",
        ":aip_test_message_redpanda_proto",
        "//src/v/redpanda/admin:aip_ordering",
        "//src/v/serde/protobuf:rpc",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/time",
        "@fmt",
        "@googletest//:gtest",
    ],
)
