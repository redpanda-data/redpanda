load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel:test.bzl", "redpanda_cc_gtest")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "aip_filter_test_messages_proto",
    srcs = ["aip_filter_test_messages.proto"],
    deps = [
        "//proto/redpanda/pbgen:options_proto",
        "@protobuf//:duration_proto",
        "@protobuf//:timestamp_proto",
    ],
)

redpanda_proto_library(
    name = "aip_filter_test_messages_redpanda_proto",
    protos = [":aip_filter_test_messages_proto"],
    visibility = ["//visibility:public"],
    deps = ["@abseil-cpp//absl/time:time"],
)

redpanda_cc_gtest(
    name = "aip_filter_test",
    timeout = "short",
    srcs = [
        "aip_filter_test.cc",
    ],
    deps = [
        ":aip_filter_test_messages_redpanda_proto",
        "//src/v/redpanda/admin:aip_filter",
        "//src/v/redpanda/admin:field_registry",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/time",
        "@googletest//:gtest",
    ],
)
