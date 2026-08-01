load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("//bazel:test.bzl", "redpanda_cc_gtest")

cc_proto_library(
    name = "shadow_link_cc_proto",
    deps = ["//proto/redpanda/core/admin/v2:shadow_link_proto"],
)

cc_proto_library(
    name = "field_behavior_cc_proto",
    deps = ["@googleapis//google/api:field_behavior_proto"],
)

redpanda_cc_gtest(
    name = "input_only_fields_test",
    timeout = "short",
    srcs = [
        "input_only_fields_test.cc",
    ],
    deps = [
        ":field_behavior_cc_proto",
        ":shadow_link_cc_proto",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@protobuf",
    ],
)

redpanda_cc_gtest(
    name = "converter_test",
    timeout = "short",
    srcs = [
        "converter_test.cc",
    ],
    deps = [
        "//src/v/cluster_link/model",
        "//src/v/crypto",
        "//src/v/redpanda/admin/services/shadow_link:converter",
        "//src/v/test_utils:gtest",
        "//src/v/utils:base64",
        "@googletest//:gtest",
    ],
)
