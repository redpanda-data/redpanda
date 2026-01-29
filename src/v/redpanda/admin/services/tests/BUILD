load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "security_test",
    timeout = "short",
    srcs = [
        "security_test.cc",
    ],
    deps = [
        "//proto/redpanda/core/admin/v2:security_redpanda_proto",
        "//src/v/redpanda/admin/services:security",
        "//src/v/security",
        "//src/v/serde/protobuf:rpc",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
