load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "coordinator_test",
    timeout = "short",
    srcs = [
        "coordinator_test.cc",
    ],
    deps = [
        "//src/v/kafka/protocol",
        "//src/v/kafka/server/nextgen:coordinator",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
